#ifndef PS_RDMA_VAN_H_
#define PS_RDMA_VAN_H_
#include <rdma/rdma_cma.h> // rdma channel management
#include <rdma/rdma_verbs.h>
#include <netdb.h>
#include <cstdlib>
#include <thread>
#include <string>
#include <cstring> // memset
#include "ps/internal/postoffice.h"
#include "ps/internal/van.h"
#include <fcntl.h>
#include <sys/stat.h>

namespace ps {
// 50 MB
const int BUFFER_SIZE = 1024*1024*50;

class RDMAVan : public Van {
public:
	RDMAVan() {}
	virtual ~RDMAVan() {}

protected:
	enum control_command{
	  MSG_MR = 0,
	  MSG_META,
	  MSG_READY,
	  MSG_DONE
	};
	struct control_message{
		int command;
		struct
		{
		  uint64_t addr;
		  uint32_t rkey;
		} mr;
		int sender_id;
		int assigned_id;
		int sender_role;
	};
	struct connection{
		struct rdma_cm_id *id;
		struct ibv_qp *qp;
		int sender;
		int sender_role;

		char* recv_buffer;
		char* send_buffer;
		struct ibv_mr *recv_buffer_mr;
		struct ibv_mr *send_buffer_mr;

		struct control_message *ctrl_send_msg;
		struct control_message *ctrl_recv_msg;
		struct ibv_mr *ctrl_send_msg_mr;
		struct ibv_mr *ctrl_recv_msg_mr;

		uint64_t peer_addr;
		uint32_t peer_rkey;
	};
 	struct context{
		struct ibv_context *ctx;
		struct ibv_pd *pd;
		struct ibv_cq *cq;
		struct ibv_comp_channel *comp_channel;
	 };

	/* poll completion queue functions */
	void Polling(){
		//PS_VLOG(1) << my_node_.ShortDebugString() << " Polling() start...";
		struct ibv_cq *cq;
		struct ibv_wc wc;
		while(!stop_){
			void *ctx;
			CHECK_EQ(ibv_get_cq_event(share_ctx_->comp_channel, &cq, &ctx), 0);
			ibv_ack_cq_events(cq, 1);
			CHECK_EQ(ibv_req_notify_cq(cq, 0), 0);

			while (ibv_poll_cq(cq, 1, &wc)) {
				if (wc.status == IBV_WC_SUCCESS){
					if(wc.opcode == IBV_WC_RECV_RDMA_WITH_IMM){
						std::lock_guard<std::mutex> lk(cqes_message_mu_);
						cqes_message_.push_back(wc);
					}else if(wc.opcode & IBV_WC_RECV){
						std::lock_guard<std::mutex> lk(cqes_ctrl_mu_);
						cqes_ctrl_.push_back(wc);
					}else{
						continue;
					}
				}
				else{
					PS_VLOG(1) <<  my_node_.ShortDebugString() << " wc status not success";
				}
			}
		}
	}
	struct ibv_wc poll_cq_ctrl(struct rdma_cm_id *id){
		ibv_wc ret;
		if(!id){
			while(true){
				{
					std::lock_guard<std::mutex> lk(cqes_ctrl_mu_);
					if(!cqes_ctrl_.empty()){
						PS_VLOG(1) << my_node_.ShortDebugString() <<
								" cqes_ctrl  still has " << cqes_ctrl_.size() << " entries.";
						ret = cqes_ctrl_.front();
						cqes_ctrl_.erase(cqes_ctrl_.begin());
						struct rdma_cm_id *id = (struct rdma_cm_id *)(uintptr_t)ret.wr_id;
						struct connection *conn = conn = (struct connection *)id->context;
						PS_VLOG(1) << my_node_.ShortDebugString() << " receive a wild card ctrl cqe from "
								<< conn->sender;
						return ret;
					}
				}
				std::this_thread::sleep_for(std::chrono::milliseconds(1));
			}
		}else{
			while(true){
				{
					std::lock_guard<std::mutex> lk(cqes_ctrl_mu_);
					for(auto it=cqes_ctrl_.begin(); it!=cqes_ctrl_.end();it++){
						if((*it).wr_id==(uintptr_t)id){
							PS_VLOG(1) << my_node_.ShortDebugString() <<
									" cqes_ctrl still has " << cqes_ctrl_.size() << " entries.";
							ret = *it;
							cqes_ctrl_.erase(it);
							struct rdma_cm_id *id = (struct rdma_cm_id *)(uintptr_t)ret.wr_id;
							struct connection *conn = conn = (struct connection *)id->context;
							PS_VLOG(1) << my_node_.ShortDebugString() << " receive a certain ctrl cqe from "
									<< conn->sender;
							return ret;
						}
					}
				}
				std::this_thread::sleep_for(std::chrono::milliseconds(1));
			}
		}
	}
	struct ibv_wc poll_cq_message(struct rdma_cm_id *id){
		ibv_wc ret;
		if(!id){
			while(true){
				{
					{
						if(send_myself_){
							check_send_myself_ = true;
							return ret;
						}
					}
					std::lock_guard<std::mutex> lk(cqes_message_mu_);
					if(!cqes_message_.empty()){
						ret = cqes_message_.front();
						if(ntohl(ret.imm_data) != 1) {// magic number
							cqes_message_.erase(cqes_message_.begin());
							struct rdma_cm_id *id = (struct rdma_cm_id *)(uintptr_t)ret.wr_id;
							struct connection *conn = conn = (struct connection *)id->context;
							PS_VLOG(1) << my_node_.ShortDebugString() << " receive a wild card message cqe from "
									<< conn->sender;
							return ret;
						}
					}
				}
				std::this_thread::sleep_for(std::chrono::milliseconds(1));
			}
		}else{
			while(true){
				{
					std::lock_guard<std::mutex> lk(cqes_message_mu_);
					for(auto it=cqes_message_.begin(); it!=cqes_message_.end();it++){
						if((*it).wr_id==(uintptr_t)id){
							PS_VLOG(1) << my_node_.ShortDebugString() <<
									" cqes_message_ still has " << cqes_message_.size() << " entries.";
							ret = *it;
							cqes_message_.erase(it);
							struct rdma_cm_id *id = (struct rdma_cm_id *)(uintptr_t)ret.wr_id;
							struct connection *conn = conn = (struct connection *)id->context;
							PS_VLOG(1) << my_node_.ShortDebugString() << " receive a certain messsage cqe from "
									<< conn->sender;
							return ret;
						}
					}
				}
				std::this_thread::sleep_for(std::chrono::milliseconds(1));
			}
		}
	}
	void connection_established(int node_id){ // block until connection established.
		while(true){
			{
				std::lock_guard<std::mutex> lk(senders_mu_);
				auto it = senders_.find(node_id);
				if(it!=senders_.end()){
					return;
				}
			}
			std::this_thread::sleep_for(std::chrono::milliseconds(1));
		}
	}
	/* end poll completion queue functions */

	/* rdma receive functions and send functions */
	void post_receive_message(struct rdma_cm_id *id){
		  struct ibv_recv_wr wr, *bad_wr = NULL;

		  memset(&wr, 0, sizeof(ibv_recv_wr));

		  wr.wr_id = (uintptr_t)id;
		  wr.sg_list = NULL;
		  wr.num_sge = 0;

		  CHECK_EQ(ibv_post_recv(id->qp, &wr, &bad_wr), 0);

		  struct connection *conn = conn = (struct connection *)id->context;
		  PS_VLOG(1) << my_node_.ShortDebugString() << " post_receive_message to  "
			<< conn->sender;
	}
	void post_receive_ctrl(struct rdma_cm_id *id){
		  struct connection *conn = (struct connection *)id->context;

		  struct ibv_recv_wr wr, *bad_wr = NULL;
		  struct ibv_sge sge;

		  memset(&wr, 0, sizeof(ibv_recv_wr));

		  wr.wr_id = (uintptr_t)id;
		  wr.sg_list = &sge;
		  wr.num_sge = 1;

		  sge.addr = (uintptr_t)conn->ctrl_recv_msg;
		  sge.length = sizeof(control_message);
		  sge.lkey = conn->ctrl_recv_msg_mr->lkey;

		  CHECK_EQ(ibv_post_recv(id->qp, &wr, &bad_wr), 0);

		  PS_VLOG(1) << my_node_.ShortDebugString() << " post_receive_ctrl to  "
			<< conn->sender;
	}
	void post_send_message(struct rdma_cm_id *id, int len){
		  struct connection *conn = (struct connection *)id->context;
		  struct ibv_send_wr wr, *bad_wr = NULL;
		  struct ibv_sge sge;

		  memset(&wr, 0, sizeof(ibv_send_wr));

		  wr.wr_id = (uintptr_t)id;
		  wr.opcode = IBV_WR_RDMA_WRITE_WITH_IMM;
		  wr.send_flags = IBV_SEND_SIGNALED;
		  wr.imm_data = htonl(len);
		  wr.wr.rdma.remote_addr = conn->peer_addr;
		  wr.wr.rdma.rkey = conn->peer_rkey;

		  if (len) {
		    wr.sg_list = &sge;
		    wr.num_sge = 1;

		    sge.addr = (uintptr_t)conn->send_buffer;
		    sge.length = len;
		    sge.lkey = conn->send_buffer_mr->lkey;
		  }

		  CHECK_EQ(ibv_post_send(id->qp, &wr, &bad_wr), 0)
		  	  << "ibv_post_send failed. ";
		  PS_VLOG(1) << my_node_.ShortDebugString() << " post_send_message to " << conn->sender;
	}
	void post_send_ctrl(struct rdma_cm_id *id){
		struct connection *conn = (struct connection *)id->context;
		struct ibv_send_wr wr, *bad_wr = NULL;
		struct ibv_sge sge;

		memset(&wr, 0, sizeof(ibv_send_wr));

		wr.wr_id = (uintptr_t)id;
		wr.opcode = IBV_WR_SEND;
		wr.sg_list = &sge;
		wr.num_sge = 1;
		wr.send_flags = IBV_SEND_SIGNALED;

		sge.addr = (uintptr_t)conn->ctrl_send_msg;
		sge.length = sizeof(control_message);
		sge.lkey = conn->ctrl_send_msg_mr->lkey;
		CHECK_EQ(ibv_post_send(id->qp, &wr, &bad_wr), 0);
		PS_VLOG(1) << my_node_.ShortDebugString() << " post_send_ctrl to "<< conn->sender;
	}
	/* end rdma receive functions and send functions */

	void build_context(struct ibv_context *verbs){
		if (share_ctx_) {
			CHECK_EQ(share_ctx_->ctx, verbs)
				<< "Cannot handle events in more than one context.";
			return;
		}
		share_ctx_ = (struct context *)malloc(sizeof(struct context));
		share_ctx_->ctx = verbs;
		CHECK_NOTNULL(share_ctx_->pd = ibv_alloc_pd(share_ctx_->ctx));
		CHECK_NOTNULL(share_ctx_->comp_channel = ibv_create_comp_channel(share_ctx_->ctx));
		CHECK_NOTNULL(share_ctx_->cq = ibv_create_cq(share_ctx_->ctx, 15, NULL, share_ctx_->comp_channel, 0)); /* cqe=10 is arbitrary */
		CHECK_EQ(ibv_req_notify_cq(share_ctx_->cq, 0), 0);
	}
	void build_qp_attr(struct ibv_qp_init_attr *qp_attr){
		memset(qp_attr, 0, sizeof(*qp_attr));

		qp_attr->send_cq = share_ctx_->cq;
		qp_attr->recv_cq = share_ctx_->cq;
		qp_attr->qp_type = IBV_QPT_RC;

		qp_attr->cap.max_send_wr = 10000;
		qp_attr->cap.max_recv_wr = 10000;
		qp_attr->cap.max_send_sge = 10;
		qp_attr->cap.max_recv_sge = 10;
	}
	void register_memory(struct connection *conn){
		posix_memalign((void **)&conn->recv_buffer, sysconf(_SC_PAGESIZE), BUFFER_SIZE);
		posix_memalign((void **)&conn->send_buffer, sysconf(_SC_PAGESIZE), BUFFER_SIZE);
		conn->ctrl_send_msg = (struct control_message *)malloc(sizeof(control_message));
		conn->ctrl_recv_msg = (struct control_message *)malloc(sizeof(control_message));
		CHECK_NOTNULL(conn->recv_buffer_mr = ibv_reg_mr(
			share_ctx_->pd,
			conn->recv_buffer,
			BUFFER_SIZE,
			IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE));
		CHECK_NOTNULL(conn->send_buffer_mr = ibv_reg_mr(
			share_ctx_->pd,
			conn->send_buffer,
			BUFFER_SIZE,
			IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE));
		CHECK_NOTNULL(conn->ctrl_send_msg_mr = ibv_reg_mr(
			share_ctx_->pd,
			conn->ctrl_send_msg,
			sizeof(control_message),
			IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE));
		CHECK_NOTNULL(conn->ctrl_recv_msg_mr = ibv_reg_mr(
			share_ctx_->pd,
			conn->ctrl_recv_msg,
			sizeof(control_message),
			IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE));
	}
	void build_connection(struct rdma_cm_id *id, struct connection *conn){
		struct ibv_qp_init_attr qp_attr;
		build_context(id->verbs);
		build_qp_attr(&qp_attr);
		CHECK_EQ(rdma_create_qp(id, share_ctx_->pd, &qp_attr), 0);
		id->context = conn;
		conn->id = id;
		conn->qp = id->qp;
	}
	void build_params(struct rdma_conn_param *params){
		  memset(params, 0, sizeof(*params));

		  params->initiator_depth = params->responder_resources = 15;
		  params->rnr_retry_count = 7; /* infinite retry */
	}
	void EventLooping(addrinfo *addr){
		struct rdma_cm_event *connect_event;
		struct rdma_cm_id *conn_id;
		struct rdma_event_channel *ec;
		ec = rdma_create_event_channel();
		CHECK_NOTNULL(ec);
		CHECK_EQ(rdma_create_id(ec, &conn_id, NULL, RDMA_PS_TCP),0)
			<< " create listenr failed.";
		CHECK_EQ(rdma_resolve_addr(conn_id, NULL, addr->ai_addr, 500), 0);
		struct connection *conn = (struct connection *)malloc(sizeof(struct connection));
		while (!stop_ && rdma_get_cm_event(ec, &connect_event) == 0) {
			struct rdma_cm_event event_copy;
			memcpy(&event_copy, connect_event, sizeof(*connect_event));
			rdma_ack_cm_event(connect_event);

			if (event_copy.event == RDMA_CM_EVENT_ADDR_RESOLVED){
				//PS_VLOG(1) << my_node_.ShortDebugString() << " address resolved. ";
				build_connection(event_copy.id, conn);
				CHECK_EQ(rdma_resolve_route(event_copy.id, 500), 0);
			}
			else if (event_copy.event == RDMA_CM_EVENT_ROUTE_RESOLVED){
				//PS_VLOG(1) << my_node_.ShortDebugString() << " route resolved. ";
				struct rdma_conn_param cm_params;
				build_params(&cm_params);
				register_memory(conn);
				post_receive_ctrl(event_copy.id);
				if(!poll_thread_){
					poll_thread_ = std::unique_ptr<std::thread>(
							new std::thread(&RDMAVan::Polling, this));
				}
				CHECK_EQ(rdma_connect(event_copy.id, &cm_params), 0);
			}
			else if (event_copy.event == RDMA_CM_EVENT_ESTABLISHED){
				//PS_VLOG(1) << my_node_.ShortDebugString() << " EventLooping side connection established.";
				conn->ctrl_send_msg->command = MSG_MR;
				conn->ctrl_send_msg->sender_id = my_node_.id;
				conn->ctrl_send_msg->mr.addr = (uintptr_t)conn->recv_buffer_mr->addr;
				conn->ctrl_send_msg->mr.rkey = conn->recv_buffer_mr->rkey;
				conn->ctrl_send_msg->sender_role = my_node_.role;
				post_send_ctrl(conn->id);
				// receive mr
				struct ibv_wc wc = poll_cq_ctrl(event_copy.id);
				if(conn->ctrl_recv_msg->command == MSG_MR){
					//PS_VLOG(1) << my_node_.ShortDebugString() << " has received a MSG_MR.";
					conn->peer_addr = conn->ctrl_recv_msg->mr.addr;
					conn->peer_rkey = conn->ctrl_recv_msg->mr.rkey;
					conn->sender = conn->ctrl_recv_msg->sender_id;
					conn->sender_role = conn->ctrl_recv_msg->sender_role;
					if(conn->sender_role == Node::SCHEDULER){
						//PS_VLOG(1) << my_node_.ShortDebugString() << " received a Rank " << conn->ctrl_recv_msg->assigned_id;
						my_node_.id = conn->ctrl_recv_msg->assigned_id;
					}
				}else{
					LOG(FATAL) << my_node_.ShortDebugString() << " should receive a MSG_MR after connected.";
				}
				// post receive message for RecvMsg and SendMsg.
				post_receive_message(conn->id);
				{
					std::lock_guard<std::mutex> lk(senders_mu_);
					auto it = senders_.find(conn->sender);
					if (it != senders_.end()) {
						PS_VLOG(1) << my_node_.ShortDebugString() << " connection already established.";
						break;
					}
					senders_[conn->sender] = conn->id;
				}
			}else if(event_copy.event == RDMA_CM_EVENT_DISCONNECTED ){
				break;
			}
			else{
				if(stop_){
					break;
				}
				PS_VLOG(1) << my_node_.ShortDebugString() <<  " unknown event type.";
			}
		}
	}
	void Listening(){
		while (!stop_ && rdma_get_cm_event(listener_channel_, &listener_event_) == 0) {
			struct rdma_cm_event event_copy;
			memcpy(&event_copy, listener_event_, sizeof(*listener_event_));
			rdma_ack_cm_event(listener_event_);

			PS_VLOG(1) <<  my_node_.ShortDebugString() << " received event type: " << event_copy.event;
			if (event_copy.event == RDMA_CM_EVENT_CONNECT_REQUEST){
				//PS_VLOG(1) << my_node_.ShortDebugString() <<" received a connection request.";
				struct rdma_conn_param cm_params;
				build_params(&cm_params);
				struct connection *conn = (struct connection *)malloc(sizeof(struct connection));
				build_connection(event_copy.id, conn);
				if(!poll_thread_){
					poll_thread_ = std::unique_ptr<std::thread>(
						new std::thread(&RDMAVan::Polling, this));
				}
				register_memory(conn);
				//post receive ctrol for control message with MSG_MR.
				post_receive_ctrl(event_copy.id);
				CHECK_EQ(rdma_accept(event_copy.id, &cm_params), 0)
					<< my_node_.ShortDebugString() << " rdma_accept failed";
			}
			else if (event_copy.event == RDMA_CM_EVENT_ESTABLISHED){
				//PS_VLOG(1) << my_node_.ShortDebugString() << " Lisenting side connection established.";
				struct connection *conn = (struct connection *)event_copy.id->context;
				struct ibv_wc wc = poll_cq_ctrl(conn->id);
				if(conn->ctrl_recv_msg->command == MSG_MR){
					//PS_VLOG(1) << my_node_.ShortDebugString() << " has received a MSG_MR";
					conn->peer_addr = conn->ctrl_recv_msg->mr.addr;
					conn->peer_rkey = conn->ctrl_recv_msg->mr.rkey;
					conn->sender = conn->ctrl_recv_msg->sender_id;
					conn->sender_role = conn->ctrl_recv_msg->sender_role;
				}else{
					LOG(FATAL) << my_node_.ShortDebugString() << " should recieve a MSG_MR after connected.";
				}

				// send my control message.
				if(is_scheduler_){
					int assigned_id = conn->sender_role == Node::SERVER ?
						   Postoffice::ServerRankToID(num_server_) :
						   Postoffice::WorkerRankToID(num_worker_);
					//PS_VLOG(1) << "assign rank=" << assigned_id << " to node " << conn->sender_role;
					conn->ctrl_send_msg->assigned_id = assigned_id;
					conn->sender = assigned_id;
					if (conn->sender_role == Node::SERVER) ++num_server_;
					if (conn->sender_role == Node::WORKER) ++num_worker_;
				}
				conn->ctrl_send_msg->command = MSG_MR;
				conn->ctrl_send_msg->sender_id = my_node_.id;
				conn->ctrl_send_msg->sender_role = my_node_.role;
				conn->ctrl_send_msg->mr.addr = (uintptr_t)conn->recv_buffer_mr->addr;
				conn->ctrl_send_msg->mr.rkey = conn->recv_buffer_mr->rkey;
				// post receive message for RecvMsg and SendMsg.
				post_receive_message(conn->id);
				post_send_ctrl(conn->id);
				// receive control message with MSG_MR.

				{
					std::lock_guard<std::mutex> lk(senders_mu_);
					auto it = senders_.find(conn->sender);
					if (it != senders_.end()) {
						PS_VLOG(1) << my_node_.ShortDebugString() << " connection already established.";
						break;
					}
					senders_[conn->sender] = conn->id;
				}
			}
			else if(event_copy.event == RDMA_CM_EVENT_DISCONNECTED ){
				if(stop_){
					break;
				}
			}
			else{
				PS_VLOG(1) << my_node_.ShortDebugString() <<  " unknown event type.";
			}
		}
	}
	/* corresponding functions to zmq_van.h */
	void Start() override {
		listener_channel_ = rdma_create_event_channel();
		CHECK_NOTNULL(listener_channel_);
		CHECK_EQ(rdma_create_id(listener_channel_, &listener_,
				NULL, RDMA_PS_TCP),0)
			<< " create listenr failed.";
		Van::Start();
	}
	void Stop() override{
		PS_VLOG(1) << my_node_.ShortDebugString() << " is stopping";		
		stop_ = true;
		for(auto i: senders_){
			rdma_disconnect(i.second);
		}
		listen_thread_->join();
		PS_VLOG(1) << my_node_.ShortDebugString() << " listen_thread_ joined";	
		poll_thread_->join();
		PS_VLOG(1) << my_node_.ShortDebugString() << " poll_thread_ joined";	
		for(int i=0; i<thread_pool_.size(); i++){
			thread_pool_[i]->join();
			PS_VLOG(1) << my_node_.ShortDebugString() << " thread " << i  <<" joined";	
		}
		PS_VLOG(1) << my_node_.ShortDebugString() << " stopped";
		Van::Stop();
		// rdma_destroy_event_channel(listener_channel_);
		// // free qp and id
		// {
		// 	std::lock_guard<std::mutex> lk(senders_mu_);
		// 	for(auto it=senders_.begin(); it!=senders_.end(); ++it){
		// 		rdma_destroy_qp(it->second);
		// 		rdma_destroy_id(it->second);
		// 	}
		// }
		// // free share_ctx_
		// CHECK_EQ(ibv_dealloc_pd(share_ctx_->pd), 0);
		// CHECK_EQ(ibv_destroy_comp_channel(share_ctx_->comp_channel), 0);
		// CHECK_EQ(ibv_destroy_cq(share_ctx_->cq), 0);
		// free(share_ctx_);
	}
	int Bind(const Node &node, int max_retry) override {
		memset(&addr_, 0, sizeof(addr_));
		addr_.sin6_family = AF_INET6;
		if(is_scheduler_){
			addr_.sin6_port = htons(my_node_.port);
			//PS_VLOG(1) << " scheduler bind to " << my_node_.port;
		}
		CHECK_EQ(rdma_bind_addr(listener_, (struct sockaddr *)&addr_), 0)
			<< "bind addr failed";
		
		int num_nodes = Postoffice::Get()->num_servers() +
				Postoffice::Get()->num_workers();
		CHECK_EQ(rdma_listen(listener_, num_nodes), 0)
			<< "rdma listen failed";
		// start listening connection request
		listen_thread_ = std::unique_ptr<std::thread>(
			new std::thread(&RDMAVan::Listening, this));
		return ntohs(rdma_get_src_port(listener_));
	}
	void Connect(const Node& node) override{
		if(is_scheduler_ || node.role==my_node_.role){ // why scheduler needs to connect to itself at zmq_van.h?
			return;
		}
		CHECK_NE(node.id, node.kEmpty);
		CHECK_NE(node.port, node.kEmpty);
		CHECK(node.hostname.size());
		int id = node.id;
		{
			std::lock_guard<std::mutex> lk(senders_mu_);
			auto it = senders_.find(id);
			if (it != senders_.end()) {
				PS_VLOG(1) << my_node_.ShortDebugString() << " connection already established.";
				return;
			}
		}
		// worker doesn't need to connect to the other workers. same for server
		if ((node.role == my_node_.role) &&
			(node.id != my_node_.id)) {
			return;
		}
		struct addrinfo *addr;
		PS_VLOG(2) << my_node_.ShortDebugString() << " is connecting to " << node.hostname << " : " << node.port;
		CHECK_EQ(getaddrinfo(node.hostname.c_str(), std::to_string(node.port).c_str(), NULL, &addr),0);
		{
			std::lock_guard<std::mutex> lk(thread_mu_);
			std::thread *temp_thread = new std::thread(&RDMAVan::EventLooping, this, addr);
			thread_pool_.push_back(temp_thread);
		}
		connection_established(id);
		freeaddrinfo(addr);
	}
	int SendMsg(const Message& msg) override{
		//PS_VLOG(1) << my_node_.ShortDebugString() << " start send a message...";
		std::lock_guard<std::mutex> lk(mu_);
		// find the socket
		int id = msg.meta.recver;
		CHECK_NE(id, Meta::kEmpty);
		if(id==my_node_.id){ // send to myself.
			{
				std::lock_guard<std::mutex> lk(send_myself_mu_);
				if(!send_myself_){
					send_myself_ = true;
					if(myself_meta_buff){
						delete[] myself_meta_buff;
					}
					PS_VLOG(1) << my_node_.ShortDebugString() << " send to myself with sender: " << 
						msg.meta.recver;

					PackMeta(msg.meta, &myself_meta_buff, &myself_meta_size);
					return sizeof(myself_meta_size);
				}
			}
			std::this_thread::sleep_for(std::chrono::milliseconds(1));
		}

		auto it = senders_.find(id);
		if (it == senders_.end()) {
		  LOG(WARNING) << " there is no socket to node " << id;
		  return -1;
		}
		connection *conn = (struct connection *)it->second->context;
		CHECK_NOTNULL(conn);
		// 1. write meta
		int meta_size;
		char* meta_buf;
		PackMeta(msg.meta, &meta_buf, &meta_size);
		memcpy(conn->send_buffer, meta_buf, meta_size);
		post_send_message(conn->id, meta_size);
		int send_bytes = meta_size;

		// 2. receive data with imm_data = 0.
		struct ibv_wc wc;
		wc = poll_cq_message(conn->id);
		if(ntohl(wc.imm_data)==1){
		}else{
			LOG(FATAL) << my_node_.ShortDebugString() << " should receive a message with imm_data = 1.";
		}
		// send data
		int n = msg.data.size();
		//PS_VLOG(1) << my_node_.ShortDebugString() << " data size is " << n;
		for(int i=0; i<n; ++i){
			SArray<char>* data = new SArray<char>(msg.data[i]);
			memcpy(conn->recv_buffer, data->data(), data->size());
			post_receive_ctrl(conn->id);
			post_send_message(conn->id, data->size());
			send_bytes += data->size();

			wc = poll_cq_ctrl(conn->id);
			if(conn->ctrl_recv_msg->command==MSG_READY){
				continue;
			}else{
				LOG(FATAL) << my_node_.ShortDebugString() << " should receive a MSG_READY after send data. ";
			}
		}
		post_receive_ctrl(conn->id);
		post_send_message(conn->id, 0);
		wc = poll_cq_ctrl(conn->id);
		if(conn->ctrl_recv_msg->command==MSG_DONE){
			post_receive_message(conn->id);
			//PS_VLOG(1) << my_node_.ShortDebugString() << " send a message: " << send_bytes << " bytes. ";
			return send_bytes;
		}else{
			LOG(FATAL) << my_node_.ShortDebugString() << " should receive a MSG_DONE at the end. ";
			return -1;
		}
	}
	int RecvMsg(Message* msg) override{
		std::lock_guard<std::mutex> lk(receive_mu_);
		msg->data.clear();
		size_t recv_bytes = 0;
		struct rdma_cm_id *id;
		struct ibv_wc wc;
		struct connection *conn;
		// receive meta.
		wc = poll_cq_message(nullptr);
		if(send_myself_ && check_send_myself_){
			UnpackMeta(myself_meta_buff, myself_meta_size, &(msg->meta));
			PS_VLOG(1) << my_node_.ShortDebugString() << " received from myself with bytes: " << myself_meta_size;
			check_send_myself_ = false;
			send_myself_ = false;
			return myself_meta_size;
		}
		id = (struct rdma_cm_id *)(uintptr_t)wc.wr_id;
		conn = (struct connection *)id->context;
		uint32_t size = ntohl(wc.imm_data);
		UnpackMeta(conn->recv_buffer, size, &(msg->meta));
		msg->meta.sender = GetNodeID(conn->recv_buffer, size);
		msg->meta.recver = my_node_.id;
		recv_bytes += size;

		// send MSG_READY
		post_receive_message(id);
		post_send_message(conn->id, 1);

		for(int i=0; ; ++i){
			wc = poll_cq_message(id);
			post_receive_message(id);
			size = ntohl(wc.imm_data);
			if(size){
				SArray<char> data;
				data.CopyFrom(conn->recv_buffer, size);
				//data.reset(conn->recv_buff, size);
				msg->data.push_back(data);
				recv_bytes += size;
				conn->ctrl_send_msg->command = MSG_READY;
				post_send_ctrl(conn->id);
			}else{
				conn->ctrl_send_msg->command = MSG_DONE;
				post_send_ctrl(conn->id);
				break;
			}
		}
		//PS_VLOG(1) << my_node_.ShortDebugString() << " received a message: " << recv_bytes << " bytes. ";
		return recv_bytes;
	}
	
private:
	int GetNodeID(char* buf, size_t size) {
		if (size > 2 && buf[0] == 'p' && buf[1] == 's') {
			int id = 0;
			size_t i = 2;
			for (; i < size; ++i) {
				if (buf[i] >= '0' && buf[i] <= '9') {
					id = id * 10 + buf[i] - '0';
				} else {
					break;
				}
			}
			if (i == size) return id;
		}
		return Meta::kEmpty;
	}

	int num_server_ = 0;
	int num_worker_ = 0;
	bool stop_ = false;
	std::mutex receive_mu_;
	std::mutex mu_;
	std::mutex senders_mu_;
	std::unordered_map<int, rdma_cm_id*> senders_;

 	struct context *share_ctx_ = NULL;
	struct sockaddr_in6 addr_;
	struct rdma_cm_event *listener_event_ = NULL;
	struct rdma_cm_id *listener_ = NULL;
	struct rdma_event_channel *listener_channel_ = NULL;

	// for completion queue.
	std::mutex cqes_ctrl_mu_;
	std::vector<ibv_wc> cqes_ctrl_;
	std::mutex cqes_message_mu_;
	std::vector<ibv_wc> cqes_message_;
	std::unique_ptr<std::thread> poll_thread_;

	// thread pool
	std::mutex thread_mu_;
	std::vector<std::thread *> thread_pool_;
	std::unique_ptr<std::thread> listen_thread_;

	// send to myself;
	std::mutex send_myself_mu_;
	bool send_myself_ = false;
	bool check_send_myself_ = false;
	char *myself_meta_buff;
	int myself_meta_size;

};

}
#endif
