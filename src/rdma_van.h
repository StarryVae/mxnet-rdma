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
	};
	struct connection{
		struct rdma_cm_id *id;
		struct ibv_qp *qp;
		int sender;
		int is_active; // if created by Connect, it is true.

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
	void poll_cq_thread(){
		struct ibv_cq *cq;
		struct ibv_wc wc;
		PS_VLOG(1) << my_node_.ShortDebugString() << " poll_cq_thread start...";
		while(true){
			void *ctx;
			CHECK_EQ(ibv_get_cq_event(share_ctx_->comp_channel, &cq, &ctx), 0);
			ibv_ack_cq_events(cq, 1);
			CHECK_EQ(ibv_req_notify_cq(cq, 0), 0);

			while (ibv_poll_cq(cq, 1, &wc)) {
				if (wc.status == IBV_WC_SUCCESS &&
						wc.opcode == IBV_WC_RECV_RDMA_WITH_IMM){
					PS_VLOG(1) << my_node_.ShortDebugString() << " polled a message cqe...";
					std::lock_guard<std::mutex> lk(cqes_mu_);
					cqes_message_.push_back(&wc);
				}else if(wc.status == IBV_WC_SUCCESS && (wc.opcode & IBV_WC_RECV)){
					PS_VLOG(1) << my_node_.ShortDebugString() << " polled a ctrl cqe...";
					std::lock_guard<std::mutex> lk(cqes_mu_);
					cqes_ctrl_.push_back(&wc);
				}else if(wc.status == IBV_WC_SUCCESS){
					continue;
				}
				else{
					LOG(FATAL) <<  my_node_.ShortDebugString() << " wc status not success";
				}
			}
		}
	}
	void build_context(struct ibv_context *verbs){
		/* build context */
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
		if(!cqes_thread_){
			cqes_thread_ = std::unique_ptr<std::thread>(
				new std::thread(&RDMAVan::poll_cq_thread, this));
		}
		/* end build context */
	}
	void build_qp_attr(struct ibv_qp_init_attr *qp_attr){
			  memset(qp_attr, 0, sizeof(*qp_attr));

			  qp_attr->send_cq = share_ctx_->cq;
			  qp_attr->recv_cq = share_ctx_->cq;
			  qp_attr->qp_type = IBV_QPT_RC;

			  qp_attr->cap.max_send_wr = 10000;
			  qp_attr->cap.max_recv_wr = 10000;
			  qp_attr->cap.max_send_sge = 1;
			  qp_attr->cap.max_recv_sge = 1;
	}
	void register_memory(struct connection *conn){
		conn->recv_buffer = (char *)malloc(BUFFER_SIZE);
		conn->send_buffer = (char *)malloc(BUFFER_SIZE);
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
			IBV_ACCESS_LOCAL_WRITE));
		CHECK_NOTNULL(conn->ctrl_send_msg_mr = ibv_reg_mr(
			share_ctx_->pd,
			conn->ctrl_send_msg,
			sizeof(control_message),
			IBV_ACCESS_LOCAL_WRITE));
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

	void post_receive(struct rdma_cm_id *id){
		  struct ibv_recv_wr wr, *bad_wr = NULL;

		  memset(&wr, 0, sizeof(wr));

		  wr.wr_id = (uintptr_t)id;
		  wr.sg_list = NULL;
		  wr.num_sge = 0;

		  CHECK_EQ(ibv_post_recv(id->qp, &wr, &bad_wr), 0);
	}
	void post_receive_ctrl(struct rdma_cm_id *id){
		  struct connection *ctx = (struct connection *)id->context;

		  struct ibv_recv_wr wr, *bad_wr = NULL;
		  struct ibv_sge sge;

		  memset(&wr, 0, sizeof(wr));

		  wr.wr_id = (uintptr_t)id;
		  wr.sg_list = &sge;
		  wr.num_sge = 1;

		  sge.addr = (uintptr_t)ctx->ctrl_recv_msg;
		  sge.length = sizeof(control_message);
		  sge.lkey = ctx->ctrl_recv_msg_mr->lkey;

		  CHECK_EQ(ibv_post_recv(id->qp, &wr, &bad_wr), 0);
	}
	void write_remote(struct connection *conn, int len){
		  PS_VLOG(1) << my_node_.ShortDebugString() << " is writing remote.."; 
		  struct ibv_send_wr wr, *bad_wr = NULL;
		  struct ibv_sge sge;

		  memset(&wr, 0, sizeof(wr));

		  wr.wr_id = (uintptr_t)conn;
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

		  CHECK_EQ(ibv_post_send(conn->qp, &wr, &bad_wr), 0)
		  	  << "ibv_post_send failed. ";
	}

	struct ibv_wc* poll_cq_ctrl(struct rdma_cm_id *id){
		ibv_wc *ret;
		PS_VLOG(1) << my_node_.ShortDebugString() << " now is at poll_cq_ctrl";
		if(id==nullptr){
			while(true){
				std::lock_guard<std::mutex> lk(cqes_mu_);
				if(!cqes_ctrl_.empty()){
					ret = cqes_ctrl_.front();
					cqes_ctrl_.erase(cqes_ctrl_.begin());
					PS_VLOG(1) << my_node_.ShortDebugString() << " found a received ctrl cqe.";
					return ret;
				}
				std::this_thread::sleep_for(std::chrono::milliseconds(1));
			}
		}
		while(true){
			std::lock_guard<std::mutex> lk(cqes_mu_);
			for(auto it=cqes_ctrl_.begin(); it!=cqes_ctrl_.end();it++){
				if((*it)->wr_id==(uintptr_t)id){
					ret = *it;
					cqes_ctrl_.erase(it);
					PS_VLOG(1) << my_node_.ShortDebugString() << " found a ctrl received cqe.";
					return ret;
				}
			}
			std::this_thread::sleep_for(std::chrono::milliseconds(1));
		}		
	}
	struct ibv_wc* poll_cq_message(struct rdma_cm_id *id){
		ibv_wc *ret;
		PS_VLOG(1) << my_node_.ShortDebugString() << " now is at poll_cq_message";
		if(id==nullptr){
			while(true){
				{
					std::lock_guard<std::mutex> lk(cqes_mu_);
					if(!cqes_message_.empty()){
						ret = cqes_message_.front();
						cqes_ctrl_.erase(cqes_message_.begin());
						PS_VLOG(1) << my_node_.ShortDebugString() << " found a received message cqe.";
						return ret;
					}
				}				
				std::this_thread::sleep_for(std::chrono::milliseconds(1));
			}
		}
		while(true){
			{
				std::lock_guard<std::mutex> lk(cqes_mu_);
				for(auto it=cqes_message_.begin(); it!=cqes_message_.end();it++){
					if((*it)->wr_id==(uintptr_t)id){
						ret = *it;
						cqes_message_.erase(it);
						PS_VLOG(1) << my_node_.ShortDebugString() << " found a ctrl message cqe.";
						return ret;
					}
				}
			}			
			std::this_thread::sleep_for(std::chrono::milliseconds(1));
		}		
	}
	void send_message(struct rdma_cm_id *id){
		struct connection *ctx = (struct connection *)id->context;

		struct ibv_send_wr wr, *bad_wr = NULL;
		struct ibv_sge sge;

		memset(&wr, 0, sizeof(wr));

		wr.wr_id = (uintptr_t)id;
		wr.opcode = IBV_WR_SEND;
		wr.sg_list = &sge;
		wr.num_sge = 1;
		wr.send_flags = IBV_SEND_SIGNALED;

		sge.addr = (uintptr_t)ctx->ctrl_send_msg;
		sge.length = sizeof(control_message);
		sge.lkey = ctx->ctrl_send_msg_mr->lkey;
		PS_VLOG(1) << my_node_.ShortDebugString() << " Send a message";
		CHECK_EQ(ibv_post_send(id->qp, &wr, &bad_wr), 0);
	}
	void on_connect_request(struct rdma_cm_id *id){
		struct rdma_conn_param cm_params;
		PS_VLOG(1) << "Received connection request.";
		struct connection *conn = (struct connection *)malloc(sizeof(struct connection));
		build_connection(id, conn);
		register_memory(conn);
		post_receive_ctrl(id);
		build_params(&cm_params);
		CHECK_EQ(rdma_accept(id, &cm_params), 0);
	}
	void Listening(){
		while (rdma_get_cm_event(listener_channel_, &listener_event_) == 0) {
			struct rdma_cm_event event_copy;
			memcpy(&event_copy, listener_event_, sizeof(*listener_event_));
			rdma_ack_cm_event(listener_event_);
			PS_VLOG(1) <<  my_node_.ShortDebugString() << "event type: " << event_copy.event;
			if (event_copy.event == RDMA_CM_EVENT_CONNECT_REQUEST){
				on_connect_request(event_copy.id);
			}
			else if (event_copy.event == RDMA_CM_EVENT_ESTABLISHED){
				struct connection *conn = (struct connection *)event_copy.id->context;
				PS_VLOG(1) << my_node_.ShortDebugString() << " Passive side connection established.";
				conn->ctrl_send_msg->command = MSG_MR;
				conn->ctrl_send_msg->sender_id = my_node_.id;
				conn->ctrl_send_msg->mr.addr = (uintptr_t)conn->recv_buffer_mr->addr;
				conn->ctrl_send_msg->mr.rkey = conn->recv_buffer_mr->rkey;
				post_receive(conn->id);
				send_message(conn->id);
				struct ibv_wc *wc;
				wc = poll_cq_ctrl(conn->id);
				if(conn->ctrl_recv_msg->command == MSG_MR){
					PS_VLOG(1) << my_node_.ShortDebugString() << " has received a MSG_MR";
					conn->peer_addr = conn->ctrl_recv_msg->mr.addr;
					conn->peer_rkey = conn->ctrl_recv_msg->mr.rkey;
					conn->sender = conn->ctrl_recv_msg->sender_id;
				}else{
					PS_VLOG(1) << my_node_.ShortDebugString() << " command received is " << conn->ctrl_recv_msg->command;
					LOG(FATAL) << my_node_.ShortDebugString() << " should recieve MSG_MR after connected.";
				}
				
				senders_[conn->ctrl_recv_msg->sender_id] = conn->id;
				num_connected_ ++;
				PS_VLOG(1) << my_node_.ShortDebugString() << " has connected " << num_connected_ << " nodes.";
			}
			else if(event_copy.event == RDMA_CM_EVENT_DISCONNECTED ){
				num_connected_--;
				if(num_connected_==0){
					break;
				}
			}
			else{
				LOG(FATAL) << my_node_.ShortDebugString() <<  " Unknown event type.";
			}
		}
	}
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
		Van::Stop();
		rdma_destroy_event_channel(listener_channel_);
		// free qp and id
		for(auto it=senders_.begin(); it!=senders_.end(); ++it){
			rdma_destroy_qp(it->second);
			rdma_destroy_id(it->second);
		}
		// free share_ctx_
		CHECK_EQ(ibv_dealloc_pd(share_ctx_->pd), 0);
		CHECK_EQ(ibv_destroy_comp_channel(share_ctx_->comp_channel), 0);
		CHECK_EQ(ibv_destroy_cq(share_ctx_->cq), 0);
		free(share_ctx_);
	}

	// passive side, wait for connection request
	int Bind(const Node &node, int max_retry) override {
		memset(&addr_, 0, sizeof(addr_));
		addr_.sin6_family = AF_INET6;
		if(is_scheduler_){
			addr_.sin6_port = htons(my_node_.port);
			PS_VLOG(1) << " scheduler bind to port: " <<my_node_.port;
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
	void block_until_connection_established(int node_id){
		while(true){
			{
				std::lock_guard<std::mutex> lk(mu_);
				auto it = senders_.find(node_id);
				if(it!=senders_.end()){
					return;
				}
			}			
			std::this_thread::sleep_for(std::chrono::milliseconds(1));
		}		
	}
	void event_loop(addrinfo *addr){
		struct rdma_cm_event *connect_event;
		struct rdma_cm_id *conn_id;
		struct rdma_event_channel *ec;
		ec = rdma_create_event_channel();
		CHECK_NOTNULL(ec);
		CHECK_EQ(rdma_create_id(ec, &conn_id, NULL, RDMA_PS_TCP),0)
			<< " create listenr failed.";
		CHECK_EQ(rdma_resolve_addr(conn_id, NULL, addr->ai_addr, 500), 0);
		struct connection *conn = (struct connection *)malloc(sizeof(struct connection));
		while (rdma_get_cm_event(ec, &connect_event) == 0) { // wait until connection established
			struct rdma_cm_event event_copy;

			memcpy(&event_copy, connect_event, sizeof(*connect_event));
			rdma_ack_cm_event(connect_event);

			if (event_copy.event == RDMA_CM_EVENT_ADDR_RESOLVED){
				PS_VLOG(1) << my_node_.ShortDebugString() << " address resolved. ";
				build_connection(event_copy.id, conn);
				CHECK_EQ(rdma_resolve_route(event_copy.id, 500), 0);
			}
			else if (event_copy.event == RDMA_CM_EVENT_ROUTE_RESOLVED){
				struct rdma_conn_param cm_params;

				PS_VLOG(1) << my_node_.ShortDebugString() << " route resolved. ";
				register_memory(conn);
				post_receive_ctrl(conn->id);
				build_params(&cm_params);
				CHECK_EQ(rdma_connect(conn->id, &cm_params), 0);
			}
			else if (event_copy.event == RDMA_CM_EVENT_ESTABLISHED){
				PS_VLOG(1) << my_node_.ShortDebugString() << " Active side connection established.";				
				struct ibv_wc *wc;
				wc = poll_cq_ctrl(conn->id);
				if(conn->ctrl_recv_msg->command == MSG_MR){
					PS_VLOG(1) << my_node_.ShortDebugString() << " has received a MSG_MR.";
					conn->peer_addr = conn->ctrl_recv_msg->mr.addr;
					conn->peer_rkey = conn->ctrl_recv_msg->mr.rkey;
					conn->sender = conn->ctrl_recv_msg->sender_id;
				}else{
					PS_VLOG(1) << my_node_.ShortDebugString() << " command received is " << conn->ctrl_recv_msg->command;
					PS_VLOG(1) << my_node_.ShortDebugString() << " sender received is " << conn->ctrl_recv_msg->sender_id;
					LOG(FATAL) << my_node_.ShortDebugString() << " should receive MSG_MR after connected.";
				}
				conn->ctrl_send_msg->command = MSG_MR;
				conn->ctrl_send_msg->sender_id = my_node_.id;
				conn->ctrl_send_msg->mr.addr = (uintptr_t)conn->recv_buffer_mr->addr;
				conn->ctrl_send_msg->mr.rkey = conn->recv_buffer_mr->rkey;
				send_message(conn->id);
				post_receive(conn->id); // why?
				{
					std::lock_guard<std::mutex> lk(mu_);
					auto it = senders_.find(conn->sender);
					if (it != senders_.end()) {
						PS_VLOG(1) << my_node_.ShortDebugString() << " connection already established.";
						break;
					}
					senders_[conn->sender] = conn->id;
				}

			}
		}
	}
	// active side, post connection request
	void Connect(const Node& node) override{
		if(is_scheduler_){ // why scheduler needs to connect to itself?
			return;
		}
		CHECK_NE(node.id, node.kEmpty);
		CHECK_NE(node.port, node.kEmpty);
		CHECK(node.hostname.size());
		int id = node.id;
		auto it = senders_.find(id);
		if (it != senders_.end()) { // already connected to the node
			return;
		}
		// worker doesn't need to connect to the other workers. same for server
		if ((node.role == my_node_.role) &&
			(node.id != my_node_.id)) {
			return;
		}
		struct addrinfo *addr;
		CHECK_EQ(getaddrinfo(node.hostname.c_str(), std::to_string(node.port).c_str(), NULL, &addr),0);
		{
			std::lock_guard<std::mutex> lk(thread_mu_);
			std::thread *temp_thread = new std::thread(&RDMAVan::event_loop, this, addr);
			thread_pool_.push_back(temp_thread);
			thread_cnt_++;
		}
		block_until_connection_established(node.id);
		freeaddrinfo(addr);
	}
	int SendMsg(const Message& msg) override{
		PS_VLOG(1) << my_node_.ShortDebugString() << " Begining at SendMsg.";
		std::lock_guard<std::mutex> lk(mu_);
		// find the socket
		int id = msg.meta.recver;
		CHECK_NE(id, Meta::kEmpty);
		auto it = senders_.find(id);
		if (it == senders_.end()) {
		  LOG(WARNING) << " there is no socket to node " << id;
		  return -1;
		}
		PS_VLOG(1) << my_node_.ShortDebugString() << " Get the connection";
		// get the connection context
		connection *conn = (struct connection *)it->second->context;
		CHECK_NOTNULL(conn);
		// to receive control message
		post_receive_ctrl(it->second);
		PS_VLOG(1) << my_node_.ShortDebugString() << " After post_receive_ctrl...................";
		// send meta
		int meta_size;
		PackMeta(msg.meta, &conn->send_buffer, &meta_size);
		write_remote(conn, meta_size);
		int send_bytes = meta_size;
		PS_VLOG(1) << my_node_.ShortDebugString() << " After write_remote.";
		struct ibv_wc *wc;

		// send data
		int n = msg.data.size();
		for(int i=0; i<n; ++i){
			wc = poll_cq_ctrl(conn->id);
			if(conn->ctrl_recv_msg->command==MSG_READY){
				post_receive_ctrl(conn->id);
				SArray<char>* data = new SArray<char>(msg.data[i]);
				memcpy(conn->recv_buffer, data->data(), data->size());
				write_remote(conn, data->size());
				send_bytes+=data->size();
			}
		}
		post_receive_ctrl(conn->id);
		write_remote(conn, 0);
		wc = poll_cq_ctrl(conn->id);
		if(conn->ctrl_recv_msg->command==MSG_DONE){
			return send_bytes;
		}
		PS_VLOG(1) << " should receive MSG_DONE at the end";
		exit(-1);
	}
	int RecvMsg(Message* msg) override{
		msg->data.clear();
		size_t recv_bytes = 0;
		struct rdma_cm_id *id;
		struct ibv_wc *wc;
		struct connection *conn;

		wc = poll_cq_message(nullptr);
		id = (struct rdma_cm_id *)(wc->wr_id);
		conn = (struct connection *)id->context;
		uint32_t size = ntohl(wc->imm_data);
		UnpackMeta(conn->recv_buffer, size, &(msg->meta));
		msg->meta.sender = conn->sender;
		msg->meta.recver = my_node_.id;
		recv_bytes += size;
		conn->ctrl_send_msg->command = MSG_READY;
		send_message(conn->id);
		for(int i=0; ; ++i){
			post_receive(id);
			wc = poll_cq_message(conn->id);
			size = ntohl(wc->imm_data);
			if(size){
				SArray<char> data;
				data.CopyFrom(conn->recv_buffer, size);
				//data.reset(conn->recv_buff, size);
				msg->data.push_back(data);
				recv_bytes += size;
				conn->ctrl_send_msg->command = MSG_READY;
				send_message(conn->id);
			}else{
				conn->ctrl_send_msg->command = MSG_DONE;
				send_message(conn->id);
				break;
			}
		}
		return recv_bytes;
	}
	
private:
	int num_connected_;
	std::mutex mu_;
 	struct context *share_ctx_ = NULL;
	std::unique_ptr<std::thread> listen_thread_;
	std::unordered_map<int, rdma_cm_id*> senders_;
	struct sockaddr_in6 addr_;
	struct rdma_cm_event *listener_event_ = NULL;
	struct rdma_cm_id *listener_ = NULL;
	struct rdma_event_channel *listener_channel_ = NULL;
	std::mutex cqes_mu_;
	std::vector<ibv_wc *> cqes_ctrl_;
	std::vector<ibv_wc *> cqes_message_;
	std::unique_ptr<std::thread> cqes_thread_;
	std::vector<std::thread *> thread_pool_;
	std::mutex thread_mu_;
	int thread_cnt_ = 0;
};

}
#endif
