#ifndef PS_RDMA_VAN_H_
#define PS_RDMA_VAN_H_
#include <rdma/rdma_cma.h> // rdma channel management
#include <netdb.h>
#include <cstdlib>
#include <thread>
#include <string>
#include <cstring> // memset
#include "ps/internal/postoffice.h"
#include "ps/internal/van.h"

namespace ps {
// 1 GB
const int BUFFER_SIZE = 1024*1024*1024;

class RDMAVan : public Van {
public:
	RDMAVan() {}
	virtual ~RDMAVan() {}

protected:
	void Start() override {
		listener_channel_ = rdma_create_event_channel();
		CHECK_NOTNULL(listener_channel_) << "create event channel failed.";
		CHECK_EQ(rdma_create_id(listener_channel_, &listener_,
				NULL, RDMA_PS_TCP),0)
			<< "create listenr failed.";
	}

	void Stop() override;

	// passive side, wait for connection request
	int Bind(const Node &node, int max_retry) override {
		memset(&addr_, 0, sizeof(addr_));
		addr_.sin6_family = AF_INET6;
		CHECK_EQ(rdma_bind_addr(listener_, (struct sockaddr *)&addr_), 0)
			<< "bind addr failed";
		/* build context */
		if (share_ctx_) {
			CHECK_EQ(share_ctx_->ctx, listener_->verbs)
				<< "Cannot handle events in more than one context.";
			return -1;
		}
		share_ctx_->ctx = listener_->verbs;
		CHECK_NOTNULL(share_ctx_->pd = ibv_alloc_pd(share_ctx_->ctx));
		CHECK_NOTNULL(share_ctx_->comp_channel = ibv_create_comp_channel(share_ctx_->ctx));
		CHECK_NOTNULL(share_ctx_->cq = ibv_create_cq(share_ctx_->ctx, 50, NULL, share_ctx_->comp_channel, 0)); /* cqe=10 is arbitrary */
		CHECK_EQ(ibv_req_notify_cq(share_ctx_->cq, 0), 0);
		/* end build context */
		int num_nodes = Postoffice::Get()->num_servers() +
				Postoffice::Get()->num_workers();
		CHECK_EQ(rdma_listen(listener_, num_nodes), 0)
			<< "rdma listen failed";
		// start listening connection request
		listen_thread_ = std::unique_ptr<std::thread>(
				new std::thread(&RDMAVan::Listening, this));
		return ntohs(rdma_get_src_port(listener_));
	}

	// active side, post connection request
	void Connect(const Node& node) override{
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
		struct rdma_cm_event *connect_event = NULL;
		struct rdma_cm_id *conn= NULL;
		struct rdma_listener_channel_ *ec = NULL;

		CHECK_EQ(getaddrinfo(node.hostname, node.port, NULL, &addr),0);
		ec = rdma_create_listener_channel_();
		CHECK_NOTNULL(ec) << "create event channel failed.";
		CHECK_EQ(rdma_create_id(ec, &conn, NULL, RDMA_PS_TCP),0)
			<< "create listenr failed.";
		CHECK_EQ(rdma_resolve_addr(conn, NULL, addr->ai_addr, TIMEOUT_IN_MS), 0);
		freeaddrinfo(addr);
		struct connection conn_context;
		while (rdma_get_cm_event(ec, &connect_event) == 0) { // wait until connection established
			struct rdma_cm_event event_copy;

			memcpy(&event_copy, event, sizeof(*event));
			rdma_ack_cm_event(event);

			if (event_copy->event == RDMA_CM_EVENT_ADDR_RESOLVED){
				PS_VLOG(1) << "address resolved. ";

				build_connection(event_copy->id, &conn_context);
				CHECK_EQ(rdma_resolve_route(event_copy->id, TIMEOUT_IN_MS), 0);
			}
			else if (event->event == RDMA_CM_EVENT_ROUTE_RESOLVED){
				struct rdma_conn_param cm_params;

				PS_VLOG(1) << "route resolved. ";
				register_memory(conn_context);
				post_recv_ctrl(conn);
				build_params(&cm_params);
				CHECK_EQ(rdma_connect(event_copy->id, &cm_params), 0);
			}
			else if (event->event == RDMA_CM_EVENT_ESTABLISHED){

				auto it = senders_.find(id);
				if (it != senders_.end()) {
				  break;
				}
				senders[id] = conn_context;
				struct ibv_cq *cq;
				struct ibv_wc wc;
				poll_cq(cq, &wc);
				if(conn_context->ctrl_recv_msg.command == MSG_MR){
					conn_context->peer_addr = conn->ctrl_recv_msg.mr.addr;
					conn_context->peer_rkey = conn->ctrl_recv_msg.mr.rkey;
				}else{
					LOG(FATAL) << "should recieve MSG_MR after connected.";
				}
				conn_context->ctrl_send_msg.command = MSG_MR;
				conn_context->ctrl_send_msg.sender_id = my_node_.id;
				conn_context->ctrl_send_msg.mr.addr = (uintptr_t)conn->recv_buffer_mr->addr;
				conn_context->ctrl_send_msg.mr.rkey = conn->recv_buffer_mr->rkey;
				send_message(conn);
				post_receive_ctrl(id);
			}
		}
		rdma_destroy_listener_channel_(ec);
	}
	int SendMsg(const Message& msg) override{
		std::lock_guard<std::mutex> lk(mu_);
			// find the socket
			int id = msg.meta.recver;
			CHECK_NE(id, Meta::kEmpty);
			auto it = senders_.find(id);
			if (it == senders_.end()) {
			  LOG(WARNING) << "there is no socket to node " << id;
			  return -1;
			}
			// get the connection context
			connection *conn = it->second;
			// to receive control message
			post_receive_ctrl(conn->id);

			// send meta
			int meta_size;
			PackMeta(msg.meta, &conn->send_buffer, &meta_size);
			write_remote(conn, meta_size);
			int send_bytes = meta_size;

			struct ibv_cq *cq;
			struct ibv_wc wc;

			// send data
			for(int i=0; i<msg.data.size(); ++i){
				poll_cq(cq, &wc);
				if(wc->opcode & IBV_WC_RECV){
					if(conn->ctrl_msg->command==MSG_READY){
						post_recv_ctrl(conn->id);
						SArray<char>* data = new SArray<char>(msg.data[i]);
						memcpy(conn->recv_buffer, data->data(), data->size());
						write_remote(conn, data->size());
						bytes+=data->size();
					}
				}
			}
			post_recv_ctrl(conn->id);
			write_remote(conn, 0);
			poll_cq(cq, &wc);
			if(wc->opcode & IBV_WC_RECV){
				if(conn->ctrl_msg->command==MSG_DONE){
					return bytes;
				}
			}
			PS_VLOG(1) << "wc status failed.";
			exit(-1);
	}
	int RecvMsg(Message* msg) override{
		 msg->data.clear();
		    size_t recv_bytes = 0;
		    struct rdma_cm_id *id;
		    struct ibv_cq *cq;
			struct ibv_wc wc;
		    for(int i=0; ; ++i){
		    	poll_cq(cq, &wc);
				if(conn->ctrl_msg->command == MSG_META){
					uint32_t size = ntohl(wc->imm_data);
					UnpackMeta(conn->recv_buff, size, &(msg->meta));
					msg->meta.sender = conn->ctrl_msg->sender_id;
					msg->meta.recver = my_node_.id;
					id = (struct rdma_cm_id *)(uintptr_t)(wc->wr_id);
					post_receive(id);
				}else if(conn->ctrl_msg->command == MSG_META){
					SArray<char> data;
					data.CopyFrom(conn->recv_buff, size);
					//data.reset(conn->recv_buff, size);
					msg->data.push_back(data);
					post_receive(id);
				}
		    }
		    return recv_bytes;
	}

private:
	void Listening(){
		while (rdma_get_cm_event(listener_channel_, &listener_event_) == 0) {
			struct rdma_cm_event event_copy;
			memcpy(&event_copy, listener_event_, sizeof(*listener_event_));
			rdma_ack_cm_event(listener_event_);

			if (event_copy->event == RDMA_CM_EVENT_CONNECT_REQUEST){
				on_connect_request(event_copy->id);
			}
			else if (event_copy->event == RDMA_CM_EVENT_ESTABLISHED){
				recv_receiver(event_copy->id->context);
				auto it = senders_.find(id);
				if (it != senders_.end()) {
				  continue;
				}
				senders[id] = event_copy->id->context;
				post_receive_meta(event_copy->id->context);
			}
			else if (event_copy->event == RDMA_CM_EVENT_DISCONNECTED){
			}
			else{
				LOG(FATAL) <<  "Unknown event type.";
				break;
			}
		}
	}

	void on_connect_request(struct rdma_cm_id *id){
		struct rdma_conn_param cm_params;
			PS_VLOG(1) << "Received connection request.";
			struct connection conn;
			build_connection(id, &conn);
			register_memory(conn);
			post_recv_ctrl(id);
			build_params(&cm_params);
			CHECK_EQ(rdma_accept(id, &cm_params), 0);

			struct ibv_cq *cq;
			struct ibv_wc wc;
			poll_cq(cq, &wc);
			if(conn->ctrl_recv_msg.command == MSG_MR){
				conn->peer_addr = conn->ctrl_recv_msg.mr.addr;
				conn->peer_rkey = conn->ctrl_recv_msg.mr.rkey;
			}else{
				LOG(FATAL) << "should recieve MSG_MR after connected.";
			}
			conn->ctrl_send_msg.command = MSG_MR;
			conn->ctrl_send_msg.sender_id = my_node_.id;
			conn->ctrl_send_msg.mr.addr = (uintptr_t)conn->recv_buffer_mr->addr;
			conn->ctrl_send_msg.mr.rkey = conn->recv_buffer_mr->rkey;
			senders_[id] = &conn;
			send_message(id);
			post_receive_ctrl(id);
	}

	void build_connection(struct rdma_cm_id *id, struct connection conn){
		  struct ibv_qp_init_attr qp_attr;
		  build_qp_attr(&qp_attr, conn);

		  CHECK_EQ(rdma_create_qp(id, share_ctx_->pd, &qp_attr), 0);

		  id->context = conn;

		  conn->id = id;
		  conn->qp = id->qp;
	}

	void build_qp_attr(struct ibv_qp_init_attr *qp_attr, struct connection *conn){
		  memset(qp_attr, 0, sizeof(*qp_attr));

		  qp_attr->send_cq = share_ctx_->cq;
		  qp_attr->recv_cq = share_ctx_->cq;
		  qp_attr->qp_type = IBV_QPT_RC;

		  qp_attr->cap.max_send_wr = 10000;
		  qp_attr->cap.max_recv_wr = 10000;
		  qp_attr->cap.max_send_sge = 1;
		  qp_attr->cap.max_recv_sge = 1;
	}

	void build_params(struct rdma_conn_param *params){
		  memset(params, 0, sizeof(*params));

		  params->initiator_depth = params->responder_resources = 1;
		  params->rnr_retry_count = 7; /* infinite retry */
	}
	
	void register_memory(struct connection *conn){
		conn->recv_buffer = malloc(BUFFER_SIZE);
		conn->send_buffer = malloc(BUFFER_SIZE);
		CHECK_NOTNULL(conn->recv_buffer_mr = ibv_reg_mr(
			share_ctx_->pd,
			conn->recv_buffer,
			BUFFER_SIZE,
			IIBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE));
		CHECK_NOTNULL(conn->send_buffer_mr = ibv_reg_mr(
			share_ctx_->pd,
			conn->send_buffer,
			BUFFER_SIZE,
			IBV_ACCESS_LOCAL_WRITE));
		CHECK_NOTNULL(conn->ctrl_send_msg_mr = ibv_reg_mr(
			share_ctx_->pd,
			&conn->ctrl_send_msg,
			sizeof(control_message),
			IBV_ACCESS_LOCAL_WRITE));
		CHECK_NOTNULL(conn->ctrl_recv_msg_mr = ibv_reg_mr(
			share_ctx_->pd,
			&conn->ctrl_recv_msg,
			sizeof(control_message),
			IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE));
	}
	
	void post_receive(struct rdma_cm_id *id){
		  struct ibv_recv_wr wr, *bad_wr = NULL;

		  memset(&wr, 0, sizeof(wr));

		  wr.wr_id = (uintptr_t)id;
		  wr.sg_list = NULL;
		  wr.num_sge = 0;

		  TEST_NZ(ibv_post_recv(id->qp, &wr, &bad_wr));
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

		  TEST_NZ(ibv_post_recv(id->qp, &wr, &bad_wr));
	}
	void write_remote(struct connection *conn, uint32_t len){
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
	void poll_cq(struct ibv_cq *cq, struct ibv_wc *wc){
		while(true){
			CHECK_EQ(ibv_get_cq_event(share_ctx->comp_channel, &cq, NULL), 0);
			ibv_ack_cq_events(cq, 1);
			CHECK_EQ(ibv_req_notify_cq(cq, 0), 0);

			while (ibv_poll_cq(cq, 1, wc)) {
				if (wc.status == IBV_WC_SUCCESS &&
						(wc->opcode == IBV_WC_RECV_RDMA_WITH_IMM || wc->opcode & IBV_WC_RECV)){
					return;
				}
				else{
					LOG(FATAL) << "wc status not success.";

				}
			}
		}
	}
	void send_message(struct rdma_cm_id *id);{
		  struct connection *ctx = (struct connection *)id->context;

		  struct ibv_send_wr wr, *bad_wr = NULL;
		  struct ibv_sge sge;

		  memset(&wr, 0, sizeof(wr));

		  wr.wr_id = (uintptr_t)id;
		  wr.opcode = IBV_WR_SEND;
		  wr.sg_list = &sge;
		  wr.num_sge = 1;
		  wr.send_flags = IBV_SEND_SIGNALED;

		  sge.addr = (uintptr_t)conn->ctrl_send_msg;
		  sge.length = sizeof(control_message);
		  sge.lkey = conn->ctrl_send_msg_mr->lkey;

		  TEST_NZ(ibv_post_send(id->qp, &wr, &bad_wr));
	}
	enum control_command
	 	{
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

		char* recv_buffer;
		char* send_buffer;
		struct ibv_mr *recv_buffer_mr;
		struct ibv_mr *send_buffer_mr;

		struct control_message ctrl_send_msg;
		struct ibv_mr *ctrl_send_msg_mr;
		struct control_message ctrl_recv_msg;
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

 	struct context *share_ctx_ = NULL;
	std::unique_ptr<std::thread> listen_thread_;
	std::unordered_map<int, connection*> senders_;
	struct sockaddr_in6 addr_;
	struct rdma_cm_event *listener_event_ = NULL;
	struct rdma_cm_id *listener_ = NULL;
	struct rdma_event_channel *listener_channel_ = NULL;
};

}
#endif
