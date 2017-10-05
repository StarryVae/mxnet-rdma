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

class RDMAVan : public Van {
public:
	RDMAVan() {}
	virtual ~RDMAVan() {}

protected:
	void Start() override {
		listener_channel_ = rdma_create_listener_channel_();
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
		return ntohs(rdma_get_src_port(listener_))
	}

	// active side, post connection request
	void Connect(const Node& node) override;
	int SendMsg(const Message& msg) override;
	int RecvMsg(Message* msg) override;

private:
	void Listening();

	void on_connect_request(struct rdma_cm_id *id);

	void build_connection(struct rdma_cm_id *id, struct connection conn);

	void build_qp_attr(struct ibv_qp_init_attr *qp_attr, struct connection *conn);

	void build_params(struct rdma_conn_param *params);
	
	void register_memory(struct connection *conn);
	
	void post_receive(struct rdma_cm_id *id);
	void post_receive_ctrl(struct rdma_cm_id *id);
	void write_remote(struct connection *conn, uint32_t len);
	void poll_cq(struct ibv_cq *cq, struct ibv_wc *wc);
	void send_message(struct rdma_cm_id *id);
	
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

	static struct context *share_ctx_ = NULL;
	std::unique_ptr<std::thread> listen_thread_;
	std::unordered_map<int, connection*> senders_;
	struct sockaddr_in6 addr_;
	struct rdma_cm_event *listener_event_ = NULL;
	struct rdma_cm_id *listener_ = NULL;
	struct rdma_listener_channel *listener_channel_ = NULL;
};

}
#endif
