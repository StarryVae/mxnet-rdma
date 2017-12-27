/**
*  Implements van using RDMA
*  Author: Liu Chang
*  Email: liuchang1437@outlook.com
*  Date: Dec. 2017
*/
#ifndef PS_NEW_RDMA_VAN_H_
#define PS_NEW_RDMA_VAN_H_
#include <rdma/rdma_cma.h> // rdma channel management
#include <rdma/rdma_verbs.h>
#include <netdb.h> // getaddrinfo
#include <cstring> // memset
#include <cstdlib> // posix_memalign
#include <thread>
#include <string>
#include <condition_variable>
#include <unordered_map>
#include "ps/internal/van.h"
#include "ps/internal/postoffice.h"
#include <fcntl.h>
#include <sys/stat.h>

namespace ps {
using BUFFER_SIZE = 1024 * 1024 * 1024; //1G
/**
* \brief RDMA based implementation
*/
class NewRdmaVan : public Van {
public:
  NewRdmaVan() { }
  virtual ~NewRdmaVan() { }

protected:
  void Start() override {
    if (!Postoffice::Get()->is_worker()) {
      passive_channel_ = rdma_create_event_channel();
      CHECK_NOTNULL(passive_channel_);
      CHECK_EQ(
        rdma_create_id(passive_channel_, &passive_listen_id_, NULL, RDMA_PS_TCP),
        0) << " Create passive_listen_id_ failed.";
    }
    Van::Start();
  }
  void Stop() override {
    stop_ = true;
    for (auto i : connections_) {
      rdma_disconnect(i.second);
    }
    passive_listen_thread_->join();
    polling_thread_->join();
    for (int i = 0; i<active_connect_threads_.size(); i++) {
      active_connect_threads_[i]->join();
    }
    Van::Stop();
  }
  int Bind(const Node &node, int max_retry) override {
    if (node.role == Node::WORKER) { // workers don't need to bind address.
      return -2;
    }
    sockaddr_in6 addr;
    memset(&addr, 0, sizeof(addr));
    addr.sin6_family = AF_INET6;
    // schedulers bind to a predefined port, 
    // servers bind to an available port, 
    // both of which can be retrieved with rdma_get_src_port
    if (is_scheduler_) {
      addr_.sin6_port = htons(my_node_.port);
    }
    CHECK_EQ(rdma_bind_addr(passive_listen_id_, (sockaddr *)&addr), 0)
      << " Bind addr failed";
    int num_nodes = 2 * (Postoffice::Get()->num_servers() +
      Postoffice::Get()->num_workers());
    CHECK_EQ(rdma_listen(passive_listen_id_, num_nodes), 0)
      << "rdma listen failed";
    // start passive side
    passive_listen_thread_ = std::unique_ptr<std::thread>(
      new std::thread(&NewRdmaVan::passive_listening, this));
    return ntohs(rdma_get_src_port(passive_listen_id_));
  }
  void Connect(const Node& node) override {
    CHECK_NE(node.id, node.kEmpty);
    CHECK_NE(node.port, node.kEmpty);
    CHECK(node.hostname.size());
    // worker doesn't need to connect to the other workers. same for server
    if (node.role == my_node_.role) {
      return;
    }
    int target_node_id = node.id;
    {
      std::lock_guard<std::mutex> lk(connections_mutex_);
      auto it = connections_.find(target_node_id);
      if (it != connections_.end()) {
        PS_VLOG(1) << my_node_.ShortDebugString() << " trying to connect to "
          << target_node_id << ", which is already connected.";
        return;
      }
    }
    if (my_node_.role == Node::SERVER && node.role == Node::WORKER) { // servers just wait for workers.
      wait_for_connected(target_node_id);
      return;
    }
    struct addrinfo *addr;
    CHECK_EQ(getaddrinfo(node.hostname.c_str(), std::to_string(node.port).c_str(), nullptr, &addr), 0);
    std::unique_ptr<std::thread> active_connect_thread = std::unique_ptr<std::thread>(
      new std::thread(&NewRdmaVan::active_connecting, this, addr));
    {
      std::lock_guard<std::mutex> lk(threads_mutex_);
      active_connect_threads_.push_back(active_connect_thread);
    }
    wait_for_connected(target_node_id);
    freeaddrinfo(addr);
  }
  int SendMsg(const Message& msg) override {
    std::lock_guard<std::mutex> lk(send_mutex_);
    // find the socket
    int id = msg.meta.recver;
    CHECK_NE(id, Meta::kEmpty);
    if (id == my_node_.id) { // send to myself.
      {
        std::unique_lock<std::mutex> lk(receive_mutex_);
        receive_mutex_cv_.wait(lk, [] { return read_; });
        read_ = false;
        receive_id_ = nullptr;
        meta_from_myself = msg.meta;
        receive_mutex_cv_.notify_all();
      }
      return sizeof(myself_meta);
    }
    decltype(connections_.find(id)) it;
    {
      std::lock_guard<std::mutex> lk(connections_mutex_);
      it = connections_.find(id);
      if (it == senders_.end()) {
        LOG(ERROR) << my_node_.ShortDebugString() << " there is no socket to node " << id;
        return -1;
      }
    }
    rdma_cm_id *rdma_id = it->second;
    connection *conn = (connection *)rdma_id->context;
    CHECK_NOTNULL(conn);
    // 1. send meta
    int meta_size;
    char* meta_buf;
    PackMeta(msg.meta, &meta_buf, &meta_size);
    memcpy(share_ctx_->send_buffer, meta_buf, meta_size);
    post_receive(rdma_id);
    conn->ctrl_send_msg->command = MSG_META;
    conn->ctrl_send_msg->sender_id = my_node_.id;
    conn->ctrl_send_msg->message_size = meta_size;
    conn->ctrl_send_msg->data_size = msg.data.size();
    post_send(rdma_id);
    int send_bytes = meta_size;
    // 2. wait until MSG_READ_DONE.
    {
      std::unique_lock<std::mutex> lk(receive_mutex_);
      receive_mutex_cv_.wait(lk, [rdma_id] {
        return read_ == false && receive_id_ = rdma_id
          && conn->ctrl_recv_msg->command == MSG_READ_DONE;
      });
      read_ = true;
    }
    // 3. send MSG_DATA
    if (msg.data.size()) {
      send_bytes += pack_data(msg, rdma_id);
      post_receive(rdma_id);
      post_send(rdma_id);
    }

    // wait until MSG_READ_DONE.
    {
      std::unique_lock<std::mutex> lk(receive_mutex_);
      receive_mutex_cv_.wait(lk, [rdma_id] {
        return read_ == false && receive_id_ = rdma_id
          && conn->ctrl_recv_msg->command == MSG_READ_DONE;
      });
      read_ = true;
    }
    post_receive(rdma_id);
    return send_bytes;
  }
  int RecvMsg(Message* msg) override {
    msg->data.clear();
    size_t recv_bytes = 0;
    rdma_cm_id *rdma_id;
    bool myself = false;
    {
      std::unique_lock<std::mutex> lk(receive_mutex_);
      receive_mutex_cv_.wait(lk, [] {
        return (read_ == false &&
          (!receive_id_ || ((connection *)receive_id_->context)->ctrl_recv_msg->command == MSG_META);
      });
        if (!receive_id_) {
          myself = true;
          msg.meta = *meta_from_myself_;
          meta_from_myself_ = nullptr;
        }
        else {
          rdma_id = receive_id_;
        }
        read_ = true;
    }
    // receive meta.
    if (myself) {
      msg->meta.sender = my_node_.id;
      msg->meta.recver = my_node_.id;
      return sizeof(msg->meta);
    }
    connection *conn = (connection *)rdma_id->context;
    size_t size = conn->ctrl_recv_msg->message_size;
    int data_size = conn->ctrl_recv_msg->data_size;
    post_read(rdma_id);
    {
      std::unique_lock<std::mutex> lk(receive_mutex_);
      receive_mutex_cv_.wait(lk, [rdma_id] {
        return read_ == false && receive_id_ == rdma_id && receive_type_ == READ;
      });
      read_ = true;
    }
    UnpackMeta(share_ctx_->recv_buffer, size, &(msg->meta));
    msg->meta.sender = conn->peer_id;
    msg->meta.recver = my_node_.id;
    recv_bytes += size;

    post_receive(rdma_id);
    conn->ctrl_send_msg->command = MSG_READ_DONE;
    conn->ctrl_send_msg->sender_id = my_node_.id;
    post_send(rdma_id);
    if (data_size) {// receive data
      {
        std::unique_lock<std::mutex> lk(receive_mutex_);
        receive_mutex_cv_.wait(lk, [rdma_id] {
          return read_ == false && ((connection *)receive_id_->context)->ctrl_recv_msg->command == MSG_DATA;
        });
        read_ = true;
      }
      post_read(rdma_id);
      char *p = share_ctx_->recv_buffer;
      for (int i = 0; i < data_size; ++i) {
        SArray<char> data;
        data.CopyFrom(p, conn->ctrl_recv_msg->data_sizes[i]);
        p += conn->ctrl_recv_msg->data_sizes[i];
      }
      recv_bytes += conn->ctrl_recv_msg->message_size;
      post_receive(rdma_id);
      conn->ctrl_send_msg->command = MSG_READ_DONE;
      conn->ctrl_send_msg->sender_id = my_node_.id;
      post_send(rdma_id);
    }
    return recv_bytes;
  }

  void build_qp_attr(ibv_qp_init_attr *qp_attr) {
    memset(qp_attr, 0, sizeof(*qp_attr));

    qp_attr->send_cq = share_ctx_->cq;
    qp_attr->recv_cq = share_ctx_->cq;
    qp_attr->qp_type = IBV_QPT_RC;

    qp_attr->cap.max_send_wr = 10;
    qp_attr->cap.max_recv_wr = 10;
    qp_attr->cap.max_send_sge = 1;
    qp_attr->cap.max_recv_sge = 1;
  }
  void build_conn_params(rdma_conn_param *params) {
    memset(params, 0, sizeof(*params));
    //The maximum number of outstanding RDMA read and atomic operations
    //that the local side will have to the remote side.
    params->initiator_depth = 1;
    //The maximum number of outstanding RDMA read and atomic operations 
    //that the local side will accept from the remote side.
    params->responder_resources = 1;
    //The maximum number of times that a send operation from the remote peer
    //should be retried on a connection after receiving a receiver not ready 
    //(RNR) error. RNR errors are generated when a send request arrives before
    //a buffer has been posted to receive the incoming data. 
    params->rnr_retry_count = 7; // infinite retry 
  }
  void wait_for_connected(const int node_id) const {
    std::unique_lock<std::mutex> lk(connections_mutex_);
    conn_cv_.wait(lk, [node_id] {
      return connections_.find(node_id) != connections_.end();
    });
  }
  void passive_listening() {
    rdma_cm_event *event = nullptr;
    rdma_conn_param cm_params;
    build_conn_params(&cm_params);
    while (rdma_get_cm_event(passive_channel_, &event) == 0) {
      struct rdma_cm_event event_copy;
      memcpy(&event_copy, event, sizeof(*event));
      rdma_ack_cm_event(event);

      if (event_copy.event == RDMA_CM_EVENT_CONNECT_REQUEST) {
        //PS_VLOG(1) << my_node_.ShortDebugString() <<" received a connection request.";
        build_connection(event_copy.id);
        post_receive(event_copy.id);
        CHECK_EQ(rdma_accept(event_copy.id, &cm_params), 0)
          << my_node_.ShortDebugString() << " rdma_accept failed";
      }
      else if (event_copy.event == RDMA_CM_EVENT_ESTABLISHED) {
        connection *conn = (connection *)event_copy.id->context;
        {
          std::unique_lock<std::mutex> lk(receive_mutex_);
          receive_mutex_cv_.wait(lk, [event_copy.id]{
            return read_ == false && receive_id_ = event_copy.id
            && conn->ctrl_recv_msg->command == MSG_MR;
          });
          read_ = true;
        }
        conn->peer_addr = conn->ctrl_recv_msg->mr.addr;
        conn->peer_rkey = conn->ctrl_recv_msg->mr.rkey;
        conn->peer_role = conn->ctrl_recv_msg->peer_role;
        conn->peer_id = conn->ctrl_recv_msg->peer_id;

        if (is_scheduler_) { // assign ID for servers and workers
          int assigned_id = conn->peer_role == Node::SERVER ?
            Postoffice::ServerRankToID(num_servers_) :
            Postoffice::WorkerRankToID(num_workers_);
          conn->ctrl_send_msg->receiver_id = assigned_id;
          conn->peer_id = assigned_id;
        }
        if (conn->sender_role == Node::SERVER) ++num_servers_;
        if (conn->sender_role == Node::WORKER) ++num_workers_;
        conn->ctrl_send_msg->command = MSG_MR;
        conn->ctrl_send_msg->sender_id = my_node_.id;
        conn->ctrl_send_msg->sender_role = my_node_.role;
        conn->ctrl_send_msg->mr.addr = (uintptr_t)share_ctx_->recv_buffer_mr->addr;
        conn->ctrl_send_msg->mr.rkey = share_ctx_->recv_buffer_mr->rkey;
        post_receive(conn->id);
        post_send(conn->id);
        {
          std::lock_guard<std::mutex> lk(connections_mutex_);
          connections_[conn->peer_id] = conn->id;
          conn_cv_.notify_all();
        }
      }
      else if (event_copy.event == RDMA_CM_EVENT_DISCONNECTED) {
        connection *conn = (connection *)event_copy.id->context;
        if (conn->peer_role == Node::WORKER) {
          num_workers_--;
        }
        if (conn->peer_role == Node::SERVER) {
          num_servers_--;
        }
        if (num_server <= 0 && num_worker <= 0) {
          break;
        }
      }
      else {
        PS_VLOG(1) << my_node_.ShortDebugString() << " unknown event type.";
      }
    }
  }
  void active_connecting(addrinfo *addr) {
    struct rdma_cm_id *active_id;
    struct rdma_event_channel *active_event_channel;
    active_event_channel = rdma_create_event_channel();
    CHECK_NOTNULL(ec) << " create event channel failed";
    CHECK_EQ(rdma_create_id(active_event_channel, &active_id, nullptr, RDMA_PS_TCP), 0)
      << " create rdma_cm_id failed.";
    CHECK_EQ(rdma_resolve_addr(active_id, nullptr, addr->ai_addr, 500), 0);

    rdma_cm_event *event = nullptr;
    rdma_conn_param cm_params;
    build_conn_params(&cm_params);
    while (rdma_get_cm_event(ec, &event) == 0) {
      struct rdma_cm_event event_copy;
      memcpy(&event_copy, event, sizeof(*event));
      rdma_ack_cm_event(event);

      if (event_copy.event == RDMA_CM_EVENT_ADDR_RESOLVED) {
        build_connection(event_copy.id);
        CHECK_EQ(rdma_resolve_route(event_copy.id, 500), 0)
          << " resolve route failed";
      }
      else if (event_copy.event == RDMA_CM_EVENT_ROUTE_RESOLVED) {
        CHECK_EQ(rdma_connect(event_copy.id, &cm_params), 0);
      }
      else if (event_copy.event == RDMA_CM_EVENT_ESTABLISHED) {
        post_receive(event_copy.id);
        connection *conn = (connection *)event_copy.id->context;
        conn->ctrl_send_msg->command = MSG_MR;
        conn->ctrl_send_msg->sender_id = my_node_.id;
        conn->ctrl_send_msg->sender_role = my_node_.role;
        conn->ctrl_send_msg->mr.addr = (uintptr_t)share_ctx_->recv_buffer_mr->addr;
        conn->ctrl_send_msg->mr.rkey = share_ctx_->recv_buffer_mr->rkey;
        post_send(conn->id);
        // receive mr
        {
          std::unique_lock<std::mutex> lk(receive_mutex_);
          receive_mutex_cv_.wait(lk, [event_copy.id]{
            return read_ == false && receive_id_ = event_copy.id
            && conn->ctrl_recv_msg->command == MSG_MR;
          });
          read_ = true;
        }
        conn->peer_addr = conn->ctrl_recv_msg->mr.addr;
        conn->peer_rkey = conn->ctrl_recv_msg->mr.rkey;
        conn->peer_id = conn->ctrl_recv_msg->sender_id;
        conn->peer_role = conn->ctrl_recv_msg->sender_role;
        if (conn->peer_role == Node::SCHEDULER) {
          my_node_.id = conn->ctrl_recv_msg->receiver_id;
        }
        post_receive(conn->id);
        {
          std::lock_guard<std::mutex> lk(connections_mutex_);
          connections_[conn->peer_id] = conn->id;
          conn_cv_.notify_all();
        }
      }
      else if (event_copy.event == RDMA_CM_EVENT_DISCONNECTED) {
        break;
      }
      else {
        PS_VLOG(1) << my_node_.ShortDebugString() << " unknown event type.";
      }
    }
  }
  void build_context(rdma_cm_id *id) {
    connection *ctx = new connection;
    id->context = ctx;
    ctx->ctrl_send_msg = new control_message;
    ctx->ctrl_recv_msg = new control_message;
    CHECK_NOTNULL(ctx->ctrl_send_msg_mr = ibv_reg_mr(
      share_ctx_->pd,
      ctx->ctrl_send_msg,
      sizeof(control_message),
      IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_READ));
    CHECK_NOTNULL(ctx->ctrl_recv_msg_mr = ibv_reg_mr(
      share_ctx_->pd,
      ctx->ctrl_recv_msg,
      sizeof(control_message),
      IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE));
  }
  void build_connection(rdma_cm_id *id) {
    // 1. build share_ctx_: protection domain, completion channel and completion queue
    if (share_ctx_) {
      CHECK_EQ(share_ctx_->ctx, id->verbs)
        << "Cannot handle events in more than one context.";
    }
    else {
      share_ctx_ = new share_context;
      share_ctx_->ctx = id->verbs;
      CHECK_NOTNULL(share_ctx_->pd = ibv_alloc_pd(share_ctx_->ctx));
      CHECK_NOTNULL(share_ctx_->comp_channel = ibv_create_comp_channel(share_ctx_->ctx));
      CHECK_NOTNULL(share_ctx_->cq = ibv_create_cq(share_ctx_->ctx, 20, // minimum cq size
        NULL, share_ctx_->comp_channel, 0));
      CHECK_EQ(ibv_req_notify_cq(share_ctx_->cq, 0), 0);
      if (!polling_thread_) {
        polling_thread_ = std::unique_ptr<std::thread>(
          new std::thread(&NewRdmaVan::polling_completion_queue, this));
      }
      // register send/recv buffer
      posix_memalign((void **)&share_ctx_->recv_buffer, sysconf(_SC_PAGESIZE), BUFFER_SIZE);
      posix_memalign((void **)&share_ctx_->send_buffer, sysconf(_SC_PAGESIZE), BUFFER_SIZE);
      CHECK_NOTNULL(share_ctx_->recv_buffer_mr = ibv_reg_mr(
        share_ctx_->pd,
        share_ctx_->recv_buffer,
        BUFFER_SIZE,
        IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE));
      CHECK_NOTNULL(share_ctx_->send_buffer_mr = ibv_reg_mr(
        share_ctx_->pd,
        share_ctx_->send_buffer,
        BUFFER_SIZE,
        IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_READ)); // send_buffer for remote read
    }
    // 2. build queue pair
    ibv_qp_init_attr qp_attr;
    build_qp_attr(&qp_attr);
    CHECK_EQ(rdma_create_qp(id, share_ctx_->pd, &qp_attr), 0);
    // 3. build connection level context
    build_context(id);
  }
  void post_receive(rdma_cm_id *id) {
    connection *conn = (connection *)id->context;
    struct ibv_recv_wr wr, *bad_wr = nullptr;
    struct ibv_sge sge;
    memset(&wr, 0, sizeof(ibv_send_wr));
    wr.wr_id = (uintptr_t)id;
    wr.sg_list = &sge;
    wr.num_sge = 1;
    sge.addr = (uintptr_t)conn->ctrl_recv_msg;
    sge.length = sizeof(control_message);
    sge.lkey = conn->ctrl_recv_msg_mr->lkey;
    CHECK_EQ(ibv_post_recv(id->qp, &wr, &bad_wr), 0);
  }
  void post_send(rdma_cm_id *id)
  {
    connection *ctx = (connection *)id->context;
    struct ibv_send_wr wr, *bad_wr = nullptr;
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

    CHECK_EQ(ibv_post_send(id->qp, &wr, &bad_wr), 0);
  }
  void post_read(rdma_cm_id *id) {
    connection *ctx = (connection *)id->context;
    struct ibv_send_wr wr, *bad_wr = nullptr;
    struct ibv_sge sge;

    memset(&sg, 0, sizeof(sg));
    sg.addr = (uintptr_t)share_ctx_->recv_buffer;
    sg.length = conn->ctrl_recv_msg->message_size;
    sg.lkey = share_ctx_->recv_buffer_mr->lkey;

    memset(&wr, 0, sizeof(wr));
    wr.wr_id = (uintptr_t)id;
    wr.sg_list = &sg;
    wr.num_sge = 1;
    wr.opcode = IBV_WR_RDMA_READ;
    wr.send_flags = IBV_SEND_SIGNALED;
    wr.wr.rdma.remote_addr = conn->peer_addr
      wr.wr.rdma.rkey = conn->peer_rkey;
    CHECK_EQ(ibv_post_send(id->qp, &wr, &bad_wr), 0);
  }
  void polling_completion_queue() {
    struct ibv_cq *cq;
    struct ibv_wc wc;
    while (!stop_) {
      void *ctx;
      CHECK_EQ(ibv_get_cq_event(share_ctx_->comp_channel, &cq, &ctx), 0);
      ibv_ack_cq_events(cq, 1);
      CHECK_EQ(ibv_req_notify_cq(cq, 0), 0);

      while (ibv_poll_cq(cq, 1, &wc)) {
        CHECK_EQ(wc.status, IBV_WC_SUCCESS) <<
          "polling_cq: status is not IBV_WC_SUCCESS";
        if ((wc.opcode & IBV_WC_RECV) || (wc.opcode & IBV_WR_RDMA_READ)) {
          std::unique_lock<std::mutex> lk(receive_mutex_);
          receive_mutex_cv_.wait(lk, [] { return read_; });
          read_ = false;
          receive_type_ = (wc.opcode & IBV_WC_RECV) ? RECEIVE : READ;
          receive_id_ = (rdma_cm_id *)(uintptr_t)(wc.wr_id);
          receive_mutex_cv_.notify_all();
        }
        else {
          continue;
        }
      }
    }
  }
  int pack_data(const Message& msg, rdma_cm_id *id) {
    int n = msg.data.size();
    CHECK_GE(n, 2);
    CHECK_LE(n, 3);
    char *p = share_ctx_->send_buffer;
    connection *conn = (connection *)id->context;
    int message_size = 0;
    for (int i = 0; i < n; ++i) {
      SArray<char>* data = new SArray<char>(msg.data[i]);
      int data_size = data->size();
      memcpy(p, data->data(), data->size());
      p += data_size;
      message_size += data_size;
      conn->ctrl_send_msg->data_sizes[i] = data_size;
    }
    if (n == 2) conn->ctrl_send_msg->data_size[3] = -1;
    conn->ctrl_send_msg->command = MSG_DATA;
    conn->ctrl_send_msg->sender_id = my_node_.id;
    conn->ctrl_send_msg->message_size = message_size;
    return message_size;
  }


private:
  enum control_command {
    MSG_MR,
    MSG_REQ_ID,
    MSG_ID,
    MSG_META,
    MSG_DATA,
    MSG_READ_DONE
  };
  struct control_message {
    control_command command;
    struct {
      uint64_t addr;
      uint32_t rkey;
    } mr;
    int sender_id;
    int sender_role;
    int receiver_id;
    int message_size;
    int data_size;
    int data_sizes[3];
  };
  struct connection {
    //rdma_cm_id *id;
    //ibv_qp *qp;

    control_message *ctrl_send_msg;
    control_message *ctrl_recv_msg;
    ibv_mr *ctrl_send_msg_mr;
    ibv_mr *ctrl_recv_msg_mr;

    uint64_t peer_addr;
    uint32_t peer_rkey;
    int peer_id;
    int peer_role;
  };
  struct share_context{
    ibv_context *ctx;
    ibv_pd *pd;
    ibv_cq *cq;
    ibv_comp_channel *comp_channel;

    struct ibv_mr *recv_buffer_mr;
    struct ibv_mr *send_buffer_mr;
    char *send_buffer;
    char *recv_buffer;
  };
  
  // for connection
  bool stop_ = false;
  share_context *share_ctx_ = nullptr;
  int num_servers_ = 0;
  int num_workers_ = 0;
  rdma_cm_id *passive_listen_id_;
  rdma_event_channel *passive_channel_;
  std::mutex connections_mutex_;
  std::condition_variable conn_cv_;
  std::unordered_map<int, rdma_cm_id*> connections_;
  std::unique_ptr<std::thread> passive_listen_thread_;
  std::unique_ptr<std::thread> polling_thread_;
  std::mutex  ;
  std::vector<std::unique_ptr<std::thread>> active_connect_threads_;

  std::mutex send_mutex_;
  // for receiving messages and polling completion queues
  std::mutex receive_mutex_;
  std::condition_variable receive_mutex_cv_;
  bool read_ = true;
  rdma_cm_id *receive_id_; // whome get message from
  Meta meta_from_myself_;  // send to/receive from myself
  enum completion_type {
    RECEIVE,
    READ
  };
  completion_type receive_type_;
};
}  // namespace ps
#endif