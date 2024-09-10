#include <iostream> // 替换 stdio.h
#include <cstdlib>  // 替换 stdlib.h
#include <cstring>  // 替换 string.h
#include <unistd.h> // 依然使用 unistd.h 处理 POSIX 相关调用
#include <rdma/rdma_cma.h>
#include <sys/time.h>
#include <netdb.h>
#include <vector>
#include <memory>

const int BUFFER_SIZE = 1 * 1024 * 1024 * 1024;
const int RECV_BUFFER_SIZE = 1024;
const int TIMEOUT_IN_MS = 500;
static long long start_time;
static long long complete_time;
struct memory_info
{
    uint64_t addr;   // 内存的虚拟地址
    uint32_t rkey;   // 远程密钥
    uint32_t length; // 内存大小
};

struct remote_addr_info
{
    uint64_t addr;
    uint32_t rkey;
};

struct context
{
    struct ibv_context *ctx;
    struct ibv_pd *pd;
    struct ibv_cq *cq;
    struct ibv_comp_channel *comp_channel;

    pthread_t cq_poller_thread;
};

struct connection
{
    struct rdma_cm_id *id;
    std::vector<struct ibv_qp *> qps;
    std::vector<struct ibv_mr *> send_mrs;

    struct ibv_mr *recv_mr;

    char *recv_region;
    char *send_region;

    int num_completions;
};
static struct memory_info *server_mem_info = new memory_info();
static struct context *s_ctx = NULL;
static struct connection *conn_static = NULL;
static int qp_nums = 1; // QP数量
static void build_context(struct ibv_context *verbs);
static void build_qp_attr(struct ibv_qp_init_attr *qp_attr);
static void *poll_cq(void *);
static void post_receives(struct connection *conn);
static void register_memory(struct connection *conn);

static int on_addr_resolved(struct rdma_cm_id *id);
static void on_completion(struct ibv_wc *wc);
static int on_connection(void *context);
static int on_disconnect(struct rdma_cm_id *id);
static int on_event(struct rdma_cm_event *event);
static int on_route_resolved(struct rdma_cm_id *id);

void build_context(struct ibv_context *verbs)
{
    if (s_ctx)
    {
        if (s_ctx->ctx != verbs)
            std::cout << "cannot handle events in more than one context." << std::endl;

        return;
    }
    s_ctx = new context();

    s_ctx->ctx = verbs;

    s_ctx->pd = ibv_alloc_pd(s_ctx->ctx);
    s_ctx->comp_channel = ibv_create_comp_channel(s_ctx->ctx);
    s_ctx->cq = ibv_create_cq(s_ctx->ctx, 10, NULL, s_ctx->comp_channel, 0); /* cqe=10 is arbitrary */
    ibv_req_notify_cq(s_ctx->cq, 0);

    pthread_create(&s_ctx->cq_poller_thread, NULL, poll_cq, NULL);
}

void build_qp_attr(struct ibv_qp_init_attr *qp_attr)
{
    memset(qp_attr, 0, sizeof(*qp_attr));

    qp_attr->send_cq = s_ctx->cq;
    qp_attr->recv_cq = s_ctx->cq;
    qp_attr->qp_type = IBV_QPT_RC;

    qp_attr->cap.max_send_wr = 10;
    qp_attr->cap.max_recv_wr = 10;
    qp_attr->cap.max_send_sge = 3;
    qp_attr->cap.max_recv_sge = 3;
}

void *poll_cq(void *ctx)
{
    struct ibv_cq *cq;
    struct ibv_wc wc[qp_nums];
    int num_entries = 0;

    while (1)
    {
        ibv_get_cq_event(s_ctx->comp_channel, &cq, &ctx);
        ibv_ack_cq_events(cq, 1);
        ibv_req_notify_cq(cq, 0);

        while ((num_entries = ibv_poll_cq(cq, qp_nums, wc)) > 0)
        {
            std::cout << "get cq number:" << num_entries << std::endl;
            on_completion(wc);
        }
    }

    return NULL;
}

void post_receives(struct connection *conn)
{
    struct ibv_recv_wr wr, *bad_wr = NULL;
    struct ibv_sge sge;

    wr.wr_id = (uintptr_t)conn;
    wr.next = NULL;
    wr.sg_list = &sge;
    wr.num_sge = 1;

    sge.addr = (uintptr_t)conn->recv_region;
    sge.length = RECV_BUFFER_SIZE;
    sge.lkey = conn->recv_mr->lkey;

    ibv_post_recv(conn->qps[0], &wr, &bad_wr);
}

void register_memory(struct connection *conn)
{
    conn->send_region = new char[BUFFER_SIZE];
    conn->recv_region = new char[RECV_BUFFER_SIZE];
    for (int i = 0; i < qp_nums; ++i)
    {
        conn->send_mrs.push_back(ibv_reg_mr(
            s_ctx->pd, conn->send_region + i * (BUFFER_SIZE / qp_nums), (BUFFER_SIZE / qp_nums),
            IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE));
    }

    conn->recv_mr = ibv_reg_mr(s_ctx->pd, conn->recv_region, RECV_BUFFER_SIZE, IBV_ACCESS_LOCAL_WRITE);
}

int on_addr_resolved(struct rdma_cm_id *id)
{
    struct ibv_qp_init_attr qp_attr;
    conn_static = new connection();

    printf("address resolved.\n");

    build_context(id->verbs);
    build_qp_attr(&qp_attr);
    for (int i = 0; i < qp_nums; i++)
    {
        rdma_create_qp(id, s_ctx->pd, &qp_attr);
        id->context = static_cast<void *>(conn_static);
        conn_static->qps.push_back(id->qp);
        conn_static->id = id;
        conn_static->num_completions = 0;
        // id->context = static_cast<void *>(conn[i]);
        // conn[i]->id = id;
        // conn[i]->qp = id->qp;
        // conn[i]->num_completions = 0;
    }

    register_memory(conn_static);
    post_receives(conn_static);

    rdma_resolve_route(id, TIMEOUT_IN_MS);

    return 0;
}
void on_completion(struct ibv_wc *wc)
{
    struct connection *conn = (struct connection *)(uintptr_t)wc->wr_id;
    struct timeval tv = {};
    gettimeofday(&tv, NULL);
    complete_time = tv.tv_sec;
    complete_time = complete_time * 1000;
    complete_time = complete_time + (tv.tv_usec / 1000);
    std::cout << "Completion status: " << wc->status << std::endl;
    std::cout << "opcode:" << wc->opcode << std::endl;

    if (wc->status != IBV_WC_SUCCESS)
        std::cout << "on_completion: status is not IBV_WC_SUCCESS." << std::endl;

    if (wc->opcode & IBV_WC_RECV)
    {
        printf("received message: %s\n", conn->recv_region);
        struct connection *conn = (struct connection *)(uintptr_t)wc->wr_id;
        memcpy(server_mem_info, conn->recv_region, sizeof(struct memory_info));
        printf("Received remote memory info: addr=%lu, rkey=%u, length=%u\n",
               server_mem_info->addr, server_mem_info->rkey, server_mem_info->length);
    }
    else if (wc->opcode == IBV_WC_SEND)
        printf("send completed successfully.\n");
    else if (wc->opcode == IBV_WC_RDMA_WRITE)
    {

        printf("rdma write successfully.\n");

        std::cout << "complete time:" << complete_time << std::endl;
        std::cout << "sum time:" << complete_time - start_time << std::endl;
    }
    else
        std::cout << "on_completion: completion isn't a send or a receive." << std::endl;
}

int on_connection(void *context)
{
    struct connection *conn = conn_static;
    struct ibv_send_wr wr[qp_nums], *bad_wr[qp_nums];
    struct ibv_sge sge[qp_nums];

    std::cout << server_mem_info->addr << std::endl;
    while (!server_mem_info->addr)
    {
    };
    std::cout << "prepare write to server" << std::endl;
    // snprintf(conn->send_region, 100, "message from client side with pid %d", getpid());
    for (int i = 0; i < qp_nums; i++)
    {
        memset(conn->send_region + i * (BUFFER_SIZE / qp_nums), 65 + i, BUFFER_SIZE / qp_nums);
    }
    struct timeval tv = {};
    gettimeofday(&tv, NULL);
    start_time = tv.tv_sec;
    start_time = start_time * 1000;
    start_time = start_time + (tv.tv_usec / 1000);
    std::cout << "start time:" << start_time << std::endl;
    // printf("connected. posting send...\n");
    for (int i = 0; i < qp_nums; ++i)
    {
        memset(&wr[i], 0, sizeof(wr[i]));
        wr[i].wr_id = (uintptr_t)conn;
        wr[i].opcode = IBV_WR_RDMA_WRITE;
        wr[i].sg_list = &sge[i];
        wr[i].num_sge = 1;
        sge[i].addr = (uintptr_t)(conn->send_region + i * (BUFFER_SIZE / qp_nums));
        sge[i].length = BUFFER_SIZE / qp_nums;
        sge[i].lkey = conn->send_mrs[i]->lkey;
        std::cout << "conn->send_mrs[i]->lkey" << conn->send_mrs[i]->lkey << std::endl;

        // 计算远程地址偏移
        wr[i].wr.rdma.remote_addr = server_mem_info->addr + i * (BUFFER_SIZE / qp_nums);
        wr[i].wr.rdma.rkey = server_mem_info->rkey;
        std::cout << "wr.remote_addr:" << wr[i].wr.rdma.remote_addr << std::endl;

        struct timeval tv = {};
        gettimeofday(&tv, NULL);
        start_time = tv.tv_sec * 1000 + tv.tv_usec / 1000;
        wr[i].send_flags = IBV_SEND_SIGNALED;
    }
    for (int i = 0; i < qp_nums; i++)
    {
        if (ibv_post_send(conn->qps[i], &wr[i], &bad_wr[i]))
        {
            fprintf(stderr, "Error, ibv_post_send() failed for chunk %d\n", i + 1);
        }
    }

    // memset(&wr, 0, sizeof(wr));

    // wr.wr_id = (uintptr_t)conn;
    // wr.opcode = IBV_WR_RDMA_WRITE;
    // wr.sg_list = &sge;
    // wr.num_sge = 1;
    // wr.send_flags = IBV_SEND_SIGNALED;

    // sge.addr = (uintptr_t)conn->send_region;
    // sge.length = 1 * 1024 * 1024 * 1024;
    // std::cout << "sge.length:" << sge.length << std::endl;
    // sge.lkey = conn->send_mr->lkey;

    // wr.wr.rdma.remote_addr = server_mem_info->addr;
    // wr.wr.rdma.rkey = server_mem_info->rkey;
    // struct timeval tv = {};
    // gettimeofday(&tv, NULL);
    // start_time = tv.tv_sec;
    // start_time = start_time * 1000;
    // start_time = start_time + (tv.tv_usec / 1000);
    // std::cout << "start time:" << start_time << std::endl;
    // if (ibv_post_send(conn->qp, &wr, &bad_wr))
    // {
    //     fprintf(stderr, "Error, ibv_post_send() failed\n");
    // }

    return 0;
}

int on_disconnect(struct rdma_cm_id *id)
{
    struct connection *conn = (struct connection *)id->context;

    printf("disconnected.\n");

    rdma_destroy_qp(id);
    for (int i = 0; i < qp_nums; i++)
    {
        ibv_dereg_mr(conn->send_mrs[i]);
        /* code */
    }

    ibv_dereg_mr(conn->recv_mr);

    free(conn->send_region);
    free(conn->recv_region);

    free(conn);

    rdma_destroy_id(id);

    return 1; /* exit event loop */
}

int on_event(struct rdma_cm_event *event)
{
    int r = 0;
    std::cout << "get event:" << event->event << std::endl;

    if (event->event == RDMA_CM_EVENT_ADDR_RESOLVED)
        r = on_addr_resolved(event->id);
    else if (event->event == RDMA_CM_EVENT_ROUTE_RESOLVED)
        r = on_route_resolved(event->id);
    else if (event->event == RDMA_CM_EVENT_ESTABLISHED)
        r = on_connection(event->id->context);
    else if (event->event == RDMA_CM_EVENT_DISCONNECTED)
        r = on_disconnect(event->id);
    else
        std::cout << "on_event: unknown event." << std::endl;

    return r;
}

int on_route_resolved(struct rdma_cm_id *id)
{
    struct rdma_conn_param cm_params;

    printf("route resolved.\n");

    memset(&cm_params, 0, sizeof(cm_params));
    int ret = rdma_connect(id, &cm_params);
    std::cout << "rdma_connect ret:" << ret << std::endl;

    return 0;
}
int main(int argc, char **argv)
{
    std::cout << "client start..." << std::endl;
    struct addrinfo *addr;
    struct remote_addr_info *rai;
    struct rdma_cm_event *event = NULL;
    struct rdma_cm_id *conn = NULL;
    struct rdma_event_channel *ec = NULL;
    if (argc != 3)
        std::cerr << "usage: client <server-address> <server-port> (<qp nums>)" << std::endl;
    if (argc == 4)
        qp_nums = atoi(argv[3]);
    std::cout << "get qp nums:" << qp_nums << std::endl;
    getaddrinfo(argv[1], argv[2], NULL, &addr);
    ec = rdma_create_event_channel();
    rdma_create_id(ec, &conn, NULL, RDMA_PS_IB);
    rdma_resolve_addr(conn, NULL, addr->ai_addr, TIMEOUT_IN_MS);
    // freeaddrinfo(addr);
    while (rdma_get_cm_event(ec, &event) == 0)
    {
        struct rdma_cm_event event_copy;

        memcpy(&event_copy, event, sizeof(*event));
        rdma_ack_cm_event(event);

        if (on_event(&event_copy))
            break;
    }

    rdma_destroy_event_channel(ec);

    return 0;
}
