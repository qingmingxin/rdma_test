#include <iostream> // 替换 stdio.h
#include <cstdlib>  // 替换 stdlib.h
#include <cstring>  // 替换 string.h
#include <unistd.h> // 依然使用 unistd.h 处理 POSIX 相关调用
#include <rdma/rdma_cma.h>
#include <sys/time.h>
#include <netdb.h>
#include <memory>

#define TEST_NZ(x)                                            \
    do                                                        \
    {                                                         \
        if ((x))                                              \
            die("error: " #x " failed (returned non-zero)."); \
    } while (0)
#define TEST_Z(x)                                              \
    do                                                         \
    {                                                          \
        if (!(x))                                              \
            die("error: " #x " failed (returned zero/null)."); \
    } while (0)

const int BUFFER_SIZE = 1 * 1024 * 1024 * 1024;

struct context
{
    struct ibv_context *ctx;
    struct ibv_pd *pd;
    struct ibv_cq *cq;
    struct ibv_comp_channel *comp_channel;

    pthread_t cq_poller_thread;
};
struct memory_info
{
    uint64_t addr;   // 内存的虚拟地址
    uint32_t rkey;   // 远程密钥
    uint32_t length; // 内存大小
};

struct connection
{
    struct ibv_qp *qp;

    struct ibv_mr *recv_mr;
    struct ibv_mr *send_mr;

    char *recv_region;
    char *send_region;
};

static void die(const char *reason);

static void build_context(struct ibv_context *verbs);
static void build_qp_attr(struct ibv_qp_init_attr *qp_attr);
static void *poll_cq(void *);
static void post_receives(struct connection *conn);
static void register_memory(struct connection *conn);

static void on_completion(struct ibv_wc *wc);
static int on_connect_request(struct rdma_cm_id *id);
static int on_connection(void *context);
static int on_disconnect(struct rdma_cm_id *id);
static int on_event(struct rdma_cm_event *event);

static struct context *s_ctx = NULL;
static struct memory_info *local_mem_info = NULL;
static struct connection *conn = NULL;
int main(int argc, char **argv)
{
#if _USE_IPV6
    struct sockaddr_in6 addr;
#else
    struct sockaddr_in addr;
#endif
    struct rdma_cm_event *event = NULL;
    struct rdma_cm_id *listener = NULL;
    struct rdma_event_channel *ec = NULL;
    uint16_t port = 0;

    memset(&addr, 0, sizeof(addr));
#if _USE_IPV6
    addr.sin6_family = AF_INET6;
#else
    addr.sin_family = AF_INET;
#endif

    TEST_Z(ec = rdma_create_event_channel());
    TEST_NZ(rdma_create_id(ec, &listener, NULL, RDMA_PS_IB));
    TEST_NZ(rdma_bind_addr(listener, (struct sockaddr *)&addr));
    TEST_NZ(rdma_listen(listener, 10)); /* backlog=10 is arbitrary */

    port = ntohs(rdma_get_src_port(listener)); // rdma_get_src_port 返回listener对应的tcp 端口

    printf("listening on port %d.\n", port);

    while (rdma_get_cm_event(ec, &event) == 0)
    {
        struct rdma_cm_event event_copy;

        memcpy(&event_copy, event, sizeof(*event));
        rdma_ack_cm_event(event);

        if (on_event(&event_copy))
            break;
    }

    rdma_destroy_id(listener);
    rdma_destroy_event_channel(ec);

    return 0;
}

void die(const char *reason)
{
    fprintf(stderr, "%s\n", reason);
    exit(EXIT_FAILURE);
}

void build_context(struct ibv_context *verbs)
{
    if (s_ctx)
    {
        if (s_ctx->ctx != verbs)
            die("cannot handle events in more than one context.");

        return;
    }

    s_ctx = (struct context *)malloc(sizeof(struct context));

    s_ctx->ctx = verbs;

    TEST_Z(s_ctx->pd = ibv_alloc_pd(s_ctx->ctx));
    TEST_Z(s_ctx->comp_channel = ibv_create_comp_channel(s_ctx->ctx));
    TEST_Z(s_ctx->cq = ibv_create_cq(s_ctx->ctx, 10, NULL, s_ctx->comp_channel, 0)); /* cqe=10 is arbitrary */
    TEST_NZ(ibv_req_notify_cq(s_ctx->cq, 0));

    TEST_NZ(pthread_create(&s_ctx->cq_poller_thread, NULL, poll_cq, NULL));
}

void build_qp_attr(struct ibv_qp_init_attr *qp_attr)
{
    memset(qp_attr, 0, sizeof(*qp_attr));

    qp_attr->send_cq = s_ctx->cq;
    qp_attr->recv_cq = s_ctx->cq;
    qp_attr->qp_type = IBV_QPT_RC;

    qp_attr->cap.max_send_wr = 10;
    qp_attr->cap.max_recv_wr = 10;
    qp_attr->cap.max_send_sge = 1;
    qp_attr->cap.max_recv_sge = 1;
}

void *poll_cq(void *ctx)
{
    struct ibv_cq *cq;
    struct ibv_wc wc;

    while (1)
    {
        TEST_NZ(ibv_get_cq_event(s_ctx->comp_channel, &cq, &ctx));
        ibv_ack_cq_events(cq, 1);
        TEST_NZ(ibv_req_notify_cq(cq, 0));

        while (ibv_poll_cq(cq, 1, &wc))
            on_completion(&wc);
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
    sge.length = BUFFER_SIZE;
    sge.lkey = conn->recv_mr->lkey;

    TEST_NZ(ibv_post_recv(conn->qp, &wr, &bad_wr));
}

void register_memory(struct connection *conn)
{
    conn->send_region = new char[1024];
    conn->recv_region = new char[1 * 1024 * 1024 * 1024];

    TEST_Z(conn->send_mr = ibv_reg_mr(
               s_ctx->pd,
               conn->send_region,
               1024,
               0));

    TEST_Z(conn->recv_mr = ibv_reg_mr(
               s_ctx->pd,
               conn->recv_region,
               (1 * 1024 * 1024 * 1024),
               IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE));
    local_mem_info = (struct memory_info *)malloc(sizeof(struct memory_info));
    local_mem_info->addr = (uintptr_t)conn->recv_mr->addr;
    local_mem_info->rkey = conn->recv_mr->rkey;
    local_mem_info->length = conn->recv_mr->length;
}

void on_completion(struct ibv_wc *wc)
{
    std::cout << "wc status" << wc->status << std::endl;
    if (wc->status != IBV_WC_SUCCESS)
        die("on_completion: status is not IBV_WC_SUCCESS.");
    std::cout << "wc opcode" << wc->opcode << std::endl;

    if (wc->opcode & IBV_WC_RECV)
    {
        struct connection *conn = (struct connection *)(uintptr_t)wc->wr_id;

        printf("received message: %s\n", conn->recv_region);
    }
    else if (wc->opcode == IBV_WC_SEND)
    {
        printf("send completed successfully.\n");
    }
}

int on_connect_request(struct rdma_cm_id *id)
{
    struct ibv_qp_init_attr qp_attr;
    struct rdma_conn_param cm_params;

    printf("received connection request.\n");

    build_context(id->verbs);
    build_qp_attr(&qp_attr);

    TEST_NZ(rdma_create_qp(id, s_ctx->pd, &qp_attr));

    id->context = conn = (struct connection *)malloc(sizeof(struct connection));
    conn->qp = id->qp;

    register_memory(conn);
    post_receives(conn);

    memset(&cm_params, 0, sizeof(cm_params));
    TEST_NZ(rdma_accept(id, &cm_params));

    return 0;
}

int on_connection(void *context)
{
    struct connection *conn = (struct connection *)context;
    struct ibv_send_wr wr, *bad_wr = NULL;
    struct ibv_sge sge;

    struct timeval tv = {};
    gettimeofday(&tv, NULL);
    long long tmp;
    tmp = tv.tv_sec;
    tmp = tmp * 1000;
    tmp = tmp + (tv.tv_usec / 1000);

    printf("connected. posting send...\n");
    printf("prepare to send local addr info...");
    memcpy(conn->send_region, local_mem_info, sizeof(struct memory_info));
    sge.addr = (uintptr_t)conn->send_region;
    sge.length = sizeof(struct memory_info);
    sge.lkey = conn->send_mr->lkey;

    memset(&wr, 0, sizeof(wr));
    wr.wr_id = (uintptr_t)&conn;
    wr.opcode = IBV_WR_SEND;
    wr.sg_list = &sge;
    wr.num_sge = 1;
    wr.send_flags = IBV_SEND_SIGNALED;

    TEST_NZ(ibv_post_send(conn->qp, &wr, &bad_wr));

    return 0;
}

int on_disconnect(struct rdma_cm_id *id)
{
    struct connection *conn = (struct connection *)id->context;

    printf("peer disconnected.\n");

    rdma_destroy_qp(id);

    ibv_dereg_mr(conn->send_mr);
    ibv_dereg_mr(conn->recv_mr);

    free(conn->send_region);
    free(conn->recv_region);

    free(conn);

    rdma_destroy_id(id);

    return 0;
}

int on_event(struct rdma_cm_event *event)
{
    int r = 0;
    printf("event:%d", event->event);

    if (event->event == RDMA_CM_EVENT_CONNECT_REQUEST)
        r = on_connect_request(event->id);
    else if (event->event == RDMA_CM_EVENT_ESTABLISHED)
        r = on_connection(event->id->context);
    else if (event->event == RDMA_CM_EVENT_DISCONNECTED)
        r = on_disconnect(event->id);
    else
        die("on_event: unknown event.");

    return r;
}