#ifndef __CLUSTER_H
#define __CLUSTER_H

/*-----------------------------------------------------------------------------
 * Redis cluster data structures, defines, exported API.
 *----------------------------------------------------------------------------*/
/* redis 集群的总槽位数量 16384 */
#define CLUSTER_SLOTS 16384
/* 集群在线 */
#define CLUSTER_OK 0            /* Everything looks ok */
/* 集群失效 */
#define CLUSTER_FAIL 1          /* The cluster can't work */
/* 集群节点名字长度 */
#define CLUSTER_NAMELEN 40      /* sha1 hex length */
/* 默认情况下；集群实际通信的端口 = 用户指定端口 + CLUSTER_PORT_INCR （6379 + 10000 = 16379）*/
#define CLUSTER_PORT_INCR 10000 /* Cluster port = baseport + PORT_INCR */

/* The following defines are amount of time, sometimes expressed as
 * multiplicators of the node timeout value (when ending with MULT). */
/* 下面是和时间相关的一些常量，以 _MULT 结尾的常量会作为时间值的乘法因子来使用 */
/* 节点故障报告的乘法因子 */
#define CLUSTER_FAIL_REPORT_VALIDITY_MULT 2 /* Fail report validity. */
/* 撤销主节点 FAIL 状态的乘法因子 */
#define CLUSTER_FAIL_UNDO_TIME_MULT 2 /* Undo fail if master is back. */
/* 在进行手动故障转移之前需要等待的超时时间 */
#define CLUSTER_MF_TIMEOUT 5000 /* Milliseconds to do a manual failover. */
#define CLUSTER_MF_PAUSE_MULT 2 /* Master pause manual failover mult. */
#define CLUSTER_SLAVE_MIGRATION_DELAY 5000 /* Delay for slave migration. */

/* Redirection errors returned by getNodeByQuery(). */
/* 下面的标识是节点之间做槽位转移的时候，客户端需要重定向节点，服务器返回给客户端的标识 */
/* 当前节点可以处理这个命令 */
#define CLUSTER_REDIR_NONE 0          /* Node can serve the request. */
/* 所请求的键在其他槽 */
#define CLUSTER_REDIR_CROSS_SLOT 1    /* -CROSSSLOT request. */
/* 键所处的槽正在进行 rehash */
#define CLUSTER_REDIR_UNSTABLE 2      /* -TRYAGAIN redirection required */
/* 需要进行 ASK 重定向 */
#define CLUSTER_REDIR_ASK 3           /* -ASK redirection required. */
/* 需要进行 MOVED 重定向 */
#define CLUSTER_REDIR_MOVED 4         /* -MOVED redirection required. */
/* 如果集群状态不是 OK 状态 */
#define CLUSTER_REDIR_DOWN_STATE 5    /* -CLUSTERDOWN, global state. */
/* 当前节点未分配槽位 */
#define CLUSTER_REDIR_DOWN_UNBOUND 6  /* -CLUSTERDOWN, unbound slot. */
/* 当前节点仅允许读 */
#define CLUSTER_REDIR_DOWN_RO_STATE 7 /* -CLUSTERDOWN, allow reads. */

struct clusterNode;

/* clusterLink encapsulates everything needed to talk with a remote node. */
/* clusterLink 包含和其他节点通信的所有信息
 * 注：可以和 client 结构联想到一起，在服务端口（6379）获取到 socket 连接最后会创建一个 client 实例
 * 在集群端口（16379）获取到的 socket 连接最后会创建 clusterLink 实例*/
typedef struct clusterLink {
    /* 连接创建时间 */
    mstime_t ctime;             /* Link creation time */
    /* 和远程节点的连接，connection 是对 socket fd 的封装 */
    connection *conn;           /* Connection to remote node */
    /* 输出缓冲区，保存着等待发送给其他节点的消息 */
    sds sndbuf;                 /* Packet send buffer */
    /* 输入缓冲区，这里指向的其实是 clusterMsg 结构体 */
    char *rcvbuf;               /* Packet reception buffer */
    /* 输入缓冲区被使用的大小 */
    size_t rcvbuf_len;          /* Used size of rcvbuf */
    /* 输入缓冲区分配的大小 */
    size_t rcvbuf_alloc;        /* Allocated size of rcvbuf */
    /* 当前连接相关的节点 */
    struct clusterNode *node;   /* Node related to this link. Initialized to NULL when unknown */
    /* 是否是入站连接，这里其实就是当前链接有没有和具体的节点实例进行关联，
     * 如果没有关联，inbound 就是 1，关联好了就是 0 */
    int inbound;                /* 1 if this link is an inbound link accepted from the related node */
} clusterLink;

/* Cluster node flags and macros. */
/* 当前节点是主节点 */
#define CLUSTER_NODE_MASTER 1     /* The node is a master */
/* 当前节点是从节点 */
#define CLUSTER_NODE_SLAVE 2      /* The node is a slave */
/* 该节点疑似下线，需要对他的状态进行确认 */
#define CLUSTER_NODE_PFAIL 4      /* Failure? Need acknowledge */
/* 节点确定下线 */
#define CLUSTER_NODE_FAIL 8       /* The node is believed to be malfunctioning */
/* 节点是自己 */
#define CLUSTER_NODE_MYSELF 16    /* This node is myself */
/* 该节点还没有于当前节点进行一个 ping 通信 */
#define CLUSTER_NODE_HANDSHAKE 32 /* We have still to exchange the first ping */
/* 该节点还没有地址 */
#define CLUSTER_NODE_NOADDR   64  /* We don't know the address of this node */
/* 该节点发送了 meet 包给当前节点 */
#define CLUSTER_NODE_MEET 128     /* Send a MEET message to this node */
/* 有资格做副本迁移的主节点（这里其实就是说被该标识标记的节点是孤儿主节点，它下面没有副本节点，需要从其他主节点迁移副本节点给它） */
#define CLUSTER_NODE_MIGRATE_TO 256 /* Master eligible for replica migration. */
/* 从节点不会去做故障转移 */
#define CLUSTER_NODE_NOFAILOVER 512 /* Slave will not try to failover. */
#define CLUSTER_NODE_NULL_NAME "\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000"

/* 下面的宏是对 flags 属性做判断，即判断该节点的状态 */
#define nodeIsMaster(n) ((n)->flags & CLUSTER_NODE_MASTER)
#define nodeIsSlave(n) ((n)->flags & CLUSTER_NODE_SLAVE)
#define nodeInHandshake(n) ((n)->flags & CLUSTER_NODE_HANDSHAKE)
#define nodeHasAddr(n) (!((n)->flags & CLUSTER_NODE_NOADDR))
#define nodeWithoutAddr(n) ((n)->flags & CLUSTER_NODE_NOADDR)
#define nodeTimedOut(n) ((n)->flags & CLUSTER_NODE_PFAIL)
#define nodeFailed(n) ((n)->flags & CLUSTER_NODE_FAIL)
#define nodeCantFailover(n) ((n)->flags & CLUSTER_NODE_NOFAILOVER)

/* Reasons why a slave is not able to failover. */
/* 为什么从节点不能够做故障转移的原因 */
#define CLUSTER_CANT_FAILOVER_NONE 0
#define CLUSTER_CANT_FAILOVER_DATA_AGE 1
#define CLUSTER_CANT_FAILOVER_WAITING_DELAY 2
#define CLUSTER_CANT_FAILOVER_EXPIRED 3
#define CLUSTER_CANT_FAILOVER_WAITING_VOTES 4
#define CLUSTER_CANT_FAILOVER_RELOG_PERIOD (60*5) /* seconds. */

/* clusterState todo_before_sleep flags. */
#define CLUSTER_TODO_HANDLE_FAILOVER (1<<0)
#define CLUSTER_TODO_UPDATE_STATE (1<<1)
#define CLUSTER_TODO_SAVE_CONFIG (1<<2)
#define CLUSTER_TODO_FSYNC_CONFIG (1<<3)
#define CLUSTER_TODO_HANDLE_MANUALFAILOVER (1<<4)

/* Message types.
 *
 * Note that the PING, PONG and MEET messages are actually the same exact
 * kind of packet. PONG is the reply to ping, in the exact format as a PING,
 * while MEET is a special PING that forces the receiver to add the sender
 * as a node (if it is not already in the list). */
/* redis 集群节点之前的通信的消息分为以下11种，
 * 最后一个 CLUSTERMSG_TYPE_COUNT 是包类型计数边界，代码中做判断
 * 5（是否可以故障转移），6（回复可以故障转移），8（在手动故障转移的时候通知集群暂停处理客户端请求）三种包只有包头没有包体
 * 其他类型的包都由包头和包体两部分组成，包头格式相同，包体内容根据具体的类型填充 */
#define CLUSTERMSG_TYPE_PING 0          /* Ping */
#define CLUSTERMSG_TYPE_PONG 1          /* Pong (reply to Ping) */
#define CLUSTERMSG_TYPE_MEET 2          /* Meet "let's join" message */
#define CLUSTERMSG_TYPE_FAIL 3          /* Mark node xxx as failing */
#define CLUSTERMSG_TYPE_PUBLISH 4       /* Pub/Sub Publish propagation */
/* failover 授权请求包，由从节点向其他节点发起一个投票请求 */
#define CLUSTERMSG_TYPE_FAILOVER_AUTH_REQUEST 5 /* May I failover? */
/* failover 授权确认包，集群中同意投票给从节点，就会发出该类型的包给从节点 */
#define CLUSTERMSG_TYPE_FAILOVER_AUTH_ACK 6     /* Yes, you have my vote   */
#define CLUSTERMSG_TYPE_UPDATE 7        /* Another node slots configuration */
/* 手动故障转移包 */
#define CLUSTERMSG_TYPE_MFSTART 8       /* Pause clients for manual failover  */
#define CLUSTERMSG_TYPE_MODULE 9        /* Module cluster API message. */
#define CLUSTERMSG_TYPE_PUBLISHSHARD 10 /* Pub/Sub Publish shard propagation */
#define CLUSTERMSG_TYPE_COUNT 11        /* Total number of message types. */

/* Flags that a module can set in order to prevent certain Redis Cluster
 * features to be enabled. Useful when implementing a different distributed
 * system on top of Redis Cluster message bus, using modules. */
#define CLUSTER_MODULE_FLAG_NONE 0
#define CLUSTER_MODULE_FLAG_NO_FAILOVER (1<<1)
#define CLUSTER_MODULE_FLAG_NO_REDIRECTION (1<<2)

/* This structure represent elements of node->fail_reports. */
/* 每个节点都保存着一条对其他节点的下线报告 */
typedef struct clusterNodeFailReport {
    /* 报告目标节点已经下线 */
    struct clusterNode *node;  /* Node reporting the failure condition. */
    /* 最后一次和目标节点通信的时间，可以用这个时间来判断节点是否下线 */
    mstime_t time;             /* Time of the last report from this node. */
} clusterNodeFailReport;

/* 当前节点自身在集群中的信息 */
typedef struct clusterNode {
    /* 节点创建时间 */
    mstime_t ctime; /* Node object creation time. */
    /* 节点名，有大小限制 40 个字符 */
    char name[CLUSTER_NAMELEN]; /* Node name, hex string, sha1-size */
    /* 当前节点的状态，也记录了节点在集群中的角色 */
    int flags;      /* CLUSTER_NODE_... */
    /* 节点当前的配置纪元，用于节点的故障转移 */
    uint64_t configEpoch; /* Last configEpoch observed for this node */
    /* 由这个节点负责处理的槽
     * 一共有 CLUSTER_SLOTS / 8 个字节长 （一个字节 8 比特位，一位标识一个槽）
     * 每个字节的每位记录一个槽的保存状态
     * 位的值为 1，标识槽正由本节点处理，为 0 标识非本节点处理的槽 */
    unsigned char slots[CLUSTER_SLOTS/8]; /* slots handled by this node */
    uint16_t *slot_info_pairs; /* Slots info represented as (start/end) pair (consecutive index). */
    int slot_info_pairs_count; /* Used number of slots in slot_info_pairs */
    /* 当前节点管理的槽位数量 */
    int numslots;   /* Number of slots handled by this node */
    /* 如果该节点是主节点，该属性记录该主节点下的从节点数量 */
    int numslaves;  /* Number of slave nodes, if this is a master */
    /* 指向从节点数组的指针 */
    struct clusterNode **slaves; /* pointers to slave nodes */
    /* 如果该节点是从节点，指向该从节点服务的主节点 */
    struct clusterNode *slaveof; /* pointer to the master node. Note that it
                                    may be NULL even if the node is a slave
                                    if we don't have the master node in our
                                    tables. */
    unsigned long long last_in_ping_gossip; /* The number of the last carried in the ping gossip section */
    mstime_t ping_sent;      /* Unix time we sent latest ping */
    mstime_t pong_received;  /* Unix time we received the pong */
    mstime_t data_received;  /* Unix time we received any data */
    mstime_t fail_time;      /* Unix time when FAIL flag was set */
    mstime_t voted_time;     /* Last time we voted for a slave of this master */
    mstime_t repl_offset_time;  /* Unix time we received offset for this node */
    mstime_t orphaned_time;     /* Starting time of orphaned master condition */
    /* 该节点已知的副本偏移量 */
    long long repl_offset;      /* Last known repl offset for this node. */
    /* ip 地址 */
    char ip[NET_IP_STR_LEN];    /* Latest known IP address of this node */
    /* 节点的主机名 */
    sds hostname;               /* The known hostname for this node */
    /* 节点的端口号 */
    int port;                   /* Latest known clients port (TLS or plain). */
    /* 客户端实际传输数据的端口 */
    int pport;                  /* Latest known clients plaintext port. Only used
                                   if the main clients port is for TLS. */
    /* 该节点的集群端口号 */
    int cport;                  /* Latest known cluster port of this node. */
    /* 下面的两个 link 需要注意一下，集群通信每个节点既要有客户端又要有服务端，
     * redis 集群设计，TCP 服务端做 read 也就是读取其他节点发送来的数据，
     * TCP 客户端服务写数据给其他节点，这里 link 是客户端生成的 socket fd 封装的。
     * inbound_link (入站连接，从名字就能看出是接收数据) 由服务端 accept 到的 socket fd 封装的。
     * 注：集群中两个节点之间如果是处于握手状态的时候，TCP 服务端会发送 PING 包，只有这里会做写，
     * TCP 客户端也类似，会读取握手阶段 TCP 服务端发送来的 PING 包，也只有这里会做读处理 */
    /* 该节点相关的连接对象（连接状态是 established），这个 link 是 TCP 客户端发送数据的 link */
    clusterLink *link;          /* TCP/IP link established toward this node */
    /* accept 到的连接，这个是 TCP 服务端生成的 link，用来接收数据 */
    clusterLink *inbound_link;  /* TCP/IP link accepted from this node */
    /* 该节点保存下线通知的链表 */
    list *fail_reports;         /* List of nodes signaling this as failing */
} clusterNode;

/* Slot to keys for a single slot. The keys in the same slot are linked together
 * using dictEntry metadata. */
/* 保存槽位中键值信息 */
typedef struct slotToKeys {
    /* 槽位中键的数量 */
    uint64_t count;             /* Number of keys in the slot. */
    /* 槽位中第一个键值对 */
    dictEntry *head;            /* The first key-value entry in the slot. */
} slotToKeys;

/* Slot to keys mapping for all slots, opaque outside this file. */
/* 集群槽位信息 */
struct clusterSlotToKeyMapping {
    /* 所有槽位信息，一个 16384 大小的数组 */
    slotToKeys by_slot[CLUSTER_SLOTS];
};

/* Dict entry metadata for cluster mode, used for the Slot to Key API to form a
 * linked list of the entries belonging to the same slot. */
typedef struct clusterDictEntryMetadata {
    dictEntry *prev;            /* Prev entry with key in the same slot */
    dictEntry *next;            /* Next entry with key in the same slot */
} clusterDictEntryMetadata;

/* 集群状态，每个节点都保存着一个这样的状态，记录了它们眼中集群的样子 */
typedef struct clusterState {
    /* 指向当前节点的指针 */
    clusterNode *myself;  /* This node */
    /* 集群当前纪元，用于故障转移 */
    uint64_t currentEpoch;
    /* 集群状态 */
    int state;            /* CLUSTER_OK, CLUSTER_FAIL, ... */
    /* 集群中至少处理一个槽的节点数量 */
    int size;             /* Num of master nodes with at least one slot */
    /* 保存集群节点的字典，键是节点名称，值是 clusterNode 结构的指针 */
    dict *nodes;          /* Hash table of name -> clusterNode structures */
    /* 集群节点黑名单（包括 myself），可以防止在集群中的节点二次加入集群
     * 黑名单可以防止被 forget 的节点重新添加到集群节点 */
    dict *nodes_black_list; /* Nodes we don't re-add for a few seconds. */
    /* 记录要从当前节点迁移到目标节点的槽，以及迁移的目标节点 */
    clusterNode *migrating_slots_to[CLUSTER_SLOTS];
    /* 记录从其他节点迁移出来的槽 */
    clusterNode *importing_slots_from[CLUSTER_SLOTS];
    /* 负责处理各个槽的节点 */
    clusterNode *slots[CLUSTER_SLOTS];
    rax *slots_to_channels;
    /* The following fields are used to take the slave state on elections. */
    /* 之前或下一次选举的时间，主要是用来限制当前节点下一次投票发起的时间 */
    mstime_t failover_auth_time; /* Time of previous or next election. */
    /* 节点获得支持的票数，从节点 */
    int failover_auth_count;    /* Number of votes received so far. */
    /* 如果为 True，表示该节点已经向其他节点发送了投票请求 */
    int failover_auth_sent;     /* True if we already asked for votes. */
    /* 该从节点在当前请求中的排名，该值根据复制偏移量计算而来，最终用于确定 slave 节点发起投票的时间
     * 注：排名就是当前从节点对应的主节点下所有从节点复制偏移量大于当前节点复制偏移量的数量，
     * 也就是说复制偏移量越大，排名越前，而排名会用作 failover_auth_time 的计算，排名越后，
     * failover_auth_time 也就越大，发起选举的时间越晚，
     * 即 rank 值越小的节点通常有更大的复制偏移量，它能越早发起选举竞争主节点 */
    int failover_auth_rank;     /* This slave rank for current auth request. */
    /* 当前选举的纪元 */
    uint64_t failover_auth_epoch; /* Epoch of the current election. */
    /* 从节点不能执行故障转移的原因 */
    int cant_failover_reason;   /* Why a slave is currently not able to
                                   failover. See the CANT_FAILOVER_* macros. */
    /* Manual failover state in common. */
    /* 为 0 表示没有正在进行手动故障转移，否则表示手动故障转移的时间限制
     * 代码逻辑里会使用该属性来判断是手动故障转移，还是自动故障转移 */
    mstime_t mf_end;            /* Manual failover time limit (ms unixtime).
                                   It is zero if there is no MF in progress. */
    /* Manual failover state of master. */
    /* 执行手动故障转移的从节点 */
    clusterNode *mf_slave;      /* Slave performing the manual failover. */
    /* Manual failover state of slave. */
    /* 从节点记录手动故障转移时的主节点偏移量 */
    long long mf_master_offset; /* Master offset the slave needs to start MF
                                   or -1 if still not received. */
    /* 非 0 表示可以手动故障转移 */
    int mf_can_start;           /* If non-zero signal that the manual failover
                                   can start requesting masters vote. */
    /* The following fields are used by masters to take state on elections. */
    /* 集群最近一次投票的纪元 */
    uint64_t lastVoteEpoch;     /* Epoch of the last vote granted. */
    /* 调用 clusterBeforeSleep() 所做的一些事 */
    int todo_before_sleep; /* Things to do in clusterBeforeSleep(). */
    /* Stats */
    /* Messages received and sent by type. */
    /* 发送的字节数 */
    long long stats_bus_messages_sent[CLUSTERMSG_TYPE_COUNT];
    /* 通过 cluster 接收到的消息数量 */
    long long stats_bus_messages_received[CLUSTERMSG_TYPE_COUNT];
    long long stats_pfail_nodes;    /* Number of nodes in PFAIL status,
                                       excluding nodes without address. */
    unsigned long long stat_cluster_links_buffer_limit_exceeded;  /* Total number of cluster links freed due to exceeding buffer limit */
} clusterState;

/* Redis cluster messages header */

/* Initially we don't know our "name", but we'll find it once we connect
 * to the first node, using the getsockname() function. Then we'll use this
 * address for all the next messages. */
/* ping 包：redis 集群中每个节点通过心跳包可以知道其他节点的当前状态并保存到本节点状态中
 * pong 包：pong 包除了在接收 ping 包和 meet 包之后会作为回复发送之外，当进行主从切换之后，新的主节点会向集群中所有节点直接发送一个 pong 包，通知
 * 主从切换后节点角色的转换
 * meet 包：当执行 cluster meet ip port [cluster_bus_port] 命令之后，执行端会向 ip:port 指定的地址发送 meet 包，连接建立之后，会定期发送 ping 包 */
typedef struct {
    /* 节点名称 */
    char nodename[CLUSTER_NAMELEN];
    /* 发送 ping 的时间 */
    uint32_t ping_sent;
    /* 接收 pong 的时间 */
    uint32_t pong_received;
    /* 节点 IP 地址 */
    char ip[NET_IP_STR_LEN];  /* IP address last time it was seen */
    /* 节点的端口 */
    uint16_t port;              /* base port last time it was seen */
    /* 节点监听集群通信端口 */
    uint16_t cport;             /* cluster port last time it was seen */
    /* 节点状态 flags */
    uint16_t flags;             /* node->flags copy */
    /* 如果是 TLS 协议，该属性标识实际通信端口 */
    uint16_t pport;             /* plaintext-port, when base port is TLS */
    /* 预留 */
    uint16_t notused1;
} clusterMsgDataGossip;

/* fail 包用来通知集群某个节点处于故障状态，当一个节点被大多数节点标记为 pfail 状态时，会进入 fail 状态
 * 当一个主节点进入 fail 状态后，该主节点下的从节点会要求进行切换 */
typedef struct {
    char nodename[CLUSTER_NAMELEN];
} clusterMsgDataFail;

/* publish 包用来广播 publish 信息 */
typedef struct {
    /* 通道长度 */
    uint32_t channel_len;
    /* 通道消息长度 */
    uint32_t message_len;
    /* 通道和消息的内容 */
    unsigned char bulk_data[8]; /* 8 bytes just as placeholder. */
} clusterMsgDataPublish;

/* 更新节点的配置信息 */
typedef struct {
    /* 配置纪元 */
    uint64_t configEpoch; /* Config epoch of the specified instance. */
    /* 节点名称 */
    char nodename[CLUSTER_NAMELEN]; /* Name of the slots owner. */
    /* 节点所服务的槽位 */
    unsigned char slots[CLUSTER_SLOTS/8]; /* Slots bitmap. */
} clusterMsgDataUpdate;

typedef struct {
    uint64_t module_id;     /* ID of the sender module. */
    uint32_t len;           /* ID of the sender module. */
    uint8_t type;           /* Type from 0 to 255. */
    unsigned char bulk_data[3]; /* 3 bytes just as placeholder. */
} clusterMsgModule;

/* The cluster supports optional extension messages that can be sent
 * along with ping/pong/meet messages to give additional info in a 
 * consistent manner. */
typedef enum {
    CLUSTERMSG_EXT_TYPE_HOSTNAME,
} clusterMsgPingtypes;

/* Helper function for making sure extensions are eight byte aligned. */
#define EIGHT_BYTE_ALIGN(size) ((((size) + 7) / 8) * 8)

typedef struct {
    char hostname[1]; /* The announced hostname, ends with \0. */
} clusterMsgPingExtHostname;

typedef struct {
    uint32_t length; /* Total length of this extension message (including this header) */
    uint16_t type; /* Type of this extension message (see clusterMsgPingExtTypes) */
    uint16_t unused; /* 16 bits of padding to make this structure 8 byte aligned. */
    union {
        clusterMsgPingExtHostname hostname;
    } ext[]; /* Actual extension information, formatted so that the data is 8 
              * byte aligned, regardless of its content. */
} clusterMsgPingExt;

/* 包体内容 */
union clusterMsgData {
    /* PING, MEET and PONG */
    struct {
        /* Array of N clusterMsgDataGossip structures */
        clusterMsgDataGossip gossip[1];
        /* Extension data that can optionally be sent for ping/meet/pong
         * messages. We can't explicitly define them here though, since
         * the gossip array isn't the real length of the gossip data. */
    } ping;

    /* FAIL */
    struct {
        clusterMsgDataFail about;
    } fail;

    /* PUBLISH */
    struct {
        clusterMsgDataPublish msg;
    } publish;

    /* UPDATE */
    struct {
        clusterMsgDataUpdate nodecfg;
    } update;

    /* MODULE */
    struct {
        clusterMsgModule msg;
    } module;
};

#define CLUSTER_PROTO_VER 1 /* Cluster bus protocol version. */

/* 用于描述集群节点间互相通信的消息的结构，包头 */
typedef struct {
    /* 固定 RCmb 标识 */
    char sig[4];        /* Signature "RCmb" (Redis Cluster message bus). */
    /* 消息的总长度 */
    uint32_t totlen;    /* Total length of this message */
    /* 协议版本，当前设置为 1 */
    uint16_t ver;       /* Protocol version, currently set to 1. */
    /* 发送方监听的端口 */
    uint16_t port;      /* TCP base port number. */
    /* 包类型，（接收到包后通过该属性来决定如何解析包体） */
    uint16_t type;      /* Message type */
    /* data 中的 gossip section 个数（供 ping pong meet 包使用） */
    uint16_t count;     /* Only used for some kind of messages. */
    /* 发送方节点记录的集群当前纪元 */
    uint64_t currentEpoch;  /* The epoch accordingly to the sending node. */
    /* 发送方节点对应的配置纪元（如果为从节点，为该从节点对应的主节点） */
    uint64_t configEpoch;   /* The config epoch if it's a master, or the last
                               epoch advertised by its master if it is a
                               slave. */
    /* 如果为主节点，该值标识复制偏移量，如果为从，该值表示从已处理的偏移量 */
    uint64_t offset;    /* Master replication offset if node is a master or
                           processed replication offset if node is a slave. */
    /* 发送方名称 */
    char sender[CLUSTER_NAMELEN]; /* Name of the sender node */
    /* 发送方提供服务的 slot 映射表，（如果为从，则为该从所对应的主提供服务的 slot 映射表） */
    unsigned char myslots[CLUSTER_SLOTS/8];
    /* 发送方如果为从，则该字段为对应的主的名称 */
    char slaveof[CLUSTER_NAMELEN];
    /* 发送方 IP */
    char myip[NET_IP_STR_LEN];    /* Sender IP, if not all zeroed. */
    /* 和该包一起发送的扩展数 */
    uint16_t extensions; /* Number of extensions sent along with this packet. */
    /* 预留属性 */
    char notused1[30];   /* 30 bytes reserved for future usage. */
    /* 发送方实际发送数据的端口 */
    uint16_t pport;      /* Sender TCP plaintext port, if base port is TLS */
    /* 发送方监听的 cluster bus 端口 */
    uint16_t cport;      /* Sender TCP cluster bus port */
    /* 发送方节点所记录的 flags */
    uint16_t flags;      /* Sender node flags */
    /* 发送方节点所记录的集群状态 */
    unsigned char state; /* Cluster state from the POV of the sender */
    /* 目前只有 mflags[0] 会在手动 failover 时使用 */
    unsigned char mflags[3]; /* Message flags: CLUSTERMSG_FLAG[012]_... */
    /* 包体内容 */
    union clusterMsgData data;
} clusterMsg;

/* clusterMsg defines the gossip wire protocol exchanged among Redis cluster
 * members, which can be running different versions of redis-server bits,
 * especially during cluster rolling upgrades.
 *
 * Therefore, fields in this struct should remain at the same offset from
 * release to release. The static asserts below ensures that incompatible
 * changes in clusterMsg be caught at compile time.
 */

static_assert(offsetof(clusterMsg, sig) == 0, "unexpected field offset");
static_assert(offsetof(clusterMsg, totlen) == 4, "unexpected field offset");
static_assert(offsetof(clusterMsg, ver) == 8, "unexpected field offset");
static_assert(offsetof(clusterMsg, port) == 10, "unexpected field offset");
static_assert(offsetof(clusterMsg, type) == 12, "unexpected field offset");
static_assert(offsetof(clusterMsg, count) == 14, "unexpected field offset");
static_assert(offsetof(clusterMsg, currentEpoch) == 16, "unexpected field offset");
static_assert(offsetof(clusterMsg, configEpoch) == 24, "unexpected field offset");
static_assert(offsetof(clusterMsg, offset) == 32, "unexpected field offset");
static_assert(offsetof(clusterMsg, sender) == 40, "unexpected field offset");
static_assert(offsetof(clusterMsg, myslots) == 80, "unexpected field offset");
static_assert(offsetof(clusterMsg, slaveof) == 2128, "unexpected field offset");
static_assert(offsetof(clusterMsg, myip) == 2168, "unexpected field offset");
static_assert(offsetof(clusterMsg, extensions) == 2214, "unexpected field offset");
static_assert(offsetof(clusterMsg, notused1) == 2216, "unexpected field offset");
static_assert(offsetof(clusterMsg, pport) == 2246, "unexpected field offset");
static_assert(offsetof(clusterMsg, cport) == 2248, "unexpected field offset");
static_assert(offsetof(clusterMsg, flags) == 2250, "unexpected field offset");
static_assert(offsetof(clusterMsg, state) == 2252, "unexpected field offset");
static_assert(offsetof(clusterMsg, mflags) == 2253, "unexpected field offset");
static_assert(offsetof(clusterMsg, data) == 2256, "unexpected field offset");

#define CLUSTERMSG_MIN_LEN (sizeof(clusterMsg)-sizeof(union clusterMsgData))

/* Message flags better specify the packet content or are used to
 * provide some information about the node state. */
/* 主节点暂停手动故障转移 */
#define CLUSTERMSG_FLAG0_PAUSED (1<<0) /* Master paused for manual failover. */
/* 即使主节点在线，也要认证故障转移 */
#define CLUSTERMSG_FLAG0_FORCEACK (1<<1) /* Give ACK to AUTH_REQUEST even if
                                            master is up. */
/* 消息包含扩展数据 */
#define CLUSTERMSG_FLAG0_EXT_DATA (1<<2) /* Message contains extension data */

/* ---------------------- API exported outside cluster.c -------------------- */
void clusterInit(void);
void clusterCron(void);
void clusterBeforeSleep(void);
clusterNode *getNodeByQuery(client *c, struct redisCommand *cmd, robj **argv, int argc, int *hashslot, int *ask);
int verifyClusterNodeId(const char *name, int length);
clusterNode *clusterLookupNode(const char *name, int length);
int clusterRedirectBlockedClientIfNeeded(client *c);
void clusterRedirectClient(client *c, clusterNode *n, int hashslot, int error_code);
void migrateCloseTimedoutSockets(void);
int verifyClusterConfigWithData(void);
unsigned long getClusterConnectionsCount(void);
int clusterSendModuleMessageToTarget(const char *target, uint64_t module_id, uint8_t type, const char *payload, uint32_t len);
void clusterPropagatePublish(robj *channel, robj *message, int sharded);
unsigned int keyHashSlot(char *key, int keylen);
void slotToKeyAddEntry(dictEntry *entry, redisDb *db);
void slotToKeyDelEntry(dictEntry *entry, redisDb *db);
void slotToKeyReplaceEntry(dictEntry *entry, redisDb *db);
void slotToKeyInit(redisDb *db);
void slotToKeyFlush(redisDb *db);
void slotToKeyDestroy(redisDb *db);
void clusterUpdateMyselfFlags(void);
void clusterUpdateMyselfIp(void);
void slotToChannelAdd(sds channel);
void slotToChannelDel(sds channel);
void clusterUpdateMyselfHostname(void);

#endif /* __CLUSTER_H */
