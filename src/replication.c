/* Asynchronous replication implementation.
 *
 * Copyright (c) 2009-2012, Salvatore Sanfilippo <antirez at gmail dot com>
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 *   * Redistributions of source code must retain the above copyright notice,
 *     this list of conditions and the following disclaimer.
 *   * Redistributions in binary form must reproduce the above copyright
 *     notice, this list of conditions and the following disclaimer in the
 *     documentation and/or other materials provided with the distribution.
 *   * Neither the name of Redis nor the names of its contributors may be used
 *     to endorse or promote products derived from this software without
 *     specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 */


#include "server.h"
#include "cluster.h"
#include "bio.h"
#include "functions.h"

#include <memory.h>
#include <sys/time.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/socket.h>
#include <sys/stat.h>

void replicationDiscardCachedMaster(void);
void replicationResurrectCachedMaster(connection *conn);
void replicationSendAck(void);
void replicaPutOnline(client *slave);
void replicaStartCommandStream(client *slave);
int cancelReplicationHandshake(int reconnect);

/* We take a global flag to remember if this instance generated an RDB
 * because of replication, so that we can remove the RDB file in case
 * the instance is configured to have no persistence. */
/* 使用全局标志来记住此实例是否因为复制而生成 RDB ，以便在实例被配置为没有持久性的情况下删除 RDB 文件 */
int RDBGeneratedByReplication = 0;

/* 从节点建立主从复制的调用链： 
 * 1. 从节点(slave)侧会调用 replicationSetMaster() 来建立与主节点的连接。
 * 2. replicationSetMaster() 完成一些准备工作之后会调用 connectWithMaster() 负责建立连接
 * 3. connectWithMaster() 发起与主节点的连接，并在连接建立后回调 syncWithMaster()
 * 4. syncWithMaster() 会负责建立主从同步的主要工作，在此过程中 syncWithMaster() 会作为 event loop 的 handler 被多次回调
 * 5. 主从握手完成后 syncWithMaster() 会调用 slaveTryPartialResynchronization() 尝试进行部分同步，后者会负责向主节点发送 PSYNC 命令，并处理主节点响应。slaveTryPartialResynchronization() 同样会被多次回调，并负责设置同步过程中的 replid 和 offset 等参数。
 * 6. 一旦 PSYNC 相关参数设置完成，syncWithMaster() 会调用 readSyncBulkPayload() 阻塞式地接受并加载主节点传来的 RDB 文件。
 * 7. RDB 文件加载完成后，从节点就可以从 redisServer.master 结构中继续接收来自主节点的后续数据
 */

/* 主从复制，主节点方主要提供两个函数
 * syncCommand: 该函数用来处理从节点发送来的 SYNC 和 PSYNC 命令，用于主从同步
 * replconfCommand: 该函数用来修改主节点方持有的从节点配置信息
 * 上面两个函数对应的命令都是 redis 内部的命令。
 * 这两个函数可以看作是主节点的入口函数
 */

/* --------------------------- Utility functions ---------------------------- */

/* Return the pointer to a string representing the slave ip:listening_port
 * pair. Mostly useful for logging, since we want to log a slave using its
 * IP address and its listening port which is more clear for the user, for
 * example: "Closing connection with replica 10.1.2.3:6380". */

/* 返回一个从节点的 ip:port 信息，主要功能用于打印日志
 * 其中记录的是从节点的 ip 以及从节点监听的端口，该端口是通过 REPLCONF 命令获取
 * 例如 "Closing connection with replica 10.1.2.3:6380"
 */
char *replicationGetSlaveName(client *c) {
    static char buf[NET_HOST_PORT_STR_LEN];
    char ip[NET_IP_STR_LEN];

    ip[0] = '\0';
    buf[0] = '\0';
    if (c->slave_addr ||
        connPeerToString(c->conn,ip,sizeof(ip),NULL) != -1)
    {
        char *addr = c->slave_addr ? c->slave_addr : ip;
        if (c->slave_listening_port)
            anetFormatAddr(buf,sizeof(buf),addr,c->slave_listening_port);
        else
            snprintf(buf,sizeof(buf),"%s:<unknown-replica-port>",addr);
    } else {
        snprintf(buf,sizeof(buf),"client id #%llu",
            (unsigned long long) c->id);
    }
    return buf;
}

/* Plain unlink() can block for quite some time in order to actually apply
 * the file deletion to the filesystem. This call removes the file in a
 * background thread instead. We actually just do close() in the thread,
 * by using the fact that if there is another instance of the same file open,
 * the foreground unlink() will only remove the fs name, and deleting the
 * file's storage space will only happen once the last reference is lost. */
int bg_unlink(const char *filename) {
    int fd = open(filename,O_RDONLY|O_NONBLOCK);
    if (fd == -1) {
        /* Can't open the file? Fall back to unlinking in the main thread. */
        return unlink(filename);
    } else {
        /* The following unlink() removes the name but doesn't free the
         * file contents because a process still has it open. */
        int retval = unlink(filename);
        if (retval == -1) {
            /* If we got an unlink error, we just return it, closing the
             * new reference we have to the file. */
            int old_errno = errno;
            close(fd);  /* This would overwrite our errno. So we saved it. */
            errno = old_errno;
            return -1;
        }
        bioCreateCloseJob(fd, 0);
        return 0; /* Success. */
    }
}

/* ---------------------------------- MASTER -------------------------------- */

/* 创建复制积压缓冲区 */
void createReplicationBacklog(void) {
    serverAssert(server.repl_backlog == NULL);
    server.repl_backlog = zmalloc(sizeof(replBacklog));
    server.repl_backlog->ref_repl_buf_node = NULL;
    server.repl_backlog->unindexed_count = 0;
    server.repl_backlog->blocks_index = raxNew();
    server.repl_backlog->histlen = 0;
    /* We don't have any data inside our buffer, but virtually the first
     * byte we have is the next byte that will be generated for the
     * replication stream. */
    server.repl_backlog->offset = server.master_repl_offset+1;
}

/* This function is called when the user modifies the replication backlog
 * size at runtime. It is up to the function to resize the buffer and setup it
 * so that it contains the same data as the previous one (possibly less data,
 * but the most recent bytes, or the same data and more free space in case the
 * buffer is enlarged). */
void resizeReplicationBacklog(void) {
    if (server.repl_backlog_size < CONFIG_REPL_BACKLOG_MIN_SIZE)
        server.repl_backlog_size = CONFIG_REPL_BACKLOG_MIN_SIZE;
    if (server.repl_backlog)
        incrementalTrimReplicationBacklog(REPL_BACKLOG_TRIM_BLOCKS_PER_CALL);
}

/* 释放复制积压缓冲区空间 */
void freeReplicationBacklog(void) {
    serverAssert(listLength(server.slaves) == 0);
    if (server.repl_backlog == NULL) return;

    /* Decrease the start buffer node reference count. */
    /* 将缓冲区块头结点的引用计数置 0 */
    if (server.repl_backlog->ref_repl_buf_node) {
        replBufBlock *o = listNodeValue(
            server.repl_backlog->ref_repl_buf_node);
        serverAssert(o->refcount == 1); /* Last reference. */
        o->refcount--;
    }

    /* Replication buffer blocks are completely released when we free the
     * backlog, since the backlog is released only when there are no replicas
     * and the backlog keeps the last reference of all blocks. */
    /* 异步释放缓冲区块链表和缓冲区块索引 */
    freeReplicationBacklogRefMemAsync(server.repl_buffer_blocks,
                            server.repl_backlog->blocks_index);
    /* 重置复制积压缓冲区 */
    resetReplicationBuffer();
    zfree(server.repl_backlog);
    server.repl_backlog = NULL;
}

/* To make search offset from replication buffer blocks quickly
 * when replicas ask partial resynchronization, we create one index
 * block every REPL_BACKLOG_INDEX_PER_BLOCKS blocks. */
/* 给新加入的复制积压缓冲区块节点添加索引 */
void createReplicationBacklogIndex(listNode *ln) {
    /* 未添加索引的节点计数器 +1 */
    server.repl_backlog->unindexed_count++;
    /* 这里是创建的稀疏索引，每 64 个节点建立一个索引 */
    if (server.repl_backlog->unindexed_count >= REPL_BACKLOG_INDEX_PER_BLOCKS) {
        replBufBlock *o = listNodeValue(ln);
        /* 获取当前节点的复制偏移量 */
        uint64_t encoded_offset = htonu64(o->repl_offset);
        /* 添加一个索引 */
        raxInsert(server.repl_backlog->blocks_index,
                  (unsigned char*)&encoded_offset, sizeof(uint64_t),
                  ln, NULL);
        server.repl_backlog->unindexed_count = 0;
    }
}

/* Rebase replication buffer blocks' offset since the initial
 * setting offset starts from 0 when master restart. */
void rebaseReplicationBuffer(long long base_repl_offset) {
    raxFree(server.repl_backlog->blocks_index);
    server.repl_backlog->blocks_index = raxNew();
    server.repl_backlog->unindexed_count = 0;

    listIter li;
    listNode *ln;
    listRewind(server.repl_buffer_blocks, &li);
    while ((ln = listNext(&li))) {
        replBufBlock *o = listNodeValue(ln);
        o->repl_offset += base_repl_offset;
        createReplicationBacklogIndex(ln);
    }
}

/* 重置复制积压缓冲区 */
void resetReplicationBuffer(void) {
    server.repl_buffer_mem = 0;
    server.repl_buffer_blocks = listCreate();
    listSetFreeMethod(server.repl_buffer_blocks, (void (*)(void*))zfree);
}

int canFeedReplicaReplBuffer(client *replica) {
    /* Don't feed replicas that only want the RDB. */
    /* 如果副本客户端仅仅同步 RDB，表示不能发送复制积压缓冲区中的数据给它 */
    if (replica->flags & CLIENT_REPL_RDBONLY) return 0;

    /* Don't feed replicas that are still waiting for BGSAVE to start. */
    /* 如果副本还在做 RDB 同步，不能发送复制积压缓冲区中的数据给它 */
    if (replica->replstate == SLAVE_STATE_WAIT_BGSAVE_START) return 0;

    return 1;
}

/* Similar with 'prepareClientToWrite', note that we must call this function
 * before feeding replication stream into global replication buffer, since
 * clientHasPendingReplies in prepareClientToWrite will access the global
 * replication buffer to make judgements. */
/* 将副本客户端加入待写处理队列中 */
int prepareReplicasToWrite(void) {
    listIter li;
    listNode *ln;
    int prepared = 0;

    /* 遍历当前主节点下的所有从节点客户端 */
    listRewind(server.slaves,&li);
    while((ln = listNext(&li))) {
        client *slave = ln->value;
        /* 如果该从节点客户端不能接收复制积压缓冲区数据，跳过 */
        if (!canFeedReplicaReplBuffer(slave)) continue;
        /* 这里其实就是将客户端放入待写处理客户端队列中 */
        if (prepareClientToWrite(slave) == C_ERR) continue;
        prepared++;
    }

    return prepared;
}

/* Wrapper for feedReplicationBuffer() that takes Redis string objects
 * as input. */
/* 向复制积压缓冲区中添加一个对象 */
void feedReplicationBufferWithObject(robj *o) {
    char llstr[LONG_STR_SIZE];
    void *p;
    size_t len;
    /* 根据不同的编码类型获取对象的实际值 */
    if (o->encoding == OBJ_ENCODING_INT) {
        len = ll2string(llstr,sizeof(llstr),(long)o->ptr);
        p = llstr;
    } else {
        len = sdslen(o->ptr);
        p = o->ptr;
    }
    /* 将实际值，以及值的长度放入复制积压缓冲区中 */
    feedReplicationBuffer(p,len);
}

/* Generally, we only have one replication buffer block to trim when replication
 * backlog size exceeds our setting and no replica reference it. But if replica
 * clients disconnect, we need to free many replication buffer blocks that are
 * referenced. It would cost much time if there are a lots blocks to free, that
 * will freeze server, so we trim replication backlog incrementally. */
/* 从复制积压缓冲区链表头部删除一些数据缓冲区块节点 */
void incrementalTrimReplicationBacklog(size_t max_blocks) {
    serverAssert(server.repl_backlog != NULL);

    size_t trimmed_blocks = 0;
    /* 如果目前复制积压缓冲区要放入的数据超过了复制积压缓冲区的限制 */
    while (server.repl_backlog->histlen > server.repl_backlog_size &&
           trimmed_blocks < max_blocks)
    {
        /* We never trim backlog to less than one block. */
        if (listLength(server.repl_buffer_blocks) <= 1) break;

        /* Replicas increment the refcount of the first replication buffer block
         * they refer to, in that case, we don't trim the backlog even if
         * backlog_histlen exceeds backlog_size. This implicitly makes backlog
         * bigger than our setting, but makes the master accept partial resync as
         * much as possible. So that backlog must be the last reference of
         * replication buffer blocks. */
        /* 获取第一个缓冲区块 */
        listNode *first = listFirst(server.repl_buffer_blocks);
        serverAssert(first == server.repl_backlog->ref_repl_buf_node);
        replBufBlock *fo = listNodeValue(first);
        /* 如果引用计数不为 1，退出循环，不能释放当前换乘区块空间 */
        if (fo->refcount != 1) break;

        /* We don't try trim backlog if backlog valid size will be lessen than
         * setting backlog size once we release the first repl buffer block. */
        /* 如果释放第一个缓冲区块节点后，空间没有超过 repl_backlog_size，退出循环，没必要释放该节点 */
        if (server.repl_backlog->histlen - (long long)fo->size <=
            server.repl_backlog_size) break;

        /* Decr refcount and release the first block later. */
        /* 这里引用计数 --，上面已经确定 refcount 为 1，这里-1，就是 0 了，可以释放了 */
        fo->refcount--;
        trimmed_blocks++;
        /* 调整复制积压缓冲区中数据的大小 */
        server.repl_backlog->histlen -= fo->size;

        /* Go to use next replication buffer block node. */
        /* 将第二个节点设置为头结点 */
        listNode *next = listNextNode(first);
        server.repl_backlog->ref_repl_buf_node = next;
        serverAssert(server.repl_backlog->ref_repl_buf_node != NULL);
        /* Incr reference count to keep the new head node. */
        ((replBufBlock *)listNodeValue(next))->refcount++;

        /* Remove the node in recorded blocks. */
        /* 删除该节点的索引 */
        uint64_t encoded_offset = htonu64(fo->repl_offset);
        raxRemove(server.repl_backlog->blocks_index,
            (unsigned char*)&encoded_offset, sizeof(uint64_t), NULL);

        /* Delete the first node from global replication buffer. */
        serverAssert(fo->refcount == 0 && fo->used == fo->size);
        /* 内存使用量调整 */
        server.repl_buffer_mem -= (fo->size +
            sizeof(listNode) + sizeof(replBufBlock));
        /* 删除节点 */
        listDelNode(server.repl_buffer_blocks, first);
    }

    /* Set the offset of the first byte we have in the backlog. */
    server.repl_backlog->offset = server.master_repl_offset -
                              server.repl_backlog->histlen + 1;
}

/* Free replication buffer blocks that are referenced by this client. */
void freeReplicaReferencedReplBuffer(client *replica) {
    if (replica->ref_repl_buf_node != NULL) {
        /* Decrease the start buffer node reference count. */
        replBufBlock *o = listNodeValue(replica->ref_repl_buf_node);
        serverAssert(o->refcount > 0);
        o->refcount--;
        incrementalTrimReplicationBacklog(REPL_BACKLOG_TRIM_BLOCKS_PER_CALL);
    }
    replica->ref_repl_buf_node = NULL;
    replica->ref_block_pos = 0;
}

/* Append bytes into the global replication buffer list, replication backlog and
 * all replica clients use replication buffers collectively, this function replace
 * 'addReply*', 'feedReplicationBacklog' for replicas and replication backlog,
 * First we add buffer into global replication buffer block list, and then
 * update replica / replication-backlog referenced node and block position. */
/* 将数据放入复制积压缓冲区块 */
void feedReplicationBuffer(char *s, size_t len) {
    static long long repl_block_id = 0;

    /* 复制积压缓冲区不存在，直接返回 */
    if (server.repl_backlog == NULL) return;
    /* 复制偏移量增加 */
    server.master_repl_offset += len;
    /* 存储的数据大小增加 */
    server.repl_backlog->histlen += len;

    size_t start_pos = 0; /* The position of referenced block to start sending. */
    listNode *start_node = NULL; /* Replica/backlog starts referenced node. */
    int add_new_block = 0; /* Create new block if current block is total used. */
    listNode *ln = listLast(server.repl_buffer_blocks);
    replBufBlock *tail = ln ? listNodeValue(ln) : NULL;

    /* Append to tail string when possible. */
    /* 尾结点的缓冲区块还没有被使用完 */
    if (tail && tail->size > tail->used) {
        /* 开始节点指向尾节点 */
        start_node = listLast(server.repl_buffer_blocks);
        /* 开始位置指向尾缓冲区块空闲位置 */
        start_pos = tail->used;
        /* Copy the part we can fit into the tail, and leave the rest for a
         * new node */
        /* 先尽量将命令放入尾节点缓冲区块，如果放不下会在后面进行处理 */
        size_t avail = tail->size - tail->used;
        size_t copy = (avail >= len) ? len : avail;
        memcpy(tail->buf + tail->used, s, copy);
        tail->used += copy;
        s += copy;
        len -= copy;
    }
    /* len > 0 表示尾结点存不下 */
    if (len) {
        /* Create a new node, make sure it is allocated to at
         * least PROTO_REPLY_CHUNK_BYTES */
        size_t usable_size;
        /* 根据剩余的 len 来创建一个缓冲区块，该缓冲区块至少会分配 PROTO_REPLY_CHUNK_BYTES 空间 */
        size_t size = (len < PROTO_REPLY_CHUNK_BYTES) ? PROTO_REPLY_CHUNK_BYTES : len;
        tail = zmalloc_usable(size + sizeof(replBufBlock), &usable_size);
        /* Take over the allocation's internal fragmentation */
        /* 设置给数据分配的空间大小 */
        tail->size = usable_size - sizeof(replBufBlock);
        /* 设置新建的缓冲区块本次需要使用到的大小 */
        tail->used = len;
        tail->refcount = 0;
        tail->repl_offset = server.master_repl_offset - tail->used + 1;
        tail->id = repl_block_id++;
        /* 将剩余的数据放入新建的缓冲区块中 */
        memcpy(tail->buf, s, len);
        /* 放入链表尾部 */
        listAddNodeTail(server.repl_buffer_blocks, tail);
        /* We also count the list node memory into replication buffer memory. */
        /* 复制积压缓冲区总共使用的内存需要加上链表节点的空间 */
        server.repl_buffer_mem += (usable_size + sizeof(listNode));
        add_new_block = 1;
        /* 如果之前尾结点数据放满了，指定尾结点为当前新建节点，并且开始的偏移量为 0 */
        if (start_node == NULL) {
            start_node = listLast(server.repl_buffer_blocks);
            start_pos = 0;
        }
    }

    /* For output buffer of replicas. */
    listIter li;
    /* 遍历所有从节点 */
    listRewind(server.slaves,&li);
    while((ln = listNext(&li))) {
        client *slave = ln->value;
        if (!canFeedReplicaReplBuffer(slave)) continue;
        /* 如果从节点可以接收复制积压缓冲区的数据 */

        /* Update shared replication buffer start position. */
        /* 更新从节点的复制位置 */
        if (slave->ref_repl_buf_node == NULL) {
            slave->ref_repl_buf_node = start_node;
            slave->ref_block_pos = start_pos;
            /* Only increase the start block reference count. */
            ((replBufBlock *)listNodeValue(start_node))->refcount++;
        }

        /* Check output buffer limit only when add new block. */
        /* 如果新建了复制积压缓冲区，需要判断客户端的输出缓冲区是否到了上限，如果到上限了需要关闭客户端 */
        if (add_new_block) closeClientOnOutputBufferLimitReached(slave, 1);
    }

    /* For replication backlog */
    /* 设置当前正在处理的复制积压缓冲区块 */
    if (server.repl_backlog->ref_repl_buf_node == NULL) {
        server.repl_backlog->ref_repl_buf_node = start_node;
        /* Only increase the start block reference count. */
        ((replBufBlock *)listNodeValue(start_node))->refcount++;

        /* Replication buffer must be empty before adding replication stream
         * into replication backlog. */
        serverAssert(add_new_block == 1 && start_pos == 0);
    }
    /* 如果新建了复制积压缓冲区块，为新节点添加索引，这里是每 64 个节点添加一个索引 */
    if (add_new_block) {
        createReplicationBacklogIndex(listLast(server.repl_buffer_blocks));
    }
    /* Try to trim replication backlog since replication backlog may exceed
     * our setting when we add replication stream. Note that it is important to
     * try to trim at least one node since in the common case this is where one
     * new backlog node is added and one should be removed. See also comments
     * in freeMemoryGetNotCountedMemory for details. */
    /* 增量的释放复制积压缓冲区中的复制积压缓冲区块 */
    incrementalTrimReplicationBacklog(REPL_BACKLOG_TRIM_BLOCKS_PER_CALL);
}

/* Propagate write commands to replication stream.
 *
 * This function is used if the instance is a master: we use the commands
 * received by our clients in order to create the replication stream.
 * Instead if the instance is a replica and has sub-replicas attached, we use
 * replicationFeedStreamFromMasterStream() */
/* replicationCron 会每隔 1s 调用一次该函数（或者当有故障转移时是 0.1s），发送复制积压缓冲区中的数据给副本节点
 * dictid 表示当前主节点所处于的数据库编号 */
void replicationFeedSlaves(list *slaves, int dictid, robj **argv, int argc) {
    int j, len;
    char llstr[LONG_STR_SIZE];

    /* In case we propagate a command that doesn't touch keys (PING, REPLCONF) we
     * pass dbid=server.slaveseldb which may be -1. */
    serverAssert(dictid == -1 || (dictid >= 0 && dictid < server.dbnum));

    /* If the instance is not a top level master, return ASAP: we'll just proxy
     * the stream of data we receive from our master instead, in order to
     * propagate *identical* replication stream. In this way this slave can
     * advertise the same replication ID as the master (since it shares the
     * master replication history and has the same backlog and offsets). */
    /* 如果当前节点不是主节点，直接返回 */
    if (server.masterhost != NULL) return;

    /* If there aren't slaves, and there is no backlog buffer to populate,
     * we can return ASAP. */
    if (server.repl_backlog == NULL && listLength(slaves) == 0) return;

    /* We can't have slaves attached and no backlog. */
    serverAssert(!(listLength(slaves) != 0 && server.repl_backlog == NULL));

    /* Must install write handler for all replicas first before feeding
     * replication stream. */
    /* 将符合同步复制积压缓冲区数据的副本 TCP 客户端加入待写客户端队列 */
    prepareReplicasToWrite();

    /* Send SELECT command to every slave if needed. */
    /* 副本选择的数据库和主节点选择的数据库不同的情况，需要发送 select 命令 */
    if (server.slaveseldb != dictid) {
        robj *selectcmd;

        /* For a few DBs we have pre-computed SELECT command. */
        /* redis 已经定义好了前 10 个数据库的 select 命令的共享字符串，如果当前 dictid < 10
         * 可以直接用共享字符串，不需要再拼接命令 */
        if (dictid >= 0 && dictid < PROTO_SHARED_SELECT_CMDS) {
            selectcmd = shared.select[dictid];
        } else {
            int dictid_len;

            /* 拼接 select 命令，这里可以看出命令发送的格式
             * 第一个字符 * ： 表示 multi bulk 类型的命令，一次执行多条命令
             * 2 ： 表示当前命令有 2 个参数（注意命令名也认为是一个参数）
             * $6 : 表示接下来的一个参数的字节数
             * SELECT : 参数
             */
            dictid_len = ll2string(llstr,sizeof(llstr),dictid);
            selectcmd = createObject(OBJ_STRING,
                sdscatprintf(sdsempty(),
                "*2\r\n$6\r\nSELECT\r\n$%d\r\n%s\r\n",
                dictid_len, llstr));
        }

        /* 将 select 命令字符串加入复制积压缓冲区中 */
        feedReplicationBufferWithObject(selectcmd);

        if (dictid < 0 || dictid >= PROTO_SHARED_SELECT_CMDS)
            decrRefCount(selectcmd);

        server.slaveseldb = dictid;
    }

    /* Write the command to the replication buffer if any. */
    char aux[LONG_STR_SIZE+3];

    /* Add the multi bulk reply length. */
    /* 第一个字符是 * */
    aux[0] = '*';
    /* 参数的数量 */
    len = ll2string(aux+1,sizeof(aux)-1,argc);
    aux[len+1] = '\r';
    aux[len+2] = '\n';
    /* 将上面的 aux 的信息放入复制积压缓冲区块 */
    feedReplicationBuffer(aux,len+3);

    /* 遍历所有参数 */
    for (j = 0; j < argc; j++) {
        long objlen = stringObjectLen(argv[j]);

        /* We need to feed the buffer with the object as a bulk reply
         * not just as a plain string, so create the $..CRLF payload len
         * and add the final CRLF */
        /* 生成参数长度字符串 */
        aux[0] = '$';
        len = ll2string(aux+1,sizeof(aux)-1,objlen);
        aux[len+1] = '\r';
        aux[len+2] = '\n';
        /* 将长度格式化后的字符串放入复制积压缓冲区 */
        feedReplicationBuffer(aux,len+3);
        /* 将参数放入复制积压缓冲区块 */
        feedReplicationBufferWithObject(argv[j]);
        /* 将 \r\n 两个字符放入复制积压缓冲区块 */
        feedReplicationBuffer(aux+len+1,2);
    }
}

/* This is a debugging function that gets called when we detect something
 * wrong with the replication protocol: the goal is to peek into the
 * replication backlog and show a few final bytes to make simpler to
 * guess what kind of bug it could be. */
void showLatestBacklog(void) {
    if (server.repl_backlog == NULL) return;
    if (listLength(server.repl_buffer_blocks) == 0) return;

    size_t dumplen = 256;
    if (server.repl_backlog->histlen < (long long)dumplen)
        dumplen = server.repl_backlog->histlen;

    sds dump = sdsempty();
    listNode *node = listLast(server.repl_buffer_blocks);
    while(dumplen) {
        if (node == NULL) break;
        replBufBlock *o = listNodeValue(node);
        size_t thislen = o->used >= dumplen ? dumplen : o->used;
        sds head = sdscatrepr(sdsempty(), o->buf+o->used-thislen, thislen);
        sds tmp = sdscatsds(head, dump);
        sdsfree(dump);
        dump = tmp;
        dumplen -= thislen;
        node = listPrevNode(node);
    }

    /* Finally log such bytes: this is vital debugging info to
     * understand what happened. */
    serverLog(LL_WARNING,"Latest backlog is: '%s'", dump);
    sdsfree(dump);
}

/* This function is used in order to proxy what we receive from our master
 * to our sub-slaves. */
#include <ctype.h>
void replicationFeedStreamFromMasterStream(char *buf, size_t buflen) {
    /* Debugging: this is handy to see the stream sent from master
     * to slaves. Disabled with if(0). */
    if (0) {
        printf("%zu:",buflen);
        for (size_t j = 0; j < buflen; j++) {
            printf("%c", isprint(buf[j]) ? buf[j] : '.');
        }
        printf("\n");
    }

    /* There must be replication backlog if having attached slaves. */
    if (listLength(server.slaves)) serverAssert(server.repl_backlog != NULL);
    if (server.repl_backlog) {
        /* Must install write handler for all replicas first before feeding
         * replication stream. */
        prepareReplicasToWrite();
        feedReplicationBuffer(buf,buflen);
    }
}

void replicationFeedMonitors(client *c, list *monitors, int dictid, robj **argv, int argc) {
    /* Fast path to return if the monitors list is empty or the server is in loading. */
    if (monitors == NULL || listLength(monitors) == 0 || server.loading) return;
    listNode *ln;
    listIter li;
    int j;
    sds cmdrepr = sdsnew("+");
    robj *cmdobj;
    struct timeval tv;

    gettimeofday(&tv,NULL);
    cmdrepr = sdscatprintf(cmdrepr,"%ld.%06ld ",(long)tv.tv_sec,(long)tv.tv_usec);
    if (c->flags & CLIENT_SCRIPT) {
        cmdrepr = sdscatprintf(cmdrepr,"[%d lua] ",dictid);
    } else if (c->flags & CLIENT_UNIX_SOCKET) {
        cmdrepr = sdscatprintf(cmdrepr,"[%d unix:%s] ",dictid,server.unixsocket);
    } else {
        cmdrepr = sdscatprintf(cmdrepr,"[%d %s] ",dictid,getClientPeerId(c));
    }

    for (j = 0; j < argc; j++) {
        if (argv[j]->encoding == OBJ_ENCODING_INT) {
            cmdrepr = sdscatprintf(cmdrepr, "\"%ld\"", (long)argv[j]->ptr);
        } else {
            cmdrepr = sdscatrepr(cmdrepr,(char*)argv[j]->ptr,
                        sdslen(argv[j]->ptr));
        }
        if (j != argc-1)
            cmdrepr = sdscatlen(cmdrepr," ",1);
    }
    cmdrepr = sdscatlen(cmdrepr,"\r\n",2);
    cmdobj = createObject(OBJ_STRING,cmdrepr);

    listRewind(monitors,&li);
    while((ln = listNext(&li))) {
        client *monitor = ln->value;
        addReply(monitor,cmdobj);
        updateClientMemUsage(c);
    }
    decrRefCount(cmdobj);
}

/* Feed the slave 'c' with the replication backlog starting from the
 * specified 'offset' up to the end of the backlog. */
/* 将复制积压缓冲区从 offset 开始的所有命令发送给副本
 * 注：这里和 redis 6 版本的区别挺大，有兴趣的可以看看 redis 6 的实现，复制积压缓冲区是使用循环数组实现的，
 * 没有使用 rax 数据结构来做快速索引 */
long long addReplyReplicationBacklog(client *c, long long offset) {
    long long skip;

    serverLog(LL_DEBUG, "[PSYNC] Replica request offset: %lld", offset);

    /* 如果副本缓冲区的大小为 0，直接返回 */
    if (server.repl_backlog->histlen == 0) {
        serverLog(LL_DEBUG, "[PSYNC] Backlog history len is zero");
        return 0;
    }

    serverLog(LL_DEBUG, "[PSYNC] Backlog size: %lld",
             server.repl_backlog_size);
    serverLog(LL_DEBUG, "[PSYNC] First byte: %lld",
             server.repl_backlog->offset);
    serverLog(LL_DEBUG, "[PSYNC] History len: %lld",
             server.repl_backlog->histlen);

    /* Compute the amount of bytes we need to discard. */
    /* 用 当前需要读取的起始偏移量 - 复制积压缓冲区第一个字节的偏移量 = 需要忽略的复制积压缓冲区的数据长度 */
    skip = offset - server.repl_backlog->offset;
    serverLog(LL_DEBUG, "[PSYNC] Skipping: %lld", skip);

    /* Iterate recorded blocks, quickly search the approximate node. */
    /* 该属性最后会定位到需要发送给副本的第一个复制积压缓冲区块对应的节点，下面 if 判断和 while 循环就是定位逻辑 */
    listNode *node = NULL;
    /* 判断复制积压缓冲区的快速索引大小是否大于 0 */
    if (raxSize(server.repl_backlog->blocks_index) > 0) {
        /* long long 转 uint64 */
        uint64_t encoded_offset = htonu64(offset);
        raxIterator ri;
        /* 根据 rax 数据结构生成一个 raxIterator 迭代器 */
        raxStart(&ri, server.repl_backlog->blocks_index);
        /* 将迭代器迭代到刚好大于 offset 的位置 */
        raxSeek(&ri, ">", (unsigned char*)&encoded_offset, sizeof(uint64_t));
        /* 如果迭代器直接结束了，表示索引目前的存储的最大偏移量是小于当前给定的偏移量 */
        if (raxEOF(&ri)) {
            /* No found, so search from the last recorded node. */
            /* 将迭代器指向最后一个元素 */
            raxSeek(&ri, "$", NULL, 0);
            /* 迭代器往前移动一个元素 */
            raxPrev(&ri);
            /* 当前节点指向了尾部的前一个节点 */
            node = (listNode *)ri.data;
        } else {
            /* 没结束的情况 */
            /* 因为迭代器是迭代到了大于 offset 的位置，所以需要前移一个节点 */
            raxPrev(&ri); /* Skip the sought node. */
            /* We should search from the prev node since the offset of current
             * sought node exceeds searching offset. */
            if (raxPrev(&ri))
                node = (listNode *)ri.data;
            else
                node = server.repl_backlog->ref_repl_buf_node;
        }
        raxStop(&ri);
    } else {
        /* No recorded blocks, just from the start node to search. */
        /* 如果复制积压缓冲区没有快速索引的话，将节点指针指向复制积压缓冲区的第一个节点 */
        node = server.repl_backlog->ref_repl_buf_node;
    }

    /* Search the exact node. */
    /* 从上面的判断中获取到节点之后，接着往后遍历节点 */
    while (node != NULL) {
        replBufBlock *o = listNodeValue(node);
        /* 如果该节点的块的起始位置对应的复制积压缓冲区偏移量 + 块被使用的大小 >= offset 了，就表示找到了需要找的块节点 */
        if (o->repl_offset + (long long)o->used >= offset) break;
        /* 没找到就接着往下找 */
        node = listNextNode(node);
    }
    serverAssert(node != NULL);

    /* Install a writer handler first.*/
    /* 副本写数据的前置处理 */
    prepareClientToWrite(c);
    /* Setting output buffer of the replica. */
    /* 从当前找到的节点中获取复制积压缓冲区块 */
    replBufBlock *o = listNodeValue(node);
    /* 块的引用计数 +1 */
    o->refcount++;
    /* 设置副本需要开始复制的节点 */
    c->ref_repl_buf_node = node;
    /* 设置副本需要从该复制积压缓冲区块的什么位置开始复制
     * 给定的需要开始复制的偏移量 - 块当前在复制积压缓冲区的复制偏移量 = 块内当前需要开始复制的偏移量*/
    c->ref_block_pos = offset - o->repl_offset;
    /* 返回当前复制积压缓冲区中需要复制的数据，skip 就是给定的 offset 之前的数据，因为要从 offset 处开始复制
     * histlen 表示复制积压缓冲区中实际有的数据的大小，减去需要跳过的数据大小，剩下的就是副本要复制的数据大小 */
    return server.repl_backlog->histlen - skip;
}

/* Return the offset to provide as reply to the PSYNC command received
 * from the slave. The returned value is only valid immediately after
 * the BGSAVE process started and before executing any other command
 * from clients. */
/* 返回偏移量，作为对从从节点接收到的 PSYNC 命令的回复。
 * 返回的值仅在 BGSAVE 进程启动后以及从节点执行任何其他命令之前才有效 */
long long getPsyncInitialOffset(void) {
    return server.master_repl_offset;
}

/* Send a FULLRESYNC reply in the specific case of a full resynchronization,
 * as a side effect setup the slave for a full sync in different ways:
 *
 * 1) Remember, into the slave client structure, the replication offset
 *    we sent here, so that if new slaves will later attach to the same
 *    background RDB saving process (by duplicating this client output
 *    buffer), we can get the right offset from this slave.
 * 2) Set the replication state of the slave to WAIT_BGSAVE_END so that
 *    we start accumulating differences from this point.
 * 3) Force the replication stream to re-emit a SELECT statement so
 *    the new slave incremental differences will start selecting the
 *    right database number.
 *
 * Normally this function should be called immediately after a successful
 * BGSAVE for replication was started, or when there is one already in
 * progress that we attached our slave to. */

/* 在完整重同步下发送一个 FULLRESYNC 回复用来设置从节点的状态，不同的回复有着不同的
 * 作用：
 * 1) 为了保证新的从节点在到达相同的后台 RDB 存储进程以后，我们也能在从节点中获取到
 *    正确的偏移量，我们需要在 slave 客户端结构中保存我们已经发送的数据的复制偏移量
 * 2) 设置从节点的复制状态为 WAIT_BGSAVE_END（RDB 文件准备完毕了，后面到的命令不
 *    能再放到 RDB 文件中，而是放在复制积压缓冲区），然后开始累积当前主节点在当前时刻之
 *    后接收到的写命令。
 * 3) 强制复制流重新发出一个 SELECT 命令以保证主节点新增加的命令可以在从节点中正确
 *    编号的数据库中执行
 *
 * 先进行 RDB 快照同步，与此同时记录复制积压缓冲区的偏移量，该偏移量之后到的命令需要在副
 * 本接收完 rdb 后发送给副本 */
int replicationSetupSlaveForFullResync(client *slave, long long offset) {
    char buf[128];
    int buflen;
    /* 将副本的部分重同步的初始偏移量设置为提供的偏移量（在开始 rdb 同步的那一刻的复制偏移量） */
    slave->psync_initial_offset = offset;
    /* 复制状态转换 */
    slave->replstate = SLAVE_STATE_WAIT_BGSAVE_END;
    /* We are going to accumulate the incremental changes for this
     * slave as well. Set slaveseldb to -1 in order to force to re-emit
     * a SELECT statement in the replication stream. */
    /* 这里将从节点的数据库编号设置为 -1，后面会强制加一个 SELECT 来选择正确的数据库 */
    server.slaveseldb = -1;

    /* Don't send this reply to slaves that approached us with
     * the old SYNC command. */
    /* 如果副本不可以识别 PSYNC 报错 */
    if (!(slave->flags & CLIENT_PRE_PSYNC)) {
        buflen = snprintf(buf,sizeof(buf),"+FULLRESYNC %s %lld\r\n",
                          server.replid,offset);
        if (connWrite(slave->conn,buf,buflen) != buflen) {
            freeClientAsync(slave);
            return C_ERR;
        }
    }
    return C_OK;
}

/* This function handles the PSYNC command from the point of view of a
 * master receiving a request for partial resynchronization.
 *
 * On success return C_OK, otherwise C_ERR is returned and we proceed
 * with the usual full resync. */
/* 部分重同步 */
int masterTryPartialResynchronization(client *c, long long psync_offset) {
    long long psync_len;
    char *master_replid = c->argv[1]->ptr;
    char buf[128];
    int buflen;

    /* Is the replication ID of this master the same advertised by the wannabe
     * slave via PSYNC? If the replication ID changed this master has a
     * different replication history, and there is no way to continue.
     *
     * Note that there are two potentially valid replication IDs: the ID1
     * and the ID2. The ID2 however is only valid up to a specific offset. */
    /* 判断当前服务（主节点）持有的复制 id 是否和想要部分重同步的从节点匹配，如果不匹配
     * 表示主节点的历史复制和该从节点不符合，将不再继续接下来的步骤。（主节点已经开始新
     * 一轮的部分重同步了，但是从节点还是以前旧的部分重同步的过程，需要重新进行完整重同
     * 步） */
    if (strcasecmp(master_replid, server.replid) &&
        (strcasecmp(master_replid, server.replid2) ||
         psync_offset > server.second_replid_offset))
    {
        /* Replid "?" is used by slaves that want to force a full resync. */
        if (master_replid[0] != '?') {
            if (strcasecmp(master_replid, server.replid) &&
                strcasecmp(master_replid, server.replid2))
            {
                serverLog(LL_NOTICE,"Partial resynchronization not accepted: "
                    "Replication ID mismatch (Replica asked for '%s', my "
                    "replication IDs are '%s' and '%s')",
                    master_replid, server.replid, server.replid2);
            } else {
                serverLog(LL_NOTICE,"Partial resynchronization not accepted: "
                    "Requested offset for second ID was %lld, but I can reply "
                    "up to %lld", psync_offset, server.second_replid_offset);
            }
        } else {
            serverLog(LL_NOTICE,"Full resync requested by replica %s",
                replicationGetSlaveName(c));
        }
        /* 这里如果进入了该 if 中标识需要完整重同步 */
        goto need_full_resync;
    }

    /* We still have the data our slave is asking for? */
    /* 下面三种情况不能进行部分重同步
     * 1.复制积压缓冲区不存在
     * 2.需要部分重同步的偏移量小于当前复制积压缓冲区的偏移量
     * 3.需要部分重同步的偏移量太大，超过了复制积压缓冲区的总数据量 */
    if (!server.repl_backlog ||
        psync_offset < server.repl_backlog->offset ||
        psync_offset > (server.repl_backlog->offset + server.repl_backlog->histlen))
    {
        serverLog(LL_NOTICE,
            "Unable to partial resync with replica %s for lack of backlog (Replica request was: %lld).", replicationGetSlaveName(c), psync_offset);
        if (psync_offset > server.master_repl_offset) {
            serverLog(LL_WARNING,
                "Warning: replica %s tried to PSYNC with an offset that is greater than the master replication offset.", replicationGetSlaveName(c));
        }
        /* 需要完整重同步 */
        goto need_full_resync;
    }

    /* If we reached this point, we are able to perform a partial resync:
     * 1) Set client state to make it a slave.
     * 2) Inform the client we can continue with +CONTINUE
     * 3) Send the backlog data (from the offset to the end) to the slave. */
    /* 如果运行到这里，说明是部分重同步 */
    /* 将客户端的状态设置为副本 */
    c->flags |= CLIENT_SLAVE;
    /* 复制状态设置为副本在线状态 */
    c->replstate = SLAVE_STATE_ONLINE;
    /* 设置副本确认时间（会用来做超时判断） */
    c->repl_ack_time = server.unixtime;
    c->repl_start_cmd_stream_on_ack = 0;
    /* 将副本添加到 server.slaves 尾部 */
    listAddNodeTail(server.slaves,c);
    /* We can't use the connection buffers since they are used to accumulate
     * new commands at this stage. But we are sure the socket send buffer is
     * empty so this write will never fail actually. */
    /* 这里判断副本支持的主从复制版本， 支持 PSYNC2 协议 */
    if (c->slave_capa & SLAVE_CAPA_PSYNC2) {
        buflen = snprintf(buf,sizeof(buf),"+CONTINUE %s\r\n", server.replid);
    } else {
        buflen = snprintf(buf,sizeof(buf),"+CONTINUE\r\n");
    }
    if (connWrite(c->conn,buf,buflen) != buflen) {
        freeClientAsync(c);
        return C_OK;
    }
    /* 重要方法，发送复制日志给副本 */
    psync_len = addReplyReplicationBacklog(c,psync_offset);
    serverLog(LL_NOTICE,
        "Partial resynchronization request from %s accepted. Sending %lld bytes of backlog starting from offset %lld.",
            replicationGetSlaveName(c),
            psync_len, psync_offset);
    /* Note that we don't need to set the selected DB at server.slaveseldb
     * to -1 to force the master to emit SELECT, since the slave already
     * has this state from the previous connection with the master. */
    /* 一般是对于一轮部分重同步第一次复制的时候会设置 server.slaveseldb 为 -1，强制从节点选择正确的
     * 数据库，这里会记录这些从节点，下一次在同一轮部分重同步的时候做复制，可以不用在加 SELECT */
    refreshGoodSlavesCount();

    /* Fire the replica change modules event. */
    /* 触发副本改变事件 */
    moduleFireServerEvent(REDISMODULE_EVENT_REPLICA_CHANGE,
                          REDISMODULE_SUBEVENT_REPLICA_CHANGE_ONLINE,
                          NULL);

    return C_OK; /* The caller can return, no full resync needed. */
/* 上面出现的一些不符合部分重同步要求的情况，返回 C_ERR 执行完成重同步 */
need_full_resync:
    /* We need a full resync for some reason... Note that we can't
     * reply to PSYNC right now if a full SYNC is needed. The reply
     * must include the master offset at the time the RDB file we transfer
     * is generated, so we need to delay the reply to that moment. */
    return C_ERR;
}

/* Start a BGSAVE for replication goals, which is, selecting the disk or
 * socket target depending on the configuration, and making sure that
 * the script cache is flushed before to start.
 *
 * The mincapa argument is the bitwise AND among all the slaves capabilities
 * of the slaves waiting for this BGSAVE, so represents the slave capabilities
 * all the slaves support. Can be tested via SLAVE_CAPA_* macros.
 *
 * Side effects, other than starting a BGSAVE:
 *
 * 1) Handle the slaves in WAIT_START state, by preparing them for a full
 *    sync if the BGSAVE was successfully started, or sending them an error
 *    and dropping them from the list of slaves.
 *
 * 2) Flush the Lua scripting script cache if the BGSAVE was actually
 *    started.
 *
 * Returns C_OK on success or C_ERR otherwise. */

/* 为复制目标启动 BGSAVE，即根据配置选择磁盘或套接字目标，并确保在启动之前刷新脚本缓存。
 * mincapa 参数是等待此 BGSAVE 的从机的所有从机功能中的逐位 AND，因此表示所有从机支持的从机功能。可以通过 SLAVE_CAPA_* 宏进行测试。
 * 除启动 BGSAVE 外的副作用：
 * 1. 处理处于 WAIT_START状态的从机，如果 BGSAVE 成功启动，则为它们准备完全同步，或者向它们发送错误并将它们从从机列表中删除。 
 * 2. 如果 BGSAVE 实际启动，则刷新 Lua 脚本脚本缓存。*/
int startBgsaveForReplication(int mincapa, int req) {
    int retval;
    int socket_target = 0;
    listIter li;
    listNode *ln;

    /* We use a socket target if slave can handle the EOF marker and we're configured to do diskless syncs.
     * Note that in case we're creating a "filtered" RDB (functions-only, for example) we also force socket replication
     * to avoid overwriting the snapshot RDB file with filtered data. */
    /* 判断是使用 socket 发送 RDB 还是保存成文件之后再发送
     * 如果当前主节点设置了主从复制通过 socket 发送 RDB 或者从节点发送的请求指定了要过滤数据 或者 过滤函数的情况，
     * 且从节点有处理 EOF 标识的情况，就使用 socket 发送 RDB */
    socket_target = (server.repl_diskless_sync || req & SLAVE_REQ_RDB_MASK) && (mincapa & SLAVE_CAPA_EOF);
    /* `SYNC` should have failed with error if we don't support socket and require a filter, assert this here */
    serverAssert(socket_target || !(req & SLAVE_REQ_RDB_MASK));

    serverLog(LL_NOTICE,"Starting BGSAVE for SYNC with target: %s",
        socket_target ? "replicas sockets" : "disk");

    rdbSaveInfo rsi, *rsiptr;
    rsiptr = rdbPopulateSaveInfo(&rsi);
    /* Only do rdbSave* when rsiptr is not NULL,
     * otherwise slave will miss repl-stream-db. */
    /* 只有 无复制链的 master、未连接 master 的 slave、不存在 cachemaster 的 slave 无法进行 rdb 生成 */
    if (rsiptr) {
        if (socket_target)
            /* 直接通过 socket 发送 RDB */
            retval = rdbSaveToSlavesSockets(req,rsiptr);
        else
            retval = rdbSaveBackground(req,server.rdb_filename,rsiptr);
    } else {
        serverLog(LL_WARNING,"BGSAVE for replication: replication information not available, can't generate the RDB file right now. Try later.");
        retval = C_ERR;
    }

    /* If we succeeded to start a BGSAVE with disk target, let's remember
     * this fact, so that we can later delete the file if needed. Note
     * that we don't set the flag to 1 if the feature is disabled, otherwise
     * it would never be cleared: the file is not deleted. This way if
     * the user enables it later with CONFIG SET, we are fine. */
    /* 如果我们成功地启动了一个带有磁盘目标的 BGSAVE ，让我们记住这个事实，以便稍后在需要时删除该文件。
     * 请注意，如果功能被禁用，我们不会将标志设置为1，否则它将永远不会被清除：文件不会被删除。
     * 这样，如果用户稍后使用 CONFIG SET 启用它，我们就可以了*/
    if (retval == C_OK && !socket_target && server.rdb_del_sync_files)
        RDBGeneratedByReplication = 1;

    /* If we failed to BGSAVE, remove the slaves waiting for a full
     * resynchronization from the list of slaves, inform them with
     * an error about what happened, close the connection ASAP. */
    /* 如果我们无法 BGSAVE，请从从机列表中删除等待完全重新同步的从机，并通知他们发生的错误，尽快关闭连接 */
    if (retval == C_ERR) {
        serverLog(LL_WARNING,"BGSAVE for replication failed");
        listRewind(server.slaves,&li);
        while((ln = listNext(&li))) {
            client *slave = ln->value;

            /* 发送给所有等待 bgsave 开始的 slave 发送错误 BGSAVE failed */
            if (slave->replstate == SLAVE_STATE_WAIT_BGSAVE_START) {
                slave->replstate = REPL_STATE_NONE;
                slave->flags &= ~CLIENT_SLAVE;
                listDelNode(server.slaves,ln);
                addReplyError(slave,
                    "BGSAVE failed, replication can't continue");
                slave->flags |= CLIENT_CLOSE_AFTER_REPLY;
            }
        }
        return retval;
    }

    /* If the target is socket, rdbSaveToSlavesSockets() already setup
     * the slaves for a full resync. Otherwise for disk target do it now.*/
    /* 如果目标是套接字，则 rdbSaveToSlavesSockets() 已设置从机以进行完全重新同步。否则，对于磁盘目标，请立即执行 */
    if (!socket_target) {
        listRewind(server.slaves,&li);
        while((ln = listNext(&li))) {
            client *slave = ln->value;

            if (slave->replstate == SLAVE_STATE_WAIT_BGSAVE_START) {
                /* Check slave has the exact requirements */
                if (slave->slave_req != req)
                    continue;
                replicationSetupSlaveForFullResync(slave, getPsyncInitialOffset());
            }
        }
    }

    return retval;
}

/* SYNC and PSYNC command implementation. */
void syncCommand(client *c) {
    /* ignore SYNC if already slave or in monitor mode */
    if (c->flags & CLIENT_SLAVE) return;

    /* Check if this is a failover request to a replica with the same replid and
     * become a master if so. */
    if (c->argc > 3 && !strcasecmp(c->argv[0]->ptr,"psync") && 
        !strcasecmp(c->argv[3]->ptr,"failover"))
    {
        serverLog(LL_WARNING, "Failover request received for replid %s.",
            (unsigned char *)c->argv[1]->ptr);
        if (!server.masterhost) {
            addReplyError(c, "PSYNC FAILOVER can't be sent to a master.");
            return;
        }

        if (!strcasecmp(c->argv[1]->ptr,server.replid)) {
            replicationUnsetMaster();
            sds client = catClientInfoString(sdsempty(),c);
            serverLog(LL_NOTICE,
                "MASTER MODE enabled (failover request from '%s')",client);
            sdsfree(client);
        } else {
            addReplyError(c, "PSYNC FAILOVER replid must match my replid.");
            return;            
        }
    }

    /* Don't let replicas sync with us while we're failing over */
    /* 正在进行故障转移，不能进行主从同步 */
    if (server.failover_state != NO_FAILOVER) {
        addReplyError(c,"-NOMASTERLINK Can't SYNC while failing over");
        return;
    }

    /* Refuse SYNC requests if we are a slave but the link with our master
     * is not ok... */
    /* 主从之间的连接还没有建立好，不能进行主从同步 */
    if (server.masterhost && server.repl_state != REPL_STATE_CONNECTED) {
        addReplyError(c,"-NOMASTERLINK Can't SYNC while not connected with my master");
        return;
    }

    /* SYNC can't be issued when the server has pending data to send to
     * the client about already issued commands. We need a fresh reply
     * buffer registering the differences between the BGSAVE and the current
     * dataset, so that we can copy to other slaves if needed. */
    if (clientHasPendingReplies(c)) {
        addReplyError(c,"SYNC and PSYNC are invalid with pending output");
        return;
    }

    /* Fail sync if slave doesn't support EOF capability but wants a filtered RDB. This is because we force filtered
     * RDB's to be generated over a socket and not through a file to avoid conflicts with the snapshot files. Forcing
     * use of a socket is handled, if needed, in `startBgsaveForReplication`. */
    if (c->slave_req & SLAVE_REQ_RDB_MASK && !(c->slave_capa & SLAVE_CAPA_EOF)) {
        addReplyError(c,"Filtered replica requires EOF capability");
        return;
    }

    serverLog(LL_NOTICE,"Replica %s asks for synchronization",
        replicationGetSlaveName(c));

    /* Try a partial resynchronization if this is a PSYNC command.
     * If it fails, we continue with usual full resynchronization, however
     * when this happens replicationSetupSlaveForFullResync will replied
     * with:
     *
     * +FULLRESYNC <replid> <offset>
     *
     * So the slave knows the new replid and offset to try a PSYNC later
     * if the connection with the master is lost. */
    /* 如果接收的命令是 PSYNC ，会尝试进行部分重同步，如果部分重同步失败，就进行完整重同步 */
    if (!strcasecmp(c->argv[0]->ptr,"psync")) {
        long long psync_offset;
        if (getLongLongFromObjectOrReply(c, c->argv[2], &psync_offset, NULL) != C_OK) {
            serverLog(LL_WARNING, "Replica %s asks for synchronization but with a wrong offset",
                      replicationGetSlaveName(c));
            return;
        }
        /* 先尝试执行部分重同步 */
        if (masterTryPartialResynchronization(c, psync_offset) == C_OK) {
            /* 部分重同步成功，直接返回 */
            server.stat_sync_partial_ok++;
            return; /* No full resync needed, return. */
        } else {
            char *master_replid = c->argv[1]->ptr;

            /* Increment stats for failed PSYNCs, but only if the
             * replid is not "?", as this is used by slaves to force a full
             * resync on purpose when they are not able to partially
             * resync. */
            if (master_replid[0] != '?') server.stat_sync_partial_err++;
        }
    } else {
        /* If a slave uses SYNC, we are dealing with an old implementation
         * of the replication protocol (like redis-cli --slave). Flag the client
         * so that we don't expect to receive REPLCONF ACK feedbacks. */
        c->flags |= CLIENT_PRE_PSYNC;
    }

    /* Full resynchronization. */
    /* 接下来开始进行完整重同步 */
    server.stat_sync_full++;

    /* Setup the slave as one waiting for BGSAVE to start. The following code
     * paths will change the state if we handle the slave differently. */
    c->replstate = SLAVE_STATE_WAIT_BGSAVE_START;
    if (server.repl_disable_tcp_nodelay)
        connDisableTcpNoDelay(c->conn); /* Non critical if it fails. */
    c->repldbfd = -1;
    c->flags |= CLIENT_SLAVE;
    listAddNodeTail(server.slaves,c);

    /* Create the replication backlog if needed. */
    /* 第一次做主从复制，需要创建复制备份日志 */
    if (listLength(server.slaves) == 1 && server.repl_backlog == NULL) {
        /* When we create the backlog from scratch, we always use a new
         * replication ID and clear the ID2, since there is no valid
         * past history. */
        /* 修改 server.replid */
        changeReplicationId();
        clearReplicationId2();
        /* 创建一个复制 backlog */
        createReplicationBacklog();
        serverLog(LL_NOTICE,"Replication backlog created, my new "
                            "replication IDs are '%s' and '%s'",
                            server.replid, server.replid2);
    }

    /* CASE 1: BGSAVE is in progress, with disk target. */
    if (server.child_type == CHILD_TYPE_RDB &&
        server.rdb_child_type == RDB_CHILD_TYPE_DISK)
    {
        /* Ok a background save is in progress. Let's check if it is a good
         * one for replication, i.e. if there is another slave that is
         * registering differences since the server forked to save. */
        client *slave;
        listNode *ln;
        listIter li;

        listRewind(server.slaves,&li);
        while((ln = listNext(&li))) {
            slave = ln->value;
            /* If the client needs a buffer of commands, we can't use
             * a replica without replication buffer. */
            if (slave->replstate == SLAVE_STATE_WAIT_BGSAVE_END &&
                (!(slave->flags & CLIENT_REPL_RDBONLY) ||
                 (c->flags & CLIENT_REPL_RDBONLY)))
                break;
        }
        /* To attach this slave, we check that it has at least all the
         * capabilities of the slave that triggered the current BGSAVE
         * and its exact requirements. */
        if (ln && ((c->slave_capa & slave->slave_capa) == slave->slave_capa) &&
            c->slave_req == slave->slave_req) {
            /* Perfect, the server is already registering differences for
             * another slave. Set the right state, and copy the buffer.
             * We don't copy buffer if clients don't want. */
            if (!(c->flags & CLIENT_REPL_RDBONLY))
                copyReplicaOutputBuffer(c,slave);
            replicationSetupSlaveForFullResync(c,slave->psync_initial_offset);
            serverLog(LL_NOTICE,"Waiting for end of BGSAVE for SYNC");
        } else {
            /* No way, we need to wait for the next BGSAVE in order to
             * register differences. */
            serverLog(LL_NOTICE,"Can't attach the replica to the current BGSAVE. Waiting for next BGSAVE for SYNC");
        }

    /* CASE 2: BGSAVE is in progress, with socket target. */
    } else if (server.child_type == CHILD_TYPE_RDB &&
               server.rdb_child_type == RDB_CHILD_TYPE_SOCKET)
    {
        /* 当前正在进行 bgsave，但是 bgsave 的输出目标是 socket，需要等待下一次 BGSAVE */
        /* There is an RDB child process but it is writing directly to
         * children sockets. We need to wait for the next BGSAVE
         * in order to synchronize. */
        serverLog(LL_NOTICE,"Current BGSAVE has socket target. Waiting for next BGSAVE for SYNC");

    /* CASE 3: There is no BGSAVE is in progress. */
    } else {
        /* 这里是没有 BGSAVE 在执行的情况 */

        /* 如果配置了主从复制主节点直接通过 socket 发送 RDB，
         * 且从节点可以节点 RDB EOF 格式流，
         * 且服务端有设置延迟处理发送 RDB（为了等待更多的从节点连接）
         * 符合上面三个条件，会通过 replicationCron 定时调度功能来做 BGSAVE 发送 RDB 文件
         * replicationCron 会调用 startBgsaveForReplication 函数，这里可以直接去看该函数
         * 中使用 socket 发送 RDB 的情况
         */
        if (server.repl_diskless_sync && (c->slave_capa & SLAVE_CAPA_EOF) &&
            server.repl_diskless_sync_delay)
        {
            /* Diskless replication RDB child is created inside
             * replicationCron() since we want to delay its start a
             * few seconds to wait for more slaves to arrive. */
            serverLog(LL_NOTICE,"Delay next BGSAVE for diskless SYNC");
        } else {
            /* We don't have a BGSAVE in progress, let's start one. Diskless
             * or disk-based mode is determined by replica's capacity. */
            /* 这里其实就是没设置延迟发送 RDB 的情况。
             * 需要先判断是否有正在运行的子进程，如果没有，就直接调用 startBgsaveForReplication 函数*/
            if (!hasActiveChildProcess()) {
                startBgsaveForReplication(c->slave_capa, c->slave_req);
            } else {
                serverLog(LL_NOTICE,
                    "No BGSAVE in progress, but another BG operation is active. "
                    "BGSAVE for replication delayed");
            }
        }
    }
    return;
}

/* REPLCONF <option> <value> <option> <value> ...
 * This command is used by a replica in order to configure the replication
 * process before starting it with the SYNC command.
 * This command is also used by a master in order to get the replication
 * offset from a replica.
 *
 * Currently we support these options:
 *
 * - listening-port <port>
 * - ip-address <ip>
 * What is the listening ip and port of the Replica redis instance, so that
 * the master can accurately lists replicas and their listening ports in the
 * INFO output.
 *
 * - capa <eof|psync2>
 * What is the capabilities of this instance.
 * eof: supports EOF-style RDB transfer for diskless replication.
 * psync2: supports PSYNC v2, so understands +CONTINUE <new repl ID>.
 *
 * - ack <offset>
 * Replica informs the master the amount of replication stream that it
 * processed so far.
 *
 * - getack
 * Unlike other subcommands, this is used by master to get the replication
 * offset from a replica.
 *
 * - rdb-only <0|1>
 * Only wants RDB snapshot without replication buffer.
 *
 * - rdb-filter-only <include-filters>
 * Define "include" filters for the RDB snapshot. Currently we only support
 * a single include filter: "functions". Passing an empty string "" will
 * result in an empty RDB. */
void replconfCommand(client *c) {
    int j;

    if ((c->argc % 2) == 0) {
        /* Number of arguments must be odd to make sure that every
         * option has a corresponding value. */
        addReplyErrorObject(c,shared.syntaxerr);
        return;
    }

    /* Process every option-value pair. */
    for (j = 1; j < c->argc; j+=2) {
        if (!strcasecmp(c->argv[j]->ptr,"listening-port")) {
            long port;

            if ((getLongFromObjectOrReply(c,c->argv[j+1],
                    &port,NULL) != C_OK))
                return;
            c->slave_listening_port = port;
        } else if (!strcasecmp(c->argv[j]->ptr,"ip-address")) {
            sds addr = c->argv[j+1]->ptr;
            if (sdslen(addr) < NET_HOST_STR_LEN) {
                if (c->slave_addr) sdsfree(c->slave_addr);
                c->slave_addr = sdsdup(addr);
            } else {
                addReplyErrorFormat(c,"REPLCONF ip-address provided by "
                    "replica instance is too long: %zd bytes", sdslen(addr));
                return;
            }
        } else if (!strcasecmp(c->argv[j]->ptr,"capa")) {
            /* Ignore capabilities not understood by this master. */
            if (!strcasecmp(c->argv[j+1]->ptr,"eof"))
                c->slave_capa |= SLAVE_CAPA_EOF;
            else if (!strcasecmp(c->argv[j+1]->ptr,"psync2"))
                c->slave_capa |= SLAVE_CAPA_PSYNC2;
        } else if (!strcasecmp(c->argv[j]->ptr,"ack")) {
            /* REPLCONF ACK is used by slave to inform the master the amount
             * of replication stream that it processed so far. It is an
             * internal only command that normal clients should never use. */
            long long offset;

            if (!(c->flags & CLIENT_SLAVE)) return;
            if ((getLongLongFromObject(c->argv[j+1], &offset) != C_OK))
                return;
            if (offset > c->repl_ack_off)
                c->repl_ack_off = offset;
            c->repl_ack_time = server.unixtime;
            /* If this was a diskless replication, we need to really put
             * the slave online when the first ACK is received (which
             * confirms slave is online and ready to get more data). This
             * allows for simpler and less CPU intensive EOF detection
             * when streaming RDB files.
             * There's a chance the ACK got to us before we detected that the
             * bgsave is done (since that depends on cron ticks), so run a
             * quick check first (instead of waiting for the next ACK. */
            if (server.child_type == CHILD_TYPE_RDB && c->replstate == SLAVE_STATE_WAIT_BGSAVE_END)
                checkChildrenDone();
            if (c->repl_start_cmd_stream_on_ack && c->replstate == SLAVE_STATE_ONLINE)
                replicaStartCommandStream(c);
            /* Note: this command does not reply anything! */
            return;
        } else if (!strcasecmp(c->argv[j]->ptr,"getack")) {
            /* REPLCONF GETACK is used in order to request an ACK ASAP
             * to the slave. */
            if (server.masterhost && server.master) replicationSendAck();
            return;
        } else if (!strcasecmp(c->argv[j]->ptr,"rdb-only")) {
           /* REPLCONF RDB-ONLY is used to identify the client only wants
            * RDB snapshot without replication buffer. */
            long rdb_only = 0;
            if (getRangeLongFromObjectOrReply(c,c->argv[j+1],
                    0,1,&rdb_only,NULL) != C_OK)
                return;
            if (rdb_only == 1) c->flags |= CLIENT_REPL_RDBONLY;
            else c->flags &= ~CLIENT_REPL_RDBONLY;
        } else if (!strcasecmp(c->argv[j]->ptr,"rdb-filter-only")) {
            /* REPLCONFG RDB-FILTER-ONLY is used to define "include" filters
             * for the RDB snapshot. Currently we only support a single
             * include filter: "functions". In the future we may want to add
             * other filters like key patterns, key types, non-volatile, module
             * aux fields, ...
             * We might want to add the complementing "RDB-FILTER-EXCLUDE" to
             * filter out certain data. */
            int filter_count, i;
            sds *filters;
            if (!(filters = sdssplitargs(c->argv[j+1]->ptr, &filter_count))) {
                addReplyErrorFormat(c, "Missing rdb-filter-only values");
                return;
            }
            /* By default filter out all parts of the rdb */
            c->slave_req |= SLAVE_REQ_RDB_EXCLUDE_DATA;
            c->slave_req |= SLAVE_REQ_RDB_EXCLUDE_FUNCTIONS;
            for (i = 0; i < filter_count; i++) {
                if (!strcasecmp(filters[i], "functions"))
                    c->slave_req &= ~SLAVE_REQ_RDB_EXCLUDE_FUNCTIONS;
                else {
                    addReplyErrorFormat(c, "Unsupported rdb-filter-only option: %s", (char*)filters[i]);
                    sdsfreesplitres(filters, filter_count);
                    return;
                }
            }
            sdsfreesplitres(filters, filter_count);
        } else {
            addReplyErrorFormat(c,"Unrecognized REPLCONF option: %s",
                (char*)c->argv[j]->ptr);
            return;
        }
    }
    addReply(c,shared.ok);
}

/* This function puts a replica in the online state, and should be called just
 * after a replica received the RDB file for the initial synchronization.
 *
 * It does a few things:
 * 1) Put the slave in ONLINE state.
 * 2) Update the count of "good replicas".
 * 3) Trigger the module event. */
/**
 * 此函数将副本置于联机状态，并应在副本收到用于初始同步的RDB文件后立即调用。
 * 其中包括以下影响：
 * 1. 将从节点设置为 online 状态
 * 2. 更新 repl_good_slaves_count 计数
 * 3. 触发 module 的事件
*/
void replicaPutOnline(client *slave) {
    /* 如果从节点只需要 RDB 文件，则不需要继续设置为 online 状态，参考：redis-cli --rdb */
    if (slave->flags & CLIENT_REPL_RDBONLY) {
        return;
    }

    slave->replstate = SLAVE_STATE_ONLINE;
    slave->repl_ack_time = server.unixtime; /* Prevent false timeout. */

    refreshGoodSlavesCount();
    /* Fire the replica change modules event. */
    moduleFireServerEvent(REDISMODULE_EVENT_REPLICA_CHANGE,
                          REDISMODULE_SUBEVENT_REPLICA_CHANGE_ONLINE,
                          NULL);
    serverLog(LL_NOTICE,"Synchronization with replica %s succeeded",
        replicationGetSlaveName(slave));
}

/* This function should be called just after a replica received the RDB file
 * for the initial synchronization, and we are finally ready to send the
 * incremental stream of commands.
 *
 * It does a few things:
 * 1) Close the replica's connection async if it doesn't need replication
 *    commands buffer stream, since it actually isn't a valid replica.
 * 2) Make sure the writable event is re-installed, since when calling the SYNC
 *    command we had no replies and it was disabled, and then we could
 *    accumulate output buffer data without sending it to the replica so it
 *    won't get mixed with the RDB stream. */

/* 在同步的 RDB 文件之后立即调用此函数，并且开始发送增量命令流
 *
 * 它做了一些事情：
 * 1. 如果不需要复制命令缓冲流，则异步关闭副本的连接，因为它实际上不是有效的副本。
 * 2. 将当前的 slave 写入 pendingWriteQueue 待写队列中 */
void replicaStartCommandStream(client *slave) {
    /* 设置当前的 slave 已经设置了可写事件，不需要重入 */
    slave->repl_start_cmd_stream_on_ack = 0;
    if (slave->flags & CLIENT_REPL_RDBONLY) {
        serverLog(LL_NOTICE,
            "Close the connection with replica %s as RDB transfer is complete",
            replicationGetSlaveName(slave));
        freeClientAsync(slave);
        return;
    }

    putClientInPendingWriteQueue(slave);
}

/* We call this function periodically to remove an RDB file that was
 * generated because of replication, in an instance that is otherwise
 * without any persistence. We don't want instances without persistence
 * to take RDB files around, this violates certain policies in certain
 * environments. */
/* 定期调用此函数以删除由于复制而生成的 RDB 文件，而该文件在其他情况下没有任何持久性。
 * 不希望没有持久性的实例到处携带 RDB 文件，这违反了某些环境中的某些策略 */
void removeRDBUsedToSyncReplicas(void) {
    /* If the feature is disabled, return ASAP but also clear the
     * RDBGeneratedByReplication flag in case it was set. Otherwise if the
     * feature was enabled, but gets disabled later with CONFIG SET, the
     * flag may remain set to one: then next time the feature is re-enabled
     * via CONFIG SET we have it set even if no RDB was generated
     * because of replication recently. */
    /* 如果该功能被禁用，请尽快返回，但如果设置了 RDBGeneratedByReplication 标志，请清除该标志。否则，如果该功能已启用，
     * 但稍后使用 CONFIG SET 被禁用，则该标志可能会保持为1
     * 那么下次通过 CONFIG NET 重新启用该功能时，即使最近由于复制而没有生成 RDB，我们也会将其设置为1 */
    if (!server.rdb_del_sync_files) {
        RDBGeneratedByReplication = 0;
        return;
    }

    if (allPersistenceDisabled() && RDBGeneratedByReplication) {
        client *slave;
        listNode *ln;
        listIter li;

        int delrdb = 1;
        listRewind(server.slaves,&li);
        while((ln = listNext(&li))) {
            slave = ln->value;
            if (slave->replstate == SLAVE_STATE_WAIT_BGSAVE_START ||
                slave->replstate == SLAVE_STATE_WAIT_BGSAVE_END ||
                slave->replstate == SLAVE_STATE_SEND_BULK)
            {
                delrdb = 0;
                break; /* No need to check the other replicas. */
            }
        }
        /* 如果不存在等待 bgsave 的从节点，则开始删除 rdb 文件 */
        if (delrdb) {
            struct stat sb;
            if (lstat(server.rdb_filename,&sb) != -1) {
                RDBGeneratedByReplication = 0;
                serverLog(LL_NOTICE,
                    "Removing the RDB file used to feed replicas "
                    "in a persistence-less instance");
                bg_unlink(server.rdb_filename);
            }
        }
    }
}

/**
 * 主节点进行主从同步时绑定的写回调事件
*/
void sendBulkToSlave(connection *conn) {
    client *slave = connGetPrivateData(conn);
    char buf[PROTO_IOBUF_LEN];
    ssize_t nwritten, buflen;

    /* Before sending the RDB file, we send the preamble as configured by the
     * replication process. Currently the preamble is just the bulk count of
     * the file in the form "$<length>\r\n". */
    /* 在发送 RDB 文件之前，需要提前发送复制内容的属性
     * 目前，属性只是包含文件的大小，格式为 “$<length>\r\n”
     * 这个字段在设置写事件时设置，并在发送后设置为 NULL */
    if (slave->replpreamble) {
        nwritten = connWrite(conn,slave->replpreamble,sdslen(slave->replpreamble));
        if (nwritten == -1) {
            serverLog(LL_WARNING,
                "Write error sending RDB preamble to replica: %s",
                connGetLastError(conn));
            freeClient(slave);
            return;
        }
        atomicIncr(server.stat_net_repl_output_bytes, nwritten);
        /* 移除 replpreamble 已发送的数据 */
        sdsrange(slave->replpreamble,nwritten,-1);
        /* 若 replpreamble 已经发送完成，则直接清空 */
        if (sdslen(slave->replpreamble) == 0) {
            sdsfree(slave->replpreamble);
            slave->replpreamble = NULL;
            /* fall through sending data. */
        } else {
            return;
        }
    }

    /* If the preamble was already transferred, send the RDB bulk data. */
    /* 发送 replpreamble 后，开始发送 RDB 文件数据，同时设置当前的偏移量，以及每次传输 16K 数据 */
    lseek(slave->repldbfd,slave->repldboff,SEEK_SET);
    buflen = read(slave->repldbfd,buf,PROTO_IOBUF_LEN);
    if (buflen <= 0) {
        serverLog(LL_WARNING,"Read error sending DB to replica: %s",
            (buflen == 0) ? "premature EOF" : strerror(errno));
        freeClient(slave);
        return;
    }
    if ((nwritten = connWrite(conn,buf,buflen)) == -1) {
        if (connGetState(conn) != CONN_STATE_CONNECTED) {
            serverLog(LL_WARNING,"Write error sending DB to replica: %s",
                connGetLastError(conn));
            freeClient(slave);
        }
        return;
    }
    slave->repldboff += nwritten;
    atomicIncr(server.stat_net_repl_output_bytes, nwritten);
    /* 当写入的数据量和RDB数据大小相同，则认为传输完成，开始关闭RDB的fd，并重置写事件 */
    if (slave->repldboff == slave->repldbsize) {
        close(slave->repldbfd);
        slave->repldbfd = -1;
        connSetWriteHandler(slave->conn,NULL);
        replicaPutOnline(slave);
        replicaStartCommandStream(slave);
    }
}

/* Remove one write handler from the list of connections waiting to be writable
 * during rdb pipe transfer. */
void rdbPipeWriteHandlerConnRemoved(struct connection *conn) {
    if (!connHasWriteHandler(conn))
        return;
    /* 注销 RDB 管道数据发送给从节点的文件事件处理器 */
    connSetWriteHandler(conn, NULL);
    client *slave = connGetPrivateData(conn);
    slave->repl_last_partial_write = 0;
    server.rdb_pipe_numconns_writing--;
    /* if there are no more writes for now for this conn, or write error: */
    if (server.rdb_pipe_numconns_writing == 0) {
        /* 本次从管道中读取的数据已经全部发送给从节点了，可以接着从管道读取数据了 */
        if (aeCreateFileEvent(server.el, server.rdb_pipe_read, AE_READABLE, rdbPipeReadHandler,NULL) == AE_ERR) {
            serverPanic("Unrecoverable error creating server.rdb_pipe_read file event.");
        }
    }
}

/* Called in diskless master during transfer of data from the rdb pipe, when
 * the replica becomes writable again. */
/* 从 RDB 管道中将数据发送给从节点处理器，该函数是作为文件事件注册给事件循环器的处理函数 */
void rdbPipeWriteHandler(struct connection *conn) {
    serverAssert(server.rdb_pipe_bufflen>0);
    client *slave = connGetPrivateData(conn);
    ssize_t nwritten;
    /* repldboff 在这里起作用了，
     * 将管道缓冲区中的数据从 repldboff 位置开始之后的数据写到从节点 */
    if ((nwritten = connWrite(conn, server.rdb_pipe_buff + slave->repldboff,
                              server.rdb_pipe_bufflen - slave->repldboff)) == -1)
    {
        if (connGetState(conn) == CONN_STATE_CONNECTED)
            return; /* equivalent to EAGAIN */
        serverLog(LL_WARNING,"Write error sending DB to replica: %s",
            connGetLastError(conn));
        freeClient(slave);
        return;
    } else {
        /* repldboff 加上本次发送出去的数据量 */
        slave->repldboff += nwritten;
        atomicIncr(server.stat_net_repl_output_bytes, nwritten);
        /* 还是没有发送完，不用管了，因为已经将管道写事件注册到事件循环器中了，
         * 本次没发送完，之后事件循环器会接着调用本函数 */
        if (slave->repldboff < server.rdb_pipe_bufflen) {
            slave->repl_last_partial_write = server.unixtime;
            return; /* more data to write.. */
        }
    }
    /* 到这里，表示数据发送完了，会把管道写处理事件注销掉 */
    rdbPipeWriteHandlerConnRemoved(conn);
}

/* Called in diskless master, when there's data to read from the child's rdb pipe */
/* startBgsaveForReplication 函数在使用 socket 发送 RDB 会调用 rdb.c 中的 rdbSaveToSlavesSockets 函数
 * 该函数会使用子进程将 rdb 内容通过管道发送给父进程，父进程调用该方法接收管道中的数据 */
void rdbPipeReadHandler(struct aeEventLoop *eventLoop, int fd, void *clientData, int mask) {
    UNUSED(mask);
    UNUSED(clientData);
    UNUSED(eventLoop);
    int i;
    /* rdb 管道缓冲区没有初始化，在这里做初始化 */
    if (!server.rdb_pipe_buff)
        server.rdb_pipe_buff = zmalloc(PROTO_IOBUF_LEN);
    serverAssert(server.rdb_pipe_numconns_writing==0);

    while (1) {
        /* 将管道中的数据读取到 rdb_pipe_buff 中 */
        server.rdb_pipe_bufflen = read(fd, server.rdb_pipe_buff, PROTO_IOBUF_LEN);
        /* 读取管道数据读取出错了 */
        if (server.rdb_pipe_bufflen < 0) {
            /* 直接返回的报错情况 */
            if (errno == EAGAIN || errno == EWOULDBLOCK)
                return;
            serverLog(LL_WARNING,"Diskless rdb transfer, read error sending DB to replicas: %s", strerror(errno));
            /* 遍历所有需要用 SOCKET 发送 RDB 的从节点连接，将这些连接的客户端关闭 */
            for (i=0; i < server.rdb_pipe_numconns; i++) {
                connection *conn = server.rdb_pipe_conns[i];
                if (!conn)
                    continue;
                client *slave = connGetPrivateData(conn);
                freeClient(slave);
                server.rdb_pipe_conns[i] = NULL;
            }
            /* 结束 RDB 子进程 */
            killRDBChild();
            return;
        }

        /* 数据读取完了，这里已经读取不到任何数据了，可以关闭管道，和返回了 */
        if (server.rdb_pipe_bufflen == 0) {
            /* EOF - write end was closed. */
            int stillUp = 0;
            /* 移除管道文件事件 */
            aeDeleteFileEvent(server.el, server.rdb_pipe_read, AE_READABLE);
            for (i=0; i < server.rdb_pipe_numconns; i++)
            {
                connection *conn = server.rdb_pipe_conns[i];
                if (!conn)
                    continue;
                stillUp++;
            }
            serverLog(LL_WARNING,"Diskless rdb transfer, done reading from pipe, %d replicas still up.", stillUp);
            /* Now that the replicas have finished reading, notify the child that it's safe to exit. 
             * When the server detects the child has exited, it can mark the replica as online, and
             * start streaming the replication buffers. */
            close(server.rdb_child_exit_pipe);
            server.rdb_child_exit_pipe = -1;
            return;
        }

        /* 到这里，表示现在能从管道中读到数据 */

        int stillAlive = 0;
        for (i=0; i < server.rdb_pipe_numconns; i++)
        {
            ssize_t nwritten;
            connection *conn = server.rdb_pipe_conns[i];
            /* 如果连接不存在，处理下一个连接 */
            if (!conn)
                continue;

            client *slave = connGetPrivateData(conn);
            /* 将本次读取到的数据发送给从节点 */
            if ((nwritten = connWrite(conn, server.rdb_pipe_buff, server.rdb_pipe_bufflen)) == -1) {
                /* 连接出错了的情况 */
                if (connGetState(conn) != CONN_STATE_CONNECTED) {
                    serverLog(LL_WARNING,"Diskless rdb transfer, write error sending DB to replica: %s",
                        connGetLastError(conn));
                    freeClient(slave);
                    server.rdb_pipe_conns[i] = NULL;
                    continue;
                }
                /* An error and still in connected state, is equivalent to EAGAIN */
                /* 从节点复制偏移量置 0 */
                slave->repldboff = 0;
            } else {
                /* Note: when use diskless replication, 'repldboff' is the offset
                 * of 'rdb_pipe_buff' sent rather than the offset of entire RDB. */
                /* 这里会设置本次写给客户端数据的偏移量
                 * 注：该属性的作用是如果一次写不完（nwritten < server.rdb_pipe_bufflen）的情况，
                 * 该属性会作为一个游标，表示本次从管道中读取的数据写到哪个位置了 */
                slave->repldboff = nwritten;
                atomicIncr(server.stat_net_repl_output_bytes, nwritten);
            }
            /* If we were unable to write all the data to one of the replicas,
             * setup write handler (and disable pipe read handler, below) */
            /* 如果不能一次把本次从管道中读到的数据发送给从节点，这里会添加一个文件事件分多次写数据 */
            if (nwritten != server.rdb_pipe_bufflen) {
                slave->repl_last_partial_write = server.unixtime;
                /* 标记当前正在写数据 */
                server.rdb_pipe_numconns_writing++;
                connSetWriteHandler(conn, rdbPipeWriteHandler);
            }
            stillAlive++;
        }

        if (stillAlive == 0) {
            serverLog(LL_WARNING,"Diskless rdb transfer, last replica dropped, killing fork child.");
            killRDBChild();
        }
        /*  Remove the pipe read handler if at least one write handler was set. */
        if (server.rdb_pipe_numconns_writing || stillAlive == 0) {
            aeDeleteFileEvent(server.el, server.rdb_pipe_read, AE_READABLE);
            break;
        }
    }
}

/* This function is called at the end of every background saving.
 *
 * The argument bgsaveerr is C_OK if the background saving succeeded
 * otherwise C_ERR is passed to the function.
 * The 'type' argument is the type of the child that terminated
 * (if it had a disk or socket target). */
void updateSlavesWaitingBgsave(int bgsaveerr, int type) {
    listNode *ln;
    listIter li;

    /* Note: there's a chance we got here from within the REPLCONF ACK command
     * so we must avoid using freeClient, otherwise we'll crash on our way up. */

    listRewind(server.slaves,&li);
    while((ln = listNext(&li))) {
        client *slave = ln->value;

        if (slave->replstate == SLAVE_STATE_WAIT_BGSAVE_END) {
            struct redis_stat buf;

            if (bgsaveerr != C_OK) {
                freeClientAsync(slave);
                serverLog(LL_WARNING,"SYNC failed. BGSAVE child returned an error");
                continue;
            }

            /* If this was an RDB on disk save, we have to prepare to send
             * the RDB from disk to the slave socket. Otherwise if this was
             * already an RDB -> Slaves socket transfer, used in the case of
             * diskless replication, our work is trivial, we can just put
             * the slave online. */
            if (type == RDB_CHILD_TYPE_SOCKET) {
                serverLog(LL_NOTICE,
                    "Streamed RDB transfer with replica %s succeeded (socket). Waiting for REPLCONF ACK from slave to enable streaming",
                        replicationGetSlaveName(slave));
                /* Note: we wait for a REPLCONF ACK message from the replica in
                 * order to really put it online (install the write handler
                 * so that the accumulated data can be transferred). However
                 * we change the replication state ASAP, since our slave
                 * is technically online now.
                 *
                 * So things work like that:
                 *
                 * 1. We end transferring the RDB file via socket.
                 * 2. The replica is put ONLINE but the write handler
                 *    is not installed.
                 * 3. The replica however goes really online, and pings us
                 *    back via REPLCONF ACK commands.
                 * 4. Now we finally install the write handler, and send
                 *    the buffers accumulated so far to the replica.
                 *
                 * But why we do that? Because the replica, when we stream
                 * the RDB directly via the socket, must detect the RDB
                 * EOF (end of file), that is a special random string at the
                 * end of the RDB (for streamed RDBs we don't know the length
                 * in advance). Detecting such final EOF string is much
                 * simpler and less CPU intensive if no more data is sent
                 * after such final EOF. So we don't want to glue the end of
                 * the RDB transfer with the start of the other replication
                 * data. */
                replicaPutOnline(slave);
                slave->repl_start_cmd_stream_on_ack = 1;
            } else {
                if ((slave->repldbfd = open(server.rdb_filename,O_RDONLY)) == -1 ||
                    redis_fstat(slave->repldbfd,&buf) == -1) {
                    freeClientAsync(slave);
                    serverLog(LL_WARNING,"SYNC failed. Can't open/stat DB after BGSAVE: %s", strerror(errno));
                    continue;
                }
                slave->repldboff = 0;
                slave->repldbsize = buf.st_size;
                slave->replstate = SLAVE_STATE_SEND_BULK;
                slave->replpreamble = sdscatprintf(sdsempty(),"$%lld\r\n",
                    (unsigned long long) slave->repldbsize);

                connSetWriteHandler(slave->conn,NULL);
                if (connSetWriteHandler(slave->conn,sendBulkToSlave) == C_ERR) {
                    freeClientAsync(slave);
                    continue;
                }
            }
        }
    }
}

/* Change the current instance replication ID with a new, random one.
 * This will prevent successful PSYNCs between this master and other
 * slaves, so the command should be called when something happens that
 * alters the current story of the dataset. */
/* 重新随机生成一个复制 ID，表示需要重新做主从复制 */
void changeReplicationId(void) {
    getRandomHexChars(server.replid,CONFIG_RUN_ID_SIZE);
    server.replid[CONFIG_RUN_ID_SIZE] = '\0';
}

/* Clear (invalidate) the secondary replication ID. This happens, for
 * example, after a full resynchronization, when we start a new replication
 * history. */
void clearReplicationId2(void) {
    memset(server.replid2,'0',sizeof(server.replid));
    server.replid2[CONFIG_RUN_ID_SIZE] = '\0';
    server.second_replid_offset = -1;
}

/* Use the current replication ID / offset as secondary replication
 * ID, and change the current one in order to start a new history.
 * This should be used when an instance is switched from slave to master
 * so that it can serve PSYNC requests performed using the master
 * replication ID. */
void shiftReplicationId(void) {
    memcpy(server.replid2,server.replid,sizeof(server.replid));
    /* We set the second replid offset to the master offset + 1, since
     * the slave will ask for the first byte it has not yet received, so
     * we need to add one to the offset: for example if, as a slave, we are
     * sure we have the same history as the master for 50 bytes, after we
     * are turned into a master, we can accept a PSYNC request with offset
     * 51, since the slave asking has the same history up to the 50th
     * byte, and is asking for the new bytes starting at offset 51. */
    server.second_replid_offset = server.master_repl_offset+1;
    changeReplicationId();
    serverLog(LL_WARNING,"Setting secondary replication ID to %s, valid up to offset: %lld. New replication ID is %s", server.replid2, server.second_replid_offset, server.replid);
}

/* ----------------------------------- SLAVE -------------------------------- */

/* Returns 1 if the given replication state is a handshake state,
 * 0 otherwise. */
int slaveIsInHandshakeState(void) {
    return server.repl_state >= REPL_STATE_RECEIVE_PING_REPLY &&
           server.repl_state <= REPL_STATE_RECEIVE_PSYNC_REPLY;
}

/* Avoid the master to detect the slave is timing out while loading the
 * RDB file in initial synchronization. We send a single newline character
 * that is valid protocol but is guaranteed to either be sent entirely or
 * not, since the byte is indivisible.
 *
 * The function is called in two contexts: while we flush the current
 * data with emptyDb(), and while we load the new data received as an
 * RDB file from the master. */
/*
 * 在初次同步过程中从节点需要花费较长时间加载 RDB 文件。为了避免主节点误认为此状态下的从节点超时，
 * 我们会向主节点发送一个 LF('\n') 字符。
 * 这里为了避免意外情况导致 CRLF 被划分到两条消息里所以只发送单个字符，
 * 并在协议中兼容只有一个 LF 字符的情况。
 * 
 * 这个函数会在两个上下文中被调用：
 * 1. 使用 emptyDb() 清空当前数据时
 * 2. 加载从主节点获得的 RDB 文件时
*/
void replicationSendNewlineToMaster(void) {
    static time_t newline_sent;
    if (time(NULL) != newline_sent) {
        newline_sent = time(NULL);
        /* Pinging back in this stage is best-effort. */
        if (server.repl_transfer_s) connWrite(server.repl_transfer_s, "\n", 1);
    }
}

/* Callback used by emptyDb() while flushing away old data to load
 * the new dataset received by the master and by discardTempDb()
 * after loading succeeded or failed. */
void replicationEmptyDbCallback(dict *d) {
    UNUSED(d);
    if (server.repl_state == REPL_STATE_TRANSFER)
        replicationSendNewlineToMaster();
}

/* Once we have a link with the master and the synchronization was
 * performed, this function materializes the master client we store
 * at server.master, starting from the specified file descriptor. */
/* 一旦我们与 master 建立了链接并执行了同步，此函数将从指定的文件描述符开始具体化我们存储在 server.master 中的 master 客户端 */
void replicationCreateMasterClient(connection *conn, int dbid) {
    server.master = createClient(conn);
    if (conn)
        connSetReadHandler(server.master->conn, readQueryFromClient);

    /**
     * Important note:
     * The CLIENT_DENY_BLOCKING flag is not, and should not, be set here.
     * For commands like BLPOP, it makes no sense to block the master
     * connection, and such blocking attempt will probably cause deadlock and
     * break the replication. We consider such a thing as a bug because
     * commands as BLPOP should never be sent on the replication link.
     * A possible use-case for blocking the replication link is if a module wants
     * to pass the execution to a background thread and unblock after the
     * execution is done. This is the reason why we allow blocking the replication
     * connection. */
    server.master->flags |= CLIENT_MASTER;

    server.master->authenticated = 1;
    server.master->reploff = server.master_initial_offset;
    server.master->read_reploff = server.master->reploff;
    server.master->user = NULL; /* This client can do everything. */
    memcpy(server.master->replid, server.master_replid,
        sizeof(server.master_replid));
    /* If master offset is set to -1, this master is old and is not
     * PSYNC capable, so we flag it accordingly. */
    if (server.master->reploff == -1)
        server.master->flags |= CLIENT_PRE_PSYNC;
    if (dbid != -1) selectDb(server.master,dbid);
}

/* This function will try to re-enable the AOF file after the
 * master-replica synchronization: if it fails after multiple attempts
 * the replica cannot be considered reliable and exists with an
 * error. */
void restartAOFAfterSYNC() {
    unsigned int tries, max_tries = 10;
    for (tries = 0; tries < max_tries; ++tries) {
        if (startAppendOnly() == C_OK) break;
        serverLog(LL_WARNING,
            "Failed enabling the AOF after successful master synchronization! "
            "Trying it again in one second.");
        sleep(1);
    }
    if (tries == max_tries) {
        serverLog(LL_WARNING,
            "FATAL: this replica instance finished the synchronization with "
            "its master, but the AOF can't be turned on. Exiting now.");
        exit(1);
    }
}

static int useDisklessLoad() {
    /* compute boolean decision to use diskless load */
    int enabled = server.repl_diskless_load == REPL_DISKLESS_LOAD_SWAPDB ||
           (server.repl_diskless_load == REPL_DISKLESS_LOAD_WHEN_DB_EMPTY && dbTotalServerKeyCount()==0);

    if (enabled) {
        /* Check all modules handle read errors, otherwise it's not safe to use diskless load. */
        if (!moduleAllDatatypesHandleErrors()) {
            serverLog(LL_WARNING,
                "Skipping diskless-load because there are modules that don't handle read errors.");
            enabled = 0;
        }
        /* Check all modules handle async replication, otherwise it's not safe to use diskless load. */
        else if (server.repl_diskless_load == REPL_DISKLESS_LOAD_SWAPDB && !moduleAllModulesHandleReplAsyncLoad()) {
            serverLog(LL_WARNING,
                "Skipping diskless-load because there are modules that are not aware of async replication.");
            enabled = 0;
        }
    }
    return enabled;
}

/* Helper function for readSyncBulkPayload() to initialize tempDb
 * before socket-loading the new db from master. The tempDb may be populated
 * by swapMainDbWithTempDb or freed by disklessLoadDiscardTempDb later. */
redisDb *disklessLoadInitTempDb(void) {
    return initTempDb();
}

/* Helper function for readSyncBulkPayload() to discard our tempDb
 * when the loading succeeded or failed. */
void disklessLoadDiscardTempDb(redisDb *tempDb) {
    discardTempDb(tempDb, replicationEmptyDbCallback);
}

/* If we know we got an entirely different data set from our master
 * we have no way to incrementally feed our replicas after that.
 * We want our replicas to resync with us as well, if we have any sub-replicas.
 * This is useful on readSyncBulkPayload in places where we just finished transferring db. */
void replicationAttachToNewMaster() { 
    /* Replica starts to apply data from new master, we must discard the cached
     * master structure. */
    serverAssert(server.master == NULL);
    replicationDiscardCachedMaster();

    disconnectSlaves(); /* Force our replicas to resync with us as well. */
    freeReplicationBacklog(); /* Don't allow our chained replicas to PSYNC. */
}

/* Asynchronously read the SYNC payload we receive from a master */
/* 异步地接收并加载主节点发来的 RDB 文件 */
#define REPL_MAX_WRITTEN_BEFORE_FSYNC (1024*1024*8) /* 8 MB */
void readSyncBulkPayload(connection *conn) {
    char buf[PROTO_IOBUF_LEN];
    ssize_t nread, readlen, nwritten;
    int use_diskless_load = useDisklessLoad();
    redisDb *diskless_load_tempDb = NULL;
    functionsLibCtx* temp_functions_lib_ctx = NULL;
    int empty_db_flags = server.repl_slave_lazy_flush ? EMPTYDB_ASYNC :
                                                        EMPTYDB_NO_FLAGS;
    off_t left;

    /* There are two possible forms for the bulk payload. One is the
     * usual $<count> bulk format. The other is used for diskless transfers
     * when the master does not know beforehand the size of the file to
     * transfer. In the latter case, the following format is used:
     *
     * $EOF:<40 bytes delimiter>
     *
     * At the end of the file the announced delimiter is transmitted. The
     * delimiter is long and random enough that the probability of a
     * collision with the actual file content can be ignored. */
    /* 主节点发来的 RDB 文件有两种格式，一种是正常的 bulk string 格式 $<count>, count 是正文的长度，
     * 另外一种是采用无盘传输时的格式。
     * 使用无盘传输时主节点在发送 RDB 之前并不知道 RDB 文件的大小，所以使用下面的格式：
     * 
     * $EOF:<40 个字节长的字符串>
     * 
     * 主节点先传输 `$EOF:<EOF 标志>` 然后传输 RDB 文件，最后会再传输一次 EOF 标志表示传输结束。
     * EOF 标志足够长（40 字节）和随机，使它与 RDB 文件内容冲突的概率可以忽略不计。
     */

    /* Static vars used to hold the EOF mark, and the last bytes received
     * from the server: when they match, we reached the end of the transfer. */
    /* 这些静态变量用于存储40个字节长的 EOF 标志以及从主节点收到最后40个字符，如果收到的最后一段字符串与 
     * EOF 标志匹配说明我们已经收到完整的 RDB 文件。
     * 
     * readSyncBulkPayload 会作为连接的 read handler 被多次回调，所以需要使用静态变量来保存之前回调中获得的 EOF Mark。
     */
    static char eofmark[CONFIG_RUN_ID_SIZE];
    static char lastbytes[CONFIG_RUN_ID_SIZE];
    static int usemark = 0; /* usemark == 1 表示使用 EOF Mark 即使用无盘传输模式 */

    /* If repl_transfer_size == -1 we still have to read the bulk length
     * from the master reply. */
    /* repl_transfer_size == -1 说明还没有收到 RDB 文件的长度，先尝试读取长度 */
    if (server.repl_transfer_size == -1) {
        nread = connSyncReadLine(conn,buf,1024,server.repl_syncio_timeout*1000);
        if (nread == -1) {
            serverLog(LL_WARNING,
                "I/O error reading bulk count from MASTER: %s",
                strerror(errno));
            goto error;
        } else {
            /* nread here is returned by connSyncReadLine(), which calls syncReadLine() and
             * convert "\r\n" to '\0' so 1 byte is lost. */
            /* connSyncReadLine() 调用的 syncReadLine() 会将 "\r\n" 转换为 "\0", 
             * 所以 connSyncReadLine() 返回的 nread 会比收到的数据少 1 个字节 */
            atomicIncr(server.stat_net_repl_input_bytes, nread+1);
        }

        if (buf[0] == '-') {
            serverLog(LL_WARNING,
                "MASTER aborted replication with an error: %s",
                buf+1);
            goto error;
        } else if (buf[0] == '\0') {
            /* At this stage just a newline works as a PING in order to take
             * the connection live. So we refresh our last interaction
             * timestamp. */
            /* 只收到了一个用来保持连接活跃的空行，更新最近交互时间 */
            server.repl_transfer_lastio = server.unixtime;
            return;
        } else if (buf[0] != '$') {
            serverLog(LL_WARNING,"Bad protocol from MASTER, the first byte is not '$' (we received '%s'), are you sure the host and port are right?", buf);
            goto error;
        }

        if (strncmp(buf+1,"EOF:",4) == 0 && strlen(buf+5) >= CONFIG_RUN_ID_SIZE) {
            /* 使用无盘传输 */
            usemark = 1;
            memcpy(eofmark,buf+5,CONFIG_RUN_ID_SIZE); /* 记录 EOF 标志 */
            memset(lastbytes,0,CONFIG_RUN_ID_SIZE);
            /* Set any repl_transfer_size to avoid entering this code path
             * at the next call. */
            /* 随便设置 repl_transfer_size 避免下次回调时再次进入读取 RDB 长度的分支 */
            server.repl_transfer_size = 0;
            serverLog(LL_NOTICE,
                "MASTER <-> REPLICA sync: receiving streamed RDB from master with EOF %s",
                use_diskless_load? "to parser":"to disk");
        } else {
            /* 使用有盘传输，记录 RDB 文件长度 */
            usemark = 0;
            server.repl_transfer_size = strtol(buf+1,NULL,10);
            serverLog(LL_NOTICE,
                "MASTER <-> REPLICA sync: receiving %lld bytes from master %s",
                (long long) server.repl_transfer_size,
                use_diskless_load? "to parser":"to disk");
        }
        return;
    }

    if (!use_diskless_load) {
        /* 使用有盘加载 
         * 有盘加载是指将从主节点收到的 RDB 文件先存到磁盘上，注意与前面注释 eofmark 时提到的有盘传输不是同一个概念
         */

        /* Read the data from the socket, store it to a file and search
         * for the EOF. */
        /* 从 socket 中读取数据存入文件并判断是否到了文件结尾 */
        if (usemark) {
            readlen = sizeof(buf);
        } else {
            left = server.repl_transfer_size - server.repl_transfer_read;
            readlen = (left < (signed)sizeof(buf)) ? left : (signed)sizeof(buf);
        }

        nread = connRead(conn,buf,readlen);
        if (nread <= 0) {
            if (connGetState(conn) == CONN_STATE_CONNECTED) {
                /* equivalent to EAGAIN */
                return;
            }
            serverLog(LL_WARNING,"I/O error trying to sync with MASTER: %s",
                (nread == -1) ? strerror(errno) : "connection lost");
            cancelReplicationHandshake(1);
            return;
        }
        atomicIncr(server.stat_net_repl_input_bytes, nread);

        /* When a mark is used, we want to detect EOF asap in order to avoid
         * writing the EOF mark into the file... */
        /* 在使用无盘传输时我们希望尽早检测到 EOF 标志，以避免将 EOF 标志写入到文件中 */
        int eof_reached = 0;

        if (usemark) {
            /* 使用无磁盘传输 */

            /* Update the last bytes array, and check if it matches our
             * delimiter. */
            /* 更新收到的最后40个字符，并检查是否为 EOF 标志 */
            if (nread >= CONFIG_RUN_ID_SIZE) {
                memcpy(lastbytes,buf+nread-CONFIG_RUN_ID_SIZE,
                       CONFIG_RUN_ID_SIZE);
            } else {
                int rem = CONFIG_RUN_ID_SIZE-nread;
                memmove(lastbytes,lastbytes+nread,rem);
                memcpy(lastbytes+rem,buf,nread);
            }
            if (memcmp(lastbytes,eofmark,CONFIG_RUN_ID_SIZE) == 0)
                eof_reached = 1;
        }

        /* Update the last I/O time for the replication transfer (used in
         * order to detect timeouts during replication), and write what we
         * got from the socket to the dump file on disk. */
        /* 更新最后通信时间（用来检测同步过程中是否出现超时）并将收到的数据写入磁盘中 */
        server.repl_transfer_lastio = server.unixtime;
        if ((nwritten = write(server.repl_transfer_fd,buf,nread)) != nread) {
            serverLog(LL_WARNING,
                "Write error or short write writing to the DB dump file "
                "needed for MASTER <-> REPLICA synchronization: %s",
                (nwritten == -1) ? strerror(errno) : "short write");
            goto error;
        }
        server.repl_transfer_read += nread;

        /* Delete the last 40 bytes from the file if we reached EOF. */
        /* 读取到 EOF 标志之后将 40 个字节的 EOF 标志从文件中删除 */
        if (usemark && eof_reached) {
            if (ftruncate(server.repl_transfer_fd,
                server.repl_transfer_read - CONFIG_RUN_ID_SIZE) == -1)
            {
                serverLog(LL_WARNING,
                    "Error truncating the RDB file received from the master "
                    "for SYNC: %s", strerror(errno));
                goto error;
            }
        }

        /* Sync data on disk from time to time, otherwise at the end of the
         * transfer we may suffer a big delay as the memory buffers are copied
         * into the actual disk. */
        /* 不时调用 fsync 将缓冲区数据同步到磁盘上，否则在传输结束时统一写入磁盘会将我们阻塞很长时间 */
        if (server.repl_transfer_read >=
            server.repl_transfer_last_fsync_off + REPL_MAX_WRITTEN_BEFORE_FSYNC)
        {
            off_t sync_size = server.repl_transfer_read -
                              server.repl_transfer_last_fsync_off;
            rdb_fsync_range(server.repl_transfer_fd,
                server.repl_transfer_last_fsync_off, sync_size);
            server.repl_transfer_last_fsync_off += sync_size;
        }

        /* Check if the transfer is now complete */
        /* 检查传输是否完成 */
        if (!usemark) {
            if (server.repl_transfer_read == server.repl_transfer_size)
                eof_reached = 1;
        }

        /* If the transfer is yet not complete, we need to read more, so
         * return ASAP and wait for the handler to be called again. */
        /* 如果传输尚未完成我们需要更多数据，那么要尽快返回并等待下次回调 */
        if (!eof_reached) return;
    }

    /* We reach this point in one of the following cases:
     *
     * 1. The replica is using diskless replication, that is, it reads data
     *    directly from the socket to the Redis memory, without using
     *    a temporary RDB file on disk. In that case we just block and
     *    read everything from the socket.
     *
     * 2. Or when we are done reading from the socket to the RDB file, in
     *    such case we want just to read the RDB file in memory. */

    /* 执行到此处，说明我们处于下列几种状态之一： 
     * 1. 使用无磁盘加载，我们直接将数据从 socket 写入到内存不需要临时的 RDB 文件。
     * 这种情况下我们只需要阻塞并从 socket 读取数据即可。
     * 
     * 2. 使用有磁盘加载并且已经将数据写入了 RDB 文件（上面 if(!use_diskless_load) 代码块完成了将数据写入文件的操作 ）。
     * 这种情况下我们需要将 RDB 文件读入内存即可。
     */

    /* We need to stop any AOF rewriting child before flushing and parsing
     * the RDB, otherwise we'll create a copy-on-write disaster. */
    /* 在解析 RDB 并刷新内存前我们需要停止所有 AOF 重写子进程，
     * 否则由于 fork 调用的 copy-on-write 机制会出现灾难性的后果。
     */
    if (server.aof_state != AOF_OFF) stopAppendOnly();
    /* Also try to stop save RDB child before flushing and parsing the RDB:
     * 1. Ensure background save doesn't overwrite synced data after being loaded.
     * 2. Avoid copy-on-write disaster. */
    /* 在解析 RDB 并刷新内存前同样需要尝试停止保存 RDB 文件的子进程：
     * 1. 确保后台保存 RDB 的进程不会覆盖从主节点同步的数据 （此处存疑）
     * 2. 避免 copy-on-write 机制带来的灾难性后果
    */
    if (server.child_type == CHILD_TYPE_RDB) {
        if (!use_diskless_load) {
            serverLog(LL_NOTICE,
                "Replica is about to load the RDB file received from the "
                "master, but there is a pending RDB child running. "
                "Killing process %ld and removing its temp file to avoid "
                "any race",
                (long) server.child_pid);
        }
        killRDBChild();
    }

    /* 清空旧数据准备加载新的 RDB 文件 */
    if (use_diskless_load && server.repl_diskless_load == REPL_DISKLESS_LOAD_SWAPDB) {
        /* 使用无盘加载且选择了 SWAPDB 策略 
         * SWAPDB 策略会先在内存中拷贝一份当前内容，当成功加载主节点传来的 RDB 文件后再删除拷贝，若解析失败则从拷贝进行恢复 */

        /* Initialize empty tempDb dictionaries. */
        /* 初始化存储拷贝的临时数据库 */
        diskless_load_tempDb = disklessLoadInitTempDb();
        temp_functions_lib_ctx = functionsLibCtxCreate();

        moduleFireServerEvent(REDISMODULE_EVENT_REPL_ASYNC_LOAD,
                              REDISMODULE_SUBEVENT_REPL_ASYNC_LOAD_STARTED,
                              NULL);
    } else {
        /* 其它情况下直接清空数据库中旧数据 */
        replicationAttachToNewMaster();

        serverLog(LL_NOTICE, "MASTER <-> REPLICA sync: Flushing old data");
        emptyData(-1,empty_db_flags,replicationEmptyDbCallback);
    }

    /* Before loading the DB into memory we need to delete the readable
     * handler, otherwise it will get called recursively since
     * rdbLoad() will call the event loop to process events from time to
     * time for non blocking loading. */
    /* 在加载数据库之前删除读回调。因为 rdbLoad() 在非阻塞加载过程中会不时调用事件循环，
     * 若不删除则本函数会被递归调用 */
    connSetReadHandler(conn, NULL);
    
    /* 开始加载主节点发来的 RDB 文件 */
    serverLog(LL_NOTICE, "MASTER <-> REPLICA sync: Loading DB in memory");
    rdbSaveInfo rsi = RDB_SAVE_INFO_INIT;
    if (use_diskless_load) {
        rio rdb;
        redisDb *dbarray;
        functionsLibCtx* functions_lib_ctx;
        int asyncLoading = 0;

        if (server.repl_diskless_load == REPL_DISKLESS_LOAD_SWAPDB) {
            /* Async loading means we continue serving read commands during full resync, and
             * "swap" the new db with the old db only when loading is done.
             * It is enabled only on SWAPDB diskless replication when master replication ID hasn't changed,
             * because in that state the old content of the db represents a different point in time of the same
             * data set we're currently receiving from the master. */
            /* 异步加载意味着在执行全量同步的过程中我们可以继续处理读命令，并且只有在加载完成时才会使用新的数据库代替旧数据库。
             * 异步加载只有使用 DISKLESS_LOAD_SWAPDB 策略且主节点的 replid 没有改变时才会启用，因为此时我们和主节点使用同一个数据集，只是我们落后于主节点有一些数据尚未收到 */
            if (memcmp(server.replid, server.master_replid, CONFIG_RUN_ID_SIZE) == 0) {
                asyncLoading = 1;
            }
            dbarray = diskless_load_tempDb;
            functions_lib_ctx = temp_functions_lib_ctx;
        } else {
            dbarray = server.db;
            functions_lib_ctx = functionsLibCtxGetCurrent();
            functionsLibCtxClear(functions_lib_ctx);
        }

        rioInitWithConn(&rdb,conn,server.repl_transfer_size);

        /* Put the socket in blocking mode to simplify RDB transfer.
         * We'll restore it when the RDB is received. */
        /* 将 socket 改为阻塞 IO 来简化传输 RDB 的过程，在 RDB 传输完成后将连接恢复为非阻塞 */
        connBlock(conn);
        connRecvTimeout(conn, server.repl_timeout*1000);
        /* 将状态修改为加载 RDB 中 */
        startLoading(server.repl_transfer_size, RDBFLAGS_REPLICATION, asyncLoading);

        int loadingFailed = 0;
        rdbLoadingCtx loadingCtx = { .dbarray = dbarray, .functions_lib_ctx = functions_lib_ctx };
        /* 从连接中读取 RDB 文件，数据加载到 loadingCtx 中 */
        if (rdbLoadRioWithLoadingCtx(&rdb,RDBFLAGS_REPLICATION,&rsi,&loadingCtx) != C_OK) {
            /* RDB loading failed. */
            /* 加载失败 */
            serverLog(LL_WARNING,
                      "Failed trying to load the MASTER synchronization DB "
                      "from socket: %s", strerror(errno));
            loadingFailed = 1;
        } else if (usemark) {
            /* 加载成功且使用无盘传输，需要校验 EOF 标志 */
            /* Verify the end mark is correct. */
            if (!rioRead(&rdb, buf, CONFIG_RUN_ID_SIZE) ||
                memcmp(buf, eofmark, CONFIG_RUN_ID_SIZE) != 0)
            {
                serverLog(LL_WARNING, "Replication stream EOF marker is broken");
                loadingFailed = 1;
            }
        }

        /* 若加载失败， 需要进行清理并恢复数据 */
        if (loadingFailed) {
            stopLoading(0);
            cancelReplicationHandshake(1);
            rioFreeConn(&rdb, NULL);

            if (server.repl_diskless_load == REPL_DISKLESS_LOAD_SWAPDB) {
                /* Discard potentially partially loaded tempDb. */
                /* 丢弃只加载了部分数据的临时数据库 */
                moduleFireServerEvent(REDISMODULE_EVENT_REPL_ASYNC_LOAD,
                                      REDISMODULE_SUBEVENT_REPL_ASYNC_LOAD_ABORTED,
                                      NULL);

                disklessLoadDiscardTempDb(diskless_load_tempDb);
                functionsLibCtxFree(temp_functions_lib_ctx);
                serverLog(LL_NOTICE, "MASTER <-> REPLICA sync: Discarding temporary DB in background");
            } else {
                /* Remove the half-loaded data in case we started with an empty replica. */
                emptyData(-1,empty_db_flags,replicationEmptyDbCallback);
            }

            /* Note that there's no point in restarting the AOF on SYNC
             * failure, it'll be restarted when sync succeeds or the replica
             * gets promoted. */
            /* 注意在同步失败时重启 AOF 是没有意义的。我们需要在同步成功或者副本被提升为主节点后载重启 AOF */
            return;
        }

        /* RDB loading succeeded if we reach this point. */
        /* 代码执行到此处说明 RDB 文件已经加载完成 */
        if (server.repl_diskless_load == REPL_DISKLESS_LOAD_SWAPDB) {
            /* 进行切换数据库操作 */

            /* We will soon swap main db with tempDb and replicas will start
             * to apply data from new master, we must discard the cached
             * master structure and force resync of sub-replicas. */
            /* 使用加载了新数据的 tempDB 替换主数据库。我们必须丢掉 cached_master
             * 并且强制我们的次级子节点重新同步 */
            replicationAttachToNewMaster();

            serverLog(LL_NOTICE, "MASTER <-> REPLICA sync: Swapping active DB with loaded DB");
            swapMainDbWithTempDb(diskless_load_tempDb);

            /* swap existing functions ctx with the temporary one */
            functionsLibCtxSwapWithCurrent(temp_functions_lib_ctx);

            moduleFireServerEvent(REDISMODULE_EVENT_REPL_ASYNC_LOAD,
                        REDISMODULE_SUBEVENT_REPL_ASYNC_LOAD_COMPLETED,
                        NULL);

            /* Delete the old db as it's useless now. */
            /* 删除已经无用的旧数据库 */
            disklessLoadDiscardTempDb(diskless_load_tempDb);
            serverLog(LL_NOTICE, "MASTER <-> REPLICA sync: Discarding old DB in background");
        }

        /* Inform about db change, as replication was diskless and didn't cause a save. */
        /* 因为无盘加载不会更新 RDB 文件，通知数据库已经改变（用于判断是否要重新保存 RDB 文件）*/
        server.dirty++;

        stopLoading(1);

        /* Cleanup and restore the socket to the original state to continue
         * with the normal replication. */
        /* 清理并恢复 socket 状态（恢复为 Non-Blocking IO）, 并且继续接收后续的同步数据 */
        rioFreeConn(&rdb, NULL);
        connNonBlock(conn);
        connRecvTimeout(conn,0);
    } else {
        /* 从 RDB 临时文件加载数据 */
        /* Make sure the new file (also used for persistence) is fully synced
         * (not covered by earlier calls to rdb_fsync_range). */
        /* 确保 RDB 文件已经完成了 fsync */
        if (fsync(server.repl_transfer_fd) == -1) {
            serverLog(LL_WARNING,
                "Failed trying to sync the temp DB to disk in "
                "MASTER <-> REPLICA synchronization: %s",
                strerror(errno));
            cancelReplicationHandshake(1);
            return;
        }

        /* Rename rdb like renaming rewrite aof asynchronously. */
        /* 重命名 RDB 文件，用从主节点接收到 RDB 文件代替当前实例的 RDB 文件 */
        int old_rdb_fd = open(server.rdb_filename,O_RDONLY|O_NONBLOCK);
        if (rename(server.repl_transfer_tmpfile,server.rdb_filename) == -1) {
            serverLog(LL_WARNING,
                "Failed trying to rename the temp DB into %s in "
                "MASTER <-> REPLICA synchronization: %s",
                server.rdb_filename, strerror(errno));
            cancelReplicationHandshake(1);
            if (old_rdb_fd != -1) close(old_rdb_fd);
            return;
        }
        /* Close old rdb asynchronously. */
        /* 异步关闭旧 rdb 文件句柄 */
        if (old_rdb_fd != -1) bioCreateCloseJob(old_rdb_fd, 0);

        /* Sync the directory to ensure rename is persisted */
        if (fsyncFileDir(server.rdb_filename) == -1) {
            serverLog(LL_WARNING,
                "Failed trying to sync DB directory %s in "
                "MASTER <-> REPLICA synchronization: %s",
                server.rdb_filename, strerror(errno));
            cancelReplicationHandshake(1);
            return;
        }

        /* 加载 RDB 文件 */
        if (rdbLoad(server.rdb_filename,&rsi,RDBFLAGS_REPLICATION) != C_OK) {
            serverLog(LL_WARNING,
                "Failed trying to load the MASTER synchronization "
                "DB from disk: %s", strerror(errno));
            cancelReplicationHandshake(1);
            if (server.rdb_del_sync_files && allPersistenceDisabled()) {
                serverLog(LL_NOTICE,"Removing the RDB file obtained from "
                                    "the master. This replica has persistence "
                                    "disabled");
                bg_unlink(server.rdb_filename);
            }
            /* Note that there's no point in restarting the AOF on sync failure,
               it'll be restarted when sync succeeds or replica promoted. */
            /* 注意在同步失败时重启 AOF 是没有意义的。我们需要在同步成功或者副本被提升为主节点后载重启 AOF */
            return;
        }

        /* Cleanup. */
        /* 清理各种临时文件和fd */
        if (server.rdb_del_sync_files && allPersistenceDisabled()) {
            serverLog(LL_NOTICE,"Removing the RDB file obtained from "
                                "the master. This replica has persistence "
                                "disabled");
            bg_unlink(server.rdb_filename);
        }

        zfree(server.repl_transfer_tmpfile);
        close(server.repl_transfer_fd);
        server.repl_transfer_fd = -1;
        server.repl_transfer_tmpfile = NULL;
    }

    /* Final setup of the connected slave <- master link */
    /* 最后设置 server.master 保存与主节点之间的链接 */
    replicationCreateMasterClient(server.repl_transfer_s,rsi.repl_stream_db);
    server.repl_state = REPL_STATE_CONNECTED;
    server.repl_down_since = 0;

    /* Fire the master link modules event. */
    /* 发送与主节点连接相关的事件 */
    moduleFireServerEvent(REDISMODULE_EVENT_MASTER_LINK_CHANGE,
                          REDISMODULE_SUBEVENT_MASTER_LINK_UP,
                          NULL);

    /* After a full resynchronization we use the replication ID and
     * offset of the master. The secondary ID / offset are cleared since
     * we are starting a new history. */
    /* 在执行全量同步后将主节点的 replid 和偏移量作为自己的 replid 和偏移量。
     * 并清空 replid2 和对应的偏移量。 replid2 的作用可以参考 server.h 中 redisServer 中 
     * replid2 字段注释 
     */
    memcpy(server.replid,server.master->replid,sizeof(server.replid));
    server.master_repl_offset = server.master->reploff;
    clearReplicationId2();

    /* Let's create the replication backlog if needed. Slaves need to
     * accumulate the backlog regardless of the fact they have sub-slaves
     * or not, in order to behave correctly if they are promoted to
     * masters after a failover. */
    /* 创建复制积压缓冲区。 作为子节点不管我们是否有次级子节点我们都需要积累复制积压数据，
     * 以便在发生故障后被提升为主节点后可以正常支持复制 */
    if (server.repl_backlog == NULL) createReplicationBacklog();
    serverLog(LL_NOTICE, "MASTER <-> REPLICA sync: Finished with success");

    if (server.supervised_mode == SUPERVISED_SYSTEMD) {
        redisCommunicateSystemd("STATUS=MASTER <-> REPLICA sync: Finished with success. Ready to accept connections in read-write mode.\n");
    }

    /* Send the initial ACK immediately to put this replica in online state. */
    /* 立即向主节点发送 REPLCONF ACK 命令。一方面要求主节点发送我们在全量同步过程中新产生的数据
     * 另一方面更新我们在主节点侧的延迟状态 */
    if (usemark) replicationSendAck();

    /* Restart the AOF subsystem now that we finished the sync. This
     * will trigger an AOF rewrite, and when done will start appending
     * to the new file. */
    /* 既然我们已经完成同步那么可以重启 AOF 功能。我们首先会进行一次 AOF 重写，
     * 在重写结束后就可以正常地把命令追加到新文件上*/
    if (server.aof_enabled) restartAOFAfterSYNC();
    return;

error:
    cancelReplicationHandshake(1);
    return;
}

char *receiveSynchronousResponse(connection *conn) {
    char buf[256];
    /* Read the reply from the server. */
    if (connSyncReadLine(conn,buf,sizeof(buf),server.repl_syncio_timeout*1000) == -1)
    {
        serverLog(LL_WARNING, "Failed to read response from the server: %s", strerror(errno));
        return NULL;
    }
    server.repl_transfer_lastio = server.unixtime;
    return sdsnew(buf);
}

/* Send a pre-formatted multi-bulk command to the connection. */
char* sendCommandRaw(connection *conn, sds cmd) {
    if (connSyncWrite(conn,cmd,sdslen(cmd),server.repl_syncio_timeout*1000) == -1) {
        return sdscatprintf(sdsempty(),"-Writing to master: %s",
                connGetLastError(conn));
    }
    return NULL;
}

/* Compose a multi-bulk command and send it to the connection.
 * Used to send AUTH and REPLCONF commands to the master before starting the
 * replication.
 *
 * Takes a list of char* arguments, terminated by a NULL argument.
 *
 * The command returns an sds string representing the result of the
 * operation. On error the first byte is a "-".
 */
char *sendCommand(connection *conn, ...) {
    va_list ap;
    sds cmd = sdsempty();
    sds cmdargs = sdsempty();
    size_t argslen = 0;
    char *arg;

    /* Create the command to send to the master, we use redis binary
     * protocol to make sure correct arguments are sent. This function
     * is not safe for all binary data. */
    va_start(ap,conn);
    while(1) {
        arg = va_arg(ap, char*);
        if (arg == NULL) break;
        cmdargs = sdscatprintf(cmdargs,"$%zu\r\n%s\r\n",strlen(arg),arg);
        argslen++;
    }

    cmd = sdscatprintf(cmd,"*%zu\r\n",argslen);
    cmd = sdscatsds(cmd,cmdargs);
    sdsfree(cmdargs);

    va_end(ap);
    char* err = sendCommandRaw(conn, cmd);
    sdsfree(cmd);
    if(err)
        return err;
    return NULL;
}

/* Compose a multi-bulk command and send it to the connection. 
 * Used to send AUTH and REPLCONF commands to the master before starting the
 * replication.
 *
 * argv_lens is optional, when NULL, strlen is used.
 *
 * The command returns an sds string representing the result of the
 * operation. On error the first byte is a "-".
 */
char *sendCommandArgv(connection *conn, int argc, char **argv, size_t *argv_lens) {
    sds cmd = sdsempty();
    char *arg;
    int i;

    /* Create the command to send to the master. */
    cmd = sdscatfmt(cmd,"*%i\r\n",argc);
    for (i=0; i<argc; i++) {
        int len;
        arg = argv[i];
        len = argv_lens ? argv_lens[i] : strlen(arg);
        cmd = sdscatfmt(cmd,"$%i\r\n",len);
        cmd = sdscatlen(cmd,arg,len);
        cmd = sdscatlen(cmd,"\r\n",2);
    }
    char* err = sendCommandRaw(conn, cmd);
    sdsfree(cmd);
    if (err)
        return err;
    return NULL;
}

/* Try a partial resynchronization with the master if we are about to reconnect.
 * If there is no cached master structure, at least try to issue a
 * "PSYNC ? -1" command in order to trigger a full resync using the PSYNC
 * command in order to obtain the master replid and the master replication
 * global offset.
 *
 * This function is designed to be called from syncWithMaster(), so the
 * following assumptions are made:
 *
 * 1) We pass the function an already connected socket "fd".
 * 2) This function does not close the file descriptor "fd". However in case
 *    of successful partial resynchronization, the function will reuse
 *    'fd' as file descriptor of the server.master client structure.
 *
 * The function is split in two halves: if read_reply is 0, the function
 * writes the PSYNC command on the socket, and a new function call is
 * needed, with read_reply set to 1, in order to read the reply of the
 * command. This is useful in order to support non blocking operations, so
 * that we write, return into the event loop, and read when there are data.
 *
 * When read_reply is 0 the function returns PSYNC_WRITE_ERR if there
 * was a write error, or PSYNC_WAIT_REPLY to signal we need another call
 * with read_reply set to 1. However even when read_reply is set to 1
 * the function may return PSYNC_WAIT_REPLY again to signal there were
 * insufficient data to read to complete its work. We should re-enter
 * into the event loop and wait in such a case.
 *
 * The function returns:
 *
 * PSYNC_CONTINUE: If the PSYNC command succeeded and we can continue.
 * PSYNC_FULLRESYNC: If PSYNC is supported but a full resync is needed.
 *                   In this case the master replid and global replication
 *                   offset is saved.
 * PSYNC_NOT_SUPPORTED: If the server does not understand PSYNC at all and
 *                      the caller should fall back to SYNC.
 * PSYNC_WRITE_ERROR: There was an error writing the command to the socket.
 * PSYNC_WAIT_REPLY: Call again the function with read_reply set to 1.
 * PSYNC_TRY_LATER: Master is currently in a transient error condition.
 *
 * Notable side effects:
 *
 * 1) As a side effect of the function call the function removes the readable
 *    event handler from "fd", unless the return value is PSYNC_WAIT_REPLY.
 * 2) server.master_initial_offset is set to the right value according
 *    to the master reply. This will be used to populate the 'server.master'
 *    structure replication offset.
 */

/* 尝试与主节点进行部分同步。
 * 如果没有 cached_master 结构体则发送 PSYNC ? -1 命令尝试进行全量同步
 * 并获得主节点的 replid 以及全局复制偏移量。
 * 
 * 这个函数设计上只会被 syncWithMaster() 调用，所以我们做出如下约定：
 * 
 *  1) 我们给这个函数传递一个已经连接的 socket 的 fd
 *  2) 这个函数不会关闭 fd。 在成功进行部分同步时，这个函数会重用 fd 作为 server.master 中的文件描述符。
 * 
 * 这个函数分为两个部分：如果 read_reply 为 0，它会通过 socket 发送一个 PSYNC 命令。
 * 此时需要将 read_reply 设为 1 并再调用一次这个函数来接收 PSYNC 命令的响应。 
 * 这样设计是为了支持非阻塞操作：发送命令 -> 返回 event loop -> 收到数据后进行读取。
 * 
 * 在 read_reply 为 0 时，本函数返回 PSYNC_WRITE_ERR 表示发送命令时遇到了错误，
 * 返回 PSYNC_WAIT_REPLY 表示发送成功。在返回 PSYNC_WAIT_REPLY 时
 * 需要将 read_reply 设为 1 并再调用一次这个函数来接收响应。即使 read_reply 为 1 这个函数也可能返回 PSYNC_WAIT_REPLY 
 * 表示只收到了部分数据，此时我们返回 event loop 并等待后续数据。
 * 
 * 函数返回值：
 * PSYNC_CONTINUE： PSYNC 命令成功完成，我们可以继续
 * PSYNC_FULLRESYNC：主节点支持 PSYNC 但是此时需要进行一次全量同步。这种情况下已经我们存储了主节点的 replid 和复制偏移量。
 * PSYNC_NOT_SUPPORTED：主节点不支持 PSYNC 需要回退到 SYNC
 * PSYNC_WRITE_ERROR: 通过 socket 发送命令时遇到了错误
 * PSYNC_WAIT_REPLY： 发送成功，需要将 read_reply 设为 1 再调用一次来处理 PSYNC 命令的响应
 * PSYNC_TRY_LATER： 主节点暂时故障
 * 
 * 需要注意的副作用：
 * 1）除了返回 PSYNC_WAIT_REPLY 时，其它情况下本函数会移除 fd 上的可读事件
 * 2）将根据主节点的返回值设置 server.master_initial_offset 字段
 */

#define PSYNC_WRITE_ERROR 0
#define PSYNC_WAIT_REPLY 1
#define PSYNC_CONTINUE 2
#define PSYNC_FULLRESYNC 3
#define PSYNC_NOT_SUPPORTED 4
#define PSYNC_TRY_LATER 5
int slaveTryPartialResynchronization(connection *conn, int read_reply) {
    char *psync_replid;
    char psync_offset[32];
    sds reply;

    /* Writing half */
    /* 发送命令部分 */
    if (!read_reply) {
        /* Initially set master_initial_offset to -1 to mark the current
         * master replid and offset as not valid. Later if we'll be able to do
         * a FULL resync using the PSYNC command we'll set the offset at the
         * right value, so that this information will be propagated to the
         * client structure representing the master into server.master. */
        /* 首先将 master_initial_offset 设为 -1 表示现在主节点的 replid 和复制偏移量不可用。
         * 我们使用 PSYNC 完成全量同步后再将复制偏移量设为正确的值，以便将这些信息传递到
         * 代表主服务器的 server.master 结构中
         */
        server.master_initial_offset = -1;

        /* 与原主节点重连时 cached_master != null, 连接新的主节点时 cached_master 为空 */
        if (server.cached_master) { 
            /* 重连，尝试进行部分同步 */
            psync_replid = server.cached_master->replid;
            snprintf(psync_offset,sizeof(psync_offset),"%lld", server.cached_master->reploff+1);
            serverLog(LL_NOTICE,"Trying a partial resynchronization (request %s:%s).", psync_replid, psync_offset);
        } else {
            /* 使用 `PSYNC ? -1` 命令连接新的主节点 */
            serverLog(LL_NOTICE,"Partial resynchronization not possible (no cached master)");
            psync_replid = "?";
            memcpy(psync_offset,"-1",3);
        }

        /* Issue the PSYNC command, if this is a master with a failover in
         * progress then send the failover argument to the replica to cause it
         * to become a master */
        /* 如果主节点正在进行故障转移, 则向副本发送 FAILOVER 参数使它成为主节点。FAILOVER 参数作用请看 syncCommand() */
        if (server.failover_state == FAILOVER_IN_PROGRESS) {
            reply = sendCommand(conn,"PSYNC",psync_replid,psync_offset,"FAILOVER",NULL);
        } else {
            reply = sendCommand(conn,"PSYNC",psync_replid,psync_offset,NULL);
        }

        if (reply != NULL) {
            serverLog(LL_WARNING,"Unable to send PSYNC to master: %s",reply);
            sdsfree(reply);
            connSetReadHandler(conn, NULL);
            return PSYNC_WRITE_ERROR;
        }
        return PSYNC_WAIT_REPLY;
    }

    /* Reading half */
    /* 接收 PSYNC 命令响应 */
    reply = receiveSynchronousResponse(conn);
    /* Master did not reply to PSYNC */
    if (reply == NULL) {
        connSetReadHandler(conn, NULL);
        serverLog(LL_WARNING, "Master did not reply to PSYNC, will try later");
        return PSYNC_TRY_LATER;
    }

    if (sdslen(reply) == 0) {
        /* The master may send empty newlines after it receives PSYNC
         * and before to reply, just to keep the connection alive. */
        /* 主节点可能为了保持连接存活而发送了一些换行符 */
        sdsfree(reply);
        return PSYNC_WAIT_REPLY;
    }

    connSetReadHandler(conn, NULL);

    if (!strncmp(reply,"+FULLRESYNC",11)) {
        /* 返回 +FULLRESYNC 进行全量同步 */
        char *replid = NULL, *offset = NULL;

        /* FULL RESYNC, parse the reply in order to extract the replid
         * and the replication offset. */
        /* 解析返回值获得 replid 和复制偏移量 */
        replid = strchr(reply,' ');
        if (replid) {
            replid++;
            offset = strchr(replid,' ');
            if (offset) offset++;
        }
        if (!replid || !offset || (offset-replid-1) != CONFIG_RUN_ID_SIZE) {
            serverLog(LL_WARNING,
                "Master replied with wrong +FULLRESYNC syntax.");
            /* This is an unexpected condition, actually the +FULLRESYNC
             * reply means that the master supports PSYNC, but the reply
             * format seems wrong. To stay safe we blank the master
             * replid to make sure next PSYNCs will fail. */
            /* 这是一个意外情况，实际上返回 +FULLRESYNC 表示主节点支持 PSYNC 命令，但是返回的结果却不正确。
             * 为了保证安全我们将 replid 清空，确保之后所有 PSYNC 命令都会失败*/
            memset(server.master_replid,0,CONFIG_RUN_ID_SIZE+1); /* server.master_replid 是 C 风格字符串，以 \0 结尾，所以 size 要加1 */
        } else {
            /* 将 replid 和 offset 存下来 */
            memcpy(server.master_replid, replid, offset-replid-1);
            server.master_replid[CONFIG_RUN_ID_SIZE] = '\0';
            server.master_initial_offset = strtoll(offset,NULL,10);
            serverLog(LL_NOTICE,"Full resync from master: %s:%lld",
                server.master_replid,
                server.master_initial_offset);
        }
        sdsfree(reply);
        return PSYNC_FULLRESYNC;
    }

    if (!strncmp(reply,"+CONTINUE",9)) {
        /* Partial resync was accepted. */
        /* 进行部分同步 */
        serverLog(LL_NOTICE,
            "Successful partial resynchronization with master.");

        /* Check the new replication ID advertised by the master. If it
         * changed, we need to set the new ID as primary ID, and set
         * secondary ID as the old master ID up to the current offset, so
         * that our sub-slaves will be able to PSYNC with us after a
         * disconnection. */
        /* 检查主节点发来的新 replid。如果它改变了我们需要将新 ID 设为主 ID(server.replid), 
         * 原来的 ID 设为次要 ID(server.replid2)，当前的复制偏移量存入 second_replid_offset，
         * 这样我们的次级子节点在重连我们时就可以进行部分同步了
         */
        char *start = reply+10;
        char *end = reply+9;
        while(end[0] != '\r' && end[0] != '\n' && end[0] != '\0') end++;
        if (end-start == CONFIG_RUN_ID_SIZE) {
            char new[CONFIG_RUN_ID_SIZE+1];
            memcpy(new,start,CONFIG_RUN_ID_SIZE);
            new[CONFIG_RUN_ID_SIZE] = '\0';

            if (strcmp(new,server.cached_master->replid)) {
                /* Master ID changed. */
                /* 主节点的复制 ID 改变了 */
                serverLog(LL_WARNING,"Master replication ID changed to %s",new);

                /* Set the old ID as our ID2, up to the current offset+1. */
                /* 主节点的旧 ID 设为 ID2, 最大偏移量为当前偏移量+1 */
                memcpy(server.replid2,server.cached_master->replid,
                    sizeof(server.replid2));
                server.second_replid_offset = server.master_repl_offset+1;

                /* Update the cached master ID and our own primary ID to the
                 * new one. */
                /* 将 cached_master 中的 replid 和我们主复制 ID 更新为新的 */
                memcpy(server.replid,new,sizeof(server.replid));
                memcpy(server.cached_master->replid,new,sizeof(server.replid));

                /* Disconnect all the sub-slaves: they need to be notified. */
                /* 与所有次级子节点断开连接，并通知他们。次级子节点可能需要重新进行全量同步 */
                disconnectSlaves();
            }
        }

        /* Setup the replication to continue. */
        sdsfree(reply);
        replicationResurrectCachedMaster(conn);

        /* If this instance was restarted and we read the metadata to
         * PSYNC from the persistence file, our replication backlog could
         * be still not initialized. Create it. */
        /*
         * 如果当前实例重启并且我们在持久化文件中读到了 PSYNC, 此时复制积压缓冲区可能是空的需要创建
         */
        if (server.repl_backlog == NULL) createReplicationBacklog();
        return PSYNC_CONTINUE;
    }

    /* If we reach this point we received either an error (since the master does
     * not understand PSYNC or because it is in a special state and cannot
     * serve our request), or an unexpected reply from the master.
     *
     * Return PSYNC_NOT_SUPPORTED on errors we don't understand, otherwise
     * return PSYNC_TRY_LATER if we believe this is a transient error. */
    /* 如果代码执行到这里我们要么遇到了一个错误要么主节点返回了意外的响应。
     * 我们可能遇到的错误可能是主节点无法理解 PSYNC 命令或者主节点暂时无法处理我们的请求。
     * 
     * 返回 PSYNC_NOT_SUPPORTED 表示主节点无法理解 PSYNC, 
     * 返回 PSYNC_TRY_LATER 表示主节点暂时故障
     */
    if (!strncmp(reply,"-NOMASTERLINK",13) ||
        !strncmp(reply,"-LOADING",8))
    {
        serverLog(LL_NOTICE,
            "Master is currently unable to PSYNC "
            "but should be in the future: %s", reply);
        sdsfree(reply);
        return PSYNC_TRY_LATER;
    }

    if (strncmp(reply,"-ERR",4)) {
        /* 返回的不是错误响应，记录这个意外事件 */
        /* If it's not an error, log the unexpected event. */
        serverLog(LL_WARNING,
            "Unexpected reply to PSYNC from master: %s", reply);
    } else {
        /* 主节点不支持 PSYNC 或者返回其它错误 */
        serverLog(LL_NOTICE,
            "Master does not support PSYNC or is in "
            "error state (reply: %s)", reply);
    }
    sdsfree(reply);
    return PSYNC_NOT_SUPPORTED;
}

/* This handler fires when the non blocking connect was able to
 * establish a connection with the master. */
/* 当与主节点建立非阻塞连接后，本函数会被调用 
 * 本函数会负责与主节点完成同步参数的协商（鉴权、是否使用无盘同步、是否使用部分同步、接收 replid 等），
 * 并在全量同步时异步下载主节点发来的 RDB 文件 */
void syncWithMaster(connection *conn) {
    char tmpfile[256], *err = NULL;
    int dfd = -1, maxtries = 5;
    int psync_result;

    /* If this event fired after the user turned the instance into a master
     * with SLAVEOF NO ONE we must just return ASAP. */
    /* 如果用户使用了 SLAVEOF NO ONE 来将当前实例转变为主节点后本函数被回调，我们要尽快返回 */
    if (server.repl_state == REPL_STATE_NONE) {
        connClose(conn);
        return;
    }

    /* Check for errors in the socket: after a non blocking connect() we
     * may find that the socket is in error state. */
    /* 在调用非阻塞的 connect() 之后 socket 可能处于错误状态，检查 socket 的报错 */
    if (connGetState(conn) != CONN_STATE_CONNECTED) {
        serverLog(LL_WARNING,"Error condition on socket for SYNC: %s",
                connGetLastError(conn));
        goto error;
    }

    /* Send a PING to check the master is able to reply without errors. */
    /* 向主节点发送一个 PING 消息，检查主节点是否能够正常响应 */
    if (server.repl_state == REPL_STATE_CONNECTING) {
        serverLog(LL_NOTICE,"Non blocking connect for SYNC fired the event.");
        /* Delete the writable event so that the readable event remains
         * registered and we can wait for the PONG reply. */
        /* 清除连接上的可写事件保留可读事件，这样我们可以等待收到 PONG 响应再次回调本函数 */
        connSetReadHandler(conn, syncWithMaster);
        connSetWriteHandler(conn, NULL);
        server.repl_state = REPL_STATE_RECEIVE_PING_REPLY;
        /* Send the PING, don't check for errors at all, we have the timeout
         * that will take care about this. */
        /* 发送 PING 消息，不需要检查报错。我们的 timeout 机制会处理错误 */
        err = sendCommand(conn,"PING",NULL);
        if (err) goto write_error;
        return;
    }

    /* Receive the PONG command. */
    /* 收到 PONG 消息 */
    if (server.repl_state == REPL_STATE_RECEIVE_PING_REPLY) {
        err = receiveSynchronousResponse(conn);

        /* The master did not reply */
        if (err == NULL) goto no_response_error;

        /* We accept only two replies as valid, a positive +PONG reply
         * (we just check for "+") or an authentication error.
         * Note that older versions of Redis replied with "operation not
         * permitted" instead of using a proper error code, so we test
         * both. */
        /* 我们只接受两种响应：+PONG 或者鉴权失败。
         * 注意旧版的 Redis 会返回 operation not permitted 而不是正确的错误码
         */
        if (err[0] != '+' &&
            strncmp(err,"-NOAUTH",7) != 0 &&
            strncmp(err,"-NOPERM",7) != 0 &&
            strncmp(err,"-ERR operation not permitted",28) != 0)
        {
            serverLog(LL_WARNING,"Error reply to PING from master: '%s'",err);
            sdsfree(err);
            goto error;
        } else {
            serverLog(LL_NOTICE,
                "Master replied to PING, replication can continue...");
        }
        sdsfree(err);
        err = NULL;
        server.repl_state = REPL_STATE_SEND_HANDSHAKE;
    }

    if (server.repl_state == REPL_STATE_SEND_HANDSHAKE) {
        /* AUTH with the master if required. */
        /* 如果需要鉴权，则向主节点发送 AUTH 命令 */
        if (server.masterauth) {
            char *args[3] = {"AUTH",NULL,NULL};
            size_t lens[3] = {4,0,0};
            int argc = 1;
            if (server.masteruser) {
                args[argc] = server.masteruser;
                lens[argc] = strlen(server.masteruser);
                argc++;
            }
            args[argc] = server.masterauth;
            lens[argc] = sdslen(server.masterauth);
            argc++;
            err = sendCommandArgv(conn, argc, args, lens);
            if (err) goto write_error;
        }

        /* Set the slave port, so that Master's INFO command can list the
         * slave listening port correctly. */
        /* 通过 REPLCONF 命令向主节点设置从节点的端口号，这样主节点的 INFO 命令才能给出正确从节点监听端口 */
        {
            int port;
            if (server.slave_announce_port)
                port = server.slave_announce_port;
            else if (server.tls_replication && server.tls_port)
                port = server.tls_port;
            else
                port = server.port;
            sds portstr = sdsfromlonglong(port);
            err = sendCommand(conn,"REPLCONF",
                    "listening-port",portstr, NULL);
            sdsfree(portstr);
            if (err) goto write_error;
        }

        /* Set the slave ip, so that Master's INFO command can list the
         * slave IP address port correctly in case of port forwarding or NAT.
         * Skip REPLCONF ip-address if there is no slave-announce-ip option set. */
        /* 设置从节点 ip，使主节点的 INFO 命令即使在从节点经过了 NAT 端口转发之后还可以正确列出从节点的地址。
         * 如果没有设置 slave-announce-ip 选项就跳过这个步骤
        */
        if (server.slave_announce_ip) {
            err = sendCommand(conn,"REPLCONF",
                    "ip-address",server.slave_announce_ip, NULL);
            if (err) goto write_error;
        }

        /* Inform the master of our (slave) capabilities.
         *
         * EOF: supports EOF-style RDB transfer for diskless replication.
         * PSYNC2: supports PSYNC v2, so understands +CONTINUE <new repl ID>.
         *
         * The master will ignore capabilities it does not understand. */
        /* 发送 REPLCONF capa 命令通知主节点我们（从节点）支持的能力
         * EOF: 支持无磁盘化主从复制
         * PSYNC2: 支持 PSYNC2, 部分复制使用 replid 代替 runid 作为判断依据
         * 
         * 主节点会忽略它不理解的能力
        */
        err = sendCommand(conn,"REPLCONF",
                "capa","eof","capa","psync2",NULL);
        if (err) goto write_error;

        server.repl_state = REPL_STATE_RECEIVE_AUTH_REPLY;
        return;
    }

    if (server.repl_state == REPL_STATE_RECEIVE_AUTH_REPLY && !server.masterauth)
        server.repl_state = REPL_STATE_RECEIVE_PORT_REPLY;

    /* Receive AUTH reply. */
    /* 接受 AUTH 命令的响应 */
    if (server.repl_state == REPL_STATE_RECEIVE_AUTH_REPLY) {
        err = receiveSynchronousResponse(conn);
        if (err == NULL) goto no_response_error;
        if (err[0] == '-') {
            serverLog(LL_WARNING,"Unable to AUTH to MASTER: %s",err);
            sdsfree(err);
            goto error;
        }
        sdsfree(err);
        err = NULL;
        server.repl_state = REPL_STATE_RECEIVE_PORT_REPLY;
        return;
    }

    /* Receive REPLCONF listening-port reply. */
    /* 接收 REPLCONF listening-port 命令的响应 */
    if (server.repl_state == REPL_STATE_RECEIVE_PORT_REPLY) {
        err = receiveSynchronousResponse(conn);
        if (err == NULL) goto no_response_error;
        /* Ignore the error if any, not all the Redis versions support
         * REPLCONF listening-port. */
        /* 因为有些主节点的 Redis 版本不支持 REPLCONF listening-port 命令，所以可以忽略主节点返回的错误 */
        if (err[0] == '-') {
            serverLog(LL_NOTICE,"(Non critical) Master does not understand "
                                "REPLCONF listening-port: %s", err);
        }
        sdsfree(err);
        server.repl_state = REPL_STATE_RECEIVE_IP_REPLY;
        return;
    }

    if (server.repl_state == REPL_STATE_RECEIVE_IP_REPLY && !server.slave_announce_ip)
        server.repl_state = REPL_STATE_RECEIVE_CAPA_REPLY;

    /* Receive REPLCONF ip-address reply. */
    /* 接受 REPLCONF ip-address 命令的响应 */
    if (server.repl_state == REPL_STATE_RECEIVE_IP_REPLY) {
        err = receiveSynchronousResponse(conn);
        if (err == NULL) goto no_response_error;
        /* Ignore the error if any, not all the Redis versions support
         * REPLCONF ip-address. */
        /* 如果主节点的版本不支持 REPLCONF ip-address 命令，则忽略返回的错误 */
        if (err[0] == '-') {
            serverLog(LL_NOTICE,"(Non critical) Master does not understand "
                                "REPLCONF ip-address: %s", err);
        }
        sdsfree(err);
        server.repl_state = REPL_STATE_RECEIVE_CAPA_REPLY;
        return;
    }

    /* Receive CAPA reply. */
    /* 接受 REPLCONF capa 命令的响应 */
    if (server.repl_state == REPL_STATE_RECEIVE_CAPA_REPLY) {
        err = receiveSynchronousResponse(conn);
        if (err == NULL) goto no_response_error;
        /* Ignore the error if any, not all the Redis versions support
         * REPLCONF capa. */
        /* 如果主节点的版本不支持 REPLCONF capa 命令，则忽略返回的错误 */
        if (err[0] == '-') {
            serverLog(LL_NOTICE,"(Non critical) Master does not understand "
                                  "REPLCONF capa: %s", err);
        }
        sdsfree(err);
        err = NULL;
        server.repl_state = REPL_STATE_SEND_PSYNC;
    }

    /* Try a partial resynchronization. If we don't have a cached master
     * slaveTryPartialResynchronization() will at least try to use PSYNC
     * to start a full resynchronization so that we get the master replid
     * and the global offset, to try a partial resync at the next
     * reconnection attempt. */
    /* 尝试进行部分同步
     * 如果 cached_master 为空（即未与主节点进行过同步），slaveTryPartialResynchronization() 会尝试使用
     * PSYNC 进行一次全量同步使得我们可以获得主节点的 replid 和复制偏移量，
     * 在下次连接断开尝试重连时就可以尝试部分同步了。
    */
    if (server.repl_state == REPL_STATE_SEND_PSYNC) {
        /* 发送 PSYNC 请求 */
        if (slaveTryPartialResynchronization(conn,0) == PSYNC_WRITE_ERROR) {
            err = sdsnew("Write error sending the PSYNC command.");
            abortFailover("Write error to failover target");
            goto write_error;
        }
        server.repl_state = REPL_STATE_RECEIVE_PSYNC_REPLY;
        return;
    }

    /* If reached this point, we should be in REPL_STATE_RECEIVE_PSYNC_REPLY. */
    /* 断言此时应处于 REPL_STATE_RECEIVE_PSYNC_REPLY 状态，即收到了主节点对 PSYNC 命令的响应 */
    if (server.repl_state != REPL_STATE_RECEIVE_PSYNC_REPLY) {
        serverLog(LL_WARNING,"syncWithMaster(): state machine error, "
                             "state should be RECEIVE_PSYNC but is %d",
                             server.repl_state);
        goto error;
    }

    /* 读取 PSYNC 命令返回值，并决定进行全量同步还是部分同步或者遇到了错误。
     * slaveTryPartialResynchronization 会更新 replid 和复制偏移量等信息，但不会实际接收数据 */
    psync_result = slaveTryPartialResynchronization(conn,1);
    if (psync_result == PSYNC_WAIT_REPLY) return; /* Try again later... */

    /* Check the status of the planned failover. We expect PSYNC_CONTINUE,
     * but there is nothing technically wrong with a full resync which
     * could happen in edge cases. */
    /* 检查是否有故障转移正在进行。
     * 我们期望可以进行部分同步，但在技术上讲在边缘情况下进行全量同步没有任何问题 */
    if (server.failover_state == FAILOVER_IN_PROGRESS) {
        if (psync_result == PSYNC_CONTINUE || psync_result == PSYNC_FULLRESYNC) {
            clearFailoverState();
        } else {
            abortFailover("Failover target rejected psync request");
            return;
        }
    }

    /* If the master is in an transient error, we should try to PSYNC
     * from scratch later, so go to the error path. This happens when
     * the server is loading the dataset or is not connected with its
     * master and so forth. */
    /* 如果主节点暂时发生故障，我们应该稍候尝试进行部分同步，所以跳转到异常处理流程。
     * 这种情况可能发生在主节点正在加载数据集或者它上游的主节点的连接断开 */
    if (psync_result == PSYNC_TRY_LATER) goto error;

    /* Note: if PSYNC does not return WAIT_REPLY, it will take care of
     * uninstalling the read handler from the file descriptor. */
    /* 注意： 如果 psync 没有返回 WAIT_REPLY 那么它将从 fd 上移除 read handler */
    if (psync_result == PSYNC_CONTINUE) {
        serverLog(LL_NOTICE, "MASTER <-> REPLICA sync: Master accepted a Partial Resynchronization.");
        if (server.supervised_mode == SUPERVISED_SYSTEMD) {
            redisCommunicateSystemd("STATUS=MASTER <-> REPLICA sync: Partial Resynchronization accepted. Ready to accept connections in read-write mode.\n");
        }
        return;
    }

    /* Fall back to SYNC if needed. Otherwise psync_result == PSYNC_FULLRESYNC
     * and the server.master_replid and master_initial_offset are
     * already populated. */
    /* 在主节点不支持 PSYNC 时回退到使用 SYNC 命令进行同步 */
    if (psync_result == PSYNC_NOT_SUPPORTED) {
        serverLog(LL_NOTICE,"Retrying with SYNC...");
        if (connSyncWrite(conn,"SYNC\r\n",6,server.repl_syncio_timeout*1000) == -1) {
            serverLog(LL_WARNING,"I/O error writing to MASTER: %s",
                strerror(errno));
            goto error;
        }
    }

    /* 执行到这里说明 psync_result == PSYNC_FULLRESYNC，且 
     * server.master_replid 和 master_initial_offset 已经有值了 */

    /* Prepare a suitable temp file for bulk transfer */
    /* 准备一个临时文件来接收主节点传来的 RDB 文件 */
    if (!useDisklessLoad()) {
        while(maxtries--) {
            snprintf(tmpfile,256,
                "temp-%d.%ld.rdb",(int)server.unixtime,(long int)getpid());
            dfd = open(tmpfile,O_CREAT|O_WRONLY|O_EXCL,0644);
            if (dfd != -1) break;
            sleep(1);
        }
        if (dfd == -1) {
            serverLog(LL_WARNING,"Opening the temp file needed for MASTER <-> REPLICA synchronization: %s",strerror(errno));
            goto error;
        }
        server.repl_transfer_tmpfile = zstrdup(tmpfile);
        server.repl_transfer_fd = dfd;
    }

    /* Setup the non blocking download of the bulk file. */
    /* 异步地下载 RDB 文件 */
    if (connSetReadHandler(conn, readSyncBulkPayload)
            == C_ERR)
    {
        char conninfo[CONN_INFO_LEN];
        serverLog(LL_WARNING,
            "Can't create readable event for SYNC: %s (%s)",
            strerror(errno), connGetInfo(conn, conninfo, sizeof(conninfo)));
        goto error;
    }

    server.repl_state = REPL_STATE_TRANSFER;
    server.repl_transfer_size = -1;
    server.repl_transfer_read = 0;
    server.repl_transfer_last_fsync_off = 0;
    server.repl_transfer_lastio = server.unixtime;
    return;

no_response_error: /* Handle receiveSynchronousResponse() error when master has no reply */
    serverLog(LL_WARNING, "Master did not respond to command during SYNC handshake");
    /* Fall through to regular error handling */

error:
    if (dfd != -1) close(dfd);
    connClose(conn);
    server.repl_transfer_s = NULL;
    if (server.repl_transfer_fd != -1)
        close(server.repl_transfer_fd);
    if (server.repl_transfer_tmpfile)
        zfree(server.repl_transfer_tmpfile);
    server.repl_transfer_tmpfile = NULL;
    server.repl_transfer_fd = -1;
    server.repl_state = REPL_STATE_CONNECT;
    return;

write_error: /* Handle sendCommand() errors. */
    serverLog(LL_WARNING,"Sending command to master in replication handshake: %s", err);
    sdsfree(err);
    goto error;
}


/* 连接主节点并进行主从复制 */
int connectWithMaster(void) {
    server.repl_transfer_s = server.tls_replication ? connCreateTLS() : connCreateSocket();
    /* syncWithMaster() 会负责进行与主节点同步 */
    if (connConnect(server.repl_transfer_s, server.masterhost, server.masterport,
                server.bind_source_addr, syncWithMaster) == C_ERR) {
        serverLog(LL_WARNING,"Unable to connect to MASTER: %s",
                connGetLastError(server.repl_transfer_s));
        connClose(server.repl_transfer_s);
        server.repl_transfer_s = NULL;
        return C_ERR;
    }


    server.repl_transfer_lastio = server.unixtime;
    server.repl_state = REPL_STATE_CONNECTING;
    serverLog(LL_NOTICE,"MASTER <-> REPLICA sync started");
    return C_OK;
}

/* This function can be called when a non blocking connection is currently
 * in progress to undo it.
 * Never call this function directly, use cancelReplicationHandshake() instead.
 */
void undoConnectWithMaster(void) {
    connClose(server.repl_transfer_s);
    server.repl_transfer_s = NULL;
}

/* Abort the async download of the bulk dataset while SYNC-ing with master.
 * Never call this function directly, use cancelReplicationHandshake() instead.
 */
void replicationAbortSyncTransfer(void) {
    serverAssert(server.repl_state == REPL_STATE_TRANSFER);
    undoConnectWithMaster();
    if (server.repl_transfer_fd!=-1) {
        close(server.repl_transfer_fd);
        bg_unlink(server.repl_transfer_tmpfile);
        zfree(server.repl_transfer_tmpfile);
        server.repl_transfer_tmpfile = NULL;
        server.repl_transfer_fd = -1;
    }
}

/* This function aborts a non blocking replication attempt if there is one
 * in progress, by canceling the non-blocking connect attempt or
 * the initial bulk transfer.
 *
 * If there was a replication handshake in progress 1 is returned and
 * the replication state (server.repl_state) set to REPL_STATE_CONNECT.
 *
 * Otherwise zero is returned and no operation is performed at all. */
int cancelReplicationHandshake(int reconnect) {
    if (server.repl_state == REPL_STATE_TRANSFER) {
        replicationAbortSyncTransfer();
        server.repl_state = REPL_STATE_CONNECT;
    } else if (server.repl_state == REPL_STATE_CONNECTING ||
               slaveIsInHandshakeState())
    {
        undoConnectWithMaster();
        server.repl_state = REPL_STATE_CONNECT;
    } else {
        return 0;
    }

    if (!reconnect)
        return 1;

    /* try to re-connect without waiting for replicationCron, this is needed
     * for the "diskless loading short read" test. */
    serverLog(LL_NOTICE,"Reconnecting to MASTER %s:%d after failure",
        server.masterhost, server.masterport);
    connectWithMaster();

    return 1;
}

/* Set replication to the specified master address and port. */
/* 建立与给定地址的主节点之间的主从复制 */
void replicationSetMaster(char *ip, int port) {
    int was_master = server.masterhost == NULL;

    sdsfree(server.masterhost);
    server.masterhost = NULL;
    if (server.master) {
        freeClient(server.master);
    }
    disconnectAllBlockedClients(); /* Clients blocked in master, now slave. */

    /* Setting masterhost only after the call to freeClient since it calls
     * replicationHandleMasterDisconnection which can trigger a re-connect
     * directly from within that call. */
    /* 因为 freeClient 会调用 replicationHandleMasterDisconnection, 而后者会直接触发重连。
     * 所以只有在调用 freeClient 之后才可重新设置 masterHost
     */
    server.masterhost = sdsnew(ip);
    server.masterport = port;

    /* Update oom_score_adj */
    setOOMScoreAdj(-1);

    /* Here we don't disconnect with replicas, since they may hopefully be able
     * to partially resync with us. We will disconnect with replicas and force
     * them to resync with us when changing replid on partially resync with new
     * master, or finishing transferring RDB and preparing loading DB on full
     * sync with new master. */
    /* 因为我们希望从节点可以与我们进行部分同步，所以这里我们不直接与从节点断开连接.
     * 当与新的主节点进行部分同步、全量同步或完成 RDB 传输时 replid 会发生改变，此时我们与从节点断开连接并强制从节点和我们进行全量同步。
     * 注：这条注释解释的是当前节点拥有若干个从节点且当前节点正在尝试成为其它节点的从节点时需要考虑的情况
     */
    cancelReplicationHandshake(0);
    /* Before destroying our master state, create a cached master using
     * our own parameters, to later PSYNC with the new master. */
    /* 在销毁我们的主节点状态之前先使用当前参数创建一个 CachedMaster 对象，以便可以与新主节点进行部分同步 */
    if (was_master) {
        replicationDiscardCachedMaster();
        replicationCacheMasterUsingMyself();
    }

    /* Fire the role change modules event. */
    /* 发送角色变化事件 */
    moduleFireServerEvent(REDISMODULE_EVENT_REPLICATION_ROLE_CHANGED,
                          REDISMODULE_EVENT_REPLROLECHANGED_NOW_REPLICA,
                          NULL);

    /* Fire the master link modules event. */
    /* 发送主节点连接变化事件 */
    if (server.repl_state == REPL_STATE_CONNECTED)
        moduleFireServerEvent(REDISMODULE_EVENT_MASTER_LINK_CHANGE,
                              REDISMODULE_SUBEVENT_MASTER_LINK_DOWN,
                              NULL);

    server.repl_state = REPL_STATE_CONNECT;
    serverLog(LL_NOTICE,"Connecting to MASTER %s:%d",
        server.masterhost, server.masterport);
    /* 与主节点建立连接并进行主从复制 */
    connectWithMaster();
}

/* Cancel replication, setting the instance as a master itself. */
void replicationUnsetMaster(void) {
    if (server.masterhost == NULL) return; /* Nothing to do. */

    /* Fire the master link modules event. */
    if (server.repl_state == REPL_STATE_CONNECTED)
        moduleFireServerEvent(REDISMODULE_EVENT_MASTER_LINK_CHANGE,
                              REDISMODULE_SUBEVENT_MASTER_LINK_DOWN,
                              NULL);

    /* Clear masterhost first, since the freeClient calls
     * replicationHandleMasterDisconnection which can attempt to re-connect. */
    sdsfree(server.masterhost);
    server.masterhost = NULL;
    if (server.master) freeClient(server.master);
    replicationDiscardCachedMaster();
    cancelReplicationHandshake(0);
    /* When a slave is turned into a master, the current replication ID
     * (that was inherited from the master at synchronization time) is
     * used as secondary ID up to the current offset, and a new replication
     * ID is created to continue with a new replication history. */
    shiftReplicationId();
    /* Disconnecting all the slaves is required: we need to inform slaves
     * of the replication ID change (see shiftReplicationId() call). However
     * the slaves will be able to partially resync with us, so it will be
     * a very fast reconnection. */
    disconnectSlaves();
    server.repl_state = REPL_STATE_NONE;

    /* We need to make sure the new master will start the replication stream
     * with a SELECT statement. This is forced after a full resync, but
     * with PSYNC version 2, there is no need for full resync after a
     * master switch. */
    server.slaveseldb = -1;

    /* Update oom_score_adj */
    setOOMScoreAdj(-1);

    /* Once we turn from slave to master, we consider the starting time without
     * slaves (that is used to count the replication backlog time to live) as
     * starting from now. Otherwise the backlog will be freed after a
     * failover if slaves do not connect immediately. */
    server.repl_no_slaves_since = server.unixtime;
    
    /* Reset down time so it'll be ready for when we turn into replica again. */
    server.repl_down_since = 0;

    /* Fire the role change modules event. */
    moduleFireServerEvent(REDISMODULE_EVENT_REPLICATION_ROLE_CHANGED,
                          REDISMODULE_EVENT_REPLROLECHANGED_NOW_MASTER,
                          NULL);

    /* Restart the AOF subsystem in case we shut it down during a sync when
     * we were still a slave. */
    if (server.aof_enabled && server.aof_state == AOF_OFF) restartAOFAfterSYNC();
}

/* This function is called when the slave lose the connection with the
 * master into an unexpected way. */
void replicationHandleMasterDisconnection(void) {
    /* Fire the master link modules event. */
    if (server.repl_state == REPL_STATE_CONNECTED)
        moduleFireServerEvent(REDISMODULE_EVENT_MASTER_LINK_CHANGE,
                              REDISMODULE_SUBEVENT_MASTER_LINK_DOWN,
                              NULL);

    server.master = NULL;
    server.repl_state = REPL_STATE_CONNECT;
    server.repl_down_since = server.unixtime;
    /* We lost connection with our master, don't disconnect slaves yet,
     * maybe we'll be able to PSYNC with our master later. We'll disconnect
     * the slaves only if we'll have to do a full resync with our master. */

    /* Try to re-connect immediately rather than wait for replicationCron
     * waiting 1 second may risk backlog being recycled. */
    if (server.masterhost) {
        serverLog(LL_NOTICE,"Reconnecting to MASTER %s:%d",
            server.masterhost, server.masterport);
        connectWithMaster();
    }
}

/* RELICAOF(SLAVEOF) 命令的入口函数 */
void replicaofCommand(client *c) {
    /* SLAVEOF is not allowed in cluster mode as replication is automatically
     * configured using the current address of the master node. */
    /* 因为在集群模式使用主节点的当前位置自动配置同步，所以在集群模式下禁止使用 RELICAOF / SLAVEOF 命令 */
    if (server.cluster_enabled) {
        addReplyError(c,"REPLICAOF not allowed in cluster mode.");
        return;
    }

    if (server.failover_state != NO_FAILOVER) {
        addReplyError(c,"REPLICAOF not allowed while failing over.");
        return;
    }

    /* The special host/port combination "NO" "ONE" turns the instance
     * into a master. Otherwise the new master address is set. */
    /* 使用特殊的 host/port 参数组合 "NO" "ONE" 可以将当前实例转变为主节点。
     * 其它参数用于设置新的主节点地址 */
    if (!strcasecmp(c->argv[1]->ptr,"no") &&
        !strcasecmp(c->argv[2]->ptr,"one")) {
        if (server.masterhost) {
            replicationUnsetMaster();
            sds client = catClientInfoString(sdsempty(),c);
            serverLog(LL_NOTICE,"MASTER MODE enabled (user request from '%s')",
                client);
            sdsfree(client);
        }
    } else {
        long port;

        if (c->flags & CLIENT_SLAVE)
        {
            /* If a client is already a replica they cannot run this command,
             * because it involves flushing all replicas (including this
             * client) */
            /* 主节点会拒绝从节点发来的 SLAVEOF 命令， 因为 SLAVEOF 命令会清空包括命令发送方在内的所有从节点。
            译者注：主节点会将与从节点的连接抽象为 CLIENT_SLAVE，
            */ 
            addReplyError(c, "Command is not valid when client is a replica.");
            return;
        }

        if (getRangeLongFromObjectOrReply(c, c->argv[2], 0, 65535, &port,
                                          "Invalid master port") != C_OK)
            return;

        /* Check if we are already attached to the specified master */
        /* 检查当前实例是否已经与命令中指定的主节点连接 */
        if (server.masterhost && !strcasecmp(server.masterhost,c->argv[1]->ptr)
            && server.masterport == port) {
            serverLog(LL_NOTICE,"REPLICAOF would result into synchronization "
                                "with the master we are already connected "
                                "with. No operation performed.");
            addReplySds(c,sdsnew("+OK Already connected to specified "
                                 "master\r\n"));
            return;
        }
        /* There was no previous master or the user specified a different one,
         * we can continue. */
        /* 之前没有主节点或者用户没有更换主节点，我们可以继续了 */
        replicationSetMaster(c->argv[1]->ptr, port);
        sds client = catClientInfoString(sdsempty(),c);
        serverLog(LL_NOTICE,"REPLICAOF %s:%d enabled (user request from '%s')",
            server.masterhost, server.masterport, client);
        sdsfree(client);
    }
    addReply(c,shared.ok);
}

/* ROLE command: provide information about the role of the instance
 * (master or slave) and additional information related to replication
 * in an easy to process format. */
void roleCommand(client *c) {
    if (server.sentinel_mode) {
        sentinelRoleCommand(c);
        return;
    }

    if (server.masterhost == NULL) {
        listIter li;
        listNode *ln;
        void *mbcount;
        int slaves = 0;

        addReplyArrayLen(c,3);
        addReplyBulkCBuffer(c,"master",6);
        addReplyLongLong(c,server.master_repl_offset);
        mbcount = addReplyDeferredLen(c);
        listRewind(server.slaves,&li);
        while((ln = listNext(&li))) {
            client *slave = ln->value;
            char ip[NET_IP_STR_LEN], *slaveaddr = slave->slave_addr;

            if (!slaveaddr) {
                if (connPeerToString(slave->conn,ip,sizeof(ip),NULL) == -1)
                    continue;
                slaveaddr = ip;
            }
            if (slave->replstate != SLAVE_STATE_ONLINE) continue;
            addReplyArrayLen(c,3);
            addReplyBulkCString(c,slaveaddr);
            addReplyBulkLongLong(c,slave->slave_listening_port);
            addReplyBulkLongLong(c,slave->repl_ack_off);
            slaves++;
        }
        setDeferredArrayLen(c,mbcount,slaves);
    } else {
        char *slavestate = NULL;

        addReplyArrayLen(c,5);
        addReplyBulkCBuffer(c,"slave",5);
        addReplyBulkCString(c,server.masterhost);
        addReplyLongLong(c,server.masterport);
        if (slaveIsInHandshakeState()) {
            slavestate = "handshake";
        } else {
            switch(server.repl_state) {
            case REPL_STATE_NONE: slavestate = "none"; break;
            case REPL_STATE_CONNECT: slavestate = "connect"; break;
            case REPL_STATE_CONNECTING: slavestate = "connecting"; break;
            case REPL_STATE_TRANSFER: slavestate = "sync"; break;
            case REPL_STATE_CONNECTED: slavestate = "connected"; break;
            default: slavestate = "unknown"; break;
            }
        }
        addReplyBulkCString(c,slavestate);
        addReplyLongLong(c,server.master ? server.master->reploff : -1);
    }
}

/* Send a REPLCONF ACK command to the master to inform it about the current
 * processed offset. If we are not connected with a master, the command has
 * no effects. */
/* 向主节点发送 REPLCONF ACK 命令，通知其当前处理的偏移量。如果我们没有与主机连接，则该命令无效 */
void replicationSendAck(void) {
    client *c = server.master;

    if (c != NULL) {
        c->flags |= CLIENT_MASTER_FORCE_REPLY;
        addReplyArrayLen(c,3);
        addReplyBulkCString(c,"REPLCONF");
        addReplyBulkCString(c,"ACK");
        addReplyBulkLongLong(c,c->reploff);
        c->flags &= ~CLIENT_MASTER_FORCE_REPLY;
    }
}

/* ---------------------- MASTER CACHING FOR PSYNC -------------------------- */

/* In order to implement partial synchronization we need to be able to cache
 * our master's client structure after a transient disconnection.
 * It is cached into server.cached_master and flushed away using the following
 * functions. */

/* This function is called by freeClient() in order to cache the master
 * client structure instead of destroying it. freeClient() will return
 * ASAP after this function returns, so every action needed to avoid problems
 * with a client that is really "suspended" has to be done by this function.
 *
 * The other functions that will deal with the cached master are:
 *
 * replicationDiscardCachedMaster() that will make sure to kill the client
 * as for some reason we don't want to use it in the future.
 *
 * replicationResurrectCachedMaster() that is used after a successful PSYNC
 * handshake in order to reactivate the cached master.
 */
void replicationCacheMaster(client *c) {
    serverAssert(server.master != NULL && server.cached_master == NULL);
    serverLog(LL_NOTICE,"Caching the disconnected master state.");

    /* Unlink the client from the server structures. */
    unlinkClient(c);

    /* Reset the master client so that's ready to accept new commands:
     * we want to discard the non processed query buffers and non processed
     * offsets, including pending transactions, already populated arguments,
     * pending outputs to the master. */
    sdsclear(server.master->querybuf);
    server.master->qb_pos = 0;
    server.master->repl_applied = 0;
    server.master->read_reploff = server.master->reploff;
    if (c->flags & CLIENT_MULTI) discardTransaction(c);
    listEmpty(c->reply);
    c->sentlen = 0;
    c->reply_bytes = 0;
    c->bufpos = 0;
    resetClient(c);

    /* Save the master. Server.master will be set to null later by
     * replicationHandleMasterDisconnection(). */
    server.cached_master = server.master;

    /* Invalidate the Peer ID cache. */
    if (c->peerid) {
        sdsfree(c->peerid);
        c->peerid = NULL;
    }
    /* Invalidate the Sock Name cache. */
    if (c->sockname) {
        sdsfree(c->sockname);
        c->sockname = NULL;
    }

    /* Caching the master happens instead of the actual freeClient() call,
     * so make sure to adjust the replication state. This function will
     * also set server.master to NULL. */
    replicationHandleMasterDisconnection();
}

/* This function is called when a master is turned into a slave, in order to
 * create from scratch a cached master for the new client, that will allow
 * to PSYNC with the slave that was promoted as the new master after a
 * failover.
 *
 * Assuming this instance was previously the master instance of the new master,
 * the new master will accept its replication ID, and potential also the
 * current offset if no data was lost during the failover. So we use our
 * current replication ID and offset in order to synthesize a cached master. */
void replicationCacheMasterUsingMyself(void) {
    serverLog(LL_NOTICE,
        "Before turning into a replica, using my own master parameters "
        "to synthesize a cached master: I may be able to synchronize with "
        "the new master with just a partial transfer.");

    /* This will be used to populate the field server.master->reploff
     * by replicationCreateMasterClient(). We'll later set the created
     * master as server.cached_master, so the replica will use such
     * offset for PSYNC. */
    server.master_initial_offset = server.master_repl_offset;

    /* The master client we create can be set to any DBID, because
     * the new master will start its replication stream with SELECT. */
    replicationCreateMasterClient(NULL,-1);

    /* Use our own ID / offset. */
    memcpy(server.master->replid, server.replid, sizeof(server.replid));

    /* Set as cached master. */
    unlinkClient(server.master);
    server.cached_master = server.master;
    server.master = NULL;
}

/* Free a cached master, called when there are no longer the conditions for
 * a partial resync on reconnection. */
void replicationDiscardCachedMaster(void) {
    if (server.cached_master == NULL) return;

    serverLog(LL_NOTICE,"Discarding previously cached master state.");
    server.cached_master->flags &= ~CLIENT_MASTER;
    freeClient(server.cached_master);
    server.cached_master = NULL;
}

/* Turn the cached master into the current master, using the file descriptor
 * passed as argument as the socket for the new master.
 *
 * This function is called when successfully setup a partial resynchronization
 * so the stream of data that we'll receive will start from where this
 * master left. */
/* 使用参数传递过来的文件描述符作为与新主节点的连接，并将 cached_master 转换为当前的主节点。
 * 
 * 当成功建立部分同步后调用此函数，因此我们将接收的数据流将从该主节点离开时开始。
 */
void replicationResurrectCachedMaster(connection *conn) {
    server.master = server.cached_master;
    server.cached_master = NULL;
    server.master->conn = conn;
    connSetPrivateData(server.master->conn, server.master);
    server.master->flags &= ~(CLIENT_CLOSE_AFTER_REPLY|CLIENT_CLOSE_ASAP);
    server.master->authenticated = 1;
    server.master->lastinteraction = server.unixtime;
    server.repl_state = REPL_STATE_CONNECTED;
    server.repl_down_since = 0;

    /* Fire the master link modules event. */
    moduleFireServerEvent(REDISMODULE_EVENT_MASTER_LINK_CHANGE,
                          REDISMODULE_SUBEVENT_MASTER_LINK_UP,
                          NULL);

    /* Re-add to the list of clients. */
    linkClient(server.master);
    if (connSetReadHandler(server.master->conn, readQueryFromClient)) {
        serverLog(LL_WARNING,"Error resurrecting the cached master, impossible to add the readable handler: %s", strerror(errno));
        freeClientAsync(server.master); /* Close ASAP. */
    }

    /* We may also need to install the write handler as well if there is
     * pending data in the write buffers. */
    if (clientHasPendingReplies(server.master)) {
        if (connSetWriteHandler(server.master->conn, sendReplyToClient)) {
            serverLog(LL_WARNING,"Error resurrecting the cached master, impossible to add the writable handler: %s", strerror(errno));
            freeClientAsync(server.master); /* Close ASAP. */
        }
    }
}

/* ------------------------- MIN-SLAVES-TO-WRITE  --------------------------- */

/* This function counts the number of slaves with lag <= min-slaves-max-lag.
 * If the option is active, the server will prevent writes if there are not
 * enough connected slaves with the specified lag (or less). */
/**
 * 只有 lag <= min-slaves-max-lag 才被认为是好的从节点。
 * 其中 repl_good_slaves_count 计数主要用于与 repl_min_slaves_to_write 对比，确定当前主节点是否可写
 * 若 repl_min_slaves_to_write 未设置，则不需要进行 repl_good_slaves_count 的计数操作
*/
void refreshGoodSlavesCount(void) {
    listIter li;
    listNode *ln;
    int good = 0;

    if (!server.repl_min_slaves_to_write ||
        !server.repl_min_slaves_max_lag) return;

    listRewind(server.slaves,&li);
    while((ln = listNext(&li))) {
        client *slave = ln->value;
        time_t lag = server.unixtime - slave->repl_ack_time;

        if (slave->replstate == SLAVE_STATE_ONLINE &&
            lag <= server.repl_min_slaves_max_lag) good++;
    }
    server.repl_good_slaves_count = good;
}

/* return true if status of good replicas is OK. otherwise false */
/**
 * 判断当前主节点是否可用：
 * 1. 从节点可用
 * 2. 未设置了偏移容忍度 repl_min_slaves_max_lag
 * 3. 未设置了最小可用 slave：repl_min_slaves_to_write
 * 4. 存在足够多的好节点
 * 若完全不满足，则会导致主节点禁止写入
 */
int checkGoodReplicasStatus(void) {
    return server.masterhost || /* not a primary status should be OK */
           !server.repl_min_slaves_max_lag || /* Min slave max lag not configured */
           !server.repl_min_slaves_to_write || /* Min slave to write not configured */
           server.repl_good_slaves_count >= server.repl_min_slaves_to_write; /* check if we have enough slaves */
}

/* ----------------------- SYNCHRONOUS REPLICATION --------------------------
 * Redis synchronous replication design can be summarized in points:
 *
 * - Redis masters have a global replication offset, used by PSYNC.
 * - Master increment the offset every time new commands are sent to slaves.
 * - Slaves ping back masters with the offset processed so far.
 *
 * So synchronous replication adds a new WAIT command in the form:
 *
 *   WAIT <num_replicas> <milliseconds_timeout>
 *
 * That returns the number of replicas that processed the query when
 * we finally have at least num_replicas, or when the timeout was
 * reached.
 *
 * The command is implemented in this way:
 *
 * - Every time a client processes a command, we remember the replication
 *   offset after sending that command to the slaves.
 * - When WAIT is called, we ask slaves to send an acknowledgement ASAP.
 *   The client is blocked at the same time (see blocked.c).
 * - Once we receive enough ACKs for a given offset or when the timeout
 *   is reached, the WAIT command is unblocked and the reply sent to the
 *   client.
 */

/* This just set a flag so that we broadcast a REPLCONF GETACK command
 * to all the slaves in the beforeSleep() function. Note that this way
 * we "group" all the clients that want to wait for synchronous replication
 * in a given event loop iteration, and send a single GETACK for them all. */
void replicationRequestAckFromSlaves(void) {
    server.get_ack_from_slaves = 1;
}

/* Return the number of slaves that already acknowledged the specified
 * replication offset. */
/* 返回已确认指定复制偏移量的从机数量 */
int replicationCountAcksByOffset(long long offset) {
    listIter li;
    listNode *ln;
    int count = 0;

    listRewind(server.slaves,&li);
    while((ln = listNext(&li))) {
        client *slave = ln->value;

        if (slave->replstate != SLAVE_STATE_ONLINE) continue;
        if (slave->repl_ack_off >= offset) count++;
    }
    return count;
}

/* WAIT for N replicas to acknowledge the processing of our latest
 * write command (and all the previous commands). */
/* 等待 N 个副本，以确认我们最新的写入命令（以及所有以前的命令）的处理 */
void waitCommand(client *c) {
    mstime_t timeout;
    long numreplicas, ackreplicas;
    long long offset = c->woff;

    if (server.masterhost) {
        addReplyError(c,"WAIT cannot be used with replica instances. Please also note that since Redis 4.0 if a replica is configured to be writable (which is not the default) writes to replicas are just local and are not propagated.");
        return;
    }

    /* Argument parsing. */
    if (getLongFromObjectOrReply(c,c->argv[1],&numreplicas,NULL) != C_OK)
        return;
    if (getTimeoutFromObjectOrReply(c,c->argv[2],&timeout,UNIT_MILLISECONDS)
        != C_OK) return;

    /* First try without blocking at all. */
    ackreplicas = replicationCountAcksByOffset(c->woff);
    if (ackreplicas >= numreplicas || c->flags & CLIENT_MULTI) {
        addReplyLongLong(c,ackreplicas);
        return;
    }

    /* Otherwise block the client and put it into our list of clients
     * waiting for ack from slaves. */
    c->bpop.timeout = timeout;
    c->bpop.reploffset = offset;
    c->bpop.numreplicas = numreplicas;
    listAddNodeHead(server.clients_waiting_acks,c);
    blockClient(c,BLOCKED_WAIT);

    /* Make sure that the server will send an ACK request to all the slaves
     * before returning to the event loop. */
    /* 在返回事件循环之前，确保服务器将向所有从节点发送ACK请求 */
    replicationRequestAckFromSlaves();
}

/* This is called by unblockClient() to perform the blocking op type
 * specific cleanup. We just remove the client from the list of clients
 * waiting for replica acks. Never call it directly, call unblockClient()
 * instead. */
void unblockClientWaitingReplicas(client *c) {
    listNode *ln = listSearchKey(server.clients_waiting_acks,c);
    serverAssert(ln != NULL);
    listDelNode(server.clients_waiting_acks,ln);
}

/* Check if there are clients blocked in WAIT that can be unblocked since
 * we received enough ACKs from slaves. */
/* 检查是否有在等待中被阻止的客户端可以被解除阻止，因为我们从从属服务器收到了足够的 ACK */
void processClientsWaitingReplicas(void) {
    long long last_offset = 0;
    int last_numreplicas = 0;

    listIter li;
    listNode *ln;

    listRewind(server.clients_waiting_acks,&li);
    while((ln = listNext(&li))) {
        client *c = ln->value;

        /* Every time we find a client that is satisfied for a given
         * offset and number of replicas, we remember it so the next client
         * may be unblocked without calling replicationCountAcksByOffset()
         * if the requested offset / replicas were equal or less. */
        if (last_offset && last_offset >= c->bpop.reploffset &&
                           last_numreplicas >= c->bpop.numreplicas)
        {
            unblockClient(c);
            addReplyLongLong(c,last_numreplicas);
        } else {
            int numreplicas = replicationCountAcksByOffset(c->bpop.reploffset);

            if (numreplicas >= c->bpop.numreplicas) {
                last_offset = c->bpop.reploffset;
                last_numreplicas = numreplicas;
                unblockClient(c);
                addReplyLongLong(c,numreplicas);
            }
        }
    }
}

/* Return the slave replication offset for this instance, that is
 * the offset for which we already processed the master replication stream. */
long long replicationGetSlaveOffset(void) {
    long long offset = 0;

    if (server.masterhost != NULL) {
        if (server.master) {
            offset = server.master->reploff;
        } else if (server.cached_master) {
            offset = server.cached_master->reploff;
        }
    }
    /* offset may be -1 when the master does not support it at all, however
     * this function is designed to return an offset that can express the
     * amount of data processed by the master, so we return a positive
     * integer. */
    if (offset < 0) offset = 0;
    return offset;
}

/* --------------------------- REPLICATION CRON  ---------------------------- */

/* Replication cron function, called 1 time per second. */
/* 主从复制定时任务，每秒执行一次。主要负责检测并处理超时、发送心跳等工作 */
void replicationCron(void) {
    static long long replication_cron_loops = 0;

    /* Check failover status first, to see if we need to start
     * handling the failover. */
    /* 检查故障转移(即master宕机后将slave提升为master的操作)状态，判断是否需要处理故障转移 */
    updateFailoverStatus();

    /* Non blocking connection timeout? */
    /* 处理连接超时 */
    if (server.masterhost &&
        (server.repl_state == REPL_STATE_CONNECTING ||
         slaveIsInHandshakeState()) &&
         (time(NULL)-server.repl_transfer_lastio) > server.repl_timeout)
    {
        serverLog(LL_WARNING,"Timeout connecting to the MASTER...");
        cancelReplicationHandshake(1);
    }

    /* Bulk transfer I/O timeout? */
    /* 处理消息传输耗时 */
    if (server.masterhost && server.repl_state == REPL_STATE_TRANSFER &&
        (time(NULL)-server.repl_transfer_lastio) > server.repl_timeout)
    {
        serverLog(LL_WARNING,"Timeout receiving bulk data from MASTER... If the problem persists try to set the 'repl-timeout' parameter in redis.conf to a larger value.");
        cancelReplicationHandshake(1);
    }

    /* Timed out master when we are an already connected slave? */
    /* 若本节点为从节点，检查主节点心跳是否超时 */
    if (server.masterhost && server.repl_state == REPL_STATE_CONNECTED &&
        (time(NULL)-server.master->lastinteraction) > server.repl_timeout)
    {
        serverLog(LL_WARNING,"MASTER timeout: no data nor PING received...");
        freeClient(server.master);
    }

    /* Check if we should connect to a MASTER */
    /* 检查是否需要重新连接主节点 */
    if (server.repl_state == REPL_STATE_CONNECT) {
        serverLog(LL_NOTICE,"Connecting to MASTER %s:%d",
            server.masterhost, server.masterport);
        connectWithMaster();
    }

    /* Send ACK to master from time to time.
     * Note that we do not send periodic acks to masters that don't
     * support PSYNC and replication offsets. */
    /* 不时向主节点发送 ACK.
     * 注意我们不会向不支持 PSYNC 的主节点定期发送 ACK
     */
    if (server.masterhost && server.master &&
        !(server.master->flags & CLIENT_PRE_PSYNC))
        replicationSendAck();

    /* If we have attached slaves, PING them from time to time.
     * So slaves can implement an explicit timeout to masters, and will
     * be able to detect a link disconnection even if the TCP connection
     * will not actually go down. */
    /* 如果我们有已经连接的从节点，不定期向他们发送 PING。这样从节点可以建立精确的主节点超时机制并检测到主从连接断开。
     * 即使在 tcp 连接未断开时，从节点也可以检测到主从连接中断（主节点进程卡死但未崩溃，此时 tcp 连接仍然存活）。
    */
    listIter li;
    listNode *ln;
    robj *ping_argv[1];

    /* First, send PING according to ping_slave_period. */
    /* 首先，按照 ping_slave_period 定义的间隔向从节点发送 PING 消息 */
    if ((replication_cron_loops % server.repl_ping_slave_period) == 0 &&
        listLength(server.slaves))
    {
        /* Note that we don't send the PING if the clients are paused during
         * a Redis Cluster manual failover: the PING we send will otherwise
         * alter the replication offsets of master and slave, and will no longer
         * match the one stored into 'mf_master_offset' state. */
        /* 注意：若客户端处于 Redis Cluster 手动故障转移过程中时我们不会向它发送 PING, 
         * 否则我们发送的 PING 会影响主从节点的复制偏移量使它不再与 mf_master_offset 中存储的状态相匹配
         */
        int manual_failover_in_progress =
            ((server.cluster_enabled &&
              server.cluster->mf_end) ||
            server.failover_end_time) &&
            checkClientPauseTimeoutAndReturnIfPaused();

        if (!manual_failover_in_progress) {
            ping_argv[0] = shared.ping;
            replicationFeedSlaves(server.slaves, server.slaveseldb,
                ping_argv, 1);
        }
    }

    /* Second, send a newline to all the slaves in pre-synchronization
     * stage, that is, slaves waiting for the master to create the RDB file.
     *
     * Also send the a newline to all the chained slaves we have, if we lost
     * connection from our master, to keep the slaves aware that their
     * master is online. This is needed since sub-slaves only receive proxied
     * data from top-level masters, so there is no explicit pinging in order
     * to avoid altering the replication offsets. This special out of band
     * pings (newlines) can be sent, they will have no effect in the offset.
     *
     * The newline will be ignored by the slave but will refresh the
     * last interaction timer preventing a timeout. In this case we ignore the
     * ping period and refresh the connection once per second since certain
     * timeouts are set at a few seconds (example: PSYNC response). */

    /* 第二，向所有 pre-synchronization 的从节点发送一个换行符。
     * pre-synchronization 状态是指从节点正在等待主节点发送 RDB 文件的状态
     * 
     * 在当前节点既是其它节点的从节点又拥有自己的从节点的情况下，如果当前节点失去了与主节点的连接
     * 我们会向下游的节点发送一个换行符，使它们知道自己的主节点（即当前节点）未超时。
     * 因为次级从节点只能收到经过中间代理才能收到顶层主节点的数据，而又没有一种 ping 消息来避免影响下游的复制偏移量，
     * 所以我们使用这种非常规 ping (即换行符) 消息来避免影响偏移量。
     * 
     * 换行符会被从节点忽略但是会更新上次交互时间，因此可以避免发生超时。
     * 在这种场景下我们会忽略通常为几秒的 ping 间隔配置，每秒发送一次来刷新连接。
    */
    listRewind(server.slaves,&li);
    while((ln = listNext(&li))) {
        client *slave = ln->value;

        int is_presync =
            (slave->replstate == SLAVE_STATE_WAIT_BGSAVE_START ||
            (slave->replstate == SLAVE_STATE_WAIT_BGSAVE_END &&
             server.rdb_child_type != RDB_CHILD_TYPE_SOCKET));

        if (is_presync) {
            connWrite(slave->conn, "\n", 1);
        }
    }

    /* Disconnect timedout slaves. */
    /* 断开与超时从节点的连接 */
    if (listLength(server.slaves)) {
        listIter li;
        listNode *ln;

        listRewind(server.slaves,&li);
        while((ln = listNext(&li))) {
            client *slave = ln->value;

            if (slave->replstate == SLAVE_STATE_ONLINE) {
                if (slave->flags & CLIENT_PRE_PSYNC)
                    continue;
                if ((server.unixtime - slave->repl_ack_time) > server.repl_timeout) {
                    serverLog(LL_WARNING, "Disconnecting timedout replica (streaming sync): %s",
                          replicationGetSlaveName(slave));
                    freeClient(slave);
                    continue;
                }
            }
            /* We consider disconnecting only diskless replicas because disk-based replicas aren't fed
             * by the fork child so if a disk-based replica is stuck it doesn't prevent the fork child
             * from terminating. */

            /* 断开与超时从节点的连接
             * 因为有磁盘复制不使用子进程发送数据，所以有磁盘复制阻塞不会阻止 fork 出的子进程结束。因此这里我们只考虑无磁盘复制。
            */
            if (slave->replstate == SLAVE_STATE_WAIT_BGSAVE_END && server.rdb_child_type == RDB_CHILD_TYPE_SOCKET) {
                if (slave->repl_last_partial_write != 0 &&
                    (server.unixtime - slave->repl_last_partial_write) > server.repl_timeout)
                {
                    serverLog(LL_WARNING, "Disconnecting timedout replica (full sync): %s",
                          replicationGetSlaveName(slave));
                    freeClient(slave);
                    continue;
                }
            }
        }
    }

    /* If this is a master without attached slaves and there is a replication
     * backlog active, in order to reclaim memory we can free it after some
     * (configured) time. Note that this cannot be done for slaves: slaves
     * without sub-slaves attached should still accumulate data into the
     * backlog, in order to reply to PSYNC queries if they are turned into
     * masters after a failover. */

    /* 如果当前节点是一个没有连接从节点的主节点但是却有一个活跃的复制积压缓冲区时，
     * 我们可以在规定时间后将缓冲区对象释放以便回收内存。
     * 
     * 注意：这个操作不适用于没有次级子节点的中间子节点。中间子节点需要持续将数据收集到复制积压缓冲区中，
     * 以防主节点故障后子节点被提升为新主节点后需要支持使用 PSYNC 进行同步。
     */
    if (listLength(server.slaves) == 0 && server.repl_backlog_time_limit &&
        server.repl_backlog && server.masterhost == NULL)
    {
        time_t idle = server.unixtime - server.repl_no_slaves_since;

        if (idle > server.repl_backlog_time_limit) {
            /* When we free the backlog, we always use a new
             * replication ID and clear the ID2. This is needed
             * because when there is no backlog, the master_repl_offset
             * is not updated, but we would still retain our replication
             * ID, leading to the following problem:
             *
             * 1. We are a master instance.
             * 2. Our slave is promoted to master. It's repl-id-2 will
             *    be the same as our repl-id.
             * 3. We, yet as master, receive some updates, that will not
             *    increment the master_repl_offset.
             * 4. Later we are turned into a slave, connect to the new
             *    master that will accept our PSYNC request by second
             *    replication ID, but there will be data inconsistency
             *    because we received writes. */

            
            /* 当释放积压缓冲区后我们会使用新的 replication ID 并清空 ID2.
             * 如果我们清空了积压缓冲区且不更新 master_repl_offset，
             * 此时我们继续维持 replication ID 不变会导致下列问题： 
             *
             * 1. 我们是一个主节点
             * 2. 我们的从节点被提升为主节点，它的 repl-id-2 和我们的 repl-id 一致
             * 3. 我们作为主节点收到了一些更新，它们并不会使 master_repl_offset 增加
             * 4. 随后我们变成了从节点，并连接到新主节点上。主节点会接受第二个 replication ID, 
             * 但是由于我们接受了写请求会导致数据不一致。
             */
            changeReplicationId();
            clearReplicationId2();
            freeReplicationBacklog();
            serverLog(LL_NOTICE,
                "Replication backlog freed after %d seconds "
                "without connected replicas.",
                (int) server.repl_backlog_time_limit);
        }
    }

    replicationStartPendingFork();

    /* Remove the RDB file used for replication if Redis is not running
     * with any persistence. */
    /* 如果 Redis 配置没有启用任何持久化，那么删除主从复制用的 RDB 文件 */
    removeRDBUsedToSyncReplicas();

    /* Sanity check replication buffer, the first block of replication buffer blocks
     * must be referenced by someone, since it will be freed when not referenced,
     * otherwise, server will OOM. also, its refcount must not be more than
     * replicas number + 1(replication backlog). */
    /* 对复制积压缓冲区进行完整性检查。
     * 缓冲区中的第一个 block 必须被引用。未被引用的必须释放，否则服务器会出现 OOM. 并且它的引用计数必须小于等于副本数 + 1
     */
    if (listLength(server.repl_buffer_blocks) > 0) {
        replBufBlock *o = listNodeValue(listFirst(server.repl_buffer_blocks));
        serverAssert(o->refcount > 0 &&
            o->refcount <= (int)listLength(server.slaves)+1);
    }

    /* Refresh the number of slaves with lag <= min-slaves-max-lag. */
    /* 刷新同步延迟 lag <= min-slaves-max-lag 的从节点数量 */
    refreshGoodSlavesCount();
    replication_cron_loops++; /* Incremented with frequency 1 HZ. */
}

int shouldStartChildReplication(int *mincapa_out, int *req_out) {
    /* We should start a BGSAVE good for replication if we have slaves in
     * WAIT_BGSAVE_START state.
     *
     * In case of diskless replication, we make sure to wait the specified
     * number of seconds (according to configuration) so that other slaves
     * have the time to arrive before we start streaming. */
    if (!hasActiveChildProcess()) {
        time_t idle, max_idle = 0;
        int slaves_waiting = 0;
        int mincapa;
        int req;
        int first = 1;
        listNode *ln;
        listIter li;

        listRewind(server.slaves,&li);
        while((ln = listNext(&li))) {
            client *slave = ln->value;
            if (slave->replstate == SLAVE_STATE_WAIT_BGSAVE_START) {
                if (first) {
                    /* Get first slave's requirements */
                    req = slave->slave_req;
                } else if (req != slave->slave_req) {
                    /* Skip slaves that don't match */
                    continue;
                }
                idle = server.unixtime - slave->lastinteraction;
                if (idle > max_idle) max_idle = idle;
                slaves_waiting++;
                mincapa = first ? slave->slave_capa : (mincapa & slave->slave_capa);
                first = 0;
            }
        }

        if (slaves_waiting &&
            (!server.repl_diskless_sync ||
             (server.repl_diskless_sync_max_replicas > 0 &&
              slaves_waiting >= server.repl_diskless_sync_max_replicas) ||
             max_idle >= server.repl_diskless_sync_delay))
        {
            if (mincapa_out)
                *mincapa_out = mincapa;
            if (req_out)
                *req_out = req;
            return 1;
        }
    }

    return 0;
}

void replicationStartPendingFork(void) {
    int mincapa = -1;
    int req = -1;

    if (shouldStartChildReplication(&mincapa, &req)) {
        /* Start the BGSAVE. The called function may start a
         * BGSAVE with socket target or disk target depending on the
         * configuration and slaves capabilities and requirements. */
        startBgsaveForReplication(mincapa, req);
    }
}

/* Find replica at IP:PORT from replica list */
static client *findReplica(char *host, int port) {
    listIter li;
    listNode *ln;
    client *replica;

    listRewind(server.slaves,&li);
    while((ln = listNext(&li))) {
        replica = ln->value;
        char ip[NET_IP_STR_LEN], *replicaip = replica->slave_addr;

        if (!replicaip) {
            if (connPeerToString(replica->conn, ip, sizeof(ip), NULL) == -1)
                continue;
            replicaip = ip;
        }

        if (!strcasecmp(host, replicaip) &&
                (port == replica->slave_listening_port))
            return replica;
    }

    return NULL;
}

const char *getFailoverStateString() {
    switch(server.failover_state) {
        case NO_FAILOVER: return "no-failover";
        case FAILOVER_IN_PROGRESS: return "failover-in-progress";
        case FAILOVER_WAIT_FOR_SYNC: return "waiting-for-sync";
        default: return "unknown";
    }
}

/* Resets the internal failover configuration, this needs
 * to be called after a failover either succeeds or fails
 * as it includes the client unpause. */
/* 重置内部故障切换配置，这需要在故障切换成功或失败后调用，因为它包括客户端取消连接 */
void clearFailoverState() {
    server.failover_end_time = 0;
    server.force_failover = 0;
    zfree(server.target_replica_host);
    server.target_replica_host = NULL;
    server.target_replica_port = 0;
    server.failover_state = NO_FAILOVER;
    unpauseClients(PAUSE_DURING_FAILOVER);
}

/* Abort an ongoing failover if one is going on. */
/* 如果正在进行故障转移，则中止正在进行的故障转移 */
void abortFailover(const char *err) {
    if (server.failover_state == NO_FAILOVER) return;

    if (server.target_replica_host) {
        serverLog(LL_NOTICE,"FAILOVER to %s:%d aborted: %s",
            server.target_replica_host,server.target_replica_port,err);  
    } else {
        serverLog(LL_NOTICE,"FAILOVER to any replica aborted: %s",err);  
    }
    if (server.failover_state == FAILOVER_IN_PROGRESS) {
        replicationUnsetMaster();
    }
    clearFailoverState();
}

/* 
 * FAILOVER [TO <HOST> <PORT> [FORCE]] [ABORT] [TIMEOUT <timeout>]
 * 
 * This command will coordinate a failover between the master and one
 * of its replicas. The happy path contains the following steps:
 * 1) The master will initiate a client pause write, to stop replication
 * traffic.
 * 2) The master will periodically check if any of its replicas has
 * consumed the entire replication stream through acks. 
 * 3) Once any replica has caught up, the master will itself become a replica.
 * 4) The master will send a PSYNC FAILOVER request to the target replica, which
 * if accepted will cause the replica to become the new master and start a sync.
 * 
 * FAILOVER ABORT is the only way to abort a failover command, as replicaof
 * will be disabled. This may be needed if the failover is unable to progress. 
 * 
 * The optional arguments [TO <HOST> <IP>] allows designating a specific replica
 * to be failed over to.
 * 
 * FORCE flag indicates that even if the target replica is not caught up,
 * failover to it anyway. This must be specified with a timeout and a target
 * HOST and IP.
 * 
 * TIMEOUT <timeout> indicates how long should the primary wait for 
 * a replica to sync up before aborting. If not specified, the failover
 * will attempt forever and must be manually aborted.
 */
/* 
 * 此命令将协调主节点和从节点之间的故障转移
 * 包含以下步骤：
 * 1. 主节点将启动客户端暂停写入，以停止复制。
 * 2. 主节点将定期检查其任何副本是否具有通过ack消耗了整个复制流。
 * 3. 一旦任何副本赶上，主节点本身将成为副本。
 * 4. 主节点将向目标副本发送 PSYNC FAILOVER 请求如果接受，将使副本成为新的主副本并开始同步。
 * 
 * 其中包括 FORCE、ABORT、TIMEOUT 的选项，分别代表强制切换、中断切换、切换超时
*/
void failoverCommand(client *c) {
    if (server.cluster_enabled) {
        addReplyError(c,"FAILOVER not allowed in cluster mode. "
                        "Use CLUSTER FAILOVER command instead.");
        return;
    }
    
    /* Handle special case for abort */
    if ((c->argc == 2) && !strcasecmp(c->argv[1]->ptr,"abort")) {
        if (server.failover_state == NO_FAILOVER) {
            addReplyError(c, "No failover in progress.");
            return;
        }

        abortFailover("Failover manually aborted");
        addReply(c,shared.ok);
        return;
    }

    long timeout_in_ms = 0;
    int force_flag = 0;
    long port = 0;
    char *host = NULL;

    /* Parse the command for syntax and arguments. */
    for (int j = 1; j < c->argc; j++) {
        if (!strcasecmp(c->argv[j]->ptr,"timeout") && (j + 1 < c->argc) &&
            timeout_in_ms == 0)
        {
            if (getLongFromObjectOrReply(c,c->argv[j + 1],
                        &timeout_in_ms,NULL) != C_OK) return;
            if (timeout_in_ms <= 0) {
                addReplyError(c,"FAILOVER timeout must be greater than 0");
                return;
            }
            j++;
        } else if (!strcasecmp(c->argv[j]->ptr,"to") && (j + 2 < c->argc) &&
            !host) 
        {
            if (getLongFromObjectOrReply(c,c->argv[j + 2],&port,NULL) != C_OK)
                return;
            host = c->argv[j + 1]->ptr;
            j += 2;
        } else if (!strcasecmp(c->argv[j]->ptr,"force") && !force_flag) {
            force_flag = 1;
        } else {
            addReplyErrorObject(c,shared.syntaxerr);
            return;
        }
    }

    if (server.failover_state != NO_FAILOVER) {
        addReplyError(c,"FAILOVER already in progress.");
        return;
    }

    if (server.masterhost) {
        addReplyError(c,"FAILOVER is not valid when server is a replica.");
        return;
    }

    if (listLength(server.slaves) == 0) {
        addReplyError(c,"FAILOVER requires connected replicas.");
        return; 
    }

    if (force_flag && (!timeout_in_ms || !host)) {
        addReplyError(c,"FAILOVER with force option requires both a timeout "
            "and target HOST and IP.");
        return;     
    }

    /* If a replica address was provided, validate that it is connected. */
    if (host) {
        client *replica = findReplica(host, port);

        if (replica == NULL) {
            addReplyError(c,"FAILOVER target HOST and PORT is not "
                            "a replica.");
            return;
        }

        /* Check if requested replica is online */
        if (replica->replstate != SLAVE_STATE_ONLINE) {
            addReplyError(c,"FAILOVER target replica is not online.");
            return;
        }

        server.target_replica_host = zstrdup(host);
        server.target_replica_port = port;
        serverLog(LL_NOTICE,"FAILOVER requested to %s:%ld.",host,port);
    } else {
        serverLog(LL_NOTICE,"FAILOVER requested to any replica.");
    }

    mstime_t now = mstime();
    if (timeout_in_ms) {
        server.failover_end_time = now + timeout_in_ms;
    }
    
    server.force_failover = force_flag;
    server.failover_state = FAILOVER_WAIT_FOR_SYNC;
    /* Cluster failover will unpause eventually */
    pauseClients(PAUSE_DURING_FAILOVER, LLONG_MAX, CLIENT_PAUSE_WRITE);
    addReply(c,shared.ok);
}

/* Failover cron function, checks coordinated failover state. 
 *
 * Implementation note: The current implementation calls replicationSetMaster()
 * to start the failover request, this has some unintended side effects if the
 * failover doesn't work like blocked clients will be unblocked and replicas will
 * be disconnected. This could be optimized further.
 */
/* 故障转移 cron 功能，检查协调的故障转移状态。
 *
 * 实施说明：当前的实现调用 replicationSetMaster() 来启动故障转移请求，如果故障转移无法正常工作，则会产生一些意外的副作用，
 * 比如被阻止的客户端将被解锁，副本将被断开连接。这可以进一步优化。
 */
void updateFailoverStatus(void) {
    if (server.failover_state != FAILOVER_WAIT_FOR_SYNC) return;
    mstime_t now = server.mstime;

    /* Check if failover operation has timed out */
    /* 如果 failover 的时间已过，则判断是否强制 failover */
    if (server.failover_end_time && server.failover_end_time <= now) {
        if (server.force_failover) {
            serverLog(LL_NOTICE,
                "FAILOVER to %s:%d time out exceeded, failing over.",
                server.target_replica_host, server.target_replica_port);
            server.failover_state = FAILOVER_IN_PROGRESS;
            /* If timeout has expired force a failover if requested. */
            /* 如果强行 failover，则直接设置 master */
            replicationSetMaster(server.target_replica_host,
                server.target_replica_port);
            return;
        } else {
            /* Force was not requested, so timeout. */
            /* 超时则直接暂停 failover 操作 */
            abortFailover("Replica never caught up before timeout");
            return;
        }
    }

    /* Check to see if the replica has caught up so failover can start */
    /* 选取一个 replica 作为新的 master */
    client *replica = NULL;
    if (server.target_replica_host) {
        replica = findReplica(server.target_replica_host, 
            server.target_replica_port);
    } else {
        listIter li;
        listNode *ln;

        listRewind(server.slaves,&li);
        /* Find any replica that has matched our repl_offset */
        while((ln = listNext(&li))) {
            replica = ln->value;
            /* 如果存在一个 replica 偏移量和当前 master 的偏移量相同，则采用当前的 replica */
            if (replica->repl_ack_off == server.master_repl_offset) {
                char ip[NET_IP_STR_LEN], *replicaaddr = replica->slave_addr;

                if (!replicaaddr) {
                    if (connPeerToString(replica->conn,ip,sizeof(ip),NULL) == -1)
                        continue;
                    replicaaddr = ip;
                }

                /* We are now failing over to this specific node */
                server.target_replica_host = zstrdup(replicaaddr);
                server.target_replica_port = replica->slave_listening_port;
                break;
            }
        }
    }

    /* We've found a replica that is caught up */
    /* 找到了一个 replica，且偏移量满足需求，则直接设置 master 信息 */
    if (replica && (replica->repl_ack_off == server.master_repl_offset)) {
        server.failover_state = FAILOVER_IN_PROGRESS;
        serverLog(LL_NOTICE,
                "Failover target %s:%d is synced, failing over.",
                server.target_replica_host, server.target_replica_port);
        /* Designated replica is caught up, failover to it. */
        replicationSetMaster(server.target_replica_host,
            server.target_replica_port);
    }
}
