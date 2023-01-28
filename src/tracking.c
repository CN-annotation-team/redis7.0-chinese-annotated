/* tracking.c - Client side caching: keys tracking and invalidation
 *
 * Copyright (c) 2019, Salvatore Sanfilippo <antirez at gmail dot com>
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

/* The tracking table is constituted by a radix tree of keys, each pointing
 * to a radix tree of client IDs, used to track the clients that may have
 * certain keys in their local, client side, cache.
 *
 * When a client enables tracking with "CLIENT TRACKING on", each key served to
 * the client is remembered in the table mapping the keys to the client IDs.
 * Later, when a key is modified, all the clients that may have local copy
 * of such key will receive an invalidation message.
 *
 * Clients will normally take frequently requested objects in memory, removing
 * them when invalidation messages are received. */
/* TrackingTable 是一个以 key 为 key，且监听的 client ID 为 value，用 rax 存储，用于定位当前 key 是否被 client 跟踪 */
rax *TrackingTable = NULL;
/* PrefixTable 是一个以 key 为 key，且 bcastState 为 value 的 rax 对象，表示一个 prefix 对应的 client 以及相关的 key */
rax *PrefixTable = NULL;
/* 整个跟踪表中存储的ID总数。这给出了一个关于CSC服务器端使用的总内存的提示 */
uint64_t TrackingTableTotalItems = 0; /* Total number of IDs stored across
                                         the whole tracking table. This gives
                                         an hint about the total memory we
                                         are using server side for CSC. */
robj *TrackingChannelName;

/* This is the structure that we have as value of the PrefixTable, and
 * represents the list of keys modified, and the list of clients that need
 * to be notified, for a given prefix. */
/* 这是作为 PrefixTable 的值的结构，它表示为给定前缀修改的 key 列表和需要通知的客户端列表 */
typedef struct bcastState {
    /* 在当前事件循环周期中修改的键 */
    rax *keys;      /* Keys modified in the current event loop cycle. */
    /* 客户端订阅了此前缀的通知事件 */
    rax *clients;   /* Clients subscribed to the notification events for this
                       prefix. */
} bcastState;

/* Remove the tracking state from the client 'c'. Note that there is not much
 * to do for us here, if not to decrement the counter of the clients in
 * tracking mode, because we just store the ID of the client in the tracking
 * table, so we'll remove the ID reference in a lazy way. Otherwise when a
 * client with many entries in the table is removed, it would cost a lot of
 * time to do the cleanup. */
/* 从客户端 “c” 中删除跟踪状态。
 * 请注意，如果不在跟踪模式下减少客户端的计数器，我们在这里没有太多工作要做，因为我们只是将客户端的 ID 存储在跟踪表中，所以我们将以惰性的方式删除 ID 引用。
 * 否则，当删除表中包含许多项的客户机时，将花费大量时间进行清理*/
void disableTracking(client *c) {
    /* If this client is in broadcasting mode, we need to unsubscribe it
     * from all the prefixes it is registered to. */
    /* 如果此客户端处于广播模式，我们需要取消订阅它注册到的所有前缀 */
    if (c->flags & CLIENT_TRACKING_BCAST) {
        raxIterator ri;
        raxStart(&ri,c->client_tracking_prefixes);
        raxSeek(&ri,"^",NULL,0);
        while(raxNext(&ri)) {
            bcastState *bs = raxFind(PrefixTable,ri.key,ri.key_len);
            serverAssert(bs != raxNotFound);
            raxRemove(bs->clients,(unsigned char*)&c,sizeof(c),NULL);
            /* Was it the last client? Remove the prefix from the
             * table. */
            if (raxSize(bs->clients) == 0) {
                raxFree(bs->clients);
                raxFree(bs->keys);
                zfree(bs);
                raxRemove(PrefixTable,ri.key,ri.key_len,NULL);
            }
        }
        raxStop(&ri);
        raxFree(c->client_tracking_prefixes);
        c->client_tracking_prefixes = NULL;
    }

    /* 清除标志并调整计数 */
    if (c->flags & CLIENT_TRACKING) {
        server.tracking_clients--;
        c->flags &= ~(CLIENT_TRACKING|CLIENT_TRACKING_BROKEN_REDIR|
                      CLIENT_TRACKING_BCAST|CLIENT_TRACKING_OPTIN|
                      CLIENT_TRACKING_OPTOUT|CLIENT_TRACKING_CACHING|
                      CLIENT_TRACKING_NOLOOP);
    }
}

static int stringCheckPrefix(unsigned char *s1, size_t s1_len, unsigned char *s2, size_t s2_len) {
    size_t min_length = s1_len < s2_len ? s1_len : s2_len;
    return memcmp(s1,s2,min_length) == 0;   
}

/* Check if any of the provided prefixes collide with one another or
 * with an existing prefix for the client. A collision is defined as two 
 * prefixes that will emit an invalidation for the same key. If no prefix 
 * collision is found, 1 is return, otherwise 0 is returned and the client 
 * has an error emitted describing the error. */
/* 检查所提供的前缀是否彼此冲突或与客户端的现有前缀冲突。
 * 冲突定义为两个前缀，这两个前缀将对同一 key 发出无效。
 * 如果未发现前缀冲突，则返回1，否则返回0，客户端将发出一个描述错误的错误*/
int checkPrefixCollisionsOrReply(client *c, robj **prefixes, size_t numprefix) {
    for (size_t i = 0; i < numprefix; i++) {
        /* Check input list has no overlap with existing prefixes. */
        /* 检查输入列表与现有前缀没有重叠 */
        if (c->client_tracking_prefixes) {
            raxIterator ri;
            raxStart(&ri,c->client_tracking_prefixes);
            raxSeek(&ri,"^",NULL,0);
            while(raxNext(&ri)) {
                if (stringCheckPrefix(ri.key,ri.key_len,
                    prefixes[i]->ptr,sdslen(prefixes[i]->ptr))) 
                {
                    sds collision = sdsnewlen(ri.key,ri.key_len);
                    addReplyErrorFormat(c,
                        "Prefix '%s' overlaps with an existing prefix '%s'. "
                        "Prefixes for a single client must not overlap.",
                        (unsigned char *)prefixes[i]->ptr,
                        (unsigned char *)collision);
                    sdsfree(collision);
                    raxStop(&ri);
                    return 0;
                }
            }
            raxStop(&ri);
        }
        /* Check input has no overlap with itself. */
        /* 检查输入与自身没有重叠 */
        for (size_t j = i + 1; j < numprefix; j++) {
            if (stringCheckPrefix(prefixes[i]->ptr,sdslen(prefixes[i]->ptr),
                prefixes[j]->ptr,sdslen(prefixes[j]->ptr)))
            {
                addReplyErrorFormat(c,
                    "Prefix '%s' overlaps with another provided prefix '%s'. "
                    "Prefixes for a single client must not overlap.",
                    (unsigned char *)prefixes[i]->ptr,
                    (unsigned char *)prefixes[j]->ptr);
                return i;
            }
        }
    }
    return 1;
}

/* Set the client 'c' to track the prefix 'prefix'. If the client 'c' is
 * already registered for the specified prefix, no operation is performed. */
/*设置客户端 “c” 以跟踪前缀 “prefix” 。如果客户端 “c” 已注册了指定前缀，则不执行任何操作 */
void enableBcastTrackingForPrefix(client *c, char *prefix, size_t plen) {
    bcastState *bs = raxFind(PrefixTable,(unsigned char*)prefix,plen);
    /* If this is the first client subscribing to such prefix, create
     * the prefix in the table. */
    /* 如果这是第一个订阅此类前缀的客户端，请在表中创建前缀 */
    if (bs == raxNotFound) {
        bs = zmalloc(sizeof(*bs));
        bs->keys = raxNew();
        bs->clients = raxNew();
        raxInsert(PrefixTable,(unsigned char*)prefix,plen,bs,NULL);
    }
    if (raxTryInsert(bs->clients,(unsigned char*)&c,sizeof(c),NULL,NULL)) {
        if (c->client_tracking_prefixes == NULL)
            c->client_tracking_prefixes = raxNew();
        raxInsert(c->client_tracking_prefixes,
                  (unsigned char*)prefix,plen,NULL,NULL);
    }
}

/* Enable the tracking state for the client 'c', and as a side effect allocates
 * the tracking table if needed. If the 'redirect_to' argument is non zero, the
 * invalidation messages for this client will be sent to the client ID
 * specified by the 'redirect_to' argument. Note that if such client will
 * eventually get freed, we'll send a message to the original client to
 * inform it of the condition. Multiple clients can redirect the invalidation
 * messages to the same client ID. */
/* 启用客户端 “c” 的跟踪状态，并根据需要分配跟踪表。
 * 如果 “redirect_to” 参数为非零，则此客户端的无效消息将发送到 “redirect_to” 参数指定的客户端 ID。
 * 请注意，如果这样的客户端最终将被释放，我们将向原始客户端发送一条消息，通知其情况。
 * 多个客户端可以将无效消息重定向到同一个客户端 ID */
void enableTracking(client *c, uint64_t redirect_to, uint64_t options, robj **prefix, size_t numprefix) {
    if (!(c->flags & CLIENT_TRACKING)) server.tracking_clients++;
    /* 清理所有的标记情况 */
    c->flags |= CLIENT_TRACKING;
    c->flags &= ~(CLIENT_TRACKING_BROKEN_REDIR|CLIENT_TRACKING_BCAST|
                  CLIENT_TRACKING_OPTIN|CLIENT_TRACKING_OPTOUT|
                  CLIENT_TRACKING_NOLOOP);
    c->client_tracking_redirection = redirect_to;

    /* This may be the first client we ever enable. Create the tracking
     * table if it does not exist. */
    /* 这可能是我们启用的第一个客户端。如果跟踪表不存在，则创建跟踪表 */
    if (TrackingTable == NULL) {
        TrackingTable = raxNew();
        PrefixTable = raxNew();
        TrackingChannelName = createStringObject("__redis__:invalidate",20);
    }

    /* For broadcasting, set the list of prefixes in the client. */
    /* 对于广播，请在客户端中设置前缀列表 */
    if (options & CLIENT_TRACKING_BCAST) {
        c->flags |= CLIENT_TRACKING_BCAST;
        if (numprefix == 0) enableBcastTrackingForPrefix(c,"",0);
        for (size_t j = 0; j < numprefix; j++) {
            sds sdsprefix = prefix[j]->ptr;
            enableBcastTrackingForPrefix(c,sdsprefix,sdslen(sdsprefix));
        }
    }

    /* Set the remaining flags that don't need any special handling. */
    /* 设置不需要任何特殊处理的其余标志 */
    c->flags |= options & (CLIENT_TRACKING_OPTIN|CLIENT_TRACKING_OPTOUT|
                           CLIENT_TRACKING_NOLOOP);
}

/* This function is called after the execution of a readonly command in the
 * case the client 'c' has keys tracking enabled and the tracking is not
 * in BCAST mode. It will populate the tracking invalidation table according
 * to the keys the user fetched, so that Redis will know what are the clients
 * that should receive an invalidation message with certain groups of keys
 * are modified. */
/* 如果客户端 “c” 已启用 key 跟踪，且跟踪未处于 BCAST 模式，则在执行只读命令后调用此函数。
 * 它将根据用户获取的 key 填充跟踪无效表，这样 Redis 将知道哪些客户端应该接收到无效消息，并修改了某些 key */
void trackingRememberKeys(client *c) {
    /* Return if we are in optin/out mode and the right CACHING command
     * was/wasn't given in order to modify the default behavior. */
    /* 如果我们处于 optin / optout 模式，并且没有给出正确的 CACHING 命令以修改默认行为，则返回 */
    uint64_t optin = c->flags & CLIENT_TRACKING_OPTIN;
    uint64_t optout = c->flags & CLIENT_TRACKING_OPTOUT;
    uint64_t caching_given = c->flags & CLIENT_TRACKING_CACHING;
    if ((optin && !caching_given) || (optout && caching_given)) return;

    getKeysResult result = GETKEYS_RESULT_INIT;
    int numkeys = getKeysFromCommand(c->cmd,c->argv,c->argc,&result);
    if (!numkeys) {
        getKeysFreeResult(&result);
        return;
    }
    /* Shard channels are treated as special keys for client
     * library to rely on `COMMAND` command to discover the node
     * to connect to. These channels doesn't need to be tracked. */
    /* 分片通道被视为客户端库的特殊 key，以依赖 “COMMAND” 命令来发现要连接到的节点。这些通道不需要跟踪 */
    if (c->cmd->flags & CMD_PUBSUB) {
        return;
    }

    keyReference *keys = result.keys;

    for(int j = 0; j < numkeys; j++) {
        int idx = keys[j].pos;
        sds sdskey = c->argv[idx]->ptr;
        rax *ids = raxFind(TrackingTable,(unsigned char*)sdskey,sdslen(sdskey));
        if (ids == raxNotFound) {
            ids = raxNew();
            int inserted = raxTryInsert(TrackingTable,(unsigned char*)sdskey,
                                        sdslen(sdskey),ids, NULL);
            serverAssert(inserted == 1);
        }
        if (raxTryInsert(ids,(unsigned char*)&c->id,sizeof(c->id),NULL,NULL))
            TrackingTableTotalItems++;
    }
    getKeysFreeResult(&result);
}

/* Given a key name, this function sends an invalidation message in the
 * proper channel (depending on RESP version: PubSub or Push message) and
 * to the proper client (in case of redirection), in the context of the
 * client 'c' with tracking enabled.
 *
 * In case the 'proto' argument is non zero, the function will assume that
 * 'keyname' points to a buffer of 'keylen' bytes already expressed in the
 * form of Redis RESP protocol. This is used for:
 * - In BCAST mode, to send an array of invalidated keys to all
 *   applicable clients
 * - Following a flush command, to send a single RESP NULL to indicate
 *   that all keys are now invalid. */
/* 给定 key 名称，此函数在启用跟踪的客户端 “c” 的上下文中，在适当的通道（取决于 RESP 版本：PubSub / Push 消息）和适当的客户端（在重定向的情况下）发送无效消息。
 * 如果 “proto” 参数为非零，则函数将假设 “keyname” 指向已以 Redis RESP 协议形式表示的 “keylen” 字节缓冲区。这用于：
 * - 在 BCAST 模式下，向所有适用的客户端发送一组无效 key
 * - 在 flush 命令之后，发送一个 RESP NULL，表示所有 key 现在都无效 */
void sendTrackingMessage(client *c, char *keyname, size_t keylen, int proto) {
    int using_redirection = 0;
    if (c->client_tracking_redirection) {
        client *redir = lookupClientByID(c->client_tracking_redirection);
        if (!redir) {
            c->flags |= CLIENT_TRACKING_BROKEN_REDIR;
            /* We need to signal to the original connection that we
             * are unable to send invalidation messages to the redirected
             * connection, because the client no longer exist. */
            /* 我们需要向原始连接发出信号，表明我们无法向重定向的连接发送无效消息，因为客户端已不存在 */
            if (c->resp > 2) {
                addReplyPushLen(c,2);
                addReplyBulkCBuffer(c,"tracking-redir-broken",21);
                addReplyLongLong(c,c->client_tracking_redirection);
            }
            return;
        }
        c = redir;
        using_redirection = 1;
    }

    /* Only send such info for clients in RESP version 3 or more. However
     * if redirection is active, and the connection we redirect to is
     * in Pub/Sub mode, we can support the feature with RESP 2 as well,
     * by sending Pub/Sub messages in the __redis__:invalidate channel. */
    /* 仅为 RESP 版本 3 或更高版本的客户端发送此类信息。
     * 但是，如果重定向处于活动状态，并且我们重定向到的连接处于 Pub/Sub 模式，我们也可以通过在 __redis__:validate 通道中发送 Pub/Sub 消息来支持 RESP 2 的功能 */
    if (c->resp > 2) {
        addReplyPushLen(c,2);
        addReplyBulkCBuffer(c,"invalidate",10);
    } else if (using_redirection && c->flags & CLIENT_PUBSUB) {
        /* We use a static object to speedup things, however we assume
         * that addReplyPubsubMessage() will not take a reference. */
        /* 我们使用静态对象来加快速度，但是我们假设 addReplyPubsubMessage（）不会引用 */
        addReplyPubsubMessage(c,TrackingChannelName,NULL,shared.messagebulk);
    } else {
        /* If are here, the client is not using RESP3, nor is
         * redirecting to another client. We can't send anything to
         * it since RESP2 does not support push messages in the same
         * connection. */
        /* 如果在此处，则客户端未使用 RESP3，也未重定向到另一个客户端。由于 RESP2 不支持同一连接中的推送消息，因此我们无法向其发送任何消息 */
        return;
    }

    /* Send the "value" part, which is the array of keys. */
    /* 发送 “value” 部分，即键数组 */
    if (proto) {
        addReplyProto(c,keyname,keylen);
    } else {
        addReplyArrayLen(c,1);
        addReplyBulkCBuffer(c,keyname,keylen);
    }
    updateClientMemUsage(c);
}

/* This function is called when a key is modified in Redis and in the case
 * we have at least one client with the BCAST mode enabled.
 * Its goal is to set the key in the right broadcast state if the key
 * matches one or more prefixes in the prefix table. Later when we
 * return to the event loop, we'll send invalidation messages to the
 * clients subscribed to each prefix. */
/* 在 Redis 中修改 key 时调用此函数，在这种情况下，我们至少有一个客户端启用了 BCAST 模式。
 * 它的目标是，如果 key 与前缀表中的一个或多个前缀匹配，则将 key 设置为正确的广播状态。
 * 稍后当我们返回事件循环时，我们将向订阅每个前缀的客户端发送无效消息 */
void trackingRememberKeyToBroadcast(client *c, char *keyname, size_t keylen) {
    raxIterator ri;
    raxStart(&ri,PrefixTable);
    raxSeek(&ri,"^",NULL,0);
    while(raxNext(&ri)) {
        if (ri.key_len > keylen) continue;
        if (ri.key_len != 0 && memcmp(ri.key,keyname,ri.key_len) != 0)
            continue;
        bcastState *bs = ri.data;
        /* We insert the client pointer as associated value in the radix
         * tree. This way we know who was the client that did the last
         * change to the key, and can avoid sending the notification in the
         * case the client is in NOLOOP mode. */
        /* 我们将客户端指针作为相关值插入基数树中。这样，我们就知道谁是上次更改 key 的客户机，并且可以避免在客户机处于 NOLOOP 模式的情况下发送通知 */
        raxInsert(bs->keys,(unsigned char*)keyname,keylen,c,NULL);
    }
    raxStop(&ri);
}

/* This function is called from signalModifiedKey() or other places in Redis
 * when a key changes value. In the context of keys tracking, our task here is
 * to send a notification to every client that may have keys about such caching
 * slot.
 *
 * Note that 'c' may be NULL in case the operation was performed outside the
 * context of a client modifying the database (for instance when we delete a
 * key because of expire).
 *
 * The last argument 'bcast' tells the function if it should also schedule
 * the key for broadcasting to clients in BCAST mode. This is the case when
 * the function is called from the Redis core once a key is modified, however
 * we also call the function in order to evict keys in the key table in case
 * of memory pressure: in that case the key didn't really change, so we want
 * just to notify the clients that are in the table for this key, that would
 * otherwise miss the fact we are no longer tracking the key for them. */
/* 当键更改值时，从 signalModifiedKey（） 或 Redis 中的其他位置调用此函数。
 * 在 key 跟踪的上下文中，我们在这里的任务是向每个可能拥有此类缓存槽 key 的客户端发送通知。
 * 请注意，如果操作是在客户端修改数据库的上下文之外执行的（例如，当我们因为过期而删除密钥时），“c” 可能为NULL。
 * 最后一个参数 “bcast” 告诉函数，它是否也应该调度密钥，以便在 bcast 模式下向客户端广播。
 * 当修改键后，从 Redis 核心调用该函数就是这种情况，但是我们也调用该函数，以便在内存压力的情况下收回键表中的键：
 * 在这种情况下，键并没有真正改变，所以我们只想通知表中的客户机该键，否则就会错过我们不再跟踪键的事实 
 */
void trackingInvalidateKey(client *c, robj *keyobj, int bcast) {
    if (TrackingTable == NULL) return;

    unsigned char *key = (unsigned char*)keyobj->ptr;
    size_t keylen = sdslen(keyobj->ptr);

    if (bcast && raxSize(PrefixTable) > 0)
        trackingRememberKeyToBroadcast(c,(char *)key,keylen);

    rax *ids = raxFind(TrackingTable,key,keylen);
    if (ids == raxNotFound) return;

    raxIterator ri;
    raxStart(&ri,ids);
    raxSeek(&ri,"^",NULL,0);
    while(raxNext(&ri)) {
        uint64_t id;
        memcpy(&id,ri.key,sizeof(id));
        client *target = lookupClientByID(id);
        /* Note that if the client is in BCAST mode, we don't want to
         * send invalidation messages that were pending in the case
         * previously the client was not in BCAST mode. This can happen if
         * TRACKING is enabled normally, and then the client switches to
         * BCAST mode. */
        /* 请注意，如果客户机处于 BCAST 模式，我们不希望发送在之前客户机不处于 BCAST 状态时挂起的无效消息。
         * 如果正常启用 TRACKING，然后客户端切换到 BCAST 模式，则会发生这种情况 */
        if (target == NULL ||
            !(target->flags & CLIENT_TRACKING)||
            target->flags & CLIENT_TRACKING_BCAST)
        {
            continue;
        }

        /* If the client enabled the NOLOOP mode, don't send notifications
         * about keys changed by the client itself. */
        /* 如果客户端启用了 NOLOOP 模式，则不要发送有关客户端自身更改的 key 的通知 */
        if (target->flags & CLIENT_TRACKING_NOLOOP &&
            target == server.current_client)
        {
            continue;
        }

        /* If target is current client, we need schedule key invalidation.
         * As the invalidation messages may be interleaved with command
         * response and should after command response */
        /* 如果目标是当前客户端，我们需要调度 key 无效。因为无效消息可以与命令响应交错，并且应该在命令响应之后 */
        if (target == server.current_client){
            incrRefCount(keyobj);
            listAddNodeTail(server.tracking_pending_keys, keyobj);
        } else {
            sendTrackingMessage(target,(char *)keyobj->ptr,sdslen(keyobj->ptr),0);
        }
    }
    raxStop(&ri);

    /* Free the tracking table: we'll create the radix tree and populate it
     * again if more keys will be modified in this caching slot. */
    /* 释放跟踪表：如果在这个缓存槽中修改更多的键，我们将创建基数树并再次填充它 */
    TrackingTableTotalItems -= raxSize(ids);
    raxFree(ids);
    raxRemove(TrackingTable,(unsigned char*)key,keylen,NULL);
}

/* 处理待发送的失效 key 信息 */
void trackingHandlePendingKeyInvalidations() {
    if (!listLength(server.tracking_pending_keys)) return;

    listNode *ln;
    listIter li;

    listRewind(server.tracking_pending_keys,&li);
    while ((ln = listNext(&li)) != NULL) {
        robj *key = listNodeValue(ln);
        /* current_client maybe freed, so we need to send invalidation
         * message only when current_client is still alive */
        /* current_client 可能被释放了，所以我们只需要在 current_client 仍处于活动状态时发送无效消息 */
        if (server.current_client != NULL) {
            if (key != NULL) {
                sendTrackingMessage(server.current_client,(char *)key->ptr,sdslen(key->ptr),0);
            } else {
                sendTrackingMessage(server.current_client,shared.null[server.current_client->resp]->ptr,
                    sdslen(shared.null[server.current_client->resp]->ptr),1);
            }
        }
        if (key != NULL) decrRefCount(key);
    }
    listEmpty(server.tracking_pending_keys);
}

/* This function is called when one or all the Redis databases are
 * flushed. Caching keys are not specific for each DB but are global: 
 * currently what we do is send a special notification to clients with 
 * tracking enabled, sending a RESP NULL, which means, "all the keys", 
 * in order to avoid flooding clients with many invalidation messages 
 * for all the keys they may hold.
 */
/* 刷新一个或所有 Redis 数据库时调用此函数。缓存 key 不是特定于每个数据库，而是全局的：
 * 目前，我们所做的是向启用了跟踪的客户端发送一个特殊通知，发送RESP NULL，这意味着“所有 key”，以避免为客户端可能持有的所有 key 发送大量无效消息。
 */
void freeTrackingRadixTreeCallback(void *rt) {
    raxFree(rt);
}

void freeTrackingRadixTree(rax *rt) {
    raxFreeWithCallback(rt,freeTrackingRadixTreeCallback);
}

/* A RESP NULL is sent to indicate that all keys are invalid */
/* 发送RESP NULL表示所有 key 都无效，主要的调用方都是在 db 清理流程中 */
void trackingInvalidateKeysOnFlush(int async) {
    if (server.tracking_clients) {
        listNode *ln;
        listIter li;
        listRewind(server.clients,&li);
        while ((ln = listNext(&li)) != NULL) {
            client *c = listNodeValue(ln);
            if (c->flags & CLIENT_TRACKING) {
                if (c == server.current_client) {
                    /* We use a special NULL to indicate that we should send null */
                    /* 我们使用一个特殊的 NULL 来表示我们应该发送 NULL */
                    listAddNodeTail(server.tracking_pending_keys,NULL);
                } else {
                    sendTrackingMessage(c,shared.null[c->resp]->ptr,sdslen(shared.null[c->resp]->ptr),1);
                }
            }
        }
    }

    /* In case of FLUSHALL, reclaim all the memory used by tracking. */
    /*在 FLUSHALL 的情况下，回收跟踪使用的所有内存 */
    if (TrackingTable) {
        if (async) {
            freeTrackingRadixTreeAsync(TrackingTable);
        } else {
            freeTrackingRadixTree(TrackingTable);
        }
        TrackingTable = raxNew();
        TrackingTableTotalItems = 0;
    }
}

/* Tracking forces Redis to remember information about which client may have
 * certain keys. In workloads where there are a lot of reads, but keys are
 * hardly modified, the amount of information we have to remember server side
 * could be a lot, with the number of keys being totally not bound.
 *
 * So Redis allows the user to configure a maximum number of keys for the
 * invalidation table. This function makes sure that we don't go over the
 * specified fill rate: if we are over, we can just evict information about
 * a random key, and send invalidation messages to clients like if the key was
 * modified. */
/* 跟踪迫使 Redis 记住有关哪个客户端可能具有某些 key 的信息。
 * 在有大量读取但 key 几乎不被修改的工作负载中，我们必须记住服务器端的信息量可能很大，而 key 的数量完全不受限制。
 * 因此，Redis允许用户为无效表配置最大数量的键。此函数可确保我们不会超过指定的填充率：
 * 如果超过了，我们可以删除有关随机 key 的信息，并向客户端发送无效消息，就像 key 被修改一样 */
void trackingLimitUsedSlots(void) {
    static unsigned int timeout_counter = 0;
    if (TrackingTable == NULL) return;
    if (server.tracking_table_max_keys == 0) return; /* No limits set. */
    size_t max_keys = server.tracking_table_max_keys;
    if (raxSize(TrackingTable) <= max_keys) {
        timeout_counter = 0;
        return; /* Limit not reached. */
    }

    /* We have to invalidate a few keys to reach the limit again. The effort
     * we do here is proportional to the number of times we entered this
     * function and found that we are still over the limit. */
    /* 我们必须使 key 失效才能再次达到极限。我们在这里所做的努力与我们进入该函数并发现仍然超出限制的次数成正比 */
    int effort = 100 * (timeout_counter+1);

    /* We just remove one key after another by using a random walk. */
    /* 我们只需通过使用随机漫步移除一个 key */
    raxIterator ri;
    raxStart(&ri,TrackingTable);
    while(effort > 0) {
        effort--;
        raxSeek(&ri,"^",NULL,0);
        raxRandomWalk(&ri,0);
        if (raxEOF(&ri)) break;
        robj *keyobj = createStringObject((char*)ri.key,ri.key_len);
        trackingInvalidateKey(NULL,keyobj,0);
        decrRefCount(keyobj);
        if (raxSize(TrackingTable) <= max_keys) {
            timeout_counter = 0;
            raxStop(&ri);
            return; /* Return ASAP: we are again under the limit. */
        }
    }

    /* If we reach this point, we were not able to go under the configured
     * limit using the maximum effort we had for this run. */
    /* 如果我们达到了这一点，我们就无法使用本次运行的最大努力达到配置的限制 */
    raxStop(&ri);
    timeout_counter++;
}

/* Generate Redis protocol for an array containing all the key names
 * in the 'keys' radix tree. If the client is not NULL, the list will not
 * include keys that were modified the last time by this client, in order
 * to implement the NOLOOP option.
 *
 * If the resulting array would be empty, NULL is returned instead. */
/* 为包含 “keys” 基数树中所有键名称的数组生成 Redis 协议。如果客户端不为 NULL，则该列表将不包含上次由该客户端修改的 key ，以便实现 NOLOOP 选项。
 * 如果结果数组为空，则返回NULL */
sds trackingBuildBroadcastReply(client *c, rax *keys) {
    raxIterator ri;
    uint64_t count;

    if (c == NULL) {
        count = raxSize(keys);
    } else {
        count = 0;
        raxStart(&ri,keys);
        raxSeek(&ri,"^",NULL,0);
        while(raxNext(&ri)) {
            if (ri.data != c) count++;
        }
        raxStop(&ri);

        if (count == 0) return NULL;
    }

    /* Create the array reply with the list of keys once, then send
    * it to all the clients subscribed to this prefix. */
   /* 使用 key 列表创建一次数组应答，然后将其发送给订阅此前缀的所有客户端 */
    char buf[32];
    size_t len = ll2string(buf,sizeof(buf),count);
    sds proto = sdsempty();
    proto = sdsMakeRoomFor(proto,count*15);
    proto = sdscatlen(proto,"*",1);
    proto = sdscatlen(proto,buf,len);
    proto = sdscatlen(proto,"\r\n",2);
    raxStart(&ri,keys);
    raxSeek(&ri,"^",NULL,0);
    while(raxNext(&ri)) {
        if (c && ri.data == c) continue;
        len = ll2string(buf,sizeof(buf),ri.key_len);
        proto = sdscatlen(proto,"$",1);
        proto = sdscatlen(proto,buf,len);
        proto = sdscatlen(proto,"\r\n",2);
        proto = sdscatlen(proto,ri.key,ri.key_len);
        proto = sdscatlen(proto,"\r\n",2);
    }
    raxStop(&ri);
    return proto;
}

/* This function will run the prefixes of clients in BCAST mode and
 * keys that were modified about each prefix, and will send the
 * notifications to each client in each prefix. */
/* 此函数将在 BCAST 模式下运行客户端的前缀和针对每个前缀修改的 key，并将每个前缀中的通知发送给每个客户端 */
void trackingBroadcastInvalidationMessages(void) {
    raxIterator ri, ri2;

    /* Return ASAP if there is nothing to do here. */
    if (TrackingTable == NULL || !server.tracking_clients) return;

    raxStart(&ri,PrefixTable);
    raxSeek(&ri,"^",NULL,0);

    /* For each prefix... */
    while(raxNext(&ri)) {
        bcastState *bs = ri.data;

        if (raxSize(bs->keys)) {
            /* Generate the common protocol for all the clients that are
             * not using the NOLOOP option. */
            /* 为所有未使用 NOLOOP 选项的客户端生成通用协议 */
            sds proto = trackingBuildBroadcastReply(NULL,bs->keys);

            /* Send this array of keys to every client in the list. */
            /* 将此 key 数组发送到列表中的每个客户端 */
            raxStart(&ri2,bs->clients);
            raxSeek(&ri2,"^",NULL,0);
            while(raxNext(&ri2)) {
                client *c;
                memcpy(&c,ri2.key,sizeof(c));
                if (c->flags & CLIENT_TRACKING_NOLOOP) {
                    /* This client may have certain keys excluded. */
                    /* 此客户端可能排除了某些 key */
                    sds adhoc = trackingBuildBroadcastReply(c,bs->keys);
                    if (adhoc) {
                        sendTrackingMessage(c,adhoc,sdslen(adhoc),1);
                        sdsfree(adhoc);
                    }
                } else {
                    sendTrackingMessage(c,proto,sdslen(proto),1);
                }
            }
            raxStop(&ri2);

            /* Clean up: we can remove everything from this state, because we
             * want to only track the new keys that will be accumulated starting
             * from now. */
            /* 清理：我们可以从这个状态中删除所有内容，因为我们只想跟踪从现在开始累积的新 key */
            sdsfree(proto);
        }
        raxFree(bs->keys);
        bs->keys = raxNew();
    }
    raxStop(&ri);
}

/* This is just used in order to access the amount of used slots in the
 * tracking table. */
/* 这仅用于访问跟踪表中已使用的插槽数量 */
uint64_t trackingGetTotalItems(void) {
    return TrackingTableTotalItems;
}

uint64_t trackingGetTotalKeys(void) {
    if (TrackingTable == NULL) return 0;
    return raxSize(TrackingTable);
}

uint64_t trackingGetTotalPrefixes(void) {
    if (PrefixTable == NULL) return 0;
    return raxSize(PrefixTable);
}
