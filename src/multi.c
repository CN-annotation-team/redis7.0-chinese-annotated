/*
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

/* ================================ MULTI/EXEC ============================== */

/* Client state initialization for MULTI/EXEC */
/* 初始化一个客户端的事务 */
void initClientMultiState(client *c) {
    c->mstate.commands = NULL;
    c->mstate.count = 0;
    c->mstate.cmd_flags = 0;
    c->mstate.cmd_inv_flags = 0;
    c->mstate.argv_len_sums = 0;
    c->mstate.alloc_count = 0;
}

/* Release all the resources associated with MULTI/EXEC state */
/* 释放一个事务 */
void freeClientMultiState(client *c) {
    int j;

    /* 遍历事务命令队列 */
    for (j = 0; j < c->mstate.count; j++) {
        int i;
        multiCmd *mc = c->mstate.commands+j;
        /* 释放所有命令参数 */
        for (i = 0; i < mc->argc; i++)
            decrRefCount(mc->argv[i]);
        zfree(mc->argv);
    }
    /* 释放事务命令队列 */
    zfree(c->mstate.commands);
}

/* Add a new command into the MULTI commands queue */
/* 添加一个命令到事务命令队列中 */
void queueMultiCommand(client *c, uint64_t cmd_flags) {
    multiCmd *mc;

    /* No sense to waste memory if the transaction is already aborted.
     * this is useful in case client sends these in a pipeline, or doesn't
     * bother to read previous responses and didn't notice the multi was already
     * aborted. */
    /* 防止该事务已经被丢弃了，还往里面加命令，导致内存浪费 */
    if (c->flags & (CLIENT_DIRTY_CAS|CLIENT_DIRTY_EXEC))
        return;
    /* 如果客户端的事务中没有命令，也就是刚初始化之后的状态 */
    if (c->mstate.count == 0) {
        /* If a client is using multi/exec, assuming it is used to execute at least
         * two commands. Hence, creating by default size of 2. */
        /* 默认认为一个 multi 至少有两个命令，所以第一次添加命令会分配两个命令的空间 */
        c->mstate.commands = zmalloc(sizeof(multiCmd)*2);
        /* 设置分配的空间为 2 */
        c->mstate.alloc_count = 2;
    }
    /* 这里是分配的空间和已经使用的空间相同，所以现在需要对事务命令队列扩容了 */
    if (c->mstate.count == c->mstate.alloc_count) {
        /* 这里就是双倍扩容，并且扩容后的大小不能整数越界 */
        c->mstate.alloc_count = c->mstate.alloc_count < INT_MAX/2 ? c->mstate.alloc_count*2 : INT_MAX;
        c->mstate.commands = zrealloc(c->mstate.commands, sizeof(multiCmd)*(c->mstate.alloc_count));
    }
    /* 将 mc 指针移动到事务命令队列中空闲空间的位置 */
    mc = c->mstate.commands+c->mstate.count;
    /* 下面是根据客户端目前持有的命令生成一个 multiCmd 实例 */
    mc->cmd = c->cmd;
    mc->argc = c->argc;
    mc->argv = c->argv;
    mc->argv_len = c->argv_len;

    /* 更新事务状态 */
    c->mstate.count++;
    /* 将添加的命令的 flags 设置到 cmd_flags 中 */
    c->mstate.cmd_flags |= cmd_flags;
    c->mstate.cmd_inv_flags |= ~cmd_flags;
    c->mstate.argv_len_sums += c->argv_len_sum + sizeof(robj*)*c->argc;

    /* Reset the client's args since we copied them into the mstate and shouldn't
     * reference them from c anymore. */
    /* 清空客户端的命令信息 */
    c->argv = NULL;
    c->argc = 0;
    c->argv_len_sum = 0;
    c->argv_len = 0;
}

/* 丢弃给定客户端的事务 */
void discardTransaction(client *c) {
    /* 释放客户端事务空间 */
    freeClientMultiState(c);
    /* 初始化客户端事务 */
    initClientMultiState(c);
    /* 将客户端与事务有关的标识清除掉 */
    c->flags &= ~(CLIENT_MULTI|CLIENT_DIRTY_CAS|CLIENT_DIRTY_EXEC);
    /* 取消当前客户端对键的监视 */
    unwatchAllKeys(c);
}

/* Flag the transaction as DIRTY_EXEC so that EXEC will fail.
 * Should be called every time there is an error while queueing a command. */
/* 当入队命令出错了，会标记不能执行 exec */
void flagTransaction(client *c) {
    if (c->flags & CLIENT_MULTI)
        c->flags |= CLIENT_DIRTY_EXEC;
}

void multiCommand(client *c) {
    /* 不能在事务中嵌套事务 */
    if (c->flags & CLIENT_MULTI) {
        addReplyError(c,"MULTI calls can not be nested");
        return;
    }
    /* 设置给定客户端存在事务 */
    c->flags |= CLIENT_MULTI;

    addReply(c,shared.ok);
}

/* 丢弃给定客户端的事务 */
void discardCommand(client *c) {
    if (!(c->flags & CLIENT_MULTI)) {
        addReplyError(c,"DISCARD without MULTI");
        return;
    }
    discardTransaction(c);
    addReply(c,shared.ok);
}

/* Aborts a transaction, with a specific error message.
 * The transaction is always aborted with -EXECABORT so that the client knows
 * the server exited the multi state, but the actual reason for the abort is
 * included too.
 * Note: 'error' may or may not end with \r\n. see addReplyErrorFormat. */
void execCommandAbort(client *c, sds error) {
    discardTransaction(c);

    if (error[0] == '-') error++;
    addReplyErrorFormat(c, "-EXECABORT Transaction discarded because of: %s", error);

    /* Send EXEC to clients waiting data from MONITOR. We did send a MULTI
     * already, and didn't send any of the queued commands, now we'll just send
     * EXEC so it is clear that the transaction is over. */
    replicationFeedMonitors(c,server.monitors,c->db->id,c->argv,c->argc);
}

/* 执行 exec 命令 */
void execCommand(client *c) {
    int j;
    robj **orig_argv;
    int orig_argc, orig_argv_len;
    struct redisCommand *orig_cmd;

    /* 没有事务，返回 */
    if (!(c->flags & CLIENT_MULTI)) {
        addReplyError(c,"EXEC without MULTI");
        return;
    }

    /* EXEC with expired watched key is disallowed*/
    /* 如果被监视的 key 过期，添加 CLIENT_DIRTY_CAS 标识 */
    if (isWatchedKeyExpired(c)) {
        c->flags |= (CLIENT_DIRTY_CAS);
    }

    /* Check if we need to abort the EXEC because:
     * 1) Some WATCHed key was touched.
     * 2) There was a previous error while queueing commands.
     * A failed EXEC in the first case returns a multi bulk nil object
     * (technically it is not an error but a special behavior), while
     * in the second an EXECABORT error is returned. */
    /* 如果出现下面的情况需要丢弃 exec
     * 1）存在被监视的 key 已经被其他客户端持有
     * 2) 入队命令的时候出错了
     */
    if (c->flags & (CLIENT_DIRTY_CAS | CLIENT_DIRTY_EXEC)) {
        if (c->flags & CLIENT_DIRTY_EXEC) {
            addReplyErrorObject(c, shared.execaborterr);
        } else {
            addReply(c, shared.nullarray[c->resp]);
        }

        discardTransaction(c);
        return;
    }

    uint64_t old_flags = c->flags;

    /* we do not want to allow blocking commands inside multi */
    c->flags |= CLIENT_DENY_BLOCKING;

    /* Exec all the queued commands */
    /* 在执行命令前，先取消对 key 的监视，防止 cpu 资源的浪费 */
    unwatchAllKeys(c); /* Unwatch ASAP otherwise we'll waste CPU cycles */

    server.in_exec = 1;

    /* 先备份这些命令和参数，防止在执行中命令和参数被修改 */
    orig_argv = c->argv;
    orig_argv_len = c->argv_len;
    orig_argc = c->argc;
    orig_cmd = c->cmd;
    addReplyArrayLen(c,c->mstate.count);
    for (j = 0; j < c->mstate.count; j++) {
        /* 因为 redis 的命令必须在客户端的上下文中执行，
         * 所以需要将事务队列中的命令，参数设置到客户端上 */
        c->argc = c->mstate.commands[j].argc;
        c->argv = c->mstate.commands[j].argv;
        c->argv_len = c->mstate.commands[j].argv_len;
        c->cmd = c->realcmd = c->mstate.commands[j].cmd;

        /* ACL permissions are also checked at the time of execution in case
         * they were changed after the commands were queued. */
        int acl_errpos;
        int acl_retval = ACLCheckAllPerm(c,&acl_errpos);
        if (acl_retval != ACL_OK) {
            char *reason;
            switch (acl_retval) {
            case ACL_DENIED_CMD:
                reason = "no permission to execute the command or subcommand";
                break;
            case ACL_DENIED_KEY:
                reason = "no permission to touch the specified keys";
                break;
            case ACL_DENIED_CHANNEL:
                reason = "no permission to access one of the channels used "
                         "as arguments";
                break;
            default:
                reason = "no permission";
                break;
            }
            addACLLogEntry(c,acl_retval,ACL_LOG_CTX_MULTI,acl_errpos,NULL,NULL);
            addReplyErrorFormat(c,
                "-NOPERM ACLs rules changed between the moment the "
                "transaction was accumulated and the EXEC call. "
                "This command is no longer allowed for the "
                "following reason: %s", reason);
        } else {
            /* 执行命令 */
            if (c->id == CLIENT_ID_AOF)
                call(c,CMD_CALL_NONE);
            else
                call(c,CMD_CALL_FULL);

            serverAssert((c->flags & CLIENT_BLOCKED) == 0);
        }

        /* Commands may alter argc/argv, restore mstate. */
        c->mstate.commands[j].argc = c->argc;
        c->mstate.commands[j].argv = c->argv;
        c->mstate.commands[j].argv_len = c->argv_len;
        c->mstate.commands[j].cmd = c->cmd;
    }

    // restore old DENY_BLOCKING value
    if (!(old_flags & CLIENT_DENY_BLOCKING))
        c->flags &= ~CLIENT_DENY_BLOCKING;

    /* 还原命令信息 */
    c->argv = orig_argv;
    c->argv_len = orig_argv_len;
    c->argc = orig_argc;
    c->cmd = c->realcmd = orig_cmd;
    /* 清除事务 */
    discardTransaction(c);

    server.in_exec = 0;
}

/* ===================== WATCH (CAS alike for MULTI/EXEC) ===================
 *
 * The implementation uses a per-DB hash table mapping keys to list of clients
 * WATCHing those keys, so that given a key that is going to be modified
 * we can mark all the associated clients as dirty.
 *
 * Also every client contains a list of WATCHed keys so that's possible to
 * un-watch such keys when the client is freed or when UNWATCH is called. */

/* In the client->watched_keys list we need to use watchedKey structures
 * as in order to identify a key in Redis we need both the key name and the
 * DB. This struct is also referenced from db->watched_keys dict, where the
 * values are lists of watchedKey pointers. */
/* 在监视一个键的时候，需要保存被监视的键，也需要保存该键所在的数据库 */
typedef struct watchedKey {
    /* 被监视的键 */
    robj *key;
    /* 键所在的数据库 */
    redisDb *db;
    /* 监视该 key 的客户端 */
    client *client;
    /* 标识被监视的 key 是否已经过期 */
    unsigned expired:1; /* Flag that we're watching an already expired key. */
} watchedKey;

/* Watch for the specified key */
/* 让客户端监视给定的 key */
void watchForKey(client *c, robj *key) {
    list *clients = NULL;
    listIter li;
    listNode *ln;
    watchedKey *wk;

    /* Check if we are already watching for this key */
    /* 下面是判断给定的客户端是否已经在监视这个 key，如果是就不需要再监视了 */
    listRewind(c->watched_keys,&li);
    while((ln = listNext(&li))) {
        wk = listNodeValue(ln);
        if (wk->db == c->db && equalStringObjects(key,wk->key))
            return; /* Key already watched */
    }
    /* This key is not already watched in this DB. Let's add it */
    /* 判断数据库该 key 是否有被监视 */
    clients = dictFetchValue(c->db->watched_keys,key);
    /* 如果没有被监视，需要将其添加到被监视的 key 字典中 */
    if (!clients) {
        /* 创建一个列表 */
        clients = listCreate();
        /* 将 key 加入 watched_keys 字典中 */
        dictAdd(c->db->watched_keys,key,clients);
        incrRefCount(key);
    }
    /* Add the new key to the list of keys watched by this client */
    /* 根据该 key 的信息，生成一个 watchedKey 实例 */
    wk = zmalloc(sizeof(*wk));
    wk->key = key;
    wk->client = c;
    wk->db = c->db;
    wk->expired = keyIsExpired(c->db, key);
    incrRefCount(key);
    /* 将实例化的 watchedKey 添加到监视客户端的 watched_keys 链表中 */
    listAddNodeTail(c->watched_keys,wk);
    listAddNodeTail(clients,wk);
}

/* Unwatch all the keys watched by this client. To clean the EXEC dirty
 * flag is up to the caller. */
/* 取消客户端对所有键的监视 */
void unwatchAllKeys(client *c) {
    listIter li;
    listNode *ln;

    if (listLength(c->watched_keys) == 0) return;
    /* 遍历给定客户端的所有监视的 key */
    listRewind(c->watched_keys,&li);
    while((ln = listNext(&li))) {
        list *clients;
        watchedKey *wk;

        /* Lookup the watched key -> clients list and remove the client's wk
         * from the list */
        wk = listNodeValue(ln);
        /* 从数据库的 watched_keys 字典中，查找监视该 key 的客户端列表 */
        clients = dictFetchValue(wk->db->watched_keys, wk->key);
        serverAssertWithInfo(c,NULL,clients != NULL);
        /* 从上面拿到的客户端列表中移除当前客户端 */
        listDelNode(clients,listSearchKey(clients,wk));
        /* Kill the entry at all if this was the only client */
        /* 如果移除当前客户端之后，列表为 null 了，需要从数据库的 watched_keys 中移除该 key */
        if (listLength(clients) == 0)
            dictDelete(wk->db->watched_keys, wk->key);
        /* Remove this watched key from the client->watched list */
        /* 删除客户端的 watched_keys 中对应的监视键 */
        listDelNode(c->watched_keys,ln);
        decrRefCount(wk->key);
        /* 释放该 watched_key 结构 */
        zfree(wk);
    }
}

/* Iterates over the watched_keys list and looks for an expired key. Keys which
 * were expired already when WATCH was called are ignored. */
 /* 判断给定客户端监视的 key 是否存在过期 key */
int isWatchedKeyExpired(client *c) {
    listIter li;
    listNode *ln;
    watchedKey *wk;
    if (listLength(c->watched_keys) == 0) return 0;
    /* 遍历所有客户端监视的 key，判断每一个 key 是否过期 */
    listRewind(c->watched_keys,&li);
    while ((ln = listNext(&li))) {
        wk = listNodeValue(ln);
        if (wk->expired) continue; /* was expired when WATCH was called */
        if (keyIsExpired(wk->db, wk->key)) return 1;
    }

    return 0;
}

/* "Touch" a key, so that if this key is being WATCHed by some client the
 * next EXEC will fail. */
/* 如果 key 正在被其他客户端监视，执行 exec 将会失败 */
void touchWatchedKey(redisDb *db, robj *key) {
    list *clients;
    listIter li;
    listNode *ln;

    /* 如果数据库中没有被监视的 key，直接范湖 */
    if (dictSize(db->watched_keys) == 0) return;
    /* 获取监视给定 key 的客户端列表 */
    clients = dictFetchValue(db->watched_keys, key);
    /* 没有客户端监视该 key，返回 */
    if (!clients) return;

    /* Mark all the clients watching this key as CLIENT_DIRTY_CAS */
    /* Check if we are already watching for this key */
    /* 遍历所有监视该 key 的客户端 */
    listRewind(clients,&li);
    while((ln = listNext(&li))) {
        watchedKey *wk = listNodeValue(ln);
        client *c = wk->client;

        /* 如果客户端的 watched_key 过期了 */
        if (wk->expired) {
            /* The key was already expired when WATCH was called. */
            /* 判断是否 watched_key 对应的 key 是否在数据库中已经被清除了
             * 如果清除了，可以清除该 watched_key 的过期标识 */
            if (db == wk->db &&
                equalStringObjects(key, wk->key) &&
                dictFind(db->dict, key->ptr) == NULL)
            {
                /* Already expired key is deleted, so logically no change. Clear
                 * the flag. Deleted keys are not flagged as expired. */
                wk->expired = 0;
                /* 这里会使用 continue 将所有监视该 key 的客户端的对应的 watched_key 的过期标识进行修改 */
                goto skip_client;
            }
            /* 如果 watched_key 过期，但是对应的 key 还存在，直接退出当前循环 */
            break;
        }

        /* 没过期的情况，设置 CLIENT_DIRTY_CAS 标识，需要取消客户端对所有键的监视 */
        c->flags |= CLIENT_DIRTY_CAS;
        /* As the client is marked as dirty, there is no point in getting here
         * again in case that key (or others) are modified again (or keep the
         * memory overhead till EXEC). */
        unwatchAllKeys(c);

    skip_client:
        continue;
    }
}

/* Set CLIENT_DIRTY_CAS to all clients of DB when DB is dirty.
 * It may happen in the following situations:
 * FLUSHDB, FLUSHALL, SWAPDB, end of successful diskless replication.
 *
 * replaced_with: for SWAPDB, the WATCH should be invalidated if
 * the key exists in either of them, and skipped only if it
 * doesn't exist in both. */
/* 如果 redis 处于 FLUSHDB, FLUSHALL, SWAPDB, diskless replication 成功之后这几种情况下，
 * 需要给客户端添加 CLIENT_DIRTY_CAS 标识，*/
void touchAllWatchedKeysInDb(redisDb *emptied, redisDb *replaced_with) {
    listIter li;
    listNode *ln;
    dictEntry *de;

    if (dictSize(emptied->watched_keys) == 0) return;

    dictIterator *di = dictGetSafeIterator(emptied->watched_keys);
    while((de = dictNext(di)) != NULL) {
        robj *key = dictGetKey(de);
        int exists_in_emptied = dictFind(emptied->dict, key->ptr) != NULL;
        if (exists_in_emptied ||
            (replaced_with && dictFind(replaced_with->dict, key->ptr)))
        {
            list *clients = dictGetVal(de);
            if (!clients) continue;
            listRewind(clients,&li);
            while((ln = listNext(&li))) {
                watchedKey *wk = listNodeValue(ln);
                if (wk->expired) {
                    if (!replaced_with || !dictFind(replaced_with->dict, key->ptr)) {
                        /* Expired key now deleted. No logical change. Clear the
                         * flag. Deleted keys are not flagged as expired. */
                        wk->expired = 0;
                        continue;
                    } else if (keyIsExpired(replaced_with, key)) {
                        /* Expired key remains expired. */
                        continue;
                    }
                } else if (!exists_in_emptied && keyIsExpired(replaced_with, key)) {
                    /* Non-existing key is replaced with an expired key. */
                    wk->expired = 1;
                    continue;
                }
                client *c = wk->client;
                /* 设置 CLIENT_DIRTY_CAS 标识 */
                c->flags |= CLIENT_DIRTY_CAS;
                /* As the client is marked as dirty, there is no point in getting here
                 * again for others keys (or keep the memory overhead till EXEC). */
                unwatchAllKeys(c);
            }
        }
    }
    dictReleaseIterator(di);
}

void watchCommand(client *c) {
    int j;

    /* 当客户端已经标识了 multi，就不能在使用 watch 命令 */
    if (c->flags & CLIENT_MULTI) {
        addReplyError(c,"WATCH inside MULTI is not allowed");
        return;
    }
    /* No point in watching if the client is already dirty. */
    /* 如果设置了 CLIENT_DIRTY_CAS 标识，不能使用 watch 命令 */
    if (c->flags & CLIENT_DIRTY_CAS) {
        addReply(c,shared.ok);
        return;
    }
    /* 遍历所有参数 */
    for (j = 1; j < c->argc; j++)
        /* 监视参数中的 key */
        watchForKey(c,c->argv[j]);
    addReply(c,shared.ok);
}

void unwatchCommand(client *c) {
    /* 取消客户端所监视的 key */
    unwatchAllKeys(c);
    /* 清除 CLIENT_DIRTY_CAS 标识 */
    c->flags &= (~CLIENT_DIRTY_CAS);
    addReply(c,shared.ok);
}

size_t multiStateMemOverhead(client *c) {
    size_t mem = c->mstate.argv_len_sums;
    /* Add watched keys overhead, Note: this doesn't take into account the watched keys themselves, because they aren't managed per-client. */
    mem += listLength(c->watched_keys) * (sizeof(listNode) + sizeof(watchedKey));
    /* Reserved memory for queued multi commands. */
    mem += c->mstate.alloc_count * sizeof(multiCmd);
    return mem;
}
