/*
 * Copyright (c) 2009-2021, Redis Ltd.
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
#include "script.h"
#include "cluster.h"

/* 支持的标记 */
scriptFlag scripts_flags_def[] = {
    {.flag = SCRIPT_FLAG_NO_WRITES, .str = "no-writes"},
    {.flag = SCRIPT_FLAG_ALLOW_OOM, .str = "allow-oom"},
    {.flag = SCRIPT_FLAG_ALLOW_STALE, .str = "allow-stale"},
    {.flag = SCRIPT_FLAG_NO_CLUSTER, .str = "no-cluster"},
    {.flag = SCRIPT_FLAG_ALLOW_CROSS_SLOT, .str = "allow-cross-slot-keys"},
    {.flag = 0, .str = NULL}, /* flags array end */
};

/* On script invocation, holding the current run context */
static scriptRunCtx *curr_run_ctx = NULL;

/* 移除脚本的超时模式 */
static void exitScriptTimedoutMode(scriptRunCtx *run_ctx) {
    serverAssert(run_ctx == curr_run_ctx);
    serverAssert(scriptIsTimedout());
    run_ctx->flags &= ~SCRIPT_TIMEDOUT;
    blockingOperationEnds();
    /* if we are a replica and we have an active master, set it for continue processing */
    if (server.masterhost && server.master) queueClientForReprocessing(server.master);
}

/* 确认脚本进入了超时模式 */
static void enterScriptTimedoutMode(scriptRunCtx *run_ctx) {
    serverAssert(run_ctx == curr_run_ctx);
    serverAssert(!scriptIsTimedout());
    /* Mark script as timedout */
    run_ctx->flags |= SCRIPT_TIMEDOUT;
    blockingOperationStarts();
}

int scriptIsTimedout() {
    return scriptIsRunning() && (curr_run_ctx->flags & SCRIPT_TIMEDOUT);
}

client* scriptGetClient() {
    serverAssert(scriptIsRunning());
    return curr_run_ctx->c;
}

client* scriptGetCaller() {
    serverAssert(scriptIsRunning());
    return curr_run_ctx->original_client;
}

/* interrupt function for scripts, should be call
 * from time to time to reply some special command (like ping)
 * and also check if the run should be terminated. */

/* 脚本的中断函数，应不时调用以回复某些特殊命令（如ping），并检查运行是否应终止 */
int scriptInterrupt(scriptRunCtx *run_ctx) {
    if (run_ctx->flags & SCRIPT_TIMEDOUT) {
        /* script already timedout
           we just need to precess some events and return */

        /* 脚本已经超时，我们只需要处理一些事件并返回 */
        processEventsWhileBlocked();
        return (run_ctx->flags & SCRIPT_KILLED) ? SCRIPT_KILL : SCRIPT_CONTINUE;
    }

    long long elapsed = elapsedMs(run_ctx->start_time);
    if (elapsed < server.busy_reply_threshold) {
        return SCRIPT_CONTINUE;
    }

    serverLog(LL_WARNING,
            "Slow script detected: still in execution after %lld milliseconds. "
                    "You can try killing the script using the %s command. Script name is: %s.",
            elapsed, (run_ctx->flags & SCRIPT_EVAL_MODE) ? "SCRIPT KILL" : "FUNCTION KILL", run_ctx->funcname);

    enterScriptTimedoutMode(run_ctx);
    /* Once the script timeouts we reenter the event loop to permit others
     * some commands execution. For this reason
     * we need to mask the client executing the script from the event loop.
     * If we don't do that the client may disconnect and could no longer be
     * here when the EVAL command will return. */

    /* 一旦脚本超时，我们就重新进入事件循环以允许其他人执行一些命令。因此，我们需要从事件循环中屏蔽执行脚本的客户端。
     * 如果我们不这样做，当 EVAL 命令返回时，客户端可能会断开连接，并且不再在这里*/
    protectClient(run_ctx->original_client);

    /* 尝试去处理 eventLoop */
    processEventsWhileBlocked();

    return (run_ctx->flags & SCRIPT_KILLED) ? SCRIPT_KILL : SCRIPT_CONTINUE;
}

/* 把脚本的 flag 转换为 cmd 的 flag，包括类似禁止 OOM，可写命令 */
uint64_t scriptFlagsToCmdFlags(uint64_t cmd_flags, uint64_t script_flags) {
    /* If the script declared flags, clear the ones from the command and use the ones it declared.*/
    cmd_flags &= ~(CMD_STALE | CMD_DENYOOM | CMD_WRITE);

    /* NO_WRITES implies ALLOW_OOM */
    if (!(script_flags & (SCRIPT_FLAG_ALLOW_OOM | SCRIPT_FLAG_NO_WRITES)))
        cmd_flags |= CMD_DENYOOM;
    if (!(script_flags & SCRIPT_FLAG_NO_WRITES))
        cmd_flags |= CMD_WRITE;
    if (script_flags & SCRIPT_FLAG_ALLOW_STALE)
        cmd_flags |= CMD_STALE;

    /* In addition the MAY_REPLICATE flag is set for these commands, but
     * if we have flags we know if it's gonna do any writes or not. */

    /* 此外，为这些命令设置了 MAY_REPLICATE 标志，但如果我们有标志，我们就知道它是否会进行任何写入 */
    cmd_flags &= ~CMD_MAY_REPLICATE;

    return cmd_flags;
}

/* Prepare the given run ctx for execution */
int scriptPrepareForRun(scriptRunCtx *run_ctx, client *engine_client, client *caller, const char *funcname, uint64_t script_flags, int ro) {
    serverAssert(!curr_run_ctx);

    /* 从节点、且当前主从复制是不正常的 */
    int running_stale = server.masterhost &&
            server.repl_state != REPL_STATE_CONNECTED &&
            server.repl_serve_stale_data == 0;
    /* 是否是一个 master client 或者是一个 aof client */
    int obey_client = mustObeyClient(caller);

    /* 存在特殊的标记 */
    if (!(script_flags & SCRIPT_FLAG_EVAL_COMPAT_MODE)) {
        /* 不允许 cluster 模式下执行 */
        if ((script_flags & SCRIPT_FLAG_NO_CLUSTER) && server.cluster_enabled) {
            addReplyError(caller, "Can not run script on cluster, 'no-cluster' flag is set.");
            return C_ERR;
        }

        /* 判断是否允许故障模式下执行 */
        if (running_stale && !(script_flags & SCRIPT_FLAG_ALLOW_STALE)) {
            addReplyError(caller, "-MASTERDOWN Link with MASTER is down, "
                             "replica-serve-stale-data is set to 'no' "
                             "and 'allow-stale' flag is not set on the script.");
            return C_ERR;
        }

        /* 判断是否允许写入操作 */
        if (!(script_flags & SCRIPT_FLAG_NO_WRITES)) {
            /* Script may perform writes we need to verify:
             * 1. we are not a readonly replica
             * 2. no disk error detected
             * 3. command is not `fcall_ro`/`eval[sha]_ro` */

            /* 脚本可能会执行我们需要验证的写入：
             * 1.我们不是只读副本
             * 2.未检测到磁盘错误
             * 3.命令不是 `fcall_ro`/`eval[sha]_ro` */
            if (server.masterhost && server.repl_slave_ro && !obey_client) {
                addReplyError(caller, "-READONLY Can not run script with write flag on readonly replica");
                return C_ERR;
            }

            /* Deny writes if we're unale to persist. */
            /* 当前写磁盘错误 */
            int deny_write_type = writeCommandsDeniedByDiskError();
            if (deny_write_type != DISK_ERROR_TYPE_NONE && !obey_client) {
                if (deny_write_type == DISK_ERROR_TYPE_RDB)
                    addReplyError(caller, "-MISCONF Redis is configured to save RDB snapshots, "
                                     "but it's currently unable to persist to disk. "
                                     "Writable scripts are blocked. Use 'no-writes' flag for read only scripts.");
                else
                    addReplyErrorFormat(caller, "-MISCONF Redis is configured to persist data to AOF, "
                                           "but it's currently unable to persist to disk. "
                                           "Writable scripts are blocked. Use 'no-writes' flag for read only scripts. "
                                           "AOF error: %s", strerror(server.aof_last_write_errno));
                return C_ERR;
            }

            /* 当前属于只读模式 */
            if (ro) {
                addReplyError(caller, "Can not execute a script with write flag using *_ro command.");
                return C_ERR;
            }

            /* Don't accept write commands if there are not enough good slaves and
             * user configured the min-slaves-to-write option. */

            /* 当前的主节点是否可用 */
            if (server.masterhost == NULL &&
                server.repl_min_slaves_max_lag &&
                server.repl_min_slaves_to_write &&
                server.repl_good_slaves_count < server.repl_min_slaves_to_write)
            {
                addReplyErrorObject(caller, shared.noreplicaserr);
                return C_ERR;
            }
        }

        /* Check OOM state. the no-writes flag imply allow-oom. we tested it
         * after the no-write error, so no need to mention it in the error reply. */

        /* 检查 OOM 状态。no writes 标志表示允许 oom 。我们在无写错误之后测试了它，因此无需在错误回复中提及它 */
        if (server.pre_command_oom_state && server.maxmemory &&
            !(script_flags & (SCRIPT_FLAG_ALLOW_OOM|SCRIPT_FLAG_NO_WRITES)))
        {
            addReplyError(caller, "-OOM allow-oom flag is not set on the script, "
                                  "can not run it when used memory > 'maxmemory'");
            return C_ERR;
        }

    } else {
        /* Special handling for backwards compatibility (no shebang eval[sha]) mode */
        /* 向后兼容（无 shebang eval[sha]）模式的特殊处理, 如果当前服务故障中，则返回错误 */
        if (running_stale) {
            addReplyErrorObject(caller, shared.masterdownerr);
            return C_ERR;
        }
    }

    run_ctx->c = engine_client;
    run_ctx->original_client = caller;
    run_ctx->funcname = funcname;

    /* 绑定 client 和上下文的关系 */
    client *script_client = run_ctx->c;
    client *curr_client = run_ctx->original_client;
    server.script_caller = curr_client;

    /* Select the right DB in the context of the Lua client */
    selectDb(script_client, curr_client->db->id);
    /* 设置协议为2 */
    script_client->resp = 2; /* Default is RESP2, scripts can change it. */

    /* If we are in MULTI context, flag Lua client as CLIENT_MULTI. */
    /* 透传事务标识 */
    if (curr_client->flags & CLIENT_MULTI) {
        script_client->flags |= CLIENT_MULTI;
    }

    /* 初始化相关时间信息, 前者用来判断执行时间, 后者用于 key 的过期判断 */
    run_ctx->start_time = getMonotonicUs();
    run_ctx->snapshot_time = mstime();

    run_ctx->flags = 0;
    /* 默认设置 aof 和 repl 标记 */
    run_ctx->repl_flags = PROPAGATE_AOF | PROPAGATE_REPL;

    /* 设置只读标记 */
    if (ro || (!(script_flags & SCRIPT_FLAG_EVAL_COMPAT_MODE) && (script_flags & SCRIPT_FLAG_NO_WRITES))) {
        /* On fcall_ro or on functions that do not have the 'write'
         * flag, we will not allow write commands. */
        run_ctx->flags |= SCRIPT_READ_ONLY;
    }
    /* 设置 OOM 标记 */
    if (!(script_flags & SCRIPT_FLAG_EVAL_COMPAT_MODE) && (script_flags & SCRIPT_FLAG_ALLOW_OOM)) {
        /* Note: we don't need to test the no-writes flag here and set this run_ctx flag,
         * since only write commands can are deny-oom. */
        run_ctx->flags |= SCRIPT_ALLOW_OOM;
    }

    /* 设置允许跨分片标记 */
    if ((script_flags & SCRIPT_FLAG_EVAL_COMPAT_MODE) || (script_flags & SCRIPT_FLAG_ALLOW_CROSS_SLOT)) {
        run_ctx->flags |= SCRIPT_ALLOW_CROSS_SLOT;
    }

    /* set the curr_run_ctx so we can use it to kill the script if needed */
    curr_run_ctx = run_ctx;

    return C_OK;
}

/* Reset the given run ctx after execution */

/* 执行后重置给定的运行 ctx */
void scriptResetRun(scriptRunCtx *run_ctx) {
    serverAssert(curr_run_ctx);

    /* After the script done, remove the MULTI state. */

    /* 脚本完成后，删除 MULTI 状态 */
    run_ctx->c->flags &= ~CLIENT_MULTI;

    server.script_caller = NULL;

    if (scriptIsTimedout()) {
        exitScriptTimedoutMode(run_ctx);
        /* Restore the client that was protected when the script timeout
         * was detected. */
        unprotectClient(run_ctx->original_client);
    }

    preventCommandPropagation(run_ctx->original_client);

    /*  unset curr_run_ctx so we will know there is no running script */
    curr_run_ctx = NULL;
}

/* return true if a script is currently running */
int scriptIsRunning() {
    return curr_run_ctx != NULL;
}

const char* scriptCurrFunction() {
    serverAssert(scriptIsRunning());
    return curr_run_ctx->funcname;
}

int scriptIsEval() {
    serverAssert(scriptIsRunning());
    return curr_run_ctx->flags & SCRIPT_EVAL_MODE;
}

/* Kill the current running script */

/* 终止当前运行的脚本 */
void scriptKill(client *c, int is_eval) {
    if (!curr_run_ctx) {
        addReplyError(c, "-NOTBUSY No scripts in execution right now.");
        return;
    }
    if (mustObeyClient(curr_run_ctx->original_client)) {
        addReplyError(c,
                "-UNKILLABLE The busy script was sent by a master instance in the context of replication and cannot be killed.");
    }
    /* 如果数据已修改，则禁止终止 */
    if (curr_run_ctx->flags & SCRIPT_WRITE_DIRTY) {
        addReplyError(c,
                "-UNKILLABLE Sorry the script already executed write "
                        "commands against the dataset. You can either wait the "
                        "script termination or kill the server in a hard way "
                        "using the SHUTDOWN NOSAVE command.");
        return;
    }
    if (is_eval && !(curr_run_ctx->flags & SCRIPT_EVAL_MODE)) {
        /* Kill a function with 'SCRIPT KILL' is not allow */
        addReplyErrorObject(c, shared.slowscripterr);
        return;
    }
    if (!is_eval && (curr_run_ctx->flags & SCRIPT_EVAL_MODE)) {
        /* Kill an eval with 'FUNCTION KILL' is not allow */
        addReplyErrorObject(c, shared.slowevalerr);
        return;
    }
    curr_run_ctx->flags |= SCRIPT_KILLED;
    addReply(c, shared.ok);
}

static int scriptVerifyCommandArity(struct redisCommand *cmd, int argc, sds *err) {
    if (!cmd || ((cmd->arity > 0 && cmd->arity != argc) || (argc < -cmd->arity))) {
        if (cmd)
            *err = sdsnew("Wrong number of args calling Redis command from script");
        else
            *err = sdsnew("Unknown Redis command called from script");
        return C_ERR;
    }
    return C_OK;
}

static int scriptVerifyACL(client *c, sds *err) {
    /* Check the ACLs. */
    int acl_errpos;
    int acl_retval = ACLCheckAllPerm(c, &acl_errpos);
    if (acl_retval != ACL_OK) {
        addACLLogEntry(c,acl_retval,ACL_LOG_CTX_LUA,acl_errpos,NULL,NULL);
        *err = sdscatfmt(sdsempty(), "The user executing the script %s", getAclErrorMessage(acl_retval));
        return C_ERR;
    }
    return C_OK;
}

/* 校验当前写命令是否能执行成功 */
static int scriptVerifyWriteCommandAllow(scriptRunCtx *run_ctx, char **err) {

    /* A write command, on an RO command or an RO script is rejected ASAP.
     * Note: For scripts, we consider may-replicate commands as write commands.
     * This also makes it possible to allow read-only scripts to be run during
     * CLIENT PAUSE WRITE. */

    /* RO 命令或 RO 脚本上的写入命令将被尽快拒绝。
     * 注意：对于脚本，我们认为可以将命令复制为写命令。 */
    if (run_ctx->flags & SCRIPT_READ_ONLY &&
        (run_ctx->c->cmd->flags & (CMD_WRITE|CMD_MAY_REPLICATE)))
    {
        *err = sdsnew("Write commands are not allowed from read-only scripts.");
        return C_ERR;
    }

    /* The other checks below are on the server state and are only relevant for
     *  write commands, return if this is not a write command. */

    /* 下面的其他检查针对服务器状态，仅与写命令相关，如果这不是写命令，则返回 */
    if (!(run_ctx->c->cmd->flags & CMD_WRITE))
        return C_OK;

    /* If the script already made a modification to the dataset, we can't
     * fail it on unpredictable error state. */

    /* 如果脚本已经对数据集进行了修改，我们不能在不可预测的错误状态下使其失败 */
    if ((run_ctx->flags & SCRIPT_WRITE_DIRTY))
        return C_OK;

    /* Write commands are forbidden against read-only slaves, or if a
     * command marked as non-deterministic was already called in the context
     * of this script. */

    /* 禁止对只读从属设备执行写入命令，或者如果在脚本上下文中已调用了标记为非确定性的命令，则禁止执行写入命令 */
    int deny_write_type = writeCommandsDeniedByDiskError();

    if (server.masterhost && server.repl_slave_ro &&
        !mustObeyClient(run_ctx->original_client))
    {
        *err = sdsdup(shared.roslaveerr->ptr);
        return C_ERR;
    }

    if (deny_write_type != DISK_ERROR_TYPE_NONE) {
        *err = writeCommandsGetDiskErrorMessage(deny_write_type);
        return C_ERR;
    }

    /* Don't accept write commands if there are not enough good slaves and
     * user configured the min-slaves-to-write option. Note this only reachable
     * for Eval scripts that didn't declare flags, see the other check in
     * scriptPrepareForRun */

    /* 如果没有足够好的从机，并且用户配置了 min-slavesto-write 选项，则不要接受 write 命令。
     * 请注意，这只适用于未声明标志的 Eval 脚本，请参阅 scriptPrepareForRun 中的其他检查 */
    if (!checkGoodReplicasStatus()) {
        *err = sdsdup(shared.noreplicaserr->ptr);
        return C_ERR;
    }

    return C_OK;
}

static int scriptVerifyOOM(scriptRunCtx *run_ctx, char **err) {
    if (run_ctx->flags & SCRIPT_ALLOW_OOM) {
        /* Allow running any command even if OOM reached */
        return C_OK;
    }

    /* If we reached the memory limit configured via maxmemory, commands that
     * could enlarge the memory usage are not allowed, but only if this is the
     * first write in the context of this script, otherwise we can't stop
     * in the middle. */

    /* 如果我们达到了通过 maxmemory 配置的内存限制，则不允许使用可能扩大内存使用量的命令，但前提是这是该脚本上下文中的第一次写入，否则我们不能中途停止 */
    if (server.maxmemory &&                            /* Maxmemory is actually enabled. */
        !mustObeyClient(run_ctx->original_client) &&   /* Don't care about mem for replicas or AOF. */
        !(run_ctx->flags & SCRIPT_WRITE_DIRTY) &&      /* Script had no side effects so far. */
        server.pre_command_oom_state &&                /* Detected OOM when script start. */
        (run_ctx->c->cmd->flags & CMD_DENYOOM))
    {
        *err = sdsdup(shared.oomerr->ptr);
        return C_ERR;
    }

    return C_OK;
}

static int scriptVerifyClusterState(scriptRunCtx *run_ctx, client *c, client *original_c, sds *err) {
    if (!server.cluster_enabled || mustObeyClient(original_c)) {
        return C_OK;
    }
    /* If this is a Redis Cluster node, we need to make sure the script is not
     * trying to access non-local keys, with the exception of commands
     * received from our master or when loading the AOF back in memory. */

    /* 如果这是一个 Redis Cluster 节点，我们需要确保脚本没有试图访问非本地密钥，除了从我们的主机接收到的命令或将 AOF 加载回内存时 */
    int error_code;
    /* Duplicate relevant flags in the script client. */
    c->flags &= ~(CLIENT_READONLY | CLIENT_ASKING);
    c->flags |= original_c->flags & (CLIENT_READONLY | CLIENT_ASKING);
    int hashslot = -1;
    if (getNodeByQuery(c, c->cmd, c->argv, c->argc, &hashslot, &error_code) != server.cluster->myself) {
        if (error_code == CLUSTER_REDIR_DOWN_RO_STATE) {
            *err = sdsnew(
                    "Script attempted to execute a write command while the "
                            "cluster is down and readonly");
        } else if (error_code == CLUSTER_REDIR_DOWN_STATE) {
            *err = sdsnew("Script attempted to execute a command while the "
                    "cluster is down");
        } else {
            *err = sdsnew("Script attempted to access a non local key in a "
                    "cluster node");
        }
        return C_ERR;
    }

    /* If the script declared keys in advanced, the cross slot error would have
     * already been thrown. This is only checking for cross slot keys being accessed
     * that weren't pre-declared. */

    /* 如果脚本以高级方式声明键，那么交叉槽错误可能已经被抛出。这只是检查正在访问的未预先声明的交叉槽密钥 */
    if (hashslot != -1 && !(run_ctx->flags & SCRIPT_ALLOW_CROSS_SLOT)) {
        if (original_c->slot == -1) {
            original_c->slot = hashslot;
        } else if (original_c->slot != hashslot) {
            *err = sdsnew("Script attempted to access keys that do not hash to "
                    "the same slot");
            return C_ERR;
        }
    }
    return C_OK;
}

/* set RESP for a given run_ctx */
int scriptSetResp(scriptRunCtx *run_ctx, int resp) {
    if (resp != 2 && resp != 3) {
        return C_ERR;
    }

    run_ctx->c->resp = resp;
    return C_OK;
}

/* set Repl for a given run_ctx
 * either: PROPAGATE_AOF | PROPAGATE_REPL*/

/* 设置当前的行为 */
int scriptSetRepl(scriptRunCtx *run_ctx, int repl) {
    if ((repl & ~(PROPAGATE_AOF | PROPAGATE_REPL)) != 0) {
        return C_ERR;
    }
    run_ctx->repl_flags = repl;
    return C_OK;
}

/* 是否禁止故障从节点执行 */
static int scriptVerifyAllowStale(client *c, sds *err) {
    if (!server.masterhost) {
        /* Not a replica, stale is irrelevant */
        return C_OK;
    }

    if (server.repl_state == REPL_STATE_CONNECTED) {
        /* Connected to replica, stale is irrelevant */
        return C_OK;
    }

    if (server.repl_serve_stale_data == 1) {
        /* Disconnected from replica but allow to serve data */
        return C_OK;
    }

    if (c->cmd->flags & CMD_STALE) {
        /* Command is allow while stale */
        return C_OK;
    }

    /* On stale replica, can not run the command */
    *err = sdsnew("Can not execute the command on a stale replica");
    return C_ERR;
}

/* Call a Redis command.
 * The reply is written to the run_ctx client and it is
 * up to the engine to take and parse.
 * The err out variable is set only if error occurs and describe the error.
 * If err is set on reply is written to the run_ctx client. */

/* 调用 Redis 命令。
 * 回复被写入 run_ctx 客户端，由引擎获取和解析。
 * 仅当发生错误并描述错误时，才会设置 err-out 变量。
 * 如果设置了 err，则会将回复写入 run_ctx 客户端 */
void scriptCall(scriptRunCtx *run_ctx, robj* *argv, int argc, sds *err) {
    client *c = run_ctx->c;

    /* Setup our fake client for command execution */
    c->argv = argv;
    c->argc = argc;
    c->user = run_ctx->original_client->user;

    /* Process module hooks */
    moduleCallCommandFilters(c);
    argv = c->argv;
    argc = c->argc;

    /* 查询对应的命令对象 */
    struct redisCommand *cmd = lookupCommand(argv, argc);
    c->cmd = c->lastcmd = c->realcmd = cmd;
    if (scriptVerifyCommandArity(cmd, argc, err) != C_OK) {
        goto error;
    }

    /* There are commands that are not allowed inside scripts. */

    /* 是否禁止调用脚本不可执行的命令 */
    if (!server.script_disable_deny_script && (cmd->flags & CMD_NOSCRIPT)) {
        *err = sdsnew("This Redis command is not allowed from script");
        goto error;
    }

    /* 是否禁止故障从节点执行 */
    if (scriptVerifyAllowStale(c, err) != C_OK) {
        goto error;
    }

    /* 校验权限状态 */
    if (scriptVerifyACL(c, err) != C_OK) {
        goto error;
    }

    /* 校验写命令是否可执行 */
    if (scriptVerifyWriteCommandAllow(run_ctx, err) != C_OK) {
        goto error;
    }

    /* 校验 oom */
    if (scriptVerifyOOM(run_ctx, err) != C_OK) {
        goto error;
    }

    if (cmd->flags & CMD_WRITE) {
        /* signify that we already change the data in this execution */

        /* 表示我们已经更改了此执行中的数据 */
        run_ctx->flags |= SCRIPT_WRITE_DIRTY;
    }

    if (scriptVerifyClusterState(run_ctx, c, run_ctx->original_client, err) != C_OK) {
        goto error;
    }

    int call_flags = CMD_CALL_SLOWLOG | CMD_CALL_STATS;
    if (run_ctx->repl_flags & PROPAGATE_AOF) {
        call_flags |= CMD_CALL_PROPAGATE_AOF;
    }
    if (run_ctx->repl_flags & PROPAGATE_REPL) {
        call_flags |= CMD_CALL_PROPAGATE_REPL;
    }
    call(c, call_flags);
    serverAssert((c->flags & CLIENT_BLOCKED) == 0);
    return;

error:
    afterErrorReply(c, *err, sdslen(*err), 0);
    incrCommandStatsOnError(cmd, ERROR_COMMAND_REJECTED);
}

/* Returns the time when the script invocation started */

/* 返回脚本调用开始的时间 */
mstime_t scriptTimeSnapshot() {
    serverAssert(curr_run_ctx);
    return curr_run_ctx->snapshot_time;
}

long long scriptRunDuration() {
    serverAssert(scriptIsRunning());
    return elapsedMs(curr_run_ctx->start_time);
}
