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

/* ֧�ֵı�� */
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

/* �Ƴ��ű��ĳ�ʱģʽ */
static void exitScriptTimedoutMode(scriptRunCtx *run_ctx) {
    serverAssert(run_ctx == curr_run_ctx);
    serverAssert(scriptIsTimedout());
    run_ctx->flags &= ~SCRIPT_TIMEDOUT;
    blockingOperationEnds();
    /* if we are a replica and we have an active master, set it for continue processing */
    if (server.masterhost && server.master) queueClientForReprocessing(server.master);
}

/* ȷ�Ͻű������˳�ʱģʽ */
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

/* �ű����жϺ�����Ӧ��ʱ�����Իظ�ĳЩ���������ping��������������Ƿ�Ӧ��ֹ */
int scriptInterrupt(scriptRunCtx *run_ctx) {
    if (run_ctx->flags & SCRIPT_TIMEDOUT) {
        /* script already timedout
           we just need to precess some events and return */

        /* �ű��Ѿ���ʱ������ֻ��Ҫ����һЩ�¼������� */
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

    /* һ���ű���ʱ�����Ǿ����½����¼�ѭ��������������ִ��һЩ�����ˣ�������Ҫ���¼�ѭ��������ִ�нű��Ŀͻ��ˡ�
     * ������ǲ����������� EVAL �����ʱ���ͻ��˿��ܻ�Ͽ����ӣ����Ҳ���������*/
    protectClient(run_ctx->original_client);

    /* ����ȥ���� eventLoop */
    processEventsWhileBlocked();

    return (run_ctx->flags & SCRIPT_KILLED) ? SCRIPT_KILL : SCRIPT_CONTINUE;
}

/* �ѽű��� flag ת��Ϊ cmd �� flag���������ƽ�ֹ OOM����д���� */
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

    /* ���⣬Ϊ��Щ���������� MAY_REPLICATE ��־������������б�־�����Ǿ�֪�����Ƿ������κ�д�� */
    cmd_flags &= ~CMD_MAY_REPLICATE;

    return cmd_flags;
}

/* Prepare the given run ctx for execution */
int scriptPrepareForRun(scriptRunCtx *run_ctx, client *engine_client, client *caller, const char *funcname, uint64_t script_flags, int ro) {
    serverAssert(!curr_run_ctx);

    /* �ӽڵ㡢�ҵ�ǰ���Ӹ����ǲ������� */
    int running_stale = server.masterhost &&
            server.repl_state != REPL_STATE_CONNECTED &&
            server.repl_serve_stale_data == 0;
    /* �Ƿ���һ�� master client ������һ�� aof client */
    int obey_client = mustObeyClient(caller);

    /* ��������ı�� */
    if (!(script_flags & SCRIPT_FLAG_EVAL_COMPAT_MODE)) {
        /* ������ cluster ģʽ��ִ�� */
        if ((script_flags & SCRIPT_FLAG_NO_CLUSTER) && server.cluster_enabled) {
            addReplyError(caller, "Can not run script on cluster, 'no-cluster' flag is set.");
            return C_ERR;
        }

        /* �ж��Ƿ��������ģʽ��ִ�� */
        if (running_stale && !(script_flags & SCRIPT_FLAG_ALLOW_STALE)) {
            addReplyError(caller, "-MASTERDOWN Link with MASTER is down, "
                             "replica-serve-stale-data is set to 'no' "
                             "and 'allow-stale' flag is not set on the script.");
            return C_ERR;
        }

        /* �ж��Ƿ�����д����� */
        if (!(script_flags & SCRIPT_FLAG_NO_WRITES)) {
            /* Script may perform writes we need to verify:
             * 1. we are not a readonly replica
             * 2. no disk error detected
             * 3. command is not `fcall_ro`/`eval[sha]_ro` */

            /* �ű����ܻ�ִ��������Ҫ��֤��д�룺
             * 1.���ǲ���ֻ������
             * 2.δ��⵽���̴���
             * 3.����� `fcall_ro`/`eval[sha]_ro` */
            if (server.masterhost && server.repl_slave_ro && !obey_client) {
                addReplyError(caller, "-READONLY Can not run script with write flag on readonly replica");
                return C_ERR;
            }

            /* Deny writes if we're unale to persist. */
            /* ��ǰд���̴��� */
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

            /* ��ǰ����ֻ��ģʽ */
            if (ro) {
                addReplyError(caller, "Can not execute a script with write flag using *_ro command.");
                return C_ERR;
            }

            /* Don't accept write commands if there are not enough good slaves and
             * user configured the min-slaves-to-write option. */

            /* ��ǰ�����ڵ��Ƿ���� */
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

        /* ��� OOM ״̬��no writes ��־��ʾ���� oom ����������д����֮�������������������ڴ���ظ����ἰ�� */
        if (server.pre_command_oom_state && server.maxmemory &&
            !(script_flags & (SCRIPT_FLAG_ALLOW_OOM|SCRIPT_FLAG_NO_WRITES)))
        {
            addReplyError(caller, "-OOM allow-oom flag is not set on the script, "
                                  "can not run it when used memory > 'maxmemory'");
            return C_ERR;
        }

    } else {
        /* Special handling for backwards compatibility (no shebang eval[sha]) mode */
        /* �����ݣ��� shebang eval[sha]��ģʽ�����⴦��, �����ǰ��������У��򷵻ش��� */
        if (running_stale) {
            addReplyErrorObject(caller, shared.masterdownerr);
            return C_ERR;
        }
    }

    run_ctx->c = engine_client;
    run_ctx->original_client = caller;
    run_ctx->funcname = funcname;

    /* �� client �������ĵĹ�ϵ */
    client *script_client = run_ctx->c;
    client *curr_client = run_ctx->original_client;
    server.script_caller = curr_client;

    /* Select the right DB in the context of the Lua client */
    selectDb(script_client, curr_client->db->id);
    /* ����Э��Ϊ2 */
    script_client->resp = 2; /* Default is RESP2, scripts can change it. */

    /* If we are in MULTI context, flag Lua client as CLIENT_MULTI. */
    /* ͸�������ʶ */
    if (curr_client->flags & CLIENT_MULTI) {
        script_client->flags |= CLIENT_MULTI;
    }

    /* ��ʼ�����ʱ����Ϣ, ǰ�������ж�ִ��ʱ��, �������� key �Ĺ����ж� */
    run_ctx->start_time = getMonotonicUs();
    run_ctx->snapshot_time = mstime();

    run_ctx->flags = 0;
    /* Ĭ������ aof �� repl ��� */
    run_ctx->repl_flags = PROPAGATE_AOF | PROPAGATE_REPL;

    /* ����ֻ����� */
    if (ro || (!(script_flags & SCRIPT_FLAG_EVAL_COMPAT_MODE) && (script_flags & SCRIPT_FLAG_NO_WRITES))) {
        /* On fcall_ro or on functions that do not have the 'write'
         * flag, we will not allow write commands. */
        run_ctx->flags |= SCRIPT_READ_ONLY;
    }
    /* ���� OOM ��� */
    if (!(script_flags & SCRIPT_FLAG_EVAL_COMPAT_MODE) && (script_flags & SCRIPT_FLAG_ALLOW_OOM)) {
        /* Note: we don't need to test the no-writes flag here and set this run_ctx flag,
         * since only write commands can are deny-oom. */
        run_ctx->flags |= SCRIPT_ALLOW_OOM;
    }

    /* ����������Ƭ��� */
    if ((script_flags & SCRIPT_FLAG_EVAL_COMPAT_MODE) || (script_flags & SCRIPT_FLAG_ALLOW_CROSS_SLOT)) {
        run_ctx->flags |= SCRIPT_ALLOW_CROSS_SLOT;
    }

    /* set the curr_run_ctx so we can use it to kill the script if needed */
    curr_run_ctx = run_ctx;

    return C_OK;
}

/* Reset the given run ctx after execution */

/* ִ�к����ø��������� ctx */
void scriptResetRun(scriptRunCtx *run_ctx) {
    serverAssert(curr_run_ctx);

    /* After the script done, remove the MULTI state. */

    /* �ű���ɺ�ɾ�� MULTI ״̬ */
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

/* ��ֹ��ǰ���еĽű� */
void scriptKill(client *c, int is_eval) {
    if (!curr_run_ctx) {
        addReplyError(c, "-NOTBUSY No scripts in execution right now.");
        return;
    }
    if (mustObeyClient(curr_run_ctx->original_client)) {
        addReplyError(c,
                "-UNKILLABLE The busy script was sent by a master instance in the context of replication and cannot be killed.");
    }
    /* ����������޸ģ����ֹ��ֹ */
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

/* У�鵱ǰд�����Ƿ���ִ�гɹ� */
static int scriptVerifyWriteCommandAllow(scriptRunCtx *run_ctx, char **err) {

    /* A write command, on an RO command or an RO script is rejected ASAP.
     * Note: For scripts, we consider may-replicate commands as write commands.
     * This also makes it possible to allow read-only scripts to be run during
     * CLIENT PAUSE WRITE. */

    /* RO ����� RO �ű��ϵ�д�����������ܾ���
     * ע�⣺���ڽű���������Ϊ���Խ������Ϊд��� */
    if (run_ctx->flags & SCRIPT_READ_ONLY &&
        (run_ctx->c->cmd->flags & (CMD_WRITE|CMD_MAY_REPLICATE)))
    {
        *err = sdsnew("Write commands are not allowed from read-only scripts.");
        return C_ERR;
    }

    /* The other checks below are on the server state and are only relevant for
     *  write commands, return if this is not a write command. */

    /* ��������������Է�����״̬������д������أ�����ⲻ��д����򷵻� */
    if (!(run_ctx->c->cmd->flags & CMD_WRITE))
        return C_OK;

    /* If the script already made a modification to the dataset, we can't
     * fail it on unpredictable error state. */

    /* ����ű��Ѿ������ݼ��������޸ģ����ǲ����ڲ���Ԥ��Ĵ���״̬��ʹ��ʧ�� */
    if ((run_ctx->flags & SCRIPT_WRITE_DIRTY))
        return C_OK;

    /* Write commands are forbidden against read-only slaves, or if a
     * command marked as non-deterministic was already called in the context
     * of this script. */

    /* ��ֹ��ֻ�������豸ִ��д�������������ڽű����������ѵ����˱��Ϊ��ȷ���Ե�������ִֹ��д������ */
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

    /* ���û���㹻�õĴӻ��������û������� min-slavesto-write ѡ���Ҫ���� write ���
     * ��ע�⣬��ֻ������δ������־�� Eval �ű�������� scriptPrepareForRun �е�������� */
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

    /* ������Ǵﵽ��ͨ�� maxmemory ���õ��ڴ����ƣ�������ʹ�ÿ��������ڴ�ʹ�����������ǰ�������Ǹýű��������еĵ�һ��д�룬�������ǲ�����;ֹͣ */
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

    /* �������һ�� Redis Cluster �ڵ㣬������Ҫȷ���ű�û����ͼ���ʷǱ�����Կ�����˴����ǵ��������յ�������� AOF ���ػ��ڴ�ʱ */
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

    /* ����ű��Ը߼���ʽ����������ô����۴�������Ѿ����׳�����ֻ�Ǽ�����ڷ��ʵ�δԤ�������Ľ������Կ */
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

/* ���õ�ǰ����Ϊ */
int scriptSetRepl(scriptRunCtx *run_ctx, int repl) {
    if ((repl & ~(PROPAGATE_AOF | PROPAGATE_REPL)) != 0) {
        return C_ERR;
    }
    run_ctx->repl_flags = repl;
    return C_OK;
}

/* �Ƿ��ֹ���ϴӽڵ�ִ�� */
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

/* ���� Redis ���
 * �ظ���д�� run_ctx �ͻ��ˣ��������ȡ�ͽ�����
 * ��������������������ʱ���Ż����� err-out ������
 * ��������� err����Ὣ�ظ�д�� run_ctx �ͻ��� */
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

    /* ��ѯ��Ӧ��������� */
    struct redisCommand *cmd = lookupCommand(argv, argc);
    c->cmd = c->lastcmd = c->realcmd = cmd;
    if (scriptVerifyCommandArity(cmd, argc, err) != C_OK) {
        goto error;
    }

    /* There are commands that are not allowed inside scripts. */

    /* �Ƿ��ֹ���ýű�����ִ�е����� */
    if (!server.script_disable_deny_script && (cmd->flags & CMD_NOSCRIPT)) {
        *err = sdsnew("This Redis command is not allowed from script");
        goto error;
    }

    /* �Ƿ��ֹ���ϴӽڵ�ִ�� */
    if (scriptVerifyAllowStale(c, err) != C_OK) {
        goto error;
    }

    /* У��Ȩ��״̬ */
    if (scriptVerifyACL(c, err) != C_OK) {
        goto error;
    }

    /* У��д�����Ƿ��ִ�� */
    if (scriptVerifyWriteCommandAllow(run_ctx, err) != C_OK) {
        goto error;
    }

    /* У�� oom */
    if (scriptVerifyOOM(run_ctx, err) != C_OK) {
        goto error;
    }

    if (cmd->flags & CMD_WRITE) {
        /* signify that we already change the data in this execution */

        /* ��ʾ�����Ѿ������˴�ִ���е����� */
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

/* ���ؽű����ÿ�ʼ��ʱ�� */
mstime_t scriptTimeSnapshot() {
    serverAssert(curr_run_ctx);
    return curr_run_ctx->snapshot_time;
}

long long scriptRunDuration() {
    serverAssert(scriptIsRunning());
    return elapsedMs(curr_run_ctx->start_time);
}
