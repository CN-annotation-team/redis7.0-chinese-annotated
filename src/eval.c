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

/**
 * 该文件中涉及到了很多lua的调用接口
 * lua 的接口原理可参照：https://www.runoob.com/manual/lua53doc/manual.html
 * 负责对 redis 提供服务的方法
 */

#include "server.h"
#include "sha1.h"
#include "rand.h"
#include "cluster.h"
#include "monotonic.h"
#include "resp_parser.h"
#include "script_lua.h"

#include <lua.h>
#include <lauxlib.h>
#include <lualib.h>
#include <ctype.h>
#include <math.h>

void ldbInit(void);
void ldbDisable(client *c);
void ldbEnable(client *c);
void evalGenericCommandWithDebugging(client *c, int evalsha);
sds ldbCatStackValue(sds s, lua_State *lua, int idx);

static void dictLuaScriptDestructor(dict *d, void *val) {
    UNUSED(d);
    if (val == NULL) return; /* Lazy freeing will set value to NULL. */
    decrRefCount(((luaScript*)val)->body);
    zfree(val);
}

static uint64_t dictStrCaseHash(const void *key) {
    return dictGenCaseHashFunction((unsigned char*)key, strlen((char*)key));
}

/* server.lua_scripts sha (as sds string) -> scripts (as luaScript) cache. */

/* server.lua_scripts sha（作为sds字符串）-> scripts（作为luaScript）缓存 */
dictType shaScriptObjectDictType = {
        dictStrCaseHash,            /* hash function */
        NULL,                       /* key dup */
        NULL,                       /* val dup */
        dictSdsKeyCaseCompare,      /* key compare */
        dictSdsDestructor,          /* key destructor */
        dictLuaScriptDestructor,    /* val destructor */
        NULL                        /* allow to expand */
};

/* Lua context */
struct luaCtx {
    /* lua执行器, 全局唯一  */
    lua_State *lua; /* The Lua interpreter. We use just one for all clients */
    /* lua客户端，用于调用redis方法  */
    client *lua_client;   /* The "fake client" to query Redis from Lua */
    /* sha1和lua脚本的映射  */
    dict *lua_scripts;         /* A dictionary of SHA1 -> Lua scripts */
    /* 脚本的内存空间  */
    unsigned long long lua_scripts_mem;  /* Cached scripts' memory + oh */
} lctx;

/* Debugger shared state is stored inside this global structure. */

/* 调试器共享状态存储在此全局结构中 */
#define LDB_BREAKPOINTS_MAX 64  /* Max number of breakpoints. */
#define LDB_MAX_LEN_DEFAULT 256 /* Default len limit for replies / var dumps. */
struct ldbState {
    connection *conn; /* Connection of the debugging client. */
    int active; /* Are we debugging EVAL right now? */
    int forked; /* Is this a fork()ed debugging session? */
    list *logs; /* List of messages to send to the client. */
    list *traces; /* Messages about Redis commands executed since last stop.*/
    list *children; /* All forked debugging sessions pids. */
    int bp[LDB_BREAKPOINTS_MAX]; /* An array of breakpoints line numbers. */
    int bpcount; /* Number of valid entries inside bp. */
    int step;   /* Stop at next line regardless of breakpoints. */
    int luabp;  /* Stop at next line because redis.breakpoint() was called. */
    sds *src;   /* Lua script source code split by line. */
    int lines;  /* Number of lines in 'src'. */
    int currentline;    /* Current line number. */
    sds cbuf;   /* Debugger client command buffer. */
    size_t maxlen;  /* Max var dump / reply length. */
    int maxlen_hint_sent; /* Did we already hint about "set maxlen"? */
} ldb;

/* ---------------------------------------------------------------------------
 * Utility functions.
 * ------------------------------------------------------------------------- */

/* Perform the SHA1 of the input string. We use this both for hashing script
 * bodies in order to obtain the Lua function name, and in the implementation
 * of redis.sha1().
 *
 * 'digest' should point to a 41 bytes buffer: 40 for SHA1 converted into an
 * hexadecimal number, plus 1 byte for null term. */

/* 执行输入字符串的 SHA1。我们将其用于散列脚本体以获得 Lua 函数名，并用于 redis.sha1() 的实现。
 *
 * “digest” 应该指向一个41字节的缓冲区：40表示 SHA1 转换为十六进制数，加上1字节表示空项 */
void sha1hex(char *digest, char *script, size_t len) {
    SHA1_CTX ctx;
    unsigned char hash[20];
    char *cset = "0123456789abcdef";
    int j;

    SHA1Init(&ctx);
    SHA1Update(&ctx,(unsigned char*)script,len);
    SHA1Final(hash,&ctx);

    for (j = 0; j < 20; j++) {
        digest[j*2] = cset[((hash[j]&0xF0)>>4)];
        digest[j*2+1] = cset[(hash[j]&0xF)];
    }
    digest[40] = '\0';
}

/* redis.breakpoint()
 *
 * Allows to stop execution during a debugging session from within
 * the Lua code implementation, like if a breakpoint was set in the code
 * immediately after the function. */

/* 允许在调试会话期间从 Lua 代码实现中停止执行，就像在函数之后的代码中设置了断点一样 */
int luaRedisBreakpointCommand(lua_State *lua) {
    if (ldb.active) {
        ldb.luabp = 1;
        lua_pushboolean(lua,1);
    } else {
        lua_pushboolean(lua,0);
    }
    return 1;
}

/* redis.debug()
 *
 * Log a string message into the output console.
 * Can take multiple arguments that will be separated by commas.
 * Nothing is returned to the caller. */

/* 将字符串消息记录到输出控制台中。
 * 可以接受多个用逗号分隔的参数。
 * 不会向呼叫者返回任何内容 */
int luaRedisDebugCommand(lua_State *lua) {
    if (!ldb.active) return 0;
    int argc = lua_gettop(lua);
    sds log = sdscatprintf(sdsempty(),"<debug> line %d: ", ldb.currentline);
    while(argc--) {
        log = ldbCatStackValue(log,lua,-1 - argc);
        if (argc != 0) log = sdscatlen(log,", ",2);
    }
    ldbLog(log);
    return 0;
}

/* redis.replicate_commands()
 *
 * DEPRECATED: Now do nothing and always return true.
 * Turn on single commands replication if the script never called
 * a write command so far, and returns true. Otherwise if the script
 * already started to write, returns false and stick to whole scripts
 * replication, which is our default. */

/* 如果脚本到目前为止从未调用过写命令，则启用单命令复制，并返回 true。否则，如果脚本已经开始编写，则返回 false 并坚持整个脚本 
 * 现在已经完全通过传播命令来保证lua的正确性，所以全部返回 ture */
int luaRedisReplicateCommandsCommand(lua_State *lua) {
    lua_pushboolean(lua,1);
    return 1;
}

/* Initialize the scripting environment.
 *
 * This function is called the first time at server startup with
 * the 'setup' argument set to 1.
 *
 * It can be called again multiple times during the lifetime of the Redis
 * process, with 'setup' set to 0, and following a scriptingRelease() call,
 * in order to reset the Lua scripting environment.
 *
 * However it is simpler to just call scriptingReset() that does just that. */

/* 初始化脚本环境。setup 表示是否为启动时的初始化.说明当前方法是可以多次调用, 同时必须在 scriptingRelease 执行后，因为后者会对相关空间进行释放 */
void scriptingInit(int setup) {
    lua_State *lua = lua_open();

    /* 如果首次调用，则初始化lctx上下文、server全局数据、luaDebug信息 */
    if (setup) {
        lctx.lua_client = NULL;
        server.script_caller = NULL;
        server.script_disable_deny_script = 0;
        ldbInit();
    }

    /* Initialize a dictionary we use to map SHAs to scripts.
     * This is useful for replication, as we need to replicate EVALSHA
     * as EVAL, so we need to remember the associated script. */
    lctx.lua_scripts = dictCreate(&shaScriptObjectDictType);
    lctx.lua_scripts_mem = 0;

    luaRegisterRedisAPI(lua);

    /* register debug commands */
    lua_getglobal(lua,"redis");

    /* redis.breakpoint */

    /* redis.breakpoint() 在 debug 模式下，主动进入断点模式 */
    lua_pushstring(lua,"breakpoint");
    lua_pushcfunction(lua,luaRedisBreakpointCommand);
    lua_settable(lua,-3);

    /* redis.debug */

    /* redis.debug(a,b) 在 debug 模式下，打印相关信息 */
    lua_pushstring(lua,"debug");
    lua_pushcfunction(lua,luaRedisDebugCommand);
    lua_settable(lua,-3);

    /* redis.replicate_commands */
    lua_pushstring(lua, "replicate_commands");
    lua_pushcfunction(lua, luaRedisReplicateCommandsCommand);
    lua_settable(lua, -3);

    lua_setglobal(lua,"redis");

    /* Add a helper function we use for pcall error reporting.
     * Note that when the error is in the C function we want to report the
     * information about the caller, that's what makes sense from the point
     * of view of the user debugging a script. */

    /* 添加一个用于pcall错误报告的帮助函数。
     * 请注意，当错误发生在C函数中时，我们希望报告有关调用者的信息，从用户调试脚本的角度来看，这是有意义的 */
    {
        char *errh_func =       "local dbg = debug\n"
                                "debug = nil\n"
                                "function __redis__err__handler(err)\n"
                                "  local i = dbg.getinfo(2,'nSl')\n"
                                "  if i and i.what == 'C' then\n"
                                "    i = dbg.getinfo(3,'nSl')\n"
                                "  end\n"
                                "  if type(err) ~= 'table' then\n"
                                "    err = {err='ERR ' .. tostring(err)}"
                                "  end"
                                "  if i then\n"
                                "    err['source'] = i.source\n"
                                "    err['line'] = i.currentline\n"
                                "  end"
                                "  return err\n"
                                "end\n";
        /* 将代码压入栈中 */
        luaL_loadbuffer(lua,errh_func,strlen(errh_func),"@err_handler_def");
        /* 尝试运行一下代码, 使其生效 */
        lua_pcall(lua,0,0,0);
    }

    /* Create the (non connected) client that we use to execute Redis commands
     * inside the Lua interpreter.
     * Note: there is no need to create it again when this function is called
     * by scriptingReset(). */

    /* 创建（未连接的）客户端，我们使用该客户端在Lua解释器中执行Redis命令。
     * 注意：当通过scriptingReset() 调用此函数时，无需再次创建它 */
    if (lctx.lua_client == NULL) {
        lctx.lua_client = createClient(NULL);
        lctx.lua_client->flags |= CLIENT_SCRIPT;

        /* We do not want to allow blocking commands inside Lua */
        lctx.lua_client->flags |= CLIENT_DENY_BLOCKING;
    }

    /* Lock the global table from any changes */

    /* 设置全局变量查询错误异常 */
    lua_pushvalue(lua, LUA_GLOBALSINDEX);
    luaSetErrorMetatable(lua);
    /* Recursively lock all tables that can be reached from the global table */

    /* 递归锁定可从全局表访问的所有表, 将其都设置为只读状态 */
    luaSetTableProtectionRecursively(lua);
    lua_pop(lua, 1);

    lctx.lua = lua;
}

/* Release resources related to Lua scripting.
 * This function is used in order to reset the scripting environment. */
void scriptingRelease(int async) {
    if (async)
        freeLuaScriptsAsync(lctx.lua_scripts);
    else
        dictRelease(lctx.lua_scripts);
    lctx.lua_scripts_mem = 0;
    lua_close(lctx.lua);
}

void scriptingReset(int async) {
    scriptingRelease(async);
    scriptingInit(0);
}

/* ---------------------------------------------------------------------------
 * EVAL and SCRIPT commands implementation
 * ------------------------------------------------------------------------- */

/* 计算当前的方法名称，即 f_{sha1 code} */
static void evalCalcFunctionName(int evalsha, sds script, char *out_funcname) {
    /* We obtain the script SHA1, then check if this function is already
     * defined into the Lua state */
    out_funcname[0] = 'f';
    out_funcname[1] = '_';
    if (!evalsha) {
        /* Hash the code if this is an EVAL call */
        sha1hex(out_funcname+2,script,sdslen(script));
    } else {
        /* We already have the SHA if it is an EVALSHA */
        int j;
        char *sha = script;

        /* Convert to lowercase. We don't use tolower since the function
         * managed to always show up in the profiler output consuming
         * a non trivial amount of time. */
        for (j = 0; j < 40; j++)
            out_funcname[j+2] = (sha[j] >= 'A' && sha[j] <= 'Z') ?
                sha[j]+('a'-'A') : sha[j];
        out_funcname[42] = '\0';
    }
}

/* Helper function to try and extract shebang flags from the script body.
 * If no shebang is found, return with success and COMPAT mode flag.
 * The err arg is optional, can be used to get a detailed error string.
 * The out_shebang_len arg is optional, can be used to trim the shebang from the script.
 * Returns C_OK on success, and C_ERR on error. */

/* Helper 函数尝试从脚本正文中提取 shebang 标志。
 * 如果未找到 shebang，则返回成功和 COMPAT 模式标志。
 * err 参数是可选的，可用于获取详细的错误字符串。
 * out_shebang_len 参数是可选的，可用于从脚本中删除 shebang
 * 成功时返回 C_OK ，错误时返回 C_ERR
 * Shebang 的名字来自于 SHArp 和 bang，或 haSH bang 的缩写，指代 Shebang 中 #! 两个符号的典型 Unix 名称
 * 参考：https://redis.io/docs/manual/programmability/eval-intro/ */
int evalExtractShebangFlags(sds body, uint64_t *out_flags, ssize_t *out_shebang_len, sds *err) {
    ssize_t shebang_len = 0;
    uint64_t script_flags = SCRIPT_FLAG_EVAL_COMPAT_MODE;
    /* 是否以 #!开头 */
    if (!strncmp(body, "#!", 2)) {
        int numparts,j;
        char *shebang_end = strchr(body, '\n');
        if (shebang_end == NULL) {
            if (err)
                *err = sdsnew("Invalid script shebang");
            return C_ERR;
        }
        shebang_len = shebang_end - body;
        sds shebang = sdsnewlen(body, shebang_len);
        /* 以空格为分隔符, 分割为多块 */
        sds *parts = sdssplitargs(shebang, &numparts);
        sdsfree(shebang);
        if (!parts || numparts == 0) {
            if (err)
                *err = sdsnew("Invalid engine in script shebang");
            sdsfreesplitres(parts, numparts);
            return C_ERR;
        }
        /* Verify lua interpreter was specified */
        if (strcmp(parts[0], "#!lua")) {
            if (err)
                *err = sdscatfmt(sdsempty(), "Unexpected engine in script shebang: %s", parts[0]);
            sdsfreesplitres(parts, numparts);
            return C_ERR;
        }
        /* 移除特殊的兼容标记 */
        script_flags &= ~SCRIPT_FLAG_EVAL_COMPAT_MODE;
        for (j = 1; j < numparts; j++) {
            if (!strncmp(parts[j], "flags=", 6)) {
                sdsrange(parts[j], 6, -1);
                int numflags, jj;
                sds *flags = sdssplitlen(parts[j], sdslen(parts[j]), ",", 1, &numflags);
                for (jj = 0; jj < numflags; jj++) {
                    scriptFlag *sf;
                    for (sf = scripts_flags_def; sf->flag; sf++) {
                        if (!strcmp(flags[jj], sf->str)) break;
                    }
                    if (!sf->flag) {
                        if (err)
                            *err = sdscatfmt(sdsempty(), "Unexpected flag in script shebang: %s", flags[jj]);
                        sdsfreesplitres(flags, numflags);
                        sdsfreesplitres(parts, numparts);
                        return C_ERR;
                    }
                    script_flags |= sf->flag;
                }
                sdsfreesplitres(flags, numflags);
            } else {
                /* We only support function flags options for lua scripts */
                if (err)
                    *err = sdscatfmt(sdsempty(), "Unknown lua shebang option: %s", parts[j]);
                sdsfreesplitres(parts, numparts);
                return C_ERR;
            }
        }
        sdsfreesplitres(parts, numparts);
    }
    if (out_shebang_len)
        *out_shebang_len = shebang_len;
    *out_flags = script_flags;
    return C_OK;
}

/* Try to extract command flags if we can, returns the modified flags.
 * Note that it does not guarantee the command arguments are right. */

/* 如果可以，尝试提取命令标志，返回修改后的标志。请注意，它不能保证命令参数是正确的
 * 主要用于在 processCommand 中判断执行条件 */
uint64_t evalGetCommandFlags(client *c, uint64_t cmd_flags) {
    char funcname[43];
    int evalsha = c->cmd->proc == evalShaCommand || c->cmd->proc == evalShaRoCommand;
    if (evalsha && sdslen(c->argv[1]->ptr) != 40)
        return cmd_flags;
    evalCalcFunctionName(evalsha, c->argv[1]->ptr, funcname);
    char *lua_cur_script = funcname + 2;
    dictEntry *de = dictFind(lctx.lua_scripts, lua_cur_script);
    uint64_t script_flags;
    if (!de) {
        if (evalsha)
            return cmd_flags;
        /* 解析当前脚本的shebang属性 */
        if (evalExtractShebangFlags(c->argv[1]->ptr, &script_flags, NULL, NULL) == C_ERR)
            return cmd_flags;
    } else {
        luaScript *l = dictGetVal(de);
        script_flags = l->flags;
    }
    if (script_flags & SCRIPT_FLAG_EVAL_COMPAT_MODE)
        return cmd_flags;
    return scriptFlagsToCmdFlags(cmd_flags, script_flags);
}

/* Define a Lua function with the specified body.
 * The function name will be generated in the following form:
 *
 *   f_<hex sha1 sum>
 *
 * The function increments the reference count of the 'body' object as a
 * side effect of a successful call.
 *
 * On success a pointer to an SDS string representing the function SHA1 of the
 * just added function is returned (and will be valid until the next call
 * to scriptingReset() function), otherwise NULL is returned.
 *
 * The function handles the fact of being called with a script that already
 * exists, and in such a case, it behaves like in the success case.
 *
 * If 'c' is not NULL, on error the client is informed with an appropriate
 * error describing the nature of the problem and the Lua interpreter error. */

/* 使用指定的主体定义Lua函数。函数名称将以以下形式生成：
 * f_＜十六进制sha1和＞
 * 该函数增加 “body” 对象的引用计数，作为成功调用的副作用。
 * 成功时，将返回一个指向 SDS 字符串的指针，该字符串表示刚刚添加的函数的函数 SHA1（在下一次调用 scriptingReset 函数之前有效），否则返回 NULL
 * 该函数处理被已经存在的脚本调用的事实，在这种情况下，它的行为与成功的情况类似。
 * 如果 “c” 不为 NULL，则在出现错误时，会通知客户端一个适当的错误，描述问题的性质和 Lua 解释器错误
 * 方法解析了 shebang，然后将 sha1 值和 script body、script flag 绑定 */
sds luaCreateFunction(client *c, robj *body) {
    char funcname[43];
    dictEntry *de;
    uint64_t script_flags;

    funcname[0] = 'f';
    funcname[1] = '_';
    sha1hex(funcname+2,body->ptr,sdslen(body->ptr));

    if ((de = dictFind(lctx.lua_scripts,funcname+2)) != NULL) {
        return dictGetKey(de);
    }

    /* Handle shebang header in script code */
    ssize_t shebang_len = 0;
    sds err = NULL;
    if (evalExtractShebangFlags(body->ptr, &script_flags, &shebang_len, &err) == C_ERR) {
        addReplyErrorSds(c, err);
        return NULL;
    }

    /* Note that in case of a shebang line we skip it but keep the line feed to conserve the user's line numbers */

    /* 请注意，对于shebang行，我们跳过它，但保留换行以保留用户的行号 */
    if (luaL_loadbuffer(lctx.lua,(char*)body->ptr + shebang_len,sdslen(body->ptr) - shebang_len,"@user_script")) {
        if (c != NULL) {
            addReplyErrorFormat(c,
                "Error compiling script (new function): %s",
                lua_tostring(lctx.lua,-1));
        }
        lua_pop(lctx.lua,1);
        return NULL;
    }

    serverAssert(lua_isfunction(lctx.lua, -1));

    /* 设置 table 的字段信息 */
    lua_setfield(lctx.lua, LUA_REGISTRYINDEX, funcname);

    /* We also save a SHA1 -> Original script map in a dictionary
     * so that we can replicate / write in the AOF all the
     * EVALSHA commands as EVAL using the original script. */
    luaScript *l = zcalloc(sizeof(luaScript));
    l->body = body;
    l->flags = script_flags;
    sds sha = sdsnewlen(funcname+2,40);
    int retval = dictAdd(lctx.lua_scripts,sha,l);
    serverAssertWithInfo(c ? c : lctx.lua_client,NULL,retval == DICT_OK);
    lctx.lua_scripts_mem += sdsZmallocSize(sha) + getStringObjectSdsUsedMemory(body);
    incrRefCount(body);
    return sha;
}

void prepareLuaClient(void) {
    /* Select the right DB in the context of the Lua client */
    selectDb(lctx.lua_client,server.script_caller->db->id);
    lctx.lua_client->resp = 2; /* Default is RESP2, scripts can change it. */

    /* If we are in MULTI context, flag Lua client as CLIENT_MULTI. */
    if (server.script_caller->flags & CLIENT_MULTI) {
        lctx.lua_client->flags |= CLIENT_MULTI;
    }
}

void resetLuaClient(void) {
    /* After the script done, remove the MULTI state. */
    lctx.lua_client->flags &= ~CLIENT_MULTI;
}

/* 统一的 lua 执行方法, evalsha 表示算法为 sha1 执行方式 */
void evalGenericCommand(client *c, int evalsha) {
    lua_State *lua = lctx.lua;
    char funcname[43];
    long long numkeys;

    /* Get the number of arguments that are keys */
    if (getLongLongFromObjectOrReply(c,c->argv[2],&numkeys,NULL) != C_OK)
        return;
    if (numkeys > (c->argc - 3)) {
        addReplyError(c,"Number of keys can't be greater than number of args");
        return;
    } else if (numkeys < 0) {
        addReplyError(c,"Number of keys can't be negative");
        return;
    }

    /* 通过lua脚本获取到对应的sha1作为funcname */
    evalCalcFunctionName(evalsha, c->argv[1]->ptr, funcname);

    /* Push the pcall error handler function on the stack. */

    /* 将错误处理方法加载到栈中，用于在调用方法错误时回调 */
    lua_getglobal(lua, "__redis__err__handler");

    /* Try to lookup the Lua function */

    /* 加载对应的方法 */
    lua_getfield(lua, LUA_REGISTRYINDEX, funcname);
    if (lua_isnil(lua,-1)) {
        lua_pop(lua,1); /* remove the nil from the stack */
        /* Function not defined... let's define it if we have the
         * body of the function. If this is an EVALSHA call we can just
         * return an error. */
        if (evalsha) {
            lua_pop(lua,1); /* remove the error handler from the stack. */
            addReplyErrorObject(c, shared.noscripterr);
            return;
        }
        if (luaCreateFunction(c,c->argv[1]) == NULL) {
            lua_pop(lua,1); /* remove the error handler from the stack. */
            /* The error is sent to the client by luaCreateFunction()
             * itself when it returns NULL. */
            return;
        }
        /* Now the following is guaranteed to return non nil */
        lua_getfield(lua, LUA_REGISTRYINDEX, funcname);
        serverAssert(!lua_isnil(lua,-1));
    }

    char *lua_cur_script = funcname + 2;
    dictEntry *de = dictFind(lctx.lua_scripts, lua_cur_script);
    luaScript *l = dictGetVal(de);
    int ro = c->cmd->proc == evalRoCommand || c->cmd->proc == evalShaRoCommand;

    /* 运行时上下文 */
    scriptRunCtx rctx;
    /* 准备上下文信息 */
    if (scriptPrepareForRun(&rctx, lctx.lua_client, c, lua_cur_script, l->flags, ro) != C_OK) {
        lua_pop(lua,2); /* Remove the function and error handler. */
        return;
    }
    /* 将当前运行标记为 EVAL（与 FCALL 相反），这样我们将获得适当的错误消息和日志 */
    rctx.flags |= SCRIPT_EVAL_MODE; /* mark the current run as EVAL (as opposed to FCALL) so we'll
                                      get appropriate error messages and logs */

    /* 整体 debug 模式都在下面的方法中通过 hook 实现 */
    luaCallFunction(&rctx, lua, c->argv+3, numkeys, c->argv+3+numkeys, c->argc-3-numkeys, ldb.active);
    lua_pop(lua,1); /* Remove the error handler. */
    scriptResetRun(&rctx);
}

/* lua 脚本执行入口函数 */
void evalCommand(client *c) {
    /* Explicitly feed monitor here so that lua commands appear after their
     * script command. */
    replicationFeedMonitors(c,server.monitors,c->db->id,c->argv,c->argc);
    if (!(c->flags & CLIENT_LUA_DEBUG))
        evalGenericCommand(c,0);
    else
        evalGenericCommandWithDebugging(c,0);
}

/* lua脚本只读方式（不进行计数）执行入口函数 */
void evalRoCommand(client *c) {
    evalCommand(c);
}

/* lua 脚本 sha 执行入口函数 */
void evalShaCommand(client *c) {
    /* Explicitly feed monitor here so that lua commands appear after their
     * script command. */

    /* 在此处显式馈送监视器，以便 lua 命令出现在其脚本命令之后 */
    replicationFeedMonitors(c,server.monitors,c->db->id,c->argv,c->argc);
    if (sdslen(c->argv[1]->ptr) != 40) {
        /* We know that a match is not possible if the provided SHA is
         * not the right length. So we return an error ASAP, this way
         * evalGenericCommand() can be implemented without string length
         * sanity check */

        /* 我们知道，如果提供的 SHA 长度不正确，则不可能匹配。
         * 因此，我们会尽快返回一个错误，这样 evalGenericCommand() 就可以在没有字符串长度健全性检查的情况下实现 */
        addReplyErrorObject(c, shared.noscripterr);
        return;
    }
    if (!(c->flags & CLIENT_LUA_DEBUG))
        evalGenericCommand(c,1);
    else {
        addReplyError(c,"Please use EVAL instead of EVALSHA for debugging");
        return;
    }
}

void evalShaRoCommand(client *c) {
    evalShaCommand(c);
}

/* script 命令处理，包括 DEBUG 开关、 EXISTS 判断 sha 值、 FLUSH 刷新缓存、KILL 杀死脚本、LOAD 加载脚本 */
void scriptCommand(client *c) {
    if (c->argc == 2 && !strcasecmp(c->argv[1]->ptr,"help")) {
        const char *help[] = {
"DEBUG (YES|SYNC|NO)",
"    Set the debug mode for subsequent scripts executed.",
"EXISTS <sha1> [<sha1> ...]",
"    Return information about the existence of the scripts in the script cache.",
"FLUSH [ASYNC|SYNC]",
"    Flush the Lua scripts cache. Very dangerous on replicas.",
"    When called without the optional mode argument, the behavior is determined by the",
"    lazyfree-lazy-user-flush configuration directive. Valid modes are:",
"    * ASYNC: Asynchronously flush the scripts cache.",
"    * SYNC: Synchronously flush the scripts cache.",
"KILL",
"    Kill the currently executing Lua script.",
"LOAD <script>",
"    Load a script into the scripts cache without executing it.",
NULL
        };
        addReplyHelp(c, help);
    } else if (c->argc >= 2 && !strcasecmp(c->argv[1]->ptr,"flush")) {
        int async = 0;
        if (c->argc == 3 && !strcasecmp(c->argv[2]->ptr,"sync")) {
            async = 0;
        } else if (c->argc == 3 && !strcasecmp(c->argv[2]->ptr,"async")) {
            async = 1;
        } else if (c->argc == 2) {
            async = server.lazyfree_lazy_user_flush ? 1 : 0;
        } else {
            addReplyError(c,"SCRIPT FLUSH only support SYNC|ASYNC option");
            return;
        }
        scriptingReset(async);
        addReply(c,shared.ok);
    } else if (c->argc >= 2 && !strcasecmp(c->argv[1]->ptr,"exists")) {
        int j;

        addReplyArrayLen(c, c->argc-2);
        for (j = 2; j < c->argc; j++) {
            if (dictFind(lctx.lua_scripts,c->argv[j]->ptr))
                addReply(c,shared.cone);
            else
                addReply(c,shared.czero);
        }
    } else if (c->argc == 3 && !strcasecmp(c->argv[1]->ptr,"load")) {
        sds sha = luaCreateFunction(c,c->argv[2]);
        if (sha == NULL) return; /* The error was sent by luaCreateFunction(). */
        addReplyBulkCBuffer(c,sha,40);
    } else if (c->argc == 2 && !strcasecmp(c->argv[1]->ptr,"kill")) {
        scriptKill(c, 1);
    } else if (c->argc == 3 && !strcasecmp(c->argv[1]->ptr,"debug")) {
        if (clientHasPendingReplies(c)) {
            addReplyError(c,"SCRIPT DEBUG must be called outside a pipeline");
            return;
        }
        if (!strcasecmp(c->argv[2]->ptr,"no")) {
            ldbDisable(c);
            addReply(c,shared.ok);
        } else if (!strcasecmp(c->argv[2]->ptr,"yes")) {
            ldbEnable(c);
            addReply(c,shared.ok);
        } else if (!strcasecmp(c->argv[2]->ptr,"sync")) {
            ldbEnable(c);
            addReply(c,shared.ok);
            c->flags |= CLIENT_LUA_DEBUG_SYNC;
        } else {
            addReplyError(c,"Use SCRIPT DEBUG YES/SYNC/NO");
            return;
        }
    } else {
        addReplySubcommandSyntaxError(c);
    }
}

unsigned long evalMemory() {
    return luaMemory(lctx.lua);
}

dict* evalScriptsDict() {
    return lctx.lua_scripts;
}

unsigned long evalScriptsMemory() {
    return lctx.lua_scripts_mem +
            dictSize(lctx.lua_scripts) * (sizeof(dictEntry) + sizeof(luaScript)) +
            dictSlots(lctx.lua_scripts) * sizeof(dictEntry*);
}

/* ---------------------------------------------------------------------------
 * LDB: Redis Lua debugging facilities
 * ------------------------------------------------------------------------- */

/* Initialize Lua debugger data structures. */

/* 初始化 Lua 调试器数据结构 */
void ldbInit(void) {
    ldb.conn = NULL;
    ldb.active = 0;
    ldb.logs = listCreate();
    listSetFreeMethod(ldb.logs,(void (*)(void*))sdsfree);
    ldb.children = listCreate();
    ldb.src = NULL;
    ldb.lines = 0;
    ldb.cbuf = sdsempty();
}

/* Remove all the pending messages in the specified list. */

/* 删除指定列表中的所有待处理的消息 */
void ldbFlushLog(list *log) {
    listNode *ln;

    while((ln = listFirst(log)) != NULL)
        listDelNode(log,ln);
}

int ldbIsEnabled(){
    return ldb.active && ldb.step;
}

/* Enable debug mode of Lua scripts for this client. */

/* 为此客户端启用 Lua 脚本的调试模式, 通过 script debug yes/sync 启动, 标记当前 client 为 deubg 模式 */
void ldbEnable(client *c) {
    c->flags |= CLIENT_LUA_DEBUG;
    ldbFlushLog(ldb.logs);
    ldb.conn = c->conn;
    ldb.step = 1;
    ldb.bpcount = 0;
    ldb.luabp = 0;
    sdsfree(ldb.cbuf);
    ldb.cbuf = sdsempty();
    ldb.maxlen = LDB_MAX_LEN_DEFAULT;
    ldb.maxlen_hint_sent = 0;
}

/* Exit debugging mode from the POV of client. This function is not enough
 * to properly shut down a client debugging session, see ldbEndSession()
 * for more information. */

/* 从客户端的 POV 退出调试模式。此函数不足以正确关闭客户端调试会话，有关详细信息，请参阅 ldbEndSession() */
void ldbDisable(client *c) {
    c->flags &= ~(CLIENT_LUA_DEBUG|CLIENT_LUA_DEBUG_SYNC);
}

/* Append a log entry to the specified LDB log. */

/* 将日志条目附加到指定的 LDB 日志 */
void ldbLog(sds entry) {
    listAddNodeTail(ldb.logs,entry);
}

/* A version of ldbLog() which prevents producing logs greater than
 * ldb.maxlen. The first time the limit is reached a hint is generated
 * to inform the user that reply trimming can be disabled using the
 * debugger "maxlen" command. */

/* ldbLog() 的一个版本，用于防止生成大于 ldb.maxlen 的日志。第一次达到限制时，会生成一个提示，通知用户可以使用调试器 maxlen 命令禁用回复修剪 */
void ldbLogWithMaxLen(sds entry) {
    int trimmed = 0;
    if (ldb.maxlen && sdslen(entry) > ldb.maxlen) {
        sdsrange(entry,0,ldb.maxlen-1);
        entry = sdscatlen(entry," ...",4);
        trimmed = 1;
    }
    ldbLog(entry);
    if (trimmed && ldb.maxlen_hint_sent == 0) {
        ldb.maxlen_hint_sent = 1;
        ldbLog(sdsnew(
        "<hint> The above reply was trimmed. Use 'maxlen 0' to disable trimming."));
    }
}

/* Send ldb.logs to the debugging client as a multi-bulk reply
 * consisting of simple strings. Log entries which include newlines have them
 * replaced with spaces. The entries sent are also consumed. */

/* 发送 ldb.logs 作为由简单字符串组成的多批量回复记录到调试客户端。包含换行符的日志条目将用空格替换。发送的条目也会被消耗 */
void ldbSendLogs(void) {
    sds proto = sdsempty();
    proto = sdscatfmt(proto,"*%i\r\n", (int)listLength(ldb.logs));
    while(listLength(ldb.logs)) {
        listNode *ln = listFirst(ldb.logs);
        proto = sdscatlen(proto,"+",1);
        sdsmapchars(ln->value,"\r\n","  ",2);
        proto = sdscatsds(proto,ln->value);
        proto = sdscatlen(proto,"\r\n",2);
        listDelNode(ldb.logs,ln);
    }
    if (connWrite(ldb.conn,proto,sdslen(proto)) == -1) {
        /* Avoid warning. We don't check the return value of write()
         * since the next read() will catch the I/O error and will
         * close the debugging session. */

        /* 避免警告。我们不检查 write() 的返回值，因为下一次 read() 将捕获 I/O 错误并关闭调试会话 */
    }
    sdsfree(proto);
}

/* Start a debugging session before calling EVAL implementation.
 * The technique we use is to capture the client socket file descriptor,
 * in order to perform direct I/O with it from within Lua hooks. This
 * way we don't have to re-enter Redis in order to handle I/O.
 *
 * The function returns 1 if the caller should proceed to call EVAL,
 * and 0 if instead the caller should abort the operation (this happens
 * for the parent in a forked session, since it's up to the children
 * to continue, or when fork returned an error).
 *
 * The caller should call ldbEndSession() only if ldbStartSession()
 * returned 1. */

/* 在调用 EVAL 实现之前启动调试会话。
 * 我们使用的技术是捕获客户端 socket 描述符，以便从 Lua 挂钩中对其执行直接 I/O。这样，我们就不必为了处理 I/O 而重新输入 Redis。
 *
 * 如果调用方继续调用 EVAL，则函数返回1；如果调用方中止操作，则返回0（这发生在分叉会话中的父级，因为由子级继续，或者分叉返回错误时）。
 *
 * 仅当ldbStartSession() 返回1时，调用方才应调用 ldbEndSession */
int ldbStartSession(client *c) {
    /* 确定当前是否需要 fork 一个子进程来运行 */
    ldb.forked = (c->flags & CLIENT_LUA_DEBUG_SYNC) == 0;
    if (ldb.forked) {
        pid_t cp = redisFork(CHILD_TYPE_LDB);
        if (cp == -1) {
            addReplyErrorFormat(c,"Fork() failed: can't run EVAL in debugging mode: %s", strerror(errno));
            return 0;
        } else if (cp == 0) {
            /* Child. Let's ignore important signals handled by the parent. */

            /* 子进程需要忽略父母处理的重要信号 */
            struct sigaction act;
            sigemptyset(&act.sa_mask);
            act.sa_flags = 0;
            act.sa_handler = SIG_IGN;
            sigaction(SIGTERM, &act, NULL);
            sigaction(SIGINT, &act, NULL);

            /* Log the creation of the child and close the listening
             * socket to make sure if the parent crashes a reset is sent
             * to the clients. */

            /* 记录子级的创建并关闭侦听套接字，以确保如果父级崩溃，则向客户端发送重置 */
            serverLog(LL_WARNING,"Redis forked for debugging eval");
        } else {
            /* Parent */

            /* 父进程将当前的子进程编号写入 list 后退出，交给子进程来通信 */
            listAddNodeTail(ldb.children,(void*)(unsigned long)cp);
            freeClientAsync(c); /* Close the client in the parent side. */
            return 0;
        }
    } else {
        serverLog(LL_WARNING,
            "Redis synchronous debugging eval session started");
    }

    /* Setup our debugging session. */

    /* 设置调试会话，为阻塞模式，因为要么同步，要么子进程模式，再后续会重新设置为非阻塞模式 */
    connBlock(ldb.conn);
    connSendTimeout(ldb.conn,5000);
    ldb.active = 1;

    /* First argument of EVAL is the script itself. We split it into different
     * lines since this is the way the debugger accesses the source code. */

    /* EVAL 的第一个参数是脚本本身。我们将它分成不同的行，因为这是调试器访问源代码的方式 */
    sds srcstring = sdsdup(c->argv[1]->ptr);
    size_t srclen = sdslen(srcstring);
    while(srclen && (srcstring[srclen-1] == '\n' ||
                     srcstring[srclen-1] == '\r'))
    {
        srcstring[--srclen] = '\0';
    }
    sdssetlen(srcstring,srclen);
    ldb.src = sdssplitlen(srcstring,sdslen(srcstring),"\n",1,&ldb.lines);
    sdsfree(srcstring);
    return 1;
}

/* End a debugging session after the EVAL call with debugging enabled
 * returned. */

/* 在返回启用调试的EVAL调用后结束调试会话 */
void ldbEndSession(client *c) {
    /* Emit the remaining logs and an <endsession> mark. */

    /* 发出剩余日志和 ＜endsession＞ 标记 */
    ldbLog(sdsnew("<endsession>"));
    ldbSendLogs();

    /* If it's a fork()ed session, we just exit. */

    /* 如果是 fork() 会话，我们就退出 */
    if (ldb.forked) {
        writeToClient(c,0);
        serverLog(LL_WARNING,"Lua debugging session child exiting");
        exitFromChild(0);
    } else {
        serverLog(LL_WARNING,
            "Redis synchronous debugging eval session ended");
    }

    /* Otherwise let's restore client's state. */

    /* 否则，让我们恢复客户端的状态
     * 设置连接相关状态 */
    connNonBlock(ldb.conn);
    connSendTimeout(ldb.conn,0);

    /* Close the client connection after sending the final EVAL reply
     * in order to signal the end of the debugging session. */

    /* 在发送最终 EVAL 回复后关闭客户端连接，以发出调试会话结束的信号 */
    c->flags |= CLIENT_CLOSE_AFTER_REPLY;

    /* Cleanup. */
    sdsfreesplitres(ldb.src,ldb.lines);
    ldb.lines = 0;
    ldb.active = 0;
}

/* If the specified pid is among the list of children spawned for
 * forked debugging sessions, it is removed from the children list.
 * If the pid was found non-zero is returned. */

/* 如果指定的 pid 在为分叉调试会话生成的子级列表中，则会从子级列表中将其删除。如果找到 pid，则返回非零 */
int ldbRemoveChild(pid_t pid) {
    listNode *ln = listSearchKey(ldb.children,(void*)(unsigned long)pid);
    if (ln) {
        listDelNode(ldb.children,ln);
        return 1;
    }
    return 0;
}

/* Return the number of children we still did not receive termination
 * acknowledge via wait() in the parent process. */

/* 返回父进程中仍然没有通过 wait() 收到终止确认的子进程数 */
int ldbPendingChildren(void) {
    return listLength(ldb.children);
}

/* Kill all the forked sessions. */
void ldbKillForkedSessions(void) {
    listIter li;
    listNode *ln;

    listRewind(ldb.children,&li);
    while((ln = listNext(&li))) {
        pid_t pid = (unsigned long) ln->value;
        serverLog(LL_WARNING,"Killing debugging session %ld",(long)pid);
        kill(pid,SIGKILL);
    }
    listRelease(ldb.children);
    ldb.children = listCreate();
}

/* Wrapper for EVAL / EVALSHA that enables debugging, and makes sure
 * that when EVAL returns, whatever happened, the session is ended. */

/* EVAL/EVALSHA 的包装器，启用调试，并确保当 EVAL 返回时，无论发生了什么，会话都结束, 如果当前 client 为 lua_debug 模式，则会进入当前逻辑 */
void evalGenericCommandWithDebugging(client *c, int evalsha) {
    /* 返回0，说明不需要保持会话，要么就是异步父进程，或者是子进程创建失败 */
    if (ldbStartSession(c)) {
        evalGenericCommand(c,evalsha);
        ldbEndSession(c);
    } else {
        ldbDisable(c);
    }
}

/* Return a pointer to ldb.src source code line, considering line to be
 * one-based, and returning a special string for out of range lines. */

/* 返回指向 ldb 的指针。src 源代码行，将该行视为一行，并为超出范围的行返回一个特殊字符串 */
char *ldbGetSourceLine(int line) {
    int idx = line-1;
    if (idx < 0 || idx >= ldb.lines) return "<out of range source code line>";
    return ldb.src[idx];
}

/* Return true if there is a breakpoint in the specified line. */
int ldbIsBreakpoint(int line) {
    int j;

    for (j = 0; j < ldb.bpcount; j++)
        if (ldb.bp[j] == line) return 1;
    return 0;
}

/* Add the specified breakpoint. Ignore it if we already reached the max.
 * Returns 1 if the breakpoint was added (or was already set). 0 if there is
 * no space for the breakpoint or if the line is invalid. */

/* 添加指定的断点。如果我们已经达到最大值，请忽略它。
 * 如果断点已添加（或已设置），则返回1。如果断点没有空格或该行无效，则为0 */
int ldbAddBreakpoint(int line) {
    if (line <= 0 || line > ldb.lines) return 0;
    if (!ldbIsBreakpoint(line) && ldb.bpcount != LDB_BREAKPOINTS_MAX) {
        ldb.bp[ldb.bpcount++] = line;
        return 1;
    }
    return 0;
}

/* Remove the specified breakpoint, returning 1 if the operation was
 * performed or 0 if there was no such breakpoint. */

/* 删除指定的断点，如果执行了操作，则返回1；如果没有此类断点，则返回0 */
int ldbDelBreakpoint(int line) {
    int j;

    for (j = 0; j < ldb.bpcount; j++) {
        if (ldb.bp[j] == line) {
            ldb.bpcount--;
            memmove(ldb.bp+j,ldb.bp+j+1,ldb.bpcount-j);
            return 1;
        }
    }
    return 0;
}

/* Expect a valid multi-bulk command in the debugging client query buffer.
 * On success the command is parsed and returned as an array of SDS strings,
 * otherwise NULL is returned and there is to read more buffer. */

/* 调试客户端查询缓冲区中应包含有效的多批量命令。
 * 成功后，命令被解析并作为 SDS 字符串数组返回，否则返回 NULL，需要读取更多缓冲区*/
sds *ldbReplParseCommand(int *argcp, char** err) {
    static char* protocol_error = "protocol error";
    sds *argv = NULL;
    int argc = 0;
    if (sdslen(ldb.cbuf) == 0) return NULL;

    /* Working on a copy is simpler in this case. We can modify it freely
     * for the sake of simpler parsing. */

    /* 在这种情况下，处理副本更简单。为了更简单的解析，我们可以自由地修改它 */
    sds copy = sdsdup(ldb.cbuf);
    char *p = copy;

    /* This Redis protocol parser is a joke... just the simplest thing that
     * works in this context. It is also very forgiving regarding broken
     * protocol. */

    /* 这个 Redis 协议解析器是一个笑话……在这个上下文中工作的最简单的东西。对于违反协议，这也是非常宽容的 */

    /* Seek and parse *<count>\r\n. */
    p = strchr(p,'*'); if (!p) goto protoerr;
    char *plen = p+1; /* Multi bulk len pointer. */
    p = strstr(p,"\r\n"); if (!p) goto keep_reading;
    *p = '\0'; p += 2;
    *argcp = atoi(plen);
    if (*argcp <= 0 || *argcp > 1024) goto protoerr;

    /* Parse each argument. */
    argv = zmalloc(sizeof(sds)*(*argcp));
    argc = 0;
    while(argc < *argcp) {
        /* reached the end but there should be more data to read */
        if (*p == '\0') goto keep_reading;

        if (*p != '$') goto protoerr;
        plen = p+1; /* Bulk string len pointer. */
        p = strstr(p,"\r\n"); if (!p) goto keep_reading;
        *p = '\0'; p += 2;
        int slen = atoi(plen); /* Length of this arg. */
        if (slen <= 0 || slen > 1024) goto protoerr;
        if ((size_t)(p + slen + 2 - copy) > sdslen(copy) ) goto keep_reading;
        argv[argc++] = sdsnewlen(p,slen);
        p += slen; /* Skip the already parsed argument. */
        if (p[0] != '\r' || p[1] != '\n') goto protoerr;
        p += 2; /* Skip \r\n. */
    }
    sdsfree(copy);
    return argv;

protoerr:
    *err = protocol_error;
keep_reading:
    sdsfreesplitres(argv,argc);
    sdsfree(copy);
    return NULL;
}

/* Log the specified line in the Lua debugger output. */

/* 在 Lua 调试器输出中记录指定的行 */
void ldbLogSourceLine(int lnum) {
    char *line = ldbGetSourceLine(lnum);
    char *prefix;
    int bp = ldbIsBreakpoint(lnum);
    int current = ldb.currentline == lnum;

    if (current && bp)
        prefix = "->#";
    else if (current)
        prefix = "-> ";
    else if (bp)
        prefix = "  #";
    else
        prefix = "   ";
    sds thisline = sdscatprintf(sdsempty(),"%s%-3d %s", prefix, lnum, line);
    ldbLog(thisline);
}

/* Implement the "list" command of the Lua debugger. If around is 0
 * the whole file is listed, otherwise only a small portion of the file
 * around the specified line is shown. When a line number is specified
 * the amount of context (lines before/after) is specified via the
 * 'context' argument. */

/* 实现 Lua 调试器的 “list” 命令。如果 around 为0，则会列出整个文件，否则只显示指定行周围文件的一小部分。
 * 当指定行号时，通过 “context” 参数指定上下文量（前后行）*/
void ldbList(int around, int context) {
    int j;

    for (j = 1; j <= ldb.lines; j++) {
        if (around != 0 && abs(around-j) > context) continue;
        ldbLogSourceLine(j);
    }
}

/* Append a human readable representation of the Lua value at position 'idx'
 * on the stack of the 'lua' state, to the SDS string passed as argument.
 * The new SDS string with the represented value attached is returned.
 * Used in order to implement ldbLogStackValue().
 *
 * The element is not automatically removed from the stack, nor it is
 * converted to a different type. */

/* 将 “Lua” 状态堆栈上位置 “idx” 处的 Lua 值的可读表示形式附加到作为参数传递的 SDS 字符串。
 * 返回附加了表示值的新 SDS 字符串。
 * 用于实现 ldbLogStackValue() 
 * 
 * 元素不会自动从堆栈中移除，也不会转换为其他类型 */
#define LDB_MAX_VALUES_DEPTH (LUA_MINSTACK/2)
sds ldbCatStackValueRec(sds s, lua_State *lua, int idx, int level) {
    int t = lua_type(lua,idx);

    if (level++ == LDB_MAX_VALUES_DEPTH)
        return sdscat(s,"<max recursion level reached! Nested table?>");

    switch(t) {
    case LUA_TSTRING:
        {
        size_t strl;
        char *strp = (char*)lua_tolstring(lua,idx,&strl);
        s = sdscatrepr(s,strp,strl);
        }
        break;
    case LUA_TBOOLEAN:
        s = sdscat(s,lua_toboolean(lua,idx) ? "true" : "false");
        break;
    case LUA_TNUMBER:
        s = sdscatprintf(s,"%g",(double)lua_tonumber(lua,idx));
        break;
    case LUA_TNIL:
        s = sdscatlen(s,"nil",3);
        break;
    case LUA_TTABLE:
        {
        int expected_index = 1; /* First index we expect in an array. */
        int is_array = 1; /* Will be set to null if check fails. */
        /* Note: we create two representations at the same time, one
         * assuming the table is an array, one assuming it is not. At the
         * end we know what is true and select the right one. */

        /* 注意：我们同时创建了两个表示，一个假设表是数组，另一个假设它不是数组。最后，我们知道什么是正确的，并选择正确的 */
        sds repr1 = sdsempty();
        sds repr2 = sdsempty();
        lua_pushnil(lua); /* The first key to start the iteration is nil. */
        while (lua_next(lua,idx-1)) {
            /* Test if so far the table looks like an array. */
            if (is_array &&
                (lua_type(lua,-2) != LUA_TNUMBER ||
                 lua_tonumber(lua,-2) != expected_index)) is_array = 0;
            /* Stack now: table, key, value */
            /* Array repr. */
            repr1 = ldbCatStackValueRec(repr1,lua,-1,level);
            repr1 = sdscatlen(repr1,"; ",2);
            /* Full repr. */
            repr2 = sdscatlen(repr2,"[",1);
            repr2 = ldbCatStackValueRec(repr2,lua,-2,level);
            repr2 = sdscatlen(repr2,"]=",2);
            repr2 = ldbCatStackValueRec(repr2,lua,-1,level);
            repr2 = sdscatlen(repr2,"; ",2);
            lua_pop(lua,1); /* Stack: table, key. Ready for next iteration. */
            expected_index++;
        }
        /* Strip the last " ;" from both the representations. */
        if (sdslen(repr1)) sdsrange(repr1,0,-3);
        if (sdslen(repr2)) sdsrange(repr2,0,-3);
        /* Select the right one and discard the other. */
        s = sdscatlen(s,"{",1);
        s = sdscatsds(s,is_array ? repr1 : repr2);
        s = sdscatlen(s,"}",1);
        sdsfree(repr1);
        sdsfree(repr2);
        }
        break;
    case LUA_TFUNCTION:
    case LUA_TUSERDATA:
    case LUA_TTHREAD:
    case LUA_TLIGHTUSERDATA:
        {
        const void *p = lua_topointer(lua,idx);
        char *typename = "unknown";
        if (t == LUA_TFUNCTION) typename = "function";
        else if (t == LUA_TUSERDATA) typename = "userdata";
        else if (t == LUA_TTHREAD) typename = "thread";
        else if (t == LUA_TLIGHTUSERDATA) typename = "light-userdata";
        s = sdscatprintf(s,"\"%s@%p\"",typename,p);
        }
        break;
    default:
        s = sdscat(s,"\"<unknown-lua-type>\"");
        break;
    }
    return s;
}

/* Higher level wrapper for ldbCatStackValueRec() that just uses an initial
 * recursion level of '0'. */

/* ldbCatStackValueRec() 的高级包装，它只使用初始递归级别 “0” */
sds ldbCatStackValue(sds s, lua_State *lua, int idx) {
    return ldbCatStackValueRec(s,lua,idx,0);
}

/* Produce a debugger log entry representing the value of the Lua object
 * currently on the top of the stack. The element is not popped nor modified.
 * Check ldbCatStackValue() for the actual implementation. */

/* 生成一个调试器日志条目，表示当前位于堆栈顶部的Lua对象的值。不会弹出或修改元素。
 * 检查 ldbCatStackValue() 以了解实际实现 */
void ldbLogStackValue(lua_State *lua, char *prefix) {
    sds s = sdsnew(prefix);
    s = ldbCatStackValue(s,lua,-1);
    ldbLogWithMaxLen(s);
}

char *ldbRedisProtocolToHuman_Int(sds *o, char *reply);
char *ldbRedisProtocolToHuman_Bulk(sds *o, char *reply);
char *ldbRedisProtocolToHuman_Status(sds *o, char *reply);
char *ldbRedisProtocolToHuman_MultiBulk(sds *o, char *reply);
char *ldbRedisProtocolToHuman_Set(sds *o, char *reply);
char *ldbRedisProtocolToHuman_Map(sds *o, char *reply);
char *ldbRedisProtocolToHuman_Null(sds *o, char *reply);
char *ldbRedisProtocolToHuman_Bool(sds *o, char *reply);
char *ldbRedisProtocolToHuman_Double(sds *o, char *reply);

/* Get Redis protocol from 'reply' and appends it in human readable form to
 * the passed SDS string 'o'.
 *
 * Note that the SDS string is passed by reference (pointer of pointer to
 * char*) so that we can return a modified pointer, as for SDS semantics. */

/* 从 “reply” 中获取 Redis 协议，并将其以人类可读的形式附加到传递的 SDS 字符串 “o” 中。
 *
 * 请注意， SDS 字符串是通过引用（指向 char* 的指针）传递的，因此我们可以返回一个修改过的指针，就像 SDS 语义一样*/
char *ldbRedisProtocolToHuman(sds *o, char *reply) {
    char *p = reply;
    switch(*p) {
    case ':': p = ldbRedisProtocolToHuman_Int(o,reply); break;
    case '$': p = ldbRedisProtocolToHuman_Bulk(o,reply); break;
    case '+': p = ldbRedisProtocolToHuman_Status(o,reply); break;
    case '-': p = ldbRedisProtocolToHuman_Status(o,reply); break;
    case '*': p = ldbRedisProtocolToHuman_MultiBulk(o,reply); break;
    case '~': p = ldbRedisProtocolToHuman_Set(o,reply); break;
    case '%': p = ldbRedisProtocolToHuman_Map(o,reply); break;
    case '_': p = ldbRedisProtocolToHuman_Null(o,reply); break;
    case '#': p = ldbRedisProtocolToHuman_Bool(o,reply); break;
    case ',': p = ldbRedisProtocolToHuman_Double(o,reply); break;
    }
    return p;
}

/* The following functions are helpers for ldbRedisProtocolToHuman(), each
 * take care of a given Redis return type. */

/* 以下函数是 ldbRedisProtocolToHuman() 的助手，每个函数负责给定的 Redis 返回类型 */
char *ldbRedisProtocolToHuman_Int(sds *o, char *reply) {
    char *p = strchr(reply+1,'\r');
    *o = sdscatlen(*o,reply+1,p-reply-1);
    return p+2;
}

char *ldbRedisProtocolToHuman_Bulk(sds *o, char *reply) {
    char *p = strchr(reply+1,'\r');
    long long bulklen;

    string2ll(reply+1,p-reply-1,&bulklen);
    if (bulklen == -1) {
        *o = sdscatlen(*o,"NULL",4);
        return p+2;
    } else {
        *o = sdscatrepr(*o,p+2,bulklen);
        return p+2+bulklen+2;
    }
}

char *ldbRedisProtocolToHuman_Status(sds *o, char *reply) {
    char *p = strchr(reply+1,'\r');

    *o = sdscatrepr(*o,reply,p-reply);
    return p+2;
}

char *ldbRedisProtocolToHuman_MultiBulk(sds *o, char *reply) {
    char *p = strchr(reply+1,'\r');
    long long mbulklen;
    int j = 0;

    string2ll(reply+1,p-reply-1,&mbulklen);
    p += 2;
    if (mbulklen == -1) {
        *o = sdscatlen(*o,"NULL",4);
        return p;
    }
    *o = sdscatlen(*o,"[",1);
    for (j = 0; j < mbulklen; j++) {
        p = ldbRedisProtocolToHuman(o,p);
        if (j != mbulklen-1) *o = sdscatlen(*o,",",1);
    }
    *o = sdscatlen(*o,"]",1);
    return p;
}

char *ldbRedisProtocolToHuman_Set(sds *o, char *reply) {
    char *p = strchr(reply+1,'\r');
    long long mbulklen;
    int j = 0;

    string2ll(reply+1,p-reply-1,&mbulklen);
    p += 2;
    *o = sdscatlen(*o,"~(",2);
    for (j = 0; j < mbulklen; j++) {
        p = ldbRedisProtocolToHuman(o,p);
        if (j != mbulklen-1) *o = sdscatlen(*o,",",1);
    }
    *o = sdscatlen(*o,")",1);
    return p;
}

char *ldbRedisProtocolToHuman_Map(sds *o, char *reply) {
    char *p = strchr(reply+1,'\r');
    long long mbulklen;
    int j = 0;

    string2ll(reply+1,p-reply-1,&mbulklen);
    p += 2;
    *o = sdscatlen(*o,"{",1);
    for (j = 0; j < mbulklen; j++) {
        p = ldbRedisProtocolToHuman(o,p);
        *o = sdscatlen(*o," => ",4);
        p = ldbRedisProtocolToHuman(o,p);
        if (j != mbulklen-1) *o = sdscatlen(*o,",",1);
    }
    *o = sdscatlen(*o,"}",1);
    return p;
}

char *ldbRedisProtocolToHuman_Null(sds *o, char *reply) {
    char *p = strchr(reply+1,'\r');
    *o = sdscatlen(*o,"(null)",6);
    return p+2;
}

char *ldbRedisProtocolToHuman_Bool(sds *o, char *reply) {
    char *p = strchr(reply+1,'\r');
    if (reply[1] == 't')
        *o = sdscatlen(*o,"#true",5);
    else
        *o = sdscatlen(*o,"#false",6);
    return p+2;
}

char *ldbRedisProtocolToHuman_Double(sds *o, char *reply) {
    char *p = strchr(reply+1,'\r');
    *o = sdscatlen(*o,"(double) ",9);
    *o = sdscatlen(*o,reply+1,p-reply-1);
    return p+2;
}

/* Log a Redis reply as debugger output, in a human readable format.
 * If the resulting string is longer than 'len' plus a few more chars
 * used as prefix, it gets truncated. */

/* 以人类可读的格式将 Redis 回复记录为调试器输出。
 * 如果生成的字符串比 “len” 长，再加上几个用作前缀的字符，它将被截断 */
void ldbLogRedisReply(char *reply) {
    sds log = sdsnew("<reply> ");
    ldbRedisProtocolToHuman(&log,reply);
    ldbLogWithMaxLen(log);
}

/* Implements the "print <var>" command of the Lua debugger. It scans for Lua
 * var "varname" starting from the current stack frame up to the top stack
 * frame. The first matching variable is printed. */

/* 实现 Lua 调试器的 “print＜var＞” 命令。它从当前堆栈帧开始扫描 Lua-var“varname” ，直到堆栈顶部帧。打印第一个匹配变量 */
void ldbPrint(lua_State *lua, char *varname) {
    lua_Debug ar;

    int l = 0; /* Stack level. */
    while (lua_getstack(lua,l,&ar) != 0) {
        l++;
        const char *name;
        int i = 1; /* Variable index. */
        while((name = lua_getlocal(lua,&ar,i)) != NULL) {
            i++;
            if (strcmp(varname,name) == 0) {
                ldbLogStackValue(lua,"<value> ");
                lua_pop(lua,1);
                return;
            } else {
                lua_pop(lua,1); /* Discard the var name on the stack. */
            }
        }
    }

    /* Let's try with global vars in two selected cases */
    if (!strcmp(varname,"ARGV") || !strcmp(varname,"KEYS")) {
        lua_getglobal(lua, varname);
        ldbLogStackValue(lua,"<value> ");
        lua_pop(lua,1);
    } else {
        ldbLog(sdsnew("No such variable."));
    }
}

/* Implements the "print" command (without arguments) of the Lua debugger.
 * Prints all the variables in the current stack frame. */

/* 实现Lua调试器的 “print” 命令（无参数）。
 * 打印当前堆栈帧中的所有变量 */
void ldbPrintAll(lua_State *lua) {
    lua_Debug ar;
    int vars = 0;

    if (lua_getstack(lua,0,&ar) != 0) {
        const char *name;
        int i = 1; /* Variable index. */
        while((name = lua_getlocal(lua,&ar,i)) != NULL) {
            i++;
            if (!strstr(name,"(*temporary)")) {
                sds prefix = sdscatprintf(sdsempty(),"<value> %s = ",name);
                ldbLogStackValue(lua,prefix);
                sdsfree(prefix);
                vars++;
            }
            lua_pop(lua,1);
        }
    }

    if (vars == 0) {
        ldbLog(sdsnew("No local variables in the current context."));
    }
}

/* Implements the break command to list, add and remove breakpoints. */

/* 实现 break 命令以列出、添加和删除断点 */
void ldbBreak(sds *argv, int argc) {
    if (argc == 1) {
        if (ldb.bpcount == 0) {
            ldbLog(sdsnew("No breakpoints set. Use 'b <line>' to add one."));
            return;
        } else {
            ldbLog(sdscatfmt(sdsempty(),"%i breakpoints set:",ldb.bpcount));
            int j;
            for (j = 0; j < ldb.bpcount; j++)
                ldbLogSourceLine(ldb.bp[j]);
        }
    } else {
        int j;
        for (j = 1; j < argc; j++) {
            char *arg = argv[j];
            long line;
            if (!string2l(arg,sdslen(arg),&line)) {
                ldbLog(sdscatfmt(sdsempty(),"Invalid argument:'%s'",arg));
            } else {
                if (line == 0) {
                    ldb.bpcount = 0;
                    ldbLog(sdsnew("All breakpoints removed."));
                } else if (line > 0) {
                    if (ldb.bpcount == LDB_BREAKPOINTS_MAX) {
                        ldbLog(sdsnew("Too many breakpoints set."));
                    } else if (ldbAddBreakpoint(line)) {
                        ldbList(line,1);
                    } else {
                        ldbLog(sdsnew("Wrong line number."));
                    }
                } else if (line < 0) {
                    if (ldbDelBreakpoint(-line))
                        ldbLog(sdsnew("Breakpoint removed."));
                    else
                        ldbLog(sdsnew("No breakpoint in the specified line."));
                }
            }
        }
    }
}

/* Implements the Lua debugger "eval" command. It just compiles the user
 * passed fragment of code and executes it, showing the result left on
 * the stack. */

/* 实现 Lua 调试器 “eval” 命令。它只编译用户传递的代码片段并执行它，显示堆栈上留下的结果 */
void ldbEval(lua_State *lua, sds *argv, int argc) {
    /* Glue the script together if it is composed of multiple arguments. */

    /* 如果脚本由多个参数组成，请将其粘合在一起 */
    sds code = sdsjoinsds(argv+1,argc-1," ",1);
    sds expr = sdscatsds(sdsnew("return "),code);

    /* Try to compile it as an expression, prepending "return ". */

    /* 尝试将其编译为表达式，并在 “return” 之前 */
    if (luaL_loadbuffer(lua,expr,sdslen(expr),"@ldb_eval")) {
        lua_pop(lua,1);
        /* Failed? Try as a statement. */
        if (luaL_loadbuffer(lua,code,sdslen(code),"@ldb_eval")) {
            ldbLog(sdscatfmt(sdsempty(),"<error> %s",lua_tostring(lua,-1)));
            lua_pop(lua,1);
            sdsfree(code);
            sdsfree(expr);
            return;
        }
    }

    /* Call it. */
    sdsfree(code);
    sdsfree(expr);
    if (lua_pcall(lua,0,1,0)) {
        ldbLog(sdscatfmt(sdsempty(),"<error> %s",lua_tostring(lua,-1)));
        lua_pop(lua,1);
        return;
    }
    ldbLogStackValue(lua,"<retval> ");
    lua_pop(lua,1);
}

/* Implement the debugger "redis" command. We use a trick in order to make
 * the implementation very simple: we just call the Lua redis.call() command
 * implementation, with ldb.step enabled, so as a side effect the Redis command
 * and its reply are logged. */

/* 执行调试器 “redis” 命令。为了使实现变得非常简单，我们使用了一个技巧：
 * 我们只需调用 Lua redis.call() 命令实现，使用 ldb.step 已启用，因此作为副作用，Redis 命令及其回复将被记录 */
void ldbRedis(lua_State *lua, sds *argv, int argc) {
    int j;

    if (!lua_checkstack(lua, argc + 1)) {
        /* Increase the Lua stack if needed to make sure there is enough room
         * to push 'argc + 1' elements to the stack. On failure, return error.
         * Notice that we need, in worst case, 'argc + 1' elements because we push all the arguments
         * given by the user (without the first argument) and we also push the 'redis' global table and
         * 'redis.call' function so:
         * (1 (redis table)) + (1 (redis.call function)) + (argc - 1 (all arguments without the first)) = argc + 1*/
        ldbLogRedisReply("max lua stack reached");
        return;
    }

    lua_getglobal(lua,"redis");
    lua_pushstring(lua,"call");
    lua_gettable(lua,-2);       /* Stack: redis, redis.call */
    for (j = 1; j < argc; j++)
        lua_pushlstring(lua,argv[j],sdslen(argv[j]));
    ldb.step = 1;               /* Force redis.call() to log. */
    lua_pcall(lua,argc-1,1,0);  /* Stack: redis, result */
    ldb.step = 0;               /* Disable logging. */
    lua_pop(lua,2);             /* Discard the result and clean the stack. */
}

/* Implements "trace" command of the Lua debugger. It just prints a backtrace
 * querying Lua starting from the current callframe back to the outer one. */

/* 实现 Lua 调试器的 “trace” 命令。它只打印从当前调用帧开始到外部调用帧的回溯查询 Lua */
void ldbTrace(lua_State *lua) {
    lua_Debug ar;
    int level = 0;

    while(lua_getstack(lua,level,&ar)) {
        lua_getinfo(lua,"Snl",&ar);
        if(strstr(ar.short_src,"user_script") != NULL) {
            ldbLog(sdscatprintf(sdsempty(),"%s %s:",
                (level == 0) ? "In" : "From",
                ar.name ? ar.name : "top level"));
            ldbLogSourceLine(ar.currentline);
        }
        level++;
    }
    if (level == 0) {
        ldbLog(sdsnew("<error> Can't retrieve Lua stack."));
    }
}

/* Implements the debugger "maxlen" command. It just queries or sets the
 * ldb.maxlen variable. */

/* 实现调试器 “maxlen” 命令。它只是查询或设置 ldb.maxlen 变量 */
void ldbMaxlen(sds *argv, int argc) {
    if (argc == 2) {
        int newval = atoi(argv[1]);
        ldb.maxlen_hint_sent = 1; /* User knows about this command. */
        if (newval != 0 && newval <= 60) newval = 60;
        ldb.maxlen = newval;
    }
    if (ldb.maxlen) {
        ldbLog(sdscatprintf(sdsempty(),"<value> replies are truncated at %d bytes.",(int)ldb.maxlen));
    } else {
        ldbLog(sdscatprintf(sdsempty(),"<value> replies are unlimited."));
    }
}

/* Read debugging commands from client.
 * Return C_OK if the debugging session is continuing, otherwise
 * C_ERR if the client closed the connection or is timing out. */

/* 从客户端读取调试命令。
 * 如果调试会话正在继续，则返回 C_OK ；否则，如果客户端关闭连接或超时，返回 C_ERR */
int ldbRepl(lua_State *lua) {
    sds *argv;
    int argc;
    char* err = NULL;

    /* We continue processing commands until a command that should return
     * to the Lua interpreter is found. */

    /* 我们继续处理命令，直到找到应该返回到 Lua 解释器的命令 */
    while(1) {
        while((argv = ldbReplParseCommand(&argc, &err)) == NULL) {
            char buf[1024];
            if (err) {
                luaPushError(lua, err);
                luaError(lua);
            }
            int nread = connRead(ldb.conn,buf,sizeof(buf));
            if (nread <= 0) {
                /* Make sure the script runs without user input since the
                 * client is no longer connected. */
                ldb.step = 0;
                ldb.bpcount = 0;
                return C_ERR;
            }
            ldb.cbuf = sdscatlen(ldb.cbuf,buf,nread);
            /* after 1M we will exit with an error
             * so that the client will not blow the memory
             */
            if (sdslen(ldb.cbuf) > 1<<20) {
                sdsfree(ldb.cbuf);
                ldb.cbuf = sdsempty();
                luaPushError(lua, "max client buffer reached");
                luaError(lua);
            }
        }

        /* Flush the old buffer. */
        sdsfree(ldb.cbuf);
        ldb.cbuf = sdsempty();

        /* Execute the command. */
        if (!strcasecmp(argv[0],"h") || !strcasecmp(argv[0],"help")) {
ldbLog(sdsnew("Redis Lua debugger help:"));
ldbLog(sdsnew("[h]elp               Show this help."));
ldbLog(sdsnew("[s]tep               Run current line and stop again."));
ldbLog(sdsnew("[n]ext               Alias for step."));
ldbLog(sdsnew("[c]ontinue           Run till next breakpoint."));
ldbLog(sdsnew("[l]ist               List source code around current line."));
ldbLog(sdsnew("[l]ist [line]        List source code around [line]."));
ldbLog(sdsnew("                     line = 0 means: current position."));
ldbLog(sdsnew("[l]ist [line] [ctx]  In this form [ctx] specifies how many lines"));
ldbLog(sdsnew("                     to show before/after [line]."));
ldbLog(sdsnew("[w]hole              List all source code. Alias for 'list 1 1000000'."));
ldbLog(sdsnew("[p]rint              Show all the local variables."));
ldbLog(sdsnew("[p]rint <var>        Show the value of the specified variable."));
ldbLog(sdsnew("                     Can also show global vars KEYS and ARGV."));
ldbLog(sdsnew("[b]reak              Show all breakpoints."));
ldbLog(sdsnew("[b]reak <line>       Add a breakpoint to the specified line."));
ldbLog(sdsnew("[b]reak -<line>      Remove breakpoint from the specified line."));
ldbLog(sdsnew("[b]reak 0            Remove all breakpoints."));
ldbLog(sdsnew("[t]race              Show a backtrace."));
ldbLog(sdsnew("[e]val <code>        Execute some Lua code (in a different callframe)."));
ldbLog(sdsnew("[r]edis <cmd>        Execute a Redis command."));
ldbLog(sdsnew("[m]axlen [len]       Trim logged Redis replies and Lua var dumps to len."));
ldbLog(sdsnew("                     Specifying zero as <len> means unlimited."));
ldbLog(sdsnew("[a]bort              Stop the execution of the script. In sync"));
ldbLog(sdsnew("                     mode dataset changes will be retained."));
ldbLog(sdsnew(""));
ldbLog(sdsnew("Debugger functions you can call from Lua scripts:"));
ldbLog(sdsnew("redis.debug()        Produce logs in the debugger console."));
ldbLog(sdsnew("redis.breakpoint()   Stop execution like if there was a breakpoint in the"));
ldbLog(sdsnew("                     next line of code."));
            ldbSendLogs();
        } else if (!strcasecmp(argv[0],"s") || !strcasecmp(argv[0],"step") ||
                   !strcasecmp(argv[0],"n") || !strcasecmp(argv[0],"next")) {
            ldb.step = 1;
            break;
        } else if (!strcasecmp(argv[0],"c") || !strcasecmp(argv[0],"continue")){
            break;
        } else if (!strcasecmp(argv[0],"t") || !strcasecmp(argv[0],"trace")) {
            ldbTrace(lua);
            ldbSendLogs();
        } else if (!strcasecmp(argv[0],"m") || !strcasecmp(argv[0],"maxlen")) {
            ldbMaxlen(argv,argc);
            ldbSendLogs();
        } else if (!strcasecmp(argv[0],"b") || !strcasecmp(argv[0],"break")) {
            ldbBreak(argv,argc);
            ldbSendLogs();
        } else if (!strcasecmp(argv[0],"e") || !strcasecmp(argv[0],"eval")) {
            ldbEval(lua,argv,argc);
            ldbSendLogs();
        } else if (!strcasecmp(argv[0],"a") || !strcasecmp(argv[0],"abort")) {
            luaPushError(lua, "script aborted for user request");
            luaError(lua);
        } else if (argc > 1 &&
                   (!strcasecmp(argv[0],"r") || !strcasecmp(argv[0],"redis"))) {
            ldbRedis(lua,argv,argc);
            ldbSendLogs();
        } else if ((!strcasecmp(argv[0],"p") || !strcasecmp(argv[0],"print"))) {
            if (argc == 2)
                ldbPrint(lua,argv[1]);
            else
                ldbPrintAll(lua);
            ldbSendLogs();
        } else if (!strcasecmp(argv[0],"l") || !strcasecmp(argv[0],"list")){
            int around = ldb.currentline, ctx = 5;
            if (argc > 1) {
                int num = atoi(argv[1]);
                if (num > 0) around = num;
            }
            if (argc > 2) ctx = atoi(argv[2]);
            ldbList(around,ctx);
            ldbSendLogs();
        } else if (!strcasecmp(argv[0],"w") || !strcasecmp(argv[0],"whole")){
            ldbList(1,1000000);
            ldbSendLogs();
        } else {
            ldbLog(sdsnew("<error> Unknown Redis Lua debugger command or "
                          "wrong number of arguments."));
            ldbSendLogs();
        }

        /* Free the command vector. */
        sdsfreesplitres(argv,argc);
    }

    /* Free the current command argv if we break inside the while loop. */
    sdsfreesplitres(argv,argc);
    return C_OK;
}

/* This is the core of our Lua debugger, called each time Lua is about
 * to start executing a new line. */

/* 这是我们的 Lua 调试器的核心，每次 Lua 即将开始执行新行时都会调用它，主要通过 lua 的 hook 功能来调用 */
void luaLdbLineHook(lua_State *lua, lua_Debug *ar) {
    scriptRunCtx* rctx = luaGetFromRegistry(lua, REGISTRY_RUN_CTX_NAME);
    lua_getstack(lua,0,ar);
    lua_getinfo(lua,"Sl",ar);
    ldb.currentline = ar->currentline;

    /* 判断当前代码行是否为断点 */
    int bp = ldbIsBreakpoint(ldb.currentline) || ldb.luabp;
    int timeout = 0;

    /* Events outside our script are not interesting. */

    /* 我们脚本之外的事件并不有趣, 参考 @user_script */
    if(strstr(ar->short_src,"user_script") == NULL) return;

    /* Check if a timeout occurred. */

    /* 检测当前是否超过了 server 设定的最大耗时 */
    if (ar->event == LUA_HOOKCOUNT && ldb.step == 0 && bp == 0) {
        mstime_t elapsed = elapsedMs(rctx->start_time);
        mstime_t timelimit = server.busy_reply_threshold ?
                             server.busy_reply_threshold : 5000;
        if (elapsed >= timelimit) {
            timeout = 1;
            ldb.step = 1;
        } else {
            return; /* No timeout, ignore the COUNT event. */
        }
    }

    if (ldb.step || bp) {
        char *reason = "step over";
        if (bp) reason = ldb.luabp ? "redis.breakpoint() called" :
                                     "break point";
        else if (timeout) reason = "timeout reached, infinite loop?";
        ldb.step = 0;
        ldb.luabp = 0;

        /* 输出 stop 日志和当前的代码行信息 */
        ldbLog(sdscatprintf(sdsempty(),
            "* Stopped at %d, stop reason = %s",
            ldb.currentline, reason));
        ldbLogSourceLine(ldb.currentline);
        ldbSendLogs();

        /* 读取客户端的 debug 命令 */
        if (ldbRepl(lua) == C_ERR && timeout) {
            /* If the client closed the connection and we have a timeout
             * connection, let's kill the script otherwise the process
             * will remain blocked indefinitely. */

            /* 如果客户端关闭了连接，我们有一个超时连接，让我们终止脚本，否则进程将无限期地被阻塞 */
            luaPushError(lua, "timeout during Lua debugging with client closing connection");
            luaError(lua);
        }

        /* 重置当前的上下文时间 */
        rctx->start_time = getMonotonicUs();
        rctx->snapshot_time = mstime();
    }
}
