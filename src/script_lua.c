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

#include "script_lua.h"

#include "server.h"
#include "sha1.h"
#include "rand.h"
#include "cluster.h"
#include "monotonic.h"
#include "resp_parser.h"
#include "version.h"
#include <lauxlib.h>
#include <lualib.h>
#include <ctype.h>
#include <math.h>

/* Globals that are added by the Lua libraries */
static char *libraries_allow_list[] = {
    "string",
    "cjson",
    "bit",
    "cmsgpack",
    "math",
    "table",
    "struct",
    NULL,
};

/* Redis Lua API globals */
static char *redis_api_allow_list[] = {
    "redis",
    "__redis__err__handler", /* error handler for eval, currently located on globals.
                                Should move to registry. */
    NULL,
};

/* Lua builtins */
static char *lua_builtins_allow_list[] = {
    "xpcall",
    "tostring",
    "getfenv",
    "setmetatable",
    "next",
    "assert",
    "tonumber",
    "rawequal",
    "collectgarbage",
    "getmetatable",
    "rawset",
    "pcall",
    "coroutine",
    "type",
    "_G",
    "select",
    "unpack",
    "gcinfo",
    "pairs",
    "rawget",
    "loadstring",
    "ipairs",
    "_VERSION",
    "setfenv",
    "load",
    "error",
    NULL,
};

/* Lua builtins which are not documented on the Lua documentation */
static char *lua_builtins_not_documented_allow_list[] = {
    "newproxy",
    NULL,
};

/* Lua builtins which are allowed on initialization but will be removed right after */
static char *lua_builtins_removed_after_initialization_allow_list[] = {
    "debug", /* debug will be set to nil after the error handler will be created */
    NULL,
};

/* Those allow lists was created from the globals that was
 * available to the user when the allow lists was first introduce.
 * Because we do not want to break backward compatibility we keep
 * all the globals. The allow lists will prevent us from accidentally
 * creating unwanted globals in the future.
 *
 * Also notice that the allow list is only checked on start time,
 * after that the global table is locked so not need to check anything.*/

/* 这些允许列表是从允许列表首次引入时用户可用的全局变量创建的。
 * 因为我们不想破坏向后兼容性，所以我们保留了所有的全局变量。允许列表将防止我们在将来意外创建不需要的全局变量。
 *
 * 还要注意，只在开始时检查允许列表，之后全局表被锁定，因此无需检查任何内容 */
static char **allow_lists[] = {
    libraries_allow_list,
    redis_api_allow_list,
    lua_builtins_allow_list,
    lua_builtins_not_documented_allow_list,
    lua_builtins_removed_after_initialization_allow_list,
    NULL,
};

/* Deny list contains elements which we know we do not want to add to globals
 * and there is no need to print a warning message form them. We will print a
 * log message only if an element was added to the globals and the element is
 * not on the allow list nor on the back list. */

/* 拒绝列表包含我们知道不想添加到全局变量中的元素，
 * 并且不需要从中打印警告消息。
 * 只有当元素被添加到全局变量中并且该元素不在允许列表或后备列表中时，我们才会打印日志消息 */
static char *deny_list[] = {
    "dofile",
    "loadfile",
    "print",
    NULL,
};

static int redis_math_random (lua_State *L);
static int redis_math_randomseed (lua_State *L);
static void redisProtocolToLuaType_Int(void *ctx, long long val, const char *proto, size_t proto_len);
static void redisProtocolToLuaType_BulkString(void *ctx, const char *str, size_t len, const char *proto, size_t proto_len);
static void redisProtocolToLuaType_NullBulkString(void *ctx, const char *proto, size_t proto_len);
static void redisProtocolToLuaType_NullArray(void *ctx, const char *proto, size_t proto_len);
static void redisProtocolToLuaType_Status(void *ctx, const char *str, size_t len, const char *proto, size_t proto_len);
static void redisProtocolToLuaType_Error(void *ctx, const char *str, size_t len, const char *proto, size_t proto_len);
static void redisProtocolToLuaType_Array(struct ReplyParser *parser, void *ctx, size_t len, const char *proto);
static void redisProtocolToLuaType_Map(struct ReplyParser *parser, void *ctx, size_t len, const char *proto);
static void redisProtocolToLuaType_Set(struct ReplyParser *parser, void *ctx, size_t len, const char *proto);
static void redisProtocolToLuaType_Null(void *ctx, const char *proto, size_t proto_len);
static void redisProtocolToLuaType_Bool(void *ctx, int val, const char *proto, size_t proto_len);
static void redisProtocolToLuaType_Double(void *ctx, double d, const char *proto, size_t proto_len);
static void redisProtocolToLuaType_BigNumber(void *ctx, const char *str, size_t len, const char *proto, size_t proto_len);
static void redisProtocolToLuaType_VerbatimString(void *ctx, const char *format, const char *str, size_t len, const char *proto, size_t proto_len);
static void redisProtocolToLuaType_Attribute(struct ReplyParser *parser, void *ctx, size_t len, const char *proto);
static void luaReplyToRedisReply(client *c, client* script_client, lua_State *lua);

/*
 * Save the give pointer on Lua registry, used to save the Lua context and
 * function context so we can retrieve them from lua_State.
 */

/* 保存Lua注册表上的给定指针，用于保存Lua上下文和函数上下文，以便我们可以从Lua_State中检索它们。*/
void luaSaveOnRegistry(lua_State* lua, const char* name, void* ptr) {
    lua_pushstring(lua, name);
    if (ptr) {
        lua_pushlightuserdata(lua, ptr);
    } else {
        lua_pushnil(lua);
    }
    lua_settable(lua, LUA_REGISTRYINDEX);
}

/*
 * Get a saved pointer from registry
 */

 /* 获取一个共享对象从寄存器中，主要用于获取上下文 */
void* luaGetFromRegistry(lua_State* lua, const char* name) {
    lua_pushstring(lua, name);
    lua_gettable(lua, LUA_REGISTRYINDEX);

    if (lua_isnil(lua, -1)) {
        return NULL;
    }
    /* must be light user data */
    serverAssert(lua_islightuserdata(lua, -1));

    void* ptr = (void*) lua_topointer(lua, -1);
    serverAssert(ptr);

    /* pops the value */
    lua_pop(lua, 1);

    return ptr;
}

/* ---------------------------------------------------------------------------
 * Redis reply to Lua type conversion functions.
 * ------------------------------------------------------------------------- */

/* Take a Redis reply in the Redis protocol format and convert it into a
 * Lua type. Thanks to this function, and the introduction of not connected
 * clients, it is trivial to implement the redis() lua function.
 *
 * Basically we take the arguments, execute the Redis command in the context
 * of a non connected client, then take the generated reply and convert it
 * into a suitable Lua type. With this trick the scripting feature does not
 * need the introduction of a full Redis internals API. The script
 * is like a normal client that bypasses all the slow I/O paths.
 *
 * Note: in this function we do not do any sanity check as the reply is
 * generated by Redis directly. This allows us to go faster.
 *
 * Errors are returned as a table with a single 'err' field set to the
 * error string.
 */

static const ReplyParserCallbacks DefaultLuaTypeParserCallbacks = {
    .null_array_callback = redisProtocolToLuaType_NullArray,
    .bulk_string_callback = redisProtocolToLuaType_BulkString,
    .null_bulk_string_callback = redisProtocolToLuaType_NullBulkString,
    .error_callback = redisProtocolToLuaType_Error,
    .simple_str_callback = redisProtocolToLuaType_Status,
    .long_callback = redisProtocolToLuaType_Int,
    .array_callback = redisProtocolToLuaType_Array,
    .set_callback = redisProtocolToLuaType_Set,
    .map_callback = redisProtocolToLuaType_Map,
    .bool_callback = redisProtocolToLuaType_Bool,
    .double_callback = redisProtocolToLuaType_Double,
    .null_callback = redisProtocolToLuaType_Null,
    .big_number_callback = redisProtocolToLuaType_BigNumber,
    .verbatim_string_callback = redisProtocolToLuaType_VerbatimString,
    .attribute_callback = redisProtocolToLuaType_Attribute,
    .error = NULL,
};

static void redisProtocolToLuaType(lua_State *lua, char* reply) {
    ReplyParser parser = {.curr_location = reply, .callbacks = DefaultLuaTypeParserCallbacks};

    /* 格式化数据 */
    parseReply(&parser, lua);
}

static void redisProtocolToLuaType_Int(void *ctx, long long val, const char *proto, size_t proto_len) {
    UNUSED(proto);
    UNUSED(proto_len);
    if (!ctx) {
        return;
    }

    lua_State *lua = ctx;
    if (!lua_checkstack(lua, 1)) {
        /* Increase the Lua stack if needed, to make sure there is enough room
         * to push elements to the stack. On failure, exit with panic. */
        serverPanic("lua stack limit reach when parsing redis.call reply");
    }
    lua_pushnumber(lua,(lua_Number)val);
}

static void redisProtocolToLuaType_NullBulkString(void *ctx, const char *proto, size_t proto_len) {
    UNUSED(proto);
    UNUSED(proto_len);
    if (!ctx) {
        return;
    }

    lua_State *lua = ctx;
    if (!lua_checkstack(lua, 1)) {
        /* Increase the Lua stack if needed, to make sure there is enough room
         * to push elements to the stack. On failure, exit with panic. */
        serverPanic("lua stack limit reach when parsing redis.call reply");
    }
    lua_pushboolean(lua,0);
}

static void redisProtocolToLuaType_NullArray(void *ctx, const char *proto, size_t proto_len) {
    UNUSED(proto);
    UNUSED(proto_len);
    if (!ctx) {
        return;
    }
    lua_State *lua = ctx;
    if (!lua_checkstack(lua, 1)) {
        /* Increase the Lua stack if needed, to make sure there is enough room
         * to push elements to the stack. On failure, exit with panic. */
        serverPanic("lua stack limit reach when parsing redis.call reply");
    }
    lua_pushboolean(lua,0);
}


static void redisProtocolToLuaType_BulkString(void *ctx, const char *str, size_t len, const char *proto, size_t proto_len) {
    UNUSED(proto);
    UNUSED(proto_len);
    if (!ctx) {
        return;
    }

    lua_State *lua = ctx;
    if (!lua_checkstack(lua, 1)) {
        /* Increase the Lua stack if needed, to make sure there is enough room
         * to push elements to the stack. On failure, exit with panic. */
        serverPanic("lua stack limit reach when parsing redis.call reply");
    }
    lua_pushlstring(lua,str,len);
}

static void redisProtocolToLuaType_Status(void *ctx, const char *str, size_t len, const char *proto, size_t proto_len) {
    UNUSED(proto);
    UNUSED(proto_len);
    if (!ctx) {
        return;
    }

    lua_State *lua = ctx;
    if (!lua_checkstack(lua, 3)) {
        /* Increase the Lua stack if needed, to make sure there is enough room
         * to push elements to the stack. On failure, exit with panic. */
        serverPanic("lua stack limit reach when parsing redis.call reply");
    }
    lua_newtable(lua);
    lua_pushstring(lua,"ok");
    lua_pushlstring(lua,str,len);
    lua_settable(lua,-3);
}

static void redisProtocolToLuaType_Error(void *ctx, const char *str, size_t len, const char *proto, size_t proto_len) {
    UNUSED(proto);
    UNUSED(proto_len);
    if (!ctx) {
        return;
    }

    lua_State *lua = ctx;
    if (!lua_checkstack(lua, 3)) {
        /* Increase the Lua stack if needed, to make sure there is enough room
         * to push elements to the stack. On failure, exit with panic. */
        serverPanic("lua stack limit reach when parsing redis.call reply");
    }
    sds err_msg = sdscatlen(sdsnew("-"), str, len);
    luaPushErrorBuff(lua,err_msg);
    /* push a field indicate to ignore updating the stats on this error
     * because it was already updated when executing the command. */
    lua_pushstring(lua,"ignore_error_stats_update");
    lua_pushboolean(lua, 1);
    lua_settable(lua,-3);
}

static void redisProtocolToLuaType_Map(struct ReplyParser *parser, void *ctx, size_t len, const char *proto) {
    UNUSED(proto);
    lua_State *lua = ctx;
    if (lua) {
        if (!lua_checkstack(lua, 3)) {
            /* Increase the Lua stack if needed, to make sure there is enough room
             * to push elements to the stack. On failure, exit with panic. */
            serverPanic("lua stack limit reach when parsing redis.call reply");
        }
        lua_newtable(lua);
        lua_pushstring(lua, "map");
        lua_newtable(lua);
    }
    for (size_t j = 0; j < len; j++) {
        parseReply(parser,lua);
        parseReply(parser,lua);
        if (lua) lua_settable(lua,-3);
    }
    if (lua) lua_settable(lua,-3);
}

static void redisProtocolToLuaType_Set(struct ReplyParser *parser, void *ctx, size_t len, const char *proto) {
    UNUSED(proto);

    lua_State *lua = ctx;
    if (lua) {
        if (!lua_checkstack(lua, 3)) {
            /* Increase the Lua stack if needed, to make sure there is enough room
             * to push elements to the stack. On failure, exit with panic. */
            serverPanic("lua stack limit reach when parsing redis.call reply");
        }
        lua_newtable(lua);
        lua_pushstring(lua, "set");
        lua_newtable(lua);
    }
    for (size_t j = 0; j < len; j++) {
        parseReply(parser,lua);
        if (lua) {
            if (!lua_checkstack(lua, 1)) {
                /* Increase the Lua stack if needed, to make sure there is enough room
                 * to push elements to the stack. On failure, exit with panic.
                 * Notice that here we need to check the stack again because the recursive
                 * call to redisProtocolToLuaType might have use the room allocated in the stack*/
                serverPanic("lua stack limit reach when parsing redis.call reply");
            }
            lua_pushboolean(lua,1);
            lua_settable(lua,-3);
        }
    }
    if (lua) lua_settable(lua,-3);
}

static void redisProtocolToLuaType_Array(struct ReplyParser *parser, void *ctx, size_t len, const char *proto) {
    UNUSED(proto);

    lua_State *lua = ctx;
    if (lua){
        if (!lua_checkstack(lua, 2)) {
            /* Increase the Lua stack if needed, to make sure there is enough room
             * to push elements to the stack. On failure, exit with panic. */
            serverPanic("lua stack limit reach when parsing redis.call reply");
        }
        lua_newtable(lua);
    }
    for (size_t j = 0; j < len; j++) {
        if (lua) lua_pushnumber(lua,j+1);
        parseReply(parser,lua);
        if (lua) lua_settable(lua,-3);
    }
}

static void redisProtocolToLuaType_Attribute(struct ReplyParser *parser, void *ctx, size_t len, const char *proto) {
    UNUSED(proto);

    /* Parse the attribute reply.
     * Currently, we do not expose the attribute to the Lua script so
     * we just need to continue parsing and ignore it (the NULL ensures that the
     * reply will be ignored). */
    for (size_t j = 0; j < len; j++) {
        parseReply(parser,NULL);
        parseReply(parser,NULL);
    }

    /* Parse the reply itself. */
    parseReply(parser,ctx);
}

static void redisProtocolToLuaType_VerbatimString(void *ctx, const char *format, const char *str, size_t len, const char *proto, size_t proto_len) {
    UNUSED(proto);
    UNUSED(proto_len);
    if (!ctx) {
        return;
    }

    lua_State *lua = ctx;
    if (!lua_checkstack(lua, 5)) {
        /* Increase the Lua stack if needed, to make sure there is enough room
         * to push elements to the stack. On failure, exit with panic. */
        serverPanic("lua stack limit reach when parsing redis.call reply");
    }
    lua_newtable(lua);
    lua_pushstring(lua,"verbatim_string");
    lua_newtable(lua);
    lua_pushstring(lua,"string");
    lua_pushlstring(lua,str,len);
    lua_settable(lua,-3);
    lua_pushstring(lua,"format");
    lua_pushlstring(lua,format,3);
    lua_settable(lua,-3);
    lua_settable(lua,-3);
}

static void redisProtocolToLuaType_BigNumber(void *ctx, const char *str, size_t len, const char *proto, size_t proto_len) {
    UNUSED(proto);
    UNUSED(proto_len);
    if (!ctx) {
        return;
    }

    lua_State *lua = ctx;
    if (!lua_checkstack(lua, 3)) {
        /* Increase the Lua stack if needed, to make sure there is enough room
         * to push elements to the stack. On failure, exit with panic. */
        serverPanic("lua stack limit reach when parsing redis.call reply");
    }
    lua_newtable(lua);
    lua_pushstring(lua,"big_number");
    lua_pushlstring(lua,str,len);
    lua_settable(lua,-3);
}

static void redisProtocolToLuaType_Null(void *ctx, const char *proto, size_t proto_len) {
    UNUSED(proto);
    UNUSED(proto_len);
    if (!ctx) {
        return;
    }

    lua_State *lua = ctx;
    if (!lua_checkstack(lua, 1)) {
        /* Increase the Lua stack if needed, to make sure there is enough room
         * to push elements to the stack. On failure, exit with panic. */
        serverPanic("lua stack limit reach when parsing redis.call reply");
    }
    lua_pushnil(lua);
}

static void redisProtocolToLuaType_Bool(void *ctx, int val, const char *proto, size_t proto_len) {
    UNUSED(proto);
    UNUSED(proto_len);
    if (!ctx) {
        return;
    }

    lua_State *lua = ctx;
    if (!lua_checkstack(lua, 1)) {
        /* Increase the Lua stack if needed, to make sure there is enough room
         * to push elements to the stack. On failure, exit with panic. */
        serverPanic("lua stack limit reach when parsing redis.call reply");
    }
    lua_pushboolean(lua,val);
}

static void redisProtocolToLuaType_Double(void *ctx, double d, const char *proto, size_t proto_len) {
    UNUSED(proto);
    UNUSED(proto_len);
    if (!ctx) {
        return;
    }

    lua_State *lua = ctx;
    if (!lua_checkstack(lua, 3)) {
        /* Increase the Lua stack if needed, to make sure there is enough room
         * to push elements to the stack. On failure, exit with panic. */
        serverPanic("lua stack limit reach when parsing redis.call reply");
    }
    lua_newtable(lua);
    lua_pushstring(lua,"double");
    lua_pushnumber(lua,d);
    lua_settable(lua,-3);
}

/* This function is used in order to push an error on the Lua stack in the
 * format used by redis.pcall to return errors, which is a lua table
 * with an "err" field set to the error string including the error code.
 * Note that this table is never a valid reply by proper commands,
 * since the returned tables are otherwise always indexed by integers, never by strings.
 *
 * The function takes ownership on the given err_buffer. */

/* 此函数用于以 redis 使用的格式在Lua堆栈上推送错误。
 * pcall 返回错误，这是一个 lua 表，其中 “err” 字段设置为包含错误代码的错误字符串。
 * 请注意，这个表从来不是正确命令的有效回复，因为返回的表总是用整数索引，而不是用字符串索引。*/
void luaPushErrorBuff(lua_State *lua, sds err_buffer) {
    sds msg;
    sds error_code;

    /* If debugging is active and in step mode, log errors resulting from
     * Redis commands. */
    if (ldbIsEnabled()) {
        ldbLog(sdscatprintf(sdsempty(),"<error> %s",err_buffer));
    }

    /* There are two possible formats for the received `error` string:
     * 1) "-CODE msg": in this case we remove the leading '-' since we don't store it as part of the lua error format.
     * 2) "msg": in this case we prepend a generic 'ERR' code since all error statuses need some error code.
     * We support format (1) so this function can reuse the error messages used in other places in redis.
     * We support format (2) so it'll be easy to pass descriptive errors to this function without worrying about format.
     */

    /* 收到的“错误”字符串有两种可能的格式：
     * 1. “-CODE msg”：在这种情况下，我们删除前导'-'，因为我们不将其存储为lua错误格式的一部分。
     * 2. “msg”：在这种情况下，由于所有错误状态都需要一些错误代码，所以我们在前面加上一个通用的 “ERR” 代码。
     * 我们支持格式（1），因此此函数可以重用 redis 中其他地方使用的错误消息。
     * 我们支持格式（2），所以很容易将描述性错误传递给这个函数，而不用担心格式。
     */
    if (err_buffer[0] == '-') {
        /* derive error code from the message */
        char *err_msg = strstr(err_buffer, " ");
        if (!err_msg) {
            msg = sdsnew(err_buffer+1);
            error_code = sdsnew("ERR");
        } else {
            *err_msg = '\0';
            msg = sdsnew(err_msg+1);
            error_code = sdsnew(err_buffer + 1);
        }
        sdsfree(err_buffer);
    } else {
        msg = err_buffer;
        error_code = sdsnew("ERR");
    }
    /* Trim newline at end of string. If we reuse the ready-made Redis error objects (case 1 above) then we might
     * have a newline that needs to be trimmed. In any case the lua Redis error table shouldn't end with a newline. */

    /* 修剪字符串末尾的换行符。如果我们重用现成的 Redis 错误对象（上面的例子1），那么我们可能会有一个需要修剪的换行符。无论如何，lua Redis 错误表不应该以换行符结尾 */
    msg = sdstrim(msg, "\r\n");
    sds final_msg = sdscatfmt(error_code, " %s", msg);

    lua_newtable(lua);
    lua_pushstring(lua,"err");
    lua_pushstring(lua, final_msg);
    lua_settable(lua,-3);

    sdsfree(msg);
    sdsfree(final_msg);
}

void luaPushError(lua_State *lua, const char *error) {
    luaPushErrorBuff(lua, sdsnew(error));
}

/* In case the error set into the Lua stack by luaPushError() was generated
 * by the non-error-trapping version of redis.pcall(), which is redis.call(),
 * this function will raise the Lua error so that the execution of the
 * script will be halted. */
int luaError(lua_State *lua) {
    return lua_error(lua);
}


/* ---------------------------------------------------------------------------
 * Lua reply to Redis reply conversion functions.
 * ------------------------------------------------------------------------- */

/* Reply to client 'c' converting the top element in the Lua stack to a
 * Redis reply. As a side effect the element is consumed from the stack.  */

/* 将 Lua 堆栈中的顶部元素转换为 Redis 回复。作为副作用，元素从堆栈中消耗 */
static void luaReplyToRedisReply(client *c, client* script_client, lua_State *lua) {
    int t = lua_type(lua,-1);

    if (!lua_checkstack(lua, 4)) {
        /* Increase the Lua stack if needed to make sure there is enough room
         * to push 4 elements to the stack. On failure, return error.
         * Notice that we need, in the worst case, 4 elements because returning a map might
         * require push 4 elements to the Lua stack.*/
        addReplyErrorFormat(c, "reached lua stack limit");
        lua_pop(lua,1); /* pop the element from the stack */
        return;
    }

    /* 主要针对 string、boolean、number、table 四种类型的特殊处理 */
    switch(t) {
    case LUA_TSTRING:
        addReplyBulkCBuffer(c,(char*)lua_tostring(lua,-1),lua_strlen(lua,-1));
        break;
    case LUA_TBOOLEAN:
        if (script_client->resp == 2)
            addReply(c,lua_toboolean(lua,-1) ? shared.cone :
                                               shared.null[c->resp]);
        else
            addReplyBool(c,lua_toboolean(lua,-1));
        break;
    case LUA_TNUMBER:
        addReplyLongLong(c,(long long)lua_tonumber(lua,-1));
        break;
    case LUA_TTABLE:
        /* We need to check if it is an array, an error, or a status reply.
         * Error are returned as a single element table with 'err' field.
         * Status replies are returned as single element table with 'ok'
         * field. */

        /* 我们需要检查它是数组、错误还是状态回复。
         * 错误作为带有 “err” 字段的单个元素表返回。
         * 状态回复作为带有 “ok” 字段的单元素表返回 */

        /* Handle error reply. */
        /* we took care of the stack size on function start */

        /* 处理错误回复*/
        /* 我们在函数启动时处理了堆栈大小 */
        lua_pushstring(lua,"err");
        lua_rawget(lua,-2);
        t = lua_type(lua,-1);
        if (t == LUA_TSTRING) {
            lua_pop(lua, 1); /* pop the error message, we will use luaExtractErrorInformation to get error information */
            errorInfo err_info = {0};
            luaExtractErrorInformation(lua, &err_info);
            addReplyErrorFormatEx(c,
                                  err_info.ignore_err_stats_update? ERR_REPLY_FLAG_NO_STATS_UPDATE: 0,
                                  "-%s",
                                  err_info.msg);
            luaErrorInformationDiscard(&err_info);
            lua_pop(lua,1); /* pop the result table */
            return;
        }
        lua_pop(lua,1); /* Discard field name pushed before. */

        /* Handle status reply. */
        lua_pushstring(lua,"ok");
        lua_rawget(lua,-2);
        t = lua_type(lua,-1);
        if (t == LUA_TSTRING) {
            sds ok = sdsnew(lua_tostring(lua,-1));
            sdsmapchars(ok,"\r\n","  ",2);
            addReplySds(c,sdscatprintf(sdsempty(),"+%s\r\n",ok));
            sdsfree(ok);
            lua_pop(lua,2);
            return;
        }
        lua_pop(lua,1); /* Discard field name pushed before. */

        /* Handle double reply. */
        lua_pushstring(lua,"double");
        lua_rawget(lua,-2);
        t = lua_type(lua,-1);
        if (t == LUA_TNUMBER) {
            addReplyDouble(c,lua_tonumber(lua,-1));
            lua_pop(lua,2);
            return;
        }
        lua_pop(lua,1); /* Discard field name pushed before. */

        /* Handle big number reply. */
        lua_pushstring(lua,"big_number");
        lua_rawget(lua,-2);
        t = lua_type(lua,-1);
        if (t == LUA_TSTRING) {
            sds big_num = sdsnewlen(lua_tostring(lua,-1), lua_strlen(lua,-1));
            sdsmapchars(big_num,"\r\n","  ",2);
            addReplyBigNum(c,big_num,sdslen(big_num));
            sdsfree(big_num);
            lua_pop(lua,2);
            return;
        }
        lua_pop(lua,1); /* Discard field name pushed before. */

        /* Handle verbatim reply. */
        lua_pushstring(lua,"verbatim_string");
        lua_rawget(lua,-2);
        t = lua_type(lua,-1);
        if (t == LUA_TTABLE) {
            lua_pushstring(lua,"format");
            lua_rawget(lua,-2);
            t = lua_type(lua,-1);
            if (t == LUA_TSTRING){
                char* format = (char*)lua_tostring(lua,-1);
                lua_pushstring(lua,"string");
                lua_rawget(lua,-3);
                t = lua_type(lua,-1);
                if (t == LUA_TSTRING){
                    size_t len;
                    char* str = (char*)lua_tolstring(lua,-1,&len);
                    addReplyVerbatim(c, str, len, format);
                    lua_pop(lua,4);
                    return;
                }
                lua_pop(lua,1);
            }
            lua_pop(lua,1);
        }
        lua_pop(lua,1); /* Discard field name pushed before. */

        /* Handle map reply. */
        /* 如果存在的是一个map字段 */
        lua_pushstring(lua,"map");
        lua_rawget(lua,-2);
        t = lua_type(lua,-1);
        if (t == LUA_TTABLE) {
            int maplen = 0;
            void *replylen = addReplyDeferredLen(c);
            /* we took care of the stack size on function start */
            lua_pushnil(lua); /* Use nil to start iteration. */
            while (lua_next(lua,-2)) {
                /* Stack now: table, key, value */
                lua_pushvalue(lua,-2);        /* Dup key before consuming. */
                /* 递归分析 */
                luaReplyToRedisReply(c, script_client, lua); /* Return key. */
                /* 递归分析 */
                luaReplyToRedisReply(c, script_client, lua); /* Return value. */
                /* Stack now: table, key. */
                maplen++;
            }
            setDeferredMapLen(c,replylen,maplen);
            lua_pop(lua,2);
            return;
        }
        lua_pop(lua,1); /* Discard field name pushed before. */

        /* Handle set reply. */
        /* 如果存在的是一个set字段 */
        lua_pushstring(lua,"set");
        lua_rawget(lua,-2);
        t = lua_type(lua,-1);
        if (t == LUA_TTABLE) {
            int setlen = 0;
            void *replylen = addReplyDeferredLen(c);
            /* we took care of the stack size on function start */
            lua_pushnil(lua); /* Use nil to start iteration. */
            while (lua_next(lua,-2)) {
                /* Stack now: table, key, true */
                lua_pop(lua,1);               /* Discard the boolean value. */
                lua_pushvalue(lua,-1);        /* Dup key before consuming. */
                /* 递归分析 */
                luaReplyToRedisReply(c, script_client, lua); /* Return key. */
                /* Stack now: table, key. */
                setlen++;
            }
            setDeferredSetLen(c,replylen,setlen);
            lua_pop(lua,2);
            return;
        }
        lua_pop(lua,1); /* Discard field name pushed before. */

        /* Handle the array reply. */

        /* 如果存在的是一个 array 字段 */
        void *replylen = addReplyDeferredLen(c);
        int j = 1, mbulklen = 0;
        while(1) {
            /* we took care of the stack size on function start */
            lua_pushnumber(lua,j++);
            lua_rawget(lua,-2);
            t = lua_type(lua,-1);
            /* 未知类型 */
            if (t == LUA_TNIL) {
                lua_pop(lua,1);
                break;
            }
            /* 递归分析 */
            luaReplyToRedisReply(c, script_client, lua);
            mbulklen++;
        }
        setDeferredArrayLen(c,replylen,mbulklen);
        break;
    default:
        addReplyNull(c);
    }
    lua_pop(lua,1);
}

/* ---------------------------------------------------------------------------
 * Lua redis.* functions implementations.
 * ------------------------------------------------------------------------- */

/* 将 lua 参数转换为 redis 参数数组 */
static robj **luaArgsToRedisArgv(lua_State *lua, int *argc) {
    int j;
    /* Require at least one argument */
    *argc = lua_gettop(lua);
    if (*argc == 0) {
        luaPushError(lua, "Please specify at least one argument for this redis lib call");
        return NULL;
    }

    /* Build the arguments vector */
    robj **argv = zcalloc(sizeof(robj*) * *argc);

    for (j = 0; j < *argc; j++) {
        char *obj_s;
        size_t obj_len;
        char dbuf[64];

        /* 根据类型来生成参数信息 */
        if (lua_type(lua,j+1) == LUA_TNUMBER) {
            /* We can't use lua_tolstring() for number -> string conversion
             * since Lua uses a format specifier that loses precision. */
            lua_Number num = lua_tonumber(lua,j+1);

            obj_len = snprintf(dbuf,sizeof(dbuf),"%.17g",(double)num);
            obj_s = dbuf;
        } else {
            obj_s = (char*)lua_tolstring(lua,j+1,&obj_len);
            if (obj_s == NULL) break; /* Not a string. */
        }

        argv[j] = createStringObject(obj_s, obj_len);
    }

    /* Pop all arguments from the stack, we do not need them anymore
     * and this way we guaranty we will have room on the stack for the result. */
    lua_pop(lua, *argc);

    /* Check if one of the arguments passed by the Lua script
     * is not a string or an integer (lua_isstring() return true for
     * integers as well). */

    /* 检查 Lua 脚本传递的参数之一是否不是字符串或整数 （ Lua_isstring 对于整数也返回 true ）*/
    if (j != *argc) {
        j--;
        while (j >= 0) {
            decrRefCount(argv[j]);
            j--;
        }
        zfree(argv);
        luaPushError(lua, "Lua redis lib command arguments must be strings or integers");
        return NULL;
    }

    return argv;
}

/* lua 调用 redis 方法, raise_error 表示是否会引发错误导致程序暂停 */
static int luaRedisGenericCommand(lua_State *lua, int raise_error) {
    int j;
    /* 获取上下文信息 */
    scriptRunCtx* rctx = luaGetFromRegistry(lua, REGISTRY_RUN_CTX_NAME);
    if (!rctx) {
        luaPushError(lua, "redis.call/pcall can only be called inside a script invocation");
        return luaError(lua);
    }
    sds err = NULL;
    client* c = rctx->c;
    sds reply;

    /* 拿到 redis 能用的参数信息 */
    int argc;
    robj **argv = luaArgsToRedisArgv(lua, &argc);
    if (argv == NULL) {
        return raise_error ? luaError(lua) : 1;
    }

    static int inuse = 0;   /* Recursive calls detection. */

    /* By using Lua debug hooks it is possible to trigger a recursive call
     * to luaRedisGenericCommand(), which normally should never happen.
     * To make this function reentrant is futile and makes it slower, but
     * we should at least detect such a misuse, and abort. */

    /* 通过使用 Lua 调试挂钩，可以触发对 luaRedisGenericCommand（） 的递归调用，这通常不应该发生。
     * 使此函数可重入是徒劳的，而且会使它变慢，但我们至少应该检测到这种误用，并终止*/
    if (inuse) {
        char *recursion_warning =
                "luaRedisGenericCommand() recursive call detected. "
                "Are you doing funny stuff with Lua debug hooks?";
        serverLog(LL_WARNING,"%s",recursion_warning);
        luaPushError(lua,recursion_warning);
        return 1;
    }
    inuse++;

    /* Log the command if debugging is active. */
    /* 如果调试处于活动状态，则记录命令 */
    if (ldbIsEnabled()) {
        sds cmdlog = sdsnew("<redis>");
        for (j = 0; j < c->argc; j++) {
            if (j == 10) {
                cmdlog = sdscatprintf(cmdlog," ... (%d more)",
                    c->argc-j-1);
                break;
            } else {
                cmdlog = sdscatlen(cmdlog," ",1);
                cmdlog = sdscatsds(cmdlog,c->argv[j]->ptr);
            }
        }
        ldbLog(cmdlog);
    }

    scriptCall(rctx, argv, argc, &err);
    if (err) {
        luaPushError(lua, err);
        sdsfree(err);
        /* push a field indicate to ignore updating the stats on this error
         * because it was already updated when executing the command. */

        /* push 字段表示忽略更新此错误的统计信息，因为它在执行命令时已更新 */
        lua_pushstring(lua,"ignore_error_stats_update");
        lua_pushboolean(lua, 1);
        lua_settable(lua,-3);
        goto cleanup;
    }

    /* Convert the result of the Redis command into a suitable Lua type.
     * The first thing we need is to create a single string from the client
     * output buffers. */

    /* 将 Redis 命令的结果转换为合适的Lua类型。
     * 我们需要的第一件事是从客户端输出缓冲区创建一个字符串*/
    if (listLength(c->reply) == 0 && (size_t)c->bufpos < c->buf_usable_size) {
        /* This is a fast path for the common case of a reply inside the
         * client static buffer. Don't create an SDS string but just use
         * the client buffer directly. */

        /* 对于客户端静态缓冲区内的常见回复情况，这是一条快速路径。不要创建 SDS 字符串，而是直接使用客户端缓冲区 */
        c->buf[c->bufpos] = '\0';
        reply = c->buf;
        c->bufpos = 0;
    } else {
        reply = sdsnewlen(c->buf,c->bufpos);
        c->bufpos = 0;
        while(listLength(c->reply)) {
            clientReplyBlock *o = listNodeValue(listFirst(c->reply));

            reply = sdscatlen(reply,o->buf,o->used);
            listDelNode(c->reply,listFirst(c->reply));
        }
    }
    /* 如果需要错误，但返回值不为错误，则修改错误状态 */
    if (raise_error && reply[0] != '-') raise_error = 0;
    redisProtocolToLuaType(lua,reply);

    /* If the debugger is active, log the reply from Redis. */
    if (ldbIsEnabled())
        ldbLogRedisReply(reply);

    if (reply != c->buf) sdsfree(reply);
    c->reply_bytes = 0;

cleanup:
    /* Clean up. Command code may have changed argv/argc so we use the
     * argv/argc of the client instead of the local variables. */

    /* 清理干净。命令代码可能已更改 argv/argc ，因此我们使用客户端的 argv/argc 而不是本地变量*/
    freeClientArgv(c);
    c->user = NULL;
    inuse--;

    if (raise_error) {
        /* If we are here we should have an error in the stack, in the
         * form of a table with an "err" field. Extract the string to
         * return the plain error. */

        /* 如果我们在这里，堆栈中应该有一个错误，以带有 “err” 字段的表的形式。提取字符串以返回纯错误 */
        return luaError(lua);
    }
    return 1;
}

/* Our implementation to lua pcall.
 * We need this implementation for backward
 * comparability with older Redis versions.
 *
 * On Redis 7, the error object is a table,
 * compare to older version where the error
 * object is a string. To keep backward
 * comparability we catch the table object
 * and just return the error message. */

/* 我们对 lua-pcall 的实现。
 * 我们需要这个实现来向后比较旧的 Redis 版本。
 *
 * 在Redis 7上，错误对象是一个表，与旧版本的错误对象是字符串相比。为了保持向后可比性，我们捕获表对象并返回错误消息 */
static int luaRedisPcall(lua_State *lua) {
    int argc = lua_gettop(lua);
    lua_pushboolean(lua, 1); /* result place holder */
    lua_insert(lua, 1);
    if (lua_pcall(lua, argc - 1, LUA_MULTRET, 0)) {
        /* Error */
        lua_remove(lua, 1); /* remove the result place holder, now we have room for at least one element */
        if (lua_istable(lua, -1)) {
            lua_getfield(lua, -1, "err");
            if (lua_isstring(lua, -1)) {
                lua_replace(lua, -2); /* replace the error message with the table */
            }
        }
        lua_pushboolean(lua, 0); /* push result */
        lua_insert(lua, 1);
    }
    return lua_gettop(lua);

}

/* redis.call() */
static int luaRedisCallCommand(lua_State *lua) {
    return luaRedisGenericCommand(lua,1);
}

/* redis.pcall() */
static int luaRedisPCallCommand(lua_State *lua) {
    return luaRedisGenericCommand(lua,0);
}

/* This adds redis.sha1hex(string) to Lua scripts using the same hashing
 * function used for sha1ing lua scripts. */

/* 这增加了 redis.sha1hex(string) 转换为 Lua 脚本，使用与转换 Lua 脚本相同的哈希函数 */
static int luaRedisSha1hexCommand(lua_State *lua) {
    int argc = lua_gettop(lua);
    char digest[41];
    size_t len;
    char *s;

    if (argc != 1) {
        luaPushError(lua, "wrong number of arguments");
        return luaError(lua);
    }

    s = (char*)lua_tolstring(lua,1,&len);
    sha1hex(digest,s,len);
    lua_pushstring(lua,digest);
    return 1;
}

/* Returns a table with a single field 'field' set to the string value
 * passed as argument. This helper function is handy when returning
 * a Redis Protocol error or status reply from Lua:
 *
 * return redis.error_reply("ERR Some Error")
 * return redis.status_reply("ERR Some Error")
 */

/* 返回一个表，其中单个字段 “field” 设置为作为参数传递的字符串值。
 * 当从 Lua 返回 Redis Protocol 错误或状态回复时会非常方便 */
static int luaRedisReturnSingleFieldTable(lua_State *lua, char *field) {
    if (lua_gettop(lua) != 1 || lua_type(lua,-1) != LUA_TSTRING) {
        luaPushError(lua, "wrong number or type of arguments");
        return 1;
    }

    lua_newtable(lua);
    lua_pushstring(lua, field);
    lua_pushvalue(lua, -3);
    lua_settable(lua, -3);
    return 1;
}

/* redis.error_reply() */
static int luaRedisErrorReplyCommand(lua_State *lua) {
    if (lua_gettop(lua) != 1 || lua_type(lua,-1) != LUA_TSTRING) {
        luaPushError(lua, "wrong number or type of arguments");
        return 1;
    }

    /* add '-' if not exists */
    const char *err = lua_tostring(lua, -1);
    sds err_buff = NULL;
    if (err[0] != '-') {
        err_buff = sdscatfmt(sdsempty(), "-%s", err);
    } else {
        err_buff = sdsnew(err);
    }
    luaPushErrorBuff(lua, err_buff);
    return 1;
}

/* redis.status_reply() */
static int luaRedisStatusReplyCommand(lua_State *lua) {
    return luaRedisReturnSingleFieldTable(lua,"ok");
}

/* redis.set_repl()
 *
 * Set the propagation of write commands executed in the context of the
 * script to on/off for AOF and slaves. */

/* 将在脚本上下文中执行的写命令的传播设置为 AOF 和 slave 的开/关 */
static int luaRedisSetReplCommand(lua_State *lua) {
    int flags, argc = lua_gettop(lua);

    scriptRunCtx* rctx = luaGetFromRegistry(lua, REGISTRY_RUN_CTX_NAME);
    if (!rctx) {
        luaPushError(lua, "redis.set_repl can only be called inside a script invocation");
        return luaError(lua);
    }

    if (argc != 1) {
        luaPushError(lua, "redis.set_repl() requires two arguments.");
         return luaError(lua);
    }

    flags = lua_tonumber(lua,-1);
    if ((flags & ~(PROPAGATE_AOF|PROPAGATE_REPL)) != 0) {
        luaPushError(lua, "Invalid replication flags. Use REPL_AOF, REPL_REPLICA, REPL_ALL or REPL_NONE.");
        return luaError(lua);
    }

    scriptSetRepl(rctx, flags);
    return 0;
}

/* redis.acl_check_cmd()
 *
 * Checks ACL permissions for given command for the current user. */
static int luaRedisAclCheckCmdPermissionsCommand(lua_State *lua) {
    scriptRunCtx* rctx = luaGetFromRegistry(lua, REGISTRY_RUN_CTX_NAME);
    if (!rctx) {
        luaPushError(lua, "redis.acl_check_cmd can only be called inside a script invocation");
        return luaError(lua);
    }
    int raise_error = 0;

    int argc;
    robj **argv = luaArgsToRedisArgv(lua, &argc);

    /* Require at least one argument */
    if (argv == NULL) return luaError(lua);

    /* Find command */
    struct redisCommand *cmd;
    if ((cmd = lookupCommand(argv, argc)) == NULL) {
        luaPushError(lua, "Invalid command passed to redis.acl_check_cmd()");
        raise_error = 1;
    } else {
        int keyidxptr;
        if (ACLCheckAllUserCommandPerm(rctx->original_client->user, cmd, argv, argc, &keyidxptr) != ACL_OK) {
            lua_pushboolean(lua, 0);
        } else {
            lua_pushboolean(lua, 1);
        }
    }

    while (argc--) decrRefCount(argv[argc]);
    zfree(argv);
    if (raise_error)
        return luaError(lua);
    else
        return 1;
}


/* redis.log() */
static int luaLogCommand(lua_State *lua) {
    int j, argc = lua_gettop(lua);
    int level;
    sds log;

    if (argc < 2) {
        luaPushError(lua, "redis.log() requires two arguments or more.");
        return luaError(lua);
    } else if (!lua_isnumber(lua,-argc)) {
        luaPushError(lua, "First argument must be a number (log level).");
        return luaError(lua);
    }
    level = lua_tonumber(lua,-argc);
    if (level < LL_DEBUG || level > LL_WARNING) {
        luaPushError(lua, "Invalid debug level.");
        return luaError(lua);
    }
    if (level < server.verbosity) return 0;

    /* Glue together all the arguments */
    log = sdsempty();
    for (j = 1; j < argc; j++) {
        size_t len;
        char *s;

        s = (char*)lua_tolstring(lua,(-argc)+j,&len);
        if (s) {
            if (j != 1) log = sdscatlen(log," ",1);
            log = sdscatlen(log,s,len);
        }
    }
    serverLogRaw(level,log);
    sdsfree(log);
    return 0;
}

/* redis.setresp() */

/* 设置当前 RESP 的协议版本 */
static int luaSetResp(lua_State *lua) {
    scriptRunCtx* rctx = luaGetFromRegistry(lua, REGISTRY_RUN_CTX_NAME);
    if (!rctx) {
        luaPushError(lua, "redis.setresp can only be called inside a script invocation");
        return luaError(lua);
    }
    int argc = lua_gettop(lua);

    if (argc != 1) {
        luaPushError(lua, "redis.setresp() requires one argument.");
        return luaError(lua);
    }

    int resp = lua_tonumber(lua,-argc);
    if (resp != 2 && resp != 3) {
        luaPushError(lua, "RESP version must be 2 or 3.");
        return luaError(lua);
    }
    scriptSetResp(rctx, resp);
    return 0;
}

/* ---------------------------------------------------------------------------
 * Lua engine initialization and reset.
 * ------------------------------------------------------------------------- */

/* 加载 lib，通过调用对于的方法实现 */
static void luaLoadLib(lua_State *lua, const char *libname, lua_CFunction luafunc) {
  lua_pushcfunction(lua, luafunc);
  lua_pushstring(lua, libname);
  lua_call(lua, 1, 0);
}

LUALIB_API int (luaopen_cjson) (lua_State *L);
LUALIB_API int (luaopen_struct) (lua_State *L);
LUALIB_API int (luaopen_cmsgpack) (lua_State *L);
LUALIB_API int (luaopen_bit) (lua_State *L);

static void luaLoadLibraries(lua_State *lua) {
    luaLoadLib(lua, "", luaopen_base);
    luaLoadLib(lua, LUA_TABLIBNAME, luaopen_table);
    luaLoadLib(lua, LUA_STRLIBNAME, luaopen_string);
    luaLoadLib(lua, LUA_MATHLIBNAME, luaopen_math);
    luaLoadLib(lua, LUA_DBLIBNAME, luaopen_debug);
    luaLoadLib(lua, "cjson", luaopen_cjson);
    luaLoadLib(lua, "struct", luaopen_struct);
    luaLoadLib(lua, "cmsgpack", luaopen_cmsgpack);
    luaLoadLib(lua, "bit", luaopen_bit);

#if 0 /* Stuff that we don't load currently, for sandboxing concerns. */
    luaLoadLib(lua, LUA_LOADLIBNAME, luaopen_package);
    luaLoadLib(lua, LUA_OSLIBNAME, luaopen_os);
#endif
}

/* Return sds of the string value located on stack at the given index.
 * Return NULL if the value is not a string. */
sds luaGetStringSds(lua_State *lua, int index) {
    if (!lua_isstring(lua, index)) {
        return NULL;
    }

    size_t len;
    const char *str = lua_tolstring(lua, index, &len);
    sds str_sds = sdsnewlen(str, len);
    return str_sds;
}

static int luaProtectedTableError(lua_State *lua) {
    int argc = lua_gettop(lua);
    if (argc != 2) {
        serverLog(LL_WARNING, "malicious code trying to call luaProtectedTableError with wrong arguments");
        luaL_error(lua, "Wrong number of arguments to luaProtectedTableError");
    }
    if (!lua_isstring(lua, -1) && !lua_isnumber(lua, -1)) {
        luaL_error(lua, "Second argument to luaProtectedTableError must be a string or number");
    }
    const char *variable_name = lua_tostring(lua, -1);
    luaL_error(lua, "Script attempted to access nonexistent global variable '%s'", variable_name);
    return 0;
}

/* Set a special metatable on the table on the top of the stack.
 * The metatable will raise an error if the user tries to fetch
 * an un-existing value.
 *
 * The function assumes the Lua stack have a least enough
 * space to push 2 element, its up to the caller to verify
 * this before calling this function. */

/* 在堆栈顶部的表上设置一个特殊的元表。
 * 如果用户试图获取不存在的值，元表将引发错误。
 *
 * 该函数假设 Lua 堆栈至少有足够的空间来推送2元素，这取决于调用方在调用该函数之前验证这一点 */
void luaSetErrorMetatable(lua_State *lua) {
    lua_newtable(lua); /* push metatable */
    lua_pushcfunction(lua, luaProtectedTableError); /* push get error handler */
    lua_setfield(lua, -2, "__index");
    lua_setmetatable(lua, -2);
}

/* 确定新增数据是否被允许 */
static int luaNewIndexAllowList(lua_State *lua) {
    int argc = lua_gettop(lua);
    /* 回调方法必须有3个数据：table、varname、value */
    if (argc != 3) {
        serverLog(LL_WARNING, "malicious code trying to call luaProtectedTableError with wrong arguments");
        luaL_error(lua, "Wrong number of arguments to luaNewIndexAllowList");
    }
    if (!lua_istable(lua, -3)) {
        luaL_error(lua, "first argument to luaNewIndexAllowList must be a table");
    }
    if (!lua_isstring(lua, -2) && !lua_isnumber(lua, -2)) {
        luaL_error(lua, "Second argument to luaNewIndexAllowList must be a string or number");
    }
    const char *variable_name = lua_tostring(lua, -2);
    /* check if the key is in our allow list */

    char ***allow_l = allow_lists;
    for (; *allow_l ; ++allow_l){
        char **c = *allow_l;
        for (; *c ; ++c) {
            if (strcmp(*c, variable_name) == 0) {
                break;
            }
        }
        if (*c) {
            break;
        }
    }

    /* allow_l 为 NULL 时，说明当前不存在满足条件的数据 */
    if (!*allow_l) {
        /* Search the value on the back list, if its there we know that it was removed
         * on purpose and there is no need to print a warning. */
        char **c = deny_list;
        for ( ; *c ; ++c) {
            if (strcmp(*c, variable_name) == 0) {
                break;
            }
        }
        /* 如果存在在 deny_list 中，则打印日志，并告知是专门被禁止的字段 */
        if (!*c) {
            serverLog(LL_WARNING, "A key '%s' was added to Lua globals which is not on the globals allow list nor listed on the deny list.", variable_name);
        }
    } else {
        /* 如果在满足条件中的字段，则绕过 __new_index 直接赋值 */
        lua_rawset(lua, -3);
    }
    return 0;
}

/* Set a metatable with '__newindex' function that verify that
 * the new index appears on our globals while list.
 *
 * The metatable is set on the table which located on the top
 * of the stack.
 */

/* __nexindex 字段是在对表中不存在的值进行赋值（set）时候执行的操作
 * 以验证新索引是否出现在全局 while 列表中 */
void luaSetAllowListProtection(lua_State *lua) {
    lua_newtable(lua); /* push metatable */
    lua_pushcfunction(lua, luaNewIndexAllowList); /* push get error handler */
    lua_setfield(lua, -2, "__newindex");
    lua_setmetatable(lua, -2);
}

/* Set the readonly flag on the table located on the top of the stack
 * and recursively call this function on each table located on the original
 * table.  Also, recursively call this function on the metatables.*/

/* 在位于堆栈顶部的表上设置只读标志，并对位于原始表上的每个表递归调用此函数。此外，在元表上递归调用此函数 */
void luaSetTableProtectionRecursively(lua_State *lua) {
    /* This protect us from a loop in case we already visited the table
     * For example, globals has '_G' key which is pointing back to globals. */

    /* 这样可以保护我们不受循环的影响，以防我们已经访问了表
     * 例如，全局变量具有“_G”键，该键指向全局变量*/
    if (lua_isreadonlytable(lua, -1)) {
        return;
    }

    /* protect the current table */
    lua_enablereadonlytable(lua, -1, 1);

    lua_checkstack(lua, 2);
    lua_pushnil(lua); /* Use nil to start iteration. */
    while (lua_next(lua,-2)) {
        /* Stack now: table, key, value */
        if (lua_istable(lua, -1)) {
            luaSetTableProtectionRecursively(lua);
        }
        lua_pop(lua, 1);
    }

    /* protect the metatable if exists */
    if (lua_getmetatable(lua, -1)) {
        luaSetTableProtectionRecursively(lua);
        lua_pop(lua, 1); /* pop the metatable */
    }
}

void luaRegisterVersion(lua_State* lua) {
    lua_pushstring(lua,"REDIS_VERSION_NUM");
    lua_pushnumber(lua,REDIS_VERSION_NUM);
    lua_settable(lua,-3);

    lua_pushstring(lua,"REDIS_VERSION");
    lua_pushstring(lua,REDIS_VERSION);
    lua_settable(lua,-3);
}

void luaRegisterLogFunction(lua_State* lua) {
    /* redis.log and log levels. */
    lua_pushstring(lua,"log");
    lua_pushcfunction(lua,luaLogCommand);
    lua_settable(lua,-3);

    lua_pushstring(lua,"LOG_DEBUG");
    lua_pushnumber(lua,LL_DEBUG);
    lua_settable(lua,-3);

    lua_pushstring(lua,"LOG_VERBOSE");
    lua_pushnumber(lua,LL_VERBOSE);
    lua_settable(lua,-3);

    lua_pushstring(lua,"LOG_NOTICE");
    lua_pushnumber(lua,LL_NOTICE);
    lua_settable(lua,-3);

    lua_pushstring(lua,"LOG_WARNING");
    lua_pushnumber(lua,LL_WARNING);
    lua_settable(lua,-3);
}

/* 注册相关的 RedisAPI */
void luaRegisterRedisAPI(lua_State* lua) {
    /* 设置全局变量的增加限制 */
    lua_pushvalue(lua, LUA_GLOBALSINDEX);
    luaSetAllowListProtection(lua);
    lua_pop(lua, 1);

    /* 加载 lib 库 */
    luaLoadLibraries(lua);

    /* 将pcall绑定至luaRedisPcall方法上 */
    lua_pushcfunction(lua,luaRedisPcall);
    lua_setglobal(lua, "pcall");

    /* Register the redis commands table and fields */
    lua_newtable(lua);
    /* 
     * 参考官方：https://redis.io/docs/manual/programmability/lua-api/，已支持的方法如下：
     * redis.call(command [,arg...])
     * redis.pcall(command [,arg...])
     * redis.error_reply(x)
     * redis.status_reply(x)
     * redis.sha1hex(x)
     * redis.log(level, message)
     * redis.setresp(x)
     * redis.set_repl(x)
     * redis.replicate_commands()
     * redis.breakpoint()
     * redis.debug(x)
     * redis.acl_check_cmd(command [,arg...])
     * redis.register_function
     * redis.REDIS_VERSION
     * redis.REDIS_VERSION_NUM
     */

    /* redis.call */
    lua_pushstring(lua,"call");
    lua_pushcfunction(lua,luaRedisCallCommand);
    /* 赋值call->luaRedisCallCommand */
    lua_settable(lua,-3);

    /* redis.pcall */
    lua_pushstring(lua,"pcall");
    lua_pushcfunction(lua,luaRedisPCallCommand);
    /* 赋值 pcall->luaRedisPCallCommand */
    lua_settable(lua,-3);

    /* 注册日志方法 */
    luaRegisterLogFunction(lua);

    /* 注册版本信息 */
    luaRegisterVersion(lua);

    /* redis.setresp */
    lua_pushstring(lua,"setresp");
    lua_pushcfunction(lua,luaSetResp);
    lua_settable(lua,-3);

    /* redis.sha1hex */
    lua_pushstring(lua, "sha1hex");
    lua_pushcfunction(lua, luaRedisSha1hexCommand);
    lua_settable(lua, -3);

    /* redis.error_reply and redis.status_reply */
    lua_pushstring(lua, "error_reply");
    lua_pushcfunction(lua, luaRedisErrorReplyCommand);
    lua_settable(lua, -3);
    lua_pushstring(lua, "status_reply");
    lua_pushcfunction(lua, luaRedisStatusReplyCommand);
    lua_settable(lua, -3);

    /* redis.set_repl and associated flags. */
    lua_pushstring(lua,"set_repl");
    lua_pushcfunction(lua,luaRedisSetReplCommand);
    lua_settable(lua,-3);

    lua_pushstring(lua,"REPL_NONE");
    lua_pushnumber(lua,PROPAGATE_NONE);
    lua_settable(lua,-3);

    lua_pushstring(lua,"REPL_AOF");
    lua_pushnumber(lua,PROPAGATE_AOF);
    lua_settable(lua,-3);

    lua_pushstring(lua,"REPL_SLAVE");
    lua_pushnumber(lua,PROPAGATE_REPL);
    lua_settable(lua,-3);

    lua_pushstring(lua,"REPL_REPLICA");
    lua_pushnumber(lua,PROPAGATE_REPL);
    lua_settable(lua,-3);

    lua_pushstring(lua,"REPL_ALL");
    lua_pushnumber(lua,PROPAGATE_AOF|PROPAGATE_REPL);
    lua_settable(lua,-3);

    /* redis.acl_check_cmd */
    lua_pushstring(lua,"acl_check_cmd");
    lua_pushcfunction(lua,luaRedisAclCheckCmdPermissionsCommand);
    lua_settable(lua,-3);

    /* Finally set the table as 'redis' global var. */

    /* 将整个 table 设置给 "redis"，用于后续的 redis.xxx 的调用 */
    lua_setglobal(lua,REDIS_API_NAME);

    /* Replace math.random and math.randomseed with our implementations. */

    /* 在注册math全局变量 */
    lua_getglobal(lua,"math");

    lua_pushstring(lua,"random");
    lua_pushcfunction(lua,redis_math_random);
    lua_settable(lua,-3);

    lua_pushstring(lua,"randomseed");
    lua_pushcfunction(lua,redis_math_randomseed);
    lua_settable(lua,-3);

    lua_setglobal(lua,"math");
}

/* Set an array of Redis String Objects as a Lua array (table) stored into a
 * global variable. */
static void luaCreateArray(lua_State *lua, robj **elev, int elec) {
    int j;

    lua_newtable(lua);
    for (j = 0; j < elec; j++) {
        lua_pushlstring(lua,(char*)elev[j]->ptr,sdslen(elev[j]->ptr));
        lua_rawseti(lua,-2,j+1);
    }
}

/* ---------------------------------------------------------------------------
 * Redis provided math.random
 * ------------------------------------------------------------------------- */

/* We replace math.random() with our implementation that is not affected
 * by specific libc random() implementations and will output the same sequence
 * (for the same seed) in every arch. */

/* The following implementation is the one shipped with Lua itself but with
 * rand() replaced by redisLrand48(). */
static int redis_math_random (lua_State *L) {
  scriptRunCtx* rctx = luaGetFromRegistry(L, REGISTRY_RUN_CTX_NAME);
  if (!rctx) {
    return luaL_error(L, "math.random can only be called inside a script invocation");
  }

  /* the `%' avoids the (rare) case of r==1, and is needed also because on
     some systems (SunOS!) `rand()' may return a value larger than RAND_MAX */
  lua_Number r = (lua_Number)(redisLrand48()%REDIS_LRAND48_MAX) /
                                (lua_Number)REDIS_LRAND48_MAX;
  switch (lua_gettop(L)) {  /* check number of arguments */
    case 0: {  /* no arguments */
      lua_pushnumber(L, r);  /* Number between 0 and 1 */
      break;
    }
    case 1: {  /* only upper limit */
      int u = luaL_checkint(L, 1);
      luaL_argcheck(L, 1<=u, 1, "interval is empty");
      lua_pushnumber(L, floor(r*u)+1);  /* int between 1 and `u' */
      break;
    }
    case 2: {  /* lower and upper limits */
      int l = luaL_checkint(L, 1);
      int u = luaL_checkint(L, 2);
      luaL_argcheck(L, l<=u, 2, "interval is empty");
      lua_pushnumber(L, floor(r*(u-l+1))+l);  /* int between `l' and `u' */
      break;
    }
    default: return luaL_error(L, "wrong number of arguments");
  }
  return 1;
}

static int redis_math_randomseed (lua_State *L) {
  scriptRunCtx* rctx = luaGetFromRegistry(L, REGISTRY_RUN_CTX_NAME);
  if (!rctx) {
    return luaL_error(L, "math.randomseed can only be called inside a script invocation");
  }
  redisSrand48(luaL_checkint(L, 1));
  return 0;
}

/* This is the Lua script "count" hook that we use to detect scripts timeout. */

/* 这是我们用来检测脚本超时的 Lua 脚本 “count” 挂钩 */
static void luaMaskCountHook(lua_State *lua, lua_Debug *ar) {
    UNUSED(ar);
    scriptRunCtx* rctx = luaGetFromRegistry(lua, REGISTRY_RUN_CTX_NAME);
    /* 如果当前脚本已经终止了，则返回错误 */
    if (scriptInterrupt(rctx) == SCRIPT_KILL) {
        serverLog(LL_WARNING,"Lua script killed by user with SCRIPT KILL.");

        /*
         * Set the hook to invoke all the time so the user
         * will not be able to catch the error with pcall and invoke
         * pcall again which will prevent the script from ever been killed
         */

        /* 将钩子设置为始终调用，这样用户将无法用 pcall 捕获错误并再次调用 pcall，这将防止脚本被终止 */
        lua_sethook(lua, luaMaskCountHook, LUA_MASKLINE, 0);

        luaPushError(lua,"Script killed by user with SCRIPT KILL...");
        luaError(lua);
    }
}

/* 释放错误信息的空间 */
void luaErrorInformationDiscard(errorInfo *err_info) {
    if (err_info->msg) sdsfree(err_info->msg);
    if (err_info->source) sdsfree(err_info->source);
    if (err_info->line) sdsfree(err_info->line);
}

/* 解析当前堆栈中的错误信息 */
void luaExtractErrorInformation(lua_State *lua, errorInfo *err_info) {
    if (lua_isstring(lua, -1)) {
        err_info->msg = sdscatfmt(sdsempty(), "ERR %s", lua_tostring(lua, -1));
        err_info->line = NULL;
        err_info->source = NULL;
        err_info->ignore_err_stats_update = 0;
    }

    lua_getfield(lua, -1, "err");
    if (lua_isstring(lua, -1)) {
        err_info->msg = sdsnew(lua_tostring(lua, -1));
    }
    lua_pop(lua, 1);

    lua_getfield(lua, -1, "source");
    if (lua_isstring(lua, -1)) {
        err_info->source = sdsnew(lua_tostring(lua, -1));
    }
    lua_pop(lua, 1);

    lua_getfield(lua, -1, "line");
    if (lua_isstring(lua, -1)) {
        err_info->line = sdsnew(lua_tostring(lua, -1));
    }
    lua_pop(lua, 1);

    lua_getfield(lua, -1, "ignore_error_stats_update");
    if (lua_isboolean(lua, -1)) {
        err_info->ignore_err_stats_update = lua_toboolean(lua, -1);
    }
    lua_pop(lua, 1);
}

/* 允许lua脚本方法 */
void luaCallFunction(scriptRunCtx* run_ctx, lua_State *lua, robj** keys, size_t nkeys, robj** args, size_t nargs, int debug_enabled) {
    client* c = run_ctx->original_client;
    int delhook = 0;

    /* We must set it before we set the Lua hook, theoretically the
     * Lua hook might be called wheneven we run any Lua instruction
     * such as 'luaSetGlobalArray' and we want the run_ctx to be available
     * each time the Lua hook is invoked. */

    /* 我们必须在设置 Lua 钩子之前设置它，理论上，当我们运行任何 Lua 指令（如 “luaSetGlobalArray” ）时，
     * 都可能会调用 Lua 钩子，并且我们希望每次调用Lua挂钩时，run_ctx 都可用 */
    luaSaveOnRegistry(lua, REGISTRY_RUN_CTX_NAME, run_ctx);

    /* 当前的 hook 信息
     * LUA_MASKCALL：调用一个函数时，就调用一次钩子函数。
     * LUA_MASKRET：从一个函数中返回时，就调用一次钩子函数。
     * LUA_MASKLINE：执行一行指令时，就回调一次钩子函数。
     * LUA_MASKCOUNT：执行指定数量的指令时，就回调一次钩子函数。 */
    if (server.busy_reply_threshold > 0 && !debug_enabled) {
        /* 如果存在忙碌限制，则增加 hook 方法 */
        lua_sethook(lua,luaMaskCountHook,LUA_MASKCOUNT,100000);
        delhook = 1;
    } else if (debug_enabled) {
        /* 如果存在 debug 限制，则增加 hook 方法 */
        lua_sethook(lua,luaLdbLineHook,LUA_MASKLINE|LUA_MASKCOUNT,100000);
        delhook = 1;
    }

    /* Populate the argv and keys table accordingly to the arguments that
     * EVAL received. */
    luaCreateArray(lua,keys,nkeys);
    /* On eval, keys and arguments are globals. */

    /* 如果是 eval 命令，则会把 key 和 arg 都做为全局变量，若是 function，则不需要设置 */
    if (run_ctx->flags & SCRIPT_EVAL_MODE){
        /* open global protection to set KEYS */
        lua_enablereadonlytable(lua, LUA_GLOBALSINDEX, 0);
        lua_setglobal(lua,"KEYS");
        lua_enablereadonlytable(lua, LUA_GLOBALSINDEX, 1);
    }
    luaCreateArray(lua,args,nargs);
    if (run_ctx->flags & SCRIPT_EVAL_MODE){
        /* open global protection to set ARGV */
        lua_enablereadonlytable(lua, LUA_GLOBALSINDEX, 0);
        lua_setglobal(lua,"ARGV");
        lua_enablereadonlytable(lua, LUA_GLOBALSINDEX, 1);
    }

    /* At this point whether this script was never seen before or if it was
     * already defined, we can call it.
     * On eval mode, we have zero arguments and expect a single return value.
     * In addition the error handler is located on position -2 on the Lua stack.
     * On function mode, we pass 2 arguments (the keys and args tables),
     * and the error handler is located on position -4 (stack: error_handler, callback, keys, args) */

    /* 在这一点上，无论这个脚本以前是否从未见过，或者它是否已经定义，我们都可以调用它。
     * 在 eval 模式下，我们有零个参数，并且期望一个返回值。 此外，错误处理程序位于 Lua 堆栈的位置-2。
     * 在函数模式下，我们传递2个参数（键和参数表），错误处理程序位于位置-4（堆栈：error_handler，callback，keys，args）*/
    int err;
    /* 参考： int lua_pcall (lua_State *L, int nargs, int nresults, int errfunc) */
    if (run_ctx->flags & SCRIPT_EVAL_MODE) {
        err = lua_pcall(lua,0,1,-2);
    } else {
        err = lua_pcall(lua,2,1,-4);
    }

    /* Call the Lua garbage collector from time to time to avoid a
     * full cycle performed by Lua, which adds too latency.
     *
     * The call is performed every LUA_GC_CYCLE_PERIOD executed commands
     * (and for LUA_GC_CYCLE_PERIOD collection steps) because calling it
     * for every command uses too much CPU. */

    /* 触发整个的 lua 的 gc 能力 */
    #define LUA_GC_CYCLE_PERIOD 50
    {
        static long gc_count = 0;

        gc_count++;
        if (gc_count == LUA_GC_CYCLE_PERIOD) {
            lua_gc(lua,LUA_GCSTEP,LUA_GC_CYCLE_PERIOD);
            gc_count = 0;
        }
    }

    /* 如果出现了错误 */
    if (err) {
        /* Error object is a table of the following format:
         * {err='<error msg>', source='<source file>', line=<line>}
         * We can construct the error message from this information */

        /* 错误对象是以下格式的表：｛err＝“＜Error msg＞”，source＝“<source file＞”，line＝＜line＞｝
         * 我们可以根据此信息构造错误消息, 如果没有实际的错误信息表，则构造一个 Error running script 的错误 */
        if (!lua_istable(lua, -1)) {
            const char *msg = "execution failure";
            if (lua_isstring(lua, -1)) {
                msg = lua_tostring(lua, -1);
            }
            addReplyErrorFormat(c,"Error running script %s, %.100s\n", run_ctx->funcname, msg);
        } else {
            errorInfo err_info = {0};
            sds final_msg = sdsempty();
            luaExtractErrorInformation(lua, &err_info);
            final_msg = sdscatfmt(final_msg, "-%s",
                                  err_info.msg);
            if (err_info.line && err_info.source) {
                final_msg = sdscatfmt(final_msg, " script: %s, on %s:%s.",
                                      run_ctx->funcname,
                                      err_info.source,
                                      err_info.line);
            }
            addReplyErrorSdsEx(c, final_msg, err_info.ignore_err_stats_update? ERR_REPLY_FLAG_NO_STATS_UPDATE : 0);
            luaErrorInformationDiscard(&err_info);
        }
        lua_pop(lua,1); /* Consume the Lua error */
    } else {
        /* On success convert the Lua return value into Redis protocol, and
         * send it to * the client. */

        /* 成功后，将 Lua 返回值转换为 Redis 协议，并将其发送 * 给客户端 */
        luaReplyToRedisReply(c, run_ctx->c, lua); /* Convert and consume the reply. */
    }

    /* Perform some cleanup that we need to do both on error and success. */
    
    /* 移除 hook 信息 */
    if (delhook) lua_sethook(lua,NULL,0,0); /* Disable hook */

    /* remove run_ctx from registry, its only applicable for the current script. */

    /* 移除上下文信息 */
    luaSaveOnRegistry(lua, REGISTRY_RUN_CTX_NAME, NULL);
}

unsigned long luaMemory(lua_State *lua) {
    return lua_gc(lua, LUA_GCCOUNT, 0) * 1024LL;
}
