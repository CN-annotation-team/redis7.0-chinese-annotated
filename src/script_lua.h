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

#ifndef __SCRIPT_LUA_H_
#define __SCRIPT_LUA_H_

/*
 * script_lua.c unit provides shared functionality between
 * eval.c and function_lua.c. Functionality provided:
 *
 * * Execute Lua code, assuming that the code is located on
 *   the top of the Lua stack. In addition, parsing the execution
 *   result and convert it to the resp and reply ot the client.
 *
 * * Run Redis commands from within the Lua code (Including
 *   parsing the reply and create a Lua object out of it).
 *
 * * Register Redis API to the Lua interpreter. Only shared
 *   API are registered (API that is only relevant on eval.c
 *   (like debugging) are registered on eval.c).
 *
 * Uses script.c for interaction back with Redis.
 */
/*
 * script_lua.c单元提供eval和function_lua之间的共享功能, 提供的功能：
 * 
 * 1. 执行Lua代码，假设代码位于Lua堆栈的顶部。此外，解析执行结果并将其转换为resp并回复客户端。
 * 2. 从Lua代码中运行Redis命令（包括解析回复并从中创建Lua对象）。
 * 3. 向Lua解释器注册Redis API。仅注册共享API（仅与eval.c相关的API（如调试）在eval.cc上注册）。
 * 
 * 使用script.c用于与Redis交互。
 * 主要定位是在完全和lua引擎交互。
 */

#include "server.h"
#include "script.h"
#include <lua.h>
#include <lauxlib.h>
#include <lualib.h>

#define REGISTRY_RUN_CTX_NAME "__RUN_CTX__"
#define REGISTRY_SET_GLOBALS_PROTECTION_NAME "__GLOBAL_PROTECTION__"
#define REDIS_API_NAME "redis"

typedef struct errorInfo {
    sds msg;
    sds source;
    sds line;
    int ignore_err_stats_update;
}errorInfo;

void luaRegisterRedisAPI(lua_State* lua);
sds luaGetStringSds(lua_State *lua, int index);
void luaRegisterGlobalProtectionFunction(lua_State *lua);
void luaSetErrorMetatable(lua_State *lua);
void luaSetAllowListProtection(lua_State *lua);
void luaSetTableProtectionRecursively(lua_State *lua);
void luaRegisterLogFunction(lua_State* lua);
void luaRegisterVersion(lua_State* lua);
void luaPushErrorBuff(lua_State *lua, sds err_buff);
void luaPushError(lua_State *lua, const char *error);
int luaError(lua_State *lua);
void luaSaveOnRegistry(lua_State* lua, const char* name, void* ptr);
void* luaGetFromRegistry(lua_State* lua, const char* name);
void luaCallFunction(scriptRunCtx* r_ctx, lua_State *lua, robj** keys, size_t nkeys, robj** args, size_t nargs, int debug_enabled);
void luaExtractErrorInformation(lua_State *lua, errorInfo *err_info);
void luaErrorInformationDiscard(errorInfo *err_info);
unsigned long luaMemory(lua_State *lua);


#endif /* __SCRIPT_LUA_H_ */
