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
#include <math.h> /* isnan(), isinf() */

/* Forward declarations */
/* 前向声明 */
int getGenericCommand(client *c);

/*-----------------------------------------------------------------------------
 * String Commands
 *----------------------------------------------------------------------------*/

/* 检查输入字符串长度，
 * 是由master发出的且字符串小于限制的最大长度512MB返回C_OK，否则返回C_ERR */
static int checkStringLength(client *c, long long size) {
    if (!(c->flags & CLIENT_MASTER) && size > server.proto_max_bulk_len) {
        addReplyError(c,"string exceeds maximum allowed size (proto-max-bulk-len)");
        return C_ERR;
    }
    return C_OK;
}

/* The setGenericCommand() function implements the SET operation with different
 * options and variants. This function is called in order to implement the
 * following commands: SET, SETEX, PSETEX, SETNX, GETSET.
 *
 * 'flags' changes the behavior of the command (NX, XX or GET, see below).
 *
 * 'expire' represents an expire to set in form of a Redis object as passed
 * by the user. It is interpreted according to the specified 'unit'.
 *
 * 'ok_reply' and 'abort_reply' is what the function will reply to the client
 * if the operation is performed, or when it is not because of NX or
 * XX flags.
 *
 * If ok_reply is NULL "+OK" is used.
 * If abort_reply is NULL, "$-1" is used. */

/* setGenericCommand（）函数使用不同的选项和变量，
 * 调用此函数是为了实现以下命令：
 * SET,SETEX,PSETEX,SETNX,GETSET.
 * 
 * flags”影响函数的执行过程（NX、XX或GET，见下文的define）
 * flags如何使用？以OBJ_SET_NX为例：
 * 没设置任何标志位的时候flags为0，
 * 设置OBJ_SET_NX标志则是用flags保存flags与OBJ_SET_NX异或结果(flags |= OBJ_SET_NX)
 * 判断是否有OBJ_SET_NX标志，若存在该标志则 flags & OBJ_SET_NX == 1
 * 
 * expire”表示给用户传递的Redis对象设置的过期时间，
 * 过期时间单位根据'unit'参数进行解析。
 * 
 * 'ok_reply'和'abort_reply'是在执行操作时，
 * 函数将向客户端回复的内容。
 * 
 * 如果ok_reply为NULL，则返回 "+OK"
 * 如果abort_reply为NULL，则返回"-1"。
*/
#define OBJ_NO_FLAGS 0
#define OBJ_SET_NX (1<<0)          /* Set if key not exists. */
#define OBJ_SET_XX (1<<1)          /* Set if key exists. */
#define OBJ_EX (1<<2)              /* Set if time in seconds is given */
#define OBJ_PX (1<<3)              /* Set if time in ms in given */
#define OBJ_KEEPTTL (1<<4)         /* Set and keep the ttl */
#define OBJ_SET_GET (1<<5)         /* Set if want to get key before set */
#define OBJ_EXAT (1<<6)            /* Set if timestamp in second is given */
#define OBJ_PXAT (1<<7)            /* Set if timestamp in ms is given */
#define OBJ_PERSIST (1<<8)         /* Set if we need to remove the ttl */

/* Forward declaration */
static int getExpireMillisecondsOrReply(client *c, robj *expire, int flags, int unit, long long *milliseconds);

void setGenericCommand(client *c, int flags, robj *key, robj *val, robj *expire, int unit, robj *ok_reply, robj *abort_reply) {
    long long milliseconds = 0; /* initialized to avoid any harmness warning */
    int found = 0;
    int setkey_flags = 0;

    /* 若命令带有设置过期功能，取出过期时间，若输入过期时间不合法返回C_ERR，导致报错并返回，
     * 调用getExpireMillisecondsOrReply函数后milliseconds将会被赋值成毫秒过期时间，
     * 注意redis都是以毫秒保存过期时间，若输入单位为秒则会转换为毫秒 */
    if (expire && getExpireMillisecondsOrReply(c, expire, flags, unit, &milliseconds) != C_OK) {
        return;
    }

    /* 若输入命令为GETSET，先执行get检查key，若key类型不为string会返回C_ERR，导致报错并返回 */
    if (flags & OBJ_SET_GET) {
        if (getGenericCommand(c) == C_ERR) return;
    }

    /* 若key存在，found = 1 */
    found = (lookupKeyWrite(c->db,key) != NULL);

    /* 若输入命令带有NX而key存在，或带有XX而key不存在，则在这返回 */
    if ((flags & OBJ_SET_NX && found) ||
        (flags & OBJ_SET_XX && !found))
    {
        if (!(flags & OBJ_SET_GET)) {
            addReply(c, abort_reply ? abort_reply : shared.null[c->resp]);
        }
        return;
    }

    /* 设置setkey_flags，setkey_flags将影响setKey函数的执行过程，
     * 第一个是set命令是否带有keepttl可选参数，若没有将会撤销key的过期时间（如果有），
     * 第二个是key是否已经存在，若不存在会在setKey时创建该key，若已经存在则重写 */
    setkey_flags |= (flags & OBJ_KEEPTTL) ? SETKEY_KEEPTTL : 0;
    setkey_flags |= found ? SETKEY_ALREADY_EXIST : SETKEY_DOESNT_EXIST;

    /* 在数据库中设置键值 */
    setKey(c,c->db,key,val,setkey_flags);

    /* dirty计数+1，
     * dirty记录服务器对本地存储而言的数据变动次数，
     * 持久化时将会削减计数，主动使用SAVE或BGSAVE成功保存将会清0 */
    server.dirty++;

    /* 发送事件通知 */
    notifyKeyspaceEvent(NOTIFY_STRING,"set",key,c->db->id);

    /* 如果命令要求设置过期时间，则设置它 */
    if (expire) {
        setExpire(c,c->db,key,milliseconds);
        /* Propagate as SET Key Value PXAT millisecond-timestamp if there is
         * EX/PX/EXAT/PXAT flag. */
        robj *milliseconds_obj = createStringObjectFromLongLong(milliseconds);
        rewriteClientCommandVector(c, 5, shared.set, key, val, shared.pxat, milliseconds_obj);
        decrRefCount(milliseconds_obj);
        notifyKeyspaceEvent(NOTIFY_GENERIC,"expire",key,c->db->id);
    }

    /* 命令非GETSET，则向客户端发送回复 */
    if (!(flags & OBJ_SET_GET)) {
        addReply(c, ok_reply ? ok_reply : shared.ok);
    }

    /* Propagate without the GET argument (Isn't needed if we had expire since in that case we completely re-written the command argv) */

    /* 如果输入命令为GETSET，
     * 在没有GET参数的情况下进行（网络）传播（如果我们已经过期了就不需要了，因为在这种情况下我们完全重新编写了命令(argv)）*/
    if ((flags & OBJ_SET_GET) && !expire) {
        int argc = 0;
        int j;
        robj **argv = zmalloc((c->argc-1)*sizeof(robj*));
        for (j=0; j < c->argc; j++) {
            char *a = c->argv[j]->ptr;
            /* Skip GET which may be repeated multiple times. */
            if (j >= 3 &&
                (a[0] == 'g' || a[0] == 'G') &&
                (a[1] == 'e' || a[1] == 'E') &&
                (a[2] == 't' || a[2] == 'T') && a[3] == '\0')
                continue;
            argv[argc++] = c->argv[j];
            incrRefCount(c->argv[j]);
        }
        replaceClientCommandVector(c, argc, argv);
    }
}

/*
 * Extract the `expire` argument of a given GET/SET command as an absolute timestamp in milliseconds.
 *
 * "client" is the client that sent the `expire` argument.
 * "expire" is the `expire` argument to be extracted.
 * "flags" represents the behavior of the command (e.g. PX or EX).
 * "unit" is the original unit of the given `expire` argument (e.g. UNIT_SECONDS).
 * "milliseconds" is output argument.
 *
 * If return C_OK, "milliseconds" output argument will be set to the resulting absolute timestamp.
 * If return C_ERR, an error reply has been added to the given client.
 */

/*
 * 提取一个给定的GET/SET命令的`expire`参数，作为一个绝对的时间戳，单位是毫秒。
 *
 * "client "是发送`expire'参数的客户端。
 * "expire "是要提取的`expire`参数。
 * "flags "代表该命令的行为（例如PX或EX）。
 * "单位 "是给定的`expire`参数的原始单位（例如UNIT_SECONDS）。
 * "milliseconds "是输出参数。
 *
 * 如果返回C_OK，"毫秒 "输出参数将被设置为结果的绝对时间戳。
 * 如果返回C_ERR，一个错误回复已被添加到发送该命令的客户端。
 */
static int getExpireMillisecondsOrReply(client *c, robj *expire, int flags, int unit, long long *milliseconds) {

    /* 调用getLongLongFromObjectOrReply后，若无报错则milliseconds被赋值成输入的过期时间数值 */
    int ret = getLongLongFromObjectOrReply(c, expire, milliseconds, NULL);
    if (ret != C_OK) {
        return ret;
    }

    if (*milliseconds <= 0 || (unit == UNIT_SECONDS && *milliseconds > LLONG_MAX / 1000)) {
        /* Negative value provided or multiplication is gonna overflow. */
        /* milliseconds为负，或超出最大限制值将导致报错并返回。 */
        addReplyErrorExpireTime(c);
        return C_ERR;
    }

    /* 若输入单位为秒，则转换为毫秒 */
    if (unit == UNIT_SECONDS) *milliseconds *= 1000;

    if ((flags & OBJ_PX) || (flags & OBJ_EX)) {
        *milliseconds += mstime();
    }

    if (*milliseconds <= 0) {
        /* Overflow detected. */
        /* 此时发生上溢 */
        addReplyErrorExpireTime(c);
        return C_ERR;
    }

    return C_OK;
}
/* 用于在下方parseExtendedStringArgumentsOrReply()函数表示命令为SET还是GET */
#define COMMAND_GET 0
#define COMMAND_SET 1
/*
 * The parseExtendedStringArgumentsOrReply() function performs the common validation for extended
 * string arguments used in SET and GET command.
 *
 * Get specific commands - PERSIST/DEL
 * Set specific commands - XX/NX/GET
 * Common commands - EX/EXAT/PX/PXAT/KEEPTTL
 *
 * Function takes pointers to client, flags, unit, pointer to pointer of expire obj if needed
 * to be determined and command_type which can be COMMAND_GET or COMMAND_SET.
 *
 * If there are any syntax violations C_ERR is returned else C_OK is returned.
 *
 * Input flags are updated upon parsing the arguments. Unit and expire are updated if there are any
 * EX/EXAT/PX/PXAT arguments. Unit is updated to millisecond if PX/PXAT is set.
 */

 /*
  * parseExtendedStringArgumentsOrReply()函数对SET和GET命令中使用的扩展命令进行普通验证。
  *
  * GET特定的命令 - PERSIST/DEL
  * SET特定的命令 - XX/NX/GET
  * 通用命令 - EX/EXAT/PX/PXAT/KEEPTTL
  *
  * 该函数需要指向客户端(c)、标志(flags)、单位(unit)的指针，如果需要的话，还需要指向过期时间的指针(expire)以及命令类型(command_type)，
  * 命令类型可以是COMMAND_GET或COMMAND_SET。
  *
  * 如果有任何违反语法的行为，将返回C_ERR，否则将返回C_OK。
  *
  * 在解析参数的时候，输入标志会被更新。如果有任何EXAT/PX/PXAT参数，单位和过期时间将被更新。
  * 如果设置了PX/PXAT，单位将被更新为毫秒。
  */
int parseExtendedStringArgumentsOrReply(client *c, int *flags, int *unit, robj **expire, int command_type) {

    int j = command_type == COMMAND_GET ? 2 : 3;
    for (; j < c->argc; j++) {
        char *opt = c->argv[j]->ptr;
        robj *next = (j == c->argc-1) ? NULL : c->argv[j+1];

        if ((opt[0] == 'n' || opt[0] == 'N') &&
            (opt[1] == 'x' || opt[1] == 'X') && opt[2] == '\0' &&
            !(*flags & OBJ_SET_XX) && (command_type == COMMAND_SET))
        {
            *flags |= OBJ_SET_NX;
        } else if ((opt[0] == 'x' || opt[0] == 'X') &&
                   (opt[1] == 'x' || opt[1] == 'X') && opt[2] == '\0' &&
                   !(*flags & OBJ_SET_NX) && (command_type == COMMAND_SET))
        {
            *flags |= OBJ_SET_XX;
        } else if ((opt[0] == 'g' || opt[0] == 'G') &&
                   (opt[1] == 'e' || opt[1] == 'E') &&
                   (opt[2] == 't' || opt[2] == 'T') && opt[3] == '\0' &&
                   (command_type == COMMAND_SET))
        {
            *flags |= OBJ_SET_GET;
        } else if (!strcasecmp(opt, "KEEPTTL") && !(*flags & OBJ_PERSIST) &&
            !(*flags & OBJ_EX) && !(*flags & OBJ_EXAT) &&
            !(*flags & OBJ_PX) && !(*flags & OBJ_PXAT) && (command_type == COMMAND_SET))
        {
            *flags |= OBJ_KEEPTTL;
        } else if (!strcasecmp(opt,"PERSIST") && (command_type == COMMAND_GET) &&
               !(*flags & OBJ_EX) && !(*flags & OBJ_EXAT) &&
               !(*flags & OBJ_PX) && !(*flags & OBJ_PXAT) &&
               !(*flags & OBJ_KEEPTTL))
        {
            *flags |= OBJ_PERSIST;
        } else if ((opt[0] == 'e' || opt[0] == 'E') &&
                   (opt[1] == 'x' || opt[1] == 'X') && opt[2] == '\0' &&
                   !(*flags & OBJ_KEEPTTL) && !(*flags & OBJ_PERSIST) &&
                   !(*flags & OBJ_EXAT) && !(*flags & OBJ_PX) &&
                   !(*flags & OBJ_PXAT) && next)
        {
            *flags |= OBJ_EX;
            *expire = next;
            j++;
        } else if ((opt[0] == 'p' || opt[0] == 'P') &&
                   (opt[1] == 'x' || opt[1] == 'X') && opt[2] == '\0' &&
                   !(*flags & OBJ_KEEPTTL) && !(*flags & OBJ_PERSIST) &&
                   !(*flags & OBJ_EX) && !(*flags & OBJ_EXAT) &&
                   !(*flags & OBJ_PXAT) && next)
        {
            *flags |= OBJ_PX;
            *unit = UNIT_MILLISECONDS;
            *expire = next;
            j++;
        } else if ((opt[0] == 'e' || opt[0] == 'E') &&
                   (opt[1] == 'x' || opt[1] == 'X') &&
                   (opt[2] == 'a' || opt[2] == 'A') &&
                   (opt[3] == 't' || opt[3] == 'T') && opt[4] == '\0' &&
                   !(*flags & OBJ_KEEPTTL) && !(*flags & OBJ_PERSIST) &&
                   !(*flags & OBJ_EX) && !(*flags & OBJ_PX) &&
                   !(*flags & OBJ_PXAT) && next)
        {
            *flags |= OBJ_EXAT;
            *expire = next;
            j++;
        } else if ((opt[0] == 'p' || opt[0] == 'P') &&
                   (opt[1] == 'x' || opt[1] == 'X') &&
                   (opt[2] == 'a' || opt[2] == 'A') &&
                   (opt[3] == 't' || opt[3] == 'T') && opt[4] == '\0' &&
                   !(*flags & OBJ_KEEPTTL) && !(*flags & OBJ_PERSIST) &&
                   !(*flags & OBJ_EX) && !(*flags & OBJ_EXAT) &&
                   !(*flags & OBJ_PX) && next)
        {
            *flags |= OBJ_PXAT;
            *unit = UNIT_MILLISECONDS;
            *expire = next;
            j++;
        } else {
            addReplyErrorObject(c,shared.syntaxerr);
            return C_ERR;
        }
    }
    return C_OK;
}

/* SET key value [NX] [XX] [KEEPTTL] [GET] [EX <seconds>] [PX <milliseconds>]
 *     [EXAT <seconds-timestamp>][PXAT <milliseconds-timestamp>] */
/* 处理set命令的函数 */
void setCommand(client *c) {
    robj *expire = NULL;
    int unit = UNIT_SECONDS;
    int flags = OBJ_NO_FLAGS;

    if (parseExtendedStringArgumentsOrReply(c,&flags,&unit,&expire,COMMAND_SET) != C_OK) {
        return;
    }

    c->argv[2] = tryObjectEncoding(c->argv[2]);
    setGenericCommand(c,flags,c->argv[1],c->argv[2],expire,unit,NULL,NULL);
}

/* 处理setnx命令的函数 */
void setnxCommand(client *c) {
    c->argv[2] = tryObjectEncoding(c->argv[2]);
    setGenericCommand(c,OBJ_SET_NX,c->argv[1],c->argv[2],NULL,0,shared.cone,shared.czero);
}

/* 处理setex命令的函数 */
void setexCommand(client *c) {
    c->argv[3] = tryObjectEncoding(c->argv[3]);
    setGenericCommand(c,OBJ_EX,c->argv[1],c->argv[3],c->argv[2],UNIT_SECONDS,NULL,NULL);
}

/* 处理psetex命令的函数 */
void psetexCommand(client *c) {
    c->argv[3] = tryObjectEncoding(c->argv[3]);
    setGenericCommand(c,OBJ_PX,c->argv[1],c->argv[3],c->argv[2],UNIT_MILLISECONDS,NULL,NULL);
}

/* get的通用实现函数 */
int getGenericCommand(client *c) {
    robj *o;

    /* 若查找不到该key，向客户端发送nil（lookupKeyReadOrReply函数中完成），返回C_OK */
    if ((o = lookupKeyReadOrReply(c,c->argv[1],shared.null[c->resp])) == NULL)
        return C_OK;

    /* 查找到key，但对应的对象类型不是string则checkType会返回1，返回C_ERR */
    if (checkType(c,o,OBJ_STRING)) {
        return C_ERR;
    }

    /* 向客户端回复对象的值 */
    addReplyBulk(c,o);
    return C_OK;
}

/* 处理get命令的函数 */
void getCommand(client *c) {
    getGenericCommand(c);
}

/*
 * GETEX <key> [PERSIST][EX seconds][PX milliseconds][EXAT seconds-timestamp][PXAT milliseconds-timestamp]
 *
 * The getexCommand() function implements extended options and variants of the GET command. Unlike GET
 * command this command is not read-only.
 *
 * The default behavior when no options are specified is same as GET and does not alter any TTL.
 *
 * Only one of the below options can be used at a given time.
 *
 * 1. PERSIST removes any TTL associated with the key.
 * 2. EX Set expiry TTL in seconds.
 * 3. PX Set expiry TTL in milliseconds.
 * 4. EXAT Same like EX instead of specifying the number of seconds representing the TTL
 *      (time to live), it takes an absolute Unix timestamp
 * 5. PXAT Same like PX instead of specifying the number of milliseconds representing the TTL
 *      (time to live), it takes an absolute Unix timestamp
 *
 * Command would either return the bulk string, error or nil.
 */


/* GETEX <key>[PERSIST][EX seconds][PX milliseconds][EXAT seconds - timestamp][PXAT milliseconds - timestamp]。
 *
 * getexCommand()函数实现了GET命令的扩展选项和变体。但不像GET命令，这个命令不是只读的
 *
 * 没有指定选项时的默认行为与GET相同，不会改变任何TTL（生存时间）
 *
 * 在一次命令内只能使用以下选项之一
 *
 * 1. PERSIST 删除任何与键相关的TTL
 * 2. EX 以秒为单位设置过期TTL
 * 3. PX 以毫秒为单位设置过期的TTL
 * 4. EXAT 与 EX 作用相同，但不是指定代表TTL的秒数,而是采用一个绝对的Unix时间戳
 *
 * 5. PXAT 与 PX 作用相同，但不是指定代表TTL的毫秒数，而是采用一个绝对的Unix时间戳
 *
 * 命令将返回批量字符串、错误或nil
 */

void getexCommand(client *c) {
    robj *expire = NULL;
    int unit = UNIT_SECONDS;
    int flags = OBJ_NO_FLAGS;

    /* 解析输入的命令字符串 */
    if (parseExtendedStringArgumentsOrReply(c,&flags,&unit,&expire,COMMAND_GET) != C_OK) {
        return;
    }

    robj *o;

    /* 在数据库中查找key，并给o赋值为key所对应的对象，若为NULL则返回 */
    if ((o = lookupKeyReadOrReply(c,c->argv[1],shared.null[c->resp])) == NULL)
        return;

    /* 检查对象类型 */
    if (checkType(c,o,OBJ_STRING)) {
        return;
    }

    /* Validate the expiration time value first */
    /* 首先验证并获取过期时间值 */
    long long milliseconds = 0;
    if (expire && getExpireMillisecondsOrReply(c, expire, flags, unit, &milliseconds) != C_OK) {
        return;
    }

    /* We need to do this before we expire the key or delete it */
    /* 在过期或删除key之前，我们需要先将value回复给客户端 */
    addReplyBulk(c,o);

    /* This command is never propagated as is. It is either propagated as PEXPIRE[AT],DEL,UNLINK or PERSIST.
     * This why it doesn't need special handling in feedAppendOnlyFile to convert relative expire time to absolute one. */

    /* 该命令不会被原封不动地传播。它要么以PEXPIRE[AT]、DEL、UNLINK或PERSIST的形式传播 */
    /* 这就是为什么它不需要在feedAppendOnlyFile函数中进行将相对过期时间转换成绝对过期时间的特殊处理 */
    if (((flags & OBJ_PXAT) || (flags & OBJ_EXAT)) && checkAlreadyExpired(milliseconds)) {
        /* When PXAT/EXAT absolute timestamp is specified, there can be a chance that timestamp
         * has already elapsed so delete the key in that case. */

        /*当指定PXAT/EXAT绝对时间戳时，有可能出现时间戳已经过期的情况，所以要删除该键 */
        /* 根据lazyfree_lazy_expire选项设置选择异步删除或是同步删除，该选项默认为0，即选择同步删除 */
        int deleted = server.lazyfree_lazy_expire ? dbAsyncDelete(c->db, c->argv[1]) :
                      dbSyncDelete(c->db, c->argv[1]);
        serverAssert(deleted);
        robj *aux = server.lazyfree_lazy_expire ? shared.unlink : shared.del;
        rewriteClientCommandVector(c,2,aux,c->argv[1]);
        signalModifiedKey(c, c->db, c->argv[1]);
        notifyKeyspaceEvent(NOTIFY_GENERIC, "del", c->argv[1], c->db->id);
        server.dirty++;
    } else if (expire) {
        setExpire(c,c->db,c->argv[1],milliseconds);
        /* Propagate as PXEXPIREAT millisecond-timestamp if there is
         * EX/PX/EXAT/PXAT flag and the key has not expired. */

        /* 如果有EX/PX/EXAT/PXAT标志且key未过期，
         * 则作为pexpireat毫秒时间戳进行传播 */
        robj *milliseconds_obj = createStringObjectFromLongLong(milliseconds);
        rewriteClientCommandVector(c,3,shared.pexpireat,c->argv[1],milliseconds_obj);
        decrRefCount(milliseconds_obj);
        signalModifiedKey(c, c->db, c->argv[1]);
        notifyKeyspaceEvent(NOTIFY_GENERIC,"expire",c->argv[1],c->db->id);
        server.dirty++;

      /* 带PERSIST参数，则执行删除该key过期时间的操作 */
    } else if (flags & OBJ_PERSIST) {
        if (removeExpire(c->db, c->argv[1])) {
            signalModifiedKey(c, c->db, c->argv[1]);
            rewriteClientCommandVector(c, 2, shared.persist, c->argv[1]);
            notifyKeyspaceEvent(NOTIFY_GENERIC,"persist",c->argv[1],c->db->id);
            server.dirty++;
        }
    }
}

/* 处理getdel命令的函数 */
void getdelCommand(client *c) {

    /* 先调用getGenericCommand函数获取值并回复客户端，再进行删除操作 */
    if (getGenericCommand(c) == C_ERR) return;
    int deleted = server.lazyfree_lazy_user_del ? dbAsyncDelete(c->db, c->argv[1]) :
                  dbSyncDelete(c->db, c->argv[1]);
    if (deleted) {
        /* Propagate as DEL/UNLINK command */
        /* 作为DEL/UNLINK命令传播 */
        robj *aux = server.lazyfree_lazy_user_del ? shared.unlink : shared.del;
        rewriteClientCommandVector(c,2,aux,c->argv[1]);
        signalModifiedKey(c, c->db, c->argv[1]);
        notifyKeyspaceEvent(NOTIFY_GENERIC, "del", c->argv[1], c->db->id);
        server.dirty++;
    }
}

/* 处理getset命令的函数 */
void getsetCommand(client *c) {

    /* 先调用getGenericCommand函数获取值并回复客户端，再进行设置操作 */
    if (getGenericCommand(c) == C_ERR) return;
    
    /* 调用tryObjectEncoding函数，给要设置的值选择合适的编码类型 */
    c->argv[2] = tryObjectEncoding(c->argv[2]);

    /* 在数据库中设置键值对 */
    setKey(c,c->db,c->argv[1],c->argv[2],0);
    notifyKeyspaceEvent(NOTIFY_STRING,"set",c->argv[1],c->db->id);
    server.dirty++;

    /* Propagate as SET command */
    /* 作为SET命令传播 */
    rewriteClientCommandArgument(c,0,shared.set);
}

/* 处理setrange命令的函数 */
void setrangeCommand(client *c) {
    robj *o;
    long offset;
    sds value = c->argv[3]->ptr;

    /* 取出offset */
    if (getLongFromObjectOrReply(c,c->argv[2],&offset,NULL) != C_OK)
        return;

    /* offset为负报错并返回 */
    if (offset < 0) {
        addReplyError(c,"offset is out of range");
        return;
    }

    o = lookupKeyWrite(c->db,c->argv[1]);
    if (o == NULL) {
        /* Return 0 when setting nothing on a non-existing string */
        /* 如果o为NULL且value长度为0，向客户端回复0并返回 */
        if (sdslen(value) == 0) {
            addReply(c,shared.czero);
            return;
        }

        /* Return when the resulting string exceeds allowed size */
        /* 如果设置后的新字符串长度会超过长度最大值限制，则报错并返回 */
        if (checkStringLength(c,offset+sdslen(value)) != C_OK)
            return;

        /* 根据输入的命令参数创建一个字符串对象 */
        o = createObject(OBJ_STRING,sdsnewlen(NULL, offset+sdslen(value)));

        /* 将对象加入到数据库 */
        dbAdd(c->db,c->argv[1],o);
    } else {
        size_t olen;

        /* Key exists, check type */
        /* Key存在，检查类型 */
        if (checkType(c,o,OBJ_STRING))
            return;

        /* Return existing string length when setting nothing */
        /* 如果要设置的值长度为0，即什么也不会修改，直接返回已经存在的字符串值 */
        olen = stringObjectLen(o);
        if (sdslen(value) == 0) {
            addReplyLongLong(c,olen);
            return;
        }

        /* Return when the resulting string exceeds allowed size */
        if (checkStringLength(c,offset+sdslen(value)) != C_OK)
            return;

        /* Create a copy when the object is shared or encoded. */
        /* 当对象被共享或编码时，创建一个副本，
         * 因为我们不要去修改一个共享对象,什么是共享对象可以拉到最底下有注释 */
        o = dbUnshareStringValue(c->db,c->argv[1],o);
    }

    if (sdslen(value) > 0) {
        
        /* 令原本的字符串值进行扩容 */
        o->ptr = sdsgrowzero(o->ptr,offset+sdslen(value));

        /* 使用memcpy将value按字节拷贝到原字符串值对应的位置(起始地址+offset) */
        memcpy((char*)o->ptr+offset,value,sdslen(value));
        signalModifiedKey(c,c->db,c->argv[1]);
        notifyKeyspaceEvent(NOTIFY_STRING,
            "setrange",c->argv[1],c->db->id);
        server.dirty++;
    }
    addReplyLongLong(c,sdslen(o->ptr));
}

/* 处理getrange命令的函数 */
void getrangeCommand(client *c) {
    robj *o;
    long long start, end;
    char *str, llbuf[32];
    size_t strlen;
    
    /* 检查输入参数start,end是否可以转化为long long，可以则取出start和end，否则报错并返回 */
    if (getLongLongFromObjectOrReply(c,c->argv[2],&start,NULL) != C_OK)
        return;
    if (getLongLongFromObjectOrReply(c,c->argv[3],&end,NULL) != C_OK)
        return;

    /* 检查key是否存在，存在就取出key对应的对象，之后检查对象类型是否为string */
    if ((o = lookupKeyReadOrReply(c,c->argv[1],shared.emptybulk)) == NULL ||
        checkType(c,o,OBJ_STRING)) return;

    /* 根据不同的字符串编码，选择不同的方法做一样的事：
     * str = 字符串值(value) , strlen = 字符串长度 */
    if (o->encoding == OBJ_ENCODING_INT) {
        str = llbuf;
        strlen = ll2string(llbuf,sizeof(llbuf),(long)o->ptr);
    } else {
        str = o->ptr;
        strlen = sdslen(str);
    }

    /* Convert negative indexes */
    /* 检查start和end是否合法与转化负数的索引start,end为正数 */
    if (start < 0 && end < 0 && start > end) {
        addReply(c,shared.emptybulk);
        return;
    }
    if (start < 0) start = strlen+start;
    if (end < 0) end = strlen+end;
    if (start < 0) start = 0;
    if (end < 0) end = 0;
    if ((unsigned long long)end >= strlen) end = strlen-1;

    /* Precondition: end >= 0 && end < strlen, so the only condition where
     * nothing can be returned is: start > end. */
    /* 前提条件：end >= 0 && end < strlen，所以唯一可以返回empty的条件是：start > end */
    if (start > end || strlen == 0) {
        addReply(c,shared.emptybulk);
    } else {
        addReplyBulkCBuffer(c,(char*)str+start,end-start+1);
    }
}

/* 处理mget命令的函数 */
void mgetCommand(client *c) {
    int j;

    addReplyArrayLen(c,c->argc-1);

    /* 查找并返回所有输入键的值 */
    for (j = 1; j < c->argc; j++) {
        robj *o = lookupKeyRead(c->db,c->argv[j]);
        if (o == NULL) {
            addReplyNull(c);
        } else {
            if (o->type != OBJ_STRING) {
                addReplyNull(c);
            } else {
                addReplyBulk(c,o);
            }
        }
    }
}

/* mset命令通用处理函数，实现mset和msetnx命令 */
void msetGenericCommand(client *c, int nx) {
    int j;

    /* mset命令输入的参数个数一定是奇数，
     * 例如:mset key1 value1 key2 value2 (5个)，
     * 如果是偶数则报错并返回 */
    if ((c->argc % 2) == 0) {
        addReplyErrorArity(c);
        return;
    }

    /* Handle the NX flag. The MSETNX semantic is to return zero and don't
     * set anything if at least one key already exists. */
    /* 先处理NX标志。如果至少有一个键已经存在，msetnx命令直接向客户端回复0并返回 */
    if (nx) {
        for (j = 1; j < c->argc; j += 2) {
            if (lookupKeyWrite(c->db,c->argv[j]) != NULL) {
                addReply(c, shared.czero);
                return;
            }
        }
    }

    /* 设置每一个键值对 */
    for (j = 1; j < c->argc; j += 2) {
        c->argv[j+1] = tryObjectEncoding(c->argv[j+1]);
        setKey(c,c->db,c->argv[j],c->argv[j+1],0);
        notifyKeyspaceEvent(NOTIFY_STRING,"set",c->argv[j],c->db->id);
    }
    server.dirty += (c->argc-1)/2;
    addReply(c, nx ? shared.cone : shared.ok);
}

/* 处理mset命令的函数 */
void msetCommand(client *c) {
    msetGenericCommand(c,0);
}

/* 处理msetnx命令的函数 */
void msetnxCommand(client *c) {
    msetGenericCommand(c,1);
}

/* incr,decr通用处理函数，
 * 实现incr,incrby,decr,decrby命令 */
void incrDecrCommand(client *c, long long incr) {
    long long value, oldvalue;
    robj *o, *new;

    /* 1.查key,对应的对象赋值给o  2.检查o的对象类型  3.取出对象的整数值并保存到value中 */
    o = lookupKeyWrite(c->db,c->argv[1]);
    if (checkType(c,o,OBJ_STRING)) return;
    if (getLongLongFromObjectOrReply(c,o,&value,NULL) != C_OK) return;

    oldvalue = value;

    /* 判断原值加上增量是否会超出范围，是的话报错并返回 */
    if ((incr < 0 && oldvalue < 0 && incr < (LLONG_MIN-oldvalue)) ||
        (incr > 0 && oldvalue > 0 && incr > (LLONG_MAX-oldvalue))) {
        addReplyError(c,"increment or decrement would overflow");
        return;
    }
    value += incr;

    /* 这里的if条件需要特别说明的有两个：
     * o->refcount == 1 : 对象的引用次数只有一次,即没有其他程序引用
     * (value < 0 || value >= OBJ_SHARED_INTEGERS) : value不在共享整数对象范围内,
     * 实际上就是o没有在引用共享对象,
     * 再满足了其他条件,这时我们就可以安全地直接修改o->ptr,否则需要创建新对象
     * 想了解什么是共享对象可以拉到文件最底下 */
    if (o && o->refcount == 1 && o->encoding == OBJ_ENCODING_INT &&
        (value < 0 || value >= OBJ_SHARED_INTEGERS) &&
        value >= LONG_MIN && value <= LONG_MAX)
    {
        new = o;
        o->ptr = (void*)((long)value);
    } else {
        new = createStringObjectFromLongLongForValue(value);
        if (o) {
            dbOverwrite(c->db,c->argv[1],new);
        } else {
            dbAdd(c->db,c->argv[1],new);
        }
    }
    signalModifiedKey(c,c->db,c->argv[1]);
    notifyKeyspaceEvent(NOTIFY_STRING,"incrby",c->argv[1],c->db->id);
    server.dirty++;
    addReplyLongLong(c, value);
}

/* 处理incr命令的函数 */
void incrCommand(client *c) {
    incrDecrCommand(c,1);
}

/* 处理decr命令的函数 */
void decrCommand(client *c) {
    incrDecrCommand(c,-1);
}

/* 处理incrby命令的函数 */
void incrbyCommand(client *c) {
    long long incr;

    if (getLongLongFromObjectOrReply(c, c->argv[2], &incr, NULL) != C_OK) return;
    incrDecrCommand(c,incr);
}

/* 处理decrby命令的函数 */
void decrbyCommand(client *c) {
    long long incr;

    if (getLongLongFromObjectOrReply(c, c->argv[2], &incr, NULL) != C_OK) return;
    /* Overflow check: negating LLONG_MIN will cause an overflow */
    /* 这里对incr做了个溢出判断,(不是很懂有什么必要在这里先检查,如果你知道请把括号这句删了并加上解释，谢谢！) */
    if (incr == LLONG_MIN) {
        addReplyError(c, "decrement would overflow");
        return;
    }
    incrDecrCommand(c,-incr);
}

/* incrbyfloat */
/* 处理incrbyfloat命令的函数 */
void incrbyfloatCommand(client *c) {
    long double incr, value;
    robj *o, *new;

    o = lookupKeyWrite(c->db,c->argv[1]);
    if (checkType(c,o,OBJ_STRING)) return;
    if (getLongDoubleFromObjectOrReply(c,o,&value,NULL) != C_OK ||
        getLongDoubleFromObjectOrReply(c,c->argv[2],&incr,NULL) != C_OK)
        return;

    /* value加上incr后判断value是否溢出 */
    value += incr;
    if (isnan(value) || isinf(value)) {
        addReplyError(c,"increment would produce NaN or Infinity");
        return;
    }

    /* 创建新的字符串对象 */
    new = createStringObjectFromLongDouble(value,1);

    /* o存在则对该key对应的字符串对象用new进行覆盖，不存在则将new加入数据库 */
    if (o)
        dbOverwrite(c->db,c->argv[1],new);
    else
        dbAdd(c->db,c->argv[1],new);
    signalModifiedKey(c,c->db,c->argv[1]);
    notifyKeyspaceEvent(NOTIFY_STRING,"incrbyfloat",c->argv[1],c->db->id);
    server.dirty++;
    addReplyBulk(c,new);

    /* Always replicate INCRBYFLOAT as a SET command with the final value
     * in order to make sure that differences in float precision or formatting
     * will not create differences in replicas or after an AOF restart. */
    rewriteClientCommandArgument(c,0,shared.set);
    rewriteClientCommandArgument(c,2,new);
    rewriteClientCommandArgument(c,3,shared.keepttl);
}

/* 处理append命令的函数 */
void appendCommand(client *c) {
    size_t totlen;
    robj *o, *append;

    o = lookupKeyWrite(c->db,c->argv[1]);
    if (o == NULL) {
        /* Create the key */
        /* key不存在，则创建一个 */
        c->argv[2] = tryObjectEncoding(c->argv[2]);
        dbAdd(c->db,c->argv[1],c->argv[2]);
        incrRefCount(c->argv[2]);
        totlen = stringObjectLen(c->argv[2]);
    } else {
        /* Key exists, check type */
        /* Key存在，先检查类型 */
        if (checkType(c,o,OBJ_STRING))
            return;

        /* "append" is an argument, so always an sds */
        /* "append"是一个输入参数，所以总是是一个sds(redis中的字符串类型) */
        append = c->argv[2];
        totlen = stringObjectLen(o)+sdslen(append->ptr);

        /* 检查追加字符串后的长度是否超出限制 */
        if (checkStringLength(c,totlen) != C_OK)
            return;

        /* Append the value */
        /* 执行追加操作 */
        o = dbUnshareStringValue(c->db,c->argv[1],o);
        o->ptr = sdscatlen(o->ptr,append->ptr,sdslen(append->ptr));
        totlen = sdslen(o->ptr);
    }
    signalModifiedKey(c,c->db,c->argv[1]);
    notifyKeyspaceEvent(NOTIFY_STRING,"append",c->argv[1],c->db->id);
    server.dirty++;
    addReplyLongLong(c,totlen);
}

/* 处理strlen命令的函数 */
void strlenCommand(client *c) {
    robj *o;
    if ((o = lookupKeyReadOrReply(c,c->argv[1],shared.czero)) == NULL ||
        checkType(c,o,OBJ_STRING)) return;

    /* 向客户端回复字符串对象的长度 */
    addReplyLongLong(c,stringObjectLen(o));
}

/* LCS key1 key2 [LEN] [IDX] [MINMATCHLEN <len>] [WITHMATCHLEN] */
/* 处理LCS命令的函数(求两个字符串的最长公共子序列) */
void lcsCommand(client *c) {
    uint32_t i, j;
    long long minmatchlen = 0;
    sds a = NULL, b = NULL;
    int getlen = 0, getidx = 0, withmatchlen = 0;
    robj *obja = NULL, *objb = NULL;

    /* 取得key1,key2所对应的字符串对象 */
    obja = lookupKeyRead(c->db,c->argv[1]);
    objb = lookupKeyRead(c->db,c->argv[2]);
    if ((obja && obja->type != OBJ_STRING) ||
        (objb && objb->type != OBJ_STRING))
    {
        /* obja或objb其中有一个对象不是string类型则向客户端报错 */
        addReplyError(c,
            "The specified keys must contain string values");
        /* Don't cleanup the objects, we need to do that
         * only after calling getDecodedObject(). */
        /* 不要清理这些对象，我们只在调用getDecodedObject()之后才用做这种事 */
        obja = NULL;
        objb = NULL;
        goto cleanup;
    }

    /* 调用getDecodedObject()会把内部编码为INT类型的字符串对象转为embstr或raw（用于比较字符串），
     * 若字符串对象为NULL则创建一个空串 */
    obja = obja ? getDecodedObject(obja) : createStringObject("",0);
    objb = objb ? getDecodedObject(objb) : createStringObject("",0);
    a = obja->ptr;
    b = objb->ptr;

    /* 解析可选参数 */
    for (j = 3; j < (uint32_t)c->argc; j++) {
        char *opt = c->argv[j]->ptr;
        int moreargs = (c->argc-1) - j;

        if (!strcasecmp(opt,"IDX")) {
            getidx = 1;
        } else if (!strcasecmp(opt,"LEN")) {
            getlen = 1;
        } else if (!strcasecmp(opt,"WITHMATCHLEN")) {
            withmatchlen = 1;
        } else if (!strcasecmp(opt,"MINMATCHLEN") && moreargs) {
            if (getLongLongFromObjectOrReply(c,c->argv[j+1],&minmatchlen,NULL)
                != C_OK) goto cleanup;
            if (minmatchlen < 0) minmatchlen = 0;
            j++;
        } else {
            addReplyErrorObject(c,shared.syntaxerr);
            goto cleanup;
        }
    }

    /* Complain if the user passed ambiguous parameters. */
    /* 如果用户传递了不明确的参数，则通过错误回复提醒用户 
     * 这里指同时用了len和idx参数 */
    if (getlen && getidx) {
        addReplyError(c,
            "If you want both the length and indexes, please just use IDX.");
        goto cleanup;
    }

    /* Detect string truncation or later overflows. */
    /* 检测字符串长度对于LCS(最长公共子序列)是否超出长度限制 */
    if (sdslen(a) >= UINT32_MAX-1 || sdslen(b) >= UINT32_MAX-1) {
        addReplyError(c, "String too long for LCS");
        goto cleanup;
    }

    /* Compute the LCS using the vanilla dynamic programming technique of
     * building a table of LCS(x,y) substrings. */
    /* 使用动态规划技术来计算LCS ,即建立一个LCS(x,y)子串表*/
    uint32_t alen = sdslen(a);
    uint32_t blen = sdslen(b);

    /* Setup an uint32_t array to store at LCS[i,j] the length of the
     * LCS A0..i-1, B0..j-1. Note that we have a linear array here, so
     * we index it as LCS[j+(blen+1)*j] */
    /* 设置一个uint32_t数组，在LCS[i,j]存储LCS A0...i-1, B0...j-1的长度
     * 请注意,我们这里有一个线性数组,所以我们的索引是LCS[j+(blen+1)*j] */
    #define LCS(A,B) lcs[(B)+((A)*(blen+1))]

    /* Try to allocate the LCS table, and abort on overflow or insufficient memory. */
    /* 尝试分配LCS表，在溢出或内存不足时中止 */
    unsigned long long lcssize = (unsigned long long)(alen+1)*(blen+1); /* Can't overflow due to the size limits above. */
    unsigned long long lcsalloc = lcssize * sizeof(uint32_t);
    uint32_t *lcs = NULL;
    if (lcsalloc < SIZE_MAX && lcsalloc / lcssize == sizeof(uint32_t)) {

        /* 内存不足，LCS的瞬时内存超过了proto-max-bulk-len */
        if (lcsalloc > (size_t)server.proto_max_bulk_len) {
            addReplyError(c, "Insufficient memory, transient memory for LCS exceeds proto-max-bulk-len");
            goto cleanup;
        }
        lcs = ztrymalloc(lcsalloc);
    }

    /* 内存不足，为LCS分配内存失败 */
    if (!lcs) {
        addReplyError(c, "Insufficient memory, failed allocating transient memory for LCS");
        goto cleanup;
    }

    /* Start building the LCS table. */
    /* 建立LCS子串表,具体的动态规划LCS算法可以自行上网学习 */
    for (uint32_t i = 0; i <= alen; i++) {
        for (uint32_t j = 0; j <= blen; j++) {
            if (i == 0 || j == 0) {
                /* If one substring has length of zero, the
                 * LCS length is zero. */

                /* 如果一个子串长度为0，则LCS长度为0 
                 * 实际上LCS数组下标为0的位置被用来做"哨兵" */
                LCS(i,j) = 0;
            } else if (a[i-1] == b[j-1]) {
                /* The len LCS (and the LCS itself) of two
                 * sequences with the same final character, is the
                 * LCS of the two sequences without the last char
                 * plus that last char. */

                /* 两个字符串当前下标字符相同，
                 * LCS(i,j)为考虑了前i和前j个字符串的最LCS,
                 * 此时LCS(i,j)应等于LCS(i-1,j-1)+1,
                 * 代表必然使用当前下标时形成的LCS长度 */
                LCS(i,j) = LCS(i-1,j-1)+1;
            } else {
                /* If the last character is different, take the longest
                 * between the LCS of the first string and the second
                 * minus the last char, and the reverse. */

                /* 如果当前下标字符不同，动态转移方程为:
                 * LCS(i,j) = max(LCS(i-1,j), LCS(i,j-1)
                 * 代表必然不使用 a[i-1]时 和 必然不使用 b[j-1] 组成LCS时的LCS长度 */
                uint32_t lcs1 = LCS(i-1,j);
                uint32_t lcs2 = LCS(i,j-1);
                LCS(i,j) = lcs1 > lcs2 ? lcs1 : lcs2;
            }
        }
    }

    /* Store the actual LCS string in "result" if needed. We create
     * it backward, but the length is already known, we store it into idx. */
    /* 如果需要,我们在之后将实际的LCS字符串存储在 "result "中。但长度已经知道了,我们先把它存入idx */
    uint32_t idx = LCS(alen,blen);
    sds result = NULL;        /* Resulting LCS string. */
    void *arraylenptr = NULL; /* Deferred length of the array for IDX. */
    uint32_t arange_start = alen, /* alen signals that values are not set. */
             arange_end = 0,
             brange_start = 0,
             brange_end = 0;

    /* Do we need to compute the actual LCS string? Allocate it in that case. */
    /* 根据和输入参数相关变量判断我们是否需要实际的LCS字符串
     * 如果需要,则给result分配内存空间 */
    int computelcs = getidx || !getlen;
    if (computelcs) result = sdsnewlen(SDS_NOINIT,idx);

    /* Start with a deferred array if we have to emit the ranges. */
    uint32_t arraylen = 0;  /* Number of ranges emitted in the array. */
    if (getidx) {
        addReplyMapLen(c,2);
        addReplyBulkCString(c,"matches");
        arraylenptr = addReplyDeferredLen(c);
    }

    i = alen, j = blen;

    /* 从字符串末尾向前比较 */
    while (computelcs && i > 0 && j > 0) {
        int emit_range = 0;
        if (a[i-1] == b[j-1]) {
            /* If there is a match, store the character and reduce
             * the indexes to look for a new match. */

            /* 如果有一个匹配的字符，则存储该字符并减少下标来寻找下一次匹配 */
            result[idx-1] = a[i-1];

            /* Track the current range. */
            /* 第一次匹配或者断开后重新匹配上相同字符，更新匹配区间 */
            if (arange_start == alen) {
                arange_start = i-1;
                arange_end = i-1;
                brange_start = j-1;
                brange_end = j-1;
            } else {
                /* Let's see if we can extend the range backward since
                 * it is contiguous. */
                
                /* 进入这里的条件实际上已是非首次在这个连续区间内匹配到相同字符
                 * 所以再连续匹配到相同字符则只减小范围的start下标,*/
                if (arange_start == i && brange_start == j) {
                    arange_start--;
                    brange_start--;
                } else {
                    emit_range = 1;
                }
            }
            /* Emit the range if we matched with the first byte of
             * one of the two strings. We'll exit the loop ASAP. */
            /* 如果两个字符串中的第一个字节相匹配，则输出最后一次匹配的区间(令emit_range = 1)。
             * 因为我们下一轮迭代将会退出这个循环 */
            if (arange_start == 0 || brange_start == 0) emit_range = 1;
            idx--; i--; j--;
        } else {
            /* Otherwise reduce i and j depending on the largest
             * LCS between, to understand what direction we need to go. */
            /* 根据最大的LCS减少i或j,即选择我们需要的方向 */
            uint32_t lcs1 = LCS(i-1,j);
            uint32_t lcs2 = LCS(i,j-1);
            if (lcs1 > lcs2)
                i--;
            else
                j--;

            /* 如果arange_start != alen , 则已经匹配到过相同字符, 
             * 即已经找到了一个新的匹配区间,而此时 */
            if (arange_start != alen) emit_range = 1;
        }

        /* Emit the current range if needed. */
        /* 把目前连续匹配到相同字符的区间回复给客户端(如果我们输入了idx参数的话) */
        /* 计算本轮连续匹配到相同字符区间的长度 */
        uint32_t match_len = arange_end - arange_start + 1;
        if (emit_range) {
            if (minmatchlen == 0 || match_len >= minmatchlen) {
                if (arraylenptr) {
                    addReplyArrayLen(c,2+withmatchlen);
                    addReplyArrayLen(c,2);
                    addReplyLongLong(c,arange_start);
                    addReplyLongLong(c,arange_end);
                    addReplyArrayLen(c,2);
                    addReplyLongLong(c,brange_start);
                    addReplyLongLong(c,brange_end);
                    if (withmatchlen) addReplyLongLong(c,match_len);
                    arraylen++;
                }
            }
            arange_start = alen; /* Restart at the next match. */
        }
    }

    /* Signal modified key, increment dirty, ... */

    /* Reply depending on the given options. */
    /* 根据输入的可选参数选择回复给客户端的信息 */
    if (arraylenptr) {
        addReplyBulkCString(c,"len");
        addReplyLongLong(c,LCS(alen,blen));
        setDeferredArrayLen(c,arraylenptr,arraylen);
    } else if (getlen) {
        addReplyLongLong(c,LCS(alen,blen));
    } else {
        addReplyBulkSds(c,result);
        result = NULL;
    }

    /* Cleanup. */
    /* 清理内存 */
    sdsfree(result);
    zfree(lcs);

cleanup:
    if (obja) decrRefCount(obja);
    if (objb) decrRefCount(objb);
    return;
}

/* Redis中的共享对象：
 * redis在初始化服务时,会创建一万个字符串对象,这些对象包含了0到9999的整数值
 * 当服务需要用到0到9999的字符串对象时,服务就会使用这些共享对象,而不是创建新的对象 
 * 因为创建大量的整数类型robj(redisObject)存在很多内存开销
 * 一个robj要远比一个整数占用的空间多得多(64位下至少16字节)
 * 所以Redis内存维护一个0到9999的整数对象池,用于节约内存 
 * 当然redis不止有0到9999这些整数共享对象
 * redis维护的共享对象在server.h文件中的sharedObjectsStruct定义中可以查看 */
