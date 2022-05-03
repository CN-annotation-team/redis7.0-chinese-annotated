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

/*-----------------------------------------------------------------------------
 * List API
 *----------------------------------------------------------------------------*/

/* The function pushes an element to the specified list object 'subject',
 * at head or tail position as specified by 'where'.
 *
 * There is no need for the caller to increment the refcount of 'value' as
 * the function takes care of it if needed. */

/* 该函数将一个元素插入到指定的列表对象 'subject'，
 * 插入位置由 'where' 决定是在列表头部还是尾部插入，
 * 调用者不需要自己来增加 'value' 的 refcount， 因为该函数会负责处理 */
void listTypePush(robj *subject, robj *value, int where) {

    /* 检查编码类型是否为 quicklist (快速列表) */
    if (subject->encoding == OBJ_ENCODING_QUICKLIST) {

        /* 根据参数 where 选择插入位置，由 pos 保存插入位置信息 */
        int pos = (where == LIST_HEAD) ? QUICKLIST_HEAD : QUICKLIST_TAIL;

        /* value 为整数编码 */
        if (value->encoding == OBJ_ENCODING_INT) {

            /* 将 value 先转换为字符串 */
            char buf[32];
            ll2string(buf, 32, (long)value->ptr);

            /* 将元素插入列表 */
            quicklistPush(subject->ptr, buf, strlen(buf), pos);

        /* value 为字符串编码 */
        } else {
            quicklistPush(subject->ptr, value->ptr, sdslen(value->ptr), pos);
        }
    } else {
        serverPanic("Unknown list encoding");
    }
}

/* 用于给列表弹出的元素创建副本的函数 */
void *listPopSaver(unsigned char *data, size_t sz) {
    return createStringObject((char*)data,sz);
}

/* 弹出列表元素的通用实现函数 */
robj *listTypePop(robj *subject, int where) {
    long long vlong;
    robj *value = NULL;

    /* 根据参数 where 选择插入位置，由 ql_where 保存插入位置信息 */
    int ql_where = where == LIST_HEAD ? QUICKLIST_HEAD : QUICKLIST_TAIL;

    /* 检查编码类型是否为 quicklist (快速列表) */
    if (subject->encoding == OBJ_ENCODING_QUICKLIST) {

        /* 从列表中弹出元素 */
        if (quicklistPopCustom(subject->ptr, ql_where, (unsigned char **)&value,
                               NULL, &vlong, listPopSaver)) {
            
            /* 如果 value 为 NULL ，则弹出元素被保存在 vlong 中（若不为 NULL 则说明保存在 value 中）
             * 则用 vlong 创建一个字符串对象并让 value 对其引用 */
            if (!value)
                value = createStringObjectFromLongLong(vlong);
        }
    } else {
        serverPanic("Unknown list encoding");
    }
    return value;
}

/* 获取列表长度（元素总数） */
unsigned long listTypeLength(const robj *subject) {
    if (subject->encoding == OBJ_ENCODING_QUICKLIST) {
        return quicklistCount(subject->ptr);
    } else {
        serverPanic("Unknown list encoding");
    }
}

/* Initialize an iterator at the specified index. */
/* 在指定的索引处初始化一个列表迭代器 */
listTypeIterator *listTypeInitIterator(robj *subject, long index,
                                       unsigned char direction) {

    /* 给列表迭代器分配内存空间 */
    listTypeIterator *li = zmalloc(sizeof(listTypeIterator));

    /* 初始化列表迭代器 */
    li->subject = subject;
    li->encoding = subject->encoding;
    li->direction = direction;
    li->iter = NULL;
    /* LIST_HEAD means start at TAIL and move *towards* head.
     * LIST_TAIL means start at HEAD and move *towards* tail. */
    
    /* LIST_HEAD 意味着从列表尾部开始，并向头部移动。
     * LIST_TAIL 表示从列表头部开始，并向尾部移动 */
    int iter_direction =
        direction == LIST_HEAD ? AL_START_TAIL : AL_START_HEAD;

    /* 检查编码类型是否为 quicklist (快速列表) */
    if (li->encoding == OBJ_ENCODING_QUICKLIST) {

        /* 初始化一个 quicklist 节点的迭代器，并且该迭代器指向列表的第 index 个元素，
         * 虽然用词是”指向“但是迭代器并不是一个指针，而是个结构体，它记录的元素信息是列表第 index 个元素 */
        li->iter = quicklistGetIteratorAtIdx(li->subject->ptr,
                                             iter_direction, index);
    } else {
        serverPanic("Unknown list encoding");
    }
    return li;
}

/* Sets the direction of an iterator. */ 
/* 设置迭代器的方向 */
void listTypeSetIteratorDirection(listTypeIterator *li, unsigned char direction) {
    li->direction = direction;
    int dir = direction == LIST_HEAD ? AL_START_TAIL : AL_START_HEAD;
    quicklistSetDirection(li->iter, dir);
}

/* Clean up the iterator. */
/* 释放迭代器 */
void listTypeReleaseIterator(listTypeIterator *li) {
    quicklistReleaseIterator(li->iter);
    zfree(li);
}

/* Stores pointer to current the entry in the provided entry structure
 * and advances the position of the iterator. Returns 1 when the current
 * entry is in fact an entry, 0 otherwise. */

/* 获取迭代器指向元素的下一个元素，并推进迭代器的位置（推进方向由迭代器成员 direction 决定）。
 * 如果 entry(下一个元素) 存在，返回1，否则返回0 */
int listTypeNext(listTypeIterator *li, listTypeEntry *entry) {
    /* Protect from converting when iterating */
    /* 保护迭代时编码不被转换，则需要迭代器记录的编码类型和迭代器指向的列表对象编码一致 */
    serverAssert(li->subject->encoding == li->encoding);

    /* 将参数 li 赋值给参数 entry 的迭代器成员 */
    entry->li = li;

    /* 检查编码类型是否为 quicklist (快速列表) */
    if (li->encoding == OBJ_ENCODING_QUICKLIST) {

        /* 调用 quicklistNext 函数获取下一个元素，
         * 元素 (quicklistEntry) 保存在 entry->entry 中，并推进迭代器位置
         * 如果 下一个元素存在，该函数返回1，否则返回0 */
        return quicklistNext(li->iter, &entry->entry);
    } else {
        serverPanic("Unknown list encoding");
    }
    return 0;
}

/* Return entry or NULL at the current position of the iterator. */
/* 获取元素的值 */
robj *listTypeGet(listTypeEntry *entry) {
    robj *value = NULL;

    /* 检查编码类型是否为 quicklist (快速列表) */
    if (entry->li->encoding == OBJ_ENCODING_QUICKLIST) {

        /* 元素的值保存在 value 中 */
        if (entry->entry.value) {
            
            /* value = 使用当前元素创建的字符串对象 */
            value = createStringObject((char *)entry->entry.value,
                                       entry->entry.sz);
        
        /* 元素的值保存在 longval 中 */
        } else {

            /* value = 使用当前（整型）元素创建的字符串对象 */
            value = createStringObjectFromLongLong(entry->entry.longval);
        }
    } else {
        serverPanic("Unknown list encoding");
    }
    return value;
}

/* 在 entry 的位置前或后方插入元素 value */
void listTypeInsert(listTypeEntry *entry, robj *value, int where) {

    /* 检查编码类型是否为 quicklist (快速列表) */
    if (entry->li->encoding == OBJ_ENCODING_QUICKLIST) {

        /* 获取解码后的 value（实际上是让编码为INT的 value 转为字符串形式（raw 或 embstr））*/
        value = getDecodedObject(value);

        /* 令 str = value（字符串值），len = value 字符串长度 */
        sds str = value->ptr;
        size_t len = sdslen(str);

        /* 在 entry 后方插入元素 */
        if (where == LIST_TAIL) {
            quicklistInsertAfter(entry->li->iter, &entry->entry, str, len);
        
        /* 在 entry 前方插入元素 */
        } else if (where == LIST_HEAD) {
            quicklistInsertBefore(entry->li->iter, &entry->entry, str, len);
        }

        /* value 的被引用次数 -1 ，value 被引用次数为0时将被释放 */
        decrRefCount(value);
    } else {
        serverPanic("Unknown list encoding");
    }
}

/* Replaces entry at the current position of the iterator. */
/* 替换 entry 中的元素 */
void listTypeReplace(listTypeEntry *entry, robj *value) {

    /* 检查编码类型是否为 quicklist (快速列表) */
    if (entry->li->encoding == OBJ_ENCODING_QUICKLIST) {

        /* 获取解码后的 value（实际上是让编码为INT的 value 转为字符串形式（raw 或 embstr））*/
        value = getDecodedObject(value);

        /* 令 str = value（字符串值），len = value 字符串长度 */
        sds str = value->ptr;
        size_t len = sdslen(str);

        /* 用 value 替换 entry 中的元素 */
        quicklistReplaceEntry(entry->li->iter, &entry->entry, str, len);

        /* value 的被引用次数 -1 ，value 被引用次数为0时将被释放 */
        decrRefCount(value);
    } else {
        serverPanic("Unknown list encoding");
    }
}

/* Compare the given object with the entry at the current position. */
/* 比较两个元素是否相同 */
int listTypeEqual(listTypeEntry *entry, robj *o) {

    /* 检查编码类型是否为 quicklist (快速列表) */
    if (entry->li->encoding == OBJ_ENCODING_QUICKLIST) {

        /* 使用断言确保 o 内部编码为字符串（raw 或 embstr） */
        serverAssertWithInfo(NULL,o,sdsEncodedObject(o));

        /* 调用比较函数进行比较，相同返回1，不相同返回0 */
        return quicklistCompare(&entry->entry,o->ptr,sdslen(o->ptr));
    } else {
        serverPanic("Unknown list encoding");
    }
}

/* Delete the element pointed to. */
/* 删除元素 */
void listTypeDelete(listTypeIterator *iter, listTypeEntry *entry) {

    /* 检查编码类型是否为 quicklist (快速列表) */
    if (entry->li->encoding == OBJ_ENCODING_QUICKLIST) {

        /* 调用删除函数将元素删除 */
        quicklistDelEntry(iter->iter, &entry->entry);
    } else {
        serverPanic("Unknown list encoding");
    }
}

/* This is a helper function for the COPY command.
 * Duplicate a list object, with the guarantee that the returned object
 * has the same encoding as the original one.
 *
 * The resulting object always has refcount set to 1 */

/* 这是一个用于 COPY 命令的辅助函数。
 * 复制一个列表对象，并保证返回的对象具有与原始对象相同的编码。
 * 返回的对象总是将 refcount 设置为1 */
robj *listTypeDup(robj *o) {
    robj *lobj;

    /* 使用断言确保 o 类型为列表 */
    serverAssert(o->type == OBJ_LIST);

    switch (o->encoding) {
        
        /* 检查编码类型是否为 quicklist (快速列表) */
        case OBJ_ENCODING_QUICKLIST:

            /* 创建 o 的副本 lobj */
            lobj = createObject(OBJ_LIST, quicklistDup(o->ptr));
            lobj->encoding = o->encoding;
            break;
        default:
            serverPanic("Unknown list encoding");
            break;
    }
    return lobj;
}

/* Delete a range of elements from the list. */
/* 在列表中删除一个指定范围内的元素 */
int listTypeDelRange(robj *subject, long start, long count) {

    /* 检查编码类型是否为 quicklist (快速列表) */
    if (subject->encoding == OBJ_ENCODING_QUICKLIST) {

        /* 调用范围删除函数进行删除 */
        return quicklistDelRange(subject->ptr, start, count);
    } else {
        serverPanic("Unknown list encoding");
    }
}

/*-----------------------------------------------------------------------------
 * List Commands
 *----------------------------------------------------------------------------*/

/* Implements LPUSH/RPUSH/LPUSHX/RPUSHX. 
 * 'xx': push if key exists. */

/* push 命令通用实现函数 
/* 实现的命令：LPUSH/RPUSH/LPUSHX/RPUSHX
 * 'xx': 如果 key 存在才能插入 */
void pushGenericCommand(client *c, int where, int xx) {
    int j;

    /* 获取 key 对应的 Redis 对象 */
    robj *lobj = lookupKeyWrite(c->db, c->argv[1]);

    /* 检查对象类型是否为 list，若不是 list 返回 true */
    if (checkType(c,lobj,OBJ_LIST)) return;

    /* 对象不存在 */
    if (!lobj) {

        /* 如果是 LPUSHX/RPUSHX 这类命令，key 不存在将不做操作 */
        if (xx) {
            
            /* 向客户端返回0并且退出函数 */
            addReply(c, shared.czero);
            return;
        }

        /* 创建 quicklist 对象 */
        lobj = createQuicklistObject();

        /* 初始设置 quicklist 对象（设置填充系数 fill 和压缩配置 compress） */
        quicklistSetOptions(lobj->ptr, server.list_max_listpack_size,
                            server.list_compress_depth);
        
        /* 将 quicklist 对象加入到数据库 */
        dbAdd(c->db,c->argv[1],lobj);
    }

    /* 读取输入的元素，并向列表插入元素 */
    for (j = 2; j < c->argc; j++) {
        listTypePush(lobj,c->argv[j],where);

        /* 脏数据 + 1，即缓存中未保存到本地的数据 */
        server.dirty++;
    }

    /* 向客户端回复列表长度 */
    addReplyLongLong(c, listTypeLength(lobj));

    char *event = (where == LIST_HEAD) ? "lpush" : "rpush";

    /* 发送 key 被修改信号 */
    signalModifiedKey(c,c->db,c->argv[1]);

    /* 发送事件通知（用于发布/订阅功能） */
    notifyKeyspaceEvent(NOTIFY_LIST,event,c->argv[1],c->db->id);
}

/* LPUSH <key> <element> [<element> ...] */
/* 处理 lpush 命令的函数 */
void lpushCommand(client *c) {
    pushGenericCommand(c,LIST_HEAD,0);
}

/* RPUSH <key> <element> [<element> ...] */
/* 处理 rpush 命令的函数 */
void rpushCommand(client *c) {
    pushGenericCommand(c,LIST_TAIL,0);
}

/* LPUSHX <key> <element> [<element> ...] */
/* 处理 lpushx 命令的函数 */
void lpushxCommand(client *c) {
    pushGenericCommand(c,LIST_HEAD,1);
}

/* RPUSH <key> <element> [<element> ...] */
/* 处理 rpushx 命令的函数 */
void rpushxCommand(client *c) {
    pushGenericCommand(c,LIST_TAIL,1);
}

/* LINSERT <key> (BEFORE|AFTER) <pivot> <element> */
/* 处理 linsert 命令的函数 */
void linsertCommand(client *c) {
    int where;
    robj *subject;
    listTypeIterator *iter;
    listTypeEntry entry;
    int inserted = 0;

    /* 解析插入方向参数，确定插入方向 */
    if (strcasecmp(c->argv[2]->ptr,"after") == 0) {
        where = LIST_TAIL;
    } else if (strcasecmp(c->argv[2]->ptr,"before") == 0) {
        where = LIST_HEAD;
    } else {

        /* 向客户端回复语法错误，并退出函数 */
        addReplyErrorObject(c,shared.syntaxerr);
        return;
    }

    /* 获取 key 对应的对象，之后检查对象类型 */
    if ((subject = lookupKeyWriteOrReply(c,c->argv[1],shared.czero)) == NULL ||
        checkType(c,subject,OBJ_LIST)) return;

    /* Seek pivot from head to tail */
    /* 从列表的头到尾部遍历寻找 pivot ，我们要插入在输入参数 pivot 所代表的元素的前或后方 */

    /* 初始化列表迭代器 */
    iter = listTypeInitIterator(subject,0,LIST_TAIL);

    /* 遍历寻找 */
    while (listTypeNext(iter,&entry)) {

        /* 找到与 pivot 相等的元素 */
        if (listTypeEqual(&entry,c->argv[3])) {

            /* 插入元素 */
            listTypeInsert(&entry,c->argv[4],where);
            inserted = 1;
            break;
        }
    }

    /* 释放迭代器 */
    listTypeReleaseIterator(iter);

    /* 插入成功 */
    if (inserted) {
        signalModifiedKey(c,c->db,c->argv[1]);
        notifyKeyspaceEvent(NOTIFY_LIST,"linsert",
                            c->argv[1],c->db->id);
        server.dirty++;
    } else {
        /* Notify client of a failed insert */
        /* 回复客户端插入失败，即回复 -1 */
        addReplyLongLong(c,-1);
        return;
    }

    addReplyLongLong(c,listTypeLength(subject));
}

/* LLEN <key> */
/* 处理 llen 命令的函数 */
void llenCommand(client *c) {

    /* 获取 key 对应的对象 */
    robj *o = lookupKeyReadOrReply(c,c->argv[1],shared.czero);

    /* 检查对象是否为空和对象类型 */
    if (o == NULL || checkType(c,o,OBJ_LIST)) return;

    /* 回复列表长度 */
    addReplyLongLong(c,listTypeLength(o));
}

/* LINDEX <key> <index> */
/* 处理 lindex 命令的函数 */
void lindexCommand(client *c) {

    /* 获取 key 对应的对象 */
    robj *o = lookupKeyReadOrReply(c,c->argv[1],shared.null[c->resp]);

    /* 检查对象是否为空和对象类型 */
    if (o == NULL || checkType(c,o,OBJ_LIST)) return;
    long index;

    /* 解析 index 参数 */
    if ((getLongFromObjectOrReply(c, c->argv[2], &index, NULL) != C_OK))
        return;

    /* 对象内部编码为 quicklist */
    if (o->encoding == OBJ_ENCODING_QUICKLIST) {
        quicklistEntry entry;

        /* 创建 index 位置的迭代器 */
        quicklistIter *iter = quicklistGetIteratorEntryAtIdx(o->ptr, index, &entry);

        /* 迭代器 iter 存在 */
        if (iter) {

            /* 元素的值为字符串类型，则保存在 value */
            if (entry.value) {
                addReplyBulkCBuffer(c, entry.value, entry.sz);
            
            /* 元素的值为整数类型，则保存在 longval */
            } else {
                addReplyBulkLongLong(c, entry.longval);
            }
        
        /* 迭代器 iter 不存在 */
        } else {
            addReplyNull(c);
        }

        /* 释放迭代器 */
        quicklistReleaseIterator(iter);
    } else {
        serverPanic("Unknown list encoding");
    }
}

/* LSET <key> <index> <element> */
/* 处理 lset 命令的函数 */
void lsetCommand(client *c) {
    robj *o = lookupKeyWriteOrReply(c,c->argv[1],shared.nokeyerr);
    if (o == NULL || checkType(c,o,OBJ_LIST)) return;
    long index;

    /* 获取输入的元素 (element) */
    robj *value = c->argv[3];

    /* 获取输入的索引 (index) */
    if ((getLongFromObjectOrReply(c, c->argv[2], &index, NULL) != C_OK))
        return;

    if (o->encoding == OBJ_ENCODING_QUICKLIST) {

        /* 获取快速列表 */
        quicklist *ql = o->ptr;

        /* 调用 quicklistReplaceAtIndex 函数替换 index 上的元素 */
        int replaced = quicklistReplaceAtIndex(ql, index,
                                               value->ptr, sdslen(value->ptr));

        /* 元素替换失败 */
        if (!replaced) {

            /* 向客户端回复错误信息（范围越界） */
            addReplyErrorObject(c,shared.outofrangeerr);
        } else {

            /* 向客户端回复 "ok" */
            addReply(c,shared.ok);

            /* 发送 key 被修改信号，发送事件通知，脏数据 + 1*/
            signalModifiedKey(c,c->db,c->argv[1]);
            notifyKeyspaceEvent(NOTIFY_LIST,"lset",c->argv[1],c->db->id);
            server.dirty++;
        }
    } else {
        serverPanic("Unknown list encoding");
    }
}

/* A helper function like addListRangeReply, more details see below.
 * The difference is that here we are returning nested arrays, like:
 * 1) keyname
 * 2) 1) element1
 *    2) element2
 *
 * And also actually pop out from the list by calling listElementsRemoved.
 * We maintain the server.dirty and notifications there.
 *
 * 'deleted' is an optional output argument to get an indication
 * if the key got deleted by this function. */

 /* 一个辅助函数，如 addListRangeReply，更多细节见下文。
  * 不同的是，这里我们返回的是嵌套数组，比如：
  * 1) keyname
  * 2) 1) element1
  *    2) element2
  *
  * 通过调用 listElementsRemoved 从列表中弹出元素。
  * 我们在 listElementsRemoved 函数中维护 server.dirty 和事件通知。
  *
  * 'deleted' 是一个可选的输出参数，含义为是否有 key 被删除（有为1，没有为0） */
void listPopRangeAndReplyWithKey(client *c, robj *o, robj *key, int where, long count, int *deleted) {
    long llen = listTypeLength(o);
    long rangelen = (count > llen) ? llen : count;
    long rangestart = (where == LIST_HEAD) ? 0 : -rangelen;
    long rangeend = (where == LIST_HEAD) ? rangelen - 1 : -1;
    int reverse = (where == LIST_HEAD) ? 0 : 1;

    /* We return key-name just once, and an array of elements */
    /* 我们只返回一次 key 的名称，以及一个指定范围内的元素数组 */
    addReplyArrayLen(c, 2);
    addReplyBulk(c, key);
    addListRangeReply(c, o, rangestart, rangeend, reverse);

    /* Pop these elements. */
    /* 弹出范围内的元素 */
    listTypeDelRange(o, rangestart, rangelen);

    /* Maintain the notifications and dirty. */
    /* 维护事件通知和脏数据 */
    listElementsRemoved(c, key, where, o, rangelen, deleted);
}

/* A helper for replying with a list's range between the inclusive start and end
 * indexes as multi-bulk, with support for negative indexes. Note that start
 * must be less than end or an empty array is returned. When the reverse
 * argument is set to a non-zero value, the reply is reversed so that elements
 * are returned from end to start. */

 /* 一个帮助器，用于回复包含开始和结束之间的列表范围 
  * 多批量索引，支持负索引。 注意开始
  * 必须小于 end 否则返回一个空数组。 反转的时候
  * 参数设置为非零值，回复被颠倒以便元素
  * 从头到尾返回 */
void addListRangeReply(client *c, robj *o, long start, long end, int reverse) {
    long rangelen, llen = listTypeLength(o);

    /* Convert negative indexes. */
    /* 将负数索引转化为正数 */
    if (start < 0) start = llen+start;
    if (end < 0) end = llen+end;
    if (start < 0) start = 0;

    /* Invariant: start >= 0, so this test will be true when end < 0.
     * The range is empty when start > end or start >= length. */

    /* 经过上面转化后一定有 start >= 0，所以当 end < 0 时，该 if 总是为真
     * 当 start > end 或 start >= llen 时，该范围是空的 */
    if (start > end || start >= llen) {

        /* 返回一个空数组并退出函数 */
        addReply(c,shared.emptyarray);
        return;
    }

    /* end 超出范围时强制令 end = llen - 1，防止越界 */
    if (end >= llen) end = llen-1;

    /* 计算范围长度 */
    rangelen = (end-start)+1;

    /* Return the result in form of a multi-bulk reply */
    /* 以 multi-bulk （可以理解为结果数组）的形式向客户端返回结果 */
    addReplyArrayLen(c,rangelen);
    if (o->encoding == OBJ_ENCODING_QUICKLIST) {

        /* 获取范围起点，迭代方向并初始化范围起点位置的迭代器 */
        int from = reverse ? end : start;
        int direction = reverse ? LIST_HEAD : LIST_TAIL;
        listTypeIterator *iter = listTypeInitIterator(o,from,direction);

        /* 循环获取范围内的元素并加入到回复中 */
        while(rangelen--) {
            listTypeEntry entry;
            serverAssert(listTypeNext(iter, &entry)); /* 若出现数据损坏将会导致失败 */
            quicklistEntry *qe = &entry.entry;

            /* 元素为字符串编码，保存在 value 中 */
            if (qe->value) {
                addReplyBulkCBuffer(c,qe->value,qe->sz);

            /* 元素为整数编码 */
            } else {
                addReplyBulkLongLong(c,qe->longval);
            }
        }
        
        /* 释放迭代器 */
        listTypeReleaseIterator(iter);
    } else {
        serverPanic("Unknown list encoding");
    }
}

/* A housekeeping helper for list elements popping tasks.
 *
 * 'deleted' is an optional output argument to get an indication
 * if the key got deleted by this function. */

/* 用于完成列表元素弹出后的后续步骤。
 *
 * 'deleted' 是一个可选的输出参数，含义为是否有 key 被删除（有为1，没有为0）*/
void listElementsRemoved(client *c, robj *key, int where, robj *o, long count, int *deleted) {
    char *event = (where == LIST_HEAD) ? "lpop" : "rpop";

    /* 发送事件通知 */
    notifyKeyspaceEvent(NOTIFY_LIST, event, key, c->db->id);

    /* 如果列表长度（在弹出元素后）为0，deleted 设置为 1，表示列表被删除 */
    if (listTypeLength(o) == 0) {
        if (deleted) *deleted = 1;

        /* 将 key 从数据库中删除 */
        dbDelete(c->db, key);

        /* 发送事件通知 */
        notifyKeyspaceEvent(NOTIFY_GENERIC, "del", key, c->db->id);
    } else {
        if (deleted) *deleted = 0;
    }
    
    /* 发送 key 被修改信号，以及脏数据加上 count（被删除元素的数量） */
    signalModifiedKey(c, c->db, key);
    server.dirty += count;
}

/* Implements the generic list pop operation for LPOP/RPOP.
 * The where argument specifies which end of the list is operated on. An
 * optional count may be provided as the third argument of the client's
 * command. */

/* 实现 LPOP/RPOP 的通用列表弹出操作函数。
 * where 参数指定了对列表的哪一端进行操作。
 * 一个可选参数 count 被客户端命令的第三个参数提供。*/
void popGenericCommand(client *c, int where) {
    int hascount = (c->argc == 3);
    long count = 0;
    robj *value;

    /* 检查参数个数，超过3个则输入有误 */
    if (c->argc > 3) {
        addReplyErrorArity(c);
        return;

    /* 有输入可选参数 count */
    } else if (hascount) {
        /* Parse the optional count argument. */
        /* 解析可选参数 count，并将输入参数 count 赋值到变量 count 中 */
        if (getPositiveLongFromObjectOrReply(c,c->argv[2],&count,NULL) != C_OK) 
            return;
    }

    /* 获取 key 对应的对象 */
    robj *o = lookupKeyWriteOrReply(c, c->argv[1], hascount ? shared.nullarray[c->resp]: shared.null[c->resp]);
    if (o == NULL || checkType(c, o, OBJ_LIST))
        return;

    /* 如果输入的 count 为 0 */
    if (hascount && !count) {
        /* Fast exit path. */
        /* 回复一个空数组并退出函数 */
        addReply(c,shared.emptyarray);
        return;
    }

    /* 如果没有输入可选参数 count */
    if (!count) {
        /* Pop a single element. This is POP's original behavior that replies
         * with a bulk string. */

        /* 弹出一个元素，这是 pop 的原始操作，会向客户端回复一个字符串 */
        value = listTypePop(o,where);
        serverAssert(value != NULL);

        /* 回复客户端 */
        addReplyBulk(c,value);

        /* 减少 value 的被引用计数 */
        decrRefCount(value);

        /* 真正删除元素 */
        listElementsRemoved(c,c->argv[1],where,o,1,NULL);
    } else {
        /* Pop a range of elements. An addition to the original POP command,
         *  which replies with a multi-bulk. */

        /* 弹出一系列的元素。是原有 pop 命令的附加功能，回复形式是一个 multi-bulk （多返回值） */
        long llen = listTypeLength(o);
        long rangelen = (count > llen) ? llen : count;
        long rangestart = (where == LIST_HEAD) ? 0 : -rangelen;
        long rangeend = (where == LIST_HEAD) ? rangelen - 1 : -1;
        int reverse = (where == LIST_HEAD) ? 0 : 1;

        addListRangeReply(c,o,rangestart,rangeend,reverse);
        listTypeDelRange(o,rangestart,rangelen);
        listElementsRemoved(c,c->argv[1],where,o,rangelen,NULL);
    }
}

/* Like popGenericCommand but work with multiple keys.
 * Take multiple keys and return multiple elements from just one key.
 *
 * 'numkeys' the number of keys.
 * 'count' is the number of elements requested to pop.
 *
 * Always reply with array. */

/* 基本就像 popGenericCommand 一样，但支持多个 key 输入。
 * 取多个 key 中输入顺序从左到右的第一个不为空的 key ，返回多个元素。
 *
 * 'numkeys'是 key 的数量。
 * 'count'是要求弹出的元素的数量。
 *
 * 总是用数组来向客户端回复。*/
void mpopGenericCommand(client *c, robj **keys, int numkeys, int where, long count) {
    int j;
    robj *o;
    robj *key;

    for (j = 0; j < numkeys; j++) {
        key = keys[j];
        o = lookupKeyWrite(c->db, key);

        /* Non-existing key, move to next key. */
        /* key 不存在，跳过到下一个 key */
        if (o == NULL) continue;

        /* 检查对象类型 */
        if (checkType(c, o, OBJ_LIST)) return;

        long llen = listTypeLength(o);
        /* Empty list, move to next key. */
        /* 空列表，跳过到下一个 key */
        if (llen == 0) continue;

        /* Pop a range of elements in a nested arrays way. */
        /* 弹出一系列元素，并以嵌套数组的方式回复给客户端 */
        listPopRangeAndReplyWithKey(c, o, key, where, count, NULL);

        /* Replicate it as [LR]POP COUNT. */
        /* 将其重写为 [LR]POP COUNT 的形式传播给AOF与从服务器 */
        robj *count_obj = createStringObjectFromLongLong((count > llen) ? llen : count);
        rewriteClientCommandVector(c, 3,
                                   (where == LIST_HEAD) ? shared.lpop : shared.rpop,
                                   key, count_obj);
        decrRefCount(count_obj);
        return;
    }

    /* Look like we are not able to pop up any elements. */
    /* 我们没有能够弹出任何元素，回复空数组 */
    addReplyNullArray(c);
}

/* LPOP <key> [count] */
/* 处理 LPOP 命令的函数 */
void lpopCommand(client *c) {
    popGenericCommand(c,LIST_HEAD);
}

/* RPOP <key> [count] */
/* 处理 RPOP 命令的函数 */
void rpopCommand(client *c) {
    popGenericCommand(c,LIST_TAIL);
}

/* LRANGE <key> <start> <stop> */
/* 处理 LRANGE 命令的函数 */
void lrangeCommand(client *c) {
    robj *o;
    long start, end;

    /* 获取输入的 start 和 end 参数 */
    if ((getLongFromObjectOrReply(c, c->argv[2], &start, NULL) != C_OK) ||
        (getLongFromObjectOrReply(c, c->argv[3], &end, NULL) != C_OK)) return;

    /* 获取 key 对应的对象 */
    if ((o = lookupKeyReadOrReply(c,c->argv[1],shared.emptyarray)) == NULL
         || checkType(c,o,OBJ_LIST)) return;

    addListRangeReply(c,o,start,end,0);
}

/* LTRIM <key> <start> <stop> */
/* 处理 LTRIM 命令的函数 */
void ltrimCommand(client *c) {
    robj *o;
    long start, end, llen, ltrim, rtrim;

    /* 获取输入的 start 和 end 参数 */
    if ((getLongFromObjectOrReply(c, c->argv[2], &start, NULL) != C_OK) ||
        (getLongFromObjectOrReply(c, c->argv[3], &end, NULL) != C_OK)) return;

    /* 获取 key 对应的对象 */
    if ((o = lookupKeyWriteOrReply(c,c->argv[1],shared.ok)) == NULL ||
        checkType(c,o,OBJ_LIST)) return;
    llen = listTypeLength(o);

    /* convert negative indexes */
    /* 将负数索引转化为正数 */
    if (start < 0) start = llen+start;
    if (end < 0) end = llen+end;
    if (start < 0) start = 0;

    /* Invariant: start >= 0, so this test will be true when end < 0.
     * The range is empty when start > end or start >= length. */

    /* 经过上面转化后一定有 start >= 0，所以当 end < 0 时，该 if 总是为真
     * 当 start > end 或 start >= llen 时，该范围是空的 */
    if (start > end || start >= llen) {
        /* Out of range start or start > end result in empty list */
        /* start 越界或 start > end 则该范围是空的 */
        ltrim = llen;
        rtrim = 0;
    } else {
        if (end >= llen) end = llen-1;
        ltrim = start;
        rtrim = llen-end-1;
    }

    /* Remove list elements to perform the trim */
    /* 通过弹出指定范围外的列表元素来修剪列表 */
    if (o->encoding == OBJ_ENCODING_QUICKLIST) {
        quicklistDelRange(o->ptr,0,ltrim);
        quicklistDelRange(o->ptr,-rtrim,rtrim);
    } else {
        serverPanic("Unknown list encoding");
    }
    
    /* 发送事件通知 */
    notifyKeyspaceEvent(NOTIFY_LIST,"ltrim",c->argv[1],c->db->id);

    /* 列表长度为0，则将它从数据库中删除 */
    if (listTypeLength(o) == 0) {
        dbDelete(c->db,c->argv[1]);
        notifyKeyspaceEvent(NOTIFY_GENERIC,"del",c->argv[1],c->db->id);
    }

    /* 发送 key 被修改信号 */
    signalModifiedKey(c,c->db,c->argv[1]);

    /* 脏数据加上被弹出的元素个数 */
    server.dirty += (ltrim + rtrim);

    /* 回复客户端 */
    addReply(c,shared.ok);
}

/* LPOS key element [RANK rank] [COUNT num-matches] [MAXLEN len]
 *
 * The "rank" is the position of the match, so if it is 1, the first match
 * is returned, if it is 2 the second match is returned and so forth.
 * It is 1 by default. If negative has the same meaning but the search is
 * performed starting from the end of the list.
 *
 * If COUNT is given, instead of returning the single element, a list of
 * all the matching elements up to "num-matches" are returned. COUNT can
 * be combined with RANK in order to returning only the element starting
 * from the Nth. If COUNT is zero, all the matching elements are returned.
 *
 * MAXLEN tells the command to scan a max of len elements. If zero (the
 * default), all the elements in the list are scanned if needed.
 *
 * The returned elements indexes are always referring to what LINDEX
 * would return. So first element from head is 0, and so forth. */

/* 命令用法：LPOS key element [RANK rank] [COUNT num-matches] [MAXLEN len]
 *
 * rank "是匹配的位置，所以如果它是1，则返回第一个匹配。
 * 如果是2，则返回第二个匹配，以此类推。
 * 它默认为1。如果是负数，含义为倒数第 rank 个元素。
 *
 * 如果给出 count，不是返回单个元素，而是返回一个列表，其中包括
 * 所有匹配的元素（匹配个数由 num-matches 决定）。
 * count 可以可以与 rank 结合，以便只返回从第 rank 个开始的元素。
 * 如果 count 为 0，则返回所有的匹配元素。
 *
 * maxlen 告诉命令要扫描的最大长度。如果是0（默认值）
 * 将扫描列表中的所有元素。
 *
 * 返回的元素索引和使用 lindex 命令会返回的索引一致，
 * 所以从头开始的第一个元素是0，以此类推。*/
void lposCommand(client *c) {
    robj *o, *ele;
    ele = c->argv[2];
    int direction = LIST_TAIL;
    long rank = 1, count = -1, maxlen = 0; /* Count -1: option not given. */

    /* Parse the optional arguments. */
    /* 解析可选参数 */
    for (int j = 3; j < c->argc; j++) {
        char *opt = c->argv[j]->ptr;
        int moreargs = (c->argc-1)-j;

        if (!strcasecmp(opt,"RANK") && moreargs) {
            j++;
            if (getLongFromObjectOrReply(c, c->argv[j], &rank, NULL) != C_OK)
                return;
            if (rank == 0) {
                addReplyError(c,"RANK can't be zero: use 1 to start from "
                                "the first match, 2 from the second, ...");
                return;
            }
        } else if (!strcasecmp(opt,"COUNT") && moreargs) {
            j++;
            if (getPositiveLongFromObjectOrReply(c, c->argv[j], &count,
              "COUNT can't be negative") != C_OK)
                return;
        } else if (!strcasecmp(opt,"MAXLEN") && moreargs) {
            j++;
            if (getPositiveLongFromObjectOrReply(c, c->argv[j], &maxlen, 
              "MAXLEN can't be negative") != C_OK)
                return;
        } else {
            addReplyErrorObject(c,shared.syntaxerr);
            return;
        }
    }

    /* A negative rank means start from the tail. */
    /* 负数意味着从尾部开始扫描 */
    if (rank < 0) {
        rank = -rank;
        direction = LIST_HEAD;
    }

    /* We return NULL or an empty array if there is no such key (or
     * if we find no matches, depending on the presence of the COUNT option. */

    /* 如果查找不到 key，我们将返回NULL或一个空数组（如果使用了 count 选项，在没有找到 key 的情况下返回空数组） */
    if ((o = lookupKeyRead(c->db,c->argv[1])) == NULL) {
        if (count != -1)
            addReply(c,shared.emptyarray);
        else
            addReply(c,shared.null[c->resp]);
        return;
    }
    if (checkType(c,o,OBJ_LIST)) return;

    /* If we got the COUNT option, prepare to emit an array. */
    /* 如果有 count 选项，准备一个数组用于回复客户端 */
    void *arraylenptr = NULL;
    if (count != -1) arraylenptr = addReplyDeferredLen(c);

    /* Seek the element. */
    /* 查找元素 */
    listTypeIterator *li;
    li = listTypeInitIterator(o,direction == LIST_HEAD ? -1 : 0,direction);
    listTypeEntry entry;
    long llen = listTypeLength(o);
    long index = 0, matches = 0, matchindex = -1, arraylen = 0;

    /* 使用迭代器在列表中遍历查询 */
    while (listTypeNext(li,&entry) && (maxlen == 0 || index < maxlen)) {
        if (listTypeEqual(&entry,ele)) {
            matches++;
            matchindex = (direction == LIST_TAIL) ? index : llen - index - 1;
            if (matches >= rank) {
                if (arraylenptr) {
                    arraylen++;
                    addReplyLongLong(c,matchindex);
                    if (count && matches-rank+1 >= count) break;
                } else {
                    break;
                }
            }
        }
        index++;
        matchindex = -1; /* '-1' 用于记录我们没有匹配到任何一个元素 */
    }
    listTypeReleaseIterator(li);

    /* Reply to the client. Note that arraylenptr is not NULL only if
     * the COUNT option was selected. */

    /* 回复客户端。请注意，只有在选择了 count 选项的情况下，
     * arraylenptr 才不会是 NULL. */
    if (arraylenptr != NULL) {
        setDeferredArrayLen(c,arraylenptr,arraylen);
    } else {
        if (matchindex != -1)
            addReplyLongLong(c,matchindex);
        else
            addReply(c,shared.null[c->resp]);
    }
}

/* LREM <key> <count> <element> */
/* 处理 LREM 命令的函数 */
void lremCommand(client *c) {
    robj *subject, *obj;
    obj = c->argv[3];
    long toremove;
    long removed = 0;

    /* 获取输入参数 count */
    if ((getLongFromObjectOrReply(c, c->argv[2], &toremove, NULL) != C_OK))
        return;

    /* 获取 key 对应的对象 */
    subject = lookupKeyWriteOrReply(c,c->argv[1],shared.czero);
    if (subject == NULL || checkType(c,subject,OBJ_LIST)) return;

    listTypeIterator *li;
    /* 根据 count 正负情况初始化迭代器，
     * 负数则初始化在列表尾部，向头部扫描，正数则初始化在列表头部，向尾部扫描 */
    if (toremove < 0) {
        toremove = -toremove;
        li = listTypeInitIterator(subject,-1,LIST_HEAD);
    } else {
        li = listTypeInitIterator(subject,0,LIST_TAIL);
    }

    listTypeEntry entry;

    /* 遍历列表查找 element 并删除 */
    while (listTypeNext(li,&entry)) {
        if (listTypeEqual(&entry,obj)) {
            listTypeDelete(li, &entry);
            server.dirty++;
            removed++;

            /* 满足要删除的元素数量，退出循环 */
            if (toremove && removed == toremove) break;
        }
    }

    /* 释放迭代器 */
    listTypeReleaseIterator(li);

    /* 有元素被删除 */
    if (removed) {
        /* 发送 key 被修改信号，发送事件通知 */
        signalModifiedKey(c,c->db,c->argv[1]);
        notifyKeyspaceEvent(NOTIFY_LIST,"lrem",c->argv[1],c->db->id);
    }

    /* 列表长度为0，则将它从数据库中删除 */
    if (listTypeLength(subject) == 0) {
        dbDelete(c->db,c->argv[1]);
        notifyKeyspaceEvent(NOTIFY_GENERIC,"del",c->argv[1],c->db->id);
    }

    /* 回复客户端删除元素个数 */
    addReplyLongLong(c,removed);
}

/* 实现 lmove 命令中向目的列表(destination)插入元素的函数 */
void lmoveHandlePush(client *c, robj *dstkey, robj *dstobj, robj *value,
                     int where) {
    /* Create the list if the key does not exist */
    /* 如果 key 不存在则创建列表 */
    if (!dstobj) {
        dstobj = createQuicklistObject();
        quicklistSetOptions(dstobj->ptr, server.list_max_listpack_size,
                            server.list_compress_depth);
        dbAdd(c->db,dstkey,dstobj);
    }
    signalModifiedKey(c,c->db,dstkey);

    /* 向列表中插入元素 */
    listTypePush(dstobj,value,where);
    notifyKeyspaceEvent(NOTIFY_LIST,
                        where == LIST_HEAD ? "lpush" : "rpush",
                        dstkey,
                        c->db->id);
    /* Always send the pushed value to the client. */
    /* 向客户端返回插入的元素值 */
    addReplyBulk(c,value);
}

/* 获取 lmove 命令中用户输入的列表位置信息 */
int getListPositionFromObjectOrReply(client *c, robj *arg, int *position) {

    /* 解析 arg 中的字符串 */
    if (strcasecmp(arg->ptr,"right") == 0) {
        *position = LIST_TAIL;
    } else if (strcasecmp(arg->ptr,"left") == 0) {
        *position = LIST_HEAD;
    } else {
        addReplyErrorObject(c,shared.syntaxerr);
        return C_ERR;
    }
    return C_OK;
}

/* 从参数 position 中获取位置信息并返回一个 Redis 共享对象 */
robj *getStringObjectFromListPosition(int position) {
    if (position == LIST_HEAD) {
        return shared.left;
    } else {
        // LIST_TAIL
        return shared.right;
    }
}

/* lmove，rpoplpush 指令的通用实现函数 */
void lmoveGenericCommand(client *c, int wherefrom, int whereto) {
    robj *sobj, *value;

    /* 获取输入参数 source 对应的对象 */
    if ((sobj = lookupKeyWriteOrReply(c,c->argv[1],shared.null[c->resp]))
        == NULL || checkType(c,sobj,OBJ_LIST)) return;

    /* source 列表长度为0 */
    if (listTypeLength(sobj) == 0) {
        /* This may only happen after loading very old RDB files. Recent
         * versions of Redis delete keys of empty lists. */

        /* 这可能只在加载非常旧的 RDB 文件后发生，
         * 因为新版本的 Redis 会删除列表为空的 key */
        addReplyNull(c);
    } else {

        /* 获取输入参数 destination 对应的对象 */
        robj *dobj = lookupKeyWrite(c->db,c->argv[2]);
        robj *touchedkey = c->argv[1];

        if (checkType(c,dobj,OBJ_LIST)) return;
        value = listTypePop(sobj,wherefrom);
        serverAssert(value); /* assertion for valgrind (avoid NPD) */
        lmoveHandlePush(c,c->argv[2],dobj,value,whereto);

        /* listTypePop returns an object with its refcount incremented */
        /* listTypePop 返回一个对象（指上文的 value），其 refcount 会增加（+1），
         * 所以在这里使用 decrRefCount 减少其被引用计数 refcount（-1）。
         * 这里其实是用于释放掉 value，因为 refcount 为1时调用则会释放对象（因为减1后会为0，即已不再被引用） */
        decrRefCount(value);

        /* Delete the source list when it is empty */
        /* source 列表若为空，则删除 source 列表 */
        notifyKeyspaceEvent(NOTIFY_LIST,
                            wherefrom == LIST_HEAD ? "lpop" : "rpop",
                            touchedkey,
                            c->db->id);
        if (listTypeLength(sobj) == 0) {
            dbDelete(c->db,touchedkey);
            notifyKeyspaceEvent(NOTIFY_GENERIC,"del",
                                touchedkey,c->db->id);
        }
        signalModifiedKey(c,c->db,touchedkey);
        server.dirty++;
        if (c->cmd->proc == blmoveCommand) {
            rewriteClientCommandVector(c,5,shared.lmove,
                                       c->argv[1],c->argv[2],c->argv[3],c->argv[4]);
        } else if (c->cmd->proc == brpoplpushCommand) {
            rewriteClientCommandVector(c,3,shared.rpoplpush,
                                       c->argv[1],c->argv[2]);
        }
    }
}

/* LMOVE <source> <destination> (LEFT|RIGHT) (LEFT|RIGHT) */
/* 处理 lmove 命令的函数 */
void lmoveCommand(client *c) {
    int wherefrom, whereto;
    if (getListPositionFromObjectOrReply(c,c->argv[3],&wherefrom)
        != C_OK) return;
    if (getListPositionFromObjectOrReply(c,c->argv[4],&whereto)
        != C_OK) return;
    lmoveGenericCommand(c, wherefrom, whereto);
}

/* This is the semantic of this command:
 *  RPOPLPUSH srclist dstlist:
 *    IF LLEN(srclist) > 0
 *      element = RPOP srclist
 *      LPUSH dstlist element
 *      RETURN element
 *    ELSE
 *      RETURN nil
 *    END
 *  END
 *
 * The idea is to be able to get an element from a list in a reliable way
 * since the element is not just returned but pushed against another list
 * as well. This command was originally proposed by Ezra Zygmuntowicz.
 */

/* 这是此命令的语义。
 *  RPOPLPUSH srclist dstlist:
 *    IF LLEN(srclist) > 0
 *      element = RPOP srclist
 *      LPUSH dstlist element
 *      RETURN element
 *    ELSE
 *      RETURN nil
 *    END
 *  END
 *
 * 我们的想法是能够以一种可靠的方式从一个列表中获得一个元素
 * 因为这个元素不仅仅是被返回，而且还被加入到另一个列表中。
 * 这个命令最初是由Ezra Zygmuntowicz提出的。
 * （目前该命令已过时，可以由可选参数更多的 lmove 代替）
 */
void rpoplpushCommand(client *c) {
    lmoveGenericCommand(c, LIST_TAIL, LIST_HEAD);
}

/*-----------------------------------------------------------------------------
 * Blocking POP operations
 *----------------------------------------------------------------------------*/

/* This is a helper function for handleClientsBlockedOnKeys(). Its work
 * is to serve a specific client (receiver) that is blocked on 'key'
 * in the context of the specified 'db', doing the following:
 *
 * 1) Provide the client with the 'value' element or a range of elements.
 *    We will do the pop in here and caller does not need to bother the return.
 * 2) If the dstkey is not NULL (we are serving a BLMOVE) also push the
 *    'value' element on the destination list (the "push" side of the command).
 * 3) Propagate the resulting BRPOP, BLPOP, BLMPOP and additional xPUSH if any into
 *    the AOF and replication channel.
 *
 * The argument 'wherefrom' is LIST_TAIL or LIST_HEAD, and indicates if the
 * 'value' element was popped from the head (BLPOP) or tail (BRPOP) so that
 * we can propagate the command properly.
 *
 * The argument 'whereto' is LIST_TAIL or LIST_HEAD, and indicates if the
 * 'value' element is to be pushed to the head or tail so that we can
 * propagate the command properly.
 *
 * 'deleted' is an optional output argument to get an indication
 * if the key got deleted by this function. */

/* 这是 handleClientsBlockedOnKeys() 的一个辅助函数。
 * 它的工作是为一个特定的客户端（接收者）提供服务，
 * 该客户端在指定的数据库的上下文中被阻塞在 "key" 上，该函数负责做以下工作：
 *
 * 1) 向客户端提供 'value' 元素或一系列元素。
 * 我们将在这里做 pop，调用者不需要理会返回的问题。
 * 2) 如果 dstkey 不为 NULL(我们是给 BLMOVE 命令提供的服务)，
 * 要将元素插入到目标列表上（插入到命令指定的一端）。
 * 3) 将产生的 BRPOP、BLPOP、BLMPOP 和额外的 xPUSH（如果有）传播到
 * AOF 和主从服务器之间的通道。
 *
 * 参数 'wherefrom' 是 LIST_TAIL 或 LIST_HEAD，
 * 表明元素是从头部 (BLPOP) 还是尾部 (BRPOP) 弹出的，
 * 以便于我们可以正确地传播该命令。
 *
 * 参数 'whereto' 是 LIST_TAIL 或 LIST_HEAD，
 * 并表明元素是否要被插入到头部或尾部。
 * 以便于我们可以正确地传播该命令。
 *
 * 'deleted' 是一个可选的输出参数，含义为是否有 key 被删除（有为1，没有为0）*/
void serveClientBlockedOnList(client *receiver, robj *o, robj *key, robj *dstkey, redisDb *db, int wherefrom, int whereto, int *deleted)
{
    robj *argv[5];
    robj *value = NULL;

    if (deleted) *deleted = 0;

    if (dstkey == NULL) {
        /* Propagate the [LR]POP operation. */
        /* 传播 [LR]POP 命令 */
        argv[0] = (wherefrom == LIST_HEAD) ? shared.lpop :
                                             shared.rpop;
        argv[1] = key;

        if (receiver->lastcmd->proc == blmpopCommand) {
            /* Propagate the [LR]POP COUNT operation. */
            /* 传播 [LR]POP COUNT 命令 */
            long count = receiver->bpop.count;
            serverAssert(count > 0);
            long llen = listTypeLength(o);
            serverAssert(llen > 0);

            argv[2] = createStringObjectFromLongLong((count > llen) ? llen : count);

            /* 传播命令 */
            alsoPropagate(db->id, argv, 3, PROPAGATE_AOF|PROPAGATE_REPL);
            decrRefCount(argv[2]);

            /* Pop a range of elements in a nested arrays way. */
            /* 弹出一系列元素，并以嵌套数组的方式回复给客户端 */
            listPopRangeAndReplyWithKey(receiver, o, key, wherefrom, count, deleted);
            return;
        }

        /* 传播命令 */
        alsoPropagate(db->id, argv, 2, PROPAGATE_AOF|PROPAGATE_REPL);

        /* BRPOP/BLPOP */
        value = listTypePop(o, wherefrom);
        serverAssert(value != NULL);

        addReplyArrayLen(receiver,2);
        addReplyBulk(receiver,key);
        addReplyBulk(receiver,value);

        /* Notify event. */
        /* 发送事件通知 */
        char *event = (wherefrom == LIST_HEAD) ? "lpop" : "rpop";
        notifyKeyspaceEvent(NOTIFY_LIST,event,key,receiver->db->id);
    } else {
        /* BLMOVE */
        robj *dstobj =
            lookupKeyWrite(receiver->db,dstkey);
        if (!(dstobj &&
             checkType(receiver,dstobj,OBJ_LIST)))
        {
            value = listTypePop(o, wherefrom);
            serverAssert(value != NULL);

            lmoveHandlePush(receiver,dstkey,dstobj,value,whereto);
            /* Propagate the LMOVE/RPOPLPUSH operation. */
            /* 传播 LMOVE/RPOPPUSH 命令 */
            int isbrpoplpush = (receiver->lastcmd->proc == brpoplpushCommand);
            argv[0] = isbrpoplpush ? shared.rpoplpush : shared.lmove;
            argv[1] = key;
            argv[2] = dstkey;
            argv[3] = getStringObjectFromListPosition(wherefrom);
            argv[4] = getStringObjectFromListPosition(whereto);
            alsoPropagate(db->id,argv,(isbrpoplpush ? 3 : 5),PROPAGATE_AOF|PROPAGATE_REPL);

            /* Notify event ("lpush" or "rpush" was notified by lmoveHandlePush). */

            /* 发送事件通知，"lpush "或 "rpush "在调用 lmoveHandlePush 函数时已经被通知了，
             * 所以我们这里只需要通知 "rpop" 或 "lpop" */
            notifyKeyspaceEvent(NOTIFY_LIST,wherefrom == LIST_TAIL ? "rpop" : "lpop",
                                key,receiver->db->id);
        }
    }

    if (value) decrRefCount(value);

    /* 列表长度为0，则将它从数据库中删除 */
    if (listTypeLength(o) == 0) {
        if (deleted) *deleted = 1;

        dbDelete(receiver->db, key);
        notifyKeyspaceEvent(NOTIFY_GENERIC, "del", key, receiver->db->id);
    }
    /* We don't call signalModifiedKey() as it was already called
     * when an element was pushed on the list. */
    /* 我们不调用 signalModifiedKey()，
     * 因为当一个元素被插入到列表上时，它已经被调用了。*/
}

/* Blocking RPOP/LPOP/LMPOP
 *
 * 'numkeys' is the number of keys.
 * 'timeout_idx' parameter position of block timeout.
 * 'where' LIST_HEAD for LEFT, LIST_TAIL for RIGHT.
 * 'count' is the number of elements requested to pop, or -1 for plain single pop.
 *
 * When count is -1, a reply of a single bulk-string will be used.
 * When count > 0, an array reply will be used. */

/* Blocking RPOP/LPOP/LMPOP
 *
 * 'numkeys' 是 key 的数量。
 * 'timeout_idx' 是 timeout（超时时间）参数的位置。
 * 'where' LIST_HEAD 代表 LEFT，LIST_TAIL 代表 RIGHT。
 * 'count' 是要求弹出的元素数，或者 -1 为普通的单个弹出。
 *
 * 当 count 为 -1 时，将使用单个字符串回复。
 * 当 count > 0 时，将使用一个数组回复。 */
void blockingPopGenericCommand(client *c, robj **keys, int numkeys, int where, int timeout_idx, long count) {
    robj *o;
    robj *key;
    mstime_t timeout;
    int j;

    /* 获取超时时间(这里输入的单位是秒，但最终都会转换为毫秒) */
    if (getTimeoutFromObjectOrReply(c,c->argv[timeout_idx],&timeout,UNIT_SECONDS)
        != C_OK) return;

    /* Traverse all input keys, we take action only based on one key. */
    /* 遍历所有输入的 key，但我们只根据输入顺序从左到右的第一个不为空的 key 来操作 */
    for (j = 0; j < numkeys; j++) {
        key = keys[j];
        o = lookupKeyWrite(c->db, key);

        /* Non-existing key, move to next key. */
        /* key 不存在，跳过到下一个 key */
        if (o == NULL) continue;

        /* 检查对象类型 */
        if (checkType(c, o, OBJ_LIST)) return;

        long llen = listTypeLength(o);
        /* Empty list, move to next key. */
        /* 列表为空，跳过到下一个 key */
        if (llen == 0) continue;

        if (count != -1) {
            /* BLMPOP, non empty list, like a normal [LR]POP with count option.
             * The difference here we pop a range of elements in a nested arrays way. */

            /* BLMPOP，非空列表，它的功能就像普通的 [LR]POP 加上了计数（count）选项。
             * 不同的是，这里我们以嵌套数组的方式向客户端回复一系列被弹出的元素。*/
            listPopRangeAndReplyWithKey(c, o, key, where, count, NULL);

            /* Replicate it as [LR]POP COUNT. */
            /* 将命令重写为 [LR]POP COUNT 的形式用于传播 */
            robj *count_obj = createStringObjectFromLongLong((count > llen) ? llen : count);
            rewriteClientCommandVector(c, 3,
                                       (where == LIST_HEAD) ? shared.lpop : shared.rpop,
                                       key, count_obj);
            decrRefCount(count_obj);
            return;
        }

        /* Non empty list, this is like a normal [LR]POP. */
        /* 非空列表，类似一个普通的 [LR]POP 操作 */
        robj *value = listTypePop(o,where);
        serverAssert(value != NULL);

        addReplyArrayLen(c,2);
        addReplyBulk(c,key);
        addReplyBulk(c,value);
        decrRefCount(value);
        listElementsRemoved(c,key,where,o,1,NULL);

        /* Replicate it as an [LR]POP instead of B[LR]POP. */
        /* 将命令重写为 [LR]POP 而不是 B[LR]POP 用于传播 */
        rewriteClientCommandVector(c,2,
            (where == LIST_HEAD) ? shared.lpop : shared.rpop,
            key);
        return;
    }

    /* If we are not allowed to block the client, the only thing
     * we can do is treating it as a timeout (even with timeout 0). */

    /* 如果我们不允许阻塞客户端，我们唯一能做的就是把它当作一个超时（即使 timeout 为0） */
    if (c->flags & CLIENT_DENY_BLOCKING) {
        /* 返回空数组 */
        addReplyNullArray(c);
        return;
    }

    /* If the keys do not exist we must block */
    /* 如果所有 key 都不存在则将所有 key 进行阻塞 */
    struct blockPos pos = {where};
    blockForKeys(c,BLOCKED_LIST,keys,numkeys,count,timeout,NULL,&pos,NULL);
}

/* BLPOP <key> [<key> ...] <timeout> */
/* 处理 blpop 命令的函数 */
void blpopCommand(client *c) {
    blockingPopGenericCommand(c,c->argv+1,c->argc-2,LIST_HEAD,c->argc-1,-1);
}

/* BRPOP <key> [<key> ...] <timeout> */
/* 处理 brpop 命令的函数 */
void brpopCommand(client *c) {
    blockingPopGenericCommand(c,c->argv+1,c->argc-2,LIST_TAIL,c->argc-1,-1);
}

/* blmove 命令的通用实现函数 */
void blmoveGenericCommand(client *c, int wherefrom, int whereto, mstime_t timeout) {

    /* 获取 source 对应的对象 */
    robj *key = lookupKeyWrite(c->db, c->argv[1]);
    if (checkType(c,key,OBJ_LIST)) return;

    /* 如果 source 为空 */
    if (key == NULL) {
        if (c->flags & CLIENT_DENY_BLOCKING) {
            /* Blocking against an empty list when blocking is not allowed
             * returns immediately. */
            
            /* 在客户端不允许阻塞的情况下，立即返回 */
            addReplyNull(c);
        } else {
            /* The list is empty and the client blocks. */
            /* 阻塞客户端 */
            struct blockPos pos = {wherefrom, whereto};
            blockForKeys(c,BLOCKED_LIST,c->argv + 1,1,-1,timeout,c->argv[2],&pos,NULL);
        }
    } else {
        /* The list exists and has elements, so
         * the regular lmoveCommand is executed. */
        /* source 不为空，所以可以执行常规的 lmove 命令 */
        serverAssertWithInfo(c,key,listTypeLength(key) > 0);
        lmoveGenericCommand(c,wherefrom,whereto);
    }
}

/* BLMOVE <source> <destination> (LEFT|RIGHT) (LEFT|RIGHT) <timeout> */
/* 处理 blmove 命令的函数 */
void blmoveCommand(client *c) {
    mstime_t timeout;
    int wherefrom, whereto;

    /* 解析输入参数中的位置和超时时间 */
    if (getListPositionFromObjectOrReply(c,c->argv[3],&wherefrom)
        != C_OK) return;
    if (getListPositionFromObjectOrReply(c,c->argv[4],&whereto)
        != C_OK) return;
    if (getTimeoutFromObjectOrReply(c,c->argv[5],&timeout,UNIT_SECONDS)
        != C_OK) return;
    
    /* 调用 blmoveGenericCommand 函数实现 blmove 具体操作 */
    blmoveGenericCommand(c,wherefrom,whereto,timeout);
}

/* BRPOPLPUSH <source> <destination> <timeout> */
/* 处理 BRPOPLPUSH 命令的函数 */ 
void brpoplpushCommand(client *c) {
    mstime_t timeout;
    if (getTimeoutFromObjectOrReply(c,c->argv[3],&timeout,UNIT_SECONDS)
        != C_OK) return;
    blmoveGenericCommand(c, LIST_TAIL, LIST_HEAD, timeout);
}

/* LMPOP/BLMPOP
 *
 * 'numkeys_idx' parameter position of key number.
 * 'is_block' this indicates whether it is a blocking variant. */

/* lmpop/blmpop
 *
 * 'numkeys_idx' 表示 numkeys 的参数位置。
 * 'is_block' 这表明它是否是阻塞的（即使用 blmpop 命令）。 */
void lmpopGenericCommand(client *c, int numkeys_idx, int is_block) {
    long j;
    long numkeys = 0;      /* Number of keys. */
    int where = 0;         /* HEAD for LEFT, TAIL for RIGHT. */
    long count = -1;       /* Reply will consist of up to count elements, depending on the list's length. */

    /* Parse the numkeys. */
    /* 解析 numkeys */
    if (getRangeLongFromObjectOrReply(c, c->argv[numkeys_idx], 1, LONG_MAX,
                                      &numkeys, "numkeys should be greater than 0") != C_OK)
        return;

    /* Parse the where. where_idx: the index of where in the c->argv. */
    /* where_idx：where 在输入参数中的索引（或者说下标/位置） */
    long where_idx = numkeys_idx + numkeys + 1;
    if (where_idx >= c->argc) {
        /* 语法错误 */
        addReplyErrorObject(c, shared.syntaxerr);
        return;
    }

    /* 获取弹出元素位置 (where) */
    if (getListPositionFromObjectOrReply(c, c->argv[where_idx], &where) != C_OK)
        return;

    /* Parse the optional arguments. */
    /* 解析可选参数 */
    for (j = where_idx + 1; j < c->argc; j++) {
        char *opt = c->argv[j]->ptr;
        int moreargs = (c->argc - 1) - j;

        if (count == -1 && !strcasecmp(opt, "COUNT") && moreargs) {
            j++;
            if (getRangeLongFromObjectOrReply(c, c->argv[j], 1, LONG_MAX,
                                              &count,"count should be greater than 0") != C_OK)
                return;
        } else {
            addReplyErrorObject(c, shared.syntaxerr);
            return;
        }
    }

    if (count == -1) count = 1;

    if (is_block) {
        /* BLOCK. We will handle CLIENT_DENY_BLOCKING flag in blockingPopGenericCommand. */
        /* 阻塞版本（blmpop）。关于 CLIENT_DENY_BLOCKING 标志我们会在 blockingPopGenericCommand 函数中进行处理。 */
        blockingPopGenericCommand(c, c->argv+numkeys_idx+1, numkeys, where, 1, count);
    } else {
        /* NON-BLOCK */
        /* 非阻塞版本（lmpop）*/
        mpopGenericCommand(c, c->argv+numkeys_idx+1, numkeys, where, count);
    }
}

/* LMPOP numkeys [<key> ...] LEFT|RIGHT [COUNT count] */
/* 处理 lmpop 命令的函数 */
void lmpopCommand(client *c) {
    lmpopGenericCommand(c, 1, 0);
}

/* BLMPOP timeout numkeys [<key> ...] LEFT|RIGHT [COUNT count] */
/* 处理 blmpop 命令的函数 */
void blmpopCommand(client *c) {
    lmpopGenericCommand(c, 2, 1);
}
