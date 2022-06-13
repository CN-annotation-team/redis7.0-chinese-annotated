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
 * Set Commands
 *----------------------------------------------------------------------------*/

void sunionDiffGenericCommand(client *c, robj **setkeys, int setnum,
                              robj *dstkey, int op);

/* Factory method to return a set that *can* hold "value". When the object has
 * an integer-encodable value, an intset will be returned. Otherwise a regular
 * hash table. */
/* 工厂方法，返回一个能容纳 "value" 的集合。当对象有一个可编码成整数编码的值时，
 * 将返回一个intset（整数集合）。否则就是一个普通的 hashtable（哈希表）。 */
robj *setTypeCreate(sds value) {
    /* 如果 value 能转换为 longlong 型的整数，说明可以使用整数编码，则创建一个整数集合 */
    if (isSdsRepresentableAsLongLong(value,NULL) == C_OK)
        return createIntsetObject();
    
    /* 创建一个普通的哈希表集合 */
    return createSetObject();
}

/* Add the specified value into a set.
 *
 * If the value was already member of the set, nothing is done and 0 is
 * returned, otherwise the new element is added and 1 is returned. */

/* 将指定的元素(value) 添加到一个集合里。
 *
 * 如果该值已经是该集合的成员，则不做任何事，返回0，否则将添加新元素，返回1。*/
int setTypeAdd(robj *subject, sds value) {
    long long llval;

    /* 对象编码类型为 hashtable */
    if (subject->encoding == OBJ_ENCODING_HT) {

        /* 获取对象的哈希表（集合） */
        dict *ht = subject->ptr;

        /* 将元素加入到哈希表中，若已经存在会返回 NULL */
        dictEntry *de = dictAddRaw(ht,value,NULL);
        if (de) {
            
            /* 给元素所在的 entry 设置 key 和 value */
            dictSetKey(ht,de,sdsdup(value));
            dictSetVal(ht,de,NULL);
            return 1;
        }
    } else if (subject->encoding == OBJ_ENCODING_INTSET) {
        if (isSdsRepresentableAsLongLong(value,&llval) == C_OK) {
            uint8_t success = 0;
            subject->ptr = intsetAdd(subject->ptr,llval,&success);
            if (success) {
                /* Convert to regular set when the intset contains
                 * too many entries. */

                /* 当整数集合中的元素数量超出限制值时，转换为普通（hashtable）集合 */
                size_t max_entries = server.set_max_intset_entries;

                /* limit to 1G entries due to intset internals. */
                /* 由于整数集合内部结构的原因，限制整数集合最大限制为 1073741824(1<<30) 个 元素 */
                if (max_entries >= 1<<30) max_entries = 1<<30;

                /* 整数集合元素数量超过限制 */
                if (intsetLen(subject->ptr) > max_entries)

                    /* 转换集合内部编码为 hashtable */
                    setTypeConvert(subject,OBJ_ENCODING_HT);
                return 1;
            }
        } else {
            /* Failed to get integer from object, convert to regular set. */
            /* 从对象中获取整数失败，转换内部编码为 hashtable */
            setTypeConvert(subject,OBJ_ENCODING_HT);

            /* The set *was* an intset and this value is not integer
             * encodable, so dictAdd should always work. */
            /* 该集合之前是一个整数集合，参数 value 不可被编码为整数，所以这里调用 dictAdd 总是有效的 */
            serverAssert(dictAdd(subject->ptr,sdsdup(value),NULL) == DICT_OK);
            return 1;
        }
    } else {
        serverPanic("Unknown set encoding");
    }
    return 0;
}

/* 删除集合中的元素 */
int setTypeRemove(robj *setobj, sds value) {
    long long llval;

    /* 对象编码类型为 hashtable */
    if (setobj->encoding == OBJ_ENCODING_HT) {

        /* 删除元素 */
        if (dictDelete(setobj->ptr,value) == DICT_OK) {

            /* 检查删除元素后是否能够缩容 */
            if (htNeedsResize(setobj->ptr)) dictResize(setobj->ptr);
            return 1;
        }
    
    /* 对象编码类型为 intset */
    } else if (setobj->encoding == OBJ_ENCODING_INTSET) {

        /* 检查 value 是否能转换为 longlong 型的整数，若不能则不会做任何操作（因为 intset 中不会有字符串编码的元素） */
        if (isSdsRepresentableAsLongLong(value,&llval) == C_OK) {
            int success; /* 用于记录是否删除元素成功 */

            /* 删除元素 */
            setobj->ptr = intsetRemove(setobj->ptr,llval,&success);
            if (success) return 1;
        }
    
    /* 对象类型不是集合，报错 */
    } else {
        serverPanic("Unknown set encoding");
    }
    return 0;
}

/* 检查元素是否存在集合中 */
int setTypeIsMember(robj *subject, sds value) {
    long long llval;

    /* 对象编码类型为 hashtable */ 
    if (subject->encoding == OBJ_ENCODING_HT) {
        /* 使用查找函数，找不到会返回 NULL，不为 NULL 返回 1，NULL 返回 0 */
        return dictFind((dict*)subject->ptr,value) != NULL;
    
    /* 对象编码类型为 intset */
    } else if (subject->encoding == OBJ_ENCODING_INTSET) {

         /* 检查 value 是否能转换为 longlong 型的整数，若不能则不会做任何操作（因为 intset 中不会有字符串编码的元素） */
        if (isSdsRepresentableAsLongLong(value,&llval) == C_OK) {

            /* 使用查找函数，查找成功返回 1，失败返回 0 */
            return intsetFind((intset*)subject->ptr,llval);
        }
    } else {
        serverPanic("Unknown set encoding");
    }
    return 0;
}

/* 初始化迭代器函数 */
setTypeIterator *setTypeInitIterator(robj *subject) {

    /* 分配内存空间 */
    setTypeIterator *si = zmalloc(sizeof(setTypeIterator));

    /* 迭代器指向的对象和对象编码类型 设置为 所输入的对象和对象编码类型 */
    si->subject = subject;
    si->encoding = subject->encoding;

    /* 编码类型为 hashtable */
    if (si->encoding == OBJ_ENCODING_HT) {

        /* 初始化迭代器成员 di（字典（或称哈希表）迭代器） */
        si->di = dictGetIterator(subject->ptr);
    
    /* 编码类型为 intset */
    } else if (si->encoding == OBJ_ENCODING_INTSET) {
        
        /* 初始化迭代器成员 li 为 0，li 的值代表了迭代器所在的元素在 intset 里的位置 */
        si->ii = 0;
    } else {
        serverPanic("Unknown set encoding");
    }
    return si;
}

/* 释放迭代器 */
void setTypeReleaseIterator(setTypeIterator *si) {

    /* 如果编码类型是 hashtable ，还要额外释放字典迭代器 di */
    if (si->encoding == OBJ_ENCODING_HT)
        dictReleaseIterator(si->di);
    zfree(si);
}

/* Move to the next entry in the set. Returns the object at the current
 * position.
 *
 * Since set elements can be internally be stored as SDS strings or
 * simple arrays of integers, setTypeNext returns the encoding of the
 * set object you are iterating, and will populate the appropriate pointer
 * (sdsele) or (llele) accordingly.
 *
 * Note that both the sdsele and llele pointers should be passed and cannot
 * be NULL since the function will try to defensively populate the non
 * used field with values which are easy to trap if misused.
 *
 * When there are no longer elements -1 is returned. */

/* 移动到集合中的下一个 entry。返回当前位置的对象
 *
 * 由于集合元素在内部可以被存储为 SDS字符串 或简单的整数数组，
 * setTypeNext 返回正在迭代的集合对象的编码，
 * 并将相应地填充适当的指针（sdsele）或（llele）。
 * 
 * 注意，sdsele 和 llele 指针都应该被传递，而且不能为空，
 * 该函数将尝试用一些被误用时能容易捕获到的值（是特殊的且出现概率极低的值）来防御性地填充未使用的字段。
 *
 * 当不再有元素时，将返回 -1 */
int setTypeNext(setTypeIterator *si, sds *sdsele, int64_t *llele) {

    /* 编码类型为 hashtable */
    if (si->encoding == OBJ_ENCODING_HT) {

        /* 获取下一个位置的 entry */
        dictEntry *de = dictNext(si->di);

        /* 若 de 为空即不存在下一个位置，返回 -1 */
        if (de == NULL) return -1;

        /* 获取下一个位置的元素（字符串值） */
        *sdsele = dictGetKey(de);

        /* llele 代表获取到的是整数值，编码类型为 hashtable 元素都是以字符串编码形式保存，
         * 所以这里用 -123456789 进行防御性填充 */
        *llele = -123456789; /* Not needed. Defensive. */

    /* 编码类型为 intset */
    } else if (si->encoding == OBJ_ENCODING_INTSET) {

        /* 获取下一个位置的元素 */
        if (!intsetGet(si->subject->ptr,si->ii++,llele))
            return -1;

        /* 整数集合中的元素都是整数编码，所以代表字符串值的 sdsele 用 NULL 进行防御性填充 */
        *sdsele = NULL; /* Not needed. Defensive. */
    } else {
        serverPanic("Wrong set encoding in setTypeNext");
    }

    /* 返回集合对象的编码类型 */
    return si->encoding;
}

/* The not copy on write friendly version but easy to use version
 * of setTypeNext() is setTypeNextObject(), returning new SDS
 * strings. So if you don't retain a pointer to this object you should call
 * sdsfree() against it.
 *
 * This function is the way to go for write operations where COW is not
 * an issue. */

/* setTypeNext() 的一个对写时复制不友好的，但容易使用的版本是 setTypeNextObject()，
 * 返回新的 SDS 字符串。所以如果你不保留这个对象的指针，你应该对它调用sdsfree() 来将它释放 */
sds setTypeNextObject(setTypeIterator *si) {
    int64_t intele;
    sds sdsele;
    int encoding;

    /* 调用 setTypeNext 函数获取下一个元素值以及集合编码类型 */
    encoding = setTypeNext(si,&sdsele,&intele);

    /* 根据集合编码类型选择返回值 */
    switch(encoding) {
        case -1:    return NULL;
        case OBJ_ENCODING_INTSET:
            return sdsfromlonglong(intele);
        case OBJ_ENCODING_HT:
            return sdsdup(sdsele);
        default:
            serverPanic("Unsupported encoding");
    }
    return NULL; /* just to suppress warnings */
}

/* Return random element from a non empty set.
 * The returned element can be an int64_t value if the set is encoded
 * as an "intset" blob of integers, or an SDS string if the set
 * is a regular set.
 *
 * The caller provides both pointers to be populated with the right
 * object. The return value of the function is the object->encoding
 * field of the object and is used by the caller to check if the
 * int64_t pointer or the sds pointer was populated.
 *
 * Note that both the sdsele and llele pointers should be passed and cannot
 * be NULL since the function will try to defensively populate the non
 * used field with values which are easy to trap if misused. */

/* 如果集合编码类型为 intset ，则返回的元素是一个 int64_t 值，
 * 如果集合是一个普通集合（hashtable），则返回一个 SDS字符串。
 *
 * 调用者提供两个指针，以填充正确的对象。
 * 该函数的返回值是对象的 object->encoding 字段，
 * 被调用者用它来检查 int64_t 指针或 sds 指针是否被填充。
 *
 * 注意，sdsele 和 llele 指针都应该被传递，而且不能为空，
 * 该函数将尝试用一些被误用时能容易捕获到的值（是特殊的且出现概率极低的值）来防御性地填充未使用的字段。 */
int setTypeRandomElement(robj *setobj, sds *sdsele, int64_t *llele) {

    /* 编码类型为 hashtable */
    if (setobj->encoding == OBJ_ENCODING_HT) {

        /* 在字典中获取随机一个 entry */
        dictEntry *de = dictGetFairRandomKey(setobj->ptr);

        /* 获取元素的值（字符串值） */
        *sdsele = dictGetKey(de);
        *llele = -123456789; /* Not needed. Defensive. */

    /* 编码类型为 intset */
    } else if (setobj->encoding == OBJ_ENCODING_INTSET) {

        /* 在整数集合中获取随机一个元素 */
        *llele = intsetRandom(setobj->ptr);
        *sdsele = NULL; /* Not needed. Defensive. */
    } else {
        serverPanic("Unknown set encoding");
    }
    return setobj->encoding;
}

/* 获取集合元素总数 */
unsigned long setTypeSize(const robj *subject) {
    if (subject->encoding == OBJ_ENCODING_HT) {
        return dictSize((const dict*)subject->ptr);
    } else if (subject->encoding == OBJ_ENCODING_INTSET) {
        return intsetLen((const intset*)subject->ptr);
    } else {
        serverPanic("Unknown set encoding");
    }
}

/* Convert the set to specified encoding. The resulting dict (when converting
 * to a hash table) is presized to hold the number of elements in the original
 * set. */

/* 将该集合转换为指定的编码（目前仅支持从 intset 到 hashtable 转换）
 * 由此产生的 dict（当转换为哈希表时）被预分配空间为足够容纳原始集合中的元素数量 */
void setTypeConvert(robj *setobj, int enc) {
    setTypeIterator *si;
    serverAssertWithInfo(NULL,setobj,setobj->type == OBJ_SET &&
                             setobj->encoding == OBJ_ENCODING_INTSET);

    /* 要转换成的编码类型为 hashtable */
    if (enc == OBJ_ENCODING_HT) {
        int64_t intele;
        dict *d = dictCreate(&setDictType);
        sds element;

        /* Presize the dict to avoid rehashing */
        /* 预分配空间来避免 rehashing（重哈希） */
        dictExpand(d,intsetLen(setobj->ptr));

        /* To add the elements we extract integers and create redis objects */
        /* 为了添加元素，我们提取整数并创建 redis 对象 */
        si = setTypeInitIterator(setobj);

        /* 遍历 intset */
        while (setTypeNext(si,&element,&intele) != -1) {
            
            /* 将整数编码的元素转换为字符串编码，并赋值给 element */
            element = sdsfromlonglong(intele);

            /* 断言元素加入字典中不会出现重复 */
            serverAssert(dictAdd(d,element,NULL) == DICT_OK);
        }

        /* 释放迭代器 */
        setTypeReleaseIterator(si);

        /* 修改对象编码信息 */
        setobj->encoding = OBJ_ENCODING_HT;

        /* 释放原来的 intset */
        zfree(setobj->ptr);

        /* 令对象指针指向转换完成的字典（哈希表） */
        setobj->ptr = d;
    } else {
        serverPanic("Unsupported set conversion");
    }
}

/* This is a helper function for the COPY command.
 * Duplicate a set object, with the guarantee that the returned object
 * has the same encoding as the original one.
 *
 * The resulting object always has refcount set to 1 */

/* 这是一个用于 COPY 命令的辅助函数。
 * 复制一个集合对象，并保证返回的对象具有与原始对象相同的编码类型。
 *
 * 返回的对象总是将 refcount 设置为1 */
robj *setTypeDup(robj *o) {
    robj *set;
    setTypeIterator *si;
    sds elesds;
    int64_t intobj;

    serverAssert(o->type == OBJ_SET);

    /* Create a new set object that have the same encoding as the original object's encoding */
    /* 创建一个新的集合对象，其编码与原对象的编码相同 */
    if (o->encoding == OBJ_ENCODING_INTSET) {
        intset *is = o->ptr;
        size_t size = intsetBlobLen(is);
        intset *newis = zmalloc(size);
        memcpy(newis,is,size);
        set = createObject(OBJ_SET, newis);
        set->encoding = OBJ_ENCODING_INTSET;
    } else if (o->encoding == OBJ_ENCODING_HT) {
        set = createSetObject();
        dict *d = o->ptr;
        dictExpand(set->ptr, dictSize(d));
        si = setTypeInitIterator(o);
        while (setTypeNext(si, &elesds, &intobj) != -1) {
            setTypeAdd(set, elesds);
        }
        setTypeReleaseIterator(si);
    } else {
        serverPanic("Unknown set encoding");
    }
    return set;
}

/* 处理 sadd 命令的函数 */
void saddCommand(client *c) {
    robj *set;
    int j, added = 0;

    /* 获取 key 对应的 Redis对象 */
    set = lookupKeyWrite(c->db,c->argv[1]);

    /* 检查对象类型 */
    if (checkType(c,set,OBJ_SET)) return;
    
    /* 如果对象不存在，则创建一个新的集合对象 */
    if (set == NULL) {

        /* 创建集合对象 */
        set = setTypeCreate(c->argv[2]->ptr);

        /* 将对象加入到数据库中 */
        dbAdd(c->db,c->argv[1],set);
    }

    /* 解析输入参数 member */
    for (j = 2; j < c->argc; j++) {

        /* 将元素加入到集合中，若加入成功则 added 计数增加 */
        if (setTypeAdd(set,c->argv[j]->ptr)) added++;
    }

    /* 如果 added 不为 0，则说明有元素插入集合 */
    if (added) {

        /* 发送 key 被修改信号 */
        signalModifiedKey(c,c->db,c->argv[1]);

        /* 发送事件通知 */
        notifyKeyspaceEvent(NOTIFY_SET,"sadd",c->argv[1],c->db->id);
    }

    /* 将 added 加到脏数据计数上（脏数据为缓存对于本地存储的数据变动次数） */
    server.dirty += added;

    /* 回复客户端加入集合成功的元素个数 */
    addReplyLongLong(c,added);
}

/* 处理 srem 命令的函数 */
void sremCommand(client *c) {
    robj *set;
    int j, deleted = 0, keyremoved = 0;

    /* 获取 key 对应的对象，获取后检查对象类型 */
    if ((set = lookupKeyWriteOrReply(c,c->argv[1],shared.czero)) == NULL ||
        checkType(c,set,OBJ_SET)) return;

    /* 解析输入参数 member */
    for (j = 2; j < c->argc; j++) {

        /* 删除集合中的元素 */
        if (setTypeRemove(set,c->argv[j]->ptr)) {

            /* 删除成功时 deleted 计数增加 */
            deleted++;

            /* 集合元素数量为0，即已将集合元素删空 */
            if (setTypeSize(set) == 0) {

                /* 在数据库中将该 key（对象）删除 */
                dbDelete(c->db,c->argv[1]);

                /* keyremoved = 1 代表 key 已经被删除 */
                keyremoved = 1;
                break;
            }
        }
    }

    /* 有元素被删除 */
    if (deleted) {

        /* 发送 key 被修改信号，发送事件通知 */
        signalModifiedKey(c,c->db,c->argv[1]);
        notifyKeyspaceEvent(NOTIFY_SET,"srem",c->argv[1],c->db->id);

        /* key 被删除时，再多发送一个关于删除 key 的事件通知 */
        if (keyremoved)
            notifyKeyspaceEvent(NOTIFY_GENERIC,"del",c->argv[1],
                                c->db->id);
        
        /* 将 deleted 加到脏数据上 */
        server.dirty += deleted;
    }

    /* 向客户端回复删除元素个数 */
    addReplyLongLong(c,deleted);
}

/* 处理 smove 命令的函数 */
void smoveCommand(client *c) {
    robj *srcset, *dstset, *ele;
    srcset = lookupKeyWrite(c->db,c->argv[1]);
    dstset = lookupKeyWrite(c->db,c->argv[2]);
    ele = c->argv[3];

    /* If the source key does not exist return 0 */
    /* 如果 source key 不存在则向客户端回复0并退出函数 */
    if (srcset == NULL) {
        addReply(c,shared.czero);
        return;
    }

    /* If the source key has the wrong type, or the destination key
     * is set and has the wrong type, return with an error. */
    /* 如果 source key 或 destination key 的编码类型错误，则退出函数 */
    if (checkType(c,srcset,OBJ_SET) ||
        checkType(c,dstset,OBJ_SET)) return;

    /* If srcset and dstset are equal, SMOVE is a no-op */
    /* 如果 srcset(source) 与 dstset(destination) 相等，smove 会是空操作 */
    if (srcset == dstset) {
        
        /* 我们只需要向客户端返回 srcset 中是否存在输入的元素即可（存在返回1，否则返回0） */
        addReply(c,setTypeIsMember(srcset,ele->ptr) ?
            shared.cone : shared.czero);
        return;
    }

    /* If the element cannot be removed from the src set, return 0. */
    /* 如果元素不能从 srcset 中被删除，返回0 */
    if (!setTypeRemove(srcset,ele->ptr)) {
        addReply(c,shared.czero);
        return;
    }

    /* 发送删除元素的事件通知 */
    notifyKeyspaceEvent(NOTIFY_SET,"srem",c->argv[1],c->db->id);

    /* Remove the src set from the database when empty */
    /* 如果 srcset 为空，则将它从数据库中删除 */
    if (setTypeSize(srcset) == 0) {
        dbDelete(c->db,c->argv[1]);
        notifyKeyspaceEvent(NOTIFY_GENERIC,"del",c->argv[1],c->db->id);
    }

    /* Create the destination set when it doesn't exist */
    /* 如果 dstset 不存在则为它创建集合 */
    if (!dstset) {
        dstset = setTypeCreate(ele->ptr);
        dbAdd(c->db,c->argv[2],dstset);
    }

    /* 发送 key 被修改信号 */
    signalModifiedKey(c,c->db,c->argv[1]);

    /* 脏数据 + 1 */
    server.dirty++;

    /* An extra key has changed when ele was successfully added to dstset */
    /* 当元素被成功添加到 dstset 时，destination key 发生了改变，则需要事件通知等操作（见下文） */
    if (setTypeAdd(dstset,ele->ptr)) {
        server.dirty++;
        signalModifiedKey(c,c->db,c->argv[2]);
        notifyKeyspaceEvent(NOTIFY_SET,"sadd",c->argv[2],c->db->id);
    }

    /* 回复客户端 : 1 */
    addReply(c,shared.cone);
}

/* 处理 sismember 命令的函数 */
void sismemberCommand(client *c) {
    robj *set;

    /* 获取 key 对应的对象 */
    if ((set = lookupKeyReadOrReply(c,c->argv[1],shared.czero)) == NULL ||
        checkType(c,set,OBJ_SET)) return;

    /* 查找元素是否存在于集合中，存在返回1，不存在返回0 */
    if (setTypeIsMember(set,c->argv[2]->ptr))
        addReply(c,shared.cone);
    else
        addReply(c,shared.czero);
}

/* 处理 smismemberCommand 命令的函数 */
void smismemberCommand(client *c) {
    robj *set;
    int j;

    /* Don't abort when the key cannot be found. Non-existing keys are empty
     * sets, where SMISMEMBER should respond with a series of zeros. */
    /* 当找不到 key 时不要中止。不存在的 key 是空集，
     * 在这里 SMISMEMBER 应该用与输入元素数量对应数量的0来回应 */
    set = lookupKeyRead(c->db,c->argv[1]);
    if (set && checkType(c,set,OBJ_SET)) return;

    /* 以数组形式回复客户端 */
    addReplyArrayLen(c,c->argc - 2);

    /* 解析输入参数 member */
    for (j = 2; j < c->argc; j++) {

        /* 查找元素是否存在于集合中，存在返回1，不存在返回0 */
        if (set && setTypeIsMember(set,c->argv[j]->ptr))
            addReply(c,shared.cone);
        else
            addReply(c,shared.czero);
    }
}

/* 处理 scard 命令的函数 */
void scardCommand(client *c) {
    robj *o;

    if ((o = lookupKeyReadOrReply(c,c->argv[1],shared.czero)) == NULL ||
        checkType(c,o,OBJ_SET)) return;

    /* 调用 setTypeSize 函数返回集合中的元素总数 */
    addReplyLongLong(c,setTypeSize(o));
}

/* Handle the "SPOP key <count>" variant. The normal version of the
 * command is handled by the spopCommand() function itself. */

/* How many times bigger should be the set compared to the remaining size
 * for us to use the "create new set" strategy? Read later in the
 * implementation for more info. */

/* 处理 "SPOP key <count>" 命令（输入了 count 参数）。
 * 该命令的普通版本由 spopCommand() 函数本身处理 */

/* 集合与剩余的大小相比应该大多少倍，我们才能使用 "创建新集合" 策略？请阅读后面的实现的更多信息。*/
#define SPOP_MOVE_STRATEGY_MUL 5

/* spop 命令带可选参数 count 时的具体实现函数 */ 
void spopWithCountCommand(client *c) {
    long l;
    unsigned long count, size;
    robj *set;

    /* Get the count argument */
    /* 获取输入参数 count */
    if (getPositiveLongFromObjectOrReply(c,c->argv[2],&l,NULL) != C_OK) return;
    count = (unsigned long) l;

    /* Make sure a key with the name inputted exists, and that it's type is
     * indeed a set. Otherwise, return nil */
    /* 确保输入的 key 存在，并且它的类型是一个集合。否则返回nil */
    if ((set = lookupKeyWriteOrReply(c,c->argv[1],shared.emptyset[c->resp]))
        == NULL || checkType(c,set,OBJ_SET)) return;

    /* If count is zero, serve an empty set ASAP to avoid special
     * cases later. */
    /* 如果 count 为 0，尽快提供一个空集，以避免以后出现特殊情况。*/
    if (count == 0) {
        addReply(c,shared.emptyset[c->resp]);
        return;
    }

    /* 获取集合元素总数 */
    size = setTypeSize(set);

    /* Generate an SPOP keyspace notification */
    /* 生成一个 spop 事件通知 */
    notifyKeyspaceEvent(NOTIFY_SET,"spop",c->argv[1],c->db->id);
    server.dirty += (count >= size) ? size : count;

    /* CASE 1:
     * The number of requested elements is greater than or equal to
     * the number of elements inside the set: simply return the whole set. */

    /* 第一种情况：
     * 请求的元素数量（count）大于或等于集合内的元素数量：简单地返回整个集合 */
    if (count >= size) {
        /* We just return the entire set */
        /* 返回整个集合 */
        sunionDiffGenericCommand(c,c->argv+1,1,NULL,SET_OP_UNION);

        /* Delete the set as it is now empty */
        /* 现在集合中的元素已被删空，可以将集合从数据库中删除 */
        dbDelete(c->db,c->argv[1]);
        notifyKeyspaceEvent(NOTIFY_GENERIC,"del",c->argv[1],c->db->id);

        /* Propagate this command as a DEL operation */
        /* 将该命令以 del 命令的形式进行传播（传播给 AOF 或 从服务器 ）*/
        rewriteClientCommandVector(c,2,shared.del,c->argv[1]);
        signalModifiedKey(c,c->db,c->argv[1]);
        return;
    }

    /* Case 2 and 3 require to replicate SPOP as a set of SREM commands.
     * Prepare our replication argument vector. Also send the array length
     * which is common to both the code paths. */

    /* 第二种情况和第三种情况要求将 SPOP 复制成一组 SREM 命令（用于 AOF 或 从服务器）。
     * 准备好我们的复制参数。同时发送数组长度，这是两个代码执行路径所共有的。*/
    robj *propargv[3];
    propargv[0] = shared.srem;
    propargv[1] = c->argv[1];
    addReplySetLen(c,count);

    /* Common iteration vars. */
    /* 通常的迭代变量 */
    sds sdsele;
    robj *objele;
    int encoding;
    int64_t llele;
    unsigned long remaining = size-count; /* 经过 SPOP 后剩下的元素数量 */

    /* If we are here, the number of requested elements is less than the
     * number of elements inside the set. Also we are sure that count < size.
     * Use two different strategies.
     *
     * CASE 2: The number of elements to return is small compared to the
     * set size. We can just extract random elements and return them to
     * the set. */

    /* 如果我们请求的元素数量少于集合内的元素数量，也就是 count < size. 我们会使用两种不同的策略。
     *
     * 第二种情况: 与集合的大小相比，要返回的元素数量很少。我们可以直接提取随机元素并返回到集合。*/
    if (remaining*SPOP_MOVE_STRATEGY_MUL > count) {
        while(count--) {
            /* Emit and remove. */
            encoding = setTypeRandomElement(set,&sdsele,&llele);
            if (encoding == OBJ_ENCODING_INTSET) {
                addReplyBulkLongLong(c,llele);
                objele = createStringObjectFromLongLong(llele);
                set->ptr = intsetRemove(set->ptr,llele,NULL);
            } else {
                addReplyBulkCBuffer(c,sdsele,sdslen(sdsele));
                objele = createStringObject(sdsele,sdslen(sdsele));
                setTypeRemove(set,sdsele);
            }

            /* Replicate/AOF this command as an SREM operation */
            /* 将此命令作为 SREM 命令进行 从服务器的复制 和 写入AOF */
            propargv[2] = objele;
            alsoPropagate(c->db->id,propargv,3,PROPAGATE_AOF|PROPAGATE_REPL);
            decrRefCount(objele);
        }
    } else {
    /* CASE 3: The number of elements to return is very big, approaching
     * the size of the set itself. After some time extracting random elements
     * from such a set becomes computationally expensive, so we use
     * a different strategy, we extract random elements that we don't
     * want to return (the elements that will remain part of the set),
     * creating a new set as we do this (that will be stored as the original
     * set). Then we return the elements left in the original set and
     * release it. */

     /* 第三种情况：要返回的元素数量非常多，接近于集合本身的元素总数。
      * 一段时间后，从这样一个集合中提取随机元素的计算成本会变得很高，
      * 所以我们使用不同的策略，我们提取我们不想返回的随机元素（这些元素将保持为集合的一部分），
      * 在我们这样做的时候创建一个新的集合（将作为新的原始集合存储）。
      * 然后，我们返回留在原始集合中的元素并释放它。*/

        robj *newset = NULL;

        /* Create a new set with just the remaining elements. */
        /* 创建新集合 newset 并只保存需要保留的元素 */
        /* 提取随机元素，只提取到满足需要保留的数量 */
        while(remaining--) {

            /* 随机获取集合中的一个元素 */
            encoding = setTypeRandomElement(set,&sdsele,&llele);

            /* 检查编码类型 */
            if (encoding == OBJ_ENCODING_INTSET) {

                /* 将整数元素（llele）转换为 SDS字符串 */
                sdsele = sdsfromlonglong(llele);
            } else {
                
                /* 创建一个副本 */
                sdsele = sdsdup(sdsele);
            }

            /* 如果 newset 为空则创建集合 */
            if (!newset) newset = setTypeCreate(sdsele);

            /* 将随机获取的元素加入到 newset，并将元素从原始集合中删除 */
            setTypeAdd(newset,sdsele);
            setTypeRemove(set,sdsele);
            sdsfree(sdsele);
        }

        /* Transfer the old set to the client. */
        /* 将原始集合中剩下的元素回复给客户端 */
        setTypeIterator *si;
        si = setTypeInitIterator(set);

        /* 遍历集合获取元素并回复给客户端 */
        while((encoding = setTypeNext(si,&sdsele,&llele)) != -1) {
            if (encoding == OBJ_ENCODING_INTSET) {
                addReplyBulkLongLong(c,llele);
                objele = createStringObjectFromLongLong(llele);
            } else {
                addReplyBulkCBuffer(c,sdsele,sdslen(sdsele));
                objele = createStringObject(sdsele,sdslen(sdsele));
            }

            /* Replicate/AOF this command as an SREM operation */
            /* 将此命令作为 SREM 命令进行 从服务器的复制 和 写入AOF */
            propargv[2] = objele;
            alsoPropagate(c->db->id,propargv,3,PROPAGATE_AOF|PROPAGATE_REPL);
            decrRefCount(objele);
        }
        setTypeReleaseIterator(si);

        /* Assign the new set as the key value. */
        /* 将 newset 覆盖到数据库中原来的 key 上 */
        dbOverwrite(c->db,c->argv[1],newset);
    }

    /* Don't propagate the command itself even if we incremented the
     * dirty counter. We don't want to propagate an SPOP command since
     * we propagated the command as a set of SREMs operations using
     * the alsoPropagate() API. */

    /* 即使我们增加了脏数据的计数，也不要传播命令本身。 
     * 我们不想传播 SPOP 命令，因为我们使用了 alsoPropagate() API 将命令作为一组 SREM 传播 */
    preventCommandPropagation(c);
    signalModifiedKey(c,c->db,c->argv[1]);
}

/* 处理 spop 命令的函数 */
void spopCommand(client *c) {
    robj *set, *ele;
    sds sdsele;
    int64_t llele;
    int encoding;

    /* 输入参数的数量为3 */
    if (c->argc == 3) {

        /* 参数数量为3时，可能输入了合法 count 参数，调用 spopWithCountCommand 函数进行处理 */
        spopWithCountCommand(c);
        return;
    
    /* 输入参数数量超过3 */
    } else if (c->argc > 3) {
        
        /* 向客户端回复语法错误 */
        addReplyErrorObject(c,shared.syntaxerr);
        return;
    }

    /* Make sure a key with the name inputted exists, and that it's type is
     * indeed a set */
    /* 确保存在输入的 key，并且它的类型是一个集合 */
    if ((set = lookupKeyWriteOrReply(c,c->argv[1],shared.null[c->resp]))
         == NULL || checkType(c,set,OBJ_SET)) return;

    /* Get a random element from the set */
    /* 从集合中获取随机元素 */
    encoding = setTypeRandomElement(set,&sdsele,&llele);

    /* Remove the element from the set */
    /* 从集合中删除获取到的元素 */
    if (encoding == OBJ_ENCODING_INTSET) {
        ele = createStringObjectFromLongLong(llele);
        set->ptr = intsetRemove(set->ptr,llele,NULL);
    } else {
        ele = createStringObject(sdsele,sdslen(sdsele));
        setTypeRemove(set,ele->ptr);
    }

    /* 发送事件通知 */
    notifyKeyspaceEvent(NOTIFY_SET,"spop",c->argv[1],c->db->id);

    /* Replicate/AOF this command as an SREM operation */
    /* 将此命令作为 SREM 命令进行 从服务器的复制 和 写入AOF */
    rewriteClientCommandVector(c,3,shared.srem,c->argv[1],ele);

    /* Add the element to the reply */
    /* 向客户端回复被弹出的元素 */
    addReplyBulk(c,ele);

    /* 减少 ele 被引用次数计数（-1）*/
    decrRefCount(ele);

    /* Delete the set if it's empty */
    /* 当集合为空时，从数据库中把集合删除 */
    if (setTypeSize(set) == 0) {
        dbDelete(c->db,c->argv[1]);
        notifyKeyspaceEvent(NOTIFY_GENERIC,"del",c->argv[1],c->db->id);
    }

    /* Set has been modified */
    /* 集合已被修改，发送 key 被修改信号 */
    signalModifiedKey(c,c->db,c->argv[1]);
    server.dirty++;
}

/* handle the "SRANDMEMBER key <count>" variant. The normal version of the
 * command is handled by the srandmemberCommand() function itself. */

/* How many times bigger should be the set compared to the requested size
 * for us to don't use the "remove elements" strategy? Read later in the
 * implementation for more info. */

/* 处理 “SRANDMEMBER key <count>” 命令（输入了 count 参数）。 该命令的普通版本由 srandmemberCommand() 函数本身处理。 */

/* 与请求的元素数量相比，该集合应该大多少倍让我们不要使用“移除元素”策略？ 阅读后面的代码实现获取更多信息。 */
#define SRANDMEMBER_SUB_STRATEGY_MUL 3

/* 输入了 count 参数的 srandmember 命令的具体实现函数 */
void srandmemberWithCountCommand(client *c) {
    long l;
    unsigned long count, size;
    int uniq = 1;
    robj *set;
    sds ele;
    int64_t llele;
    int encoding;

    dict *d;

    /* 获取输入参数 count */
    if (getLongFromObjectOrReply(c,c->argv[2],&l,NULL) != C_OK) return;
    if (l >= 0) {
        count = (unsigned long) l;
    } else {
        /* A negative count means: return the same elements multiple times
         * (i.e. don't remove the extracted element after every extraction). */

        /* 负数表示：多次返回相同的元素。
         * （即不要在每次提取后删除提取的元素） */
        count = -l; /* 将负数的 count 取相反数转换为正数 */
        uniq = 0; /* uniq 设为 0，表示结果不需要去重 */
    }

    /* 获取 key 对应的对象 */
    if ((set = lookupKeyReadOrReply(c,c->argv[1],shared.emptyarray))
        == NULL || checkType(c,set,OBJ_SET)) return;

    /* 获取集合中的元素总数 */
    size = setTypeSize(set);

    /* If count is zero, serve it ASAP to avoid special cases later. */
    /* 如果 count 为 0，尽快返回空数组，避免之后的特殊情况 */
    if (count == 0) {
        addReply(c,shared.emptyarray);
        return;
    }

    /* CASE 1: The count was negative, so the extraction method is just:
     * "return N random elements" sampling the whole set every time.
     * This case is trivial and can be served without auxiliary data
     * structures. This case is the only one that also needs to return the
     * elements in random order. */

    /* 第一种情况: 输入的 count 是负数，所以提取随机元素方法就是：
     * “返回N个随机元素” ，每次对整个集合进行采样。
     * 这种情况很简单，可以在没有辅助数据结构的情况下完成。
     * 这种情况是唯一需要以随机顺序返回元素的情况。 */
    if (!uniq || count == 1) {
        addReplyArrayLen(c,count);

        /* 遍历 count 次，获取 count 个随机元素 */
        while(count--) {

            /* 获取一个随机元素 */
            encoding = setTypeRandomElement(set,&ele,&llele);

            /* 根据对象编码类型确定 llele 还是 ele 保存了元素的值并回复客户端 */
            if (encoding == OBJ_ENCODING_INTSET) {
                addReplyBulkLongLong(c,llele);
            } else {
                addReplyBulkCBuffer(c,ele,sdslen(ele));
            }
        }
        return;
    }

    /* CASE 2:
     * The number of requested elements is greater than the number of
     * elements inside the set: simply return the whole set. */

    /* CASE 2:
     * 请求元素的数量大于集合内的元素数量：只需返回整个集合 */
    if (count >= size) {
        setTypeIterator *si;
        addReplyArrayLen(c,size);
        si = setTypeInitIterator(set);

        /* 遍历整个集合，回复所有元素给客户端 */
        while ((encoding = setTypeNext(si,&ele,&llele)) != -1) {
            if (encoding == OBJ_ENCODING_INTSET) {
                addReplyBulkLongLong(c,llele);
            } else {
                addReplyBulkCBuffer(c,ele,sdslen(ele));
            }
            size--;
        }
        setTypeReleaseIterator(si);
        serverAssert(size==0);
        return;
    }

    /* For CASE 3 and CASE 4 we need an auxiliary dictionary. */
    /* 对于第三种情况和第四种情况，我们需要一个辅助字典（哈希表） */
    d = dictCreate(&sdsReplyDictType);

    /* CASE 3:
     * The number of elements inside the set is not greater than
     * SRANDMEMBER_SUB_STRATEGY_MUL times the number of requested elements.
     * In this case we create a set from scratch with all the elements, and
     * subtract random elements to reach the requested number of elements.
     *
     * This is done because if the number of requested elements is just
     * a bit less than the number of elements in the set, the natural approach
     * used into CASE 4 is highly inefficient. */

    /* 第三种情况：
     * 集合内的元素个数不大于 SRANDMEMBER_SUB_STRATEGY_MUL * count
     * 
     * 在这种情况下，我们从头开始创建一个包含所有元素的集合（字典），
     * 并减去随机元素以达到请求的元素数量。
     *
     * 这样做是因为如果请求的元素数量只是比集合中的元素总数少一点，
     * 那么使用第四种情况中使用的普通方法将会是非常低效的 */
    if (count*SRANDMEMBER_SUB_STRATEGY_MUL > size) {
        setTypeIterator *si;

        /* Add all the elements into the temporary dictionary. */
        /* 将集合中所有元素加入到辅助字典中 */
        si = setTypeInitIterator(set);
        dictExpand(d, size);
        while ((encoding = setTypeNext(si,&ele,&llele)) != -1) {
            int retval = DICT_ERR;

            if (encoding == OBJ_ENCODING_INTSET) {
                retval = dictAdd(d,sdsfromlonglong(llele),NULL);
            } else {
                retval = dictAdd(d,sdsdup(ele),NULL);
            }
            serverAssert(retval == DICT_OK);
        }
        setTypeReleaseIterator(si);
        serverAssert(dictSize(d) == size);

        /* Remove random elements to reach the right count. */
        /* 在辅助字典中随机获取元素并删除，来降低字典中的元素数量直至等于 count */
        while (size > count) {
            dictEntry *de;
            de = dictGetFairRandomKey(d);
            dictUnlink(d,dictGetKey(de));
            sdsfree(dictGetKey(de));
            dictFreeUnlinkedEntry(d,de);
            size--;
        }
    }

    /* CASE 4: We have a big set compared to the requested number of elements.
     * In this case we can simply get random elements from the set and add
     * to the temporary set, trying to eventually get enough unique elements
     * to reach the specified count. */
    
    /* 第四种情况: 与要求的元素数量相比，我们有一个很大的集合。
     * 在这种情况下，我们可以简单地从集合中获取随机元素并添加到辅助字典中，用这种方法获取到指定数量的不重复元素 */
    else {
        unsigned long added = 0;
        sds sdsele;

        dictExpand(d, count);

        /* 循环获取 count 个不重复元素 */
        while (added < count) {
            encoding = setTypeRandomElement(set,&ele,&llele);
            if (encoding == OBJ_ENCODING_INTSET) {
                sdsele = sdsfromlonglong(llele);
            } else {
                sdsele = sdsdup(ele);
            }
            /* Try to add the object to the dictionary. If it already exists
             * free it, otherwise increment the number of objects we have
             * in the result dictionary. */

            /* 尝试将该对象添加到字典中。如果它已经存在就释放它，否则就增加我们在结果字典中的元素数量。*/
            if (dictAdd(d,sdsele,NULL) == DICT_OK)
                added++;
            else
                sdsfree(sdsele);
        }
    }

    /* CASE 3 & 4: send the result to the user. */
    /* 第三种情况和第四种情况：将结果回复给客户端 */
    {
        dictIterator *di;
        dictEntry *de;

        addReplyArrayLen(c,count);
        di = dictGetIterator(d);

        /* 遍历辅助字典，将辅助字典的元素回复给客户端 */
        while((de = dictNext(di)) != NULL)
            addReplyBulkSds(c,dictGetKey(de));
        dictReleaseIterator(di);
        dictRelease(d);
    }
}

/* SRANDMEMBER <key> [<count>] */
/* 处理 srandmember 命令的函数 */
void srandmemberCommand(client *c) {
    robj *set;
    sds ele;
    int64_t llele;
    int encoding;

    /* 输入参数的数量为3 */
    if (c->argc == 3) {
        
        /* 输入参数数量为3时，可能输入了合法 count 参数，调用 srandmemberWithCountCommand 函数进行具体处理 */
        srandmemberWithCountCommand(c);
        return;
    
    /* 输入参数的数量超过3 */
    } else if (c->argc > 3) {
        
        /* 向客户端回复语法错误 */
        addReplyErrorObject(c,shared.syntaxerr);
        return;
    }

    /* Handle variant without <count> argument. Reply with simple bulk string */
    /* 处理没有 <count> 参数的命令，用简单的字符串进行回复 */
    if ((set = lookupKeyReadOrReply(c,c->argv[1],shared.null[c->resp]))
        == NULL || checkType(c,set,OBJ_SET)) return;

    /* 获取一个随机元素 */
    encoding = setTypeRandomElement(set,&ele,&llele);

    /* 根据对象编码类型选择变量回复给客户端 */
    if (encoding == OBJ_ENCODING_INTSET) {
        addReplyBulkLongLong(c,llele);
    } else {
        addReplyBulkCBuffer(c,ele,sdslen(ele));
    }
}

/* qsort 自定义比较函数，根据集合元素数量从小到大排序 */
int qsortCompareSetsByCardinality(const void *s1, const void *s2) {
    if (setTypeSize(*(robj**)s1) > setTypeSize(*(robj**)s2)) return 1;
    if (setTypeSize(*(robj**)s1) < setTypeSize(*(robj**)s2)) return -1;
    return 0;
}

/* This is used by SDIFF and in this case we can receive NULL that should
 * be handled as empty sets. */

/* 这是由 SDIFF 命令使用的，在这种自定义比较函数下，
 * 我们可能收到 NULL，收到 NULL 时应该作为空集处理。
 * 该自定义比较函数根据集合元素数量从大到小排序 */
int qsortCompareSetsByRevCardinality(const void *s1, const void *s2) {
    robj *o1 = *(robj**)s1, *o2 = *(robj**)s2;
    unsigned long first = o1 ? setTypeSize(o1) : 0;
    unsigned long second = o2 ? setTypeSize(o2) : 0;

    if (first < second) return 1;
    if (first > second) return -1;
    return 0;
}

/* SINTER / SMEMBERS / SINTERSTORE / SINTERCARD
 *
 * 'cardinality_only' work for SINTERCARD, only return the cardinality
 * with minimum processing and memory overheads.
 *
 * 'limit' work for SINTERCARD, stop searching after reaching the limit.
 * Passing a 0 means unlimited.
 */

/* SINTER / SINTERSTORE / SINTERCARD
 *
 * 'cardinality_only' 只在 SINTERCARD 命令下使用，只返回元素数量并尽量减少处理和内存的开销。
 *
 * 'limit' 适用于 SINTERCARD，在达到限制值（limit）后停止搜索。
 * 若 limit 传递一个 0 意味着无限制，即需要搜索所有元素。
 */
void sinterGenericCommand(client *c, robj **setkeys,
                          unsigned long setnum, robj *dstkey,
                          int cardinality_only, unsigned long limit) {
    robj **sets = zmalloc(sizeof(robj*)*setnum);
    setTypeIterator *si;
    robj *dstset = NULL;
    sds elesds;
    int64_t intobj;
    void *replylen = NULL;
    unsigned long j, cardinality = 0;
    int encoding, empty = 0;

    /* 获取输入的所有 key */
    for (j = 0; j < setnum; j++) {
        robj *setobj = lookupKeyRead(c->db, setkeys[j]);
        if (!setobj) {
            /* A NULL is considered an empty set */
            /* 对象为 NULL 则被认为是一个空集 */
            empty += 1;
            sets[j] = NULL;
            continue;
        }

        /* 检查对象类型 */
        if (checkType(c,setobj,OBJ_SET)) {

            /* 若对象类型不为 set，则释放 sets 并退出函数 */
            zfree(sets);
            return;
        }
        sets[j] = setobj;
    }

    /* Set intersection with an empty set always results in an empty set.
     * Return ASAP if there is an empty set. */

    /* 集合与空集相交的结果总是一个空集。
     * 如果有一个空集，则尽快返回 */
    if (empty > 0) {
        zfree(sets);
        if (dstkey) {
            if (dbDelete(c->db,dstkey)) {
                signalModifiedKey(c,c->db,dstkey);
                notifyKeyspaceEvent(NOTIFY_GENERIC,"del",dstkey,c->db->id);
                server.dirty++;
            }
            addReply(c,shared.czero);
        } else if (cardinality_only) {
            addReplyLongLong(c,cardinality);
        } else {
            addReply(c,shared.emptyset[c->resp]);
        }
        return;
    }

    /* Sort sets from the smallest to largest, this will improve our
     * algorithm's performance */

    /* 将集合从小到大排序，这将提高我们的算法的性能 */
    qsort(sets,setnum,sizeof(robj*),qsortCompareSetsByCardinality);

    /* The first thing we should output is the total number of elements...
     * since this is a multi-bulk write, but at this stage we don't know
     * the intersection set size, so we use a trick, append an empty object
     * to the output list and save the pointer to later modify it with the
     * right length */

    /* 我们首先应该输出的是元素的总数。
     * 因为这是一个多元素的写法，但是在这个阶段我们不知道交集的大小，
     * 所以我们使用了一个技巧，在输出列表中附加一个空对象，并保存指针，
     * 以便以后用正确的长度来修改它。 */
    if (dstkey) {
        /* If we have a target key where to store the resulting set
         * create this key with an empty set inside */

        /* 如果我们有一个目标 key 来存储结果集，
         * 创建该 key，并给它初始化成一个空集合 */
        dstset = createIntsetObject();
    } else if (!cardinality_only) {
        replylen = addReplyDeferredLen(c);
    }

    /* Iterate all the elements of the first (smallest) set, and test
     * the element against all the other sets, if at least one set does
     * not include the element it is discarded */
    /* 遍历第一个（最小的）集合的所有元素，并在其他集合中检查是否含有最小的集合中的元素，
     * 如果至少有一个集合不包括该元素，则将其丢弃 */
    si = setTypeInitIterator(sets[0]);
    while((encoding = setTypeNext(si,&elesds,&intobj)) != -1) {
        for (j = 1; j < setnum; j++) {
            if (sets[j] == sets[0]) continue;

            /* 最小的集合编码为 intset */
            if (encoding == OBJ_ENCODING_INTSET) {
                /* intset with intset is simple... and fast */
                /* 若两集合编码类型都是 intset，在 intset 中查找元素是简单又迅速的（intset 有序且使用了二分查找） */ 
                if (sets[j]->encoding == OBJ_ENCODING_INTSET &&
                    !intsetFind((intset*)sets[j]->ptr,intobj))
                {
                    break;
                /* in order to compare an integer with an object we
                 * have to use the generic function, creating an object
                 * for this */

                /* 为了比较一个整数和一个对象，我们必须使用通用函数，将整数转换为字符串 */
                } else if (sets[j]->encoding == OBJ_ENCODING_HT) {
                    elesds = sdsfromlonglong(intobj);
                    if (!setTypeIsMember(sets[j],elesds)) {
                        sdsfree(elesds);
                        break;
                    }
                    sdsfree(elesds);
                }

            /* 最小的集合编码为 hashtable */
            } else if (encoding == OBJ_ENCODING_HT) {
                if (!setTypeIsMember(sets[j],elesds)) {
                    break;
                }
            }
        }

        /* Only take action when all sets contain the member */
        /* 只有当所有集合都包含最小集合中的成员时才进行以下操作 */
        if (j == setnum) {

            /* 使用 sintercard 命令会执行的逻辑 */
            if (cardinality_only) {
                cardinality++;

                /* We stop the searching after reaching the limit. */
                /* 在搜索到达限制值时停止 */
                if (limit && cardinality >= limit)
                    break;
            
            /* 使用 sinter 命令会执行的逻辑 */
            } else if (!dstkey) {

                /* 根据对象类型选择变量回复给客户端 */
                if (encoding == OBJ_ENCODING_HT)
                    addReplyBulkCBuffer(c,elesds,sdslen(elesds));
                else
                    addReplyBulkLongLong(c,intobj);
                cardinality++;
            
            /* 使用 sinterstore 命令会执行的逻辑 */
            } else {

                /* 若最小的集合类型为 intset，则要将整数编码的元素转换为字符串后加入目的集合(dstset) */
                if (encoding == OBJ_ENCODING_INTSET) {
                    elesds = sdsfromlonglong(intobj);
                    setTypeAdd(dstset,elesds);
                    sdsfree(elesds);
                } else {
                    setTypeAdd(dstset,elesds);
                }
            }
        }
    }

    /* 释放迭代器 */
    setTypeReleaseIterator(si);

    /* 根据不同的命令选择不同的回复 */
    /* sintercard 命令回复所扫描到的交集中的元素个数给客户端 */
    if (cardinality_only) {
        addReplyLongLong(c,cardinality);

    /* sinterstore 命令回复交集中的元素总数 */
    } else if (dstkey) {
        /* Store the resulting set into the target, if the intersection
         * is not an empty set. */

        /* 如果交集不是空集，则将结果集存储到目标集中 */
        if (setTypeSize(dstset) > 0) {
            setKey(c,c->db,dstkey,dstset,0);

            /* 回复交集大小给客户端 */
            addReplyLongLong(c,setTypeSize(dstset));
            notifyKeyspaceEvent(NOTIFY_SET,"sinterstore",
                dstkey,c->db->id);
            server.dirty++;

        /* 交集为空集，向客户端回复0，并将 dstkey 删除 */
        } else {
            addReply(c,shared.czero);
            if (dbDelete(c->db,dstkey)) {
                server.dirty++;
                signalModifiedKey(c,c->db,dstkey);
                notifyKeyspaceEvent(NOTIFY_GENERIC,"del",dstkey,c->db->id);
            }
        }
        decrRefCount(dstset);
    
    /* sinter 命令回复交集中的元素及元素的序号给客户端 */
    } else {
        setDeferredSetLen(c,replylen,cardinality);
    }

    /* 释放 sets */
    zfree(sets);
}

/* SINTER key [key ...] */
/* 处理 sinter 命令的函数 */
void sinterCommand(client *c) {
    sinterGenericCommand(c, c->argv+1,  c->argc-1, NULL, 0, 0);
}

/* SINTERCARD numkeys key [key ...] [LIMIT limit] */
/* 处理 sintercard 命令的函数 */
void sinterCardCommand(client *c) {
    long j;
    long numkeys = 0; /* Number of keys. */
    long limit = 0;   /* 0 means not limit. */

    /* 获取输入参数 numkeys */
    if (getRangeLongFromObjectOrReply(c, c->argv[1], 1, LONG_MAX,
                                      &numkeys, "numkeys should be greater than 0") != C_OK)
        return;

    /* numkeys 大于 key 的数量则报错(小于 key 的情况实际上会在下方的循环中走到 else 触发语法错误) */
    if (numkeys > (c->argc - 2)) {
        addReplyError(c, "Number of keys can't be greater than number of args");
        return;
    }

    /* 解析可选参数 limit */
    for (j = 2 + numkeys; j < c->argc; j++) {
        char *opt = c->argv[j]->ptr;
        int moreargs = (c->argc - 1) - j;

        if (!strcasecmp(opt, "LIMIT") && moreargs) {
            j++;
            if (getPositiveLongFromObjectOrReply(c, c->argv[j], &limit,
                                                 "LIMIT can't be negative") != C_OK)
                return;
        } else {
            addReplyErrorObject(c, shared.syntaxerr);
            return;
        }
    }

    sinterGenericCommand(c, c->argv+2, numkeys, NULL, 1, limit);
}

/* SINTERSTORE destination key [key ...] */
/* 处理 sinterstore 命令的函数 */
void sinterstoreCommand(client *c) {
    sinterGenericCommand(c, c->argv+2, c->argc-2, c->argv[1], 0, 0);
}

/* sunion 与 sdiff 命令通用实现函数 */
void sunionDiffGenericCommand(client *c, robj **setkeys, int setnum,
                              robj *dstkey, int op) {
    robj **sets = zmalloc(sizeof(robj*)*setnum);
    setTypeIterator *si;
    robj *dstset = NULL;
    sds ele;
    int j, cardinality = 0;
    int diff_algo = 1;
    int sameset = 0; 

    /* 获取所有输入的 key */
    for (j = 0; j < setnum; j++) {
        robj *setobj = lookupKeyRead(c->db, setkeys[j]);
        if (!setobj) {
            sets[j] = NULL;
            continue;
        }
        if (checkType(c,setobj,OBJ_SET)) {
            zfree(sets);
            return;
        }
        sets[j] = setobj;
        if (j > 0 && sets[0] == sets[j]) {
            sameset = 1; 
        }
    }

    /* Select what DIFF algorithm to use.
     *
     * Algorithm 1 is O(N*M) where N is the size of the element first set
     * and M the total number of sets.
     *
     * Algorithm 2 is O(N) where N is the total number of elements in all
     * the sets.
     *
     * We compute what is the best bet with the current input here. */

    /* 选择要使用的DIFF算法。
     *
     * 算法1是 O(N*M)，其中 N 是元素第一集合的大小，M是集合的总数。
     *
     * 算法2是 O(N)，其中 N 是所有集合中的元素总数。
     *
     * 我们在这里计算什么是当前输入情况下的最佳选择 */
    if (op == SET_OP_DIFF && sets[0] && !sameset) {
        long long algo_one_work = 0, algo_two_work = 0;

        for (j = 0; j < setnum; j++) {
            if (sets[j] == NULL) continue;

            algo_one_work += setTypeSize(sets[0]);
            algo_two_work += setTypeSize(sets[j]);
        }

        /* Algorithm 1 has better constant times and performs less operations
         * if there are elements in common. Give it some advantage. */

        /* 算法1有更好的常数时间，并且如果有共同的元素时执行的操作更少，所以我们给它增加一些被选中的优势 */
        algo_one_work /= 2;
        diff_algo = (algo_one_work <= algo_two_work) ? 1 : 2;

        if (diff_algo == 1 && setnum > 1) {
            /* With algorithm 1 it is better to order the sets to subtract
             * by decreasing size, so that we are more likely to find
             * duplicated elements ASAP. */

            /* 在算法1中，最好按降序排列要减去的集合，这样我们就更有可能尽快找到重复的元素。*/
            qsort(sets+1,setnum-1,sizeof(robj*),
                qsortCompareSetsByRevCardinality);
        }
    }

    /* We need a temp set object to store our union. If the dstkey
     * is not NULL (that is, we are inside an SUNIONSTORE operation) then
     * this set object will be the resulting object to set into the target key*/

        /* 我们需要一个临时的集合对象来存储我们的并集。
         * 如果 dstkey 不是 NULL（也就是说，我们在一个 SUNIONSTORE 操作里面），
         * 那么这个集合对象将是被设置为 dstkey 的结果对象 */
    dstset = createIntsetObject();

    /* 并运算 */
    if (op == SET_OP_UNION) {
        /* Union is trivial, just add every element of every set to the
         * temporary set. */

        /* 求并集是很简单的，只要把每个集合的每个元素都加到临时集合。*/
        for (j = 0; j < setnum; j++) {
            if (!sets[j]) continue; /* 不存在的 key 等于空集，直接跳过 */

            /* 初始化当前集合的迭代器 */
            si = setTypeInitIterator(sets[j]);

            /* 遍历当前集合并将所有元素加入到临时集合 dstset 中 */
            while((ele = setTypeNextObject(si)) != NULL) {
                if (setTypeAdd(dstset,ele)) cardinality++;
                sdsfree(ele);
            }

            /* 释放当前集合的迭代器 */
            setTypeReleaseIterator(si);
        }

    /* 差运算 */
    } else if (op == SET_OP_DIFF && sameset) {
        /* At least one of the sets is the same one (same key) as the first one, result must be empty. */
    } else if (op == SET_OP_DIFF && sets[0] && diff_algo == 1) {
        /* DIFF Algorithm 1:
         *
         * We perform the diff by iterating all the elements of the first set,
         * and only adding it to the target set if the element does not exist
         * into all the other sets.
         *
         * This way we perform at max N*M operations, where N is the size of
         * the first set, and M the number of sets. */

        /* DIFF 算法1：
         *
         * 我们通过遍历第一个集合的所有元素来进行差运算，只有当该元素不存在于所有其他集合中时，才将其加入目标集合。
         *
         * 这样我们最多执行 N*M 的操作，其中 N 是第一个集合的大小，M是目标集合的数量。*/
        si = setTypeInitIterator(sets[0]);
        while((ele = setTypeNextObject(si)) != NULL) {
            for (j = 1; j < setnum; j++) {
                if (!sets[j]) continue; /* key 不存在则相当于空集 */
                if (sets[j] == sets[0]) break; /* 集合相同 */
                if (setTypeIsMember(sets[j],ele)) break;
            }
            if (j == setnum) {
                /* There is no other set with this element. Add it. */
                /* 没有其他集合有当前元素，添加它到 dstset中 */
                setTypeAdd(dstset,ele);
                cardinality++;
            }
            sdsfree(ele);
        }
        setTypeReleaseIterator(si);
    } else if (op == SET_OP_DIFF && sets[0] && diff_algo == 2) {
        /* DIFF Algorithm 2:
         *
         * Add all the elements of the first set to the auxiliary set.
         * Then remove all the elements of all the next sets from it.
         *
         * This is O(N) where N is the sum of all the elements in every
         * set. */

        /* DIFF 算法2：
         
         * 将第一个集合的所有元素加入到辅助集合中。
         * 然后从其中删除所有下一个集合的所有元素。
         *
         * 这样的算法时间是O(N)，其中 N 是每个集合中所有元素的总和 */
        for (j = 0; j < setnum; j++) {
            if (!sets[j]) continue; /* key 不存在则相当于空集 */

            si = setTypeInitIterator(sets[j]);
            while((ele = setTypeNextObject(si)) != NULL) {
                if (j == 0) {
                    if (setTypeAdd(dstset,ele)) cardinality++;
                } else {
                    if (setTypeRemove(dstset,ele)) cardinality--;
                }
                sdsfree(ele);
            }
            setTypeReleaseIterator(si);

            /* Exit if result set is empty as any additional removal
             * of elements will have no effect. */

            /* 如果结果集是空的则退出，因为任何额外的移除元素都不会有任何效果。*/
            if (cardinality == 0) break;
        }
    }

    /* Output the content of the resulting set, if not in STORE mode */
    /* 如果不是在 STORE 模式下，则输出结果集的内容 */
    if (!dstkey) {
        addReplySetLen(c,cardinality);
        si = setTypeInitIterator(dstset);
        while((ele = setTypeNextObject(si)) != NULL) {
            addReplyBulkCBuffer(c,ele,sdslen(ele));
            sdsfree(ele);
        }
        setTypeReleaseIterator(si);
        server.lazyfree_lazy_server_del ? freeObjAsync(NULL, dstset, -1) :
                                          decrRefCount(dstset);
    } else {
        /* If we have a target key where to store the resulting set
         * create this key with the result set inside */

        /* 如果我们有一个用来存储结果集的目标 key，则创建这个 key，并将结果集给它 */
        if (setTypeSize(dstset) > 0) {
            setKey(c,c->db,dstkey,dstset,0);
            addReplyLongLong(c,setTypeSize(dstset));
            notifyKeyspaceEvent(NOTIFY_SET,
                op == SET_OP_UNION ? "sunionstore" : "sdiffstore",
                dstkey,c->db->id);
            server.dirty++;
        } else {
            addReply(c,shared.czero);
            if (dbDelete(c->db,dstkey)) {
                server.dirty++;
                signalModifiedKey(c,c->db,dstkey);
                notifyKeyspaceEvent(NOTIFY_GENERIC,"del",dstkey,c->db->id);
            }
        }
        decrRefCount(dstset);
    }
    zfree(sets);
}

/* SUNION key [key ...] */
/* 处理 sunion 命令的函数 */
void sunionCommand(client *c) {
    sunionDiffGenericCommand(c,c->argv+1,c->argc-1,NULL,SET_OP_UNION);
}

/* SUNIONSTORE destination key [key ...] */
/* 处理 sunionstore 命令的函数 */
void sunionstoreCommand(client *c) {
    sunionDiffGenericCommand(c,c->argv+2,c->argc-2,c->argv[1],SET_OP_UNION);
}

/* SDIFF key [key ...] */
/* 处理 sdiff 命令的函数 */
void sdiffCommand(client *c) {
    sunionDiffGenericCommand(c,c->argv+1,c->argc-1,NULL,SET_OP_DIFF);
}

/* SDIFFSTORE destination key [key ...] */
/* 处理 sdiffstore 命令的函数 */
void sdiffstoreCommand(client *c) {
    sunionDiffGenericCommand(c,c->argv+2,c->argc-2,c->argv[1],SET_OP_DIFF);
}

/* 处理 sscan 命令的函数 */
void sscanCommand(client *c) {
    robj *set;
    unsigned long cursor;

    /* 解析输入参数 cursor */
    if (parseScanCursorOrReply(c,c->argv[2],&cursor) == C_ERR) return;

    /* 获取 key 对应的对象，并检查对象类型 */
    if ((set = lookupKeyReadOrReply(c,c->argv[1],shared.emptyscan)) == NULL ||
        checkType(c,set,OBJ_SET)) return;

    /* 调用 scan 命令通用实现函数 */
    scanGenericCommand(c,set,cursor);
}
