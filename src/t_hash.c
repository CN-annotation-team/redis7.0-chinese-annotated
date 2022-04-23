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
#include <math.h>

/*-----------------------------------------------------------------------------
 * Hash type API
 *----------------------------------------------------------------------------*/

/* Check the length of a number of objects to see if we need to convert a
 * listpack to a real hash. Note that we only check string encoded objects
 * as their string length can be queried in constant time. */
/* 检查对象的长度，看我们是否需要将一个 listpack(紧凑列表) 转换为真正的 hashtable(哈希表)
 * 注意，我们只检查字符串编码的对象，因为它的字符串长度可以在 O(1) 时间内被查询到 */
void hashTypeTryConversion(robj *o, robj **argv, int start, int end) {
    int i;
    size_t sum = 0;

    /* 如果内部编码类型不是 listpack ,就不用转换了 */
    if (o->encoding != OBJ_ENCODING_LISTPACK) return;

    /* 使用给定的 start 到 end 范围读取 argv 参数 */
    for (i = start; i <= end; i++) {

        /* 检查对象类型是否为字符串编码，如果不是就不用继续检查长度 */
        if (!sdsEncodedObject(argv[i]))
            continue;

        /* 获取字符串长度 */
        size_t len = sdslen(argv[i]->ptr);

        /* 检查长度是否超过限制值 hash_max_listpack_value ,
         * 若超过限制则调用 hashTypeConvert 函数转换编码类型为 hashtable */
        if (len > server.hash_max_listpack_value) {
            hashTypeConvert(o, OBJ_ENCODING_HT);
            return;
        }

        /* 计算字符串编码对象总和 */
        sum += len;
    }

    /* 对字符串编码对象长度总和与目前 listpack 的大小进行加法运算，
     * 检查结果是否超出 listpack 最大安全容量限制 (默认1GB)
     * 若超出限制返回0，并转换编码为 hashtable */
    if (!lpSafeToAdd(o->ptr, sum))
        hashTypeConvert(o, OBJ_ENCODING_HT);
}

/* Get the value from a listpack encoded hash, identified by field.
 * Returns -1 when the field cannot be found. */

/* 从 listpack 编码的哈希类型对象获取 value ,
 * 如果查找不到返回 -1 */
int hashTypeGetFromListpack(robj *o, sds field,
                            unsigned char **vstr,
                            unsigned int *vlen,
                            long long *vll)
{
    unsigned char *zl, *fptr = NULL, *vptr = NULL;

    /* 断言：o 内部编码类型一定是 OBJ_ENCODING_LISTPACK ，否则中止 */
    serverAssert(o->encoding == OBJ_ENCODING_LISTPACK);

    zl = o->ptr;
    fptr = lpFirst(zl);

    /* 从头开始遍历整个 listpack 来查找 field 所在的 entry */
    if (fptr != NULL) {
        fptr = lpFind(zl, fptr, (unsigned char*)field, sdslen(field), 1);
        if (fptr != NULL) {
            /* Grab pointer to the value (fptr points to the field) */

            /* fptr 为指向 field 的指针， vptr 为指向 value 的指针 ,
             * 对于哈希类型，value 被保存在对应的 field 的下一个 entry */
            vptr = lpNext(zl, fptr);
            serverAssert(vptr != NULL);
        }
    }

    /* 找到了 value , 若 value 为字符串编码则令 vstr 指向该 value,
     * 若 value 为 整数编码则 vll 会指向 value */
    if (vptr != NULL) {
        *vstr = lpGetValue(vptr, vlen, vll);
        return 0;
    }

    return -1;
}

/* Get the value from a hash table encoded hash, identified by field.
 * Returns NULL when the field cannot be found, otherwise the SDS value
 * is returned. */
/* 从 hashtable 编码的哈希类型对象获取 value ,
 * 查找不到返回 NULL ,否则返回一个 sds 类型变量（ sds 是 redis 中的字符串，底层为 char* ） */
sds hashTypeGetFromHashTable(robj *o, sds field) {
    dictEntry *de;

    serverAssert(o->encoding == OBJ_ENCODING_HT);

    /* 在字典/哈希表 dict 中查找 field 对应的 dictEntry */
    de = dictFind(o->ptr, field);
    if (de == NULL) return NULL;
    return dictGetVal(de);
}

/* Higher level function of hashTypeGet*() that returns the hash value
 * associated with the specified field. If the field is found C_OK
 * is returned, otherwise C_ERR. The returned object is returned by
 * reference in either *vstr and *vlen if it's returned in string form,
 * or stored in *vll if it's returned as a number.
 *
 * If *vll is populated *vstr is set to NULL, so the caller
 * can always check the function return by checking the return value
 * for C_OK and checking if vll (or vstr) is NULL. */

/* hashTypeGet*() 的高级函数（我认为叫通用函数更容易理解），返回 field 对应的 value 
 * 如果查找成功，返回 C_OK ，否则为 C_ERR
 * 如果是以字符串形式返回，则字符串值和长度的引用存储在 *vstr 和 *vlen 中
 * 如果以数字形式返回，则存储在 *vll 中 (只有 listpack 会有整数编码， hashtable 依然以字符串编码保存整数）
 *
 * 如果 *vll 被赋值，*vstr 被设置为NULL ，反之亦然
 * 所以调用者可以通过检查返回值是否返回 C_OK ，返回C_OK后检查 vll（或 vstr）是否为 NULL
 */
int hashTypeGetValue(robj *o, sds field, unsigned char **vstr, unsigned int *vlen, long long *vll) {

    /* 判断内部编码类型并调用相应的函数获取 value */
    if (o->encoding == OBJ_ENCODING_LISTPACK) {
        *vstr = NULL;
        if (hashTypeGetFromListpack(o, field, vstr, vlen, vll) == 0)
            return C_OK;
    } else if (o->encoding == OBJ_ENCODING_HT) {
        sds value;
        if ((value = hashTypeGetFromHashTable(o, field)) != NULL) {
            *vstr = (unsigned char*) value;
            *vlen = sdslen(value);
            return C_OK;
        }
    } else {
        serverPanic("Unknown hash encoding");
    }
    return C_ERR;
}

/* Like hashTypeGetValue() but returns a Redis object, which is useful for
 * interaction with the hash type outside t_hash.c.
 * The function returns NULL if the field is not found in the hash. Otherwise
 * a newly allocated string object with the value is returned. */

/* 与 hashTypeGetValue() 类似，但返回一个 redisObject(Redis对象)，
 * 这对与 t_hash.c 以外的哈希类型的交互很有用
 * 如果在哈希对象中没有找到 field ，该函数返回 NULL。否则，将返回一个新分配的带有该值的字符串对象 */
robj *hashTypeGetValueObject(robj *o, sds field) {
    unsigned char *vstr;
    unsigned int vlen;
    long long vll;

    if (hashTypeGetValue(o,field,&vstr,&vlen,&vll) == C_ERR) return NULL;
    if (vstr) return createStringObject((char*)vstr,vlen);
    else return createStringObjectFromLongLong(vll);
}

/* Higher level function using hashTypeGet*() to return the length of the
 * object associated with the requested field, or 0 if the field does not
 * exist. */

/* hashTypeGet*() 的高级函数，返回与 field 相关的对象的长度。
 * 对象的长度，如果 field 不存在，则返回0。 */
size_t hashTypeGetValueLength(robj *o, sds field) {
    size_t len = 0;
    unsigned char *vstr = NULL;
    unsigned int vlen = UINT_MAX;
    long long vll = LLONG_MAX;

    if (hashTypeGetValue(o, field, &vstr, &vlen, &vll) == C_OK)
        len = vstr ? vlen : sdigits10(vll);

    return len;
}

/* Test if the specified field exists in the given hash. Returns 1 if the field
 * exists, and 0 when it doesn't. */

/* 测试指定的 field 是否存在于给定的哈希类型对象中，如果存在则返回1，不存在则返回0 */
int hashTypeExists(robj *o, sds field) {
    unsigned char *vstr = NULL;
    unsigned int vlen = UINT_MAX;
    long long vll = LLONG_MAX;

    return hashTypeGetValue(o, field, &vstr, &vlen, &vll) == C_OK;
}

/* Add a new field, overwrite the old with the new value if it already exists.
 * Return 0 on insert and 1 on update.
 *
 * By default, the key and value SDS strings are copied if needed, so the
 * caller retains ownership of the strings passed. However this behavior
 * can be effected by passing appropriate flags (possibly bitwise OR-ed):
 *
 * HASH_SET_TAKE_FIELD -- The SDS field ownership passes to the function.
 * HASH_SET_TAKE_VALUE -- The SDS value ownership passes to the function.
 *
 * When the flags are used the caller does not need to release the passed
 * SDS string(s). It's up to the function to use the string to create a new
 * entry or to free the SDS string before returning to the caller.
 *
 * HASH_SET_COPY corresponds to no flags passed, and means the default
 * semantics of copying the values if needed.
 *
 */

/* 添加一个新 field-value (在 set 中 field 和 value 是成对的，field-value 表示一个 field value 对)
 * 如果 field 已经存在，则用新值覆盖。
 * 插入新的 field-value 时返回0，更新时返回1。
 *
 * 默认情况下，如果需要 field 和 value 是返回它们的字符串副本，因此调用者保留了对所传递字符串的所有权。
 * 可以通过传递的标志 (flags) 来决定函数的执行方法。
 *
 * HASH_SET_TAKE_FIELD -- SDS field 的所有权传递给函数。
 * HASH_SET_TAKE_VALUE -- SDS value 的所有权传给了函数。
 *
 * 当这些标志被使用时，调用者不需要自己释放所传递的 SDS 字符串，
 * 函数会根据标志决定是否使用该字符串创建一个新的 entry，或在返回给调用者之前释放 SDS 字符串。
 *
 * HASH_SET_COPY 对应于没有设置任何标志，意味着默认的语义是在需要时复制 value。
 */
#define HASH_SET_TAKE_FIELD (1<<0)
#define HASH_SET_TAKE_VALUE (1<<1)
#define HASH_SET_COPY 0
int hashTypeSet(robj *o, sds field, sds value, int flags) {
    int update = 0;

    /* Check if the field is too long for listpack, and convert before adding the item.
     * This is needed for HINCRBY* case since in other commands this is handled early by
     * hashTypeTryConversion, so this check will be a NOP. */

    /* 检查 field 和 value 是否对 listpack 来说太长，并在添加前进行编码类型转换。
     * 这是在使用 HINCRBY 和 HINCRBYFLOAT 命令的情况下需要的，而在其他命令中，
     * 这个编码转换问题会被 hashTypeTryConversion 提前处理，所以这个检查将是一个空操作 */
    if (o->encoding == OBJ_ENCODING_LISTPACK) {
        if (sdslen(field) > server.hash_max_listpack_value || sdslen(value) > server.hash_max_listpack_value)
            hashTypeConvert(o, OBJ_ENCODING_HT);
    }
    
    /* 哈希对象编码类型为listpack */
    if (o->encoding == OBJ_ENCODING_LISTPACK) {
        unsigned char *zl, *fptr, *vptr;

        zl = o->ptr;
        fptr = lpFirst(zl);

        /* 从头开始遍历 listpack 找到对应的 field-value */
        if (fptr != NULL) {
            fptr = lpFind(zl, fptr, (unsigned char*)field, sdslen(field), 1);
            if (fptr != NULL) {
                /* Grab pointer to the value (fptr points to the field) */
                vptr = lpNext(zl, fptr);
                serverAssert(vptr != NULL);

                /* update 设为1表示查找成功 */
                update = 1;

                /* Replace value */
                /* 替换 value 为所要设置的值 */
                zl = lpReplace(zl, &vptr, (unsigned char*)value, sdslen(value));
            }
        }

        /* 查找失败 */
        if (!update) {
            /* Push new field/value pair onto the tail of the listpack */
            /* 在 listpack 尾部追加 field-value */
            zl = lpAppend(zl, (unsigned char*)field, sdslen(field));
            zl = lpAppend(zl, (unsigned char*)value, sdslen(value));
        }
        o->ptr = zl;

        /* Check if the listpack needs to be converted to a hash table */
        /* entry 数量超过限制的最大值，哈希编码类型需要转换为 hashtable */
        if (hashTypeLength(o) > server.hash_max_listpack_entries)
            hashTypeConvert(o, OBJ_ENCODING_HT);

      /* 哈希编码类型为 hashtable */
    } else if (o->encoding == OBJ_ENCODING_HT) {
        dictEntry *de = dictFind(o->ptr,field);

        /* 查找成功 */
        if (de) {
            
            /* 先将 dictEntry 中原来的 value 释放 */
            sdsfree(dictGetVal(de));

            /* 设置了 HASH_SET_TAKE_VALUE 标志的情况下，直接让 dictEntry 的 val 引用这个 value，
             * 并让 value = NULL ，即不再引用任何地方 */
            if (flags & HASH_SET_TAKE_VALUE) {
                dictGetVal(de) = value;
                value = NULL;
            } else {
                /* 没有设置 HASH_SET_TAKE_VALUE 标志，我们复制一个 value 并让 dicrEntry 引用这个副本 */
                dictGetVal(de) = sdsdup(value);
            }
            update = 1;

        /* 查找失败，设置 field, value 具体过程与上方查找成功类似 */
        } else {
            sds f,v;
            if (flags & HASH_SET_TAKE_FIELD) {
                f = field;
                field = NULL;
            } else {
                f = sdsdup(field);
            }
            if (flags & HASH_SET_TAKE_VALUE) {
                v = value;
                value = NULL;
            } else {
                v = sdsdup(value);
            }
            dictAdd(o->ptr,f,v);
        }
    } else {
        serverPanic("Unknown hash encoding");
    }

    /* Free SDS strings we did not referenced elsewhere if the flags
     * want this function to be responsible. */
    /* 如果有把所有权交给函数的标志，释放我们没有在引用其他地方的 SDS 字符串 */
    if (flags & HASH_SET_TAKE_FIELD && field) sdsfree(field);
    if (flags & HASH_SET_TAKE_VALUE && value) sdsfree(value);
    return update;
}

/* Delete an element from a hash.
 * Return 1 on deleted and 0 on not found. */
/* 从哈希类型对象中删除一个元素，返回1表示被删除，返回0表示未找到 */
int hashTypeDelete(robj *o, sds field) {
    int deleted = 0;

    if (o->encoding == OBJ_ENCODING_LISTPACK) {
        unsigned char *zl, *fptr;

        zl = o->ptr;
        fptr = lpFirst(zl);
        if (fptr != NULL) {
            fptr = lpFind(zl, fptr, (unsigned char*)field, sdslen(field), 1);
            if (fptr != NULL) {
                /* Delete both of the key and the value. */
                /* 将field 和 value 一并删除 */
                zl = lpDeleteRangeWithEntry(zl,&fptr,2);
                o->ptr = zl;
                deleted = 1;
            }
        }
    } else if (o->encoding == OBJ_ENCODING_HT) {
        if (dictDelete((dict*)o->ptr, field) == C_OK) {
            deleted = 1;

            /* Always check if the dictionary needs a resize after a delete. */
            /* 检查删除元素后是否需要重新分配哈希表的内存空间 */
            if (htNeedsResize(o->ptr)) dictResize(o->ptr);
        }

    } else {
        serverPanic("Unknown hash encoding");
    }
    return deleted;
}

/* Return the number of elements in a hash. */
/* 返回哈希类型对象中的元素数量 */
unsigned long hashTypeLength(const robj *o) {
    unsigned long length = ULONG_MAX;

    if (o->encoding == OBJ_ENCODING_LISTPACK) {

        /* field-value 对为一个元素，所以元素数量是 listpack 的 entry 数量除以2 */
        length = lpLength(o->ptr) / 2;
    } else if (o->encoding == OBJ_ENCODING_HT) {

        /* redis 的哈希表是渐进式 rehash 的，所以 dict 中有一新一旧的两个哈希表，
         * 所以 dictSize 计算元素数量方法是将两个哈希表的元素数量相加 */
        length = dictSize((const dict*)o->ptr);
    } else {
        serverPanic("Unknown hash encoding");
    }
    return length;
}

/* 哈希类型对象迭代器的初始化函数 */
hashTypeIterator *hashTypeInitIterator(robj *subject) {
    hashTypeIterator *hi = zmalloc(sizeof(hashTypeIterator));
    hi->subject = subject;
    hi->encoding = subject->encoding;

    if (hi->encoding == OBJ_ENCODING_LISTPACK) {
        hi->fptr = NULL;
        hi->vptr = NULL;
    } else if (hi->encoding == OBJ_ENCODING_HT) {
        hi->di = dictGetIterator(subject->ptr);
    } else {
        serverPanic("Unknown hash encoding");
    }
    return hi;
}

/* 释放哈希类型对象迭代器 */
void hashTypeReleaseIterator(hashTypeIterator *hi) {
    if (hi->encoding == OBJ_ENCODING_HT)
        dictReleaseIterator(hi->di);
    zfree(hi);
}

/* Move to the next entry in the hash. Return C_OK when the next entry
 * could be found and C_ERR when the iterator reaches the end. */

/* 移动到哈希表的下一个 entry。当找到下一个 entry 时返回C_OK，当迭代器到达终点时返回C_ERR */
int hashTypeNext(hashTypeIterator *hi) {

    /* 编码类型为 listpack */
    if (hi->encoding == OBJ_ENCODING_LISTPACK) {
        unsigned char *zl;
        unsigned char *fptr, *vptr;

        zl = hi->subject->ptr;
        fptr = hi->fptr;
        vptr = hi->vptr;

        if (fptr == NULL) {
            /* Initialize cursor */
            /* 初始化游标，即令 fptr 等于 listpack 的第一个 entry */
            serverAssert(vptr == NULL);
            fptr = lpFirst(zl);
        } else {
            /* Advance cursor */
            /* 游标前进，即令 fptr 等于 vptr 的下一个entry */
            serverAssert(vptr != NULL);
            fptr = lpNext(zl, vptr);
        }

        /* 如果到这 fptr 仍为空，则 fptr 指向了终点，返回 C_ERR */
        if (fptr == NULL) return C_ERR;

        /* Grab pointer to the value (fptr points to the field) */
        /* vptr 指向 fptr 下一个节点，即指向了 field 对应的 value */
        vptr = lpNext(zl, fptr);
        serverAssert(vptr != NULL);

        /* fptr, vptr now point to the first or next pair */
        /* 改变迭代器中的 fptr 和 vptr */
        hi->fptr = fptr;
        hi->vptr = vptr;
    
    /* 编码类型为 hashtable */
    } else if (hi->encoding == OBJ_ENCODING_HT) {
        if ((hi->de = dictNext(hi->di)) == NULL) return C_ERR;
    } else {
        serverPanic("Unknown hash encoding");
    }
    return C_OK;
}

/* Get the field or value at iterator cursor, for an iterator on a hash value
 * encoded as a listpack. Prototype is similar to `hashTypeGetFromListpack`. */

/* 在迭代器游标处获取 field 或 value ,哈希类型为 listpack 时使用*/
void hashTypeCurrentFromListpack(hashTypeIterator *hi, int what,
                                 unsigned char **vstr,
                                 unsigned int *vlen,
                                 long long *vll)
{
    serverAssert(hi->encoding == OBJ_ENCODING_LISTPACK);

    /* what & OBJ_HASH_KEY == true ,则取出 field ，否则取出 value */
    if (what & OBJ_HASH_KEY) {
        *vstr = lpGetValue(hi->fptr, vlen, vll);
    } else {
        *vstr = lpGetValue(hi->vptr, vlen, vll);
    }
}

/* Get the field or value at iterator cursor, for an iterator on a hash value
 * encoded as a hash table. Prototype is similar to
 * `hashTypeGetFromHashTable`. */

/* 在迭代器游标处获取 field 或 value ,哈希类型为 hashtable 时使用*/
sds hashTypeCurrentFromHashTable(hashTypeIterator *hi, int what) {
    serverAssert(hi->encoding == OBJ_ENCODING_HT);

    if (what & OBJ_HASH_KEY) {
        return dictGetKey(hi->de);
    } else {
        return dictGetVal(hi->de);
    }
}

/* Higher level function of hashTypeCurrent*() that returns the hash value
 * at current iterator position.
 *
 * The returned element is returned by reference in either *vstr and *vlen if
 * it's returned in string form, or stored in *vll if it's returned as
 * a number.
 *
 * If *vll is populated *vstr is set to NULL, so the caller
 * can always check the function return by checking the return value
 * type checking if vstr == NULL. */

/* hashTypeCurrent*()的高级函数，返回当前哈希迭代器位置的值 */
void hashTypeCurrentObject(hashTypeIterator *hi, int what, unsigned char **vstr, unsigned int *vlen, long long *vll) {

    /* 根据编码类型，调用不同的函数获取值 */
    if (hi->encoding == OBJ_ENCODING_LISTPACK) {
        *vstr = NULL;
        hashTypeCurrentFromListpack(hi, what, vstr, vlen, vll);
    } else if (hi->encoding == OBJ_ENCODING_HT) {
        sds ele = hashTypeCurrentFromHashTable(hi, what);
        *vstr = (unsigned char*) ele;
        *vlen = sdslen(ele);
    } else {
        serverPanic("Unknown hash encoding");
    }
}

/* Return the key or value at the current iterator position as a new
 * SDS string. */

/* 返回当前迭代器位置的 key 或 value ，返回的是一个新创建的 SDS 字符串(或者说副本) */
sds hashTypeCurrentObjectNewSds(hashTypeIterator *hi, int what) {
    unsigned char *vstr;
    unsigned int vlen;
    long long vll;

    hashTypeCurrentObject(hi,what,&vstr,&vlen,&vll);
    if (vstr) return sdsnewlen(vstr,vlen);
    return sdsfromlonglong(vll);
}

/* 在写模式下查找key，如果查找不到则创建该key */
robj *hashTypeLookupWriteOrCreate(client *c, robj *key) {
    robj *o = lookupKeyWrite(c->db,key);
    if (checkType(c,o,OBJ_HASH)) return NULL;

    if (o == NULL) {
        o = createHashObject();
        dbAdd(c->db,key,o);
    }
    return o;
}


/* 将哈希编码类型从 listpack 转化为 hashtable */
void hashTypeConvertListpack(robj *o, int enc) {
    serverAssert(o->encoding == OBJ_ENCODING_LISTPACK);

    /* enc 指定了转化类型，如果转化类型为 listpack 则什么也不做 */
    if (enc == OBJ_ENCODING_LISTPACK) {
        /* Nothing to do... */

    } else if (enc == OBJ_ENCODING_HT) {
        hashTypeIterator *hi;
        dict *dict;
        int ret;

        /* 初始化迭代器 hi 和字典(哈希表) dict */
        hi = hashTypeInitIterator(o);
        dict = dictCreate(&hashDictType);

        /* Presize the dict to avoid rehashing */
        /* 预分配内存空间，避免rehashing重新分配空间导致性能下降 */
        dictExpand(dict,hashTypeLength(o));

        /* 利用迭代器遍历 listpack */
        while (hashTypeNext(hi) != C_ERR) {
            sds key, value;

            /* 取出迭代器当前游标下的 key 和 value */
            key = hashTypeCurrentObjectNewSds(hi,OBJ_HASH_KEY);
            value = hashTypeCurrentObjectNewSds(hi,OBJ_HASH_VALUE);

            /* 将 key 和 value 加入 dict  */
            ret = dictAdd(dict, key, value);

            /* key 和 value 添加到 dict 失败，释放 key, value, hi ，并向服务器报错 */
            if (ret != DICT_OK) {
                sdsfree(key); sdsfree(value); /* Needed for gcc ASAN */
                hashTypeReleaseIterator(hi);  /* Needed for gcc ASAN */
                serverLogHexDump(LL_WARNING,"listpack with dup elements dump",
                    o->ptr,lpBytes(o->ptr));
                serverPanic("Listpack corruption detected");
            }
        }

        /* 释放迭代器内存空间 */
        hashTypeReleaseIterator(hi);

        /* 释放原来的 listpack */
        zfree(o->ptr);

        /* 修改 o 的编码类型标识为 hashtable */
        o->encoding = OBJ_ENCODING_HT;

        /* 修改 o 的 ptr 指针指向 dict */
        o->ptr = dict;
    } else {
        serverPanic("Unknown hash encoding");
    }
}

/* 哈希编码类型转换 */
void hashTypeConvert(robj *o, int enc) {
    if (o->encoding == OBJ_ENCODING_LISTPACK) {
        hashTypeConvertListpack(o, enc);
    } else if (o->encoding == OBJ_ENCODING_HT) {
        serverPanic("Not implemented");
    } else {
        serverPanic("Unknown hash encoding");
    }
}

/* This is a helper function for the COPY command.
 * Duplicate a hash object, with the guarantee that the returned object
 * has the same encoding as the original one.
 *
 * The resulting object always has refcount set to 1 */

/* 这是一个用于 COPY 命令的辅助函数。
 * 复制一个哈希对象，并保证返回的对象具有与原始对象相同的编码。
 * 返回的对象总是将 refcount(被引用次数) 设置为1 */
robj *hashTypeDup(robj *o) {
    robj *hobj;
    hashTypeIterator *hi;

    serverAssert(o->type == OBJ_HASH);

    if(o->encoding == OBJ_ENCODING_LISTPACK) {
        unsigned char *zl = o->ptr;
        size_t sz = lpBytes(zl);

        /* 为副本开辟内存空间 */
        unsigned char *new_zl = zmalloc(sz);

        /* 复制原来的 listpack */
        memcpy(new_zl, zl, sz);

        /* 创建新的哈希类型对象，并设置编码类型标识为 listpack */
        hobj = createObject(OBJ_HASH, new_zl);
        hobj->encoding = OBJ_ENCODING_LISTPACK;
    } else if(o->encoding == OBJ_ENCODING_HT){

        /* 创建副本的字典(哈希表) dict 并预分配空间 */
        dict *d = dictCreate(&hashDictType);
        dictExpand(d, dictSize((const dict*)o->ptr));

        hi = hashTypeInitIterator(o);

        /* 利用迭代器遍历源 hashtable */
        while (hashTypeNext(hi) != C_ERR) {
            sds field, value;
            sds newfield, newvalue;
            /* Extract a field-value pair from an original hash object.*/
            /* 从原哈希对象中提取一对 field-value */
            field = hashTypeCurrentFromHashTable(hi, OBJ_HASH_KEY);
            value = hashTypeCurrentFromHashTable(hi, OBJ_HASH_VALUE);

            /* 对 field, value 进行复制 */
            newfield = sdsdup(field);
            newvalue = sdsdup(value);

            /* Add a field-value pair to a new hash object. */
            /* 将 field-value 加入到新哈希对象中 */
            dictAdd(d,newfield,newvalue);
        }
        hashTypeReleaseIterator(hi);

        /* 创建新的哈希类型对象，并设置编码类型标识为 hashtable */
        hobj = createObject(OBJ_HASH, d);
        hobj->encoding = OBJ_ENCODING_HT;
    } else {
        serverPanic("Unknown hash encoding");
    }
    return hobj;
}

/* Create a new sds string from the listpack entry. */
/* 获取参数给定的 listpack 的 entry 中的值 ，并返回一个以该值创建的 sds 字符串 */
sds hashSdsFromListpackEntry(listpackEntry *e) {
    return e->sval ? sdsnewlen(e->sval, e->slen) : sdsfromlonglong(e->lval);
}

/* Reply with bulk string from the listpack entry. */
/* 将参数给定的 listpack 的 entry 中的值回复给客户端 */
void hashReplyFromListpackEntry(client *c, listpackEntry *e) {
    if (e->sval)
        addReplyBulkCBuffer(c, e->sval, e->slen);
    else
        addReplyBulkLongLong(c, e->lval);
}

/* Return random element from a non empty hash.
 * 'key' and 'val' will be set to hold the element.
 * The memory in them is not to be freed or modified by the caller.
 * 'val' can be NULL in which case it's not extracted. */

/* 从一个非空哈希类型对象中返回随机元素
 * 'key' 和 'val' 将持有该元素
 * 它们的内存不能被释放或被调用者修改。
 * 'val' 可以是 NULL，在这种情况下它不会被进行赋值 */
void hashTypeRandomElement(robj *hashobj, unsigned long hashsize, listpackEntry *key, listpackEntry *val) {
    if (hashobj->encoding == OBJ_ENCODING_HT) {

        /* 调用 dictGetFairRandomKey 随机获取一个 dictEntry */
        dictEntry *de = dictGetFairRandomKey(hashobj->ptr);
        sds s = dictGetKey(de);
        key->sval = (unsigned char*)s;
        key->slen = sdslen(s);

        /* val 不为 NULL, 要求获取 value */
        if (val) {
            sds s = dictGetVal(de);
            val->sval = (unsigned char*)s;
            val->slen = sdslen(s);
        }
    } else if (hashobj->encoding == OBJ_ENCODING_LISTPACK) {

        /* 调用 lpRandomPair 随机获取一个 field 或 field-value (若 val 不为 NULL) */
        lpRandomPair(hashobj->ptr, hashsize, key, val);
    } else {
        serverPanic("Unknown hash encoding");
    }
}


/*-----------------------------------------------------------------------------
 * Hash type commands
 *----------------------------------------------------------------------------*/

/* 处理hsetnx命令的函数 */
void hsetnxCommand(client *c) {
    robj *o;

    /* 查找 key */
    if ((o = hashTypeLookupWriteOrCreate(c,c->argv[1])) == NULL) return;

    /* key 已存在，向客户端回复0 */
    if (hashTypeExists(o, c->argv[2]->ptr)) {
        addReply(c, shared.czero);
    } else {

        /* 尝试哈希编码类型转换(该函数仅判断输入 field 和 value 长度是否大于限制) */
        hashTypeTryConversion(o,c->argv,2,3);

        /* 设置 field-value */
        hashTypeSet(o,c->argv[2]->ptr,c->argv[3]->ptr,HASH_SET_COPY);

        /* 向客户端回复1 */
        addReply(c, shared.cone);

        /* 发送数据库被修改的通知 */
        signalModifiedKey(c,c->db,c->argv[1]);

        /* 发送事件通知 */
        notifyKeyspaceEvent(NOTIFY_HASH,"hset",c->argv[1],c->db->id);

        /* 距离上次成功保存的数据变动次数 + 1 */
        server.dirty++;
    }
}

/* 处理hset命令的函数 */
void hsetCommand(client *c) {
    int i, created = 0;
    robj *o;

    /* 该命令输入参数个数必为偶数，若为奇数则输入有误，将导致报错并返回 */
    if ((c->argc % 2) == 1) {
        addReplyErrorArity(c);
        return;
    }

    /* 查找key */
    if ((o = hashTypeLookupWriteOrCreate(c,c->argv[1])) == NULL) return;
    hashTypeTryConversion(o,c->argv,2,c->argc-1);

    /* 解析输入的 field value 参数 */
    for (i = 2; i < c->argc; i += 2)

        /* 设置 field-value ，同时给记录新创建的 field-value 数量的变量 created 计数
         * 若 field-value 是新建的 hashTypeSet 返回0，所以需要前面加上 '!' 逻辑非符号转化为1 */
        created += !hashTypeSet(o,c->argv[i]->ptr,c->argv[i+1]->ptr,HASH_SET_COPY);

    /* HMSET (deprecated) and HSET return value is different. */
    /* HMSET 命令 (不推荐使用，已过时，可被HSET替代) 和 HSET 命令的返回值不相同 */
    char *cmdname = c->argv[0]->ptr;
    if (cmdname[1] == 's' || cmdname[1] == 'S') {
        /* HSET */
        /* HSET 返回值为新的 field-value 个数 */
        addReplyLongLong(c, created);
    } else {
        /* HMSET */
        /* HMSET 返回值为 'OK' */
        addReply(c, shared.ok);
    }

    signalModifiedKey(c,c->db,c->argv[1]);
    notifyKeyspaceEvent(NOTIFY_HASH,"hset",c->argv[1],c->db->id);
    server.dirty += (c->argc - 2)/2;
}

/* 处理hincrby命令的函数 */
void hincrbyCommand(client *c) {
    long long value, incr, oldvalue;
    robj *o;
    sds new;
    unsigned char *vstr;
    unsigned int vlen;

    /* 取出输入参数 increment 并赋值给 incr */
    if (getLongLongFromObjectOrReply(c,c->argv[3],&incr,NULL) != C_OK) return;
    if ((o = hashTypeLookupWriteOrCreate(c,c->argv[1])) == NULL) return;

    /* 取出哈希对象的 value */
    if (hashTypeGetValue(o,c->argv[2]->ptr,&vstr,&vlen,&value) == C_OK) {
        if (vstr) {

            /* 将哈希对象的 value 转换为 long long 并赋值给 value */
            if (string2ll((char*)vstr,vlen,&value) == 0) {

                /* value 无法转换，报错并返回 */
                addReplyError(c,"hash value is not an integer");
                return;
            }
        } /* Else hashTypeGetValue() already stored it into &value */
    } else {
        
        /* 哈希对象不存在，value 设置为默认值0 */
        value = 0;
    }

    oldvalue = value;

    /* 检查原 value 加上 incr 是否会溢出 */
    if ((incr < 0 && oldvalue < 0 && incr < (LLONG_MIN-oldvalue)) ||
        (incr > 0 && oldvalue > 0 && incr > (LLONG_MAX-oldvalue))) {
        addReplyError(c,"increment or decrement would overflow");
        return;
    }

    /* value 加上 incr */
    value += incr;
    
    /* new 是以 value 的值创建而来的字符串 */
    new = sdsfromlonglong(value);
    hashTypeSet(o,c->argv[2]->ptr,new,HASH_SET_TAKE_VALUE);
    addReplyLongLong(c,value);
    signalModifiedKey(c,c->db,c->argv[1]);
    notifyKeyspaceEvent(NOTIFY_HASH,"hincrby",c->argv[1],c->db->id);
    server.dirty++;
}

/* 处理 hincrbyfloat 命令的函数，具体实现与 hincrby 类似 */
void hincrbyfloatCommand(client *c) {
    long double value, incr;
    long long ll;
    robj *o;
    sds new;
    unsigned char *vstr;
    unsigned int vlen;

    if (getLongDoubleFromObjectOrReply(c,c->argv[3],&incr,NULL) != C_OK) return;
    if ((o = hashTypeLookupWriteOrCreate(c,c->argv[1])) == NULL) return;
    if (hashTypeGetValue(o,c->argv[2]->ptr,&vstr,&vlen,&ll) == C_OK) {
        if (vstr) {
            if (string2ld((char*)vstr,vlen,&value) == 0) {
                addReplyError(c,"hash value is not a float");
                return;
            }
        } else {
            value = (long double)ll;
        }
    } else {
        value = 0;
    }

    value += incr;
    if (isnan(value) || isinf(value)) {
        addReplyError(c,"increment would produce NaN or Infinity");
        return;
    }

    char buf[MAX_LONG_DOUBLE_CHARS];
    int len = ld2string(buf,sizeof(buf),value,LD_STR_HUMAN);
    new = sdsnewlen(buf,len);
    hashTypeSet(o,c->argv[2]->ptr,new,HASH_SET_TAKE_VALUE);
    addReplyBulkCBuffer(c,buf,len);
    signalModifiedKey(c,c->db,c->argv[1]);
    notifyKeyspaceEvent(NOTIFY_HASH,"hincrbyfloat",c->argv[1],c->db->id);
    server.dirty++;

    /* Always replicate HINCRBYFLOAT as an HSET command with the final value
     * in order to make sure that differences in float precision or formatting
     * will not create differences in replicas or after an AOF restart. */
    
    /* 始终将 HINCRBYFLOAT 作为 HSET 命令进行复制，
     * 以确保浮点数精度或格式化的差异不会在复制中或AOF重启后产生差异 */
    robj *newobj;
    newobj = createRawStringObject(buf,len);
    rewriteClientCommandArgument(c,0,shared.hset);
    rewriteClientCommandArgument(c,3,newobj);
    decrRefCount(newobj);
}

/* 查找指定的 field-value 并将 value 回复给客户端 */
static void addHashFieldToReply(client *c, robj *o, sds field) {
    if (o == NULL) {
        addReplyNull(c);
        return;
    }

    unsigned char *vstr = NULL;
    unsigned int vlen = UINT_MAX;
    long long vll = LLONG_MAX;

    if (hashTypeGetValue(o, field, &vstr, &vlen, &vll) == C_OK) {
        if (vstr) {
            addReplyBulkCBuffer(c, vstr, vlen);
        } else {
            addReplyBulkLongLong(c, vll);
        }
    } else {
        addReplyNull(c);
    }
}

/* 处理 hget 命令的函数 */
void hgetCommand(client *c) {
    robj *o;

    /* 查找 key 对应的 redis 对象 ，找到后检查对象类型是否为 hashtable */
    if ((o = lookupKeyReadOrReply(c,c->argv[1],shared.null[c->resp])) == NULL ||
        checkType(c,o,OBJ_HASH)) return;
    
    /* 调用 addHashFieldToReply 进行查找与回复 value 给客户端 */
    addHashFieldToReply(c, o, c->argv[2]->ptr);
}

/* 处理 hmget 命令的函数 */
void hmgetCommand(client *c) {
    robj *o;
    int i;

    /* Don't abort when the key cannot be found. Non-existing keys are empty
     * hashes, where HMGET should respond with a series of null bulks. */

    /* 当找不到 key 的时候不要中止，不存在的 key 即空哈希对象 ，HMGET 应该用空值来回应 */
    o = lookupKeyRead(c->db, c->argv[1]);
    if (checkType(c,o,OBJ_HASH)) return;

    /* 将以列表的形式回复客户端，这里设置列表的长度 */
    addReplyArrayLen(c, c->argc-2);

    /* 解析输入的 field 和 value 参数，并调用 addHashFieldToReply 进行查找与回复 value 给客户端 */
    for (i = 2; i < c->argc; i++) {
        addHashFieldToReply(c, o, c->argv[i]->ptr);
    }
}

/* 处理 hdel 命令的函数 */
void hdelCommand(client *c) {
    robj *o;
    int j, deleted = 0, keyremoved = 0;

    if ((o = lookupKeyWriteOrReply(c,c->argv[1],shared.czero)) == NULL ||
        checkType(c,o,OBJ_HASH)) return;

    /* 解析输入的 field 和 value 参数 */
    for (j = 2; j < c->argc; j++) {

        /* 调用 hashTypeDelete 进行删除 */
        if (hashTypeDelete(o,c->argv[j]->ptr)) {
            deleted++; /* 被删除的元素个数 + 1 */

            /* 如果哈希对象的元素已经被删除完毕，则从数据库中将该 key 删除 */
            if (hashTypeLength(o) == 0) {
                dbDelete(c->db,c->argv[1]);
                keyremoved = 1;
                break;
            }
        }
    }

    if (deleted) {
        signalModifiedKey(c,c->db,c->argv[1]);
        notifyKeyspaceEvent(NOTIFY_HASH,"hdel",c->argv[1],c->db->id);

        /* 如果 key 被删除，添加一个事件通知 */
        if (keyremoved)
            notifyKeyspaceEvent(NOTIFY_GENERIC,"del",c->argv[1],
                                c->db->id);
        server.dirty += deleted;
    }
    addReplyLongLong(c,deleted);
}

/* 处理 hlen 命令的函数 */
void hlenCommand(client *c) {
    robj *o;

    if ((o = lookupKeyReadOrReply(c,c->argv[1],shared.czero)) == NULL ||
        checkType(c,o,OBJ_HASH)) return;

    /* 回复哈希类型对象的元素数量给客户端 */
    addReplyLongLong(c,hashTypeLength(o));
}

/* 处理 hstrlen 命令的函数 */
void hstrlenCommand(client *c) {
    robj *o;

    if ((o = lookupKeyReadOrReply(c,c->argv[1],shared.czero)) == NULL ||
        checkType(c,o,OBJ_HASH)) return;
    
    /* 回复 value 的长度给客户端 */
    addReplyLongLong(c,hashTypeGetValueLength(o,c->argv[2]->ptr));
}

/* 查找迭代器对应的 value 并回复给客户端 */
static void addHashIteratorCursorToReply(client *c, hashTypeIterator *hi, int what) {
    if (hi->encoding == OBJ_ENCODING_LISTPACK) {
        unsigned char *vstr = NULL;
        unsigned int vlen = UINT_MAX;
        long long vll = LLONG_MAX;

        hashTypeCurrentFromListpack(hi, what, &vstr, &vlen, &vll);
        if (vstr)
            addReplyBulkCBuffer(c, vstr, vlen);
        else
            addReplyBulkLongLong(c, vll);
    } else if (hi->encoding == OBJ_ENCODING_HT) {
        sds value = hashTypeCurrentFromHashTable(hi, what);
        addReplyBulkCBuffer(c, value, sdslen(value));
    } else {
        serverPanic("Unknown hash encoding");
    }
}

/* hkeys, hvals, hgetall 命令的通用实现函数 */
void genericHgetallCommand(client *c, int flags) {
    robj *o;
    hashTypeIterator *hi;
    int length, count = 0;

    /* 根据 flags 选择返回空值时是返回一个空的 map 还是一个空的 array */
    robj *emptyResp = (flags & OBJ_HASH_KEY && flags & OBJ_HASH_VALUE) ?
        shared.emptymap[c->resp] : shared.emptyarray;
    if ((o = lookupKeyReadOrReply(c,c->argv[1],emptyResp))
        == NULL || checkType(c,o,OBJ_HASH)) return;

    /* We return a map if the user requested keys and values, like in the
     * HGETALL case. Otherwise to use a flat array makes more sense. */

    /* 如果用户同时请求 keys 和 vals，我们会回复一个 map ，比如使用 HGETALL 命令，
     * 否则，使用一个一维数组更好 */
    length = hashTypeLength(o);
    if (flags & OBJ_HASH_KEY && flags & OBJ_HASH_VALUE) {
        addReplyMapLen(c, length);
    } else {
        addReplyArrayLen(c, length);
    }

    hi = hashTypeInitIterator(o);

    /* 利用迭代器遍历哈希对象 */
    while (hashTypeNext(hi) != C_ERR) {

        /* 若设置了 OBJ_HASH_KEY 则将 field 取出并回复 */
        if (flags & OBJ_HASH_KEY) {
            addHashIteratorCursorToReply(c, hi, OBJ_HASH_KEY);
            count++;
        }

        /* 若设置了 OBJ_HASH_VALUE 则将 value 取出并回复 */
        if (flags & OBJ_HASH_VALUE) {
            addHashIteratorCursorToReply(c, hi, OBJ_HASH_VALUE);
            count++;
        }
    }

    hashTypeReleaseIterator(hi);

    /* Make sure we returned the right number of elements. */
    /* 确保我们返回了正确数量的元素 */
    if (flags & OBJ_HASH_KEY && flags & OBJ_HASH_VALUE) count /= 2;
    serverAssert(count == length);
}

/* 处理 hkeys 命令的函数 */
void hkeysCommand(client *c) {
    genericHgetallCommand(c,OBJ_HASH_KEY);
}

/* 处理 hvals 命令的函数 */
void hvalsCommand(client *c) {
    genericHgetallCommand(c,OBJ_HASH_VALUE);
}

/* 处理 hgetall 命令的函数 */
void hgetallCommand(client *c) {
    genericHgetallCommand(c,OBJ_HASH_KEY|OBJ_HASH_VALUE);
}

/* 处理 hexists 命令的函数 */
void hexistsCommand(client *c) {
    robj *o;
    if ((o = lookupKeyReadOrReply(c,c->argv[1],shared.czero)) == NULL ||
        checkType(c,o,OBJ_HASH)) return;

    /* 检查 field 是否存在， 存在回复1，不存在回复0 */
    addReply(c, hashTypeExists(o,c->argv[2]->ptr) ? shared.cone : shared.czero);
}

/* 处理hscan 命令的函数 */
void hscanCommand(client *c) {
    robj *o;
    unsigned long cursor;

    /* 将输入参数 cursor 解析到变量 cursor */
    if (parseScanCursorOrReply(c,c->argv[2],&cursor) == C_ERR) return;
    if ((o = lookupKeyReadOrReply(c,c->argv[1],shared.emptyscan)) == NULL ||
        checkType(c,o,OBJ_HASH)) return;
    
    /* 调用 scan 类命令通用实现函数 */
    scanGenericCommand(c,o,cursor);
}

/* hrandfield 命令对 listpack 编码类型哈希对象的具体实现函数 */
static void hrandfieldReplyWithListpack(client *c, unsigned int count, listpackEntry *keys, listpackEntry *vals) {
    for (unsigned long i = 0; i < count; i++) {
        if (vals && c->resp > 2)
            addReplyArrayLen(c,2);
        if (keys[i].sval)
            addReplyBulkCBuffer(c, keys[i].sval, keys[i].slen);
        else
            addReplyBulkLongLong(c, keys[i].lval);
        if (vals) {
            if (vals[i].sval)
                addReplyBulkCBuffer(c, vals[i].sval, vals[i].slen);
            else
                addReplyBulkLongLong(c, vals[i].lval);
        }
    }
}

/* How many times bigger should be the hash compared to the requested size
 * for us to not use the "remove elements" strategy? Read later in the
 * implementation for more info. */

/* 与要求的大小相比，哈希对象应该大多少倍，我们才不会使用 "移除元素" 策略？
 * 请阅读后面的实现，了解更多信息 */
#define HRANDFIELD_SUB_STRATEGY_MUL 3

/* If client is trying to ask for a very large number of random elements,
 * queuing may consume an unlimited amount of memory, so we want to limit
 * the number of randoms per time. */

/* 如果客户端试图请求非常多的随机元素，排队等待可能会消耗极大的内存且无法限制，
 * 所以我们要限制每次随机元素的数量 */
#define HRANDFIELD_RANDOM_SAMPLE_LIMIT 1000

/* hrandfield 命令带可选参数 count 的具体实现函数 */
void hrandfieldWithCountCommand(client *c, long l, int withvalues) {
    unsigned long count, size;
    int uniq = 1;
    robj *hash;

    if ((hash = lookupKeyReadOrReply(c,c->argv[1],shared.emptyarray))
        == NULL || checkType(c,hash,OBJ_HASH)) return;
    size = hashTypeLength(hash);

    if(l >= 0) {
        count = (unsigned long) l;
    } else {
        count = -l;
        uniq = 0;
    }

    /* If count is zero, serve it ASAP to avoid special cases later. */
    /* 如果输入的 count 是 0，回复一个空数组并返回 */
    if (count == 0) {
        addReply(c,shared.emptyarray);
        return;
    }

    /* CASE 1: The count was negative, so the extraction method is just:
     * "return N random elements" sampling the whole set every time.
     * This case is trivial and can be served without auxiliary data
     * structures. This case is the only one that also needs to return the
     * elements in random order. */

    /* 第一种情况：count 为负或等于1，所以实现方法是 "返回 N 个随机元素"，每次都对整个集合进行采样
     * 这种是普通的，不需要辅助数据结构就可以完成的情况 */
    if (!uniq || count == 1) {
        if (withvalues && c->resp == 2)
            addReplyArrayLen(c, count*2);
        else
            addReplyArrayLen(c, count);
        if (hash->encoding == OBJ_ENCODING_HT) {
            sds key, value;

            /* 循环 count 次 */
            while (count--) {

                /* 调用 dictGetFairRandomKey 随机取出一个 dictEntry */
                dictEntry *de = dictGetFairRandomKey(hash->ptr);
                key = dictGetKey(de);
                value = dictGetVal(de);
                if (withvalues && c->resp > 2)
                    addReplyArrayLen(c,2);
                addReplyBulkCBuffer(c, key, sdslen(key));
                if (withvalues)
                    addReplyBulkCBuffer(c, value, sdslen(value));
            }
        } else if (hash->encoding == OBJ_ENCODING_LISTPACK) {
            listpackEntry *keys, *vals = NULL;
            unsigned long limit, sample_count;

            /* 检查 count 是否超过限制值，若超过则强制等于限制值 */
            limit = count > HRANDFIELD_RANDOM_SAMPLE_LIMIT ? HRANDFIELD_RANDOM_SAMPLE_LIMIT : count;

            /* 给keys分配内存空间 */
            keys = zmalloc(sizeof(listpackEntry)*limit);

            /* 若命令带有 withvalues 参数则给 vals 分配内存空间 */
            if (withvalues)
                vals = zmalloc(sizeof(listpackEntry)*limit);
            
            while (count) {

                /* 检查并限制每轮最多采样 limit 个随机元素 */
                sample_count = count > limit ? limit : count;
                count -= sample_count;

                /* 通过调用 lpRandomPairs 函数随机获取 sample_count 个 listpackEntry ,
                 * 并将 field 的 listpackEntry 保存在 keys 中，
                 * 若 vals 不为 NULL 则将 value 的 listpackEntry 保存在 vals 中 */
                lpRandomPairs(hash->ptr, sample_count, keys, vals);
                hrandfieldReplyWithListpack(c, sample_count, keys, vals);
            }
            zfree(keys);
            zfree(vals);
        }
        return;
    }

    /* Initiate reply count, RESP3 responds with nested array, RESP2 with flat one. */
    /* 初始化向客户端回复的数组长度，RESP3 协议下用嵌套数组响应，RESP2 用一维数组回复 
     * 什么是 RESP 协议？官方文档：https://redis.io/docs/reference/protocol-spec/ */
    long reply_size = count < size ? count : size;
    if (withvalues && c->resp == 2)
        addReplyArrayLen(c, reply_size*2);
    else
        addReplyArrayLen(c, reply_size);

    /* CASE 2:
    * The number of requested elements is greater than the number of
    * elements inside the hash: simply return the whole hash. */

    /* 第二种情况：count 大于哈希对象中的元素数量，只需通过遍历回复整个哈希对象的 field-value */
    if(count >= size) {
        hashTypeIterator *hi = hashTypeInitIterator(hash);
        while (hashTypeNext(hi) != C_ERR) {
            if (withvalues && c->resp > 2)
                addReplyArrayLen(c,2);
            addHashIteratorCursorToReply(c, hi, OBJ_HASH_KEY);
            if (withvalues)
                addHashIteratorCursorToReply(c, hi, OBJ_HASH_VALUE);
        }
        hashTypeReleaseIterator(hi);
        return;
    }

    /* CASE 3:
     * The number of elements inside the hash is not greater than
     * HRANDFIELD_SUB_STRATEGY_MUL times the number of requested elements.
     * In this case we create a hash from scratch with all the elements, and
     * subtract random elements to reach the requested number of elements.
     *
     * This is done because if the number of requested elements is just
     * a bit less than the number of elements in the hash, the natural approach
     * used into CASE 4 is highly inefficient. */

    /* 第三种情况：
     * 哈希对象内的元素数量不超过 HRANDFIELD_SUB_STRATEGY_MUL 与 count 的乘积
     * 在这种情况下，我们创建一个哈希对象的副本，然后减去一些随机元素以达到要求的数量
     * 这样做是因为如果 count 只是比哈希对象中的元素数量小一点，
     * 如果使用在CASE 4中用的普通方法是非常低效的 */
    if (count*HRANDFIELD_SUB_STRATEGY_MUL > size) {
        dict *d = dictCreate(&sdsReplyDictType);
        dictExpand(d, size);
        hashTypeIterator *hi = hashTypeInitIterator(hash);

        /* Add all the elements into the temporary dictionary. */
        /* 将所有的元素加入到副本字典 d */
        while ((hashTypeNext(hi)) != C_ERR) {
            int ret = DICT_ERR;
            sds key, value = NULL;

            key = hashTypeCurrentObjectNewSds(hi,OBJ_HASH_KEY);
            if (withvalues)
                value = hashTypeCurrentObjectNewSds(hi,OBJ_HASH_VALUE);
            ret = dictAdd(d, key, value);

            serverAssert(ret == DICT_OK);
        }
        serverAssert(dictSize(d) == size);
        hashTypeReleaseIterator(hi);

        /* Remove random elements to reach the right count. */
        /* 随机删除元素，直到 size 被减到等于 count */
        while (size > count) {
            dictEntry *de;
            de = dictGetFairRandomKey(d);
            dictUnlink(d,dictGetKey(de));
            sdsfree(dictGetKey(de));
            sdsfree(dictGetVal(de));
            dictFreeUnlinkedEntry(d,de);
            size--;
        }

        /* Reply with what's in the dict and release memory */
        /* 回复副本字典中的元素给客户端，之后进行内存释放操作 */
        dictIterator *di;
        dictEntry *de;
        di = dictGetIterator(d);
        while ((de = dictNext(di)) != NULL) {
            sds key = dictGetKey(de);
            sds value = dictGetVal(de);
            if (withvalues && c->resp > 2)
                addReplyArrayLen(c,2);
            addReplyBulkSds(c, key);
            if (withvalues)
                addReplyBulkSds(c, value);
        }

        dictReleaseIterator(di);
        dictRelease(d);
    }

    /* CASE 4: We have a big hash compared to the requested number of elements.
     * In this case we can simply get random elements from the hash and add
     * to the temporary hash, trying to eventually get enough unique elements
     * to reach the specified count. */

    /* 第四种情况: 与 count 相比，哈希对象的容量要大得多 。
     * 在这种情况下，我们可以简单地从哈希对象中获取随机元素并添加到副本中，
     * 通过这种方法最终得到指定数量的唯一元素 */
    else {
        if (hash->encoding == OBJ_ENCODING_LISTPACK) {
            /* it is inefficient to repeatedly pick one random element from a
             * listpack. so we use this instead: */

            /* 从一个 listpack 中反复挑选一个随机元素的效率很低，所以我们用下面的方法来代替 */
            listpackEntry *keys, *vals = NULL;
            keys = zmalloc(sizeof(listpackEntry)*count);
            if (withvalues)
                vals = zmalloc(sizeof(listpackEntry)*count);
            serverAssert(lpRandomPairsUnique(hash->ptr, count, keys, vals) == count);
            hrandfieldReplyWithListpack(c, count, keys, vals);
            zfree(keys);
            zfree(vals);
            return;
        }

        /* Hashtable encoding (generic implementation) */
        unsigned long added = 0;
        listpackEntry key, value;
        dict *d = dictCreate(&hashDictType);
        dictExpand(d, count);
        while(added < count) {

            /* 在哈希对象中随机获取一个元素 */
            hashTypeRandomElement(hash, size, &key, withvalues? &value : NULL);

            /* Try to add the object to the dictionary. If it already exists
            * free it, otherwise increment the number of objects we have
            * in the result dictionary. */

            /* 尝试将该对象 (skey) 添加到字典中。如果已经存在就释放它，
             * 否则加入字典并增加我们在结果字典中的对象数量 (added) */
            sds skey = hashSdsFromListpackEntry(&key);
            if (dictAdd(d,skey,NULL) != DICT_OK) {
                sdsfree(skey);
                continue;
            }
            added++;

            /* We can reply right away, so that we don't need to store the value in the dict. */
            /* 我们可以立即回复，这样我们就不需要在 dict 中存储 value */
            if (withvalues && c->resp > 2)
                addReplyArrayLen(c,2);
            hashReplyFromListpackEntry(c, &key);
            if (withvalues)
                hashReplyFromListpackEntry(c, &value);
        }

        /* Release memory */
        dictRelease(d);
    }
}

/* HRANDFIELD key [<count> [WITHVALUES]] */
/* 处理hrandfield 命令的函数 */
void hrandfieldCommand(client *c) {
    long l;
    int withvalues = 0;
    robj *hash;
    listpackEntry ele;

    /* 解析可选参数 */
    if (c->argc >= 3) {
        if (getLongFromObjectOrReply(c,c->argv[2],&l,NULL) != C_OK) return;
        if (c->argc > 4 || (c->argc == 4 && strcasecmp(c->argv[3]->ptr,"withvalues"))) {
            addReplyErrorObject(c,shared.syntaxerr);
            return;
        } else if (c->argc == 4)
            withvalues = 1;
        hrandfieldWithCountCommand(c, l, withvalues);
        return;
    }

    /* Handle variant without <count> argument. Reply with simple bulk string */
    /* 接下来是处理没有 <count> 参数的变量。用简单的字符串进行回复 */

    /* 首先检查 key 是否存在，存在的话该对象是否是哈希类型 */
    if ((hash = lookupKeyReadOrReply(c,c->argv[1],shared.null[c->resp]))== NULL ||
        checkType(c,hash,OBJ_HASH)) {
        return;
    }

    /* 随机获取一个元素回复客户端 */
    hashTypeRandomElement(hash,hashTypeLength(hash),&ele,NULL);
    hashReplyFromListpackEntry(c, &ele);
}
