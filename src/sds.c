/* SDSLib 2.0 -- A C dynamic strings library
 *
 * Copyright (c) 2006-2015, Salvatore Sanfilippo <antirez at gmail dot com>
 * Copyright (c) 2015, Oran Agra
 * Copyright (c) 2015, Redis Labs, Inc
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

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <ctype.h>
#include <assert.h>
#include <limits.h>
#include "sds.h"
#include "sdsalloc.h"

const char *SDS_NOINIT = "SDS_NOINIT";

static inline int sdsHdrSize(char type) {
    switch(type&SDS_TYPE_MASK) {
        case SDS_TYPE_5:
            return sizeof(struct sdshdr5);
        case SDS_TYPE_8:
            return sizeof(struct sdshdr8);
        case SDS_TYPE_16:
            return sizeof(struct sdshdr16);
        case SDS_TYPE_32:
            return sizeof(struct sdshdr32);
        case SDS_TYPE_64:
            return sizeof(struct sdshdr64);
    }
    return 0;
}

static inline char sdsReqType(size_t string_size) {
    if (string_size < 1<<5)
        return SDS_TYPE_5;
    if (string_size < 1<<8)
        return SDS_TYPE_8;
    if (string_size < 1<<16)
        return SDS_TYPE_16;
#if (LONG_MAX == LLONG_MAX)
    if (string_size < 1ll<<32)
        return SDS_TYPE_32;
    return SDS_TYPE_64;
#else
    return SDS_TYPE_32;
#endif
}

static inline size_t sdsTypeMaxSize(char type) {
    if (type == SDS_TYPE_5)
        return (1<<5) - 1;
    if (type == SDS_TYPE_8)
        return (1<<8) - 1;
    if (type == SDS_TYPE_16)
        return (1<<16) - 1;
#if (LONG_MAX == LLONG_MAX)
    if (type == SDS_TYPE_32)
        return (1ll<<32) - 1;
#endif
    return -1; /* this is equivalent to the max SDS_TYPE_64 or SDS_TYPE_32 */
}

/* 使用 'init' 指针和 'initlen' 指定的内容创建一个新的 sds 字符串.
 * 'init' 如果是 NULL, 字符串初始化为 0 字节.
 * 如果使用了 SDS_NOINIT, 那么缓冲区不会被初始化.
 *
 * 字符串总是以空终结符结束 (所有的 sds 字符串都是),
 * 即使你使用如下方式创建一个 sds 字符串也是如此:
 *
 * mystring = sdsnewlen("abc",3);
 *
 * 您可以使用 printf() 打印字符串, 因为字符串末尾有一个隐式的 \0.
 * 不过, 字符串是二进制安全的, 中间可以包含 \0 字符, 因为长度存储在 sds 头部信息中 */
sds _sdsnewlen(const void *init, size_t initlen, int trymalloc) {
    void *sh;
    sds s;
    char type = sdsReqType(initlen);
    /* 创建空字符串通常是为了之后扩展.
     * 因为 sds5 不利于扩展, 所以会被转化为 sds8 */
    if (type == SDS_TYPE_5 && initlen == 0) type = SDS_TYPE_8;
    int hdrlen = sdsHdrSize(type); /* 计算不同头部所需的长度 */
    unsigned char *fp; /* 指向 flags 的指针. */
    size_t usable;

    assert(initlen + hdrlen + 1 > initlen); /* 处理 size_t 溢出情形 */
    sh = trymalloc?
        s_trymalloc_usable(hdrlen+initlen+1, &usable) : /* +1 是为了结束符 \0 */
        s_malloc_usable(hdrlen+initlen+1, &usable);
    if (sh == NULL) return NULL;
    if (init==SDS_NOINIT)
        init = NULL;
    else if (!init)
        memset(sh, 0, hdrlen+initlen+1);
    s = (char*)sh+hdrlen;
    fp = ((unsigned char*)s)-1; /* 柔性数组 buf 的前面一个字节就是 flags */
    usable = usable-hdrlen-1;
    if (usable > sdsTypeMaxSize(type))
        usable = sdsTypeMaxSize(type);
    switch(type) {
        case SDS_TYPE_5: {
            *fp = type | (initlen << SDS_TYPE_BITS);
            break;
        }
        case SDS_TYPE_8: {
            SDS_HDR_VAR(8,s);
            sh->len = initlen;
            sh->alloc = usable;
            *fp = type;
            break;
        }
        case SDS_TYPE_16: {
            SDS_HDR_VAR(16,s);
            sh->len = initlen;
            sh->alloc = usable;
            *fp = type;
            break;
        }
        case SDS_TYPE_32: {
            SDS_HDR_VAR(32,s);
            sh->len = initlen;
            sh->alloc = usable;
            *fp = type;
            break;
        }
        case SDS_TYPE_64: {
            SDS_HDR_VAR(64,s);
            sh->len = initlen;
            sh->alloc = usable;
            *fp = type;
            break;
        }
    }
    if (initlen && init)
        memcpy(s, init, initlen);
    s[initlen] = '\0';
    return s;
}

sds sdsnewlen(const void *init, size_t initlen) {
    return _sdsnewlen(init, initlen, 0);
}

sds sdstrynewlen(const void *init, size_t initlen) {
    return _sdsnewlen(init, initlen, 1);
}

/* 创建一个空的 (长度为 0) sds 字符串. 即使在这种情况下，字符串也总是有一个隐式的空终结符项. */
sds sdsempty(void) {
    return sdsnewlen("",0);
}

/* 从一个以空终结符结束的 C 字符串中创建一个新的 sds 字符串. */
sds sdsnew(const char *init) {
    size_t initlen = (init == NULL) ? 0 : strlen(init);
    return sdsnewlen(init, initlen);
}

/* 复制一个 sds 字符串. */
sds sdsdup(const sds s) {
    return sdsnewlen(s, sdslen(s));
}

/* 释放一个 sds 字符串. 如果 's' 为 NULL, 则不执行任何操作. */
void sdsfree(sds s) {
    if (s == NULL) return;
    s_free((char*)s-sdsHdrSize(s[-1]));
}

/* 将 sds 字符串长度设置为使用 strlen() 获得的长度,
 * 因此实际内容为到达第一个 null 项字符前的内容.
 *
 * 当以某种方式手动处理 sds 字符串时, 这个函数很有用, 如下面的示例所示:
 *
 * s = sdsnew("foobar");
 * s[2] = '\0';
 * sdsupdatelen(s);
 * printf("%d\n", sdslen(s));
 *
 * 输出将会是 "2", 但如果我们注释掉 对sdsupdatelen() 的调用
 * 输出将会是 "6" 因为字符串虽被修改, 但逻辑长度 (头部信息) 仍然是6字节. */
void sdsupdatelen(sds s) {
    size_t reallen = strlen(s);
    sdssetlen(s, reallen);
}

/* 在其原位置把一个 sds 变量 's' 设为空字符串 (将其 len 设为 0).
 * 注意, 旧的缓冲区不会被释放, 而是被设置为空闲状态,
 * 这样的话, 下一次的扩展就不需要把之前已分配缓冲区再分配一次 */
void sdsclear(sds s) {
    sdssetlen(s, 0);
    s[0] = '\0';
}

/* 扩大 sds 字符串末端的空闲空间,
 * 这样, 调用者就可以确定在调用这个函数之后,
 * 字符串末尾的 addlen 字节是可覆写的, 然后再加上 nul 项的一个字节.
 * 如果已经有足够的空闲空间, 这个函数会返回且不做任何动作,
 * 如果没有足够的空闲空间, 它将分配不足的部分, 甚至比不足部分更多:
 * 当 greedy 为 1 时, 会分配比所需更多的空间, 以避免在增量增长上需要未来的再分配.
 * 当 greedy 为 0 时, 仅分配 addlen 所需要的空间.
 *
 * 注意: 这不会改变 sdslen() 返回的 SDS 字符串的 *长度*, 而只改变我们拥有的空闲缓冲区空间. */
sds _sdsMakeRoomFor(sds s, size_t addlen, int greedy) {
    void *sh, *newsh;
    size_t avail = sdsavail(s);
    size_t len, newlen, reqlen;
    char type, oldtype = s[-1] & SDS_TYPE_MASK;
    int hdrlen;
    size_t usable;

    /* 空间足够时的优化. */
    if (avail >= addlen) return s;

    len = sdslen(s);
    sh = (char*)s-sdsHdrSize(oldtype);
    reqlen = newlen = (len+addlen);
    assert(newlen > len);   /* 处理 size_t 溢出 */
    if (greedy == 1) {
        if (newlen < SDS_MAX_PREALLOC)
            newlen *= 2;
        else
            newlen += SDS_MAX_PREALLOC;
    }

    type = sdsReqType(newlen);

    /* 不要使用 sds5: 用户正在向字符串添加内容,
     * sds5 没有记录空闲空间, 所以必须在每次追加操作时调用 sdsMakeRoomFor(). */
    if (type == SDS_TYPE_5) type = SDS_TYPE_8;

    hdrlen = sdsHdrSize(type);
    assert(hdrlen + newlen + 1 > reqlen);  /* 处理 size_t溢出 */
    if (oldtype==type) {
        newsh = s_realloc_usable(sh, hdrlen+newlen+1, &usable);
        if (newsh == NULL) return NULL;
        s = (char*)newsh+hdrlen;
    } else {
        /* 由于头部大小改变, 需要将字符串向前移动, 不能使用 realloc */
        newsh = s_malloc_usable(hdrlen+newlen+1, &usable);
        if (newsh == NULL) return NULL;
        memcpy((char*)newsh+hdrlen, s, len+1);
        s_free(sh);
        s = (char*)newsh+hdrlen;
        s[-1] = type;
        sdssetlen(s, len);
    }
    usable = usable-hdrlen-1;
    if (usable > sdsTypeMaxSize(type))
        usable = sdsTypeMaxSize(type);
    sdssetalloc(s, usable);
    return s;
}

/* 扩展 sds 字符串末尾的空闲空间时, 分配比所需更大的空间,
 * 这有助于避免在多次追加 sds 时重复的再分配. */
sds sdsMakeRoomFor(sds s, size_t addlen) {
    return _sdsMakeRoomFor(s, addlen, 1);
}

/* 不像 sdsMakeRoomFor(), 这个函数只会扩充字符串所需要的空间. */
sds sdsMakeRoomForNonGreedy(sds s, size_t addlen) {
    return _sdsMakeRoomFor(s, addlen, 0);
}

/* 再分配 sds 字符串, 分配目的是移出未使用的尾部空闲空间.
 * 所包含的字符串并没有被改变, 但是下一次拼接时将会需要再分配.
 *
 * 在这次调用后, 传入的字符串将会失效,
 * 所有相关引用的指针都需要被替换成本次调用返回的新指针 */
sds sdsRemoveFreeSpace(sds s) {
    void *sh, *newsh;
    char type, oldtype = s[-1] & SDS_TYPE_MASK;
    int hdrlen, oldhdrlen = sdsHdrSize(oldtype);
    size_t len = sdslen(s);
    size_t avail = sdsavail(s);
    sh = (char*)s-oldhdrlen;

    /* 如果没有空闲空间, 直接返回. */
    if (avail == 0) return s;

    /* 找出可以适配字符串的最小的 sds 头部. */
    type = sdsReqType(len);
    hdrlen = sdsHdrSize(type);

    /* 如果适配类型和之前相同, 或者至少这个类型足够大满足所需
     * 那么只会进行 realloc(),
     * 只有真正需要这么做时, 分配器才会进行拷贝.
     * 此外, 如果变动十分巨大 (else)
     * 我们会亲自去分配构造一个使用了不同的头部类型的字符串. */
    if (oldtype==type || type > SDS_TYPE_8) {
        newsh = s_realloc(sh, oldhdrlen+len+1);
        if (newsh == NULL) return NULL;
        s = (char*)newsh+oldhdrlen;
    } else {
        newsh = s_malloc(hdrlen+len+1);
        if (newsh == NULL) return NULL;
        memcpy((char*)newsh+hdrlen, s, len+1);
        s_free(sh);
        s = (char*)newsh+hdrlen;
        s[-1] = type;
        sdssetlen(s, len);
    }
    sdssetalloc(s, len);
    return s;
}

/* 改变现有分配内存的规格, 可能会使其更大, 也有可能更小,
 * 如果缩小且比之前已使用的字符串的长度还要小, 数据部分将会被截断 */
sds sdsResize(sds s, size_t size) {
    void *sh, *newsh;
    char type, oldtype = s[-1] & SDS_TYPE_MASK;
    int hdrlen, oldhdrlen = sdsHdrSize(oldtype);
    size_t len = sdslen(s);
    sh = (char*)s-oldhdrlen;

    /*  如果之前的大小就恰好, 直接返回. */
    if (sdsalloc(s) == size) return s;

    /* 必要时进行截断. */
    if (size < len) len = size;

    /* 找出可以适配字符串的最小的 sds 头部 */
    type = sdsReqType(size);
    /* 不使用 sds5, 其不适用于改变大小的字符串. */
    if (type == SDS_TYPE_5) type = SDS_TYPE_8;
    hdrlen = sdsHdrSize(type);

    /* 如果适配类型和之前相同, 或者可以以更低的开销适配大小, (最低开销为 sds8 类型)
     * 那么只会进行 realloc(),
     * 只有真正需要这么做时, 分配器才会进行拷贝.
     * 此外, 如果变动十分巨大 (else)
     * 我们会亲自去分配构造一个使用了不同的头部类型的字符串. */
    if (oldtype==type || (type < oldtype && type > SDS_TYPE_8)) {
        newsh = s_realloc(sh, oldhdrlen+size+1);
        if (newsh == NULL) return NULL;
        s = (char*)newsh+oldhdrlen;
    } else {
        newsh = s_malloc(hdrlen+size+1);
        if (newsh == NULL) return NULL;
        memcpy((char*)newsh+hdrlen, s, len);
        s_free(sh);
        s = (char*)newsh+hdrlen;
        s[-1] = type;
    }
    s[len] = 0;
    sdssetlen(s, len);
    sdssetalloc(s, size);
    return s;
}

/* 返回被指定的 sds 字符串中分配空间的总大小,
 * 包括:
 * 1) 指针前的 sds 头部信息.
 * 2) 字符串.
 * 3) (如果有) 尾部的空闲空间.
 * 4) 隐式的空终结符.
 */
size_t sdsAllocSize(sds s) {
    size_t alloc = sdsalloc(s);
    return sdsHdrSize(s[-1])+alloc+1;
}

/* 返回指向 SDS 全部分配空间的指针
 * (一般都是返回指向其 buf 缓冲区的指针) */
void *sdsAllocPtr(sds s) {
    return (void*) (s-sdsHdrSize(s[-1]));
}

/* Increment the sds length and decrements the left free space at the
 * end of the string according to 'incr'. Also set the null term
 * in the new end of the string.
 *
 * This function is used in order to fix the string length after the
 * user calls sdsMakeRoomFor(), writes something after the end of
 * the current string, and finally needs to set the new length.
 *
 * Note: it is possible to use a negative increment in order to
 * right-trim the string.
 *
 * Usage example:
 *
 * Using sdsIncrLen() and sdsMakeRoomFor() it is possible to mount the
 * following schema, to cat bytes coming from the kernel to the end of an
 * sds string without copying into an intermediate buffer:
 *
 * oldlen = sdslen(s);
 * s = sdsMakeRoomFor(s, BUFFER_SIZE);
 * nread = read(fd, s+oldlen, BUFFER_SIZE);
 * ... check for nread <= 0 and handle it ...
 * sdsIncrLen(s, nread);
 */
void sdsIncrLen(sds s, ssize_t incr) {
    unsigned char flags = s[-1];
    size_t len;
    switch(flags&SDS_TYPE_MASK) {
        case SDS_TYPE_5: {
            unsigned char *fp = ((unsigned char*)s)-1;
            unsigned char oldlen = SDS_TYPE_5_LEN(flags);
            assert((incr > 0 && oldlen+incr < 32) || (incr < 0 && oldlen >= (unsigned int)(-incr)));
            *fp = SDS_TYPE_5 | ((oldlen+incr) << SDS_TYPE_BITS);
            len = oldlen+incr;
            break;
        }
        case SDS_TYPE_8: {
            SDS_HDR_VAR(8,s);
            assert((incr >= 0 && sh->alloc-sh->len >= incr) || (incr < 0 && sh->len >= (unsigned int)(-incr)));
            len = (sh->len += incr);
            break;
        }
        case SDS_TYPE_16: {
            SDS_HDR_VAR(16,s);
            assert((incr >= 0 && sh->alloc-sh->len >= incr) || (incr < 0 && sh->len >= (unsigned int)(-incr)));
            len = (sh->len += incr);
            break;
        }
        case SDS_TYPE_32: {
            SDS_HDR_VAR(32,s);
            assert((incr >= 0 && sh->alloc-sh->len >= (unsigned int)incr) || (incr < 0 && sh->len >= (unsigned int)(-incr)));
            len = (sh->len += incr);
            break;
        }
        case SDS_TYPE_64: {
            SDS_HDR_VAR(64,s);
            assert((incr >= 0 && sh->alloc-sh->len >= (uint64_t)incr) || (incr < 0 && sh->len >= (uint64_t)(-incr)));
            len = (sh->len += incr);
            break;
        }
        default: len = 0; /* Just to avoid compilation warnings. */
    }
    s[len] = '\0';
}

/* 扩展 sds 到指定的长度, 扩展部分会使用 0 初始化
 *
 * 如果指定的长度小于当前长度, 将不会采取任何动作 */
sds sdsgrowzero(sds s, size_t len) {
    size_t curlen = sdslen(s);

    if (len <= curlen) return s;
    s = sdsMakeRoomFor(s,len-curlen);
    if (s == NULL) return NULL;

    /* 确保添加的区域内全部被初始化 */
    memset(s+curlen,0,(len-curlen+1)); /* 仍要添加 \0 字节 */
    sdssetlen(s, len);
    return s;
}

/* 将长度为 'len' 由 't' 指向的二进制安全字符串添加到指定的 sds 字符串 's' 的末尾.
 *
 * 该函数调用之后, 传入的 sds 字符串不再有效, 所有相关引用都必须被替换为调用后返回的新指针. */
sds sdscatlen(sds s, const void *t, size_t len) {
    size_t curlen = sdslen(s);

    s = sdsMakeRoomFor(s,len);
    if (s == NULL) return NULL;
    memcpy(s+curlen, t, len);
    sdssetlen(s, curlen+len);
    s[curlen+len] = '\0';
    return s;
}

/* 追加一个以 null 结尾的 c 字符串到 sds 字符串 's' 后面
 *
 * 该函数调用之后, 传入的 sds 字符串不再有效, 所有相关引用都必须被替换为调用后返回的新指针. */
sds sdscat(sds s, const char *t) {
    return sdscatlen(s, t, strlen(t));
}

/* 将指定的sds 't' 追加到现有的sds 's' 之后.
 *
 * 调用之后, 被修改过的sds字符串将不再有效,
 * 所有对其的引用都必须被替换为本次调用返回的新指针. */
sds sdscatsds(sds s, const sds t) {
    return sdscatlen(s, t, sdslen(t));
}

/* 破坏性地去修改 sds 字符串 's', 让其指向 't' 指向的, 类型安全且长度为 'len' 的字符串 */
sds sdscpylen(sds s, const char *t, size_t len) {
    if (sdsalloc(s) < len) {
        s = sdsMakeRoomFor(s,len-sdslen(s));
        if (s == NULL) return NULL;
    }
    memcpy(s, t, len);
    s[len] = '\0';
    sdssetlen(s, len);
    return s;
}

/* 和 sdscpylen() 很像, 但是 't' 必须是一个以空终结符结尾的字符串, 这样才可以通过 strlen() 获取它的长度 */
sds sdscpy(sds s, const char *t) {
    return sdscpylen(s, t, strlen(t));
}

/* 帮助 sdscatlonglong() 完成真正的 数字 -> 字符串 的转换.
 * 's' 指向的字符串, 至少要含有 SDS_LLSTR_SIZE 字节的空间.
 *
 * 这个函数返回存储在 's' 中的以空终结符结尾的字符串的长度 */
#define SDS_LLSTR_SIZE 21
int sdsll2str(char *s, long long value) {
    char *p, aux;
    unsigned long long v;
    size_t l;

    /* 生成字符串一种表现形式, 这个方法会制造一个反向的字符串. */
    if (value < 0) {
        /* 因为 v 是无符号的, 如果它的大小等于 LLONG_MIN, 它的相反数将会溢出. */
        if (value != LLONG_MIN) {
            v = -value;
        } else {
            v = ((unsigned long long)LLONG_MAX) + 1;
        }
    } else {
        v = value;
    }

    p = s;
    do {
        *p++ = '0'+(v%10);
        v /= 10;
    } while(v);
    if (value < 0) *p++ = '-';

    /* 计算长度, 添加 null. */
    l = p-s;
    *p = '\0';

    /* 翻转字符串. */
    p--;
    while(s < p) {
        aux = *s;
        *s = *p;
        *p = aux;
        s++;
        p--;
    }
    return l;
}

/* Identical sdsll2str(), but for unsigned long long type. */
int sdsull2str(char *s, unsigned long long v) {
    char *p, aux;
    size_t l;

    /* Generate the string representation, this method produces
     * a reversed string. */
    p = s;
    do {
        *p++ = '0'+(v%10);
        v /= 10;
    } while(v);

    /* Compute length and add null term. */
    l = p-s;
    *p = '\0';

    /* Reverse the string. */
    p--;
    while(s < p) {
        aux = *s;
        *s = *p;
        *p = aux;
        s++;
        p--;
    }
    return l;
}

/* Create an sds string from a long long value. It is much faster than:
 *
 * sdscatprintf(sdsempty(),"%lld\n", value);
 */
sds sdsfromlonglong(long long value) {
    char buf[SDS_LLSTR_SIZE];
    int len = sdsll2str(buf,value);

    return sdsnewlen(buf,len);
}

/* Like sdscatprintf() but gets va_list instead of being variadic. */
sds sdscatvprintf(sds s, const char *fmt, va_list ap) {
    va_list cpy;
    char staticbuf[1024], *buf = staticbuf, *t;
    size_t buflen = strlen(fmt)*2;
    int bufstrlen;

    /* We try to start using a static buffer for speed.
     * If not possible we revert to heap allocation. */
    if (buflen > sizeof(staticbuf)) {
        buf = s_malloc(buflen);
        if (buf == NULL) return NULL;
    } else {
        buflen = sizeof(staticbuf);
    }

    /* Alloc enough space for buffer and \0 after failing to
     * fit the string in the current buffer size. */
    while(1) {
        va_copy(cpy,ap);
        bufstrlen = vsnprintf(buf, buflen, fmt, cpy);
        va_end(cpy);
        if (bufstrlen < 0) {
            if (buf != staticbuf) s_free(buf);
            return NULL;
        }
        if (((size_t)bufstrlen) >= buflen) {
            if (buf != staticbuf) s_free(buf);
            buflen = ((size_t)bufstrlen) + 1;
            buf = s_malloc(buflen);
            if (buf == NULL) return NULL;
            continue;
        }
        break;
    }

    /* Finally concat the obtained string to the SDS string and return it. */
    t = sdscatlen(s, buf, bufstrlen);
    if (buf != staticbuf) s_free(buf);
    return t;
}

/* Append to the sds string 's' a string obtained using printf-alike format
 * specifier.
 *
 * After the call, the modified sds string is no longer valid and all the
 * references must be substituted with the new pointer returned by the call.
 *
 * Example:
 *
 * s = sdsnew("Sum is: ");
 * s = sdscatprintf(s,"%d+%d = %d",a,b,a+b).
 *
 * Often you need to create a string from scratch with the printf-alike
 * format. When this is the need, just use sdsempty() as the target string:
 *
 * s = sdscatprintf(sdsempty(), "... your format ...", args);
 */
sds sdscatprintf(sds s, const char *fmt, ...) {
    va_list ap;
    char *t;
    va_start(ap, fmt);
    t = sdscatvprintf(s,fmt,ap);
    va_end(ap);
    return t;
}

/* This function is similar to sdscatprintf, but much faster as it does
 * not rely on sprintf() family functions implemented by the libc that
 * are often very slow. Moreover directly handling the sds string as
 * new data is concatenated provides a performance improvement.
 *
 * However this function only handles an incompatible subset of printf-alike
 * format specifiers:
 *
 * %s - C String
 * %S - SDS string
 * %i - signed int
 * %I - 64 bit signed integer (long long, int64_t)
 * %u - unsigned int
 * %U - 64 bit unsigned integer (unsigned long long, uint64_t)
 * %% - Verbatim "%" character.
 */
sds sdscatfmt(sds s, char const *fmt, ...) {
    size_t initlen = sdslen(s);
    const char *f = fmt;
    long i;
    va_list ap;

    /* To avoid continuous reallocations, let's start with a buffer that
     * can hold at least two times the format string itself. It's not the
     * best heuristic but seems to work in practice. */
    s = sdsMakeRoomFor(s, strlen(fmt)*2);
    va_start(ap,fmt);
    f = fmt;    /* Next format specifier byte to process. */
    i = initlen; /* Position of the next byte to write to dest str. */
    while(*f) {
        char next, *str;
        size_t l;
        long long num;
        unsigned long long unum;

        /* Make sure there is always space for at least 1 char. */
        if (sdsavail(s)==0) {
            s = sdsMakeRoomFor(s,1);
        }

        switch(*f) {
        case '%':
            next = *(f+1);
            if (next == '\0') break;
            f++;
            switch(next) {
            case 's':
            case 'S':
                str = va_arg(ap,char*);
                l = (next == 's') ? strlen(str) : sdslen(str);
                if (sdsavail(s) < l) {
                    s = sdsMakeRoomFor(s,l);
                }
                memcpy(s+i,str,l);
                sdsinclen(s,l);
                i += l;
                break;
            case 'i':
            case 'I':
                if (next == 'i')
                    num = va_arg(ap,int);
                else
                    num = va_arg(ap,long long);
                {
                    char buf[SDS_LLSTR_SIZE];
                    l = sdsll2str(buf,num);
                    if (sdsavail(s) < l) {
                        s = sdsMakeRoomFor(s,l);
                    }
                    memcpy(s+i,buf,l);
                    sdsinclen(s,l);
                    i += l;
                }
                break;
            case 'u':
            case 'U':
                if (next == 'u')
                    unum = va_arg(ap,unsigned int);
                else
                    unum = va_arg(ap,unsigned long long);
                {
                    char buf[SDS_LLSTR_SIZE];
                    l = sdsull2str(buf,unum);
                    if (sdsavail(s) < l) {
                        s = sdsMakeRoomFor(s,l);
                    }
                    memcpy(s+i,buf,l);
                    sdsinclen(s,l);
                    i += l;
                }
                break;
            default: /* Handle %% and generally %<unknown>. */
                s[i++] = next;
                sdsinclen(s,1);
                break;
            }
            break;
        default:
            s[i++] = *f;
            sdsinclen(s,1);
            break;
        }
        f++;
    }
    va_end(ap);

    /* Add null-term */
    s[i] = '\0';
    return s;
}

/* Remove the part of the string from left and from right composed just of
 * contiguous characters found in 'cset', that is a null terminated C string.
 *
 * After the call, the modified sds string is no longer valid and all the
 * references must be substituted with the new pointer returned by the call.
 *
 * Example:
 *
 * s = sdsnew("AA...AA.a.aa.aHelloWorld     :::");
 * s = sdstrim(s,"Aa. :");
 * printf("%s\n", s);
 *
 * Output will be just "HelloWorld".
 */
sds sdstrim(sds s, const char *cset) {
    char *end, *sp, *ep;
    size_t len;

    sp = s;
    ep = end = s+sdslen(s)-1;
    while(sp <= end && strchr(cset, *sp)) sp++;
    while(ep > sp && strchr(cset, *ep)) ep--;
    len = (ep-sp)+1;
    if (s != sp) memmove(s, sp, len);
    s[len] = '\0';
    sdssetlen(s,len);
    return s;
}

/* Changes the input string to be a subset of the original.
 * It does not release the free space in the string, so a call to
 * sdsRemoveFreeSpace may be wise after. */
void sdssubstr(sds s, size_t start, size_t len) {
    /* Clamp out of range input */
    size_t oldlen = sdslen(s);
    if (start >= oldlen) start = len = 0;
    if (len > oldlen-start) len = oldlen-start;

    /* Move the data */
    if (len) memmove(s, s+start, len);
    s[len] = 0;
    sdssetlen(s,len);
}

/* Turn the string into a smaller (or equal) string containing only the
 * substring specified by the 'start' and 'end' indexes.
 *
 * start and end can be negative, where -1 means the last character of the
 * string, -2 the penultimate character, and so forth.
 *
 * The interval is inclusive, so the start and end characters will be part
 * of the resulting string.
 *
 * The string is modified in-place.
 *
 * NOTE: this function can be misleading and can have unexpected behaviour,
 * specifically when you want the length of the new string to be 0.
 * Having start==end will result in a string with one character.
 * please consider using sdssubstr instead.
 *
 * Example:
 *
 * s = sdsnew("Hello World");
 * sdsrange(s,1,-1); => "ello World"
 */
void sdsrange(sds s, ssize_t start, ssize_t end) {
    size_t newlen, len = sdslen(s);
    if (len == 0) return;
    if (start < 0)
        start = len + start;
    if (end < 0)
        end = len + end;
    newlen = (start > end) ? 0 : (end-start)+1;
    sdssubstr(s, start, newlen);
}

/* Apply tolower() to every character of the sds string 's'. */
void sdstolower(sds s) {
    size_t len = sdslen(s), j;

    for (j = 0; j < len; j++) s[j] = tolower(s[j]);
}

/* Apply toupper() to every character of the sds string 's'. */
void sdstoupper(sds s) {
    size_t len = sdslen(s), j;

    for (j = 0; j < len; j++) s[j] = toupper(s[j]);
}

/* Compare two sds strings s1 and s2 with memcmp().
 *
 * Return value:
 *
 *     positive if s1 > s2.
 *     negative if s1 < s2.
 *     0 if s1 and s2 are exactly the same binary string.
 *
 * If two strings share exactly the same prefix, but one of the two has
 * additional characters, the longer string is considered to be greater than
 * the smaller one. */
int sdscmp(const sds s1, const sds s2) {
    size_t l1, l2, minlen;
    int cmp;

    l1 = sdslen(s1);
    l2 = sdslen(s2);
    minlen = (l1 < l2) ? l1 : l2;
    cmp = memcmp(s1,s2,minlen);
    if (cmp == 0) return l1>l2? 1: (l1<l2? -1: 0);
    return cmp;
}

/* Split 's' with separator in 'sep'. An array
 * of sds strings is returned. *count will be set
 * by reference to the number of tokens returned.
 *
 * On out of memory, zero length string, zero length
 * separator, NULL is returned.
 *
 * Note that 'sep' is able to split a string using
 * a multi-character separator. For example
 * sdssplit("foo_-_bar","_-_"); will return two
 * elements "foo" and "bar".
 *
 * This version of the function is binary-safe but
 * requires length arguments. sdssplit() is just the
 * same function but for zero-terminated strings.
 */
sds *sdssplitlen(const char *s, ssize_t len, const char *sep, int seplen, int *count) {
    int elements = 0, slots = 5;
    long start = 0, j;
    sds *tokens;

    if (seplen < 1 || len <= 0) {
        *count = 0;
        return NULL;
    }
    tokens = s_malloc(sizeof(sds)*slots);
    if (tokens == NULL) return NULL;

    for (j = 0; j < (len-(seplen-1)); j++) {
        /* make sure there is room for the next element and the final one */
        if (slots < elements+2) {
            sds *newtokens;

            slots *= 2;
            newtokens = s_realloc(tokens,sizeof(sds)*slots);
            if (newtokens == NULL) goto cleanup;
            tokens = newtokens;
        }
        /* search the separator */
        if ((seplen == 1 && *(s+j) == sep[0]) || (memcmp(s+j,sep,seplen) == 0)) {
            tokens[elements] = sdsnewlen(s+start,j-start);
            if (tokens[elements] == NULL) goto cleanup;
            elements++;
            start = j+seplen;
            j = j+seplen-1; /* skip the separator */
        }
    }
    /* Add the final element. We are sure there is room in the tokens array. */
    tokens[elements] = sdsnewlen(s+start,len-start);
    if (tokens[elements] == NULL) goto cleanup;
    elements++;
    *count = elements;
    return tokens;

cleanup:
    {
        int i;
        for (i = 0; i < elements; i++) sdsfree(tokens[i]);
        s_free(tokens);
        *count = 0;
        return NULL;
    }
}

/* Free the result returned by sdssplitlen(), or do nothing if 'tokens' is NULL. */
void sdsfreesplitres(sds *tokens, int count) {
    if (!tokens) return;
    while(count--)
        sdsfree(tokens[count]);
    s_free(tokens);
}

/* Append to the sds string "s" an escaped string representation where
 * all the non-printable characters (tested with isprint()) are turned into
 * escapes in the form "\n\r\a...." or "\x<hex-number>".
 *
 * After the call, the modified sds string is no longer valid and all the
 * references must be substituted with the new pointer returned by the call. */
sds sdscatrepr(sds s, const char *p, size_t len) {
    s = sdscatlen(s,"\"",1);
    while(len--) {
        switch(*p) {
        case '\\':
        case '"':
            s = sdscatprintf(s,"\\%c",*p);
            break;
        case '\n': s = sdscatlen(s,"\\n",2); break;
        case '\r': s = sdscatlen(s,"\\r",2); break;
        case '\t': s = sdscatlen(s,"\\t",2); break;
        case '\a': s = sdscatlen(s,"\\a",2); break;
        case '\b': s = sdscatlen(s,"\\b",2); break;
        default:
            if (isprint(*p))
                s = sdscatprintf(s,"%c",*p);
            else
                s = sdscatprintf(s,"\\x%02x",(unsigned char)*p);
            break;
        }
        p++;
    }
    return sdscatlen(s,"\"",1);
}

/* Returns one if the string contains characters to be escaped
 * by sdscatrepr(), zero otherwise.
 *
 * Typically, this should be used to help protect aggregated strings in a way
 * that is compatible with sdssplitargs(). For this reason, also spaces will be
 * treated as needing an escape.
 */
int sdsneedsrepr(const sds s) {
    size_t len = sdslen(s);
    const char *p = s;

    while (len--) {
        if (*p == '\\' || *p == '"' || *p == '\n' || *p == '\r' ||
            *p == '\t' || *p == '\a' || *p == '\b' || !isprint(*p) || isspace(*p)) return 1;
        p++;
    }

    return 0;
}

/* Helper function for sdssplitargs() that returns non zero if 'c'
 * is a valid hex digit. */
int is_hex_digit(char c) {
    return (c >= '0' && c <= '9') || (c >= 'a' && c <= 'f') ||
           (c >= 'A' && c <= 'F');
}

/* Helper function for sdssplitargs() that converts a hex digit into an
 * integer from 0 to 15 */
int hex_digit_to_int(char c) {
    switch(c) {
    case '0': return 0;
    case '1': return 1;
    case '2': return 2;
    case '3': return 3;
    case '4': return 4;
    case '5': return 5;
    case '6': return 6;
    case '7': return 7;
    case '8': return 8;
    case '9': return 9;
    case 'a': case 'A': return 10;
    case 'b': case 'B': return 11;
    case 'c': case 'C': return 12;
    case 'd': case 'D': return 13;
    case 'e': case 'E': return 14;
    case 'f': case 'F': return 15;
    default: return 0;
    }
}

/* Split a line into arguments, where every argument can be in the
 * following programming-language REPL-alike form:
 *
 * foo bar "newline are supported\n" and "\xff\x00otherstuff"
 *
 * The number of arguments is stored into *argc, and an array
 * of sds is returned.
 *
 * The caller should free the resulting array of sds strings with
 * sdsfreesplitres().
 *
 * Note that sdscatrepr() is able to convert back a string into
 * a quoted string in the same format sdssplitargs() is able to parse.
 *
 * The function returns the allocated tokens on success, even when the
 * input string is empty, or NULL if the input contains unbalanced
 * quotes or closed quotes followed by non space characters
 * as in: "foo"bar or "foo'
 */
sds *sdssplitargs(const char *line, int *argc) {
    const char *p = line;
    char *current = NULL;
    char **vector = NULL;

    *argc = 0;
    while(1) {
        /* skip blanks */
        while(*p && isspace(*p)) p++;
        if (*p) {
            /* get a token */
            int inq=0;  /* set to 1 if we are in "quotes" */
            int insq=0; /* set to 1 if we are in 'single quotes' */
            int done=0;

            if (current == NULL) current = sdsempty();
            while(!done) {
                if (inq) {
                    if (*p == '\\' && *(p+1) == 'x' &&
                                             is_hex_digit(*(p+2)) &&
                                             is_hex_digit(*(p+3)))
                    {
                        unsigned char byte;

                        byte = (hex_digit_to_int(*(p+2))*16)+
                                hex_digit_to_int(*(p+3));
                        current = sdscatlen(current,(char*)&byte,1);
                        p += 3;
                    } else if (*p == '\\' && *(p+1)) {
                        char c;

                        p++;
                        switch(*p) {
                        case 'n': c = '\n'; break;
                        case 'r': c = '\r'; break;
                        case 't': c = '\t'; break;
                        case 'b': c = '\b'; break;
                        case 'a': c = '\a'; break;
                        default: c = *p; break;
                        }
                        current = sdscatlen(current,&c,1);
                    } else if (*p == '"') {
                        /* closing quote must be followed by a space or
                         * nothing at all. */
                        if (*(p+1) && !isspace(*(p+1))) goto err;
                        done=1;
                    } else if (!*p) {
                        /* unterminated quotes */
                        goto err;
                    } else {
                        current = sdscatlen(current,p,1);
                    }
                } else if (insq) {
                    if (*p == '\\' && *(p+1) == '\'') {
                        p++;
                        current = sdscatlen(current,"'",1);
                    } else if (*p == '\'') {
                        /* closing quote must be followed by a space or
                         * nothing at all. */
                        if (*(p+1) && !isspace(*(p+1))) goto err;
                        done=1;
                    } else if (!*p) {
                        /* unterminated quotes */
                        goto err;
                    } else {
                        current = sdscatlen(current,p,1);
                    }
                } else {
                    switch(*p) {
                    case ' ':
                    case '\n':
                    case '\r':
                    case '\t':
                    case '\0':
                        done=1;
                        break;
                    case '"':
                        inq=1;
                        break;
                    case '\'':
                        insq=1;
                        break;
                    default:
                        current = sdscatlen(current,p,1);
                        break;
                    }
                }
                if (*p) p++;
            }
            /* add the token to the vector */
            vector = s_realloc(vector,((*argc)+1)*sizeof(char*));
            vector[*argc] = current;
            (*argc)++;
            current = NULL;
        } else {
            /* Even on empty input string return something not NULL. */
            if (vector == NULL) vector = s_malloc(sizeof(void*));
            return vector;
        }
    }

err:
    while((*argc)--)
        sdsfree(vector[*argc]);
    s_free(vector);
    if (current) sdsfree(current);
    *argc = 0;
    return NULL;
}

/* Modify the string substituting all the occurrences of the set of
 * characters specified in the 'from' string to the corresponding character
 * in the 'to' array.
 *
 * For instance: sdsmapchars(mystring, "ho", "01", 2)
 * will have the effect of turning the string "hello" into "0ell1".
 *
 * The function returns the sds string pointer, that is always the same
 * as the input pointer since no resize is needed. */
sds sdsmapchars(sds s, const char *from, const char *to, size_t setlen) {
    size_t j, i, l = sdslen(s);

    for (j = 0; j < l; j++) {
        for (i = 0; i < setlen; i++) {
            if (s[j] == from[i]) {
                s[j] = to[i];
                break;
            }
        }
    }
    return s;
}

/* Join an array of C strings using the specified separator (also a C string).
 * Returns the result as an sds string. */
sds sdsjoin(char **argv, int argc, char *sep) {
    sds join = sdsempty();
    int j;

    for (j = 0; j < argc; j++) {
        join = sdscat(join, argv[j]);
        if (j != argc-1) join = sdscat(join,sep);
    }
    return join;
}

/* Like sdsjoin, but joins an array of SDS strings. */
sds sdsjoinsds(sds *argv, int argc, const char *sep, size_t seplen) {
    sds join = sdsempty();
    int j;

    for (j = 0; j < argc; j++) {
        join = sdscatsds(join, argv[j]);
        if (j != argc-1) join = sdscatlen(join,sep,seplen);
    }
    return join;
}

/* Wrappers to the allocators used by SDS. Note that SDS will actually
 * just use the macros defined into sdsalloc.h in order to avoid to pay
 * the overhead of function calls. Here we define these wrappers only for
 * the programs SDS is linked to, if they want to touch the SDS internals
 * even if they use a different allocator. */
void *sds_malloc(size_t size) { return s_malloc(size); }
void *sds_realloc(void *ptr, size_t size) { return s_realloc(ptr,size); }
void sds_free(void *ptr) { s_free(ptr); }

/* Perform expansion of a template string and return the result as a newly
 * allocated sds.
 *
 * Template variables are specified using curly brackets, e.g. {variable}.
 * An opening bracket can be quoted by repeating it twice.
 */
sds sdstemplate(const char *template, sdstemplate_callback_t cb_func, void *cb_arg)
{
    sds res = sdsempty();
    const char *p = template;

    while (*p) {
        /* Find next variable, copy everything until there */
        const char *sv = strchr(p, '{');
        if (!sv) {
            /* Not found: copy till rest of template and stop */
            res = sdscat(res, p);
            break;
        } else if (sv > p) {
            /* Found: copy anything up to the beginning of the variable */
            res = sdscatlen(res, p, sv - p);
        }

        /* Skip into variable name, handle premature end or quoting */
        sv++;
        if (!*sv) goto error;       /* Premature end of template */
        if (*sv == '{') {
            /* Quoted '{' */
            p = sv + 1;
            res = sdscat(res, "{");
            continue;
        }

        /* Find end of variable name, handle premature end of template */
        const char *ev = strchr(sv, '}');
        if (!ev) goto error;

        /* Pass variable name to callback and obtain value. If callback failed,
         * abort. */
        sds varname = sdsnewlen(sv, ev - sv);
        sds value = cb_func(varname, cb_arg);
        sdsfree(varname);
        if (!value) goto error;

        /* Append value to result and continue */
        res = sdscat(res, value);
        sdsfree(value);
        p = ev + 1;
    }

    return res;

error:
    sdsfree(res);
    return NULL;
}

#ifdef REDIS_TEST
#include <stdio.h>
#include <limits.h>
#include "testhelp.h"

#define UNUSED(x) (void)(x)

static sds sdsTestTemplateCallback(sds varname, void *arg) {
    UNUSED(arg);
    static const char *_var1 = "variable1";
    static const char *_var2 = "variable2";

    if (!strcmp(varname, _var1)) return sdsnew("value1");
    else if (!strcmp(varname, _var2)) return sdsnew("value2");
    else return NULL;
}

int sdsTest(int argc, char **argv, int flags) {
    UNUSED(argc);
    UNUSED(argv);
    UNUSED(flags);

    {
        sds x = sdsnew("foo"), y;

        test_cond("Create a string and obtain the length",
            sdslen(x) == 3 && memcmp(x,"foo\0",4) == 0);

        sdsfree(x);
        x = sdsnewlen("foo",2);
        test_cond("Create a string with specified length",
            sdslen(x) == 2 && memcmp(x,"fo\0",3) == 0);

        x = sdscat(x,"bar");
        test_cond("Strings concatenation",
            sdslen(x) == 5 && memcmp(x,"fobar\0",6) == 0);

        x = sdscpy(x,"a");
        test_cond("sdscpy() against an originally longer string",
            sdslen(x) == 1 && memcmp(x,"a\0",2) == 0);

        x = sdscpy(x,"xyzxxxxxxxxxxyyyyyyyyyykkkkkkkkkk");
        test_cond("sdscpy() against an originally shorter string",
            sdslen(x) == 33 &&
            memcmp(x,"xyzxxxxxxxxxxyyyyyyyyyykkkkkkkkkk\0",33) == 0);

        sdsfree(x);
        x = sdscatprintf(sdsempty(),"%d",123);
        test_cond("sdscatprintf() seems working in the base case",
            sdslen(x) == 3 && memcmp(x,"123\0",4) == 0);

        sdsfree(x);
        x = sdscatprintf(sdsempty(),"a%cb",0);
        test_cond("sdscatprintf() seems working with \\0 inside of result",
            sdslen(x) == 3 && memcmp(x,"a\0""b\0",4) == 0);

        {
            sdsfree(x);
            char etalon[1024*1024];
            for (size_t i = 0; i < sizeof(etalon); i++) {
                etalon[i] = '0';
            }
            x = sdscatprintf(sdsempty(),"%0*d",(int)sizeof(etalon),0);
            test_cond("sdscatprintf() can print 1MB",
                sdslen(x) == sizeof(etalon) && memcmp(x,etalon,sizeof(etalon)) == 0);
        }

        sdsfree(x);
        x = sdsnew("--");
        x = sdscatfmt(x, "Hello %s World %I,%I--", "Hi!", LLONG_MIN,LLONG_MAX);
        test_cond("sdscatfmt() seems working in the base case",
            sdslen(x) == 60 &&
            memcmp(x,"--Hello Hi! World -9223372036854775808,"
                     "9223372036854775807--",60) == 0);
        printf("[%s]\n",x);

        sdsfree(x);
        x = sdsnew("--");
        x = sdscatfmt(x, "%u,%U--", UINT_MAX, ULLONG_MAX);
        test_cond("sdscatfmt() seems working with unsigned numbers",
            sdslen(x) == 35 &&
            memcmp(x,"--4294967295,18446744073709551615--",35) == 0);

        sdsfree(x);
        x = sdsnew(" x ");
        sdstrim(x," x");
        test_cond("sdstrim() works when all chars match",
            sdslen(x) == 0);

        sdsfree(x);
        x = sdsnew(" x ");
        sdstrim(x," ");
        test_cond("sdstrim() works when a single char remains",
            sdslen(x) == 1 && x[0] == 'x');

        sdsfree(x);
        x = sdsnew("xxciaoyyy");
        sdstrim(x,"xy");
        test_cond("sdstrim() correctly trims characters",
            sdslen(x) == 4 && memcmp(x,"ciao\0",5) == 0);

        y = sdsdup(x);
        sdsrange(y,1,1);
        test_cond("sdsrange(...,1,1)",
            sdslen(y) == 1 && memcmp(y,"i\0",2) == 0);

        sdsfree(y);
        y = sdsdup(x);
        sdsrange(y,1,-1);
        test_cond("sdsrange(...,1,-1)",
            sdslen(y) == 3 && memcmp(y,"iao\0",4) == 0);

        sdsfree(y);
        y = sdsdup(x);
        sdsrange(y,-2,-1);
        test_cond("sdsrange(...,-2,-1)",
            sdslen(y) == 2 && memcmp(y,"ao\0",3) == 0);

        sdsfree(y);
        y = sdsdup(x);
        sdsrange(y,2,1);
        test_cond("sdsrange(...,2,1)",
            sdslen(y) == 0 && memcmp(y,"\0",1) == 0);

        sdsfree(y);
        y = sdsdup(x);
        sdsrange(y,1,100);
        test_cond("sdsrange(...,1,100)",
            sdslen(y) == 3 && memcmp(y,"iao\0",4) == 0);

        sdsfree(y);
        y = sdsdup(x);
        sdsrange(y,100,100);
        test_cond("sdsrange(...,100,100)",
            sdslen(y) == 0 && memcmp(y,"\0",1) == 0);

        sdsfree(y);
        y = sdsdup(x);
        sdsrange(y,4,6);
        test_cond("sdsrange(...,4,6)",
            sdslen(y) == 0 && memcmp(y,"\0",1) == 0);

        sdsfree(y);
        y = sdsdup(x);
        sdsrange(y,3,6);
        test_cond("sdsrange(...,3,6)",
            sdslen(y) == 1 && memcmp(y,"o\0",2) == 0);

        sdsfree(y);
        sdsfree(x);
        x = sdsnew("foo");
        y = sdsnew("foa");
        test_cond("sdscmp(foo,foa)", sdscmp(x,y) > 0);

        sdsfree(y);
        sdsfree(x);
        x = sdsnew("bar");
        y = sdsnew("bar");
        test_cond("sdscmp(bar,bar)", sdscmp(x,y) == 0);

        sdsfree(y);
        sdsfree(x);
        x = sdsnew("aar");
        y = sdsnew("bar");
        test_cond("sdscmp(bar,bar)", sdscmp(x,y) < 0);

        sdsfree(y);
        sdsfree(x);
        x = sdsnewlen("\a\n\0foo\r",7);
        y = sdscatrepr(sdsempty(),x,sdslen(x));
        test_cond("sdscatrepr(...data...)",
            memcmp(y,"\"\\a\\n\\x00foo\\r\"",15) == 0);

        {
            unsigned int oldfree;
            char *p;
            int i;
            size_t step = 10, j;

            sdsfree(x);
            sdsfree(y);
            x = sdsnew("0");
            test_cond("sdsnew() free/len buffers", sdslen(x) == 1 && sdsavail(x) == 0);

            /* Run the test a few times in order to hit the first two
             * SDS header types. */
            for (i = 0; i < 10; i++) {
                size_t oldlen = sdslen(x);
                x = sdsMakeRoomFor(x,step);
                int type = x[-1]&SDS_TYPE_MASK;

                test_cond("sdsMakeRoomFor() len", sdslen(x) == oldlen);
                if (type != SDS_TYPE_5) {
                    test_cond("sdsMakeRoomFor() free", sdsavail(x) >= step);
                    oldfree = sdsavail(x);
                    UNUSED(oldfree);
                }
                p = x+oldlen;
                for (j = 0; j < step; j++) {
                    p[j] = 'A'+j;
                }
                sdsIncrLen(x,step);
            }
            test_cond("sdsMakeRoomFor() content",
                memcmp("0ABCDEFGHIJABCDEFGHIJABCDEFGHIJABCDEFGHIJABCDEFGHIJABCDEFGHIJABCDEFGHIJABCDEFGHIJABCDEFGHIJABCDEFGHIJ",x,101) == 0);
            test_cond("sdsMakeRoomFor() final length",sdslen(x)==101);

            sdsfree(x);
        }

        /* Simple template */
        x = sdstemplate("v1={variable1} v2={variable2}", sdsTestTemplateCallback, NULL);
        test_cond("sdstemplate() normal flow",
                  memcmp(x,"v1=value1 v2=value2",19) == 0);
        sdsfree(x);

        /* Template with callback error */
        x = sdstemplate("v1={variable1} v3={doesnotexist}", sdsTestTemplateCallback, NULL);
        test_cond("sdstemplate() with callback error", x == NULL);

        /* Template with empty var name */
        x = sdstemplate("v1={", sdsTestTemplateCallback, NULL);
        test_cond("sdstemplate() with empty var name", x == NULL);

        /* Template with truncated var name */
        x = sdstemplate("v1={start", sdsTestTemplateCallback, NULL);
        test_cond("sdstemplate() with truncated var name", x == NULL);

        /* Template with quoting */
        x = sdstemplate("v1={{{variable1}} {{} v2={variable2}", sdsTestTemplateCallback, NULL);
        test_cond("sdstemplate() with quoting",
                  memcmp(x,"v1={value1} {} v2=value2",24) == 0);
        sdsfree(x);

        /* Test sdsresize - extend */
        x = sdsnew("1234567890123456789012345678901234567890");
        x = sdsResize(x, 200);
        test_cond("sdsrezie() expand len", sdslen(x) == 40);
        test_cond("sdsrezie() expand strlen", strlen(x) == 40);
        test_cond("sdsrezie() expand alloc", sdsalloc(x) == 200);
        /* Test sdsresize - trim free space */
        x = sdsResize(x, 80);
        test_cond("sdsrezie() shrink len", sdslen(x) == 40);
        test_cond("sdsrezie() shrink strlen", strlen(x) == 40);
        test_cond("sdsrezie() shrink alloc", sdsalloc(x) == 80);
        /* Test sdsresize - crop used space */
        x = sdsResize(x, 30);
        test_cond("sdsrezie() crop len", sdslen(x) == 30);
        test_cond("sdsrezie() crop strlen", strlen(x) == 30);
        test_cond("sdsrezie() crop alloc", sdsalloc(x) == 30);
        /* Test sdsresize - extend to different class */
        x = sdsResize(x, 400);
        test_cond("sdsrezie() expand len", sdslen(x) == 30);
        test_cond("sdsrezie() expand strlen", strlen(x) == 30);
        test_cond("sdsrezie() expand alloc", sdsalloc(x) == 400);
        /* Test sdsresize - shrink to different class */
        x = sdsResize(x, 4);
        test_cond("sdsrezie() crop len", sdslen(x) == 4);
        test_cond("sdsrezie() crop strlen", strlen(x) == 4);
        test_cond("sdsrezie() crop alloc", sdsalloc(x) == 4);
        sdsfree(x);
    }
    return 0;
}
#endif
