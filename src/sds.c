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
    return -1; /* 该值和 SDS_TYPE_64 或 SDS_TYPE_32 类型的最大值相等 */
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
 * 不过, sds 字符串本身是二进制安全的, 中间可以存储 \0, 因为字符串的实际长度是存储在 sds header 中 */
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

/* 创建一个空的 (长度为 0) sds 字符串. 即使在这种情况下, 字符串也总是有一个隐式的空终结符项. */
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

/* 自动扩容机制 - 扩大 sds 字符串末端的空闲空间,
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

    /* 剩余空间大于等于新增空间，无需扩容，直接返回原字符串 - 空间足够时的优化 */
    if (avail >= addlen) return s;

    len = sdslen(s);
    sh = (char*)s-sdsHdrSize(oldtype);
    reqlen = newlen = (len+addlen);
    assert(newlen > len);   /* 处理 size_t 溢出 */
    if (greedy == 1) {
        /* 新增后长度小于 1MB ，则按新长度的两倍扩容 */
        if (newlen < SDS_MAX_PREALLOC)
            newlen *= 2;
        else
            /* 新增后长度大于 1MB ，则按新长度加上 1MB 扩容 */
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

/* 再分配 sds 字符串, 分配目的是移除未使用的 buf 尾部空闲空间，真正释放 SDS 未使用空间.
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

/* 增加该 sds 字符串的长度, 同时根据 'incr' 缩短尾部空闲空间的大小.
 * 该函数的调用也会在字符串的尾部设置 null 终结符.
 *
 * 在用户调用 sdsMakeRoomFor() 函数并追加部分内容到字符串后, 需要为其设置新的长度.
 * 该函数主要作用便是在上述场景中修复字符串的长度.
 *
 * 注意: 可以使用负增长来从右侧对字符串进行裁剪
 *
 * 使用示例:
 *
 * 使用 sdsIncrLen() 和 sdsMakeRoomFor() 函数时, 可以通过以下范式,
 * 在无需中间缓冲区的情况下, 将来自内核的字节拼接到一个 sds 字符串的尾部:
 *
 * oldlen = sdslen(s);
 * s = sdsMakeRoomFor(s, BUFFER_SIZE);
 * nread = read(fd, s+oldlen, BUFFER_SIZE);
 * ... 检查 nread <= 0 然后处理它 ...
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
        default: len = 0; /* 这里是为了避免编译时的警告信息. */
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

/* 和 sdsll2str() 完全相同, 只不过这是用于无符号数类型的版本. */
int sdsull2str(char *s, unsigned long long v) {
    char *p, aux;
    size_t l;

    /* 将其转换成字符串, 该方法产生的字符串是倒序的 */
    p = s;
    do {
        *p++ = '0'+(v%10);
        v /= 10;
    } while(v);

    /* 计算字符串长度, 并添加空终结符. */
    l = p-s;
    *p = '\0';

    /* 将字符串翻转. */
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

/* 从一个 long long 类型的数值中生成一个字符串. 这个方法比下列方式快很多:
 *
 * sdscatprintf(sdsempty(),"%lld\n", value);
 */
sds sdsfromlonglong(long long value) {
    char buf[SDS_LLSTR_SIZE];
    int len = sdsll2str(buf,value);

    return sdsnewlen(buf,len);
}

/* 该函数和 sdscatprintf() 几乎一样, 除了接受的是 va_list 而不是一个变参列表. */
sds sdscatvprintf(sds s, const char *fmt, va_list ap) {
    va_list cpy;
    char staticbuf[1024], *buf = staticbuf, *t;
    size_t buflen = strlen(fmt)*2;
    int bufstrlen;

    /* 我们正在尝试使用静态数组来提升处理速度.
     * 如果失败的话, 我们会换回在堆上进行分配. */
    if (buflen > sizeof(staticbuf)) {
        buf = s_malloc(buflen);
        if (buf == NULL) return NULL;
    } else {
        buflen = sizeof(staticbuf);
    }

    /* 如果当前字符串空间不能满足需求, 为缺失的空间和尾部的 \0 终结符分配空间 */
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

    /* 在最后, 拼接获取到的字符串到 sds 字符串中, 然后将其返回. */
    t = sdscatlen(s, buf, bufstrlen);
    if (buf != staticbuf) s_free(buf);
    return t;
}

/* 在 sds 字符串 's' 后面追加一个字符串, 这个字符串是通过类似于 printf 函数那样的格式说明符得到的
 *
 * 在本次函数调用后, 被修改的 sds 字符串不再有效, 所有对其引用的指针都要替换成本次函数返回的新指针.
 *
 * 示例:
 *
 * s = sdsnew("Sum is: ");
 * s = sdscatprintf(s,"%d+%d = %d",a,b,a+b).
 *
 * 通常, 你需要使用类 printf 格式新建一个字符串.
 * 当需要这种场景时, 可以使用 sdsempty() 函数的返回值作为目标字符串
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

/* 这个函数和 sdscatprintf 很相似, 但是比它快很多. 因为该函数不依赖于 sprintf() 函数体系.
 * 那个函数体系是由 libc 实现的, 而 libc 通常很慢.
 * 在此之外, 在拼接新数据时直接处理字符串也带来了一定的性能提升.
 *
 * 不过, 这个函数不兼容类 printf 格式说明符, 只能处理一些子集.
 *
 * %s - C 风格字符串
 * %S - SDS 字符串
 * %i - 有符号整型
 * %I - 64 bit signed integer (long long, int64_t)
 * %I - 64 位有符号整型 (long long, int64_t)
 * %u - 无符号整型
 * %U - 64 位无符号整型 (unsigned long long, uint64_t)
 * %% - 用来表示 "%" 字符.
 */
sds sdscatfmt(sds s, char const *fmt, ...) {
    size_t initlen = sdslen(s);
    const char *f = fmt;
    long i;
    va_list ap;

    /* 为了避免连续进行内存分配, 再开始的时候就使用一个至少可以容纳两倍格式字符串的缓存空间
     * 这可能不是目前最好的, 但是看上去也很高效. */
    s = sdsMakeRoomFor(s, strlen(fmt)*2);
    va_start(ap,fmt);
    f = fmt;    /* 下一个需要处理的格式说明字节. */
    i = initlen; /* 下一个需要写入的字节. */
    while(*f) {
        char next, *str;
        size_t l;
        long long num;
        unsigned long long unum;

        /* 确保至少有一字符的空间. */
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
            default: /* 处理 %% 和其他 %<unknown>. */
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

    /* 添加 null 终结符 */
    s[i] = '\0';
    return s;
}

/* 在字符串的左右两边开始移出字符, 直到出现第一个不存在于 'cset' 中的字符.
 * 'cset' 是一个以 null 结尾的 C 风格字符串.
 *
 * 调用之后, 被修改过的sds字符串将不再有效, 所有对其的引用都必须被替换为本次调用返回的新指针.
 *
 * 演示代码:
 *
 * s = sdsnew("AA...AA.a.aa.aHelloWorld     :::");
 * s = sdstrim(s,"Aa. :");
 * printf("%s\n", s);
 *
 * 这段代码最终的输出为 "HelloWorld".
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
    /* 仅仅更新了 len 而没进行实际的内存释放（惰性空间释放），真正释放可以看函数 sdsRemoveFreeSpace */
    sdssetlen(s,len);
    return s;
}

/* 把字符串缩短为它的子集.
 * 这个函数并不会释放字符串中的空闲内存, 所以最好再之后调用 sdsRemoveFreeSpace 函数. */
void sdssubstr(sds s, size_t start, size_t len) {
    /* 限制缩小范围 */
    size_t oldlen = sdslen(s);
    if (start >= oldlen) start = len = 0;
    if (len > oldlen-start) len = oldlen-start;

    /* 移出数据 */
    if (len) memmove(s, s+start, len);
    s[len] = 0;
    sdssetlen(s,len);
}

/* 根据 'start' 和 'end' 下标缩小字符串, 使字符串只包含这部分子集.
 *
 * start 和 end 可以是负数, 此时 -1 代表是字符串的最后一个字符, -2 代表倒数第二个, 以此类推.
 *
 * 这段区间是闭区间, 所以 start 和 end 处的字符也会成为结果字符串的一部分.
 *
 * 这个字符串是被原地修改的.
 *
 * 注意: 这个函数有一处容易被误解, 容易导致发生一些意想不到的事情.
 * 尤其是当您想把新字符串的长度设为 0 时, 此时如果 start == end, 结果字符串仍有一个字符.
 * 此时请使用 sdssubstr 函数来代替.
 *
 * 代码示例:
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

/* 对 sds字符串 's' 中的每个字符应用 tolower() 函数. */
void sdstolower(sds s) {
    size_t len = sdslen(s), j;

    for (j = 0; j < len; j++) s[j] = tolower(s[j]);
}

/* 对 sds字符串 's' 中的每个字符应用 toupper() 函数. */
void sdstoupper(sds s) {
    size_t len = sdslen(s), j;

    for (j = 0; j < len; j++) s[j] = toupper(s[j]);
}

/* 使用 memcmp() 比较两个 sds字符串 s1 和 s2.
 *
 * 返回值:
 *
 *     如果 s1 > s2 返回正数.
 *     如果 s1 < s2 返回负数.
 *     如果 s1 和 s2 完全相同, 返回 0.
 *
 * 如果两个字符串拥有同样的前缀, 但其中一个含有更多的字符, 那么认为, 长字符比短字符更大. */
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

/* 使用 'sep' 中的分隔符对 's' 进行分割.
 * 返回一个 sds字符串的数组.
 * *count 中的值将被设置为返回的的词组的数量.
 *
 * 在内存不足, 或者字符串长度为 0, 没有分隔符时, 返回 NULL.
 *
 * 注意: 在分割字符串时, 'sep' 可以是多字符的分隔符.
 * 例如, sdssplit("foo_-_bar","_-_");
 * 将会返回两个元素 "foo" 和 "bar".
 *
 * 该版本的这个函数是类型安全的, 但是也需要 length 作为参数.
 * sdssplit() 函数与该函数相同, 只不过是为使用 0 作为终结符的字符串准备的.
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
        /* 确保留有下一个元素和最后一个元素的空间 */
        if (slots < elements+2) {
            sds *newtokens;

            slots *= 2;
            newtokens = s_realloc(tokens,sizeof(sds)*slots);
            if (newtokens == NULL) goto cleanup;
            tokens = newtokens;
        }
        /* 查找分割符 */
        if ((seplen == 1 && *(s+j) == sep[0]) || (memcmp(s+j,sep,seplen) == 0)) {
            tokens[elements] = sdsnewlen(s+start,j-start);
            if (tokens[elements] == NULL) goto cleanup;
            elements++;
            start = j+seplen;
            j = j+seplen-1; /* 越过这个分隔符 */
        }
    }
    /* 添加最后一个元素. 已确保在这个符号数组中仍有空间. */
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

/* 释放 sdssplitlen() 函数调用所产生的字符串, 如果 'tokens' 为 NULL 的话, 不采取任何动作. */
void sdsfreesplitres(sds *tokens, int count) {
    if (!tokens) return;
    while(count--)
        sdsfree(tokens[count]);
    s_free(tokens);
}

/* 在 sds字符串 "s" 后面追加一个转义字符串表达形式, 其中所有不可打印的字符 (可以通过 isprint() 函数检测)
 * 会被转换成 "\n\r\a...." 或者 "\x<hex-number>" 的转义形式.
 *
 * 该函数调用之后, 传入的 sds 字符串不再有效, 所有相关引用都必须被替换为调用后返回的新指针. */
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

/* 如果字符串包含要被 sdscatrepr() 转义的字符, 则返回 1, 否则返回 0.
 *
 * 一个典型例子, 这应该用于帮助保护聚合字符串, 这种方式要与 sdssplitargs() 函数兼容.
 * 出于这个原因, 空格也将被视为需要转义.
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

/* sdssplitargs() 的辅助函数, 如果 'c' 是合法的十六进制数的话, 返回非零值 */
int is_hex_digit(char c) {
    return (c >= '0' && c <= '9') || (c >= 'a' && c <= 'f') ||
           (c >= 'A' && c <= 'F');
}

/* sdssplitargs() 的辅助函数, 把一个十六进制数转换为 0 到 15 的十进制数 */
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

/* 在一行中分割出参数, 每一个参数都应有类似编程语言 REPL 的格式.
 *
 * foo bar "newline are supported\n" 和 "\xff\x00otherstuff"
 *
 * 参数的个数存储在 *argc 中, 之后会返回一个 sds 的数组.
 *
 * 调用者应该通过 sdsfreesplitres() 函数释放返回的字符串的空间.
 *
 * 注意, sdscatrepr() 函数可以将字符串转换回 sdssplitargs() 函数能够解析的引号字符串的格式.
 *
 * 如果成功, 函数返回已分配好空间的解析出的符号们, 就算输入的字符串为空或者是 NULL,
 * 或者存在双引号不匹配的情况, 或双引号字符串后面有未被空格分隔的字符.
 * 就像这样: "foo"bar or "foo'
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
            int inq=0;  /* 设为 1 如果在 "quotes" 中 */
            int insq=0; /* 设为 1 如果在 'single quotes' 中 */
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
                        /* 关闭的引号后面必须有一个空格, 或者没有根本后面. */
                        if (*(p+1) && !isspace(*(p+1))) goto err;
                        done=1;
                    } else if (!*p) {
                        /* 未终止的双引号 */
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
            /* 把符号加入数组 */
            vector = s_realloc(vector,((*argc)+1)*sizeof(char*));
            vector[*argc] = current;
            (*argc)++;
            current = NULL;
        } else {
            /* 即使输入为空字符串, 也不能返回 NULL. */
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

/* 修改字符串, 将' from' 字符串中指定的所有字符集合替换为 'to' 数组中相应的字符.
 *
 * 例如: sdsmapchars(mystring, "ho", "01", 2)
 * 最终, 字符串 "hello" 被转化成 "0ell1".
 *
 * 这个函数会返回 sds字符串 的指针, 如果中间没有发生再分配, 这个指针和传入的指针相同. */
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

/* 使用指定的分隔符 (也是 C字符串 ) 拼接 C字符串 的数组. 返回的是 sds字符串. */
sds sdsjoin(char **argv, int argc, char *sep) {
    sds join = sdsempty();
    int j;

    for (j = 0; j < argc; j++) {
        join = sdscat(join, argv[j]);
        if (j != argc-1) join = sdscat(join,sep);
    }
    return join;
}

/* 和 sdsjoin 函数很像, 但这个函数拼接的是 sds字符串. */
sds sdsjoinsds(sds *argv, int argc, const char *sep, size_t seplen) {
    sds join = sdsempty();
    int j;

    for (j = 0; j < argc; j++) {
        join = sdscatsds(join, argv[j]);
        if (j != argc-1) join = sdscatlen(join,sep,seplen);
    }
    return join;
}

/* 包装 SDS 所使用的分配器. 注意, SDS将只会使用定义在 sdsalloc.h 中的宏, 目的是减少函数调用的开销.
 * 我们在这定义装饰器只是为了那些 SDS 被链接的程序, 如果那些程序想要触及 SDS 内部的话,
 * 虽然他们使用了不同的分配器. */
void *sds_malloc(size_t size) { return s_malloc(size); }
void *sds_realloc(void *ptr, size_t size) { return s_realloc(ptr,size); }
void sds_free(void *ptr) { s_free(ptr); }

/* 对模板字符串进行展开, 并将结果作为新分配的 sds 返回.
 *
 * 模板字符串由花括号指定, 例如 {variable}.
 * 开括号可通过重复两次来引用.
 */
sds sdstemplate(const char *template, sdstemplate_callback_t cb_func, void *cb_arg)
{
    sds res = sdsempty();
    const char *p = template;

    while (*p) {
        /* 寻找下一个变量, 拷贝到那之前的一切字符 */
        const char *sv = strchr(p, '{');
        if (!sv) {
            /* 未找到: 拷贝剩余模板之后停止 */
            res = sdscat(res, p);
            break;
        } else if (sv > p) {
            /* 找到: 复制变量开头之前的所有内容 */
            res = sdscatlen(res, p, sv - p);
        }

        /* 切换到变量名, 处理提前结束或引用 */
        sv++;
        if (!*sv) goto error;       /* 模板提前结束 */
        if (*sv == '{') {
            /* 被引用的 '{' */
            p = sv + 1;
            res = sdscat(res, "{");
            continue;
        }

        /* 找到变量名的末尾, 处理模板的过早结束 */
        const char *ev = strchr(sv, '}');
        if (!ev) goto error;

        /* 将变量名传递给回调函数并获取值. 如果回调失败, 那么终止. */
        sds varname = sdsnewlen(sv, ev - sv);
        sds value = cb_func(varname, cb_arg);
        sdsfree(varname);
        if (!value) goto error;

        /* 把值附加到结果后面, 继续运行 */
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
