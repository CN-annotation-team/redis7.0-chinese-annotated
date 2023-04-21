/* 字典的实现 (使用哈希表)
 *
 * 本文件通过内存哈希表实现了字典中 插入/删除/替换/查找/获取随机元素操作 的操作.
 * 哈希表的大小会在哈希表充满时以 2 的倍数扩充 (256, 512, 1024),
 * 并使用链表解决 hash 冲突.
 * 阅读源码以获得更多信息. :)
 */

/* Hash Tables Implementation.
 *
 * This file implements in-memory hash tables with insert/del/replace/find/
 * get-random-element operations. Hash tables will auto-resize if needed
 * tables of power of two in size are used, collisions are handled by
 * chaining. See the source code for more information... :)
 *
 * Copyright (c) 2006-2012, Salvatore Sanfilippo <antirez at gmail dot com>
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

#ifndef __DICT_H
#define __DICT_H

#include "mt19937-64.h"
#include <limits.h>
#include <stdint.h>
#include <stdlib.h>

#define DICT_OK 0
#define DICT_ERR 1

typedef struct dictEntry {
    /* void * 类型的 key，可以指向任意类型的键 */
    void *key;
    /* 联合体 v 中包含了指向实际值的指针 *val、无符号的 64 位整数、有符号的 64 位整数，以及 double 双精度浮点数。
     * 这是一种节省内存的方式，因为当值为整数或者双精度浮点数时，由于它们本身就是 64 位的，void *val 指针也是占用 64 位（64 操作系统下），
     * 所以它们可以直接存在键值对的结构体中，避免再使用一个指针，从而节省内存开销（8 个字节）
     * 当然也可以是 void *，存储任何类型的数据，最早 redis1.0 版本就只是 void* */
    union {
        void *val;
        uint64_t u64;
        int64_t s64;
        double d;
    } v;
    struct dictEntry *next;     /* Next entry in the same hash bucket. */
                                /* 同一个 hash 桶中的下一个条目.
                                 * 通过形成一个链表解决桶内的哈希冲突. */
    void *metadata[];           /* An arbitrary number of bytes (starting at a
                                 * pointer-aligned address) of size as returned
                                 * by dictType's dictEntryMetadataBytes(). */
                                /* 一块任意长度的数据 (按 void* 的大小对齐),
                                 * 具体长度由 'dictType' 中的
                                 * dictEntryMetadataBytes() 返回. */
} dictEntry;

typedef struct dict dict;

/* 字典类型，因为我们会将字典用在各个地方，例如键空间、过期字典等等等，只要是想用字典（哈希表）的场景都可以用
 * 这样的话每种类型的字典，它对应的 key / value 肯定类型是不一致的，这就需要有一些自定义的方法，例如键值对复制、析构等 */
typedef struct dictType {
    /* 字典里哈希表的哈希算法，目前使用的是基于 DJB 实现的字符串哈希算法
     * 比较出名的有 siphash，redis 4.0 中引进了它。3.0 之前使用的是 DJBX33A，3.0 - 4.0 使用的是 MurmurHash2 */
    uint64_t (*hashFunction)(const void *key);
    /* 键拷贝 */
    void *(*keyDup)(dict *d, const void *key);
    /* 值拷贝 */
    void *(*valDup)(dict *d, const void *obj);
    /* 键比较 */
    int (*keyCompare)(dict *d, const void *key1, const void *key2);
    /* 键析构 */
    void (*keyDestructor)(dict *d, void *key);
    /* 值析构 */
    void (*valDestructor)(dict *d, void *obj);
    /* 字典里的哈希表是否允许扩容 */
    int (*expandAllowed)(size_t moreMem, double usedRatio);
    /* Allow a dictEntry to carry extra caller-defined metadata.  The
     * extra memory is initialized to 0 when a dictEntry is allocated. */
    /* 允许调用者向条目 (dictEntry) 中添加额外的元信息.
     * 这段额外信息的内存会在条目分配时被零初始化. */
    size_t (*dictEntryMetadataBytes)(dict *d);
} dictType;

/* 通过指数计算哈希表的大小，见下面 exp，哈希表大小目前是严格的 2 的幂 */
#define DICTHT_SIZE(exp) ((exp) == -1 ? 0 : (unsigned long)1<<(exp))
/* 计算掩码，哈希表的长度 - 1，用于计算键在哈希表中的位置（下标索引） */
#define DICTHT_SIZE_MASK(exp) ((exp) == -1 ? 0 : (DICTHT_SIZE(exp))-1)

/* 7.0 版本之前的字典结构

typedef struct dictht {
    dictEntry **table; // 8 bytes
    unsigned long size; // 8 bytes
    unsigned long sizemask; // 8 bytes
    unsigned long used; // 8 bytes
} dictht;

typedef struct dict {
    dictType *type; // 8 bytes
    void *privdata; // 8 bytes
    dictht ht[2]; // 32 bytes * 2 = 64 bytes
    long rehashidx; // 8 bytes
    int16_t pauserehash; // 2 bytes
} dict;
 *
 * 做的优化大概是这样的：
 * 1. 从字典结构里删除 privdata （这个扩展其实一直是个 dead code，会影响很多行，社区里的做法都是想尽量减少 diff 变更，避免说破坏 git blame log）
 * 2. 将 dictht 字典哈希表结构融合进 dict 字典结构里，相关元数据直接放到了 dict 中
 * 3. 去掉 sizemark 字段，这个值可以通过 size - 1 计算得到，这样就可以少 8 字节
 * 4. 将 size 字段转变为 size_exp（就是 2 的 n 次方，指数），因为 size 目前是严格都是 2 的幂，size_exp 存储指数而不是具体数值，size 内存占用从 8 字节降到了 1 字节
 *
 * 内存方面：
 *   默认情况下通过 sizeof 我们是可以看到新 dict 是 56 个字节
 *   dict：一个指针 + 两个指针 + 两个 unsigned long + 一个 long + 一个 int16_t + 两个 char，总共实际上是 52 个字节，但是因为 jemalloc 内存分配机制，实际会分配 56 个字节
 *   而实际上因为对齐，最后的 int16_t pauserehash 和 char ht_size_exp[2] 加起来是占用 8 个字节，代码注释也有说，将小变量放到最后来获得最小的填充。
 */

struct dict {
    /* 字典类型，8 bytes */
    dictType *type;
    /* 字典中使用了两个哈希表,
     * (看看那些以 'ht_' 为前缀的成员, 它们都是一个长度为 2 的数组)
     *
     * 我们可以将它们视为
     * struct{
     *   ht_table[2];
     *   ht_used[2];
     *   ht_size_exp[2];
     * } hash_table[2];
     * 为了优化字典的内存结构,
     * 减少对齐产生的空洞,
     * 我们将这些数据分散于整个结构体中.
     *
     * 平时只使用下标为 0 的哈希表.
     * 当需要进行 rehash 时 ('rehashidx' != -1),
     * 下标为 1 的一组数据会作为一组新的哈希表,
     * 渐进地进行 rehash 避免一次性 rehash 造成长时间的阻塞.
     * 当 rehash 完成时, 将新的哈希表置入下标为 0 的组别中,
     * 同时将 'rehashidx' 置为 -1.
     */
    dictEntry **ht_table[2];
    /* 哈希表存储的键数量，它与哈希表的大小 size 的比值就是 load factor 负载因子，
     * 值越大说明哈希碰撞的可能性也越大，字典的平均查找效率也越低
     * 理论上负载因子 <=1 的时候，字典能保持平均 O(1) 的时间复杂度查询
     * 当负载因子等于哈希表大小的时候，说明哈希表退化成链表了，此时查询的时间复杂度退化为 O(N)
     * redis 会监控字典的负载因子，在负载因子变大的时候，会对哈希表进行扩容，后面会提到的渐进式 rehash */
    unsigned long ht_used[2];

    long rehashidx; /* rehashing not in progress if rehashidx == -1 */
                    /* rehash 的进度.
                     * 如果此变量值为 -1, 则当前未进行 rehash. */
    /* Keep small vars at end for optimal (minimal) struct padding */
    /* 将小尺寸的变量置于结构体的尾部, 减少对齐产生的额外空间开销. */
    int16_t pauserehash; /* If >0 rehashing is paused (<0 indicates coding error) */
                         /* 如果此变量值 >0 表示 rehash 暂停
                          * (<0 表示编写的代码出错了). */
    /* 存储哈希表大小的指数表示，通过这个可以直接计算出哈希表的大小，例如 exp = 10, size = 2 ** 10
     * 能避免说直接存储 size 的实际值，以前 8 字节存储的数值现在变成 1 字节进行存储 */
    signed char ht_size_exp[2]; /* exponent of size. (size = 1<<exp) */
                                /* 哈希表大小的指数表示.
                                 * (以 2 为底, 大小 = 1 << 指数) */
};

/* If safe is set to 1 this is a safe iterator, that means, you can call
 * dictAdd, dictFind, and other functions against the dictionary even while
 * iterating. Otherwise it is a non safe iterator, and only dictNext()
 * should be called while iterating. */
/* 如果 'safe' 为 1, 表明这是一个安全的迭代器,
 * 你可以在遍历哈希表时进行插入, 查找和其它操作, 而不用担心迭代器失效.
 * 如果 'safe' 不为 1, 则是不安全的迭代器, 在遍历时应该只调用 dictNext(). */
typedef struct dictIterator {
    dict *d;
    long index;
    int table, safe;
    dictEntry *entry, *nextEntry;
    /* unsafe iterator fingerprint for misuse detection. */
    /* 指纹, 用于检查不安全迭代器的误用. */
    unsigned long long fingerprint;
} dictIterator;

typedef void (dictScanFunction)(void *privdata, const dictEntry *de);
typedef void (dictScanBucketFunction)(dict *d, dictEntry **bucketref);

/* This is the initial size of every hash table */
/* 此处定义了每一个哈希表的初始大小: 4 (1<<2). */
#define DICT_HT_INITIAL_EXP      2
#define DICT_HT_INITIAL_SIZE     (1<<(DICT_HT_INITIAL_EXP))

/* ------------------------------- Macros ------------------------------------*/
/* ------------------------------- 宏定义 ------------------------------------*/
#define dictFreeVal(d, entry) \
    if ((d)->type->valDestructor) \
        (d)->type->valDestructor((d), (entry)->v.val)

#define dictSetVal(d, entry, _val_) do { \
    if ((d)->type->valDup) \
        (entry)->v.val = (d)->type->valDup((d), _val_); \
    else \
        (entry)->v.val = (_val_); \
} while(0)

#define dictSetSignedIntegerVal(entry, _val_) \
    do { (entry)->v.s64 = _val_; } while(0)

#define dictSetUnsignedIntegerVal(entry, _val_) \
    do { (entry)->v.u64 = _val_; } while(0)

#define dictSetDoubleVal(entry, _val_) \
    do { (entry)->v.d = _val_; } while(0)

#define dictFreeKey(d, entry) \
    if ((d)->type->keyDestructor) \
        (d)->type->keyDestructor((d), (entry)->key)

/* 字典在给 entry 实体 key 赋值时会检查该字典是否有设置键拷贝方法 */
#define dictSetKey(d, entry, _key_) do { \
    if ((d)->type->keyDup) \
        (entry)->key = (d)->type->keyDup((d), _key_); \
    else \
        (entry)->key = (_key_); \
} while(0)

#define dictCompareKeys(d, key1, key2) \
    (((d)->type->keyCompare) ? \
        (d)->type->keyCompare((d), key1, key2) : \
        (key1) == (key2))

#define dictMetadata(entry) (&(entry)->metadata)
#define dictMetadataSize(d) ((d)->type->dictEntryMetadataBytes \
                             ? (d)->type->dictEntryMetadataBytes(d) : 0)

#define dictHashKey(d, key) (d)->type->hashFunction(key)
#define dictGetKey(he) ((he)->key)
#define dictGetVal(he) ((he)->v.val)
#define dictGetSignedIntegerVal(he) ((he)->v.s64)
#define dictGetUnsignedIntegerVal(he) ((he)->v.u64)
#define dictGetDoubleVal(he) ((he)->v.d)
#define dictSlots(d) (DICTHT_SIZE((d)->ht_size_exp[0])+DICTHT_SIZE((d)->ht_size_exp[1]))
#define dictSize(d) ((d)->ht_used[0]+(d)->ht_used[1])
#define dictIsRehashing(d) ((d)->rehashidx != -1)
#define dictPauseRehashing(d) (d)->pauserehash++
#define dictResumeRehashing(d) (d)->pauserehash--

/* If our unsigned long type can store a 64 bit number, use a 64 bit PRNG. */
/* 如果 unsigned long 能够存储 64位的整数,
 * 则使用 64位的伪随机数生成器(PseudoRandom number generator, PRNG). */
#if ULONG_MAX >= 0xffffffffffffffff
#define randomULong() ((unsigned long) genrand64_int64())
#else
#define randomULong() random()
#endif

/* API */
dict *dictCreate(dictType *type);
int dictExpand(dict *d, unsigned long size);
int dictTryExpand(dict *d, unsigned long size);
int dictAdd(dict *d, void *key, void *val);
dictEntry *dictAddRaw(dict *d, void *key, dictEntry **existing);
dictEntry *dictAddOrFind(dict *d, void *key);
int dictReplace(dict *d, void *key, void *val);
int dictDelete(dict *d, const void *key);
dictEntry *dictUnlink(dict *d, const void *key);
void dictFreeUnlinkedEntry(dict *d, dictEntry *he);
void dictRelease(dict *d);
dictEntry * dictFind(dict *d, const void *key);
void *dictFetchValue(dict *d, const void *key);
int dictResize(dict *d);
dictIterator *dictGetIterator(dict *d);
dictIterator *dictGetSafeIterator(dict *d);
dictEntry *dictNext(dictIterator *iter);
void dictReleaseIterator(dictIterator *iter);
dictEntry *dictGetRandomKey(dict *d);
dictEntry *dictGetFairRandomKey(dict *d);
unsigned int dictGetSomeKeys(dict *d, dictEntry **des, unsigned int count);
void dictGetStats(char *buf, size_t bufsize, dict *d);
uint64_t dictGenHashFunction(const void *key, size_t len);
uint64_t dictGenCaseHashFunction(const unsigned char *buf, size_t len);
void dictEmpty(dict *d, void(callback)(dict*));
void dictEnableResize(void);
void dictDisableResize(void);
int dictRehash(dict *d, int n);
int dictRehashMilliseconds(dict *d, int ms);
void dictSetHashFunctionSeed(uint8_t *seed);
uint8_t *dictGetHashFunctionSeed(void);
unsigned long dictScan(dict *d, unsigned long v, dictScanFunction *fn, dictScanBucketFunction *bucketfn, void *privdata);
uint64_t dictGetHash(dict *d, const void *key);
dictEntry **dictFindEntryRefByPtrAndHash(dict *d, const void *oldptr, uint64_t hash);

#ifdef REDIS_TEST
int dictTest(int argc, char *argv[], int flags);
#endif

#endif /* __DICT_H */
