/* 7.0前的旧 dict 字典结构
 *    |展开
 * ht[2] 属性 -> dictht 结构，两个哈希表，0 号 ht[0] 和 1 号 ht[1]，实际数据在 ht[0] 中存储，rehash 时才用到 ht[1] 辅助
 *    |展开
 * table 属性 -> dictEntry 结构，哈希节点
 *    |展开
 * bucket 桶，一个 bucket 对应哈希表数组中的一条 entry 链表。
 * hash table 由多个 bucket 组成，bucket 以 hash key 为索引，每个 bucket 中存放着所有 hash key 相同的 key-value（解决 hash 冲突的方法：链地址法，即拉链法）
 *    |展开
 * key-value 链表
 * 
 * 7.0已经没有 dictht 结构
 * 结构逻辑链：
 * dict
 *   |
 * dictEntry
 *   |
 * bucket
 *   |
 * key-value 
 */

/* Hash Tables Implementation.
 * 字典的实现 (使用哈希表)
 *
 * This file implements in memory hash tables with insert/del/replace/find/
 * get-random-element operations. Hash tables will auto resize if needed
 * tables of power of two in size are used, collisions are handled by
 * chaining. See the source code for more information... :)
 * 
 * 本文件通过内存哈希表实现了字典中 插入 / 删除 / 替换 / 查找 / 获取随机元素 的操作.
 * 哈希表的大小会在哈希表充满时以 2 的倍数扩充 (256, 512, 1024),
 * 并使用链表解决 hash 冲突.
 * 阅读源码以获得更多信息. :)
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

#include "fmacros.h"

#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <string.h>
#include <stdarg.h>
#include <limits.h>
#include <sys/time.h>

#include "dict.h"
#include "zmalloc.h"
#include "redisassert.h"

/* Using dictEnableResize() / dictDisableResize() we make possible to
 * enable/disable resizing of the hash table as needed. This is very important
 * for Redis, as we use copy-on-write and don't want to move too much memory
 * around when there is a child performing saving operations.
 * 
 * 通过 dictEnableResize() 和 dictDisableResize() 两个函数，
 * 程序可以手动地允许或阻止哈希表进行 rehash ，
 * 这在 Redis 使用子进程进行保存操作时，可以有效地利用 copy-on-write 机制。
 *
 * Note that even when dict_can_resize is set to 0, not all resizes are
 * prevented: a hash table is still allowed to grow if the ratio between
 * the number of elements and the buckets > dict_force_resize_ratio. 
 * 
 * 需要注意的是，并非所有 rehash 都会被 dictDisableResize 阻止：
 * 如果已使用节点的数量和字典大小之间的比率，
 * 大于字典强制 rehash 比率 dict_force_resize_ratio ，
 * 那么 rehash 仍然会（强制）进行。*/

// 指示字典是否启用 rehash 的标识
static int dict_can_resize = 1; // dictEnableResize 中置 1   dictDisableResize 中置 0

// 强制 rehash 的比率 
// 1）总的元素个数 除 DICT 桶的个数得到每个桶平均存储的元素个数 (pre_num),如果 pre_num > dict_force_resize_ratio,就会触发 dict 扩大操作。dict_force_resize_ratio = 5。
static unsigned int dict_force_resize_ratio = 5;

/* -------------------------- private prototypes ---------------------------- */

static int _dictExpandIfNeeded(dict *d);
static signed char _dictNextExp(unsigned long size);
static long _dictKeyIndex(dict *d, const void *key, uint64_t hash, dictEntry **existing);
static int _dictInit(dict *d, dictType *type);

/* -------------------------- hash functions -------------------------------- */

static uint8_t dict_hash_function_seed[16];

void dictSetHashFunctionSeed(uint8_t *seed) {
    memcpy(dict_hash_function_seed,seed,sizeof(dict_hash_function_seed));
}

uint8_t *dictGetHashFunctionSeed(void) {
    return dict_hash_function_seed;
}

/* The default hashing function uses SipHash implementation
 * in siphash.c. */
/* 默认的哈希函数使用 SipHash 实现 */

uint64_t siphash(const uint8_t *in, const size_t inlen, const uint8_t *k);
uint64_t siphash_nocase(const uint8_t *in, const size_t inlen, const uint8_t *k);

uint64_t dictGenHashFunction(const void *key, size_t len) {
    return siphash(key,len,dict_hash_function_seed);
}

uint64_t dictGenCaseHashFunction(const unsigned char *buf, size_t len) {
    return siphash_nocase(buf,len,dict_hash_function_seed);
}

/* ----------------------------- API implementation ------------------------- */

/* Reset hash table parameters already initialized with _dictInit()*/
/* 重置已经用 _dictInit() 初始化的哈希表参数 */
static void _dictReset(dict *d, int htidx)
{
    d->ht_table[htidx] = NULL;
    d->ht_size_exp[htidx] = -1;
    d->ht_used[htidx] = 0;
}

/* Create a new hash table */
/* 创建一个新的哈希表 */
dict *dictCreate(dictType *type)
{
    dict *d = zmalloc(sizeof(*d));

    _dictInit(d,type);
    return d;
}

/* Initialize the hash table */
/* 初始化哈希表 */
int _dictInit(dict *d, dictType *type)
{
    _dictReset(d, 0);
    _dictReset(d, 1);
    d->type = type;
    d->rehashidx = -1;
    d->pauserehash = 0;
    return DICT_OK;
}

/* Resize the table to the minimal size that contains all the elements,
 * but with the invariant of a USED/BUCKETS ratio near to <= 1 */
/* 调整表的大小，使之成为包含所有元素的最小尺寸。
 * 但是 USED/BUCKETS 的比例要接近于 <=1 */
int dictResize(dict *d)
{
    unsigned long minimal;

    if (!dict_can_resize || dictIsRehashing(d)) return DICT_ERR;
    minimal = d->ht_used[0];
    if (minimal < DICT_HT_INITIAL_SIZE)
        minimal = DICT_HT_INITIAL_SIZE;
    return dictExpand(d, minimal);
}

/* Expand or create the hash table,
 * when malloc_failed is non-NULL, it'll avoid panic if malloc fails (in which case it'll be set to 1).
 * Returns DICT_OK if expand was performed, and DICT_ERR if skipped. */
/* 扩展或创建哈希表。
 * 当 malloc_failed 为非NULL时，如果 malloc 失败，它将避免恐慌（在这种情况下它将被设置为1）。
 * 如果执行了扩展，返回 DICT_OK，如果跳过，返回 DICT_ERR 。*/
int _dictExpand(dict *d, unsigned long size, int* malloc_failed)
{
    if (malloc_failed) *malloc_failed = 0;

    /* the size is invalid if it is smaller than the number of
     * elements already inside the hash table */
    /* 如果大小小于哈希表内已有的元素数，则无效。*/
    if (dictIsRehashing(d) || d->ht_used[0] > size)
        return DICT_ERR;

    /* the new hash table */
    /* 新哈希表 */
    dictEntry **new_ht_table;
    unsigned long new_ht_used;
    signed char new_ht_size_exp = _dictNextExp(size);

    /* Detect overflows */
    /* 检测溢出 */
    size_t newsize = 1ul<<new_ht_size_exp;
    if (newsize < size || newsize * sizeof(dictEntry*) < newsize)
        return DICT_ERR;

    /* Rehashing to the same table size is not useful. */
    /* 按照相同的表格大小重做是没有用的 */
    if (new_ht_size_exp == d->ht_size_exp[0]) return DICT_ERR;

    /* Allocate the new hash table and initialize all pointers to NULL */
    /* 为哈希表分配空间，并将所有指针指向 NULL */
    if (malloc_failed) {
        new_ht_table = ztrycalloc(newsize*sizeof(dictEntry*));
        *malloc_failed = new_ht_table == NULL;
        if (*malloc_failed)
            return DICT_ERR;
    } else
        new_ht_table = zcalloc(newsize*sizeof(dictEntry*));

    new_ht_used = 0;

    /* Is this the first initialization? If so it's not really a rehashing
     * we just set the first hash table so that it can accept keys. */
    /* 如果 0 号哈希表为空，那么这是一次初始化：
     * 程序将新哈希表赋给 0 号哈希表的指针，然后字典就可以开始处理键值对了。*/
    if (d->ht_table[0] == NULL) {
        d->ht_size_exp[0] = new_ht_size_exp;
        d->ht_used[0] = new_ht_used;
        d->ht_table[0] = new_ht_table;
        return DICT_OK;
    }

    /* Prepare a second hash table for incremental rehashing */
    /* 如果 0 号哈希表非空，那么这是一次 rehash ：
     * 程序将新哈希表设置为 1 号哈希表，
     * 并将字典的 rehash 标识打开，让程序可以开始对字典进行 rehash */
    d->ht_size_exp[1] = new_ht_size_exp;
    d->ht_used[1] = new_ht_used;
    d->ht_table[1] = new_ht_table;
    d->rehashidx = 0;
    return DICT_OK;
}

/* return DICT_ERR if expand was not performed */
/* 如果没有进行扩展，返回 DICT_ERR */
int dictExpand(dict *d, unsigned long size) {
    return _dictExpand(d, size, NULL);
}

/* return DICT_ERR if expand failed due to memory allocation failure */
/* 如果由于内存分配失败导致扩展失败，返回 DICT_ERR */
int dictTryExpand(dict *d, unsigned long size) {
    int malloc_failed;
    _dictExpand(d, size, &malloc_failed);
    return malloc_failed? DICT_ERR : DICT_OK;
}

/* Performs N steps of incremental rehashing. Returns 1 if there are still
 * keys to move from the old to the new hash table, otherwise 0 is returned.
 * 
 * 执行 N 步渐进式 rehash 。
 *
 * 返回 1 表示仍有键需要从 0 号哈希表移动到 1 号哈希表，
 * 返回 0 则表示所有键都已经迁移完毕。
 *
 * Note that a rehashing step consists in moving a bucket (that may have more
 * than one key as we use chaining) from the old to the new hash table, however
 * since part of the hash table may be composed of empty spaces, it is not
 * guaranteed that this function will rehash even a single bucket, since it
 * will visit at max N*10 empty buckets in total, otherwise the amount of
 * work it does would be unbound and the function may block for a long time. 
 * 
 * 注意，每步 rehash 都是以一个哈希表索引（桶）作为单位的，
 * 一个桶里可能会有多个节点，
 * 被 rehash 的桶里的所有节点都会被移动到新哈希表。
 * 
 * 但是由于哈希表的一部分可能是由空的空间组成的，所以不能保证这个函数 rehash 一个单独的桶。
 * 因为它最多将访问 N*10 个空桶，否则它的工作量将是无限制的。
 * 否则，它所做的工作将是无限制的，并且该函数可能会阻塞很长时间。*/
int dictRehash(dict *d, int n) {
    int empty_visits = n*10; /* Max number of empty buckets to visit. */ /* 访问空桶的最大数量 */

    /* 只可以在 rehash 进行中时执行 */
    if (!dictIsRehashing(d)) return 0;

    /* 进行 N 步迁移 */
    while(n-- && d->ht_used[0] != 0) {
        dictEntry *de, *nextde;

        /* Note that rehashidx can't overflow as we are sure there are more
        /* 确保 rehashidx 没有越界 */
        assert(DICTHT_SIZE(d->ht_size_exp[0]) > (unsigned long)d->rehashidx);

        /* 略过数组中为空的索引，找到下一个非空索引 */
        while(d->ht_table[0][d->rehashidx] == NULL) {
            d->rehashidx++;
            if (--empty_visits == 0) return 1;
        }

        /* 指向该索引的链表表头节点 */
        de = d->ht_table[0][d->rehashidx];

        /* Move all the keys in this bucket from the old to the new hash HT */
        /* 将链表中的所有节点迁移到新哈希表 */
        while(de) {
            uint64_t h;

            /* 保存下个节点的指针 */
            nextde = de->next;

            /* Get the index in the new hash table */
            /* 获取新哈希表的索引，以及节点插入的索引位置 */
            h = dictHashKey(d, de->key) & DICTHT_SIZE_MASK(d->ht_size_exp[1]);

            /* 插入节点到新哈希表 */
            de->next = d->ht_table[1][h];
            d->ht_table[1][h] = de;

            /* 更新计数器 */
            d->ht_used[0]--;
            d->ht_used[1]++;

            /* 继续处理下个节点 */
            de = nextde;
        }
        /* 将刚迁移完的哈希表索引的指针设为空 */
        d->ht_table[0][d->rehashidx] = NULL;
        /* 更新 rehash 索引 */
        d->rehashidx++;
    }

    /* Check if we already rehashed the whole table... */
    /* 如果 0 号哈希表为空，那么表示 rehash 执行完毕 */
    if (d->ht_used[0] == 0) {
        zfree(d->ht_table[0]);
        /* Copy the new ht onto the old one */
        /* 将新的 ht_table 复制到旧的 ht_table 上 */
        d->ht_table[0] = d->ht_table[1];
        d->ht_used[0] = d->ht_used[1];
        d->ht_size_exp[0] = d->ht_size_exp[1];
        _dictReset(d, 1);
        d->rehashidx = -1;
        return 0;
    }

    /* More to rehash... */
    /* 更多rehash... */
    return 1;
}

/* 返回以毫秒为单位的 UNIX 时间戳
 *
 * T = O(1)
 */
long long timeInMilliseconds(void) {
    struct timeval tv;

    gettimeofday(&tv,NULL);
    return (((long long)tv.tv_sec)*1000)+(tv.tv_usec/1000);
}

/* Rehash in ms+"delta" milliseconds. The value of "delta" is larger 
 * than 0, and is smaller than 1 in most cases. The exact upper bound 
 * depends on the running time of dictRehash(d,100).*/
/*
 * 在给定毫秒数内，以 100 步为单位，对字典进行 rehash 。
 *
 * T = O(N)
 * 
 * rehash 分为主动 rehash，dictRehashMilliseconds 也就是定时去 rehash，怕有的 KV 长时间不访问一直不迁移到 ht_table[1],
 * 另一种是由客户端访问的时候被动进行 rehash，见 _dictRehashStep
 */
int dictRehashMilliseconds(dict *d, int ms) {
    if (d->pauserehash > 0) return 0;

    // 记录开始时间
    long long start = timeInMilliseconds();
    int rehashes = 0;

    while(dictRehash(d,100)) {
        rehashes += 100;
        // 如果时间已过，跳出
        if (timeInMilliseconds()-start > ms) break;
    }
    return rehashes;
}

/* This function performs just a step of rehashing, and only if hashing has
 * not been paused for our hash table. When we have iterators in the
 * middle of a rehashing we can't mess with the two hash tables otherwise
 * some element can be missed or duplicated.
 * 
 * 在字典不存在安全迭代器的情况下，对字典进行单步 rehash 。
 * 
 * 字典有安全迭代器的情况下不能进行 rehash ，
 * 因为两种不同的迭代和修改操作可能会弄乱字典
 *
 * This function is called by common lookup or update operations in the
 * dictionary so that the hash table automatically migrates from H1 to H2
 * while it is actively used. 
 * 
 * 这个函数被多个通用的查找、更新操作调用，
 * 它可以让字典在被使用的同时进行 rehash 。
 *
 * T = O(1)
 * 
 * */
static void _dictRehashStep(dict *d) {
    if (d->pauserehash == 0) dictRehash(d,1);
}

/* Add an element to the target hash table */
/*
 * 尝试将给定键值对添加到字典中
 *
 * 只有给定键 key 不存在于字典时，添加操作才会成功
 *
 * 添加成功返回 DICT_OK ，失败返回 DICT_ERR
 *
 * 最坏 T = O(N) ，平滩 O(1) 
 * 
 * 把 key 和 val 对应的 obj 添加到一个 entry 中 
 */  
int dictAdd(dict *d, void *key, void *val)
{
    // 尝试添加键到字典，并返回包含了这个键的新哈希节点
    // T = O(N)
    dictEntry *entry = dictAddRaw(d,key,NULL);

    // 键已存在，添加失败
    if (!entry) return DICT_ERR;

    // 键不存在，设置节点的值
    // T = O(1)
    dictSetVal(d, entry, val);

    // 添加成功
    return DICT_OK;
}

/* Low level add or find:
 * This function adds the entry but instead of setting a value returns the
 * dictEntry structure to the user, that will make sure to fill the value
 * field as they wish.
 *
 * This function is also directly exposed to the user API to be called
 * mainly in order to store non-pointers inside the hash value, example:
 *
 * entry = dictAddRaw(dict,mykey,NULL);
 * if (entry != NULL) dictSetSignedIntegerVal(entry,1000);
 *
 * Return values:
 *
 * If key already exists NULL is returned, and "*existing" is populated
 * with the existing entry if existing is not NULL.
 *
 * If key was added, the hash entry is returned to be manipulated by the caller.
 */
/*
 * 尝试将键插入到字典中
 *
 * 如果键已经在字典存在，那么返回 NULL
 *
 * 如果键不存在，那么程序创建新的哈希节点，
 * 将节点和键关联，并插入到字典，然后返回节点本身。
 *
 * T = O(N)
 * 
 * rehash 扩大调用关系调用过程:
 * dictAddRaw -> _dictKeyIndex -> _dictExpandIfNeeded(这里决定是否需要扩展hash) -> dictExpand 
 * 
 * 缩减 hash 过程:
 * serverCron -> tryResizeHashTables -> dictResize(这里绝对缩减后的桶数) -> dictExpand
 */ 
dictEntry *dictAddRaw(dict *d, void *key, dictEntry **existing)
{
    long index;
    dictEntry *entry;
    int htidx;

    // 如果条件允许的话，进行单步 rehash
    // T = O(1)
    if (dictIsRehashing(d)) _dictRehashStep(d);

    /* Get the index of the new element, or -1 if
     * the element already exists. */
    // 计算键在哈希表中的索引值
    // 如果值为 -1 ，那么表示键已经存在
    // T = O(N)
    if ((index = _dictKeyIndex(d, key, dictHashKey(d,key), existing)) == -1)
        return NULL;

    /* Allocate the memory and store the new entry.
     * Insert the element in top, with the assumption that in a database
     * system it is more likely that recently added entries are accessed
     * more frequently. */
    // 如果字典正在 rehash ，那么将新键添加到 1 号哈希表
    // 否则，将新键添加到 0 号哈希表
    // T = O(1)
    htidx = dictIsRehashing(d) ? 1 : 0;
    size_t metasize = dictMetadataSize(d);

    // 为新节点分配空间
    entry = zmalloc(sizeof(*entry) + metasize);
    if (metasize > 0) {
        memset(dictMetadata(entry), 0, metasize);
    }

    // 将新节点插入到链表表头
    entry->next = d->ht_table[htidx][index];
    d->ht_table[htidx][index] = entry;

    // 更新哈希表已使用节点数量
    d->ht_used[htidx]++;

    /* Set the hash entry fields. */
    // 设置新节点的键
    // T = O(1)
    dictSetKey(d, entry, key);
    return entry;
}

/* Add or Overwrite:
 * Add an element, discarding the old value if the key already exists.
 *
 * 将给定的键值对添加到字典中，如果键已经存在，那么删除旧有的键值对。
 * 
 * Return 1 if the key was added from scratch, 0 if there was already an
 * element with such key and dictReplace() just performed a value update
 * operation. 
 * 
 * 如果键值对为全新添加，那么返回 1 。
 * 如果键值对是通过对原有的键值对更新得来的，那么返回 0 。
 *
 * T = O(N)
 * 
 * 将给定的键值对添加到字典里面， 如果键已经存在于字典，那么用新值取代原有的值 */
int dictReplace(dict *d, void *key, void *val)
{
    dictEntry *entry, *existing, auxentry;

    /* Try to add the element. If the key
     * does not exists dictAdd will succeed. */
    // 尝试直接将键值对添加到字典
    // 如果键 key 不存在的话，添加会成功
    entry = dictAddRaw(d,key,&existing);
    if (entry) {
        dictSetVal(d, entry, val);
        return 1;
    }

    /* Set the new value and free the old one. Note that it is important
     * to do that in this order, as the value may just be exactly the same
     * as the previous one. In this context, think to reference counting,
     * you want to increment (set), and then decrement (free), and not the
     * reverse. */
    // 先保存原有的值的指针
    auxentry = *existing;

    // 然后设置新的值
    // T = O(1)
    dictSetVal(d, existing, val);

    // 然后释放旧值
    // T = O(1)
    dictFreeVal(d, &auxentry);
    return 0;
}

/* Add or Find:
 * dictAddOrFind() is simply a version of dictAddRaw() that always
 * returns the hash entry of the specified key, even if the key already
 * exists and can't be added (in that case the entry of the already
 * existing key is returned.)
 *
 * See dictAddRaw() for more information. */
/*
 * dictAddRaw() 根据给定 key 释放存在，执行以下动作：
 *
 * 1) key 已经存在，返回包含该 key 的字典节点
 * 2) key 不存在，那么将 key 添加到字典
 *
 * 不论发生以上的哪一种情况，
 * dictAddRaw() 都总是返回包含给定 key 的字典节点。
 *
 * T = O(N)
 */
dictEntry *dictAddOrFind(dict *d, void *key) {
    dictEntry *entry, *existing;

    // 使用 key 在字典中查找节点
    // 如果节点找到了直接返回节点，否则添加并返回一个新节点
    entry = dictAddRaw(d,key,&existing);

    return entry ? entry : existing;
}

/* Search and remove an element. This is a helper function for
 * dictDelete() and dictUnlink(), please check the top comment
 * of those functions. */
/*
 * 查找并删除包含给定键的节点
 *
 * 参数 nofree 决定是否调用键和值的释放函数
 * 0 表示调用，1 表示不调用
 *
 * 找到并成功删除返回 DICT_OK ，没找到则返回 DICT_ERR
 *
 */
static dictEntry *dictGenericDelete(dict *d, const void *key, int nofree) {
    uint64_t h, idx;
    dictEntry *he, *prevHe;
    int table;

    /* dict is empty */
    // 字典（的哈希表）为空
    if (dictSize(d) == 0) return NULL;

    // 进行单步 rehash
    if (dictIsRehashing(d)) _dictRehashStep(d);

    // 计算哈希值
    h = dictHashKey(d, key);

    // 遍历哈希表
    for (table = 0; table <= 1; table++) {
        // 计算索引值 
        idx = h & DICTHT_SIZE_MASK(d->ht_size_exp[table]);

        // 指向该索引上的链表
        he = d->ht_table[table][idx];
        prevHe = NULL;

        // 遍历链表上的所有节点
        while(he) {
            if (key==he->key || dictCompareKeys(d, key, he->key)) {
                /* Unlink the element from the list */
                // 从链表中删除
                if (prevHe)
                    prevHe->next = he->next;
                else
                    d->ht_table[table][idx] = he->next;
                if (!nofree) {
                    dictFreeUnlinkedEntry(d, he);
                }

                // 更新已使用节点数量
                d->ht_used[table]--;
                return he;
            }
            prevHe = he;
            he = he->next;
        }

        // 如果执行到这里，说明在 0 号哈希表中找不到给定键
        // 那么根据字典是否正在进行 rehash ，决定要不要查找 1 号哈希表
        if (!dictIsRehashing(d)) break;
    }

    // 没找到
    return NULL; /* not found */
}

/* Remove an element, returning DICT_OK on success or DICT_ERR if the
 * element was not found. */
/*
 * 从字典中删除包含给定键的节点
 * 
 * 并且调用键值的释放函数来删除键值
 *
 * 找到并成功删除返回 DICT_OK ，没找到则返回 DICT_ERR
 * T = O(1)
 */
int dictDelete(dict *ht, const void *key) {
    return dictGenericDelete(ht,key,0) ? DICT_OK : DICT_ERR;
}

/* Remove an element from the table, but without actually releasing
 * the key, value and dictionary entry. The dictionary entry is returned
 * if the element was found (and unlinked from the table), and the user
 * should later call `dictFreeUnlinkedEntry()` with it in order to release it.
 * Otherwise if the key is not found, NULL is returned.
 *
 * 从表中删除一个元素，但实际上没有释放
 * key、value 和 dictionary entry。dictionary entry 被返回
 * 如果该元素被找到(并从表中解除链接)，
 * 用户应该随后调用 `dictFreeUnlinkedEntry()` 来释放它。
 * 否则如果没有找到键，则返回 NULL。
 * 
 * This function is useful when we want to remove something from the hash
 * table but want to use its value before actually deleting the entry.
 * Without this function the pattern would require two lookups:
 * 
 * 当我们想从哈希表中删除一些东西时，这个函数是很有用的。
 * 表中删除某样东西，但希望在实际删除该条目之前使用其值。
 * 如果没有这个函数，这个模式将需要进行两次查找
 *
 *  entry = dictFind(...);
 *  // Do something with entry
 *  dictDelete(dictionary,entry);
 *
 * Thanks to this function it is possible to avoid this, and use
 * instead:
 *
 * entry = dictUnlink(dictionary,entry);
 * // Do something with entry
 * dictFreeUnlinkedEntry(entry); // <- This does not need to lookup again.
 */
dictEntry *dictUnlink(dict *d, const void *key) {
    return dictGenericDelete(d,key,1);
}

/* You need to call this function to really free the entry after a call
 * to dictUnlink(). It's safe to call this function with 'he' = NULL. */
/* 在调用了 dictUnlink() 之后，需要调用本函数来真正释放 entry */
void dictFreeUnlinkedEntry(dict *d, dictEntry *he) {
    if (he == NULL) return;
    dictFreeKey(d, he);
    dictFreeVal(d, he);
    zfree(he);
}

/* Destroy an entire dictionary */
/* 删除哈希表上的所有节点，并重置哈希表的各项属性 */
int _dictClear(dict *d, int htidx, void(callback)(dict*)) {
    unsigned long i;

    /* Free all the elements */
    // 遍历整个哈希表
    for (i = 0; i < DICTHT_SIZE(d->ht_size_exp[htidx]) && d->ht_used[htidx] > 0; i++) {
        dictEntry *he, *nextHe;

        if (callback && (i & 65535) == 0) callback(d); // 清除过程中这里做一次 callback 调用

        // 跳过空索引
        if ((he = d->ht_table[htidx][i]) == NULL) continue;

        // 遍历整个链表
        while(he) {
            nextHe = he->next;
            dictFreeKey(d, he);
            dictFreeVal(d, he);
            zfree(he);
            d->ht_used[htidx]--;
            he = nextHe;
        }
    }
    /* Free the table and the allocated cache structure */
    // 释放哈希表结构
    zfree(d->ht_table[htidx]);
    /* Re-initialize the table */
    // 重置哈希表属性
    _dictReset(d, htidx);
    return DICT_OK; /* never fails */
}

/* Clear & Release the hash table */
/* 删除并释放整个字典 T = O(N) */
void dictRelease(dict *d)
{
    // 删除并清空两个哈希表
    _dictClear(d,0,NULL);
    _dictClear(d,1,NULL);
    // 释放节点结构
    zfree(d);
}

/*
 * 返回字典中包含键 key 的节点
 *
 * 找到返回节点，找不到返回 NULL
 *
 * T = O(1)
 */
dictEntry *dictFind(dict *d, const void *key)
{
    dictEntry *he;
    uint64_t h, idx, table;

    // 字典（的哈希表）为空
    if (dictSize(d) == 0) return NULL; /* dict is empty */

    // 如果条件允许的话，进行单步 rehash
    if (dictIsRehashing(d)) _dictRehashStep(d);

    // 计算键的哈希值
    h = dictHashKey(d, key);

    // 在字典的哈希表中查找这个键
    // T = O(1)
    for (table = 0; table <= 1; table++) {
        // 计算索引值
        idx = h & DICTHT_SIZE_MASK(d->ht_size_exp[table]);

        // 遍历给定索引上的链表的所有节点，查找 key
        he = d->ht_table[table][idx];

        // T = O(1)
        while(he) {
            if (key==he->key || dictCompareKeys(d, key, he->key))
                return he;
            he = he->next;
        }

        // 如果程序遍历完 0 号哈希表，仍然没找到指定的键的节点
        // 那么程序会检查字典是否在进行 rehash ，
        // 然后才决定是直接返回 NULL ，还是继续查找 1 号哈希表
        if (!dictIsRehashing(d)) return NULL;
    }

    // 进行到这里时，说明两个哈希表都没找到
    return NULL;
}

/*
 * 获取包含给定键的节点的值
 *
 * 如果节点不为空，返回节点的值
 * 否则返回 NULL
 *
 * T = O(1)
 */
void *dictFetchValue(dict *d, const void *key) {
    dictEntry *he;

    he = dictFind(d,key);
    return he ? dictGetVal(he) : NULL;
}

/* A fingerprint is a 64 bit number that represents the state of the dictionary
 * at a given time, it's just a few dict properties xored together.
 * When an unsafe iterator is initialized, we get the dict fingerprint, and check
 * the fingerprint again when the iterator is released.
 * If the two fingerprints are different it means that the user of the iterator
 * performed forbidden operations against the dictionary while iterating. */
/* 指纹是一个 64 位的数字，代表字典在特定时间的状态。
 * 在某一特定时间，它只是几个 dict 属性 xored 在一起。
 * 当一个不安全的迭代器被初始化时，我们得到dict的指纹，并在迭代器被释放时再次检查指纹。
 * 当迭代器被释放时再次检查指纹。
 * 如果这两个指纹不一样，就意味着迭代器的用户
 * 在迭代时对 dictionary 进行了禁止的操作。*/
unsigned long long dictFingerprint(dict *d) {
    unsigned long long integers[6], hash = 0;
    int j;

    integers[0] = (long) d->ht_table[0];
    integers[1] = d->ht_size_exp[0];
    integers[2] = d->ht_used[0];
    integers[3] = (long) d->ht_table[1];
    integers[4] = d->ht_size_exp[1];
    integers[5] = d->ht_used[1];

    /* We hash N integers by summing every successive integer with the integer
     * hashing of the previous sum. Basically:
     *
     * Result = hash(hash(hash(int1)+int2)+int3) ...
     *
     * This way the same set of integers in a different order will (likely) hash
     * to a different number. */
    for (j = 0; j < 6; j++) {
        hash += integers[j];
        /* For the hashing step we use Tomas Wang's 64 bit integer hash. */
        hash = (~hash) + (hash << 21); // hash = (hash << 21) - hash - 1;
        hash = hash ^ (hash >> 24);
        hash = (hash + (hash << 3)) + (hash << 8); // hash * 265
        hash = hash ^ (hash >> 14);
        hash = (hash + (hash << 2)) + (hash << 4); // hash * 21
        hash = hash ^ (hash >> 28);
        hash = hash + (hash << 31);
    }
    return hash;
}

/*
 * 创建并返回给定字典的不安全迭代器
 *
 * T = O(1)
 */
dictIterator *dictGetIterator(dict *d)
{
    dictIterator *iter = zmalloc(sizeof(*iter));

    iter->d = d;
    iter->table = 0;
    iter->index = -1;
    iter->safe = 0;
    iter->entry = NULL;
    iter->nextEntry = NULL;
    return iter;
}

/*
 * 创建并返回给定节点的安全迭代器
 *
 * T = O(1)
 */
dictIterator *dictGetSafeIterator(dict *d) {
    dictIterator *i = dictGetIterator(d);

    // 设置安全迭代器标识
    i->safe = 1;
    return i;
}

/*
 * 返回迭代器指向的当前节点
 *
 * 字典迭代完毕时，返回 NULL
 *
 * T = O(1)
 * 
 * 遍历桶中的所有节点，第一次遍历该函数的时候直接获取具体桶首元素，否则使用 nextEntry
 * 
 */
dictEntry *dictNext(dictIterator *iter)
{
    while (1) {

        // 进入这个循环有两种可能：
        // 1) 这是迭代器第一次运行
        // 2) 当前索引链表中的节点已经迭代完（NULL 为链表的表尾）
        if (iter->entry == NULL) {

            // 初次迭代时执行
            if (iter->index == -1 && iter->table == 0) {
                // 如果是安全迭代器，那么更新安全迭代器计数器
                if (iter->safe)
                    dictPauseRehashing(iter->d);
                // 如果是不安全迭代器，那么计算指纹
                else
                    iter->fingerprint = dictFingerprint(iter->d);
            }

            // 更新索引
            iter->index++; //默认值为 -1，自加后刚好为 0，即第一个桶

            // 如果迭代器的当前索引大于当前被迭代的哈希表的大小
            // 那么说明这个哈希表已经迭代完毕
            if (iter->index >= (long) DICTHT_SIZE(iter->d->ht_size_exp[iter->table])) {

                // 如果正在 rehash 的话，那么说明 1 号哈希表也正在使用中
                // 那么继续对 1 号哈希表进行迭代
                if (dictIsRehashing(iter->d) && iter->table == 0) {
                    iter->table++;
                    iter->index = 0;

                // 如果没有 rehash ，那么说明迭代已经完成
                } else {
                    break;
                }
            }

            // 如果进行到这里，说明这个哈希表并未迭代完
            // 更新节点指针，指向下个索引链表的表头节点
            iter->entry = iter->d->ht_table[iter->table][iter->index];
        } else {
            // 执行到这里，说明程序正在迭代某个链表
            // 将节点指针指向链表的下个节点
            iter->entry = iter->nextEntry;
        }

        // 如果当前节点不为空，那么也记录下该节点的下个节点
        // 因为安全迭代器有可能会将迭代器返回的当前节点删除
        if (iter->entry) {
            /* We need to save the 'next' here, the iterator user
             * may delete the entry we are returning. */
            iter->nextEntry = iter->entry->next;
            return iter->entry;
        }
    }

    // 迭代完毕
    return NULL;
}

/*
 * 释放给定字典迭代器
 *
 * T = O(1)
 */
void dictReleaseIterator(dictIterator *iter)
{
    if (!(iter->index == -1 && iter->table == 0)) {
        // 释放安全迭代器时，安全迭代器计数器减一
        if (iter->safe)
            dictResumeRehashing(iter->d);

        // 释放不安全迭代器时，验证指纹是否有变化
        else
            assert(iter->fingerprint == dictFingerprint(iter->d));
    }
    zfree(iter);
}

/* Return a random entry from the hash table. Useful to
 * implement randomized algorithms */
/*
 * 随机返回字典中任意一个节点。
 *
 * 可用于实现随机化算法。
 *
 * 如果字典为空，返回 NULL 。
 *
 * T = O(N)
*/
dictEntry *dictGetRandomKey(dict *d)
{
    dictEntry *he, *orighe;
    unsigned long h;
    int listlen, listele;

    // 字典为空
    if (dictSize(d) == 0) return NULL;

    // 如果条件允许的话，进行单步 rehash
    if (dictIsRehashing(d)) _dictRehashStep(d);

    // 如果正在 rehash ，那么将 1 号哈希表也作为随机查找的目标
    if (dictIsRehashing(d)) {
        unsigned long s0 = DICTHT_SIZE(d->ht_size_exp[0]);
        do {
            /* We are sure there are no elements in indexes from 0
             * to rehashidx-1 */
            /* 我们确定从 0 到 rehashidx-1 的索引中没有元素 */
            h = d->rehashidx + (randomULong() % (dictSlots(d) - d->rehashidx));
            he = (h >= s0) ? d->ht_table[1][h - s0] : d->ht_table[0][h];
        } while(he == NULL);
    
    // 否则，只从 0 号哈希表中查找节点
    } else {
        unsigned long m = DICTHT_SIZE_MASK(d->ht_size_exp[0]);
        do {
            h = randomULong() & m;
            he = d->ht_table[0][h];
        } while(he == NULL);
    }

    /* Now we found a non empty bucket, but it is a linked
     * list and we need to get a random element from the list.
     * The only sane way to do so is counting the elements and
     * select a random index. */
    // 目前 he 已经指向一个非空的节点链表
    // 程序将从这个链表随机返回一个节点
    listlen = 0;
    orighe = he;

    // 计算节点数量, T = O(1)
    // 遍历整个桶，计算这个捅里面的KV数
    while(he) {
        he = he->next;
        listlen++;
    }

    // 取模，得出随机节点的索引
    // 从具体桶里面随机取出一个 KV 节点
    listele = random() % listlen;
    he = orighe;

    // 按索引查找节点
    // T = O(1)
    while(listele--) he = he->next;

    // 返回随机节点
    return he;
}

/* This function samples the dictionary to return a few keys from random
 * locations.
 *
 * 这个函数对字典进行采样，从随机返回的几个键的位置。
 *
 * It does not guarantee to return all the keys specified in 'count', nor
 * it does guarantee to return non-duplicated elements, however it will make
 * some effort to do both things.
 * 
 * 它不保证返回 'count' 中指定的所有键，也不保证返回不重复的元素。
 * 它不保证返回不重复的元素，但它将努力做到这两点。
 * 
 * Returned pointers to hash table entries are stored into 'des' that
 * points to an array of dictEntry pointers. The array must have room for
 * at least 'count' elements, that is the argument we pass to the function
 * to tell how many random elements we need.
 * 
 * 返回的哈希表项的指针被存储到 'des' 中，该指针
 * 指向一个 dictEntry 指针的数组。该数组必须有空间容纳
 * 至少 'count' 元素，这就是我们传递给函数的参数
 * 来告诉我们需要多少个随机元素。
 * 
 * The function returns the number of items stored into 'des', that may
 * be less than 'count' if the hash table has less than 'count' elements
 * inside, or if not enough elements were found in a reasonable amount of
 * steps.
 * 
 * 该函数返回存储到 'des' 中的项数，如果哈希表中的元素少于 'count'，或者如果在合理的步骤数中找不到足够的元素，则该项数可能小于 'count'。
 *
 * Note that this function is not suitable when you need a good distribution
 * of the returned items, but only when you need to "sample" a given number
 * of continuous elements to run some kind of algorithm or to produce
 * statistics. However the function is much faster than dictGetRandomKey()
 * at producing N elements. 
 * 
 * 请注意，当您需要返回项目的良好分布时，此函数不适用，但仅当您需要 “采样” 给定数量的连续元素以运行某种算法或生成统计信息时，此函数才适用。
 * 但是，该函数在生成 N 个元素时比 dictGetRandomKey（）快得多。*/
unsigned int dictGetSomeKeys(dict *d, dictEntry **des, unsigned int count) {
    unsigned long j; /* internal hash table id, 0 or 1. */
    unsigned long tables; /* 1 or 2 tables? */
    unsigned long stored = 0, maxsizemask;
    unsigned long maxsteps;

    if (dictSize(d) < count) count = dictSize(d);
    maxsteps = count*10;

    /* Try to do a rehashing work proportional to 'count'. */
    for (j = 0; j < count; j++) {
        if (dictIsRehashing(d))
            _dictRehashStep(d);
        else
            break;
    }

    tables = dictIsRehashing(d) ? 2 : 1;
    maxsizemask = DICTHT_SIZE_MASK(d->ht_size_exp[0]);
    if (tables > 1 && maxsizemask < DICTHT_SIZE_MASK(d->ht_size_exp[1]))
        maxsizemask = DICTHT_SIZE_MASK(d->ht_size_exp[1]);

    /* Pick a random point inside the larger table. */
    unsigned long i = randomULong() & maxsizemask;
    unsigned long emptylen = 0; /* Continuous empty entries so far. */
    while(stored < count && maxsteps--) {
        for (j = 0; j < tables; j++) {
            /* Invariant of the dict.c rehashing: up to the indexes already
             * visited in ht[0] during the rehashing, there are no populated
             * buckets, so we can skip ht[0] for indexes between 0 and idx-1. */
            if (tables == 2 && j == 0 && i < (unsigned long) d->rehashidx) {
                /* Moreover, if we are currently out of range in the second
                 * table, there will be no elements in both tables up to
                 * the current rehashing index, so we jump if possible.
                 * (this happens when going from big to small table). */
                if (i >= DICTHT_SIZE(d->ht_size_exp[1]))
                    i = d->rehashidx;
                else
                    continue;
            }
            if (i >= DICTHT_SIZE(d->ht_size_exp[j])) continue; /* Out of range for this table. */
            dictEntry *he = d->ht_table[j][i];

            /* Count contiguous empty buckets, and jump to other
             * locations if they reach 'count' (with a minimum of 5). */
            if (he == NULL) {
                emptylen++;
                if (emptylen >= 5 && emptylen > count) {
                    i = randomULong() & maxsizemask;
                    emptylen = 0;
                }
            } else {
                emptylen = 0;
                while (he) {
                    /* Collect all the elements of the buckets found non
                     * empty while iterating. */
                    *des = he;
                    des++;
                    he = he->next;
                    stored++;
                    if (stored == count) return stored;
                }
            }
        }
        i = (i+1) & maxsizemask;
    }
    return stored;
}

/* This is like dictGetRandomKey() from the POV of the API, but will do more
 * work to ensure a better distribution of the returned element.
 * 
 * 这类似于 API POV 中的 dictGetRandomKey（），但将做更多的工作来确保更好地分布返回的元素
 * 
 * This function improves the distribution because the dictGetRandomKey()
 * problem is that it selects a random bucket, then it selects a random
 * element from the chain in the bucket. However elements being in different
 * chain lengths will have different probabilities of being reported. With
 * this function instead what we do is to consider a "linear" range of the table
 * that may be constituted of N buckets with chains of different lengths
 * appearing one after the other. Then we report a random element in the range.
 * In this way we smooth away the problem of different chain lengths. */
#define GETFAIR_NUM_ENTRIES 15
dictEntry *dictGetFairRandomKey(dict *d) {
    dictEntry *entries[GETFAIR_NUM_ENTRIES];
    unsigned int count = dictGetSomeKeys(d,entries,GETFAIR_NUM_ENTRIES);
    /* Note that dictGetSomeKeys() may return zero elements in an unlucky
     * run() even if there are actually elements inside the hash table. So
     * when we get zero, we call the true dictGetRandomKey() that will always
     * yield the element if the hash table has at least one. */
    if (count == 0) return dictGetRandomKey(d);
    unsigned int idx = rand() % count;
    return entries[idx];
}

/* Function to reverse bits. Algorithm from:
 * http://graphics.stanford.edu/~seander/bithacks.html#ReverseParallel */
static unsigned long rev(unsigned long v) {
    unsigned long s = CHAR_BIT * sizeof(v); // bit size; must be power of 2
    unsigned long mask = ~0UL;
    while ((s >>= 1) > 0) {
        mask ^= (mask << s);
        v = ((v >> s) & mask) | ((v << s) & ~mask);
    }
    return v;
}

/* dictScan() is used to iterate over the elements of a dictionary.
 * 
 * dictScan() 函数用于迭代给定字典中的元素。
 * 
 * Iterating works the following way:
 * 
 * 迭代按以下方式执行：
 * 
 * 1) Initially you call the function using a cursor (v) value of 0.
 *    一开始，你使用 0 作为游标来调用函数。
 * 2) The function performs one step of the iteration, and returns the
 *    new cursor value you must use in the next call.
 *    函数执行一步迭代操作，
 *    并返回一个下次迭代时使用的新游标。
 * 3) When the returned cursor is 0, the iteration is complete.
 *    当函数返回的游标为 0 时，迭代完成。
 *
 * The function guarantees all elements present in the
 * dictionary get returned between the start and end of the iteration.
 * However it is possible some elements get returned multiple times.
 * 
 * 函数保证，在迭代从开始到结束期间，一直存在于字典的元素肯定会被迭代到，
 * 但一个元素可能会被返回多次。
 *
 * For every element returned, the callback argument 'fn' is
 * called with 'privdata' as first argument and the dictionary entry
 * 'de' as second argument.
 * 
 * 每当一个元素被返回时，回调函数 fn 就会被执行，
 * fn 函数的第一个参数是 privdata ，而第二个参数则是字典节点 de 。
 * 
 * HOW IT WORKS.
 * 
 * 工作原理
 *
 * The iteration algorithm was designed by Pieter Noordhuis.
 * The main idea is to increment a cursor starting from the higher order
 * bits. That is, instead of incrementing the cursor normally, the bits
 * of the cursor are reversed, then the cursor is incremented, and finally
 * the bits are reversed again.
 * 
 * 迭代所使用的算法是由 Pieter Noordhuis 设计的，
 * 算法的主要思路是在二进制高位上对游标进行加法计算
 * 也即是说，不是按正常的办法来对游标进行加法计算，
 * 而是首先将游标的二进制位翻转（reverse）过来，
 * 然后对翻转后的值进行加法计算，
 * 最后再次对加法计算之后的结果进行翻转。
 *
 * This strategy is needed because the hash table may be resized between
 * iteration calls.
 * 
 * 这一策略是必要的，因为在一次完整的迭代过程中，
 * 哈希表的大小有可能在两次迭代之间发生改变。
 *
 * dict.c hash tables are always power of two in size, and they
 * use chaining, so the position of an element in a given table is given
 * by computing the bitwise AND between Hash(key) and SIZE-1
 * (where SIZE-1 is always the mask that is equivalent to taking the rest
 *  of the division between the Hash of the key and SIZE).
 * 
 * 哈希表的大小总是 2 的某个次方，并且哈希表使用链表来解决冲突，
 * 因此一个给定元素在一个给定表的位置总可以通过 Hash(key) & SIZE-1
 * 公式来计算得出，
 * 其中 SIZE-1 是哈希表的最大索引值，
 * 这个最大索引值就是哈希表的 mask （掩码）。
 *
 * For example if the current hash table size is 16, the mask is
 * (in binary) 1111. The position of a key in the hash table will always be
 * the last four bits of the hash output, and so forth.
 * 
 * 举个例子，如果当前哈希表的大小为 16 ，
 * 那么它的掩码就是二进制值 1111 ，
 * 这个哈希表的所有位置都可以使用哈希值的最后四个二进制位来记录。
 *
 * WHAT HAPPENS IF THE TABLE CHANGES IN SIZE?
 * 
 * 如果哈希表的大小改变了怎么办？
 *
 * If the hash table grows, elements can go anywhere in one multiple of
 * the old bucket: for example let's say we already iterated with
 * a 4 bit cursor 1100 (the mask is 1111 because hash table size = 16).
 * 
 * 当对哈希表进行扩展时，元素可能会从一个槽移动到另一个槽，
 * 举个例子，假设我们刚好迭代至 4 位游标 1100 ，
 * 而哈希表的 mask 为 1111 （哈希表的大小为 16 ）。
 *
 * If the hash table will be resized to 64 elements, then the new mask will
 * be 111111. The new buckets you obtain by substituting in ??1100
 * with either 0 or 1 can be targeted only by keys we already visited
 * when scanning the bucket 1100 in the smaller hash table.
 * 
 * 如果这时哈希表将大小改为 64 ，那么哈希表的 mask 将变为 111111 ，
 *
 * By iterating the higher bits first, because of the inverted counter, the
 * cursor does not need to restart if the table size gets bigger. It will
 * continue iterating using cursors without '1100' at the end, and also
 * without any other combination of the final 4 bits already explored.
 * 
 * 通过先迭代较高的位，因为有倒置的计数器，如果表的大小变大，游标不需要重新启动。
 * 如果表的大小变大，游标不需要重新启动。它将继续使用结尾不带 “1100” 的游标进行迭代，
 * 而且也没有任何其他已经探索过的最后 4 位的组合。
 * 
 * Similarly when the table size shrinks over time, for example going from
 * 16 to 8, if a combination of the lower three bits (the mask for size 8
 * is 111) were already completely explored, it would not be visited again
 * because we are sure we tried, for example, both 0111 and 1111 (all the
 * variations of the higher bit) so we don't need to test it again.
 * 
 * 同样，当表的大小随着时间的推移而缩小时，例如从 16 到 8 。
 * 如果低三位的组合（大小为 8 的掩码是 111 ）已经被完全探索过了，它就不会再被访问了
 * 因为我们确信我们试过了，例如 0111 和 1111（所有的高位的变化），所以我们不需要再去测试它。
 *
 * WAIT... YOU HAVE *TWO* TABLES DURING REHASHING!
 * 
 * 等等。。。在 rehash 的时候可是会出现两个哈希表的阿
 *
 * Yes, this is true, but we always iterate the smaller table first, then
 * we test all the expansions of the current cursor into the larger
 * table. For example if the current cursor is 101 and we also have a
 * larger table of size 16, we also test (0)101 and (1)101 inside the larger
 * table. This reduces the problem back to having only one table, where
 * the larger one, if it exists, is just an expansion of the smaller one.
 * 
 * 是的，这是真的，但我们总是先迭代较小的表，然后我们测试当前光标在较大表中的所有扩展。
 * 例如，如果当前光标为 101，并且我们还有一个大小为 16 的较大表，那么我们也会在较大的表中测试（0）101和（1）101。
 * 这将问题简化为只有一个表，其中较大的表（如果存在）只是较小表的扩展。
 *
 * LIMITATIONS
 * 
 * 限制
 *
 * This iterator is completely stateless, and this is a huge advantage,
 * including no additional memory used.
 * 
 * 这个迭代器是完全无状态的，这是一个巨大的优势，
 * 因为迭代可以在不使用任何额外内存的情况下进行。
 *
 * The disadvantages resulting from this design are:
 * 
 * 这个设计的缺陷在于：
 *
 * 1) It is possible we return elements more than once. However this is usually
 *    easy to deal with in the application level.
 *    函数可能会返回重复的元素，不过这个问题可以很容易在应用层解决。
 * 2) The iterator must return multiple elements per call, as it needs to always
 *    return all the keys chained in a given bucket, and all the expansions, so
 *    we are sure we don't miss keys moving during rehashing.
 *    为了不错过任何元素，
 *    迭代器需要返回给定桶上的所有键，
 *    以及因为扩展哈希表而产生出来的新表，
 *    所以迭代器必须在一次迭代中返回多个元素。
 * 3) The reverse cursor is somewhat hard to understand at first, but this
 *    comment is supposed to help.
 *    对游标进行翻转（reverse）的原因初看上去比较难以理解，
 *    不过阅读这份注释应该会有所帮助。
 */
unsigned long dictScan(dict *d,
                       unsigned long v,
                       dictScanFunction *fn,
                       dictScanBucketFunction* bucketfn,
                       void *privdata)
{
    int htidx0, htidx1;
    const dictEntry *de, *next;
    unsigned long m0, m1;
    
    // 跳过空字典
    if (dictSize(d) == 0) return 0;

    /* This is needed in case the scan callback tries to do dictFind or alike. */
    // 迭代只有一个哈希表的字典
    dictPauseRehashing(d);

    if (!dictIsRehashing(d)) {
        // 指向哈希表
        htidx0 = 0;

        // 记录 mask
        m0 = DICTHT_SIZE_MASK(d->ht_size_exp[htidx0]);

        /* Emit entries at cursor */
        // 指向哈希桶
        if (bucketfn) bucketfn(d, &d->ht_table[htidx0][v & m0]);
        de = d->ht_table[htidx0][v & m0];
        // 遍历桶中的所有节点
        while (de) {
            next = de->next;
            fn(privdata, de);
            de = next;
        }

        /* Set unmasked bits so incrementing the reversed cursor
         * operates on the masked bits */
        // 设置未被屏蔽的位，使反转光标的增量在被屏蔽的位上操作
        v |= ~m0;

        /* Increment the reverse cursor */
        // 递增反向游标
        v = rev(v);
        v++;
        v = rev(v);

    } else {
        htidx0 = 0;
        htidx1 = 1;

        /* Make sure t0 is the smaller and t1 is the bigger table */
        // 确保 t0 是较小的表，t1 是较大的表。
        if (DICTHT_SIZE(d->ht_size_exp[htidx0]) > DICTHT_SIZE(d->ht_size_exp[htidx1])) {
            htidx0 = 1;
            htidx1 = 0;
        }

        m0 = DICTHT_SIZE_MASK(d->ht_size_exp[htidx0]);
        m1 = DICTHT_SIZE_MASK(d->ht_size_exp[htidx1]);

        /* Emit entries at cursor */
        // 指向哈希桶
        if (bucketfn) bucketfn(d, &d->ht_table[htidx0][v & m0]);
        de = d->ht_table[htidx0][v & m0];
        // 遍历桶中的所有节点
        while (de) {
            next = de->next;
            fn(privdata, de);
            de = next;
        }

        /* Iterate over indices in larger table that are the expansion
         * of the index pointed to by the cursor in the smaller table */
        // 遍历大表中的索引，这些索引是游标在小表中指向的索引的扩展。
        do {
            /* Emit entries at cursor */
            // 指向哈希桶
            if (bucketfn) bucketfn(d, &d->ht_table[htidx1][v & m1]);
            de = d->ht_table[htidx1][v & m1];
            // 遍历桶中的所有节点
            while (de) {
                next = de->next;
                fn(privdata, de);
                de = next;
            }

            /* Increment the reverse cursor not covered by the smaller mask.*/
            // 递增未被较小掩码覆盖的反向游标
            v |= ~m1;
            v = rev(v);
            v++;
            v = rev(v);

            /* Continue while bits covered by mask difference is non-zero */
            // 在掩码差所覆盖的位数非零时继续进行
        } while (v & (m0 ^ m1));
    }

    dictResumeRehashing(d);

    return v;
}

/* ------------------------- private functions ------------------------------ */

/* Because we may need to allocate huge memory chunk at once when dict
 * expands, we will check this allocation is allowed or not if the dict
 * type has expandAllowed member function. */
/* 因为当 dict 扩展时，我们可能需要一次性分配巨大的内存块，我们将检查这种分配是否允许，
 * 如果 dict 类型有 expandAllowed 成员函数，我们将检查这种分配是否被允许。*/
static int dictTypeExpandAllowed(dict *d) {
    if (d->type->expandAllowed == NULL) return 1;
    return d->type->expandAllowed(
                    DICTHT_SIZE(_dictNextExp(d->ht_used[0] + 1)) * sizeof(dictEntry*),
                    (double)d->ht_used[0] / DICTHT_SIZE(d->ht_size_exp[0]));
}

/* Expand the hash table if needed */
/*
 * 根据需要，初始化字典（的哈希表），或者对字典（的现有哈希表）进行扩展
 *
 * T = O(N)
 * 
 * rehash 扩大调用关系调用过程:
 * dictAddRaw -> _dictKeyIndex-> _dictExpandIfNeeded(这里决定是否需要扩展hash) -> dictExpand 
 * 
 * 缩减 hash 过程:
 * serverCron -> tryResizeHashTables -> dictResize(这里绝对缩减后的桶数) -> dictExpand
 */
static int _dictExpandIfNeeded(dict *d)
{
    /* Incremental rehashing already in progress. Return. */
    // 渐进式 rehash 已经在进行了，直接返回
    if (dictIsRehashing(d)) return DICT_OK;

    /* If the hash table is empty expand it to the initial size. */
    // 如果字典（的 0 号哈希表）为空，那么创建并返回初始化大小的 0 号哈希表
    // T = O(1)
    if (DICTHT_SIZE(d->ht_size_exp[0]) == 0) return dictExpand(d, DICT_HT_INITIAL_SIZE);

    /* If we reached the 1:1 ratio, and we are allowed to resize the hash
     * table (global setting) or we should avoid it but the ratio between
     * elements/buckets is over the "safe" threshold, we resize doubling
     * the number of buckets. */
    // 一下两个条件之一为真时，对字典进行扩展
    // 1）字典已使用节点数和字典大小之间的比率接近 1：1
    //    并且 dict_can_resize 为真
    // 2）已使用节点数和字典大小之间的比率超过 dict_force_resize_ratio
    if (d->ht_used[0] >= DICTHT_SIZE(d->ht_size_exp[0]) &&
        (dict_can_resize ||
         d->ht_used[0]/ DICTHT_SIZE(d->ht_size_exp[0]) > dict_force_resize_ratio) &&
        dictTypeExpandAllowed(d))
    {
        return dictExpand(d, d->ht_used[0] + 1);
    }
    return DICT_OK;
}

/* TODO: clz optimization */
/* Our hash table capability is a power of two */
/*
 * 计算第一个大于等于 size 的 2 的 N 次方，用作哈希表的值
 *
 * T = O(1)
 */
static signed char _dictNextExp(unsigned long size)
{
    unsigned char e = DICT_HT_INITIAL_EXP;

    if (size >= LONG_MAX) return (8*sizeof(long)-1);
    while(1) {
        if (((unsigned long)1<<e) >= size)
            return e;
        e++;
    }
}

/* Returns the index of a free slot that can be populated with
 * a hash entry for the given 'key'.
 * If the key already exists, -1 is returned
 * and the optional output parameter may be filled.
 * 
 * 返回可以将 key 插入到哈希表的索引位置
 * 如果 key 已经存在于哈希表，那么返回 -1
 *
 * Note that if we are in the process of rehashing the hash table, the
 * index is always returned in the context of the second (new) hash table. 
 * 
 * 注意，如果字典正在进行 rehash ，那么总是返回 1 号哈希表的索引。
 * 因为在字典进行 rehash 时，新节点总是插入到 1 号哈希表。
 *
 * T = O(N)
 * 
 * rehash 扩大调用关系调用过程:
 * dictAddRaw -> _dictKeyIndex -> _dictExpandIfNeeded(这里决定是否需要扩展hash) -> dictExpand 
 * 
 * 缩减hash过程:
 * serverCron -> tryResizeHashTables -> dictResize(这里绝对缩减后的桶数) -> dictExpand
 */
static long _dictKeyIndex(dict *d, const void *key, uint64_t hash, dictEntry **existing)
{
    unsigned long idx, table;
    dictEntry *he;
    if (existing) *existing = NULL;

    /* Expand the hash table if needed */
    // 单步 rehash
    // T = O(N)
    if (_dictExpandIfNeeded(d) == DICT_ERR)
        return -1;
    for (table = 0; table <= 1; table++) {

        // 计算索引值
        idx = hash & DICTHT_SIZE_MASK(d->ht_size_exp[table]);

        /* Search if this slot does not already contain the given key */
        // 查找 key 是否存在
        // T = O(1)
        he = d->ht_table[table][idx];
        while(he) {
            if (key==he->key || dictCompareKeys(d, key, he->key)) {
                if (existing) *existing = he;
                return -1;
            }
            he = he->next;
        }

        // 如果运行到这里时，说明 0 号哈希表中所有节点都不包含 key
        // 如果这时 rehahs 正在进行，那么继续对 1 号哈希表进行 rehash
        if (!dictIsRehashing(d)) break;
    }

    // 返回索引值
    return idx;
}

/*
 * 清空字典上的所有哈希表节点，并重置字典属性
 *
 * T = O(N)
 */
void dictEmpty(dict *d, void(callback)(dict*)) {

    // 删除两个哈希表上的所有节点
    // T = O(N)
    _dictClear(d,0,callback);
    _dictClear(d,1,callback);

    // 重置属性
    d->rehashidx = -1;
    d->pauserehash = 0;
}

/*
 * 开启自动 rehash
 *
 * T = O(1)
 */
void dictEnableResize(void) {
    dict_can_resize = 1;
}

/*
 * 关闭自动 rehash
 *
 * T = O(1)
 */
void dictDisableResize(void) {
    dict_can_resize = 0;
}

uint64_t dictGetHash(dict *d, const void *key) {
    return dictHashKey(d, key);
}

/* Finds the dictEntry reference by using pointer and pre-calculated hash.
 *
 * 使用指针和预先计算的哈希查找 dictEntry 引用
 *
 * oldkey is a dead pointer and should not be accessed.
 * 
 * oldkey 是一个死指针，不应该被访问
 * 
 * the hash value should be provided using dictGetHash.
 * 
 * 应使用 dictGetHash 提供哈希值
 * 
 * no string / key comparison is performed.
 * 
 * 不进行 字符串 / 键 的比较
 * 
 * return value is the reference to the dictEntry if found, or NULL if not found. 
 * 
 * 如果找到，返回值是对 dictEntry 的引用，如果没有找到，则为 NULL。*/
dictEntry **dictFindEntryRefByPtrAndHash(dict *d, const void *oldptr, uint64_t hash) {
    dictEntry *he, **heref;
    unsigned long idx, table;

    if (dictSize(d) == 0) return NULL; /* dict is empty */
    for (table = 0; table <= 1; table++) {
        idx = hash & DICTHT_SIZE_MASK(d->ht_size_exp[table]);
        heref = &d->ht_table[table][idx];
        he = *heref;
        while(he) {
            if (oldptr==he->key)
                return heref;
            heref = &he->next;
            he = *heref;
        }
        if (!dictIsRehashing(d)) return NULL;
    }
    return NULL;
}

/* ------------------------------- Debugging ---------------------------------*/

#define DICT_STATS_VECTLEN 50
size_t _dictGetStatsHt(char *buf, size_t bufsize, dict *d, int htidx) {
    unsigned long i, slots = 0, chainlen, maxchainlen = 0;
    unsigned long totchainlen = 0;
    unsigned long clvector[DICT_STATS_VECTLEN];
    size_t l = 0;

    if (d->ht_used[htidx] == 0) {
        return snprintf(buf,bufsize,
            "No stats available for empty dictionaries\n");
    }

    /* Compute stats. */
    for (i = 0; i < DICT_STATS_VECTLEN; i++) clvector[i] = 0;
    for (i = 0; i < DICTHT_SIZE(d->ht_size_exp[htidx]); i++) {
        dictEntry *he;

        if (d->ht_table[htidx][i] == NULL) {
            clvector[0]++;
            continue;
        }
        slots++;
        /* For each hash entry on this slot... */
        chainlen = 0;
        he = d->ht_table[htidx][i];
        while(he) {
            chainlen++;
            he = he->next;
        }
        clvector[(chainlen < DICT_STATS_VECTLEN) ? chainlen : (DICT_STATS_VECTLEN-1)]++;
        if (chainlen > maxchainlen) maxchainlen = chainlen;
        totchainlen += chainlen;
    }

    /* Generate human readable stats. */
    l += snprintf(buf+l,bufsize-l,
        "Hash table %d stats (%s):\n"
        " table size: %lu\n"
        " number of elements: %lu\n"
        " different slots: %lu\n"
        " max chain length: %lu\n"
        " avg chain length (counted): %.02f\n"
        " avg chain length (computed): %.02f\n"
        " Chain length distribution:\n",
        htidx, (htidx == 0) ? "main hash table" : "rehashing target",
        DICTHT_SIZE(d->ht_size_exp[htidx]), d->ht_used[htidx], slots, maxchainlen,
        (float)totchainlen/slots, (float)d->ht_used[htidx]/slots);

    for (i = 0; i < DICT_STATS_VECTLEN-1; i++) {
        if (clvector[i] == 0) continue;
        if (l >= bufsize) break;
        l += snprintf(buf+l,bufsize-l,
            "   %ld: %ld (%.02f%%)\n",
            i, clvector[i], ((float)clvector[i]/DICTHT_SIZE(d->ht_size_exp[htidx]))*100);
    }

    /* Unlike snprintf(), return the number of characters actually written. */
    if (bufsize) buf[bufsize-1] = '\0';
    return strlen(buf);
}

void dictGetStats(char *buf, size_t bufsize, dict *d) {
    size_t l;
    char *orig_buf = buf;
    size_t orig_bufsize = bufsize;

    l = _dictGetStatsHt(buf,bufsize,d,0);
    buf += l;
    bufsize -= l;
    if (dictIsRehashing(d) && bufsize > 0) {
        _dictGetStatsHt(buf,bufsize,d,1);
    }
    /* Make sure there is a NULL term at the end. */
    if (orig_bufsize) orig_buf[orig_bufsize-1] = '\0';
}

/* ------------------------------- Benchmark ---------------------------------*/

#ifdef REDIS_TEST
#include "testhelp.h"

#define UNUSED(V) ((void) V)

uint64_t hashCallback(const void *key) {
    return dictGenHashFunction((unsigned char*)key, strlen((char*)key));
}

int compareCallback(dict *d, const void *key1, const void *key2) {
    int l1,l2;
    UNUSED(d);

    l1 = strlen((char*)key1);
    l2 = strlen((char*)key2);
    if (l1 != l2) return 0;
    return memcmp(key1, key2, l1) == 0;
}

void freeCallback(dict *d, void *val) {
    UNUSED(d);

    zfree(val);
}

char *stringFromLongLong(long long value) {
    char buf[32];
    int len;
    char *s;

    len = sprintf(buf,"%lld",value);
    s = zmalloc(len+1);
    memcpy(s, buf, len);
    s[len] = '\0';
    return s;
}

dictType BenchmarkDictType = {
    hashCallback,
    NULL,
    NULL,
    compareCallback,
    freeCallback,
    NULL,
    NULL
};

#define start_benchmark() start = timeInMilliseconds()
#define end_benchmark(msg) do { \
    elapsed = timeInMilliseconds()-start; \
    printf(msg ": %ld items in %lld ms\n", count, elapsed); \
} while(0)

/* ./redis-server test dict [<count> | --accurate] */
int dictTest(int argc, char **argv, int flags) {
    long j;
    long long start, elapsed;
    dict *dict = dictCreate(&BenchmarkDictType);
    long count = 0;
    int accurate = (flags & REDIS_TEST_ACCURATE);

    if (argc == 4) {
        if (accurate) {
            count = 5000000;
        } else {
            count = strtol(argv[3],NULL,10);
        }
    } else {
        count = 5000;
    }

    start_benchmark();
    for (j = 0; j < count; j++) {
        int retval = dictAdd(dict,stringFromLongLong(j),(void*)j);
        assert(retval == DICT_OK);
    }
    end_benchmark("Inserting");
    assert((long)dictSize(dict) == count);

    /* Wait for rehashing. */
    while (dictIsRehashing(dict)) {
        dictRehashMilliseconds(dict,100);
    }

    start_benchmark();
    for (j = 0; j < count; j++) {
        char *key = stringFromLongLong(j);
        dictEntry *de = dictFind(dict,key);
        assert(de != NULL);
        zfree(key);
    }
    end_benchmark("Linear access of existing elements");

    start_benchmark();
    for (j = 0; j < count; j++) {
        char *key = stringFromLongLong(j);
        dictEntry *de = dictFind(dict,key);
        assert(de != NULL);
        zfree(key);
    }
    end_benchmark("Linear access of existing elements (2nd round)");

    start_benchmark();
    for (j = 0; j < count; j++) {
        char *key = stringFromLongLong(rand() % count);
        dictEntry *de = dictFind(dict,key);
        assert(de != NULL);
        zfree(key);
    }
    end_benchmark("Random access of existing elements");

    start_benchmark();
    for (j = 0; j < count; j++) {
        dictEntry *de = dictGetRandomKey(dict);
        assert(de != NULL);
    }
    end_benchmark("Accessing random keys");

    start_benchmark();
    for (j = 0; j < count; j++) {
        char *key = stringFromLongLong(rand() % count);
        key[0] = 'X';
        dictEntry *de = dictFind(dict,key);
        assert(de == NULL);
        zfree(key);
    }
    end_benchmark("Accessing missing");

    start_benchmark();
    for (j = 0; j < count; j++) {
        char *key = stringFromLongLong(j);
        int retval = dictDelete(dict,key);
        assert(retval == DICT_OK);
        key[0] += 17; /* Change first number to letter. */
        retval = dictAdd(dict,key,(void*)j);
        assert(retval == DICT_OK);
    }
    end_benchmark("Removing and adding");
    dictRelease(dict);
    return 0;
}
#endif
