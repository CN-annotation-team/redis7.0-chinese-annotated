/* Maxmemory directive handling (LRU eviction and other policies).
 *
 * ----------------------------------------------------------------------------
 *
 * Copyright (c) 2009-2016, Salvatore Sanfilippo <antirez at gmail dot com>
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
#include "bio.h"
#include "atomicvar.h"
#include "script.h"
#include <math.h>

/* 该文件的入口函数是 performEvictions 函数，
 * 该函数是在 server.c 的 processCommand 函数中被调用 */

/* ----------------------------------------------------------------------------
 * Data structures
 * --------------------------------------------------------------------------*/

/* To improve the quality of the LRU approximation we take a set of keys
 * that are good candidate for eviction across performEvictions() calls.
 *
 * Entries inside the eviction pool are taken ordered by idle time, putting
 * greater idle times to the right (ascending order).
 *
 * When an LFU policy is used instead, a reverse frequency indication is used
 * instead of the idle time, so that we still evict by larger value (larger
 * inverse frequency means to evict keys with the least frequent accesses).
 *
 * Empty entries have the key pointer set to NULL. */
#define EVPOOL_SIZE 16
#define EVPOOL_CACHED_SDS_SIZE 255
/* 定义淘汰池中每个节点的数据结构 */
struct evictionPoolEntry {
    /* 存的是 lru/ttl 相关的空闲时间或者 LFU 的反向频率 */
    unsigned long long idle;    /* Object idle time (inverse frequency for LFU) */
    /* 键名 */
    sds key;                    /* Key name. */
    sds cached;                 /* Cached SDS object for key name. */
    int dbid;                   /* Key DB number. */
};

/* 该指针会指向 evictionPool 实例 */
static struct evictionPoolEntry *EvictionPoolLRU;

/* ----------------------------------------------------------------------------
 * Implementation of eviction, aging and LRU
 * --------------------------------------------------------------------------*/

/* Return the LRU clock, based on the clock resolution. This is a time
 * in a reduced-bits format that can be used to set and check the
 * object->lru field of redisObject structures. */
/* 生成一个 LRU 时钟，LRU_CLOCK_RESOLUTION 是 1000，这里获取的是时间戳的秒数
 * 然后取后 LRU_CLOCK_MAX(24) 位，这个生成的值最后会作为一个 redisObject 实例的 lru 属性上
 * 该属性占用 24 位 */
unsigned int getLRUClock(void) {
    return (mstime()/LRU_CLOCK_RESOLUTION) & LRU_CLOCK_MAX;
}

/* This function is used to obtain the current LRU clock.
 * If the current resolution is lower than the frequency we refresh the
 * LRU clock (as it should be in production servers) we return the
 * precomputed value, otherwise we need to resort to a system call. */
unsigned int LRU_CLOCK(void) {
    unsigned int lruclock;
    if (1000/server.hz <= LRU_CLOCK_RESOLUTION) {
        atomicGet(server.lruclock,lruclock);
    } else {
        lruclock = getLRUClock();
    }
    return lruclock;
}

/* Given an object returns the min number of milliseconds the object was never
 * requested, using an approximated LRU algorithm. */
/* 获取对象距上一次被访问过去了多久，使用毫秒表示，该函数用于评估在 LRU 策略下，键值对的空闲时间
 * 空闲时间越大，表示对象最近越少被使用 */
unsigned long long estimateObjectIdleTime(robj *o) {
    /* 获取当前的 LRU 时钟 */
    unsigned long long lruclock = LRU_CLOCK();
    /* 如果当前 LRU 时钟 >= 给定对象的 LRU，可以直接通过相减来计算
     * 如果大于，认为上一次访问的时间已经过了一轮 LRU 时钟，需要在相减的基础上加一轮 LRU 时钟 */
    if (lruclock >= o->lru) {
        return (lruclock - o->lru) * LRU_CLOCK_RESOLUTION;
    } else {
        return (lruclock + (LRU_CLOCK_MAX - o->lru)) *
                    LRU_CLOCK_RESOLUTION;
    }
}

/* LRU approximation algorithm
 *
 * Redis uses an approximation of the LRU algorithm that runs in constant
 * memory. Every time there is a key to expire, we sample N keys (with
 * N very small, usually in around 5) to populate a pool of best keys to
 * evict of M keys (the pool size is defined by EVPOOL_SIZE).
 *
 * The N keys sampled are added in the pool of good keys to expire (the one
 * with an old access time) if they are better than one of the current keys
 * in the pool.
 *
 * After the pool is populated, the best key we have in the pool is expired.
 * However note that we don't remove keys from the pool when they are deleted
 * so the pool may contain keys that no longer exist.
 *
 * When we try to evict a key, and all the entries in the pool don't exist
 * we populate it again. This time we'll be sure that the pool has at least
 * one key that can be evicted, if there is at least one key that can be
 * evicted in the whole database. */

/* Create a new eviction pool. */
/* 创建一个淘汰池 */
void evictionPoolAlloc(void) {
    struct evictionPoolEntry *ep;
    int j;
    /* 淘汰池总共 EVPOOL_SIZE（16） 个元素 */
    ep = zmalloc(sizeof(*ep)*EVPOOL_SIZE);
    for (j = 0; j < EVPOOL_SIZE; j++) {
        ep[j].idle = 0;
        ep[j].key = NULL;
        ep[j].cached = sdsnewlen(NULL,EVPOOL_CACHED_SDS_SIZE);
        ep[j].dbid = 0;
    }
    EvictionPoolLRU = ep;
}

/* This is a helper function for performEvictions(), it is used in order
 * to populate the evictionPool with a few entries every time we want to
 * expire a key. Keys with idle time bigger than one of the current
 * keys are added. Keys are always added if there are free entries.
 *
 * We insert keys on place in ascending order, so keys with the smaller
 * idle time are on the left, and keys with the higher idle time on the
 * right. */
/* sampledict 是取样字典，会根据给定的淘汰策略是对所有数据还是过期数据来选择数据库的数据字典或过期字典
 * keydict 是数据库的数据字典 */
void evictionPoolPopulate(int dbid, dict *sampledict, dict *keydict, struct evictionPoolEntry *pool) {
    int j, k, count;
    /* 定义一个用来存储样本的临时数组 */
    dictEntry *samples[server.maxmemory_samples];

    /* 从 sampledict 字典中随机拿最多 server.maxmemory_samples (默认为 5，可以在配置文件中通过 maxmemory-samples 设置)
     * 个元素放入 samples 数组中，
     * 注：这里不是一定会拿 server.maxmemory_samples 个键值对，存在小于该数量的情况，具体看 dict.c 该函数的逻辑 */
    count = dictGetSomeKeys(sampledict,samples,server.maxmemory_samples);
    for (j = 0; j < count; j++) {
        unsigned long long idle;
        sds key;
        robj *o;
        dictEntry *de;

        de = samples[j];
        key = dictGetKey(de);

        /* If the dictionary we are sampling from is not the main
         * dictionary (but the expires one) we need to lookup the key
         * again in the key dictionary to obtain the value object. */
        /* 如果我们取样的数据不是从数据字典中取的，而是从过期字典中取的，就需要去数据
         * 字典中获取对应的键值对 */
        if (server.maxmemory_policy != MAXMEMORY_VOLATILE_TTL) {
            if (sampledict != keydict) de = dictFind(keydict, key);
            o = dictGetVal(de);
        }

        /* Calculate the idle time according to the policy. This is called
         * idle just because the code initially handled LRU, but is in fact
         * just a score where an higher score means better candidate. */
        /* 这里根据不同的淘汰策略来评估一个键值对的空闲时间 */
        if (server.maxmemory_policy & MAXMEMORY_FLAG_LRU) {
            /* 对于 LRU 直接拿最后一次访问的时间间隔 */
            idle = estimateObjectIdleTime(o);
        } else if (server.maxmemory_policy & MAXMEMORY_FLAG_LFU) {
            /* When we use an LRU policy, we sort the keys by idle time
             * so that we expire keys starting from greater idle time.
             * However when the policy is an LFU one, we have a frequency
             * estimation, and we want to evict keys with lower frequency
             * first. So inside the pool we put objects using the inverted
             * frequency subtracting the actual frequency to the maximum
             * frequency of 255. */
            /* LFU 需要根据最后一个访问时间间隔和对象的访问频度来做计算，可以看具体方法，也可以大概看做
             * 访问次数越多，idle 越小
             * 上一次访问时间距离现在的间隔越小，idle 越小
             * 即 idle 存储的是反向频率，即使用频率较低的，反向后数值较大，这样升序后是排在后面 */
            idle = 255-LFUDecrAndReturn(o);
        } else if (server.maxmemory_policy == MAXMEMORY_VOLATILE_TTL) {
            /* In this case the sooner the expire the better. */
            /* 如果是 VOLATILE_TTL 直接用当前时间减去过期时间 */
            idle = ULLONG_MAX - (long)dictGetVal(de);
        } else {
            serverPanic("Unknown eviction policy in evictionPoolPopulate()");
        }

        /* Insert the element inside the pool.
         * First, find the first empty bucket or the first populated
         * bucket that has an idle time smaller than our idle time. */
        k = 0;
        while (k < EVPOOL_SIZE &&
               pool[k].key &&
               pool[k].idle < idle) k++;
        if (k == 0 && pool[EVPOOL_SIZE-1].key != NULL) {
            /* Can't insert if the element is < the worst element we have
             * and there are no empty buckets. */
            /* 这里 k==0 就是表示淘汰池里最小的 idle 都大于当前对象的 idle，说明该对象比池子里的所有元素都更热点
             * 且池子里最后一个位置也有元素，表示池子满了，所以不应该放入池子，继续处理下一个元素 */
            continue;
        } else if (k < EVPOOL_SIZE && pool[k].key == NULL) {
            /* Inserting into empty position. No setup needed before insert. */
            /* 这里 pool[k].key == null 表示当前对象是池子里 idle 最大的元素，直接插入就行了 */
        } else {
            /* Inserting in the middle. Now k points to the first element
             * greater than the element to insert.  */
            /* 上面已经处理了元素处于边缘位置的情况了，下面是元素出现在池子中间 */
            if (pool[EVPOOL_SIZE-1].key == NULL) {
                /* Free space on the right? Insert at k shifting
                 * all the elements from k to end to the right. */

                /* 当前 if 表示池子还有空间 */
                /* Save SDS before overwriting. */
                sds cached = pool[EVPOOL_SIZE-1].cached;
                /* 这里是把 k 位置开始的元素都后移了一个位置 */
                memmove(pool+k+1,pool+k,
                    sizeof(pool[0])*(EVPOOL_SIZE-k-1));
                pool[k].cached = cached;
            } else {
                /* 这里是池子满了的情况 */
                /* No free space on right? Insert at k-1 */
                k--;
                /* Shift all elements on the left of k (included) to the
                 * left, so we discard the element with smaller idle time. */
                sds cached = pool[0].cached; /* Save SDS before overwriting. */
                /* 将第一个元素从淘汰池中移除掉 */
                if (pool[0].key != pool[0].cached) sdsfree(pool[0].key);
                /* k 位置之前的元素向前移动一个位置 */
                memmove(pool,pool+1,sizeof(pool[0])*k);
                pool[k].cached = cached;
            }
        }

        /* Try to reuse the cached SDS string allocated in the pool entry,
         * because allocating and deallocating this object is costly
         * (according to the profiler, not my fantasy. Remember:
         * premature optimization bla bla bla. */
        /* 这里就是将当前对象放入淘汰池的第 k 个位置，并封装信息 */
        int klen = sdslen(key);
        if (klen > EVPOOL_CACHED_SDS_SIZE) {
            pool[k].key = sdsdup(key);
        } else {
            memcpy(pool[k].cached,key,klen+1);
            sdssetlen(pool[k].cached,klen);
            pool[k].key = pool[k].cached;
        }
        pool[k].idle = idle;
        pool[k].dbid = dbid;
    }
}

/* ----------------------------------------------------------------------------
 * LFU (Least Frequently Used) implementation.

 * We have 24 total bits of space in each object in order to implement
 * an LFU (Least Frequently Used) eviction policy, since we re-use the
 * LRU field for this purpose.
 *
 * We split the 24 bits into two fields:
 *
 *          16 bits      8 bits
 *     +----------------+--------+
 *     + Last decr time | LOG_C  |
 *     +----------------+--------+
 *
 * LOG_C is a logarithmic counter that provides an indication of the access
 * frequency. However this field must also be decremented otherwise what used
 * to be a frequently accessed key in the past, will remain ranked like that
 * forever, while we want the algorithm to adapt to access pattern changes.
 *
 * So the remaining 16 bits are used in order to store the "decrement time",
 * a reduced-precision Unix time (we take 16 bits of the time converted
 * in minutes since we don't care about wrapping around) where the LOG_C
 * counter is halved if it has an high value, or just decremented if it
 * has a low value.
 *
 * New keys don't start at zero, in order to have the ability to collect
 * some accesses before being trashed away, so they start at LFU_INIT_VAL.
 * The logarithmic increment performed on LOG_C takes care of LFU_INIT_VAL
 * when incrementing the key, so that keys starting at LFU_INIT_VAL
 * (or having a smaller value) have a very high chance of being incremented
 * on access.
 *
 * During decrement, the value of the logarithmic counter is halved if
 * its current value is greater than two times the LFU_INIT_VAL, otherwise
 * it is just decremented by one.
 * --------------------------------------------------------------------------*/

/* Return the current time in minutes, just taking the least significant
 * 16 bits. The returned time is suitable to be stored as LDT (last decrement
 * time) for the LFU implementation. */
/* 获取 LFU 时间。
 * 获取当前时间的时间戳，用分钟表示，然后取后 16 位 */
unsigned long LFUGetTimeInMinutes(void) {
    return (server.unixtime/60) & 65535;
}

/* Given an object last access time, compute the minimum number of minutes
 * that elapsed since the last access. Handle overflow (ldt greater than
 * the current 16 bits minutes time) considering the time as wrapping
 * exactly once. */
/* 根据一个对象的最后一次访问时间，获取当前时间与其之间的时间间隔 */
unsigned long LFUTimeElapsed(unsigned long ldt) {
    /* 获取当前 LFU 时间 */
    unsigned long now = LFUGetTimeInMinutes();
    /* 如果当前 LFU 时间大于对象最后一次访问的 LFU 时间，直接用当前 LFU 时间减去最后一次访问的 LFU 时间 */
    if (now >= ldt) return now-ldt;
    /* 如果当前 LFU 时间小于对象最后一次访问的 LFU 时间（其实就是两个时间不再一个 LFU 时间轮中，当前时间大了
     * 最后一次访问时间一个 LFU 时间轮，因为 LFU 时间只有 16bit 位，所以一轮 LFU 时间只有 65535 分钟） */
    return 65535-ldt+now;
}

/* Logarithmically increment a counter. The greater is the current counter value
 * the less likely is that it gets really incremented. Saturate it at 255. */
uint8_t LFULogIncr(uint8_t counter) {
    if (counter == 255) return 255;
    double r = (double)rand()/RAND_MAX;
    double baseval = counter - LFU_INIT_VAL;
    if (baseval < 0) baseval = 0;
    double p = 1.0/(baseval*server.lfu_log_factor+1);
    if (r < p) counter++;
    return counter;
}

/* If the object decrement time is reached decrement the LFU counter but
 * do not update LFU fields of the object, we update the access time
 * and counter in an explicit way when the object is really accessed.
 * And we will times halve the counter according to the times of
 * elapsed time than server.lfu_decay_time.
 * Return the object frequency counter.
 *
 * This function is used in order to scan the dataset for the best object
 * to fit: as we check for the candidate, we incrementally decrement the
 * counter of the scanned objects if needed. */
/* 该函数用来评估 LFU 内存淘汰策略下，对象被使用的衰减访问计数 */
unsigned long LFUDecrAndReturn(robj *o) {
    /* o->lru 总共有 24 bit */
    /* 获取高 16 位，高 16 位表示最近使用的访问时间 */
    unsigned long ldt = o->lru >> 8;
    /* 获取低 8 位，低 8 位表示最近使用的次数 */
    unsigned long counter = o->lru & 255;
    /* 这里会根据 LFU 时间因子来计算最后一次访问时间的影响，lfu_decay_time 默认为 1
     * num_periods = 对象的最后一次访问时间到当前的间隔（单位是分钟） / 因子 */
    unsigned long num_periods = server.lfu_decay_time ? LFUTimeElapsed(ldt) / server.lfu_decay_time : 0;
    /* 如果时间影响超过访问频度的影响，直接返回 0，否则返回他们之间的差值，即返回衰减后的访问计数 */
    if (num_periods)
        counter = (num_periods > counter) ? 0 : counter - num_periods;
    return counter;
}

/* We don't want to count AOF buffers and slaves output buffers as
 * used memory: the eviction should use mostly data size, because
 * it can cause feedback-loop when we push DELs into them, putting
 * more and more DELs will make them bigger, if we count them, we
 * need to evict more keys, and then generate more DELs, maybe cause
 * massive eviction loop, even all keys are evicted.
 *
 * This function returns the sum of AOF and replication buffer. */
/* 该函数用于计算 复制缓冲区多出来的空间 + aof 缓冲区的空间的大小 */
size_t freeMemoryGetNotCountedMemory(void) {
    size_t overhead = 0;

    /* Since all replicas and replication backlog share global replication
     * buffer, we think only the part of exceeding backlog size is the extra
     * separate consumption of replicas.
     *
     * Note that although the backlog is also initially incrementally grown
     * (pushing DELs consumes memory), it'll eventually stop growing and
     * remain constant in size, so even if its creation will cause some
     * eviction, it's capped, and also here to stay (no resonance effect)
     *
     * Note that, because we trim backlog incrementally in the background,
     * backlog size may exceeds our setting if slow replicas that reference
     * vast replication buffer blocks disconnect. To avoid massive eviction
     * loop, we don't count the delayed freed replication backlog into used
     * memory even if there are no replicas, i.e. we still regard this memory
     * as replicas'. */
    /* 计算复制缓冲区多出来的空间，先判断复制缓冲区分配的总空间是否大于复制缓冲区存储的数据大小 */
    if ((long long)server.repl_buffer_mem > server.repl_backlog_size) {
        /* We use list structure to manage replication buffer blocks, so backlog
         * also occupies some extra memory, we can't know exact blocks numbers,
         * we only get approximate size according to per block size. */
        /* 下面的计算分步解释一下，extra_approx_size（是个估算值） 其实是复制缓冲区用到的结构体的内存大概使用量（不包括实际的数据）：
         * 1) server.repl_backlog_size / PROTO_REPLY_CHUNK_BYTES + 1
         *    用 复制缓冲区中总数据大小 / 每个复制缓冲区块的大小 ，因为整数除法是向下取整，存在最后一块未满的情况，
         *    这里直接 +1，不做刚好除尽的情况，所以最后的的结果是估算值
         * 2) sizeof(replBufBlock) + sizeof(listNode)
         *    一个复制缓冲区块结构体的内存占用 + 链表节点内存占用（复制缓冲区块是使用链表的形式来存储的），
         *    这里计算出每个复制缓冲区块额外需要多少空间
         * 3) 上面的两个结果做乘法就是复制缓冲区块数据结构占用的内存大小
         */
        size_t extra_approx_size =
            (server.repl_backlog_size/PROTO_REPLY_CHUNK_BYTES + 1) *
            (sizeof(replBufBlock)+sizeof(listNode));
        /* 数据大小加上这些数据使用的结构占用的大小 */
        size_t counted_mem = server.repl_backlog_size + extra_approx_size;
        /* 如果内存分配超过上面计算的大小，表示有剩余空间 */
        if (server.repl_buffer_mem > counted_mem) {
            /* 获取复制缓冲区剩余空间大小 */
            overhead += (server.repl_buffer_mem - counted_mem);
        }
    }
    /* 再加上 aof 缓冲区大小 */
    if (server.aof_state != AOF_OFF) {
        overhead += sdsAllocSize(server.aof_buf);
    }
    return overhead;
}

/* Get the memory status from the point of view of the maxmemory directive:
 * if the memory used is under the maxmemory setting then C_OK is returned.
 * Otherwise, if we are over the memory limit, the function returns
 * C_ERR.
 *
 * The function may return additional info via reference, only if the
 * pointers to the respective arguments is not NULL. Certain fields are
 * populated only when C_ERR is returned:
 *
 *  'total'     total amount of bytes used.
 *              (Populated both for C_ERR and C_OK)
 *
 *  'logical'   the amount of memory used minus the slaves/AOF buffers.
 *              (Populated when C_ERR is returned)
 *
 *  'tofree'    the amount of memory that should be released
 *              in order to return back into the memory limits.
 *              (Populated when C_ERR is returned)
 *
 *  'level'     this usually ranges from 0 to 1, and reports the amount of
 *              memory currently used. May be > 1 if we are over the memory
 *              limit.
 *              (Populated both for C_ERR and C_OK)
 */
/* 该函数用来判断当前 redis 的内存使用量是否达到 redis 内存使用量的限制了，
 * 如果没达到就返回 C_OK，否则返回 C_ERR */
int getMaxmemoryState(size_t *total, size_t *logical, size_t *tofree, float *level) {
    size_t mem_reported, mem_used, mem_tofree;

    /* Check if we are over the memory usage limit. If we are not, no need
     * to subtract the slaves output buffers. We can just return ASAP. */
    /* 获取当前 redis 分配的内存总量 */
    mem_reported = zmalloc_used_memory();
    /* 将使用内存总量赋值给 total */
    if (total) *total = mem_reported;

    /* We may return ASAP if there is no need to compute the level. */
    /* 如果没有设置最大内存限制，返回 C_OK */
    if (!server.maxmemory) {
        if (level) *level = 0;
        return C_OK;
    }
    /* 内存的使用量没有达到最大内存限制，且给定的 level 不为 0，返回 C_OK */
    if (mem_reported <= server.maxmemory && !level) return C_OK;

    /* Remove the size of slaves output buffers and AOF buffer from the
     * count of used memory. */
    mem_used = mem_reported;
    /* 计算主从复制缓冲区的剩余空间大小 + aof 缓冲区大小 */
    size_t overhead = freeMemoryGetNotCountedMemory();
    /* 总内存大小 - 上面计算的内存空间大小 = 实际使用的空间大小 */
    mem_used = (mem_used > overhead) ? mem_used-overhead : 0;

    /* Compute the ratio of memory usage. */
    /* 计算实际内存使用百分比 */
    if (level) *level = (float)mem_used / (float)server.maxmemory;

    if (mem_reported <= server.maxmemory) return C_OK;

    /* Check if we are still over the memory limit. */
    /* 如果实际使用内存没达到最大内存使用量返回 C_OK */
    if (mem_used <= server.maxmemory) return C_OK;

    /* Compute how much memory we need to free. */
    /* 计算需要释放的内存大小 */
    mem_tofree = mem_used - server.maxmemory;

    /* 赋值逻辑上使用的内存大小，和需要释放的内存大小 */
    if (logical) *logical = mem_used;
    if (tofree) *tofree = mem_tofree;

    return C_ERR;
}

/* Return 1 if used memory is more than maxmemory after allocating more memory,
 * return 0 if not. Redis may reject user's requests or evict some keys if used
 * memory exceeds maxmemory, especially, when we allocate huge memory at once. */
int overMaxmemoryAfterAlloc(size_t moremem) {
    if (!server.maxmemory) return  0; /* No limit. */

    /* Check quickly. */
    size_t mem_used = zmalloc_used_memory();
    if (mem_used + moremem <= server.maxmemory) return 0;

    size_t overhead = freeMemoryGetNotCountedMemory();
    mem_used = (mem_used > overhead) ? mem_used - overhead : 0;
    return mem_used + moremem > server.maxmemory;
}

/* The evictionTimeProc is started when "maxmemory" has been breached and
 * could not immediately be resolved.  This will spin the event loop with short
 * eviction cycles until the "maxmemory" condition has resolved or there are no
 * more evictable items.  */
/* 标识是否有需要执行的淘汰事件 */
static int isEvictionProcRunning = 0;
/* 淘汰事件的处理函数，就是调用 performEvictions 方法 */
static int evictionTimeProc(
        struct aeEventLoop *eventLoop, long long id, void *clientData) {
    UNUSED(eventLoop);
    UNUSED(id);
    UNUSED(clientData);

    if (performEvictions() == EVICT_RUNNING) return 0;  /* keep evicting */

    /* For EVICT_OK - things are good, no need to keep evicting.
     * For EVICT_FAIL - there is nothing left to evict.  */
    /* 将执行标志置 0 */
    isEvictionProcRunning = 0;
    return AE_NOMORE;
}

/* 如果当前没有运行中的淘汰事件，就向全局事件循环器中加一个淘汰事件 */
void startEvictionTimeProc(void) {
    if (!isEvictionProcRunning) {
        isEvictionProcRunning = 1;
        aeCreateTimeEvent(server.el, 0,
                evictionTimeProc, NULL, NULL);
    }
}

/* Check if it's safe to perform evictions.
 *   Returns 1 if evictions can be performed
 *   Returns 0 if eviction processing should be skipped
 */
/* 判断是否可以执行内存淘汰 */
static int isSafeToPerformEvictions(void) {
    /* - There must be no script in timeout condition.
     * - Nor we are loading data right now.  */
    /* 正在处理脚本，或者还在加载数据，不能进行内存淘汰 */
    if (isInsideYieldingLongCommand() || server.loading) return 0;

    /* By default replicas should ignore maxmemory
     * and just be masters exact copies. */
    /* 当前节点是从节点且从节点是直接从主节点拷贝数据，不进行内存淘汰
     * 注：主节点删除数据的时候会告诉从节点也要删除对应的数据 */
    if (server.masterhost && server.repl_slave_ignore_maxmemory) return 0;

    /* When clients are paused the dataset should be static not just from the
     * POV of clients not being able to write, but also from the POV of
     * expires and evictions of keys not being performed. */
    /* 客户端处于暂停状态 */
    if (checkClientPauseTimeoutAndReturnIfPaused()) return 0;

    return 1;
}

/* Algorithm for converting tenacity (0-100) to a time limit.  */
static unsigned long evictionTimeLimitUs() {
    serverAssert(server.maxmemory_eviction_tenacity >= 0);
    serverAssert(server.maxmemory_eviction_tenacity <= 100);

    if (server.maxmemory_eviction_tenacity <= 10) {
        /* A linear progression from 0..500us */
        return 50uL * server.maxmemory_eviction_tenacity;
    }

    if (server.maxmemory_eviction_tenacity < 100) {
        /* A 15% geometric progression, resulting in a limit of ~2 min at tenacity==99  */
        return (unsigned long)(500.0 * pow(1.15, server.maxmemory_eviction_tenacity - 10.0));
    }

    return ULONG_MAX;   /* No limit to eviction time */
}

/* Check that memory usage is within the current "maxmemory" limit.  If over
 * "maxmemory", attempt to free memory by evicting data (if it's safe to do so).
 *
 * It's possible for Redis to suddenly be significantly over the "maxmemory"
 * setting.  This can happen if there is a large allocation (like a hash table
 * resize) or even if the "maxmemory" setting is manually adjusted.  Because of
 * this, it's important to evict for a managed period of time - otherwise Redis
 * would become unresponsive while evicting.
 *
 * The goal of this function is to improve the memory situation - not to
 * immediately resolve it.  In the case that some items have been evicted but
 * the "maxmemory" limit has not been achieved, an aeTimeProc will be started
 * which will continue to evict items until memory limits are achieved or
 * nothing more is evictable.
 *
 * This should be called before execution of commands.  If EVICT_FAIL is
 * returned, commands which will result in increased memory usage should be
 * rejected.
 *
 * Returns:
 *   EVICT_OK       - memory is OK or it's not possible to perform evictions now
 *   EVICT_RUNNING  - memory is over the limit, but eviction is still processing
 *   EVICT_FAIL     - memory is over the limit, and there's nothing to evict
 * */
int performEvictions(void) {
    /* Note, we don't goto update_metrics here because this check skips eviction
     * as if it wasn't triggered. it's a fake EVICT_OK. */
    if (!isSafeToPerformEvictions()) return EVICT_OK;

    int keys_freed = 0;
    size_t mem_reported, mem_tofree;
    /* 需要释放的内存大小 */
    long long mem_freed; /* May be negative */
    mstime_t latency, eviction_latency;
    long long delta;
    int slaves = listLength(server.slaves);
    int result = EVICT_FAIL;

    /* 判断内存使用是否达到最大内存限制，对 mem_tofree 进行了赋值，返回 C_OK 表示未达到 */
    if (getMaxmemoryState(&mem_reported,NULL,&mem_tofree,NULL) == C_OK) {
        result = EVICT_OK;
        goto update_metrics;
    }

    /* 设置的淘汰策略为永不淘汰的情况 */
    if (server.maxmemory_policy == MAXMEMORY_NO_EVICTION) {
        result = EVICT_FAIL;  /* We need to free memory, but policy forbids. */
        goto update_metrics;
    }

    unsigned long eviction_time_limit_us = evictionTimeLimitUs();

    mem_freed = 0;

    latencyStartMonitor(latency);

    monotime evictionTimer;
    elapsedStart(&evictionTimer);

    /* Unlike active-expire and blocked client, we can reach here from 'CONFIG SET maxmemory'
     * so we have to back-up and restore server.core_propagates. */
    int prev_core_propagates = server.core_propagates;
    serverAssert(server.also_propagate.numops == 0);
    server.core_propagates = 1;
    server.propagate_no_multi = 1;

    /* 循环处理，直到释放的内存不小于需要释放的内存 */
    while (mem_freed < (long long)mem_tofree) {
        int j, k, i;
        static unsigned int next_db = 0;
        sds bestkey = NULL;
        int bestdbid;
        redisDb *db;
        dict *dict;
        dictEntry *de;

        if (server.maxmemory_policy & (MAXMEMORY_FLAG_LRU|MAXMEMORY_FLAG_LFU) ||
            server.maxmemory_policy == MAXMEMORY_VOLATILE_TTL)
        {
            struct evictionPoolEntry *pool = EvictionPoolLRU;

            while (bestkey == NULL) {
                unsigned long total_keys = 0, keys;

                /* We don't want to make local-db choices when expiring keys,
                 * so to start populate the eviction pool sampling keys from
                 * every DB. */
                /* 对 redis 的所有数据库做数据淘汰 */
                for (i = 0; i < server.dbnum; i++) {
                    db = server.db+i;
                    /* 如果淘汰策略是 ALLKEYS，需要淘汰的数据从数据字典中选取，否则在过期字典中选取 */
                    dict = (server.maxmemory_policy & MAXMEMORY_FLAG_ALLKEYS) ?
                            db->dict : db->expires;
                    if ((keys = dictSize(dict)) != 0) {
                        /* 做一次淘汰处理，会把符合条件的数据放入淘汰池中 */
                        evictionPoolPopulate(i, dict, db->dict, pool);
                        total_keys += keys;
                    }
                }
                if (!total_keys) break; /* No keys to evict. */

                /* Go backward from best to worst element to evict. */
                /* 淘汰池是根据对象的 idle 升序存储的，idle 越大，越先被淘汰 */
                /* 对淘汰池从尾部向头部遍历 */
                for (k = EVPOOL_SIZE-1; k >= 0; k--) {
                    if (pool[k].key == NULL) continue;
                    /* 获取淘汰对象所在的数据库索引 */
                    bestdbid = pool[k].dbid;

                    /* 根据是否是对所有数据还是过期数据，从数据字典和过期字典中获取该淘汰键值对 */
                    if (server.maxmemory_policy & MAXMEMORY_FLAG_ALLKEYS) {
                        de = dictFind(server.db[bestdbid].dict,
                            pool[k].key);
                    } else {
                        de = dictFind(server.db[bestdbid].expires,
                            pool[k].key);
                    }

                    /* Remove the entry from the pool. */
                    /* 擦除淘汰池中当前位置的信息 */
                    if (pool[k].key != pool[k].cached)
                        sdsfree(pool[k].key);
                    pool[k].key = NULL;
                    pool[k].idle = 0;

                    /* If the key exists, is our pick. Otherwise it is
                     * a ghost and we need to try the next element. */
                    if (de) {
                        /* 获取需要淘汰的 key，拿到需要淘汰的键，就跳出当前循环 */
                        bestkey = dictGetKey(de);
                        break;
                    } else {
                        /* Ghost... Iterate again. */
                    }
                }
            }
        }

        /* volatile-random and allkeys-random policy */
        /* 下面是随机淘汰策略 */
        else if (server.maxmemory_policy == MAXMEMORY_ALLKEYS_RANDOM ||
                 server.maxmemory_policy == MAXMEMORY_VOLATILE_RANDOM)
        {
            /* When evicting a random key, we try to evict a key for
             * each DB, so we use the static 'next_db' variable to
             * incrementally visit all DBs. */
            for (i = 0; i < server.dbnum; i++) {
                j = (++next_db) % server.dbnum;
                db = server.db+j;
                dict = (server.maxmemory_policy == MAXMEMORY_ALLKEYS_RANDOM) ?
                        db->dict : db->expires;
                if (dictSize(dict) != 0) {
                    /* 从字典中随机获取一个键值对 */
                    de = dictGetRandomKey(dict);
                    /* 赋值需要淘汰的键 */
                    bestkey = dictGetKey(de);
                    bestdbid = j;
                    break;
                }
            }
        }

        /* Finally remove the selected key. */
        /* 下面是释放键值对的逻辑 */
        if (bestkey) {
            db = server.db+bestdbid;
            robj *keyobj = createStringObject(bestkey,sdslen(bestkey));
            /* We compute the amount of memory freed by db*Delete() alone.
             * It is possible that actually the memory needed to propagate
             * the DEL in AOF and replication link is greater than the one
             * we are freeing removing the key, but we can't account for
             * that otherwise we would never exit the loop.
             *
             * Same for CSC invalidation messages generated by signalModifiedKey.
             *
             * AOF and Output buffer memory will be freed eventually so
             * we only care about memory used by the key space. */
            /* 先记录当前的内存使用量 */
            delta = (long long) zmalloc_used_memory();
            latencyStartMonitor(eviction_latency);
            /* 删除数据，惰性删除和或者是同步删除 */
            if (server.lazyfree_lazy_eviction)
                dbAsyncDelete(db,keyobj);
            else
                dbSyncDelete(db,keyobj);
            latencyEndMonitor(eviction_latency);
            latencyAddSampleIfNeeded("eviction-del",eviction_latency);
            /* 计算删除数据前后的差值 */
            delta -= (long long) zmalloc_used_memory();
            /* 累积差值到释放的空间 */
            mem_freed += delta;
            server.stat_evictedkeys++;
            /* 触发 db 数据库 key 发生改变钩子函数 */
            signalModifiedKey(NULL,db,keyobj);
            /* 触发数据库空间改变钩子函数 */
            notifyKeyspaceEvent(NOTIFY_EVICTED, "evicted",
                keyobj, db->id);
            /* 广播删除操作给所有从节点 */
            propagateDeletion(db,keyobj,server.lazyfree_lazy_eviction);
            decrRefCount(keyobj);
            keys_freed++;

            /* 每释放 16 个键值对做一次下面的操作，加速释放（渐进式淘汰） */
            if (keys_freed % 16 == 0) {
                /* When the memory to free starts to be big enough, we may
                 * start spending so much time here that is impossible to
                 * deliver data to the replicas fast enough, so we force the
                 * transmission here inside the loop. */
                /* 如果当前节点是主节点，将待发送给从节点的数据发送出去，清空其输出缓冲区 */
                if (slaves) flushSlavesOutputBuffers();

                /* Normally our stop condition is the ability to release
                 * a fixed, pre-computed amount of memory. However when we
                 * are deleting objects in another thread, it's better to
                 * check, from time to time, if we already reached our target
                 * memory, since the "mem_freed" amount is computed only
                 * across the dbAsyncDelete() call, while the thread can
                 * release the memory all the time. */
                /* 如果是惰性释放空间，需要每隔一段时间再判断当前内存是否下降到最大内存使用量以下，
                 * 如果达到了，就退出本轮淘汰 */
                if (server.lazyfree_lazy_eviction) {
                    if (getMaxmemoryState(NULL,NULL,NULL,NULL) == C_OK) {
                        break;
                    }
                }

                /* After some time, exit the loop early - even if memory limit
                 * hasn't been reached.  If we suddenly need to free a lot of
                 * memory, don't want to spend too much time here.  */
                /* 如果本轮淘汰时间结束了，但是释放的内存还不够 */
                if (elapsedUs(evictionTimer) > eviction_time_limit_us) {
                    // We still need to free memory - start eviction timer proc
                    /* 添加一个淘汰事件到事件循环器中 */
                    startEvictionTimeProc();
                    break;
                }
            }
        } else {
            goto cant_free; /* nothing to free... */
        }
    }
    /* at this point, the memory is OK, or we have reached the time limit */
    result = (isEvictionProcRunning) ? EVICT_RUNNING : EVICT_OK;

cant_free:
    if (result == EVICT_FAIL) {
        /* At this point, we have run out of evictable items.  It's possible
         * that some items are being freed in the lazyfree thread.  Perform a
         * short wait here if such jobs exist, but don't wait long.  */
        mstime_t lazyfree_latency;
        latencyStartMonitor(lazyfree_latency);
        while (bioPendingJobsOfType(BIO_LAZY_FREE) &&
              elapsedUs(evictionTimer) < eviction_time_limit_us) {
            if (getMaxmemoryState(NULL,NULL,NULL,NULL) == C_OK) {
                result = EVICT_OK;
                break;
            }
            usleep(eviction_time_limit_us < 1000 ? eviction_time_limit_us : 1000);
        }
        latencyEndMonitor(lazyfree_latency);
        latencyAddSampleIfNeeded("eviction-lazyfree",lazyfree_latency);
    }

    serverAssert(server.core_propagates); /* This function should not be re-entrant */

    /* Propagate all DELs */
    propagatePendingCommands();

    server.core_propagates = prev_core_propagates;
    server.propagate_no_multi = 0;

    latencyEndMonitor(latency);
    latencyAddSampleIfNeeded("eviction-cycle",latency);

update_metrics:
    if (result == EVICT_RUNNING || result == EVICT_FAIL) {
        if (server.stat_last_eviction_exceeded_time == 0)
            elapsedStart(&server.stat_last_eviction_exceeded_time);
    } else if (result == EVICT_OK) {
        if (server.stat_last_eviction_exceeded_time != 0) {
            server.stat_total_eviction_exceeded_time += elapsedUs(server.stat_last_eviction_exceeded_time);
            server.stat_last_eviction_exceeded_time = 0;
        }
    }
    return result;
}
