/*
 * Copyright (c) 2009-2012, Pieter Noordhuis <pcnoordhuis at gmail dot com>
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

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "intset.h"
#include "zmalloc.h"
#include "endianconv.h"
#include "redisassert.h"

/* Note that these encodings are ordered, so:
 * INTSET_ENC_INT16 < INTSET_ENC_INT32 < INTSET_ENC_INT64. */

/* 设置不同编码对应的空间大小 */
#define INTSET_ENC_INT16 (sizeof(int16_t))
#define INTSET_ENC_INT32 (sizeof(int32_t))
#define INTSET_ENC_INT64 (sizeof(int64_t))

/* Return the required encoding for the provided value. */
/* 返回符合传参整数 v 编码的大小(从绝对值从大到小判断) */
static uint8_t _intsetValueEncoding(int64_t v) {
    if (v < INT32_MIN || v > INT32_MAX)
        return INTSET_ENC_INT64;
    else if (v < INT16_MIN || v > INT16_MAX)
        return INTSET_ENC_INT32;
    else
        return INTSET_ENC_INT16;
}

/* Return the value at pos, given an encoding. */
/* 明确编码 enc 的情况下返回给定位置元素 pos 的值 */
static int64_t _intsetGetEncoded(intset *is, int pos, uint8_t enc) {
    int64_t v64;
    int32_t v32;
    int16_t v16;

    if (enc == INTSET_ENC_INT64) {
        /* 从整数集合的源地址,拷贝64位大小的数据，到目的地址 */
        memcpy(&v64,((int64_t*)is->contents)+pos,sizeof(v64));
        /* 将对应内存的值转换成小端序存储(反序列化) */
        memrev64ifbe(&v64);
        return v64;
    } else if (enc == INTSET_ENC_INT32) {
        memcpy(&v32,((int32_t*)is->contents)+pos,sizeof(v32));
        memrev32ifbe(&v32);
        return v32;
    } else {
        memcpy(&v16,((int16_t*)is->contents)+pos,sizeof(v16));
        memrev16ifbe(&v16);
        return v16;
    }
}

/* Return the value at pos, using the configured encoding. */
/* 返回给定位置元素的值 */
static int64_t _intsetGet(intset *is, int pos) {
    return _intsetGetEncoded(is,pos,intrev32ifbe(is->encoding));
}

/* Set the value at pos, using the configured encoding. */
/* 将 value 设置到整数集合的给定位置 pos 上 */
static void _intsetSet(intset *is, int pos, int64_t value) {
    /* 存储的时候进行一次 intrev32ifbe 转换(序列化) */
    uint32_t encoding = intrev32ifbe(is->encoding);
    /* 判断符合编码的分支 */
    if (encoding == INTSET_ENC_INT64) {
        /* 对应数组赋值 */
        ((int64_t*)is->contents)[pos] = value;
        /* 对应内存的值转换成64位小端序存储
         * 例如: 数据的高位字节存储在地址的高端；低位字节存储在地址的低端(参考字节序的实现) */
        memrev64ifbe(((int64_t*)is->contents)+pos);
    } else if (encoding == INTSET_ENC_INT32) {
        ((int32_t*)is->contents)[pos] = value;
        memrev32ifbe(((int32_t*)is->contents)+pos);
    } else {
        ((int16_t*)is->contents)[pos] = value;
        memrev16ifbe(((int16_t*)is->contents)+pos);
    }
}

/* Create an empty intset. */
/* 初始化一个空的整数集合 */
intset *intsetNew(void) {
    intset *is = zmalloc(sizeof(intset));
    /* 初始化的编码为 INTSET_ENC_INT16
     * 例如: encoding 选项为 INTSET_ENC_INT16 时, 数组里的每个项都为一个 int16_t 类型的整数 */
    is->encoding = intrev32ifbe(INTSET_ENC_INT16);
    /* 集合内包含的元素数量 */
    is->length = 0;
    return is;
}

/* Resize the intset */
/* 重新分配整数集合的空间 */
static intset *intsetResize(intset *is, uint32_t len) {
    /* 新增元素空间大小 = 集合的元素数量*当前编码 */
    uint64_t size = (uint64_t)len*intrev32ifbe(is->encoding);
    /* 断言 分配空间后是否会超过 SIZE_MAX(指针持有的最大范围) */
    assert(size <= SIZE_MAX - sizeof(intset));
    /* 重新分配空间大小为 当前空间大小+新增空间大小 */
    is = zrealloc(is,sizeof(intset)+size);
    return is;
}

/* Search for the position of "value". Return 1 when the value was found and
 * sets "pos" to the position of the value within the intset. Return 0 when
 * the value is not present in the intset and sets "pos" to the position
 * where "value" can be inserted. */

/* 搜索 value 的位置。
 * 找到值时返回 1,并将 pos 设置为 intset 中值的位置。
 * 找不到值时返回 0,并将 pos 设置为可以插入 value 的位置。*/
static uint8_t intsetSearch(intset *is, int64_t value, uint32_t *pos) {
    int min = 0, max = intrev32ifbe(is->length)-1, mid = -1;
    int64_t cur = -1;

    /* The value can never be found when the set is empty */
    /* 当集合为空时,无法查找到这个值的位置 */
    if (intrev32ifbe(is->length) == 0) {
        if (pos) *pos = 0;
        return 0;
    } else {
        /* Check for the case where we know we cannot find the value,
         * but do know the insert position. */

        /* 考虑 集合不为空且查找不到值
         * 但是需确认新元素可插入位置的情况 */
        if (value > _intsetGet(is,max)) {
            if (pos) *pos = intrev32ifbe(is->length);
            return 0;
        } else if (value < _intsetGet(is,0)) {
            if (pos) *pos = 0;
            return 0;
        }
    }
    /* 通过二分法查找元素
     * 查找不到 value 时, 则最终 min 为 intset元素个数+1(即 is->length) */
    while(max >= min) {
        mid = ((unsigned int)min + (unsigned int)max) >> 1;
        cur = _intsetGet(is,mid);
        if (value > cur) {
            min = mid+1;
        } else if (value < cur) {
            max = mid-1;
        } else {
            break;
        }
    }
    /* 查找到元素 */
    if (value == cur) {
        /* 修改 pos 的值为对应元素下标,且返回 1 */
        if (pos) *pos = mid;
        return 1;
    } else {
        /* 否则,修改 pos 值为 max+1,且返回 0 */
        if (pos) *pos = min;
        return 0;
    }
}

/* Upgrades the intset to a larger encoding and inserts the given integer. */
/* 将整数集合升级为更大的编码 并插入给定的整数 */
static intset *intsetUpgradeAndAdd(intset *is, int64_t value) {
    /* 当前的编码 */
    uint8_t curenc = intrev32ifbe(is->encoding);
    /* 新插入 value 的编码 */
    uint8_t newenc = _intsetValueEncoding(value);
    /* 当前整数集合的元素个数 */
    int length = intrev32ifbe(is->length);
    int prepend = value < 0 ? 1 : 0;

    /* First set new encoding and resize */
    /* 设置新的编码 并且 重新分配空间大小 */
    is->encoding = intrev32ifbe(newenc);
    is = intsetResize(is,intrev32ifbe(is->length)+1);

    /* Upgrade back-to-front so we don't overwrite values.
     * Note that the "prepend" variable is used to make sure we have an empty
     * space at either the beginning or the end of the intset. */

    /* 从数组末尾到头部按照新的编码重新分配空间
     * prepend 变量用于确保在 intset 开头或结尾处有一个空位(预留给新元素)
     * 例如: value 为负数,原数组重新分配后,头部会剩余一个空位留给 value,反之 value 会分配至尾部 */
    while(length--)
        _intsetSet(is,length+prepend,_intsetGetEncoded(is,length,curenc));

    /* Set the value at the beginning or the end. */
    /* 如果值为负数,分配至数组头部
     * 如果值为非负数,分配至数组末尾 */
    if (prepend)
        _intsetSet(is,0,value);
    else
        _intsetSet(is,intrev32ifbe(is->length),value);
    /* 新元素分配后,维护整数集合元素数量+1 */
    is->length = intrev32ifbe(intrev32ifbe(is->length)+1);
    return is;
}

/* 移动特定范围内的 intset 元素 */
static void intsetMoveTail(intset *is, uint32_t from, uint32_t to) {
    void *src, *dst;
    uint32_t bytes = intrev32ifbe(is->length)-from;
    uint32_t encoding = intrev32ifbe(is->encoding);
    /* 找到符合当前集合编码的条件分支 */
    if (encoding == INTSET_ENC_INT64) {
        /* 源数组下标 指针 */
        src = (int64_t*)is->contents+from;
        /* 目的数组下标 指针 */
        dst = (int64_t*)is->contents+to;
        /* 需要复制字节的大小: 需移动元素数量*编码(对应的数值大小) */
        bytes *= sizeof(int64_t);
    } else if (encoding == INTSET_ENC_INT32) {
        src = (int32_t*)is->contents+from;
        dst = (int32_t*)is->contents+to;
        bytes *= sizeof(int32_t);
    } else {
        src = (int16_t*)is->contents+from;
        dst = (int16_t*)is->contents+to;
        bytes *= sizeof(int16_t);
    }
    memmove(dst,src,bytes);
}

/* Insert an integer in the intset */
/* 在整数集合中插入给定整数 */
intset *intsetAdd(intset *is, int64_t value, uint8_t *success) {
    /* 返回 符合插入整数 编码的大小 */
    uint8_t valenc = _intsetValueEncoding(value);
    /* 插入整数位于数组的位置 */
    uint32_t pos;
    if (success) *success = 1;

    /* Upgrade encoding if necessary. If we need to upgrade, we know that
     * this value should be either appended (if > 0) or prepended (if < 0),
     * because it lies outside the range of existing values. */

    /* 判断是否有必要升级整数集合的编码
     * (如果新插入整数元素的大小超过当前编码大小，则整数集合升级为高一级的编码大小) */
    if (valenc > intrev32ifbe(is->encoding)) {
        /* This always succeeds, so we don't need to curry *success. */
        /* 编码的升级总会成功(包括原数组的空间重分配),故不需要判断编码的升级是否成功 */
        return intsetUpgradeAndAdd(is,value);
    } else {
        /* Abort if the value is already present in the set.
         * This call will populate "pos" with the right position to insert
         * the value when it cannot be found. */

        /* 如果值已经存在集合中，则停止插入操作 */
        if (intsetSearch(is,value,&pos)) {
            if (success) *success = 0;
            return is;
        }
        /* 整数集合空间增大 */
        is = intsetResize(is,intrev32ifbe(is->length)+1);
        /* 如果插入元素位于原集合数组中间，则需要将 pos 后面对应的元素空间后移 */
        if (pos < intrev32ifbe(is->length)) intsetMoveTail(is,pos,pos+1);
    }
    /* 在 pos 下标对应位置上插入新元素 */
    _intsetSet(is,pos,value);
    /* 维护集合元素数量 */
    is->length = intrev32ifbe(intrev32ifbe(is->length)+1);
    return is;
}

/* Delete integer from intset */
/* 从整数集合中删除给定整数 */
intset *intsetRemove(intset *is, int64_t value, int *success) {
    uint8_t valenc = _intsetValueEncoding(value);
    uint32_t pos;
    if (success) *success = 0;

    if (valenc <= intrev32ifbe(is->encoding) && intsetSearch(is,value,&pos)) {
        uint32_t len = intrev32ifbe(is->length);

        /* We know we can delete */
        /* 进入条件分支，则说明可以删除该整数 */
        if (success) *success = 1;

        /* Overwrite value with tail and update length */
        /* 覆盖掉被删除的整数,并维护长度 */
        if (pos < (len-1)) intsetMoveTail(is,pos+1,pos);
        is = intsetResize(is,len-1);
        is->length = intrev32ifbe(len-1);
    }
    return is;
}

/* Determine whether a value belongs to this set */
/* 判断给定值是否位于整数集合中 */
uint8_t intsetFind(intset *is, int64_t value) {
    uint8_t valenc = _intsetValueEncoding(value);
    return valenc <= intrev32ifbe(is->encoding) && intsetSearch(is,value,NULL);
}

/* Return random member */
/* 返回整数集合中一个随机的整数 */
int64_t intsetRandom(intset *is) {
    uint32_t len = intrev32ifbe(is->length);
    assert(len); /* avoid division by zero on corrupt intset payload. */
    return _intsetGet(is,rand()%len);
}

/* Get the value at the given position. When this position is
 * out of range the function returns 0, when in range it returns 1. */

/* 从整数集合中,取出底层数组(contents)在给定索引(pos)上的元素(*value)
 * 当给定索引超过整数集合的元素个数，则获取是被返回 0,否则获取成功且返回 1. */
uint8_t intsetGet(intset *is, uint32_t pos, int64_t *value) {
    if (pos < intrev32ifbe(is->length)) {
        *value = _intsetGet(is,pos);
        return 1;
    }
    return 0;
}

/* Return intset length */
/* 返回整数集合包含的元素个数 */
uint32_t intsetLen(const intset *is) {
    return intrev32ifbe(is->length);
}

/* Return intset blob size in bytes. */
/* 返回整数集合占用的内存字节数 */
size_t intsetBlobLen(intset *is) {
    /* 内存字节: 整数集合结构体大小+(元素个数*当前编码) */
    return sizeof(intset)+(size_t)intrev32ifbe(is->length)*intrev32ifbe(is->encoding);
}

/* Validate the integrity of the data structure.
 * when `deep` is 0, only the integrity of the header is validated.
 * when `deep` is 1, we make sure there are no duplicate or out of order records. */

/* 验证数据结构的完整性。 (用于RDB数据恢复)
 * 当 deep 为 0, 仅验证标头(即 intset 结构体)的完整性
 * 当 deep 为 1, 额外验证没有重复或无序的整数元素 */
int intsetValidateIntegrity(const unsigned char *p, size_t size, int deep) {
    intset *is = (intset *)p;
    /* check that we can actually read the header. */
    if (size < sizeof(*is))
        return 0;

    uint32_t encoding = intrev32ifbe(is->encoding);

    size_t record_size;
    if (encoding == INTSET_ENC_INT64) {
        record_size = INTSET_ENC_INT64;
    } else if (encoding == INTSET_ENC_INT32) {
        record_size = INTSET_ENC_INT32;
    } else if (encoding == INTSET_ENC_INT16){
        record_size = INTSET_ENC_INT16;
    } else {
        return 0;
    }

    /* check that the size matches (all records are inside the buffer). */
    /* 检测内存大小是否匹配 (所有的记录都在缓冲区中) */
    uint32_t count = intrev32ifbe(is->length);
    if (sizeof(*is) + count*record_size != size)
        return 0;

    /* check that the set is not empty. */
    /* 元素长度为空,则在数据恢复时直接释放这部分的内存 */
    if (count==0)
        return 0;
    /* deep 为 0,则验证到这一步 */
    if (!deep)
        return 1;

    /* check that there are no dup or out of order records. */
    /* 检查没有重复或者无序的整数 */
    int64_t prev = _intsetGet(is,0);
    /* 从前往后依次两两比较 */
    for (uint32_t i=1; i<count; i++) {
        int64_t cur = _intsetGet(is,i);
        if (cur <= prev)
            return 0;
        prev = cur;
    }

    return 1;
}

#ifdef REDIS_TEST
#include <sys/time.h>
#include <time.h>

#if 0
static void intsetRepr(intset *is) {
    for (uint32_t i = 0; i < intrev32ifbe(is->length); i++) {
        printf("%lld\n", (uint64_t)_intsetGet(is,i));
    }
    printf("\n");
}

static void error(char *err) {
    printf("%s\n", err);
    exit(1);
}
#endif

static void ok(void) {
    printf("OK\n");
}

static long long usec(void) {
    struct timeval tv;
    gettimeofday(&tv,NULL);
    return (((long long)tv.tv_sec)*1000000)+tv.tv_usec;
}

static intset *createSet(int bits, int size) {
    uint64_t mask = (1<<bits)-1;
    uint64_t value;
    intset *is = intsetNew();

    for (int i = 0; i < size; i++) {
        if (bits > 32) {
            value = (rand()*rand()) & mask;
        } else {
            value = rand() & mask;
        }
        is = intsetAdd(is,value,NULL);
    }
    return is;
}

static void checkConsistency(intset *is) {
    for (uint32_t i = 0; i < (intrev32ifbe(is->length)-1); i++) {
        uint32_t encoding = intrev32ifbe(is->encoding);

        if (encoding == INTSET_ENC_INT16) {
            int16_t *i16 = (int16_t*)is->contents;
            assert(i16[i] < i16[i+1]);
        } else if (encoding == INTSET_ENC_INT32) {
            int32_t *i32 = (int32_t*)is->contents;
            assert(i32[i] < i32[i+1]);
        } else {
            int64_t *i64 = (int64_t*)is->contents;
            assert(i64[i] < i64[i+1]);
        }
    }
}

#define UNUSED(x) (void)(x)
int intsetTest(int argc, char **argv, int flags) {
    uint8_t success;
    int i;
    intset *is;
    srand(time(NULL));

    UNUSED(argc);
    UNUSED(argv);
    UNUSED(flags);

    printf("Value encodings: "); {
        assert(_intsetValueEncoding(-32768) == INTSET_ENC_INT16);
        assert(_intsetValueEncoding(+32767) == INTSET_ENC_INT16);
        assert(_intsetValueEncoding(-32769) == INTSET_ENC_INT32);
        assert(_intsetValueEncoding(+32768) == INTSET_ENC_INT32);
        assert(_intsetValueEncoding(-2147483648) == INTSET_ENC_INT32);
        assert(_intsetValueEncoding(+2147483647) == INTSET_ENC_INT32);
        assert(_intsetValueEncoding(-2147483649) == INTSET_ENC_INT64);
        assert(_intsetValueEncoding(+2147483648) == INTSET_ENC_INT64);
        assert(_intsetValueEncoding(-9223372036854775808ull) ==
                    INTSET_ENC_INT64);
        assert(_intsetValueEncoding(+9223372036854775807ull) ==
                    INTSET_ENC_INT64);
        ok();
    }

    printf("Basic adding: "); {
        is = intsetNew();
        is = intsetAdd(is,5,&success); assert(success);
        is = intsetAdd(is,6,&success); assert(success);
        is = intsetAdd(is,4,&success); assert(success);
        is = intsetAdd(is,4,&success); assert(!success);
        ok();
        zfree(is);
    }

    printf("Large number of random adds: "); {
        uint32_t inserts = 0;
        is = intsetNew();
        for (i = 0; i < 1024; i++) {
            is = intsetAdd(is,rand()%0x800,&success);
            if (success) inserts++;
        }
        assert(intrev32ifbe(is->length) == inserts);
        checkConsistency(is);
        ok();
        zfree(is);
    }

    printf("Upgrade from int16 to int32: "); {
        is = intsetNew();
        is = intsetAdd(is,32,NULL);
        assert(intrev32ifbe(is->encoding) == INTSET_ENC_INT16);
        is = intsetAdd(is,65535,NULL);
        assert(intrev32ifbe(is->encoding) == INTSET_ENC_INT32);
        assert(intsetFind(is,32));
        assert(intsetFind(is,65535));
        checkConsistency(is);
        zfree(is);

        is = intsetNew();
        is = intsetAdd(is,32,NULL);
        assert(intrev32ifbe(is->encoding) == INTSET_ENC_INT16);
        is = intsetAdd(is,-65535,NULL);
        assert(intrev32ifbe(is->encoding) == INTSET_ENC_INT32);
        assert(intsetFind(is,32));
        assert(intsetFind(is,-65535));
        checkConsistency(is);
        ok();
        zfree(is);
    }

    printf("Upgrade from int16 to int64: "); {
        is = intsetNew();
        is = intsetAdd(is,32,NULL);
        assert(intrev32ifbe(is->encoding) == INTSET_ENC_INT16);
        is = intsetAdd(is,4294967295,NULL);
        assert(intrev32ifbe(is->encoding) == INTSET_ENC_INT64);
        assert(intsetFind(is,32));
        assert(intsetFind(is,4294967295));
        checkConsistency(is);
        zfree(is);

        is = intsetNew();
        is = intsetAdd(is,32,NULL);
        assert(intrev32ifbe(is->encoding) == INTSET_ENC_INT16);
        is = intsetAdd(is,-4294967295,NULL);
        assert(intrev32ifbe(is->encoding) == INTSET_ENC_INT64);
        assert(intsetFind(is,32));
        assert(intsetFind(is,-4294967295));
        checkConsistency(is);
        ok();
        zfree(is);
    }

    printf("Upgrade from int32 to int64: "); {
        is = intsetNew();
        is = intsetAdd(is,65535,NULL);
        assert(intrev32ifbe(is->encoding) == INTSET_ENC_INT32);
        is = intsetAdd(is,4294967295,NULL);
        assert(intrev32ifbe(is->encoding) == INTSET_ENC_INT64);
        assert(intsetFind(is,65535));
        assert(intsetFind(is,4294967295));
        checkConsistency(is);
        zfree(is);

        is = intsetNew();
        is = intsetAdd(is,65535,NULL);
        assert(intrev32ifbe(is->encoding) == INTSET_ENC_INT32);
        is = intsetAdd(is,-4294967295,NULL);
        assert(intrev32ifbe(is->encoding) == INTSET_ENC_INT64);
        assert(intsetFind(is,65535));
        assert(intsetFind(is,-4294967295));
        checkConsistency(is);
        ok();
        zfree(is);
    }

    printf("Stress lookups: "); {
        long num = 100000, size = 10000;
        int i, bits = 20;
        long long start;
        is = createSet(bits,size);
        checkConsistency(is);

        start = usec();
        for (i = 0; i < num; i++) intsetSearch(is,rand() % ((1<<bits)-1),NULL);
        printf("%ld lookups, %ld element set, %lldusec\n",
               num,size,usec()-start);
        zfree(is);
    }

    printf("Stress add+delete: "); {
        int i, v1, v2;
        is = intsetNew();
        for (i = 0; i < 0xffff; i++) {
            v1 = rand() % 0xfff;
            is = intsetAdd(is,v1,NULL);
            assert(intsetFind(is,v1));

            v2 = rand() % 0xfff;
            is = intsetRemove(is,v2,NULL);
            assert(!intsetFind(is,v2));
        }
        checkConsistency(is);
        ok();
        zfree(is);
    }

    return 0;
}
#endif
