/* The ziplist is a specially encoded dually linked list that is designed
 * to be very memory efficient. It stores both strings and integer values,
 * where integers are encoded as actual integers instead of a series of
 * characters. It allows push and pop operations on either side of the list
 * in O(1) time. However, because every operation requires a reallocation of
 * the memory used by the ziplist, the actual complexity is related to the
 * amount of memory used by the ziplist.
 *
 * ziplist 压缩列表是一个特殊编码的双端链表（内存上连续），为了尽可能节省内存而设计的。
 * ziplist 可以存储字符串或者整数值，其中整数被编码保存为实际的整数，而不是字符数组。
 * ziplist 支持 O(1) 的时间复杂度在列表的两端进行 push 和 pop 操作。
 * 然而因为这些操作都需要对整个 ziplist 进行内存重分配（因为是一块连续的内存），
 * 所以操作的实际复杂度和 ziplist 占用的内存大小有关。
 *
 * 注意：在 7.0 版本里，ziplist 已经全面被 listpack 替换了（主要是因为连锁更新较影响性能）。
 *
 * ----------------------------------------------------------------------------
 *
 * ZIPLIST OVERALL LAYOUT
 * ======================
 *
 * The general layout of the ziplist is as follows:
 * ziplist 压缩列表的布局如下：
 *
 * <zlbytes> <zltail> <zllen> <entry> <entry> ... <entry> <zlend>
 *
 * NOTE: all fields are stored in little endian, if not specified otherwise.
 *
 * <uint32_t zlbytes> is an unsigned integer to hold the number of bytes that
 * the ziplist occupies, including the four bytes of the zlbytes field itself.
 * This value needs to be stored to be able to resize the entire structure
 * without the need to traverse it first.
 *
 * zlbytes: 是一个 32 位无符号整数（4 bytes），记录整个 ziplist 占用的内存字节数，包含 4 个字节的 zlbytes 本身。
 * 记录这个值我们可以 O(1) 知道 ziplist 的字节长度，然后进行 resize 大小调整，不然的话需要完整遍历整个 ziplist。
 *
 * <uint32_t zltail> is the offset to the last entry in the list. This allows
 * a pop operation on the far side of the list without the need for full
 * traversal.
 *
 * zltail: 是一个 32 位无符号整数（4 bytes），记录 ziplist 到尾节点的位置偏移量。
 * 通过这个偏移量我们可以直接定位到表尾节点，例如进行表尾的 pop 操作，不然得完整遍历 ziplist。
 *
 * <uint16_t zllen> is the number of entries. When there are more than
 * 2^16-2 entries, this value is set to 2^16-1 and we need to traverse the
 * entire list to know how many items it holds.
 *
 * zllen: 是一个 16 位无符号整数（2 bytes），记录 ziplist 里的节点数量。
 * 由于它设计只用 2 个字节进行存储，2 字节实际最大可以表示为 2^16 - 1 即: 65535。
 * 当数字小于它时，则 zllen 的值就是实际的节点数量（O(1) 时间复杂度）, 也就是注释里的 2^16 - 2 的含义。
 * 否则当 zllen 值为 65535 时即 2^16-1，用它作为一个标识，表示需要完整遍历整个压缩列表 O(N) 时间复杂度才能计算出真实的节点数量。
 * 所以 ziplist 不适合存储过多元素（遍历计算节点数量开销很大，且我们假设它只用于元素数量较少的场景）。
 *
 * <uint8_t zlend> is a special entry representing the end of the ziplist.
 * Is encoded as a single byte equal to 255. No other normal entry starts
 * with a byte set to the value of 255.
 *
 * zlend: 是一个 8 位无符号整数（1 byte），是一个特殊的标志位来标记压缩列表的结尾，0xFF(十进制表示为: 255)。
 * 其它正常节点不会有以这个字节开头的，在遍历 ziplist 的时候通过这个标记来判断是否遍历结束。
 *
 * ZIPLIST ENTRIES
 * ===============
 *
 * Every entry in the ziplist is prefixed by metadata that contains two pieces
 * of information. First, the length of the previous entry is stored to be
 * able to traverse the list from back to front. Second, the entry encoding is
 * provided. It represents the entry type, integer or string, and in the case
 * of strings it also represents the length of the string payload.
 * So a complete entry is stored like this:
 *
 * <prevlen> <encoding> <entry-data>
 *
 * 每一个 ziplist entry 压缩列表节点在实际的节点数据之前都会包含两部分元数据，也叫 entry header。
 * 1. prevlen: 前置节点的字节长度，以支持我们从后往前遍历（通过指针偏移量定位前一个节点）
 * 2. encoding: 当前节点 entry-data 节点数据部分的类型和编码，例如存储的是整数还是字符串，类型下还会细分多种编码。
 *
 *
 * Sometimes the encoding represents the entry itself, like for small integers
 * as we'll see later. In such a case the <entry-data> part is missing, and we
 * could have just:
 *
 * <prevlen> <encoding>
 *
 * 有时候节点可以不用有 entry-data，可以在 encoding 部分直接存储节点数据。
 * 例如一些小整数，可以直接在 encoding 部分用几位来存储表示，对每一位都物尽其用。
 *
 * The length of the previous entry, <prevlen>, is encoded in the following way:
 * If this length is smaller than 254 bytes, it will only consume a single
 * byte representing the length as an unsigned 8 bit integer. When the length
 * is greater than or equal to 254, it will consume 5 bytes. The first byte is
 * set to 254 (FE) to indicate a larger value is following. The remaining 4
 * bytes take the length of the previous entry as value.
 *
 * 当前节点的前节点字节长度，prevlen 的编码方式如下（同时我们将存储 prevlen 所需的字节数为 prevlensize，即下面的 1 或者 5 字节）：
 * 1. 如果前节点的字节长度 < 254 字节，那么 prevlen 使用 1 个字节来保存它，一个 8 位无符号的整数
 * 2. 如果前节点的字节长度 >= 254 字节，那么 prevlen 使用 5 个字节来保存它：
 *    a. 第 1 个字节会被置为 0xFE 十进制的 254 (后面的 ZIP_BIG_PREVLEN)，用来标识我们是用 5 个字节编码存储 prevlen
 *    b. prevlen 实际的值被保存在后 4 个字节里
 *
 * So practically an entry is encoded in the following way:
 *
 * <prevlen from 0 to 253> <encoding> <entry>
 *
 * Or alternatively if the previous entry length is greater than 253 bytes
 * the following encoding is used:
 *
 * 0xFE <4 bytes unsigned little endian prevlen> <encoding> <entry>
 *
 * 编码结构示意图：
 * 1. 如果前节点字节长度 < 254 字节，那么当前节点的编码布局如下所示：
 *    <prevlen from 0 to 253>               <encoding>               <entry>
 *   前节点的长度，值介于 [0, 253]  ｜ 当前节点的实际数据类型以及长度 ｜ 当前节点的实际数据
 
 * 2. 如果前节点字节长度 >= 254 字节，那么当前节点的编码布局如下所示：
 *             0xFE               <4 bytes unsigned little endian prevlen>        <encoding>                   <entry>
 *  zlend 标识，值为 254（1 字节）｜  当前节点的实际长度（4 字节）             ｜ 当前节点的实际数据类型以及长度 ｜ 当前节点的实际数据
 *
 * The encoding field of the entry depends on the content of the
 * entry. When the entry is a string, the first 2 bits of the encoding first
 * byte will hold the type of encoding used to store the length of the string,
 * followed by the actual length of the string. When the entry is an integer
 * the first 2 bits are both set to 1. The following 2 bits are used to specify
 * what kind of integer will be stored after this header. An overview of the
 * different types and encodings is as follows. The first byte is always enough
 * to determine the kind of entry.
 *
 * |00pppppp| - 1 byte
 *      String value with length less than or equal to 63 bytes (6 bits).
 *      "pppppp" represents the unsigned 6 bit length.
 * |01pppppp|qqqqqqqq| - 2 bytes
 *      String value with length less than or equal to 16383 bytes (14 bits).
 *      IMPORTANT: The 14 bit number is stored in big endian.
 * |10000000|qqqqqqqq|rrrrrrrr|ssssssss|tttttttt| - 5 bytes
 *      String value with length greater than or equal to 16384 bytes.
 *      Only the 4 bytes following the first byte represents the length
 *      up to 2^32-1. The 6 lower bits of the first byte are not used and
 *      are set to zero.
 *      IMPORTANT: The 32 bit number is stored in big endian.
 * |11000000| - 3 bytes
 *      Integer encoded as int16_t (2 bytes).
 * |11010000| - 5 bytes
 *      Integer encoded as int32_t (4 bytes).
 * |11100000| - 9 bytes
 *      Integer encoded as int64_t (8 bytes).
 * |11110000| - 4 bytes
 *      Integer encoded as 24 bit signed (3 bytes).
 * |11111110| - 2 bytes
 *      Integer encoded as 8 bit signed (1 byte).
 * |1111xxxx| - (with xxxx between 0001 and 1101) immediate 4 bit integer.
 *      Unsigned integer from 0 to 12. The encoded value is actually from
 *      1 to 13 because 0000 and 1111 can not be used, so 1 should be
 *      subtracted from the encoded 4 bit value to obtain the right value.
 * |11111111| - End of ziplist special entry.
 *
 * Like for the ziplist header, all the integers are represented in little
 * endian byte order, even when this code is compiled in big endian systems.
 *
 * EXAMPLES OF ACTUAL ZIPLISTS
 * ===========================
 *
 * The following is a ziplist containing the two elements representing
 * the strings "2" and "5". It is composed of 15 bytes, that we visually
 * split into sections:
 *
 *  [0f 00 00 00] [0c 00 00 00] [02 00] [00 f3] [02 f6] [ff]
 *        |             |          |       |       |     |
 *     zlbytes        zltail    entries   "2"     "5"   end
 *
 * The first 4 bytes represent the number 15, that is the number of bytes
 * the whole ziplist is composed of. The second 4 bytes are the offset
 * at which the last ziplist entry is found, that is 12, in fact the
 * last entry, that is "5", is at offset 12 inside the ziplist.
 * The next 16 bit integer represents the number of elements inside the
 * ziplist, its value is 2 since there are just two elements inside.
 * Finally "00 f3" is the first entry representing the number 2. It is
 * composed of the previous entry length, which is zero because this is
 * our first entry, and the byte F3 which corresponds to the encoding
 * |1111xxxx| with xxxx between 0001 and 1101. We need to remove the "F"
 * higher order bits 1111, and subtract 1 from the "3", so the entry value
 * is "2". The next entry has a prevlen of 02, since the first entry is
 * composed of exactly two bytes. The entry itself, F6, is encoded exactly
 * like the first entry, and 6-1 = 5, so the value of the entry is 5.
 * Finally the special entry FF signals the end of the ziplist.
 *
 * Adding another element to the above string with the value "Hello World"
 * allows us to show how the ziplist encodes small strings. We'll just show
 * the hex dump of the entry itself. Imagine the bytes as following the
 * entry that stores "5" in the ziplist above:
 *
 * [02] [0b] [48 65 6c 6c 6f 20 57 6f 72 6c 64]
 *
 * The first byte, 02, is the length of the previous entry. The next
 * byte represents the encoding in the pattern |00pppppp| that means
 * that the entry is a string of length <pppppp>, so 0B means that
 * an 11 bytes string follows. From the third byte (48) to the last (64)
 * there are just the ASCII characters for "Hello World".
 *
 * ----------------------------------------------------------------------------
 *
 * Copyright (c) 2009-2012, Pieter Noordhuis <pcnoordhuis at gmail dot com>
 * Copyright (c) 2009-2017, Salvatore Sanfilippo <antirez at gmail dot com>
 * Copyright (c) 2020, Redis Labs, Inc
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
#include <stdint.h>
#include <limits.h>
#include "zmalloc.h"
#include "util.h"
#include "ziplist.h"
#include "config.h"
#include "endianconv.h"
#include "redisassert.h"

/* ZIP_END 标识 ziplist 的末尾，放在最后一个字节里 */
#define ZIP_END 255         /* Special "end of ziplist" entry. */
/* PREVLEN 一个字节最大能表示到 254 - 1 即 253，用 254 标识 prevlen 是用 5 个字节编码存储的 */
#define ZIP_BIG_PREVLEN 254 /* ZIP_BIG_PREVLEN - 1 is the max number of bytes of
                               the previous entry, for the "prevlen" field prefixing
                               each entry, to be represented with just a single byte.
                               Otherwise it is represented as FE AA BB CC DD, where
                               AA BB CC DD are a 4 bytes unsigned integer
                               representing the previous entry len. */

/* Different encoding/length possibilities */
#define ZIP_STR_MASK 0xc0
#define ZIP_INT_MASK 0x30
#define ZIP_STR_06B (0 << 6)
#define ZIP_STR_14B (1 << 6)
#define ZIP_STR_32B (2 << 6)
#define ZIP_INT_16B (0xc0 | 0<<4)
#define ZIP_INT_32B (0xc0 | 1<<4)
#define ZIP_INT_64B (0xc0 | 2<<4)
#define ZIP_INT_24B (0xc0 | 3<<4)
#define ZIP_INT_8B 0xfe

/* 4 bit integer immediate encoding |1111xxxx| with xxxx between
 * 0001 and 1101. */
#define ZIP_INT_IMM_MASK 0x0f   /* Mask to extract the 4 bits value. To add
                                   one is needed to reconstruct the value. */
#define ZIP_INT_IMM_MIN 0xf1    /* 11110001 */
#define ZIP_INT_IMM_MAX 0xfd    /* 11111101 */

#define INT24_MAX 0x7fffff
#define INT24_MIN (-INT24_MAX - 1)

/* Macro to determine if the entry is a string. String entries never start
 * with "11" as most significant bits of the first byte. */
#define ZIP_IS_STR(enc) (((enc) & ZIP_STR_MASK) < ZIP_STR_MASK)

/* Utility macros.*/

/* Return total bytes a ziplist is composed of. */
/* 获取 ziplist 的占用字节数，即 zlbytes 的值 */
#define ZIPLIST_BYTES(zl)       (*((uint32_t*)(zl)))

/* Return the offset of the last item inside the ziplist. */
/* 根据 zl 往后偏移 zlbytes 4 个字节，然后获取到尾节点的偏移量，即 zltail 的值 */
#define ZIPLIST_TAIL_OFFSET(zl) (*((uint32_t*)((zl)+sizeof(uint32_t))))

/* Return the length of a ziplist, or UINT16_MAX if the length cannot be
 * determined without scanning the whole ziplist. */
/* 根据 zl 往后偏移 zlybtes 4 个字节和 zltail 4 个字节，然后获取节点数量，即 zllen 的值 */
#define ZIPLIST_LENGTH(zl)      (*((uint16_t*)((zl)+sizeof(uint32_t)*2)))

/* The size of a ziplist header: two 32 bit integers for the total
 * bytes count and last item offset. One 16 bit integer for the number
 * of items field. */
/* 获取 ziplist header 占用的字节数
 * 4 个字节的 zlbytes + 4 个字节的 zltail + 2 个字节的 zllen */
#define ZIPLIST_HEADER_SIZE     (sizeof(uint32_t)*2+sizeof(uint16_t))

/* Size of the "end of ziplist" entry. Just one byte. */
/* 获取 ziplist 末端占用的字节数，其实就是一个字节的 ZIP_END */
#define ZIPLIST_END_SIZE        (sizeof(uint8_t))

/* Return the pointer to the first entry of a ziplist. */
/* 获取 ziplist 的第一个节点指针，即 zl 偏移 header 占用的字节数，此时就指向第一个节点的起始位置 */
#define ZIPLIST_ENTRY_HEAD(zl)  ((zl)+ZIPLIST_HEADER_SIZE)

/* Return the pointer to the last entry of a ziplist, using the
 * last entry offset inside the ziplist header. */
/* 获取 ziplist 的最后一个节点指针，即 zl 偏移 zltail 偏移量，此时就指向最后一个节点的起始位置 */
#define ZIPLIST_ENTRY_TAIL(zl)  ((zl)+intrev32ifbe(ZIPLIST_TAIL_OFFSET(zl)))

/* Return the pointer to the last byte of a ziplist, which is, the
 * end of ziplist FF entry. */
/* 获取指向 ziplist 末端 ZIP_END 的指针，即 zl 偏移 zlbytes - 1，此时就指向 ZIP_END */
#define ZIPLIST_ENTRY_END(zl)   ((zl)+intrev32ifbe(ZIPLIST_BYTES(zl))-1)

/* copy from 黄健宏老师的 redis3.0 代码注释

空 ziplist 示例图：
area        |<---- ziplist header ---->|<-- end -->|
size          4 bytes   4 bytes 2 bytes  1 byte
            +---------+--------+-------+-----------+
component   | zlbytes | zltail | zllen | zlend     |
            |         |        |       |           |
value       |  1011   |  1010  |   0   | 1111 1111 |
            +---------+--------+-------+-----------+
                                       ^
                                       |
                               ZIPLIST_ENTRY_HEAD
                                       &
address                        ZIPLIST_ENTRY_TAIL
                                       &
                               ZIPLIST_ENTRY_END

非空 ziplist 示例图：
area        |<---- ziplist header ---->|<----------- entries ------------->|<-end->|
size          4 bytes  4 bytes  2 bytes    ?        ?        ?        ?     1 byte
            +---------+--------+-------+--------+--------+--------+--------+-------+
component   | zlbytes | zltail | zllen | entry1 | entry2 |  ...   | entryN | zlend |
            +---------+--------+-------+--------+--------+--------+--------+-------+
                                       ^                          ^        ^
address                                |                          |        |
                                ZIPLIST_ENTRY_HEAD                |   ZIPLIST_ENTRY_END
                                                                  |
                                                        ZIPLIST_ENTRY_TAIL
*/

/* Increment the number of items field in the ziplist header. Note that this
 * macro should never overflow the unsigned 16 bit integer, since entries are
 * always pushed one at a time. When UINT16_MAX is reached we want the count
 * to stay there to signal that a full scan is needed to get the number of
 * items inside the ziplist. */
/* 取决于 incr 增加或者减少 ziplist 的节点数
 * 增加节点场景：incr 一直为 1 因为我们每次只会增加一个元素，此时会检查 UINT16_MAX，
 * 如果节点数大于它，则不继续自增，因为使用 UINT16_MAX 来标识我们需要完整遍历 ziplist 才能获取到节点数
 *
 * 减少节点场景：incr 会是一个负数，可以看到如果原本 zllen 大于 UINT16_MAX 的话，
 * 就算减少节点到小于 UINT16_MAX 我们也不会在这里维护 zllen 的值，而是在 ziplistLen 计算 zllen 的时候维护
 * */
#define ZIPLIST_INCR_LENGTH(zl,incr) { \
    if (intrev16ifbe(ZIPLIST_LENGTH(zl)) < UINT16_MAX) \
        ZIPLIST_LENGTH(zl) = intrev16ifbe(intrev16ifbe(ZIPLIST_LENGTH(zl))+incr); \
}

/* Don't let ziplists grow over 1GB in any case, don't wanna risk overflow in
 * zlbytes */
/* 检查是否可以安全增加 ziplist 大小，确保不让 ziplist 大小超过 1GB，防止 zlbytes 溢出 */
#define ZIPLIST_MAX_SAFETY_SIZE (1<<30)
int ziplistSafeToAdd(unsigned char* zl, size_t add) {
    size_t len = zl? ziplistBlobLen(zl): 0;
    if (len + add > ZIPLIST_MAX_SAFETY_SIZE)
        return 0;
    return 1;
}


/* We use this function to receive information about a ziplist entry.
 * Note that this is not how the data is actually encoded, is just what we
 * get filled by a function in order to operate more easily. */
/* 这是一个很关键的结构体，将 ziplist 节点信息填充成一个 zlentry 结构体，方便后面进行函数操作
 * 需要注意这并不是一个 ziplist 节点在内存中实际的编码布局，只是为了方便我们使用
 * */
typedef struct zlentry {
    /* 存储下面 prevrawlen 所需要的字节数 */
    unsigned int prevrawlensize; /* Bytes used to encode the previous entry len*/
    /* 存储前一个节点的字节长度 */
    unsigned int prevrawlen;     /* Previous entry len. */
    /* 存储下面 len 所需要的字节数 */
    unsigned int lensize;        /* Bytes used to encode this entry type/len.
                                    For example strings have a 1, 2 or 5 bytes
                                    header. Integers always use a single byte.*/
    /* 存储当前节点的字节长度 */
    unsigned int len;            /* Bytes used to represent the actual entry.
                                    For strings this is just the string length
                                    while for integers it is 1, 2, 3, 4, 8 or
                                    0 (for 4 bit immediate) depending on the
                                    number range. */
    /* prevrawlensize + lensize 当前节点的头部字节，
     * 其实是 prevlen + encoding 两项占用的字节数 */
    unsigned int headersize;     /* prevrawlensize + lensize. */
    /* 存储当前节点的数据编码格式 */
    unsigned char encoding;      /* Set to ZIP_STR_* or ZIP_INT_* depending on
                                    the entry encoding. However for 4 bits
                                    immediate integers this can assume a range
                                    of values and must be range-checked. */
    /* 指向当前节点开头第一个字节的指针 */
    unsigned char *p;            /* Pointer to the very start of the entry, that
                                    is, this points to prev-entry-len field. */
} zlentry;

#define ZIPLIST_ENTRY_ZERO(zle) { \
    (zle)->prevrawlensize = (zle)->prevrawlen = 0; \
    (zle)->lensize = (zle)->len = (zle)->headersize = 0; \
    (zle)->encoding = 0; \
    (zle)->p = NULL; \
}

/* Extract the encoding from the byte pointed by 'ptr' and set it into
 * 'encoding' field of the zlentry structure. */
/* 从 ptr 中取出节点值的编码类型，保存在 encoding 变量中
 * 时间复杂度 O(1) */
#define ZIP_ENTRY_ENCODING(ptr, encoding) do {  \
    (encoding) = ((ptr)[0]); \
    if ((encoding) < ZIP_STR_MASK) (encoding) &= ZIP_STR_MASK; \
} while(0)

#define ZIP_ENCODING_SIZE_INVALID 0xff
/* Return the number of bytes required to encode the entry type + length.
 * On error, return ZIP_ENCODING_SIZE_INVALID */
static inline unsigned int zipEncodingLenSize(unsigned char encoding) {
    if (encoding == ZIP_INT_16B || encoding == ZIP_INT_32B ||
        encoding == ZIP_INT_24B || encoding == ZIP_INT_64B ||
        encoding == ZIP_INT_8B)
        return 1;
    if (encoding >= ZIP_INT_IMM_MIN && encoding <= ZIP_INT_IMM_MAX)
        return 1;
    if (encoding == ZIP_STR_06B)
        return 1;
    if (encoding == ZIP_STR_14B)
        return 2;
    if (encoding == ZIP_STR_32B)
        return 5;
    return ZIP_ENCODING_SIZE_INVALID;
}

#define ZIP_ASSERT_ENCODING(encoding) do {                                     \
    assert(zipEncodingLenSize(encoding) != ZIP_ENCODING_SIZE_INVALID);         \
} while (0)

/* Return bytes needed to store integer encoded by 'encoding' */
static inline unsigned int zipIntSize(unsigned char encoding) {
    switch(encoding) {
    case ZIP_INT_8B:  return 1;
    case ZIP_INT_16B: return 2;
    case ZIP_INT_24B: return 3;
    case ZIP_INT_32B: return 4;
    case ZIP_INT_64B: return 8;
    }
    if (encoding >= ZIP_INT_IMM_MIN && encoding <= ZIP_INT_IMM_MAX)
        return 0; /* 4 bit immediate */
    /* bad encoding, covered by a previous call to ZIP_ASSERT_ENCODING */
    redis_unreachable();
    return 0;
}

/* Write the encoding header of the entry in 'p'. If p is NULL it just returns
 * the amount of bytes required to encode such a length. Arguments:
 *
 * 'encoding' is the encoding we are using for the entry. It could be
 * ZIP_INT_* or ZIP_STR_* or between ZIP_INT_IMM_MIN and ZIP_INT_IMM_MAX
 * for single-byte small immediate integers.
 *
 * 'rawlen' is only used for ZIP_STR_* encodings and is the length of the
 * string that this entry represents.
 *
 * The function returns the number of bytes used by the encoding/length
 * header stored in 'p'. */
unsigned int zipStoreEntryEncoding(unsigned char *p, unsigned char encoding, unsigned int rawlen) {
    unsigned char len = 1, buf[5];

    if (ZIP_IS_STR(encoding)) {
        /* Although encoding is given it may not be set for strings,
         * so we determine it here using the raw length. */
        if (rawlen <= 0x3f) {
            if (!p) return len;
            buf[0] = ZIP_STR_06B | rawlen;
        } else if (rawlen <= 0x3fff) {
            len += 1;
            if (!p) return len;
            buf[0] = ZIP_STR_14B | ((rawlen >> 8) & 0x3f);
            buf[1] = rawlen & 0xff;
        } else {
            len += 4;
            if (!p) return len;
            buf[0] = ZIP_STR_32B;
            buf[1] = (rawlen >> 24) & 0xff;
            buf[2] = (rawlen >> 16) & 0xff;
            buf[3] = (rawlen >> 8) & 0xff;
            buf[4] = rawlen & 0xff;
        }
    } else {
        /* Implies integer encoding, so length is always 1. */
        if (!p) return len;
        buf[0] = encoding;
    }

    /* Store this length at p. */
    memcpy(p,buf,len);
    return len;
}

/* Decode the entry encoding type and data length (string length for strings,
 * number of bytes used for the integer for integer entries) encoded in 'ptr'.
 * The 'encoding' variable is input, extracted by the caller, the 'lensize'
 * variable will hold the number of bytes required to encode the entry
 * length, and the 'len' variable will hold the entry length.
 * On invalid encoding error, lensize is set to 0. */
#define ZIP_DECODE_LENGTH(ptr, encoding, lensize, len) do {                    \
    if ((encoding) < ZIP_STR_MASK) {                                           \
        if ((encoding) == ZIP_STR_06B) {                                       \
            (lensize) = 1;                                                     \
            (len) = (ptr)[0] & 0x3f;                                           \
        } else if ((encoding) == ZIP_STR_14B) {                                \
            (lensize) = 2;                                                     \
            (len) = (((ptr)[0] & 0x3f) << 8) | (ptr)[1];                       \
        } else if ((encoding) == ZIP_STR_32B) {                                \
            (lensize) = 5;                                                     \
            (len) = ((uint32_t)(ptr)[1] << 24) |                               \
                    ((uint32_t)(ptr)[2] << 16) |                               \
                    ((uint32_t)(ptr)[3] <<  8) |                               \
                    ((uint32_t)(ptr)[4]);                                      \
        } else {                                                               \
            (lensize) = 0; /* bad encoding, should be covered by a previous */ \
            (len) = 0;     /* ZIP_ASSERT_ENCODING / zipEncodingLenSize, or  */ \
                           /* match the lensize after this macro with 0.    */ \
        }                                                                      \
    } else {                                                                   \
        (lensize) = 1;                                                         \
        if ((encoding) == ZIP_INT_8B)  (len) = 1;                              \
        else if ((encoding) == ZIP_INT_16B) (len) = 2;                         \
        else if ((encoding) == ZIP_INT_24B) (len) = 3;                         \
        else if ((encoding) == ZIP_INT_32B) (len) = 4;                         \
        else if ((encoding) == ZIP_INT_64B) (len) = 8;                         \
        else if (encoding >= ZIP_INT_IMM_MIN && encoding <= ZIP_INT_IMM_MAX)   \
            (len) = 0; /* 4 bit immediate */                                   \
        else                                                                   \
            (lensize) = (len) = 0; /* bad encoding */                          \
    }                                                                          \
} while(0)

/* Encode the length of the previous entry and write it to "p". This only
 * uses the larger encoding (required in __ziplistCascadeUpdate). */
/* 这个是明确知道我们想用 5 个字节来编码存储 prevlen 调用它
 *
 * 它会在两个场景下调用：
 * 1. 本身就需要用 5 个字节来编码，例如 len > ZIP_BIG_PREVLEN - 1
 *
 * 2. 可以用 1 个字节来编码，但是还是用 5 个字节来编码存储（见后面连锁更新部分）
 * 这种情况发生在某节点的 prevlen 是用 5 个字节存储，但是因为更新 / 删除
 * 它前一个节点的 size 变小了，即某节点的 prevlen 可以用 1 个字节存储了
 * 理论上我们可以进行缩容回收那 4 个字节，但是我们为了避免更多的连锁更新，不进行缩容
 */
int zipStorePrevEntryLengthLarge(unsigned char *p, unsigned int len) {
    uint32_t u32;
    if (p != NULL) {
        /* 设置第一个字节为 ZIP_BIG_PREVLEN 254 标识它 */
        p[0] = ZIP_BIG_PREVLEN;
        /* 把 len 值写进后面四个字节里 */
        u32 = len;
        memcpy(p+1,&u32,sizeof(u32));
        memrev32ifbe(p+1);
    }
    /* 返回编码存储 len 需要的字节数，其实就是 5 个字节 */
    return 1 + sizeof(uint32_t);
}

/* Encode the length of the previous entry and write it to "p". Return the
 * number of bytes needed to encode this length if "p" is NULL. */
/* 根据 len 将节点的 prevlen 属性写进节点（写到 p 的位置）
 * 如果 p 为 NULL 的话，返回编码存储 len 需要的字节数，1 或者 5 个字节 */
unsigned int zipStorePrevEntryLength(unsigned char *p, unsigned int len) {
    if (p == NULL) {
        /* p 为 NULL 根据 len 的值返回编码所需的字节数
         * len < ZIP_BIG_PREVLEN (254) 则用 1 个字节，否则用 5 个字节 */
        return (len < ZIP_BIG_PREVLEN) ? 1 : sizeof(uint32_t) + 1;
    } else {
        if (len < ZIP_BIG_PREVLEN) {
            /* len < ZIP_BIG_PREVLEN (254) 用 1 个字节编码存储 len */
            p[0] = len;
            return 1;
        } else {
            /* len >= ZIP_BIG_PREVLEN (254) 用 5 个字节编码存储 len */
            return zipStorePrevEntryLengthLarge(p,len);
        }
    }
}

/* Return the number of bytes used to encode the length of the previous
 * entry. The length is returned by setting the var 'prevlensize'. */
/* 取出编码前一个节点长度所需的字节数，并将它保存到 prevlensize 变量中
 * 可以看到是直接根据 (ptr)[0] < ZIP_BIG_PREVLEN 来判断的，时间复杂度为 O(1) */
#define ZIP_DECODE_PREVLENSIZE(ptr, prevlensize) do {                          \
    if ((ptr)[0] < ZIP_BIG_PREVLEN) {                                          \
        (prevlensize) = 1;                                                     \
    } else {                                                                   \
        (prevlensize) = 5;                                                     \
    }                                                                          \
} while(0)

/* Return the length of the previous element, and the number of bytes that
 * are used in order to encode the previous element length.
 * 'ptr' must point to the prevlen prefix of an entry (that encodes the
 * length of the previous entry in order to navigate the elements backward).
 * The length of the previous entry is stored in 'prevlen', the number of
 * bytes needed to encode the previous entry length are stored in
 * 'prevlensize'. */
/* 根据 prevlensize 知道 prevlen 的编码方式，然后获取 prevlen 的值
 * 一个字节编码，对应字节 (ptr)[0] 的值就是 prevlen
 * 五个字节编码，具体的 prevlen 是存储在后四个字节，后四个字节进行位运算获得实际的 prevlen
 */
#define ZIP_DECODE_PREVLEN(ptr, prevlensize, prevlen) do {                     \
    ZIP_DECODE_PREVLENSIZE(ptr, prevlensize);                                  \
    if ((prevlensize) == 1) {                                                  \
        (prevlen) = (ptr)[0];                                                  \
    } else { /* prevlensize == 5 */                                            \
        (prevlen) = ((ptr)[4] << 24) |                                         \
                    ((ptr)[3] << 16) |                                         \
                    ((ptr)[2] <<  8) |                                         \
                    ((ptr)[1]);                                                \
    }                                                                          \
} while(0)

/* Given a pointer 'p' to the prevlen info that prefixes an entry, this
 * function returns the difference in number of bytes needed to encode
 * the prevlen if the previous entry changes of size.
 *
 * So if A is the number of bytes used right now to encode the 'prevlen'
 * field.
 *
 * And B is the number of bytes that are needed in order to encode the
 * 'prevlen' if the previous element will be updated to one of size 'len'.
 *
 * Then the function returns B - A
 *
 * So the function returns a positive number if more space is needed,
 * a negative number if less space is needed, or zero if the same space
 * is needed. */
/* 根据 p 获取它的 prevlensize，记为 A，即当前节点 prevlensize 值
 * 根据 len 获取编码存储它需要的字节数，记为 B，即前一个节点的 prevlen 改变了，编码存储它需要多少字节
 * 计算 B - A 的差值，即返回重新编码 prevlen 所需要的字节数的差值
 * 根据现在的设计它只有三种结果：
 *   1. 差值为 0，说明空间刚刚好，例如两者都为 1 或者两者都为 5
 *   2. 差值为 +4，说明之前是用 5 个字节，len 改变后可以用 1 个字节，如果要缩容的话可以回收 4 个字节
 *   3. 差值为 -4，说明之前是用 1 个字节，len 改变后要用 5 个字节，这种情况代表一定需要扩容，不然无法存储 prevlen
 */
int zipPrevLenByteDiff(unsigned char *p, unsigned int len) {
    unsigned int prevlensize;
    ZIP_DECODE_PREVLENSIZE(p, prevlensize);
    return zipStorePrevEntryLength(NULL, len) - prevlensize;
}

/* Check if string pointed to by 'entry' can be encoded as an integer.
 * Stores the integer value in 'v' and its encoding in 'encoding'. */
int zipTryEncoding(unsigned char *entry, unsigned int entrylen, long long *v, unsigned char *encoding) {
    long long value;

    if (entrylen >= 32 || entrylen == 0) return 0;
    if (string2ll((char*)entry,entrylen,&value)) {
        /* Great, the string can be encoded. Check what's the smallest
         * of our encoding types that can hold this value. */
        if (value >= 0 && value <= 12) {
            *encoding = ZIP_INT_IMM_MIN+value;
        } else if (value >= INT8_MIN && value <= INT8_MAX) {
            *encoding = ZIP_INT_8B;
        } else if (value >= INT16_MIN && value <= INT16_MAX) {
            *encoding = ZIP_INT_16B;
        } else if (value >= INT24_MIN && value <= INT24_MAX) {
            *encoding = ZIP_INT_24B;
        } else if (value >= INT32_MIN && value <= INT32_MAX) {
            *encoding = ZIP_INT_32B;
        } else {
            *encoding = ZIP_INT_64B;
        }
        *v = value;
        return 1;
    }
    return 0;
}

/* Store integer 'value' at 'p', encoded as 'encoding' */
void zipSaveInteger(unsigned char *p, int64_t value, unsigned char encoding) {
    int16_t i16;
    int32_t i32;
    int64_t i64;
    if (encoding == ZIP_INT_8B) {
        ((int8_t*)p)[0] = (int8_t)value;
    } else if (encoding == ZIP_INT_16B) {
        i16 = value;
        memcpy(p,&i16,sizeof(i16));
        memrev16ifbe(p);
    } else if (encoding == ZIP_INT_24B) {
        i32 = ((uint64_t)value)<<8;
        memrev32ifbe(&i32);
        memcpy(p,((uint8_t*)&i32)+1,sizeof(i32)-sizeof(uint8_t));
    } else if (encoding == ZIP_INT_32B) {
        i32 = value;
        memcpy(p,&i32,sizeof(i32));
        memrev32ifbe(p);
    } else if (encoding == ZIP_INT_64B) {
        i64 = value;
        memcpy(p,&i64,sizeof(i64));
        memrev64ifbe(p);
    } else if (encoding >= ZIP_INT_IMM_MIN && encoding <= ZIP_INT_IMM_MAX) {
        /* Nothing to do, the value is stored in the encoding itself. */
    } else {
        assert(NULL);
    }
}

/* Read integer encoded as 'encoding' from 'p' */
int64_t zipLoadInteger(unsigned char *p, unsigned char encoding) {
    int16_t i16;
    int32_t i32;
    int64_t i64, ret = 0;
    if (encoding == ZIP_INT_8B) {
        ret = ((int8_t*)p)[0];
    } else if (encoding == ZIP_INT_16B) {
        memcpy(&i16,p,sizeof(i16));
        memrev16ifbe(&i16);
        ret = i16;
    } else if (encoding == ZIP_INT_32B) {
        memcpy(&i32,p,sizeof(i32));
        memrev32ifbe(&i32);
        ret = i32;
    } else if (encoding == ZIP_INT_24B) {
        i32 = 0;
        memcpy(((uint8_t*)&i32)+1,p,sizeof(i32)-sizeof(uint8_t));
        memrev32ifbe(&i32);
        ret = i32>>8;
    } else if (encoding == ZIP_INT_64B) {
        memcpy(&i64,p,sizeof(i64));
        memrev64ifbe(&i64);
        ret = i64;
    } else if (encoding >= ZIP_INT_IMM_MIN && encoding <= ZIP_INT_IMM_MAX) {
        ret = (encoding & ZIP_INT_IMM_MASK)-1;
    } else {
        assert(NULL);
    }
    return ret;
}

/* Fills a struct with all information about an entry.
 * This function is the "unsafe" alternative to the one below.
 * Generally, all function that return a pointer to an element in the ziplist
 * will assert that this element is valid, so it can be freely used.
 * Generally functions such ziplistGet assume the input pointer is already
 * validated (since it's the return value of another function). */
/* 将 ziplist 节点内存填充为 zlentry 结构体，这并不是一个节点的存储布局，是方便我们进行节点表示 / 操作的 */
static inline void zipEntry(unsigned char *p, zlentry *e) {
    /* 根据 p 目前的指针，获取 entry 的 prevlen 相关属性 */
    ZIP_DECODE_PREVLEN(p, e->prevrawlensize, e->prevrawlen);
    /* p+prevrawlensize 位置的第一个字节，获取 entry 当前的 encoding 属性 */
    ZIP_ENTRY_ENCODING(p + e->prevrawlensize, e->encoding);
    /* p+prevrawlensize 根据 encoding 获取 entry 的 len 相关属性 */
    ZIP_DECODE_LENGTH(p + e->prevrawlensize, e->encoding, e->lensize, e->len);
    assert(e->lensize != 0); /* check that encoding was valid. */
    /* entry 的 headersize 部分由 prevrawlensize+lensize 组成，即节点的 prevlen + encoding 部分占用的字节数 */
    e->headersize = e->prevrawlensize + e->lensize;
    /* 将节点的开头指针存到 zlentry->p */
    e->p = p;
}

/* Fills a struct with all information about an entry.
 * This function is safe to use on untrusted pointers, it'll make sure not to
 * try to access memory outside the ziplist payload.
 * Returns 1 if the entry is valid, and 0 otherwise. */
/* 上面 zipEntry 的安全版本，会校验确保不会访问到 ziplist 以外的内存空间 */
static inline int zipEntrySafe(unsigned char* zl, size_t zlbytes, unsigned char *p, zlentry *e, int validate_prevlen) {
    unsigned char *zlfirst = zl + ZIPLIST_HEADER_SIZE;
    unsigned char *zllast = zl + zlbytes - ZIPLIST_END_SIZE;
#define OUT_OF_RANGE(p) (unlikely((p) < zlfirst || (p) > zllast))

    /* If there's no possibility for the header to reach outside the ziplist,
     * take the fast path. (max lensize and prevrawlensize are both 5 bytes) */
    if (p >= zlfirst && p + 10 < zllast) {
        ZIP_DECODE_PREVLEN(p, e->prevrawlensize, e->prevrawlen);
        ZIP_ENTRY_ENCODING(p + e->prevrawlensize, e->encoding);
        ZIP_DECODE_LENGTH(p + e->prevrawlensize, e->encoding, e->lensize, e->len);
        e->headersize = e->prevrawlensize + e->lensize;
        e->p = p;
        /* We didn't call ZIP_ASSERT_ENCODING, so we check lensize was set to 0. */
        if (unlikely(e->lensize == 0))
            return 0;
        /* Make sure the entry doesn't reach outside the edge of the ziplist */
        if (OUT_OF_RANGE(p + e->headersize + e->len))
            return 0;
        /* Make sure prevlen doesn't reach outside the edge of the ziplist */
        if (validate_prevlen && OUT_OF_RANGE(p - e->prevrawlen))
            return 0;
        return 1;
    }

    /* Make sure the pointer doesn't reach outside the edge of the ziplist */
    if (OUT_OF_RANGE(p))
        return 0;

    /* Make sure the encoded prevlen header doesn't reach outside the allocation */
    ZIP_DECODE_PREVLENSIZE(p, e->prevrawlensize);
    if (OUT_OF_RANGE(p + e->prevrawlensize))
        return 0;

    /* Make sure encoded entry header is valid. */
    ZIP_ENTRY_ENCODING(p + e->prevrawlensize, e->encoding);
    e->lensize = zipEncodingLenSize(e->encoding);
    if (unlikely(e->lensize == ZIP_ENCODING_SIZE_INVALID))
        return 0;

    /* Make sure the encoded entry header doesn't reach outside the allocation */
    if (OUT_OF_RANGE(p + e->prevrawlensize + e->lensize))
        return 0;

    /* Decode the prevlen and entry len headers. */
    ZIP_DECODE_PREVLEN(p, e->prevrawlensize, e->prevrawlen);
    ZIP_DECODE_LENGTH(p + e->prevrawlensize, e->encoding, e->lensize, e->len);
    e->headersize = e->prevrawlensize + e->lensize;

    /* Make sure the entry doesn't reach outside the edge of the ziplist */
    if (OUT_OF_RANGE(p + e->headersize + e->len))
        return 0;

    /* Make sure prevlen doesn't reach outside the edge of the ziplist */
    if (validate_prevlen && OUT_OF_RANGE(p - e->prevrawlen))
        return 0;

    e->p = p;
    return 1;
#undef OUT_OF_RANGE
}

/* Return the total number of bytes used by the entry pointed to by 'p'. */
static inline unsigned int zipRawEntryLengthSafe(unsigned char* zl, size_t zlbytes, unsigned char *p) {
    zlentry e;
    assert(zipEntrySafe(zl, zlbytes, p, &e, 0));
    return e.headersize + e.len;
}

/* Return the total number of bytes used by the entry pointed to by 'p'. */
/* 返回 p 指向的节点占用的字节总数和 */
static inline unsigned int zipRawEntryLength(unsigned char *p) {
    zlentry e;
    zipEntry(p, &e);
    return e.headersize + e.len;
}

/* Validate that the entry doesn't reach outside the ziplist allocation. */
static inline void zipAssertValidEntry(unsigned char* zl, size_t zlbytes, unsigned char *p) {
    zlentry e;
    assert(zipEntrySafe(zl, zlbytes, p, &e, 1));
}

/* Create a new empty ziplist. */
/* 创建一个空 ziplist 只包含 <zlbytes><zltail><zllen><zlend> */
unsigned char *ziplistNew(void) {
    /* ziplist_header，两个 uint32_t + 一个 uint16_t，即 zlbytes(4) + zltail(4) + zllen(2) = 10 bytes
     * ziplist_end，一个 uint8_t 即 zlend 为 1 byte
     * 初始化好header与end共11字节 */
    unsigned int bytes = ZIPLIST_HEADER_SIZE+ZIPLIST_END_SIZE;
    /* 给 ziplist 分配内存空间 */
    unsigned char *zl = zmalloc(bytes);

    /* zlbytes: 将 ziplist 总字节数写进内存
     * 既为ziplist的起始地址，又负责记录ziplist的字节长度，zlbytes固定4字节，也就代表了一个ziplist最长为(2^32)-1字节*/
    ZIPLIST_BYTES(zl) = intrev32ifbe(bytes);
    /* zltail: 将到尾节点的偏移量写进内存，因为是刚初始化的 ziplist，
     * 偏移量其实就是 HEADER_SIZE 值，此时它刚好指向 zlend，因此能够以 O(1) 时间复杂度快速在尾部进行 push 或 pop 操作 */
    ZIPLIST_TAIL_OFFSET(zl) = intrev32ifbe(ZIPLIST_HEADER_SIZE);
    /* zllen: 将 ziplist 节点数量写进内存，初始化是 0 */
    ZIPLIST_LENGTH(zl) = 0;
    /* zlend: 最后一个字节设置为 ZIP_END，标识 ziplist 结尾 */
    zl[bytes-1] = ZIP_END;
    return zl;
}

/* Resize the ziplist. */
/* 调整压缩列表为 len 大小 */
unsigned char *ziplistResize(unsigned char *zl, size_t len) {
    assert(len < UINT32_MAX);
    /* 给 zl 重新分配空间，如果 len 大于原来的大小，会保留原有的元素 */
    zl = zrealloc(zl,len);
    /* 更新 zlbytes */
    ZIPLIST_BYTES(zl) = intrev32ifbe(len);
    /* 重新设置 ZIP_END */
    zl[len-1] = ZIP_END;
    return zl;
}

/* 这个是 6.2 版本之前的连锁更新函数代码，后面紧跟着的是 6.2 版本（7.0最新）的代码
 * 通过两者的差异来看是怎么对函数进行的优化 */

/* When an entry is inserted, we need to set the prevlen field of the next
 * entry to equal the length of the inserted entry. It can occur that this
 * length cannot be encoded in 1 byte and the next entry needs to be grow
 * a bit larger to hold the 5-byte encoded prevlen. This can be done for free,
 * because this only happens when an entry is already being inserted (which
 * causes a realloc and memmove). However, encoding the prevlen may require
 * that this entry is grown as well. This effect may cascade throughout
 * the ziplist when there are consecutive entries with a size close to
 * ZIP_BIG_PREVLEN, so we need to check that the prevlen can be encoded in
 * every consecutive entry.
 *
 * Note that this effect can also happen in reverse, where the bytes required
 * to encode the prevlen field can shrink. This effect is deliberately ignored,
 * because it can cause a "flapping" effect where a chain prevlen fields is
 * first grown and then shrunk again after consecutive inserts. Rather, the
 * field is allowed to stay larger than necessary, because a large prevlen
 * field implies the ziplist is holding large entries anyway.
 *
 * The pointer "p" points to the first entry that does NOT need to be
 * updated, i.e. consecutive fields MAY need an update. */
/* 当一个新节点插入到某个节点之前的时候，如果原节点 header 不足以保存新节点的长度
 * 即新节点的后继节点的 prevlen 字段不足以保存新节点的长度，此时需要对后继节点扩展
 * 但是当对后继节点扩展的时候，扩展后的它也有可能会导致它的后继节点扩展
 * 这种情况在多个连续节点，长度再接近 ZIP_BIG_PREVLEN == 254 的时候会发生 see #9218
 *
 * 反过来说，因为节点长度变小然后引起的连续缩小也是有可能出现的
 * 例如前一个节点长度变小，后一个节点的 prevlen 是可以从 5 缩小到 1
 * 不过为了避免 扩展 -> 缩小 -> 扩展 -> 缩小这样的情况老是出现，避免过多的连锁更新
 * 这种缩容的情况不会进行处理，而是继续让 prevlen 保持 5 个字节，存储可以用 1 个字节存储的值
 *
 * 函数检查并修复后续连续节点的连锁更新问题，针对指针 p 的后面节点进行检查
 * 注意不包含 p 对应的节点，因为 p 在传入之前就已经完成了扩展操作 */
unsigned char *__ziplistCascadeUpdate_before_62(unsigned char *zl, unsigned char *p) {
    /* curlen 保存当前 ziplist 的总字节数 */
    size_t curlen = intrev32ifbe(ZIPLIST_BYTES(zl)), rawlen, rawlensize;
    size_t offset, noffset, extra;
    unsigned char *np;
    zlentry cur, next;

    /* 只要没有达到末尾就一直循环 */
    while (p[0] != ZIP_END) {
        /* 对 p 指向的节点信息保存到 cur 中 */
        zipEntry(p, &cur);
        /* 当前节点占用的内存字节数 rawlen = prevrawlensize + lensize + len(value) */
        rawlen = cur.headersize + cur.len;
        /* 计算编码当前字节长度所需的字节数即 prevlenSize */
        rawlensize = zipStorePrevEntryLength(NULL,rawlen);

        /* Abort if there is no next entry. */
        /* 如果没有下一个节点则跳出
           连锁更新的第一个结束条件 */
        if (p[rawlen] == ZIP_END) break;

        /* 将 p+rawlen 后继节点的信息保存在 next 中 */
        zipEntry(p+rawlen, &next);

        /* Abort when "prevlen" has not changed. */
        /* 如果 next 的 prevrawlen == rawlen
           即 next 节点 prevrawlen 锁保存的长度就等于 p 节点的长度
           这种就是长度刚好，不需要进行变动，后面的节点也不用更新，跳出
           连锁更新的第二个结束条件 */
        if (next.prevrawlen == rawlen) break;

        if (next.prevrawlensize < rawlensize) {
            /* The "prevlen" field of "next" needs more bytes to hold
             * the raw length of "cur". */
            /* next 节点的 prevlenSize 小于编码 p 节点需要的字节长度，说明 next 节点的 header 需要扩展
               offset 记录当前 p 的偏移量，在后面内存重分配后可以再精准定位 p */
            offset = p-zl;
            /* 需要扩展的字节数，其实就是 insert 部分里的 nextdiff */
            extra = rawlensize-next.prevrawlensize;
            /* 调整 ziplist 的空间大小，需要扩展 extra */
            zl = ziplistResize(zl,curlen+extra);
            /* 根据偏移量定位回 p */
            p = zl+offset;

            /* Current pointer and offset for next element. */
            /* next p 指向 next 节点的新地址 */
            np = p+rawlen;
            /* next offset 记录 next 节点的偏移量 */
            noffset = np-zl;

            /* Update tail offset when next element is not the tail element. */
            /* 如果 next 节点不是尾节点，此时需要更新 tail offset */
            if ((zl+intrev32ifbe(ZIPLIST_TAIL_OFFSET(zl))) != np) {
                ZIPLIST_TAIL_OFFSET(zl) =
                        intrev32ifbe(intrev32ifbe(ZIPLIST_TAIL_OFFSET(zl))+extra);
            }

            /* Move the tail to the back. */
            /* 从 np+next.prevrawlensize 复制 curlen-noffset-next.prevrawlensize-1 个字符到 np+rawlensize
               将 next 节点之后的所有内存往后移动一下，空出空间扩展 next.prevlenSize */
            memmove(np+rawlensize,
                    np+next.prevrawlensize,
                    curlen-noffset-next.prevrawlensize-1);
            /* 将 next 节点的 prevlenSize 以 rawlen 重新编码，更新 next 的 prevrawlen / prevrawlensize */
            zipStorePrevEntryLength(np,rawlen);

            /* Advance the cursor */
            /* 将 p 指针后移，移动到 next 节点，下一次循环处理 next.next */
            p += rawlen;
            /* 更新 ziplist 的总字节数，加上扩展的字节数 */
            curlen += extra;
        } else {
            /* next 节点的 prevrawlensize >= 新节点的 rawlensize，说明空间足够，不需要扩展 */
            if (next.prevrawlensize > rawlensize) {
                /* This would result in shrinking, which we want to avoid.
                 * So, set "rawlen" in the available bytes. */
                /* 到这里实际上是 prevrawlensize == 5, rawlensize == 1 的情况
                   说明有富余的空间，但是为了避免过多的连锁更新，不会进行缩容，用5字节重新对1字节的编码重新编码 */
                zipStorePrevEntryLengthLarge(p+rawlen,rawlen);
            } else {
                /* prevrawlensize == rawlensize 说明空间刚好足够，更新 header 里的 prevrawlen 即可 */
                zipStorePrevEntryLength(p+rawlen,rawlen);
            }

            /* Stop here, as the raw length of "next" has not changed. */
            /* next 节点实际上没有进行扩展，它后面的节点也不需要扩展，在这里跳出
               连锁更新的第三个跳出条件 */
            break;
        }
    }
    return zl;
}

/* When an entry is inserted, we need to set the prevlen field of the next
 * entry to equal the length of the inserted entry. It can occur that this
 * length cannot be encoded in 1 byte and the next entry needs to be grow
 * a bit larger to hold the 5-byte encoded prevlen. This can be done for free,
 * because this only happens when an entry is already being inserted (which
 * causes a realloc and memmove). However, encoding the prevlen may require
 * that this entry is grown as well. This effect may cascade throughout
 * the ziplist when there are consecutive entries with a size close to
 * ZIP_BIG_PREVLEN, so we need to check that the prevlen can be encoded in
 * every consecutive entry.
 *
 * Note that this effect can also happen in reverse, where the bytes required
 * to encode the prevlen field can shrink. This effect is deliberately ignored,
 * because it can cause a "flapping" effect where a chain prevlen fields is
 * first grown and then shrunk again after consecutive inserts. Rather, the
 * field is allowed to stay larger than necessary, because a large prevlen
 * field implies the ziplist is holding large entries anyway.
 *
 * The pointer "p" points to the first entry that does NOT need to be
 * updated, i.e. consecutive fields MAY need an update. */
/* 当一个新节点插入到某个节点之前的时候，如果原节点 header 不足以保存新节点的长度
 * 即新节点的后继节点的 prevlen 字段不足以保存新节点的长度，此时需要对后继节点扩展
 * 但是当对后继节点扩展的时候，扩展后的它也有可能会导致它的后继节点扩展
 * 这种情况在多个连续节点，长度再接近 ZIP_BIG_PREVLEN == 254 的时候会发生 see #9218
 *
 * 反过来说，因为节点长度变小然后引起的连续缩小也是有可能出现的
 * 例如前一个节点长度变小，后一个节点的 prevlen 是可以从 5 缩小到 1
 * 不过为了避免 扩展 -> 缩小 -> 扩展 -> 缩小这样的情况老是出现，避免过多的连锁更新
 * 这种缩容的情况不会进行处理，而是继续让 prevlen 保持 5 个字节，存储可以用 1 个字节存储的值
 *
 * 函数检查并修复后续连续节点的连锁更新问题，针对指针 p 的后面节点进行检查
 * 注意不包含 p 对应的节点，因为 p 在传入之前就已经完成了扩展操作 */

/* 在插入或者删除或者编辑节点时，我们需要检查或者维护后面节点的 prevlen，如果导致后面节点需要扩容或者缩容，这种就是连锁更新
 * 在 Redis 中称为 cascade update。连锁更新需要对后面的所有节点进行检查维护 prevlen，当然如果相安无事不会连锁更新的话可以提前跳出
 * 在原本的实现中，连锁更新在最坏情况下需要对压缩列表进行 N 次空间的重分配操作（老实现中是对单个节点单个节点这样重分配，当然好处是实现简单，对节点进行循环重分配），
 * 而每次空间重分配的最坏复杂度为 O(N)，所以整体连锁更新的最坏复杂度为 O(N^2)。
 * 不过其实性能问题很低，因为这样多个节点连续又刚好在 [250, 253] 条件上很难刚好发生，
 * 而就算发生，因为 ziplist 又只用在少量节点的场景下，节点数量不多，就不会真的对性能造成严重损失。
 *
 * 不过在 PR#6886 中，对连锁更新算法进行了优化，上面说到的时间复杂度最坏降为了 O(N)
 * it's basically an optimization, instead of multiple reallocs and multiple memmove of the entire tail of the list,
 * it does one realloc and multiple single-entry memmoves.
 *
 * 后面代码是 #6886 中进行的优化，在之前版本的实现里，也就是前面部分的实现，是 O(n^2) 的时间复杂度，
 * 它遍历整个压缩列表，针对需要扩展的节点，一个个的进行扩展，
 * 一个节点就需要做一次小的 realloc 重新分配空间（只处理单个节点要扩展的）和一次大的 memmove 内存拷贝（需要移动整个压缩列表）。
 *
 * 这个优化里，是 O(n) 的时间复杂度，它通过先遍历完整个压缩列表，算出需要扩展的字节数，
 * 然后只进行一次大 realloc 将需要扩展的所有空间先重新分配好，然后针对每个节点进行一次小的 memmove 处理单个节点的数据，可以看到这里的优化点主要是 realloc 和 memmove：
    ● realloc
      ○ 在之前是每有一个要扩展的节点就要申请下空间，有 n 个节点要扩展就要执行 n 次 realloc
      ○ 在之后，提前遍历完压缩列表，将要扩展的空间都先计算出来，有 n 个节点也只执行 1 次 realloc
    ● memmove
      ○ 在之前的话，是按照列表头到尾的方式扩展，每扩展一个节点内存后，就需要将整个 ziplist 都往后拷贝移动
      ○ 在之后，是按照列表尾到头的方式扩展（从最后一个需要扩展的节点开始往前），内存已经在前面就扩展完了，处理一个节点只需要将对应节点往后拷贝移动
 */
unsigned char *__ziplistCascadeUpdate(unsigned char *zl, unsigned char *p) {
    zlentry cur;
    size_t prevlen, prevlensize, prevoffset; /* Informat of the last changed entry. */
    size_t firstentrylen; /* Used to handle insert at head. */
    /* curlen: ziplist 的字节长度 */
    size_t rawlen, curlen = intrev32ifbe(ZIPLIST_BYTES(zl));
    size_t extra = 0, cnt = 0, offset;
    size_t delta = 4; /* Extra bytes needed to update a entry's prevlen (5-1). */
    unsigned char *tail = zl + intrev32ifbe(ZIPLIST_TAIL_OFFSET(zl));

    /* Empty ziplist */
    /* 空的压缩列表，直接返回 */
    if (p[0] == ZIP_END) return zl;

    zipEntry(p, &cur); /* no need for "safe" variant since the input pointer was validated by the function that returned it. */
    /* firstentrylen 第一个节点的字节长度，这个节点是不参与扩展的，这个节点在函数进来前就处理好了，它用于例如头部插入的场景
       在 ziplist 头部插入一个节点后，此时 p 就为新头节点，原头节点变成后继节点，变量存储 p 的长度，后面用于更新老头的 prevlen
       prevlen 变量用于存储前一个节点的字节长度 */
    firstentrylen = prevlen = cur.headersize + cur.len;
    /* 计算存储 prevlen 需要的字节数 prevlenSize */
    prevlensize = zipStorePrevEntryLength(NULL, prevlen);
    /* 记录目前 prev 的偏移量 */
    prevoffset = p - zl;
    /* p 加上 cur.headersize + cur.len 实际上是跳到下一个节点 */
    p += prevlen;

    /* Iterate ziplist to find out how many extra bytes do we need to update it. */
    /* 只要没有到末尾就一直循环它，先迭代 ziplist 找出一共需要多少额外字节来更新它 */
    while (p[0] != ZIP_END) {
        /* 将当前 p 指向的节点信息保存在 cur 中 */
        assert(zipEntrySafe(zl, curlen, p, &cur, 0));

        /* Abort when "prevlen" has not changed. */
        /* 如果 cur 当前节点的 prevrawlen 等于前一个节点的长度 prevlen
           这种情况就是长度刚好，不需要变动，后面的节点也不用更新，跳出
           连锁更新的第一个结束条件 */
        if (cur.prevrawlen == prevlen) break;

        /* Abort when entry's "prevlensize" is big enough. */
        /* 如果 cur 当前节点的 prevrawlensize >= prevlensize
           这种情况就是空间足够，不需要扩展，后面节点也不用更新，在更新好 prevlen 后跳出
           连锁更新的第二个结束条件 */
        if (cur.prevrawlensize >= prevlensize) {
            if (cur.prevrawlensize == prevlensize) {
                /* (1 == 1 or 5 == 5) 如果刚好相等，直接将 prevlen 写入 */
                zipStorePrevEntryLength(p, prevlen);
            } else {
                /* This would result in shrinking, which we want to avoid.
                 * So, set "prevlen" in the available bytes. */
                /* 到这里实际上是 prevrawlensize == 5, rawlensize == 1 的情况
                   说明有富余的空间，但是为了避免过多的连锁更新，不会进行缩容，用5字节重新对1字节的编码重新编码 */
                zipStorePrevEntryLengthLarge(p, prevlen);
            }
            break;
        }

        /* cur.prevrawlen means cur is the former head entry. */
        /* 断言一下：cur 要么是前头节点；要么是要扩展的情况，断言差值为 4 个字节 */
        assert(cur.prevrawlen == 0 || cur.prevrawlen + delta == prevlen);

        /* Update prev entry's info and advance the cursor. */
        /* rawlen: 当前 cur 节点占用的字节数 */
        rawlen = cur.headersize + cur.len;
        /* prevlen: 因为 cur 节点将要进行扩展，它后一个节点将来的 prevlen 在 rawlen 基础上加 delta */
        prevlen = rawlen + delta;
        /* prevlensize: 计算出存储 prevlen 需要的字节数 */
        prevlensize = zipStorePrevEntryLength(NULL, prevlen);
        /* prevoffset: 记录下当前处理了的 cur 节点的偏移量，站在后一个节点的角度为 prevoffset */
        prevoffset = p - zl;
        /* p 加上 rawlen 跳到后一个节点 */
        p += rawlen;
        /* extra: 记录了 ziplist 扩展需要的总额外字节，加上当前节点要扩展的 4 个字节 */
        extra += delta;
        /* cnt: 要扩展的节点数 */
        cnt++;
    }

    /* Extra bytes is zero all update has been done(or no need to update). */
    /* 出来循环后如果 extra == 0 说明 ziplist 不需要更新，直接返回 */
    if (extra == 0) return zl;

    /* Update tail offset after loop. */
    if (tail == zl + prevoffset) {
        /* When the last entry we need to update is also the tail, update tail offset
         * unless this is the only entry that was updated (so the tail offset didn't change). */
        /* 根据前面循环的处理逻辑，如果 tail == zl + prevoffset 即说明最后处理需要扩展的节点 cur 为尾节点
           同时需要判断 extra-delta，如果结果为 0 说明只有一个节点需要扩展，且它是尾节点，这种情况不用更新尾节点偏移量
           因为 ziplist 只有最后的尾节点需要进行扩展，但是尾节点前面的节点都没有变动，所以 zl 到尾节点的 offset 是不用变的
           如果结果不为 0 说明有在尾节点前面的节点需要扩展，此时维护到尾节点的偏移量，为 extra-delta 偏移量要减掉尾节点的扩展 */
        if (extra - delta != 0) {
            ZIPLIST_TAIL_OFFSET(zl) =
                intrev32ifbe(intrev32ifbe(ZIPLIST_TAIL_OFFSET(zl))+extra-delta);
        }
    } else {
        /* Update the tail offset in cases where the last entry we updated is not the tail. */
        /* 尾节点没有参与连锁更新的扩展，到尾节点的偏移量直接加上所有节点扩展的和 extra 即可 */
        ZIPLIST_TAIL_OFFSET(zl) =
            intrev32ifbe(intrev32ifbe(ZIPLIST_TAIL_OFFSET(zl))+extra);
    }

    /* Now "p" points at the first unchanged byte in original ziplist,
     * move data after that to new ziplist. */
    /* 现在 p 指向第一个不需要更改的字节，记录它的偏移量 */
    offset = p - zl;
    /* ziplist 在原本的 curlen 加上 extra 扩展部分进行 resize (zrealloc) */
    /* 对比 6.2 版本之前的优化点就是这边是针对所有节点扩展一次性进行 resize，之前是每个节点进行一个 resize */
    zl = ziplistResize(zl, curlen + extra);
    /* 根据偏移量定位回 p */
    p = zl + offset;
    /* 在 p 位置移动 curlen - offset - 1 到 p + extra 位置 */
    /* 即把后面不需要改动的节点数据移动到 p+extra 位置，空出 extra 给前面的节点进行扩展 header */
    memmove(p + extra, p, curlen - offset - 1);
    /* p 加上 extra 偏移量，此时 p 指向的是[第一个]不需要动的节点，它前面的部分是需要动的 */
    p += extra;

    /* Iterate all entries that need to be updated tail to head. */
    /* cnt 记录的是有多少个节点需要扩展，现在循环对每个节点进行扩展，注意是从后往前处理节点的 */
    while (cnt) {
        /* prevoffset 第一次进来的时候，它记录了最后一个需要扩展的节点，之后每次往前进行偏移一个节点 */
        /* 将对应需要扩展的节点信息存储在 cur 中，从后往前处理 */
        zipEntry(zl + prevoffset, &cur); /* no need for "safe" variant since we already iterated on all these entries above. */
        /* rawlen 记录 cur 当前节点的字节长度 */
        rawlen = cur.headersize + cur.len;
        /* Move entry to tail and reset prevlen. */
        /* 将要扩展的节点往后移动一下，空出前面的位置扩展 header (prevlen 部分)
           p - (rawlen - cur.prevrawlensize): p 目前指向不要动的第一个字节，往前偏移目前 rawlen-cur.prevrawlensize
           zl + prevoffset + cur.prevrawlensize: 它指向 cur 目前的 encoding+data 部分
           rawlen - cur.prevrawlensize: 它表示上一行里 encoding+data 部分的字节长度 */
        memmove(p - (rawlen - cur.prevrawlensize),
                zl + prevoffset + cur.prevrawlensize,
                rawlen - cur.prevrawlensize);
        /* p 往前偏移当前节点的字节长度，再偏移一个 delta 即再往前偏移 4 个字节（用于扩展的）
           此时需要在 p 的新位置重新编码当前节点的 prevlen，数据移动完然后重编码 prevlen，完成 cur 节点的扩展 */
        p -= (rawlen + delta);
        if (cur.prevrawlen == 0) {
            /* "cur" is the previous head entry, update its prevlen with firstentrylen. */
            /* cur.prevrawlen == 0 说明 cur 节点是 p(函数最开始的) 插入之前，ziplist 的头节点
               因为之前它是头节点，那时它是没有 prev 节点的，所以 prevrawlen == 0
               在 p(函数最开始的) 插入后它的 prev 就变成了 p，此时 prevlen 就为 p 节点的长度，在前面维护了 */
            zipStorePrevEntryLength(p, firstentrylen);
        } else {
            /* An entry's prevlen can only increment 4 bytes. */
            /* 在原本的 prevranlen 基础上扩展 4 个字节，目前 ziplist 规定就是只能扩展 4 个字节 (1->5) */
            zipStorePrevEntryLength(p, cur.prevrawlen+delta);
        }
        /* Forward to previous entry. */
        /* 往前走到下一个要处理的节点，例如 prevoffset = 100，前一个节点长度为 10，那么 zl + (100-10) 就是前一个要处理的节点 */
        prevoffset -= cur.prevrawlen;
        cnt--;
    }
    return zl;
}

/* Delete "num" entries, starting at "p". Returns pointer to the ziplist. */
/* 从 p 位置删除 num 个节点，包含删除 p */
unsigned char *__ziplistDelete(unsigned char *zl, unsigned char *p, unsigned int num) {
    unsigned int i, totlen, deleted = 0;
    size_t offset;
    int nextdiff = 0;
    zlentry first, tail;
    size_t zlbytes = intrev32ifbe(ZIPLIST_BYTES(zl));

    zipEntry(p, &first); /* no need for "safe" variant since the input pointer was validated by the function that returned it. */
    for (i = 0; p[0] != ZIP_END && i < num; i++) {
        /* p 一直加上当前节点的字节总数，往后遍历直到遇到列表结尾 */
        p += zipRawEntryLengthSafe(zl, zlbytes, p);
        /* 记录被删除节点的总个数 */
        deleted++;
    }

    /* 此时 first 指向第一个要删除的节点，经过循环后，p 指向最后要保留的第一个节点 或者 列表尾
       例如 a b c，从 b 开始删除 1 个节点 即变成 a c，此时 first 指向 b，p 指向 c
       此时 p - first.p 的值，就是删除这么多节点，它们所占用的字节总数 */
    assert(p >= first.p);
    totlen = p-first.p; /* Bytes taken by the element(s) to delete. */
    if (totlen > 0) {
        uint32_t set_tail;
        if (p[0] != ZIP_END) {
            /* 此时表明 p 不是列表结尾，删除节点后，还有后面的节点，后面这部分节点需要往前移动 */
            /* Storing `prevrawlen` in this entry may increase or decrease the
             * number of bytes required compare to the current `prevrawlen`.
             * There always is room to store this, because it was previously
             * stored by an entry that is now being deleted. */
            /* 因为删除节点后，后面的第一个节点的 prevlenSize 可能会不够，先计算 diff
               后面的第一个节点，即现在 p 指向的那个节点，判断它的 prevlenSize 是否够 first.prevlenSize */
            nextdiff = zipPrevLenByteDiff(p,first.prevrawlen);

            /* Note that there is always space when p jumps backward: if
             * the new previous entry is large, one of the deleted elements
             * had a 5 bytes prevlen header, so there is for sure at least
             * 5 bytes free and we need just 4. */
            /* 这里是一定会有足够空间的，即不需要说像 insert 那样扩展 prevlenSize
               因为如果 first 的前一个节点很大，那么本身 first.prevlenSize 就是足够的
               因为是删除节点，包含删除 first 节点，这里删除的空间是肯定够 p 节点 prevlenSize 扩展的
               将 p 向后移动 nextdiff 差值的长度，减少需要删除的内存，用来扩展第一个节点（都删除后的）的 prevlen */
            p -= nextdiff;
            assert(p >= first.p && p<zl+zlbytes-1);
            /* 将 first 的前一个节点的长度编码扩展到 p 当前的位置 */
            zipStorePrevEntryLength(p,first.prevrawlen);

            /* Update offset for tail */
            /* 更新列表末尾的偏移量，原本的 减去 所有被删除的内存 */
            set_tail = intrev32ifbe(ZIPLIST_TAIL_OFFSET(zl))-totlen;

            /* When the tail contains more than one entry, we need to take
             * "nextdiff" in account as well. Otherwise, a change in the
             * size of prevlen doesn't have an effect on the *tail* offset. */
            /* 如果被删除节点后有多于一个节点，那么需要将 nextdiff 也计算到表尾偏移量中
               因为当前 p 指向的不是尾节点，因此要加上 nextdiff 才能让表尾偏移量正确 */
            assert(zipEntrySafe(zl, zlbytes, p, &tail, 1));
            if (p[tail.headersize+tail.len] != ZIP_END) {
                set_tail = set_tail + nextdiff;
            }

            /* Move tail to the front of the ziplist */
            /* since we asserted that p >= first.p. we know totlen >= 0,
             * so we know that p > first.p and this is guaranteed not to reach
             * beyond the allocation, even if the entries lens are corrupted. */
            /* 末尾向前面移动数据，覆盖被删除节点 */
            size_t bytes_to_move = zlbytes-(p-zl)-1;
            memmove(first.p,p,bytes_to_move);
        } else {
            /* The entire tail was deleted. No need to move memory. */
            /* p[0] == ZIP_END 到达末尾，说明后面其实没有节点，无需移动内存
               更新尾节点偏移量到前一个节点的地址，因为此时 first 前一个节点是尾节点 */
            set_tail = (first.p-zl)-first.prevrawlen;
        }

        /* Resize the ziplist */
        /* 因为删除了节点，需要删除多余的内存，维护列表元数据，例如 resize 后设置 ZIP_END */
        offset = first.p-zl;
        zlbytes -= totlen - nextdiff;
        zl = ziplistResize(zl, zlbytes);
        p = zl+offset;

        /* Update record count */
        /* 维护压缩列表的节点数 */
        ZIPLIST_INCR_LENGTH(zl,-deleted);

        /* Set the tail offset computed above */
        assert(set_tail <= zlbytes - ZIPLIST_END_SIZE);
        /* 维护压缩列表达到尾部的偏移量 */
        ZIPLIST_TAIL_OFFSET(zl) = intrev32ifbe(set_tail);

        /* When nextdiff != 0, the raw length of the next entry has changed, so
         * we need to cascade the update throughout the ziplist */
        /* 如果 p 指向的节点大小有改变，那么检查是否需要连锁更新 */
        if (nextdiff != 0)
            zl = __ziplistCascadeUpdate(zl,p);
    }
    return zl;
}

/* Insert item at "p". */
/* 将 entry s 插入到 ziplist zl 的 p 位置之前 (插到 p 处，p 向后移此时 ziplist 变成 sp)
 * slen 为 entry s 的数据长度，即需要插入的元素的数据长度（是数据部分的长度，例如保存 hello，长度 slen 为 5）
 * 如果是字符串则为字符串长度; 如果是整数则需要根据编码计算对应需要的字节数
 *
 * 插入一个新的 entry 需要更新:
 *     插入 entry 后面节点的 prevlen 字段 (插入后需要维护后一个节点的元数据信息)
 *     维护 ziplist 首部的一些字段 => zlbytes,zltail,zllen */
unsigned char *__ziplistInsert(unsigned char *zl, unsigned char *p, unsigned char *s, unsigned int slen) {
    /* curlen: 当前 ziplist 的完整字节长度
       reqlen: 新节点插入需要的最终字节长度，即新节点的长度（请求长度）
       newlen: 插入后 ziplist 新的字节长度 */
    size_t curlen = intrev32ifbe(ZIPLIST_BYTES(zl)), reqlen, newlen;
    /* prevlen: 前一个 entry 的真实字节长度
       prevlensize: 用于表示 prevlen 占用的字节 (只会是 1 or 5) */
    unsigned int prevlensize, prevlen = 0;
    size_t offset;
    int nextdiff = 0;
    unsigned char encoding = 0;
    long long value = 123456789; /* initialized to avoid warning. Using a value
                                    that is easy to see if for some reason
                                    we use it uninitialized. */
    zlentry tail;

    /* Find out prevlen for the entry that is inserted. */
    /* 计算即将插入的新节点的 prevlen */
    if (p[0] != ZIP_END) {
        /* 不是在 zl 的尾部插入，即在中间插入，需要计算原来 p 元素的 prevlen，插入后也就是新元素的 prevlen */
        ZIP_DECODE_PREVLEN(p, prevlensize, prevlen);
    } else {
        /* 是在 zl 的尾部插入，通过 zltail 计算出 zl 最后一个 entry，然后计算 prevlen */
        unsigned char *ptail = ZIPLIST_ENTRY_TAIL(zl);
        if (ptail[0] != ZIP_END) {
            /* 此时 ptail 是指向最后一个节点，取出为节点的长度，作为插入节点的 prevlen */
            prevlen = zipRawEntryLengthSafe(zl, curlen, ptail);
        }
    }

    /* See if the entry can be encoded */
    if (zipTryEncoding(s,slen,&value,&encoding)) {
        /* 'encoding' is set to the appropriate integer encoding */
        /* 尝试将输入字符串转换为整数
           value: 保存了转换后的整数值
           encoding: 保存了 value 的编码方式
           reqlen: 整数的话, 就根据 encoding 确定保存节点值需要的字节长度 */
        reqlen = zipIntSize(encoding);
    } else {
        /* 'encoding' is untouched, however zipStoreEntryEncoding will use the
         * string length to figure out how to encode it. */
        /* reqlen: 字符串的话，当然直接用字符串的长度 */
        reqlen = slen;
    }

    /* We need space for both the length of the previous entry and
     * the length of the payload. */
    /* reqlen 加上 prevlenSize 加上 encodingSize */

    /* prevlen 是前一个 entry 中占有的字节总数，然后 prevlensize 为：存储这个`字节总数数字`需要几个字节 (1 or 5)
       实际上这里算出的 prevlenSize 是实际能表示 prevlen 的字节数，但不一定就是 p 的 prevlenSize，看后面 */
    reqlen += zipStorePrevEntryLength(NULL,prevlen);
    /* 算出存储当前节点的 encoding 和 length，需要多少个字节 */
    reqlen += zipStoreEntryEncoding(NULL,encoding,slen);

    /* 到这里，插入一个节点需要的字节长度已经确认了：reqlen = prevlenSize + encodingSize + dataSize */

    /* When the insert position is not equal to the tail, we need to
     * make sure that the next entry can hold this entry's length in
     * its prevlen field. */
    /* 当不是在结尾插入时，我们需要确保插入位置的后一个节点，有足够多的空间存储新插入节点的字节长度（即确保后节点的 prevlen 足够）
       即在 AC (此时p指向C) 中插入 B 变成 ABC，我们需要维护好 C 节点的 prevlen 信息 (元数据存储：从 A 变成 B) */
    int forcelarge = 0;
    /* nextdiff: 在尾部插入，此时当然没有后一个节点，相差的 diff 是 0
       在中间插入：此时 p 的前一个节点会变成 s，需要比较 p->prevlenSize 能否存放下 s 的长度即 reqlen
       这里的 nextdiff 新旧编码的差值，求出如果插入，两者之间相差的 diff，如果大于 0 说明要对 p 指向的节点的 header 进行扩展 */
    nextdiff = (p[0] != ZIP_END) ? zipPrevLenByteDiff(p,reqlen) : 0;

    /* nextdiff 连锁更新是否扩展: 0 空间相等（刚好，不需要扩展）, 4 需要更多空间（需要扩展）, -4 空间富余（可以缩容）*/
    if (nextdiff == -4 && reqlen < 4) {
        /* 1-5 = -4 说明新元素的长度只需要 1 个字节存储, p 原本的 prevlenSize 为 5 个字节
           如果 reqlen 再小于 4 的话, 就这样这样... 但是感觉这个 if 是不可能发生的
           因为 reqlen 本身组成是: prevlenSize + encodingSize + dataSize
           prevlenSize 为 5 的话, reqlen 中包含 prevlenSize，又怎么会 < 4

           实际上在连锁更新的时候会发生(看后面)，但是会稍难理解...
           https://github.com/redis/redis/commit/8327b813 and https://github.com/redis/redis/commit/0dbfb1d
           在连锁更新的时，为了防止大量的重新分配空间，如果一个新 entry 的长度只需要 1 个字节就能保存
           但是连锁更新时，如果原先已经为 prevlen 分配了 5 个字节，此时富余部分是不会进行缩容的（防止连续更新）
           也就是连锁更新里会跳过有这种情况的节点，不会进行缩容，所以就会有节点实际上用 5 个字节保存 < 254 的长度

           nextdiff 需要置为 0，这种情况下如果 nextdiff + reqlen < 0 的话会造成缩容
           然后后面 realloc 真进行缩容的话，数据的写入地址就是有问题的，可能会造成数据破坏 */
        nextdiff = 0;
        forcelarge = 1;
    }

    /* 此时 nextdiff 只会为 0 或者 4 这两种情况，不需要扩展 和 需要扩展两种情况，不会进行缩容 */

    /* Store offset because a realloc may change the address of zl. */
    /* 提前存一下 offset，p 位置相较于 ziplist 的偏移量，因为 realloc 可能会改变 zl 的地址
       记住偏移量的话，之后根据 ziplist 的初始地址，加上偏移量就还是能直接定位到 p */
    offset = p-zl;
    /* newlen: ziplist 原本长度 + 新节点需要的长度 + 可能要扩展的长度 */
    newlen = curlen+reqlen+nextdiff;
    /* 根据新的大小重新 resize realloc 扩展内存空间 */
    zl = ziplistResize(zl,newlen);
    /* ziplist 新地址加上偏移量，获取此时 p 的准确位置 */
    p = zl+offset;

    /* Apply memory move when necessary and update tail offset. */
    /* p 前面的部分都是 ok 的，但是 p 因为插入新元素，后面的部分可能需要进行移动 */
    if (p[0] != ZIP_END) {
        /* Subtract one because of the ZIP_END bytes */
        /* 不是在末尾插入，此时 nextdiff 其实只会为 0 或者 4，移动现有节点，腾出位置
           将 p-nextdiff 位置的 curlen-offset-1+nextdiff 字节数复制到 p+reqlen */
        memmove(p+reqlen,p-nextdiff,curlen-offset-1+nextdiff);

        /* Encode this entry's raw length in the next entry. */
        /* 将新节点的长度存储到后置节点去 */
        if (forcelarge)
            /* 这里就是对应连锁更新 nextdiff == -4 && reqlen < 4 的情况，实际上只需要 1 个字节就能存储
               但是为了避免大量连锁更新，所以这里后一个节点的 prevSize 不能缩成 1，还是得需要用 4 个字节 */
            zipStorePrevEntryLengthLarge(p+reqlen,reqlen);
        else
            /* 计算新节点的 prevlen，保存到新节点的后一个节点的 header 中 */
            zipStorePrevEntryLength(p+reqlen,reqlen);

        /* Update offset for tail */
        /* 维护 zltail, 将新节点的长度也算上 */
        ZIPLIST_TAIL_OFFSET(zl) =
            intrev32ifbe(intrev32ifbe(ZIPLIST_TAIL_OFFSET(zl))+reqlen);

        /* When the tail contains more than one entry, we need to take
         * "nextdiff" in account as well. Otherwise, a change in the
         * size of prevlen doesn't have an effect on the *tail* offset. */
        /* 这里如果 nextdiff == 4 > 0，说明后个节点的 prevSize 其实变大了的
           也需要把这部分增加落实到 zltail 算在偏移量里，这样才能真的对齐表尾节点
           感觉在这里提前判断下 nextdiff > 0 在往下执行会好点 */
        assert(zipEntrySafe(zl, newlen, p+reqlen, &tail, 1));
        if (p[reqlen+tail.headersize+tail.len] != ZIP_END) {
            ZIPLIST_TAIL_OFFSET(zl) =
                intrev32ifbe(intrev32ifbe(ZIPLIST_TAIL_OFFSET(zl))+nextdiff);
        }
    } else {
        /* This element will be the new tail. */
        /* 新节点就是表尾节点 */
        ZIPLIST_TAIL_OFFSET(zl) = intrev32ifbe(p-zl);
    }

    /* When nextdiff != 0, the raw length of the next entry has changed, so
     * we need to cascade the update throughout the ziplist */
    /* nextdiff != 0 其实就是为 4，后一个元素的 prevlenSize 变了，如果后面的元素都因为这个增加需要改变 prevlenSize
       就可能会产生连锁更新(代码略复杂)，实际上这个情况比较难出现，而且 ziplist 元素本来就小，不过极端情况总是要考虑的
       这里最坏的时空复杂度貌似会是 O(N^2) */
    if (nextdiff != 0) {
        /* 同样是提前记下来 p 的偏移量，因为连锁更新（级联更新）会可能会重新分配内存空间 */
        offset = p-zl;
        zl = __ziplistCascadeUpdate(zl,p+reqlen);
        p = zl+offset;
    }

    /* Write the entry */
    /* 此时 p 指向新节点需要插入的位置
       将前一个节点的长度 和 新节点的长度 写到新节点的 header 中 */
    p += zipStorePrevEntryLength(p,prevlen);
    p += zipStoreEntryEncoding(p,encoding,slen);
    if (ZIP_IS_STR(encoding)) {
        /* 拷贝写入字符串，O(N) */
        memcpy(p,s,slen);
    } else {
        /* 写入整数，O(1) */
        zipSaveInteger(p,value,encoding);
    }
    /* ziplist 节点数量 += 1 */
    ZIPLIST_INCR_LENGTH(zl,1);
    return zl;
}

/* Merge ziplists 'first' and 'second' by appending 'second' to 'first'.
 *
 * NOTE: The larger ziplist is reallocated to contain the new merged ziplist.
 * Either 'first' or 'second' can be used for the result.  The parameter not
 * used will be free'd and set to NULL.
 *
 * After calling this function, the input parameters are no longer valid since
 * they are changed and free'd in-place.
 *
 * The result ziplist is the contents of 'first' followed by 'second'.
 *
 * On failure: returns NULL if the merge is impossible.
 * On success: returns the merged ziplist (which is expanded version of either
 * 'first' or 'second', also frees the other unused input ziplist, and sets the
 * input ziplist argument equal to newly reallocated ziplist return value. */
/* 给定 first second 两个 ziplist，将它们进行合并，合并后的新 ziplist 为 first second 这样
   返回新的 ziplist 地址，因为经过 realloc 和 free 入参的 ziplist 返回后都是不可用的
   合并是扩展其中较大（节点数多）的那个 ziplist，然后复制另外个 ziplist，然后释放另外个 ziplist */
unsigned char *ziplistMerge(unsigned char **first, unsigned char **second) {
    /* If any params are null, we can't merge, so NULL. */
    /* 入参校验，有空 ziplist 直接返回 NULL */
    if (first == NULL || *first == NULL || second == NULL || *second == NULL)
        return NULL;

    /* Can't merge same list into itself. */
    /* 如果传入的是相同的两个 ziplist 无法进行合并 */
    if (*first == *second)
        return NULL;

    /* 获取第一个 ziplist 的字节大小和节点数量 */
    size_t first_bytes = intrev32ifbe(ZIPLIST_BYTES(*first));
    size_t first_len = intrev16ifbe(ZIPLIST_LENGTH(*first));

    /* 获取第二个 ziplist 的字节大小和节点数量 */
    size_t second_bytes = intrev32ifbe(ZIPLIST_BYTES(*second));
    size_t second_len = intrev16ifbe(ZIPLIST_LENGTH(*second));

    /* append == 1: 扩展 first，在 first 后面追加 second */
    /* append == 0: 扩展 second，将 first 插入在 second 前面 */
    int append;
    /* source: 源 ziplist; target: 进行扩展的那个 ziplist */
    unsigned char *source, *target;
    size_t target_bytes, source_bytes;
    /* Pick the largest ziplist so we can resize easily in-place.
     * We must also track if we are now appending or prepending to
     * the target ziplist. */
    /* 选择更大的那个 ziplist 进行扩展，因为这样申请需要的内存相对更少，更容易 in-place resize
       这里是根据 len 节点数量进行的选择，理论上根据 bytes 判断大小压缩列表感觉会更好 */
    if (first_len >= second_len) {
        /* retain first, append second to first. */
        /* 保留扩展 first 将 second 附加到 first 后面，即为 first second */
        target = *first;
        target_bytes = first_bytes;
        source = *second;
        source_bytes = second_bytes;
        append = 1;
    } else {
        /* else, retain second, prepend first to second. */
        /* 保留扩展 second 将 second 后移，然后把 first 插入到它前面，即还是 first second */
        target = *second;
        target_bytes = second_bytes;
        source = *first;
        source_bytes = first_bytes;
        append = 0;
    }

    /* Calculate final bytes (subtract one pair of metadata) */
    /* 计算合并后的新压缩列表大小，两个 bytes 相加然后减去一份元数据长度（元数据保留一份即可）
       元数据包括：header + end 即 zlbytes + zltail_offset + zllen + zlend */
    size_t zlbytes = first_bytes + second_bytes -
                     ZIPLIST_HEADER_SIZE - ZIPLIST_END_SIZE;
    /* 新压缩列表长度（节点大小），两个 len 直接相加 */
    size_t zllength = first_len + second_len;

    /* Combined zl length should be limited within UINT16_MAX */
    /* 处理 zllen 大于等于 UINT16_MAX 的情况，大于等于的话我们无法用两个字节保存这个长度
       就用 UINT16_MAX 作为标识，标识压缩列表长度（节点数量）需要完整遍历压缩列表才能算出，O(1) 退化成 O(N) */
    zllength = zllength < UINT16_MAX ? zllength : UINT16_MAX;

    /* larger values can't be stored into ZIPLIST_BYTES */
    /* 断言判断 zlbytes 不会溢出，最多 4 个字节表示它 */
    assert(zlbytes < UINT32_MAX);

    /* Save offset positions before we start ripping memory apart. */
    /* 记录 first / second 两各自到尾节点的偏移量，用于后面合并 */
    size_t first_offset = intrev32ifbe(ZIPLIST_TAIL_OFFSET(*first));
    size_t second_offset = intrev32ifbe(ZIPLIST_TAIL_OFFSET(*second));

    /* Extend target to new zlbytes then append or prepend source. */
    /* realloc target 对 target 扩展到新 ziplist 长度 */
    target = zrealloc(target, zlbytes);

    if (append) {
        /* append == appending to target */
        /* Copy source after target (copying over original [END]):
         *   [TARGET - END, SOURCE - HEADER] */
        /* 在 target 之后复制 source，即扩展 first，然后在 first 后面追加 second

           这个 memcpy 就是将 source (second) 数据部分复制到 first 后面
           void *memcpy(void *str1, const void *str2, size_t n) 从存储区 str2 复制 n 个字节到存储区 str1
           从 source+ZIPLIST_HEADER_SIZE 复制 source_bytes-ZIPLIST_HEADER_SIZE 字节到 target+target_bytes-ZIPLIST_END_SIZE
           source + ZIPLIST_HEADER_SIZE: 源 ziplist 跳过 header 的大小，此时它指向节点数据部分（第一个节点），从这里开始复制
           source_bytes - ZIPLIST_HEADER_SIZE: 源 ziplist 大小减去 header 的大小，即源节点数据占用的字节数，要复制的字节数
           target + target_bytes - ZIPLIST_END_SIZE: 目标 ziplist 的末端，即将 source 节点数据复制到 target 的末端 */
        memcpy(target + target_bytes - ZIPLIST_END_SIZE,
               source + ZIPLIST_HEADER_SIZE,
               source_bytes - ZIPLIST_HEADER_SIZE);
    } else {
        /* !append == prepending to target */
        /* Move target *contents* exactly size of (source - [END]),
         * then copy source into vacated space (source - [END]):
         *   [SOURCE - END, TARGET - HEADER] */

        /* 在 target 之前复制 source，即扩展 second，然后将 second 原本的数据部分后移，空出前面的部分插入 first

           这个 memmove 就是把 target (second) 数据部分移动到后面，空出前面插入 first
           void *memmove(void *str1, const void *str2, size_t n) 从 str2 复制 n 个字符到 str1，但是在重叠内存块这方面，memmove() 是比 memcpy() 更安全的方法
           从 target+ZIPLIST_HEADER_SIZE 复制 target_bytes-ZIPLIST_HEADER_SIZE 字节到 target+source_bytes-ZIPLIST_END_SIZE
           target + ZIPLIST_HEADER_SIZE: 目标 ziplist 大小跳过 header 的大小，此时它指向节点数据部分（第一个节点），这这里开始复制移动
           target_bytes - ZIPLIST_HEADER_SIZE: 目标 ziplist 大小减去 header 的大小，即目标节点数据占用的字节数，要移动复制的字节数
           target + source_bytes - ZIPLIST_END_SIZE: 将 target 数据部分后移，空出前面的空间放 first */
        memmove(target + source_bytes - ZIPLIST_END_SIZE,
                target + ZIPLIST_HEADER_SIZE,
                target_bytes - ZIPLIST_HEADER_SIZE);

        /* 这个 memcpy 就是将 source (first) 数据部分复制到现在的 second 前面
           将 source (first) 数据部分复制到新压缩列表的最前面 */
        memcpy(target, source, source_bytes - ZIPLIST_END_SIZE);
    }

    /* Update header metadata. */
    /* 维护新 ziplist 的 header，即新字节长度和节点数量 */
    ZIPLIST_BYTES(target) = intrev32ifbe(zlbytes);
    ZIPLIST_LENGTH(target) = intrev16ifbe(zllength);
    /* New tail offset is:
     *   + N bytes of first ziplist
     *   - 1 byte for [END] of first ziplist
     *   + M bytes for the offset of the original tail of the second ziplist
     *   - J bytes for HEADER because second_offset keeps no header. */
    /* 到尾节点的偏移量为（别人的注释写的真好）：
       + N bytes 加上 first ziplist 的字节长度
       - 1 byte 减去 first ziplist ZLEND 一个字节
       + M bytes 加上 second ziplist 原来的 tail_offset
       - J bytes 减去 second header 部分的字节长度，因为合并后的新 ziplist 只会保留一个 header */
    ZIPLIST_TAIL_OFFSET(target) = intrev32ifbe(
                                   (first_bytes - ZIPLIST_END_SIZE) +
                                   (second_offset - ZIPLIST_HEADER_SIZE));

    /* __ziplistCascadeUpdate just fixes the prev length values until it finds a
     * correct prev length value (then it assumes the rest of the list is okay).
     * We tell CascadeUpdate to start at the first ziplist's tail element to fix
     * the merge seam. */
    /* 从 first_offset 即 first 的最后一个节点，即 second 的第一个节点，即合并后的第一个位置，开始检查是否需要连锁更新 */
    target = __ziplistCascadeUpdate(target, target+first_offset);

    /* Now free and NULL out what we didn't realloc */
    /* 合并是扩展了其中一个 ziplist，然后将另外个 ziplist 数据部分复制过来，此时释放掉那一个没扩展的 ziplist */
    if (append) {
        zfree(*second);
        *second = NULL;
        *first = target;
    } else {
        zfree(*first);
        *first = NULL;
        *second = target;
    }
    return target;
}

unsigned char *ziplistPush(unsigned char *zl, unsigned char *s, unsigned int slen, int where) {
    unsigned char *p;
    p = (where == ZIPLIST_HEAD) ? ZIPLIST_ENTRY_HEAD(zl) : ZIPLIST_ENTRY_END(zl);
    return __ziplistInsert(zl,p,s,slen);
}

/* Returns an offset to use for iterating with ziplistNext. When the given
 * index is negative, the list is traversed back to front. When the list
 * doesn't contain an element at the provided index, NULL is returned. */
unsigned char *ziplistIndex(unsigned char *zl, int index) {
    unsigned char *p;
    unsigned int prevlensize, prevlen = 0;
    size_t zlbytes = intrev32ifbe(ZIPLIST_BYTES(zl));
    if (index < 0) {
        index = (-index)-1;
        p = ZIPLIST_ENTRY_TAIL(zl);
        if (p[0] != ZIP_END) {
            /* No need for "safe" check: when going backwards, we know the header
             * we're parsing is in the range, we just need to assert (below) that
             * the size we take doesn't cause p to go outside the allocation. */
            ZIP_DECODE_PREVLENSIZE(p, prevlensize);
            assert(p + prevlensize < zl + zlbytes - ZIPLIST_END_SIZE);
            ZIP_DECODE_PREVLEN(p, prevlensize, prevlen);
            while (prevlen > 0 && index--) {
                p -= prevlen;
                assert(p >= zl + ZIPLIST_HEADER_SIZE && p < zl + zlbytes - ZIPLIST_END_SIZE);
                ZIP_DECODE_PREVLEN(p, prevlensize, prevlen);
            }
        }
    } else {
        p = ZIPLIST_ENTRY_HEAD(zl);
        while (index--) {
            /* Use the "safe" length: When we go forward, we need to be careful
             * not to decode an entry header if it's past the ziplist allocation. */
            p += zipRawEntryLengthSafe(zl, zlbytes, p);
            if (p[0] == ZIP_END)
                break;
        }
    }
    if (p[0] == ZIP_END || index > 0)
        return NULL;
    zipAssertValidEntry(zl, zlbytes, p);
    return p;
}

/* Return pointer to next entry in ziplist.
 *
 * zl is the pointer to the ziplist
 * p is the pointer to the current element
 *
 * The element after 'p' is returned, otherwise NULL if we are at the end. */
/* 返回 p 后面的一个节点 */
unsigned char *ziplistNext(unsigned char *zl, unsigned char *p) {
    ((void) zl);
    size_t zlbytes = intrev32ifbe(ZIPLIST_BYTES(zl));

    /* "p" could be equal to ZIP_END, caused by ziplistDelete,
     * and we should return NULL. Otherwise, we should return NULL
     * when the *next* element is ZIP_END (there is no next entry). */
    if (p[0] == ZIP_END) {
        /* 如果 p 就指向 ZIP_END 返回 NULL */
        return NULL;
    }

    /* 在 p 当前节点的基础上，往后偏移 p 节点的字节长度，即指向下一个节点
       如果之后的 p 指向 ZIP_END 说明已经达到 ziplist 尾部，没有下一个节点 */
    p += zipRawEntryLength(p);
    if (p[0] == ZIP_END) {
        return NULL;
    }

    zipAssertValidEntry(zl, zlbytes, p);
    return p;
}

/* Return pointer to previous entry in ziplist. */
/* 返回 p 前面的一个节点 */
unsigned char *ziplistPrev(unsigned char *zl, unsigned char *p) {
    unsigned int prevlensize, prevlen = 0;

    /* Iterating backwards from ZIP_END should return the tail. When "p" is
     * equal to the first element of the list, we're already at the head,
     * and should return NULL. */
    if (p[0] == ZIP_END) {
        /* 如果 p 就指向 ZIP_END 说明它的前一个节点是 ziplist 的尾节点 */
        p = ZIPLIST_ENTRY_TAIL(zl);
        /* 尾节点通过偏移量进行定位，继续判断一下 ZIP_END 进行返回 */
        return (p[0] == ZIP_END) ? NULL : p;
    } else if (p == ZIPLIST_ENTRY_HEAD(zl)) {
        /* p == (zl)+ZIPLIST_HEADER_SIZE 说明是 ziplist 的第一个节点，前节点为 NULL */
        return NULL;
    } else {
        /* 解析出 prevlen 往前进行偏移，然后解析出对应节点 */
        ZIP_DECODE_PREVLEN(p, prevlensize, prevlen);
        assert(prevlen > 0);
        p-=prevlen;
        size_t zlbytes = intrev32ifbe(ZIPLIST_BYTES(zl));
        zipAssertValidEntry(zl, zlbytes, p);
        return p;
    }
}

/* Get entry pointed to by 'p' and store in either '*sstr' or 'sval' depending
 * on the encoding of the entry. '*sstr' is always set to NULL to be able
 * to find out whether the string pointer or the integer value was set.
 * Return 0 if 'p' points to the end of the ziplist, 1 otherwise. */
unsigned int ziplistGet(unsigned char *p, unsigned char **sstr, unsigned int *slen, long long *sval) {
    zlentry entry;
    if (p == NULL || p[0] == ZIP_END) return 0;
    if (sstr) *sstr = NULL;

    zipEntry(p, &entry); /* no need for "safe" variant since the input pointer was validated by the function that returned it. */
    if (ZIP_IS_STR(entry.encoding)) {
        if (sstr) {
            *slen = entry.len;
            *sstr = p+entry.headersize;
        }
    } else {
        if (sval) {
            *sval = zipLoadInteger(p+entry.headersize,entry.encoding);
        }
    }
    return 1;
}

/* Insert an entry at "p". */
unsigned char *ziplistInsert(unsigned char *zl, unsigned char *p, unsigned char *s, unsigned int slen) {
    return __ziplistInsert(zl,p,s,slen);
}

/* Delete a single entry from the ziplist, pointed to by *p.
 * Also update *p in place, to be able to iterate over the
 * ziplist, while deleting entries. */
unsigned char *ziplistDelete(unsigned char *zl, unsigned char **p) {
    size_t offset = *p-zl;
    zl = __ziplistDelete(zl,*p,1);

    /* Store pointer to current element in p, because ziplistDelete will
     * do a realloc which might result in a different "zl"-pointer.
     * When the delete direction is back to front, we might delete the last
     * entry and end up with "p" pointing to ZIP_END, so check this. */
    *p = zl+offset;
    return zl;
}

/* Delete a range of entries from the ziplist. */
unsigned char *ziplistDeleteRange(unsigned char *zl, int index, unsigned int num) {
    unsigned char *p = ziplistIndex(zl,index);
    return (p == NULL) ? zl : __ziplistDelete(zl,p,num);
}

/* Replaces the entry at p. This is equivalent to a delete and an insert,
 * but avoids some overhead when replacing a value of the same size. */
unsigned char *ziplistReplace(unsigned char *zl, unsigned char *p, unsigned char *s, unsigned int slen) {

    /* get metadata of the current entry */
    zlentry entry;
    zipEntry(p, &entry);

    /* compute length of entry to store, excluding prevlen */
    unsigned int reqlen;
    unsigned char encoding = 0;
    long long value = 123456789; /* initialized to avoid warning. */
    if (zipTryEncoding(s,slen,&value,&encoding)) {
        reqlen = zipIntSize(encoding); /* encoding is set */
    } else {
        reqlen = slen; /* encoding == 0 */
    }
    reqlen += zipStoreEntryEncoding(NULL,encoding,slen);

    if (reqlen == entry.lensize + entry.len) {
        /* Simply overwrite the element. */
        p += entry.prevrawlensize;
        p += zipStoreEntryEncoding(p,encoding,slen);
        if (ZIP_IS_STR(encoding)) {
            memcpy(p,s,slen);
        } else {
            zipSaveInteger(p,value,encoding);
        }
    } else {
        /* Fallback. */
        zl = ziplistDelete(zl,&p);
        zl = ziplistInsert(zl,p,s,slen);
    }
    return zl;
}

/* Compare entry pointer to by 'p' with 'sstr' of length 'slen'. */
/* Return 1 if equal. */
unsigned int ziplistCompare(unsigned char *p, unsigned char *sstr, unsigned int slen) {
    zlentry entry;
    unsigned char sencoding;
    long long zval, sval;
    if (p[0] == ZIP_END) return 0;

    zipEntry(p, &entry); /* no need for "safe" variant since the input pointer was validated by the function that returned it. */
    if (ZIP_IS_STR(entry.encoding)) {
        /* Raw compare */
        if (entry.len == slen) {
            return memcmp(p+entry.headersize,sstr,slen) == 0;
        } else {
            return 0;
        }
    } else {
        /* Try to compare encoded values. Don't compare encoding because
         * different implementations may encoded integers differently. */
        if (zipTryEncoding(sstr,slen,&sval,&sencoding)) {
          zval = zipLoadInteger(p+entry.headersize,entry.encoding);
          return zval == sval;
        }
    }
    return 0;
}

/* Find pointer to the entry equal to the specified entry. Skip 'skip' entries
 * between every comparison. Returns NULL when the field could not be found. */
unsigned char *ziplistFind(unsigned char *zl, unsigned char *p, unsigned char *vstr, unsigned int vlen, unsigned int skip) {
    int skipcnt = 0;
    unsigned char vencoding = 0;
    long long vll = 0;
    size_t zlbytes = ziplistBlobLen(zl);

    while (p[0] != ZIP_END) {
        struct zlentry e;
        unsigned char *q;

        assert(zipEntrySafe(zl, zlbytes, p, &e, 1));
        q = p + e.prevrawlensize + e.lensize;

        if (skipcnt == 0) {
            /* Compare current entry with specified entry */
            if (ZIP_IS_STR(e.encoding)) {
                if (e.len == vlen && memcmp(q, vstr, vlen) == 0) {
                    return p;
                }
            } else {
                /* Find out if the searched field can be encoded. Note that
                 * we do it only the first time, once done vencoding is set
                 * to non-zero and vll is set to the integer value. */
                if (vencoding == 0) {
                    if (!zipTryEncoding(vstr, vlen, &vll, &vencoding)) {
                        /* If the entry can't be encoded we set it to
                         * UCHAR_MAX so that we don't retry again the next
                         * time. */
                        vencoding = UCHAR_MAX;
                    }
                    /* Must be non-zero by now */
                    assert(vencoding);
                }

                /* Compare current entry with specified entry, do it only
                 * if vencoding != UCHAR_MAX because if there is no encoding
                 * possible for the field it can't be a valid integer. */
                if (vencoding != UCHAR_MAX) {
                    long long ll = zipLoadInteger(q, e.encoding);
                    if (ll == vll) {
                        return p;
                    }
                }
            }

            /* Reset skip count */
            skipcnt = skip;
        } else {
            /* Skip entry */
            skipcnt--;
        }

        /* Move to next entry */
        p = q + e.len;
    }

    return NULL;
}

/* Return length of ziplist. */
/* 获取压缩列表节点数量
 * 如果 len < UINT16_MAX 可以 O(1) 时间复杂度直接获取
 * 否则只能完整遍历整个 ziplist 来获取节点数量，时间复杂度是 O(N)
 * */
unsigned int ziplistLen(unsigned char *zl) {
    unsigned int len = 0;
    if (intrev16ifbe(ZIPLIST_LENGTH(zl)) < UINT16_MAX) {
        len = intrev16ifbe(ZIPLIST_LENGTH(zl));
    } else {
        unsigned char *p = zl+ZIPLIST_HEADER_SIZE;
        size_t zlbytes = intrev32ifbe(ZIPLIST_BYTES(zl));
        /* 遍历整个 ziplist 获取长度 */
        while (*p != ZIP_END) {
            p += zipRawEntryLengthSafe(zl, zlbytes, p);
            len++;
        }

        /* Re-store length if small enough */
        /* 如果经过删除等操作，长度 < UINT16_MAX 可以重新存储实际长度值 */
        if (len < UINT16_MAX) ZIPLIST_LENGTH(zl) = intrev16ifbe(len);
    }
    return len;
}

/* Return ziplist blob size in bytes. */
size_t ziplistBlobLen(unsigned char *zl) {
    return intrev32ifbe(ZIPLIST_BYTES(zl));
}

void ziplistRepr(unsigned char *zl) {
    unsigned char *p;
    int index = 0;
    zlentry entry;
    size_t zlbytes = ziplistBlobLen(zl);

    printf(
        "{total bytes %u} "
        "{num entries %u}\n"
        "{tail offset %u}\n",
        intrev32ifbe(ZIPLIST_BYTES(zl)),
        intrev16ifbe(ZIPLIST_LENGTH(zl)),
        intrev32ifbe(ZIPLIST_TAIL_OFFSET(zl)));
    p = ZIPLIST_ENTRY_HEAD(zl);
    while(*p != ZIP_END) {
        assert(zipEntrySafe(zl, zlbytes, p, &entry, 1));
        printf(
            "{\n"
                "\taddr 0x%08lx,\n"
                "\tindex %2d,\n"
                "\toffset %5lu,\n"
                "\thdr+entry len: %5u,\n"
                "\thdr len%2u,\n"
                "\tprevrawlen: %5u,\n"
                "\tprevrawlensize: %2u,\n"
                "\tpayload %5u\n",
            (long unsigned)p,
            index,
            (unsigned long) (p-zl),
            entry.headersize+entry.len,
            entry.headersize,
            entry.prevrawlen,
            entry.prevrawlensize,
            entry.len);
        printf("\tbytes: ");
        for (unsigned int i = 0; i < entry.headersize+entry.len; i++) {
            printf("%02x|",p[i]);
        }
        printf("\n");
        p += entry.headersize;
        if (ZIP_IS_STR(entry.encoding)) {
            printf("\t[str]");
            if (entry.len > 40) {
                if (fwrite(p,40,1,stdout) == 0) perror("fwrite");
                printf("...");
            } else {
                if (entry.len &&
                    fwrite(p,entry.len,1,stdout) == 0) perror("fwrite");
            }
        } else {
            printf("\t[int]%lld", (long long) zipLoadInteger(p,entry.encoding));
        }
        printf("\n}\n");
        p += entry.len;
        index++;
    }
    printf("{end}\n\n");
}

/* Validate the integrity of the data structure.
 * when `deep` is 0, only the integrity of the header is validated.
 * when `deep` is 1, we scan all the entries one by one. */
int ziplistValidateIntegrity(unsigned char *zl, size_t size, int deep,
    ziplistValidateEntryCB entry_cb, void *cb_userdata) {
    /* check that we can actually read the header. (and ZIP_END) */
    if (size < ZIPLIST_HEADER_SIZE + ZIPLIST_END_SIZE)
        return 0;

    /* check that the encoded size in the header must match the allocated size. */
    size_t bytes = intrev32ifbe(ZIPLIST_BYTES(zl));
    if (bytes != size)
        return 0;

    /* the last byte must be the terminator. */
    if (zl[size - ZIPLIST_END_SIZE] != ZIP_END)
        return 0;

    /* make sure the tail offset isn't reaching outside the allocation. */
    if (intrev32ifbe(ZIPLIST_TAIL_OFFSET(zl)) > size - ZIPLIST_END_SIZE)
        return 0;

    if (!deep)
        return 1;

    unsigned int count = 0;
    unsigned int header_count = intrev16ifbe(ZIPLIST_LENGTH(zl));
    unsigned char *p = ZIPLIST_ENTRY_HEAD(zl);
    unsigned char *prev = NULL;
    size_t prev_raw_size = 0;
    while(*p != ZIP_END) {
        struct zlentry e;
        /* Decode the entry headers and fail if invalid or reaches outside the allocation */
        if (!zipEntrySafe(zl, size, p, &e, 1))
            return 0;

        /* Make sure the record stating the prev entry size is correct. */
        if (e.prevrawlen != prev_raw_size)
            return 0;

        /* Optionally let the caller validate the entry too. */
        if (entry_cb && !entry_cb(p, header_count, cb_userdata))
            return 0;

        /* Move to the next entry */
        prev_raw_size = e.headersize + e.len;
        prev = p;
        p += e.headersize + e.len;
        count++;
    }

    /* Make sure 'p' really does point to the end of the ziplist. */
    if (p != zl + bytes - ZIPLIST_END_SIZE)
        return 0;

    /* Make sure the <zltail> entry really do point to the start of the last entry. */
    if (prev != NULL && prev != ZIPLIST_ENTRY_TAIL(zl))
        return 0;

    /* Check that the count in the header is correct */
    if (header_count != UINT16_MAX && count != header_count)
        return 0;

    return 1;
}

/* Randomly select a pair of key and value.
 * total_count is a pre-computed length/2 of the ziplist (to avoid calls to ziplistLen)
 * 'key' and 'val' are used to store the result key value pair.
 * 'val' can be NULL if the value is not needed. */
void ziplistRandomPair(unsigned char *zl, unsigned long total_count, ziplistEntry *key, ziplistEntry *val) {
    int ret;
    unsigned char *p;

    /* Avoid div by zero on corrupt ziplist */
    assert(total_count);

    /* Generate even numbers, because ziplist saved K-V pair */
    int r = (rand() % total_count) * 2;
    p = ziplistIndex(zl, r);
    ret = ziplistGet(p, &key->sval, &key->slen, &key->lval);
    assert(ret != 0);

    if (!val)
        return;
    p = ziplistNext(zl, p);
    ret = ziplistGet(p, &val->sval, &val->slen, &val->lval);
    assert(ret != 0);
}

/* int compare for qsort */
int uintCompare(const void *a, const void *b) {
    return (*(unsigned int *) a - *(unsigned int *) b);
}

/* Helper method to store a string into from val or lval into dest */
static inline void ziplistSaveValue(unsigned char *val, unsigned int len, long long lval, ziplistEntry *dest) {
    dest->sval = val;
    dest->slen = len;
    dest->lval = lval;
}

/* Randomly select count of key value pairs and store into 'keys' and
 * 'vals' args. The order of the picked entries is random, and the selections
 * are non-unique (repetitions are possible).
 * The 'vals' arg can be NULL in which case we skip these. */
void ziplistRandomPairs(unsigned char *zl, unsigned int count, ziplistEntry *keys, ziplistEntry *vals) {
    unsigned char *p, *key, *value;
    unsigned int klen = 0, vlen = 0;
    long long klval = 0, vlval = 0;

    /* Notice: the index member must be first due to the use in uintCompare */
    typedef struct {
        unsigned int index;
        unsigned int order;
    } rand_pick;
    rand_pick *picks = zmalloc(sizeof(rand_pick)*count);
    unsigned int total_size = ziplistLen(zl)/2;

    /* Avoid div by zero on corrupt ziplist */
    assert(total_size);

    /* create a pool of random indexes (some may be duplicate). */
    for (unsigned int i = 0; i < count; i++) {
        picks[i].index = (rand() % total_size) * 2; /* Generate even indexes */
        /* keep track of the order we picked them */
        picks[i].order = i;
    }

    /* sort by indexes. */
    qsort(picks, count, sizeof(rand_pick), uintCompare);

    /* fetch the elements form the ziplist into a output array respecting the original order. */
    unsigned int zipindex = picks[0].index, pickindex = 0;
    p = ziplistIndex(zl, zipindex);
    while (ziplistGet(p, &key, &klen, &klval) && pickindex < count) {
        p = ziplistNext(zl, p);
        assert(ziplistGet(p, &value, &vlen, &vlval));
        while (pickindex < count && zipindex == picks[pickindex].index) {
            int storeorder = picks[pickindex].order;
            ziplistSaveValue(key, klen, klval, &keys[storeorder]);
            if (vals)
                ziplistSaveValue(value, vlen, vlval, &vals[storeorder]);
             pickindex++;
        }
        zipindex += 2;
        p = ziplistNext(zl, p);
    }

    zfree(picks);
}

/* Randomly select count of key value pairs and store into 'keys' and
 * 'vals' args. The selections are unique (no repetitions), and the order of
 * the picked entries is NOT-random.
 * The 'vals' arg can be NULL in which case we skip these.
 * The return value is the number of items picked which can be lower than the
 * requested count if the ziplist doesn't hold enough pairs. */
unsigned int ziplistRandomPairsUnique(unsigned char *zl, unsigned int count, ziplistEntry *keys, ziplistEntry *vals) {
    unsigned char *p, *key;
    unsigned int klen = 0;
    long long klval = 0;
    unsigned int total_size = ziplistLen(zl)/2;
    unsigned int index = 0;
    if (count > total_size)
        count = total_size;

    /* To only iterate once, every time we try to pick a member, the probability
     * we pick it is the quotient of the count left we want to pick and the
     * count still we haven't visited in the dict, this way, we could make every
     * member be equally picked.*/
    p = ziplistIndex(zl, 0);
    unsigned int picked = 0, remaining = count;
    while (picked < count && p) {
        double randomDouble = ((double)rand()) / RAND_MAX;
        double threshold = ((double)remaining) / (total_size - index);
        if (randomDouble <= threshold) {
            assert(ziplistGet(p, &key, &klen, &klval));
            ziplistSaveValue(key, klen, klval, &keys[picked]);
            p = ziplistNext(zl, p);
            assert(p);
            if (vals) {
                assert(ziplistGet(p, &key, &klen, &klval));
                ziplistSaveValue(key, klen, klval, &vals[picked]);
            }
            remaining--;
            picked++;
        } else {
            p = ziplistNext(zl, p);
            assert(p);
        }
        p = ziplistNext(zl, p);
        index++;
    }
    return picked;
}

#ifdef REDIS_TEST
#include <sys/time.h>
#include "adlist.h"
#include "sds.h"
#include "testhelp.h"

#define debug(f, ...) { if (DEBUG) printf(f, __VA_ARGS__); }

static unsigned char *createList() {
    unsigned char *zl = ziplistNew();
    zl = ziplistPush(zl, (unsigned char*)"foo", 3, ZIPLIST_TAIL);
    zl = ziplistPush(zl, (unsigned char*)"quux", 4, ZIPLIST_TAIL);
    zl = ziplistPush(zl, (unsigned char*)"hello", 5, ZIPLIST_HEAD);
    zl = ziplistPush(zl, (unsigned char*)"1024", 4, ZIPLIST_TAIL);
    return zl;
}

static unsigned char *createIntList() {
    unsigned char *zl = ziplistNew();
    char buf[32];

    sprintf(buf, "100");
    zl = ziplistPush(zl, (unsigned char*)buf, strlen(buf), ZIPLIST_TAIL);
    sprintf(buf, "128000");
    zl = ziplistPush(zl, (unsigned char*)buf, strlen(buf), ZIPLIST_TAIL);
    sprintf(buf, "-100");
    zl = ziplistPush(zl, (unsigned char*)buf, strlen(buf), ZIPLIST_HEAD);
    sprintf(buf, "4294967296");
    zl = ziplistPush(zl, (unsigned char*)buf, strlen(buf), ZIPLIST_HEAD);
    sprintf(buf, "non integer");
    zl = ziplistPush(zl, (unsigned char*)buf, strlen(buf), ZIPLIST_TAIL);
    sprintf(buf, "much much longer non integer");
    zl = ziplistPush(zl, (unsigned char*)buf, strlen(buf), ZIPLIST_TAIL);
    return zl;
}

static long long usec(void) {
    struct timeval tv;
    gettimeofday(&tv,NULL);
    return (((long long)tv.tv_sec)*1000000)+tv.tv_usec;
}

static void stress(int pos, int num, int maxsize, int dnum) {
    int i,j,k;
    unsigned char *zl;
    char posstr[2][5] = { "HEAD", "TAIL" };
    long long start;
    for (i = 0; i < maxsize; i+=dnum) {
        zl = ziplistNew();
        for (j = 0; j < i; j++) {
            zl = ziplistPush(zl,(unsigned char*)"quux",4,ZIPLIST_TAIL);
        }

        /* Do num times a push+pop from pos */
        start = usec();
        for (k = 0; k < num; k++) {
            zl = ziplistPush(zl,(unsigned char*)"quux",4,pos);
            zl = ziplistDeleteRange(zl,0,1);
        }
        printf("List size: %8d, bytes: %8d, %dx push+pop (%s): %6lld usec\n",
            i,intrev32ifbe(ZIPLIST_BYTES(zl)),num,posstr[pos],usec()-start);
        zfree(zl);
    }
}

static unsigned char *pop(unsigned char *zl, int where) {
    unsigned char *p, *vstr;
    unsigned int vlen;
    long long vlong;

    p = ziplistIndex(zl,where == ZIPLIST_HEAD ? 0 : -1);
    if (ziplistGet(p,&vstr,&vlen,&vlong)) {
        if (where == ZIPLIST_HEAD)
            printf("Pop head: ");
        else
            printf("Pop tail: ");

        if (vstr) {
            if (vlen && fwrite(vstr,vlen,1,stdout) == 0) perror("fwrite");
        }
        else {
            printf("%lld", vlong);
        }

        printf("\n");
        return ziplistDelete(zl,&p);
    } else {
        printf("ERROR: Could not pop\n");
        exit(1);
    }
}

static int randstring(char *target, unsigned int min, unsigned int max) {
    int p = 0;
    int len = min+rand()%(max-min+1);
    int minval, maxval;
    switch(rand() % 3) {
    case 0:
        minval = 0;
        maxval = 255;
    break;
    case 1:
        minval = 48;
        maxval = 122;
    break;
    case 2:
        minval = 48;
        maxval = 52;
    break;
    default:
        assert(NULL);
    }

    while(p < len)
        target[p++] = minval+rand()%(maxval-minval+1);
    return len;
}

static void verify(unsigned char *zl, zlentry *e) {
    int len = ziplistLen(zl);
    zlentry _e;

    ZIPLIST_ENTRY_ZERO(&_e);

    for (int i = 0; i < len; i++) {
        memset(&e[i], 0, sizeof(zlentry));
        zipEntry(ziplistIndex(zl, i), &e[i]);

        memset(&_e, 0, sizeof(zlentry));
        zipEntry(ziplistIndex(zl, -len+i), &_e);

        assert(memcmp(&e[i], &_e, sizeof(zlentry)) == 0);
    }
}

static unsigned char *insertHelper(unsigned char *zl, char ch, size_t len, unsigned char *pos) {
    assert(len <= ZIP_BIG_PREVLEN);
    unsigned char data[ZIP_BIG_PREVLEN] = {0};
    memset(data, ch, len);
    return ziplistInsert(zl, pos, data, len);
}

static int compareHelper(unsigned char *zl, char ch, size_t len, int index) {
    assert(len <= ZIP_BIG_PREVLEN);
    unsigned char data[ZIP_BIG_PREVLEN] = {0};
    memset(data, ch, len);
    unsigned char *p = ziplistIndex(zl, index);
    assert(p != NULL);
    return ziplistCompare(p, data, len);
}

static size_t strEntryBytesSmall(size_t slen) {
    return slen + zipStorePrevEntryLength(NULL, 0) + zipStoreEntryEncoding(NULL, 0, slen);
}

static size_t strEntryBytesLarge(size_t slen) {
    return slen + zipStorePrevEntryLength(NULL, ZIP_BIG_PREVLEN) + zipStoreEntryEncoding(NULL, 0, slen);
}

/* ./redis-server test ziplist <randomseed> */
int ziplistTest(int argc, char **argv, int flags) {
    int accurate = (flags & REDIS_TEST_ACCURATE);
    unsigned char *zl, *p;
    unsigned char *entry;
    unsigned int elen;
    long long value;
    int iteration;

    /* If an argument is given, use it as the random seed. */
    if (argc >= 4)
        srand(atoi(argv[3]));

    zl = createIntList();
    ziplistRepr(zl);

    zfree(zl);

    zl = createList();
    ziplistRepr(zl);

    zl = pop(zl,ZIPLIST_TAIL);
    ziplistRepr(zl);

    zl = pop(zl,ZIPLIST_HEAD);
    ziplistRepr(zl);

    zl = pop(zl,ZIPLIST_TAIL);
    ziplistRepr(zl);

    zl = pop(zl,ZIPLIST_TAIL);
    ziplistRepr(zl);

    zfree(zl);

    printf("Get element at index 3:\n");
    {
        zl = createList();
        p = ziplistIndex(zl, 3);
        if (!ziplistGet(p, &entry, &elen, &value)) {
            printf("ERROR: Could not access index 3\n");
            return 1;
        }
        if (entry) {
            if (elen && fwrite(entry,elen,1,stdout) == 0) perror("fwrite");
            printf("\n");
        } else {
            printf("%lld\n", value);
        }
        printf("\n");
        zfree(zl);
    }

    printf("Get element at index 4 (out of range):\n");
    {
        zl = createList();
        p = ziplistIndex(zl, 4);
        if (p == NULL) {
            printf("No entry\n");
        } else {
            printf("ERROR: Out of range index should return NULL, returned offset: %ld\n", (long)(p-zl));
            return 1;
        }
        printf("\n");
        zfree(zl);
    }

    printf("Get element at index -1 (last element):\n");
    {
        zl = createList();
        p = ziplistIndex(zl, -1);
        if (!ziplistGet(p, &entry, &elen, &value)) {
            printf("ERROR: Could not access index -1\n");
            return 1;
        }
        if (entry) {
            if (elen && fwrite(entry,elen,1,stdout) == 0) perror("fwrite");
            printf("\n");
        } else {
            printf("%lld\n", value);
        }
        printf("\n");
        zfree(zl);
    }

    printf("Get element at index -4 (first element):\n");
    {
        zl = createList();
        p = ziplistIndex(zl, -4);
        if (!ziplistGet(p, &entry, &elen, &value)) {
            printf("ERROR: Could not access index -4\n");
            return 1;
        }
        if (entry) {
            if (elen && fwrite(entry,elen,1,stdout) == 0) perror("fwrite");
            printf("\n");
        } else {
            printf("%lld\n", value);
        }
        printf("\n");
        zfree(zl);
    }

    printf("Get element at index -5 (reverse out of range):\n");
    {
        zl = createList();
        p = ziplistIndex(zl, -5);
        if (p == NULL) {
            printf("No entry\n");
        } else {
            printf("ERROR: Out of range index should return NULL, returned offset: %ld\n", (long)(p-zl));
            return 1;
        }
        printf("\n");
        zfree(zl);
    }

    printf("Iterate list from 0 to end:\n");
    {
        zl = createList();
        p = ziplistIndex(zl, 0);
        while (ziplistGet(p, &entry, &elen, &value)) {
            printf("Entry: ");
            if (entry) {
                if (elen && fwrite(entry,elen,1,stdout) == 0) perror("fwrite");
            } else {
                printf("%lld", value);
            }
            p = ziplistNext(zl,p);
            printf("\n");
        }
        printf("\n");
        zfree(zl);
    }

    printf("Iterate list from 1 to end:\n");
    {
        zl = createList();
        p = ziplistIndex(zl, 1);
        while (ziplistGet(p, &entry, &elen, &value)) {
            printf("Entry: ");
            if (entry) {
                if (elen && fwrite(entry,elen,1,stdout) == 0) perror("fwrite");
            } else {
                printf("%lld", value);
            }
            p = ziplistNext(zl,p);
            printf("\n");
        }
        printf("\n");
        zfree(zl);
    }

    printf("Iterate list from 2 to end:\n");
    {
        zl = createList();
        p = ziplistIndex(zl, 2);
        while (ziplistGet(p, &entry, &elen, &value)) {
            printf("Entry: ");
            if (entry) {
                if (elen && fwrite(entry,elen,1,stdout) == 0) perror("fwrite");
            } else {
                printf("%lld", value);
            }
            p = ziplistNext(zl,p);
            printf("\n");
        }
        printf("\n");
        zfree(zl);
    }

    printf("Iterate starting out of range:\n");
    {
        zl = createList();
        p = ziplistIndex(zl, 4);
        if (!ziplistGet(p, &entry, &elen, &value)) {
            printf("No entry\n");
        } else {
            printf("ERROR\n");
        }
        printf("\n");
        zfree(zl);
    }

    printf("Iterate from back to front:\n");
    {
        zl = createList();
        p = ziplistIndex(zl, -1);
        while (ziplistGet(p, &entry, &elen, &value)) {
            printf("Entry: ");
            if (entry) {
                if (elen && fwrite(entry,elen,1,stdout) == 0) perror("fwrite");
            } else {
                printf("%lld", value);
            }
            p = ziplistPrev(zl,p);
            printf("\n");
        }
        printf("\n");
        zfree(zl);
    }

    printf("Iterate from back to front, deleting all items:\n");
    {
        zl = createList();
        p = ziplistIndex(zl, -1);
        while (ziplistGet(p, &entry, &elen, &value)) {
            printf("Entry: ");
            if (entry) {
                if (elen && fwrite(entry,elen,1,stdout) == 0) perror("fwrite");
            } else {
                printf("%lld", value);
            }
            zl = ziplistDelete(zl,&p);
            p = ziplistPrev(zl,p);
            printf("\n");
        }
        printf("\n");
        zfree(zl);
    }

    printf("Delete inclusive range 0,0:\n");
    {
        zl = createList();
        zl = ziplistDeleteRange(zl, 0, 1);
        ziplistRepr(zl);
        zfree(zl);
    }

    printf("Delete inclusive range 0,1:\n");
    {
        zl = createList();
        zl = ziplistDeleteRange(zl, 0, 2);
        ziplistRepr(zl);
        zfree(zl);
    }

    printf("Delete inclusive range 1,2:\n");
    {
        zl = createList();
        zl = ziplistDeleteRange(zl, 1, 2);
        ziplistRepr(zl);
        zfree(zl);
    }

    printf("Delete with start index out of range:\n");
    {
        zl = createList();
        zl = ziplistDeleteRange(zl, 5, 1);
        ziplistRepr(zl);
        zfree(zl);
    }

    printf("Delete with num overflow:\n");
    {
        zl = createList();
        zl = ziplistDeleteRange(zl, 1, 5);
        ziplistRepr(zl);
        zfree(zl);
    }

    printf("Delete foo while iterating:\n");
    {
        zl = createList();
        p = ziplistIndex(zl,0);
        while (ziplistGet(p,&entry,&elen,&value)) {
            if (entry && strncmp("foo",(char*)entry,elen) == 0) {
                printf("Delete foo\n");
                zl = ziplistDelete(zl,&p);
            } else {
                printf("Entry: ");
                if (entry) {
                    if (elen && fwrite(entry,elen,1,stdout) == 0)
                        perror("fwrite");
                } else {
                    printf("%lld",value);
                }
                p = ziplistNext(zl,p);
                printf("\n");
            }
        }
        printf("\n");
        ziplistRepr(zl);
        zfree(zl);
    }

    printf("Replace with same size:\n");
    {
        zl = createList(); /* "hello", "foo", "quux", "1024" */
        unsigned char *orig_zl = zl;
        p = ziplistIndex(zl, 0);
        zl = ziplistReplace(zl, p, (unsigned char*)"zoink", 5);
        p = ziplistIndex(zl, 3);
        zl = ziplistReplace(zl, p, (unsigned char*)"yy", 2);
        p = ziplistIndex(zl, 1);
        zl = ziplistReplace(zl, p, (unsigned char*)"65536", 5);
        p = ziplistIndex(zl, 0);
        assert(!memcmp((char*)p,
                       "\x00\x05zoink"
                       "\x07\xf0\x00\x00\x01" /* 65536 as int24 */
                       "\x05\x04quux" "\x06\x02yy" "\xff",
                       23));
        assert(zl == orig_zl); /* no reallocations have happened */
        zfree(zl);
        printf("SUCCESS\n\n");
    }

    printf("Replace with different size:\n");
    {
        zl = createList(); /* "hello", "foo", "quux", "1024" */
        p = ziplistIndex(zl, 1);
        zl = ziplistReplace(zl, p, (unsigned char*)"squirrel", 8);
        p = ziplistIndex(zl, 0);
        assert(!strncmp((char*)p,
                        "\x00\x05hello" "\x07\x08squirrel" "\x0a\x04quux"
                        "\x06\xc0\x00\x04" "\xff",
                        28));
        zfree(zl);
        printf("SUCCESS\n\n");
    }

    printf("Regression test for >255 byte strings:\n");
    {
        char v1[257] = {0}, v2[257] = {0};
        memset(v1,'x',256);
        memset(v2,'y',256);
        zl = ziplistNew();
        zl = ziplistPush(zl,(unsigned char*)v1,strlen(v1),ZIPLIST_TAIL);
        zl = ziplistPush(zl,(unsigned char*)v2,strlen(v2),ZIPLIST_TAIL);

        /* Pop values again and compare their value. */
        p = ziplistIndex(zl,0);
        assert(ziplistGet(p,&entry,&elen,&value));
        assert(strncmp(v1,(char*)entry,elen) == 0);
        p = ziplistIndex(zl,1);
        assert(ziplistGet(p,&entry,&elen,&value));
        assert(strncmp(v2,(char*)entry,elen) == 0);
        printf("SUCCESS\n\n");
        zfree(zl);
    }

    printf("Regression test deleting next to last entries:\n");
    {
        char v[3][257] = {{0}};
        zlentry e[3] = {{.prevrawlensize = 0, .prevrawlen = 0, .lensize = 0,
                         .len = 0, .headersize = 0, .encoding = 0, .p = NULL}};
        size_t i;

        for (i = 0; i < (sizeof(v)/sizeof(v[0])); i++) {
            memset(v[i], 'a' + i, sizeof(v[0]));
        }

        v[0][256] = '\0';
        v[1][  1] = '\0';
        v[2][256] = '\0';

        zl = ziplistNew();
        for (i = 0; i < (sizeof(v)/sizeof(v[0])); i++) {
            zl = ziplistPush(zl, (unsigned char *) v[i], strlen(v[i]), ZIPLIST_TAIL);
        }

        verify(zl, e);

        assert(e[0].prevrawlensize == 1);
        assert(e[1].prevrawlensize == 5);
        assert(e[2].prevrawlensize == 1);

        /* Deleting entry 1 will increase `prevrawlensize` for entry 2 */
        unsigned char *p = e[1].p;
        zl = ziplistDelete(zl, &p);

        verify(zl, e);

        assert(e[0].prevrawlensize == 1);
        assert(e[1].prevrawlensize == 5);

        printf("SUCCESS\n\n");
        zfree(zl);
    }

    printf("Create long list and check indices:\n");
    {
        unsigned long long start = usec();
        zl = ziplistNew();
        char buf[32];
        int i,len;
        for (i = 0; i < 1000; i++) {
            len = sprintf(buf,"%d",i);
            zl = ziplistPush(zl,(unsigned char*)buf,len,ZIPLIST_TAIL);
        }
        for (i = 0; i < 1000; i++) {
            p = ziplistIndex(zl,i);
            assert(ziplistGet(p,NULL,NULL,&value));
            assert(i == value);

            p = ziplistIndex(zl,-i-1);
            assert(ziplistGet(p,NULL,NULL,&value));
            assert(999-i == value);
        }
        printf("SUCCESS. usec=%lld\n\n", usec()-start);
        zfree(zl);
    }

    printf("Compare strings with ziplist entries:\n");
    {
        zl = createList();
        p = ziplistIndex(zl,0);
        if (!ziplistCompare(p,(unsigned char*)"hello",5)) {
            printf("ERROR: not \"hello\"\n");
            return 1;
        }
        if (ziplistCompare(p,(unsigned char*)"hella",5)) {
            printf("ERROR: \"hella\"\n");
            return 1;
        }

        p = ziplistIndex(zl,3);
        if (!ziplistCompare(p,(unsigned char*)"1024",4)) {
            printf("ERROR: not \"1024\"\n");
            return 1;
        }
        if (ziplistCompare(p,(unsigned char*)"1025",4)) {
            printf("ERROR: \"1025\"\n");
            return 1;
        }
        printf("SUCCESS\n\n");
        zfree(zl);
    }

    printf("Merge test:\n");
    {
        /* create list gives us: [hello, foo, quux, 1024] */
        zl = createList();
        unsigned char *zl2 = createList();

        unsigned char *zl3 = ziplistNew();
        unsigned char *zl4 = ziplistNew();

        if (ziplistMerge(&zl4, &zl4)) {
            printf("ERROR: Allowed merging of one ziplist into itself.\n");
            return 1;
        }

        /* Merge two empty ziplists, get empty result back. */
        zl4 = ziplistMerge(&zl3, &zl4);
        ziplistRepr(zl4);
        if (ziplistLen(zl4)) {
            printf("ERROR: Merging two empty ziplists created entries.\n");
            return 1;
        }
        zfree(zl4);

        zl2 = ziplistMerge(&zl, &zl2);
        /* merge gives us: [hello, foo, quux, 1024, hello, foo, quux, 1024] */
        ziplistRepr(zl2);

        if (ziplistLen(zl2) != 8) {
            printf("ERROR: Merged length not 8, but: %u\n", ziplistLen(zl2));
            return 1;
        }

        p = ziplistIndex(zl2,0);
        if (!ziplistCompare(p,(unsigned char*)"hello",5)) {
            printf("ERROR: not \"hello\"\n");
            return 1;
        }
        if (ziplistCompare(p,(unsigned char*)"hella",5)) {
            printf("ERROR: \"hella\"\n");
            return 1;
        }

        p = ziplistIndex(zl2,3);
        if (!ziplistCompare(p,(unsigned char*)"1024",4)) {
            printf("ERROR: not \"1024\"\n");
            return 1;
        }
        if (ziplistCompare(p,(unsigned char*)"1025",4)) {
            printf("ERROR: \"1025\"\n");
            return 1;
        }

        p = ziplistIndex(zl2,4);
        if (!ziplistCompare(p,(unsigned char*)"hello",5)) {
            printf("ERROR: not \"hello\"\n");
            return 1;
        }
        if (ziplistCompare(p,(unsigned char*)"hella",5)) {
            printf("ERROR: \"hella\"\n");
            return 1;
        }

        p = ziplistIndex(zl2,7);
        if (!ziplistCompare(p,(unsigned char*)"1024",4)) {
            printf("ERROR: not \"1024\"\n");
            return 1;
        }
        if (ziplistCompare(p,(unsigned char*)"1025",4)) {
            printf("ERROR: \"1025\"\n");
            return 1;
        }
        printf("SUCCESS\n\n");
        zfree(zl);
    }

    printf("Stress with random payloads of different encoding:\n");
    {
        unsigned long long start = usec();
        int i,j,len,where;
        unsigned char *p;
        char buf[1024];
        int buflen;
        list *ref;
        listNode *refnode;

        /* Hold temp vars from ziplist */
        unsigned char *sstr;
        unsigned int slen;
        long long sval;

        iteration = accurate ? 20000 : 20;
        for (i = 0; i < iteration; i++) {
            zl = ziplistNew();
            ref = listCreate();
            listSetFreeMethod(ref,(void (*)(void*))sdsfree);
            len = rand() % 256;

            /* Create lists */
            for (j = 0; j < len; j++) {
                where = (rand() & 1) ? ZIPLIST_HEAD : ZIPLIST_TAIL;
                if (rand() % 2) {
                    buflen = randstring(buf,1,sizeof(buf)-1);
                } else {
                    switch(rand() % 3) {
                    case 0:
                        buflen = sprintf(buf,"%lld",(0LL + rand()) >> 20);
                        break;
                    case 1:
                        buflen = sprintf(buf,"%lld",(0LL + rand()));
                        break;
                    case 2:
                        buflen = sprintf(buf,"%lld",(0LL + rand()) << 20);
                        break;
                    default:
                        assert(NULL);
                    }
                }

                /* Add to ziplist */
                zl = ziplistPush(zl, (unsigned char*)buf, buflen, where);

                /* Add to reference list */
                if (where == ZIPLIST_HEAD) {
                    listAddNodeHead(ref,sdsnewlen(buf, buflen));
                } else if (where == ZIPLIST_TAIL) {
                    listAddNodeTail(ref,sdsnewlen(buf, buflen));
                } else {
                    assert(NULL);
                }
            }

            assert(listLength(ref) == ziplistLen(zl));
            for (j = 0; j < len; j++) {
                /* Naive way to get elements, but similar to the stresser
                 * executed from the Tcl test suite. */
                p = ziplistIndex(zl,j);
                refnode = listIndex(ref,j);

                assert(ziplistGet(p,&sstr,&slen,&sval));
                if (sstr == NULL) {
                    buflen = sprintf(buf,"%lld",sval);
                } else {
                    buflen = slen;
                    memcpy(buf,sstr,buflen);
                    buf[buflen] = '\0';
                }
                assert(memcmp(buf,listNodeValue(refnode),buflen) == 0);
            }
            zfree(zl);
            listRelease(ref);
        }
        printf("Done. usec=%lld\n\n", usec()-start);
    }

    printf("Stress with variable ziplist size:\n");
    {
        unsigned long long start = usec();
        int maxsize = accurate ? 16384 : 16;
        stress(ZIPLIST_HEAD,100000,maxsize,256);
        stress(ZIPLIST_TAIL,100000,maxsize,256);
        printf("Done. usec=%lld\n\n", usec()-start);
    }

    /* Benchmarks */
    {
        zl = ziplistNew();
        iteration = accurate ? 100000 : 100;
        for (int i=0; i<iteration; i++) {
            char buf[4096] = "asdf";
            zl = ziplistPush(zl, (unsigned char*)buf, 4, ZIPLIST_TAIL);
            zl = ziplistPush(zl, (unsigned char*)buf, 40, ZIPLIST_TAIL);
            zl = ziplistPush(zl, (unsigned char*)buf, 400, ZIPLIST_TAIL);
            zl = ziplistPush(zl, (unsigned char*)buf, 4000, ZIPLIST_TAIL);
            zl = ziplistPush(zl, (unsigned char*)"1", 1, ZIPLIST_TAIL);
            zl = ziplistPush(zl, (unsigned char*)"10", 2, ZIPLIST_TAIL);
            zl = ziplistPush(zl, (unsigned char*)"100", 3, ZIPLIST_TAIL);
            zl = ziplistPush(zl, (unsigned char*)"1000", 4, ZIPLIST_TAIL);
            zl = ziplistPush(zl, (unsigned char*)"10000", 5, ZIPLIST_TAIL);
            zl = ziplistPush(zl, (unsigned char*)"100000", 6, ZIPLIST_TAIL);
        }

        printf("Benchmark ziplistFind:\n");
        {
            unsigned long long start = usec();
            for (int i = 0; i < 2000; i++) {
                unsigned char *fptr = ziplistIndex(zl, ZIPLIST_HEAD);
                fptr = ziplistFind(zl, fptr, (unsigned char*)"nothing", 7, 1);
            }
            printf("%lld\n", usec()-start);
        }

        printf("Benchmark ziplistIndex:\n");
        {
            unsigned long long start = usec();
            for (int i = 0; i < 2000; i++) {
                ziplistIndex(zl, 99999);
            }
            printf("%lld\n", usec()-start);
        }

        printf("Benchmark ziplistValidateIntegrity:\n");
        {
            unsigned long long start = usec();
            for (int i = 0; i < 2000; i++) {
                ziplistValidateIntegrity(zl, ziplistBlobLen(zl), 1, NULL, NULL);
            }
            printf("%lld\n", usec()-start);
        }

        printf("Benchmark ziplistCompare with string\n");
        {
            unsigned long long start = usec();
            for (int i = 0; i < 2000; i++) {
                unsigned char *eptr = ziplistIndex(zl,0);
                while (eptr != NULL) {
                    ziplistCompare(eptr,(unsigned char*)"nothing",7);
                    eptr = ziplistNext(zl,eptr);
                }
            }
            printf("Done. usec=%lld\n", usec()-start);
        }

        printf("Benchmark ziplistCompare with number\n");
        {
            unsigned long long start = usec();
            for (int i = 0; i < 2000; i++) {
                unsigned char *eptr = ziplistIndex(zl,0);
                while (eptr != NULL) {
                    ziplistCompare(eptr,(unsigned char*)"99999",5);
                    eptr = ziplistNext(zl,eptr);
                }
            }
            printf("Done. usec=%lld\n", usec()-start);
        }

        zfree(zl);
    }

    printf("Stress __ziplistCascadeUpdate:\n");
    {
        char data[ZIP_BIG_PREVLEN];
        zl = ziplistNew();
        iteration = accurate ? 100000 : 100;
        for (int i = 0; i < iteration; i++) {
            zl = ziplistPush(zl, (unsigned char*)data, ZIP_BIG_PREVLEN-4, ZIPLIST_TAIL);
        }
        unsigned long long start = usec();
        zl = ziplistPush(zl, (unsigned char*)data, ZIP_BIG_PREVLEN-3, ZIPLIST_HEAD);
        printf("Done. usec=%lld\n\n", usec()-start);
        zfree(zl);
    }

    printf("Edge cases of __ziplistCascadeUpdate:\n");
    {
        /* Inserting a entry with data length greater than ZIP_BIG_PREVLEN-4 
         * will leads to cascade update. */
        size_t s1 = ZIP_BIG_PREVLEN-4, s2 = ZIP_BIG_PREVLEN-3;
        zl = ziplistNew();

        zlentry e[4] = {{.prevrawlensize = 0, .prevrawlen = 0, .lensize = 0,
                         .len = 0, .headersize = 0, .encoding = 0, .p = NULL}};

        zl = insertHelper(zl, 'a', s1, ZIPLIST_ENTRY_HEAD(zl));
        verify(zl, e);

        assert(e[0].prevrawlensize == 1 && e[0].prevrawlen == 0);
        assert(compareHelper(zl, 'a', s1, 0));
        ziplistRepr(zl);

        /* No expand. */
        zl = insertHelper(zl, 'b', s1, ZIPLIST_ENTRY_HEAD(zl));
        verify(zl, e);

        assert(e[0].prevrawlensize == 1 && e[0].prevrawlen == 0);
        assert(compareHelper(zl, 'b', s1, 0));

        assert(e[1].prevrawlensize == 1 && e[1].prevrawlen == strEntryBytesSmall(s1));
        assert(compareHelper(zl, 'a', s1, 1));

        ziplistRepr(zl);

        /* Expand(tail included). */
        zl = insertHelper(zl, 'c', s2, ZIPLIST_ENTRY_HEAD(zl));
        verify(zl, e);

        assert(e[0].prevrawlensize == 1 && e[0].prevrawlen == 0);
        assert(compareHelper(zl, 'c', s2, 0));

        assert(e[1].prevrawlensize == 5 && e[1].prevrawlen == strEntryBytesSmall(s2));
        assert(compareHelper(zl, 'b', s1, 1));

        assert(e[2].prevrawlensize == 5 && e[2].prevrawlen == strEntryBytesLarge(s1));
        assert(compareHelper(zl, 'a', s1, 2));

        ziplistRepr(zl);

        /* Expand(only previous head entry). */
        zl = insertHelper(zl, 'd', s2, ZIPLIST_ENTRY_HEAD(zl));
        verify(zl, e);

        assert(e[0].prevrawlensize == 1 && e[0].prevrawlen == 0);
        assert(compareHelper(zl, 'd', s2, 0));

        assert(e[1].prevrawlensize == 5 && e[1].prevrawlen == strEntryBytesSmall(s2));
        assert(compareHelper(zl, 'c', s2, 1));

        assert(e[2].prevrawlensize == 5 && e[2].prevrawlen == strEntryBytesLarge(s2));
        assert(compareHelper(zl, 'b', s1, 2));

        assert(e[3].prevrawlensize == 5 && e[3].prevrawlen == strEntryBytesLarge(s1));
        assert(compareHelper(zl, 'a', s1, 3));

        ziplistRepr(zl);

        /* Delete from mid. */
        unsigned char *p = ziplistIndex(zl, 2);
        zl = ziplistDelete(zl, &p);
        verify(zl, e);

        assert(e[0].prevrawlensize == 1 && e[0].prevrawlen == 0);
        assert(compareHelper(zl, 'd', s2, 0));

        assert(e[1].prevrawlensize == 5 && e[1].prevrawlen == strEntryBytesSmall(s2));
        assert(compareHelper(zl, 'c', s2, 1));

        assert(e[2].prevrawlensize == 5 && e[2].prevrawlen == strEntryBytesLarge(s2));
        assert(compareHelper(zl, 'a', s1, 2));

        ziplistRepr(zl);

        zfree(zl);
    }

    printf("__ziplistInsert nextdiff == -4 && reqlen < 4 (issue #7170):\n");
    {
        zl = ziplistNew();

        /* We set some values to almost reach the critical point - 254 */
        char A_252[253] = {0}, A_250[251] = {0};
        memset(A_252, 'A', 252);
        memset(A_250, 'A', 250);

        /* After the rpush, the list look like: [one two A_252 A_250 three 10] */
        zl = ziplistPush(zl, (unsigned char*)"one", 3, ZIPLIST_TAIL);
        zl = ziplistPush(zl, (unsigned char*)"two", 3, ZIPLIST_TAIL);
        zl = ziplistPush(zl, (unsigned char*)A_252, strlen(A_252), ZIPLIST_TAIL);
        zl = ziplistPush(zl, (unsigned char*)A_250, strlen(A_250), ZIPLIST_TAIL);
        zl = ziplistPush(zl, (unsigned char*)"three", 5, ZIPLIST_TAIL);
        zl = ziplistPush(zl, (unsigned char*)"10", 2, ZIPLIST_TAIL);
        ziplistRepr(zl);

        p = ziplistIndex(zl, 2);
        if (!ziplistCompare(p, (unsigned char*)A_252, strlen(A_252))) {
            printf("ERROR: not \"A_252\"\n");
            return 1;
        }

        /* When we remove A_252, the list became: [one two A_250 three 10]
         * A_250's prev node became node two, because node two quite small
         * So A_250's prevlenSize shrink to 1, A_250's total size became 253(1+2+250)
         * The prev node of node three is still node A_250.
         * We will not shrink the node three's prevlenSize, keep it at 5 bytes */
        zl = ziplistDelete(zl, &p);
        ziplistRepr(zl);

        p = ziplistIndex(zl, 3);
        if (!ziplistCompare(p, (unsigned char*)"three", 5)) {
            printf("ERROR: not \"three\"\n");
            return 1;
        }

        /* We want to insert a node after A_250, the list became: [one two A_250 10 three 10]
         * Because the new node is quite small, node three prevlenSize will shrink to 1 */
        zl = ziplistInsert(zl, p, (unsigned char*)"10", 2);
        ziplistRepr(zl);

        /* Last element should equal 10 */
        p = ziplistIndex(zl, -1);
        if (!ziplistCompare(p, (unsigned char*)"10", 2)) {
            printf("ERROR: not \"10\"\n");
            return 1;
        }

        zfree(zl);
    }

    printf("ALL TESTS PASSED!\n");
    return 0;
}
#endif
