/* quicklist.h - A generic doubly linked quicklist implementation
 *
 * Copyright (c) 2014, Matt Stancliff <matt@genges.com>
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 *   * Redistributions of source code must retain the above copyright notice,
 *     this quicklist of conditions and the following disclaimer.
 *   * Redistributions in binary form must reproduce the above copyright
 *     notice, this quicklist of conditions and the following disclaimer in the
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

#include <stdint.h> // for UINTPTR_MAX

#ifndef __QUICKLIST_H__
#define __QUICKLIST_H__

/* Node, quicklist, and Iterator are the only data structures used currently. */

/* quicklistNode is a 32 byte struct describing a listpack for a quicklist.
 * We use bit fields keep the quicklistNode at 32 bytes.
 * count: 16 bits, max 65536 (max lp bytes is 65k, so max count actually < 32k).
 * encoding: 2 bits, RAW=1, LZF=2.
 * container: 2 bits, PLAIN=1, PACKED=2.
 * recompress: 1 bit, bool, true if node is temporary decompressed for usage.
 * attempted_compress: 1 bit, boolean, used for verifying during testing.
 * extra: 10 bits, free for future use; pads out the remainder of 32 bits */

/* Node, quicklist, Iterator 是目前唯一使用的数据结构。*/

/* quicklistNode 是一个32字节的结构，描述一个 quicklist 中的 listpack (listpack 作为 quicklist 中的节点)
 * 我们使用 bit 字段使 quicklistNode 保持在32字节。
 * prev: 上一个节点
 * count: 16位，代表 listpack 中的元素总数，最大值65536（最大 listpack 字节数是 65k，所以 count 最大值实际上 < 32k）
 * encoding。2位，代表编码类型，RAW = 1，LZF = 2。
 * container：2位，容器，PLAIN = 1，PACKED = 2，
 * 容器类型为 PLAIN 则该节点是一个字符串元素（元素本身即是节点），可以保存超过4GB的数据，见 redis 仓库的 pr #9357，
 * PACKED 则节点为 listpack 并可以容纳多个元素，一般情况下都为 PACKED
 * recompress。1位，bool，如果节点被临时解压使用，则为true。
 * attempted_compress: 1比特，bool，用于测试期间的验证。
 * extra: 10 bit, 空闲的，供将来使用；目前是用来填补结构体最后32位剩下的空间 */
typedef struct quicklistNode {
    struct quicklistNode *prev;
    struct quicklistNode *next;
    unsigned char *entry;
    size_t sz;             /* entry size in bytes */
    unsigned int count : 16;     /* count of items in listpack */
    unsigned int encoding : 2;   /* RAW==1 or LZF==2 */
    unsigned int container : 2;  /* PLAIN==1 or PACKED==2 */
    unsigned int recompress : 1; /* was this node previous compressed? */
    unsigned int attempted_compress : 1; /* node can't compress; too small */
    unsigned int extra : 10; /* more bits to steal for future usage */
} quicklistNode;

/* quicklistLZF is a 8+N byte struct holding 'sz' followed by 'compressed'.
 * 'sz' is byte length of 'compressed' field.
 * 'compressed' is LZF data with total (compressed) length 'sz'
 * NOTE: uncompressed length is stored in quicklistNode->sz.
 * When quicklistNode->entry is compressed, node->entry points to a quicklistLZF */

/* quicklistLZF 是一个 8+N 字节的结构，其中包含 'sz' 和 'compressed'
 * 'sz' 是'压缩'字段的字节长度。
 * 'compressed' 是LZF数据，总长度（压缩后）为 'sz'。
 * 注意：未压缩的长度被存储在 quicklistNode->sz 中。
 * 当 quicklistNode->entry 被压缩时，node->entry 指向 quicklistLZF */
typedef struct quicklistLZF {
    size_t sz; /* LZF size in bytes*/
    char compressed[];
} quicklistLZF;

/* Bookmarks are padded with realloc at the end of of the quicklist struct.
 * They should only be used for very big lists if thousands of nodes were the
 * excess memory usage is negligible, and there's a real need to iterate on them
 * in portions.
 * When not used, they don't add any memory overhead, but when used and then
 * deleted, some overhead remains (to avoid resonance).
 * The number of bookmarks used should be kept to minimum since it also adds
 * overhead on node deletion (searching for a bookmark to update). */

/* Bookmarks 是在快速列表结构的末尾用 realloc 填充的。
 * 它们应该只用于非常大的列表，如果有成千上万的节点，那么多余的内存占用可以忽略不计，
 * 而且确实有必要对它们进行部分迭代。
 * 当不使用时，它们不会增加任何内存开销，但当使用后再删除时，一些开销仍然存在（以避免再申请内存分配）。
 * 使用的 bookmarks 的数量应该保持在最低限度，因为它也会增加节点删除时的开销（查找 bookmark 并更新） */
typedef struct quicklistBookmark {
    quicklistNode *node;
    char *name;
} quicklistBookmark;

#if UINTPTR_MAX == 0xffffffff
/* 32-bit */
/* 32位下的quicklist 结构体成员 fill, compress, bookmark_count的所占空间(单位 bit) */
#   define QL_FILL_BITS 14
#   define QL_COMP_BITS 14
#   define QL_BM_BITS 4
#elif UINTPTR_MAX == 0xffffffffffffffff
/* 64-bit */
/* 64位下的quicklist 结构体成员 fill, compress, bookmark_count的所占空间(单位 bit) */
#   define QL_FILL_BITS 16
#   define QL_COMP_BITS 16
#   define QL_BM_BITS 4 /* we can encode more, but we rather limit the user
                           since they cause performance degradation. */
#else
#   error unknown arch bits count
#endif

/* quicklist is a 40 byte struct (on 64-bit systems) describing a quicklist.
 * 'count' is the number of total entries.
 * 'len' is the number of quicklist nodes.
 * 'compress' is: 0 if compression disabled, otherwise it's the number
 *                of quicklistNodes to leave uncompressed at ends of quicklist.
 * 'fill' is the user-requested (or default) fill factor.
 * 'bookmarks are an optional feature that is used by realloc this struct,
 *      so that they don't consume memory when not used. */

/* quicklist(快速列表) 是一个 40 字节的结构（在64位系统上），
 * quicklist 实际上是一个双链表，它的每个节点是一个 listpack(紧凑列表)
 * 'head' 是头节点
 * 'tail' 是尾节点
 * 'count' 是元素总数。
 * 'len' 是快速列表的节点数。
 * 'fill' 16位，是用户要求的（或默认）填充系数，代表每个节点的最大容量，
 * 不同数值有不同的含义，默认为-2,表示每个 quicklistNode 的 listpack 所占字节数不能超过8kb。
 * 'compress'：16位，压缩配置，如果为0表示禁用压缩，
 * 大于0则该数字就是在快速列表的两端保留未压缩的节点数量。
 * 'bookmark_count' 4位，代表 bookmarks 数组的大小
 * 'bookmarks' 是一个可选的字段，用于重新分配 quicklist 的空间，不使用时不会消耗内存。*/
typedef struct quicklist {
    quicklistNode *head;
    quicklistNode *tail;
    unsigned long count;        /* total count of all entries in all listpacks */
    unsigned long len;          /* number of quicklistNodes */
    signed int fill : QL_FILL_BITS;       /* fill factor for individual nodes */
    unsigned int compress : QL_COMP_BITS; /* depth of end nodes not to compress;0=off */
    unsigned int bookmark_count: QL_BM_BITS;
    quicklistBookmark bookmarks[];
} quicklist;

/* quicklist 迭代器
 * 'quicklist' 指向迭代器所在的快速列表 
 * 'current' 指向迭代器所在的列表节点 
 * 'zi' 指向迭代器所在的元素
 * 'offset' 为迭代器所在元素在当前 listpack 中的偏移量 
 * 'direction' 为迭代方向（往头部或尾部前进）*/
typedef struct quicklistIter {
    quicklist *quicklist;
    quicklistNode *current;
    unsigned char *zi;
    long offset; /* offset in current listpack */
    int direction;
} quicklistIter;

/* quicklistEntry
 * quicklistEntry 不知道该如何准确翻译，你可以看成是存储一个列表中的元素的结构，
 * 看下方对每个结构体成员的注释你应该就能有自己的理解了，
 * 'quicklist' 指向 entry 所在的快速列表
 * 'node' 指向 entry 所在的节点 
 * 'zi' 指向一个元素 
 * 'value' 元素的值（如果是字符串类型）
 * 'longval' 元素的值（如果是整型）
 * 'sz' 元素的字节数（如果元素是字符串类型） 
 * 'offset' 元素在所在 listpack 的偏移量 */
typedef struct quicklistEntry {
    const quicklist *quicklist;
    quicklistNode *node;
    unsigned char *zi;
    unsigned char *value;
    long long longval;
    size_t sz;
    int offset;
} quicklistEntry;

/* 位置 */
#define QUICKLIST_HEAD 0 /* 头部 */
#define QUICKLIST_TAIL -1 /* 尾部 */

/* quicklist node encodings */
/* 快速列表节点的编码类型 */
#define QUICKLIST_NODE_ENCODING_RAW 1 /* RAW 编码（未压缩） */
#define QUICKLIST_NODE_ENCODING_LZF 2 /* LZF 编码（压缩） */

/* quicklist compression disable */
/* 快速列表禁用压缩 */
#define QUICKLIST_NOCOMPRESS 0

/* quicklist container formats */
/* 快速列表节点容器格式 */
#define QUICKLIST_NODE_CONTAINER_PLAIN 1
#define QUICKLIST_NODE_CONTAINER_PACKED 2

/* 判断节点容器是否为 PLAIN */
#define QL_NODE_IS_PLAIN(node) ((node)->container == QUICKLIST_NODE_CONTAINER_PLAIN)

/* 判断节点是否被压缩 */
#define quicklistNodeIsCompressed(node)                                        \
    ((node)->encoding == QUICKLIST_NODE_ENCODING_LZF)

/* Prototypes */
/* 函数原型 */
quicklist *quicklistCreate(void);
quicklist *quicklistNew(int fill, int compress);
void quicklistSetCompressDepth(quicklist *quicklist, int depth);
void quicklistSetFill(quicklist *quicklist, int fill);
void quicklistSetOptions(quicklist *quicklist, int fill, int depth);
void quicklistRelease(quicklist *quicklist);
int quicklistPushHead(quicklist *quicklist, void *value, const size_t sz);
int quicklistPushTail(quicklist *quicklist, void *value, const size_t sz);
void quicklistPush(quicklist *quicklist, void *value, const size_t sz,
                   int where);
void quicklistAppendListpack(quicklist *quicklist, unsigned char *zl);
void quicklistAppendPlainNode(quicklist *quicklist, unsigned char *data, size_t sz);
void quicklistInsertAfter(quicklistIter *iter, quicklistEntry *entry,
                          void *value, const size_t sz);
void quicklistInsertBefore(quicklistIter *iter, quicklistEntry *entry,
                           void *value, const size_t sz);
void quicklistDelEntry(quicklistIter *iter, quicklistEntry *entry);
void quicklistReplaceEntry(quicklistIter *iter, quicklistEntry *entry,
                           void *data, size_t sz);
int quicklistReplaceAtIndex(quicklist *quicklist, long index, void *data,
                            const size_t sz);
int quicklistDelRange(quicklist *quicklist, const long start, const long stop);
quicklistIter *quicklistGetIterator(quicklist *quicklist, int direction);
quicklistIter *quicklistGetIteratorAtIdx(quicklist *quicklist,
                                         int direction, const long long idx);
quicklistIter *quicklistGetIteratorEntryAtIdx(quicklist *quicklist, const long long index,
                                              quicklistEntry *entry);
int quicklistNext(quicklistIter *iter, quicklistEntry *entry);
void quicklistSetDirection(quicklistIter *iter, int direction);
void quicklistReleaseIterator(quicklistIter *iter);
quicklist *quicklistDup(quicklist *orig);
void quicklistRotate(quicklist *quicklist);
int quicklistPopCustom(quicklist *quicklist, int where, unsigned char **data,
                       size_t *sz, long long *sval,
                       void *(*saver)(unsigned char *data, size_t sz));
int quicklistPop(quicklist *quicklist, int where, unsigned char **data,
                 size_t *sz, long long *slong);
unsigned long quicklistCount(const quicklist *ql);
int quicklistCompare(quicklistEntry *entry, unsigned char *p2, const size_t p2_len);
size_t quicklistGetLzf(const quicklistNode *node, void **data);
void quicklistRepr(unsigned char *ql, int full);

/* bookmarks */
int quicklistBookmarkCreate(quicklist **ql_ref, const char *name, quicklistNode *node);
int quicklistBookmarkDelete(quicklist *ql, const char *name);
quicklistNode *quicklistBookmarkFind(quicklist *ql, const char *name);
void quicklistBookmarksClear(quicklist *ql);
int quicklistisSetPackedThreshold(size_t sz);

#ifdef REDIS_TEST
int quicklistTest(int argc, char *argv[], int flags);
#endif

/* Directions for iterators */
#define AL_START_HEAD 0
#define AL_START_TAIL 1

#endif /* __QUICKLIST_H__ */
