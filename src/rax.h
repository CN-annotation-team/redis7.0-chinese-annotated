/* Rax -- A radix tree implementation.
 *
 * Copyright (c) 2017-2018, Salvatore Sanfilippo <antirez at gmail dot com>
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

#ifndef RAX_H
#define RAX_H

#include <stdint.h>

/* Representation of a radix tree as implemented in this file, that contains
 * the strings "foo", "foobar" and "footer" after the insertion of each
 * word. When the node represents a key inside the radix tree, we write it
 * between [], otherwise it is written between ().
 *
 * This is the vanilla representation:
 *
 *              (f) ""
 *                \
 *                (o) "f"
 *                  \
 *                  (o) "fo"
 *                    \
 *                  [t   b] "foo"
 *                  /     \
 *         "foot" (e)     (a) "foob"
 *                /         \
 *      "foote" (r)         (r) "fooba"
 *              /             \
 *    "footer" []             [] "foobar"
 *
 * However, this implementation implements a very common optimization where
 * successive nodes having a single child are "compressed" into the node
 * itself as a string of characters, each representing a next-level child,
 * and only the link to the node representing the last character node is
 * provided inside the representation. So the above representation is turned
 * into:
 *
 *                  ["foo"] ""
 *                     |
 *                  [t   b] "foo"
 *                  /     \
 *        "foot" ("er")    ("ar") "foob"
 *                 /          \
 *       "footer" []          [] "foobar"
 *
 * However this optimization makes the implementation a bit more complex.
 * For instance if a key "first" is added in the above radix tree, a
 * "node splitting" operation is needed, since the "foo" prefix is no longer
 * composed of nodes having a single child one after the other. This is the
 * above tree and the resulting node splitting after this event happens:
 *
 *
 *                    (f) ""
 *                    /
 *                 (i o) "f"
 *                 /   \
 *    "firs"  ("rst")  (o) "fo"
 *              /        \
 *    "first" []       [t   b] "foo"
 *                     /     \
 *           "foot" ("er")    ("ar") "foob"
 *                    /          \
 *          "footer" []          [] "foobar"
 *
 * Similarly after deletion, if a new chain of nodes having a single child
 * is created (the chain must also not include nodes that represent keys),
 * it must be compressed back into a single node.
 *
 */
/* rax 是 Redis 对基数树(Radix tree) 的实现，这种数据结构比普通的前缀树(Trie) 更节省空间，又被称为压缩前缀树。
 * 下面结合上面作者的注释对 rax 做一个简单介绍：
 *
 * 在 redis 的前缀树实现中，
 *   1. 对任意节点，data 部分存储了子节点的值以及子节点的指针，其中非叶子节点的子节点数量至少有一个，叶子节点的子节点数量为 0。
 *      下图中用`()`或`[]`中的内容表示子节点的值，用连线`\`或`/`表示子节点的指针。
 *   2. 对于前缀树中的一个字符序列，它不是保存在节点中，而是由节点的位置决定。
 *      从根节点到当前节点的路径上(不包含当前节点)的字符串，就是这个节点表示的字符序列，下图中用 `"xxx"` 表示节点的字符序列
 *   3. 不是所有位置的节点都是有效的 key，在具体实现中用标志位 iskey=1 标注的节点才是 key，在下图中，用`[]`表示这个位置是一个 key。
 *
 * 下图是一个普通前缀树的实现：
 *
 *              (f) ""
 *                \
 *                (o) "f"
 *                  \
 *                  (o) "fo"
 *                    \
 *                  [t   b] "foo"
 *                  /     \
 *         "foot" (e)     (a) "foob"
 *                /         \
 *      "foote" (r)         (r) "fooba"
 *              /             \
 *    "footer" []             [] "foobar"
 *
 *
 * 压缩前缀树实现 (radix tree 模型）：
 * 普通前缀树中存在一些连续的节点，它们都只有一个子节点。这些连续的特殊节点中的内容可以被压缩进一个节点中，表现形式为一个包含之前完整路径的字符串。
 * 下图中，foo，er，ar 都做了压缩处理，这些压缩后的节点被称为压缩节点，其他节点为非压缩节点。
 *
 *                  ("foo") ""
 *                     |
 *                  [t   b] "foo"
 *                  /     \
 *        "foot" ("er")    ("ar") "foob"
 *                 /          \
 *       "footer" []          [] "foobar"
 *
 * 注意：只有连续的单孩子节点才可以压缩。有些节点存在多个子节点则不可压缩。，比如上图中的 `[t   b]`。
 *
 * 在 redis raxNode 结构体中，一个节点是否为压缩节点用 iscompr 表示。两种的节点的数据保存形式有所不同:
 * 节点 `("foo")`
 *   1. 压缩节点，数据被写进 `()` 中，所以不是一个 key。
 *   2. 数据内容为: [header iskey=0,iscompr=1][foo][foo-ptr]
 * 节点 `[t   b]`
 *   1. 非压缩节点，在上图中表示 "foo" 这个 key，一般携带一个对应的 value 指针。
 *   2. 数据内容为: [header iskey=1,iscompr=0][tb][t-ptr][b-ptr][foo-value-ptr]
 *
 *
 * 虽然上述优化节省了空间，但额外增加了复杂度。如果 key "first" 要插入上面的 radix tree 中，节点 `(f)` 在已有一个子节点 `(o)` 的情况下，
 * 又多了一个 `(i)`。即前缀 "foo" 不再是连续的单孩子节点，所以它需要进行切割。
 * 下图是压缩节点 `("foo")` 被切割后的样子。
 *
 *                    (f) ""
 *                    /
 *                 (i o) "f"
 *                 /   \
 *    "firs"  ("rst")  (o) "fo"
 *              /        \
 *    "first" []       [t   b] "foo"
 *                     /     \
 *           "foot" ("er")    ("ar") "foob"
 *                    /          \
 *          "footer" []          [] "foobar"
 *
 * 插入一个 key 可能引起节点切割，同理删除一个 key 后，不满足压缩条件的前缀可能会重新满足条件，
 * 这个时候又可以将这个前缀压缩回一个节点中。
 *
 * rax 比较特殊的点:
 * rax 为了出于性能的考虑，将边（edge，字符以及指针），存储在父节点中，这确实造成了一些可读性方面的困难。
 * 也就是说，从根节点到树中任意节点N，所有的边组成一个字符序列，这个序列不包含 N 中存储的任意数据，
 * 假设这个序列是一个 key，那么对应的 value 指针却存储在 N 中。
 *
 * 所以下图中从上到下第四个节点 `[t  b]` 表示字符串 "foo"，因为它有依次为 'f' 'o' 'o' 的三条边，
 * 其自身的两个字符 t,b 是它与两个子节点的边，不参与当前字符序列的计算。
 *
 * 边存储在父节点中，查找一个字符序列的时候，可以在父节点中直接确定路径。避免进入子节点后发现路径不对又退回父节点遍历下一个子节点的情况
 * 将边的字符放一起，边的指针放一起，增加缓存行的命中效率
 *
 * 关于作者对 rax 特性的详细解释，见：https://github.com/antirez/rax/blob/master/README.md
 * 一个传统 radix tree 实现，见：https://en.wikipedia.org/wiki/Radix_tree
 */

#define RAX_NODE_MAX_SIZE ((1<<29)-1)
/* radix tree 的节点，前四个字段占一个 32 位 INT，可以认为该结构的 header。
 * 柔性数组 char data[] 不占空间，sizeof(raxNode) = 32
 */
typedef struct raxNode {
    uint32_t iskey:1;     /* Does this node contain a key? */
    uint32_t isnull:1;    /* Associated value is NULL (don't store it). */
    uint32_t iscompr:1;   /* Node is compressed. */
    uint32_t size:29;     /* Number of children, or compressed string len. */
    /*
     * isKey 从根节点到当前节点路径上的字符序列（不包含当前节点）是否为一个 key。
     * isnull key 对应的 value 是否为 NULL
     * size 如果 iscompr=0，表示子节点的数量，如果 iscompr=1，表示压缩节点字符序列的长度
     * data 数组
     *   1. 非压缩节点：N 个子节点对应的字符，指向 N 个子节点的指针，以及节点为 key 时对应的 value 指针。
     *   2. 压缩节点：子孙节点对应的合并字符串，指向最后一个子孙节点的指针，以及节点为 key 时的 value 指针。
     *
     * 对于压缩和非压缩节点，rax tee 中的任意节点都可以表示 key。由于压缩节点的存在，这些 key 不可能囊括所有的字符序列。
     * 比如压缩节点 "window"] 表示 key ""，非压缩节点 [] 表示 key "window"
     *                  ["window"] ""
     *                     \
     *                     [] "window"
     *
     * 如果想将序列 win 也标记为 key，那么需要将压缩节点拆分
     *
     *                  ("win") ""
     *                     \
     *                   ["dow"] "win"
     *                       \
     *                       [] "window"
     *
     * 如果某个节点是一个 key 且不为 null（iskey=1,isnull=0），数组 data 的最后会额外分配一块空间，用来存储 value 指针。*/

    /* Data layout is as follows:
     *
     * If node is not compressed we have 'size' bytes, one for each children
     * character, and 'size' raxNode pointers, point to each child node.
     * Note how the character is not stored in the children but in the
     * edge of the parents:
     *
     * [header iscompr=0][abc][a-ptr][b-ptr][c-ptr](value-ptr?)
     *
     * if node is compressed (iscompr bit is 1) the node has 1 children.
     * In that case the 'size' bytes of the string stored immediately at
     * the start of the data section, represent a sequence of successive
     * nodes linked one after the other, for which only the last one in
     * the sequence is actually represented as a node, and pointed to by
     * the current compressed node.
     *
     * [header iscompr=1][xyz][z-ptr](value-ptr?)
     *
     * Both compressed and not compressed nodes can represent a key
     * with associated data in the radix tree at any level (not just terminal
     * nodes).
     *
     * If the node has an associated key (iskey=1) and is not NULL
     * (isnull=0), then after the raxNode pointers pointing to the
     * children, an additional value pointer is present (as you can see
     * in the representation above as "value-ptr" field).
     */
    unsigned char data[];
} raxNode;

/* 表示一颗 radix tree */
typedef struct rax {
    raxNode *head;
    /* 元素 (key) 的数量 */
    uint64_t numele;
    /* 节点的数量 */
    uint64_t numnodes;
} rax;

/* Stack data structure used by raxLowWalk() in order to, optionally, return
 * a list of parent nodes to the caller. The nodes do not have a "parent"
 * field for space concerns, so we use the auxiliary stack when needed. */
/* 为了减少空间占用，raxNode 结构中没有父节点的引用。
 * 作为补充，raxStack 这个辅助结构用于在查找 rax 过程中记录 raxNode 的父节点信息。
 * rax 迭代器以这些内容为辅助，来完成前一个 key 或者 后一个 key 的遍历跳转 */
#define RAX_STACK_STATIC_ITEMS 32
typedef struct raxStack {
    void **stack; /* Points to static_items or an heap allocated array. */
    size_t items, maxitems; /* Number of items contained and total space. */
    /* Up to RAXSTACK_STACK_ITEMS items we avoid to allocate on the heap
     * and use this static array of pointers instead. */
    void *static_items[RAX_STACK_STATIC_ITEMS];
    int oom; /* True if pushing into this stack failed for OOM at some point. */
} raxStack;

/* Optional callback used for iterators and be notified on each rax node,
 * including nodes not representing keys. If the callback returns true
 * the callback changed the node pointer in the iterator structure, and the
 * iterator implementation will have to replace the pointer in the radix tree
 * internals. This allows the callback to reallocate the node to perform
 * very special operations, normally not needed by normal applications.
 *
 * This callback is used to perform very low level analysis of the radix tree
 * structure, scanning each possible node (but the root node), or in order to
 * reallocate the nodes to reduce the allocation fragmentation (this is the
 * Redis application for this callback).
 *
 * This is currently only supported in forward iterations (raxNext) */
typedef int (*raxNodeCallback)(raxNode **noderef);

/* Radix tree iterator state is encapsulated into this data structure. */
#define RAX_ITER_STATIC_LEN 128
#define RAX_ITER_JUST_SEEKED (1<<0) /* Iterator was just seeked. Return current
                                       element for the first iteration and
                                       clear the flag. */
#define RAX_ITER_EOF (1<<1)    /* End of iteration reached. */
#define RAX_ITER_SAFE (1<<2)   /* Safe iterator, allows operations while
                                  iterating. But it is slower. */
typedef struct raxIterator {
    int flags;
    rax *rt;                /* Radix tree we are iterating. */
    unsigned char *key;     /* The current string. */
    void *data;             /* Data associated to this key. */
    size_t key_len;         /* Current key length. */
    size_t key_max;         /* Max key len the current key buffer can hold. */
    unsigned char key_static_string[RAX_ITER_STATIC_LEN];
    raxNode *node;          /* Current node. Only for unsafe iteration. */
    raxStack stack;         /* Stack used for unsafe iteration. */
    raxNodeCallback node_cb; /* Optional node callback. Normally set to NULL. */
} raxIterator;

/* A special pointer returned for not found items. */
extern void *raxNotFound;

/* Exported API. */
rax *raxNew(void);
int raxInsert(rax *rax, unsigned char *s, size_t len, void *data, void **old);
int raxTryInsert(rax *rax, unsigned char *s, size_t len, void *data, void **old);
int raxRemove(rax *rax, unsigned char *s, size_t len, void **old);
void *raxFind(rax *rax, unsigned char *s, size_t len);
void raxFree(rax *rax);
void raxFreeWithCallback(rax *rax, void (*free_callback)(void*));
void raxStart(raxIterator *it, rax *rt);
int raxSeek(raxIterator *it, const char *op, unsigned char *ele, size_t len);
int raxNext(raxIterator *it);
int raxPrev(raxIterator *it);
int raxRandomWalk(raxIterator *it, size_t steps);
int raxCompare(raxIterator *iter, const char *op, unsigned char *key, size_t key_len);
void raxStop(raxIterator *it);
int raxEOF(raxIterator *it);
void raxShow(rax *rax);
uint64_t raxSize(rax *rax);
unsigned long raxTouch(raxNode *n);
void raxSetDebugMsg(int onoff);

/* Internal API. May be used by the node callback in order to access rax nodes
 * in a low level way, so this function is exported as well. */
void raxSetData(raxNode *n, void *data);

#endif
