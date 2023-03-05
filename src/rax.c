/* Rax -- A radix tree implementation.
 *
 * Version 1.2 -- 7 February 2019
 *
 * Copyright (c) 2017-2019, Salvatore Sanfilippo <antirez at gmail dot com>
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

#include <stdlib.h>
#include <string.h>
#include <assert.h>
#include <stdio.h>
#include <errno.h>
#include <math.h>
#include "rax.h"

#ifndef RAX_MALLOC_INCLUDE
#define RAX_MALLOC_INCLUDE "rax_malloc.h"
#endif

#include RAX_MALLOC_INCLUDE

/* This is a special pointer that is guaranteed to never have the same value
 * of a radix tree node. It's used in order to report "not found" error without
 * requiring the function to have multiple return values. */
void *raxNotFound = (void*)"rax-not-found-pointer";

/* -------------------------------- Debugging ------------------------------ */

void raxDebugShowNode(const char *msg, raxNode *n);

/* Turn debugging messages on/off by compiling with RAX_DEBUG_MSG macro on.
 * When RAX_DEBUG_MSG is defined by default Rax operations will emit a lot
 * of debugging info to the standard output, however you can still turn
 * debugging on/off in order to enable it only when you suspect there is an
 * operation causing a bug using the function raxSetDebugMsg(). */
#define RAX_DEBUG_MSG
#ifdef RAX_DEBUG_MSG
#define debugf(...)                                                            \
    if (raxDebugMsg) {                                                         \
        printf("%s:%s:%d:\t", __FILE__, __func__, __LINE__);                   \
        printf(__VA_ARGS__);                                                   \
        fflush(stdout);                                                        \
    }

#define debugnode(msg,n) raxDebugShowNode(msg,n)
#else
#define debugf(...)
#define debugnode(msg,n)
#endif

/* By default log debug info if RAX_DEBUG_MSG is defined. */
static int raxDebugMsg = 1;

/* When debug messages are enabled, turn them on/off dynamically. By
 * default they are enabled. Set the state to 0 to disable, and 1 to
 * re-enable. */
void raxSetDebugMsg(int onoff) {
    raxDebugMsg = onoff;
}

/* ------------------------- raxStack functions --------------------------
 * The raxStack is a simple stack of pointers that is capable of switching
 * from using a stack-allocated array to dynamic heap once a given number of
 * items are reached. It is used in order to retain the list of parent nodes
 * while walking the radix tree in order to implement certain operations that
 * need to navigate the tree upward.
 * ------------------------------------------------------------------------- */

/* Initialize the stack. */
static inline void raxStackInit(raxStack *ts) {
    ts->stack = ts->static_items;
    ts->items = 0;
    ts->maxitems = RAX_STACK_STATIC_ITEMS;
    ts->oom = 0;
}

/* Push an item into the stack, returns 1 on success, 0 on out of memory. */
static inline int raxStackPush(raxStack *ts, void *ptr) {
    if (ts->items == ts->maxitems) {
        if (ts->stack == ts->static_items) {
            ts->stack = rax_malloc(sizeof(void*)*ts->maxitems*2);
            if (ts->stack == NULL) {
                ts->stack = ts->static_items;
                ts->oom = 1;
                errno = ENOMEM;
                return 0;
            }
            memcpy(ts->stack,ts->static_items,sizeof(void*)*ts->maxitems);
        } else {
            void **newalloc = rax_realloc(ts->stack,sizeof(void*)*ts->maxitems*2);
            if (newalloc == NULL) {
                ts->oom = 1;
                errno = ENOMEM;
                return 0;
            }
            ts->stack = newalloc;
        }
        ts->maxitems *= 2;
    }
    ts->stack[ts->items] = ptr;
    ts->items++;
    return 1;
}

/* Pop an item from the stack, the function returns NULL if there are no
 * items to pop. */
static inline void *raxStackPop(raxStack *ts) {
    if (ts->items == 0) return NULL;
    ts->items--;
    return ts->stack[ts->items];
}

/* Return the stack item at the top of the stack without actually consuming
 * it. */
static inline void *raxStackPeek(raxStack *ts) {
    if (ts->items == 0) return NULL;
    return ts->stack[ts->items-1];
}

/* Free the stack in case we used heap allocation. */
static inline void raxStackFree(raxStack *ts) {
    if (ts->stack != ts->static_items) rax_free(ts->stack);
}

/* ----------------------------------------------------------------------------
 * Radix tree implementation
 * --------------------------------------------------------------------------*/

/* Return the padding needed in the characters section of a node having size
 * 'nodesize'. The padding is needed to store the child pointers to aligned
 * addresses. Note that we add 4 to the node size because the node has a four
 * bytes header. */
/* 计算指针对齐最少需要填充的字节数量，对齐时需要将 raxNode 4 bytes 的 header 考虑在内。
 * 设输入值（入参）为 n，输出值（返回值）为 x，x 为满足 (n+x+4) % size(void*) == 0 条件的最小值
 * 32 位系统表现: (4 - (n+4)%4) & 3
 * 64 位系统表现: (8 - (n+4)%8) & 7
 * 这个填充恰好使包含 data 数组在内的节点长度为 4 或者 8 的倍数 */
#define raxPadding(nodesize) ((sizeof(void*)-((nodesize+4) % sizeof(void*))) & (sizeof(void*)-1))

/* Return the pointer to the last child pointer in a node. For the compressed
 * nodes this is the only child pointer. */
/* 返回最后一个子节点指针的地址
 * 直接以 raxNode 外第一个字节为起点，如果它是一个 key，左移两个指针长度，如果它不是一个 key，左移一个指针长度 */
#define raxNodeLastChildPtr(n) ((raxNode**) ( \
    ((char*)(n)) + \
    raxNodeCurrentLength(n) - \
    sizeof(raxNode*) - \
    (((n)->iskey && !(n)->isnull) ? sizeof(void*) : 0) \
))

/* Return the pointer to the first child pointer. */
/* 返回第一个子节点指针的地址
 * 以 data 数组为起点，右移 size(子节点的数量或者压缩序列的长度) 个字节，再跨过填充的字节数量 */
#define raxNodeFirstChildPtr(n) ((raxNode**) ( \
    (n)->data + \
    (n)->size + \
    raxPadding((n)->size)))

/* Return the current total size of the node. Note that the second line
 * computes the padding after the string of characters, needed in order to
 * save pointers to aligned addresses. */
/* 返回包含 raxNode header 和 柔性数组 raxNode->data 在内的总长度
 * 因为 raxPadding 填充的原因，这个长度一定是 4(32位) 或者 8(64位) 的倍数 */
#define raxNodeCurrentLength(n) ( \
    sizeof(raxNode)+(n)->size+ \
    raxPadding((n)->size)+ \
    ((n)->iscompr ? sizeof(raxNode*) : sizeof(raxNode*)*(n)->size)+ \
    (((n)->iskey && !(n)->isnull)*sizeof(void*)) \
)

/* Allocate a new non compressed node with the specified number of children.
 * If datafield is true, the allocation is made large enough to hold the
 * associated data pointer.
 * Returns the new node pointer. On out of memory NULL is returned. */
/* 创建一个新的，指定孩子数量的非压缩节点，并根据 datafield 的值决定是否预留一个 value 指针的空间 */
raxNode *raxNewNode(size_t children, int datafield) {
    /* 数据形式 [raxNode HDR, data(children, padding, children pointers) ]*/
    size_t nodesize = sizeof(raxNode)+children+raxPadding(children)+
                      sizeof(raxNode*)*children;
    /* 是否要预留一个 value 指针的空间 */
    if (datafield) nodesize += sizeof(void*);
    raxNode *node = rax_malloc(nodesize);
    if (node == NULL) return NULL;
    node->iskey = 0;
    node->isnull = 0;
    node->iscompr = 0;
    node->size = children;
    return node;
}

/* Allocate a new rax and return its pointer. On out of memory the function
 * returns NULL. */
/* 创建一颗空的 rax 树，这颗树只有一个无孩子的根节点 */
rax *raxNew(void) {
    rax *rax = rax_malloc(sizeof(*rax));
    if (rax == NULL) return NULL;
    rax->numele = 0;
    rax->numnodes = 1;
    rax->head = raxNewNode(0,0);
    if (rax->head == NULL) {
        rax_free(rax);
        return NULL;
    } else {
        return rax;
    }
}

/* realloc the node to make room for auxiliary data in order
 * to store an item in that node. On out of memory NULL is returned. */
/* 重新分配内存，增加一个存放 value 指针的空间，返回新分配的内存地址 */
raxNode *raxReallocForData(raxNode *n, void *data) {
    if (data == NULL) return n; /* No reallocation needed, setting isnull=1 */
    size_t curlen = raxNodeCurrentLength(n);
    return rax_realloc(n,curlen+sizeof(void*));
}

/* Set the node auxiliary data to the specified pointer. */
/* 将节点 n 标记为 key，如果数据指针 data 不为 NULL，将其值拷贝到 n->data 字段的最后一个指针大小空位 */
void raxSetData(raxNode *n, void *data) {
    n->iskey = 1;
    if (data != NULL) {
        n->isnull = 0;
        void **ndata = (void**)
            ((char*)n+raxNodeCurrentLength(n)-sizeof(void*));
        memcpy(ndata,&data,sizeof(data));
    } else {
        n->isnull = 1;
    }
}

/* Get the node auxiliary data. */
/* 返回指定 rax 节点 n 中 value 指针，需要调用方保证节点 n 是一个 key */
void *raxGetData(raxNode *n) {
    if (n->isnull) return NULL;
    void **ndata =(void**)((char*)n+raxNodeCurrentLength(n)-sizeof(void*));
    void *data;
    memcpy(&data,ndata,sizeof(data));
    return data;
}

/* Add a new child to the node 'n' representing the character 'c' and return
 * its new pointer, as well as the child pointer by reference. Additionally
 * '***parentlink' is populated with the raxNode pointer-to-pointer of where
 * the new child was stored, which is useful for the caller to replace the
 * child pointer if it gets reallocated.
 *
 * On success the new parent node pointer is returned (it may change because
 * of the realloc, so the caller should discard 'n' and use the new value).
 * On out of memory NULL is returned, and the old node is still valid. */
/* 非压缩节点 n 增加一个表示字符 'c' 的子节点。
 * 方法返回值为 n 的新地址，新增的子节点地址通过二级指针 childptr 写回调用发起方。
 * 参数 parentlink 的作用:
 *   生成的新节点可能在 raxAddChild() 完成之后的操作里重新分配内存（比如扩容来增加一个存放 value 指针的空间），
 *   如果新节点地址因此被改变，那么它父节点里的相应指针的值也应该跟着改变，三级指针 parentlink 的作用就是将这个指针的地址写回调用发起方，
 *   以便发生上述情况时，可以将新地址的值直接写入 *parentlink 指向的位置。*/
raxNode *raxAddChild(raxNode *n, unsigned char c, raxNode **childptr, raxNode ***parentlink) {
    assert(n->iscompr == 0);

    /* 暂时将孩子数目加 1，来计算节点 n 扩容后的大小 */
    size_t curlen = raxNodeCurrentLength(n);
    n->size++;
    size_t newlen = raxNodeCurrentLength(n);
    n->size--; /* For now restore the original size. We'll update it only on
                  success at the end. */

    /* 生成一个无孩子，无 value 指针的新节点，它将以字符 c 的形式作为节点 n 的子节点 */
    /* Alloc the new child we will link to 'n'. */
    raxNode *child = raxNewNode(0,0);
    if (child == NULL) return NULL;

    /* 节点 n 扩容 */
    /* Make space in the original node. */
    raxNode *newn = rax_realloc(n,newlen);
    if (newn == NULL) {
        rax_free(child);
        return NULL;
    }
    n = newn;

    /* After the reallocation, we have up to 8/16 (depending on the system
     * pointer size, and the required node padding) bytes at the end, that is,
     * the additional char in the 'data' section, plus one pointer to the new
     * child, plus the padding needed in order to store addresses into aligned
     * locations.
     *
     * So if we start with the following node, having "abde" edges.
     *
     * Note:
     * - We assume 4 bytes pointer for simplicity.
     * - Each space below corresponds to one byte
     *
     * [HDR*][abde][Aptr][Bptr][Dptr][Eptr]|AUXP|
     *
     * After the reallocation we need: 1 byte for the new edge character
     * plus 4 bytes for a new child pointer (assuming 32 bit machine).
     * However after adding 1 byte to the edge char, the header + the edge
     * characters are no longer aligned, so we also need 3 bytes of padding.
     * In total the reallocation will add 1+4+3 bytes = 8 bytes:
     *
     * (Blank bytes are represented by ".")
     *
     * [HDR*][abde][Aptr][Bptr][Dptr][Eptr]|AUXP|[....][....]
     *
     * Let's find where to insert the new child in order to make sure
     * it is inserted in-place lexicographically. Assuming we are adding
     * a child "c" in our case pos will be = 2 after the end of the following
     * loop. */
    /* 由于指针需要内存对齐，所以 raxNode->data 字段的 字符区域和指针区域之前可能有一定数量的填充，填充的大小见宏 raxPadding 的计算逻辑。
     * 以 32位系统为例，
     *   1. 当存在填充时，新字符可以直接占用一个字节的填充空间，额外再需要分配 4 bytes 的空间来存放新节点指针。这时重新分配内存增加的空间为 4 bytes；
     *   2. 若此时填充大小为 0，新增的一个字符需要额外 3 bytes 的填充来保证后面的指针时内存对齐的，此时就增加的空间最多，为 1+3+4 = 8 bytes
     * 同理可计算 64 位系统增加的内存最多为 16 bytes。
     * 新节点的字符需要按照字典序存储在父节点 n 的指定位置，如何确定这个位置，需要对 raxNode->data 字段的内存结构有一定了解 */
    /* 确定字符和指针应该存放在第几个位置，
     * 下面会分别对 value 指针、孩子指针、填充、字符向后移动，以便空出新字符和新指针的地址 */
    int pos;
    for (pos = 0; pos < n->size; pos++) {
        if (n->data[pos] > c) break;
    }

    /* Now, if present, move auxiliary data pointer at the end
     * so that we can mess with the other data without overwriting it.
     * We will obtain something like that:
     *
     * [HDR*][abde][Aptr][Bptr][Dptr][Eptr][....][....]|AUXP|
     */
    /* 如果当前节点是一个 key 且 value 有值，直接将 value 指针复制到最后面 */
    unsigned char *src, *dst;
    if (n->iskey && !n->isnull) {
        src = ((unsigned char*)n+curlen-sizeof(void*));
        dst = ((unsigned char*)n+newlen-sizeof(void*));
        memmove(dst,src,sizeof(void*));
    }

    /* Compute the "shift", that is, how many bytes we need to move the
     * pointers section forward because of the addition of the new child
     * byte in the string section. Note that if we had no padding, that
     * would be always "1", since we are adding a single byte in the string
     * section of the node (where now there is "abde" basically).
     *
     * However we have padding, so it could be zero, or up to 8.
     *
     * Another way to think at the shift is, how many bytes we need to
     * move child pointers forward *other than* the obvious sizeof(void*)
     * needed for the additional pointer itself. */
    /* shift 的值就是增加的填充大小，32位系统可能为 0 或者 4 bytes，64位系统可能为 0 或者 8 bytes */
    size_t shift = newlen - curlen - sizeof(void*);

    /* We said we are adding a node with edge 'c'. The insertion
     * point is between 'b' and 'd', so the 'pos' variable value is
     * the index of the first child pointer that we need to move forward
     * to make space for our new pointer.
     *
     * To start, move all the child pointers after the insertion point
     * of shift+sizeof(pointer) bytes on the right, to obtain:
     *
     * [HDR*][abde][Aptr][Bptr][....][....][Dptr][Eptr]|AUXP|
     */
    /* 将比新节点的字典序大的所有子节点指针向后移动 shift + sizeof(raxNode*) 个字节，其实就是 newlen - curlen 个字节 */
    src = n->data+n->size+
          raxPadding(n->size)+
          sizeof(raxNode*)*pos;
    memmove(src+shift+sizeof(raxNode*),src,sizeof(raxNode*)*(n->size-pos));

    /* Move the pointers to the left of the insertion position as well. Often
     * we don't need to do anything if there was already some padding to use. In
     * that case the final destination of the pointers will be the same, however
     * in our example there was no pre-existing padding, so we added one byte
     * plus there bytes of padding. After the next memmove() things will look
     * like that:
     *
     * [HDR*][abde][....][Aptr][Bptr][....][Dptr][Eptr]|AUXP|
     */
    /* 如果本次扩容增加了填充空间，将比新节点的字典序小的所有子节点指针向后移动 shift 个字节 */
    if (shift) {
        src = (unsigned char*) raxNodeFirstChildPtr(n);
        memmove(src+shift,src,sizeof(raxNode*)*pos);
    }

    /* Now make the space for the additional char in the data section,
     * but also move the pointers before the insertion point to the right
     * by shift bytes, in order to obtain the following:
     *
     * [HDR*][ab.d][e...][Aptr][Bptr][....][Dptr][Eptr]|AUXP|
     */
    /* 当前已经确保字符区域后面已经空出了足够的安全空间，将 c 后面的所有字符后移 1 byte */
    src = n->data+pos;
    memmove(src+1,src,n->size-pos);

    /* We can now set the character and its child node pointer to get:
     *
     * [HDR*][abcd][e...][Aptr][Bptr][....][Dptr][Eptr]|AUXP|
     * [HDR*][abcd][e...][Aptr][Bptr][Cptr][Dptr][Eptr]|AUXP|
     */
    /* 目前可以安全的将新字符和新节点地址写入腾挪空出的位置 */
    n->data[pos] = c;
    n->size++;
    /* 对于指针区域，后面的指针向后移动了 shift + sizeof(raxNode*) 个字节，前面的指针向后移动了 shift 个字节，中间正好空出了一个存放指针的空间
     * 把新节点的地址拷贝到这个空间 */
    src = (unsigned char*) raxNodeFirstChildPtr(n);
    raxNode **childfield = (raxNode**)(src+sizeof(raxNode*)*pos);
    memcpy(childfield,&child,sizeof(child));
    /* 将新节点的地址和对应父节点中的指针地址写回方法调用方 */
    *childptr = child;
    *parentlink = childfield;
    return n;
}

/* Turn the node 'n', that must be a node without any children, into a
 * compressed node representing a set of nodes linked one after the other
 * and having exactly one child each. The node can be a key or not: this
 * property and the associated value if any will be preserved.
 *
 * The function also returns a child node, since the last node of the
 * compressed chain cannot be part of the chain: it has zero children while
 * we can only compress inner nodes with exactly one child each. */
/* 将字符序列 s 以压缩节点的形式放入节点 n 中，方法返回节点 n 的新地址。
 * 方法要求节点 n 一定没有任何子节点（即 n 为叶子节点），字符序列 s 放入 n 后，需要重新创建一个叶子节点作为 n 的子节点，
 * 来表示以 s 为后缀的字符序列，二级指针 child 用来记录新创建的叶子节点地址 */
raxNode *raxCompressNode(raxNode *n, unsigned char *s, size_t len, raxNode **child) {
    assert(n->size == 0 && n->iscompr == 0);
    void *data = NULL; /* Initialized only to avoid warnings. */
    size_t newsize;

    debugf("Compress node: %.*s\n", (int)len,s);

    /* Allocate the child to link to this node. */
    *child = raxNewNode(0,0);
    if (*child == NULL) return NULL;

    /* Make space in the parent node. */
    newsize = sizeof(raxNode)+len+raxPadding(len)+sizeof(raxNode*);
    if (n->iskey) {
        data = raxGetData(n); /* To restore it later. */
        if (!n->isnull) newsize += sizeof(void*);
    }
    raxNode *newn = rax_realloc(n,newsize);
    if (newn == NULL) {
        rax_free(*child);
        return NULL;
    }
    n = newn;

    n->iscompr = 1;
    n->size = len;
    memcpy(n->data,s,len);
    if (n->iskey) raxSetData(n,data);
    raxNode **childfield = raxNodeLastChildPtr(n);
    memcpy(childfield,child,sizeof(*child));
    return n;
}

/* Low level function that walks the tree looking for the string
 * 's' of 'len' bytes. The function returns the number of characters
 * of the key that was possible to process: if the returned integer
 * is the same as 'len', then it means that the node corresponding to the
 * string was found (however it may not be a key in case the node->iskey is
 * zero or if simply we stopped in the middle of a compressed node, so that
 * 'splitpos' is non zero).
 *
 * Otherwise if the returned integer is not the same as 'len', there was an
 * early stop during the tree walk because of a character mismatch.
 *
 * The node where the search ended (because the full string was processed
 * or because there was an early stop) is returned by reference as
 * '*stopnode' if the passed pointer is not NULL. This node link in the
 * parent's node is returned as '*plink' if not NULL. Finally, if the
 * search stopped in a compressed node, '*splitpos' returns the index
 * inside the compressed node where the search ended. This is useful to
 * know where to split the node for insertion.
 *
 * Note that when we stop in the middle of a compressed node with
 * a perfect match, this function will return a length equal to the
 * 'len' argument (all the key matched), and will return a *splitpos which is
 * always positive (that will represent the index of the character immediately
 * *after* the last match in the current compressed node).
 *
 * When instead we stop at a compressed node and *splitpos is zero, it
 * means that the current node represents the key (that is, none of the
 * compressed node characters are needed to represent the key, just all
 * its parents nodes). */
/* 在 rax 树中查找字符序列 s 的底层方法，在增删查改中均有调用
 * 方法如入参:
 *   rax: 待查找的 rax 树
 *   s/len: 待查找的字符序列（地址以及长度）
 *   stopnode: 查找过程中的终止节点，可能查找到这个节点时，待查找的字符序列匹配完成；也可能是第一个无法与待查找的序列匹配的节点
 *   plink: 三级指针，在父节点中有一个指向 stopnode 节点的指针，这个三级指针用于记录该指针的地址返回给方法调用方
 *   splitpos: 如果查找过程在一个压缩节点终止，这个二级指针用于记录该节点中压缩字符串的匹配位置，用于压缩节点后续的切割或插入。
 *   ts: 辅助栈，如果 ts != NULL，记录查找字符序列的路径。
 *
 * 方法返回序列 s 成功匹配的长度，设 i 为返回值
 *   1. 当 i == len 时，表示树中存在一个同 s 一样的序列，但该序列可能为 key，可能不是 key，也可能停在压缩节点中间的某个元素以至于根本不可能表现为 key
 *     a. 终止节点 stopnode 是非压缩节点，根据节点的 iskey 字段判断序列是否为 key 即可
 *     b. 终止节点 stopnode 是压缩节点
 *        如果 splitnode != 0，表示匹配了压缩节点至少一个但小于 stopnode->size 个字符，按压缩节点的特点，这个序列不可能是 key；
 *        如果 splitnode == 0，根据节点的 iskey 字段判断序列是否为 key 即可
 *   2. 当 i != len 时，表示未查找到完整的序列 s。
 *
 * 在 redis rax 树的实现中，某个节点的字符不是存在本节点中，而是存在父节点中。
 * 如果换成边的概念理解：一个父节点有多个子节点，那么就对应多个边，每条边包括一个字符和一个节点指针，这些内容全部存放在父节点。
 * 子节点的数据块中并不存在它本身的字符内容。详细的解释和为什么这么做，见 raxNode 数据结构的注释。
 *
 * 如果我们有这样一颗树，要查找的序列 s = "win"，那么 stopnode 节点为 `["window"]`，splitpos = 3，树中存在序列 s，但不是 key。
 *                  ["window"] ""
 *                     \
 *                     [] "window"
 *
 * 如果将树中的序列 "win" 修改为一个 key，那么需要将树转变为如下:
 *                  ("win") ""
 *                     \
 *                   ["dow"] "win"
 *                       \
 *                       [] "window"
 * 再次查找序列 s = "win"，它会停在压缩节点 `["dow"]`，此时 splitpos = 0，树中存在序列 s，且这个序列是一个 key。
 */
static inline size_t raxLowWalk(rax *rax, unsigned char *s, size_t len, raxNode **stopnode, raxNode ***plink, int *splitpos, raxStack *ts) {
    raxNode *h = rax->head;
    raxNode **parentlink = &rax->head;

    /* i 表示当前正在匹配的字符索引，其值等价于已经匹配成功的长度，当 i == len 时，表示字符串 s 已经全部匹配完成
     * j 在压缩节点中表示第几个字符，在非压缩节点中表示第几个孩子 */
    size_t i = 0; /* Position in the string. */
    size_t j = 0; /* Position in the node children (or bytes if compressed).*/
    while(h->size && i < len) {
        debugnode("Lookup current node",h);
        unsigned char *v = h->data;

        if (h->iscompr) {
            /* 压缩节点由连续的单孩子节点序列压缩而来，它表示的是一段序列；每匹配成功一个压缩字符，代表 rax 树的逻辑深度 +1
             * j++, i++，继续比较下一对字符 */
            for (j = 0; j < h->size && i < len; j++, i++) {
                if (v[j] != s[i]) break;
            }
            /* 压缩节点未遍历完只有两种原因
             *   1. 压缩字符串太长，提前触发 i == len，虽然找到了目标序列，但 stopnode 是压缩节点且 splitpos(j) != 0
             *   2. 因 v[j] != s[i] 提前退出 for 循环，字符串 s 失配，此时 i != len，树中不存在目标序列。
             * 无论是匹配完成(1) 还是已经失配(2)，都没有再遍历的必要了，所以退出这个 while 循环
             * 由于没走 if/else 后面的逻辑，它们的 stopnode 都是当前节点 */
            if (j != h->size) break;
            /* 若 j == h->size:
             *      如果它的 child-ptr 指向的是一个非压缩节点（理论上肯定满足这个条件，如果是压缩节点，为什么不合并进当前节点呢），
             *      那么它是一个 key
             *  */
        } else {
            /*  */
            /* Even when h->size is large, linear scan provides good
             * performances compared to other approaches that are in theory
             * more sounding, like performing a binary search. */
            for (j = 0; j < h->size; j++) {
                /* 如果节点 h 的第 j 个孩子等于字符 s[i]，则退出 for 循环，准备去树的下一层找 s[i+1] */
                if (v[j] == s[i]) break;
            }
            /* 当前节点所有孩子中，没有一个等于 s[i]，查找失败，退出 while 循环，stopnode 是当前节点 */
            if (j == h->size) break;
            /* 准备匹配序列 s 的下一个字符，在下面的代码中，游标 h 会挪到表示字符 v[j] 的子节点中 */
            i++;
        }

        /* 如果 raxStack ts 传了值，以栈的形式记录查找过程中经过的每个 raxNode 节点，在节点 h 入栈之前，栈顶元素总是 h 的父节点 */
        if (ts) raxStackPush(ts,h); /* Save stack of parent nodes. */
        /* h 的第一个子节点指针地址，也可以认为 children 是一个指针数组 */
        raxNode **children = raxNodeFirstChildPtr(h);
        /* 后面用 j 的值作为指针数组的索引，如果 h 是压缩节点，那么它只有一个子节点，这个子节点指针的索引为 0
         * 补充以下，如果从 `if (h->iscompr)` 这个条件分支到达了这个位置，那么 j 必定等于 h-size */
        if (h->iscompr) j = 0; /* Compressed node only child is at index 0. */
        /* 跳转到第 j 个子节点中, 即 rax 树的下一层 */
        memcpy(&h,children+j,sizeof(h));
        /* 随时更新节点 h 在父节点中，对应子指针的地址 */
        parentlink = children+j;
        /* 复位 j，刚刚我们跳到了子节点中，在 i == len 的情况下，
         *   如果新节点不是压缩节点，splitpos 对外没有意义，也不会做赋值动作，j 不复位也无所谓
         *   如果新节点是压缩节点，splitpos 此时应该为 0，j 才有必要复位
         * 所以英文注释的 'non compressed' 应该是 'compressed'，被改错了 */
        j = 0; /* If the new node is non compressed and we do not
                  iterate again (since i == len) set the split
                  position to 0 to signal this node represents
                  the searched key. */
    }
    debugnode("Lookup stop node is",h);
    if (stopnode) *stopnode = h;
    /* 将 stopnode 在父节点中 对应指针的地址 写回方法调用方，以便 stopnode 因为扩容导致地址变动的时候，可以直接修改这个指针的值 */
    if (plink) *plink = parentlink;
    if (splitpos && h->iscompr) *splitpos = j;
    /* 返回字符串 s 已经被成功匹配的长度 */
    return i;
}

/* Insert the element 's' of size 'len', setting as auxiliary data
 * the pointer 'data'. If the element is already present, the associated
 * data is updated (only if 'overwrite' is set to 1), and 0 is returned,
 * otherwise the element is inserted and 1 is returned. On out of memory the
 * function returns 0 as well but sets errno to ENOMEM, otherwise errno will
 * be set to 0.
 */
/* 向 rax 树中插入字符序列 s，并将其设置为 key。
 * 该方法是两个插入方法的底层实现，最开始使用 raxLowWalk() 查找一次序列 s，根据几个返回值可以归类为下面几类情况
 *
 *   1. rax 中存在完整的序列 s，且这个序列就是从 根节点 到 stopnode 所有边组成的字符序列，等价于 (!stopnode->iscompr || splitpos == 0)
 *      此时不需要插入任何字符，也不需要分割任何节点。记录 kv 后方法结束。
 *   2. rax 中不存在完整的序列 s，stopnode 是压缩节点，splitpos != 0；
 *      失配的位置在压缩节点中间，用 ALGO 1 的逻辑将压缩节点切割，切割后符合条件 4，继续执行 4 的逻辑
 *   3. rax 中存在完整的序列 s，stopnode 是压缩节点，且 splitpos != 0。
 *      用 ALGO 2 的逻辑将压缩字符串从 splitpos 位置切开，分为一对父子节点。将新的子节点标记为等于序列 s 的 key，记录 kv 后方法结束。
 *
 *   4. 不符合分支 1，3，或者经过分支 2 处理后的插入操作会进入这个最终步骤。
 *      此时所在的节点一定是非压缩节点，假设我们还剩 "xyz" 没匹配上，那么当前节点中需要补充一个表示字符 'x' 的孩子，
 *      孩子的指针指向新创建的压缩节点，压缩字符串为 "xyz"，最后再创建一个叶子节点，作为压缩节点的孩子。
 *      叶子节点的字符序列就是 s，记录 kv 后方法结束。
 *
 * 方法如参：overwrite 表示是否覆盖原来的 value。
 */
int raxGenericInsert(rax *rax, unsigned char *s, size_t len, void *data, void **old, int overwrite) {
    size_t i;
    /* 切割位置，raxLowWalk()方法的 splitpos */
    int j = 0; /* Split position. If raxLowWalk() stops in a compressed
                  node, the index 'j' represents the char we stopped within the
                  compressed node, that is, the position where to split the
                  node for insertion. */
    /* parentlink 是 stopnode h 在它的父节点中，对应指针的地址（引用），若节点 h 的地址改变，直接用这个引用更新指针的值 */
    raxNode *h, **parentlink;

    debugf("### Insert %.*s with value %p\n", (int)len, s, data);
    i = raxLowWalk(rax,s,len,&h,&parentlink,&j,NULL);

    /* If i == len we walked following the whole string. If we are not
     * in the middle of a compressed node, the string is either already
     * inserted or this middle node is currently not a key, but can represent
     * our key. We have just to reallocate the node and make space for the
     * data pointer. */
    /* 分支 1，rax 中存在完整的序列 s，且这个序列就是从 根节点 到 stopnode 所有边组成的字符序列
     *   i == len 表示 rax 中存在完整的序列 s
     *   stopnode (节点h) 不是压缩节点 或者 是压缩节点，但 splitpos == 0，表示节点不需要切割。
     * 此时稍加处理便可以结束方法。*/
    if (i == len && (!h->iscompr || j == 0 /* not in the middle if j is 0 */)) {
        debugf("### Insert: node representing key exists\n");
        /* Make space for the value pointer if needed. */
        /* 如果之前没有 value 指针，并且这次是覆盖插入，那么重新分配内存，以便增加一个存储 value 指针的空间 */
        if (!h->iskey || (h->isnull && overwrite)) {
            h = raxReallocForData(h,data);
            if (h) memcpy(parentlink,&h,sizeof(h));
        }
        if (h == NULL) {
            errno = ENOMEM;
            return 0;
        }

        /* Update the existing key if there is already one. */
        /* 如果之前已经是存在 kv 对，则记录旧 value，并且在覆盖插入的场景下，写入新 value */
        if (h->iskey) {
            if (old) *old = raxGetData(h);
            if (overwrite) raxSetData(h,data);
            errno = 0;
            return 0; /* Element already exists. */
        }

        /* Otherwise set the node as a key. Note that raxSetData()
         * will set h->iskey. */
        /* 如果当前表示的字符序列之前不是一个 key，则只写入新 value，并且将 rax 树的 key 计数器加一 */
        raxSetData(h,data);
        rax->numele++;
        return 1; /* Element inserted. */
    }

    /* 以下是分支 2 和分支 3 */
    /* If the node we stopped at is a compressed node, we need to
     * split it before to continue.
     *
     * Splitting a compressed node have a few possible cases.
     * Imagine that the node 'h' we are currently at is a compressed
     * node containing the string "ANNIBALE" (it means that it represents
     * nodes A -> N -> N -> I -> B -> A -> L -> E with the only child
     * pointer of this node pointing at the 'E' node, because remember that
     * we have characters at the edges of the graph, not inside the nodes
     * themselves.
     * 因为 rax 实现中，字符存储在父节点而不是子节点中，所有上文中的 'E' node 指压缩节点 `"SCO"`
     *
     * In order to show a real case imagine our node to also point to
     * another compressed node, that finally points at the node without
     * children, representing 'O':
     * 假设当前压缩节点 h 的压缩序列为 "ANNIBALE"，h 的子节点是另一个压缩节点 'E' node，其压缩序列为 "SCO"，
     * 最后需要提醒的是，'O' node 是叶子节点，没有任何子节点。
     *
     *     "ANNIBALE" -> "SCO" -> []
     *
     * When inserting we may face the following cases. Note that all the cases
     * require the insertion of a non compressed node with exactly two
     * children, except for the last case which just requires splitting a
     * compressed node.
     *
     * 方法描述部分讨论的 分支2 和 分支3，都需要对压缩节点的压缩序列进行分割，
     * 分割后的序列最多分成三份，按顺序装填进 压缩节点、非压缩节点、压缩节点中
     * 我们把前面的压缩节点叫 trimmed，后面的叫 postfix，中间的非压缩节点叫 split node
     *
     * 分支2 因为缺少完整的序列 s
     *   需要将原压缩节点 h 在索引 j 位置的字符 h->data[j] 单独拆出来，和匹配失败的字符 s[i] 一起放在非压缩节点 split 中。
     *   对应下面的 case 1~4
     * 分支3 情况下，已经存在完整的序列 s
     *   只需要将原压缩节点的 [0,j-1] 拆到新压缩节点 trimmed 中，[j,h->size) 拆到新压缩节点 postfix 中。
     *   对应下面的 case 5
     *
     * 1) Inserting "ANNIENTARE"
     *
     *               |B| -> "ALE" -> "SCO" -> []
     *     "ANNI" -> |-|
     *               |E| -> (... continue algo ...) "NTARE" -> []
     *
     *   "ANNI" 为 trimmed 节点，"ALE" 为 postfix 节点，"ANNIBALE" 中的字符 'B' 被单独拆出来和 s[i]('E') 组合为一个非压缩节点 split。
     *   待插入序列 "ANNIENTARE" 的剩余的字符 "NTARE" 将进入分支 4 后处理。
     *
     * 2) Inserting "ANNIBALI"
     *
     *                  |E| -> "SCO" -> []
     *     "ANNIBAL" -> |-|
     *                  |I| -> (... continue algo ...) []
     *
     *   h->data[j] 已经是原压缩节点的最后一个字符，postfixlen = 0，此时只拆成了 trimmed 和 split 两部分，
     *   后面的压缩节点 postfix 不存在，仍然需要进入分支 4 添加一个叶子节点
     *
     * 3) Inserting "AGO" (Like case 1, but set iscompr = 0 into original node)
     *
     *            |N| -> "NIBALE" -> "SCO" -> []
     *     |A| -> |-|
     *            |G| -> (... continue algo ...) |O| -> []
     *
     *   同 case 1，但 trimmedlen = 1，此时可以将 trimmed 标记为非压缩节点
     *
     * 4) Inserting "CIAO"
     *
     *     |A| -> "NNIBALE" -> "SCO" -> []
     *     |-|
     *     |C| -> (... continue algo ...) "IAO" -> []
     *
     *   raxLowWalk() 返回值 j = 0，trimmedlen = 0，只存在中间的非压缩节点 split 和后面的压缩节点 postfix
     *   如果原节点 `["ANNIBALE"]` 本来就是一个 key(假设为 "")，那么拆出去的字符 'A' 与字符 s[i]('C') 组成的节点仍应该被标记为 key
     *   树形图如下：
     *            [A         C] ""
     *            /           \
     *   ("NNIBALE") "A"      ("IAO") "C"
     *         /                 \
     *   ["SCO"] "ANNIBALE"     [] "CIAO"
     *     /
     *   [] "ANNIBALESCO"
     *
     * 5) Inserting "ANNI"
     *
     *     "ANNI" -> "BALE" -> "SCO" -> []
     *
     *   rax 树中存在完整的序列 "ANNI"，仅需要将后面的 "BALE" 拆出去，分为前面的 trimmed 和 后面的 postfix，没有中间的非压缩节点 split，
     *   树形图如下：
     *     ("ANNI") ""
     *        \
     *      ["BALE"] "ANNI"
     *          \
     *         ["SCO"] "ANNIBALE"
     *            \
     *            [] "ANNIBALESCO"
     *
     * The final algorithm for insertion covering all the above cases is as
     * follows.
     *
     * ============================= ALGO 1 =============================
     *
     * 算法 1 针对 case 1-4，即出现字符不匹配的位置，是在某个压缩节点中间
     *
     * 索引 splitpos 从 0 开始，在算法 1 的针对的情况下下，它的值表示压缩序列索引为 splitpos 位置的字符发生不匹配。
     * 比如含有序列 "ANNIBALE" 的压缩节点，若我们插入 "ANNIENTARE"，那么在索引为 4 的位置出现不匹配，splitpos 的值为 4。
     *
     * 一个压缩节点总会被切割为 3 部分，trimmed, split, postfix，按上面这个情况，分为 "ANNI", "B", "ALE"，
     * trimmedLen=4, splitLen=1, postfixLen=3，在 case 1-4，前后两段可能存在一段长度为 0，中间 splitLen 总是为 1
     * 下面是处理上述 case 的主要步骤
     *
     * 1. 保存当前压缩节点的 NEXT 指针（压缩节点唯一的子节点）
     * 2. 在未匹配位置创建切割节点 `split node`，存储压缩节点中未匹配的那个字符 'B'，这个字符只作为 split 节点的孩子之一，
     *    split 节点的另一个孩子是序列 s 中未匹配的那个字符 'E'，这个孩子将在步骤 6 处理。
     * 3. splitpos 是否为 0 (第一部分 trimmed 长度是否为 0)
     *   a. splitpos == 0，用 split 节点接替 trimed 节点的位置，作为原压缩节点的父节点的新子节点，顶替原压缩节点。
     *   b. splitpos != 0，第一部分 trimmed 节点，作为原压缩节点的父节点的新子节点，顶替原压缩节点；并设置 split 为 trimmed 的子节点。
     *      如果 trimmedlen == 1，将其标记为非压缩节点 (对应上文中的 case 3)
     * 4. 第三部分 postfix 长度是否为 0
     *   a. postfixlen ！= 0，NEXT 节点设置为 postfix 的子节点。如果 postfixlen == 0，将其标记为非压缩节点
     *   b. postfixlen == 0，原压缩节点切割为两部分，公共前缀 trimmed 和 split node，将 NEXT 节点作为 postfix。
     * 5. 将 postfix 设置为 split 索引为 0 的子节点 (这是 split 的第一个孩子，之后序列 s 中那个未匹配字符将作为第二个孩子放在合适的位置)
     * 6. 此时 h's parent -> trimmed -> split -> postfix -> NEXT 已经串起来了，将当前节点设置为 split 节点，继续处理序列 s 中剩下
     *    未匹配的字符。
     *
     * For the above cases 1 to 4, that is, all cases where we stopped in
     * the middle of a compressed node for a character mismatch, do:
     *
     * Let $SPLITPOS be the zero-based index at which, in the
     * compressed node array of characters, we found the mismatching
     * character. For example if the node contains "ANNIBALE" and we add
     * "ANNIENTARE" the $SPLITPOS is 4, that is, the index at which the
     * mismatching character is found.
     *
     * 1. Save the current compressed node $NEXT pointer (the pointer to the
     *    child element, that is always present in compressed nodes).
     *
     * 2. Create "split node" having as child the non common letter
     *    at the compressed node. The other non common letter (at the key)
     *    will be added later as we continue the normal insertion algorithm
     *    at step "6".
     *
     * 3a. IF $SPLITPOS == 0:
     *     Replace the old node with the split node, by copying the auxiliary
     *     data if any. Fix parent's reference. Free old node eventually
     *     (we still need its data for the next steps of the algorithm).
     *
     * 3b. IF $SPLITPOS != 0:
     *     Trim the compressed node (reallocating it as well) in order to
     *     contain $splitpos characters. Change child pointer in order to link
     *     to the split node. If new compressed node len is just 1, set
     *     iscompr to 0 (layout is the same). Fix parent's reference.
     *
     * 4a. IF the postfix len (the length of the remaining string of the
     *     original compressed node after the split character) is non zero,
     *     create a "postfix node". If the postfix node has just one character
     *     set iscompr to 0, otherwise iscompr to 1. Set the postfix node
     *     child pointer to $NEXT.
     *
     * 4b. IF the postfix len is zero, just use $NEXT as postfix pointer.
     *
     * 5. Set child[0] of split node to postfix node.
     *
     * 6. Set the split node as the current node, set current index at child[1]
     *    and continue insertion algorithm as usually.
     *
     * ============================= ALGO 2 =============================
     * 算法 2 针对 case 5，即在树中找到了字符序列 s，但匹配终点恰好在压缩节点中间；
     * 这种情况采用分支 3 的逻辑处理，只分成 trimmed 和 postfix 两个部分，做好 key 的标记和 value 信息的处理后，直接退出方法。
     *
     * For case 5, that is, if we stopped in the middle of a compressed
     * node but no mismatch was found, do:
     *
     * Let $SPLITPOS be the zero-based index at which, in the
     * compressed node array of characters, we stopped iterating because
     * there were no more keys character to match. So in the example of
     * the node "ANNIBALE", adding the string "ANNI", the $SPLITPOS is 4.
     *
     * 1. Save the current compressed node $NEXT pointer (the pointer to the
     *    child element, that is always present in compressed nodes).
     *
     * 2. Create a "postfix node" containing all the characters from $SPLITPOS
     *    to the end. Use $NEXT as the postfix node child pointer.
     *    If the postfix node length is 1, set iscompr to 0.
     *    Set the node as a key with the associated value of the new
     *    inserted key.
     *
     * 3. Trim the current node to contain the first $SPLITPOS characters.
     *    As usually if the new node length is just 1, set iscompr to 0.
     *    Take the iskey / associated value as it was in the original node.
     *    Fix the parent's reference.
     *
     * 4. Set the postfix node as the only child pointer of the trimmed
     *    node created at step 1.
     */

    /* ------------------------- ALGORITHM 1 --------------------------- */
    if (h->iscompr && i != len) {
    /* 分支2: 在压缩节点中间失配 */
        debugf("ALGO 1: Stopped at compressed node %.*s (%p)\n",
            h->size, h->data, (void*)h);
        debugf("Still to insert: %.*s\n", (int)(len-i), s+i);
        debugf("Splitting at %d: '%c'\n", j, ((char*)h->data)[j]);
        debugf("Other (key) letter is '%c'\n", s[i]);

        /* 1: Save next pointer. */
        /* 1: 保存压缩节点的孩子指针 */
        raxNode **childfield = raxNodeLastChildPtr(h);
        raxNode *next;
        memcpy(&next,childfield,sizeof(next));
        debugf("Next is %p\n", (void*)next);
        debugf("iskey %d\n", h->iskey);
        if (h->iskey) {
            debugf("key value is %p\n", raxGetData(h));
        }

        /* Set the length of the additional nodes we will need. */
        size_t trimmedlen = j;
        size_t postfixlen = h->size - j - 1;
        /* 如果 j == 0 且原压缩节点之前表示一个 key，后面新创建的 splitnode 也应该是一个表示 key 的节点
         * 在创建 split 节点时，split_node_is_key 用于判断是否需要预留 value 指针的存储空间 */
        int split_node_is_key = !trimmedlen && h->iskey && !h->isnull;
        size_t nodesize;

        /* 2: Create the split node. Also allocate the other nodes we'll need
         *    ASAP, so that it will be simpler to handle OOM. */
        /* 2: 创建中间的 split 节点，同时视情况将第一部分的 trimmed 节点和第三部分的 postfix 节点创建出来
         * split_node_is_key 表示是否需要预留 value 指针 */
        raxNode *splitnode = raxNewNode(1, split_node_is_key);
        raxNode *trimmed = NULL;
        raxNode *postfix = NULL;

        /* 处理第一部分 trimmed */
        if (trimmedlen) {
            nodesize = sizeof(raxNode)+trimmedlen+raxPadding(trimmedlen)+
                       sizeof(raxNode*);
            if (h->iskey && !h->isnull) nodesize += sizeof(void*);
            trimmed = rax_malloc(nodesize);
        }

        /* 处理第三部分，postfix */
        if (postfixlen) {
            nodesize = sizeof(raxNode)+postfixlen+raxPadding(postfixlen)+
                       sizeof(raxNode*);
            postfix = rax_malloc(nodesize);
        }

        /* OOM? Abort now that the tree is untouched. */
        if (splitnode == NULL ||
            (trimmedlen && trimmed == NULL) ||
            (postfixlen && postfix == NULL))
        {
            rax_free(splitnode);
            rax_free(trimmed);
            rax_free(postfix);
            errno = ENOMEM;
            return 0;
        }
        /* split 节点存储中间分割出来的那个字符 */
        splitnode->data[0] = h->data[j];

        if (j == 0) {
            /* 3a: Replace the old node with the split node. */
            /* 3a: trimmedlen == 0，
             * 把 split 节点当 trimmed 操作，原压缩节点是个 key 的情况下，将原节点的 value 指针拷贝到新节点 split */
            if (h->iskey) {
                void *ndata = raxGetData(h);
                raxSetData(splitnode,ndata);
            }
            /* 用 split 节点替代原节点 h，parentlink 的值是节点 h 在其父节点中 指针的地址，修改这个指针使其指向 split */
            memcpy(parentlink,&splitnode,sizeof(splitnode));
        } else {
            /* 3b: Trim the compressed node. */
            /* 3b: trimedlen != 0，用 trimmed 节点替代原节点 h，继承 h 的内容；split 作为 trimmed 的子节点 */
            trimmed->size = j;
            memcpy(trimmed->data,h->data,j);
            trimmed->iscompr = j > 1 ? 1 : 0;
            trimmed->iskey = h->iskey;
            trimmed->isnull = h->isnull;
            if (h->iskey && !h->isnull) {
                void *ndata = raxGetData(h);
                raxSetData(trimmed,ndata);
            }
            /* 将 split 节点作为 trimmed 节点的子节点，trimmed 节点作为原 h 父节点的子节点 */
            raxNode **cp = raxNodeLastChildPtr(trimmed);
            memcpy(cp,&splitnode,sizeof(splitnode));
            memcpy(parentlink,&trimmed,sizeof(trimmed));
            parentlink = cp; /* Set parentlink to splitnode parent. */
            rax->numnodes++;
        }

        /* 4: Create the postfix node: what remains of the original
         * compressed node after the split. */
        /* 4: 处理 postfix，将最开始保存的 NEXT 指针作为 postfix 节点的孩子指针 */
        if (postfixlen) {
            /* 4a: create a postfix node. */
            /* 4a: postfixlen != 0，处理分割的第三部分，将原 h 节点剩下的序列 [j+1, h->size) 保存进 postfix */
            postfix->iskey = 0;
            postfix->isnull = 0;
            postfix->size = postfixlen;
            postfix->iscompr = postfixlen > 1;
            memcpy(postfix->data,h->data+j+1,postfixlen);
            /* 如果第三部分割下来的长度大于 0, 将最开始保存的 NEXT 指针作为它的孩子指针，(NEXT 本来是原 h 节点的孩子) */
            raxNode **cp = raxNodeLastChildPtr(postfix);
            memcpy(cp,&next,sizeof(next));
            rax->numnodes++;
        } else {
            /* 4b: just use next as postfix node. */
            /* 4b: 第三部分的长度为 0，将原 h 节点的孩子作为 postfix */
            postfix = next;
        }

        /* 5: Set splitnode first child as the postfix node. */
        /* 5. 第三部分 postfix 作为中间的 split 节点的孩子 */
        raxNode **splitchild = raxNodeLastChildPtr(splitnode);
        memcpy(splitchild,&postfix,sizeof(postfix));

        /* 6. Continue insertion: this will cause the splitnode to
         * get a new child (the non common character at the currently
         * inserted key). */
        /* 6: 将当前节点设置为 split，处理字符串 s 剩下的序列 [i+1, len)
         * 目前已经将原压缩节点 h 分割为合适的 2~3 个部分，后面开始着手插入序列 s[i+1, end] 未匹配的后缀
         *
         * 以 case 1 举例，rax 树的初始状态 "ANNIBALE" -> "SCO" -> []，假设我们要插入序列 s = "ANNIENTARE"，目前在这种状态：
         *
         *     "ANNI" -> |B| -> "ALE" -> "SCO" -> []
         *
         * "ANNI" 是压缩节点 trimmed，|B| 是非压缩节点 split，"ALE" 是压缩节点 postfix。
         * 距离最终的目标:
         *
         *             |B| -> "ALE" -> "SCO" -> []
         *   "ANNI" -> |-|
         *             |E| -> (... continue algo ...) "NTARE" -> []
         *
         * 还需要继续插入序列缺少的后缀 "ENTARE"，剩下的这部分进入方法头讨论的分支 4 继续处理 */
        rax_free(h);
        h = splitnode;
    } else if (h->iscompr && i == len) {
    /* ------------------------- ALGORITHM 2 --------------------------- */
    /* 对应分支 3：存在的完整序列 s，但正好停在压缩节点中间
     * 这块逻辑只对应 case 5，在 "ANNIBALE" -> "SCO" -> [] 插入 "ANNI"
     * 最终目标：
     *
     *   "ANNI" -> "BALE" -> "SCO" -> []
     *
     * 只需要第一部分 trimmed，第三部分 postfix，并没有因为匹配失败产生的 split 部分
     * 也不需要继续插入缺少的后缀，这块逻辑处理完后方法直接结束 */
        debugf("ALGO 2: Stopped at compressed node %.*s (%p) j = %d\n",
            h->size, h->data, (void*)h, j);

        /* Allocate postfix & trimmed nodes ASAP to fail for OOM gracefully. */
        size_t postfixlen = h->size - j;
        size_t nodesize = sizeof(raxNode)+postfixlen+raxPadding(postfixlen)+
                          sizeof(raxNode*);
        if (data != NULL) nodesize += sizeof(void*);
        raxNode *postfix = rax_malloc(nodesize);

        nodesize = sizeof(raxNode)+j+raxPadding(j)+sizeof(raxNode*);
        if (h->iskey && !h->isnull) nodesize += sizeof(void*);
        raxNode *trimmed = rax_malloc(nodesize);

        if (postfix == NULL || trimmed == NULL) {
            rax_free(postfix);
            rax_free(trimmed);
            errno = ENOMEM;
            return 0;
        }

        /* 1: Save next pointer. */
        raxNode **childfield = raxNodeLastChildPtr(h);
        raxNode *next;
        memcpy(&next,childfield,sizeof(next));

        /* 2: Create the postfix node. */
        postfix->size = postfixlen;
        postfix->iscompr = postfixlen > 1;
        postfix->iskey = 1;
        postfix->isnull = 0;
        memcpy(postfix->data,h->data+j,postfixlen);
        raxSetData(postfix,data);
        raxNode **cp = raxNodeLastChildPtr(postfix);
        memcpy(cp,&next,sizeof(next));
        rax->numnodes++;

        /* 3: Trim the compressed node. */
        trimmed->size = j;
        trimmed->iscompr = j > 1;
        trimmed->iskey = 0;
        trimmed->isnull = 0;
        memcpy(trimmed->data,h->data,j);
        memcpy(parentlink,&trimmed,sizeof(trimmed));
        if (h->iskey) {
            void *aux = raxGetData(h);
            raxSetData(trimmed,aux);
        }

        /* Fix the trimmed node child pointer to point to
         * the postfix node. */
        cp = raxNodeLastChildPtr(trimmed);
        memcpy(cp,&postfix,sizeof(postfix));

        /* Finish! We don't need to continue with the insertion
         * algorithm for ALGO 2. The key is already inserted. */
        rax->numele++;
        rax_free(h);
        return 1; /* Key inserted. */
    }

    /* We walked the radix tree as far as we could, but still there are left
     * chars in our string. We need to insert the missing nodes. */
    /* 分支 4，循环添加 rax 中字符串 s 剩下的序列，当前的 h 节点一定是一个非压缩节点 */
    while(i < len) {
        raxNode *child;

        /* If this node is going to have a single child, and there
         * are other characters, so that that would result in a chain
         * of single-childed nodes, turn it into a compressed node. */
        if (h->size == 0 && len-i > 1) {
        /* 如果当前节点 h 的孩子数量为 0 (叶子节点)，并且 rax 中缺少的序列长度大于 1，
         * 直接将缺少的序列放入一个压缩节点中 (如果按照普通的插入方法，将构造一串连续的单孩子节点) */
            debugf("Inserting compressed node\n");
            size_t comprsize = len-i;
            if (comprsize > RAX_NODE_MAX_SIZE)
                comprsize = RAX_NODE_MAX_SIZE;
            raxNode *newh = raxCompressNode(h,s+i,comprsize,&child);
            if (newh == NULL) goto oom;
            h = newh;
            memcpy(parentlink,&h,sizeof(h));
            parentlink = raxNodeLastChildPtr(h);
            i += comprsize;
        } else {
        /* 节点 h 至少有一个孩子 (分支 2 结束时总是这种状况)，也可能缺少的序列长度仅剩一
         * 这两种情况都只能将当前字符 s[i] 作为当前节点 h 的孩子存入子节点列表中
         * 扩容的时候，为新增的孩子创建了一个叶子节点，完成这次循环后，重新进入的时候可能符合第一个 if 语句 h->size == 0 的情况 */
            debugf("Inserting normal node\n");
            raxNode **new_parentlink;
            /* 为了增加一个存放孩子的空间，扩容，并增加一个对应这个孩子的叶子节点 */
            raxNode *newh = raxAddChild(h,s[i],&child,&new_parentlink);
            if (newh == NULL) goto oom;
            h = newh;
            memcpy(parentlink,&h,sizeof(h));
            parentlink = new_parentlink;
            i++;
        }
        /* rax 树的节点数量 +1 */
        rax->numnodes++;
        h = child;
    }
    /* 最后添加一个叶子节点作为表示序列 s 的 key，重新分配下内存，留出存在 value 指针的空间 */
    raxNode *newh = raxReallocForData(h,data);
    if (newh == NULL) goto oom;
    h = newh;
    /* key 的数量 +1 */
    if (!h->iskey) rax->numele++;
    raxSetData(h,data);
    memcpy(parentlink,&h,sizeof(h));
    return 1; /* Element inserted. */

oom:
    /* This code path handles out of memory after part of the sub-tree was
     * already modified. Set the node as a key, and then remove it. However we
     * do that only if the node is a terminal node, otherwise if the OOM
     * happened reallocating a node in the middle, we don't need to free
     * anything. */
    if (h->size == 0) {
        h->isnull = 1;
        h->iskey = 1;
        rax->numele++; /* Compensate the next remove. */
        assert(raxRemove(rax,s,i,NULL) != 0);
    }
    errno = ENOMEM;
    return 0;
}

/* Overwriting insert. Just a wrapper for raxGenericInsert() that will
 * update the element if there is already one for the same key. */
/* 向 rax 插入一对 kv（s 和 data），如果 kv 已存在，覆盖原有的 value */
int raxInsert(rax *rax, unsigned char *s, size_t len, void *data, void **old) {
    return raxGenericInsert(rax,s,len,data,old,1);
}

/* Non overwriting insert function: if an element with the same key
 * exists, the value is not updated and the function returns 0.
 * This is just a wrapper for raxGenericInsert(). */
/* 向 rax 插入一对 kv（s 和 data），如果 kv 已存在，不覆盖原有的 value */
int raxTryInsert(rax *rax, unsigned char *s, size_t len, void *data, void **old) {
    return raxGenericInsert(rax,s,len,data,old,0);
}

/* Find a key in the rax, returns raxNotFound special void pointer value
 * if the item was not found, otherwise the value associated with the
 * item is returned. */
/* 在 rax 中查找字符序列为 s 的 key；若 key 存在，返回对应的 value 指针，否则返回常量 raxNotFound */
void *raxFind(rax *rax, unsigned char *s, size_t len) {
    raxNode *h;

    debugf("### Lookup: %.*s\n", (int)len, s);
    int splitpos = 0;
    size_t i = raxLowWalk(rax,s,len,&h,NULL,&splitpos,NULL);
    if (i != len || (h->iscompr && splitpos != 0) || !h->iskey)
        return raxNotFound;
    // 压缩节点可以是一个 key, 但不能匹配压缩序列中的任何字符，匹配了就处在压缩节点中。
    return raxGetData(h);
}

/* Return the memory address where the 'parent' node stores the specified
 * 'child' pointer, so that the caller can update the pointer with another
 * one if needed. The function assumes it will find a match, otherwise the
 * operation is an undefined behavior (it will continue scanning the
 * memory without any bound checking). */
raxNode **raxFindParentLink(raxNode *parent, raxNode *child) {
    raxNode **cp = raxNodeFirstChildPtr(parent);
    raxNode *c;
    while(1) {
        memcpy(&c,cp,sizeof(c));
        if (c == child) break;
        cp++;
    }
    return cp;
}

/* Low level child removal from node. The new node pointer (after the child
 * removal) is returned. Note that this function does not fix the pointer
 * of the parent node in its parent, so this task is up to the caller.
 * The function never fails for out of memory. */
raxNode *raxRemoveChild(raxNode *parent, raxNode *child) {
    debugnode("raxRemoveChild before", parent);
    /* If parent is a compressed node (having a single child, as for definition
     * of the data structure), the removal of the child consists into turning
     * it into a normal node without children. */
    if (parent->iscompr) {
        void *data = NULL;
        if (parent->iskey) data = raxGetData(parent);
        parent->isnull = 0;
        parent->iscompr = 0;
        parent->size = 0;
        if (parent->iskey) raxSetData(parent,data);
        debugnode("raxRemoveChild after", parent);
        return parent;
    }

    /* Otherwise we need to scan for the child pointer and memmove()
     * accordingly.
     *
     * 1. To start we seek the first element in both the children
     *    pointers and edge bytes in the node. */
    raxNode **cp = raxNodeFirstChildPtr(parent);
    raxNode **c = cp;
    unsigned char *e = parent->data;

    /* 2. Search the child pointer to remove inside the array of children
     *    pointers. */
    while(1) {
        raxNode *aux;
        memcpy(&aux,c,sizeof(aux));
        if (aux == child) break;
        c++;
        e++;
    }

    /* 3. Remove the edge and the pointer by memmoving the remaining children
     *    pointer and edge bytes one position before. */
    int taillen = parent->size - (e - parent->data) - 1;
    debugf("raxRemoveChild tail len: %d\n", taillen);
    memmove(e,e+1,taillen);

    /* Compute the shift, that is the amount of bytes we should move our
     * child pointers to the left, since the removal of one edge character
     * and the corresponding padding change, may change the layout.
     * We just check if in the old version of the node there was at the
     * end just a single byte and all padding: in that case removing one char
     * will remove a whole sizeof(void*) word. */
    size_t shift = ((parent->size+4) % sizeof(void*)) == 1 ? sizeof(void*) : 0;

    /* Move the children pointers before the deletion point. */
    if (shift)
        memmove(((char*)cp)-shift,cp,(parent->size-taillen-1)*sizeof(raxNode**));

    /* Move the remaining "tail" pointers at the right position as well. */
    size_t valuelen = (parent->iskey && !parent->isnull) ? sizeof(void*) : 0;
    memmove(((char*)c)-shift,c+1,taillen*sizeof(raxNode**)+valuelen);

    /* 4. Update size. */
    parent->size--;

    /* realloc the node according to the theoretical memory usage, to free
     * data if we are over-allocating right now. */
    raxNode *newnode = rax_realloc(parent,raxNodeCurrentLength(parent));
    if (newnode) {
        debugnode("raxRemoveChild after", newnode);
    }
    /* Note: if rax_realloc() fails we just return the old address, which
     * is valid. */
    return newnode ? newnode : parent;
}

/* Remove the specified item. Returns 1 if the item was found and
 * deleted, 0 otherwise. */
/* 在 rax 树中删除长度为 len 的字符序列 s */
int raxRemove(rax *rax, unsigned char *s, size_t len, void **old) {
    raxNode *h;
    raxStack ts;

    debugf("### Delete: %.*s\n", (int)len, s);
    raxStackInit(&ts);
    int splitpos = 0;
    size_t i = raxLowWalk(rax,s,len,&h,NULL,&splitpos,&ts);
    if (i != len || (h->iscompr && splitpos != 0) || !h->iskey) {
        raxStackFree(&ts);
        return 0;
    }
    if (old) *old = raxGetData(h);
    h->iskey = 0;
    rax->numele--;

    /* If this node has no children, the deletion needs to reclaim the
     * no longer used nodes. This is an iterative process that needs to
     * walk the three upward, deleting all the nodes with just one child
     * that are not keys, until the head of the rax is reached or the first
     * node with more than one child is found. */

    int trycompress = 0; /* Will be set to 1 if we should try to optimize the
                            tree resulting from the deletion. */

    if (h->size == 0) {
        debugf("Key deleted in node without children. Cleanup needed.\n");
        raxNode *child = NULL;
        while(h != rax->head) {
            child = h;
            debugf("Freeing child %p [%.*s] key:%d\n", (void*)child,
                (int)child->size, (char*)child->data, child->iskey);
            rax_free(child);
            rax->numnodes--;
            h = raxStackPop(&ts);
             /* If this node has more then one child, or actually holds
              * a key, stop here. */
            if (h->iskey || (!h->iscompr && h->size != 1)) break;
        }
        if (child) {
            debugf("Unlinking child %p from parent %p\n",
                (void*)child, (void*)h);
            raxNode *new = raxRemoveChild(h,child);
            if (new != h) {
                raxNode *parent = raxStackPeek(&ts);
                raxNode **parentlink;
                if (parent == NULL) {
                    parentlink = &rax->head;
                } else {
                    parentlink = raxFindParentLink(parent,h);
                }
                memcpy(parentlink,&new,sizeof(new));
            }

            /* If after the removal the node has just a single child
             * and is not a key, we need to try to compress it. */
            if (new->size == 1 && new->iskey == 0) {
                trycompress = 1;
                h = new;
            }
        }
    } else if (h->size == 1) {
        /* If the node had just one child, after the removal of the key
         * further compression with adjacent nodes is potentially possible. */
        trycompress = 1;
    }

    /* Don't try node compression if our nodes pointers stack is not
     * complete because of OOM while executing raxLowWalk() */
    if (trycompress && ts.oom) trycompress = 0;

    /* Recompression: if trycompress is true, 'h' points to a radix tree node
     * that changed in a way that could allow to compress nodes in this
     * sub-branch. Compressed nodes represent chains of nodes that are not
     * keys and have a single child, so there are two deletion events that
     * may alter the tree so that further compression is needed:
     *
     * 1) A node with a single child was a key and now no longer is a key.
     * 2) A node with two children now has just one child.
     *
     * We try to navigate upward till there are other nodes that can be
     * compressed, when we reach the upper node which is not a key and has
     * a single child, we scan the chain of children to collect the
     * compressible part of the tree, and replace the current node with the
     * new one, fixing the child pointer to reference the first non
     * compressible node.
     *
     * Example of case "1". A tree stores the keys "FOO" = 1 and
     * "FOOBAR" = 2:
     *
     *
     * "FOO" -> "BAR" -> [] (2)
     *           (1)
     *
     * After the removal of "FOO" the tree can be compressed as:
     *
     * "FOOBAR" -> [] (2)
     *
     *
     * Example of case "2". A tree stores the keys "FOOBAR" = 1 and
     * "FOOTER" = 2:
     *
     *          |B| -> "AR" -> [] (1)
     * "FOO" -> |-|
     *          |T| -> "ER" -> [] (2)
     *
     * After the removal of "FOOTER" the resulting tree is:
     *
     * "FOO" -> |B| -> "AR" -> [] (1)
     *
     * That can be compressed into:
     *
     * "FOOBAR" -> [] (1)
     */
    if (trycompress) {
        debugf("After removing %.*s:\n", (int)len, s);
        debugnode("Compression may be needed",h);
        debugf("Seek start node\n");

        /* Try to reach the upper node that is compressible.
         * At the end of the loop 'h' will point to the first node we
         * can try to compress and 'parent' to its parent. */
        raxNode *parent;
        while(1) {
            parent = raxStackPop(&ts);
            if (!parent || parent->iskey ||
                (!parent->iscompr && parent->size != 1)) break;
            h = parent;
            debugnode("Going up to",h);
        }
        raxNode *start = h; /* Compression starting node. */

        /* Scan chain of nodes we can compress. */
        size_t comprsize = h->size;
        int nodes = 1;
        while(h->size != 0) {
            raxNode **cp = raxNodeLastChildPtr(h);
            memcpy(&h,cp,sizeof(h));
            if (h->iskey || (!h->iscompr && h->size != 1)) break;
            /* Stop here if going to the next node would result into
             * a compressed node larger than h->size can hold. */
            if (comprsize + h->size > RAX_NODE_MAX_SIZE) break;
            nodes++;
            comprsize += h->size;
        }
        if (nodes > 1) {
            /* If we can compress, create the new node and populate it. */
            size_t nodesize =
                sizeof(raxNode)+comprsize+raxPadding(comprsize)+sizeof(raxNode*);
            raxNode *new = rax_malloc(nodesize);
            /* An out of memory here just means we cannot optimize this
             * node, but the tree is left in a consistent state. */
            if (new == NULL) {
                raxStackFree(&ts);
                return 1;
            }
            new->iskey = 0;
            new->isnull = 0;
            new->iscompr = 1;
            new->size = comprsize;
            rax->numnodes++;

            /* Scan again, this time to populate the new node content and
             * to fix the new node child pointer. At the same time we free
             * all the nodes that we'll no longer use. */
            comprsize = 0;
            h = start;
            while(h->size != 0) {
                memcpy(new->data+comprsize,h->data,h->size);
                comprsize += h->size;
                raxNode **cp = raxNodeLastChildPtr(h);
                raxNode *tofree = h;
                memcpy(&h,cp,sizeof(h));
                rax_free(tofree); rax->numnodes--;
                if (h->iskey || (!h->iscompr && h->size != 1)) break;
            }
            debugnode("New node",new);

            /* Now 'h' points to the first node that we still need to use,
             * so our new node child pointer will point to it. */
            raxNode **cp = raxNodeLastChildPtr(new);
            memcpy(cp,&h,sizeof(h));

            /* Fix parent link. */
            if (parent) {
                raxNode **parentlink = raxFindParentLink(parent,start);
                memcpy(parentlink,&new,sizeof(new));
            } else {
                rax->head = new;
            }

            debugf("Compressed %d nodes, %d total bytes\n",
                nodes, (int)comprsize);
        }
    }
    raxStackFree(&ts);
    return 1;
}

/* This is the core of raxFree(): performs a depth-first scan of the
 * tree and releases all the nodes found. */
/* 采用深度优先遍历的方式释放节点 n 下所有的子孙节点 */
void raxRecursiveFree(rax *rax, raxNode *n, void (*free_callback)(void*)) {
    debugnode("free traversing",n);
    int numchildren = n->iscompr ? 1 : n->size;
    raxNode **cp = raxNodeLastChildPtr(n);
    while(numchildren--) {
        raxNode *child;
        memcpy(&child,cp,sizeof(child));
        raxRecursiveFree(rax,child,free_callback);
        cp--;
    }
    debugnode("free depth-first",n);
    /* 释放一对 key-value 时的回调函数，目前这个函数总是为 NULL */
    if (free_callback && n->iskey && !n->isnull)
        free_callback(raxGetData(n));
    rax_free(n);
    rax->numnodes--;
}

/* Free a whole radix tree, calling the specified callback in order to
 * free the auxiliary data. */
/* 采用深度优先遍历的方式释放整颗 rax 树 */
void raxFreeWithCallback(rax *rax, void (*free_callback)(void*)) {
    raxRecursiveFree(rax,rax->head,free_callback);
    assert(rax->numnodes == 0);
    rax_free(rax);
}

/* Free a whole radix tree. */
void raxFree(rax *rax) {
    raxFreeWithCallback(rax,NULL);
}

/* ------------------------------- Iterator --------------------------------- */
/* rax 迭代器，用于遍历 rax 树中所有的 key，可按字典序向前向后双向遍历
 * 主要方法有：
 *   1. void raxStart()，用于初始化 raxIterator 结构。
 *   2. int raxSeek()，用于初始化迭代器的位置。
 *   3. int raxNext()，遍历下一个 key。
 *   4. int raxPrev()，遍历上一个 key。
 */

/* Initialize a Rax iterator. This call should be performed a single time
 * to initialize the iterator, and must be followed by a raxSeek() call,
 * otherwise the raxPrev()/raxNext() functions will just return EOF. */
/* 初始化 raxIterator 迭代器，初始化后的迭代器必须调用 raxSeek() 重置 flag 并确定位置 ，否则迭代器无法遍历 */
void raxStart(raxIterator *it, rax *rt) {
    /* 初始化为 EOF 状态，确保在 raxSeek() 之前无法使用 raxPrev()/raxNext() 来进行前后遍历 */
    it->flags = RAX_ITER_EOF; /* No crash if the iterator is not seeked. */
    it->rt = rt;
    it->key_len = 0;
    it->key = it->key_static_string;
    it->key_max = RAX_ITER_STATIC_LEN;
    it->data = NULL;
    it->node_cb = NULL;
    raxStackInit(&it->stack);
}

/* Append characters at the current key string of the iterator 'it'. This
 * is a low level function used to implement the iterator, not callable by
 * the user. Returns 0 on out of memory, otherwise 1 is returned. */
int raxIteratorAddChars(raxIterator *it, unsigned char *s, size_t len) {
    if (len == 0) return 1;
    if (it->key_max < it->key_len+len) {
        unsigned char *old = (it->key == it->key_static_string) ? NULL :
                                                                  it->key;
        size_t new_max = (it->key_len+len)*2;
        it->key = rax_realloc(old,new_max);
        if (it->key == NULL) {
            it->key = (!old) ? it->key_static_string : old;
            errno = ENOMEM;
            return 0;
        }
        if (old == NULL) memcpy(it->key,it->key_static_string,it->key_len);
        it->key_max = new_max;
    }
    /* Use memmove since there could be an overlap between 's' and
     * it->key when we use the current key in order to re-seek. */
    memmove(it->key+it->key_len,s,len);
    it->key_len += len;
    return 1;
}

/* Remove the specified number of chars from the right of the current
 * iterator key. */
void raxIteratorDelChars(raxIterator *it, size_t count) {
    it->key_len -= count;
}

/* Do an iteration step towards the next element. At the end of the step the
 * iterator key will represent the (new) current key. If it is not possible
 * to step in the specified direction since there are no longer elements, the
 * iterator is flagged with RAX_ITER_EOF.
 *
 * If 'noup' is true the function starts directly scanning for the next
 * lexicographically smaller children, and the current node is already assumed
 * to be the parent of the last key node, so the first operation to go back to
 * the parent will be skipped. This option is used by raxSeek() when
 * implementing seeking a non existing element with the ">" or "<" options:
 * the starting node is not a key in that particular case, so we start the scan
 * from a node that does not represent the key set.
 *
 * The function returns 1 on success or 0 on out of memory. */
/* 寻找下一个元素的底层实现 */
int raxIteratorNextStep(raxIterator *it, int noup) {
    if (it->flags & RAX_ITER_EOF) {
        return 1;
    } else if (it->flags & RAX_ITER_JUST_SEEKED) {
        it->flags &= ~RAX_ITER_JUST_SEEKED;
        return 1;
    }

    /* Save key len, stack items and the node where we are currently
     * so that on iterator EOF we can restore the current key and state. */
    size_t orig_key_len = it->key_len;
    size_t orig_stack_items = it->stack.items;
    raxNode *orig_node = it->node;

    /* noup == 0 表示向下找，noup == 1 表示向上找 */
    while(1) {
        int children = it->node->iscompr ? 1 : it->node->size;
        if (!noup && children) {
        /* 迭代器当前所在节点存在子节点，我们才能继续向下找 */
            debugf("GO DEEPER\n");
            /* Seek the lexicographically smaller key in this subtree, which
             * is the first one found always going towards the first child
             * of every successive node. */
            if (!raxStackPush(&it->stack,it->node)) return 0;
            raxNode **cp = raxNodeFirstChildPtr(it->node);
            if (!raxIteratorAddChars(it,it->node->data,
                it->node->iscompr ? it->node->size : 1)) return 0;
            memcpy(&it->node,cp,sizeof(it->node));
            /* Call the node callback if any, and replace the node pointer
             * if the callback returns true. */
            if (it->node_cb && it->node_cb(&it->node))
                memcpy(cp,&it->node,sizeof(it->node));
            /* For "next" step, stop every time we find a key along the
             * way, since the key is lexicographically smaller compared to
             * what follows in the sub-children. */
            if (it->node->iskey) {
                it->data = raxGetData(it->node);
                return 1;
            }
        } else {
        /* noup == 1 或者当前节点已经是叶子节点，则开始向上回溯 */
            /* If we finished exploring the previous sub-tree, switch to the
             * new one: go upper until a node is found where there are
             * children representing keys lexicographically greater than the
             * current key. */
            /*  */
            while(1) {
                int old_noup = noup;

                /* Already on head? Can't go up, iteration finished. */
                /* 已经到 rax 树的根节点了，无法继续向上，这棵树无法找到满足条件的 key，迭代器状态设置为 EOF，结束遍历 */
                if (!noup && it->node == it->rt->head) {
                    it->flags |= RAX_ITER_EOF;
                    it->stack.items = orig_stack_items;
                    it->key_len = orig_key_len;
                    it->node = orig_node;
                    return 1;
                }
                /* If there are no children at the current node, try parent's
                 * next child. */
                unsigned char prevchild = it->key[it->key_len-1];
                if (!noup) {
                    it->node = raxStackPop(&it->stack);
                } else {
                    noup = 0;
                }
                /* Adjust the current key to represent the node we are
                 * at. */
                int todel = it->node->iscompr ? it->node->size : 1;
                raxIteratorDelChars(it,todel);

                /* Try visiting the next child if there was at least one
                 * additional child. */
                if (!it->node->iscompr && it->node->size > (old_noup ? 0 : 1)) {
                    raxNode **cp = raxNodeFirstChildPtr(it->node);
                    int i = 0;
                    while (i < it->node->size) {
                        debugf("SCAN NEXT %c\n", it->node->data[i]);
                        if (it->node->data[i] > prevchild) break;
                        i++;
                        cp++;
                    }
                    if (i != it->node->size) {
                        debugf("SCAN found a new node\n");
                        raxIteratorAddChars(it,it->node->data+i,1);
                        if (!raxStackPush(&it->stack,it->node)) return 0;
                        memcpy(&it->node,cp,sizeof(it->node));
                        /* Call the node callback if any, and replace the node
                         * pointer if the callback returns true. */
                        if (it->node_cb && it->node_cb(&it->node))
                            memcpy(cp,&it->node,sizeof(it->node));
                        if (it->node->iskey) {
                            it->data = raxGetData(it->node);
                            return 1;
                        }
                        break;
                    }
                }
            }
        }
    }
}

/* Seek the greatest key in the subtree at the current node. Return 0 on
 * out of memory, otherwise 1. This is a helper function for different
 * iteration functions below. */
/* 在当前节点的子孙中查找最右子节点，并将迭代器移动到那个位置 */
int raxSeekGreatest(raxIterator *it) {
    while(it->node->size) {
        if (it->node->iscompr) {
            if (!raxIteratorAddChars(it,it->node->data,
                it->node->size)) return 0;
        } else {
            if (!raxIteratorAddChars(it,it->node->data+it->node->size-1,1))
                return 0;
        }
        raxNode **cp = raxNodeLastChildPtr(it->node);
        if (!raxStackPush(&it->stack,it->node)) return 0;
        memcpy(&it->node,cp,sizeof(it->node));
    }
    return 1;
}

/* Like raxIteratorNextStep() but implements an iteration step moving
 * to the lexicographically previous element. The 'noup' option has a similar
 * effect to the one of raxIteratorNextStep(). */
/* 寻找上一个元素的底层实现 */
int raxIteratorPrevStep(raxIterator *it, int noup) {
    if (it->flags & RAX_ITER_EOF) {
        return 1;
    } else if (it->flags & RAX_ITER_JUST_SEEKED) {
        it->flags &= ~RAX_ITER_JUST_SEEKED;
        return 1;
    }

    /* Save key len, stack items and the node where we are currently
     * so that on iterator EOF we can restore the current key and state. */
    size_t orig_key_len = it->key_len;
    size_t orig_stack_items = it->stack.items;
    raxNode *orig_node = it->node;

    while(1) {
        int old_noup = noup;

        /* Already on head? Can't go up, iteration finished. */
        if (!noup && it->node == it->rt->head) {
            it->flags |= RAX_ITER_EOF;
            it->stack.items = orig_stack_items;
            it->key_len = orig_key_len;
            it->node = orig_node;
            return 1;
        }

        unsigned char prevchild = it->key[it->key_len-1];
        if (!noup) {
            it->node = raxStackPop(&it->stack);
        } else {
            noup = 0;
        }

        /* Adjust the current key to represent the node we are
         * at. */
        int todel = it->node->iscompr ? it->node->size : 1;
        raxIteratorDelChars(it,todel);

        /* Try visiting the prev child if there is at least one
         * child. */
        if (!it->node->iscompr && it->node->size > (old_noup ? 0 : 1)) {
            raxNode **cp = raxNodeLastChildPtr(it->node);
            int i = it->node->size-1;
            while (i >= 0) {
                debugf("SCAN PREV %c\n", it->node->data[i]);
                if (it->node->data[i] < prevchild) break;
                i--;
                cp--;
            }
            /* If we found a new subtree to explore in this node,
             * go deeper following all the last children in order to
             * find the key lexicographically greater. */
            if (i != -1) {
                debugf("SCAN found a new node\n");
                /* Enter the node we just found. */
                if (!raxIteratorAddChars(it,it->node->data+i,1)) return 0;
                if (!raxStackPush(&it->stack,it->node)) return 0;
                memcpy(&it->node,cp,sizeof(it->node));
                /* Seek sub-tree max. */
                /* 目前的前缀是最接近目标序列但不大于它，当前节点的最右子孙节点表示的就是不大于目标序列的条件下，最大的那个 key */
                if (!raxSeekGreatest(it)) return 0;
            }
        }

        /* Return the key: this could be the key we found scanning a new
         * subtree, or if we did not find a new subtree to explore here,
         * before giving up with this node, check if it's a key itself. */
        if (it->node->iskey) {
            it->data = raxGetData(it->node);
            return 1;
        }
    }
}

/* Seek an iterator at the specified element.
 * Return 0 if the seek failed for syntax error or out of memory. Otherwise
 * 1 is returned. When 0 is returned for out of memory, errno is set to
 * the ENOMEM value. */
/* 方法参数介绍
 * it: 初始化后的迭代器
 * op: 查找操作符，支持以下操作符
 *     >, <, >=, <=, =, ^(字典序最小的 key), $(字典序最大的 key)
 * ele: 目标 key
 * len: key 的长度
 *
 * 用该方法查找之前，需要使用 raxStart() 方法对迭代器进行初始化。*/
int raxSeek(raxIterator *it, const char *op, unsigned char *ele, size_t len) {
    int eq = 0, lt = 0, gt = 0, first = 0, last = 0;

    /* 每次 seek 之前，都需要将迭代器的一些状态位重置 */
    it->stack.items = 0; /* Just resetting. Initialized by raxStart(). */
    it->flags |= RAX_ITER_JUST_SEEKED;
    it->flags &= ~RAX_ITER_EOF;
    it->key_len = 0;
    it->node = NULL;

    /* Set flags according to the operator used to perform the seek. */
    if (op[0] == '>') {
        gt = 1;
        if (op[1] == '=') eq = 1;
    } else if (op[0] == '<') {
        lt = 1;
        if (op[1] == '=') eq = 1;
    } else if (op[0] == '=') {
        eq = 1;
    } else if (op[0] == '^') {
        first = 1;
    } else if (op[0] == '$') {
        last = 1;
    } else {
        errno = 0;
        return 0; /* Error. */
    }

    /* If there are no elements, set the EOF condition immediately and
     * return. */
    if (it->rt->numele == 0) {
        it->flags |= RAX_ITER_EOF;
        return 1;
    }

    /* 查找最小的 key 被转化为大于等于 NULL 的操作 */
    if (first) {
        /* Seeking the first key greater or equal to the empty string
         * is equivalent to seeking the smaller key available. */
        return raxSeek(it,">=",NULL,0);
    }

    /* '$' 查找最大的 key 等价于查找 rax 树的最右子节点，它表示的 key 是这颗 rax 树中字典序最大的字符序列 */
    if (last) {
        /* Find the greatest key taking always the last child till a
         * final node is found. */
        it->node = it->rt->head;
        /* 移动迭代器到最右子节点 */
        if (!raxSeekGreatest(it)) return 0;
        /* 最右子节点一定是叶子节点，叶子节点一定是一个 key */
        assert(it->node->iskey);
        it->data = raxGetData(it->node);
        return 1;
    }

    /* We need to seek the specified key. What we do here is to actually
     * perform a lookup, and later invoke the prev/next key code that
     * we already use for iteration. */
    /* 第一步先查找到目标 key */
    int splitpos = 0;
    /* 从根节点开始 look up 长度为 len 的目标字符串，并将 stopnode 的值写入 it->node 字段 */
    size_t i = raxLowWalk(it->rt,ele,len,&it->node,NULL,&splitpos,&it->stack);

    /* Return OOM on incomplete stack info. */
    if (it->stack.oom) return 0;

    /* 1. 如果包含了等于操作，且 key 被找到，结束这次 seek 操作
     * 2. 如果包含了大于或小于操作
     *    a. key 未被找到，stopnode 是非压缩节点
     *    b. key 未被找到，stopnode 是压缩节点
     *    c. key 找到了，只是操作符不包含等于操作
     * 3. 如果包含了等于操作，但 key 未被找到，将迭代器设置为 EOF 状态，结束这次 seek 操作
     * */
    if (eq && i == len && (!it->node->iscompr || splitpos == 0) &&
        it->node->iskey)
    {
        /* We found our node, since the key matches and we have an
         * "equal" condition. */
        /* 迭代器根据指定的 key `ele` 找到了对应的 node，现在需要将这个 node 表示的字符序列保存起来 */
        if (!raxIteratorAddChars(it,ele,len)) return 0; /* OOM. */
        it->data = raxGetData(it->node);
    } else if (lt || gt) {
        /* 经过第一个 if 条件过滤，在当前包含大于/小于操作符的情况下，
         *   (2a, 2b) 要么 key未被找到， raxLowWalk() 只匹配成功了部分
         *   (2c) 要么完整的 key 被找到但不包含等于操作符，
         * 几种情况都需要找到下一个更大或更小的 key 来满足 seek 要求。
         *
         * raxLowWalk 匹配成功的序列长度为 i，但可能存在长度为 splitpos 的后缀存储在压缩节点中，这部分为直接舍弃
         * 迭代器目前的序列 取 ele 的前 i-splitpos 个字符。然后使用 next/prev 操作来跳转到更大或更小的 key */
        /* Exact key not found or eq flag not set. We have to set as current
         * key the one represented by the node we stopped at, and perform
         * a next/prev operation to seek. */
        raxIteratorAddChars(it, ele, i-splitpos);

        /* We need to set the iterator in the correct state to call next/prev
         * step in order to seek the desired element. */
        debugf("After initial seek: i=%d len=%d key=%.*s\n",
            (int)i, (int)len, (int)it->key_len, it->key);
        if (i != len && !it->node->iscompr) {
            /* 2a. key 未被找到，stopnode 是非压缩节点。
             * 此时第一个匹配失败的字符在一个非压缩节点中:
             *   迭代器当前节点表示的序列是目标序列的一个前缀，但这个节点的所有子节点没有一个和目标序列的下一个字符 ele[i] 相等。
             * 当前节点中的子节点字符按字典序排列
             *   如果找比 ele 更大的下一个 key，就是在第一个比 ele[i] 大的子节点的子孙中找一个最小的 key
             *   如果找比 ele 更小的上一个 key，就是在最后一个比 ele[i] 小的子节点的子孙中找一个最大的 key
             * 如果 ele[i] 比所有的子孩子都大或者都小，以致于无法找到比 ele[i] 更大/更小的子节点，那么需要回溯到当前节点的父节点，对 ele[i-1] 重复上述步骤 */
            /* If we stopped in the middle of a normal node because of a
             * mismatch, add the mismatching character to the current key
             * and call the iterator with the 'noup' flag so that it will try
             * to seek the next/prev child in the current node directly based
             * on the mismatching character. */
            if (!raxIteratorAddChars(it,ele+i,1)) return 0;
            debugf("Seek normal node on mismatch: %.*s\n",
                (int)it->key_len, (char*)it->key);

            it->flags &= ~RAX_ITER_JUST_SEEKED;
            if (lt && !raxIteratorPrevStep(it,1)) return 0;
            if (gt && !raxIteratorNextStep(it,1)) return 0;
            it->flags |= RAX_ITER_JUST_SEEKED; /* Ignore next call. */
        } else if (i != len && it->node->iscompr) {
            /* 2b. key 未被找到，stopnode 是压缩节点 */
            debugf("Compressed mismatch: %.*s\n",
                (int)it->key_len, (char*)it->key);
            /* In case of a mismatch within a compressed node. */
            /* nodechar > keychar:
             *   表示已匹配前缀加上压缩字符后的序列比 ele 大，推导出当前压缩节点的所有子孙中，任意一个 key 都比 ele 大
             * nodechar < keychar:
             *   表示已匹配前缀加上压缩字符后的序列比 ele 小，推导出当前压缩节点的所有子孙中，任意一个 key 都比 ele 小 */
            int nodechar = it->node->data[splitpos];
            int keychar = ele[i];
            it->flags &= ~RAX_ITER_JUST_SEEKED;
            /* 操作符是大于
             *   1. 如果 nodechar > keychar，表示加上压缩字符后的序列比 ele 大，直接在当前压缩节点的子孙中找一个最小的 key
             *   2. 如果 nodechar < keychar，代表压缩节点所有子孙表示的 key 都比 ele 小，需要回溯到父节点来找一个更大的前缀 */
            if (gt) {
                /* If the key the compressed node represents is greater
                 * than our seek element, continue forward, otherwise set the
                 * state in order to go back to the next sub-tree. */
                if (nodechar > keychar) {
                    if (!raxIteratorNextStep(it,0)) return 0;
                } else {
                    if (!raxIteratorAddChars(it,it->node->data,it->node->size))
                        return 0;
                    /* 回溯到父节点寻找更大的前缀， noup == 1，向上查找 */
                    if (!raxIteratorNextStep(it,1)) return 0;
                }
            }
            /* 操作符是小于
             *   1. 如果 nodechar < keychar，代表压缩节点所有子孙表示的 key 都比 ele 小，
             *      直接在当前压缩节点的子孙中找一个最大的 key，即最右子孙节点
             *   2. 如果 nodechar > keychar，表示加上压缩字符后的序列比 ele 大，需要回溯到父节点来找一个更小的前缀 */
            if (lt) {
                /* If the key the compressed node represents is smaller
                 * than our seek element, seek the greater key in this
                 * subtree, otherwise set the state in order to go back to
                 * the previous sub-tree. */
                if (nodechar < keychar) {
                    if (!raxSeekGreatest(it)) return 0;
                    it->data = raxGetData(it->node);
                } else {
                    if (!raxIteratorAddChars(it,it->node->data,it->node->size))
                        return 0;
                    /* 回溯到父节点寻找更小的前缀， noup == 1，向上查找 */
                    if (!raxIteratorPrevStep(it,1)) return 0;
                }
            }
            it->flags |= RAX_ITER_JUST_SEEKED; /* Ignore next call. */
        /* 2c. key 找到了，只是操作符不包含等于操作 */
        } else {
            debugf("No mismatch: %.*s\n",
                (int)it->key_len, (char*)it->key);
            /* If there was no mismatch we are into a node representing the
             * key, (but which is not a key or the seek operator does not
             * include 'eq'), or we stopped in the middle of a compressed node
             * after processing all the key. Continue iterating as this was
             * a legitimate key we stopped at. */
            it->flags &= ~RAX_ITER_JUST_SEEKED;
            if (it->node->iscompr && it->node->iskey && splitpos && lt) {
                /* ele 可以在 rax 树中存在完全匹配成功的序列，只是存在长度至少为 1 的后缀在压缩字符串中。
                 * 这个 iskey == 1 的压缩节点就是就是比 ele 小的，且最接近 ele 的 key。
                 * 比如 op(<), ele("foo"),
                 *     ...
                 *    /   \
                 *      ["oobar"] ("f")
                 *         ...
                 * 节点 `["oobar"]` 表示的 key "f" 就是 ele 序列的前一个 key，
                 *
                 * 给当前压缩节点起个名字叫 A，A 节点的子孙 >= "foobar"，A 的哥哥一系 >= "g" (f 的下一个字符）
                 * A 节点的祖先比 A 更短更小，A 的弟弟一系 < "f"，序列 "f" 是比序列 "foo" 小的里面最大的，节点表示的序列。
                 * 这种情况命中目标，直接返回 */

                /* If we stopped in the middle of a compressed node with
                 * perfect match, and the condition is to seek a key "<" than
                 * the specified one, then if this node is a key it already
                 * represents our match. For instance we may have nodes:
                 *
                 * "f" -> "oobar" = 1 -> "" = 2
                 *
                 * Representing keys "f" = 1, "foobar" = 2. A seek for
                 * the key < "foo" will stop in the middle of the "oobar"
                 * node, but will be our match, representing the key "f".
                 *
                 * So in that case, we don't seek backward. */
                it->data = raxGetData(it->node);
            } else {
                if (gt && !raxIteratorNextStep(it,0)) return 0;
                if (lt && !raxIteratorPrevStep(it,0)) return 0;
            }
            it->flags |= RAX_ITER_JUST_SEEKED; /* Ignore next call. */
        }
    /* 3. 如果包含了等于操作，但 key 未被找到，将迭代器设置为 EOF 状态，结束这次 seek 操作 */
    } else {
        /* If we are here just eq was set but no match was found. */
        it->flags |= RAX_ITER_EOF;
        return 1;
    }
    return 1;
}

/* Go to the next element in the scope of the iterator 'it'.
 * If EOF (or out of memory) is reached, 0 is returned, otherwise 1 is
 * returned. In case 0 is returned because of OOM, errno is set to ENOMEM. */
/* 迭代器按字典序的下一个 key，成功移动到下一个 key 返回 1，迭代器存在结束标识 EOF 或者 OOM 了返回 0
 * RAX_ITER_EOF:
 *   1. 迭代器只初始化未 seek 确定位置会标记为 EOF
 *   2. 在 rax 树中没有比目前更大的 key，也会设置 EOF 标识 */
int raxNext(raxIterator *it) {
    if (!raxIteratorNextStep(it,0)) {
        errno = ENOMEM;
        return 0;
    }
    if (it->flags & RAX_ITER_EOF) {
        errno = 0;
        return 0;
    }
    return 1;
}

/* Go to the previous element in the scope of the iterator 'it'.
 * If EOF (or out of memory) is reached, 0 is returned, otherwise 1 is
 * returned. In case 0 is returned because of OOM, errno is set to ENOMEM. */
/* 迭代器按字典序跳转到上一个 key，成功移动到上一个 key 返回 1，迭代器存在结束标识 EOF 或者 OOM 了返回 0
 * RAX_ITER_EOF:
 *   1. 迭代器只初始化未 seek 确定位置会标记为 EOF
 *   2. rax 树中没有比目前更小的 key，也会设置 EOF 标识 */
int raxPrev(raxIterator *it) {
    if (!raxIteratorPrevStep(it,0)) {
        errno = ENOMEM;
        return 0;
    }
    if (it->flags & RAX_ITER_EOF) {
        errno = 0;
        return 0;
    }
    return 1;
}

/* Perform a random walk starting in the current position of the iterator.
 * Return 0 if the tree is empty or on out of memory. Otherwise 1 is returned
 * and the iterator is set to the node reached after doing a random walk
 * of 'steps' steps. If the 'steps' argument is 0, the random walk is performed
 * using a random number of steps between 1 and two times the logarithm of
 * the number of elements.
 *
 * NOTE: if you use this function to generate random elements from the radix
 * tree, expect a disappointing distribution. A random walk produces good
 * random elements if the tree is not sparse, however in the case of a radix
 * tree certain keys will be reported much more often than others. At least
 * this function should be able to explore every possible element eventually. */
/* 从迭代器当前所在的 key 开始，根据字典序每次随机跳到树中前一个 key 或者后一个 key，返回重复 steps 次后，迭代器指向的 key
 * 如果入参 steps 的值为 0，则为它取一个随机值。
 * 该方法可用来在一颗 rax 树中随机取一个 key，但如果 rax 树比较稀疏，随机效果不太理想
 *
 * 个人认为 rax 树在逻辑是一个有序的字符串数组，每个字符串是一个 key，字符串的数量是 it->rt->numele
 * 从 rax 树中随机取一个 key 最好的方式是取一个 [0, numele) 范围内的随机数 x，迭代器从最小的 key 开始 raxNext() x 步，
 * 以此时的 key 作为 rax 树的随机元素 */
int raxRandomWalk(raxIterator *it, size_t steps) {
    if (it->rt->numele == 0) {
        it->flags |= RAX_ITER_EOF;
        return 0;
    }

    if (steps == 0) {
        size_t fle = 1+floor(log(it->rt->numele));
        fle *= 2;
        steps = 1 + rand() % fle;
    }

    raxNode *n = it->node;
    while(steps > 0 || !n->iskey) {
        int numchildren = n->iscompr ? 1 : n->size;
        int r = rand() % (numchildren+(n != it->rt->head));

        if (r == numchildren) {
            /* Go up to parent. */
            n = raxStackPop(&it->stack);
            int todel = n->iscompr ? n->size : 1;
            raxIteratorDelChars(it,todel);
        } else {
            /* Select a random child. */
            if (n->iscompr) {
                if (!raxIteratorAddChars(it,n->data,n->size)) return 0;
            } else {
                if (!raxIteratorAddChars(it,n->data+r,1)) return 0;
            }
            raxNode **cp = raxNodeFirstChildPtr(n)+r;
            if (!raxStackPush(&it->stack,n)) return 0;
            memcpy(&n,cp,sizeof(n));
        }
        if (n->iskey) steps--;
    }
    it->node = n;
    it->data = raxGetData(it->node);
    return 1;
}

/* Compare the key currently pointed by the iterator to the specified
 * key according to the specified operator. Returns 1 if the comparison is
 * true, otherwise 0 is returned. */
/* 迭代器当前 key 与入参 key 比较，支持 ==, >, >=, <, <= 这 5 种操作 */
int raxCompare(raxIterator *iter, const char *op, unsigned char *key, size_t key_len) {
    int eq = 0, lt = 0, gt = 0;

    if (op[0] == '=' || op[1] == '=') eq = 1;
    if (op[0] == '>') gt = 1;
    else if (op[0] == '<') lt = 1;
    else if (op[1] != '=') return 0; /* Syntax error. */

    size_t minlen = key_len < iter->key_len ? key_len : iter->key_len;
    int cmp = memcmp(iter->key,key,minlen);

    /* Handle == */
    if (lt == 0 && gt == 0) return cmp == 0 && key_len == iter->key_len;

    /* Handle >, >=, <, <= */
    if (cmp == 0) {
        /* 一方是另一方的前缀，双方一样长就相等，否则谁长谁大 */
        /* Same prefix: longer wins. */
        if (eq && key_len == iter->key_len) return 1;
        else if (lt) return iter->key_len < key_len;
        else if (gt) return iter->key_len > key_len;
        else return 0; /* Avoid warning, just 'eq' is handled before. */
    } else if (cmp > 0) {
        return gt ? 1 : 0;
    } else /* (cmp < 0) */ {
        return lt ? 1 : 0;
    }
}

/* Free the iterator. */
void raxStop(raxIterator *it) {
    if (it->key != it->key_static_string) rax_free(it->key);
    raxStackFree(&it->stack);
}

/* Return if the iterator is in an EOF state. This happens when raxSeek()
 * failed to seek an appropriate element, so that raxNext() or raxPrev()
 * will return zero, or when an EOF condition was reached while iterating
 * with raxNext() and raxPrev(). */
/* 添加结束标记
 * 刚 raxStart() 初始化的迭代器自带 EOF 标记
 * 当 raxSeek() 无法找到一个符合要求的目标元素的时候，调用此方法设置一个 EOF 标记 */
int raxEOF(raxIterator *it) {
    return it->flags & RAX_ITER_EOF;
}

/* Return the number of elements inside the radix tree. */
/* 返回 rax 树中 key 的数量 */
uint64_t raxSize(rax *rax) {
    return rax->numele;
}

/* ----------------------------- Introspection ------------------------------ */

/* 以下方法主要用于辅助开发和 debug:
 *   1. redisShow(rax *rax) 用于打印整颗 rax 树，作辅助开发用，生产代码中未使用该方法。
 *   2. raxDebugShowNode() debug 用途，通过定义宏 `RAX_DEBUG_MSG` 打开，打开后输出 rax 树各个操作过程中的 log 日志。
 *   3. raxTouch(raxNode *n) 递归计算校验和，用于校验数据完整性? */

/* This function is mostly used for debugging and learning purposes.
 * It shows an ASCII representation of a tree on standard output, outline
 * all the nodes and the contained keys.
 *
 * The representation is as follow:
 *
 *  "foobar" (compressed node)
 *  [abc] (normal node with three children)
 *  [abc]=0x12345678 (node is a key, pointing to value 0x12345678)
 *  [] (a normal empty node)
 *
 *  Children are represented in new indented lines, each children prefixed by
 *  the "`-(x)" string, where "x" is the edge byte.
 *
 *  [abc]
 *   `-(a) "ladin"
 *   `-(b) [kj]
 *   `-(c) []
 *
 *  However when a node has a single child the following representation
 *  is used instead:
 *
 *  [abc] -> "ladin" -> []
 */

/* The actual implementation of raxShow(). */
void raxRecursiveShow(int level, int lpad, raxNode *n) {
    char s = n->iscompr ? '"' : '[';
    char e = n->iscompr ? '"' : ']';

    int numchars = printf("%c%.*s%c", s, n->size, n->data, e);
    if (n->iskey) {
        numchars += printf("=%p",raxGetData(n));
    }

    int numchildren = n->iscompr ? 1 : n->size;
    /* Note that 7 and 4 magic constants are the string length
     * of " `-(x) " and " -> " respectively. */
    if (level) {
        lpad += (numchildren > 1) ? 7 : 4;
        if (numchildren == 1) lpad += numchars;
    }
    raxNode **cp = raxNodeFirstChildPtr(n);
    for (int i = 0; i < numchildren; i++) {
        char *branch = " `-(%c) ";
        if (numchildren > 1) {
            printf("\n");
            for (int j = 0; j < lpad; j++) putchar(' ');
            printf(branch,n->data[i]);
        } else {
            printf(" -> ");
        }
        raxNode *child;
        memcpy(&child,cp,sizeof(child));
        raxRecursiveShow(level+1,lpad,child);
        cp++;
    }
}

/* Show a tree, as outlined in the comment above. */
void raxShow(rax *rax) {
    raxRecursiveShow(0,0,rax->head);
    putchar('\n');
}

/* Used by debugnode() macro to show info about a given node. */
void raxDebugShowNode(const char *msg, raxNode *n) {
    if (raxDebugMsg == 0) return;
    printf("%s: %p [%.*s] key:%u size:%u children:",
        msg, (void*)n, (int)n->size, (char*)n->data, n->iskey, n->size);
    int numcld = n->iscompr ? 1 : n->size;
    raxNode **cldptr = raxNodeLastChildPtr(n) - (numcld-1);
    while(numcld--) {
        raxNode *child;
        memcpy(&child,cldptr,sizeof(child));
        cldptr++;
        printf("%p ", (void*)child);
    }
    printf("\n");
    fflush(stdout);
}

/* Touch all the nodes of a tree returning a check sum. This is useful
 * in order to make Valgrind detect if there is something wrong while
 * reading the data structure.
 *
 * This function was used in order to identify Rax bugs after a big refactoring
 * using this technique:
 *
 * 1. The rax-test is executed using Valgrind, adding a printf() so that for
 *    the fuzz tester we see what iteration in the loop we are in.
 * 2. After every modification of the radix tree made by the fuzz tester
 *    in rax-test.c, we add a call to raxTouch().
 * 3. Now as soon as an operation will corrupt the tree, raxTouch() will
 *    detect it (via Valgrind) immediately. We can add more calls to narrow
 *    the state.
 * 4. At this point a good idea is to enable Rax debugging messages immediately
 *    before the moment the tree is corrupted, to see what happens.
 */
unsigned long raxTouch(raxNode *n) {
    debugf("Touching %p\n", (void*)n);
    unsigned long sum = 0;
    if (n->iskey) {
        sum += (unsigned long)raxGetData(n);
    }

    int numchildren = n->iscompr ? 1 : n->size;
    raxNode **cp = raxNodeFirstChildPtr(n);
    int count = 0;
    for (int i = 0; i < numchildren; i++) {
        if (numchildren > 1) {
            sum += (long)n->data[i];
        }
        raxNode *child;
        memcpy(&child,cp,sizeof(child));
        if (child == (void*)0x65d1760) count++;
        if (count > 1) exit(1);
        sum += raxTouch(child);
        cp++;
    }
    return sum;
}
