/*
 * Copyright (c) 2009-2012, Salvatore Sanfilippo <antirez at gmail dot com>
 * Copyright (c) 2009-2012, Pieter Noordhuis <pcnoordhuis at gmail dot com>
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

/*-----------------------------------------------------------------------------
 * Sorted set API
 *----------------------------------------------------------------------------*/

/* ZSETs are ordered sets using two data structures to hold the same elements
 * in order to get O(log(N)) INSERT and REMOVE operations into a sorted
 * data structure.
 *
 * The elements are added to a hash table mapping Redis objects to scores.
 * At the same time the elements are added to a skip list mapping scores
 * to Redis objects (so objects are sorted by scores in this "view").
 *
 * Note that the SDS string representing the element is the same in both
 * the hash table and skiplist in order to save memory. What we do in order
 * to manage the shared SDS string more easily is to free the SDS string
 * only in zslFreeNode(). The dictionary has no value free method set.
 * So we should always remove an element from the dictionary, and later from
 * the skiplist.
 *
 * This skiplist implementation is almost a C translation of the original
 * algorithm described by William Pugh in "Skip Lists: A Probabilistic
 * Alternative to Balanced Trees", modified in three ways:
 * a) this implementation allows for repeated scores.
 * b) the comparison is not just by key (our 'score') but by satellite data.
 * c) there is a back pointer, so it's a doubly linked list with the back
 * pointers being only at "level 1". This allows to traverse the list
 * from tail to head, useful for ZREVRANGE. */

#include "server.h"
#include <math.h>

/*-----------------------------------------------------------------------------
 * Skiplist implementation of the low level API
 *----------------------------------------------------------------------------*/


/* 有序集合类型为 skiplist 时的 API 实现 */

/* 温馨提示：
 * 1. 中文注释所提到的有序集合的 “成员” 是一个 [element,score] （即使英文注释写 element 也翻译成 “成员”），
 * 元素单指 element ，分数单指 score 。
 * 2. 跳表的前向指针（forward）不是我们经常写的双向链表中的 prev，而是 next，
 * 后向指针（backward）才是双链表中的 prev，不要看着名字搞混了哦~ */

int zslLexValueGteMin(sds value, zlexrangespec *spec);
int zslLexValueLteMax(sds value, zlexrangespec *spec);

/* Create a skiplist node with the specified number of levels.
 * The SDS string 'ele' is referenced by the node after the call. */

/* 创建一个具有指定层数的跳表节点, SDS字符串 'ele' 在调用后被节点引用 */
zskiplistNode *zslCreateNode(int level, double score, sds ele) {
    zskiplistNode *zn =
        zmalloc(sizeof(*zn)+level*sizeof(struct zskiplistLevel));
    zn->score = score;
    zn->ele = ele;
    return zn;
}

/* Create a new skiplist. */
/* 创建新跳表 */
zskiplist *zslCreate(void) {
    int j;
    zskiplist *zsl;

    zsl = zmalloc(sizeof(*zsl));
    zsl->level = 1;
    zsl->length = 0;
    zsl->header = zslCreateNode(ZSKIPLIST_MAXLEVEL,0,NULL);

    /* 初始化每层跳表 */
    for (j = 0; j < ZSKIPLIST_MAXLEVEL; j++) {
        zsl->header->level[j].forward = NULL;
        zsl->header->level[j].span = 0;
    }
    zsl->header->backward = NULL;
    zsl->tail = NULL;
    return zsl;
}

/* Free the specified skiplist node. The referenced SDS string representation
 * of the element is freed too, unless node->ele is set to NULL before calling
 * this function. */

/* 释放指定的跳表节点。
 * 成员的引用 SDS字符串 也会被释放，除非在调用此函数之前将 node->ele 设置为 NULL */
void zslFreeNode(zskiplistNode *node) {
    sdsfree(node->ele);
    zfree(node);
}

/* Free a whole skiplist. */
/* 释放整个跳表 */
void zslFree(zskiplist *zsl) {
    zskiplistNode *node = zsl->header->level[0].forward, *next;

    /* 释放头指针 */
    zfree(zsl->header);

    /* 遍历并释放剩下的所有节点 */
    while(node) {
        next = node->level[0].forward;
        zslFreeNode(node);
        node = next;
    }

    /* 释放跳表结构 */
    zfree(zsl);
}

/* Returns a random level for the new skiplist node we are going to create.
 * The return value of this function is between 1 and ZSKIPLIST_MAXLEVEL
 * (both inclusive), with a powerlaw-alike distribution where higher
 * levels are less likely to be returned. */

/* 为我们将要创建的新跳表节点返回一个随机的 level(最高层数)。
 * 这个函数的返回值介于 1 和 ZSKIPLIST_MAXLEVEL 之间（包括两者），
 * 返回值有一个类似幂律的分布，越高的 level 返回可能性就越低。 */
int zslRandomLevel(void) {

    /* 计算阈值 */
    static const int threshold = ZSKIPLIST_P*RAND_MAX;
    int level = 1; 

    /* 当随机数小于阈值时，level 继续增加 */
    while (random() < threshold)
        level += 1;

    /* 返回 level，同时做不要让 level 大于最大层数的操作 */
    return (level<ZSKIPLIST_MAXLEVEL) ? level : ZSKIPLIST_MAXLEVEL;
}

/* Insert a new node in the skiplist. Assumes the element does not already
 * exist (up to the caller to enforce that). The skiplist takes ownership
 * of the passed SDS string 'ele'. */

/* 在 skiplist 中插入一个新节点。假设要加入的成员不存在（由调用者决定是否执行）。
 * skiplist 将拥有传递的 SDS字符串 'ele' 的所有权（即直接引用 ele 而非复制一个副本并引用该副本） */
zskiplistNode *zslInsert(zskiplist *zsl, double score, sds ele) {
    zskiplistNode *update[ZSKIPLIST_MAXLEVEL], *x; /* update 记录比插入元素 score 或字典序小的最近节点 */
    unsigned long rank[ZSKIPLIST_MAXLEVEL]; /* 用于记录插入位置在每层中跨越了多少个节点 */
    int i, level;

    serverAssert(!isnan(score));
    x = zsl->header;

    /* 从最高层向下查找插入位置 */
    for (i = zsl->level-1; i >= 0; i--) {
        /* store rank that is crossed to reach the insert position */
        /* rank 存储到达插入位置而跨越的节点数 */
        rank[i] = i == (zsl->level-1) ? 0 : rank[i+1];

        while (x->level[i].forward &&
                (x->level[i].forward->score < score ||
                    (x->level[i].forward->score == score &&
                    sdscmp(x->level[i].forward->ele,ele) < 0)))
        {
            rank[i] += x->level[i].span;
            x = x->level[i].forward;
        }
        update[i] = x;
    }
    /* we assume the element is not already inside, since we allow duplicated
     * scores, reinserting the same element should never happen since the
     * caller of zslInsert() should test in the hash table if the element is
     * already inside or not. */

    /* 我们假设成员不在内部，
     * 我们允许重复的分数（score），但重新插入相同的成员应永远不会发生，
     * 因为zslInsert（）的调用方应测试成员是否已经在哈希表内部
     * （zset 使用跳表结构时，用哈希表保存成员和分数的对应关系，而跳表用于根据分数查询数据） */

    /* 获取随机最高层数 */
    level = zslRandomLevel();

    /* 随机获取的 level 比跳表原来的 level 大，则在比原来 level 高的层级上初始化 rank 和 update  */
    if (level > zsl->level) {
        for (i = zsl->level; i < level; i++) {
            rank[i] = 0;
            update[i] = zsl->header;
            update[i]->level[i].span = zsl->length;
        }

        /* 将跳表的 level(最高层数) 替换为随机获取到的 level */
        zsl->level = level;
    }

    /* 创建跳表节点 */
    x = zslCreateNode(level,score,ele);

    /* 插入新节点 */
    for (i = 0; i < level; i++) {
        x->level[i].forward = update[i]->level[i].forward;
        update[i]->level[i].forward = x;

        /* update span covered by update[i] as x is inserted here */
        /* 更新 update[i] 所涵盖的跨度，因为有新节点(x)被插入了 */

        /* 首先更新新节点的跨度 */
        x->level[i].span = update[i]->level[i].span - (rank[0] - rank[i]);

        /* 更新 update 的跨度 */
        update[i]->level[i].span = (rank[0] - rank[i]) + 1;
    }

    /* increment span for untouched levels */
    /* 对未触及到的层数（插入节点的最高层与整个跳表中最高层之间）更新跨度 */
    for (i = level; i < zsl->level; i++) {
        update[i]->level[i].span++;
    }

    /* 设置新节点的后向指针 */
    x->backward = (update[0] == zsl->header) ? NULL : update[0];

    /* 设置新节点的下一个节点的后向指针，若下一个节点不存在，则将尾指针指向新节点 */
    if (x->level[0].forward)
        x->level[0].forward->backward = x;
    else
        zsl->tail = x;

    /* 节点数计数增加 */
    zsl->length++;

    /* 返回新节点 */
    return x;
}

/* Internal function used by zslDelete, zslDeleteRangeByScore and
 * zslDeleteRangeByRank. */

/* 由 zslDelete、zslDeleteRangeByScore 和 zslDeleteRangeByRank 使用的内部实现函数，
 * 用于删除跳表中的节点 */
void zslDeleteNode(zskiplist *zsl, zskiplistNode *x, zskiplistNode **update) {
    int i;

    /* 更新 update[i] 的前向指针以及跨度 */
    for (i = 0; i < zsl->level; i++) {
        if (update[i]->level[i].forward == x) {
            update[i]->level[i].span += x->level[i].span - 1;
            update[i]->level[i].forward = x->level[i].forward;
        } else {
            update[i]->level[i].span -= 1;
        }
    }

    /* 更新 x（被删除节点） 的下一个节点的后向指针，如果下一个节点不存在，则将尾指针指向 x 的上一个节点 */
    if (x->level[0].forward) {
        x->level[0].forward->backward = x->backward;
    } else {
        zsl->tail = x->backward;
    }

    /* 若被删除节点拥有最高的层数，则需要将跳表的最高层数下调至当前剩余节点中的最高层 */
    while(zsl->level > 1 && zsl->header->level[zsl->level-1].forward == NULL)
        zsl->level--;
    zsl->length--;
}

/* Delete an element with matching score/element from the skiplist.
 * The function returns 1 if the node was found and deleted, otherwise
 * 0 is returned.
 *
 * If 'node' is NULL the deleted node is freed by zslFreeNode(), otherwise
 * it is not freed (but just unlinked) and *node is set to the node pointer,
 * so that it is possible for the caller to reuse the node (including the
 * referenced SDS string at node->ele). */

/* 从跳表中删除一个指定的成员（由输入参数的 score 和 ele 指定） 。
 * 如果找到并删除了该节点，该函数返回1，否则返回0。
 *
 * 如果 'node' 是 NULL，被删除的节点将被 zslFreeNode() 释放，
 * 否则它不会被释放（只是取消与跳表的链接），*node 被设置为指向所删除的节点，
 * 这样调用者就有可能重新使用该节点（包括 node->ele 引用的 SDS字符串）。*/
int zslDelete(zskiplist *zsl, double score, sds ele, zskiplistNode **node) {
    zskiplistNode *update[ZSKIPLIST_MAXLEVEL], *x;
    int i;

    x = zsl->header;

    /* 从最高层向下查找要删除的节点 */
    for (i = zsl->level-1; i >= 0; i--) {
        while (x->level[i].forward &&
                (x->level[i].forward->score < score ||
                    (x->level[i].forward->score == score &&
                     sdscmp(x->level[i].forward->ele,ele) < 0)))
        {
            x = x->level[i].forward;
        }
        update[i] = x;
    }
    /* We may have multiple elements with the same score, what we need
     * is to find the element with both the right score and object. */

    /* 我们可能有多个具有相同分数的成员，我们所需要的是具有正确分数和元素的成员
     * （比对元素（字符串）由sdscmp 比较函数完成，两元素相同返回0） */
    x = x->level[0].forward;
    if (x && score == x->score && sdscmp(x->ele,ele) == 0) {

        /* 删除节点 */
        zslDeleteNode(zsl, x, update);

        /* 输入参数 node 不为空则释放被删除节点，否则令 *node 指向它 */
        if (!node)
            zslFreeNode(x);
        else
            *node = x;
        return 1;
    }
    return 0; /* not found */
}

/* Update the score of an element inside the sorted set skiplist.
 * Note that the element must exist and must match 'score'.
 * This function does not update the score in the hash table side, the
 * caller should take care of it.
 *
 * Note that this function attempts to just update the node, in case after
 * the score update, the node would be exactly at the same position.
 * Otherwise the skiplist is modified by removing and re-adding a new
 * element, which is more costly.
 *
 * The function returns the updated element skiplist node pointer. */

/* 更新跳表中的一个成员的分数。
 * 注意，该成员必须存在，并且必须与 'score' 相匹配。
 * 这个函数不更新哈希表中的分数，而是由调用者负责。
 *
 * 这个函数试图只更新当前节点，在分数更新后，该节点能完全处在同一位置。
 * 如果无法做到，跳表将通过删除和重新添加一个新的成员，这样做的开销代价更大。
 *
 * 该函数返回更新的成员的跳表节点指针。*/
zskiplistNode *zslUpdateScore(zskiplist *zsl, double curscore, sds ele, double newscore) {
    zskiplistNode *update[ZSKIPLIST_MAXLEVEL], *x;
    int i;

    /* We need to seek to element to update to start: this is useful anyway,
     * we'll have to update or remove it. */
    /* 我们需要先找到成员，再开始执行更新操作 */
    x = zsl->header;

    /* 从最高层向下查找成员 */
    for (i = zsl->level-1; i >= 0; i--) {
        while (x->level[i].forward &&
                (x->level[i].forward->score < curscore ||
                    (x->level[i].forward->score == curscore &&
                     sdscmp(x->level[i].forward->ele,ele) < 0)))
        {
            x = x->level[i].forward;
        }
        update[i] = x;
    }

    /* Jump to our element: note that this function assumes that the
     * element with the matching score exists. */
    /* 跳转到我们的成员：注意，这个断言假定具有与指定分数匹配的成员的存在 */
    x = x->level[0].forward;
    serverAssert(x && curscore == x->score && sdscmp(x->ele,ele) == 0);

    /* If the node, after the score update, would be still exactly
     * at the same position, we can just update the score without
     * actually removing and re-inserting the element in the skiplist. */

    /* 如果节点在分数更新后，仍然完全处于相同的位置（有序性不被破坏），
     * 我们可以只更新分数，而不需要实际删除和重新插入该成员到跳表中。*/
    if ((x->backward == NULL || x->backward->score < newscore) &&
        (x->level[0].forward == NULL || x->level[0].forward->score > newscore))
    {
        x->score = newscore;
        return x;
    }

    /* No way to reuse the old node: we need to remove and insert a new
     * one at a different place. */

    /* 没有办法重新使用旧的节点（有序性被破坏）：我们需要移除旧节点并在不同的位置插入一个新的节点。*/
    zslDeleteNode(zsl, x, update);
    zskiplistNode *newnode = zslInsert(zsl,newscore,x->ele);
    /* We reused the old node x->ele SDS string, free the node now
     * since zslInsert created a new one. */

    /* 我们重新使用了旧节点的 x->ele（因为新节点直接引用了 x->ele ），
     * 现在释放这个节点，因为我们已经用 zslInsert 创建了一个新的节点 */
    x->ele = NULL;
    zslFreeNode(x);
    return newnode;
}

/* 根据最小值是否具有排他性，检查 value 是否合法 */
int zslValueGteMin(double value, zrangespec *spec) {
    return spec->minex ? (value > spec->min) : (value >= spec->min);
}

/* 根据最大值是否具有排他性，检查 value 是否合法 */
int zslValueLteMax(double value, zrangespec *spec) {
    return spec->maxex ? (value < spec->max) : (value <= spec->max);
}

/* Returns if there is a part of the zset is in range. */
/* 检查 zset 中是否存在输入的范围。
 * 如果 zset 的某一部分在范围内，则返回1，否则返回 0 */
int zslIsInRange(zskiplist *zsl, zrangespec *range) {
    zskiplistNode *x;

    /* Test for ranges that will always be empty. */
    /* 测试永远为空的范围 */
    if (range->min > range->max ||
            (range->min == range->max && (range->minex || range->maxex)))
        return 0;
    x = zsl->tail; /* 令 x 跳到尾部（分数最大的成员） */
    if (x == NULL || !zslValueGteMin(x->score,range))
        return 0;
    x = zsl->header->level[0].forward; /* 令 x 跳到头部（分数最大的成员） */
    if (x == NULL || !zslValueLteMax(x->score,range))
        return 0;
    return 1;
}

/* Find the first node that is contained in the specified range.
 * Returns NULL when no element is contained in the range. */

/* 寻找包含在指定范围内的第一个节点。如果没有成员包含在该范围内，则返回 NULL。*/
zskiplistNode *zslFirstInRange(zskiplist *zsl, zrangespec *range) {
    zskiplistNode *x;
    int i;

    /* If everything is out of range, return early. */
    /* 如果 zset 内不存在输入的范围，则提前返回 */
    if (!zslIsInRange(zsl,range)) return NULL;

    x = zsl->header;
    for (i = zsl->level-1; i >= 0; i--) {
        /* Go forward while *OUT* of range. */
        /* 从跳表头部（分数最小的一端）开始寻找，直到 x 下一个节点的分数大于范围最小值时终止 */
        while (x->level[i].forward &&
            !zslValueGteMin(x->level[i].forward->score,range))
                x = x->level[i].forward;
    }

    /* This is an inner range, so the next node cannot be NULL. */
    /* 输入的范围是 zset 的内部范围，所以下一个节点不可能是 NULL。
     * x 的下一个节点即是该内部范围的第一个元素 */
    x = x->level[0].forward;
    serverAssert(x != NULL);

    /* Check if score <= max. */
    /* 检查 score 是否小于范围的最大值 */
    if (!zslValueLteMax(x->score,range)) return NULL;
    return x;
}

/* Find the last node that is contained in the specified range.
 * Returns NULL when no element is contained in the range. */

/* 寻找包含在指定范围内的最后一个节点。如果没有成员包含在该范围内，则返回 NULL */
zskiplistNode *zslLastInRange(zskiplist *zsl, zrangespec *range) {
    zskiplistNode *x;
    int i;

    /* If everything is out of range, return early. */
     /* 如果 zset 内不存在输入的范围，则提前返回 */
    if (!zslIsInRange(zsl,range)) return NULL;

    x = zsl->header;
    for (i = zsl->level-1; i >= 0; i--) {
        /* Go forward while *IN* range. */
        /* 从跳表头部（分数最小的一端）开始寻找，直到 x 下一个节点的分数大于范围最大值时终止 */
        while (x->level[i].forward &&
            zslValueLteMax(x->level[i].forward->score,range))
                x = x->level[i].forward;
    }

    /* This is an inner range, so this node cannot be NULL. */
    /* 输入的范围是 zset 的内部范围，所以当前节点不可能是 NULL。
     * 当前节点即是该内部范围的最后一个元素 */
    serverAssert(x != NULL);

    /* Check if score >= min. */
    /* 检查 score 是否大于范围的最小值 */
    if (!zslValueGteMin(x->score,range)) return NULL;
    return x;
}

/* Delete all the elements with score between min and max from the skiplist.
 * Both min and max can be inclusive or exclusive (see range->minex and
 * range->maxex). When inclusive a score >= min && score <= max is deleted.
 * Note that this function takes the reference to the hash table view of the
 * sorted set, in order to remove the elements from the hash table too. */

/* 从跳表中删除所有分数在 min 和 max 之间的成员。
 * min 和 max 都可以是包容的或排他的（见 range->minex 和 range->maxex）。
 * 例如：当包含时，满足 score >= min && score <= max 的成员被删除。
 * 请注意，这个函数需要对哈希表视图进行引用，以便也从哈希表中删除成员 */
unsigned long zslDeleteRangeByScore(zskiplist *zsl, zrangespec *range, dict *dict) {
    zskiplistNode *update[ZSKIPLIST_MAXLEVEL], *x;
    unsigned long removed = 0;
    int i;

    x = zsl->header;

    for (i = zsl->level-1; i >= 0; i--) {
        while (x->level[i].forward &&
            !zslValueGteMin(x->level[i].forward->score, range))
                x = x->level[i].forward;
        update[i] = x;
    }

    /* Current node is the last with score < or <= min. */

    /* 当前节点是满足 score < min 或 score <= min 的最后一个节点。
     * 我们这里令 x 指向下一个节点，即此时 x 是范围内的分数最小的节点 */
    x = x->level[0].forward;

    /* Delete nodes while in range. */
    /* 将范围内的成员遍历删除 */
    while (x && zslValueLteMax(x->score, range)) {
        zskiplistNode *next = x->level[0].forward;
        zslDeleteNode(zsl,x,update); 
        dictDelete(dict,x->ele); /* 将成员也从哈希表中删除 */
        zslFreeNode(x); /* 在这里实际释放被删除的跳表节点 */
        removed++; /* 删除成员的计数增加 */
        x = next;
    }
    return removed;
}

/* 删除指定字典序范围内的成员。
 * zrangebylex 命令使用提示：使用该命令时需要有序集合中所有成员的分数相等，
 * 否则结果不准确！因为我们并不判断分数大小，只判断字典序大小 */
unsigned long zslDeleteRangeByLex(zskiplist *zsl, zlexrangespec *range, dict *dict) {
    zskiplistNode *update[ZSKIPLIST_MAXLEVEL], *x;
    unsigned long removed = 0;
    int i;


    x = zsl->header;
    for (i = zsl->level-1; i >= 0; i--) {
        while (x->level[i].forward &&
            !zslLexValueGteMin(x->level[i].forward->ele,range))
                x = x->level[i].forward;
        update[i] = x;
    }

    /* Current node is the last with score < or <= min. */
    
    /* 当前节点是满足 lexicographic（字典序） < min 或 lexicographic <= min 的最后一个节点。
     * 我们这里令 x 指向下一个节点，即此时 x 是范围内的分数最小的节点 */
    x = x->level[0].forward;

    /* Delete nodes while in range. */
    /* 将范围内的成员遍历删除 */
    while (x && zslLexValueLteMax(x->ele,range)) {
        zskiplistNode *next = x->level[0].forward;
        zslDeleteNode(zsl,x,update);
        dictDelete(dict,x->ele); /* 将成员也从哈希表中删除 */
        zslFreeNode(x); /* 在这里实际释放被删除的跳表节点 */
        removed++; /* 删除成员的计数增加 */
        x = next;
    }
    return removed;
}

/* Delete all the elements with rank between start and end from the skiplist.
 * Start and end are inclusive. Note that start and end need to be 1-based */

/* 从跳表中删除所有 rank（排位）在 start 和 end 之间的成员。
 * 被删除的成员会包括 start 和 end 在内。
 * 注意，start 和 end 是以 1 为第一个成员的 rank */
unsigned long zslDeleteRangeByRank(zskiplist *zsl, unsigned int start, unsigned int end, dict *dict) {
    zskiplistNode *update[ZSKIPLIST_MAXLEVEL], *x;
    unsigned long traversed = 0, removed = 0;
    int i;

    x = zsl->header;
    for (i = zsl->level-1; i >= 0; i--) {
        while (x->level[i].forward && (traversed + x->level[i].span) < start) {
            traversed += x->level[i].span;
            x = x->level[i].forward;
        }
        update[i] = x;
    }

    traversed++; /* 记录当前成员的 rank */

    /* 我们这里令 x 指向下一个节点，此时 x 是第 start 个节点  */
    x = x->level[0].forward;

    /* 遍历并删除第 start 个 到第 end 个成员 */
    while (x && traversed <= end) {
        zskiplistNode *next = x->level[0].forward;
        zslDeleteNode(zsl,x,update);
        dictDelete(dict,x->ele);
        zslFreeNode(x);
        removed++;
        traversed++;
        x = next;
    }
    return removed;
}

/* Find the rank for an element by both score and key.
 * Returns 0 when the element cannot be found, rank otherwise.
 * Note that the rank is 1-based due to the span of zsl->header to the
 * first element. */

/* 通过 score 和 key 找到一个成员的 rank（排位）。
 * 当找不到成员时返回0，否则返回 rank。
 * 注意，rank 是从 1 开始的（而非以0代表第一个成员）
 * 这是由于 rank 的计数方式为 rank += x->level[i].span，
 * 而 zsl->header 到第一个成员的 span 为 1，所以第一个元素的 rank 为 1 */
unsigned long zslGetRank(zskiplist *zsl, double score, sds ele) {
    zskiplistNode *x;
    unsigned long rank = 0;
    int i;

    x = zsl->header;

    /* 从最高层向下查找成员 */
    for (i = zsl->level-1; i >= 0; i--) {
        while (x->level[i].forward &&
            (x->level[i].forward->score < score ||
                (x->level[i].forward->score == score &&
                sdscmp(x->level[i].forward->ele,ele) <= 0))) {
            rank += x->level[i].span;
            x = x->level[i].forward;
        }

        /* x might be equal to zsl->header, so test if obj is non-NULL */
        /* x 可能等于 zsl->header，所以测试 x->ele 是否为非 NULL */
        if (x->ele && x->score == score && sdscmp(x->ele,ele) == 0) {
            return rank;
        }
    }
    return 0;
}

/* Finds an element by its rank. The rank argument needs to be 1-based. */
/* 根据一个成员的 rank 来查找到该成员。rank 参数是需要基于1的（原因可以看上面函数的注解） */
zskiplistNode* zslGetElementByRank(zskiplist *zsl, unsigned long rank) {
    zskiplistNode *x;
    unsigned long traversed = 0;
    int i;

    x = zsl->header;
    for (i = zsl->level-1; i >= 0; i--) {
        while (x->level[i].forward && (traversed + x->level[i].span) <= rank)
        {
            traversed += x->level[i].span;
            x = x->level[i].forward;
        }
        if (traversed == rank) {
            return x;
        }
    }
    return NULL;
}

/* Populate the rangespec according to the objects min and max. */
/* 根据对象的 min 和 max 来填充 rangespec。*/
static int zslParseRange(robj *min, robj *max, zrangespec *spec) {
    char *eptr;
    spec->minex = spec->maxex = 0;

    /* Parse the min-max interval. If one of the values is prefixed
     * by the "(" character, it's considered "open". For instance
     * ZRANGEBYSCORE zset (1.5 (2.5 will match min < x < max
     * ZRANGEBYSCORE zset 1.5 2.5 will instead match min <= x <= max */

    /* 解析最小-最大区间。如果其中一个值的前缀是"("字符，它就被认为是 "开区间"。
     * 例如:
     * ZRANGEBYSCORE zset (1.5 (2.5 将等于 1.5 < x < 2.5 
     * ZRANGEBYSCORE zset 1.5 2.5 将等于 1.5 <= x <= 2.5 */
    if (min->encoding == OBJ_ENCODING_INT) {
        spec->min = (long)min->ptr;
    } else {
        if (((char*)min->ptr)[0] == '(') {
            spec->min = strtod((char*)min->ptr+1,&eptr);
            if (eptr[0] != '\0' || isnan(spec->min)) return C_ERR;
            spec->minex = 1;
        } else {
            spec->min = strtod((char*)min->ptr,&eptr);
            if (eptr[0] != '\0' || isnan(spec->min)) return C_ERR;
        }
    }
    if (max->encoding == OBJ_ENCODING_INT) {
        spec->max = (long)max->ptr;
    } else {
        if (((char*)max->ptr)[0] == '(') {
            spec->max = strtod((char*)max->ptr+1,&eptr);
            if (eptr[0] != '\0' || isnan(spec->max)) return C_ERR;
            spec->maxex = 1;
        } else {
            spec->max = strtod((char*)max->ptr,&eptr);
            if (eptr[0] != '\0' || isnan(spec->max)) return C_ERR;
        }
    }

    return C_OK;
}

/* ------------------------ Lexicographic ranges ---------------------------- */

/* Parse max or min argument of ZRANGEBYLEX.
  * (foo means foo (open interval)
  * [foo means foo (closed interval)
  * - means the min string possible
  * + means the max string possible
  *
  * If the string is valid the *dest pointer is set to the redis object
  * that will be used for the comparison, and ex will be set to 0 or 1
  * respectively if the item is exclusive or inclusive. C_OK will be
  * returned.
  *
  * If the string is not a valid range C_ERR is returned, and the value
  * of *dest and *ex is undefined. */

 /* 解析 ZRANGEBYLEX 的最大或最小参数。
  * (foo 表示 foo (开放区间)
  * [foo 表示 foo (封闭区间)
  * - 表示可能的最小字符串
  * + 表示可能的最大字符串
  *
  * 如果字符串有效，*dest 指针将被设置为将用于比较的 redis对象，
  * 根据 item 是排他性还是包容性，ex 将被分别设置为 0 或 1，最后 C_OK 将被返回。
  *
  * 如果字符串不是一个有效的范围，将返回 C_ERR，并且 *dest 和 *ex 的值是未被定义的。*/
int zslParseLexRangeItem(robj *item, sds *dest, int *ex) {
    char *c = item->ptr;

    switch(c[0]) {
    case '+':
        if (c[1] != '\0') return C_ERR;
        *ex = 1;
        *dest = shared.maxstring;
        return C_OK;
    case '-':
        if (c[1] != '\0') return C_ERR;
        *ex = 1;
        *dest = shared.minstring;
        return C_OK;
    case '(':
        *ex = 1;
        *dest = sdsnewlen(c+1,sdslen(c)-1);
        return C_OK;
    case '[':
        *ex = 0;
        *dest = sdsnewlen(c+1,sdslen(c)-1);
        return C_OK;
    default:
        return C_ERR;
    }
}

/* Free a lex range structure, must be called only after zslParseLexRange()
 * populated the structure with success (C_OK returned). */

/* 释放一个 zlexrangespec 结构，必须在 zslParseLexRange() 成功填充该结构（返回 C_OK）后才能调用 */
void zslFreeLexRange(zlexrangespec *spec) {
    if (spec->min != shared.minstring &&
        spec->min != shared.maxstring) sdsfree(spec->min);
    if (spec->max != shared.minstring &&
        spec->max != shared.maxstring) sdsfree(spec->max);
}

/* Populate the lex rangespec according to the objects min and max.
 *
 * Return C_OK on success. On error C_ERR is returned.
 * When OK is returned the structure must be freed with zslFreeLexRange(),
 * otherwise no release is needed. */

/* 根据对象 min 和 max 来填充 zlexrangespec。
 *
 * 成功时返回 C_OK，错误时返回 C_ERR。
 * 当返回 OK 时，该结构必须用 zslFreeLexRange() 释放。
 * 否则不需要释放。*/
int zslParseLexRange(robj *min, robj *max, zlexrangespec *spec) {
    /* The range can't be valid if objects are integer encoded.
     * Every item must start with ( or [. */

    /* 如果对象是整数编码的，范围无效。
     * min 和 max 都必须以（ 或 [ 做前缀 */
    if (min->encoding == OBJ_ENCODING_INT ||
        max->encoding == OBJ_ENCODING_INT) return C_ERR;

    spec->min = spec->max = NULL;
    if (zslParseLexRangeItem(min, &spec->min, &spec->minex) == C_ERR ||
        zslParseLexRangeItem(max, &spec->max, &spec->maxex) == C_ERR) {
        zslFreeLexRange(spec);
        return C_ERR;
    } else {
        return C_OK;
    }
}

/* This is just a wrapper to sdscmp() that is able to
 * handle shared.minstring and shared.maxstring as the equivalent of
 * -inf and +inf for strings */
/* 这只是 sdscmp() 的一个封装函数，
 * 它能够处理 shared.minstring 和 shared.maxstring，相当于字符串的 -inf 和 +inf */
int sdscmplex(sds a, sds b) {
    if (a == b) return 0;
    if (a == shared.minstring || b == shared.maxstring) return -1;
    if (a == shared.maxstring || b == shared.minstring) return 1;
    return sdscmp(a,b);
}

/* 根据（字典序）最小值是否具有排他性，检查 value 是否合法 */
int zslLexValueGteMin(sds value, zlexrangespec *spec) {
    return spec->minex ?
        (sdscmplex(value,spec->min) > 0) :
        (sdscmplex(value,spec->min) >= 0);
}

/* 根据（字典序）最大值是否具有排他性，检查 value 是否合法 */
int zslLexValueLteMax(sds value, zlexrangespec *spec) {
    return spec->maxex ?
        (sdscmplex(value,spec->max) < 0) :
        (sdscmplex(value,spec->max) <= 0);
}

/* Returns if there is a part of the zset is in the lex range. */
/* 如果 zset 的某一部分在范围内，则返回1，否则返回0 */
int zslIsInLexRange(zskiplist *zsl, zlexrangespec *range) {
    zskiplistNode *x;

    /* Test for ranges that will always be empty. */
    /* 测试永远为空的情况 */
    int cmp = sdscmplex(range->min,range->max);
    if (cmp > 0 || (cmp == 0 && (range->minex || range->maxex)))
        return 0;
    x = zsl->tail;
    if (x == NULL || !zslLexValueGteMin(x->ele,range))
        return 0;
    x = zsl->header->level[0].forward;
    if (x == NULL || !zslLexValueLteMax(x->ele,range))
        return 0;
    return 1;
}

/* Find the first node that is contained in the specified lex range.
 * Returns NULL when no element is contained in the range. */

/* 寻找包含在指定范围内的第一个节点。
 * 当范围内没有包含任何成员时，返回 NULL。*/
zskiplistNode *zslFirstInLexRange(zskiplist *zsl, zlexrangespec *range) {
    zskiplistNode *x;
    int i;

    /* If everything is out of range, return early. */
    /* 如果 zset 内不存在该范围，则提前返回。*/
    if (!zslIsInLexRange(zsl,range)) return NULL;

    x = zsl->header;
    for (i = zsl->level-1; i >= 0; i--) {
        /* Go forward while *OUT* of range. */
        /* 从跳表头部（字典序最小的一端）开始寻找，直到 x 下一个节点的字典序大于范围最小值时终止 */
        while (x->level[i].forward &&
            !zslLexValueGteMin(x->level[i].forward->ele,range))
                x = x->level[i].forward;
    }

    /* This is an inner range, so the next node cannot be NULL. */
    /* 我们令 x 指向下一个节点，下一个节点即为范围内的第一个节点 */
    x = x->level[0].forward;
    serverAssert(x != NULL);

    /* Check if score <= max. */
    /* 检查字典序是否小于等于范围最大值 */
    if (!zslLexValueLteMax(x->ele,range)) return NULL;
    return x;
}

/* Find the last node that is contained in the specified range.
 * Returns NULL when no element is contained in the range. */

/* 寻找包含在指定范围内的最后一个节点。
 * 当范围内没有包含任何成员时，返回 NULL。*/
zskiplistNode *zslLastInLexRange(zskiplist *zsl, zlexrangespec *range) {
    zskiplistNode *x;
    int i;

    /* If everything is out of range, return early. */
    /* 如果 zset 内不存在该范围，则提前返回。*/
    if (!zslIsInLexRange(zsl,range)) return NULL;

    x = zsl->header;
    for (i = zsl->level-1; i >= 0; i--) {
        /* Go forward while *IN* range. */
        /* 从跳表头部（字典序最小的一端）开始寻找，直到 x 下一个节点的字典序大于范围最大值时终止 */
        while (x->level[i].forward &&
            zslLexValueLteMax(x->level[i].forward->ele,range))
                x = x->level[i].forward;
    }

    /* This is an inner range, so this node cannot be NULL. */
    /* 当前节点为范围内的最后一个节点，且范围是属于 zset 内的，所以当前节点不可能是 NULL */
    serverAssert(x != NULL);

    /* Check if score >= min. */
    /* 检查字典序是否小于等于范围最大值 */
    if (!zslLexValueGteMin(x->ele,range)) return NULL;
    return x;
}

/*-----------------------------------------------------------------------------
 * Listpack-backed sorted set API
 *----------------------------------------------------------------------------*/

/* 有序集合类型为 listpack 时的 API 实现 */

/* 字符串转 double */
double zzlStrtod(unsigned char *vstr, unsigned int vlen) {
    char buf[128];
    if (vlen > sizeof(buf) - 1)
        vlen = sizeof(buf) - 1;
    memcpy(buf,vstr,vlen);
    buf[vlen] = '\0';
    return strtod(buf,NULL);
 }

/* 从 listpack 中获取分数（score） */
double zzlGetScore(unsigned char *sptr) {
    unsigned char *vstr;
    unsigned int vlen;
    long long vlong;
    double score;

    serverAssert(sptr != NULL);
    vstr = lpGetValue(sptr,&vlen,&vlong); /* 获取分数 */

    /* 如果 vstr 存在，则分数是字符串编码，*/
    if (vstr) {
        score = zzlStrtod(vstr,vlen); /* 将字符串转 double */

    /* vstr 为 NULL，则分数是整数编码，保存在 vlong 中 */
    } else {
        score = vlong;
    }

    return score;
}

/* Return a listpack element as an SDS string. */
/* 将一个 listpack 中的元素作为 SDS字符串 返回，注意返回的是一个副本 */
sds lpGetObject(unsigned char *sptr) {
    unsigned char *vstr;
    unsigned int vlen;
    long long vlong;

    serverAssert(sptr != NULL);
    vstr = lpGetValue(sptr,&vlen,&vlong);

    if (vstr) {
        return sdsnewlen((char*)vstr,vlen);
    } else {
        return sdsfromlonglong(vlong);
    }
}

/* Compare element in sorted set with given element. */
/* 将有序集合中的元素与给定的元素进行字典序比较
 * 第一个元素(eptr) = 第二个元素(cstr) 返回0，
 * 第一个元素(eptr) > 第二个元素(cstr) 返回正数，
 * 第一个元素(eptr) < 第二个元素(cstr) 返回负数 */
int zzlCompareElements(unsigned char *eptr, unsigned char *cstr, unsigned int clen) {
    unsigned char *vstr;
    unsigned int vlen;
    long long vlong;
    unsigned char vbuf[32];
    int minlen, cmp;

    vstr = lpGetValue(eptr,&vlen,&vlong);
    if (vstr == NULL) {
        /* Store string representation of long long in buf. */
        /* 若有序集合中的元素是整数编码，则需要转为字符串来进行比较 */
        vlen = ll2string((char*)vbuf,sizeof(vbuf),vlong);
        vstr = vbuf;
    }

    /* 获取两个元素长度之间的最短的字符串长度 */
    minlen = (vlen < clen) ? vlen : clen;

    /* 对两个元素的相同长度部分进行比较 */
    cmp = memcmp(vstr,cstr,minlen);
    if (cmp == 0) return vlen-clen; /* 两个字符串相同长度部分都相等，则返回长度差值令调用方用长度比较 */
    return cmp;
}

/* 获取有序集合中的成员个数 */
unsigned int zzlLength(unsigned char *zl) {

    /* 返回 listpack 总长度的一半 
     * 因为 score 和 element 是同一个成员，而 score 和 element 需要2个 listpack entry 分别存储,
     * 所以有序集合成员个数 = listpack 总长度 / 2 */
    return lpLength(zl)/2;
}

/* Move to next entry based on the values in eptr and sptr. Both are set to
 * NULL when there is no next entry. */

/* 获取下一个元素和它的分数。
 * 根据 eptr 和 sptr 中的值，移动到下一个 entry。
 * 当没有下一个 entry 时，eptr 和 sptr 都被设置为 NULL。*/
void zzlNext(unsigned char *zl, unsigned char **eptr, unsigned char **sptr) {
    unsigned char *_eptr, *_sptr;
    serverAssert(*eptr != NULL && *sptr != NULL);

    /* 首先获取下一个元素，
     * zset 的一个成员在 listpack 中的存储顺序是先存元素，后存分数 | element entry | score entry | 
     * 所以我们获取指向当前分数的指针 sptr 的下一个 entry，即为下一个元素 */
    _eptr = lpNext(zl,*sptr);

    /* 如果下一个元素不为 NULL ，则获取它的分数 */
    if (_eptr != NULL) {
        _sptr = lpNext(zl,_eptr);
        serverAssert(_sptr != NULL);
    } else {
        /* No next entry. */
        /* 没有下一个 entry */
        _sptr = NULL;
    }

    *eptr = _eptr;
    *sptr = _sptr;
}

/* Move to the previous entry based on the values in eptr and sptr. Both are
 * set to NULL when there is no prev entry. */

/* 获取上一个元素和它的分数。
 * 根据 eptr 和 sptr 中的值移动到上一个 entry。
 * 两者都是当没有上一个 entry 时设置为 NULL。 */
void zzlPrev(unsigned char *zl, unsigned char **eptr, unsigned char **sptr) {
    unsigned char *_eptr, *_sptr;
    serverAssert(*eptr != NULL && *sptr != NULL);

    _sptr = lpPrev(zl,*eptr);
    if (_sptr != NULL) {
        _eptr = lpPrev(zl,_sptr);
        serverAssert(_eptr != NULL);
    } else {
        /* No previous entry. */
        /* 没有上一个 entry */
        _eptr = NULL;
    }

    *eptr = _eptr;
    *sptr = _sptr;
}

/* Returns if there is a part of the zset is in range. Should only be used
 * internally by zzlFirstInRange and zzlLastInRange. */

/* 如果 zset 的一部分在范围内，则返回1，否则返回0。 
 * 只能由 zzlFirstInRange 和 zzlLastInRange 函数使用。 */
int zzlIsInRange(unsigned char *zl, zrangespec *range) {
    unsigned char *p;
    double score;

    /* Test for ranges that will always be empty. */
    /* 测试永远为空的范围 */
    if (range->min > range->max ||
            (range->min == range->max && (range->minex || range->maxex)))
        return 0;

    p = lpSeek(zl,-1); /* 有序集合中最大的 score */
    if (p == NULL) return 0; /* 有序集合为空的情况 */
    score = zzlGetScore(p);
    if (!zslValueGteMin(score,range))
        return 0;

    p = lpSeek(zl,1); /* 有序集合中最小的 score */
    serverAssert(p != NULL);
    score = zzlGetScore(p);
    if (!zslValueLteMax(score,range))
        return 0;

    return 1;
}

/* Find pointer to the first element contained in the specified range.
 * Returns NULL when no element is contained in the range. */

/* 查找指向包含在指定范围内的第一个（分数最小的）元素的指针。 
 * 当范围内不包含任何元素时返回 NULL。 */
unsigned char *zzlFirstInRange(unsigned char *zl, zrangespec *range) {
    unsigned char *eptr = lpSeek(zl,0), *sptr;
    double score;

    /* If everything is out of range, return early. */
    /* 如果 zset 内不存在输入的范围，则提前返回 */
    if (!zzlIsInRange(zl,range)) return NULL;

    /* 从 listpack 第一个（分数最小的）成员向后遍历 */
    while (eptr != NULL) {
        sptr = lpNext(zl,eptr);
        serverAssert(sptr != NULL);

        score = zzlGetScore(sptr);

        /* 该条件第一次为真时，即是找到了指定范围内的第一个成员 */
        if (zslValueGteMin(score,range)) {
            /* Check if score <= max. */
            /* 检查范围内第一个成员分数是否满足 score <= max */
            if (zslValueLteMax(score,range))
                return eptr;
            return NULL;
        }

        /* Move to next element. */
        eptr = lpNext(zl,sptr);
    }

    return NULL;
}

/* Find pointer to the last element contained in the specified range.
 * Returns NULL when no element is contained in the range. */

/* 查找指向指定范围内最后一个（分数最大的）成员的指针。
 * 当范围内不包含任何成员时，返回 NULL。 */
unsigned char *zzlLastInRange(unsigned char *zl, zrangespec *range) {
    unsigned char *eptr = lpSeek(zl,-2), *sptr;
    double score;

    /* If everything is out of range, return early. */
    /* 如果 zset 内不存在输入的范围，则提前返回 */
    if (!zzlIsInRange(zl,range)) return NULL;

    /* 从 listpack 中最后一个成员向前遍历 */
    while (eptr != NULL) {
        sptr = lpNext(zl,eptr);
        serverAssert(sptr != NULL);

        score = zzlGetScore(sptr);

        /* 该条件第一次为真时，即是找到了指定范围内的最后一个成员 */
        if (zslValueLteMax(score,range)) {
            /* Check if score >= min. */
             /* 检查范围内的最后一个成员是否满足 score >= min */
            if (zslValueGteMin(score,range))
                return eptr;
            return NULL;
        }

        /* Move to previous element by moving to the score of previous element.
         * When this returns NULL, we know there also is no element. */

        /* 通过移动到前一个成员的分数来移动到前一个成员。
         * 如果 sptr 返回 NULL ，我们就知道前面已经没有成员了 */
        sptr = lpPrev(zl,eptr);
        if (sptr != NULL)
            serverAssert((eptr = lpPrev(zl,sptr)) != NULL);
        else
            eptr = NULL;
    }

    return NULL;
}

/* 根据（字典序）最小值是否具有排他性，检查 value 是否合法 */
int zzlLexValueGteMin(unsigned char *p, zlexrangespec *spec) {
    sds value = lpGetObject(p);
    int res = zslLexValueGteMin(value,spec); /* 共用了跳表的字典序比较函数 */
    sdsfree(value);
    return res;
}

/* 根据（字典序）最大值是否具有排他性，检查 value 是否合法 */
int zzlLexValueLteMax(unsigned char *p, zlexrangespec *spec) {
    sds value = lpGetObject(p);
    int res = zslLexValueLteMax(value,spec);
    sdsfree(value);
    return res;
}

/* Returns if there is a part of the zset is in range. Should only be used
 * internally by zzlFirstInLexRange and zzlLastInLexRange. */

/* 如果 zset 的一部分在指定的字典序范围内，则返回。
 * 只能由 zzlFirstInLexRange 和 zzlLastInLexRange 函数使用。 */
int zzlIsInLexRange(unsigned char *zl, zlexrangespec *range) {
    unsigned char *p;

    /* Test for ranges that will always be empty. */
    /* 测试永远为空的范围 */
    int cmp = sdscmplex(range->min,range->max);
    if (cmp > 0 || (cmp == 0 && (range->minex || range->maxex)))
        return 0;

    p = lpSeek(zl,-2); /* 最后一个（分数最大的）成员 */
    if (p == NULL) return 0;
    if (!zzlLexValueGteMin(p,range))
        return 0;

    p = lpSeek(zl,0); /* 第一个（分数最小的）成员 */
    serverAssert(p != NULL);
    if (!zzlLexValueLteMax(p,range))
        return 0;

    return 1;
}

/* Find pointer to the first element contained in the specified lex range.
 * Returns NULL when no element is contained in the range. */

/* 查找指向包含在指定字典序范围内的第一个元素的指针。 当范围内不包含任何元素时返回 NULL。 */
unsigned char *zzlFirstInLexRange(unsigned char *zl, zlexrangespec *range) {
    unsigned char *eptr = lpSeek(zl,0), *sptr;

    /* If everything is out of range, return early. */
    /* 如果 zset 内不存在输入的范围，则提前返回 */
    if (!zzlIsInLexRange(zl,range)) return NULL;

    while (eptr != NULL) {
        if (zzlLexValueGteMin(eptr,range)) {
            /* Check if score <= max. */
            /* 检查范围内第一个元素字典序是否小于范围最大值 */
            if (zzlLexValueLteMax(eptr,range))
                return eptr;
            return NULL;
        }

        /* 移动到下一个成员 */
        sptr = lpNext(zl,eptr); /* This element score. Skip it. */
        serverAssert(sptr != NULL);
        eptr = lpNext(zl,sptr); /* Next element. */
    }

    return NULL;
}

/* Find pointer to the last element contained in the specified lex range.
 * Returns NULL when no element is contained in the range. */

/* 查找指向包含在指定字典序范围内的最后一个元素的指针。
 * 当范围内不包含任何元素时，返回 NULL。 */
unsigned char *zzlLastInLexRange(unsigned char *zl, zlexrangespec *range) {
    unsigned char *eptr = lpSeek(zl,-2), *sptr;

    /* If everything is out of range, return early. */
    /* 如果 zset 内不存在输入的范围，则提前返回 */
    if (!zzlIsInLexRange(zl,range)) return NULL;

    while (eptr != NULL) {
        if (zzlLexValueLteMax(eptr,range)) {
            /* Check if score >= min. */
            /* 检查范围内最后一个元素字典序是否大于范围最小值 */
            if (zzlLexValueGteMin(eptr,range))
                return eptr;
            return NULL;
        }

        /* Move to previous element by moving to the score of previous element.
         * When this returns NULL, we know there also is no element. */
        /* 通过移动到前一个成员的分数来移动到前一个元素。
         * 如果 sptr 返回 NULL ，我们就知道前面已经没有成员了 */
        sptr = lpPrev(zl,eptr);
        if (sptr != NULL)
            serverAssert((eptr = lpPrev(zl,sptr)) != NULL);
        else
            eptr = NULL;
    }

    return NULL;
}

/* 根据指定的元素（ele）来查找该成员是否存在，
 * 存在返回指向该成员元素的指针，同时令 *score 指向该成员的分数，不存在返回 NULL。*/
unsigned char *zzlFind(unsigned char *lp, sds ele, double *score) {
    unsigned char *eptr, *sptr;

    if ((eptr = lpFirst(lp)) == NULL) return NULL;
    eptr = lpFind(lp, eptr, (unsigned char*)ele, sdslen(ele), 1);
    if (eptr) {
        sptr = lpNext(lp,eptr);
        serverAssert(sptr != NULL);

        /* Matching element, pull out score. */
        /* 找到匹配的元素后获取它的分数 */
        if (score != NULL) *score = zzlGetScore(sptr);
        return eptr;
    }

    return NULL;
}

/* Delete (element,score) pair from listpack. Use local copy of eptr because we
 * don't want to modify the one given as argument. */

/* 从 listpack 中删除一个成员(element,score)。 
 * 使用 eptr 的副本，因为我们不想修改作为输入参数的那一个。 */
unsigned char *zzlDelete(unsigned char *zl, unsigned char *eptr) {
    return lpDeleteRangeWithEntry(zl,&eptr,2);
}

/* 将成员加入到底层为 listpack 的有序集合中，
 * 新成员插入的位置是在 eptr 之前 */
unsigned char *zzlInsertAt(unsigned char *zl, unsigned char *eptr, sds ele, double score) {
    unsigned char *sptr;
    char scorebuf[MAX_D2STRING_CHARS];
    int scorelen;
    long long lscore;
    int score_is_long = double2ll(score, &lscore);

    /* 如果 score 不能转换为 long long，则将它转换为字符串 */
    if (!score_is_long)
        scorelen = d2string(scorebuf,sizeof(scorebuf),score);

    /* 如果 eptr（要插入的位置上现存的元素） 为 NULL，则直接在 listpack 末尾追加要插入的成员 */
    if (eptr == NULL) {
        zl = lpAppend(zl,(unsigned char*)ele,sdslen(ele));
        if (score_is_long)
            zl = lpAppendInteger(zl,lscore);
        else
            zl = lpAppend(zl,(unsigned char*)scorebuf,scorelen);
    } else {
        /* Insert member before the element 'eptr'. */
        /* 在 eptr 前插入元素 */
        zl = lpInsertString(zl,(unsigned char*)ele,sdslen(ele),eptr,LP_BEFORE,&sptr);

        /* Insert score after the member. */
        /* 在元素后插入分数 */
        if (score_is_long)
            zl = lpInsertInteger(zl,lscore,sptr,LP_AFTER,NULL);
        else
            zl = lpInsertString(zl,(unsigned char*)scorebuf,scorelen,sptr,LP_AFTER,NULL);
    }
    return zl;
}

/* Insert (element,score) pair in listpack. This function assumes the element is
 * not yet present in the list. */

/* 在 listpack 中插入成员(element,score)。 此函数假定该成员尚未出现在 listpack 中 */
unsigned char *zzlInsert(unsigned char *zl, sds ele, double score) {
    unsigned char *eptr = lpSeek(zl,0), *sptr;
    double s;

    while (eptr != NULL) {
        sptr = lpNext(zl,eptr);
        serverAssert(sptr != NULL);
        s = zzlGetScore(sptr);

        if (s > score) {
            /* First element with score larger than score for element to be
             * inserted. This means we should take its spot in the list to
             * maintain ordering. */

            /* listpack 中第一个成员的分数大于要插入成员的分数。 
             * 这意味着我们应该在 listpack 中占据第一个位置以保持有序性。 */
            zl = zzlInsertAt(zl,eptr,ele,score);
            break;
        } else if (s == score) {
            /* Ensure lexicographical ordering for elements. */
            /* 确保元素的字典顺序 */
            if (zzlCompareElements(eptr,(unsigned char*)ele,sdslen(ele)) > 0) {
                zl = zzlInsertAt(zl,eptr,ele,score);
                break;
            }
        }

        /* Move to next element. */
        eptr = lpNext(zl,sptr);
    }

    /* Push on tail of list when it was not yet inserted. */
    /* 如果遍历完毕后未找到合适的插入位置，则在 listpack 末尾追加该成员 */
    if (eptr == NULL)
        zl = zzlInsertAt(zl,NULL,ele,score);
    return zl;
}

/* 删除指定分数范围内的成员 */
unsigned char *zzlDeleteRangeByScore(unsigned char *zl, zrangespec *range, unsigned long *deleted) {
    unsigned char *eptr, *sptr;
    double score;
    unsigned long num = 0;

    if (deleted != NULL) *deleted = 0;

    eptr = zzlFirstInRange(zl,range); /* 获取指定范围内的第一个元素 */
    if (eptr == NULL) return zl;

    /* When the tail of the listpack is deleted, eptr will be NULL. */
    /* 当 listpack 的尾部成员被删除时， eptr 将为 NULL。*/
    while (eptr && (sptr = lpNext(zl,eptr)) != NULL) {
        score = zzlGetScore(sptr);
        if (zslValueLteMax(score,range)) {
            /* Delete both the element and the score. */
            /* 将元素和分数一起删除 */
            
            /* 你可能会问：“元素被删除了那 eptr 指针指向的地址不会失效了吗？”
             * 其实在下方的 lpDeleteRangeWithEntry 函数中不仅删除了元素和分数，
             * 还重新调整了 eptr 指向的地址，详情请自行查看该函数的具体实现 */
            zl = lpDeleteRangeWithEntry(zl,&eptr,2);
            num++;
        } else {
            /* No longer in range. */
            /* 当前成员分数已不在指定的范围内 */
            break;
        }
    }

    /* 如果输入参数的 deleted 不为 NULL，则表示需要返回（通过 *deleted 引用）删除的元素个数 */
    if (deleted != NULL) *deleted = num;
    return zl;
}

/* 删除指定字典序范围内的成员 */
unsigned char *zzlDeleteRangeByLex(unsigned char *zl, zlexrangespec *range, unsigned long *deleted) {
    unsigned char *eptr, *sptr;
    unsigned long num = 0;

    if (deleted != NULL) *deleted = 0;

    eptr = zzlFirstInLexRange(zl,range); /* 获取指定范围内的第一个元素 */
    if (eptr == NULL) return zl;

    /* When the tail of the listpack is deleted, eptr will be NULL. */
    /* 当 listpack 的尾部成员被删除时， eptr 将为 NULL。*/
    while (eptr && (sptr = lpNext(zl,eptr)) != NULL) {
        if (zzlLexValueLteMax(eptr,range)) {
            /* Delete both the element and the score. */
            /* 将元素和分数一起删除 */
            zl = lpDeleteRangeWithEntry(zl,&eptr,2);
            num++;
        } else {
            /* No longer in range. */
            /* 当前元素字典序已不在指定的范围内 */
            break;
        }
    }

    if (deleted != NULL) *deleted = num;
    return zl;
}

/* Delete all the elements with rank between start and end from the skiplist.
 * Start and end are inclusive. Note that start and end need to be 1-based */

/* 从跳表中删除所有 rank（排位）在 start 和 end 之间的成员。
 * start 和 end 也包含在内，注意 start 和 end 需要从 1 开始 */
unsigned char *zzlDeleteRangeByRank(unsigned char *zl, unsigned int start, unsigned int end, unsigned long *deleted) {
    unsigned int num = (end-start)+1;
    if (deleted) *deleted = num;
    zl = lpDeleteRange(zl,2*(start-1),2*num);
    return zl;
}

/*-----------------------------------------------------------------------------
 * Common sorted set API
 *----------------------------------------------------------------------------*/

/* 获取 zset 的成员数 */
unsigned long zsetLength(const robj *zobj) {
    unsigned long length = 0;
    if (zobj->encoding == OBJ_ENCODING_LISTPACK) {
        length = zzlLength(zobj->ptr);
    } else if (zobj->encoding == OBJ_ENCODING_SKIPLIST) {
        length = ((const zset*)zobj->ptr)->zsl->length;
    } else {
        serverPanic("Unknown sorted set encoding");
    }
    return length;
}

/* zset 的内部编码转换 */
void zsetConvert(robj *zobj, int encoding) {
    zset *zs;
    zskiplistNode *node, *next;
    sds ele;
    double score;

    if (zobj->encoding == encoding) return; /* 要转换的编码和原来的编码相同，则不做任何操作，直接返回 */
    if (zobj->encoding == OBJ_ENCODING_LISTPACK) {
        unsigned char *zl = zobj->ptr;
        unsigned char *eptr, *sptr;
        unsigned char *vstr;
        unsigned int vlen;
        long long vlong;

        if (encoding != OBJ_ENCODING_SKIPLIST)
            serverPanic("Unknown target encoding");

        /* 创建 zset 以及它的成员（字典与跳表） */
        zs = zmalloc(sizeof(*zs));
        zs->dict = dictCreate(&zsetDictType);
        zs->zsl = zslCreate();

        eptr = lpSeek(zl,0);
        if (eptr != NULL) {
            sptr = lpNext(zl,eptr);
            serverAssertWithInfo(NULL,zobj,sptr != NULL);
        }

        /* 遍历 listpack，将所有成员加入到跳表与字典（哈希表）中 */
        while (eptr != NULL) {
            score = zzlGetScore(sptr);
            vstr = lpGetValue(eptr,&vlen,&vlong);
            if (vstr == NULL)
                ele = sdsfromlonglong(vlong);
            else
                ele = sdsnewlen((char*)vstr,vlen);

            node = zslInsert(zs->zsl,score,ele);
            serverAssert(dictAdd(zs->dict,ele,&node->score) == DICT_OK);
            zzlNext(zl,&eptr,&sptr);
        }

        zfree(zobj->ptr); /* 释放原来的 listpack */
        zobj->ptr = zs; /* 改变有序集合对象的指针指向 */
        zobj->encoding = OBJ_ENCODING_SKIPLIST; /* 改变有序集合对象的编码信息 */
    } else if (zobj->encoding == OBJ_ENCODING_SKIPLIST) {
        unsigned char *zl = lpNew(0);

        if (encoding != OBJ_ENCODING_LISTPACK)
            serverPanic("Unknown target encoding");

        /* Approach similar to zslFree(), since we want to free the skiplist at
         * the same time as creating the listpack. */
        /* 类似于 zslFree() 的方法，因为我们希望在创建 listpack 的同时释放跳表。*/
        zs = zobj->ptr;
        dictRelease(zs->dict);
        node = zs->zsl->header->level[0].forward;
        zfree(zs->zsl->header);
        zfree(zs->zsl);

        /* 遍历跳表所有节点，将所有成员加入到 listpack 中 */
        while (node) {
            zl = zzlInsertAt(zl,NULL,node->ele,node->score);
            next = node->level[0].forward;
            zslFreeNode(node);
            node = next;
        }

        zfree(zs);
        zobj->ptr = zl;
        zobj->encoding = OBJ_ENCODING_LISTPACK;
    } else {
        serverPanic("Unknown sorted set encoding");
    }
}

/* Convert the sorted set object into a listpack if it is not already a listpack
 * and if the number of elements and the maximum element size and total elements size
 * are within the expected ranges. */

/* 如果有序集合对象不是一个 listpack，
 * 并且元素数量、最大元素大小和总元素大小在预期范围内，
 * 则将其转换为一个 listpack。*/
void zsetConvertToListpackIfNeeded(robj *zobj, size_t maxelelen, size_t totelelen) {
    if (zobj->encoding == OBJ_ENCODING_LISTPACK) return;
    zset *zset = zobj->ptr;

    if (zset->zsl->length <= server.zset_max_listpack_entries &&
        maxelelen <= server.zset_max_listpack_value &&
        lpSafeToAdd(NULL, totelelen))
    {
        zsetConvert(zobj,OBJ_ENCODING_LISTPACK);
    }
}

/* Return (by reference) the score of the specified member of the sorted set
 * storing it into *score. If the element does not exist C_ERR is returned
 * otherwise C_OK is returned and *score is correctly populated.
 * If 'zobj' or 'member' is NULL, C_ERR is returned. */

/* 返回（通过引用的方式）有序集合中指定成员的分数，也就是将分数存储到 *score 中。
 * 如果该成员不存在，则返回 C_ERR，
 * 否则返回C_OK，并且*score被正确填充。
 * 如果 'zobj' 或 'member' 是 NULL，则返回 C_ERR。*/
int zsetScore(robj *zobj, sds member, double *score) {
    if (!zobj || !member) return C_ERR;

    if (zobj->encoding == OBJ_ENCODING_LISTPACK) {
        if (zzlFind(zobj->ptr, member, score) == NULL) return C_ERR;
    } else if (zobj->encoding == OBJ_ENCODING_SKIPLIST) {
        zset *zs = zobj->ptr;
        dictEntry *de = dictFind(zs->dict, member); /* 在哈希表里查找是否存在该成员，而非使用跳表查找 */
        if (de == NULL) return C_ERR;
        *score = *(double*)dictGetVal(de);
    } else {
        serverPanic("Unknown sorted set encoding");
    }
    return C_OK;
}

/* Add a new element or update the score of an existing element in a sorted
 * set, regardless of its encoding.
 *
 * The set of flags change the command behavior. 
 *
 * The input flags are the following:
 *
 * ZADD_INCR: Increment the current element score by 'score' instead of updating
 *            the current element score. If the element does not exist, we
 *            assume 0 as previous score.
 * ZADD_NX:   Perform the operation only if the element does not exist.
 * ZADD_XX:   Perform the operation only if the element already exist.
 * ZADD_GT:   Perform the operation on existing elements only if the new score is 
 *            greater than the current score.
 * ZADD_LT:   Perform the operation on existing elements only if the new score is 
 *            less than the current score.
 *
 * When ZADD_INCR is used, the new score of the element is stored in
 * '*newscore' if 'newscore' is not NULL.
 *
 * The returned flags are the following:
 *
 * ZADD_NAN:     The resulting score is not a number.
 * ZADD_ADDED:   The element was added (not present before the call).
 * ZADD_UPDATED: The element score was updated.
 * ZADD_NOP:     No operation was performed because of NX or XX.
 *
 * Return value:
 *
 * The function returns 1 on success, and sets the appropriate flags
 * ADDED or UPDATED to signal what happened during the operation (note that
 * none could be set if we re-added an element using the same score it used
 * to have, or in the case a zero increment is used).
 *
 * The function returns 0 on error, currently only when the increment
 * produces a NAN condition, or when the 'score' value is NAN since the
 * start.
 *
 * The command as a side effect of adding a new element may convert the sorted
 * set internal encoding from listpack to hashtable+skiplist.
 *
 * Memory management of 'ele':
 *
 * The function does not take ownership of the 'ele' SDS string, but copies
 * it if needed. */

/* 在有序集合里添加一个新的成员或更新一个现有成员的分数
 * 
 * 标志改变了命令的行为。
 * 输入的标志有以下几种：
 *
 * ZADD_IN_INCR: 用 'score' 增加当前成员的分数，而不是更新当前成员的分数。
 * 如果该成员不存在，我们假设当前成员的分数为0。
 * ZADD_IN_NX: 只有在成员不存在的情况下才执行操作。
 * ZADD_IN_XX: 只在成员已经存在的情况下执行操作。
 * ZADD_IN_GT: 只有在新的分数大于当前分数时，才对现有的成员进行操作。
 * ZADD_IN_LT: 只有当新的分数小于当前分数时，才对现有的成员进行操作。
 *
 * 当使用 ZADD_INCR 时，元素的新分数被存储在 *newscore 中（如果 *newscore 不为 NULL）
 *
 * 返回的标志有以下几种：
 *
 * ZADD_OUT_NAN: 得到的分数不是一个数字。
 * ZADD_OUT_ADDED: 该元素被添加（调用前不存在）。
 * ZADD_OUT_UPDATED: 该元素的分数被更新。
 * ZADD_OUT_NOP: 由于 NX 或 XX 的原因，没有进行任何操作。
 *
 * 返回值：
 * 
 * 该函数成功执行时返回1，并设置适当的标志。
 * ADDED 或 UPDATED 来表示操作过程中发生了什么
 *（注意，如果我们用一个元素原来的分数进行更新，或者在使用0增量（INCR）的情况下，可以不设置）
 * 该函数在出错时返回0，目前只有当增量产生 NAN 的情况下，
 * 或者当 'score' 的值从一开始就是NAN时，该函数才会返回0。
 *
 * 该命令作为添加新元素的副作用，可能会将排序后的
 * 的内部编码从 listpack 转换成 hashtable+skiplist。
 *
 * "ele" 的内存管理：
 * 该函数不占用 'ele' SDS字符串的所有权，只在需要时复制它并使用它的副本 */
int zsetAdd(robj *zobj, double score, sds ele, int in_flags, int *out_flags, double *newscore) {
    /* Turn options into simple to check vars. */
    /* 将输入参数中的选项变成简单的变量 */
    int incr = (in_flags & ZADD_IN_INCR) != 0;
    int nx = (in_flags & ZADD_IN_NX) != 0;
    int xx = (in_flags & ZADD_IN_XX) != 0;
    int gt = (in_flags & ZADD_IN_GT) != 0;
    int lt = (in_flags & ZADD_IN_LT) != 0;
    *out_flags = 0; /* We'll return our response flags. */
    double curscore;

    /* NaN as input is an error regardless of all the other parameters. */
    /* 输入的 score 为 NAN（非数字）是一个与其他参数无关的错误 */
    if (isnan(score)) {

        /* 设置回复标志 */
        *out_flags = ZADD_OUT_NAN;
        return 0;
    }

    /* Update the sorted set according to its encoding. */
    /* 根据内部编码来更新有序集合 */
    if (zobj->encoding == OBJ_ENCODING_LISTPACK) {
        unsigned char *eptr;

        if ((eptr = zzlFind(zobj->ptr,ele,&curscore)) != NULL) {
            /* NX? Return, same element already exists. */
            /* 如果有 nx 标志，因为相同元素已经存在，所以提前返回 */
            if (nx) {
                *out_flags |= ZADD_OUT_NOP;
                return 1;
            }

            /* Prepare the score for the increment if needed. */
            /* 如果有 incr 标志，则准备好加上增量后的分数 */
            if (incr) {
                score += curscore;
                if (isnan(score)) {
                    *out_flags |= ZADD_OUT_NAN;
                    return 0;
                }
            }

            /* GT/LT? Only update if score is greater/less than current. */
            /* 如果有 GT 或 LT 标志，则要更新的分数 大于/小于 当前成员的分数时才进行更新，否则提前返回 */
            if ((lt && score >= curscore) || (gt && score <= curscore)) {
                *out_flags |= ZADD_OUT_NOP;
                return 1;
            }

            if (newscore) *newscore = score;

            /* Remove and re-insert when score changed. */
            /* 若分数更新后发生改变，则先将原成员删除，再插入带有新分数的成员 */
            if (score != curscore) {
                zobj->ptr = zzlDelete(zobj->ptr,eptr);
                zobj->ptr = zzlInsert(zobj->ptr,ele,score);
                *out_flags |= ZADD_OUT_UPDATED;
            }
            return 1;
        } else if (!xx) {
            /* check if the element is too large or the list
             * becomes too long *before* executing zzlInsert. */
            /* 在执行 zzlInsert 之前，检查元素是否过大或 listpack 是否过长 */
            if (zzlLength(zobj->ptr)+1 > server.zset_max_listpack_entries ||
                sdslen(ele) > server.zset_max_listpack_value ||
                !lpSafeToAdd(zobj->ptr, sdslen(ele)))
            {
                zsetConvert(zobj,OBJ_ENCODING_SKIPLIST); /* 转换为跳表 */
            } else {
                zobj->ptr = zzlInsert(zobj->ptr,ele,score);
                if (newscore) *newscore = score;
                *out_flags |= ZADD_OUT_ADDED;
                return 1;
            }
        } else {
            *out_flags |= ZADD_OUT_NOP;
            return 1;
        }
    }

    /* Note that the above block handling listpack would have either returned or
     * converted the key to skiplist. */

    /* 注意，上述处理 listpack 的代码会返回或转换为 skiplist，
     * 若转换为 skiplist 还需要继续执行操作，所以这里用的是 if 而不是 else if */
    if (zobj->encoding == OBJ_ENCODING_SKIPLIST) {
        zset *zs = zobj->ptr;
        zskiplistNode *znode;
        dictEntry *de;

        de = dictFind(zs->dict,ele);
        if (de != NULL) {
            /* NX? Return, same element already exists. */
            if (nx) {
                *out_flags |= ZADD_OUT_NOP;
                return 1;
            }

            curscore = *(double*)dictGetVal(de);

            /* Prepare the score for the increment if needed. */
            if (incr) {
                score += curscore;
                if (isnan(score)) {
                    *out_flags |= ZADD_OUT_NAN;
                    return 0;
                }
            }

            /* GT/LT? Only update if score is greater/less than current. */
            if ((lt && score >= curscore) || (gt && score <= curscore)) {
                *out_flags |= ZADD_OUT_NOP;
                return 1;
            }

            if (newscore) *newscore = score;

            /* Remove and re-insert when score changes. */
            if (score != curscore) {
                znode = zslUpdateScore(zs->zsl,curscore,ele,score);
                /* Note that we did not removed the original element from
                 * the hash table representing the sorted set, so we just
                 * update the score. */

                /* 注意，我们并没有从代表有序集合的哈希表中删除原来的成员，
                 * 所以我们只是更新它的分数。*/
                dictGetVal(de) = &znode->score; /* Update score ptr. */
                *out_flags |= ZADD_OUT_UPDATED;
            }
            return 1;

        /* 当前元素不在集合中且没有 xx 标志，则创建一个新成员 */
        } else if (!xx) {
            ele = sdsdup(ele);
            znode = zslInsert(zs->zsl,score,ele);
            serverAssert(dictAdd(zs->dict,ele,&znode->score) == DICT_OK);
            *out_flags |= ZADD_OUT_ADDED;
            if (newscore) *newscore = score;
            return 1;
        } else {
            *out_flags |= ZADD_OUT_NOP;
            return 1;
        }
    } else {
        serverPanic("Unknown sorted set encoding");
    }
    return 0; /* Never reached. */
}

/* Deletes the element 'ele' from the sorted set encoded as a skiplist+dict,
 * returning 1 if the element existed and was deleted, 0 otherwise (the
 * element was not there). It does not resize the dict after deleting the
 * element. */

/* 从底层为 skiplist + dict（跳表+哈希表） 的有序集合中删除元素 'ele' ，
 * 如果该元素存在并被删除返回1，否则返回0（该元素不存在）。
 * 在删除元素后，不会调整 dict 的大小。*/
static int zsetRemoveFromSkiplist(zset *zs, sds ele) {
    dictEntry *de;
    double score;

    de = dictUnlink(zs->dict,ele);
    if (de != NULL) {
        /* Get the score in order to delete from the skiplist later. */
        /* 获取分数，以便之后从跳表中删除 */
        score = *(double*)dictGetVal(de);

        /* Delete from the hash table and later from the skiplist.
         * Note that the order is important: deleting from the skiplist
         * actually releases the SDS string representing the element,
         * which is shared between the skiplist and the hash table, so
         * we need to delete from the skiplist as the final step. */

        /* 从哈希表中删除，然后再从跳表中删除。
         * 注意这个顺序很重要：
         * 从跳表中删除实际上是释放代表该元素的 SDS 字符串。
         * 它是在跳表和哈希表之间共享的，
         * 所以我们需要在最后一步从跳表中删除。*/
        dictFreeUnlinkedEntry(zs->dict,de);

        /* Delete from skiplist. */
        /* 从哈希表中删除 */
        int retval = zslDelete(zs->zsl,score,ele,NULL);
        serverAssert(retval);

        return 1;
    }

    return 0;
}

/* Delete the element 'ele' from the sorted set, returning 1 if the element
 * existed and was deleted, 0 otherwise (the element was not there). */

/* 从有序集合中删除元素 'ele'，
 * 如果该元素存在并被删除，否则返回0（该元素不存在） */
int zsetDel(robj *zobj, sds ele) {
    if (zobj->encoding == OBJ_ENCODING_LISTPACK) {
        unsigned char *eptr;

        if ((eptr = zzlFind(zobj->ptr,ele,NULL)) != NULL) {
            zobj->ptr = zzlDelete(zobj->ptr,eptr);
            return 1;
        }
    } else if (zobj->encoding == OBJ_ENCODING_SKIPLIST) {
        zset *zs = zobj->ptr;
        if (zsetRemoveFromSkiplist(zs, ele)) {

            /* 删除元素后尝试将哈希表缩容 */
            if (htNeedsResize(zs->dict)) dictResize(zs->dict);
            return 1;
        }
    } else {
        serverPanic("Unknown sorted set encoding");
    }
    return 0; /* No such element found. */
}

/* Given a sorted set object returns the 0-based rank of the object or
 * -1 if the object does not exist.
 *
 * For rank we mean the position of the element in the sorted collection
 * of elements. So the first element has rank 0, the second rank 1, and so
 * forth up to length-1 elements.
 *
 * If 'reverse' is false, the rank is returned considering as first element
 * the one with the lowest score. Otherwise if 'reverse' is non-zero
 * the rank is computed considering as element with rank 0 the one with
 * the highest score. */

/* 给定一个有序集合对象，返回该对象的排名 "rank"，
 * 如果对象不存在，则返回-1。
 *
 * 对于 rank，我们指的是该元素在有序集合中的位置。
 * 所以第一个元素的 rank 是 0，第二个是1，以此类推。
 *
 * 如果 'reverse' 为 false，那么返回的 rank 将把分数最低的元素作为第一个元素。（从小到大）
 * 否则，在计算排名时，会考虑将分数最高的元素作为 rank 为 0 的元素。（从大到小） */
long zsetRank(robj *zobj, sds ele, int reverse) {
    unsigned long llen;
    unsigned long rank;

    llen = zsetLength(zobj);

    if (zobj->encoding == OBJ_ENCODING_LISTPACK) {
        unsigned char *zl = zobj->ptr;
        unsigned char *eptr, *sptr;

        /* 将 eptr 和 sptr 初始化，指向第一个（分数最小的）成员 */
        eptr = lpSeek(zl,0);
        serverAssert(eptr != NULL);
        sptr = lpNext(zl,eptr);
        serverAssert(sptr != NULL);

        rank = 1;

        /* 从第一个元素开始向后遍历查找成员 */
        while(eptr != NULL) {
            if (lpCompare(eptr,(unsigned char*)ele,sdslen(ele)))
                break;
            rank++;
            zzlNext(zl,&eptr,&sptr);
        }

        if (eptr != NULL) {

            /* 如果是 reverse，则按分数从大到小的顺序返回排名 */
            if (reverse)
                return llen-rank;
            else
                return rank-1;
        } else {
            return -1;
        }
    } else if (zobj->encoding == OBJ_ENCODING_SKIPLIST) {
        zset *zs = zobj->ptr;
        zskiplist *zsl = zs->zsl;
        dictEntry *de;
        double score;

        de = dictFind(zs->dict,ele);

        /* 在哈希表查找到对应的成员后，到跳表中获取该成员的排名 */
        if (de != NULL) {
            score = *(double*)dictGetVal(de);
            rank = zslGetRank(zsl,score,ele);
            /* Existing elements always have a rank. */
            serverAssert(rank != 0);
            if (reverse)
                return llen-rank;
            else
                return rank-1;
        } else {
            return -1;
        }
    } else {
        serverPanic("Unknown sorted set encoding");
    }
}

/* This is a helper function for the COPY command.
 * Duplicate a sorted set object, with the guarantee that the returned object
 * has the same encoding as the original one.
 *
 * The resulting object always has refcount set to 1 */

/* 这是一个用于 COPY 命令的辅助函数。
 * 复制一个有序集合对象，并保证返回的对象与原始对象的编码相同。
 * 返回的对象总是将 refcount 设置为1 */

robj *zsetDup(robj *o) {
    robj *zobj;
    zset *zs;
    zset *new_zs;

    serverAssert(o->type == OBJ_ZSET);

    /* Create a new sorted set object that have the same encoding as the original object's encoding */
    /* 创建一个新的有序集合对象，其编码与原对象的编码相同 */
    if (o->encoding == OBJ_ENCODING_LISTPACK) {
        unsigned char *zl = o->ptr;
        size_t sz = lpBytes(zl);
        unsigned char *new_zl = zmalloc(sz);
        memcpy(new_zl, zl, sz);
        zobj = createObject(OBJ_ZSET, new_zl);
        zobj->encoding = OBJ_ENCODING_LISTPACK;
    } else if (o->encoding == OBJ_ENCODING_SKIPLIST) {
        zobj = createZsetObject();
        zs = o->ptr;
        new_zs = zobj->ptr;
        dictExpand(new_zs->dict,dictSize(zs->dict));
        zskiplist *zsl = zs->zsl;
        zskiplistNode *ln;
        sds ele;
        long llen = zsetLength(o);

        /* We copy the skiplist elements from the greatest to the
         * smallest (that's trivial since the elements are already ordered in
         * the skiplist): this improves the load process, since the next loaded
         * element will always be the smaller, so adding to the skiplist
         * will always immediately stop at the head, making the insertion
         * O(1) instead of O(log(N)). */

        /* 我们将跳表中的成员从分数最大复制到分数最小的（这样的顺序是开销是最小的，因为元素在跳表中已经排序过了）
         * 这改善了插入过程，因为下一个插入的成员分数总是当前最小的，
         * 所以添加到新跳表中的成员总是能停在跳表头部（不需要调整位置），使得插入是 O(1)而不是 O(log(N)) */
        ln = zsl->tail;
        while (llen--) {
            ele = ln->ele;
            sds new_ele = sdsdup(ele);
            zskiplistNode *znode = zslInsert(new_zs->zsl,ln->score,new_ele);
            dictAdd(new_zs->dict,new_ele,&znode->score);
            ln = ln->backward;
        }
    } else {
        serverPanic("Unknown sorted set encoding");
    }
    return zobj;
}

/* Create a new sds string from the listpack entry. */
/* 用 listpack 中的元素创建一个新的 sds 字符串 */
sds zsetSdsFromListpackEntry(listpackEntry *e) {
    return e->sval ? sdsnewlen(e->sval, e->slen) : sdsfromlonglong(e->lval);
}

/* Reply with bulk string from the listpack entry. */
/* 将 listpack 中的元素值回复给客户端 */
void zsetReplyFromListpackEntry(client *c, listpackEntry *e) {
    if (e->sval)
        addReplyBulkCBuffer(c, e->sval, e->slen);
    else
        addReplyBulkLongLong(c, e->lval);
}


/* Return random element from a non empty zset.
 * 'key' and 'val' will be set to hold the element.
 * The memory in `key` is not to be freed or modified by the caller.
 * 'score' can be NULL in which case it's not extracted. */

/* 从一个非空的 zset 中返回随机元素。
 * 'key' 和 'val' 将用来存储该元素。
 * `key` 的内存不能被释放或被调用者修改。
 * 'score' 可以是 NULL，在这种情况下我们不会提取成员的分数。*/
void zsetTypeRandomElement(robj *zsetobj, unsigned long zsetsize, listpackEntry *key, double *score) {
    if (zsetobj->encoding == OBJ_ENCODING_SKIPLIST) {
        zset *zs = zsetobj->ptr;
        dictEntry *de = dictGetFairRandomKey(zs->dict); /* 从哈希表中随机获取一个成员 */
        sds s = dictGetKey(de);
        key->sval = (unsigned char*)s;
        key->slen = sdslen(s);

        /* 根据输入参数 score 是否为 NULL，决定是否提取成员的分数 */
        if (score)
            *score = *(double*)dictGetVal(de);
    } else if (zsetobj->encoding == OBJ_ENCODING_LISTPACK) {
        listpackEntry val;
        lpRandomPair(zsetobj->ptr, zsetsize, key, &val); /* 从 listpack 中随机获取一个成员  */
        if (score) {
            if (val.sval) {
                *score = zzlStrtod(val.sval,val.slen);
            } else {
                *score = (double)val.lval;
            }
        }
    } else {
        serverPanic("Unknown zset encoding");
    }
}

/*-----------------------------------------------------------------------------
 * Sorted set commands
 *----------------------------------------------------------------------------*/

/* This generic command implements both ZADD and ZINCRBY. */
/* ZADD 和 ZINCRBY 命令的通用实现函数 */
void zaddGenericCommand(client *c, int flags) {
    static char *nanerr = "resulting score is not a number (NaN)";
    robj *key = c->argv[1];
    robj *zobj;
    sds ele;
    double score = 0, *scores = NULL;
    int j, elements, ch = 0;
    int scoreidx = 0;
    /* The following vars are used in order to track what the command actually
     * did during the execution, to reply to the client and to trigger the
     * notification of keyspace change. */

    /* 以下变量用于跟踪命令在执行过程中的实际操作，以回复客户端并触发关于 key 变化的事件通知。*/
    int added = 0;      /* 增加的新成员数量。*/
    int updated = 0;    /* 发生分数更新的成员数量。*/
    int processed = 0;  /* 处理的元素数量，在有 XX 等选项的情况下可能保持为0。*/

    /* Parse options. At the end 'scoreidx' is set to the argument position
     * of the score of the first score-element pair. */
    /* 解析选项。最后 'scoreidx' 被设置为第一个成员分数的参数位置 */
    scoreidx = 2;
    while(scoreidx < c->argc) {
        char *opt = c->argv[scoreidx]->ptr;
        if (!strcasecmp(opt,"nx")) flags |= ZADD_IN_NX;
        else if (!strcasecmp(opt,"xx")) flags |= ZADD_IN_XX;
        else if (!strcasecmp(opt,"ch")) ch = 1; /* Return num of elements added or updated. */
        else if (!strcasecmp(opt,"incr")) flags |= ZADD_IN_INCR;
        else if (!strcasecmp(opt,"gt")) flags |= ZADD_IN_GT;
        else if (!strcasecmp(opt,"lt")) flags |= ZADD_IN_LT;
        else break;
        scoreidx++;
    }

    /* Turn options into simple to check vars. */
    /* 将选项变成简单的变量 */
    int incr = (flags & ZADD_IN_INCR) != 0;
    int nx = (flags & ZADD_IN_NX) != 0;
    int xx = (flags & ZADD_IN_XX) != 0;
    int gt = (flags & ZADD_IN_GT) != 0;
    int lt = (flags & ZADD_IN_LT) != 0;

    /* After the options, we expect to have an even number of args, since
     * we expect any number of score-element pairs. */

    /* 在解析输入参数中的选项之后，我们期望的剩下的输入参数是偶数个，
     * 因为剩下的输入参数是要添加或更新的成员，
     * 而一个成员是一个 score-element 对（每个成员占2个参数），
     * 如果不满足偶数个（或者为0），则回复客户端语法错误 */
    elements = c->argc-scoreidx;
    if (elements % 2 || !elements) {
        addReplyErrorObject(c,shared.syntaxerr);
        return;
    }
    elements /= 2; /* 成员的数量 */

    /* Check for incompatible options. */
    /* 检查是否有不兼容的选项 */
    if (nx && xx) {
        addReplyError(c,
            "XX and NX options at the same time are not compatible");
        return;
    }
    
    if ((gt && nx) || (lt && nx) || (gt && lt)) {
        addReplyError(c,
            "GT, LT, and/or NX options at the same time are not compatible");
        return;
    }
    /* Note that XX is compatible with either GT or LT */
    /* 注意，XX 与 GT 或 LT 兼容 */

    if (incr && elements > 1) {
        addReplyError(c,
            "INCR option supports a single increment-element pair");
        return;
    }

    /* Start parsing all the scores, we need to emit any syntax error
     * before executing additions to the sorted set, as the command should
     * either execute fully or nothing at all. */

    /* 开始解析所有的分数，
     * 在执行对排序集的添加之前，我们需要检查任何语法错误，
     * 因为该命令要么完全执行，要么什么都不执行。 */

    /* 给 scores 数组分配内存空间，scores 按输入顺序记录每一个成员的分数 */
    scores = zmalloc(sizeof(double)*elements);
    for (j = 0; j < elements; j++) {
        if (getDoubleFromObjectOrReply(c,c->argv[scoreidx+j*2],&scores[j],NULL)
            != C_OK) goto cleanup;
    }

    /* Lookup the key and create the sorted set if does not exist. */
    /* 查找 key，如果不存在，则创建一个有序集合 */
    zobj = lookupKeyWrite(c->db,key);
    if (checkType(c,zobj,OBJ_ZSET)) goto cleanup;
    if (zobj == NULL) {
        if (xx) goto reply_to_client; /* 若 key 不存在，且有 xx 标志，则不执行创建操作 */
        if (server.zset_max_listpack_entries == 0 ||
            server.zset_max_listpack_value < sdslen(c->argv[scoreidx+1]->ptr))
        {
            zobj = createZsetObject();
        } else {
            zobj = createZsetListpackObject();
        }
        dbAdd(c->db,key,zobj); /* 将创建的有序集合对象加入到数据库 */
    }

    /* 遍历输入的元素，执行每一个成员的添加或更新操作 */
    for (j = 0; j < elements; j++) {
        double newscore;
        score = scores[j]; 
        int retflags = 0;

        ele = c->argv[scoreidx+1+j*2]->ptr;
        int retval = zsetAdd(zobj, score, ele, flags, &retflags, &newscore);
        if (retval == 0) {
            addReplyError(c,nanerr);
            goto cleanup;
        }
        if (retflags & ZADD_OUT_ADDED) added++;
        if (retflags & ZADD_OUT_UPDATED) updated++;
        if (!(retflags & ZADD_OUT_NOP)) processed++;
        score = newscore;
    }
    server.dirty += (added+updated);

reply_to_client:
    if (incr) { /* ZINCRBY or INCR option. */
        if (processed)
            addReplyDouble(c,score);
        else
            addReplyNull(c);
    } else { /* ZADD. */
        addReplyLongLong(c,ch ? added+updated : added);
    }

cleanup:
    zfree(scores);

    /* 如果有成员添加或被更新，我们就发送 key 被修改信号和发出事件通知 */
    if (added || updated) {
        signalModifiedKey(c,c->db,key);
        notifyKeyspaceEvent(NOTIFY_ZSET,
            incr ? "zincr" : "zadd", key, c->db->id);
    }
}

/* 处理 zadd 命令的函数 */
void zaddCommand(client *c) {
    zaddGenericCommand(c,ZADD_IN_NONE);
}

/* 处理 zincrby 命令的函数 */
void zincrbyCommand(client *c) {
    zaddGenericCommand(c,ZADD_IN_INCR);
}

/* 处理 zrem 命令的函数 */
void zremCommand(client *c) {
    robj *key = c->argv[1];
    robj *zobj;
    int deleted = 0, keyremoved = 0, j;

    /* 获取 key 对应的有序集合对象，之后检查对象类型 */
    if ((zobj = lookupKeyWriteOrReply(c,key,shared.czero)) == NULL ||
        checkType(c,zobj,OBJ_ZSET)) return;

    /* 解析输入参数，将指定的元素从有序集合中删除 */
    for (j = 2; j < c->argc; j++) {
        if (zsetDel(zobj,c->argv[j]->ptr)) deleted++;

        /* 若有序集合已为空，则将该有序集合对象从数据库中删除 */
        if (zsetLength(zobj) == 0) {
            dbDelete(c->db,key);
            keyremoved = 1;
            break;
        }
    }

    /* 有成员被删除 */
    if (deleted) {
        /* 发送事件通知 */
        notifyKeyspaceEvent(NOTIFY_ZSET,"zrem",key,c->db->id);

        /* 如果 key 被删除，还要再发送一个关于 key 被删除的事件通知 */
        if (keyremoved)
            notifyKeyspaceEvent(NOTIFY_GENERIC,"del",key,c->db->id);
        signalModifiedKey(c,c->db,key); /* 发送 key 被修改信号 */
        server.dirty += deleted; /* 增加脏数据 */
    }
    addReplyLongLong(c,deleted);
}

typedef enum {
    ZRANGE_AUTO = 0,
    ZRANGE_RANK,
    ZRANGE_SCORE,
    ZRANGE_LEX,
} zrange_type;

/* Implements ZREMRANGEBYRANK, ZREMRANGEBYSCORE, ZREMRANGEBYLEX commands. */
/* ZREMRANGEBYRANK, ZREMRANGEBYSCORE, ZREMRANGEBYLEX 命令的通用实现函数 */
void zremrangeGenericCommand(client *c, zrange_type rangetype) {
    robj *key = c->argv[1];
    robj *zobj;
    int keyremoved = 0;
    unsigned long deleted = 0;
    zrangespec range;
    zlexrangespec lexrange;
    long start, end, llen;
    char *notify_type = NULL;

    /* Step 1: Parse the range. */
    /* 第一步：解析指定的范围 */
    if (rangetype == ZRANGE_RANK) {
        notify_type = "zremrangebyrank";
        if ((getLongFromObjectOrReply(c,c->argv[2],&start,NULL) != C_OK) ||
            (getLongFromObjectOrReply(c,c->argv[3],&end,NULL) != C_OK))
            return;
    } else if (rangetype == ZRANGE_SCORE) {
        notify_type = "zremrangebyscore";
        if (zslParseRange(c->argv[2],c->argv[3],&range) != C_OK) {
            addReplyError(c,"min or max is not a float");
            return;
        }
    } else if (rangetype == ZRANGE_LEX) {
        notify_type = "zremrangebylex";
        if (zslParseLexRange(c->argv[2],c->argv[3],&lexrange) != C_OK) {
            addReplyError(c,"min or max not valid string range item");
            return;
        }
    } else {
        serverPanic("unknown rangetype %d", (int)rangetype);
    }

    /* Step 2: Lookup & range sanity checks if needed. */
    /* 第二步：查找 key 和检查范围是否需要转换 */
    if ((zobj = lookupKeyWriteOrReply(c,key,shared.czero)) == NULL ||
        checkType(c,zobj,OBJ_ZSET)) goto cleanup;

    if (rangetype == ZRANGE_RANK) {
        /* Sanitize indexes. */
        /* 转换负数的索引 */
        llen = zsetLength(zobj);
        if (start < 0) start = llen+start;
        if (end < 0) end = llen+end;
        if (start < 0) start = 0;

        /* Invariant: start >= 0, so this test will be true when end < 0.
         * The range is empty when start > end or start >= length. */

        /* 前提操作令 start >= 0，所以当 end < 0 时，该条件将为真。
         * 当 start > end 或 start >= length 时，范围是空的。*/
        if (start > end || start >= llen) {
            addReply(c,shared.czero);
            goto cleanup;
        }
        if (end >= llen) end = llen-1;
    }

    /* Step 3: Perform the range deletion operation. */
    /* 第三步：执行范围删除操作 */
    if (zobj->encoding == OBJ_ENCODING_LISTPACK) {
        switch(rangetype) {
        case ZRANGE_AUTO:
        case ZRANGE_RANK:
            zobj->ptr = zzlDeleteRangeByRank(zobj->ptr,start+1,end+1,&deleted);
            break;
        case ZRANGE_SCORE:
            zobj->ptr = zzlDeleteRangeByScore(zobj->ptr,&range,&deleted);
            break;
        case ZRANGE_LEX:
            zobj->ptr = zzlDeleteRangeByLex(zobj->ptr,&lexrange,&deleted);
            break;
        }
        if (zzlLength(zobj->ptr) == 0) {
            dbDelete(c->db,key);
            keyremoved = 1;
        }
    } else if (zobj->encoding == OBJ_ENCODING_SKIPLIST) {
        zset *zs = zobj->ptr;
        switch(rangetype) {
        case ZRANGE_AUTO:
        case ZRANGE_RANK:
            deleted = zslDeleteRangeByRank(zs->zsl,start+1,end+1,zs->dict);
            break;
        case ZRANGE_SCORE:
            deleted = zslDeleteRangeByScore(zs->zsl,&range,zs->dict);
            break;
        case ZRANGE_LEX:
            deleted = zslDeleteRangeByLex(zs->zsl,&lexrange,zs->dict);
            break;
        }
        if (htNeedsResize(zs->dict)) dictResize(zs->dict);
        if (dictSize(zs->dict) == 0) {
            dbDelete(c->db,key);
            keyremoved = 1;
        }
    } else {
        serverPanic("Unknown sorted set encoding");
    }

    /* Step 4: Notifications and reply. */
    /* 第四步：发送通知和回复客户端 */
    if (deleted) {
        signalModifiedKey(c,c->db,key);
        notifyKeyspaceEvent(NOTIFY_ZSET,notify_type,key,c->db->id);
        if (keyremoved)
            notifyKeyspaceEvent(NOTIFY_GENERIC,"del",key,c->db->id);
    }
    server.dirty += deleted;
    addReplyLongLong(c,deleted); /* 将删除成员的数量回复给客户端 */

cleanup:
    if (rangetype == ZRANGE_LEX) zslFreeLexRange(&lexrange);
}

/* 处理 zremrangebyrank 命令的函数 */
void zremrangebyrankCommand(client *c) {
    zremrangeGenericCommand(c,ZRANGE_RANK);
}

/* 处理 zremrangebyscore 命令的函数 */
void zremrangebyscoreCommand(client *c) {
    zremrangeGenericCommand(c,ZRANGE_SCORE);
}

/* 处理 zremrangebylex 命令的函数 */
void zremrangebylexCommand(client *c) {
    zremrangeGenericCommand(c,ZRANGE_LEX);
}

typedef struct {
    robj *subject;
    int type; /* Set, sorted set */
    int encoding;
    double weight;

    union {
        /* Set iterators. */
        union _iterset {
            struct {
                intset *is;
                int ii;
            } is;
            struct {
                dict *dict;
                dictIterator *di;
                dictEntry *de;
            } ht;
        } set;

        /* Sorted set iterators. */
        union _iterzset {
            struct {
                unsigned char *zl;
                unsigned char *eptr, *sptr;
            } zl;
            struct {
                zset *zs;
                zskiplistNode *node;
            } sl;
        } zset;
    } iter;
} zsetopsrc; /* 集合/有序集合迭代器 */


/* Use dirty flags for pointers that need to be cleaned up in the next
 * iteration over the zsetopval. The dirty flag for the long long value is
 * special, since long long values don't need cleanup. Instead, it means that
 * we already checked that "ell" holds a long long, or tried to convert another
 * representation into a long long value. When this was successful,
 * OPVAL_VALID_LL is set as well. */

/* 对需要在下一次 zsetopval 的迭代中进行清理的指针使用 dirty 标志。
 * long long 型的值的 dirty 标志很特别，因为 long long 的值不需要清理。
 * 相反，它意味着我们已经检查并发现 "ell" 是持有元素的，
 * 或者试图将字符串编码的值转换成一个 long long，
 * 当这个转换成功的时候，OPVAL_VALID_LL 也会被设置到标志上。 */
#define OPVAL_DIRTY_SDS 1
#define OPVAL_DIRTY_LL 2
#define OPVAL_VALID_LL 4

/* Store value retrieved from the iterator. */
/* 存储从迭代器中获取的值 */
typedef struct {
    int flags;
    unsigned char _buf[32]; /* Private buffer. */
    sds ele;
    unsigned char *estr;
    unsigned int elen;
    long long ell;
    double score;
} zsetopval;

typedef union _iterset iterset;
typedef union _iterzset iterzset;

/* 初始化集合/有序集合迭代器 */
void zuiInitIterator(zsetopsrc *op) {
    if (op->subject == NULL)
        return;

    /* 初始化集合迭代器 */
    if (op->type == OBJ_SET) {
        iterset *it = &op->iter.set;
        if (op->encoding == OBJ_ENCODING_INTSET) {
            it->is.is = op->subject->ptr;
            it->is.ii = 0;
        } else if (op->encoding == OBJ_ENCODING_HT) {
            it->ht.dict = op->subject->ptr;
            it->ht.di = dictGetIterator(op->subject->ptr);
            it->ht.de = dictNext(it->ht.di);
        } else {
            serverPanic("Unknown set encoding");
        }
    
    /* 初始化有序集合迭代器 */
    } else if (op->type == OBJ_ZSET) {
        /* Sorted sets are traversed in reverse order to optimize for
         * the insertion of the elements in a new list as in
         * ZDIFF/ZINTER/ZUNION */

        /* 有序集合是以反向遍历的，以优化在一个新的有序集合中插入元素，
         * 如 ZDIFF/ZINTER/ZUNION ，所以我们将迭代器初始化在尾部节点 */
        iterzset *it = &op->iter.zset;
        if (op->encoding == OBJ_ENCODING_LISTPACK) {
            it->zl.zl = op->subject->ptr;
            it->zl.eptr = lpSeek(it->zl.zl,-2);
            if (it->zl.eptr != NULL) {
                it->zl.sptr = lpNext(it->zl.zl,it->zl.eptr);
                serverAssert(it->zl.sptr != NULL);
            }
        } else if (op->encoding == OBJ_ENCODING_SKIPLIST) {
            it->sl.zs = op->subject->ptr;
            it->sl.node = it->sl.zs->zsl->tail;
        } else {
            serverPanic("Unknown sorted set encoding");
        }
    } else {
        serverPanic("Unsupported type");
    }
}

/* 释放集合/有序集合迭代器 */
void zuiClearIterator(zsetopsrc *op) {
    if (op->subject == NULL)
        return;

    if (op->type == OBJ_SET) {
        iterset *it = &op->iter.set;
        if (op->encoding == OBJ_ENCODING_INTSET) {
            UNUSED(it); /* skip */
        } else if (op->encoding == OBJ_ENCODING_HT) {
            dictReleaseIterator(it->ht.di);
        } else {
            serverPanic("Unknown set encoding");
        }
    } else if (op->type == OBJ_ZSET) {
        iterzset *it = &op->iter.zset;
        if (op->encoding == OBJ_ENCODING_LISTPACK) {
            UNUSED(it); /* skip */
        } else if (op->encoding == OBJ_ENCODING_SKIPLIST) {
            UNUSED(it); /* skip */
        } else {
            serverPanic("Unknown sorted set encoding");
        }
    } else {
        serverPanic("Unsupported type");
    }
}

/* 清理迭代器中引用的字符串值 */
void zuiDiscardDirtyValue(zsetopval *val) {
    if (val->flags & OPVAL_DIRTY_SDS) {
        sdsfree(val->ele); /* 释放 SDS 字符串 */
        val->ele = NULL;
        val->flags &= ~OPVAL_DIRTY_SDS; /* 将 OPVAL_DIRTY_SDS 标志位清0 */
    }
}

/* 获取迭代器所在对象的元素总数  */
unsigned long zuiLength(zsetopsrc *op) {
    if (op->subject == NULL)
        return 0;

    if (op->type == OBJ_SET) {
        if (op->encoding == OBJ_ENCODING_INTSET) {
            return intsetLen(op->subject->ptr);
        } else if (op->encoding == OBJ_ENCODING_HT) {
            dict *ht = op->subject->ptr;
            return dictSize(ht);
        } else {
            serverPanic("Unknown set encoding");
        }
    } else if (op->type == OBJ_ZSET) {
        if (op->encoding == OBJ_ENCODING_LISTPACK) {
            return zzlLength(op->subject->ptr);
        } else if (op->encoding == OBJ_ENCODING_SKIPLIST) {
            zset *zs = op->subject->ptr;
            return zs->zsl->length;
        } else {
            serverPanic("Unknown sorted set encoding");
        }
    } else {
        serverPanic("Unsupported type");
    }
}

/* Check if the current value is valid. If so, store it in the passed structure
 * and move to the next element. If not valid, this means we have reached the
 * end of the structure and can abort. */

/* 检查当前值是否有效。如果有效，将其存储在传递的结构（zsetopval）中，并移动到下一个成员。
 * 如果无效，这意味着我们已经到达结构（zsetorsrc）所在的集合/有序集合对象的末端，放弃操作。*/
int zuiNext(zsetopsrc *op, zsetopval *val) {
    if (op->subject == NULL)
        return 0;

    zuiDiscardDirtyValue(val);

    memset(val,0,sizeof(zsetopval));

    if (op->type == OBJ_SET) {
        iterset *it = &op->iter.set;
        if (op->encoding == OBJ_ENCODING_INTSET) {
            int64_t ell;

            if (!intsetGet(it->is.is,it->is.ii,&ell))
                return 0;
            val->ell = ell;
            val->score = 1.0;

            /* Move to next element. */
            it->is.ii++;
        } else if (op->encoding == OBJ_ENCODING_HT) {
            if (it->ht.de == NULL)
                return 0;
            val->ele = dictGetKey(it->ht.de);
            val->score = 1.0;

            /* Move to next element. */
            it->ht.de = dictNext(it->ht.di);
        } else {
            serverPanic("Unknown set encoding");
        }
    } else if (op->type == OBJ_ZSET) {
        iterzset *it = &op->iter.zset;
        if (op->encoding == OBJ_ENCODING_LISTPACK) {
            /* No need to check both, but better be explicit. */
            /* 其实不需要都检查 eptr 和 sptr ，但最好写明确一点 */
            if (it->zl.eptr == NULL || it->zl.sptr == NULL)
                return 0;
            val->estr = lpGetValue(it->zl.eptr,&val->elen,&val->ell);
            val->score = zzlGetScore(it->zl.sptr);

            /* Move to next element (going backwards, see zuiInitIterator). */
            /* 移动到下一个元素（实际上是向头部移动，见 zuiInitIterator 函数的实现）*/
            zzlPrev(it->zl.zl,&it->zl.eptr,&it->zl.sptr);
        } else if (op->encoding == OBJ_ENCODING_SKIPLIST) {
            if (it->sl.node == NULL)
                return 0;
            val->ele = it->sl.node->ele;
            val->score = it->sl.node->score;

            /* Move to next element. (going backwards, see zuiInitIterator) */
            it->sl.node = it->sl.node->backward;
        } else {
            serverPanic("Unknown sorted set encoding");
        }
    } else {
        serverPanic("Unsupported type");
    }
    return 1;
}

/* 从迭代器中获取 long long 型整数值 */
int zuiLongLongFromValue(zsetopval *val) {
    if (!(val->flags & OPVAL_DIRTY_LL)) {
        val->flags |= OPVAL_DIRTY_LL;

        if (val->ele != NULL) {
            if (string2ll(val->ele,sdslen(val->ele),&val->ell))
                val->flags |= OPVAL_VALID_LL;
        } else if (val->estr != NULL) {
            if (string2ll((char*)val->estr,val->elen,&val->ell))
                val->flags |= OPVAL_VALID_LL;
        } else {
            /* The long long was already set, flag as valid. */
            /* long long 型整数值已经被设置成功，标志为有效（valid） */
            val->flags |= OPVAL_VALID_LL;
        }
    }
    return val->flags & OPVAL_VALID_LL;
}

/* 从迭代器中获取 SDS 字符串值 */
sds zuiSdsFromValue(zsetopval *val) {
    if (val->ele == NULL) {
        if (val->estr != NULL) {
            val->ele = sdsnewlen((char*)val->estr,val->elen);
        } else {
            val->ele = sdsfromlonglong(val->ell);
        }

        /* 设置 OPVAL_DIRTY_SDS 标志位为1，
         * 因为原来的值由结构体中其他成员保存，
         * 但我们还是创建了一个用其他形式保存的值创建出来的 sds 字符串，
         * 并令 ele 指针引用了该字符串。
         * 从 ele 取值不是必须的，所以我们称它为 ”脏SDS“。
         * 那为什么要让 ele 引用这个生成出来的字符串呢？
         * 我的看法是为了之后可以再利用，可以看看下方 zuiNewSdsFromValue 函数的做法 */
        val->flags |= OPVAL_DIRTY_SDS; 
    }
    return val->ele;
}

/* This is different from zuiSdsFromValue since returns a new SDS string
 * which is up to the caller to free. */

/* 这与 zuiSdsFromValue 不同，
 * 因为它返回一个新的 SDS 字符串（或者说副本），由调用者决定是否释放。*/
sds zuiNewSdsFromValue(zsetopval *val) {

    /* 迭代器的 OPVAL_DIRTY_SDS 标志位为1，表示 val->ele 有一个 脏SDS字符串，
     * 脏字符串是该迭代器不需要使用的，我们就可以直接返回该字符串，而不用复制一个副本返回 */
    if (val->flags & OPVAL_DIRTY_SDS) {
        /* We have already one to return! */
        /* 我们已经有一个可以直接返回的字符串 */
        sds ele = val->ele;
        val->flags &= ~OPVAL_DIRTY_SDS;
        val->ele = NULL;
        return ele;
    } else if (val->ele) {
        return sdsdup(val->ele); 
    } else if (val->estr) {
        return sdsnewlen((char*)val->estr,val->elen);
    } else {
        return sdsfromlonglong(val->ell);
    }
}

/* 从迭代器中返回字符数组 */
int zuiBufferFromValue(zsetopval *val) {
    if (val->estr == NULL) {
        if (val->ele != NULL) {
            val->elen = sdslen(val->ele);
            val->estr = (unsigned char*)val->ele;
        } else {
            val->elen = ll2string((char*)val->_buf,sizeof(val->_buf),val->ell);
            val->estr = val->_buf;
        }
    }
    return 1;
}

/* Find value pointed to by val in the source pointer to by op. When found,
 * return 1 and store its score in target. Return 0 otherwise. */


/* 在 op 所指向的集合/有序集合对象中找到 val 所指向的值。
 * 当找到时返回1，并将其分数存储在参数 score 中，否则返回0 */
int zuiFind(zsetopsrc *op, zsetopval *val, double *score) {
    if (op->subject == NULL)
        return 0;

    if (op->type == OBJ_SET) {
        if (op->encoding == OBJ_ENCODING_INTSET) {
            if (zuiLongLongFromValue(val) &&
                intsetFind(op->subject->ptr,val->ell))
            {
                *score = 1.0; /* 普通集合默认分数为1.0 */
                return 1;
            } else {
                return 0;
            }
        } else if (op->encoding == OBJ_ENCODING_HT) {
            dict *ht = op->subject->ptr;
            zuiSdsFromValue(val);
            if (dictFind(ht,val->ele) != NULL) {
                *score = 1.0;
                return 1;
            } else {
                return 0;
            }
        } else {
            serverPanic("Unknown set encoding");
        }
    } else if (op->type == OBJ_ZSET) {
        zuiSdsFromValue(val);

        if (op->encoding == OBJ_ENCODING_LISTPACK) {
            if (zzlFind(op->subject->ptr,val->ele,score) != NULL) {
                /* Score is already set by zzlFind. */
                /* score 在 zzlFind 函数中已经被设置过 */
                return 1;
            } else {
                return 0;
            }
        } else if (op->encoding == OBJ_ENCODING_SKIPLIST) {
            zset *zs = op->subject->ptr;
            dictEntry *de;
            if ((de = dictFind(zs->dict,val->ele)) != NULL) {
                *score = *(double*)dictGetVal(de);
                return 1;
            } else {
                return 0;
            }
        } else {
            serverPanic("Unknown sorted set encoding");
        }
    } else {
        serverPanic("Unsupported type");
    }
}

/* 比较两个集合/有序集合迭代器所在对象的元素总数 */
int zuiCompareByCardinality(const void *s1, const void *s2) {
    unsigned long first = zuiLength((zsetopsrc*)s1);
    unsigned long second = zuiLength((zsetopsrc*)s2);
    if (first > second) return 1;
    if (first < second) return -1;
    return 0;
}

/* 反向比较两个集合/有序集合迭代器所在对象的元素总数，即返回值和原来的比较会相反 */
static int zuiCompareByRevCardinality(const void *s1, const void *s2) {
    return zuiCompareByCardinality(s1, s2) * -1;
}

#define REDIS_AGGR_SUM 1
#define REDIS_AGGR_MIN 2
#define REDIS_AGGR_MAX 3
#define zunionInterDictValue(_e) (dictGetVal(_e) == NULL ? 1.0 : *(double*)dictGetVal(_e))

/* zset 求交集或并集时，根据 Aggregate 参数(SUM/MIN/MAX) 选择不同的分数汇总方式 */
inline static void zunionInterAggregate(double *target, double val, int aggregate) {
    if (aggregate == REDIS_AGGR_SUM) {
        *target = *target + val;
        /* The result of adding two doubles is NaN when one variable
         * is +inf and the other is -inf. When these numbers are added,
         * we maintain the convention of the result being 0.0. */

         /* 当一个变量是 +inf，另一个是 -inf 时，相加的结果是 NaN。
          * 当结果是 NaN 时，我们保持结果为 0.0 。*/
        if (isnan(*target)) *target = 0.0;
    } else if (aggregate == REDIS_AGGR_MIN) {
        *target = val < *target ? val : *target;
    } else if (aggregate == REDIS_AGGR_MAX) {
        *target = val > *target ? val : *target;
    } else {
        /* safety net */
        serverPanic("Unknown ZUNION/INTER aggregate type");
    }
}

/* 从哈希表中获取最大元素长度 */
static size_t zsetDictGetMaxElementLength(dict *d, size_t *totallen) {
    dictIterator *di;
    dictEntry *de;
    size_t maxelelen = 0;

    di = dictGetIterator(d); /* 初始化哈希表迭代器 */

    /* 遍历整个哈希表找最大元素长度 */
    while((de = dictNext(di)) != NULL) {
        sds ele = dictGetKey(de);
        if (sdslen(ele) > maxelelen) maxelelen = sdslen(ele);
        if (totallen)
            (*totallen) += sdslen(ele);
    }

    dictReleaseIterator(di); /* 释放哈希表迭代器 */

    return maxelelen;
}

/* zset 求差集算法1 */
static void zdiffAlgorithm1(zsetopsrc *src, long setnum, zset *dstzset, size_t *maxelelen, size_t *totelelen) {
    /* DIFF Algorithm 1:
     *
     * We perform the diff by iterating all the elements of the first set,
     * and only adding it to the target set if the element does not exist
     * into all the other sets.
     *
     * This way we perform at max N*M operations, where N is the size of
     * the first set, and M the number of sets.
     *
     * There is also a O(K*log(K)) cost for adding the resulting elements
     * to the target set, where K is the final size of the target set.
     *
     * The final complexity of this algorithm is O(N*M + K*log(K)). */

     /* DIFF 算法1：
     *
     * 我们通过遍历第一个集合的所有成员来执行差运算。
     * 只有当该成员不存在于所有其他集合中时，才将其添加到目标集合中。
     *
     * 这样我们最多执行 N*M 的操作，其中 N 是第一个集合的大小，M 是目标集合的数量。
     * 第一个集合的大小，和M个集合的数量。
     *
     * 该算法用于将得到的元素添加到目标集合中的时间复杂度是 O (K*log(K)) ，
     * 其中 K 是目标集的最终大小。
     *
     * 这个算法的最终时间复杂度是O(N*M + K*log(K))。*/
    int j;
    zsetopval zval;
    zskiplistNode *znode;
    sds tmp;

    /* With algorithm 1 it is better to order the sets to subtract
     * by decreasing size, so that we are more likely to find
     * duplicated elements ASAP. */

    /* 在算法1中，将要减去的集合按降序排序，这样我们就更有可能尽快找到重复的成员*/
    qsort(src+1,setnum-1,sizeof(zsetopsrc),zuiCompareByRevCardinality);

    memset(&zval, 0, sizeof(zval));
    zuiInitIterator(&src[0]);
    while (zuiNext(&src[0],&zval)) {
        double value;
        int exists = 0;

        for (j = 1; j < setnum; j++) {
            /* It is not safe to access the zset we are
             * iterating, so explicitly check for equal object.
             * This check isn't really needed anymore since we already
             * check for a duplicate set in the zsetChooseDiffAlgorithm
             * function, but we're leaving it for future-proofing. */

            /* 访问我们正在迭代的 zset 是不安全的，所以明确地检查集合对象是否相等。
             * 由于我们已经在 zsetChooseDiffAlgorithm 中检查了重复的集合，
             * 所以这个检查实际上不再需要检查是否有重复的集合，
             * 但我们还是把它留下来以备将来之用。*/
            if (src[j].subject == src[0].subject ||
                zuiFind(&src[j],&zval,&value)) {
                exists = 1;
                break;
            }
        }

        /* 如果是不出现重复的元素，则可以加入到结果集合 */
        if (!exists) {
            tmp = zuiNewSdsFromValue(&zval);
            znode = zslInsert(dstzset->zsl,zval.score,tmp);
            dictAdd(dstzset->dict,tmp,&znode->score);
            if (sdslen(tmp) > *maxelelen) *maxelelen = sdslen(tmp);
            (*totelelen) += sdslen(tmp);
        }
    }
    zuiClearIterator(&src[0]);
}

/* zset 求差集算法2 */
static void zdiffAlgorithm2(zsetopsrc *src, long setnum, zset *dstzset, size_t *maxelelen, size_t *totelelen) {
    /* DIFF Algorithm 2:
     *
     * Add all the elements of the first set to the auxiliary set.
     * Then remove all the elements of all the next sets from it.
     *

     * This is O(L + (N-K)log(N)) where L is the sum of all the elements in every
     * set, N is the size of the first set, and K is the size of the result set.
     *
     * Note that from the (L-N) dict searches, (N-K) got to the zsetRemoveFromSkiplist
     * which costs log(N)
     *
     * There is also a O(K) cost at the end for finding the largest element
     * size, but this doesn't change the algorithm complexity since K < L, and
     * O(2L) is the same as O(L). */

    /* DIFF 算法2：
     
     * 将第一个集合的所有成员加入到辅助集合中。
     * 然后从其中删除其他集合的所有成员。
     *
     * 这个算法时间是 O(L + (N-K)log(N))，其中 L 是每个集合中所有成员的总数，
     * N 是第一个集合的大小，K 是结果集的大小。
     *
     * 注意，(L-N) 是在哈希表中搜索次数，(N-K) 是调用了(N-K)次 zsetRemoveFromSkiplist，
     * 而 zsetRemoveFromSkiplist 需要花费log(N)
     *
     * 最后还有一个O(K)的成本，用于寻找最大的成员，
     * 但是这并没有改变算法的复杂性，因为 K < L，而且 O(2L) 和 O(L) 是一样的。 */
    int j;
    int cardinality = 0;
    zsetopval zval;
    zskiplistNode *znode;
    sds tmp;

    for (j = 0; j < setnum; j++) {
        if (zuiLength(&src[j]) == 0) continue;

        memset(&zval, 0, sizeof(zval));
        zuiInitIterator(&src[j]);
        while (zuiNext(&src[j],&zval)) {

            /* 将第一个集合的所有成员加入到辅助集合中 */
            if (j == 0) {
                tmp = zuiNewSdsFromValue(&zval);
                znode = zslInsert(dstzset->zsl,zval.score,tmp);
                dictAdd(dstzset->dict,tmp,&znode->score);
                cardinality++;

            /* 非第一个集合，尝试在结果集合中删除自身拥有的成员 */
            } else {
                tmp = zuiSdsFromValue(&zval);
                if (zsetRemoveFromSkiplist(dstzset, tmp)) {
                    cardinality--;
                }
            }

            /* Exit if result set is empty as any additional removal
                * of elements will have no effect. */
            /* 如果结果集为空，则提前退出 */
            if (cardinality == 0) break;
        }
        zuiClearIterator(&src[j]);

        if (cardinality == 0) break;
    }

    /* Resize dict if needed after removing multiple elements */
    /* 在移除元素后，尝试给哈希表进行缩容 */
    if (htNeedsResize(dstzset->dict)) dictResize(dstzset->dict);

    /* Using this algorithm, we can't calculate the max element as we go,
     * we have to iterate through all elements to find the max one after. */

    /* 使用这种算法，我们不能在上面的遍历中一边遍历一边计算最大的元素，
     * 我们必须遍历所有的元素，然后找到最大的一个。*/
    *maxelelen = zsetDictGetMaxElementLength(dstzset->dict, totelelen);
}

/* 选择求差集算法 */
static int zsetChooseDiffAlgorithm(zsetopsrc *src, long setnum) {
    int j;

    /* Select what DIFF algorithm to use.
     *
     * Algorithm 1 is O(N*M + K*log(K)) where N is the size of the
     * first set, M the total number of sets, and K is the size of the
     * result set.
     *
     * Algorithm 2 is O(L + (N-K)log(N)) where L is the total number of elements
     * in all the sets, N is the size of the first set, and K is the size of the
     * result set.
     *
     * We compute what is the best bet with the current input here. */

    /* 选择要使用的DIFF算法。
     *
     * 算法1是 O(N*M + K*log(K))，其中 N 是第一个集合的大小，
     * M 是集合的总数，K 是结果集的大小。
     *
     * 算法2是 O(L + (N-K)log(N)) 其中 L 是所有元素的总数
     * N 是第一个集合的大小，而 K 是结果集合的大小。
     *
     * 我们在这里计算什么是当前输入情况下的最佳选择 */
    long long algo_one_work = 0;
    long long algo_two_work = 0;

    for (j = 0; j < setnum; j++) {
        /* If any other set is equal to the first set, there is nothing to be
         * done, since we would remove all elements anyway. */

        /* 如果任何其他集合等于第一个集合，那么就没有什么可做的了，
         * 因为无论如何我们都会删除所有的元素 */
        if (j > 0 && src[0].subject == src[j].subject) {
            return 0;
        }

        algo_one_work += zuiLength(&src[0]);
        algo_two_work += zuiLength(&src[j]);
    }

    /* Algorithm 1 has better constant times and performs less operations
     * if there are elements in common. Give it some advantage. */

    /* 算法1有更好的常数时间，如果有共同的元素，则执行的操作更少，
     * 所以我们给它增大被选中的概率。 */
    algo_one_work /= 2;
    return (algo_one_work <= algo_two_work) ? 1 : 2;
}

static void zdiff(zsetopsrc *src, long setnum, zset *dstzset, size_t *maxelelen, size_t *totelelen) {
    /* Skip everything if the smallest input is empty. */
    /* 如果第一个输入的集合为空，则不会执行以下代码 */
    if (zuiLength(&src[0]) > 0) {
        int diff_algo = zsetChooseDiffAlgorithm(src, setnum);
        if (diff_algo == 1) {
            zdiffAlgorithm1(src, setnum, dstzset, maxelelen, totelelen);
        } else if (diff_algo == 2) {
            zdiffAlgorithm2(src, setnum, dstzset, maxelelen, totelelen);
        } else if (diff_algo != 0) {
            serverPanic("Unknown algorithm");
        }
    }
}

dictType setAccumulatorDictType = {
    dictSdsHash,               /* hash function */
    NULL,                      /* key dup */
    NULL,                      /* val dup */
    dictSdsKeyCompare,         /* key compare */
    NULL,                      /* key destructor */
    NULL,                      /* val destructor */
    NULL                       /* allow to expand */
};

/* The zunionInterDiffGenericCommand() function is called in order to implement the
 * following commands: ZUNION, ZINTER, ZDIFF, ZUNIONSTORE, ZINTERSTORE, ZDIFFSTORE,
 * ZINTERCARD.
 *
 * 'numkeysIndex' parameter position of key number. for ZUNION/ZINTER/ZDIFF command,
 * this value is 1, for ZUNIONSTORE/ZINTERSTORE/ZDIFFSTORE command, this value is 2.
 *
 * 'op' SET_OP_INTER, SET_OP_UNION or SET_OP_DIFF.
 *
 * 'cardinality_only' is currently only applicable when 'op' is SET_OP_INTER.
 * Work for SINTERCARD, only return the cardinality with minimum processing and memory overheads.
 */

/* 调用 zunionInterDiffGenericCommand() 函数，以实现以下命令：
 * zunion、zinter、zdiff、zunionstore、zinterstore、zdiffstore、zintercard
 *
 * 'numkeysIndex' 为 numkeys 在输入参数中的位置。对于 ZUNION/ZINTER/ZDIFF 命令这个值是1，
 * 对于 ZINTER/ZDIFF 命令这个值是1，对于 ZUNIONSTORE/ZINTERSTORE/ZDIFFSTORE 命令这个值是2。
 *
 * 'op'：SET_OP_INTER, SET_OP_UNION 或 SET_OP_DIFF，该参数指定了要执行的操作（交/并/差）。
 *
 * 'cardinality_only' 目前只适用于 'op' 为 SET_OP_INTER 时且使用 SINTERCARD 命令，
 * 只返回 cardinality（结果集的元素总数） 并且处理和内存开销最小。
 */
void zunionInterDiffGenericCommand(client *c, robj *dstkey, int numkeysIndex, int op,
                                   int cardinality_only) {
    int i, j;
    long setnum;
    int aggregate = REDIS_AGGR_SUM;
    zsetopsrc *src;
    zsetopval zval;
    sds tmp;
    size_t maxelelen = 0, totelelen = 0;
    robj *dstobj;
    zset *dstzset;
    zskiplistNode *znode;
    int withscores = 0;
    unsigned long cardinality = 0;
    long limit = 0; /* Stop searching after reaching the limit. 0 means unlimited. */

    /* expect setnum input keys to be given */
    /* 获取输入参数中的 numkeys */
    if ((getLongFromObjectOrReply(c, c->argv[numkeysIndex], &setnum, NULL) != C_OK))
        return;

    if (setnum < 1) {
        addReplyErrorFormat(c,
            "at least 1 input key is needed for '%s' command", c->cmd->fullname);
        return;
    }

    /* test if the expected number of keys would overflow */
    /* 测试预期数量的 key 是否会溢出 */
    if (setnum > (c->argc-(numkeysIndex+1))) {
        addReplyErrorObject(c,shared.syntaxerr);
        return;
    }

    /* read keys to be used for input */
    /* 获取输入的 keys，将它们对应的对象保存到 src 数组中 */
    src = zcalloc(sizeof(zsetopsrc) * setnum);
    for (i = 0, j = numkeysIndex+1; i < setnum; i++, j++) {
        robj *obj = lookupKeyRead(c->db, c->argv[j]);
        if (obj != NULL) {
            if (obj->type != OBJ_ZSET && obj->type != OBJ_SET) {
                zfree(src);
                addReplyErrorObject(c,shared.wrongtypeerr);
                return;
            }

            src[i].subject = obj;
            src[i].type = obj->type;
            src[i].encoding = obj->encoding;
        } else {
            src[i].subject = NULL;
        }

        /* Default all weights to 1. */
        /* 默认权重为1.0 */
        src[i].weight = 1.0;
    }

    /* parse optional extra arguments */
    /* 解析输入的可选参数 */
    if (j < c->argc) {
        int remaining = c->argc - j; /* 剩下的输入参数数量 */

        while (remaining) {
            if (op != SET_OP_DIFF && !cardinality_only &&
                remaining >= (setnum + 1) &&
                !strcasecmp(c->argv[j]->ptr,"weights"))
            {
                j++; remaining--;
                for (i = 0; i < setnum; i++, j++, remaining--) {
                    if (getDoubleFromObjectOrReply(c,c->argv[j],&src[i].weight,
                            "weight value is not a float") != C_OK)
                    {
                        zfree(src);
                        return;
                    }
                }
            } else if (op != SET_OP_DIFF && !cardinality_only &&
                       remaining >= 2 &&
                       !strcasecmp(c->argv[j]->ptr,"aggregate"))
            {
                j++; remaining--;
                if (!strcasecmp(c->argv[j]->ptr,"sum")) {
                    aggregate = REDIS_AGGR_SUM;
                } else if (!strcasecmp(c->argv[j]->ptr,"min")) {
                    aggregate = REDIS_AGGR_MIN;
                } else if (!strcasecmp(c->argv[j]->ptr,"max")) {
                    aggregate = REDIS_AGGR_MAX;
                } else {
                    zfree(src);
                    addReplyErrorObject(c,shared.syntaxerr);
                    return;
                }
                j++; remaining--;
            } else if (remaining >= 1 &&
                       !dstkey && !cardinality_only &&
                       !strcasecmp(c->argv[j]->ptr,"withscores"))
            {
                j++; remaining--;
                withscores = 1;
            } else if (cardinality_only && remaining >= 2 &&
                       !strcasecmp(c->argv[j]->ptr, "limit"))
            {
                j++; remaining--;
                if (getPositiveLongFromObjectOrReply(c, c->argv[j], &limit,
                                                     "LIMIT can't be negative") != C_OK)
                {
                    zfree(src);
                    return;
                }
                j++; remaining--;
            } else {
                zfree(src);
                addReplyErrorObject(c,shared.syntaxerr);
                return;
            }
        }
    }

    if (op != SET_OP_DIFF) {
        /* sort sets from the smallest to largest, this will improve our
         * algorithm's performance */
        /* 如果不是求差集，则将集合从小到大排序，这将提高我们算法的性能 */
        qsort(src,setnum,sizeof(zsetopsrc),zuiCompareByCardinality);
    }

    dstobj = createZsetObject();
    dstzset = dstobj->ptr;
    memset(&zval, 0, sizeof(zval));

    /* 交集运算 */
    if (op == SET_OP_INTER) {
        /* Skip everything if the smallest input is empty. */
        /* 如果第一个集合是空的，则直接跳过求交集的操作 */
        if (zuiLength(&src[0]) > 0) {
            /* Precondition: as src[0] is non-empty and the inputs are ordered
             * by size, all src[i > 0] are non-empty too. */

            /* 前提条件：由于 src[0] 是非空的，而且输入是从小到大排序的，所以所有src[i > 0]也都是非空的 */
            zuiInitIterator(&src[0]);
            while (zuiNext(&src[0],&zval)) {
                double score, value;

                score = src[0].weight * zval.score;
                if (isnan(score)) score = 0;

                for (j = 1; j < setnum; j++) {
                    /* It is not safe to access the zset we are
                     * iterating, so explicitly check for equal object. */

                    /* 访问我们正在迭代的 zset 是不安全的，所以要明确检查是否有相同的对象 */
                    if (src[j].subject == src[0].subject) {
                        value = zval.score*src[j].weight; /* 计算当前成员的 value （score * weight 分数*权重）*/
                        zunionInterAggregate(&score,value,aggregate); /* 通过指定的分数汇总方式计算当前成员在结果集中的 score */
                    } else if (zuiFind(&src[j],&zval,&value)) {
                        value *= src[j].weight;
                        zunionInterAggregate(&score,value,aggregate);
                    } else {
                        break;
                    }
                }

                /* Only continue when present in every input. */
                /* 只有在遍历过每个输入的 key 时才继续（能够遍历所有 key 说明该元素满足交集条件） */
                /* zintercard 命令的情况 */
                if (j == setnum && cardinality_only) {
                    cardinality++;

                    /* We stop the searching after reaching the limit. */
                    /* 我们在达到最大扫描限制（limit）后停止搜索。*/
                    if (limit && cardinality >= (unsigned long)limit) {
                        /* Cleanup before we break the zuiNext loop. */
                        /* 在我们中断 while 循环之前清理 zval */
                        zuiDiscardDirtyValue(&zval);
                        break;
                    }

                /* 如果不是 zintercard 命令，我们需要向有序临时集合中添加该成员 */
                } else if (j == setnum) {
                    tmp = zuiNewSdsFromValue(&zval);
                    znode = zslInsert(dstzset->zsl,score,tmp);
                    dictAdd(dstzset->dict,tmp,&znode->score);
                    totelelen += sdslen(tmp);
                    if (sdslen(tmp) > maxelelen) maxelelen = sdslen(tmp);
                }
            }
            zuiClearIterator(&src[0]);
        }

    /* 并集运算 */
    } else if (op == SET_OP_UNION) {
        dict *accumulator = dictCreate(&setAccumulatorDictType); /* 临时字典 */
        dictIterator *di;
        dictEntry *de, *existing;
        double score;

        if (setnum) {
            /* Our union is at least as large as the largest set.
             * Resize the dictionary ASAP to avoid useless rehashing. */
            /* 我们的临时字典至少和最大的集合一样大。 尽快调整字典大小以避免无用的 rehashing（重新哈希）。 */
            dictExpand(accumulator,zuiLength(&src[setnum-1]));
        }

        /* Step 1: Create a dictionary of elements -> aggregated-scores
         * by iterating one sorted set after the other. */

        /* 第 1 步：创建元素字典 -> 聚合分数，
         * 通过一个接一个地迭代一个有序集合来完成。 */
        for (i = 0; i < setnum; i++) {
            if (zuiLength(&src[i]) == 0) continue;

            zuiInitIterator(&src[i]);
            while (zuiNext(&src[i],&zval)) {
                /* Initialize value */
                /* 初始化分数 */
                score = src[i].weight * zval.score;
                if (isnan(score)) score = 0;

                /* Search for this element in the accumulating dictionary. */
                /* 在字典中搜索这个元素 */
                de = dictAddRaw(accumulator,zuiSdsFromValue(&zval),&existing);
                /* If we don't have it, we need to create a new entry. */
                /* 如果字典中不存在该元素，则将它添加到字典中 */
                if (!existing) {
                    tmp = zuiNewSdsFromValue(&zval);
                    /* Remember the longest single element encountered,
                     * to understand if it's possible to convert to listpack
                     * at the end. */

                    /* 记住遇到的最长的单个元素，在最后检查是否可以转换为 listpack。 */
                     totelelen += sdslen(tmp);
                     if (sdslen(tmp) > maxelelen) maxelelen = sdslen(tmp);
                    /* Update the element with its initial score. */
                    /* 向字典添加该元素与它的初始分数 */
                    dictSetKey(accumulator, de, tmp);
                    dictSetDoubleVal(de,score);
                } else {
                    /* Update the score with the score of the new instance
                     * of the element found in the current sorted set.
                     *
                     * Here we access directly the dictEntry double
                     * value inside the union as it is a big speedup
                     * compared to using the getDouble/setDouble API. */

                    /* 使用在当前有序集合中找到的元素的分数来更新结果集合中该元素的分数。
                     *
                     * 在这里，我们直接访问字典内部的 dictEntry 中的 double值，
                     * 因为与使用 getDouble/setDouble API 相比，它的速度更快。 */
                    zunionInterAggregate(&existing->v.d,score,aggregate);
                }
            }
            zuiClearIterator(&src[i]);
        }

        /* Step 2: convert the dictionary into the final sorted set. */
        /* 第 2 步：将字典转换为最终的排序集。 */
        di = dictGetIterator(accumulator);

        /* We now are aware of the final size of the resulting sorted set,
         * let's resize the dictionary embedded inside the sorted set to the
         * right size, in order to save rehashing time. */

        /* 我们现在知道结果集的最终大小，
         * 让我们将嵌入结果集中的字典调整为正确的大小，以节省重新哈希花费的时间。 */
        dictExpand(dstzset->dict,dictSize(accumulator));

        /* 遍历临时字典，将临时字典中的成员加入到结果有序集合中 */
        while((de = dictNext(di)) != NULL) {
            sds ele = dictGetKey(de);
            score = dictGetDoubleVal(de);
            znode = zslInsert(dstzset->zsl,score,ele);
            dictAdd(dstzset->dict,ele,&znode->score);
        }
        dictReleaseIterator(di);
        dictRelease(accumulator);
    
    /* 差集运算 */
    } else if (op == SET_OP_DIFF) {
        zdiff(src, setnum, dstzset, &maxelelen, &totelelen);
    } else {
        serverPanic("Unknown operator");
    }

    /* 根据不同类型的命令发送不同的回复给客户端，
     * dstkey 存在为 zinterstore/zunionstore/zdiffstore 命令 */
    if (dstkey) {

        /* 结果集元素数量不为0 */
        if (dstzset->zsl->length) {
            zsetConvertToListpackIfNeeded(dstobj, maxelelen, totelelen); /* 尝试转换成 listpack */
            setKey(c, c->db, dstkey, dstobj, 0); /* 在数据库中设置 dstkey 对应结果集合 */
            addReplyLongLong(c, zsetLength(dstobj)); /* 返回结果集的元素总数 */

            /* 发送事件通知 */
            notifyKeyspaceEvent(NOTIFY_ZSET,
                                (op == SET_OP_UNION) ? "zunionstore" :
                                    (op == SET_OP_INTER ? "zinterstore" : "zdiffstore"),
                                dstkey, c->db->id);
            server.dirty++; /* 脏数据 + 1 */

        /* 结果集元素数量为0 */
        } else {
            addReply(c, shared.czero); 

            /* 将 dstkey 从数据库中删除 */
            if (dbDelete(c->db, dstkey)) {
                signalModifiedKey(c, c->db, dstkey); /* 发送 key 被修改信号 */
                notifyKeyspaceEvent(NOTIFY_GENERIC, "del", dstkey, c->db->id); 
                server.dirty++;
            }
        }
    
    /* zintercard 命令 */
    } else if (cardinality_only) {
        addReplyLongLong(c, cardinality); /* 回复结果集的元素数量 */

    /* 其他命令 */
    } else {
        unsigned long length = dstzset->zsl->length;
        zskiplist *zsl = dstzset->zsl;
        zskiplistNode *zn = zsl->header->level[0].forward;
        /* In case of WITHSCORES, respond with a single array in RESP2, and
         * nested arrays in RESP3. We can't use a map response type since the
         * client library needs to know to respect the order. */

        /* 在有 WITHSCORES 的情况下，在 RESP2 中使用单个数组响应，在 RESP3 中使用嵌套数组。
         * 我们不能使用 map 作为响应类型，因为客户端库需要知道顺序 */
        if (withscores && c->resp == 2)
            addReplyArrayLen(c, length*2);
        else
            addReplyArrayLen(c, length);


        /* 遍历跳表，按分数从小到大回复所有节点给客户端 */
        while (zn != NULL) {
            if (withscores && c->resp > 2) addReplyArrayLen(c,2);
            addReplyBulkCBuffer(c,zn->ele,sdslen(zn->ele));
            if (withscores) addReplyDouble(c,zn->score);
            zn = zn->level[0].forward;
        }
    }
    decrRefCount(dstobj);
    zfree(src);
}

/* ZUNIONSTORE destination numkeys key [key ...] [WEIGHTS weight] [AGGREGATE SUM|MIN|MAX] */
/* 处理 zunionstore 命令的函数 */
void zunionstoreCommand(client *c) {
    zunionInterDiffGenericCommand(c, c->argv[1], 2, SET_OP_UNION, 0);
}

/* ZINTERSTORE destination numkeys key [key ...] [WEIGHTS weight] [AGGREGATE SUM|MIN|MAX] */
/* 处理 zinterstore 命令的函数 */
void zinterstoreCommand(client *c) {
    zunionInterDiffGenericCommand(c, c->argv[1], 2, SET_OP_INTER, 0);
}

/* ZDIFFSTORE destination numkeys key [key ...] */
/* 处理 zdiffstore 命令的函数 */
void zdiffstoreCommand(client *c) {
    zunionInterDiffGenericCommand(c, c->argv[1], 2, SET_OP_DIFF, 0);
}

/* ZUNION numkeys key [key ...] [WEIGHTS weight] [AGGREGATE SUM|MIN|MAX] [WITHSCORES] */
/* 处理 zunion 命令的函数 */
void zunionCommand(client *c) {
    zunionInterDiffGenericCommand(c, NULL, 1, SET_OP_UNION, 0);
}

/* ZINTER numkeys key [key ...] [WEIGHTS weight] [AGGREGATE SUM|MIN|MAX] [WITHSCORES] */
/* 处理 zinter 命令的函数 */
void zinterCommand(client *c) {
    zunionInterDiffGenericCommand(c, NULL, 1, SET_OP_INTER, 0);
}

/* ZINTERCARD numkeys key [key ...] [LIMIT limit] */
/* 处理 zintercard 命令的函数 */
void zinterCardCommand(client *c) {
    zunionInterDiffGenericCommand(c, NULL, 1, SET_OP_INTER, 1);
}

/* ZDIFF numkeys key [key ...] [WITHSCORES] */
/* 处理 zdiff 命令的函数 */
void zdiffCommand(client *c) {
    zunionInterDiffGenericCommand(c, NULL, 1, SET_OP_DIFF, 0);
}

typedef enum {
    ZRANGE_DIRECTION_AUTO = 0,
    ZRANGE_DIRECTION_FORWARD,
    ZRANGE_DIRECTION_REVERSE
} zrange_direction;

typedef enum {
    ZRANGE_CONSUMER_TYPE_CLIENT = 0,
    ZRANGE_CONSUMER_TYPE_INTERNAL
} zrange_consumer_type;

typedef struct zrange_result_handler zrange_result_handler;

typedef void (*zrangeResultBeginFunction)(zrange_result_handler *c, long length);
typedef void (*zrangeResultFinalizeFunction)(
    zrange_result_handler *c, size_t result_count);
typedef void (*zrangeResultEmitCBufferFunction)(
    zrange_result_handler *c, const void *p, size_t len, double score);
typedef void (*zrangeResultEmitLongLongFunction)(
    zrange_result_handler *c, long long ll, double score);

void zrangeGenericCommand (zrange_result_handler *handler, int argc_start, int store,
                           zrange_type rangetype, zrange_direction direction);

/* Interface struct for ZRANGE/ZRANGESTORE generic implementation.
 * There is one implementation of this interface that sends a RESP reply to clients.
 * and one implementation that stores the range result into a zset object. */

/* 用于 ZRANGE/ZRANGESTORE 通用实现的接口结构。
 * 这个接口有一个实现，向客户发送 RESP 回复，还有一个实现是将范围存储到 zset 对象中 */
struct zrange_result_handler {
    zrange_consumer_type                 type;
    client                              *client;
    robj                                *dstkey;
    robj                                *dstobj;
    void                                *userdata;
    int                                  withscores;
    int                                  should_emit_array_length;
    zrangeResultBeginFunction            beginResultEmission;
    zrangeResultFinalizeFunction         finalizeResultEmission;
    zrangeResultEmitCBufferFunction      emitResultFromCBuffer;
    zrangeResultEmitLongLongFunction     emitResultFromLongLong;
};

/* Result handler methods for responding the ZRANGE to clients.
 * length can be used to provide the result length in advance (avoids deferred reply overhead).
 * length can be set to -1 if the result length is not know in advance.
 */

/* 用于将 ZRANGE 回复给客户端的结果处理方法。
 * length 可以用来提前提供结果的长度（避免了延迟回复的开销）。
 * 如果事先不知道结果的长度，length 可以被设置为-1。
 */
static void zrangeResultBeginClient(zrange_result_handler *handler, long length) {
    if (length > 0) {
        /* In case of WITHSCORES, respond with a single array in RESP2, and
        * nested arrays in RESP3. We can't use a map response type since the
        * client library needs to know to respect the order. */

        /* 在 WITHSCORES 的情况下，在 RESP2 中用单个数组响应，在 RESP3 中用嵌套数组。
         * 我们不能使用 map 响应类型，因为客户端库需要知道顺序 */
        if (handler->withscores && (handler->client->resp == 2)) {
            length *= 2; /* withscores 不仅返回元素还要返回分数，所以需要将 length * 2 */
        }

        /* 添加回复数组的长度 */
        addReplyArrayLen(handler->client, length);
        handler->userdata = NULL;
        return;
    }
    handler->userdata = addReplyDeferredLen(handler->client); /* 如果不知道结果的长度，则使用延迟回复 */
}

/* 将字符串型的元素添加到回复数组中 */
static void zrangeResultEmitCBufferToClient(zrange_result_handler *handler,
    const void *value, size_t value_length_in_bytes, double score)
{
    if (handler->should_emit_array_length) {
        addReplyArrayLen(handler->client, 2);
    }

    addReplyBulkCBuffer(handler->client, value, value_length_in_bytes);

    if (handler->withscores) {
        addReplyDouble(handler->client, score);
    }
}

/* 将 long long 型的元素添加到回复数组中 */
static void zrangeResultEmitLongLongToClient(zrange_result_handler *handler,
    long long value, double score)
{
    if (handler->should_emit_array_length) {
        addReplyArrayLen(handler->client, 2);
    }

    addReplyBulkLongLong(handler->client, value);

    if (handler->withscores) {
        addReplyDouble(handler->client, score);
    }
}

static void zrangeResultFinalizeClient(zrange_result_handler *handler,
    size_t result_count)
{
    /* If the reply size was know at start there's nothing left to do */
    /* 如果在开始时就知道回复的大小，就没有什么可做的了 */
    if (!handler->userdata)
        return;
    /* In case of WITHSCORES, respond with a single array in RESP2, and
     * nested arrays in RESP3. We can't use a map response type since the
     * client library needs to know to respect the order. */

    /* 在 WITHSCORES 的情况下，在 RESP2 中用单个数组响应，在 RESP3 中用嵌套数组。
     * 我们不能使用 map 响应类型，因为客户端库需要知道顺序 */
    if (handler->withscores && (handler->client->resp == 2)) {
        result_count *= 2;
    }

    /* 设置延迟回复数组的长度 */
    setDeferredArrayLen(handler->client, handler->userdata, result_count);
}

/* Result handler methods for storing the ZRANGESTORE to a zset. */
/* 用于将 ZRANGESTORE 命令的结果开始存储到目的集合前，将目的集合创建 */
static void zrangeResultBeginStore(zrange_result_handler *handler, long length)
{
    if (length > (long)server.zset_max_listpack_entries)
        handler->dstobj = createZsetObject();
    else
        handler->dstobj = createZsetListpackObject();
}

/* 用于将 ZRANGESTORE 命令结果的字符串型的元素加入到目的集合中 */
static void zrangeResultEmitCBufferForStore(zrange_result_handler *handler,
    const void *value, size_t value_length_in_bytes, double score)
{
    double newscore;
    int retflags = 0;
    sds ele = sdsnewlen(value, value_length_in_bytes);
    int retval = zsetAdd(handler->dstobj, score, ele, ZADD_IN_NONE, &retflags, &newscore);
    sdsfree(ele);
    serverAssert(retval);
}

/* 用于将 ZRANGESTORE 命令结果的 long long 型的元素加入到目的集合中 */
static void zrangeResultEmitLongLongForStore(zrange_result_handler *handler,
    long long value, double score)
{
    double newscore;
    int retflags = 0;
    sds ele = sdsfromlonglong(value);
    int retval = zsetAdd(handler->dstobj, score, ele, ZADD_IN_NONE, &retflags, &newscore);
    sdsfree(ele);
    serverAssert(retval);
}

/* 用于 ZRANGESTORE 命令最后对于目的集合的处理 */
static void zrangeResultFinalizeStore(zrange_result_handler *handler, size_t result_count)
{
    /* 如果目的集合的元素不为0，则在数据库中设置 dstkey 对应的对象为目的集合 */
    if (result_count) {
        setKey(handler->client, handler->client->db, handler->dstkey, handler->dstobj, 0);
        addReplyLongLong(handler->client, result_count);
        notifyKeyspaceEvent(NOTIFY_ZSET, "zrangestore", handler->dstkey, handler->client->db->id);
        server.dirty++;

    /* 如果目的集合没有元素，则在数据库中将 dstkey 删除 */
    } else {
        addReply(handler->client, shared.czero);
        if (dbDelete(handler->client->db, handler->dstkey)) {
            signalModifiedKey(handler->client, handler->client->db, handler->dstkey);
            notifyKeyspaceEvent(NOTIFY_GENERIC, "del", handler->dstkey, handler->client->db->id);
            server.dirty++;
        }
    }
    decrRefCount(handler->dstobj);
}

/* Initialize the consumer interface type with the requested type. */
/* 用请求的类型初始化消费者接口类型 */
static void zrangeResultHandlerInit(zrange_result_handler *handler,
    client *client, zrange_consumer_type type)
{
    memset(handler, 0, sizeof(*handler));

    handler->client = client;

    switch (type) {
    case ZRANGE_CONSUMER_TYPE_CLIENT:
        handler->beginResultEmission = zrangeResultBeginClient;
        handler->finalizeResultEmission = zrangeResultFinalizeClient;
        handler->emitResultFromCBuffer = zrangeResultEmitCBufferToClient;
        handler->emitResultFromLongLong = zrangeResultEmitLongLongToClient;
        break;

    case ZRANGE_CONSUMER_TYPE_INTERNAL:
        handler->beginResultEmission = zrangeResultBeginStore;
        handler->finalizeResultEmission = zrangeResultFinalizeStore;
        handler->emitResultFromCBuffer = zrangeResultEmitCBufferForStore;
        handler->emitResultFromLongLong = zrangeResultEmitLongLongForStore;
        break;
    }
}

/* 打开接口将分数添加到回复内容中的功能的函数 */
static void zrangeResultHandlerScoreEmissionEnable(zrange_result_handler *handler) {
    handler->withscores = 1;
    handler->should_emit_array_length = (handler->client->resp > 2);
}

/* 设置接口 dstkey 的函数 */
static void zrangeResultHandlerDestinationKeySet (zrange_result_handler *handler,
    robj *dstkey)
{
    handler->dstkey = dstkey;
}

/* This command implements ZRANGE, ZREVRANGE. */
/* zrangev, zevrange 命令的通用实现函数 */
void genericZrangebyrankCommand(zrange_result_handler *handler,
    robj *zobj, long start, long end, int withscores, int reverse) {

    client *c = handler->client;
    long llen;
    long rangelen;
    size_t result_cardinality;

    /* Sanitize indexes. */
    /* 令 llen 等于 zobj 的长度，并转换输入为负数的 start 和 end，以及 start 小于0时强制等于0 */
    llen = zsetLength(zobj);
    if (start < 0) start = llen+start;
    if (end < 0) end = llen+end;
    if (start < 0) start = 0;


    /* Invariant: start >= 0, so this test will be true when end < 0.
     * The range is empty when start > end or start >= length. */
    /* 测试永远为空的范围 */
    if (start > end || start >= llen) {
        handler->beginResultEmission(handler, 0);
        handler->finalizeResultEmission(handler, 0);
        return;
    }
    if (end >= llen) end = llen-1;
    rangelen = (end-start)+1;
    result_cardinality = rangelen;

    handler->beginResultEmission(handler, rangelen);
    if (zobj->encoding == OBJ_ENCODING_LISTPACK) {
        unsigned char *zl = zobj->ptr;
        unsigned char *eptr, *sptr;
        unsigned char *vstr;
        unsigned int vlen;
        long long vlong;
        double score = 0.0;

        /* 定位 start 的位置，eptr 指向了范围内的第一个元素
         *（若 reverse 为真则是范围内分数最大的元素，否则是分数最小的） */
        if (reverse)
            eptr = lpSeek(zl,-2-(2*start));
        else
            eptr = lpSeek(zl,2*start);

        serverAssertWithInfo(c,zobj,eptr != NULL);
        sptr = lpNext(zl,eptr); /* sptr 指向 eptr 的下一个位置，也就是 eptr 所指向元素的 score */

        while (rangelen--) {
            serverAssertWithInfo(c,zobj,eptr != NULL && sptr != NULL);
            vstr = lpGetValue(eptr,&vlen,&vlong);

            if (withscores) /* don't bother to extract the score if it's gonna be ignored. */
                score = zzlGetScore(sptr);

            /* 如果 vstr == NULL，说明该成员的保存方式是整数，
             * 把 vlong 加入 handler，否则将 vstr 和 vlen 加入 handler */
            if (vstr == NULL) {
                handler->emitResultFromLongLong(handler, vlong, score);
            } else {
                handler->emitResultFromCBuffer(handler, vstr, vlen, score);
            }

            /* reverse 为真则向前（分数小的）定位，否则向后（分数大的）定位 */
            if (reverse)
                zzlPrev(zl,&eptr,&sptr);
            else
                zzlNext(zl,&eptr,&sptr);
        }

    } else if (zobj->encoding == OBJ_ENCODING_SKIPLIST) {
        zset *zs = zobj->ptr;
        zskiplist *zsl = zs->zsl;
        zskiplistNode *ln;

        /* Check if starting point is trivial, before doing log(N) lookup. */
        /* 判断方向，并在跳表中找到范围的第一个节点，
         *（若 reverse 为真则是范围内分数最大的元素，否则是分数最小的）*/
        if (reverse) {
            ln = zsl->tail;
            if (start > 0)
                ln = zslGetElementByRank(zsl,llen-start);
        } else {
            ln = zsl->header->level[0].forward;
            if (start > 0)
                ln = zslGetElementByRank(zsl,start+1);
        }

        /* 将范围内的成员加入到 handler 中 */
        while(rangelen--) {
            serverAssertWithInfo(c,zobj,ln != NULL);
            sds ele = ln->ele;
            handler->emitResultFromCBuffer(handler, ele, sdslen(ele), ln->score);
            ln = reverse ? ln->backward : ln->level[0].forward;
        }
    } else {
        serverPanic("Unknown sorted set encoding");
    }

    /* 确定并设置结果回复数组的长度 */
    handler->finalizeResultEmission(handler, result_cardinality);
}

/* ZRANGESTORE <dst> <src> <min> <max> [BYSCORE | BYLEX] [REV] [LIMIT offset count] */
/* 处理 zrangestore 命令的函数 */
void zrangestoreCommand (client *c) {
    robj *dstkey = c->argv[1];
    zrange_result_handler handler;
    zrangeResultHandlerInit(&handler, c, ZRANGE_CONSUMER_TYPE_INTERNAL);
    zrangeResultHandlerDestinationKeySet(&handler, dstkey);
    zrangeGenericCommand(&handler, 2, 1, ZRANGE_AUTO, ZRANGE_DIRECTION_AUTO);
}

/* ZRANGE <key> <min> <max> [BYSCORE | BYLEX] [REV] [WITHSCORES] [LIMIT offset count] */
/* 处理 zrange 命令的函数 */
void zrangeCommand(client *c) {
    zrange_result_handler handler;
    zrangeResultHandlerInit(&handler, c, ZRANGE_CONSUMER_TYPE_CLIENT);
    zrangeGenericCommand(&handler, 1, 0, ZRANGE_AUTO, ZRANGE_DIRECTION_AUTO);
}

/* ZREVRANGE <key> <start> <stop> [WITHSCORES] */
/* 处理 zrevrangeCommand 命令的函数 */
void zrevrangeCommand(client *c) {
    zrange_result_handler handler;
    zrangeResultHandlerInit(&handler, c, ZRANGE_CONSUMER_TYPE_CLIENT);
    zrangeGenericCommand(&handler, 1, 0, ZRANGE_RANK, ZRANGE_DIRECTION_REVERSE);
}

/* This command implements ZRANGEBYSCORE, ZREVRANGEBYSCORE. */
/* ZRANGEBYSCORE, ZREVRANGEBYSCORE 命令的通用实现函数 */
void genericZrangebyscoreCommand(zrange_result_handler *handler,
    zrangespec *range, robj *zobj, long offset, long limit, 
    int reverse) {
    unsigned long rangelen = 0;

    handler->beginResultEmission(handler, -1);

    /* For invalid offset, return directly. */
    /* 偏移量无效，直接返回 */
    if (offset > 0 && offset >= (long)zsetLength(zobj)) {
        handler->finalizeResultEmission(handler, 0);
        return;
    }

    if (zobj->encoding == OBJ_ENCODING_LISTPACK) {
        unsigned char *zl = zobj->ptr;
        unsigned char *eptr, *sptr;
        unsigned char *vstr;
        unsigned int vlen;
        long long vlong;

        /* If reversed, get the last node in range as starting point. */
        /* 如果 reverse 为真，用范围内的最后一个节点（分数最大）作为起点 */
        if (reverse) {
            eptr = zzlLastInRange(zl,range);
        } else {
            eptr = zzlFirstInRange(zl,range);
        }

        /* Get score pointer for the first element. */
        /* 获取起点的分数 */
        if (eptr)
            sptr = lpNext(zl,eptr);

        /* If there is an offset, just traverse the number of elements without
         * checking the score because that is done in the next loop. */
        /* 如果输入了偏移量（大于0），只移动节点而不检查分数，
         * 因为在下一个循环中我们才需要去检查它 */
        while (eptr && offset--) {
            if (reverse) {
                zzlPrev(zl,&eptr,&sptr);
            } else {
                zzlNext(zl,&eptr,&sptr);
            }
        }

        while (eptr && limit--) {
            double score = zzlGetScore(sptr);

            /* Abort when the node is no longer in range. */
            /* 如果当前节点的成员分数不在范围内，则放弃操作 */
            if (reverse) {
                if (!zslValueGteMin(score,range)) break;
            } else {
                if (!zslValueLteMax(score,range)) break;
            }

            vstr = lpGetValue(eptr,&vlen,&vlong);
            rangelen++;
            if (vstr == NULL) {
                handler->emitResultFromLongLong(handler, vlong, score);
            } else {
                handler->emitResultFromCBuffer(handler, vstr, vlen, score);
            }

            /* Move to next node */
            /* 移动到下一个节点 */
            if (reverse) {
                zzlPrev(zl,&eptr,&sptr);
            } else {
                zzlNext(zl,&eptr,&sptr);
            }
        }
    } else if (zobj->encoding == OBJ_ENCODING_SKIPLIST) {
        zset *zs = zobj->ptr;
        zskiplist *zsl = zs->zsl;
        zskiplistNode *ln;

        /* If reversed, get the last node in range as starting point. */
        /* 如果 reverse 为真，用范围内的最后一个节点（分数最大）作为起点 */
        if (reverse) {
            ln = zslLastInRange(zsl,range);
        } else {
            ln = zslFirstInRange(zsl,range);
        }

        /* If there is an offset, just traverse the number of elements without
         * checking the score because that is done in the next loop. */

        /* 如果输入了偏移量（大于0），只移动节点而不检查分数，
         * 因为在下一个循环中我们才需要去检查它 */
        while (ln && offset--) {
            if (reverse) {
                ln = ln->backward;
            } else {
                ln = ln->level[0].forward;
            }
        }

        while (ln && limit--) {
            /* Abort when the node is no longer in range. */
            /* 如果当前节点的成员分数不在范围内，则放弃操作 */
            if (reverse) {
                if (!zslValueGteMin(ln->score,range)) break;
            } else {
                if (!zslValueLteMax(ln->score,range)) break;
            }

            rangelen++;
            handler->emitResultFromCBuffer(handler, ln->ele, sdslen(ln->ele), ln->score);

            /* Move to next node */
            /* 移动到下一个节点 */
            if (reverse) {
                ln = ln->backward;
            } else {
                ln = ln->level[0].forward;
            }
        }
    } else {
        serverPanic("Unknown sorted set encoding");
    }

    /* 设置回复数组的长度 */
    handler->finalizeResultEmission(handler, rangelen);
}

/* ZRANGEBYSCORE <key> <min> <max> [WITHSCORES] [LIMIT offset count] */
/* 处理 zrangebyscore 命令的函数 */
void zrangebyscoreCommand(client *c) {
    zrange_result_handler handler;
    zrangeResultHandlerInit(&handler, c, ZRANGE_CONSUMER_TYPE_CLIENT);
    zrangeGenericCommand(&handler, 1, 0, ZRANGE_SCORE, ZRANGE_DIRECTION_FORWARD);
}

/* ZREVRANGEBYSCORE <key> <max> <min> [WITHSCORES] [LIMIT offset count] */
/* 处理 zrevrangebyscore 命令的函数 */
void zrevrangebyscoreCommand(client *c) {
    zrange_result_handler handler;
    zrangeResultHandlerInit(&handler, c, ZRANGE_CONSUMER_TYPE_CLIENT);
    zrangeGenericCommand(&handler, 1, 0, ZRANGE_SCORE, ZRANGE_DIRECTION_REVERSE);
}

/* 处理 zcount 命令的函数 */
void zcountCommand(client *c) {
    robj *key = c->argv[1];
    robj *zobj;
    zrangespec range;
    unsigned long count = 0;

    /* Parse the range arguments */
    /* 解析范围参数 */
    if (zslParseRange(c->argv[2],c->argv[3],&range) != C_OK) {
        addReplyError(c,"min or max is not a float");
        return;
    }

    /* Lookup the sorted set */
    /* 查找 key 对应的有序集合对象 */
    if ((zobj = lookupKeyReadOrReply(c, key, shared.czero)) == NULL ||
        checkType(c, zobj, OBJ_ZSET)) return;

    if (zobj->encoding == OBJ_ENCODING_LISTPACK) {
        unsigned char *zl = zobj->ptr;
        unsigned char *eptr, *sptr;
        double score;

        /* Use the first element in range as the starting point */
        /* 将范围内的第一个（分数最小的）元素作为起点 */
        eptr = zzlFirstInRange(zl,&range);

        /* No "first" element */
        /* 有序集合不存在分数满足范围的第一个成员，则向客户端回复0，并退出函数 */
        if (eptr == NULL) {
            addReply(c, shared.czero);
            return;
        }

        /* First element is in range */
        /* 有序集合内存在分数满足范围的第一个成员 */
        sptr = lpNext(zl,eptr);
        score = zzlGetScore(sptr);
        serverAssertWithInfo(c,zobj,zslValueLteMax(score,&range));

        /* Iterate over elements in range */
        /* 迭代范围内的成员 */
        while (eptr) {
            score = zzlGetScore(sptr);

            /* Abort when the node is no longer in range. */
            /* 如果当前节点的成员分数不在范围内，则放弃操作 */
            if (!zslValueLteMax(score,&range)) {
                break;
            } else {
                count++;
                zzlNext(zl,&eptr,&sptr);
            }
        }
    } else if (zobj->encoding == OBJ_ENCODING_SKIPLIST) {
        zset *zs = zobj->ptr;
        zskiplist *zsl = zs->zsl;
        zskiplistNode *zn;
        unsigned long rank;

        /* Find first element in range */
        /* 获取范围内的第一个元素 */
        zn = zslFirstInRange(zsl, &range);

        /* Use rank of first element, if any, to determine preliminary count */
        /* 使用第一个元素的排名（如果有）来确定初步计数 */
        if (zn != NULL) {
            rank = zslGetRank(zsl, zn->score, zn->ele);
            count = (zsl->length - (rank - 1));

            /* Find last element in range */
            /* 获取范围内的最后一个元素 */
            zn = zslLastInRange(zsl, &range);

            /* Use rank of last element, if any, to determine the actual count */
            /* 使用最后一个元素的排名（如果有）来确定实际计数 */
            if (zn != NULL) {
                rank = zslGetRank(zsl, zn->score, zn->ele);
                count -= (zsl->length - rank); /* 计算范围内的成员数 */
            }
        }
    } else {
        serverPanic("Unknown sorted set encoding");
    }

    /* 回复范围内的成员数给客户端 */
    addReplyLongLong(c, count);
}

/* 处理 zlexcount 命令的函数 */
void zlexcountCommand(client *c) {
    robj *key = c->argv[1];
    robj *zobj;
    zlexrangespec range;
    unsigned long count = 0;

    /* Parse the range arguments */
    /* 解析范围参数 */
    if (zslParseLexRange(c->argv[2],c->argv[3],&range) != C_OK) {
        addReplyError(c,"min or max not valid string range item");
        return;
    }

    /* Lookup the sorted set */
    /* 查找 key 对应的有序集合对象 */
    if ((zobj = lookupKeyReadOrReply(c, key, shared.czero)) == NULL ||
        checkType(c, zobj, OBJ_ZSET))
    {
        zslFreeLexRange(&range);
        return;
    }

    if (zobj->encoding == OBJ_ENCODING_LISTPACK) {
        unsigned char *zl = zobj->ptr;
        unsigned char *eptr, *sptr;

        /* Use the first element in range as the starting point */
        /* 将范围内的第一个（分数最小的）元素作为起点 */
        eptr = zzlFirstInLexRange(zl,&range);

        /* No "first" element */
        /* 有序集合不存在分数满足范围的第一个成员，则向客户端回复0，并退出函数 */
        if (eptr == NULL) {
            zslFreeLexRange(&range);
            addReply(c, shared.czero);
            return;
        }

        /* First element is in range */
        /* 有序集合内存在分数满足范围的第一个成员 */
        sptr = lpNext(zl,eptr);
        serverAssertWithInfo(c,zobj,zzlLexValueLteMax(eptr,&range));

        /* Iterate over elements in range */
        /* 迭代范围内所有元素 */
        while (eptr) {
            /* Abort when the node is no longer in range. */
            /* 如果当前节点的成员分数不在范围内，则放弃操作 */
            if (!zzlLexValueLteMax(eptr,&range)) {
                break;
            } else {
                count++;
                zzlNext(zl,&eptr,&sptr);
            }
        }
    } else if (zobj->encoding == OBJ_ENCODING_SKIPLIST) {
        zset *zs = zobj->ptr;
        zskiplist *zsl = zs->zsl;
        zskiplistNode *zn;
        unsigned long rank;

        /* Find first element in range */
        /* 获取范围内的第一个元素 */
        zn = zslFirstInLexRange(zsl, &range);

        /* Use rank of first element, if any, to determine preliminary count */
        /* 使用第一个元素的排名（如果有）来确定初步计数 */
        if (zn != NULL) {
            rank = zslGetRank(zsl, zn->score, zn->ele);
            count = (zsl->length - (rank - 1));

            /* Find last element in range */
            /* 获取范围内的最后一个元素 */
            zn = zslLastInLexRange(zsl, &range);

            /* Use rank of last element, if any, to determine the actual count */
            /* 使用最后一个元素的排名（如果有）来确定实际计数 */
            if (zn != NULL) {
                rank = zslGetRank(zsl, zn->score, zn->ele);
                count -= (zsl->length - rank);
            }
        }
    } else {
        serverPanic("Unknown sorted set encoding");
    }

    zslFreeLexRange(&range);
    addReplyLongLong(c, count);
}

/* This command implements ZRANGEBYLEX, ZREVRANGEBYLEX. */
/* zrangebylex , zrevrangebylex 命令的通用实现函数 */
void genericZrangebylexCommand(zrange_result_handler *handler,
    zlexrangespec *range, robj *zobj, int withscores, long offset, long limit,
    int reverse)
{
    unsigned long rangelen = 0;

    handler->beginResultEmission(handler, -1);

    if (zobj->encoding == OBJ_ENCODING_LISTPACK) {
        unsigned char *zl = zobj->ptr;
        unsigned char *eptr, *sptr;
        unsigned char *vstr;
        unsigned int vlen;
        long long vlong;

        /* If reversed, get the last node in range as starting point. */
        /* 如果 reversed 为真，将范围内最后一个（分数最大的）节点作为起点 */
        if (reverse) {
            eptr = zzlLastInLexRange(zl,range);
        } else {
            eptr = zzlFirstInLexRange(zl,range);
        }

        /* Get score pointer for the first element. */
        /* 获取起点成员的分数*/
        if (eptr)
            sptr = lpNext(zl,eptr);

        /* If there is an offset, just traverse the number of elements without
         * checking the score because that is done in the next loop. */

        /* 如果输入了偏移量（大于0），只移动节点而不用检查分数，
         * 我们在下一个循环才需要检查分数 */
        while (eptr && offset--) {
            if (reverse) {
                zzlPrev(zl,&eptr,&sptr);
            } else {
                zzlNext(zl,&eptr,&sptr);
            }
        }

        while (eptr && limit--) {
            double score = 0;
            if (withscores) /* withscores 为真则提取分数，如果 withscores 为假，请不要费心提取分数 */
                score = zzlGetScore(sptr);

            /* Abort when the node is no longer in range. */
            /* 如果当前节点的成员分数不在范围内，则放弃操作 */
            if (reverse) {
                if (!zzlLexValueGteMin(eptr,range)) break;
            } else {
                if (!zzlLexValueLteMax(eptr,range)) break;
            }

            vstr = lpGetValue(eptr,&vlen,&vlong);
            rangelen++;
            if (vstr == NULL) {
                handler->emitResultFromLongLong(handler, vlong, score);
            } else {
                handler->emitResultFromCBuffer(handler, vstr, vlen, score);
            }

            /* Move to next node */
            /* 移动到下一个节点 */
            if (reverse) {
                zzlPrev(zl,&eptr,&sptr);
            } else {
                zzlNext(zl,&eptr,&sptr);
            }
        }
    } else if (zobj->encoding == OBJ_ENCODING_SKIPLIST) {
        zset *zs = zobj->ptr;
        zskiplist *zsl = zs->zsl;
        zskiplistNode *ln;

        /* If reversed, get the last node in range as starting point. */
        /* 如果 reverse 为真，则取范围内的最后一个（分数最大的）节点作为起点 */
        if (reverse) {
            ln = zslLastInLexRange(zsl,range);
        } else {
            ln = zslFirstInLexRange(zsl,range);
        }

        /* If there is an offset, just traverse the number of elements without
         * checking the score because that is done in the next loop. */
        /* 如果输入了偏移量（大于0），只移动节点而不用检查分数，
         * 我们在下一个循环才需要检查分数 */
        while (ln && offset--) {
            if (reverse) {
                ln = ln->backward;
            } else {
                ln = ln->level[0].forward;
            }
        }

        while (ln && limit--) {
            /* Abort when the node is no longer in range. */
            /* 如果当前节点的成员分数不在范围内，则放弃操作 */
            if (reverse) {
                if (!zslLexValueGteMin(ln->ele,range)) break;
            } else {
                if (!zslLexValueLteMax(ln->ele,range)) break;
            }

            rangelen++;
            handler->emitResultFromCBuffer(handler, ln->ele, sdslen(ln->ele), ln->score);

            /* Move to next node */
            /* 移动到下一个节点 */
            if (reverse) {
                ln = ln->backward;
            } else {
                ln = ln->level[0].forward;
            }
        }
    } else {
        serverPanic("Unknown sorted set encoding");
    }

    handler->finalizeResultEmission(handler, rangelen);
}

/* ZRANGEBYLEX <key> <min> <max> [LIMIT offset count] */
/* 处理 zrangebylex 命令的函数 */
void zrangebylexCommand(client *c) {
    zrange_result_handler handler;
    zrangeResultHandlerInit(&handler, c, ZRANGE_CONSUMER_TYPE_CLIENT);
    zrangeGenericCommand(&handler, 1, 0, ZRANGE_LEX, ZRANGE_DIRECTION_FORWARD);
}

/* ZREVRANGEBYLEX <key> <max> <min> [LIMIT offset count] */
/* 处理 zrevrangebylex 命令的函数 */
void zrevrangebylexCommand(client *c) {
    zrange_result_handler handler;
    zrangeResultHandlerInit(&handler, c, ZRANGE_CONSUMER_TYPE_CLIENT);
    zrangeGenericCommand(&handler, 1, 0, ZRANGE_LEX, ZRANGE_DIRECTION_REVERSE);
}

/**
 * This function handles ZRANGE and ZRANGESTORE, and also the deprecated
 * Z[REV]RANGE[BYPOS|BYLEX] commands.
 *
 * The simple ZRANGE and ZRANGESTORE can take _AUTO in rangetype and direction,
 * other command pass explicit value.
 *
 * The argc_start points to the src key argument, so following syntax is like:
 * <src> <min> <max> [BYSCORE | BYLEX] [REV] [WITHSCORES] [LIMIT offset count]
 */

/*
* 这个函数处理 ZRANGE 和 ZRANGESTORE，以及不推荐使用的 Z[REV]RANGE[BYPOS|BYLEX] 命令
* 
* 对于简单的 ZRANGE 和 ZRANGESTORE 可以在 rangetype 和 direction 参数上使用 _AUTO，
* 对于其他的命令需要明确的参数值
* 
* argc_start 为客户端输入的命令中 src key 的下标，用于定位 key 的位置，
* 这是因为不同命令的输入参数顺序可能是不同的，比如 zrangestore dst {src} 和 zrange {key}
*/
void zrangeGenericCommand(zrange_result_handler *handler, int argc_start, int store,
                          zrange_type rangetype, zrange_direction direction)
{
    client *c = handler->client;
    robj *key = c->argv[argc_start];
    robj *zobj;
    zrangespec range;
    zlexrangespec lexrange;
    int minidx = argc_start + 1;
    int maxidx = argc_start + 2;

    /* Options common to all */
    /* 与所有的可选参数相关的变量 */
    long opt_start = 0;
    long opt_end = 0;
    int opt_withscores = 0;
    long opt_offset = 0;
    long opt_limit = -1;

    /* Step 1: Skip the <src> <min> <max> args and parse remaining optional arguments. */
    /* 第 1 步： 跳过<src> <min> <max>，解析之后的可选参数 */
    for (int j=argc_start + 3; j < c->argc; j++) {
        int leftargs = c->argc-j-1; /* 剩余未解析参数个数 */

        /* strcasecmp 用于比较两个字符串，两个字符串相等时返回0，
         * 这里的 if 条件为“非 zrangestore 命令且解析到有 withscores ” */
        if (!store && !strcasecmp(c->argv[j]->ptr,"withscores")) {
            opt_withscores = 1;

        /* 条件：解析到 limit 且剩余未解析参数个数 >= 2（因为 limit 至少要跟两个参数 offset 和 count）*/
        } else if (!strcasecmp(c->argv[j]->ptr,"limit") && leftargs >= 2) {
            if ((getLongFromObjectOrReply(c, c->argv[j+1], &opt_offset, NULL) != C_OK) ||
                (getLongFromObjectOrReply(c, c->argv[j+2], &opt_limit, NULL) != C_OK))
            {
                return;
            }
            j += 2; /* 跳过 offset 和 count */

          /* 条件：使用 zrange 命令解析到 rev 参数，则更改方向相关变量 direction 为 ZRANGE_DIRECTION_REVERSE */
        } else if (direction == ZRANGE_DIRECTION_AUTO &&
                   !strcasecmp(c->argv[j]->ptr,"rev"))
        {
            direction = ZRANGE_DIRECTION_REVERSE;

          /* 条件：使用 zrange 命令解析到 bylex 参数，则更改与范围类型相关变量 rangetype 为 ZRANGE_LEX ，下方类似*/
        } else if (rangetype == ZRANGE_AUTO &&
                   !strcasecmp(c->argv[j]->ptr,"bylex"))
        {
            rangetype = ZRANGE_LEX;
        } else if (rangetype == ZRANGE_AUTO &&
                   !strcasecmp(c->argv[j]->ptr,"byscore"))
        {
            rangetype = ZRANGE_SCORE;
        } else {
            addReplyErrorObject(c,shared.syntaxerr);
            return;
        }
    }

    /* Use defaults if not overridden by arguments. */
    /* 如果代码执行到这 direction 或 rangetype 还是 AUTO，则使用默认值 */
    if (direction == ZRANGE_DIRECTION_AUTO)
        direction = ZRANGE_DIRECTION_FORWARD;
    if (rangetype == ZRANGE_AUTO)
        rangetype = ZRANGE_RANK;

    /* Check for conflicting arguments. */
    /* 检查是否有冲突的参数（算语法错误），具体什么错误请看下方报错返回的字符串信息就可以理解了 */
    if (opt_limit != -1 && rangetype == ZRANGE_RANK) {
        addReplyError(c,"syntax error, LIMIT is only supported in combination with either BYSCORE or BYLEX");
        return;
    }
    if (opt_withscores && rangetype == ZRANGE_LEX) {
        addReplyError(c,"syntax error, WITHSCORES not supported in combination with BYLEX");
        return;
    }

    /* 当 direction 是 ZRANGE_DIRECTION_REVERSE，将原来输入参数的 min 和 max 做个交换 */
    if (direction == ZRANGE_DIRECTION_REVERSE &&
        ((ZRANGE_SCORE == rangetype) || (ZRANGE_LEX == rangetype)))
    {
        /* Range is given as [max,min] */
        int tmp = maxidx;
        maxidx = minidx;
        minidx = tmp;
    }

    /* Step 2: Parse the range. */
    /* 第 2 步：解析范围类型，
     * 以下会根据你输入的命令参数分 AUTO/RANK, SCORE, LEX 三种情况来解析入参 min, max */
    switch (rangetype) {
    case ZRANGE_AUTO:
    case ZRANGE_RANK:
        /* Z[REV]RANGE, ZRANGESTORE [REV]RANGE */
        if ((getLongFromObjectOrReply(c, c->argv[minidx], &opt_start,NULL) != C_OK) ||
            (getLongFromObjectOrReply(c, c->argv[maxidx], &opt_end,NULL) != C_OK))
        {
            return;
        }
        break;

    case ZRANGE_SCORE:
        /* Z[REV]RANGEBYSCORE, ZRANGESTORE [REV]RANGEBYSCORE */
        if (zslParseRange(c->argv[minidx], c->argv[maxidx], &range) != C_OK) {
            addReplyError(c, "min or max is not a float");
            return;
        }
        break;

    case ZRANGE_LEX:
        /* Z[REV]RANGEBYLEX, ZRANGESTORE [REV]RANGEBYLEX */
        if (zslParseLexRange(c->argv[minidx], c->argv[maxidx], &lexrange) != C_OK) {
            addReplyError(c, "min or max not valid string range item");
            return;
        }
        break;
    }

    if (opt_withscores || store) {
        zrangeResultHandlerScoreEmissionEnable(handler);
    }

    /* Step 3: Lookup the key and get the range. */
    /* 第 3 步： 查找 key */
    zobj = lookupKeyRead(c->db, key);

    /* key不存在 */
    if (zobj == NULL) {
        if (store) {
            handler->beginResultEmission(handler, -1);
            handler->finalizeResultEmission(handler, 0);
        } else {
            addReply(c, shared.emptyarray);
        }
        goto cleanup;
    }

    /* key 存在，但类型不为 ZSET */
    if (checkType(c,zobj,OBJ_ZSET)) goto cleanup;

    /* Step 4: Pass this to the command-specific handler. */
    /* 第 4 步：根据范围类型，调用专门的处理函数进行处理 */
    switch (rangetype) {
    case ZRANGE_AUTO:
    case ZRANGE_RANK:
        genericZrangebyrankCommand(handler, zobj, opt_start, opt_end,
            opt_withscores || store, direction == ZRANGE_DIRECTION_REVERSE);
        break;

    case ZRANGE_SCORE:
        genericZrangebyscoreCommand(handler, &range, zobj, opt_offset,
            opt_limit, direction == ZRANGE_DIRECTION_REVERSE);
        break;

    case ZRANGE_LEX:
        genericZrangebylexCommand(handler, &lexrange, zobj, opt_withscores || store,
            opt_offset, opt_limit, direction == ZRANGE_DIRECTION_REVERSE);
        break;
    }

    /* Instead of returning here, we'll just fall-through the clean-up. */

cleanup:
    
    /* 如果范围类型为 ZRANGE_LEX，则需要清理内存，
     * 因为 zlexrangespec 结构体中有两个 sds 成员，它们创建时是动态分配 */
    if (rangetype == ZRANGE_LEX) {
        zslFreeLexRange(&lexrange);
    }
}

/* 处理 zcard 命令的函数 */
void zcardCommand(client *c) {
    robj *key = c->argv[1];
    robj *zobj;

    if ((zobj = lookupKeyReadOrReply(c,key,shared.czero)) == NULL ||
        checkType(c,zobj,OBJ_ZSET)) return;

    addReplyLongLong(c,zsetLength(zobj)); /* 返回有序集合的元素总数 */
}

/* 处理 zscore 命令的函数 */
void zscoreCommand(client *c) {
    robj *key = c->argv[1];
    robj *zobj;
    double score;

    if ((zobj = lookupKeyReadOrReply(c,key,shared.null[c->resp])) == NULL ||
        checkType(c,zobj,OBJ_ZSET)) return;

    /* 返回有序集合中指定成员的分数，不存在则返回 NULL */
    if (zsetScore(zobj,c->argv[2]->ptr,&score) == C_ERR) {
        addReplyNull(c);
    } else {
        addReplyDouble(c,score);
    }
}

/* 处理 zmscore 命令的函数 */
void zmscoreCommand(client *c) {
    robj *key = c->argv[1];
    robj *zobj;
    double score;
    zobj = lookupKeyRead(c->db,key);
    if (checkType(c,zobj,OBJ_ZSET)) return;

    addReplyArrayLen(c,c->argc - 2);
    for (int j = 2; j < c->argc; j++) {
        /* Treat a missing set the same way as an empty set */
        /* 将不存在的集合看作空集 */
        if (zobj == NULL || zsetScore(zobj,c->argv[j]->ptr,&score) == C_ERR) {
            addReplyNull(c);
        } else {
            addReplyDouble(c,score);
        }
    }
}

/* zrank, zrevrank 命令通用实现函数 */
void zrankGenericCommand(client *c, int reverse) {
    robj *key = c->argv[1];
    robj *ele = c->argv[2];
    robj *zobj;
    long rank;

    if ((zobj = lookupKeyReadOrReply(c,key,shared.null[c->resp])) == NULL ||
        checkType(c,zobj,OBJ_ZSET)) return;

    serverAssertWithInfo(c,ele,sdsEncodedObject(ele));
    rank = zsetRank(zobj,ele->ptr,reverse);
    if (rank >= 0) {
        addReplyLongLong(c,rank);
    } else {
        addReplyNull(c);
    }
}

/* 处理 zrank 命令的函数 */
void zrankCommand(client *c) {
    zrankGenericCommand(c, 0);
}

/* 处理 zrevrank 命令的函数 */
void zrevrankCommand(client *c) {
    zrankGenericCommand(c, 1);
}

/* 处理 zscan 命令的函数 */
void zscanCommand(client *c) {
    robj *o;
    unsigned long cursor;

    if (parseScanCursorOrReply(c,c->argv[2],&cursor) == C_ERR) return;
    if ((o = lookupKeyReadOrReply(c,c->argv[1],shared.emptyscan)) == NULL ||
        checkType(c,o,OBJ_ZSET)) return;
    scanGenericCommand(c,o,cursor);
}

/* This command implements the generic zpop operation, used by:
 * ZPOPMIN, ZPOPMAX, BZPOPMIN, BZPOPMAX and ZMPOP. This function is also used
 * inside blocked.c in the unblocking stage of BZPOPMIN, BZPOPMAX and BZMPOP.
 *
 * If 'emitkey' is true also the key name is emitted, useful for the blocking
 * behavior of BZPOP[MIN|MAX], since we can block into multiple keys.
 * Or in ZMPOP/BZMPOP, because we also can take multiple keys.
 *
 * 'count' is the number of elements requested to pop, or -1 for plain single pop.
 *
 * 'use_nested_array' when false it generates a flat array (with or without key name).
 * When true, it generates a nested 2 level array of field + score pairs, or 3 level when emitkey is set.
 *
 * 'reply_nil_when_empty' when true we reply a NIL if we are not able to pop up any elements.
 * Like in ZMPOP/BZMPOP we reply with a structured nested array containing key name
 * and member + score pairs. In these commands, we reply with null when we have no result.
 * Otherwise in ZPOPMIN/ZPOPMAX we reply an empty array by default.
 *
 * 'deleted' is an optional output argument to get an indication
 * if the key got deleted by this function.
 * */

/* 这条命令实现了通用的 zpop 操作，它被用于:
 * ZPOPMIN, ZPOPMAX, BZPOPMIN, BZPOPMAX 和 ZMPOP 命令。
 * 这个函数也在 blocked.c 中用于 BZPOPMIN、BZPOPMAX 和 BZMPOP 的解除阻塞阶段。
 *
 * 如果 'emitkey' 为真，key名 也会被回复给客户端，
 * 这对 BZPOP[MIN|MAX] 的阻塞行为很有用，因为我们可以阻塞到多个键。
 * 或者在 ZMPOP/BZMPOP 中，因为我们也可以取多个 key。
 * （有了 key名 就能知道是从哪个有序集合中弹出成员了）
 * 
 * 'count' 是请求弹出的成员数，-1 表示普通的单个弹出。
 *
 * 'use_nested_array' 当为 false 时，它​​会生成一个一层（平面）数组（有或没有 key名）。
 * 当为 true 时，它​​会生成一个嵌套的二层（元素 + 分数）数组，或者在设置了 emitkey 时生成三层数组。
 *
 * reply_nil_when_empty' 为真时，如果我们不能弹出任何成员，我们会回复一个 nil。
 * 就像在 ZMPOP/BZMPOP 中，我们用一个结构化的嵌套数组来回复，其中包含 key名 和 “元素+分数”对。
 * 在这两个命令中，当我们结果为空时，我们会回复 nil。而在 ZPOPMIN/ZPOPMAX 中，我们默认回复一个空数组。
 *
 * 'delete' 是一个可选的输出参数，标志 key 是否被该函数删除。
 */
void genericZpopCommand(client *c, robj **keyv, int keyc, int where, int emitkey,
                        long count, int use_nested_array, int reply_nil_when_empty, int *deleted) {
    int idx;
    robj *key = NULL;
    robj *zobj = NULL;
    sds ele;
    double score;

    if (deleted) *deleted = 0;

    /* Check type and break on the first error, otherwise identify candidate. */
    /* 检查类型，并在出现第一个错误时中断循环，否则确定被删除候选成员 */
    idx = 0;
    while (idx < keyc) {
        key = keyv[idx++];
        zobj = lookupKeyWrite(c->db,key);
        if (!zobj) continue;
        if (checkType(c,zobj,OBJ_ZSET)) return;
        break;
    }

    /* No candidate for zpopping, return empty. */
    /* 没有能被删除的候选成员，回复空数组 */
    if (!zobj) {
        if (reply_nil_when_empty) {
            addReplyNullArray(c);
        } else {
            addReply(c,shared.emptyarray);
        }
        return;
    }

    if (count == 0) {
        /* ZPOPMIN/ZPOPMAX with count 0. */
        /* 使用 ZPOPMIN/ZPOPMAX 命令且输入的 count 为 0，回复空数组 */
        addReply(c, shared.emptyarray);
        return;
    }

    long result_count = 0;

    /* When count is -1, we need to correct it to 1 for plain single pop. */
    /* 当 count 为 -1 时，我们需要将其纠正为1，以实现普通的单一元素弹出 */
    if (count == -1) count = 1;

    long llen = zsetLength(zobj);
    long rangelen = (count > llen) ? llen : count;

    if (!use_nested_array && !emitkey) {
        /* ZPOPMIN/ZPOPMAX with or without COUNT option in RESP2. */
        /* 使用 RESP2 协议回复，并检查 ZPOPMIN/ZPOPMAX 是否输入有 COUNT 选项 */
        addReplyArrayLen(c, rangelen * 2);
    } else if (use_nested_array && !emitkey) {
        /* ZPOPMIN/ZPOPMAX with COUNT option in RESP3. */
        /* 使用 RESP3 协议回复，并检查 ZPOPMIN/ZPOPMAX 是否输入有 COUNT 选项 */
        addReplyArrayLen(c, rangelen);
    } else if (!use_nested_array && emitkey) {
        /* BZPOPMIN/BZPOPMAX in RESP2 and RESP3. */
        /* RESP2 和 RESP3 中的 BZPOPMIN/BZPOPMAX  */
        addReplyArrayLen(c, rangelen * 2 + 1);
        addReplyBulk(c, key);
    } else if (use_nested_array && emitkey) {
        /* ZMPOP/BZMPOP in RESP2 and RESP3. */
        /* RESP2 和 RESP3 中的 ZMPOP/BZMPOP  */
        addReplyArrayLen(c, 2);
        addReplyBulk(c, key);
        addReplyArrayLen(c, rangelen);
    }

    /* Remove the element. */
    /* 删除成员 */
    do {
        if (zobj->encoding == OBJ_ENCODING_LISTPACK) {
            unsigned char *zl = zobj->ptr;
            unsigned char *eptr, *sptr;
            unsigned char *vstr;
            unsigned int vlen;
            long long vlong;

            /* Get the first or last element in the sorted set. */
            /* 获取有序集合的第一个或最后一个元素（由参数 where 决定）*/
            eptr = lpSeek(zl,where == ZSET_MAX ? -2 : 0);
            serverAssertWithInfo(c,zobj,eptr != NULL);
            vstr = lpGetValue(eptr,&vlen,&vlong);
            if (vstr == NULL)
                ele = sdsfromlonglong(vlong);
            else
                ele = sdsnewlen(vstr,vlen);

            /* Get the score. */
            /* 获取分数 */
            sptr = lpNext(zl,eptr);
            serverAssertWithInfo(c,zobj,sptr != NULL);
            score = zzlGetScore(sptr);
        } else if (zobj->encoding == OBJ_ENCODING_SKIPLIST) {
            zset *zs = zobj->ptr;
            zskiplist *zsl = zs->zsl;
            zskiplistNode *zln;

            /* Get the first or last element in the sorted set. */
            /* 获取有序集合的第一个或最后一个元素（由参数 where 决定）*/
            zln = (where == ZSET_MAX ? zsl->tail :
                                       zsl->header->level[0].forward);

            /* There must be an element in the sorted set. */
            /* 一定有一个元素在当前的有序集合中 */
            serverAssertWithInfo(c,zobj,zln != NULL);
            ele = sdsdup(zln->ele);
            score = zln->score;
        } else {
            serverPanic("Unknown sorted set encoding");
        }

        serverAssertWithInfo(c,zobj,zsetDel(zobj,ele));
        server.dirty++;

        if (result_count == 0) { /* 只用在第一次迭代时发送事件通知 */
            char *events[2] = {"zpopmin","zpopmax"};
            notifyKeyspaceEvent(NOTIFY_ZSET,events[where],key,c->db->id);
            signalModifiedKey(c,c->db,key);
        }

        if (use_nested_array) {
            addReplyArrayLen(c,2);
        }
        addReplyBulkCBuffer(c,ele,sdslen(ele));
        addReplyDouble(c,score);
        sdsfree(ele);
        ++result_count;
    } while(--rangelen);

    /* Remove the key, if indeed needed. */
    /* 如果有序集合已空，则将该 key 从数据库中删除 */
    if (zsetLength(zobj) == 0) {
        if (deleted) *deleted = 1;

        dbDelete(c->db,key);
        notifyKeyspaceEvent(NOTIFY_GENERIC,"del",key,c->db->id);
    }

    /* 如果输入的命令为 zmpop */
    if (c->cmd->proc == zmpopCommand) {
        /* Always replicate it as ZPOP[MIN|MAX] with COUNT option instead of ZMPOP. */
        /* 始终将其复制为带有 COUNT 选项的 ZPOP[MIN|MAX] 进行传播（传播到 AOF 或 从服务器），而不是 ZMPOP。 */
        robj *count_obj = createStringObjectFromLongLong((count > llen) ? llen : count);
        rewriteClientCommandVector(c, 3,
                                   (where == ZSET_MAX) ? shared.zpopmax : shared.zpopmin,
                                   key, count_obj);
        decrRefCount(count_obj);
    }
}

/* ZPOPMIN/ZPOPMAX key [<count>] */
/* 处理 zpopmin / zpopmax 命令的函数 */
void zpopMinMaxCommand(client *c, int where) {
    if (c->argc > 3) {
        addReplyErrorObject(c,shared.syntaxerr);
        return;
    }

    long count = -1; /* 默认值设置为-1，含义为普通地弹出单个元素 */
    if (c->argc == 3 && getPositiveLongFromObjectOrReply(c, c->argv[2], &count, NULL) != C_OK)
        return;

    /* Respond with a single (flat) array in RESP2 or if count is -1
     * (returning a single element). In RESP3, when count > 0 use nested array. */

    /* 若回复客户端使用一层（平面）数组，则条件为使用 RESP2 协议或者 count 为 -1（返回单个元素）。
     * 若使用 RESP3 ，当 count > 0 时使用嵌套数组。 */
    int use_nested_array = (c->resp > 2 && count != -1);

    genericZpopCommand(c, &c->argv[1], 1, where, 0, count, use_nested_array, 0, NULL);
}

/* ZPOPMIN key [<count>] */
/* 处理 zpopmin 命令的函数 */ 
void zpopminCommand(client *c) {
    zpopMinMaxCommand(c, ZSET_MIN);
}

/* ZPOPMAX key [<count>] */
/* 处理 zpopmax 命令的函数 */ 
void zpopmaxCommand(client *c) {
    zpopMinMaxCommand(c, ZSET_MAX);
}

/* BZPOPMIN, BZPOPMAX, BZMPOP actual implementation.
 *
 * 'numkeys' is the number of keys.
 *
 * 'timeout_idx' parameter position of block timeout.
 *
 * 'where' ZSET_MIN or ZSET_MAX.
 *
 * 'count' is the number of elements requested to pop, or -1 for plain single pop.
 *
 * 'use_nested_array' when false it generates a flat array (with or without key name).
 * When true, it generates a nested 3 level array of keyname, field + score pairs.
 * */

/* BZPOPMIN、BZPOPMAX、BZMPOP 的通用实现函数。
 *
 * 'numkeys' 是 key 的数量。
 *
 * 'timeout_idx' 阻塞超时时间 timeout 的参数位置。
 *
 * 'where' 是 ZSET_MIN 或 ZSET_MAX，表示从最小还是最大端弹出成员。
 *
 * 'count' 是请求弹出的成员数，或 -1 表示普通的单个弹出。
 *
 * 'use_nested_array' 当为 false 时，它会生成一个一层（平面）数组（有或没有 key名）。
 *  当为 true 时，它会生成一个嵌套的三层（key名，“元素+分数”对）数组。
 * */
void blockingGenericZpopCommand(client *c, robj **keys, int numkeys, int where,
                                int timeout_idx, long count, int use_nested_array, int reply_nil_when_empty) {
    robj *o;
    robj *key;
    mstime_t timeout;
    int j;

    /* 获取输入参数 timeout */
    if (getTimeoutFromObjectOrReply(c,c->argv[timeout_idx],&timeout,UNIT_SECONDS)
        != C_OK) return;

    for (j = 0; j < numkeys; j++) {
        key = keys[j];
        o = lookupKeyWrite(c->db,key);
        /* Non-existing key, move to next key. */
        /* key 不存在，移动到下一个 key */
        if (o == NULL) continue;

        if (checkType(c,o,OBJ_ZSET)) return;

        long llen = zsetLength(o);
        /* Empty zset, move to next key. */
        /* 有序集合为空，移动到下一个 key */
        if (llen == 0) continue;

        /* Non empty zset, this is like a normal ZPOP[MIN|MAX]. */
        /* 有序集合非空，这里的操作像普通的 ZPOP[MIN|MAX] 命令 */
        genericZpopCommand(c, &key, 1, where, 1, count, use_nested_array, reply_nil_when_empty, NULL);

        /* count 为 -1，只弹出单个元素 */
        if (count == -1) {
            /* Replicate it as ZPOP[MIN|MAX] instead of BZPOP[MIN|MAX]. */
            /* 将其复制为 ZPOP[MIN|MAX] 命令进行传播，而不是 BZPOP[MIN|MAX]。 */
            rewriteClientCommandVector(c,2,
                                       (where == ZSET_MAX) ? shared.zpopmax : shared.zpopmin,
                                       key);
        } else {
            /* Replicate it as ZPOP[MIN|MAX] with COUNT option. */
            /* 将其复制为带有 COUNT 参数的 ZPOP[MIN|MAX] 命令进行传播 */
            robj *count_obj = createStringObjectFromLongLong((count > llen) ? llen : count);
            rewriteClientCommandVector(c, 3,
                                       (where == ZSET_MAX) ? shared.zpopmax : shared.zpopmin,
                                       key, count_obj);
            decrRefCount(count_obj);
        }

        return;
    }

    /* If we are not allowed to block the client and the zset is empty the only thing
     * we can do is treating it as a timeout (even with timeout 0). */
    /* 如果我们不允许阻塞客户端并且 zset 为空，我们唯一能做的就是将其视为超时（即使输入的 timeout 为 0） */
    if (c->flags & CLIENT_DENY_BLOCKING) {
        addReplyNullArray(c);
        return;
    }

    /* If the keys do not exist we must block */
    /* 如果所有 key 都不存在则将它们阻塞 */
    struct blockPos pos = {where};
    blockForKeys(c,BLOCKED_ZSET,keys,numkeys,count,timeout,NULL,&pos,NULL);
}

// BZPOPMIN key [key ...] timeout
/* 处理 bzpopmin 命令的函数 */
void bzpopminCommand(client *c) {
    blockingGenericZpopCommand(c, c->argv+1, c->argc-2, ZSET_MIN, c->argc-1, -1, 0, 0);
}

// BZPOPMAX key [key ...] timeout
/* 处理 bzpopmax 命令的函数 */
void bzpopmaxCommand(client *c) {
    blockingGenericZpopCommand(c, c->argv+1, c->argc-2, ZSET_MAX, c->argc-1, -1, 0, 0);
}

/* 用于读取存储了 listpack 随机元素/分数 的参数 keys 和 vals，并把它们添加到回复中  */
static void zrandmemberReplyWithListpack(client *c, unsigned int count, listpackEntry *keys, listpackEntry *vals) {
    for (unsigned long i = 0; i < count; i++) {
        if (vals && c->resp > 2)
            addReplyArrayLen(c,2);
        if (keys[i].sval)
            addReplyBulkCBuffer(c, keys[i].sval, keys[i].slen);
        else
            addReplyBulkLongLong(c, keys[i].lval);
        if (vals) {
            if (vals[i].sval) {
                addReplyDouble(c, zzlStrtod(vals[i].sval,vals[i].slen));
            } else
                addReplyDouble(c, vals[i].lval);
        }
    }
}

/* How many times bigger should be the zset compared to the requested size
 * for us to not use the "remove elements" strategy? Read later in the
 * implementation for more info. */
/* 与请求的大小相比，zset应该大多少倍，我们才不会使用 "移除成员" 策略？
 * 请阅读后面的实现，了解更多信息。 */
#define ZRANDMEMBER_SUB_STRATEGY_MUL 3

/* If client is trying to ask for a very large number of random elements,
 * queuing may consume an unlimited amount of memory, so we want to limit
 * the number of randoms per time. */

/* 如果客户端试图请求非常多的随机成员，排队可能会消耗无限的内存，
 * 所以我们要限制每次随机的成员数量。 */
#define ZRANDMEMBER_RANDOM_SAMPLE_LIMIT 1000

void zrandmemberWithCountCommand(client *c, long l, int withscores) {
    unsigned long count, size;
    int uniq = 1;
    robj *zsetobj;

    if ((zsetobj = lookupKeyReadOrReply(c, c->argv[1], shared.emptyarray))
        == NULL || checkType(c, zsetobj, OBJ_ZSET)) return;
    size = zsetLength(zsetobj);

    if(l >= 0) {
        count = (unsigned long) l;
    } else {
        count = -l;
        uniq = 0;
    }

    /* If count is zero, serve it ASAP to avoid special cases later. */
    /* 如果 count 为 0，尽快返回，以避免之后的特殊情况。*/
    if (count == 0) {
        addReply(c,shared.emptyarray);
        return;
    }

    /* CASE 1: The count was negative, so the extraction method is just:
     * "return N random elements" sampling the whole set every time.
     * This case is trivial and can be served without auxiliary data
     * structures. This case is the only one that also needs to return the
     * elements in random order. */

    /* 第一种情况：count 为负数，所以方法为：
     * "返回 N 个随机成员"，每次都对整个集合进行采样。
     * 这种情况是很简单的，不需要辅助的数据结构就可以完成。
     * 这种情况是唯一真正需要以随机顺序返回成员的情况。 */
    if (!uniq || count == 1) {
        if (withscores && c->resp == 2)
            addReplyArrayLen(c, count*2);
        else
            addReplyArrayLen(c, count);

        /* 有序集合的编码类型为跳表 */
        if (zsetobj->encoding == OBJ_ENCODING_SKIPLIST) {
            zset *zs = zsetobj->ptr;
            while (count--) {
                dictEntry *de = dictGetFairRandomKey(zs->dict); /* 在字典中随机获取一个 entry */
                sds key = dictGetKey(de);
                if (withscores && c->resp > 2)
                    addReplyArrayLen(c,2);
                addReplyBulkCBuffer(c, key, sdslen(key));
                if (withscores)
                    addReplyDouble(c, *(double*)dictGetVal(de)); 
            }

        /* 有序集合的编码类型为 listpack */
        } else if (zsetobj->encoding == OBJ_ENCODING_LISTPACK) {
            listpackEntry *keys, *vals = NULL;
            unsigned long limit, sample_count;
            limit = count > ZRANDMEMBER_RANDOM_SAMPLE_LIMIT ? ZRANDMEMBER_RANDOM_SAMPLE_LIMIT : count;
            keys = zmalloc(sizeof(listpackEntry)*limit);
            if (withscores)
                vals = zmalloc(sizeof(listpackEntry)*limit);
            while (count) {
                sample_count = count > limit ? limit : count;
                count -= sample_count;
                lpRandomPairs(zsetobj->ptr, sample_count, keys, vals);
                zrandmemberReplyWithListpack(c, sample_count, keys, vals);
            }
            zfree(keys);
            zfree(vals);
        }
        return;
    }

    zsetopsrc src;
    zsetopval zval;
    src.subject = zsetobj;
    src.type = zsetobj->type;
    src.encoding = zsetobj->encoding;
    zuiInitIterator(&src);
    memset(&zval, 0, sizeof(zval));

    /* Initiate reply count, RESP3 responds with nested array, RESP2 with flat one. */
    /* 启动回复计数，RESP3 用嵌套数组响应，RESP2 用平面数组响应 */
    long reply_size = count < size ? count : size;
    if (withscores && c->resp == 2)
        addReplyArrayLen(c, reply_size*2);
    else
        addReplyArrayLen(c, reply_size);

    /* CASE 2:
    * The number of requested elements is greater than the number of
    * elements inside the zset: simply return the whole zset. */

   /* 第二种情况:
    * 请求的成员数量大于 zset 内的成员数量：简单地返回整个 zset */
    if (count >= size) {
        while (zuiNext(&src, &zval)) {
            if (withscores && c->resp > 2)
                addReplyArrayLen(c,2);
            addReplyBulkSds(c, zuiNewSdsFromValue(&zval));
            if (withscores)
                addReplyDouble(c, zval.score);
        }
        zuiClearIterator(&src);
        return;
    }

    /* CASE 3:
     * The number of elements inside the zset is not greater than
     * ZRANDMEMBER_SUB_STRATEGY_MUL times the number of requested elements.
     * In this case we create a dict from scratch with all the elements, and
     * subtract random elements to reach the requested number of elements.
     *
     * This is done because if the number of requested elements is just
     * a bit less than the number of elements in the set, the natural approach
     * used into CASE 4 is highly inefficient. */

    /* 第三种情况：
     * zset 内的成员数不大于 ZRANDMEMBER_SUB_STRATEGY_MUL * count 。
     * 在这种情况下，我们用所有的成员从头开始创建一个字典，
     * 然后减去随机成员以达到要求的成员数量。
     *
     * 这样做是因为如果要求的成员数量只是比集合中的成员数量少一点，
     * 在第四种情况中使用的普通方法是非常低效的。*/
    if (count*ZRANDMEMBER_SUB_STRATEGY_MUL > size) {
        dict *d = dictCreate(&sdsReplyDictType);
        dictExpand(d, size);
        /* Add all the elements into the temporary dictionary. */
        /* 把所有的成员加进临时字典中 */
        while (zuiNext(&src, &zval)) {
            sds key = zuiNewSdsFromValue(&zval);
            dictEntry *de = dictAddRaw(d, key, NULL);
            serverAssert(de);
            if (withscores)
                dictSetDoubleVal(de, zval.score);
        }
        serverAssert(dictSize(d) == size);

        /* Remove random elements to reach the right count. */
        /* 从字典中随机删除成员直至达到要求的成员数量 */
        while (size > count) {
            dictEntry *de;
            de = dictGetFairRandomKey(d);
            dictUnlink(d,dictGetKey(de));
            sdsfree(dictGetKey(de));
            dictFreeUnlinkedEntry(d,de);
            size--;
        }

        /* Reply with what's in the dict and release memory */
        /* 回复字典中的所有成员，并释放字典的内存空间 */
        dictIterator *di;
        dictEntry *de;
        di = dictGetIterator(d);
        while ((de = dictNext(di)) != NULL) {
            if (withscores && c->resp > 2)
                addReplyArrayLen(c,2);
            addReplyBulkSds(c, dictGetKey(de));
            if (withscores)
                addReplyDouble(c, dictGetDoubleVal(de));
        }

        dictReleaseIterator(di);
        dictRelease(d);
    }

    /* CASE 4: We have a big zset compared to the requested number of elements.
     * In this case we can simply get random elements from the zset and add
     * to the temporary set, trying to eventually get enough unique elements
     * to reach the specified count. */

    /* 第四种情况: 与要求的成员数量相比，我们有一个很大的 zset。
     * 在这种情况下，我们可以简单地从 zset 中获取随机成员并添加到临时集合中，
     * 直至字典中的成员达到指定的数量。*/
    else {
        if (zsetobj->encoding == OBJ_ENCODING_LISTPACK) {
            /* it is inefficient to repeatedly pick one random element from a
             * listpack. so we use this instead: */
            /* 从一个 listpack 中反复挑选一个随机成员的效率很低。所以我们用以下方法代替：*/
            listpackEntry *keys, *vals = NULL;
            keys = zmalloc(sizeof(listpackEntry)*count);
            if (withscores)
                vals = zmalloc(sizeof(listpackEntry)*count);
            
            /* 我们使用 lpRandomPairsUnique 函数获取 count 个随机元素和相应的分数，并保存在 keys 和 vals 中 */
            serverAssert(lpRandomPairsUnique(zsetobj->ptr, count, keys, vals) == count);
            zrandmemberReplyWithListpack(c, count, keys, vals);
            zfree(keys);
            zfree(vals);
            zuiClearIterator(&src);
            return;
        }

        /* Hashtable encoding (generic implementation) */
        /* 创建辅助字典 */
        unsigned long added = 0;
        dict *d = dictCreate(&hashDictType);
        dictExpand(d, count);

        /* 获取随机成员并加入到辅助字典中，直至达到要求的成员数量 */
        while (added < count) {
            listpackEntry key;
            double score;
            zsetTypeRandomElement(zsetobj, size, &key, withscores ? &score: NULL);

            /* Try to add the object to the dictionary. If it already exists
            * free it, otherwise increment the number of objects we have
            * in the result dictionary. */

            /* 尝试将该对象添加到字典中。如果它已经存在就释放它，
             * 否则就增加我们在结果字典中的成员数量。*/
            sds skey = zsetSdsFromListpackEntry(&key);
            if (dictAdd(d,skey,NULL) != DICT_OK) {
                sdsfree(skey);
                continue;
            }
            added++;

            if (withscores && c->resp > 2)
                addReplyArrayLen(c,2);
            zsetReplyFromListpackEntry(c, &key);
            if (withscores)
                addReplyDouble(c, score);
        }

        /* Release memory */
        /* 释放辅助字典 */
        dictRelease(d);
    }
    zuiClearIterator(&src);
}

/* ZRANDMEMBER key [<count> [WITHSCORES]] */
/* 处理 zrandmember 命令的函数 */
void zrandmemberCommand(client *c) {
    long l;
    int withscores = 0;
    robj *zset;
    listpackEntry ele;

    if (c->argc >= 3) {
        if (getLongFromObjectOrReply(c,c->argv[2],&l,NULL) != C_OK) return;
        if (c->argc > 4 || (c->argc == 4 && strcasecmp(c->argv[3]->ptr,"withscores"))) {
            addReplyErrorObject(c,shared.syntaxerr);
            return;
        } else if (c->argc == 4)
            withscores = 1;
        zrandmemberWithCountCommand(c, l, withscores);
        return;
    }

    /* Handle variant without <count> argument. Reply with simple bulk string */
    /* 处理没有 <count> 参数的命令，用简单的字符串回复给客户端 */
    if ((zset = lookupKeyReadOrReply(c,c->argv[1],shared.null[c->resp]))== NULL ||
        checkType(c,zset,OBJ_ZSET)) {
        return;
    }

    zsetTypeRandomElement(zset, zsetLength(zset), &ele,NULL);
    zsetReplyFromListpackEntry(c,&ele);
}

/* ZMPOP/BZMPOP
 *
 * 'numkeys_idx' parameter position of key number.
 * 'is_block' this indicates whether it is a blocking variant. */

/* zmpop/bzmpop 命令的通用实现函数
 *
 * 'numkeys_idx' 表示 numkeys 的参数位置。
 * 'is_block' 这表明它是否是一个阻塞的命令。*/
void zmpopGenericCommand(client *c, int numkeys_idx, int is_block) {
    long j;
    long numkeys = 0;      /* Number of keys. */
    int where = 0;         /* ZSET_MIN or ZSET_MAX. */
    long count = -1;       /* Reply will consist of up to count elements, depending on the zset's length. */

    /* 解析输入参数 numkeys */
    if (getRangeLongFromObjectOrReply(c, c->argv[numkeys_idx], 1, LONG_MAX,
                                      &numkeys, "numkeys should be greater than 0") != C_OK)
        return;

    /* Parse the where. where_idx: the index of where in the c->argv. */
    /* 解析输入的 where（MIN|MAX）， where_idx：c->argv（客户端输入参数） 中 where 的索引。 */
    long where_idx = numkeys_idx + numkeys + 1;
    if (where_idx >= c->argc) {
        addReplyErrorObject(c, shared.syntaxerr);
        return;
    }
    if (!strcasecmp(c->argv[where_idx]->ptr, "MIN")) {
        where = ZSET_MIN;
    } else if (!strcasecmp(c->argv[where_idx]->ptr, "MAX")) {
        where = ZSET_MAX;
    } else {
        addReplyErrorObject(c, shared.syntaxerr);
        return;
    }

    /* Parse the optional arguments. */
    /* 解析可选参数 */
    for (j = where_idx + 1; j < c->argc; j++) {
        char *opt = c->argv[j]->ptr;
        int moreargs = (c->argc - 1) - j;

        if (count == -1 && !strcasecmp(opt, "COUNT") && moreargs) {
            j++;
            if (getRangeLongFromObjectOrReply(c, c->argv[j], 1, LONG_MAX,
                                              &count,"count should be greater than 0") != C_OK)
                return;
        } else {
            addReplyErrorObject(c, shared.syntaxerr);
            return;
        }
    }

    if (count == -1) count = 1; /* 将 count 的 -1 转换为 1，表示弹出单个元素 */

    if (is_block) {
        /* BLOCK. We will handle CLIENT_DENY_BLOCKING flag in blockingGenericZpopCommand. */
        /* 该情况是命令为 bzmpop，
         * 我们会在 blockingGenericZpopCommand 中处理 CLIENT_DENY_BLOCKING （客户端不允许阻塞）标志。 */
        blockingGenericZpopCommand(c, c->argv+numkeys_idx+1, numkeys, where, 1, count, 1, 1);
    } else {
        /* NON-BLOCK */
        /* 该情况是命令为 zmpop，不阻塞 */
        genericZpopCommand(c, c->argv+numkeys_idx+1, numkeys, where, 1, count, 1, 1, NULL);
    }
}

/* ZMPOP numkeys key [<key> ...] MIN|MAX [COUNT count] */
/* 处理 zmpop 命令的函数 */
void zmpopCommand(client *c) {
    zmpopGenericCommand(c, 1, 0);
}

/* BZMPOP timeout numkeys key [<key> ...] MIN|MAX [COUNT count] */
/* 处理 bzmpop 命令的函数 */
void bzmpopCommand(client *c) {
    zmpopGenericCommand(c, 2, 1);
}
