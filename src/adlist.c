/* adlist.c - A generic doubly linked list implementation
 *
 * Copyright (c) 2006-2010, Salvatore Sanfilippo <antirez at gmail dot com>
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
#include "adlist.h"
#include "zmalloc.h"

/* Create a new list. The created list can be freed with
 * listRelease(), but private value of every node need to be freed
 * by the user before to call listRelease(), or by setting a free method using
 * listSetFreeMethod.
 *
 * On error, NULL is returned. Otherwise the pointer to the new list. */

/* 创建一个新链表，链表可以使用 listRelease 进行释放，如果链表节点有自己的私有值 void *value，
 * 需要设置 listSetFreeMethod 来定义自己的释放函数，用于释放链表节点空间。*/
list *listCreate(void)
{
    struct list *list;

    /* 申请链表空间 */
    if ((list = zmalloc(sizeof(*list))) == NULL)
        return NULL;
    /* 初始化链表 */
    list->head = list->tail = NULL;
    list->len = 0;
    list->dup = NULL;
    list->free = NULL;
    list->match = NULL;
    return list;
}

/* Remove all the elements from the list without destroying the list itself. */
/* 清空一个链表，即清空回收所有链表节点，这个不会回收链表本身，时间复杂度 O(N) */
void listEmpty(list *list)
{
    unsigned long len;
    listNode *current, *next;

    /* 当前指针先指向头节点 */
    current = list->head;
    /* O(1) 时间复杂度获取链表长度（节点数量） */
    len = list->len;
    while(len--) {
        /* 先记录下一跳节点 */
        next = current->next;
        /* 如果链表有定义 free 函数，调用它来释放节点的 value */
        if (list->free) list->free(current->value);
        /* 释放链表节点 */
        zfree(current);
        /* 当前指针指向下一跳节点，处理下一个节点 */
        current = next;
    }
    /* 清空完链表后，将头尾节点指针置空，将链表长度置 0 */
    list->head = list->tail = NULL;
    list->len = 0;
}

/* Free the whole list.
 *
 * This function can't fail. */
/* 释放一个链表，通过调用 listEmpty 清空链表节点，然后 zfree 链表空间 */
void listRelease(list *list)
{
    listEmpty(list);
    zfree(list);
}

/* Add a new node to the list, to head, containing the specified 'value'
 * pointer as value.
 *
 * On error, NULL is returned and no operation is performed (i.e. the
 * list remains unaltered).
 * On success the 'list' pointer you pass to the function is returned. */

/* 在链表头部插入节点（头插法），时间复杂度 O(1)
 * 例如 B->C->D 插入 A 节点变成 A->B->C->D */
list *listAddNodeHead(list *list, void *value)
{
    listNode *node;

    /* 分配节点空间 */
    if ((node = zmalloc(sizeof(*node))) == NULL)
        return NULL;
    /* 设置节点 value */
    node->value = value;
    if (list->len == 0) {
        /* 链表长度为 0 说明是一个空链表，插入后只有一个节点，
         * 新节点同时也是链表的头尾节点，前驱节点和后继节点都为空。 */
        list->head = list->tail = node;
        node->prev = node->next = NULL;
    } else {
        /* 链表不为空，插入的节点将作为链表的新头节点，
         * 新节点作为新头节点前驱节点为空，后继节点为链表原先的头节点，
         * 链表原先的头节点的前驱节点为新节点。*/
        node->prev = NULL;
        node->next = list->head;
        list->head->prev = node;
        list->head = node;
    }
    /* 维护链表长度 */
    list->len++;
    return list;
}

/* Add a new node to the list, to tail, containing the specified 'value'
 * pointer as value.
 *
 * On error, NULL is returned and no operation is performed (i.e. the
 * list remains unaltered).
 * On success the 'list' pointer you pass to the function is returned. */
/* 在链表尾部插入节点（尾插法），时间复杂度 O(1)
 * 例如 A->B->C 插入 D 节点变成 A->B->C->D */
list *listAddNodeTail(list *list, void *value)
{
    listNode *node;

    /* 分配节点空间 */
    if ((node = zmalloc(sizeof(*node))) == NULL)
        return NULL;
    /* 设置节点 value */
    node->value = value;
    if (list->len == 0) {
        /* 链表长度为 0 说明是一个空链表，插入后只有一个节点，
         * 新节点同时也是链表的头尾节点，前驱节点和后继节点都为空。 */
        list->head = list->tail = node;
        node->prev = node->next = NULL;
    } else {
        /* 链表不为空，插入的节点将作为链表的新尾节点，
         * 新节点作为新尾节点后继节点为空，前驱节点为链表原先的尾节点，
         * 链表原先的尾节点的后继节点为新节点。*/
        node->prev = list->tail;
        node->next = NULL;
        list->tail->next = node;
        list->tail = node;
    }
    /* 维护链表长度 */
    list->len++;
    return list;
}

/* 在 old_node 的前面或者后面插入一个新节点，时间复杂度 O(1) */
list *listInsertNode(list *list, listNode *old_node, void *value, int after) {
    listNode *node;

    /* 分配节点空间 */
    if ((node = zmalloc(sizeof(*node))) == NULL)
        return NULL;
    /* 设置节点 value */
    node->value = value;
    if (after) {
        /* 在 old_node 后面插入新节点
         * 新节点的前驱节点为 old_node，新节点的后继节点为 old_node 的后继节点 */
        node->prev = old_node;
        node->next = old_node->next;
        /* 如果 old_node 是链表尾节点，在它后面插入新节点，新节点将变成链表新尾节点 */
        if (list->tail == old_node) {
            list->tail = node;
        }
    } else {
        /* 在 old_node 前面插入新节点
         * 新节点的后继节点为 old_node，新节点的前驱节点为 old_node 的前驱节点 */
        node->next = old_node;
        node->prev = old_node->prev;
        /* 如果 old_node 是链表头节点，在它前面插入新节点，新节点将变成链表新头节点 */
        if (list->head == old_node) {
            list->head = node;
        }
    }
    /* 维护好前后节点的指针 */
    if (node->prev != NULL) {
        node->prev->next = node;
    }
    if (node->next != NULL) {
        node->next->prev = node;
    }
    /* 维护链表长度 */
    list->len++;
    return list;
}

/* Remove the specified node from the specified list.
 * It's up to the caller to free the private value of the node.
 *
 * This function can't fail. */
/* 在链表中删除指定节点，时间复杂度 O(1) */
void listDelNode(list *list, listNode *node)
{
    /* 如果节点有前驱节点，前驱节点的后继节点需要变更为删除节点的后继节点
     * 如果没有前驱节点，说明是链表头节点，链表新头节点变成删除节点的后继节点 */
    if (node->prev)
        node->prev->next = node->next;
    else
        list->head = node->next;
    /* 如果节点有后继节点，后继节点的前驱节点需要变更为删除节点的前驱节点
     * 如果没有后继节点，说明是链表尾节点，链表新尾节点变成删除节点的前驱节点 */
    if (node->next)
        node->next->prev = node->prev;
    else
        list->tail = node->prev;
    /* 如果链表有定义 free 函数，调用它释放 node->value */
    if (list->free) list->free(node->value);
    /* 释放删除节点空间 */
    zfree(node);
    /* 维护链表长度 */
    list->len--;
}

/* Returns a list iterator 'iter'. After the initialization every
 * call to listNext() will return the next element of the list.
 *
 * This function can't fail. */
/* 根据遍历方向获得一个 iterator，可以看到是根据方向来决定从头节点还是尾节点开始
 * 调用方通过调用 listNext 来获取下一个节点 */
listIter *listGetIterator(list *list, int direction)
{
    listIter *iter;

    if ((iter = zmalloc(sizeof(*iter))) == NULL) return NULL;
    /* 根据方向设置 iter->next 为头节点或者尾节点 */
    if (direction == AL_START_HEAD)
        iter->next = list->head;
    else
        iter->next = list->tail;
    /* 设置方向 */
    iter->direction = direction;
    return iter;
}

/* Release the iterator memory */
/* 释放 listGetIterator 获取到的 iterator */
void listReleaseIterator(listIter *iter) {
    zfree(iter);
}

/* Create an iterator in the list private iterator structure */
void listRewind(list *list, listIter *li) {
    li->next = list->head;
    li->direction = AL_START_HEAD;
}

void listRewindTail(list *list, listIter *li) {
    li->next = list->tail;
    li->direction = AL_START_TAIL;
}

/* Return the next element of an iterator.
 * It's valid to remove the currently returned element using
 * listDelNode(), but not to remove other elements.
 *
 * The function returns a pointer to the next element of the list,
 * or NULL if there are no more elements, so the classical usage
 * pattern is:
 *
 * iter = listGetIterator(list,<direction>);
 * while ((node = listNext(iter)) != NULL) {
 *     doSomethingWith(listNodeValue(node));
 * }
 *
 * */
/* 根据迭代器从链表中获取下一个节点，时间复杂度 O(1)
 * 获取的节点可以通过 listDelNode 进行删除 */
listNode *listNext(listIter *iter)
{
    listNode *current = iter->next;

    /* current 不为空，根据方向获取链表的下一个节点
     * HEAD: 通过 node->next 获取后继节点
     * TAIL: 通过 node->prev 获取前驱节点 */
    if (current != NULL) {
        if (iter->direction == AL_START_HEAD)
            iter->next = current->next;
        else
            iter->next = current->prev;
    }
    return current;
}

/* Duplicate the whole list. On out of memory NULL is returned.
 * On success a copy of the original list is returned.
 *
 * The 'Dup' method set with listSetDupMethod() function is used
 * to copy the node value. Otherwise the same pointer value of
 * the original node is used as value of the copied node.
 *
 * The original list both on success or error is never modified. */
/* 复制一个链表，返回新链表的指针，时间复杂度 O(N) */
list *listDup(list *orig)
{
    list *copy;
    listIter iter;
    listNode *node;

    if ((copy = listCreate()) == NULL)
        return NULL;
    /* 初始化相关的复制/释放/比较函数 */
    copy->dup = orig->dup;
    copy->free = orig->free;
    copy->match = orig->match;
    /* 从链表头开始遍历，复制节点 */
    listRewind(orig, &iter);
    while((node = listNext(&iter)) != NULL) {
        void *value;

        if (copy->dup) {
            /* 如果链表有定义自己的复制函数，调用它复制链表节点 */
            value = copy->dup(node->value);
            if (value == NULL) {
                /* 如果节点复制失败，释放新链表并返回 NULL */
                listRelease(copy);
                return NULL;
            }
        } else {
            /* 否则单纯进行 value 的引用复制 */
            value = node->value;
        }

        /* 将节点插入到新链表的尾部 */
        if (listAddNodeTail(copy, value) == NULL) {
            /* Free value if dup succeed but listAddNodeTail failed. */
            /* 如果添加节点失败并且链表有定义自己的 free 函数，通过它释放 value */
            if (copy->free) copy->free(value);
            /* 释放新链表并且返回 NULL */
            listRelease(copy);
            return NULL;
        }
    }
    /* 返回复制的新链表 */
    return copy;
}

/* Search the list for a node matching a given key.
 * The match is performed using the 'match' method
 * set with listSetMatchMethod(). If no 'match' method
 * is set, the 'value' pointer of every node is directly
 * compared with the 'key' pointer.
 *
 * On success the first matching node pointer is returned
 * (search starts from head). If no matching node exists
 * NULL is returned. */

/* 根据节点 key 在链表中搜索对应的节点，时间复杂度 O(N) */
listNode *listSearchKey(list *list, void *key)
{
    listIter iter;
    listNode *node;

    /* 从链表头开始遍历 */
    listRewind(list, &iter);
    while((node = listNext(&iter)) != NULL) {
        if (list->match) {
            /* 如果链表有定义自己的 match 函数，调用它来进行比较 */
            if (list->match(node->value, key)) {
                return node;
            }
        } else {
            /* 否则就单纯比较 key 和 node->value */
            if (key == node->value) {
                return node;
            }
        }
    }
    return NULL;
}

/* Return the element at the specified zero-based index
 * where 0 is the head, 1 is the element next to head
 * and so on. Negative integers are used in order to count
 * from the tail, -1 is the last element, -2 the penultimate
 * and so on. If the index is out of range NULL is returned. */
/* 通过下标返回链表节点，时间复杂度 O(N)
 * 支持传入负下标，例如 -1 为最后一个节点，-2 为倒数第二个节点 */
listNode *listIndex(list *list, long index) {
    listNode *n;

    if (index < 0) {
        /* 支持传入负下标，从尾节点开始向前遍历
         * 例如获取倒数第二个节点，index = -2, (-index)-1 = 1
         * 从尾节点开始跳过一个节点就到达目标节点了 */
        index = (-index)-1;
        n = list->tail;
        while(index-- && n) n = n->prev;
    } else {
        /* 正下标，从链表头开始向后遍历 */
        n = list->head;
        while(index-- && n) n = n->next;
    }
    return n;
}

/* Rotate the list removing the tail node and inserting it to the head. */
/* 将尾节点移动到链表头部，时间复杂度 O(1) */
void listRotateTailToHead(list *list) {
    if (listLength(list) <= 1) return;

    /* Detach current tail */
    /* 将尾节点从链表中摘出来，维护链表指针
     * 链表新尾节点为 tail 的前驱节点
     * 链表新尾节点的后继节点为空 */
    listNode *tail = list->tail;
    list->tail = tail->prev;
    list->tail->next = NULL;

    /* Move it as head */
    /* 将链表老尾节点移动到链表头部，此时它为新头节点
     * 链表老头节点的前驱节点为新头节点
     * 链表新头节点的前驱节点为空，后继节点为原先的老头节点 */
    list->head->prev = tail;
    tail->prev = NULL;
    tail->next = list->head;

    /* 链表的新头节点为老尾节点 */
    list->head = tail;
}

/* Rotate the list removing the head node and inserting it to the tail. */
/* 将头节点移动到链表尾部，时间复杂度 O(1)
 * 这个处理过程同上，维护好相关指针 */
void listRotateHeadToTail(list *list) {
    if (listLength(list) <= 1) return;

    listNode *head = list->head;
    /* Detach current head */
    list->head = head->next;
    list->head->prev = NULL;
    /* Move it as tail */
    list->tail->next = head;
    head->next = NULL;
    head->prev = list->tail;
    list->tail = head;
}

/* Add all the elements of the list 'o' at the end of the
 * list 'l'. The list 'other' remains empty but otherwise valid. */
/* 连接两个链表，将 o 链表连到 l 链表的尾部 */
void listJoin(list *l, list *o) {
    /* 如果 o 链表为空直接返回，不处理 */
    if (o->len == 0) return;

    /* o 链表头节点的前驱节点为 l 链表的尾节点 */
    o->head->prev = l->tail;

    /* 如果 l 链表不为空，l 链表尾节点的后继节点为 o 链表的头节点
     * 如果 l 链表为空，l 链表的头节点直接就有 o 链表的头节点 */
    if (l->tail)
        l->tail->next = o->head;
    else
        l->head = o->head;

    /* 连接 o 链表，此时 l 链表的尾节点直接置为 o 链表的尾节点 */
    l->tail = o->tail;
    /* l 链表的长度直接再加上 o 链表的长度 */
    l->len += o->len;

    /* Setup other as an empty list. */
    /* 将 o 链表的 head / tail / len 元数据置空 */
    o->head = o->tail = NULL;
    o->len = 0;
}
