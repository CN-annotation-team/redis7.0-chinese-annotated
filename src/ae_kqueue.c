/* Kqueue(2)-based ae.c module
 *
 * Copyright (C) 2009 Harish Mallipeddi - harish.mallipeddi@gmail.com
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

/* kqueue 是一个可扩展的事件通知接口，在 OS X、FreeBSD 中均有其实现，在这里作为 redis polling api 的实现方式之一
 * 同 evport 和 epoll，事件(描述符)提取的算法复杂度为 O(1)，支持超过 1024 个文件描述符
 *
 * kqueue api 在 redis 下的使用场景:
 *
 * int kqueue(void);
 *   创建一个新的内核事件队列，返回一个描述符 (kqfd)
 *
 * int kevent(int kq, const struct kevent *changelist, int nchanges, struct kevent *eventlist, int nevents, const struct timespec *timeout);
 *   1. 注册/删除新的可读/可写事件
 *   2. 获取已触发事件，需要注意的是，对同一个 fd 的不同事件 (比如读和写)，kqueue 实现会分多个不同的就绪事件给到
 *
 * EV_SET(_kev, ident, filter, flags, fflags, data, udata);
 *   初始化 struct kevent 对象的宏
 *
 * 以上仅是 redis 使用到的功能，相关 api 的详细介绍见:
 * https://www.freebsd.org/cgi/man.cgi?query=kevent&apropos=0&sektion=0&manpath=FreeBSD+6.1-RELEASE&format=html
 */


#include <sys/types.h>
#include <sys/event.h>
#include <sys/time.h>

/* kqueue 的私有数据
 *     kqfd: 系统调用 kqueue() 分配的 kqueue 队列文件描述符，后续都基于这个 kqfd 来 注册/注销 事件监听
 *     events: kevent() 系统调用返回的已触发事件，这里是数组首元素的指针，数组会初始化 setsize 个位置
 *     eventsMasks: char 数组，其 size 和 数组 events 的长度相关
 *         数组 events 中的每个 event，都有可读/可写两个独立状态，一个掩码需要 2bits 来存储
 *         因此一个 8bits 的 char 字符可以存储 4 个 event 的掩码
 *         两个数组的长度关系为 len(eventsMasks) = (len(events) + 3) / 4，见宏定义: EVENT_MASK_MALLOC_SIZE
 */
typedef struct aeApiState {
    int kqfd;
    struct kevent *events;

    /* Events mask for merge read and write event.
     * To reduce memory consumption, we use 2 bits to store the mask
     * of an event, so that 1 byte will store the mask of 4 events. */
    char *eventsMask; 
} aeApiState;

/* EVENT_MASK_MALLOC_SIZE: 用于根据数组 aeApiState.events 的长度计算掩码数组 aeApiState.eventsMask 的长度
 * EVENT_MASK_OFFSET: 计算 2 bits 掩码在 8 bits char 字符中的位置
 * EVENT_MASK_ENCODE: 将掩码调整到 8 bits char 中应该在的位置 */
#define EVENT_MASK_MALLOC_SIZE(sz) (((sz) + 3) / 4)
#define EVENT_MASK_OFFSET(fd) ((fd) % 4 * 2)
#define EVENT_MASK_ENCODE(fd, mask) (((mask) & 0x3) << EVENT_MASK_OFFSET(fd))

/* 从 aeApiState.eventsMask 中获取 指定 fd 的 2 bits 掩码
 * 计算过程：先获取掩码所在的 char 字符，然后将其移位到最后两位 */
static inline int getEventMask(const char *eventsMask, int fd) {
    return (eventsMask[fd/4] >> EVENT_MASK_OFFSET(fd)) & 0x3;
}

/* 保存指定 fd 的掩码 */
static inline void addEventMask(char *eventsMask, int fd, int mask) {
    eventsMask[fd/4] |= EVENT_MASK_ENCODE(fd, mask);
}

/* 复位 fd 对应的掩码，将掩码的 2 bits 设置为 0 */
static inline void resetEventMask(char *eventsMask, int fd) {
    eventsMask[fd/4] &= ~EVENT_MASK_ENCODE(fd, 0x3);
}

/* 创建 kqueue 多路复用库的私有数据
 *     1. 创建 kqueue event 数组 (state->events)，掩码数组 (state->eventsMask)
 *     2. 创建 kqueue 队列，得到文件描述符 kqfd
 * 最后将 kqueue 的 aeApiState 私有数据保存在全局变量 aeEventLoop.apidata 中
 */
static int aeApiCreate(aeEventLoop *eventLoop) {
    aeApiState *state = zmalloc(sizeof(aeApiState));

    if (!state) return -1;
    state->events = zmalloc(sizeof(struct kevent)*eventLoop->setsize);
    if (!state->events) {
        zfree(state);
        return -1;
    }
    state->kqfd = kqueue();
    if (state->kqfd == -1) {
        zfree(state->events);
        zfree(state);
        return -1;
    }
    /* 避免 fd 泄漏，设置 kqueue fd 运行时可关闭 */
    anetCloexec(state->kqfd);
    state->eventsMask = zmalloc(EVENT_MASK_MALLOC_SIZE(eventLoop->setsize));
    /* 掩码数组的比特位都初始化为 0 */
    memset(state->eventsMask, 0, EVENT_MASK_MALLOC_SIZE(eventLoop->setsize));
    eventLoop->apidata = state;
    return 0;
}

/* 按照给定的 setsieze，重新分配 kqueue event 数组和相对应掩码数组的大小 */
static int aeApiResize(aeEventLoop *eventLoop, int setsize) {
    aeApiState *state = eventLoop->apidata;

    state->events = zrealloc(state->events, sizeof(struct kevent)*setsize);
    state->eventsMask = zrealloc(state->eventsMask, EVENT_MASK_MALLOC_SIZE(setsize));
    memset(state->eventsMask, 0, EVENT_MASK_MALLOC_SIZE(setsize));
    return 0;
}

/* 释放 kqueue 私有数据占据的内存 */
static void aeApiFree(aeEventLoop *eventLoop) {
    aeApiState *state = eventLoop->apidata;

    close(state->kqfd);
    zfree(state->events);
    zfree(state->eventsMask);
    zfree(state);
}

/* 注册可读/可写事件 (主要是 kqueue fd 和 io 相关 fd 的绑定)
 * 参数用途介绍:
 *   eventLoop: 主要用于获取系统调用 kqueue() 创建的 kqfd (eventLoop->apidata->kqfd)
 *   fd: 客户端连接生成的 socket_fd 或者 其他途径生成的 pipe_fd
 *   mask: 标记监听事件类型，读事件(AE_READABLE) or 写事件(AE_WRITABLE) */
static int aeApiAddEvent(aeEventLoop *eventLoop, int fd, int mask) {
    aeApiState *state = eventLoop->apidata;
    struct kevent ke;

    if (mask & AE_READABLE) {
        EV_SET(&ke, fd, EVFILT_READ, EV_ADD, 0, 0, NULL);
        if (kevent(state->kqfd, &ke, 1, NULL, 0, NULL) == -1) return -1;
    }
    if (mask & AE_WRITABLE) {
        EV_SET(&ke, fd, EVFILT_WRITE, EV_ADD, 0, 0, NULL);
        if (kevent(state->kqfd, &ke, 1, NULL, 0, NULL) == -1) return -1;
    }
    return 0;
}

/* 删除事件, 将宏定义 EV_SET 的 EV_ADD 参数替换为 EV_DELETE, 其他同 aeApiAddEvent 函数 */
static void aeApiDelEvent(aeEventLoop *eventLoop, int fd, int mask) {
    aeApiState *state = eventLoop->apidata;
    struct kevent ke;

    if (mask & AE_READABLE) {
        EV_SET(&ke, fd, EVFILT_READ, EV_DELETE, 0, 0, NULL);
        kevent(state->kqfd, &ke, 1, NULL, 0, NULL);
    }
    if (mask & AE_WRITABLE) {
        EV_SET(&ke, fd, EVFILT_WRITE, EV_DELETE, 0, 0, NULL);
        kevent(state->kqfd, &ke, 1, NULL, 0, NULL);
    }
}

/* 获取就绪事件, 并将 kqueue 事件数据转化为 redis 事件数据
 * return: 就绪事件涉及到的 fd 数量 */
static int aeApiPoll(aeEventLoop *eventLoop, struct timeval *tvp) {
    aeApiState *state = eventLoop->apidata;
    int retval, numevents = 0;

    if (tvp != NULL) {
        /* 设置阻塞(超时)时间, 若该值为 NULL, 则无限期阻塞, 直到有事件触发 */
        struct timespec timeout;
        timeout.tv_sec = tvp->tv_sec;
        timeout.tv_nsec = tvp->tv_usec * 1000;
        /* 至多获取 setsize 个就绪事件，将就绪事件放在 kqueue 私有的数组 events 中，返回获取的就绪事件数量；若无就绪事件，超时返回 0 */
        retval = kevent(state->kqfd, NULL, 0, state->events, eventLoop->setsize,
                        &timeout);
    } else {
        retval = kevent(state->kqfd, NULL, 0, state->events, eventLoop->setsize,
                        NULL);
    }

    if (retval > 0) {
        int j;

        /* Normally we execute the read event first and then the write event.
         * When the barrier is set, we will do it reverse.
         * 
         * However, under kqueue, read and write events would be separate
         * events, which would make it impossible to control the order of
         * reads and writes. So we store the event's mask we've got and merge
         * the same fd events later. */
        /* 第一次遍历就绪事件，目的：kqueue 同一个 fd 的可读事件和可写事件，kqueue 中会用两个就绪事件来表示，这里的目的是将事件的状态合并
        * 例如将 kqueue 返回的 fd 可读和 fd 可写事件，合并表示成一个 fd 可读可写，不然的话无法控制 kqueue 中事件读写的顺序 */
        for (j = 0; j < retval; j++) {
            struct kevent *e = state->events+j;
            int fd = e->ident;
            int mask = 0; 

            if (e->filter == EVFILT_READ) mask = AE_READABLE;
            else if (e->filter == EVFILT_WRITE) mask = AE_WRITABLE;
            addEventMask(state->eventsMask, fd, mask);
        }

        /* Re-traversal to merge read and write events, and set the fd's mask to
         * 0 so that events are not added again when the fd is encountered again. */
        /* fd 掩码合并后的第二次遍历，numevents 表示存在就绪事件的 fd 的数量，而不是就绪事件的数量 */
        numevents = 0;
        for (j = 0; j < retval; j++) {
            struct kevent *e = state->events+j;
            /* 之前和 kqfd 绑定的描述符 */
            int fd = e->ident;
            /* 获取可读/可写状态掩码 */
            int mask = getEventMask(state->eventsMask, fd);

            /* 1. 若为可读/可写，将 kqueue 私有数据转化为 eventLoop 数据
             * 2. 复位私有的掩码值:
             *      若一个 fd 有多个就绪事件，后续事件在遍历的时候，mask == 0，将不会走下面逻辑，保证了 numevents 对同 fd 的多个事件，只会计数一次. */
            if (mask) {
                eventLoop->fired[numevents].fd = fd;
                eventLoop->fired[numevents].mask = mask;
                resetEventMask(state->eventsMask, fd);
                numevents++;
            }
        }
    } else if (retval == -1 && errno != EINTR) {
        panic("aeApiPoll: kevent, %s", strerror(errno));
    }

    return numevents;
}

/* 返回该 polling api 实现的名字 */
static char *aeApiName(void) {
    return "kqueue";
}
