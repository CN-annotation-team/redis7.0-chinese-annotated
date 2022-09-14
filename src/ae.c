/* A simple event-driven programming library. Originally I wrote this code
 * for the Jim's event-loop (Jim is a Tcl interpreter) but later translated
 * it in form of a library for easy reuse.
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

#include "ae.h"
#include "anet.h"
#include "redisassert.h"

#include <stdio.h>
#include <sys/time.h>
#include <sys/types.h>
#include <unistd.h>
#include <stdlib.h>
#include <poll.h>
#include <string.h>
#include <time.h>
#include <errno.h>

#include "zmalloc.h"
#include "config.h"

/* Include the best multiplexing layer supported by this system.
 * The following should be ordered by performances, descending. */
/* 这里会根据当前 处理器架构 和 性能 来选择一种多路复用器，性能排序 evport > epoll > kqueue > select */
#ifdef HAVE_EVPORT
#include "ae_evport.c"
#else
    #ifdef HAVE_EPOLL
    #include "ae_epoll.c"
    #else
        #ifdef HAVE_KQUEUE
        #include "ae_kqueue.c"
        #else
        #include "ae_select.c"
        #endif
    #endif
#endif

/* 初始化事件循环处理器，该函数在 server.c 的 initServer 函数中被调用，setsize是个固定值 */
aeEventLoop *aeCreateEventLoop(int setsize) {
    aeEventLoop *eventLoop;
    int i;

    monotonicInit();    /* just in case the calling app didn't initialize */
    /* 空间分配和属性初始化 */
    if ((eventLoop = zmalloc(sizeof(*eventLoop))) == NULL) goto err;
    eventLoop->events = zmalloc(sizeof(aeFileEvent)*setsize);
    eventLoop->fired = zmalloc(sizeof(aeFiredEvent)*setsize);
    if (eventLoop->events == NULL || eventLoop->fired == NULL) goto err;
    eventLoop->setsize = setsize;
    eventLoop->timeEventHead = NULL;
    eventLoop->timeEventNextId = 0;
    eventLoop->stop = 0;
    eventLoop->maxfd = -1;
    eventLoop->beforesleep = NULL;
    eventLoop->aftersleep = NULL;
    eventLoop->flags = 0;
    /* 这里会调用对应 ae_[select/epoll/evport/kqueue] 文件的 aeApiCreate 函数 */
    if (aeApiCreate(eventLoop) == -1) goto err;
    /* Events with mask == AE_NONE are not set. So let's initialize the
     * vector with it. */
    /* 这里是根据 setsize 大小创建了 setsize 个事件位置
     * 记住 events 有 setsize 个位置，后面每添加一个 event ，其实就是在对应的位置（下标就是 fd ）上做处理*/
    for (i = 0; i < setsize; i++)
        eventLoop->events[i].mask = AE_NONE;
    return eventLoop;

err:
    if (eventLoop) {
        zfree(eventLoop->events);
        zfree(eventLoop->fired);
        zfree(eventLoop);
    }
    return NULL;
}

/* Return the current set size. */
/* 返回当前事件循环中文件描述符的数量 */
int aeGetSetSize(aeEventLoop *eventLoop) {
    return eventLoop->setsize;
}

/* Tells the next iteration/s of the event processing to set timeout of 0. */
/* 根据noWait设置事件处理的阻塞标识 */
void aeSetDontWait(aeEventLoop *eventLoop, int noWait) {
    if (noWait)
        eventLoop->flags |= AE_DONT_WAIT;
    else
        eventLoop->flags &= ~AE_DONT_WAIT;
}

/* Resize the maximum set size of the event loop.
 * If the requested set size is smaller than the current set size, but
 * there is already a file descriptor in use that is >= the requested
 * set size minus one, AE_ERR is returned and the operation is not
 * performed at all.
 *
 * Otherwise AE_OK is returned and the operation is successful. */
/* 重新设置 eventloop 的 setsize */
int aeResizeSetSize(aeEventLoop *eventLoop, int setsize) {
    int i;
    /* 如果给定的 setsize 和以前的一样，不做处理 */
    if (setsize == eventLoop->setsize) return AE_OK;
    /* 现在已有的最大的 fd 大于等于 setsize ( fd 就是 events 的下标，setsize 是 events 数组大小)，报错 */
    if (eventLoop->maxfd >= setsize) return AE_ERR;
    if (aeApiResize(eventLoop,setsize) == -1) return AE_ERR;

    /* 重新分配了 events 和 fired 空间大小*/
    eventLoop->events = zrealloc(eventLoop->events,sizeof(aeFileEvent)*setsize);
    eventLoop->fired = zrealloc(eventLoop->fired,sizeof(aeFiredEvent)*setsize);
    /* 重新设置 setsize */
    eventLoop->setsize = setsize;

    /* Make sure that if we created new slots, they are initialized with
     * an AE_NONE mask. */
    for (i = eventLoop->maxfd+1; i < setsize; i++)
        eventLoop->events[i].mask = AE_NONE;
    return AE_OK;
}

/* 删除一个事件循环器 */
void aeDeleteEventLoop(aeEventLoop *eventLoop) {
    aeApiFree(eventLoop);
    zfree(eventLoop->events);
    zfree(eventLoop->fired);

    /* Free the time events list. */
    aeTimeEvent *next_te, *te = eventLoop->timeEventHead;
    while (te) {
        next_te = te->next;
        zfree(te);
        te = next_te;
    }
    zfree(eventLoop);
}

/* 将事件循环器的停止标识置为 1 */
void aeStop(aeEventLoop *eventLoop) {
    eventLoop->stop = 1;
}

/* 创建一个文件事件 */
int aeCreateFileEvent(aeEventLoop *eventLoop, int fd, int mask,
        aeFileProc *proc, void *clientData)
{
    /* 给定的文件描述符超过了 redis 能追踪的最大文件描述符限制，报错 */
    if (fd >= eventLoop->setsize) {
        errno = ERANGE;
        return AE_ERR;
    }
    /* 获取 events 数组中 fd 位置的指针 */
    aeFileEvent *fe = &eventLoop->events[fd];
    /* 添加对应多路复用的数据 */
    if (aeApiAddEvent(eventLoop, fd, mask) == -1)
        return AE_ERR;
    fe->mask |= mask;
    /* 设置读写函数 */
    if (mask & AE_READABLE) fe->rfileProc = proc;
    if (mask & AE_WRITABLE) fe->wfileProc = proc;
    fe->clientData = clientData;
    /* 修改 maxfd */
    if (fd > eventLoop->maxfd)
        eventLoop->maxfd = fd;
    return AE_OK;
}

/* 删除文件事件 */
void aeDeleteFileEvent(aeEventLoop *eventLoop, int fd, int mask)
{
    if (fd >= eventLoop->setsize) return;
    /* 找到要删除的 event 在 events 中的指针 */
    aeFileEvent *fe = &eventLoop->events[fd];
    if (fe->mask == AE_NONE) return;

    /* We want to always remove AE_BARRIER if set when AE_WRITABLE
     * is removed. */
    if (mask & AE_WRITABLE) mask |= AE_BARRIER;
    /* 删除事件的私有数据 */
    aeApiDelEvent(eventLoop, fd, mask);
    fe->mask = fe->mask & (~mask);
    if (fd == eventLoop->maxfd && fe->mask == AE_NONE) {
        /* Update the max fd */
        int j;

        for (j = eventLoop->maxfd-1; j >= 0; j--)
            if (eventLoop->events[j].mask != AE_NONE) break;
        eventLoop->maxfd = j;
    }
}

/* 获取对应 fd 位置的事件的 socket 连接数据 */
void *aeGetFileClientData(aeEventLoop *eventLoop, int fd) {
    if (fd >= eventLoop->setsize) return NULL;
    aeFileEvent *fe = &eventLoop->events[fd];
    if (fe->mask == AE_NONE) return NULL;

    return fe->clientData;
}

/* 获取 fd 位置的文件事件 */
int aeGetFileEvents(aeEventLoop *eventLoop, int fd) {
    if (fd >= eventLoop->setsize) return 0;
    aeFileEvent *fe = &eventLoop->events[fd];

    return fe->mask;
}

/* 创建时间事件，不同于文件事件，时间事件使用链表存储，用 id 进行唯一标识 */
long long aeCreateTimeEvent(aeEventLoop *eventLoop, long long milliseconds,
        aeTimeProc *proc, void *clientData,
        aeEventFinalizerProc *finalizerProc)
{
    long long id = eventLoop->timeEventNextId++;
    /* 初始化一个 aeTimeEvent 结构实例 */
    aeTimeEvent *te;

    te = zmalloc(sizeof(*te));
    if (te == NULL) return AE_ERR;
    te->id = id;
    te->when = getMonotonicUs() + milliseconds * 1000;
    te->timeProc = proc;
    te->finalizerProc = finalizerProc;
    te->clientData = clientData;
    te->prev = NULL;
    te->next = eventLoop->timeEventHead;
    te->refcount = 0;
    if (te->next)
        te->next->prev = te;
    /* 将新的 timeEvent 添加到头结点 */
    eventLoop->timeEventHead = te;
    return id;
}

/* 根据 id 删除一个时间事件 */
int aeDeleteTimeEvent(aeEventLoop *eventLoop, long long id)
{
    aeTimeEvent *te = eventLoop->timeEventHead;
    /* 从头节点遍历，删除给定 id 的事件 */
    while(te) {
        if (te->id == id) {
            te->id = AE_DELETED_EVENT_ID;
            return AE_OK;
        }
        te = te->next;
    }
    return AE_ERR; /* NO event with the specified ID found */
}

/* How many microseconds until the first timer should fire.
 * If there are no timers, -1 is returned.
 *
 * Note that's O(N) since time events are unsorted.
 * Possible optimizations (not needed by Redis so far, but...):
 * 1) Insert the event in order, so that the nearest is just the head.
 *    Much better but still insertion or deletion of timers is O(N).
 * 2) Use a skiplist to have this operation as O(1) and insertion as O(log(N)).
 */
/* 获取当前时间之后第一个要执行的事件的时间间隔，用微妙表示
 * 由于时间事件是无序的链表，所以该方法的时间复杂度是 O(N)
 * 可以优化，但是redis目前不需要，要是优化的话可以如下优化：
 * 1) 有序的插入事件，保证最近的一个事件总处于头节点，但是插入和删除的时间复杂度依旧是 O(N)
 * 2) 使用跳表，则可优化到 O(1) ,插入 O(log(N))*/
static int64_t usUntilEarliestTimer(aeEventLoop *eventLoop) {
    aeTimeEvent *te = eventLoop->timeEventHead;
    if (te == NULL) return -1;

    aeTimeEvent *earliest = NULL;
    /* 遍历事件链表，找到 when 最小的事件  */
    while (te) {
        if (!earliest || te->when < earliest->when)
            earliest = te;
        te = te->next;
    }

    monotime now = getMonotonicUs();
    /* 返回 when - now 即时间间隔 */
    return (now >= earliest->when) ? 0 : earliest->when - now;
}

/* Process time events */
/* 处理时间事件 */
static int processTimeEvents(aeEventLoop *eventLoop) {
    int processed = 0;
    aeTimeEvent *te;
    long long maxId;

    te = eventLoop->timeEventHead;
    maxId = eventLoop->timeEventNextId-1;
    monotime now = getMonotonicUs();
    while(te) {
        long long id;

        /* Remove events scheduled for deletion. */
        /* 如果事件的 id 为 AE_DELETED_EVENT_ID (-1, 该事件被标记为删除) */
        if (te->id == AE_DELETED_EVENT_ID) {
            aeTimeEvent *next = te->next;
            /* If a reference exists for this timer event,
             * don't free it. This is currently incremented
             * for recursive timerProc calls */
            /* 如果事件存在引用，就处理下一个事件 */
            if (te->refcount) {
                te = next;
                continue;
            }
            /* 执行到这里表示事件没有引用，可以直接删除了 */
            /* 下面是节点在双向链表中的删除逻辑 */
            if (te->prev)
                te->prev->next = te->next;
            else
                eventLoop->timeEventHead = te->next;
            if (te->next)
                te->next->prev = te->prev;
            if (te->finalizerProc) {
                te->finalizerProc(eventLoop, te->clientData);
                now = getMonotonicUs();
            }
            /* 释放空间 */
            zfree(te);
            te = next;
            continue;
        }

        /* Make sure we don't process time events created by time events in
         * this iteration. Note that this check is currently useless: we always
         * add new timers on the head, however if we change the implementation
         * detail, this check may be useful again: we keep it here for future
         * defense. */
        /* 这里是确保在当前遍历过程中加入进来的事件不会被执行，正常情况下不会出现这种情况，
         * 主要是避免以后可能会进行修改 */
        if (te->id > maxId) {
            te = te->next;
            continue;
        }
        /* 只要是任务执行的时间小于当前时刻，走下面的逻辑 */
        if (te->when <= now) {
            int retval;

            id = te->id;
            te->refcount++;
            /* 执行实际提供的事件处理回调函数 */
            retval = te->timeProc(eventLoop, id, te->clientData);
            /* 引用-1 */
            te->refcount--;
            /* 被处理的事件+1 */
            processed++;
            now = getMonotonicUs();
            /* 判断返回的值是否是 AE_NOMORE （该事件是否还要继续执行） */
            if (retval != AE_NOMORE) {
                /* 还要继续执行，返回的值就是还要执行的毫秒，when 是到微秒级别，所以要 * 1000 */
                te->when = now + retval * 1000;
            } else {
                /* 不需要就标记为删除，下一次遍历会释放掉 */
                te->id = AE_DELETED_EVENT_ID;
            }
        }
        te = te->next;
    }
    return processed;
}

/* Process every pending time event, then every pending file event
 * (that may be registered by time event callbacks just processed).
 * Without special flags the function sleeps until some file event
 * fires, or when the next time event occurs (if any).
 *
 * If flags is 0, the function does nothing and returns.
 * if flags has AE_ALL_EVENTS set, all the kind of events are processed.
 * if flags has AE_FILE_EVENTS set, file events are processed.
 * if flags has AE_TIME_EVENTS set, time events are processed.
 * if flags has AE_DONT_WAIT set, the function returns ASAP once all
 * the events that can be handled without a wait are processed.
 * if flags has AE_CALL_AFTER_SLEEP set, the aftersleep callback is called.
 * if flags has AE_CALL_BEFORE_SLEEP set, the beforesleep callback is called.
 *
 * The function returns the number of events processed. */
/* 处理事件，通过 flags 来指定处理事件类型，具体类型可见 ae.h 56-68行
 * 这里直接看 aeMain 函数调用该函数时提供的 flags
 * AE_ALL_EVENTS(AE_FILE_EVENTS|AE_TIME_EVENTS)|AE_CALL_BEFORE_SLEEP|AE_CALL_AFTER_SLEEP*/
int aeProcessEvents(aeEventLoop *eventLoop, int flags)
{
    int processed = 0, numevents;

    /* Nothing to do? return ASAP */
    /* 既不是文件事件也不是时间事件直接返回 */
    if (!(flags & AE_TIME_EVENTS) && !(flags & AE_FILE_EVENTS)) return 0;

    /* Note that we want to call select() even if there are no
     * file events to process as long as we want to process time
     * events, in order to sleep until the next time event is ready
     * to fire. */
    /* 判断最大描述符不为-1，标识有文件事件
     * 或者指定可以处理时间事件，且没有指定AE_DONT_WAIT */
    if (eventLoop->maxfd != -1 ||
        ((flags & AE_TIME_EVENTS) && !(flags & AE_DONT_WAIT))) {
        int j;
        /* 定制超时时间 */
        struct timeval tv, *tvp;
        int64_t usUntilTimer = -1;
        /* 有 AE_TIME_EVENTS，没有 AE_DONT_WAIT */
        if (flags & AE_TIME_EVENTS && !(flags & AE_DONT_WAIT))
            /* 计算之后一次事件的处理时间间隔，单位为微秒 */
            usUntilTimer = usUntilEarliestTimer(eventLoop);
        /* 上面计算的值大于0，标识在非 AE_DONT_WAIT 情况下有任务  */
        if (usUntilTimer >= 0) {
            /* 设置超时时间 */
            /* 秒级 */
            tv.tv_sec = usUntilTimer / 1000000;
            /* 微秒级 */
            tv.tv_usec = usUntilTimer % 1000000;
            tvp = &tv;
        } else {
            /* If we have to check for events but need to return
             * ASAP because of AE_DONT_WAIT we need to set the timeout
             * to zero */
            /* 如果是指定了 AE_DONT_WAIT 超时时间置0，（不阻塞直接执行） */
            if (flags & AE_DONT_WAIT) {
                tv.tv_sec = tv.tv_usec = 0;
                tvp = &tv;
            } else {
                /* Otherwise we can block */
                tvp = NULL; /* wait forever */
            }
        }
        /* 如果是指定了 AE_DONT_WAIT 超时时间置0，（不阻塞直接执行） */
        if (eventLoop->flags & AE_DONT_WAIT) {
            tv.tv_sec = tv.tv_usec = 0;
            tvp = &tv;
        }
        /* 前置处理 */
        if (eventLoop->beforesleep != NULL && flags & AE_CALL_BEFORE_SLEEP)
            eventLoop->beforesleep(eventLoop);

        /* Call the multiplexing API, will return only on timeout or when
         * some event fires. */
        /* 调用具体的多路复用 api ,返回就绪事件数量 */
        numevents = aeApiPoll(eventLoop, tvp);

        /* After sleep callback. */
        /* 后置处理 */
        if (eventLoop->aftersleep != NULL && flags & AE_CALL_AFTER_SLEEP)
            eventLoop->aftersleep(eventLoop);
        /* 遍历所有就绪事件 */
        for (j = 0; j < numevents; j++) {
            int fd = eventLoop->fired[j].fd;
            /* 获取指定事件 */
            aeFileEvent *fe = &eventLoop->events[fd];
            int mask = eventLoop->fired[j].mask;
            int fired = 0; /* Number of events fired for current fd. */

            /* Normally we execute the readable event first, and the writable
             * event later. This is useful as sometimes we may be able
             * to serve the reply of a query immediately after processing the
             * query.
             *
             * However if AE_BARRIER is set in the mask, our application is
             * asking us to do the reverse: never fire the writable event
             * after the readable. In such a case, we invert the calls.
             * This is useful when, for instance, we want to do things
             * in the beforeSleep() hook, like fsyncing a file to disk,
             * before replying to a client. */
            /* 正常情况下，我们先执行可读事件，再执行可写事件。因为有时候我们可以在处理查询之后
             * 立即对这个查询进行回复。
             *
             * 如果设置了 AE_BARRIER 掩码，
             * 在一些情况下，应用可能要求我们不要再执行可读事件之后执行可写事件，
             * 在这种情况下，我们需要反转调用，例如我们需要在回复客户端之前在
             * beforeSleep 钩子函数中同步文件到磁盘，*/
            int invert = fe->mask & AE_BARRIER;

            /* Note the "fe->mask & mask & ..." code: maybe an already
             * processed event removed an element that fired and we still
             * didn't processed, so we check if the event is still valid.
             *
             * Fire the readable event if the call sequence is not
             * inverted. */
            /* 如果无需逆转读写且可读，执行读取回调函数，
             * fe->mask 是注册事件的掩码， mask 是本次就绪事件的掩码，可能存在之前的读写事件没有
             * 处理的情况，所以这里进行检查，一并处理 */
            if (!invert && fe->mask & mask & AE_READABLE) {
                fe->rfileProc(eventLoop,fd,fe->clientData,mask);
                fired++;
                fe = &eventLoop->events[fd]; /* Refresh in case of resize. */
            }

            /* Fire the writable event. */
            /* 如果可写，执行写入回调函数 */
            if (fe->mask & mask & AE_WRITABLE) {
                if (!fired || fe->wfileProc != fe->rfileProc) {
                    fe->wfileProc(eventLoop,fd,fe->clientData,mask);
                    fired++;
                }
            }

            /* If we have to invert the call, fire the readable event now
             * after the writable one. */
            /* 如果需要逆转读写操作，在这里执行读取逻辑 */
            if (invert) {
                fe = &eventLoop->events[fd]; /* Refresh in case of resize. */
                if ((fe->mask & mask & AE_READABLE) &&
                    (!fired || fe->wfileProc != fe->rfileProc))
                {
                    fe->rfileProc(eventLoop,fd,fe->clientData,mask);
                    fired++;
                }
            }

            processed++;
        }
    }
    /* Check time events */
    /* 如果是时间事件，执行时间事件处理函数 */
    if (flags & AE_TIME_EVENTS)
        processed += processTimeEvents(eventLoop);

    return processed; /* return the number of processed file/time events */
}

/* Wait for milliseconds until the given file descriptor becomes
 * writable/readable/exception */
/* 等待 milliseconds 毫秒，知道 fd 有事件触发 */
int aeWait(int fd, int mask, long long milliseconds) {
    struct pollfd pfd;
    int retmask = 0, retval;

    memset(&pfd, 0, sizeof(pfd));
    pfd.fd = fd;
    if (mask & AE_READABLE) pfd.events |= POLLIN;
    if (mask & AE_WRITABLE) pfd.events |= POLLOUT;

    if ((retval = poll(&pfd, 1, milliseconds))== 1) {
        if (pfd.revents & POLLIN) retmask |= AE_READABLE;
        if (pfd.revents & POLLOUT) retmask |= AE_WRITABLE;
        if (pfd.revents & POLLERR) retmask |= AE_WRITABLE;
        if (pfd.revents & POLLHUP) retmask |= AE_WRITABLE;
        return retmask;
    } else {
        return retval;
    }
}

/*
 * 事件处理的入口方法
 * @param eventLoop
 */
void aeMain(aeEventLoop *eventLoop) {
    // 设置循环处理器的停止标识为0，标识可以处理事件
    eventLoop->stop = 0;
    // stop为0，开始循环处理事件
    while (!eventLoop->stop) {
        // 开始处理事件，设置了标识
        // AE_ALL_EVENTS是文件事件和时间事件的合集
        // 下面的标识指定可以执行文件事件，事件事件，事件处理前后的钩子函数
        aeProcessEvents(eventLoop, AE_ALL_EVENTS|
                                   AE_CALL_BEFORE_SLEEP|
                                   AE_CALL_AFTER_SLEEP);
    }
}

/* 获取对应多路复用库名 */
char *aeGetApiName(void) {
    return aeApiName();
}
/* 设置前置处理函数 */
void aeSetBeforeSleepProc(aeEventLoop *eventLoop, aeBeforeSleepProc *beforesleep) {
    eventLoop->beforesleep = beforesleep;
}
/* 设置后置处理函数 */
void aeSetAfterSleepProc(aeEventLoop *eventLoop, aeBeforeSleepProc *aftersleep) {
    eventLoop->aftersleep = aftersleep;
}
