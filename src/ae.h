/* A simple event-driven programming library. Originally I wrote this code
 * for the Jim's event-loop (Jim is a Tcl interpreter) but later translated
 * it in form of a library for easy reuse.
 *
 * Copyright (c) 2006-2012, Salvatore Sanfilippo <antirez at gmail dot com>
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

#ifndef __AE_H__
#define __AE_H__

#include "monotonic.h"
/* 时间事件执行状态 */
/* 成功 */
#define AE_OK 0
/* 失败 */
#define AE_ERR -1

/* 文件事件状态 */
/* 未设置 */
#define AE_NONE 0       /* No events registered. */
/* 可读 */
#define AE_READABLE 1   /* Fire when descriptor is readable. */
/* 可写 */
#define AE_WRITABLE 2   /* Fire when descriptor is writable. */
/* 逆序读写事件，一般情况下先读后写，这里是为了处理先写后读的情况 */
#define AE_BARRIER 4    /* With WRITABLE, never fire the event if the
                           READABLE event already fired in the same event
                           loop iteration. Useful when you want to persist
                           things to disk before sending replies, and want
                           to do that in a group fashion. */
/* 事件处理器的执行标识 */
/* 文件事件 */
#define AE_FILE_EVENTS (1<<0)
/* 时间事件 */
#define AE_TIME_EVENTS (1<<1)
/* 所有事件 */
#define AE_ALL_EVENTS (AE_FILE_EVENTS|AE_TIME_EVENTS)
/* 不阻塞，也不进行等待 */
#define AE_DONT_WAIT (1<<2)
/* 等待 socket 就绪之前调用 */
#define AE_CALL_BEFORE_SLEEP (1<<3)
/* 等待 socket 就绪之后调用 */
#define AE_CALL_AFTER_SLEEP (1<<4)

/* 决定时间事件是否要持续执行的标识 */
#define AE_NOMORE -1
#define AE_DELETED_EVENT_ID -1

/* Macros */
#define AE_NOTUSED(V) ((void) V)

/* 事件循环处理器 */
struct aeEventLoop;

/* Types and data structures */
typedef void aeFileProc(struct aeEventLoop *eventLoop, int fd, void *clientData, int mask);
typedef int aeTimeProc(struct aeEventLoop *eventLoop, long long id, void *clientData);
typedef void aeEventFinalizerProc(struct aeEventLoop *eventLoop, void *clientData);
typedef void aeBeforeSleepProc(struct aeEventLoop *eventLoop);

/* File event structure */
typedef struct aeFileEvent {
    /* 监听事件类型的掩码，可以是 AE_(READABLE|WRITABLE|BARRIER) */
    int mask; /* one of AE_(READABLE|WRITABLE|BARRIER) */
    /* 读事件处理器 */
    aeFileProc *rfileProc;
    /* 写事件处理器 */
    aeFileProc *wfileProc;
    /* socket 连接信息 */
    void *clientData;
} aeFileEvent;

/* Time event structure */
typedef struct aeTimeEvent {
    /* 时间事件 id */
    long long id; /* time event identifier. */
    /* 事件的到达时间 */
    monotime when;
    /* 时间处理函数 */
    aeTimeProc *timeProc;
    /* 事件释放函数 */
    aeEventFinalizerProc *finalizerProc;
    /* 该事件持有的数据 */
    void *clientData;
    /* 指向上个时间事件结构，形成链表 */
    struct aeTimeEvent *prev;
    /* 指向下个时间事件结构 */
    struct aeTimeEvent *next;
    /* 引用计数 */
    int refcount; /* refcount to prevent timer events from being
  		   * freed in recursive time event calls. */
} aeTimeEvent;

/* A fired event */
/* 已就绪事件 */
typedef struct aeFiredEvent {
    /* 已就绪文件描述符 */
    int fd;
    /* 事件类型掩码，文件事件的三个类型 */
    int mask;
} aeFiredEvent;

/* State of an event based program */
/* 事件处理器的状态 */
typedef struct aeEventLoop {
    /* 目前已经注册的最大文件描述符 */
    int maxfd;   /* highest file descriptor currently registered */
    /* 目前追踪的最大文件描述符数量，redis 初始化的时候会设置好的一个固定值
     * 初始化事件循环器的时候会把这个值赋值给 setsize，见 server.h 文件的 CONFIG_FDSET_INCR
     * server.maxclients + RESERVED_FDS（32） + 96 */
    int setsize; /* max number of file descriptors tracked */
    /* 用于生成时间事件的 ID */
    long long timeEventNextId;
    /* 已注册的文件事件，在初始化的时候会初始化 setsize 个位置 */
    aeFileEvent *events; /* Registered events */
    /* 已就绪的文件事件 */
    aeFiredEvent *fired; /* Fired events */
    /* 时间事件链表的头节点 */
    aeTimeEvent *timeEventHead;
    /* 事件处理器的开关 */
    int stop;
    /* 多路复用库的私有数据 */
    void *apidata; /* This is used for polling API specific data */
    /* 在处理事件前要执行的函数 */
    aeBeforeSleepProc *beforesleep;
    /* 在处理事件后要执行的函数 */
    aeBeforeSleepProc *aftersleep;
    int flags;
} aeEventLoop;

/* Prototypes */
/* 创建 aeEventLoop，内部的 fileEvent 和就绪的事件个数是 setsize */
aeEventLoop *aeCreateEventLoop(int setsize);
/* 删除 eventLoop */
void aeDeleteEventLoop(aeEventLoop *eventLoop);
/* 设置 eventLoop 中的停止属性置为 1 */
void aeStop(aeEventLoop *eventLoop);
/* 在 eventLoop 中创建文件事件 */
int aeCreateFileEvent(aeEventLoop *eventLoop, int fd, int mask,
        aeFileProc *proc, void *clientData);
/* 删除文件事件 */
void aeDeleteFileEvent(aeEventLoop *eventLoop, int fd, int mask);
/* 根据文件描述符 ID ，找出文件的属性是读事件还是写事件 */
int aeGetFileEvents(aeEventLoop *eventLoop, int fd);
/* 获取多路复用私有数据 */
void *aeGetFileClientData(aeEventLoop *eventLoop, int fd);
/* 在 eventLoop 中添加时间事件，创建的时间为当前时间加上自己传入的时间 */
long long aeCreateTimeEvent(aeEventLoop *eventLoop, long long milliseconds,
        aeTimeProc *proc, void *clientData,
        aeEventFinalizerProc *finalizerProc);
/* 根据 id 删除时间事件 */
int aeDeleteTimeEvent(aeEventLoop *eventLoop, long long id);
/* 处理 eventLoop 中的所有类型事件 */
int aeProcessEvents(aeEventLoop *eventLoop, int flags);
/* 让一个事件等待多少毫秒 */
int aeWait(int fd, int mask, long long milliseconds);
/* ae 事件的主程序 */
void aeMain(aeEventLoop *eventLoop);
char *aeGetApiName(void);
/* 下面两个是设置处理器前后的钩子函数 */
void aeSetBeforeSleepProc(aeEventLoop *eventLoop, aeBeforeSleepProc *beforesleep);
void aeSetAfterSleepProc(aeEventLoop *eventLoop, aeBeforeSleepProc *aftersleep);
/* 下面两个函数是获取和重新设置 eventLoop 的 setsize */
int aeGetSetSize(aeEventLoop *eventLoop);
int aeResizeSetSize(aeEventLoop *eventLoop, int setsize);
/* 设置事件不阻塞 */
void aeSetDontWait(aeEventLoop *eventLoop, int noWait);

#endif
