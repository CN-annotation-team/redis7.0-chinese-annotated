/* Background I/O service for Redis.
 *
 * This file implements operations that we need to perform in the background.
 * Currently there is only a single operation, that is a background close(2)
 * system call. This is needed as when the process is the last owner of a
 * reference to a file closing it means unlinking it, and the deletion of the
 * file is slow, blocking the server.
 *
 * In the future we'll either continue implementing new things we need or
 * we'll switch to libeio. However there are probably long term uses for this
 * file as we may want to put here Redis specific background tasks (for instance
 * it is not impossible that we'll need a non blocking FLUSHDB/FLUSHALL
 * implementation).
 *
 * DESIGN
 * ------
 *
 * The design is trivial, we have a structure representing a job to perform
 * and a different thread and job queue for every job type.
 * Every thread waits for new jobs in its queue, and process every job
 * sequentially.
 *
 * Jobs of the same type are guaranteed to be processed from the least
 * recently inserted to the most recently inserted (older jobs processed
 * first).
 *
 * Currently there is no way for the creator of the job to be notified about
 * the completion of the operation, this will only be added when/if needed.
 *
 * ----------------------------------------------------------------------------
 *
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


#include "server.h"
#include "bio.h"

/* redis BIO(background IO)
 * redis 后台 I/O 线程功能实现，做一些异步操作：
 * 1) BIO_AOF_FSYNC：AOF 会用后台线程做 AOF 文件 fsync 刷盘操作
 * 2) BIO_LAZY_FREE：LazyFree 功能会评估释放一个数据结构实例需要的代价（大概就是对于集合类型，1 个元素
 *    代价加 1，而字符串类型代价为 1），代价大于 LAZYFREE_THRESHOLD（64） 就后台线程删除
 * 3) BIO_CLOSE_FILE：异步关闭文件描述符，例如 AOF 历史文件关闭（AOF 会用变量记录历史文件路径，交给后台执行关闭操作） */

/* BIO_NUM_OPS 是 3，见 bio.h */
/* 线程数组 */
static pthread_t bio_threads[BIO_NUM_OPS];
/* 互斥锁数组 */
static pthread_mutex_t bio_mutex[BIO_NUM_OPS];
static pthread_cond_t bio_newjob_cond[BIO_NUM_OPS];
/* 其他线程等待后台任务执行完毕的 condition */
static pthread_cond_t bio_step_cond[BIO_NUM_OPS];
/* 保存每个类型线程消费的任务列表（如果列表为空，这个线程则什么都不做） */
static list *bio_jobs[BIO_NUM_OPS];
/* The following array is used to hold the number of pending jobs for every
 * OP type. This allows us to export the bioPendingJobsOfType() API that is
 * useful when the main thread wants to perform some operation that may involve
 * objects shared with the background thread. The main thread will just wait
 * that there are no longer jobs of this type to be executed before performing
 * the sensible operation. This data is also useful for reporting. */
/* 这个数组用来保存每个操作类型线程待执行的任务的数量，主要用于当主线程想要执行一些操作
 * 并且这些操作涉及到的对象被后台线程共享了，主线程会等待这种类型的待执行任务数为 0，再执行操作 */
static unsigned long long bio_pending[BIO_NUM_OPS];

/* This structure represents a background Job. It is only used locally to this
 * file as the API does not expose the internals at all. */
/* 后台 IO 结构体，仅仅在本地使用，相关的 API 不会被暴露出去 */
struct bio_job {
    /* Job specific arguments.*/
    /* 后台任务持有的文件描述符 */
    int fd; /* Fd for file based background jobs */
    /* 懒释放函数，释放在 free_args 中存储的对象 */
    lazy_free_fn *free_fn; /* Function that will free the provided arguments */
    /* 要被释放的对象参数 */
    void *free_args[]; /* List of arguments to be passed to the free function */
};

void *bioProcessBackgroundJobs(void *arg);

/* Make sure we have enough stack to perform all the things we do in the
 * main thread. */
#define REDIS_THREAD_STACK_SIZE (1024*1024*4)

/* Initialize the background system, spawning the thread. */
/* 初始化后台系统，将线程和其需要的变量初始化 */
void bioInit(void) {
    pthread_attr_t attr;
    pthread_t thread;
    size_t stacksize;
    int j;

    /* Initialization of state vars and objects */
    for (j = 0; j < BIO_NUM_OPS; j++) {
        /* 初始化互斥锁 */
        pthread_mutex_init(&bio_mutex[j],NULL);
        pthread_cond_init(&bio_newjob_cond[j],NULL);
        pthread_cond_init(&bio_step_cond[j],NULL);
        /* 初始化任务列表 */
        bio_jobs[j] = listCreate();
        bio_pending[j] = 0;
    }

    /* Set the stack size as by default it may be small in some system */
    /* 设置线程栈大小 */
    pthread_attr_init(&attr);
    pthread_attr_getstacksize(&attr,&stacksize);
    if (!stacksize) stacksize = 1; /* The world is full of Solaris Fixes */
    while (stacksize < REDIS_THREAD_STACK_SIZE) stacksize *= 2;
    pthread_attr_setstacksize(&attr, stacksize);

    /* Ready to spawn our threads. We use the single argument the thread
     * function accepts in order to pass the job ID the thread is
     * responsible of. */
    for (j = 0; j < BIO_NUM_OPS; j++) {
        void *arg = (void*)(unsigned long) j;
        /* 创建线程，绑定执行任务函数 */
        if (pthread_create(&thread,&attr,bioProcessBackgroundJobs,arg) != 0) {
            serverLog(LL_WARNING,"Fatal: Can't initialize Background Jobs.");
            exit(1);
        }
        /* 加入数组 */
        bio_threads[j] = thread;
    }
}

/* 提交任务，将任务放入对应类型操作线程的任务列表 */
void bioSubmitJob(int type, struct bio_job *job) {
    pthread_mutex_lock(&bio_mutex[type]);
    listAddNodeTail(bio_jobs[type],job);
    bio_pending[type]++;
    pthread_cond_signal(&bio_newjob_cond[type]);
    pthread_mutex_unlock(&bio_mutex[type]);
}

/* 创建一个 lazy free 任务
 * free_fn 释放空间的函数
 * arg_count 要释放的对象数量
 * ... 要释放的对象
 *
 * 主要步骤就是创建 bio_job 结构体，然后填充 free_fn 和 arg_count
 * 最后把 bio_job 实例提交到任务列表
 */
void bioCreateLazyFreeJob(lazy_free_fn free_fn, int arg_count, ...) {
    va_list valist;
    /* Allocate memory for the job structure and all required
     * arguments */
    struct bio_job *job = zmalloc(sizeof(*job) + sizeof(void *) * (arg_count));
    job->free_fn = free_fn;

    va_start(valist, arg_count);
    for (int i = 0; i < arg_count; i++) {
        job->free_args[i] = va_arg(valist, void *);
    }
    va_end(valist);
    bioSubmitJob(BIO_LAZY_FREE, job);
}

/* 创建 close file 任务，填充 fd 属性，提交任务 */
void bioCreateCloseJob(int fd) {
    struct bio_job *job = zmalloc(sizeof(*job));
    job->fd = fd;

    bioSubmitJob(BIO_CLOSE_FILE, job);
}

/* 创建 AOF file fsync 任务，填充 fd 属性，提交任务 */
void bioCreateFsyncJob(int fd) {
    struct bio_job *job = zmalloc(sizeof(*job));
    job->fd = fd;

    bioSubmitJob(BIO_AOF_FSYNC, job);
}

/* 后台 IO 线程执行任务函数 */
void *bioProcessBackgroundJobs(void *arg) {
    struct bio_job *job;
    unsigned long type = (unsigned long) arg;
    sigset_t sigset;

    /* Check that the type is within the right interval. */
    /* 类型校验 */
    if (type >= BIO_NUM_OPS) {
        serverLog(LL_WARNING,
            "Warning: bio thread started with wrong type %lu",type);
        return NULL;
    }
    /* 不同的类型设置不同的线程名 */
    switch (type) {
    case BIO_CLOSE_FILE:
        redis_set_thread_title("bio_close_file");
        break;
    case BIO_AOF_FSYNC:
        redis_set_thread_title("bio_aof_fsync");
        break;
    case BIO_LAZY_FREE:
        redis_set_thread_title("bio_lazy_free");
        break;
    }

    /* 设置 cpu 亲和度，线程绑定具体 cpu 核，和主线程分开了，具体可以看 redis.conf 2248-2271 行 */
    redisSetCpuAffinity(server.bio_cpulist);

    makeThreadKillable();
    /* 加锁 */
    pthread_mutex_lock(&bio_mutex[type]);
    /* Block SIGALRM so we are sure that only the main thread will
     * receive the watchdog signal. */
    /* 保证只有主线程会接收到信号 */
    sigemptyset(&sigset);
    sigaddset(&sigset, SIGALRM);
    if (pthread_sigmask(SIG_BLOCK, &sigset, NULL))
        serverLog(LL_WARNING,
            "Warning: can't mask SIGALRM in bio.c thread: %s", strerror(errno));

    while(1) {
        listNode *ln;

        /* The loop always starts with the lock hold. */
        /* 如果任务列表没有任务 */
        if (listLength(bio_jobs[type]) == 0) {
            /* pthread_cond_wait 会释放之前获取到的锁，在其他线程执行 pthread_cond_signal 时会重新获取锁 */
            pthread_cond_wait(&bio_newjob_cond[type],&bio_mutex[type]);
            continue;
        }
        /* Pop the job from the queue. */
        /* 获取任务列表首个任务 */
        ln = listFirst(bio_jobs[type]);
        job = ln->value;
        /* It is now possible to unlock the background system as we know have
         * a stand alone job structure to process.*/
        /* 释放锁 */
        pthread_mutex_unlock(&bio_mutex[type]);

        /* Process the job accordingly to its type. */
        /* 不同的操作类型执行具体的操作 */
        if (type == BIO_CLOSE_FILE) {
            /* 关闭文件 */
            close(job->fd);
        } else if (type == BIO_AOF_FSYNC) {
            /* The fd may be closed by main thread and reused for another
             * socket, pipe, or file. We just ignore these errno because
             * aof fsync did not really fail. */
            /* 做 fsync，会忽略错误，原子的设置 AOF 的 FSYNC 状态信息 */
            if (redis_fsync(job->fd) == -1 &&
                errno != EBADF && errno != EINVAL)
            {
                int last_status;
                atomicGet(server.aof_bio_fsync_status,last_status);
                atomicSet(server.aof_bio_fsync_status,C_ERR);
                atomicSet(server.aof_bio_fsync_errno,errno);
                if (last_status == C_OK) {
                    serverLog(LL_WARNING,
                        "Fail to fsync the AOF file: %s",strerror(errno));
                }
            } else {
                atomicSet(server.aof_bio_fsync_status,C_OK);
            }
        } else if (type == BIO_LAZY_FREE) {
            /* 调用惰性释放函数释放对象 */
            job->free_fn(job->free_args);
        } else {
            serverPanic("Wrong job type in bioProcessBackgroundJobs().");
        }
        /* 释放 job */
        zfree(job);

        /* Lock again before reiterating the loop, if there are no longer
         * jobs to process we'll block again in pthread_cond_wait(). */
        /* 加锁，删除任务列表中的对应任务节点 */
        pthread_mutex_lock(&bio_mutex[type]);
        listDelNode(bio_jobs[type],ln);
        /* 待执行任务数 -1 */
        bio_pending[type]--;

        /* Unblock threads blocked on bioWaitStepOfType() if any. */
        /* 唤醒 bioWaitStepOfType 的阻塞 */
        pthread_cond_broadcast(&bio_step_cond[type]);
    }
}

/* Return the number of pending jobs of the specified type. */
/* 返回指定任务类型的待处理任务数量，作用可以看 bio_pending 的注释 */
unsigned long long bioPendingJobsOfType(int type) {
    unsigned long long val;
    pthread_mutex_lock(&bio_mutex[type]);
    val = bio_pending[type];
    pthread_mutex_unlock(&bio_mutex[type]);
    return val;
}

/* If there are pending jobs for the specified type, the function blocks
 * and waits that the next job was processed. Otherwise the function
 * does not block and returns ASAP.
 *
 * The function returns the number of jobs still to process of the
 * requested type.
 *
 * This function is useful when from another thread, we want to wait
 * a bio.c thread to do more work in a blocking way.
 */
/* 该函数主要是为了给其他线程调用，判断给定操作类型的线程任务是否还有，还有就阻塞，等待
 * 这种类型的任务处理完，再执行后续逻辑 */
unsigned long long bioWaitStepOfType(int type) {
    unsigned long long val;
    pthread_mutex_lock(&bio_mutex[type]);
    val = bio_pending[type];
    if (val != 0) {
        pthread_cond_wait(&bio_step_cond[type],&bio_mutex[type]);
        val = bio_pending[type];
    }
    pthread_mutex_unlock(&bio_mutex[type]);
    return val;
}

/* Kill the running bio threads in an unclean way. This function should be
 * used only when it's critical to stop the threads for some reason.
 * Currently Redis does this only on crash (for instance on SIGSEGV) in order
 * to perform a fast memory check without other threads messing with memory. */
/* redis 运行出错了，需要关闭当前后台线程 */
void bioKillThreads(void) {
    int err, j;

    for (j = 0; j < BIO_NUM_OPS; j++) {
        if (bio_threads[j] == pthread_self()) continue;
        if (bio_threads[j] && pthread_cancel(bio_threads[j]) == 0) {
            if ((err = pthread_join(bio_threads[j],NULL)) != 0) {
                serverLog(LL_WARNING,
                    "Bio thread for job type #%d can not be joined: %s",
                        j, strerror(err));
            } else {
                serverLog(LL_WARNING,
                    "Bio thread for job type #%d terminated",j);
            }
        }
    }
}
