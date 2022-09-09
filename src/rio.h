/*
 * Copyright (c) 2009-2012, Pieter Noordhuis <pcnoordhuis at gmail dot com>
 * Copyright (c) 2009-2019, Salvatore Sanfilippo <antirez at gmail dot com>
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


#ifndef __REDIS_RIO_H
#define __REDIS_RIO_H

#include <stdio.h>
#include <stdint.h>
#include "sds.h"
#include "connection.h"

/* 读错误标识 */
#define RIO_FLAG_READ_ERROR (1<<0)
/* 写错误标识 */
#define RIO_FLAG_WRITE_ERROR (1<<1)

/* redis 提供统一的 IO 操作接口，在 rio.c 中封装了各种 io 的区别
 * redis 中分为四种 IO 操作
 * 1. file (文件的读写)
 * 2. buffer (直接使用 SDS 字符串)
 * 3. connection (目前只有 RDB 将主节点的rdb文件加载到从服务器内存使用了这种 IO ,只提供读操作)
 * 4. fd (文件描述符)
 */
#define RIO_TYPE_FILE (1<<0)
#define RIO_TYPE_BUFFER (1<<1)
#define RIO_TYPE_CONN (1<<2)
#define RIO_TYPE_FD (1<<3)

struct _rio {
    /* Backend functions.
     * Since this functions do not tolerate short writes or reads the return
     * value is simplified to: zero on error, non zero on complete success. */
    /* 数据流读方法 */
    size_t (*read)(struct _rio *, void *buf, size_t len);
    /* 数据流写方法 */
    size_t (*write)(struct _rio *, const void *buf, size_t len);
    /* 获取当前的读写偏移量 */
    off_t (*tell)(struct _rio *);
    /* 把数据流刷到文件 */
    int (*flush)(struct _rio *);
    /* The update_cksum method if not NULL is used to compute the checksum of
     * all the data that was read or written so far. The method should be
     * designed so that can be called with the current checksum, and the buf
     * and len fields pointing to the new block of data to add to the checksum
     * computation. */
    /* 当读或者写新的数据块的时候，更新校验和 */
    void (*update_cksum)(struct _rio *, const void *buf, size_t len);

    /* The current checksum and flags (see RIO_FLAG_*) */
    /* 当前校验和以及读写错误标识 */
    uint64_t cksum, flags;

    /* number of bytes read or written */
    /* 当前读取或者写入的字节大小 */
    size_t processed_bytes;

    /* maximum single read or write chunk size */
    /* 单次读写数据的上限 */
    size_t max_processing_chunk;

    /* Backend-specific vars. */
    /* 对应 rio 的四种 IO 操作 */
    union {
        /* In-memory buffer target. */
        /* buffer 结构体 */
        struct {
            /* 具体内容 */
            sds ptr;
            /* 读取或者写入数据的偏移量 */
            off_t pos;
        } buffer;
        /* Stdio file pointer target. */
        /* 标准文件 结构体 */
        struct {
            /* 文件指针 */
            FILE *fp;
            /* 从上一次调用 fsync (该系统函数可以把文件相关的所有脏数据回写到磁盘) 之后写入的字节 */
            off_t buffered; /* Bytes written since last fsync. */
            /* 这个字段是一个阈值，file 的写是增量进行的（分批次），autosync 可以看做每批次可以写的数据量 */
            off_t autosync; /* fsync after 'autosync' bytes written. */
        } file;
        /* Connection object (used to read from socket) */
        /* Connection 结构体，用于从 socket 中读数据 */
        struct {
            /* 对应的 socket 连接 */
            connection *conn;   /* Connection */
            /* 返回的缓冲区中的偏移量 */
            off_t pos;    /* pos in buf that was returned */
            /* 缓冲区数据 */
            sds buf;      /* buffered data */
            /* 可以读取数据量的上限 */
            size_t read_limit;  /* don't allow to buffer/read more than that */
            /* 已经读取的数据量 */
            size_t read_so_far; /* amount of data read from the rio (not buffered) */
        } conn;
        /* FD target (used to write to pipe). */
        /* fd 结构体 */
        struct {
            int fd;       /* File descriptor. */
            off_t pos;
            sds buf;
        } fd;
    } io;
};

typedef struct _rio rio;

/* The following functions are our interface with the stream. They'll call the
 * actual implementation of read / write / tell, and will update the checksum
 * if needed. */
/* 这里抽取了 rio 的 read / write /tell 等操作的公共逻辑，会根据不同 rio 类型执行具体的函数 */

/* rio 写函数 */
static inline size_t rioWrite(rio *r, const void *buf, size_t len) {
    /* 如果 rio 存在写错误标识，直接返回 */
    if (r->flags & RIO_FLAG_WRITE_ERROR) return 0;
    while (len) {
        /* 判断当前写操作字节长度是否超过了最大长度（ len ），如果超过就设置要写的字节数为 len */
        size_t bytes_to_write = (r->max_processing_chunk && r->max_processing_chunk < len) ? r->max_processing_chunk : len;
        /* 更新校验和 */
        if (r->update_cksum) r->update_cksum(r,buf,bytes_to_write);
        /* 调用具体 rio 类型的 write 方法 */
        if (r->write(r,buf,bytes_to_write) == 0) {
            /* 如果上面 write 返回0，出错了，设置错误标识，不可再写 */
            r->flags |= RIO_FLAG_WRITE_ERROR;
            return 0;
        }
        /* buf 增加 */
        buf = (char*)buf + bytes_to_write;
        /* 可用长度减少 */
        len -= bytes_to_write;
        r->processed_bytes += bytes_to_write;
    }
    return 1;
}

/* rio 读操作，具体逻辑和写操作差不多 */
static inline size_t rioRead(rio *r, void *buf, size_t len) {
    /* 检错 */
    if (r->flags & RIO_FLAG_READ_ERROR) return 0;
    while (len) {
        /* 获取读字节数，不能超过 len */
        size_t bytes_to_read = (r->max_processing_chunk && r->max_processing_chunk < len) ? r->max_processing_chunk : len;
        if (r->read(r,buf,bytes_to_read) == 0) {
            r->flags |= RIO_FLAG_READ_ERROR;
            return 0;
        }
        if (r->update_cksum) r->update_cksum(r,buf,bytes_to_read);
        buf = (char*)buf + bytes_to_read;
        len -= bytes_to_read;
        r->processed_bytes += bytes_to_read;
    }
    return 1;
}

/* tell 操作 */
static inline off_t rioTell(rio *r) {
    return r->tell(r);
}

/* flush 操作 */
static inline int rioFlush(rio *r) {
    return r->flush(r);
}

/* This function allows to know if there was a read error in any past
 * operation, since the rio stream was created or since the last call
 * to rioClearError(). */
/* 判断一个 rio 是否出现的读错误 */
static inline int rioGetReadError(rio *r) {
    return (r->flags & RIO_FLAG_READ_ERROR) != 0;
}

/* Like rioGetReadError() but for write errors. */
/* 判断一个 rio 是否出现了写错误 */
static inline int rioGetWriteError(rio *r) {
    return (r->flags & RIO_FLAG_WRITE_ERROR) != 0;
}

/* 清除错误标识 */
static inline void rioClearErrors(rio *r) {
    r->flags &= ~(RIO_FLAG_READ_ERROR|RIO_FLAG_WRITE_ERROR);
}

void rioInitWithFile(rio *r, FILE *fp);
void rioInitWithBuffer(rio *r, sds s);
void rioInitWithConn(rio *r, connection *conn, size_t read_limit);
void rioInitWithFd(rio *r, int fd);

void rioFreeFd(rio *r);
void rioFreeConn(rio *r, sds* out_remainingBufferedData);

size_t rioWriteBulkCount(rio *r, char prefix, long count);
size_t rioWriteBulkString(rio *r, const char *buf, size_t len);
size_t rioWriteBulkLongLong(rio *r, long long l);
size_t rioWriteBulkDouble(rio *r, double d);

struct redisObject;
int rioWriteBulkObject(rio *r, struct redisObject *obj);

void rioGenericUpdateChecksum(rio *r, const void *buf, size_t len);
void rioSetAutoSync(rio *r, off_t bytes);
uint8_t rioCheckType(rio *r);
#endif
