/* rio.c is a simple stream-oriented I/O abstraction that provides an interface
 * to write code that can consume/produce data using different concrete input
 * and output devices. For instance the same rdb.c code using the rio
 * abstraction can be used to read and write the RDB format using in-memory
 * buffers or files.
 *
 * A rio object provides the following methods:
 *  read: read from stream.
 *  write: write to stream.
 *  tell: get the current offset.
 *
 * It is also possible to set a 'checksum' method that is used by rio.c in order
 * to compute a checksum of the data written or read, or to query the rio object
 * for the current checksum.
 *
 * ----------------------------------------------------------------------------
 *
 * Copyright (c) 2009-2012, Pieter Noordhuis <pcnoordhuis at gmail dot com>
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


#include "fmacros.h"
#include <string.h>
#include <stdio.h>
#include <unistd.h>
#include "rio.h"
#include "util.h"
#include "crc64.h"
#include "config.h"
#include "server.h"

/* ------------------------- Buffer I/O implementation ----------------------- */
/* buffer RIO 的功能 */

/* Returns 1 or 0 for success/failure. */
/* buffer 类型的 RIO 写入操作，就是 sds 字符串操作 */
static size_t rioBufferWrite(rio *r, const void *buf, size_t len) {
    /* 将 buf 写入 rio 的 buffer 缓冲区中 */
    r->io.buffer.ptr = sdscatlen(r->io.buffer.ptr,(char*)buf,len);
    /* 偏移量增加 */
    r->io.buffer.pos += len;
    return 1;
}

/* Returns 1 or 0 for success/failure. */
/* buffer RIO 读取操作，和写逻辑类似 */
static size_t rioBufferRead(rio *r, void *buf, size_t len) {
    if (sdslen(r->io.buffer.ptr)-r->io.buffer.pos < len)
        return 0; /* not enough buffer to return len bytes. */
    memcpy(buf,r->io.buffer.ptr+r->io.buffer.pos,len);
    r->io.buffer.pos += len;
    return 1;
}

/* Returns read/write position in buffer. */
/* 获取 buffer RIO 的读写偏移量 */
static off_t rioBufferTell(rio *r) {
    return r->io.buffer.pos;
}

/* Flushes any buffer to target device if applicable. Returns 1 on success
 * and 0 on failures. */
/* buffer 存在于内存中，没有 flush ,不做任何操作 */
static int rioBufferFlush(rio *r) {
    UNUSED(r);
    return 1; /* Nothing to do, our write just appends to the buffer. */
}

/* 定义了一个 rio 结构的实体 */
static const rio rioBufferIO = {
    /* 四个方法 */
    rioBufferRead,
    rioBufferWrite,
    rioBufferTell,
    rioBufferFlush,
    /* buffer 无需校验 */
    NULL,           /* update_checksum */
    0,              /* current checksum */
    0,              /* flags */
    0,              /* bytes read or written */
    0,              /* read/write chunk size */
    { { NULL, 0 } } /* union for io-specific vars */
};

/* 初始化 buffer rio */
void rioInitWithBuffer(rio *r, sds s) {
    *r = rioBufferIO;
    r->io.buffer.ptr = s;
    r->io.buffer.pos = 0;
}

/* --------------------- Stdio file pointer implementation ------------------- */
/* file rio 功能 */

/* Returns 1 or 0 for success/failure. */
/* file rio 写数据 */
static size_t rioFileWrite(rio *r, const void *buf, size_t len) {
    if (!r->io.file.autosync) return fwrite(buf,len,1,r->io.file.fp);

    /* 记录已经写入的数据的大小 */
    size_t nwritten = 0;
    /* Incrementally write data to the file, avoid a single write larger than
     * the autosync threshold (so that the kernel's buffer cache never has too
     * many dirty pages at once). */
    /* 增量写数据到文件中，避免单次写的数据量大于 autosync 的阈值
     * 注： 为了避免内核缓冲区一次出现太多的脏页 */
    while (len != nwritten) {
        serverAssert(r->io.file.autosync > r->io.file.buffered);
        /* 计算 autosync 阈值（可以看做一个中转空间的大小，如果达到这个值表示中转空间放满了，需要直接刷到磁盘，清空中转空间）还剩下多少 */
        size_t nalign = (size_t)(r->io.file.autosync - r->io.file.buffered);
        /* 计算要写入文件的字节数，判断规则
         * 如果 len - nwritten (即 buf 中剩下要写的字节数) < nalign (即 autosync 剩下的字节数)
         * 这种的情况表示 autosync 阈值剩下的空间可以存放下 buf 中要写入的字节数，直接一次写入，否则只能
         * 写入 autosync 剩下空间的字节数*/
        size_t towrite = nalign > len-nwritten ? len-nwritten : nalign;

        /* 将被给的 buf 的 nwritten(该位置之前的数据是已经写入文件的数据) ~ towrite(要写入的数据量) 之间的数据写入文件 */
        if (fwrite((char*)buf+nwritten,towrite,1,r->io.file.fp) == 0) return 0;
        /* 已写入文件的数据量增加加 */
        nwritten += towrite;
        /* 从上次 fsync 到现在写入的字节数大小增加 */
        r->io.file.buffered += towrite;

        /* 如果从上次 fsync 之后写入的字节数达到了 autosync 阈值，则自动将文件缓冲区刷到磁盘 */
        if (r->io.file.buffered >= r->io.file.autosync) {
            fflush(r->io.file.fp);

            size_t processed = r->processed_bytes + nwritten;
            serverAssert(processed % r->io.file.autosync == 0);
            serverAssert(r->io.file.buffered == r->io.file.autosync);

#if HAVE_SYNC_FILE_RANGE
            /* Start writeout asynchronously. */
            if (sync_file_range(fileno(r->io.file.fp),
                    processed - r->io.file.autosync, r->io.file.autosync,
                    SYNC_FILE_RANGE_WRITE) == -1)
                return 0;

            if (processed >= (size_t)r->io.file.autosync * 2) {
                /* To keep the promise to 'autosync', we should make sure last
                 * asynchronous writeout persists into disk. This call may block
                 * if last writeout is not finished since disk is slow. */
                if (sync_file_range(fileno(r->io.file.fp),
                        processed - r->io.file.autosync*2,
                        r->io.file.autosync, SYNC_FILE_RANGE_WAIT_BEFORE|
                        SYNC_FILE_RANGE_WRITE|SYNC_FILE_RANGE_WAIT_AFTER) == -1)
                    return 0;
            }
#else
            if (redis_fsync(fileno(r->io.file.fp)) == -1) return 0;
#endif
            r->io.file.buffered = 0;
        }
    }
    return 1;
}

/* Returns 1 or 0 for success/failure. */
/* 将文件读到 buf 中 */
static size_t rioFileRead(rio *r, void *buf, size_t len) {
    return fread(buf,len,1,r->io.file.fp);
}

/* Returns read/write position in file. */
/* 文件大小 */
static off_t rioFileTell(rio *r) {
    return ftello(r->io.file.fp);
}

/* Flushes any buffer to target device if applicable. Returns 1 on success
 * and 0 on failures. */
/* flush 刷盘操作 */
static int rioFileFlush(rio *r) {
    return (fflush(r->io.file.fp) == 0) ? 1 : 0;
}

static const rio rioFileIO = {
    /* file rio 的四个操作函数*/
    rioFileRead,
    rioFileWrite,
    rioFileTell,
    rioFileFlush,
    /* 文件读取，无需校验 */
    NULL,           /* update_checksum */
    0,              /* current checksum */
    0,              /* flags */
    0,              /* bytes read or written */
    0,              /* read/write chunk size */
    { { NULL, 0 } } /* union for io-specific vars */
};

/* 初始化一个 file rio */
void rioInitWithFile(rio *r, FILE *fp) {
    *r = rioFileIO;
    r->io.file.fp = fp;
    r->io.file.buffered = 0;
    r->io.file.autosync = 0;
}

/* ------------------- Connection implementation -------------------
 * We use this RIO implementation when reading an RDB file directly from
 * the connection to the memory via rdbLoadRio(), thus this implementation
 * only implements reading from a connection that is, normally,
 * just a socket. */
/* connection rio 功能 */
/* connection rio 只提供读操作，无写错误，不做事 */
static size_t rioConnWrite(rio *r, const void *buf, size_t len) {
    UNUSED(r);
    UNUSED(buf);
    UNUSED(len);
    return 0; /* Error, this target does not yet support writing. */
}

/* Returns 1 or 0 for success/failure. */
/* connection rio 读操作 */
static size_t rioConnRead(rio *r, void *buf, size_t len) {
    /* 获取 connection 结构体中可读的数据的起始位置 */
    size_t avail = sdslen(r->io.conn.buf)-r->io.conn.pos;

    /* If the buffer is too small for the entire request: realloc. */
    if (sdslen(r->io.conn.buf) + sdsavail(r->io.conn.buf) < len)
        /* 将 connection 的 buf 空间调整到 len 大小 */
        r->io.conn.buf = sdsMakeRoomFor(r->io.conn.buf, len - sdslen(r->io.conn.buf));

    /* If the remaining unused buffer is not large enough: memmove so that we
     * can read the rest. */
    if (len > avail && sdsavail(r->io.conn.buf) < len - avail) {
        sdsrange(r->io.conn.buf, r->io.conn.pos, -1);
        r->io.conn.pos = 0;
    }

    /* Make sure the caller didn't request to read past the limit.
     * If they didn't we'll buffer till the limit, if they did, we'll
     * return an error. */
    if (r->io.conn.read_limit != 0 && r->io.conn.read_limit < r->io.conn.read_so_far + len) {
        errno = EOVERFLOW;
        return 0;
    }

    /* If we don't already have all the data in the sds, read more */
    while (len > sdslen(r->io.conn.buf) - r->io.conn.pos) {
        size_t buffered = sdslen(r->io.conn.buf) - r->io.conn.pos;
        size_t needs = len - buffered;
        /* Read either what's missing, or PROTO_IOBUF_LEN, the bigger of
         * the two. */
        size_t toread = needs < PROTO_IOBUF_LEN ? PROTO_IOBUF_LEN: needs;
        if (toread > sdsavail(r->io.conn.buf)) toread = sdsavail(r->io.conn.buf);
        if (r->io.conn.read_limit != 0 &&
            r->io.conn.read_so_far + buffered + toread > r->io.conn.read_limit)
        {
            toread = r->io.conn.read_limit - r->io.conn.read_so_far - buffered;
        }
        /* 这里调用了 connection 的 read 方法 */
        int retval = connRead(r->io.conn.conn,
                          (char*)r->io.conn.buf + sdslen(r->io.conn.buf),
                          toread);
        if (retval == 0) {
            return 0;
        } else if (retval < 0) {
            if (connLastErrorRetryable(r->io.conn.conn)) continue;
            if (errno == EWOULDBLOCK) errno = ETIMEDOUT;
            return 0;
        }
        sdsIncrLen(r->io.conn.buf, retval);
    }
    /* RIO 中 connection 的 buf 从 pos 位置读 len 个数据到给定的 buf中  */
    memcpy(buf, (char*)r->io.conn.buf + r->io.conn.pos, len);
    r->io.conn.read_so_far += len;
    r->io.conn.pos += len;
    return len;
}

/* Returns read/write position in file. */
/* 获取 connection 已经读取的数据大小 */
static off_t rioConnTell(rio *r) {
    return r->io.conn.read_so_far;
}

/* Flushes any buffer to target device if applicable. Returns 1 on success
 * and 0 on failures. */
/* 没有写操作，直接写空数据 */
static int rioConnFlush(rio *r) {
    /* Our flush is implemented by the write method, that recognizes a
     * buffer set to NULL with a count of zero as a flush request. */
    return rioConnWrite(r,NULL,0);
}

static const rio rioConnIO = {
    rioConnRead,
    rioConnWrite,
    rioConnTell,
    rioConnFlush,
    NULL,           /* update_checksum */
    0,              /* current checksum */
    0,              /* flags */
    0,              /* bytes read or written */
    0,              /* read/write chunk size */
    { { NULL, 0 } } /* union for io-specific vars */
};

/* Create an RIO that implements a buffered read from an fd
 * read_limit argument stops buffering when the reaching the limit. */
/* 初始化一个 connection rio */
void rioInitWithConn(rio *r, connection *conn, size_t read_limit) {
    *r = rioConnIO;
    r->io.conn.conn = conn;
    r->io.conn.pos = 0;
    r->io.conn.read_limit = read_limit;
    r->io.conn.read_so_far = 0;
    r->io.conn.buf = sdsnewlen(NULL, PROTO_IOBUF_LEN);
    sdsclear(r->io.conn.buf);
}

/* Release the RIO stream. Optionally returns the unread buffered data
 * when the SDS pointer 'remaining' is passed. */
/* 释放 rio connection 资源 */
void rioFreeConn(rio *r, sds *remaining) {
    if (remaining && (size_t)r->io.conn.pos < sdslen(r->io.conn.buf)) {
        if (r->io.conn.pos > 0) sdsrange(r->io.conn.buf, r->io.conn.pos, -1);
        *remaining = r->io.conn.buf;
    } else {
        sdsfree(r->io.conn.buf);
        if (remaining) *remaining = NULL;
    }
    r->io.conn.buf = NULL;
}

/* ------------------- File descriptor implementation ------------------
 * This target is used to write the RDB file to pipe, when the master just
 * streams the data to the replicas without creating an RDB on-disk image
 * (diskless replication option).
 * It only implements writes. */

/* fd rio 功能 */

/* Returns 1 or 0 for success/failure.
 *
 * When buf is NULL and len is 0, the function performs a flush operation
 * if there is some pending buffer, so this function is also used in order
 * to implement rioFdFlush(). */
/* fd rio 写操作，
 * 如果提供的 buf 是 NULL 且 len 为0，当 fd 存在内核缓冲区，会执行 flush 操作
 * 所以 fd rio 的 flush 方法也是调用该方法，可以看到 rioFdFlush 的参数 buf 为 NULL, len 为0*/
static size_t rioFdWrite(rio *r, const void *buf, size_t len) {
    ssize_t retval;
    unsigned char *p = (unsigned char*) buf;
    /* buf == NULL and len == 0 表示需要 flush */
    int doflush = (buf == NULL && len == 0);

    /* For small writes, we rather keep the data in user-space buffer, and flush
     * it only when it grows. however for larger writes, we prefer to flush
     * any pre-existing buffer, and write the new one directly without reallocs
     * and memory copying. */
    if (len > PROTO_IOBUF_LEN) {
        /* First, flush any pre-existing buffered data. */
        if (sdslen(r->io.fd.buf)) {
            if (rioFdWrite(r, NULL, 0) == 0)
                return 0;
        }
        /* Write the new data, keeping 'p' and 'len' from the input. */
    } else {
        if (len) {
            r->io.fd.buf = sdscatlen(r->io.fd.buf,buf,len);
            if (sdslen(r->io.fd.buf) > PROTO_IOBUF_LEN)
                doflush = 1;
            if (!doflush)
                return 1;
        }
        /* Flushing the buffered data. set 'p' and 'len' accordingly. */
        p = (unsigned char*) r->io.fd.buf;
        len = sdslen(r->io.fd.buf);
    }

    size_t nwritten = 0;
    while(nwritten != len) {
        retval = write(r->io.fd.fd,p+nwritten,len-nwritten);
        if (retval <= 0) {
            if (retval == -1 && errno == EINTR) continue;
            /* With blocking io, which is the sole user of this
             * rio target, EWOULDBLOCK is returned only because of
             * the SO_SNDTIMEO socket option, so we translate the error
             * into one more recognizable by the user. */
            if (retval == -1 && errno == EWOULDBLOCK) errno = ETIMEDOUT;
            return 0; /* error. */
        }
        nwritten += retval;
    }

    r->io.fd.pos += len;
    sdsclear(r->io.fd.buf);
    return 1;
}

/* Returns 1 or 0 for success/failure. */
/* rio fd 没有读操作 */
static size_t rioFdRead(rio *r, void *buf, size_t len) {
    UNUSED(r);
    UNUSED(buf);
    UNUSED(len);
    return 0; /* Error, this target does not support reading. */
}

/* Returns read/write position in file. */
/* 获取写位置 */
static off_t rioFdTell(rio *r) {
    return r->io.fd.pos;
}

/* Flushes any buffer to target device if applicable. Returns 1 on success
 * and 0 on failures. */
/* flush 调用器write */
static int rioFdFlush(rio *r) {
    /* Our flush is implemented by the write method, that recognizes a
     * buffer set to NULL with a count of zero as a flush request. */
    return rioFdWrite(r,NULL,0);
}

static const rio rioFdIO = {
    rioFdRead,
    rioFdWrite,
    rioFdTell,
    rioFdFlush,
    NULL,           /* update_checksum */
    0,              /* current checksum */
    0,              /* flags */
    0,              /* bytes read or written */
    0,              /* read/write chunk size */
    { { NULL, 0 } } /* union for io-specific vars */
};

void rioInitWithFd(rio *r, int fd) {
    *r = rioFdIO;
    r->io.fd.fd = fd;
    r->io.fd.pos = 0;
    r->io.fd.buf = sdsempty();
}

/* release the rio stream. */
void rioFreeFd(rio *r) {
    sdsfree(r->io.fd.buf);
}

/* ---------------------------- Generic functions ---------------------------- */

/* This function can be installed both in memory and file streams when checksum
 * computation is needed. */
void rioGenericUpdateChecksum(rio *r, const void *buf, size_t len) {
    r->cksum = crc64(r->cksum,buf,len);
}

/* Set the file-based rio object to auto-fsync every 'bytes' file written.
 * By default this is set to zero that means no automatic file sync is
 * performed.
 *
 * This feature is useful in a few contexts since when we rely on OS write
 * buffers sometimes the OS buffers way too much, resulting in too many
 * disk I/O concentrated in very little time. When we fsync in an explicit
 * way instead the I/O pressure is more distributed across time. */
void rioSetAutoSync(rio *r, off_t bytes) {
    if(r->write != rioFileIO.write) return;
    r->io.file.autosync = bytes;
}

/* Check the type of rio. */
uint8_t rioCheckType(rio *r) {
    if (r->read == rioFileRead) {
        return RIO_TYPE_FILE;
    } else if (r->read == rioBufferRead) {
        return RIO_TYPE_BUFFER;
    } else if (r->read == rioConnRead) {
        return RIO_TYPE_CONN;
    } else {
        /* r->read == rioFdRead */
        return RIO_TYPE_FD;
    }
}

/* --------------------------- Higher level interface --------------------------
 *
 * The following higher level functions use lower level rio.c functions to help
 * generating the Redis protocol for the Append Only File. */

/* Write multi bulk count in the format: "*<count>\r\n". */
size_t rioWriteBulkCount(rio *r, char prefix, long count) {
    char cbuf[128];
    int clen;

    cbuf[0] = prefix;
    clen = 1+ll2string(cbuf+1,sizeof(cbuf)-1,count);
    cbuf[clen++] = '\r';
    cbuf[clen++] = '\n';
    if (rioWrite(r,cbuf,clen) == 0) return 0;
    return clen;
}

/* Write binary-safe string in the format: "$<count>\r\n<payload>\r\n". */
size_t rioWriteBulkString(rio *r, const char *buf, size_t len) {
    size_t nwritten;

    if ((nwritten = rioWriteBulkCount(r,'$',len)) == 0) return 0;
    if (len > 0 && rioWrite(r,buf,len) == 0) return 0;
    if (rioWrite(r,"\r\n",2) == 0) return 0;
    return nwritten+len+2;
}

/* Write a long long value in format: "$<count>\r\n<payload>\r\n". */
size_t rioWriteBulkLongLong(rio *r, long long l) {
    char lbuf[32];
    unsigned int llen;

    llen = ll2string(lbuf,sizeof(lbuf),l);
    return rioWriteBulkString(r,lbuf,llen);
}

/* Write a double value in the format: "$<count>\r\n<payload>\r\n" */
size_t rioWriteBulkDouble(rio *r, double d) {
    char dbuf[128];
    unsigned int dlen;

    dlen = snprintf(dbuf,sizeof(dbuf),"%.17g",d);
    return rioWriteBulkString(r,dbuf,dlen);
}
