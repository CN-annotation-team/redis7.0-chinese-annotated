/* 命令行接口 (command line interface,CLI) 通用接口
 *
 * Copyright (c) 2020, Redis Labs
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
#include "cli_common.h"
#include <stdio.h>
#include <stdlib.h>
#include <fcntl.h>
#include <errno.h>
#include <hiredis.h>
#include <sdscompat.h> /* 使用 hiredis 的 sds 兼容头文件将 sds 调用映射到对应的以 "hi_" 为前缀的变种 */
#include <sds.h> /* 使用 hiredis 中的 sds.h, 使程序中只存在一套 sds 函数 */
#include <unistd.h>
#include <string.h>
#include <ctype.h>
#ifdef USE_OPENSSL
#include <openssl/ssl.h>
#include <openssl/err.h>
#include <hiredis_ssl.h>
#endif

#define UNUSED(V) ((void) V)

/* 包装 redisSecureConnection 以在不支持 TLS 的构建下
 * 避免 hiredis_ssl 的依赖.
 */
int cliSecureConnection(redisContext *c, cliSSLconfig config, const char **err) {
#ifdef USE_OPENSSL
    static SSL_CTX *ssl_ctx = NULL;

    if (!ssl_ctx) {
        ssl_ctx = SSL_CTX_new(SSLv23_client_method());
        if (!ssl_ctx) {
            *err = "Failed to create SSL_CTX";
            goto error;
        }
        SSL_CTX_set_options(ssl_ctx, SSL_OP_NO_SSLv2 | SSL_OP_NO_SSLv3);
        SSL_CTX_set_verify(ssl_ctx, config.skip_cert_verify ? SSL_VERIFY_NONE : SSL_VERIFY_PEER, NULL);

        if (config.cacert || config.cacertdir) {
            if (!SSL_CTX_load_verify_locations(ssl_ctx, config.cacert, config.cacertdir)) {
                *err = "Invalid CA Certificate File/Directory";
                goto error;
            }
        } else {
            if (!SSL_CTX_set_default_verify_paths(ssl_ctx)) {
                *err = "Failed to use default CA paths";
                goto error;
            }
        }

        if (config.cert && !SSL_CTX_use_certificate_chain_file(ssl_ctx, config.cert)) {
            *err = "Invalid client certificate";
            goto error;
        }

        if (config.key && !SSL_CTX_use_PrivateKey_file(ssl_ctx, config.key, SSL_FILETYPE_PEM)) {
            *err = "Invalid private key";
            goto error;
        }
        if (config.ciphers && !SSL_CTX_set_cipher_list(ssl_ctx, config.ciphers)) {
            *err = "Error while configuring ciphers";
            goto error;
        }
#ifdef TLS1_3_VERSION
        if (config.ciphersuites && !SSL_CTX_set_ciphersuites(ssl_ctx, config.ciphersuites)) {
            *err = "Error while setting cypher suites";
            goto error;
        }
#endif
    }

    SSL *ssl = SSL_new(ssl_ctx);
    if (!ssl) {
        *err = "Failed to create SSL object";
        return REDIS_ERR;
    }

    if (config.sni && !SSL_set_tlsext_host_name(ssl, config.sni)) {
        *err = "Failed to configure SNI";
        SSL_free(ssl);
        return REDIS_ERR;
    }

    return redisInitiateSSL(c, ssl);

error:
    SSL_CTX_free(ssl_ctx);
    ssl_ctx = NULL;
    return REDIS_ERR;
#else
    (void) config;
    (void) c;
    (void) err;
    return REDIS_OK;
#endif
}

/* 包装 hiredis 来实现任意的读与写.
 *
 * 我们利用 hiredis 来透明地实现对 TLS 的支持,
 * 并使用 hiredis 内部的缓冲区,
 * 使缓冲区能在多个调用间与连接共存.
 *
 * 接口与 read()/write() 足够相近, 所以应该能够透明地工作.
 * (像调用 read()/wirte() 一样调用它们).
 */

/* 将一段原始数据写入上下文 'redisContext' 中.
 * 如果 hiredis 内部的缓冲区中仍有数据(由 hiredis 留下的)
 * 我们会将这些数据先写入.
 */
ssize_t cliWriteConn(redisContext *c, const char *buf, size_t buf_len)
{
    int done = 0;

    /* 将数据附加到缓冲区中,
     * 这个缓冲区 *通常* 是空的,但我们不会这么假设,
     * 然后将缓冲区写入 socket 中.
     */
    c->obuf = sdscatlen(c->obuf, buf, buf_len);
    if (redisBufferWrite(c, &done) == REDIS_ERR) {
        if (!(c->flags & REDIS_BLOCK))
            errno = EAGAIN;

        /* 当发生错误时, 我们假设没有进行任何写入,
         * 并将缓冲区回滚到原来的状态.
         */
        if (sdslen(c->obuf) > buf_len)
            sdsrange(c->obuf, 0, -(buf_len+1));
        else
            sdsclear(c->obuf);

        return -1;
    }

    /* 如果我们成功写入了整个缓冲区, 则释放整个缓冲区.
     * 我们可能写入了比 'buf_len' 更多的数据 (如果 'c->obuf' 在最初时不是空的),
     * 但我们不用告知调用者.
     */
    if (done) {
        sdsclear(c->obuf);
        return buf_len;
    }

    /* 写入成功了,
     * 但还有一些剩下的数据,我们需要将它们从缓冲区中移除.
     *
     * 是否还有遗留在我们的 'buf' 之前的数据?
     * 如果有, 将我们的数据从缓冲区中移除, 并报告没有写入新的数据.
     */
    if (sdslen(c->obuf) > buf_len) {
        sdsrange(c->obuf, 0, -(buf_len+1));
        return 0;
    }

    /* 现在我们确定没有先前的数据留在缓冲区中.
     * 我们清空缓冲区, 并报告写入了多少数据.
     */
    size_t left = sdslen(c->obuf);
    sdsclear(c->obuf);
    return buf_len - left;
}

/* 包装 OpenSSL(libssl 和 libcrypto) 的初始化
 */
int cliSecureInit()
{
#ifdef USE_OPENSSL
    ERR_load_crypto_strings();
    SSL_load_error_strings();
    SSL_library_init();
#endif
    return REDIS_OK;
}

/* 从标准输入流创建简单动态字符串(Simple Dynamic String,sds) */
sds readArgFromStdin(void) {
    char buf[1024];
    sds arg = sdsempty();

    while(1) {
        int nread = read(fileno(stdin),buf,1024);

        if (nread == 0) break;
        else if (nread == -1) {
            perror("Reading from standard input");
            exit(1);
        }
        arg = sdscatlen(arg,buf,nread);
    }
    return arg;
}

/*
 * 从命令行参数 'argv' 创建一个 sds 字符串数组,
 * 如果 'quoted' 为 false, 直接存储 'avgv' 中每一个参数.
 * 如果 'quoted' 为 true, 则将每一个参数视为被引号包裹的字符串,
 * 进行解析后存入数组,
 * 遇到无效的引号包裹的字符串则返回 NULL.
 * Create an sds array from argv, either as-is or by dequoting every
 * element. When quoted is non-zero, may return a NULL to indicate an
 * invalid quoted string.
 *
 * 调用者应当使用 sdsfreesplitres() 释放返回的 sds 字符串数组.
 * The caller should free the resulting array of sds strings with
 * sdsfreesplitres().
 */
sds *getSdsArrayFromArgv(int argc,char **argv, int quoted) {
    sds *res = sds_malloc(sizeof(sds) * argc);

    for (int j = 0; j < argc; j++) {
        if (quoted) {
            sds unquoted = unquoteCString(argv[j]);
            if (!unquoted) {
                while (--j >= 0) sdsfree(res[j]);
                sds_free(res);
                return NULL;
            }
            res[j] = unquoted;
        } else {
            res[j] = sdsnew(argv[j]);
        }
    }

    return res;
}

/* 解析一个零终止的字符串, 以二进制安全的 sds 返回.
 * Unquote a null-terminated string and return it as a binary-safe sds.
 */
sds unquoteCString(char *str) {
    int count;
    sds *unquoted = sdssplitargs(str, &count);
    sds res = NULL;

    if (unquoted && count == 1) {
        res = unquoted[0];
        unquoted[0] = NULL;
    }

    if (unquoted)
        sdsfreesplitres(unquoted, count);

    return res;
}


/* URL 风格的 % 解码, 例如: "%F2" -> '\xF2'
 * URL-style percent decoding.
 */
#define isHexChar(c) (isdigit(c) || ((c) >= 'a' && (c) <= 'f'))
#define decodeHexChar(c) (isdigit(c) ? (c) - '0' : (c) - 'a' + 10)
#define decodeHex(h, l) ((decodeHexChar(h) << 4) + decodeHexChar(l))

static sds percentDecode(const char *pe, size_t len) {
    const char *end = pe + len;
    sds ret = sdsempty();
    const char *curr = pe;

    while (curr < end) {
        if (*curr == '%') {
            if ((end - curr) < 2) {
                fprintf(stderr, "Incomplete URI encoding\n");
                exit(1);
            }

            char h = tolower(*(++curr));
            char l = tolower(*(++curr));
            if (!isHexChar(h) || !isHexChar(l)) {
                fprintf(stderr, "Illegal character in URI encoding\n");
                exit(1);
            }
            char c = decodeHex(h, l);
            ret = sdscatlen(ret, &c, 1);
            curr++;
        } else {
            ret = sdscatlen(ret, curr++, 1);
        }
    }

    return ret;
}

/* 解析一个 URI, 提取链接的信息.
 * URI 协议基于现有的规格[1], 但排除对查询参数的支持.
 * 有效的 URIs 如下:
 *   协议:  "redis://"
 *   认证:  [[用户名] ":"] <密码> "@"] [<主机名> [":" <端口>]]
 *   路径:  ["/" [<数据库索引>]]
 *
 * Parse a URI and extract the server connection information.
 * URI scheme is based on the provisional specification[1] excluding support
 * for query parameters. Valid URIs are:
 *   scheme:    "redis://"
 *   authority: [[<username> ":"] <password> "@"] [<hostname> [":" <port>]]
 *   path:      ["/" [<db>]]
 *
 *  [1]: https://www.iana.org/assignments/uri-schemes/prov/redis */
void parseRedisUri(const char *uri, const char* tool_name, cliConnInfo *connInfo, int *tls_flag) {
#ifdef USE_OPENSSL
    UNUSED(tool_name);
#else
    UNUSED(tls_flag);
#endif

    const char *scheme = "redis://";
    const char *tlsscheme = "rediss://";
    const char *curr = uri;
    const char *end = uri + strlen(uri);
    const char *userinfo, *username, *port, *host, *path;

    /* URL 必须以一个有效的协议开头.
     * URI must start with a valid scheme.
     */
    if (!strncasecmp(tlsscheme, curr, strlen(tlsscheme))) {
#ifdef USE_OPENSSL
        *tls_flag = 1;
        curr += strlen(tlsscheme);
#else
        fprintf(stderr,"rediss:// is only supported when %s is compiled with OpenSSL\n", tool_name);
        exit(1);
#endif
    } else if (!strncasecmp(scheme, curr, strlen(scheme))) {
        curr += strlen(scheme);
    } else {
        fprintf(stderr,"Invalid URI scheme\n");
        exit(1);
    }
    if (curr == end) return;

    /* 提取用户信息.
     * Extract user info.
     */
    if ((userinfo = strchr(curr,'@'))) {
        if ((username = strchr(curr, ':')) && username < userinfo) {
            connInfo->user = percentDecode(curr, username - curr);
            curr = username + 1;
        }

        connInfo->auth = percentDecode(curr, userinfo - curr);
        curr = userinfo + 1;
    }
    if (curr == end) return;

    /* 提取主机和端口.
     * Extract host and port.
     */
    path = strchr(curr, '/');
    if (*curr != '/') {
        host = path ? path - 1 : end;
        if ((port = strchr(curr, ':'))) {
            connInfo->hostport = atoi(port + 1);
            host = port - 1;
        }
        sdsfree(connInfo->hostip);
        connInfo->hostip = sdsnewlen(curr, host - curr + 1);
    }
    curr = path ? path + 1 : end;
    if (curr == end) return;

    /* 提取数据库的索引.
     * Extract database number.
     */
    connInfo->input_dbnum = atoi(curr);
}

void freeCliConnInfo(cliConnInfo connInfo){
    if (connInfo.hostip) sdsfree(connInfo.hostip);
    if (connInfo.auth) sdsfree(connInfo.auth);
    if (connInfo.user) sdsfree(connInfo.user);
}

/*
 * 将一个 Unicode 字符串转义为 JSON 字符串 (--json), 遵从 RFC 7159.
 * (此处采用的是 UTF-32)
 *
 * Escape a Unicode string for JSON output (--json), following RFC 7159:
 * https://datatracker.ietf.org/doc/html/rfc7159#section-7
*/
sds escapeJsonString(sds s, const char *p, size_t len) {
    s = sdscatlen(s,"\"",1);
    while(len--) {
        switch(*p) {
        case '\\':
        case '"':
            s = sdscatprintf(s,"\\%c",*p);
            break;
        case '\n': s = sdscatlen(s,"\\n",2); break;
        case '\f': s = sdscatlen(s,"\\f",2); break;
        case '\r': s = sdscatlen(s,"\\r",2); break;
        case '\t': s = sdscatlen(s,"\\t",2); break;
        case '\b': s = sdscatlen(s,"\\b",2); break;
        default:
            s = sdscatprintf(s,*(unsigned char *)p <= 0x1f ? "\\u%04x" : "%c",*p);
        }
        p++;
    }
    return sdscatlen(s,"\"",1);
}
