/* Copyright (c) 2014, Matt Stancliff <matt@genges.com>
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
 * POSSIBILITY OF SUCH DAMAGE. */

#ifndef CRCSPEED_H
#define CRCSPEED_H

/* 本文件中crc16计算相关未在项目中使用(使用crc16.c中的实现)
* 主要是基于 ​Slice-by-8 技术 优化的算法用于crc64的计算
* 题外：迭代历史可查看 https://matt.sh/redis-crcspeed
*/

#include <inttypes.h>
#include <stdio.h>

/* 类型声明，内部使用均为crc64文件中的_crc64函数，使用此方法均需调用crc64_init初始化table */
typedef uint64_t (*crcfn64)(uint64_t, const void *, const uint64_t);
typedef uint16_t (*crcfn16)(uint16_t, const void *, const uint64_t);

/* CRC-64 */
/* CRC 表初始化函数：初始化 64 位 CRC 查找表，用于小端架构 */
void crcspeed64little_init(crcfn64 fn, uint64_t table[8][256]);
/* 大端架构 CRC 表初始化函数：初始化 64 位 CRC 查找表，用于大端架构 */
void crcspeed64big_init(crcfn64 fn, uint64_t table[8][256]);
/* 自动选择架构的 CRC 表初始化函数 */
void crcspeed64native_init(crcfn64 fn, uint64_t table[8][256]);

/* CRC 计算函数（小端架构）：在小端架构上计算 64 位 CRC */
uint64_t crcspeed64little(uint64_t table[8][256], uint64_t crc, void *buf,
                          size_t len);
/* CRC 计算函数（大端架构）：在大端架构上计算 64 位 CRC */
uint64_t crcspeed64big(uint64_t table[8][256], uint64_t crc, void *buf,
                       size_t len);
/* 自动选择架构的 CRC 计算函数：根据当前架构自动选择小端或大端的 CRC 计算函数 */
uint64_t crcspeed64native(uint64_t table[8][256], uint64_t crc, void *buf,
                          size_t len);

/* CRC-16 */
/* CRC16 表初始化函数：初始化 16 位 CRC 查找表，用于小端架构 */
void crcspeed16little_init(crcfn16 fn, uint16_t table[8][256]);
/* 大端架构 CRC16 表初始化函数：初始化 16 位 CRC 查找表，用于大端架构 */
void crcspeed16big_init(crcfn16 fn, uint16_t table[8][256]);
/* 自动选择架构的 CRC16 表初始化函数 */
void crcspeed16native_init(crcfn16 fn, uint16_t table[8][256]);

/* CRC16 计算函数（小端架构）：在小端架构上计算 16 位 CRC */
uint16_t crcspeed16little(uint16_t table[8][256], uint16_t crc, void *buf,
                          size_t len);
/* CRC16 计算函数（大端架构）：在大端架构上计算 16 位 CRC */
uint16_t crcspeed16big(uint16_t table[8][256], uint16_t crc, void *buf,
                       size_t len);
/* 自动选择架构的 CRC16 计算函数：根据当前架构自动选择小端或大端的 CRC16 计算函数 */
uint16_t crcspeed16native(uint16_t table[8][256], uint16_t crc, void *buf,
                          size_t len);
#endif
