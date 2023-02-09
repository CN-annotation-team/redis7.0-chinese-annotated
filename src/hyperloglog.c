/* hyperloglog.c - Redis HyperLogLog probabilistic cardinality approximation.
 * This file implements the algorithm and the exported Redis commands.
 *
 * Copyright (c) 2014, Salvatore Sanfilippo <antirez at gmail dot com>
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

#include <stdint.h>
#include <math.h>

/* The Redis HyperLogLog implementation is based on the following ideas:
 *
 * * The use of a 64 bit hash function as proposed in [1], in order to estimate
 *   cardinalities larger than 10^9, at the cost of just 1 additional bit per
 *   register.
 * * The use of 16384 6-bit registers for a great level of accuracy, using
 *   a total of 12k per key.
 * * The use of the Redis string data type. No new type is introduced.
 * * No attempt is made to compress the data structure as in [1]. Also the
 *   algorithm used is the original HyperLogLog Algorithm as in [2], with
 *   the only difference that a 64 bit hash function is used, so no correction
 *   is performed for values near 2^32 as in [1].
 *
 * [1] Heule, Nunkesser, Hall: HyperLogLog in Practice: Algorithmic
 *     Engineering of a State of The Art Cardinality Estimation Algorithm.
 *
 * [2] P. Flajolet, Éric Fusy, O. Gandouet, and F. Meunier. Hyperloglog: The
 *     analysis of a near-optimal cardinality estimation algorithm.
 *
 * Redis uses two representations:
 *
 * 1) A "dense" representation where every entry is represented by
 *    a 6-bit integer.
 * 2) A "sparse" representation using run length compression suitable
 *    for representing HyperLogLogs with many registers set to 0 in
 *    a memory efficient way.
 *
 *
 * HLL header
 * ===
 *
 * Both the dense and sparse representation have a 16 byte header as follows:
 *
 * +------+---+-----+----------+
 * | HYLL | E | N/U | Cardin.  |
 * +------+---+-----+----------+
 *
 * The first 4 bytes are a magic string set to the bytes "HYLL".
 * "E" is one byte encoding, currently set to HLL_DENSE or
 * HLL_SPARSE. N/U are three not used bytes.
 *
 * The "Cardin." field is a 64 bit integer stored in little endian format
 * with the latest cardinality computed that can be reused if the data
 * structure was not modified since the last computation (this is useful
 * because there are high probabilities that HLLADD operations don't
 * modify the actual data structure and hence the approximated cardinality).
 *
 * When the most significant bit in the most significant byte of the cached
 * cardinality is set, it means that the data structure was modified and
 * we can't reuse the cached value that must be recomputed.
 *
 * Dense representation
 * ===
 *
 * The dense representation used by Redis is the following:
 *
 * +--------+--------+--------+------//      //--+
 * |11000000|22221111|33333322|55444444 ....     |
 * +--------+--------+--------+------//      //--+
 *
 * The 6 bits counters are encoded one after the other starting from the
 * LSB to the MSB, and using the next bytes as needed.
 *
 * Sparse representation
 * ===
 *
 * The sparse representation encodes registers using a run length
 * encoding composed of three opcodes, two using one byte, and one using
 * of two bytes. The opcodes are called ZERO, XZERO and VAL.
 *
 * ZERO opcode is represented as 00xxxxxx. The 6-bit integer represented
 * by the six bits 'xxxxxx', plus 1, means that there are N registers set
 * to 0. This opcode can represent from 1 to 64 contiguous registers set
 * to the value of 0.
 *
 * XZERO opcode is represented by two bytes 01xxxxxx yyyyyyyy. The 14-bit
 * integer represented by the bits 'xxxxxx' as most significant bits and
 * 'yyyyyyyy' as least significant bits, plus 1, means that there are N
 * registers set to 0. This opcode can represent from 0 to 16384 contiguous
 * registers set to the value of 0.
 *
 * VAL opcode is represented as 1vvvvvxx. It contains a 5-bit integer
 * representing the value of a register, and a 2-bit integer representing
 * the number of contiguous registers set to that value 'vvvvv'.
 * To obtain the value and run length, the integers vvvvv and xx must be
 * incremented by one. This opcode can represent values from 1 to 32,
 * repeated from 1 to 4 times.
 *
 * The sparse representation can't represent registers with a value greater
 * than 32, however it is very unlikely that we find such a register in an
 * HLL with a cardinality where the sparse representation is still more
 * memory efficient than the dense representation. When this happens the
 * HLL is converted to the dense representation.
 *
 * The sparse representation is purely positional. For example a sparse
 * representation of an empty HLL is just: XZERO:16384.
 *
 * An HLL having only 3 non-zero registers at position 1000, 1020, 1021
 * respectively set to 2, 3, 3, is represented by the following three
 * opcodes:
 *
 * XZERO:1000 (Registers 0-999 are set to 0)
 * VAL:2,1    (1 register set to value 2, that is register 1000)
 * ZERO:19    (Registers 1001-1019 set to 0)
 * VAL:3,2    (2 registers set to value 3, that is registers 1020,1021)
 * XZERO:15362 (Registers 1022-16383 set to 0)
 *
 * In the example the sparse representation used just 7 bytes instead
 * of 12k in order to represent the HLL registers. In general for low
 * cardinality there is a big win in terms of space efficiency, traded
 * with CPU time since the sparse representation is slower to access:
 *
 * The following table shows average cardinality vs bytes used, 100
 * samples per cardinality (when the set was not representable because
 * of registers with too big value, the dense representation size was used
 * as a sample).
 *
 * 100 267
 * 200 485
 * 300 678
 * 400 859
 * 500 1033
 * 600 1205
 * 700 1375
 * 800 1544
 * 900 1713
 * 1000 1882
 * 2000 3480
 * 3000 4879
 * 4000 6089
 * 5000 7138
 * 6000 8042
 * 7000 8823
 * 8000 9500
 * 9000 10088
 * 10000 10591
 *
 * The dense representation uses 12288 bytes, so there is a big win up to
 * a cardinality of ~2000-3000. For bigger cardinalities the constant times
 * involved in updating the sparse representation is not justified by the
 * memory savings. The exact maximum length of the sparse representation
 * when this implementation switches to the dense representation is
 * configured via the define server.hll_sparse_max_bytes.
 */

/* 阅读该文件的源码可以先从 hllAdd 函数开始看，再看 hllCount 函数，这两个函数其实就对应 pfadd 和 pfcount 两个命令的实际逻辑
 *
 * 一.
 * 使用场景：
 * hyperloglog 可以用于估算页面访问的用户量（用户访问量特别大）
 * 主要是两个作用
 * 1) 去重，对于相同的数据对 hyperloglog 造成的影响是相同的
 * 2) 估算数量，不能做到精准统计，官方给出的误差是 0.81%
 *
 * 二.
 * hyperloglog 数据结构（密集型编码的情况，密集型是真实的结构，稀疏型是对于数据量少，做空间优化压缩后的形式）
 * 注：每个存进来的数据都有一个索引 index(不会大于 2^14)，和要存的数据 count(不会大于 51)，第三部分会说明
 * 1) hyperloglog 结构由桶（分组）构成，总共有 2^14 也就是 16384 个桶（因为 index 不会大于 2^14）， 每个数据会根据自己的
 * index 分配到其中一个桶中。
 * 2) 每个桶的实际数据就是 6 个 bit 位（0~63），每存一个数据到桶中，会比较自己的 count 和桶的 6bit 的数据大小，
 * 如果自己的 count 比这 6bit 的数据大，就替换掉该桶中的 6bit 数据为自己的 count
 * 3) 空间占用 = 2^14（桶的数量） * 6bit（每个桶的大小）= 12288Byte。
 * 该结构的空间占用就是 12288Byte 也就是 12kB
 *
 * 具体存储的形式如下，下面的一串数据（从英文注释中拿的），
 * 注：每个数字表示的是自己是哪一个桶的数据（注意这个数字不是实际的数据，实际的数据都是 0,1 bit 串）
 * +--------+--------+--------+------//      //--+
 * |11000000|22221111|33333322|55444444 ....     |
 * +--------+--------+--------+------//      //--+
 *
 * 拿前八个数字(也就是第一个字节)来讲 11000000
 * 0 表示自己是第 0 号桶，有六个是因为桶的大小为 6bit
 * 1 表示自己是第 1 号桶，这边有两个，加上后面 2221111 里的四个，是完整的 1 号桶数据
 *
 * 因为每个桶占 6bit 位，所以每个数字都会出现 6 次，当桶跨字节的时候，可以看到相同的数字并不连续，这种设计方式其实
 * 是方便进行位运算获取数据和修改数据，
 * 比如我现在要拿第 2 号桶的数据，
 * 1) 获取 2 号桶的数据从字节数组的哪个字节开始： 2 * HLL_BITS(6) / 8 = 1  拿到字节 b0(22221111)
 * 2) 获取 2 号桶的数据在该字节的哪个 bit 位结束： 2 * HLL_BITS(6) & 7 = 4  获取到 fb(4)
 * 3) 获取 b0的后一个字节 b1(33333322)， (b0[22221111] >> fb[4]) | (b1[33333322] << (8 - fb(4))) & HLL_REGISTER_MAX(63)
 *                                         00002222           |     33220000                  &    00111111        (这里的数字 0,1 是实际的 bit 位数据了)
 *    最后的结果就是 00222222，将 2 号桶的数据拿出来了
 *    333333|22
 *             2222|1111
 *   第 1 个字节的高位会作为桶数据的低位，第 2 个字节的低位会作为桶数据的高位
 *    这个计算步骤是宏 HLL_DENSE_GET_REGISTER 的计算步骤
 *
 *
 * 三.
 * 数据存储步骤：
 * 1) 对添加进来的值 hash 化，生成一个无符号 64 位整型，hash 函数是 MurmurHash64A 函数.
 * 对于上面的使用场景，拿到用户 ID，使用 Murmurhash64A 函数计算出一个 64 位的 hash 值
 * 2) 拿到这个 64bit 位的 hash 值，进行分割：
 *    2.1) 低 14 位作为 index，所以 index <= 2^14 即 16384。
 *    2.2) 高 50 位，获取高 50 位中低位开始第一次出现 1 的位置，该位置就是 count 值，如果 50 位都是 0
 *    会认为第 51 位为1，所以 count <= 51
 * 3) 获取到数据的 index 和 count，将其放入 hyperloglog 数据结构中，具体怎么将数据放入 hyperloglog 需要看
 * HLL_DENSE_SET_REGISTER 宏
 *
 * 四.
 * 原理和计数（访问的用户量）的计算：
 * 伯努利过程（简化理解），抛硬币，抛出反面为 0，抛出正面为 1，抛一次两者出现的概率都为（1/2）
 *    1) 一轮测试，一直抛直到抛出正面或者抛了 50 次了，本轮测试停止
 *    （这里一轮测试可以看做一个数据进 hyperloglog 数据结构，该数据的 count 值就是出现正面(1)是第多少次
 *    每抛一次可以看做这个数据取 hash 之后高 50 位的一个 bit 位 ，对于用户 ID 相同，hash 值一样，得到的结果一样）
 *    2) 每次抛出反面的概率都是 1/2，那么第 count 次才出现正面(1) 的概率: P = (1/2)^(count - 1次反面) * (1/2)。
 *    后面的 1/2 是最后出正面的概率。所以概率就是 P = (1/2)^count
 *    3) 既然第 count 次才出现正面(1) 的概率是 (1/2)^count。那么理想情况下，做 2^count 轮测试，会出现且仅出现一轮测试
 *    是第 count 次才出现正面(1)的情况.
 *    对于大于 count 次才出现正面(1)的情况，理想情况下做 2^count 轮测试不足以出现大于 count 次才出现正面（1）的情况
 *    （这里 2^count 轮测试可以看做是一个桶的数据量，桶的值就是这 2^count 轮测试抛出最多连续为 0 的那轮测试的值）
 *    注：到这里可以反推出，理想情况下，如果有且仅有一轮测试出现了第 count 次才出现正面，那么可以认为做了 2^count 轮测试，所以可以根据这个
 *   最大的 count 值来推算出进来的不同数据的数量是 2^count 次方
 *    4) 上面获取的结论都是在理想情况下，但是现实中，总是存在偏差，为了减少误差就需要进行分桶，将数据打散到所有桶里面，
 *    然后用所有桶中的最大 count 值计算平均数 c，
 *    5) 最后的计算公式： 2^c * 桶的数量 = 用户数量
 *
 */

struct hllhdr {
    /* 定义魔数，HYLL */
    char magic[4];      /* "HYLL" */
    /* 编码类型，HLL_DENSE 密集型，HLL_SPARSE 稀疏型 */
    uint8_t encoding;   /* HLL_DENSE or HLL_SPARSE. */
    uint8_t notused[3]; /* Reserved for future use, must be zero. */
    /* 缓存上一次获取计数的值，每次修改 registers 会将该值无效化，获取计数的时候会使用该属性缓存，使用小端字节序存储 */
    uint8_t card[8];    /* Cached cardinality, little endian. */
    /* 数据字节数组，这里就是存储实际 hyperloglog 数据结构的字节数组 */
    uint8_t registers[]; /* Data bytes. */
};

/* The cached cardinality MSB is used to signal validity of the cached value. */
/* 将缓存的计数 card 无效化，因为是小端存储所以是将第 8 个字节的第一位置 1
 * 注：如果不熟悉大小端字节序，可以直接看做将 64 位数字的最高位标记为 1
 */
#define HLL_INVALIDATE_CACHE(hdr) (hdr)->card[7] |= (1<<7)
/* 判断 card 是否是有效的，可以直接看做判断最高位是不是 0，如果是 0 就是合法的 */
#define HLL_VALID_CACHE(hdr) (((hdr)->card[7] & (1<<7)) == 0)

/* 定义将 hash 后的数据低多少位作为索引，该值越大，最后计算计数的误差越小，
 * 这个用来决定分桶的数量，因为最后会计算平均数（这里不是算术平均数，是调和平均数），如果桶越多，相当于样本越多
 * 也就是在概率上，用来做实验的样本基数越大，最后的值越趋近于理想值 */
#define HLL_P 14 /* The greater is P, the smaller the error. */
/* 高 50 位用来计算这 50 位的低位有多少个连续的 0 */
#define HLL_Q (64-HLL_P) /* The number of bits of the hash value used for
                            determining the number of leading zeros. */
/* 桶的数量 */
#define HLL_REGISTERS (1<<HLL_P) /* With P=14, 16384 registers. */
/* 桶数量的掩码 */
#define HLL_P_MASK (HLL_REGISTERS-1) /* Mask to index register. */
/* 以 6bits 作为一个桶的数据，用来记录当前桶中所有数据起始的连续的 0 的数量 */
#define HLL_BITS 6 /* Enough to count up to 63 leading zeroes. */
/* 桶数据的掩码 63 */
#define HLL_REGISTER_MAX ((1<<HLL_BITS)-1)
/* hllhdr 结构的头部大小 */
#define HLL_HDR_SIZE sizeof(struct hllhdr)
/* 头部加数据部分的总大小， 后面 +7 是因为整数除法会向下取整 */
#define HLL_DENSE_SIZE (HLL_HDR_SIZE+((HLL_REGISTERS*HLL_BITS+7)/8))
/* 密集编码 */
#define HLL_DENSE 0 /* Dense encoding. */
/* 稀疏编码 */
#define HLL_SPARSE 1 /* Sparse encoding. */
/* RAW 编码，仅 redis 内部使用，不会暴露出去 */
#define HLL_RAW 255 /* Only used internally, never exposed. */
#define HLL_MAX_ENCODING 1

static char *invalid_hll_err = "-INVALIDOBJ Corrupted HLL object detected";

/* =========================== Low level bit macros ========================= */

/* Macros to access the dense representation.
 *
 * We need to get and set 6 bit counters in an array of 8 bit bytes.
 * We use macros to make sure the code is inlined since speed is critical
 * especially in order to compute the approximated cardinality in
 * HLLCOUNT where we need to access all the registers at once.
 * For the same reason we also want to avoid conditionals in this code path.
 *
 * +--------+--------+--------+------//
 * |11000000|22221111|33333322|55444444
 * +--------+--------+--------+------//
 *
 * Note: in the above representation the most significant bit (MSB)
 * of every byte is on the left. We start using bits from the LSB to MSB,
 * and so forth passing to the next byte.
 *
 * Example, we want to access to counter at pos = 1 ("111111" in the
 * illustration above).
 *
 * The index of the first byte b0 containing our data is:
 *
 *  b0 = 6 * pos / 8 = 0
 *
 *   +--------+
 *   |11000000|  <- Our byte at b0
 *   +--------+
 *
 * The position of the first bit (counting from the LSB = 0) in the byte
 * is given by:
 *
 *  fb = 6 * pos % 8 -> 6
 *
 * Right shift b0 of 'fb' bits.
 *
 *   +--------+
 *   |11000000|  <- Initial value of b0
 *   |00000011|  <- After right shift of 6 pos.
 *   +--------+
 *
 * Left shift b1 of bits 8-fb bits (2 bits)
 *
 *   +--------+
 *   |22221111|  <- Initial value of b1
 *   |22111100|  <- After left shift of 2 bits.
 *   +--------+
 *
 * OR the two bits, and finally AND with 111111 (63 in decimal) to
 * clean the higher order bits we are not interested in:
 *
 *   +--------+
 *   |00000011|  <- b0 right shifted
 *   |22111100|  <- b1 left shifted
 *   |22111111|  <- b0 OR b1
 *   |  111111|  <- (b0 OR b1) AND 63, our value.
 *   +--------+
 *
 * We can try with a different example, like pos = 0. In this case
 * the 6-bit counter is actually contained in a single byte.
 *
 *  b0 = 6 * pos / 8 = 0
 *
 *   +--------+
 *   |11000000|  <- Our byte at b0
 *   +--------+
 *
 *  fb = 6 * pos % 8 = 0
 *
 *  So we right shift of 0 bits (no shift in practice) and
 *  left shift the next byte of 8 bits, even if we don't use it,
 *  but this has the effect of clearing the bits so the result
 *  will not be affected after the OR.
 *
 * -------------------------------------------------------------------------
 *
 * Setting the register is a bit more complex, let's assume that 'val'
 * is the value we want to set, already in the right range.
 *
 * We need two steps, in one we need to clear the bits, and in the other
 * we need to bitwise-OR the new bits.
 *
 * Let's try with 'pos' = 1, so our first byte at 'b' is 0,
 *
 * "fb" is 6 in this case.
 *
 *   +--------+
 *   |11000000|  <- Our byte at b0
 *   +--------+
 *
 * To create an AND-mask to clear the bits about this position, we just
 * initialize the mask with the value 63, left shift it of "fs" bits,
 * and finally invert the result.
 *
 *   +--------+
 *   |00111111|  <- "mask" starts at 63
 *   |11000000|  <- "mask" after left shift of "ls" bits.
 *   |00111111|  <- "mask" after invert.
 *   +--------+
 *
 * Now we can bitwise-AND the byte at "b" with the mask, and bitwise-OR
 * it with "val" left-shifted of "ls" bits to set the new bits.
 *
 * Now let's focus on the next byte b1:
 *
 *   +--------+
 *   |22221111|  <- Initial value of b1
 *   +--------+
 *
 * To build the AND mask we start again with the 63 value, right shift
 * it by 8-fb bits, and invert it.
 *
 *   +--------+
 *   |00111111|  <- "mask" set at 2&6-1
 *   |00001111|  <- "mask" after the right shift by 8-fb = 2 bits
 *   |11110000|  <- "mask" after bitwise not.
 *   +--------+
 *
 * Now we can mask it with b+1 to clear the old bits, and bitwise-OR
 * with "val" left-shifted by "rs" bits to set the new value.
 */

/* Note: if we access the last counter, we will also access the b+1 byte
 * that is out of the array, but sds strings always have an implicit null
 * term, so the byte exists, and we can skip the conditional (or the need
 * to allocate 1 byte more explicitly). */

/* Store the value of the register at position 'regnum' into variable 'target'.
 * 'p' is an array of unsigned bytes. */
/* 这里是获取某个桶的当前数据，可以看最开始的注释的第二部分，已经给出了该宏的计算方式
 * target 是计算后得到的数据
 * p 是 hllhdr 的 registers 字节数组
 * regnum 是给定的桶的下标 index*/
#define HLL_DENSE_GET_REGISTER(target,p,regnum) do { \
    uint8_t *_p = (uint8_t*) p; \
    unsigned long _byte = regnum*HLL_BITS/8; \
    unsigned long _fb = regnum*HLL_BITS&7; \
    unsigned long _fb8 = 8 - _fb; \
    unsigned long b0 = _p[_byte]; \
    unsigned long b1 = _p[_byte+1]; \
    target = ((b0 >> _fb) | (b1 << _fb8)) & HLL_REGISTER_MAX; \
} while(0)

/* Set the value of the register at position 'regnum' to 'val'.
 * 'p' is an array of unsigned bytes. */
/* 将数据存到到桶中
 * p 表示 hyperloglog 存储数据的部分，也就是 hllhdr 结构的 registers，一个字节数据
 * regnum 表示桶的索引 index
 * val 表示实际要存到桶里面的数据 count
 * regnum * HLL_BITS/8 获取桶所在字节数组中的开始字节位置
 * regnum * HLL_BITS&7 获取开始字节中桶的的结束 bit 位（需要从低位往高位看）
 * _p[_byte] &= ~(HLL_REGISTER_MAX << _fb)   将该字节中该桶的高 8 - _fb 位置 0
 * _p[_byte] |= _v << _fb   将给出的 regnum 的低 8 - _fb位赋值给该字节的高 8 - _fb 位
 * 最后两步计算和前面两部计算差不多，就是将 regnum 的高 6 - (8 - _fb) 位赋值给后面一个字节的低 6 - (8 - _fb) 位
 */
#define HLL_DENSE_SET_REGISTER(p,regnum,val) do { \
    uint8_t *_p = (uint8_t*) p; \
    unsigned long _byte = regnum*HLL_BITS/8; \
    unsigned long _fb = regnum*HLL_BITS&7; \
    unsigned long _fb8 = 8 - _fb; \
    unsigned long _v = val; \
    _p[_byte] &= ~(HLL_REGISTER_MAX << _fb); \
    _p[_byte] |= _v << _fb; \
    _p[_byte+1] &= ~(HLL_REGISTER_MAX >> _fb8); \
    _p[_byte+1] |= _v >> _fb8; \
} while(0)

/* Macros to access the sparse representation.
 * The macros parameter is expected to be an uint8_t pointer. */
/* 下面是稀疏类型编码使用到的宏，稀疏编码使用到三种操作码
 * 1) xzero 01xxxxxx yyyyyyyy 两个字节，前两位表示操作码类型，后面 14 位表示连续多个桶的值为 0，
 *    14 位大小刚好是所有桶的数量 16384，刚初始化的 hyperloglog 就是所有桶值都是0，也就是初始化的
 *    hyperloglog 数据只需要两个字节就可以存下
 * 2) zero 00xxxxxx 占一个字节，前两位表示操作码类型，后 6 位表示 1~64 个连续的桶的值为 0
 * 3) val 1vvvvvxx 占一个字节，第一位表示操作码类型，2~6 位表示值为多少，占 5 位，可以表示 1~32
 *    7,8 位表示连续几个桶。整个字节的意思就是，连续 xx 个桶的值为 vvvvvv
 */

/* xzero 操作码 */
#define HLL_SPARSE_XZERO_BIT 0x40 /* 01xxxxxx */
/* val 操作码 */
#define HLL_SPARSE_VAL_BIT 0x80 /* 1vvvvvxx */
/* 判断是否是 zero 操作码 */
#define HLL_SPARSE_IS_ZERO(p) (((*(p)) & 0xc0) == 0) /* 00xxxxxx */
/* 判断是否是 xzero 操作码 */
#define HLL_SPARSE_IS_XZERO(p) (((*(p)) & 0xc0) == HLL_SPARSE_XZERO_BIT)
/* 判断是否是 val 操作码 */
#define HLL_SPARSE_IS_VAL(p) ((*(p)) & HLL_SPARSE_VAL_BIT)
/* 获取 zero(00xxxxxx) 操作码表示的长度，也就是后 6 位的值 + 1，有多少个空桶 */
#define HLL_SPARSE_ZERO_LEN(p) (((*(p)) & 0x3f)+1)
/* 获取 xzero(01xxxxxx yyyyyyyy) 操作码表示的长度，获取当前字节的后六位然后将其前移 8 位，然后再获取后一个字节，进行或运算，最后在 + 1 */
#define HLL_SPARSE_XZERO_LEN(p) (((((*(p)) & 0x3f) << 8) | (*((p)+1)))+1)
/* 获取 val(1vvvvvxx) 类型操作码表示的值，左移两位清除 x，获取 5 位拿到 vvvvv，最后+1 */
#define HLL_SPARSE_VAL_VALUE(p) ((((*(p)) >> 2) & 0x1f)+1)
/* 获取 val 类型操作码的长度，获取最后 2 位 xx，再 +1 */
#define HLL_SPARSE_VAL_LEN(p) (((*(p)) & 0x3)+1)
/* val 操作码的最大值是 32，大于 32 后需要转成密集编码，稀疏编码最大存储的 count 值是 32 */
#define HLL_SPARSE_VAL_MAX_VALUE 32
/* val 操作码的最大长度 */
#define HLL_SPARSE_VAL_MAX_LEN 4
/* zero 操作码的最大长度 */
#define HLL_SPARSE_ZERO_MAX_LEN 64
/* xzero 操作码的最大长度 */
#define HLL_SPARSE_XZERO_MAX_LEN 16384
/* 设置 val 类型的数据 */
#define HLL_SPARSE_VAL_SET(p,val,len) do { \
    *(p) = (((val)-1)<<2|((len)-1))|HLL_SPARSE_VAL_BIT; \
} while(0)
/* 设置 zero 类型的数据 */
#define HLL_SPARSE_ZERO_SET(p,len) do { \
    *(p) = (len)-1; \
} while(0)
/* 设置 xzero 类型的数据 */
#define HLL_SPARSE_XZERO_SET(p,len) do { \
    int _l = (len)-1; \
    *(p) = (_l>>8) | HLL_SPARSE_XZERO_BIT; \
    *((p)+1) = (_l&0xff); \
} while(0)
/* 修正因子，用于计算 hyperloglog 最后的结果 */
#define HLL_ALPHA_INF 0.721347520444481703680 /* constant for 0.5/ln(2) */

/* ========================= HyperLogLog algorithm  ========================= */

/* Our hash function is MurmurHash2, 64 bit version.
 * It was modified for Redis in order to provide the same result in
 * big and little endian archs (endian neutral). */
/* 64 位的 MurmurHash2 函数，针对 redis 做了修改，以便在大端和小端字节序的情况下得到相同的结果 */
REDIS_NO_SANITIZE("alignment")
uint64_t MurmurHash64A (const void * key, int len, unsigned int seed) {
    const uint64_t m = 0xc6a4a7935bd1e995;
    const int r = 47;
    uint64_t h = seed ^ (len * m);
    const uint8_t *data = (const uint8_t *)key;
    const uint8_t *end = data + (len-(len&7));

    while(data != end) {
        uint64_t k;

/* 对不同的处理器字节序进行不同的处理 */
#if (BYTE_ORDER == LITTLE_ENDIAN)
    #ifdef USE_ALIGNED_ACCESS
        memcpy(&k,data,sizeof(uint64_t));
    #else
        k = *((uint64_t*)data);
    #endif
#else
        k = (uint64_t) data[0];
        k |= (uint64_t) data[1] << 8;
        k |= (uint64_t) data[2] << 16;
        k |= (uint64_t) data[3] << 24;
        k |= (uint64_t) data[4] << 32;
        k |= (uint64_t) data[5] << 40;
        k |= (uint64_t) data[6] << 48;
        k |= (uint64_t) data[7] << 56;
#endif

        k *= m;
        k ^= k >> r;
        k *= m;
        h ^= k;
        h *= m;
        data += 8;
    }

    switch(len & 7) {
    case 7: h ^= (uint64_t)data[6] << 48; /* fall-thru */
    case 6: h ^= (uint64_t)data[5] << 40; /* fall-thru */
    case 5: h ^= (uint64_t)data[4] << 32; /* fall-thru */
    case 4: h ^= (uint64_t)data[3] << 24; /* fall-thru */
    case 3: h ^= (uint64_t)data[2] << 16; /* fall-thru */
    case 2: h ^= (uint64_t)data[1] << 8; /* fall-thru */
    case 1: h ^= (uint64_t)data[0];
            h *= m; /* fall-thru */
    };

    h ^= h >> r;
    h *= m;
    h ^= h >> r;
    return h;
}

/* Given a string element to add to the HyperLogLog, returns the length
 * of the pattern 000..1 of the element hash. As a side effect 'regp' is
 * set to the register index this element hashes to. */
/* 用给定的元素 ele 做 hash 化，然后获取到 index 存到 regp 中，获取到 hash 化后的数字的高位连续的 0 的个数作为返回值 */
int hllPatLen(unsigned char *ele, size_t elesize, long *regp) {
    uint64_t hash, bit, index;
    int count;

    /* Count the number of zeroes starting from bit HLL_REGISTERS
     * (that is a power of two corresponding to the first bit we don't use
     * as index). The max run can be 64-P+1 = Q+1 bits.
     *
     * Note that the final "1" ending the sequence of zeroes must be
     * included in the count, so if we find "001" the count is 3, and
     * the smallest count possible is no zeroes at all, just a 1 bit
     * at the first position, that is a count of 1.
     *
     * This may sound like inefficient, but actually in the average case
     * there are high probabilities to find a 1 after a few iterations. */
    /* 将给定的 ele 散列化 */
    hash = MurmurHash64A(ele,elesize,0xadc83b19ULL);
    /* 获取散列化后的数据的低 14 位作为 index */
    index = hash & HLL_P_MASK; /* Register index. */
    /* 取 hash 值的高 50 位 */
    hash >>= HLL_P; /* Remove bits used to address the register. */
    /* 在高 50 位前添加一个 bit 位并置为 1，便于 50 位都是 0 的情况下可以终止循环 */
    hash |= ((uint64_t)1<<HLL_Q); /* Make sure the loop terminates
                                     and count will be <= Q+1. */
    bit = 1;
    /* 定义 count 记录第多少位才出现 1 */
    count = 1; /* Initialized to 1 since we count the "00000...1" pattern. */
    /* 从低位向高位判断，直到出现第一个 1，退出循环 */
    while((hash & bit) == 0) {
        /* 计数 +1 */
        count++;
        /* 每次循环向右移以为 */
        bit <<= 1;
    }
    /* 将 index 赋值给 regp */
    *regp = (int) index;
    /* 返回计算得到的 count */
    return count;
}

/* ================== Dense representation implementation  ================== */

/* Low level function to set the dense HLL register at 'index' to the
 * specified value if the current value is smaller than 'count'.
 *
 * 'registers' is expected to have room for HLL_REGISTERS plus an
 * additional byte on the right. This requirement is met by sds strings
 * automatically since they are implicitly null terminated.
 *
 * The function always succeed, however if as a result of the operation
 * the approximated cardinality changed, 1 is returned. Otherwise 0
 * is returned. */
/* 该函数用于密集型编码设置值 */
int hllDenseSet(uint8_t *registers, long index, uint8_t count) {
    uint8_t oldcount;
    /* 获取到 index 号桶中的值 */
    HLL_DENSE_GET_REGISTER(oldcount,registers,index);
    /* 如果当前给定的值 count 大于桶中的值 */
    if (count > oldcount) {
        /* 替换桶中的值为给定的值 count */
        HLL_DENSE_SET_REGISTER(registers,index,count);
        return 1;
    } else {
        return 0;
    }
}

/* "Add" the element in the dense hyperloglog data structure.
 * Actually nothing is added, but the max 0 pattern counter of the subset
 * the element belongs to is incremented if needed.
 *
 * This is just a wrapper to hllDenseSet(), performing the hashing of the
 * element in order to retrieve the index and zero-run count. */
/* 向 hyperloglog 数据结构中添加一个元素 */
int hllDenseAdd(uint8_t *registers, unsigned char *ele, size_t elesize) {
    long index;
    /* 计算桶的位置，以及获取该元素的 count 值 */
    uint8_t count = hllPatLen(ele,elesize,&index);
    /* Update the register if this element produced a longer run of zeroes. */
    /* 如果该元素的 count 值大于桶中的值，就替换掉桶中的值 */
    return hllDenseSet(registers,index,count);
}

/* Compute the register histogram in the dense representation. */
/* 计算密集型 hyperloglog 的数据直方图，registers 是实际的数据，reghisto 是计算后得到的直方图 */
void hllDenseRegHisto(uint8_t *registers, int* reghisto) {
    int j;

    /* Redis default is to use 16384 registers 6 bits each. The code works
     * with other values by modifying the defines, but for our target value
     * we take a faster path with unrolled loops. */
    /* redis 默认 16384 个桶和每个桶 6bit，并针对这种情况做了优化处理 */
    if (HLL_REGISTERS == 16384 && HLL_BITS == 6) {
        uint8_t *r = registers;
        unsigned long r0, r1, r2, r3, r4, r5, r6, r7, r8, r9,
                      r10, r11, r12, r13, r14, r15;
        /* 循环 1024 次，每次处理 16 个桶，
         * 计算优化：
         * 1) 每四个桶占用三个字节，存在自然规律，不需要使用 HLL_DENSE_GET_REGISTER 强行计算每个桶的值（可以看到计算过程明显减少了）
         * 2) 每次循环处理 12 个字节也就是 16 个桶，使用了循环展开的优化方式来减少循环开销 */
        for (j = 0; j < 1024; j++) {
            /* Handle 16 registers per iteration. */
            /* 下面所有的计算 & 63 会将字节高 2 位的数据清除掉 */
            /* 获取第 0 个字节的后 6 位数据，这是第一个桶的数据 */
            r0 = r[0] & 63;
            /* 获取第 0 个字节的前 2 位和第 1 个字节的后 4 位，拼接出第二个桶的数据 */
            r1 = (r[0] >> 6 | r[1] << 2) & 63; // 0
            /* 获取第 1 个字节的前 4 位和第 2 个字节的后 2 位，拼接出第三个桶的数据 */
            r2 = (r[1] >> 4 | r[2] << 4) & 63;
            /* 获取第 2 个字节的前 6 位作为第四个桶的数据 */
            r3 = (r[2] >> 2) & 63;

            /* 下面的计算方式一样了，每四个桶占三个字节，作为一轮计算 */

            r4 = r[3] & 63;
            r5 = (r[3] >> 6 | r[4] << 2) & 63;
            r6 = (r[4] >> 4 | r[5] << 4) & 63;
            r7 = (r[5] >> 2) & 63;
            r8 = r[6] & 63;
            r9 = (r[6] >> 6 | r[7] << 2) & 63;
            r10 = (r[7] >> 4 | r[8] << 4) & 63;
            r11 = (r[8] >> 2) & 63;
            r12 = r[9] & 63;
            r13 = (r[9] >> 6 | r[10] << 2) & 63;
            r14 = (r[10] >> 4 | r[11] << 4) & 63;
            r15 = (r[11] >> 2) & 63;

            /* 这里是统计 count 值相同的桶的数量，做成一个直方图 */
            reghisto[r0]++;
            reghisto[r1]++;
            reghisto[r2]++;
            reghisto[r3]++;
            reghisto[r4]++;
            reghisto[r5]++;
            reghisto[r6]++;
            reghisto[r7]++;
            reghisto[r8]++;
            reghisto[r9]++;
            reghisto[r10]++;
            reghisto[r11]++;
            reghisto[r12]++;
            reghisto[r13]++;
            reghisto[r14]++;
            reghisto[r15]++;
            /* 每次循环处理 12 个字节，也就是 16 个桶 */
            r += 12;
        }
    } else {
        /* 这里就是直接计算每个桶的值，然后做直方图 */
        for(j = 0; j < HLL_REGISTERS; j++) {
            unsigned long reg;
            HLL_DENSE_GET_REGISTER(reg,registers,j);
            reghisto[reg]++;
        }
    }
}

/* ================== Sparse representation implementation  ================= */

/* Convert the HLL with sparse representation given as input in its dense
 * representation. Both representations are represented by SDS strings, and
 * the input representation is freed as a side effect.
 *
 * The function returns C_OK if the sparse representation was valid,
 * otherwise C_ERR is returned if the representation was corrupted. */
/* 将稀疏型编码转成密集型编码 */
int hllSparseToDense(robj *o) {
    sds sparse = o->ptr, dense;
    struct hllhdr *hdr, *oldhdr = (struct hllhdr*)sparse;
    int idx = 0, runlen, regval;
    uint8_t *p = (uint8_t*)sparse, *end = p+sdslen(sparse);

    /* If the representation is already the right one return ASAP. */
    hdr = (struct hllhdr*) sparse;
    /* 如果编码模式就是密集型，直接返回 */
    if (hdr->encoding == HLL_DENSE) return C_OK;

    /* Create a string of the right size filled with zero bytes.
     * Note that the cached cardinality is set to 0 as a side effect
     * that is exactly the cardinality of an empty HLL. */
    /* 重新生成一个密集型的 hllhdr 结构 */
    dense = sdsnewlen(NULL,HLL_DENSE_SIZE);
    hdr = (struct hllhdr*) dense;
    /* 将魔数和之前保留的计算结果复制过来 */
    *hdr = *oldhdr; /* This will copy the magic and cached cardinality. */
    /* 设置编码类型 */
    hdr->encoding = HLL_DENSE;

    /* Now read the sparse representation and set non-zero registers
     * accordingly. */
    p += HLL_HDR_SIZE;
    /* 根据不同的编码类型做不同的处理，zero 和 xzero 表示的都是空桶，所以只需要移动游标就行了 */
    while(p < end) {
        /* zero 操作码 */
        if (HLL_SPARSE_IS_ZERO(p)) {
            /* 获取 zero 类型表示的长度 */
            runlen = HLL_SPARSE_ZERO_LEN(p);
            idx += runlen;
            p++;
        } else if (HLL_SPARSE_IS_XZERO(p)) {
            /* 获取 xzero 类型表示的长度 */
            runlen = HLL_SPARSE_XZERO_LEN(p);
            idx += runlen;
            /* xzero 占两个字节 +2 */
            p += 2;
        } else {
            /* 获取 val 类型表示的长度 */
            runlen = HLL_SPARSE_VAL_LEN(p);
            /* 获取 val 类型表示的值 */
            regval = HLL_SPARSE_VAL_VALUE(p);
            /* 超过了桶的总数了，退出循环 */
            if ((runlen + idx) > HLL_REGISTERS) break; /* Overflow. */
            /* val 类型存在具体的值，所以需要使用密集型编码的形式来填充数据 */
            while(runlen--) {
                HLL_DENSE_SET_REGISTER(hdr->registers,idx,regval);
                idx++;
            }
            p++;
        }
    }

    /* If the sparse representation was valid, we expect to find idx
     * set to HLL_REGISTERS. */
    /* 如果最后解析出来的桶的数量不正确，报错 */
    if (idx != HLL_REGISTERS) {
        sdsfree(dense);
        return C_ERR;
    }

    /* Free the old representation and set the new one. */
    /* 释放旧的 hyperloglog 对象 */
    sdsfree(o->ptr);
    o->ptr = dense;
    return C_OK;
}

/* Low level function to set the sparse HLL register at 'index' to the
 * specified value if the current value is smaller than 'count'.
 *
 * The object 'o' is the String object holding the HLL. The function requires
 * a reference to the object in order to be able to enlarge the string if
 * needed.
 *
 * On success, the function returns 1 if the cardinality changed, or 0
 * if the register for this element was not updated.
 * On error (if the representation is invalid) -1 is returned.
 *
 * As a side effect the function may promote the HLL representation from
 * sparse to dense: this happens when a register requires to be set to a value
 * not representable with the sparse representation, or when the resulting
 * size would be greater than server.hll_sparse_max_bytes. */
/* 向稀疏编码 hyperloglog 中设置一个数据 */
int hllSparseSet(robj *o, long index, uint8_t count) {
    struct hllhdr *hdr;
    uint8_t oldcount, *sparse, *end, *p, *prev, *next;
    long first, span;
    long is_zero = 0, is_xzero = 0, is_val = 0, runlen = 0;

    /* If the count is too big to be representable by the sparse representation
     * switch to dense representation. */
    /* 超出了 val 操作码的最大值，需要将该 hyperloglog 转成密集型编码 */
    if (count > HLL_SPARSE_VAL_MAX_VALUE) goto promote;

    /* When updating a sparse representation, sometimes we may need to
     * enlarge the buffer for up to 3 bytes in the worst case (XZERO split
     * into XZERO-VAL-XZERO). Make sure there is enough space right now
     * so that the pointers we take during the execution of the function
     * will be valid all the time. */
    /* 这里是可能存在添加一个数据后导致 一个 xzero 操作码变成 xzero-val-xzero 三个操作码
     * 这会多出 3 个字节，所以需要先预留 3 字节空间 */
    o->ptr = sdsMakeRoomFor(o->ptr,3);

    /* Step 1: we need to locate the opcode we need to modify to check
     * if a value update is actually needed. */
    /* 需要先找到 index 位置的桶  */
    /* 这里就是将指针 p 指向 hllhdr 的 registers 数据部分 */
    sparse = p = ((uint8_t*)o->ptr) + HLL_HDR_SIZE;
    /* end 指向数据的结尾 */
    end = p + sdslen(o->ptr) - HLL_HDR_SIZE;

    /* 记录当前遍历了多少个桶了 */
    first = 0;
    prev = NULL; /* Points to previous opcode at the end of the loop. */
    next = NULL; /* Points to the next opcode at the end of the loop. */
    /* 记录当前遍历到的操作码表示的桶的数量 */
    span = 0;
    /* 开始遍历所有数据 */
    while(p < end) {
        long oplen;

        /* Set span to the number of registers covered by this opcode.
         *
         * This is the most performance critical loop of the sparse
         * representation. Sorting the conditionals from the most to the
         * least frequent opcode in many-bytes sparse HLLs is faster. */
        oplen = 1;
        /* 不同的编码表示的桶的数量 */
        if (HLL_SPARSE_IS_ZERO(p)) {
            span = HLL_SPARSE_ZERO_LEN(p);
        } else if (HLL_SPARSE_IS_VAL(p)) {
            span = HLL_SPARSE_VAL_LEN(p);
        } else { /* XZERO. */
            span = HLL_SPARSE_XZERO_LEN(p);
            /* xzero 占两个字节 */
            oplen = 2;
        }
        /* Break if this opcode covers the register as 'index'. */
        /* 如果遍历了的桶的数量 >= 数据需要存到的桶的索引 */
        if (index <= first+span-1) break;
        /* 将 prev 指针指向当前遍历到的字节 */
        prev = p;
        /* 移动指针到下一个操作码 */
        p += oplen;
        /* 遍历的桶的数量增加 */
        first += span;
    }

    /* 出循环了，表示找到了要找的位置了，
     * 记住：
     * p 目前指向找到的操作码字节，
     * prev 指向 p 的前一个操作码
     * first 表示 p 之前有多少桶，不包括 p 指向的操作码的桶数量
     * span 表示 p 指向的操作码有多少个桶 */

    /* 如果出现了非法的值，直接返回 */
    if (span == 0 || p >= end) return -1; /* Invalid format. */

    /* next 指向 p 的下一个操作码，如果没有下一个，设置为 NULL */
    next = HLL_SPARSE_IS_XZERO(p) ? p+2 : p+1;
    if (next >= end) next = NULL;

    /* Cache current opcode type to avoid using the macro again and
     * again for something that will not change.
     * Also cache the run-length of the opcode. */
    /* 记录当前操作码的类型，和操作码表示的桶的数量 */
    if (HLL_SPARSE_IS_ZERO(p)) {
        is_zero = 1;
        runlen = HLL_SPARSE_ZERO_LEN(p);
    } else if (HLL_SPARSE_IS_XZERO(p)) {
        is_xzero = 1;
        runlen = HLL_SPARSE_XZERO_LEN(p);
    } else {
        is_val = 1;
        runlen = HLL_SPARSE_VAL_LEN(p);
    }

    /* Step 2: After the loop:
     *
     * 'first' stores to the index of the first register covered
     *  by the current opcode, which is pointed by 'p'.
     *
     * 'next' ad 'prev' store respectively the next and previous opcode,
     *  or NULL if the opcode at 'p' is respectively the last or first.
     *
     * 'span' is set to the number of registers covered by the current
     *  opcode.
     *
     * There are different cases in order to update the data structure
     * in place without generating it from scratch:
     *
     * A) If it is a VAL opcode already set to a value >= our 'count'
     *    no update is needed, regardless of the VAL run-length field.
     *    In this case PFADD returns 0 since no changes are performed.
     *
     * B) If it is a VAL opcode with len = 1 (representing only our
     *    register) and the value is less than 'count', we just update it
     *    since this is a trivial case. */
    /* 如果是 val 操作码 */
    if (is_val) {
        /* 获取该操作码目前存储的值 */
        oldcount = HLL_SPARSE_VAL_VALUE(p);
        /* Case A. */
        /* 如果给定的 count 不大于目前存储的值，直接返回，不需要存储 */
        if (oldcount >= count) return 0;

        /* Case B. */
        /* count 大于目前存储的值的情况，且该 val 操作码只有一个桶 */
        if (runlen == 1) {
            /* 设置值 */
            HLL_SPARSE_VAL_SET(p,count,1);
            goto updated;
        }
    }

    /* C) Another trivial to handle case is a ZERO opcode with a len of 1.
     * We can just replace it with a VAL opcode with our value and len of 1. */
    /* 如果是 zero 操作码，且表示的长度为 1 */
    if (is_zero && runlen == 1) {
        /* 将 p 指向的字节改成 val 操作码，并添加数据 */
        HLL_SPARSE_VAL_SET(p,count,1);
        goto updated;
    }

    /* D) General case.
     *
     * The other cases are more complex: our register requires to be updated
     * and is either currently represented by a VAL opcode with len > 1,
     * by a ZERO opcode with len > 1, or by an XZERO opcode.
     *
     * In those cases the original opcode must be split into multiple
     * opcodes. The worst case is an XZERO split in the middle resulting into
     * XZERO - VAL - XZERO, so the resulting sequence max length is
     * 5 bytes.
     *
     * We perform the split writing the new sequence into the 'new' buffer
     * with 'newlen' as length. Later the new sequence is inserted in place
     * of the old one, possibly moving what is on the right a few bytes
     * if the new sequence is longer than the older one. */
    /* 下面开始处理一般的情况 */
    /* 定义了seq，之前说了，最坏的情况下会从 xzero 操作码变成 xzero-val-xzero 三个操作码，三个操作码占 5 个字节
     * 这里使用 seq 来临时存储操作码被分割后形成的操作码，指针 n 指向该数组的起始位置 */
    uint8_t seq[5], *n = seq;
    int last = first+span-1; /* Last register covered by the sequence. */
    int len;

    /* 下面的逻辑在进行分割的时候，会将分割后形成的操作码通过指针 n 存入 seq 字节数组中 */
    if (is_zero || is_xzero) {
        /* Handle splitting of ZERO / XZERO. */
        /* 开始处理 zero 和 xzero 操作码类型 */
        if (index != first) {
            /* 如果 index 不是当前操作码的第一个桶位置 */
            /* 获取 index 指向的桶在当前操作码中的偏移量 */
            len = index-first;
            /* 判断上面得到的偏移量是否小于 zero 能表示的最大长度，如果大于就用 zero 来存储该偏移量之前的桶
             * 否则用 xzero 来存储该偏移量之前的桶*/
            if (len > HLL_SPARSE_ZERO_MAX_LEN) {
                /* 新建一个 xzero 操作码，存储的桶数量是偏移量 len */
                HLL_SPARSE_XZERO_SET(n,len);
                /* 字节数 +2 */
                n += 2;
            } else {
                /* 新建一个 zero 操作码 */
                HLL_SPARSE_ZERO_SET(n,len);
                n++;
            }
        }
        /* 上面已经处理当前操作码在 index 之前的桶，这里可以把 count 添加到 index 桶中了，创建一个 val 操作码 */
        HLL_SPARSE_VAL_SET(n,count,1);
        n++;
        /* 处理当前操作码 index 之后的桶，和处理 index 之前的桶逻辑一样 */
        if (index != last) {
            len = last-index;
            if (len > HLL_SPARSE_ZERO_MAX_LEN) {
                HLL_SPARSE_XZERO_SET(n,len);
                n += 2;
            } else {
                HLL_SPARSE_ZERO_SET(n,len);
                n++;
            }
        }
    } else {
        /* Handle splitting of VAL. */
        /* 操作码是 val 类型的情况 */
        /* 获取当前操作码的值 */
        int curval = HLL_SPARSE_VAL_VALUE(p);
        /* 处理当前操作码在 index 之前的桶 */
        if (index != first) {
            len = index-first;
            HLL_SPARSE_VAL_SET(n,curval,len);
            n++;
        }
        /* 设置值 */
        HLL_SPARSE_VAL_SET(n,count,1);
        n++;
        /* 处理当前操作码在 index 之后的桶  */
        if (index != last) {
            len = last-index;
            HLL_SPARSE_VAL_SET(n,curval,len);
            n++;
        }
    }

    /* Step 3: substitute the new sequence with the old one.
     *
     * Note that we already allocated space on the sds string
     * calling sdsMakeRoomFor(). */
    /* n - seq 其实就是分割之后形成的多个操作码占用的字节数 */
     int seqlen = n-seq;
     /* 分割之前占用的字节数 */
     int oldlen = is_xzero ? 2 : 1;
     /* 分割前后字节数的差值 */
     int deltalen = seqlen-oldlen;

     /* 如果分割之后该 hyperloglog 结构占的总字节数超过了当前 redis 配置稀疏编码类型字节数上限了
      * 转成密集型编码 */
     if (deltalen > 0 &&
         sdslen(o->ptr)+deltalen > server.hll_sparse_max_bytes) goto promote;
     /* 如果差值存在，且当前操作码不是最后一个操作码，需要移动出差值个字节空间出来放置新的操作码 */
     if (deltalen && next) memmove(next+deltalen,next,end-next);
     /* 修改 sds 的使用长度，添加差值大小 */
     sdsIncrLen(o->ptr,deltalen);
     /* 把分割后生成的操作码插入 hyperloglog 的字节数组中 */
     memcpy(p,seq,seqlen);
     end += deltalen;

updated:
    /* Step 4: Merge adjacent values if possible.
     *
     * The representation was updated, however the resulting representation
     * may not be optimal: adjacent VAL opcodes can sometimes be merged into
     * a single one. */
    /* 这里是整合相邻的 val 操作码，添加数据之后，会生成新的 val 操作码，会判断它是否可以和相邻的 val 操作码合并 */
    /* 这里会判断是否存在 prev，因为要合并相邻的操作码，肯定需要看他前面的操作码
     * 这里 prev 不存在的情况就是给定的 index 位置的桶刚好在 hyperloglog 的第一个操作码中，就直接从头开始遍历 */
    p = prev ? prev : sparse;
    /* 从 prev 开始扫描 5 个操作码 */
    int scanlen = 5; /* Scan up to 5 upcodes starting from prev. */
    while (p < end && scanlen--) {
        /* 我们需要整合的是 val 操作码，如果操作码是 xzero 或者 zero 类型，直接跳过 */
        if (HLL_SPARSE_IS_XZERO(p)) {
            p += 2;
            continue;
        } else if (HLL_SPARSE_IS_ZERO(p)) {
            p++;
            continue;
        }
        /* 走到这里，说明当前遍历到的操作码是 val 类型 */
        /* We need two adjacent VAL opcodes to try a merge, having
         * the same value, and a len that fits the VAL opcode max len. */
        /* 如果下一个操作码也是 val 类型 */
        if (p+1 < end && HLL_SPARSE_IS_VAL(p+1)) {
            /* 获取两个 val 操作码的值 */
            int v1 = HLL_SPARSE_VAL_VALUE(p);
            int v2 = HLL_SPARSE_VAL_VALUE(p+1);
            /* 如果值相同 */
            if (v1 == v2) {
                /* 判断合并后的总长度会不会大于 val 操作码能表示的最大长度 */
                int len = HLL_SPARSE_VAL_LEN(p)+HLL_SPARSE_VAL_LEN(p+1);
                if (len <= HLL_SPARSE_VAL_MAX_LEN) {
                    /* 如果可以合并，将其合并到后一个 val 操作码上 */
                    HLL_SPARSE_VAL_SET(p+1,v1,len);
                    /* 移除掉第一个操作码的空间 */
                    memmove(p,p+1,end-p);
                    /* sds 设置被使用的长度 -1 个字节 */
                    sdsIncrLen(o->ptr,-1);
                    end--;
                    /* After a merge we reiterate without incrementing 'p'
                     * in order to try to merge the just merged value with
                     * a value on its right. */
                    continue;
                }
            }
        }
        p++;
    }

    /* Invalidate the cached cardinality. */
    hdr = o->ptr;
    HLL_INVALIDATE_CACHE(hdr);
    return 1;

promote: /* Promote to dense representation. */
    /* 转成密集型编码 */
    if (hllSparseToDense(o) == C_ERR) return -1; /* Corrupted HLL. */
    hdr = o->ptr;

    /* We need to call hllDenseAdd() to perform the operation after the
     * conversion. However the result must be 1, since if we need to
     * convert from sparse to dense a register requires to be updated.
     *
     * Note that this in turn means that PFADD will make sure the command
     * is propagated to slaves / AOF, so if there is a sparse -> dense
     * conversion, it will be performed in all the slaves as well. */
    /* 调用密集型编码形式设置值 */
    int dense_retval = hllDenseSet(hdr->registers,index,count);
    serverAssert(dense_retval == 1);
    return dense_retval;
}

/* "Add" the element in the sparse hyperloglog data structure.
 * Actually nothing is added, but the max 0 pattern counter of the subset
 * the element belongs to is incremented if needed.
 *
 * This function is actually a wrapper for hllSparseSet(), it only performs
 * the hashing of the element to obtain the index and zeros run length. */
/* 添加一个数据到 hyperloglog 数据结构中，编码类型是稀疏类型 */
int hllSparseAdd(robj *o, unsigned char *ele, size_t elesize) {
    long index;
    /* 计算 index 和 count 值 */
    uint8_t count = hllPatLen(ele,elesize,&index);
    /* Update the register if this element produced a longer run of zeroes. */
    /* 更新数据结构的数据 */
    return hllSparseSet(o,index,count);
}

/* Compute the register histogram in the sparse representation. */
/* 计算稀疏编码 hyperloglog 的直方图 */
void hllSparseRegHisto(uint8_t *sparse, int sparselen, int *invalid, int* reghisto) {
    int idx = 0, runlen, regval;
    uint8_t *end = sparse+sparselen, *p = sparse;

    while(p < end) {
        /* 如果是 zero 和 xzero 只需要计算该操作码的长度加到 reghisto[0] 中就行了
         * 如果是 val，获取操作码的值 regval，然后给 reghisto[regval] 中加上对应的 runlen */
        if (HLL_SPARSE_IS_ZERO(p)) {
            runlen = HLL_SPARSE_ZERO_LEN(p);
            idx += runlen;
            reghisto[0] += runlen;
            p++;
        } else if (HLL_SPARSE_IS_XZERO(p)) {
            runlen = HLL_SPARSE_XZERO_LEN(p);
            idx += runlen;
            reghisto[0] += runlen;
            p += 2;
        } else {
            runlen = HLL_SPARSE_VAL_LEN(p);
            regval = HLL_SPARSE_VAL_VALUE(p);
            idx += runlen;
            reghisto[regval] += runlen;
            p++;
        }
    }
    if (idx != HLL_REGISTERS && invalid) *invalid = 1;
}

/* ========================= HyperLogLog Count ==============================
 * This is the core of the algorithm where the approximated count is computed.
 * The function uses the lower level hllDenseRegHisto() and hllSparseRegHisto()
 * functions as helpers to compute histogram of register values part of the
 * computation, which is representation-specific, while all the rest is common. */

/* Implements the register histogram calculation for uint8_t data type
 * which is only used internally as speedup for PFCOUNT with multiple keys. */
/* 这里是计算 raw 编码方式的直方图，可以看到每个桶是使用一个字节来存储值 */
void hllRawRegHisto(uint8_t *registers, int* reghisto) {
    /* 用 word 指针指向 registers 数组，并转成每个元素占 8 个字节 */
    uint64_t *word = (uint64_t*) registers;
    uint8_t *bytes;
    int j;
    /* 每次循环处理 8 个桶 */
    for (j = 0; j < HLL_REGISTERS/8; j++) {
        if (*word == 0) {
            reghisto[0] += 8;
        } else {
            /* 一个 word 元素占 8 个字节，将其转换成一个长度为 8 的字节数组 */
            bytes = (uint8_t*) word;
            reghisto[bytes[0]]++;
            reghisto[bytes[1]]++;
            reghisto[bytes[2]]++;
            reghisto[bytes[3]]++;
            reghisto[bytes[4]]++;
            reghisto[bytes[5]]++;
            reghisto[bytes[6]]++;
            reghisto[bytes[7]]++;
        }
        /* 指针后移 */
        word++;
    }
}

/* Helper function sigma as defined in
 * "New cardinality estimation algorithms for HyperLogLog sketches"
 * Otmar Ertl, arXiv:1702.01284 */
double hllSigma(double x) {
    if (x == 1.) return INFINITY;
    double zPrime;
    double y = 1;
    double z = x;
    do {
        x *= x;
        zPrime = z;
        z += x * y;
        y += y;
    } while(zPrime != z);
    return z;
}

/* Helper function tau as defined in
 * "New cardinality estimation algorithms for HyperLogLog sketches"
 * Otmar Ertl, arXiv:1702.01284 */
double hllTau(double x) {
    if (x == 0. || x == 1.) return 0.;
    double zPrime;
    double y = 1.0;
    double z = 1 - x;
    do {
        x = sqrt(x);
        zPrime = z;
        y *= 0.5;
        z -= pow(1 - x, 2)*y;
    } while(zPrime != z);
    return z / 3;
}

/* Return the approximated cardinality of the set based on the harmonic
 * mean of the registers values. 'hdr' points to the start of the SDS
 * representing the String object holding the HLL representation.
 *
 * If the sparse representation of the HLL object is not valid, the integer
 * pointed by 'invalid' is set to non-zero, otherwise it is left untouched.
 *
 * hllCount() supports a special internal-only encoding of HLL_RAW, that
 * is, hdr->registers will point to an uint8_t array of HLL_REGISTERS element.
 * This is useful in order to speedup PFCOUNT when called against multiple
 * keys (no need to work with 6-bit integers encoding). */
/* 获取当前 hyperloglog 中计数值 */
uint64_t hllCount(struct hllhdr *hdr, int *invalid) {
    double m = HLL_REGISTERS;
    double E;
    int j;
    /* Note that reghisto size could be just HLL_Q+2, because HLL_Q+1 is
     * the maximum frequency of the "000...1" sequence the hash function is
     * able to return. However it is slow to check for sanity of the
     * input: instead we history array at a safe size: overflows will
     * just write data to wrong, but correctly allocated, places. */
    /* reghisto 数组，数组索引可以看做是桶的数据，数组的数据就是有多少个桶的数据是该位置索引的大小
     * 数组大小是 64，其实只有第 1 ~ 51 号位置被使用了 */
    int reghisto[64] = {0};

    /* Compute register histogram */
    /* 根据不同的编码类型调用其计算数据的直方图函数，计算的结果存入 reghisto 数组中 */
    if (hdr->encoding == HLL_DENSE) {
        hllDenseRegHisto(hdr->registers,reghisto);
    } else if (hdr->encoding == HLL_SPARSE) {
        hllSparseRegHisto(hdr->registers,
                         sdslen((sds)hdr)-HLL_HDR_SIZE,invalid,reghisto);
    } else if (hdr->encoding == HLL_RAW) {
        hllRawRegHisto(hdr->registers,reghisto);
    } else {
        serverPanic("Unknown HyperLogLog encoding in hllCount()");
    }

    /* Estimate cardinality from register histogram. See:
     * "New cardinality estimation algorithms for HyperLogLog sketches"
     * Otmar Ertl, arXiv:1702.01284 */
    /* 根据直方图估算出 cardinality 值，作为当前 hyperloglog 的计数值 */
    /* m - reghisto[HLL_Q + 1] 这里是减去出现连续 50 个 0 的数据的桶的数量
     * 注：出现连续 50 个 0 在理想情况下可以认为该桶的数据 >= 2^51，基本上不会出现这种情况
     * 而 hllTau 函数就是针对这种情况做的处理，
     * 可以先认为这里就是 m * hllTau(m / m) 来看后续处理， hllTau(1) = 0 也就是 z = 0 */
    double z = m * hllTau((m-reghisto[HLL_Q+1])/(double)m);
    /* 该 for 循环的计算方式，这里其实就是调和平均数的分母计算方式
     * (2^0 * r[HLL_Q - 0] + 2^1 * r[HLL_Q - 1] + 2^2 * r[HLL_Q - 2] + ***** + 2^(HLL_Q - 1) * r[1]) / 2^HLL_Q */
    for (j = HLL_Q; j >= 1; --j) {
        z += reghisto[j];
        z *= 0.5;
    }
    /* 第 0 号位的数据，这里其实就是桶没有任何数据的情况，没有任何数据进过桶，桶的值就是 0 */
    z += m * hllSigma(reghisto[0]/(double)m);
    /* 计算结果 E = HLL_ALPHA_INF(修正因子) * m(桶的数量) * m/z(调和平均数加上了两种极端情况的影响) */
    E = llroundl(HLL_ALPHA_INF*m*m/z);

    return (uint64_t) E;
}

/* Call hllDenseAdd() or hllSparseAdd() according to the HLL encoding. */
/* 向 hyperloglog 结构中添加一条记录 */
int hllAdd(robj *o, unsigned char *ele, size_t elesize) {
    struct hllhdr *hdr = o->ptr;
    /* 根据不同的编码调用其具体的添加数据的函数 */
    switch(hdr->encoding) {
    case HLL_DENSE: return hllDenseAdd(hdr->registers,ele,elesize);
    case HLL_SPARSE: return hllSparseAdd(o,ele,elesize);
    default: return -1; /* Invalid representation. */
    }
}

/* Merge by computing MAX(registers[i],hll[i]) the HyperLogLog 'hll'
 * with an array of uint8_t HLL_REGISTERS registers pointed by 'max'.
 *
 * The hll object must be already validated via isHLLObjectOrReply()
 * or in some other way.
 *
 * If the HyperLogLog is sparse and is found to be invalid, C_ERR
 * is returned, otherwise the function always succeeds. */
int hllMerge(uint8_t *max, robj *hll) {
    struct hllhdr *hdr = hll->ptr;
    int i;

    if (hdr->encoding == HLL_DENSE) {
        uint8_t val;

        for (i = 0; i < HLL_REGISTERS; i++) {
            HLL_DENSE_GET_REGISTER(val,hdr->registers,i);
            if (val > max[i]) max[i] = val;
        }
    } else {
        uint8_t *p = hll->ptr, *end = p + sdslen(hll->ptr);
        long runlen, regval;

        p += HLL_HDR_SIZE;
        i = 0;
        while(p < end) {
            if (HLL_SPARSE_IS_ZERO(p)) {
                runlen = HLL_SPARSE_ZERO_LEN(p);
                i += runlen;
                p++;
            } else if (HLL_SPARSE_IS_XZERO(p)) {
                runlen = HLL_SPARSE_XZERO_LEN(p);
                i += runlen;
                p += 2;
            } else {
                runlen = HLL_SPARSE_VAL_LEN(p);
                regval = HLL_SPARSE_VAL_VALUE(p);
                if ((runlen + i) > HLL_REGISTERS) break; /* Overflow. */
                while(runlen--) {
                    if (regval > max[i]) max[i] = regval;
                    i++;
                }
                p++;
            }
        }
        if (i != HLL_REGISTERS) return C_ERR;
    }
    return C_OK;
}

/* ========================== HyperLogLog commands ========================== */

/* Create an HLL object. We always create the HLL using sparse encoding.
 * This will be upgraded to the dense representation as needed. */
robj *createHLLObject(void) {
    robj *o;
    struct hllhdr *hdr;
    sds s;
    uint8_t *p;
    int sparselen = HLL_HDR_SIZE +
                    (((HLL_REGISTERS+(HLL_SPARSE_XZERO_MAX_LEN-1)) /
                     HLL_SPARSE_XZERO_MAX_LEN)*2);
    int aux;

    /* Populate the sparse representation with as many XZERO opcodes as
     * needed to represent all the registers. */
    aux = HLL_REGISTERS;
    s = sdsnewlen(NULL,sparselen);
    p = (uint8_t*)s + HLL_HDR_SIZE;
    while(aux) {
        int xzero = HLL_SPARSE_XZERO_MAX_LEN;
        if (xzero > aux) xzero = aux;
        HLL_SPARSE_XZERO_SET(p,xzero);
        p += 2;
        aux -= xzero;
    }
    serverAssert((p-(uint8_t*)s) == sparselen);

    /* Create the actual object. */
    o = createObject(OBJ_STRING,s);
    hdr = o->ptr;
    memcpy(hdr->magic,"HYLL",4);
    hdr->encoding = HLL_SPARSE;
    return o;
}

/* Check if the object is a String with a valid HLL representation.
 * Return C_OK if this is true, otherwise reply to the client
 * with an error and return C_ERR. */
int isHLLObjectOrReply(client *c, robj *o) {
    struct hllhdr *hdr;

    /* Key exists, check type */
    if (checkType(c,o,OBJ_STRING))
        return C_ERR; /* Error already sent. */

    if (!sdsEncodedObject(o)) goto invalid;
    if (stringObjectLen(o) < sizeof(*hdr)) goto invalid;
    hdr = o->ptr;

    /* Magic should be "HYLL". */
    if (hdr->magic[0] != 'H' || hdr->magic[1] != 'Y' ||
        hdr->magic[2] != 'L' || hdr->magic[3] != 'L') goto invalid;

    if (hdr->encoding > HLL_MAX_ENCODING) goto invalid;

    /* Dense representation string length should match exactly. */
    if (hdr->encoding == HLL_DENSE &&
        stringObjectLen(o) != HLL_DENSE_SIZE) goto invalid;

    /* All tests passed. */
    return C_OK;

invalid:
    addReplyError(c,"-WRONGTYPE Key is not a valid "
               "HyperLogLog string value.");
    return C_ERR;
}

/* PFADD var ele ele ele ... ele => :0 or :1 */
void pfaddCommand(client *c) {
    robj *o = lookupKeyWrite(c->db,c->argv[1]);
    struct hllhdr *hdr;
    int updated = 0, j;

    if (o == NULL) {
        /* Create the key with a string value of the exact length to
         * hold our HLL data structure. sdsnewlen() when NULL is passed
         * is guaranteed to return bytes initialized to zero. */
        o = createHLLObject();
        dbAdd(c->db,c->argv[1],o);
        updated++;
    } else {
        if (isHLLObjectOrReply(c,o) != C_OK) return;
        o = dbUnshareStringValue(c->db,c->argv[1],o);
    }
    /* Perform the low level ADD operation for every element. */
    for (j = 2; j < c->argc; j++) {
        int retval = hllAdd(o, (unsigned char*)c->argv[j]->ptr,
                               sdslen(c->argv[j]->ptr));
        switch(retval) {
        case 1:
            updated++;
            break;
        case -1:
            addReplyError(c,invalid_hll_err);
            return;
        }
    }
    hdr = o->ptr;
    if (updated) {
        signalModifiedKey(c,c->db,c->argv[1]);
        notifyKeyspaceEvent(NOTIFY_STRING,"pfadd",c->argv[1],c->db->id);
        server.dirty += updated;
        HLL_INVALIDATE_CACHE(hdr);
    }
    addReply(c, updated ? shared.cone : shared.czero);
}

/* PFCOUNT var -> approximated cardinality of set. */
void pfcountCommand(client *c) {
    robj *o;
    struct hllhdr *hdr;
    uint64_t card;

    /* Case 1: multi-key keys, cardinality of the union.
     *
     * When multiple keys are specified, PFCOUNT actually computes
     * the cardinality of the merge of the N HLLs specified. */
    if (c->argc > 2) {
        uint8_t max[HLL_HDR_SIZE+HLL_REGISTERS], *registers;
        int j;

        /* Compute an HLL with M[i] = MAX(M[i]_j). */
        memset(max,0,sizeof(max));
        hdr = (struct hllhdr*) max;
        hdr->encoding = HLL_RAW; /* Special internal-only encoding. */
        registers = max + HLL_HDR_SIZE;
        for (j = 1; j < c->argc; j++) {
            /* Check type and size. */
            robj *o = lookupKeyRead(c->db,c->argv[j]);
            if (o == NULL) continue; /* Assume empty HLL for non existing var.*/
            if (isHLLObjectOrReply(c,o) != C_OK) return;

            /* Merge with this HLL with our 'max' HLL by setting max[i]
             * to MAX(max[i],hll[i]). */
            if (hllMerge(registers,o) == C_ERR) {
                addReplyError(c,invalid_hll_err);
                return;
            }
        }

        /* Compute cardinality of the resulting set. */
        addReplyLongLong(c,hllCount(hdr,NULL));
        return;
    }

    /* Case 2: cardinality of the single HLL.
     *
     * The user specified a single key. Either return the cached value
     * or compute one and update the cache.
     *
     * Since a HLL is a regular Redis string type value, updating the cache does
     * modify the value. We do a lookupKeyRead anyway since this is flagged as a
     * read-only command. The difference is that with lookupKeyWrite, a
     * logically expired key on a replica is deleted, while with lookupKeyRead
     * it isn't, but the lookup returns NULL either way if the key is logically
     * expired, which is what matters here. */
    o = lookupKeyRead(c->db,c->argv[1]);
    if (o == NULL) {
        /* No key? Cardinality is zero since no element was added, otherwise
         * we would have a key as HLLADD creates it as a side effect. */
        addReply(c,shared.czero);
    } else {
        if (isHLLObjectOrReply(c,o) != C_OK) return;
        o = dbUnshareStringValue(c->db,c->argv[1],o);

        /* Check if the cached cardinality is valid. */
        hdr = o->ptr;
        if (HLL_VALID_CACHE(hdr)) {
            /* Just return the cached value. */
            card = (uint64_t)hdr->card[0];
            card |= (uint64_t)hdr->card[1] << 8;
            card |= (uint64_t)hdr->card[2] << 16;
            card |= (uint64_t)hdr->card[3] << 24;
            card |= (uint64_t)hdr->card[4] << 32;
            card |= (uint64_t)hdr->card[5] << 40;
            card |= (uint64_t)hdr->card[6] << 48;
            card |= (uint64_t)hdr->card[7] << 56;
        } else {
            int invalid = 0;
            /* Recompute it and update the cached value. */
            card = hllCount(hdr,&invalid);
            if (invalid) {
                addReplyError(c,invalid_hll_err);
                return;
            }
            hdr->card[0] = card & 0xff;
            hdr->card[1] = (card >> 8) & 0xff;
            hdr->card[2] = (card >> 16) & 0xff;
            hdr->card[3] = (card >> 24) & 0xff;
            hdr->card[4] = (card >> 32) & 0xff;
            hdr->card[5] = (card >> 40) & 0xff;
            hdr->card[6] = (card >> 48) & 0xff;
            hdr->card[7] = (card >> 56) & 0xff;
            /* This is considered a read-only command even if the cached value
             * may be modified and given that the HLL is a Redis string
             * we need to propagate the change. */
            signalModifiedKey(c,c->db,c->argv[1]);
            server.dirty++;
        }
        addReplyLongLong(c,card);
    }
}

/* PFMERGE dest src1 src2 src3 ... srcN => OK */
void pfmergeCommand(client *c) {
    uint8_t max[HLL_REGISTERS];
    struct hllhdr *hdr;
    int j;
    int use_dense = 0; /* Use dense representation as target? */

    /* Compute an HLL with M[i] = MAX(M[i]_j).
     * We store the maximum into the max array of registers. We'll write
     * it to the target variable later. */
    memset(max,0,sizeof(max));
    for (j = 1; j < c->argc; j++) {
        /* Check type and size. */
        robj *o = lookupKeyRead(c->db,c->argv[j]);
        if (o == NULL) continue; /* Assume empty HLL for non existing var. */
        if (isHLLObjectOrReply(c,o) != C_OK) return;

        /* If at least one involved HLL is dense, use the dense representation
         * as target ASAP to save time and avoid the conversion step. */
        hdr = o->ptr;
        if (hdr->encoding == HLL_DENSE) use_dense = 1;

        /* Merge with this HLL with our 'max' HLL by setting max[i]
         * to MAX(max[i],hll[i]). */
        if (hllMerge(max,o) == C_ERR) {
            addReplyError(c,invalid_hll_err);
            return;
        }
    }

    /* Create / unshare the destination key's value if needed. */
    robj *o = lookupKeyWrite(c->db,c->argv[1]);
    if (o == NULL) {
        /* Create the key with a string value of the exact length to
         * hold our HLL data structure. sdsnewlen() when NULL is passed
         * is guaranteed to return bytes initialized to zero. */
        o = createHLLObject();
        dbAdd(c->db,c->argv[1],o);
    } else {
        /* If key exists we are sure it's of the right type/size
         * since we checked when merging the different HLLs, so we
         * don't check again. */
        o = dbUnshareStringValue(c->db,c->argv[1],o);
    }

    /* Convert the destination object to dense representation if at least
     * one of the inputs was dense. */
    if (use_dense && hllSparseToDense(o) == C_ERR) {
        addReplyError(c,invalid_hll_err);
        return;
    }

    /* Write the resulting HLL to the destination HLL registers and
     * invalidate the cached value. */
    for (j = 0; j < HLL_REGISTERS; j++) {
        if (max[j] == 0) continue;
        hdr = o->ptr;
        switch(hdr->encoding) {
        case HLL_DENSE: hllDenseSet(hdr->registers,j,max[j]); break;
        case HLL_SPARSE: hllSparseSet(o,j,max[j]); break;
        }
    }
    hdr = o->ptr; /* o->ptr may be different now, as a side effect of
                     last hllSparseSet() call. */
    HLL_INVALIDATE_CACHE(hdr);

    signalModifiedKey(c,c->db,c->argv[1]);
    /* We generate a PFADD event for PFMERGE for semantical simplicity
     * since in theory this is a mass-add of elements. */
    notifyKeyspaceEvent(NOTIFY_STRING,"pfadd",c->argv[1],c->db->id);
    server.dirty++;
    addReply(c,shared.ok);
}

/* ========================== Testing / Debugging  ========================== */

/* PFSELFTEST
 * This command performs a self-test of the HLL registers implementation.
 * Something that is not easy to test from within the outside. */
#define HLL_TEST_CYCLES 1000
void pfselftestCommand(client *c) {
    unsigned int j, i;
    sds bitcounters = sdsnewlen(NULL,HLL_DENSE_SIZE);
    struct hllhdr *hdr = (struct hllhdr*) bitcounters, *hdr2;
    robj *o = NULL;
    uint8_t bytecounters[HLL_REGISTERS];

    /* Test 1: access registers.
     * The test is conceived to test that the different counters of our data
     * structure are accessible and that setting their values both result in
     * the correct value to be retained and not affect adjacent values. */
    for (j = 0; j < HLL_TEST_CYCLES; j++) {
        /* Set the HLL counters and an array of unsigned byes of the
         * same size to the same set of random values. */
        for (i = 0; i < HLL_REGISTERS; i++) {
            unsigned int r = rand() & HLL_REGISTER_MAX;

            bytecounters[i] = r;
            HLL_DENSE_SET_REGISTER(hdr->registers,i,r);
        }
        /* Check that we are able to retrieve the same values. */
        for (i = 0; i < HLL_REGISTERS; i++) {
            unsigned int val;

            HLL_DENSE_GET_REGISTER(val,hdr->registers,i);
            if (val != bytecounters[i]) {
                addReplyErrorFormat(c,
                    "TESTFAILED Register %d should be %d but is %d",
                    i, (int) bytecounters[i], (int) val);
                goto cleanup;
            }
        }
    }

    /* Test 2: approximation error.
     * The test adds unique elements and check that the estimated value
     * is always reasonable bounds.
     *
     * We check that the error is smaller than a few times than the expected
     * standard error, to make it very unlikely for the test to fail because
     * of a "bad" run.
     *
     * The test is performed with both dense and sparse HLLs at the same
     * time also verifying that the computed cardinality is the same. */
    memset(hdr->registers,0,HLL_DENSE_SIZE-HLL_HDR_SIZE);
    o = createHLLObject();
    double relerr = 1.04/sqrt(HLL_REGISTERS);
    int64_t checkpoint = 1;
    uint64_t seed = (uint64_t)rand() | (uint64_t)rand() << 32;
    uint64_t ele;
    for (j = 1; j <= 10000000; j++) {
        ele = j ^ seed;
        hllDenseAdd(hdr->registers,(unsigned char*)&ele,sizeof(ele));
        hllAdd(o,(unsigned char*)&ele,sizeof(ele));

        /* Make sure that for small cardinalities we use sparse
         * encoding. */
        if (j == checkpoint && j < server.hll_sparse_max_bytes/2) {
            hdr2 = o->ptr;
            if (hdr2->encoding != HLL_SPARSE) {
                addReplyError(c, "TESTFAILED sparse encoding not used");
                goto cleanup;
            }
        }

        /* Check that dense and sparse representations agree. */
        if (j == checkpoint && hllCount(hdr,NULL) != hllCount(o->ptr,NULL)) {
                addReplyError(c, "TESTFAILED dense/sparse disagree");
                goto cleanup;
        }

        /* Check error. */
        if (j == checkpoint) {
            int64_t abserr = checkpoint - (int64_t)hllCount(hdr,NULL);
            uint64_t maxerr = ceil(relerr*6*checkpoint);

            /* Adjust the max error we expect for cardinality 10
             * since from time to time it is statistically likely to get
             * much higher error due to collision, resulting into a false
             * positive. */
            if (j == 10) maxerr = 1;

            if (abserr < 0) abserr = -abserr;
            if (abserr > (int64_t)maxerr) {
                addReplyErrorFormat(c,
                    "TESTFAILED Too big error. card:%llu abserr:%llu",
                    (unsigned long long) checkpoint,
                    (unsigned long long) abserr);
                goto cleanup;
            }
            checkpoint *= 10;
        }
    }

    /* Success! */
    addReply(c,shared.ok);

cleanup:
    sdsfree(bitcounters);
    if (o) decrRefCount(o);
}

/* Different debugging related operations about the HLL implementation.
 *
 * PFDEBUG GETREG <key>
 * PFDEBUG DECODE <key>
 * PFDEBUG ENCODING <key>
 * PFDEBUG TODENSE <key>
 */
void pfdebugCommand(client *c) {
    char *cmd = c->argv[1]->ptr;
    struct hllhdr *hdr;
    robj *o;
    int j;

    o = lookupKeyWrite(c->db,c->argv[2]);
    if (o == NULL) {
        addReplyError(c,"The specified key does not exist");
        return;
    }
    if (isHLLObjectOrReply(c,o) != C_OK) return;
    o = dbUnshareStringValue(c->db,c->argv[2],o);
    hdr = o->ptr;

    /* PFDEBUG GETREG <key> */
    if (!strcasecmp(cmd,"getreg")) {
        if (c->argc != 3) goto arityerr;

        if (hdr->encoding == HLL_SPARSE) {
            if (hllSparseToDense(o) == C_ERR) {
                addReplyError(c,invalid_hll_err);
                return;
            }
            server.dirty++; /* Force propagation on encoding change. */
        }

        hdr = o->ptr;
        addReplyArrayLen(c,HLL_REGISTERS);
        for (j = 0; j < HLL_REGISTERS; j++) {
            uint8_t val;

            HLL_DENSE_GET_REGISTER(val,hdr->registers,j);
            addReplyLongLong(c,val);
        }
    }
    /* PFDEBUG DECODE <key> */
    else if (!strcasecmp(cmd,"decode")) {
        if (c->argc != 3) goto arityerr;

        uint8_t *p = o->ptr, *end = p+sdslen(o->ptr);
        sds decoded = sdsempty();

        if (hdr->encoding != HLL_SPARSE) {
            sdsfree(decoded);
            addReplyError(c,"HLL encoding is not sparse");
            return;
        }

        p += HLL_HDR_SIZE;
        while(p < end) {
            int runlen, regval;

            if (HLL_SPARSE_IS_ZERO(p)) {
                runlen = HLL_SPARSE_ZERO_LEN(p);
                p++;
                decoded = sdscatprintf(decoded,"z:%d ",runlen);
            } else if (HLL_SPARSE_IS_XZERO(p)) {
                runlen = HLL_SPARSE_XZERO_LEN(p);
                p += 2;
                decoded = sdscatprintf(decoded,"Z:%d ",runlen);
            } else {
                runlen = HLL_SPARSE_VAL_LEN(p);
                regval = HLL_SPARSE_VAL_VALUE(p);
                p++;
                decoded = sdscatprintf(decoded,"v:%d,%d ",regval,runlen);
            }
        }
        decoded = sdstrim(decoded," ");
        addReplyBulkCBuffer(c,decoded,sdslen(decoded));
        sdsfree(decoded);
    }
    /* PFDEBUG ENCODING <key> */
    else if (!strcasecmp(cmd,"encoding")) {
        char *encodingstr[2] = {"dense","sparse"};
        if (c->argc != 3) goto arityerr;

        addReplyStatus(c,encodingstr[hdr->encoding]);
    }
    /* PFDEBUG TODENSE <key> */
    else if (!strcasecmp(cmd,"todense")) {
        int conv = 0;
        if (c->argc != 3) goto arityerr;

        if (hdr->encoding == HLL_SPARSE) {
            if (hllSparseToDense(o) == C_ERR) {
                addReplyError(c,invalid_hll_err);
                return;
            }
            conv = 1;
            server.dirty++; /* Force propagation on encoding change. */
        }
        addReply(c,conv ? shared.cone : shared.czero);
    } else {
        addReplyErrorFormat(c,"Unknown PFDEBUG subcommand '%s'", cmd);
    }
    return;

arityerr:
    addReplyErrorFormat(c,
        "Wrong number of arguments for the '%s' subcommand",cmd);
}

