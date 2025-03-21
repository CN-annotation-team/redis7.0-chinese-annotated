/*
   A C-program for MT19937-64 (2004/9/29 version).
   Coded by Takuji Nishimura and Makoto Matsumoto.

   This is a 64-bit version of Mersenne Twister pseudorandom number
   generator.

   Before using, initialize the state by using init_genrand64(seed)
   or init_by_array64(init_key, key_length).

   Copyright (C) 2004, Makoto Matsumoto and Takuji Nishimura,
   All rights reserved.

   Redistribution and use in source and binary forms, with or without
   modification, are permitted provided that the following conditions
   are met:

     1. Redistributions of source code must retain the above copyright
        notice, this list of conditions and the following disclaimer.

     2. Redistributions in binary form must reproduce the above copyright
        notice, this list of conditions and the following disclaimer in the
        documentation and/or other materials provided with the distribution.

     3. The names of its contributors may not be used to endorse or promote
        products derived from this software without specific prior written
        permission.

   THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
   "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
   LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
   A PARTICULAR PURPOSE ARE DISCLAIMED.  IN NO EVENT SHALL THE COPYRIGHT OWNER OR
   CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL,
   EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO,
   PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR
   PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF
   LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING
   NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
   SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

   References:
   T. Nishimura, ``Tables of 64-bit Mersenne Twisters''
     ACM Transactions on Modeling and
     Computer Simulation 10. (2000) 348--357.
   M. Matsumoto and T. Nishimura,
     ``Mersenne Twister: a 623-dimensionally equidistributed
       uniform pseudorandom number generator''
     ACM Transactions on Modeling and
     Computer Simulation 8. (Jan. 1998) 3--30.

   Any feedback is very welcome.
   http://www.math.hiroshima-u.ac.jp/~m-mat/MT/emt.html
   email: m-mat @ math.sci.hiroshima-u.ac.jp (remove spaces)
*/

/* 模块功能： ​64 位梅森旋转算法（Mersenne Twister）​ 的标准实现，用于生成高质量伪随机数 */

#include "mt19937-64.h"
#include <stdio.h>

#define NN 312                          // 状态数组长度（梅森素数周期参数）
#define MM 156                          // 旋转偏移量（算法核心参数）
#define MATRIX_A 0xB5026F5AA96619E9ULL  // 旋转矩阵常数
#define UM 0xFFFFFFFF80000000ULL        // 高位掩码（保留最高33位）
#define LM 0x7FFFFFFFULL                // 低位掩码（保留最低31位）


/* The array for the state vector */
static unsigned long long mt[NN];       // 状态数组，用于存储随机数生成器的状态
/* mti==NN+1 means mt[NN] is not initialized */
static int mti=NN+1;                    // 当前状态数组的索引，mti==NN+1 表示未初始化

/* initializes mt[NN] with a seed */
/* ​单种子初始化函数
* 操作逻辑：用种子生成初始状态数组，采用 ​线性同余混合算法 确保随机性
*/
void init_genrand64(unsigned long long seed)
{
    mt[0] = seed;                       // 使用种子初始化状态数组的第一个元素
    for (mti=1; mti<NN; mti++)          // 递推填充状态数组
        mt[mti] =  (6364136223846793005ULL * (mt[mti-1] ^ (mt[mti-1] >> 62)) + mti);
}

/* initialize by an array with array-length */
/* init_key is the array for initializing keys */
/* key_length is its length */
/* ​多种子数组初始化函数
* 操作逻辑：首先使用基准种子初始化，然后通过非线性混合算法将多个种子混合到状态数组中
*/
void init_by_array64(unsigned long long init_key[],
                     unsigned long long key_length)
{
    unsigned long long i, j, k;
    init_genrand64(19650218ULL);              // 使用基准种子初始化状态数组
    i=1; j=0;
    k = (NN>key_length ? NN : key_length);    // 选择较大的值作为循环次数
    for (; k; k--) {                          // 确保状态数组和种子数组都被充分处理
        /* 非线性混合算法：将种子数组 init_key 混合到状态数组 mt 中 */
        mt[i] = (mt[i] ^ ((mt[i-1] ^ (mt[i-1] >> 62)) * 3935559000370003845ULL))  // （位运算）提取前一个状态值的高位信息，增加混合性
          + init_key[j] + j; /* non linear */ // 加法运算，将种子数组的值和索引值混合到状态数组中
        i++; j++;
        if (i>=NN) { mt[0] = mt[NN-1]; i=1; } // 循环回状态数组开头
        if (j>=key_length) j=0;               // 循环回种子数组开头
    }
    for (k=NN-1; k; k--) {                    // 处理状态数组的每个元素
        /* 进一步非线性混合：增强状态数组的随机性 */
        mt[i] = (mt[i] ^ ((mt[i-1] ^ (mt[i-1] >> 62)) * 2862933555777941757ULL))
          - i; /* non linear */               // 此处引入索引值，增加差异性
        i++;
        if (i>=NN) { mt[0] = mt[NN-1]; i=1; } // 循环回状态数组开头
    }

    /* 确保状态数组的最高位为 1，避免全零状态 */
    mt[0] = 1ULL << 63; /* MSB is 1; assuring non-zero initial array */
}

/* generates a random number on [0, 2^64-1]-interval */
/* 生成 64 位随机数函数
* 操作逻辑：该函数生成一个 64 位的随机数。首先检查状态数组是否需要更新，
* 然后通过旋转操作生成新的状态数组。最后对状态数组中的元素进行一系列位操作，生成最终的随机数
*/
unsigned long long genrand64_int64(void)
{
    int i;
    unsigned long long x;
    static unsigned long long mag01[2]={0ULL, MATRIX_A};  // mag01 数组，用于旋转操作

    if (mti >= NN) { /* generate NN words at one time */  // 如果状态数组已用完，重新生成

        /* if init_genrand64() has not been called, */
        /* a default initial seed is used     */
        if (mti == NN+1)
            init_genrand64(5489ULL);                      // 如果未初始化，使用默认种子初始化

        /* 对状态数组进行旋转操作，生成新的状态值 */
        for (i=0;i<NN-MM;i++) {
            /* 提取状态数组 mt[i] 的高 33 位和 mt[i + 1] 的低 31 位，组合成一个新的值 x */
            x = (mt[i]&UM)|(mt[i+1]&LM);
            /* 对 x 进行右移 1 位操作，并根据 x 的最低位决定是否与 MATRIX_A 进行异或操作，生成新的状态值 */
            mt[i] = mt[i+MM] ^ (x>>1) ^ mag01[(int)(x&1ULL)];
        }
        /* 与第一个 for 循环类似，但使用不同的偏移量 MM - NN，确保状态数组的连续性 */
        for (;i<NN-1;i++) {
            x = (mt[i]&UM)|(mt[i+1]&LM);
            mt[i] = mt[i+(MM-NN)] ^ (x>>1) ^ mag01[(int)(x&1ULL)];
        }
        /* 对状态数组的最后一个元素进行旋转操作，确保状态数组的完整性 */
        x = (mt[NN-1]&UM)|(mt[0]&LM);
        mt[NN-1] = mt[MM-1] ^ (x>>1) ^ mag01[(int)(x&1ULL)];

        mti = 0;                                          // 重置索引
    }

    x = mt[mti++];                                        // 提取当前状态值
    /* 随机数生成公式，对状态值进行位操作，生成最终的随机数 */
    x ^= (x >> 29) & 0x5555555555555555ULL;               // 用于提取和混合奇数位信息
    x ^= (x << 17) & 0x71D67FFFEDA60000ULL;               // 提取和混合特定位置的位信息
    x ^= (x << 37) & 0xFFF7EEE000000000ULL;               // 提取和混合特定位置的位信息
    x ^= (x >> 43);                                       // 进一步混合

    /* 返回生成的 64 位随机数 */
    return x;
}

/* generates a random number on [0, 2^63-1]-interval */
/* 生成 63 位随机数函数
* 操作逻辑：该函数生成一个 63 位的随机数，通过将 64 位随机数右移一位得到
*/
long long genrand64_int63(void)
{
    return (long long)(genrand64_int64() >> 1);
}

/* generates a random number on [0,1]-real-interval */
/* 生成 [0,1) 区间内的随机数函数
* 操作逻辑：通过将 64 位随机数右移 11 位并乘以一个常数得到
*/
double genrand64_real1(void)
{
    return (genrand64_int64() >> 11) * (1.0/9007199254740991.0);
}

/* generates a random number on [0,1)-real-interval */
/* 生成 [0,1) 区间内的随机数函数
* 操作逻辑：通过将 64 位随机数右移 11 位并乘以一个常数得到
*/
double genrand64_real2(void)
{
    return (genrand64_int64() >> 11) * (1.0/9007199254740992.0);
}

/* generates a random number on (0,1)-real-interval */
/* 生成 (0,1) 区间内的随机数函数
* 操作逻辑：通过将 64 位随机数右移 12 位并加上 0.5，再乘以一个常数得到
*/
double genrand64_real3(void)
{
    return ((genrand64_int64() >> 12) + 0.5) * (1.0/4503599627370496.0);
}

#ifdef MT19937_64_MAIN
int main(void)
{
    int i;
    unsigned long long init[4]={0x12345ULL, 0x23456ULL, 0x34567ULL, 0x45678ULL}, length=4;
    init_by_array64(init, length);
    printf("1000 outputs of genrand64_int64()\n");
    for (i=0; i<1000; i++) {
      printf("%20llu ", genrand64_int64());
      if (i%5==4) printf("\n");
    }
    printf("\n1000 outputs of genrand64_real2()\n");
    for (i=0; i<1000; i++) {
      printf("%10.8f ", genrand64_real2());
      if (i%5==4) printf("\n");
    }
    return 0;
}
#endif
