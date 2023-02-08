/*
 * Copyright (c) 2013-2014, yinqiwen <yinqiwen@gmail.com>
 * Copyright (c) 2014, Matt Stancliff <matt@genges.com>.
 * Copyright (c) 2015-2016, Salvatore Sanfilippo <antirez@gmail.com>.
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 *  * Redistributions of source code must retain the above copyright notice,
 *    this list of conditions and the following disclaimer.
 *  * Redistributions in binary form must reproduce the above copyright
 *    notice, this list of conditions and the following disclaimer in the
 *    documentation and/or other materials provided with the distribution.
 *  * Neither the name of Redis nor the names of its contributors may be used
 *    to endorse or promote products derived from this software without
 *    specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS
 * BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF
 * THE POSSIBILITY OF SUCH DAMAGE.
 */
#include "geohash.h"

/**
 * Hashing works like this:
 * Divide the world into 4 buckets.  Label each one as such:
 *  -----------------
 *  |       |       |
 *  |       |       |
 *  | 0,1   | 1,1   |
 *  -----------------
 *  |       |       |
 *  |       |       |
 *  | 0,0   | 1,0   |
 *  -----------------
 */

/* GEOHASH 算法
 *
 *
 * */

/* Interleave lower bits of x and y, so the bits of x
 * are in the even positions and bits from y in the odd;
 * x and y must initially be less than 2**32 (4294967296).
 * From:  https://graphics.stanford.edu/~seander/bithacks.html#InterleaveBMN
 */
/* 经纬度编码，这里看起来一堆的位运算，但是把 B[] 数组的字节展开就很容易理解了
 * xlo（纬度）,ylo（经度） 这两个值在之前的步骤中被限制了小于 32 位（初始的是 26 位）
 * 经过 (x | (x << S[4])) & B[4] 该计算后，S[4] 是 16，其实就是把纬度的 32-17 位移动到了 48-33 位，下面是 B[4] 展开的样子
 * 0000 0000 0000 0000 1111 1111 1111 1111 0000 0000 0000 0000 1111 1111 1111 1111
 * 接着经过 S[3],B[3] S[2],B[2] S[1],B[1] S[0],B[0] 就把 32 位的纬度分散到了 64 位数的奇数位
 *
 * 经度的计算也是一样的。
 *
 * 最后一步计算 x|(y<<1) 则把经度放到了偶数位，然后合并
 */
static inline uint64_t interleave64(uint32_t xlo, uint32_t ylo) {
    static const uint64_t B[] = {0x5555555555555555ULL, 0x3333333333333333ULL,
                                 0x0F0F0F0F0F0F0F0FULL, 0x00FF00FF00FF00FFULL,
                                 0x0000FFFF0000FFFFULL};
    static const unsigned int S[] = {1, 2, 4, 8, 16};

    uint64_t x = xlo;
    uint64_t y = ylo;

    x = (x | (x << S[4])) & B[4];
    y = (y | (y << S[4])) & B[4];

    x = (x | (x << S[3])) & B[3];
    y = (y | (y << S[3])) & B[3];

    x = (x | (x << S[2])) & B[2];
    y = (y | (y << S[2])) & B[2];

    x = (x | (x << S[1])) & B[1];
    y = (y | (y << S[1])) & B[1];

    x = (x | (x << S[0])) & B[0];
    y = (y | (y << S[0])) & B[0];

    return x | (y << 1);
}

/* reverse the interleave process
 * derived from http://stackoverflow.com/questions/4909263
 */
/* 该函数就是 interleave64 的解码方式，把奇数位拿出来就是纬度，偶数位拿出来就是经度，
 * 这里比上面多计算了一轮，因为第一轮是把偶数位置零，这样就能得到正确的经纬度数据，
 * 最后的 x | (y << 32) 用 64 位的高 32 位存储经度，低 32 位存储纬度 */
static inline uint64_t deinterleave64(uint64_t interleaved) {
    static const uint64_t B[] = {0x5555555555555555ULL, 0x3333333333333333ULL,
                                 0x0F0F0F0F0F0F0F0FULL, 0x00FF00FF00FF00FFULL,
                                 0x0000FFFF0000FFFFULL, 0x00000000FFFFFFFFULL};
    static const unsigned int S[] = {0, 1, 2, 4, 8, 16};

    uint64_t x = interleaved;
    uint64_t y = interleaved >> 1;

    x = (x | (x >> S[0])) & B[0];
    y = (y | (y >> S[0])) & B[0];

    x = (x | (x >> S[1])) & B[1];
    y = (y | (y >> S[1])) & B[1];

    x = (x | (x >> S[2])) & B[2];
    y = (y | (y >> S[2])) & B[2];

    x = (x | (x >> S[3])) & B[3];
    y = (y | (y >> S[3])) & B[3];

    x = (x | (x >> S[4])) & B[4];
    y = (y | (y >> S[4])) & B[4];

    x = (x | (x >> S[5])) & B[5];
    y = (y | (y >> S[5])) & B[5];

    return x | (y << 32);
}

/* 填充经纬度的范围 */
void geohashGetCoordRange(GeoHashRange *long_range, GeoHashRange *lat_range) {
    /* These are constraints from EPSG:900913 / EPSG:3785 / OSGEO:41001 */
    /* We can't geocode at the north/south pole. */
    long_range->max = GEO_LONG_MAX;
    long_range->min = GEO_LONG_MIN;
    lat_range->max = GEO_LAT_MAX;
    lat_range->min = GEO_LAT_MIN;
}

int geohashEncode(const GeoHashRange *long_range, const GeoHashRange *lat_range,
                  double longitude, double latitude, uint8_t step,
                  GeoHashBits *hash) {
    /* Check basic arguments sanity. */
    /* 如果 hash 是空，step 大于 32，step 等于 0，lat_range 和 long_range 还没有初始化，直接返回
     * 注：step 是经纬度的分割次数，可以看做每分割一次要占用一个 bit 位，分割 32 次即经纬度都要占 32bit，最后合并就是 64bit */
    if (hash == NULL || step > 32 || step == 0 ||
        RANGEPISZERO(lat_range) || RANGEPISZERO(long_range)) return 0;

    /* Return an error when trying to index outside the supported
     * constraints. */
    if (longitude > GEO_LONG_MAX || longitude < GEO_LONG_MIN ||
        latitude > GEO_LAT_MAX || latitude < GEO_LAT_MIN) return 0;

    hash->bits = 0;
    hash->step = step;

    if (latitude < lat_range->min || latitude > lat_range->max ||
        longitude < long_range->min || longitude > long_range->max) {
        return 0;
    }

    /* 获取当前经纬度到最小经纬度与总经纬度的比例 */
    double lat_offset =
        (latitude - lat_range->min) / (lat_range->max - lat_range->min);
    double long_offset =
        (longitude - long_range->min) / (long_range->max - long_range->min);

    /* convert to fixed point based on the step size */
    /* 上面得到的比例乘以 2^step，可以看成将经纬度分割成 2^step 块，当前经纬度在哪一块中 */
    lat_offset *= (1ULL << step);
    long_offset *= (1ULL << step);
    /* 编码过程 */
    hash->bits = interleave64(lat_offset, long_offset);
    return 1;
}

int geohashEncodeType(double longitude, double latitude, uint8_t step, GeoHashBits *hash) {
    /* 定义一个两个槽位的 GeoHashRange 数组，用来保存经纬度的最大值和最小值 */
    GeoHashRange r[2] = {{0}};
    /* 在上面的数组中填充经纬度最大值和最小值 */
    geohashGetCoordRange(&r[0], &r[1]);
    /* 对经纬度进行编码 */
    return geohashEncode(&r[0], &r[1], longitude, latitude, step, hash);
}

/* 对经纬度进行编码 */
int geohashEncodeWGS84(double longitude, double latitude, uint8_t step,
                       GeoHashBits *hash) {
    return geohashEncodeType(longitude, latitude, step, hash);
}

int geohashDecode(const GeoHashRange long_range, const GeoHashRange lat_range,
                   const GeoHashBits hash, GeoHashArea *area) {
    /* 验证参数 */
    if (HASHISZERO(hash) || NULL == area || RANGEISZERO(lat_range) ||
        RANGEISZERO(long_range)) {
        return 0;
    }

    area->hash = hash;
    uint8_t step = hash.step;
    /* 这里就是解码的函数调用，结果的高 32 位为经度，低 32 位为纬度 */
    uint64_t hash_sep = deinterleave64(hash.bits); /* hash = [LAT][LONG] */

    double lat_scale = lat_range.max - lat_range.min;
    double long_scale = long_range.max - long_range.min;

    uint32_t ilato = hash_sep;       /* get lat part of deinterleaved hash */
    uint32_t ilono = hash_sep >> 32; /* shift over to get long part of hash */

    /* divide by 2**step.
     * Then, for 0-1 coordinate, multiply times scale and add
       to the min to get the absolute coordinate. */
    /* 下面的计算其实是计算当前经纬度所处于的分割后的那一块的左右经度和上下纬度
     * ilato * 1.0 / (1ull << step) 获取到该块的最小纬度到总纬度的最小纬度占总纬度的比例
     * 然后乘以 lat_scale(总纬度) 再加上 lat_range.min(最小纬度) 就是当前块最小纬度的的值.
     * 最大纬度，最小经度，最大经度的计算类似 */
    area->latitude.min =
        lat_range.min + (ilato * 1.0 / (1ull << step)) * lat_scale;
    area->latitude.max =
        lat_range.min + ((ilato + 1) * 1.0 / (1ull << step)) * lat_scale;
    area->longitude.min =
        long_range.min + (ilono * 1.0 / (1ull << step)) * long_scale;
    area->longitude.max =
        long_range.min + ((ilono + 1) * 1.0 / (1ull << step)) * long_scale;

    return 1;
}

int geohashDecodeType(const GeoHashBits hash, GeoHashArea *area) {
    /* 初始化经纬度的范围 */
    GeoHashRange r[2] = {{0}};
    geohashGetCoordRange(&r[0], &r[1]);
    /* 解码获得该地点处于的区域的最小经纬度和最大经纬度 */
    return geohashDecode(r[0], r[1], hash, area);
}

int geohashDecodeWGS84(const GeoHashBits hash, GeoHashArea *area) {
    return geohashDecodeType(hash, area);
}

/* 获取经纬度 */
int geohashDecodeAreaToLongLat(const GeoHashArea *area, double *xy) {
    if (!xy) return 0;
    /* 其实就是取该区域的中间点作为最终的经纬度 */
    xy[0] = (area->longitude.min + area->longitude.max) / 2;
    if (xy[0] > GEO_LONG_MAX) xy[0] = GEO_LONG_MAX;
    if (xy[0] < GEO_LONG_MIN) xy[0] = GEO_LONG_MIN;
    xy[1] = (area->latitude.min + area->latitude.max) / 2;
    if (xy[1] > GEO_LAT_MAX) xy[1] = GEO_LAT_MAX;
    if (xy[1] < GEO_LAT_MIN) xy[1] = GEO_LAT_MIN;
    return 1;
}

int geohashDecodeToLongLatType(const GeoHashBits hash, double *xy) {
    GeoHashArea area = {{0}};
    /* 对编码进行解码，并将经纬度放入 area 中 */
    if (!xy || !geohashDecodeType(hash, &area))
        return 0;
    /* 从 area 中获取经纬度放入 xy 数组中 */
    return geohashDecodeAreaToLongLat(&area, xy);
}

int geohashDecodeToLongLatWGS84(const GeoHashBits hash, double *xy) {
    /* 解码 */
    return geohashDecodeToLongLatType(hash, xy);
}

static void geohash_move_x(GeoHashBits *hash, int8_t d) {
    if (d == 0)
        return;

    /* 获取 geohash 编码之后的经度的编码数据 */
    uint64_t x = hash->bits & 0xaaaaaaaaaaaaaaaaULL;
    uint64_t y = hash->bits & 0x5555555555555555ULL;

    /* zz 变量使用来辅助 x 做加减法的工具，
     * x 的所有奇数位为 0，偶数位是实际数据，而 zz 经过 step * 2 位对齐后，低于
     * step * 2 的位，奇数位是 1，偶数位为 0，进行加法的时候会将数字加到偶数位上 */
    uint64_t zz = 0x5555555555555555ULL >> (64 - hash->step * 2);

    if (d > 0) {
        /* 真实的 x + 1 */
        x = x + (zz + 1);
    } else {
        /* 真实的 x - 1 */
        x = x | zz;
        x = x - (zz + 1);
    }

    /* 对齐 */
    x &= (0xaaaaaaaaaaaaaaaaULL >> (64 - hash->step * 2));
    /* 和 y 值合并成新的值 */
    hash->bits = (x | y);
}

/* 纬度方向移动一格，d > 0 向北移动一格，否则向南移动一格 */
static void geohash_move_y(GeoHashBits *hash, int8_t d) {
    if (d == 0)
        return;

    uint64_t x = hash->bits & 0xaaaaaaaaaaaaaaaaULL;
    uint64_t y = hash->bits & 0x5555555555555555ULL;

    uint64_t zz = 0xaaaaaaaaaaaaaaaaULL >> (64 - hash->step * 2);
    if (d > 0) {
        y = y + (zz + 1);
    } else {
        y = y | zz;
        y = y - (zz + 1);
    }
    y &= (0x5555555555555555ULL >> (64 - hash->step * 2));
    hash->bits = (x | y);
}

/* 计算出当前区域周围 8 个区域的的 geohash 编码 */
void geohashNeighbors(const GeoHashBits *hash, GeoHashNeighbors *neighbors) {
    neighbors->east = *hash;
    neighbors->west = *hash;
    neighbors->north = *hash;
    neighbors->south = *hash;
    neighbors->south_east = *hash;
    neighbors->south_west = *hash;
    neighbors->north_east = *hash;
    neighbors->north_west = *hash;

    /* 东方的区域，从当前区域向东移动一格 */
    geohash_move_x(&neighbors->east, 1);
    geohash_move_y(&neighbors->east, 0);

    /* 西方的区域，从当前区域向西移动一格 */
    geohash_move_x(&neighbors->west, -1);
    geohash_move_y(&neighbors->west, 0);

    geohash_move_x(&neighbors->south, 0);
    geohash_move_y(&neighbors->south, -1);

    geohash_move_x(&neighbors->north, 0);
    geohash_move_y(&neighbors->north, 1);

    geohash_move_x(&neighbors->north_west, -1);
    geohash_move_y(&neighbors->north_west, 1);

    geohash_move_x(&neighbors->north_east, 1);
    geohash_move_y(&neighbors->north_east, 1);

    geohash_move_x(&neighbors->south_east, 1);
    geohash_move_y(&neighbors->south_east, -1);

    geohash_move_x(&neighbors->south_west, -1);
    geohash_move_y(&neighbors->south_west, -1);
}
