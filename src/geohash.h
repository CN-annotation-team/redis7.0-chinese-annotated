/*
 * Copyright (c) 2013-2014, yinqiwen <yinqiwen@gmail.com>
 * Copyright (c) 2014, Matt Stancliff <matt@genges.com>.
 * Copyright (c) 2015, Salvatore Sanfilippo <antirez@gmail.com>.
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

#ifndef GEOHASH_H_
#define GEOHASH_H_

#include <stddef.h>
#include <stdint.h>

#if defined(__cplusplus)
extern "C" {
#endif

#define HASHISZERO(r) (!(r).bits && !(r).step)
/* 判断 GeoHashRange 实例的 min 和 max 是否都为 0 */
#define RANGEISZERO(r) (!(r).max && !(r).min)
/* 该宏判断 GeoHashRange 实例结构是否为空或其中的元素都是 0 */
#define RANGEPISZERO(r) (r == NULL || RANGEISZERO(*r))

#define GEO_STEP_MAX 26 /* 26*2 = 52 bits. */

/* Limits from EPSG:900913 / EPSG:3785 / OSGEO:41001 */
/* 下面四个宏分别是纬度和经度的最大值和最小值 */
#define GEO_LAT_MIN -85.05112878
#define GEO_LAT_MAX 85.05112878
#define GEO_LONG_MIN -180
#define GEO_LONG_MAX 180

typedef enum {
    GEOHASH_NORTH = 0,
    GEOHASH_EAST,
    GEOHASH_WEST,
    GEOHASH_SOUTH,
    GEOHASH_SOUTH_WEST,
    GEOHASH_SOUTH_EAST,
    GEOHASH_NORT_WEST,
    GEOHASH_NORT_EAST
} GeoDirection;

typedef struct {
    /* 编码后的 bit 串 */
    uint64_t bits;
    /* 将经纬度都分割成 2^step 块 */
    uint8_t step;
} GeoHashBits;

/* 存储经纬度的范围 */
typedef struct {
    /* 最小值 */
    double min;
    /* 最大值 */
    double max;
} GeoHashRange;

/* 存储经纬度和其编码信息信息 */
typedef struct {
    /* 编码信息 */
    GeoHashBits hash;
    /* 经度 */
    GeoHashRange longitude;
    /* 纬度 */
    GeoHashRange latitude;
} GeoHashArea;

/* 存储一个区域的附近八个区域的 geohash 编码信息 */
typedef struct {
    GeoHashBits north;
    GeoHashBits east;
    GeoHashBits west;
    GeoHashBits south;
    GeoHashBits north_east;
    GeoHashBits south_east;
    GeoHashBits north_west;
    GeoHashBits south_west;
} GeoHashNeighbors;

/* 下面两个宏是搜索类型 */
/* 圆形 */
#define CIRCULAR_TYPE 1
/* 矩形 */
#define RECTANGLE_TYPE 2
typedef struct {
    /* 范围搜索类型 */
    int type; /* search type */
    /* 存储中心点的经纬度，以该点为基准进行查找 */
    double xy[2]; /* search center point, xy[0]: lon, xy[1]: lat */
    /* 单位转换，基础单位是 m，conversion = 1000 则表示 km */
    double conversion; /* km: 1000 */
    double bounds[4]; /* bounds[0]: min_lon, bounds[1]: min_lat
                       * bounds[2]: max_lon, bounds[3]: max_lat */
    union {
        /* CIRCULAR_TYPE */
        /* 半径 */
        double radius;
        /* RECTANGLE_TYPE */
        struct {
            double height;
            double width;
        } r;
    } t;
} GeoShape;

/*
 * 0:success
 * -1:failed
 */
void geohashGetCoordRange(GeoHashRange *long_range, GeoHashRange *lat_range);
int geohashEncode(const GeoHashRange *long_range, const GeoHashRange *lat_range,
                  double longitude, double latitude, uint8_t step,
                  GeoHashBits *hash);
int geohashEncodeType(double longitude, double latitude,
                      uint8_t step, GeoHashBits *hash);
int geohashEncodeWGS84(double longitude, double latitude, uint8_t step,
                       GeoHashBits *hash);
int geohashDecode(const GeoHashRange long_range, const GeoHashRange lat_range,
                  const GeoHashBits hash, GeoHashArea *area);
int geohashDecodeType(const GeoHashBits hash, GeoHashArea *area);
int geohashDecodeWGS84(const GeoHashBits hash, GeoHashArea *area);
int geohashDecodeAreaToLongLat(const GeoHashArea *area, double *xy);
int geohashDecodeToLongLatType(const GeoHashBits hash, double *xy);
int geohashDecodeToLongLatWGS84(const GeoHashBits hash, double *xy);
void geohashNeighbors(const GeoHashBits *hash, GeoHashNeighbors *neighbors);

#if defined(__cplusplus)
}
#endif
#endif /* GEOHASH_H_ */
