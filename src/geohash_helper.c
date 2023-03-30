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

/* This is a C++ to C conversion from the ardb project.
 * This file started out as:
 * https://github.com/yinqiwen/ardb/blob/d42503/src/geo/geohash_helper.cpp
 */

#include "fmacros.h"
#include "geohash_helper.h"
#include "debugmacro.h"
#include <math.h>

#define D_R (M_PI / 180.0)
#define R_MAJOR 6378137.0
#define R_MINOR 6356752.3142
#define RATIO (R_MINOR / R_MAJOR)
#define ECCENT (sqrt(1.0 - (RATIO *RATIO)))
#define COM (0.5 * ECCENT)

/// @brief The usual PI/180 constant
const double DEG_TO_RAD = 0.017453292519943295769236907684886;
/// @brief Earth's quatratic mean radius for WGS-84
/* 地球半径，这里使用 m 作为单位的数据，所以 geohash 中的计算的基础单位就是 m */
const double EARTH_RADIUS_IN_METERS = 6372797.560856;

/* 赤道长度的一半 */
const double MERCATOR_MAX = 20037726.37;
const double MERCATOR_MIN = -20037726.37;

static inline double deg_rad(double ang) { return ang * D_R; }
static inline double rad_deg(double ang) { return ang / D_R; }

/* This function is used in order to estimate the step (bits precision)
 * of the 9 search area boxes during radius queries. */
uint8_t geohashEstimateStepsByRadius(double range_meters, double lat) {
    if (range_meters == 0) return 26;
    int step = 1;
    while (range_meters < MERCATOR_MAX) {
        range_meters *= 2;
        step++;
    }
    step -= 2; /* Make sure range is included in most of the base cases. */

    /* Wider range towards the poles... Note: it is possible to do better
     * than this approximation by computing the distance between meridians
     * at this latitude, but this does the trick for now. */
    /* 纬度到 66 的时候，比起赤道，圆的半径会缩小一半，相对的周长也缩小一半，所以同长度的情况下跨经度会扩大一倍 */
    if (lat > 66 || lat < -66) {
        step--;
        /* 纬度到 80 的时候，半径又会缩小一半 */
        if (lat > 80 || lat < -80) step--;
    }

    /* Frame to valid range. */
    /* step 规范化 */
    if (step < 1) step = 1;
    if (step > 26) step = 26;
    return step;
}

/* Return the bounding box of the search area by shape (see geohash.h GeoShape)
 * bounds[0] - bounds[2] is the minimum and maximum longitude
 * while bounds[1] - bounds[3] is the minimum and maximum latitude.
 * since the higher the latitude, the shorter the arc length, the box shape is as follows
 * (left and right edges are actually bent), as shown in the following diagram:
 *
 *    \-----------------/          --------               \-----------------/
 *     \               /         /          \              \               /
 *      \  (long,lat) /         / (long,lat) \              \  (long,lat) /
 *       \           /         /              \             /             \
 *         ---------          /----------------\           /---------------\
 *  Northern Hemisphere       Southern Hemisphere         Around the equator
 */
/* 计算搜索范围的最大最小经纬度 */
int geohashBoundingBox(GeoShape *shape, double *bounds) {
    if (!bounds) return 0;
    double longitude = shape->xy[0];
    double latitude = shape->xy[1];
    /* 根据不同的搜索类型获取宽度和高度.
     * 注意：这个高度和宽度表示的是以中心点向上下和左右各延伸多少 m，是整个范围的高度和宽度的一半 */
    double height = shape->conversion * (shape->type == CIRCULAR_TYPE ? shape->t.radius : shape->t.r.height/2);
    double width = shape->conversion * (shape->type == CIRCULAR_TYPE ? shape->t.radius : shape->t.r.width/2);

    /* 下面的一堆计算看起来很麻烦，但是结合上面方法注释给的图像就很清除了
     * 用第一个图为例，在北半球，要计算最大最小经纬度，如果用一个矩形放到北半球，那么矩形的上边界跨的经度肯定比下边界跨的经度大
     * 而南半球相反
     *              /---\
     *        |---/------\--|
     *        | /    .    \ |
     *        /------------\|
     *      /               \
     * 左边界和右边界的纬度跨度都是一样的，不用特殊处理，需要计算的就是北半球的上边界和南半球的下边界跨的经度
     */

    /* 计算中心点到上下边界跨的纬度，公式就是通过圆弧计算出角度 (h / r) / (3.14 / 180)，即为跨的纬度 */
    const double lat_delta = rad_deg(height/EARTH_RADIUS_IN_METERS);
    /* 计算上边界所处于的纬度的经度跨度，公式 (w / r / cos(上边界的纬度 * 3.14 / 180)) / (3.14 / 180) */
    const double long_delta_top = rad_deg(width/EARTH_RADIUS_IN_METERS/cos(deg_rad(latitude+lat_delta)));
    /* 计算下边界所处于的纬度的经度跨度 */
    const double long_delta_bottom = rad_deg(width/EARTH_RADIUS_IN_METERS/cos(deg_rad(latitude-lat_delta)));
    /* The directions of the northern and southern hemispheres
     * are opposite, so we choice different points as min/max long/lat */
    int southern_hemisphere = latitude < 0 ? 1 : 0;
    /* 最小经度 */
    bounds[0] = southern_hemisphere ? longitude-long_delta_bottom : longitude-long_delta_top;
    /* 最大经度 */
    bounds[2] = southern_hemisphere ? longitude+long_delta_bottom : longitude+long_delta_top;
    /* 最小纬度 */
    bounds[1] = latitude - lat_delta;
    /* 最大纬度 */
    bounds[3] = latitude + lat_delta;
    return 1;
}

/* Calculate a set of areas (center + 8) that are able to cover a range query
 * for the specified position and shape (see geohash.h GeoShape).
 * the bounding box saved in shaple.bounds */
GeoHashRadius geohashCalculateAreasByShapeWGS84(GeoShape *shape) {
    /* 总经纬度的范围 */
    GeoHashRange long_range, lat_range;
    /* 当前坐标半径内的所有区域的信息 */
    GeoHashRadius radius;
    /* 当前区域的 geohash 信息 */
    GeoHashBits hash;
    /* 当前区域的附近八个区域的信息 */
    GeoHashNeighbors neighbors;
    /* 当前区域 */
    GeoHashArea area;
    /* 搜索范围区域的最大最小经纬度信息 */
    double min_lon, max_lon, min_lat, max_lat;
    int steps;

    /* 计算搜索区域的最大最小经纬度 */
    geohashBoundingBox(shape, shape->bounds);
    min_lon = shape->bounds[0];
    min_lat = shape->bounds[1];
    max_lon = shape->bounds[2];
    max_lat = shape->bounds[3];

    double longitude = shape->xy[0];
    double latitude = shape->xy[1];
    /* radius_meters is calculated differently in different search types:
     * 1) CIRCULAR_TYPE, just use radius.
     * 2) RECTANGLE_TYPE, we use sqrt((width/2)^2 + (height/2)^2) to
     * calculate the distance from the center point to the corner */
    /* 对搜索范围类型的不同计算不同的搜索范围半径，圆直接用半径，矩形计算中心点到顶点的距离 */
    double radius_meters = shape->type == CIRCULAR_TYPE ? shape->t.radius :
            sqrt((shape->t.r.width/2)*(shape->t.r.width/2) + (shape->t.r.height/2)*(shape->t.r.height/2));
    /* 乘以当前使用的单位 */
    radius_meters *= shape->conversion;

    /* 计算对经纬度都分割成 2 的多少次方刚好适合当前的区域，即该搜索范围粗略最多只会有 9 个区域（当前坐标点所在的区域和该区域附近的
     * 8 个区域，这 9 个区域不一定都在搜索范围内，后面会对其进行排除），可能出现搜索范围超出了这 9 个区域，后面会处理 */
    steps = geohashEstimateStepsByRadius(radius_meters,latitude);

    /* 填充经纬度范围值 */
    geohashGetCoordRange(&long_range,&lat_range);
    /* 对当前中心点坐标进行 geohash 编码，step 使用上面计算出的 steps */
    geohashEncode(&long_range,&lat_range,longitude,latitude,steps,&hash);
    /* 根据当前中心点 geohash 编码计算附近 8 个区域的 geohash 编码 */
    geohashNeighbors(&hash,&neighbors);
    /* 中心点解码获取到中心区域的坐标信息 */
    geohashDecode(long_range,lat_range,hash,&area);

    /* Check if the step is enough at the limits of the covered area.
     * Sometimes when the search area is near an edge of the
     * area, the estimated step is not small enough, since one of the
     * north / south / west / east square is too near to the search area
     * to cover everything. */
    /* 下面的作用域里面对搜索范围超出 9 个区域的情况进行处理 */
    int decrease_step = 0;
    {
        GeoHashArea north, south, east, west;

        /* 获取东南西北四个方向上的邻居区域的坐标信息 */
        geohashDecode(long_range, lat_range, neighbors.north, &north);
        geohashDecode(long_range, lat_range, neighbors.south, &south);
        geohashDecode(long_range, lat_range, neighbors.east, &east);
        geohashDecode(long_range, lat_range, neighbors.west, &west);

        /* 判断搜索范围是否越过了当前的 9 个区域范围 */
        /* 北方区域的纬度最大值小于搜索范围纬度最大值，越界 */
        if (north.latitude.max < max_lat) 
            decrease_step = 1;
        /* 南方区域的纬度最小值大于搜索范围纬度最小值，越界 */
        if (south.latitude.min > min_lat) 
            decrease_step = 1;
        /* 东方区域的经度最大值小于搜索范围经度最大值，越界 */
        if (east.longitude.max < max_lon) 
            decrease_step = 1;
        /* 西方区域的经度最小值大于搜索范围经度最小值，越界 */
        if (west.longitude.min > min_lon)  
            decrease_step = 1;
    }

    /* 如果搜索范围越界了，使用大一号的分割方式进行经纬度分割 */
    if (steps > 1 && decrease_step) {
        steps--;
        /* 下面重新计算中心区域和附近 8 个区域的坐标信息 */
        geohashEncode(&long_range,&lat_range,longitude,latitude,steps,&hash);
        geohashNeighbors(&hash,&neighbors);
        geohashDecode(long_range,lat_range,hash,&area);
    }

    /* Exclude the search areas that are useless. */
    /* 下面是 9 个区域中一些区域没有在搜索范围内的情况的处理，steps = 1 没必要处理 */
    if (steps >= 2) {
        /* 中心区域的纬度最小值小于搜索范围纬度最小值，将位于中心区域南边的三个区域的数据置 0。
         * 下面的几个判断都类似*/
        if (area.latitude.min < min_lat) {
            GZERO(neighbors.south);
            GZERO(neighbors.south_west);
            GZERO(neighbors.south_east);
        }
        if (area.latitude.max > max_lat) {
            GZERO(neighbors.north);
            GZERO(neighbors.north_east);
            GZERO(neighbors.north_west);
        }
        if (area.longitude.min < min_lon) {
            GZERO(neighbors.west);
            GZERO(neighbors.south_west);
            GZERO(neighbors.north_west);
        }
        if (area.longitude.max > max_lon) {
            GZERO(neighbors.east);
            GZERO(neighbors.south_east);
            GZERO(neighbors.north_east);
        }
    }
    /* 将最终的 9 个区域的信息放入 radius 中 */
    radius.hash = hash;
    radius.neighbors = neighbors;
    radius.area = area;
    return radius;
}

/* 52 字节对齐 */
GeoHashFix52Bits geohashAlign52Bits(const GeoHashBits hash) {
    uint64_t bits = hash.bits;
    bits <<= (52 - hash.step * 2);
    return bits;
}

/* Calculate distance using haversine great circle distance formula. */
/* 获取两个地点之间的距离
 * 通过两点的经纬度计算球面距离的公式 */
double geohashGetDistance(double lon1d, double lat1d, double lon2d, double lat2d) {
    double lat1r, lon1r, lat2r, lon2r, u, v;
    lat1r = deg_rad(lat1d);
    lon1r = deg_rad(lon1d);
    lat2r = deg_rad(lat2d);
    lon2r = deg_rad(lon2d);
    u = sin((lat2r - lat1r) / 2);
    v = sin((lon2r - lon1r) / 2);
    return 2.0 * EARTH_RADIUS_IN_METERS *
           asin(sqrt(u * u + cos(lat1r) * cos(lat2r) * v * v));
}

/* 获取 x1,y1 和 x2,y2 两个坐标之间的距离，判断是否超出半径 */
int geohashGetDistanceIfInRadius(double x1, double y1,
                                 double x2, double y2, double radius,
                                 double *distance) {
    *distance = geohashGetDistance(x1, y1, x2, y2);
    if (*distance > radius) return 0;
    return 1;
}

/* 判断坐标 x2,y2 是否在以 x1,y1 为中心，半径为 radius 的范围内，别返回中心点和坐标点的之间的距离
 * 直接计算两点之间的距离，判断是否超出半径即可 */
int geohashGetDistanceIfInRadiusWGS84(double x1, double y1, double x2,
                                      double y2, double radius,
                                      double *distance) {
    return geohashGetDistanceIfInRadius(x1, y1, x2, y2, radius, distance);
}

/* Judge whether a point is in the axis-aligned rectangle, when the distance
 * between a searched point and the center point is less than or equal to
 * height/2 or width/2 in height and width, the point is in the rectangle.
 *
 * width_m, height_m: the rectangle
 * x1, y1 : the center of the box
 * x2, y2 : the point to be searched
 */
/* 判断坐标 x2,y2 是否在以 x1,y1 为中兴，宽为 width_m 高位 height_m 的矩形范围中
 * 计算 x2,y2 以 x1,y1 为参考点的相对坐标（long_distance, lat_distance） 如果该相对坐标
 * 没有超过 （width_m/2 ,height_m/2）即在范围内 */
int geohashGetDistanceIfInRectangle(double width_m, double height_m, double x1, double y1,
                                    double x2, double y2, double *distance) {
    double lon_distance = geohashGetDistance(x2, y2, x1, y2);
    double lat_distance = geohashGetDistance(x2, y2, x2, y1);
    if (lon_distance > width_m/2 || lat_distance > height_m/2) {
        return 0;
    }
    *distance = geohashGetDistance(x1, y1, x2, y2);
    return 1;
}
