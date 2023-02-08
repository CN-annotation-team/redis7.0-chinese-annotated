/*
 * Copyright (c) 2014, Matt Stancliff <matt@genges.com>.
 * Copyright (c) 2015-2016, Salvatore Sanfilippo <antirez@gmail.com>.
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

#include "geo.h"
#include "geohash_helper.h"
#include "debugmacro.h"
#include "pqsort.h"

/* 该文件阅读顺序建议：
 * 先看 geoaddCommand,geoposCommand 这两个函数，了解地点的经纬度怎么存储和怎么取出来
 * 然后看 geodistCommand 函数，计算两个坐标之间的距离
 * 最后看 georadiusGeneric 范围搜索方法
 */

/* Things exported from t_zset.c only for geo.c, since it is the only other
 * part of Redis that requires close zset introspection. */
unsigned char *zzlFirstInRange(unsigned char *zl, zrangespec *range);
int zslValueLteMax(double value, zrangespec *spec);

/* ====================================================================
 * This file implements the following commands:
 *
 *   - geoadd - add coordinates for value to geoset
 *   - georadius - search radius by coordinates in geoset
 *   - georadiusbymember - search radius based on geoset member position
 * ==================================================================== */

/* ====================================================================
 * geoArray implementation
 * ==================================================================== */

/* Create a new array of geoPoints. */
/* 创建一个 geoArray */
geoArray *geoArrayCreate(void) {
    geoArray *ga = zmalloc(sizeof(*ga));
    /* It gets allocated on first geoArrayAppend() call. */
    ga->array = NULL;
    ga->buckets = 0;
    ga->used = 0;
    return ga;
}

/* Add a new entry and return its pointer so that the caller can populate
 * it with data. */
geoPoint *geoArrayAppend(geoArray *ga) {
    /* 如果给定的 ga 的桶的数量和被使用的桶的数量相同 */
    if (ga->used == ga->buckets) {
        /* 分配新的桶的数量，初始的时候加 8 个桶，之后每次扩容增加一倍的桶 */
        ga->buckets = (ga->buckets == 0) ? 8 : ga->buckets*2;
        ga->array = zrealloc(ga->array,sizeof(geoPoint)*ga->buckets);
    }
    /* 将 geoPoint 指向未被使用的第一个桶 */
    geoPoint *gp = ga->array+ga->used;
    ga->used++;
    return gp;
}

/* Destroy a geoArray created with geoArrayCreate(). */
/* 释放 geoArray 结构 */
void geoArrayFree(geoArray *ga) {
    size_t i;
    for (i = 0; i < ga->used; i++) sdsfree(ga->array[i].member);
    zfree(ga->array);
    zfree(ga);
}

/* ====================================================================
 * Helpers
 * ==================================================================== */
int decodeGeohash(double bits, double *xy) {
    /* bits 为编码，step 默认是 26 */
    GeoHashBits hash = { .bits = (uint64_t)bits, .step = GEO_STEP_MAX };
    /* 解码 */
    return geohashDecodeToLongLatWGS84(hash, xy);
}

/* Input Argument Helper */
/* Take a pointer to the latitude arg then use the next arg for longitude.
 * On parse error C_ERR is returned, otherwise C_OK. */
/* 该方法会将命令中的经纬度放到 xy 数组中，xy 只有两个槽位，第 1 个槽存放经度，第 2 个槽位存放纬度 */
int extractLongLatOrReply(client *c, robj **argv, double *xy) {
    int i;
    for (i = 0; i < 2; i++) {
        if (getDoubleFromObjectOrReply(c, argv[i], xy + i, NULL) !=
            C_OK) {
            return C_ERR;
        }
    }
    /* 判断经度和纬度是否在规定范围内 */
    if (xy[0] < GEO_LONG_MIN || xy[0] > GEO_LONG_MAX ||
        xy[1] < GEO_LAT_MIN  || xy[1] > GEO_LAT_MAX) {
        addReplyErrorFormat(c,
            "-ERR invalid longitude,latitude pair %f,%f\r\n",xy[0],xy[1]);
        return C_ERR;
    }
    return C_OK;
}

/* Input Argument Helper */
/* Decode lat/long from a zset member's score.
 * Returns C_OK on successful decoding, otherwise C_ERR is returned. */
/* 从 zset 中获取 member 地点的经纬度填入 xy 数组中 */
int longLatFromMember(robj *zobj, robj *member, double *xy) {
    double score = 0;

    if (zsetScore(zobj, member->ptr, &score) == C_ERR) return C_ERR;
    if (!decodeGeohash(score, xy)) return C_ERR;
    return C_OK;
}

/* Check that the unit argument matches one of the known units, and returns
 * the conversion factor to meters (you need to divide meters by the conversion
 * factor to convert to the right unit).
 *
 * If the unit is not valid, an error is reported to the client, and a value
 * less than zero is returned. */
/* 单位的换算，基础单位是 m，支持 km,ft,mi 等单位，需要将其换算成多少米 */
double extractUnitOrReply(client *c, robj *unit) {
    char *u = unit->ptr;

    if (!strcasecmp(u, "m")) {
        return 1;
    } else if (!strcasecmp(u, "km")) {
        return 1000;
    } else if (!strcasecmp(u, "ft")) {
        return 0.3048;
    } else if (!strcasecmp(u, "mi")) {
        return 1609.34;
    } else {
        addReplyError(c,
            "unsupported unit provided. please use M, KM, FT, MI");
        return -1;
    }
}

/* Input Argument Helper.
 * Extract the distance from the specified two arguments starting at 'argv'
 * that should be in the form: <number> <unit>, and return C_OK or C_ERR means success or failure
 * *conversions is populated with the coefficient to use in order to convert meters to the unit.*/
/* 该函数获取命令参数的 distance 参数，和换算 unit 参数 */
int extractDistanceOrReply(client *c, robj **argv,
                              double *conversion, double *radius) {
    double distance;
    if (getDoubleFromObjectOrReply(c, argv[0], &distance,
                                   "need numeric radius") != C_OK) {
        return C_ERR;
    }

    if (distance < 0) {
        addReplyError(c,"radius cannot be negative");
        return C_ERR;
    }
    if (radius) *radius = distance;

    double to_meters = extractUnitOrReply(c,argv[1]);
    if (to_meters < 0) {
        return C_ERR;
    }

    if (conversion) *conversion = to_meters;
    return C_OK;
}

/* Input Argument Helper.
 * Extract height and width from the specified three arguments starting at 'argv'
 * that should be in the form: <number> <number> <unit>, and return C_OK or C_ERR means success or failure
 * *conversions is populated with the coefficient to use in order to convert meters to the unit.*/
int extractBoxOrReply(client *c, robj **argv, double *conversion,
                         double *width, double *height) {
    double h, w;
    if ((getDoubleFromObjectOrReply(c, argv[0], &w, "need numeric width") != C_OK) ||
        (getDoubleFromObjectOrReply(c, argv[1], &h, "need numeric height") != C_OK)) {
        return C_ERR;
    }

    if (h < 0 || w < 0) {
        addReplyError(c, "height or width cannot be negative");
        return C_ERR;
    }
    if (height) *height = h;
    if (width) *width = w;

    double to_meters = extractUnitOrReply(c,argv[2]);
    if (to_meters < 0) {
        return C_ERR;
    }

    if (conversion) *conversion = to_meters;
    return C_OK;
}

/* The default addReplyDouble has too much accuracy.  We use this
 * for returning location distances. "5.2145 meters away" is nicer
 * than "5.2144992818115 meters away." We provide 4 digits after the dot
 * so that the returned value is decently accurate even when the unit is
 * the kilometer. */
void addReplyDoubleDistance(client *c, double d) {
    char dbuf[128];
    int dlen = snprintf(dbuf, sizeof(dbuf), "%.4f", d);
    addReplyBulkCBuffer(c, dbuf, dlen);
}

/* Helper function for geoGetPointsInRange(): given a sorted set score
 * representing a point, and a GeoShape, appends this entry as a geoPoint
 * into the specified geoArray only if the point is within the search area.
 *
 * returns C_OK if the point is included, or C_ERR if it is outside. */
/* 计算成员是否在搜索范围内 */
int geoAppendIfWithinShape(geoArray *ga, GeoShape *shape, double score, sds member) {
    double distance = 0, xy[2];

    /* 解码成员的 geohash 获取坐标 */
    if (!decodeGeohash(score,xy)) return C_ERR; /* Can't decode. */
    /* Note that geohashGetDistanceIfInRadiusWGS84() takes arguments in
     * reverse order: longitude first, latitude later. */
    /* 搜索类型为圆形的情况 */
    if (shape->type == CIRCULAR_TYPE) {
        if (!geohashGetDistanceIfInRadiusWGS84(shape->xy[0], shape->xy[1], xy[0], xy[1],
                                               shape->t.radius*shape->conversion, &distance)) return C_ERR;
    } else if (shape->type == RECTANGLE_TYPE) {
        /* 搜索类型为矩形的情况 */
        if (!geohashGetDistanceIfInRectangle(shape->t.r.width * shape->conversion,
                                             shape->t.r.height * shape->conversion,
                                             shape->xy[0], shape->xy[1], xy[0], xy[1], &distance))
            return C_ERR;
    }

    /* Append the new element. */
    /* 符合条件了，将成员信息封装成 geoPoint 数据结构，方法 ga 中 */
    geoPoint *gp = geoArrayAppend(ga);
    gp->longitude = xy[0];
    gp->latitude = xy[1];
    gp->dist = distance;
    gp->member = member;
    gp->score = score;
    return C_OK;
}

/* Query a Redis sorted set to extract all the elements between 'min' and
 * 'max', appending them into the array of geoPoint structures 'geoArray'.
 * The command returns the number of elements added to the array.
 *
 * Elements which are farther than 'radius' from the specified 'x' and 'y'
 * coordinates are not included.
 *
 * The ability of this function to append to an existing set of points is
 * important for good performances because querying by radius is performed
 * using multiple queries to the sorted set, that we later need to sort
 * via qsort. Similarly we need to be able to reject points outside the search
 * radius area ASAP in order to allocate and process more points than needed. */
int geoGetPointsInRange(robj *zobj, double min, double max, GeoShape *shape, geoArray *ga, unsigned long limit) {
    /* minex 0 = include min in range; maxex 1 = exclude max in range */
    /* That's: min <= val < max */
    /* 填充范围区域，最大值需要排除，所以 .maxex = 1 */
    zrangespec range = { .min = min, .max = max, .minex = 0, .maxex = 1 };
    size_t origincount = ga->used;
    sds member;

    /* 针对不同的 zset 编码，做范围查询 */
    if (zobj->encoding == OBJ_ENCODING_LISTPACK) {
        unsigned char *zl = zobj->ptr;
        unsigned char *eptr, *sptr;
        unsigned char *vstr = NULL;
        unsigned int vlen = 0;
        long long vlong = 0;
        double score = 0;

        if ((eptr = zzlFirstInRange(zl, &range)) == NULL) {
            /* Nothing exists starting at our min.  No results. */
            return 0;
        }

        sptr = lpNext(zl, eptr);
        while (eptr) {
            score = zzlGetScore(sptr);

            /* If we fell out of range, break. */
            if (!zslValueLteMax(score, &range))
                break;

            vstr = lpGetValue(eptr, &vlen, &vlong);
            member = (vstr == NULL) ? sdsfromlonglong(vlong) :
                                      sdsnewlen(vstr,vlen);
            /* 判断成员是否在搜索范围内，在范围内的成员信息会放入 ga 中 */
            if (geoAppendIfWithinShape(ga,shape,score,member)
                == C_ERR) sdsfree(member);
            if (ga->used && limit && ga->used >= limit) break;
            zzlNext(zl, &eptr, &sptr);
        }
    } else if (zobj->encoding == OBJ_ENCODING_SKIPLIST) {
        zset *zs = zobj->ptr;
        zskiplist *zsl = zs->zsl;
        zskiplistNode *ln;

        if ((ln = zslFirstInRange(zsl, &range)) == NULL) {
            /* Nothing exists starting at our min.  No results. */
            return 0;
        }

        while (ln) {
            sds ele = ln->ele;
            /* Abort when the node is no longer in range. */
            if (!zslValueLteMax(ln->score, &range))
                break;

            ele = sdsdup(ele);
            /* 判断成员是否在搜索范围内，在范围内的成员信息会放入 ga 中 */
            if (geoAppendIfWithinShape(ga,shape,ln->score,ele)
                == C_ERR) sdsfree(ele);
            if (ga->used && limit && ga->used >= limit) break;
            ln = ln->level[0].forward;
        }
    }
    return ga->used - origincount;
}

/* Compute the sorted set scores min (inclusive), max (exclusive) we should
 * query in order to retrieve all the elements inside the specified area
 * 'hash'. The two scores are returned by reference in *min and *max. */
/* 获取给定的 hash 做 52 字节对齐后的区间范围，左闭右开 [min max) */
void scoresOfGeoHashBox(GeoHashBits hash, GeoHashFix52Bits *min, GeoHashFix52Bits *max) {
    /* We want to compute the sorted set scores that will include all the
     * elements inside the specified Geohash 'hash', which has as many
     * bits as specified by hash.step * 2.
     *
     * So if step is, for example, 3, and the hash value in binary
     * is 101010, since our score is 52 bits we want every element which
     * is in binary: 101010?????????????????????????????????????????????
     * Where ? can be 0 or 1.
     *
     * To get the min score we just use the initial hash value left
     * shifted enough to get the 52 bit value. Later we increment the
     * 6 bit prefix (see the hash.bits++ statement), and get the new
     * prefix: 101011, which we align again to 52 bits to get the maximum
     * value (which is excluded from the search). So we get everything
     * between the two following scores (represented in binary):
     *
     * 1010100000000000000000000000000000000000000000000000 (included)
     * and
     * 1010110000000000000000000000000000000000000000000000 (excluded).
     */
    /* 做 52 字节对齐之后的就是最小值 */
    *min = geohashAlign52Bits(hash);
    /* 对 hash.bits + 1 之后做 52 字节对齐就是最大值 + 1 的情况 */
    hash.bits++;
    *max = geohashAlign52Bits(hash);
}

/* Obtain all members between the min/max of this geohash bounding box.
 * Populate a geoArray of GeoPoints by calling geoGetPointsInRange().
 * Return the number of points added to the array. */
int membersOfGeoHashBox(robj *zobj, GeoHashBits hash, geoArray *ga, GeoShape *shape, unsigned long limit) {
    GeoHashFix52Bits min, max;

    /* 获取当前区域的 geohash 码做 52 字节对齐后，区域中的所有按 step=26 进行分割的区域的 geohash 的最大最小值 (左闭右开区间) */
    scoresOfGeoHashBox(hash,&min,&max);
    /* 获取 zset 中在最小值至最大值之间的成员 */
    return geoGetPointsInRange(zobj, min, max, shape, ga, limit);
}

/* Search all eight neighbors + self geohash box */
/* 查找所有在 n 中表示的九个区域范围内的地点成员
 * 先说一下查找的流程
 * 1. 遍历这九个区域，获取其 geohash 码，做 52 字节对齐，例如该区域的 geohash码为
 *  0000 0000 0000 0000 0000 0000 0000 0000 0000 0000 1111 1111 1111 1111 1111 1111
 *  做 52 字节对齐后如下：
 *  0000 0000 0000 1111 1111 1111 1111 1111 1111 0000 0000 0000 0000 0000 0000 0000
 * 2. 我们用 geoadd 添加的成员的 step 都是 26，所以可以认为在
 *  0000 0000 0000 1111 1111 1111 1111 1111 1111 0000 0000 0000 0000 0000 0000 0000
 *  至
 *  0000 0000 0000 1111 1111 1111 1111 1111 1111 1111 1111 1111 1111 1111 1111 1111
 *  这个范围内的 geohash 码都是该区域的
 * 3. 用这两个最大值和最小值在 zset 中查找出 score 在这个范围内的成员就是该区域中所有存储的地点成员
 */
int membersOfAllNeighbors(robj *zobj, const GeoHashRadius *n, GeoShape *shape, geoArray *ga, unsigned long limit) {
    GeoHashBits neighbors[9];
    unsigned int i, count = 0, last_processed = 0;
    int debugmsg = 0;

    /* 将九个区域的 geohash 编码信息放到数组中 */
    neighbors[0] = n->hash;
    neighbors[1] = n->neighbors.north;
    neighbors[2] = n->neighbors.south;
    neighbors[3] = n->neighbors.east;
    neighbors[4] = n->neighbors.west;
    neighbors[5] = n->neighbors.north_east;
    neighbors[6] = n->neighbors.north_west;
    neighbors[7] = n->neighbors.south_east;
    neighbors[8] = n->neighbors.south_west;

    /* For each neighbor (*and* our own hashbox), get all the matching
     * members and add them to the potential result list. */
    /* 遍历九个区域 */
    for (i = 0; i < sizeof(neighbors) / sizeof(*neighbors); i++) {
        /* geohash 里面的属性都为 0 的情况就是之前判断区域不在搜索范围区域中情况，直接跳过 */
        if (HASHISZERO(neighbors[i])) {
            if (debugmsg) D("neighbors[%d] is zero",i);
            continue;
        }

        /* Debugging info. */
        if (debugmsg) {
            GeoHashRange long_range, lat_range;
            geohashGetCoordRange(&long_range,&lat_range);
            GeoHashArea myarea = {{0}};
            geohashDecode(long_range, lat_range, neighbors[i], &myarea);

            /* Dump center square. */
            D("neighbors[%d]:\n",i);
            D("area.longitude.min: %f\n", myarea.longitude.min);
            D("area.longitude.max: %f\n", myarea.longitude.max);
            D("area.latitude.min: %f\n", myarea.latitude.min);
            D("area.latitude.max: %f\n", myarea.latitude.max);
            D("\n");
        }

        /* When a huge Radius (in the 5000 km range or more) is used,
         * adjacent neighbors can be the same, leading to duplicated
         * elements. Skip every range which is the same as the one
         * processed previously. */
        /* 这里是当搜索范围的半径超过 5000km 的情况，对之前处理过的区域不做处理
         * 默认的 MERCATOR_MAX 为赤道的一半长度即 20000km，所以 径大于等于 5000km 的时候经过 geohash_helper.c 中的 geohashEstimateStepsByRadius
         * 函数计算后地球只会被分成 4 块，即和默认的 9 块区域有冲突，会出现区域重合 */
        if (last_processed &&
            neighbors[i].bits == neighbors[last_processed].bits &&
            neighbors[i].step == neighbors[last_processed].step)
        {
            if (debugmsg)
                D("Skipping processing of %d, same as previous\n",i);
            continue;
        }
        if (ga->used && limit && ga->used >= limit) break;
        /* 查找当前遍历区域范围内的成员 */
        count += membersOfGeoHashBox(zobj, neighbors[i], ga, shape, limit);
        /* 记录本次处理的区域 */
        last_processed = i;
    }
    return count;
}

/* Sort comparators for qsort() */
static int sort_gp_asc(const void *a, const void *b) {
    const struct geoPoint *gpa = a, *gpb = b;
    /* We can't do adist - bdist because they are doubles and
     * the comparator returns an int. */
    if (gpa->dist > gpb->dist)
        return 1;
    else if (gpa->dist == gpb->dist)
        return 0;
    else
        return -1;
}

static int sort_gp_desc(const void *a, const void *b) {
    return -sort_gp_asc(a, b);
}

/* ====================================================================
 * Commands
 * ==================================================================== */

/* GEOADD key [CH] [NX|XX] long lat name [long2 lat2 name2 ... longN latN nameN] */
/* geoadd 命令 */
void geoaddCommand(client *c) {
    /* xx 记录命令中 xx 参数是否存在，nx 记录命令中 nx 参数是否存在，longidx 记录 geoadd key [ch] [nx|xx] 这几个参数的数量
     * geoadd key 是必须要的所以默认参数是 2 */
    int xx = 0, nx = 0, longidx = 2;
    int i;

    /* Parse options. At the end 'longidx' is set to the argument position
     * of the longitude of the first element. */
    /* 这里会遍历所有参数，给 nx,xx 赋值，并获取 longidx 的实际大小 */
    while (longidx < c->argc) {
        char *opt = c->argv[longidx]->ptr;
        if (!strcasecmp(opt,"nx")) nx = 1;
        else if (!strcasecmp(opt,"xx")) xx = 1;
        else if (!strcasecmp(opt,"ch")) { /* Handle in zaddCommand. */ }
        else break;
        longidx++;
    }

    /* 1. 如果所有参数的数量 - longidx 后必须能整除 3，（即参数后面的一串不符合 [经度 纬度 地名 ....] 这种格式）
     * 2. xx 和 nx 只能有一个
     * 不符合这两种情况，参数错误 */
    if ((c->argc - longidx) % 3 || (xx && nx)) {
        /* Need an odd number of arguments if we got this far... */
            addReplyErrorObject(c,shared.syntaxerr);
        return;
    }

    /* Set up the vector for calling ZADD. */
    /* 计算有多少个地点的经纬度 */
    int elements = (c->argc - longidx) / 3;
    int argc = longidx+elements*2; /* ZADD key [CH] [NX|XX] score ele ... */
    robj **argv = zcalloc(argc*sizeof(robj*));
    argv[0] = createRawStringObject("zadd",4);
    for (i = 1; i < longidx; i++) {
        argv[i] = c->argv[i];
        incrRefCount(argv[i]);
    }

    /* Create the argument vector to call ZADD in order to add all
     * the score,value pairs to the requested zset, where score is actually
     * an encoded version of lat,long. */
    for (i = 0; i < elements; i++) {
        /* 定义一个两个槽位的数组用来存放当前地点的经纬度 */
        double xy[2];

        /* (c->argv + longidx)+(i*3) 指向目前遍历到的元素的第一个参数，该函数会获取到经纬度放入 xy 数组中 */
        if (extractLongLatOrReply(c, (c->argv+longidx)+(i*3),xy) == C_ERR) {
            for (i = 0; i < argc; i++)
                if (argv[i]) decrRefCount(argv[i]);
            zfree(argv);
            return;
        }

        /* Turn the coordinates into the score of the element. */
        GeoHashBits hash;
        /* 对经纬度进行编码，GEO_STEP_MAX 为 26，表示将经纬度都分割成 2^26 次方块  */
        geohashEncodeWGS84(xy[0], xy[1], GEO_STEP_MAX, &hash);
        /* 做 52 字节对齐，对于 step 小于 26 的情况。
         * 这里需要注意：对于编码后的结果，如果 step 越小，那么占用的 bit 为越少，编码值也就越小，如果 step 不同，就无法比较了，
         * 所以需要将 step 小于 26 的编码值向左移 （52 - step*2）位，这样就只是精确度变低了，还是能比较 */
        GeoHashFix52Bits bits = geohashAlign52Bits(hash);
        /* 将编码值作为分数 */
        robj *score = createObject(OBJ_STRING, sdsfromlonglong(bits));
        /* 获取地点 */
        robj *val = c->argv[longidx + i * 3 + 2];
        argv[longidx+i*2] = score;
        argv[longidx+1+i*2] = val;
        incrRefCount(val);
    }

    /* Finally call ZADD that will do the work for us. */
    replaceClientCommandVector(c,argc,argv);
    /* 存入 zset */
    zaddCommand(c);
}

#define SORT_NONE 0
#define SORT_ASC 1
#define SORT_DESC 2

#define RADIUS_COORDS (1<<0)    /* Search around coordinates. */
#define RADIUS_MEMBER (1<<1)    /* Search around member. */
#define RADIUS_NOSTORE (1<<2)   /* Do not accept STORE/STOREDIST option. */
#define GEOSEARCH (1<<3)        /* GEOSEARCH command variant (different arguments supported) */
#define GEOSEARCHSTORE (1<<4)   /* GEOSEARCHSTORE just accept STOREDIST option */

/* GEORADIUS key x y radius unit [WITHDIST] [WITHHASH] [WITHCOORD] [ASC|DESC]
 *                               [COUNT count [ANY]] [STORE key] [STOREDIST key]
 * GEORADIUSBYMEMBER key member radius unit ... options ...
 * GEOSEARCH key [FROMMEMBER member] [FROMLONLAT long lat] [BYRADIUS radius unit]
 *               [BYBOX width height unit] [WITHCOORD] [WITHDIST] [WITHASH] [COUNT count [ANY]] [ASC|DESC]
 * GEOSEARCHSTORE dest_key src_key [FROMMEMBER member] [FROMLONLAT long lat] [BYRADIUS radius unit]
 *               [BYBOX width height unit] [COUNT count [ANY]] [ASC|DESC] [STOREDIST]
 *  */
/* 范围搜索的入口方法，该方法代码很多，但是真正和搜索有关的代码只有
 * geohashCalculateAreasByShapeWGS84，membersOfAllNeighbors
 * 这两个函数，可以直接定位到这两个函数开始看，
 * 粗略说一下搜索过程，
 * 1. 先计算适合搜索范围的分块方式，即寻找合适的 step 对经纬度进行分割，
 * 做到尽量可以让搜索范围处于 9 个区域中（中心点所在的区域以及其附近 8 个区域）。
 * 2. 获取坐标在这 9 个区域范围内的成员。
 * 3. 判断步骤 2 中获得的成员是否在搜索范围内。
 * 注：这里先判断坐标是否在 9 个区域范围内，再过滤在搜索范围内的成员是因为直接判断
 * 成员在搜索范围内的计算量会更大 */
void georadiusGeneric(client *c, int srcKeyIndex, int flags) {
    robj *storekey = NULL;
    int storedist = 0; /* 0 for STORE, 1 for STOREDIST. */

    /* Look up the requested zset */
    robj *zobj = lookupKeyRead(c->db, c->argv[srcKeyIndex]);
    if (checkType(c, zobj, OBJ_ZSET)) return;

    /* Find long/lat to use for radius or box search based on inquiry type */
    int base_args;
    /* 定义 GeoShape 用于保存搜索范围信息 */
    GeoShape shape = {0};
    /* 如果是根据坐标进行范围搜索 */
    if (flags & RADIUS_COORDS) {
        /* GEORADIUS or GEORADIUS_RO */
        base_args = 6;
        /* 搜索范围类型是圆形 */
        shape.type = CIRCULAR_TYPE;
        /* 获取坐标经纬度放入 shape.xy 中 */
        if (extractLongLatOrReply(c, c->argv + 2, shape.xy) == C_ERR) return;
        /* 获取单位填充 shape.conversion，获取圆的半径填充 shape.t.radius */
        if (extractDistanceOrReply(c, c->argv+base_args-2, &shape.conversion, &shape.t.radius) != C_OK) return;
    } else if ((flags & RADIUS_MEMBER) && !zobj) {
        /* We don't have a source key, but we need to proceed with argument
         * parsing, so we know which reply to use depending on the STORE flag. */
        base_args = 5;
    } else if (flags & RADIUS_MEMBER) {
        /* GEORADIUSBYMEMBER or GEORADIUSBYMEMBER_RO */
        base_args = 5;
        shape.type = CIRCULAR_TYPE;
        robj *member = c->argv[2];
        if (longLatFromMember(zobj, member, shape.xy) == C_ERR) {
            addReplyError(c, "could not decode requested zset member");
            return;
        }
        if (extractDistanceOrReply(c, c->argv+base_args-2, &shape.conversion, &shape.t.radius) != C_OK) return;
    } else if (flags & GEOSEARCH) {
        /* GEOSEARCH or GEOSEARCHSTORE */
        base_args = 2;
        if (flags & GEOSEARCHSTORE) {
            base_args = 3;
            storekey = c->argv[1];
        }
    } else {
        addReplyError(c, "Unknown georadius search type");
        return;
    }

    /* Discover and populate all optional parameters. */
    int withdist = 0, withhash = 0, withcoords = 0;
    int frommember = 0, fromloc = 0, byradius = 0, bybox = 0;
    int sort = SORT_NONE;
    int any = 0; /* any=1 means a limited search, stop as soon as enough results were found. */
    long long count = 0;  /* Max number of results to return. 0 means unlimited. */
    if (c->argc > base_args) {
        int remaining = c->argc - base_args;
        for (int i = 0; i < remaining; i++) {
            char *arg = c->argv[base_args + i]->ptr;
            if (!strcasecmp(arg, "withdist")) {
                withdist = 1;
            } else if (!strcasecmp(arg, "withhash")) {
                withhash = 1;
            } else if (!strcasecmp(arg, "withcoord")) {
                withcoords = 1;
            } else if (!strcasecmp(arg, "any")) {
                any = 1;
            } else if (!strcasecmp(arg, "asc")) {
                sort = SORT_ASC;
            } else if (!strcasecmp(arg, "desc")) {
                sort = SORT_DESC;
            } else if (!strcasecmp(arg, "count") && (i+1) < remaining) {
                if (getLongLongFromObjectOrReply(c, c->argv[base_args+i+1],
                                                 &count, NULL) != C_OK) return;
                if (count <= 0) {
                    addReplyError(c,"COUNT must be > 0");
                    return;
                }
                i++;
            } else if (!strcasecmp(arg, "store") &&
                       (i+1) < remaining &&
                       !(flags & RADIUS_NOSTORE) &&
                       !(flags & GEOSEARCH))
            {
                storekey = c->argv[base_args+i+1];
                storedist = 0;
                i++;
            } else if (!strcasecmp(arg, "storedist") &&
                       (i+1) < remaining &&
                       !(flags & RADIUS_NOSTORE) &&
                       !(flags & GEOSEARCH))
            {
                storekey = c->argv[base_args+i+1];
                storedist = 1;
                i++;
            } else if (!strcasecmp(arg, "storedist") &&
                       (flags & GEOSEARCH) &&
                       (flags & GEOSEARCHSTORE))
            {
                storedist = 1;
            } else if (!strcasecmp(arg, "frommember") &&
                      (i+1) < remaining &&
                      flags & GEOSEARCH &&
                      !fromloc)
            {
                /* No source key, proceed with argument parsing and return an error when done. */
                if (zobj == NULL) {
                    frommember = 1;
                    i++;
                    continue;
                }

                if (longLatFromMember(zobj, c->argv[base_args+i+1], shape.xy) == C_ERR) {
                    addReplyError(c, "could not decode requested zset member");
                    return;
                }
                frommember = 1;
                i++;
            } else if (!strcasecmp(arg, "fromlonlat") &&
                       (i+2) < remaining &&
                       flags & GEOSEARCH &&
                       !frommember)
            {
                if (extractLongLatOrReply(c, c->argv+base_args+i+1, shape.xy) == C_ERR) return;
                fromloc = 1;
                i += 2;
            } else if (!strcasecmp(arg, "byradius") &&
                       (i+2) < remaining &&
                       flags & GEOSEARCH &&
                       !bybox)
            {
                if (extractDistanceOrReply(c, c->argv+base_args+i+1, &shape.conversion, &shape.t.radius) != C_OK)
                    return;
                shape.type = CIRCULAR_TYPE;
                byradius = 1;
                i += 2;
            } else if (!strcasecmp(arg, "bybox") &&
                       (i+3) < remaining &&
                       flags & GEOSEARCH &&
                       !byradius)
            {
                if (extractBoxOrReply(c, c->argv+base_args+i+1, &shape.conversion, &shape.t.r.width,
                        &shape.t.r.height) != C_OK) return;
                shape.type = RECTANGLE_TYPE;
                bybox = 1;
                i += 3;
            } else {
                addReplyErrorObject(c,shared.syntaxerr);
                return;
            }
        }
    }

    /* Trap options not compatible with STORE and STOREDIST. */
    if (storekey && (withdist || withhash || withcoords)) {
        addReplyErrorFormat(c,
            "%s is not compatible with WITHDIST, WITHHASH and WITHCOORD options",
            flags & GEOSEARCHSTORE? "GEOSEARCHSTORE": "STORE option in GEORADIUS");
        return;
    }

    if ((flags & GEOSEARCH) && !(frommember || fromloc)) {
        addReplyErrorFormat(c,
            "exactly one of FROMMEMBER or FROMLONLAT can be specified for %s",
            (char *)c->argv[0]->ptr);
        return;
    }

    if ((flags & GEOSEARCH) && !(byradius || bybox)) {
        addReplyErrorFormat(c,
            "exactly one of BYRADIUS and BYBOX can be specified for %s",
            (char *)c->argv[0]->ptr);
        return;
    }

    if (any && !count) {
        addReplyErrorFormat(c, "the ANY argument requires COUNT argument");
        return;
    }

    /* Return ASAP when src key does not exist. */
    if (zobj == NULL) {
        if (storekey) {
            /* store key is not NULL, try to delete it and return 0. */
            if (dbDelete(c->db, storekey)) {
                signalModifiedKey(c, c->db, storekey);
                notifyKeyspaceEvent(NOTIFY_GENERIC, "del", storekey, c->db->id);
                server.dirty++;
            }
            addReply(c, shared.czero);
        } else {
            /* Otherwise we return an empty array. */
            addReply(c, shared.emptyarray);
        }
        return;
    }

    /* COUNT without ordering does not make much sense (we need to
     * sort in order to return the closest N entries),
     * force ASC ordering if COUNT was specified but no sorting was
     * requested. Note that this is not needed for ANY option. */
    if (count != 0 && sort == SORT_NONE && !any) sort = SORT_ASC;

    /* Get all neighbor geohash boxes for our radius search */
    /* 获取中心区域和附近 8 个区域的信息 */
    GeoHashRadius georadius = geohashCalculateAreasByShapeWGS84(&shape);

    /* Search the zset for all matching points */
    /* 创建一个 geoArray 数据结构来存储在搜索范围内的地点成员 */
    geoArray *ga = geoArrayCreate();
    /* 查找在 georadius 的九个区域内的地点成员，并将在 shape 搜索范围内的成员放入 ga 结构中 */
    membersOfAllNeighbors(zobj, &georadius, &shape, ga, any ? count : 0);

    /* If no matching results, the user gets an empty reply. */
    if (ga->used == 0 && storekey == NULL) {
        addReply(c,shared.emptyarray);
        geoArrayFree(ga);
        return;
    }

    long result_length = ga->used;
    long returned_items = (count == 0 || result_length < count) ?
                          result_length : count;
    long option_length = 0;

    /* Process [optional] requested sorting */
    if (sort != SORT_NONE) {
        int (*sort_gp_callback)(const void *a, const void *b) = NULL;
        if (sort == SORT_ASC) {
            sort_gp_callback = sort_gp_asc;
        } else if (sort == SORT_DESC) {
            sort_gp_callback = sort_gp_desc;
        }

        if (returned_items == result_length) {
            qsort(ga->array, result_length, sizeof(geoPoint), sort_gp_callback);
        } else {
            pqsort(ga->array, result_length, sizeof(geoPoint), sort_gp_callback,
                0, (returned_items - 1));
        }
    }

    if (storekey == NULL) {
        /* No target key, return results to user. */

        /* Our options are self-contained nested multibulk replies, so we
         * only need to track how many of those nested replies we return. */
        if (withdist)
            option_length++;

        if (withcoords)
            option_length++;

        if (withhash)
            option_length++;

        /* The array len we send is exactly result_length. The result is
         * either all strings of just zset members  *or* a nested multi-bulk
         * reply containing the zset member string _and_ all the additional
         * options the user enabled for this request. */
        addReplyArrayLen(c, returned_items);

        /* Finally send results back to the caller */
        int i;
        for (i = 0; i < returned_items; i++) {
            geoPoint *gp = ga->array+i;
            gp->dist /= shape.conversion; /* Fix according to unit. */

            /* If we have options in option_length, return each sub-result
             * as a nested multi-bulk.  Add 1 to account for result value
             * itself. */
            if (option_length)
                addReplyArrayLen(c, option_length + 1);

            addReplyBulkSds(c,gp->member);
            gp->member = NULL;

            if (withdist)
                addReplyDoubleDistance(c, gp->dist);

            if (withhash)
                addReplyLongLong(c, gp->score);

            if (withcoords) {
                addReplyArrayLen(c, 2);
                addReplyHumanLongDouble(c, gp->longitude);
                addReplyHumanLongDouble(c, gp->latitude);
            }
        }
    } else {
        /* Target key, create a sorted set with the results. */
        robj *zobj;
        zset *zs;
        int i;
        size_t maxelelen = 0, totelelen = 0;

        if (returned_items) {
            zobj = createZsetObject();
            zs = zobj->ptr;
        }

        for (i = 0; i < returned_items; i++) {
            zskiplistNode *znode;
            geoPoint *gp = ga->array+i;
            gp->dist /= shape.conversion; /* Fix according to unit. */
            double score = storedist ? gp->dist : gp->score;
            size_t elelen = sdslen(gp->member);

            if (maxelelen < elelen) maxelelen = elelen;
            totelelen += elelen;
            znode = zslInsert(zs->zsl,score,gp->member);
            serverAssert(dictAdd(zs->dict,gp->member,&znode->score) == DICT_OK);
            gp->member = NULL;
        }

        if (returned_items) {
            zsetConvertToListpackIfNeeded(zobj,maxelelen,totelelen);
            setKey(c,c->db,storekey,zobj,0);
            decrRefCount(zobj);
            notifyKeyspaceEvent(NOTIFY_ZSET,flags & GEOSEARCH ? "geosearchstore" : "georadiusstore",storekey,
                                c->db->id);
            server.dirty += returned_items;
        } else if (dbDelete(c->db,storekey)) {
            signalModifiedKey(c,c->db,storekey);
            notifyKeyspaceEvent(NOTIFY_GENERIC,"del",storekey,c->db->id);
            server.dirty++;
        }
        addReplyLongLong(c, returned_items);
    }
    geoArrayFree(ga);
}

/* GEORADIUS wrapper function. */
void georadiusCommand(client *c) {
    georadiusGeneric(c, 1, RADIUS_COORDS);
}

/* GEORADIUSBYMEMBER wrapper function. */
void georadiusbymemberCommand(client *c) {
    georadiusGeneric(c, 1, RADIUS_MEMBER);
}

/* GEORADIUS_RO wrapper function. */
void georadiusroCommand(client *c) {
    georadiusGeneric(c, 1, RADIUS_COORDS|RADIUS_NOSTORE);
}

/* GEORADIUSBYMEMBER_RO wrapper function. */
void georadiusbymemberroCommand(client *c) {
    georadiusGeneric(c, 1, RADIUS_MEMBER|RADIUS_NOSTORE);
}

void geosearchCommand(client *c) {
    georadiusGeneric(c, 1, GEOSEARCH);
}

void geosearchstoreCommand(client *c) {
    georadiusGeneric(c, 2, GEOSEARCH|GEOSEARCHSTORE);
}

/* GEOHASH key ele1 ele2 ... eleN
 *
 * Returns an array with an 11 characters geohash representation of the
 * position of the specified elements. */
/* 返回地点的 hash 编码值 */
void geohashCommand(client *c) {
    char *geoalphabet= "0123456789bcdefghjkmnpqrstuvwxyz";
    int j;

    /* Look up the requested zset */
    robj *zobj = lookupKeyRead(c->db, c->argv[1]);
    if (checkType(c, zobj, OBJ_ZSET)) return;

    /* Geohash elements one after the other, using a null bulk reply for
     * missing elements. */
    addReplyArrayLen(c,c->argc-2);
    /* 遍历所有地点 */
    for (j = 2; j < c->argc; j++) {
        double score;
        /* 获取当前地点的分数 */
        if (!zobj || zsetScore(zobj, c->argv[j]->ptr, &score) == C_ERR) {
            addReplyNull(c);
        } else {
            /* The internal format we use for geocoding is a bit different
             * than the standard, since we use as initial latitude range
             * -85,85, while the normal geohashing algorithm uses -90,90.
             * So we have to decode our position and re-encode using the
             * standard ranges in order to output a valid geohash string. */

            /* Decode... */
            /* 解码获得经纬度 */
            double xy[2];
            if (!decodeGeohash(score,xy)) {
                addReplyNull(c);
                continue;
            }

            /* Re-encode */
            GeoHashRange r[2];
            GeoHashBits hash;
            r[0].min = -180;
            r[0].max = 180;
            r[1].min = -90;
            r[1].max = 90;
            /* 再次编码，用 step = 26 来进行编码，这样获取到的编码就都是同一种 step 计算出来的了 */
            geohashEncode(&r[0],&r[1],xy[0],xy[1],26,&hash);

            char buf[12];
            int i;
            for (i = 0; i < 11; i++) {
                int idx;
                if (i == 10) {
                    /* We have just 52 bits, but the API used to output
                     * an 11 bytes geohash. For compatibility we assume
                     * zero. */
                    idx = 0;
                } else {
                    /* 每 5 个 bit 位作为一个索引 */
                    idx = (hash.bits >> (52-((i+1)*5))) & 0x1f;
                }
                /* 从 geoalphabet 字符数组中获取字符填充到 hash 码中 */
                buf[i] = geoalphabet[idx];
            }
            buf[11] = '\0';
            addReplyBulkCBuffer(c,buf,11);
        }
    }
}

/* GEOPOS key ele1 ele2 ... eleN
 *
 * Returns an array of two-items arrays representing the x,y position of each
 * element specified in the arguments. For missing elements NULL is returned. */
void geoposCommand(client *c) {
    int j;

    /* Look up the requested zset */
    /* 从数据库中根据 key 获取对应的 zset */
    robj *zobj = lookupKeyRead(c->db, c->argv[1]);
    if (checkType(c, zobj, OBJ_ZSET)) return;

    /* Report elements one after the other, using a null bulk reply for
     * missing elements. */
    addReplyArrayLen(c,c->argc-2);
    for (j = 2; j < c->argc; j++) {
        double score;
        /* 获取当前遍历到的地点的 score */
        if (!zobj || zsetScore(zobj, c->argv[j]->ptr, &score) == C_ERR) {
            addReplyNullArray(c);
        } else {
            /* Decode... */
            /* 将 score 解码成经纬度 */
            double xy[2];
            if (!decodeGeohash(score,xy)) {
                addReplyNullArray(c);
                continue;
            }
            addReplyArrayLen(c,2);
            addReplyHumanLongDouble(c,xy[0]);
            addReplyHumanLongDouble(c,xy[1]);
        }
    }
}

/* GEODIST key ele1 ele2 [unit]
 *
 * Return the distance, in meters by default, otherwise according to "unit",
 * between points ele1 and ele2. If one or more elements are missing NULL
 * is returned. */
/* 获取两个地点之间的距离 */
void geodistCommand(client *c) {
    double to_meter = 1;

    /* Check if there is the unit to extract, otherwise assume meters. */
    if (c->argc == 5) {
        to_meter = extractUnitOrReply(c,c->argv[4]);
        if (to_meter < 0) return;
    } else if (c->argc > 5) {
        addReplyErrorObject(c,shared.syntaxerr);
        return;
    }

    /* Look up the requested zset */
    /* 获取对应的 zset */
    robj *zobj = NULL;
    if ((zobj = lookupKeyReadOrReply(c, c->argv[1], shared.null[c->resp]))
        == NULL || checkType(c, zobj, OBJ_ZSET)) return;

    /* Get the scores. We need both otherwise NULL is returned. */
    double score1, score2, xyxy[4];
    /* 获取两个地点的分数 */
    if (zsetScore(zobj, c->argv[2]->ptr, &score1) == C_ERR ||
        zsetScore(zobj, c->argv[3]->ptr, &score2) == C_ERR)
    {
        addReplyNull(c);
        return;
    }

    /* Decode & compute the distance. */
    /* 对两个地点的分数进行解码获取经纬度 */
    if (!decodeGeohash(score1,xyxy) || !decodeGeohash(score2,xyxy+2))
        addReplyNull(c);
    else
        addReplyDoubleDistance(c,
            /* 通过两点的经纬度计算球面距离 */
            geohashGetDistance(xyxy[0],xyxy[1],xyxy[2],xyxy[3]) / to_meter);
}
