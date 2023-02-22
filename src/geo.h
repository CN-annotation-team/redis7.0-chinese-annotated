#ifndef __GEO_H__
#define __GEO_H__

#include "server.h"

/* Structures used inside geo.c in order to represent points and array of
 * points on the earth. */
typedef struct geoPoint {
    double longitude;
    double latitude;
    double dist;
    double score;
    char *member;
} geoPoint;

typedef struct geoArray {
    struct geoPoint *array;
    /* 总桶的数量，即 array 的大小 */
    size_t buckets;
    /* 已使用的桶的数量 */
    size_t used;
} geoArray;

#endif
