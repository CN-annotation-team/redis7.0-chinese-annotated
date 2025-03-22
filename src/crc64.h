#ifndef CRC64_H
#define CRC64_H

#include <stdint.h>

/* CRC64 表初始化函数 */
/* 应在程序初始化时调用（一次即可） */
void crc64_init(void);
/* CRC64 计算函数 */
uint64_t crc64(uint64_t crc, const unsigned char *s, uint64_t l);

#ifdef REDIS_TEST
int crc64Test(int argc, char *argv[], int flags);
#endif

#endif
