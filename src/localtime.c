/*
 * Copyright (c) 2018, Salvatore Sanfilippo <antirez at gmail dot com>
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

#include <time.h>

/* This is a safe version of localtime() which contains no locks and is
 * fork() friendly. Even the _r version of localtime() cannot be used safely
 * in Redis. Another thread may be calling localtime() while the main thread
 * forks(). Later when the child process calls localtime() again, for instance
 * in order to log something to the Redis log, it may deadlock: in the copy
 * of the address space of the forked process the lock will never be released.
 *
 * This function takes the timezone 'tz' as argument, and the 'dst' flag is
 * used to check if daylight saving time is currently in effect. The caller
 * of this function should obtain such information calling tzset() ASAP in the
 * main() function to obtain the timezone offset from the 'timezone' global
 * variable. To obtain the daylight information, if it is currently active or not,
 * one trick is to call localtime() in main() ASAP as well, and get the
 * information from the tm_isdst field of the tm structure. However the daylight
 * time may switch in the future for long running processes, so this information
 * should be refreshed at safe times.
 *
 * Note that this function does not work for dates < 1/1/1970, it is solely
 * designed to work with what time(NULL) may return, and to support Redis
 * logging of the dates, it's not really a complete implementation. */

/* 判断是否为闰年 */
static int is_leap_year(time_t year) {
    if (year % 4) return 0;         /* A year not divisible by 4 is not leap. */
    else if (year % 100) return 1;  /* If div by 4 and not 100 is surely leap. */
    else if (year % 400) return 0;  /* If div by 100 *and* not by 400 is not leap. */
    else return 1;                  /* If div by 100 and 400 is leap. */
}

/* 功能：将时间戳 t 转换为本地时间，存储到 tm 结构体 tmp
* 参数：
*   tmp   - 输出时间结构体
*   t     - 时间戳（自1970-01-01 UTC的秒数）
*   tz    - 时区偏移秒数（东区为正）
*   dst   - 夏令时标志（0=未生效，1=生效） 
* 补充说明：此函数是 Redis 5.0.0 引入的无锁本地时间转换工具，用于替代 localtime_r，​避免多线程或 fork 场景下的死锁风险
*/
void nolocks_localtime(struct tm *tmp, time_t t, time_t tz, int dst) {
    /* 定义时间单位常量（秒） */
    const time_t secs_min = 60;
    const time_t secs_hour = 3600;
    const time_t secs_day = 3600 * 24;

    /* 调整时区和夏令时 */
    t -= tz;                                        // 减去时区偏移（如东八区 -28800 秒）
    t += 3600 * dst;                                // 增加夏令时带来的1小时偏移（若生效）

    /* 计算自纪元以来的天数和剩余秒数 */
    time_t days = t / secs_day;                     // 总天数
    time_t seconds = t % secs_day;                  // 当天剩余秒数

    /* 填充基本时间字段 */
    tmp->tm_isdst = dst;                            // 夏令时标志
    tmp->tm_hour = seconds / secs_hour;             // 小时（0-23）
    tmp->tm_min = (seconds % secs_hour) / secs_min; // 分钟（0-59）
    tmp->tm_sec = (seconds % secs_hour) % secs_min; // 秒（0-59）

    /* 计算星期几（1970-01-01 是星期四，故 days+4 后模7） */
    tmp->tm_wday = (days + 4) % 7;                  // 0=周日，1=周一，... 6=周六

    /* 计算年份 */
    tmp->tm_year = 1970;                            // 起始年份
    while (1) {
        time_t days_this_year = 365 + is_leap_year(tmp->tm_year); // 闰年判断
        if (days_this_year > days) break;           // 剩余天数不足一年则退出
        days -= days_this_year;                     // 减去当前年份天数
        tmp->tm_year++;                             // 年份递增
    }
    tmp->tm_yday = days;                            // 一年中的第几天（0-365）

    /* 计算月份和日期 */
    int mdays[12] = {31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};
    mdays[1] += is_leap_year(tmp->tm_year);         // 闰年2月加1天

    tmp->tm_mon = 0;                                // 月份从0（1月）开始
    while (days >= mdays[tmp->tm_mon]) {
        days -= mdays[tmp->tm_mon];
        tmp->tm_mon++;                              // 递增月份直至剩余天数不足一个月
    }

    tmp->tm_mday = days + 1;                        // 日期从1开始（days为0-based）
    tmp->tm_year -= 1900;                           // tm_year存储年份-1900（兼容标准tm结构）
}

#ifdef LOCALTIME_TEST_MAIN
#include <stdio.h>

int main(void) {
    /* Obtain timezone and daylight info. */
    tzset(); /* Now 'timezone' global is populated. */
    time_t t = time(NULL);
    struct tm *aux = localtime(&t);
    int daylight_active = aux->tm_isdst;

    struct tm tm;
    char buf[1024];

    nolocks_localtime(&tm,t,timezone,daylight_active);
    strftime(buf,sizeof(buf),"%d %b %H:%M:%S",&tm);
    printf("[timezone: %d, dl: %d] %s\n", (int)timezone, (int)daylight_active, buf);
}
#endif
