#include "server.h"
#include "bio.h"
#include "atomicvar.h"
#include "functions.h"

/* lazy free 惰性释放对象空间功能，
 * 当前文件的函数可以分为三类
 * 1) lazyfreeXXX 执行实际释放对象空间的函数 (这些函数只有该文件内部使用，对外部不可见)
 * 2) lazyfreeGetXXX，lazyfreeResetXXX 获取和设置懒加载的一些信息和限制函数 （对外部可见）
 * 3) XXXAsync 提交惰性释放任务给后台线程，不执行实际删除任务，这里会把1)中的函数传递给 bio_job
 */
/* 待执行对象的数量 */
static redisAtomic size_t lazyfree_objects = 0;
/* 已经执行释放对象的数量 */
static redisAtomic size_t lazyfreed_objects = 0;

/* Release objects from the lazyfree thread. It's just decrRefCount()
 * updating the count of objects to release. */
/* 将对象的引用计数 -1 */
void lazyfreeFreeObject(void *args[]) {
    robj *o = (robj *) args[0];
    /* 这里方法的逻辑会对引用计数为 1 的 robj 对象进行空间释放 */
    decrRefCount(o);
    /* 待释放对象 -1 */
    atomicDecr(lazyfree_objects,1);
    /* 已经释放对象 +1 */
    atomicIncr(lazyfreed_objects,1);
}

/* Release a database from the lazyfree thread. The 'db' pointer is the
 * database which was substituted with a fresh one in the main thread
 * when the database was logically deleted. */
/* 释放数据库，逻辑和 lazyfreeFreeObject 类似，只是具体调用的释放方法不同 */
void lazyfreeFreeDatabase(void *args[]) {
    /* 获取具体对象，ht1 是数据库的字典，ht2 是数据库的过期数据字典 */
    dict *ht1 = (dict *) args[0];
    dict *ht2 = (dict *) args[1];

    /* 获取数据库字典大小 */
    size_t numkeys = dictSize(ht1);
    /* 释放两个字典的空间 */
    dictRelease(ht1);
    dictRelease(ht2);
    /* 修改两个计数器 */
    atomicDecr(lazyfree_objects,numkeys);
    atomicIncr(lazyfreed_objects,numkeys);
}

/* Release the key tracking table. */
/* 释放追踪表，rax 结构释放，逻辑同上 */
void lazyFreeTrackingTable(void *args[]) {
    rax *rt = args[0];
    size_t len = rt->numele;
    freeTrackingRadixTree(rt);
    atomicDecr(lazyfree_objects,len);
    atomicIncr(lazyfreed_objects,len);
}

/* Release the lua_scripts dict. */
/* 释放 LUA 脚本字典，逻辑同上 */
void lazyFreeLuaScripts(void *args[]) {
    dict *lua_scripts = args[0];
    long long len = dictSize(lua_scripts);
    dictRelease(lua_scripts);
    atomicDecr(lazyfree_objects,len);
    atomicIncr(lazyfreed_objects,len);
}

/* Release the functions ctx. */
/* 释放函数上下文，逻辑同上 */
void lazyFreeFunctionsCtx(void *args[]) {
    functionsLibCtx *functions_lib_ctx = args[0];
    size_t len = functionsLibCtxfunctionsLen(functions_lib_ctx);
    functionsLibCtxFree(functions_lib_ctx);
    atomicDecr(lazyfree_objects,len);
    atomicIncr(lazyfreed_objects,len);
}

/* Release replication backlog referencing memory. */
/* 释放复制缓冲区，逻辑同上 */
void lazyFreeReplicationBacklogRefMem(void *args[]) {
    /* 获取缓冲区块列表和块索引 */
    list *blocks = args[0];
    rax *index = args[1];
    /* 两者大小相加为总对象数 */
    long long len = listLength(blocks);
    len += raxSize(index);
    listRelease(blocks);
    raxFree(index);
    atomicDecr(lazyfree_objects,len);
    atomicIncr(lazyfreed_objects,len);
}

/* Return the number of currently pending objects to free. */
/* 获取待释放对象的数量 lazyfree_objects */
size_t lazyfreeGetPendingObjectsCount(void) {
    size_t aux;
    atomicGet(lazyfree_objects,aux);
    return aux;
}

/* Return the number of objects that have been freed. */
/* 获取已经释放的对象的数量，lazyfreed_objects */
size_t lazyfreeGetFreedObjectsCount(void) {
    size_t aux;
    atomicGet(lazyfreed_objects,aux);
    return aux;
}

/* 重置已释放对象的数量 */
void lazyfreeResetStats() {
    atomicSet(lazyfreed_objects,0);
}

/* Return the amount of work needed in order to free an object.
 * The return value is not always the actual number of allocations the
 * object is composed of, but a number proportional to it.
 *
 * For strings the function always returns 1.
 *
 * For aggregated objects represented by hash tables or other data structures
 * the function just returns the number of elements the object is composed of.
 *
 * Objects composed of single allocations are always reported as having a
 * single item even if they are actually logical composed of multiple
 * elements.
 *
 * For lists the function returns the number of elements in the quicklist
 * representing the list. */
/* 该函数是对象要释放的对象进行代价计算，如果代价大于 LAZYFREE_THRESHOLD 就需要后台线程异步释放
 * 字符串对象的代价为 -> 1
 * 对于集合对象代价为 -> 集合中元素的数量
 * 由多个对象组成的单个对象 -> 1 */
size_t lazyfreeGetFreeEffort(robj *key, robj *obj, int dbid) {
    if (obj->type == OBJ_LIST) {
        /* 对象的类型是 OBJ_LIST 返回该快速列表的长度 */
        quicklist *ql = obj->ptr;
        return ql->len;
    } else if (obj->type == OBJ_SET && obj->encoding == OBJ_ENCODING_HT) {
        /* 对象的类型是 OBJ_SET 且编码是 OBJ_ENCODING_HT，返回该字典的大小 */
        dict *ht = obj->ptr;
        return dictSize(ht);
    } else if (obj->type == OBJ_ZSET && obj->encoding == OBJ_ENCODING_SKIPLIST){
        /* 对象的类型是 OBJ_ZSET 且编码是 OBJ_ENCODING_SKIPLIST，返回该压缩表的长度 */
        zset *zs = obj->ptr;
        return zs->zsl->length;
    } else if (obj->type == OBJ_HASH && obj->encoding == OBJ_ENCODING_HT) {
        /* 对象的类型是 OBJ_HASH 且编码是 OBJ_ENCODING_HT，返回该字典的大小 */
        dict *ht = obj->ptr;
        return dictSize(ht);
    } else if (obj->type == OBJ_STREAM) {
        /* 对象的类型是 OBJ_STREAM */
        size_t effort = 0;
        stream *s = obj->ptr;

        /* Make a best effort estimate to maintain constant runtime. Every macro
         * node in the Stream is one allocation. */
        /* 对 stream 类型的 rax 的每个节点都为 1 个代价，多少个节点多少代价 */
        effort += s->rax->numnodes;

        /* Every consumer group is an allocation and so are the entries in its
         * PEL. We use size of the first group's PEL as an estimate for all
         * others. */
        /* stream 对象的所有消费者组的释放代价计算，这里直接计算
         * 第一个消费者组的释放代价 * 总消费者组的数量 ≈ 所有消费者组的释放代价 */
        if (s->cgroups && raxSize(s->cgroups)) {
            raxIterator ri;
            streamCG *cg;
            /* 获取 raxIterator 迭代器 */
            raxStart(&ri,s->cgroups);
            /* 移动到头部 */
            raxSeek(&ri,"^",NULL,0);
            /* There must be at least one group so the following should always
             * work. */
            serverAssert(raxNext(&ri));
            /* 获取当前迭代器指向节点的消费者组数据 */
            cg = ri.data;
            /* 消费者组数量（s->cgroups） * 释放消费者组的代价（1 + raxSize(cg-pel)）
             * 这个 +1 是释放 rax 结构体的代价为 1 */
            effort += raxSize(s->cgroups)*(1+raxSize(cg->pel));
            raxStop(&ri);
        }
        return effort;
    } else if (obj->type == OBJ_MODULE) {
        /* 对象类型是 OBJ_MODULE，module.c 实现了自己计算释放代价的逻辑。
         * 默认是 1，如果该函数返回 0，表示代价很高，需要异步释放 */
        size_t effort = moduleGetFreeEffort(key, obj, dbid);
        /* If the module's free_effort returns 0, we will use asynchronous free
         * memory by default. */
        return effort == 0 ? ULONG_MAX : effort;
    } else {
        /* 其他类型的 robj 都返回 1 */
        return 1; /* Everything else is a single allocation. */
    }
}

/* If there are enough allocations to free the value object asynchronously, it
 * may be put into a lazy free list instead of being freed synchronously. The
 * lazy free list will be reclaimed in a different bio.c thread. If the value is
 * composed of a few allocations, to free in a lazy way is actually just
 * slower... So under a certain limit we just free the object synchronously. */
/* 惰性释放空间的代价阈值，超过这个阈值就需要惰性释放，否则就主线程自己执行释放逻辑 */
#define LAZYFREE_THRESHOLD 64


/* Free an object, if the object is huge enough, free it in async way. */
/* 异步释放一个对象（只是向 bio 提交了一个释放任务） */
void freeObjAsync(robj *key, robj *obj, int dbid) {
    /* 计算释放该对象需要的代价 */
    size_t free_effort = lazyfreeGetFreeEffort(key,obj,dbid);
    /* Note that if the object is shared, to reclaim it now it is not
     * possible. This rarely happens, however sometimes the implementation
     * of parts of the Redis core may call incrRefCount() to protect
     * objects, and then call dbDelete(). */
    /* 如果代价超过阈值且 refcount 为 1，就执行释放逻辑，这里进行 refcount == 1 的判断可以减少任务数量 */
    if (free_effort > LAZYFREE_THRESHOLD && obj->refcount == 1) {
        /* 待删除对象数量 +1 */
        atomicIncr(lazyfree_objects,1);
        bioCreateLazyFreeJob(lazyfreeFreeObject,1,obj);
    } else {
        decrRefCount(obj);
    }
}

/* Empty a Redis DB asynchronously. What the function does actually is to
 * create a new empty set of hash tables and scheduling the old ones for
 * lazy freeing. */
/* 异步清空数据库，删除数据库的数据字典以及过期数据字典 */
void emptyDbAsync(redisDb *db) {
    /* 获取数据库字典和过期数据字典 */
    dict *oldht1 = db->dict, *oldht2 = db->expires;
    /* 重新初始化两个字典 */
    db->dict = dictCreate(&dbDictType);
    db->expires = dictCreate(&dbExpiresDictType);
    atomicIncr(lazyfree_objects,dictSize(oldht1));
    /* 对旧的两个字典进行异步释放空间 */
    bioCreateLazyFreeJob(lazyfreeFreeDatabase,2,oldht1,oldht2);
}

/* Free the key tracking table.
 * If the table is huge enough, free it in async way. */
/* 异步释放追踪字典书 */
void freeTrackingRadixTreeAsync(rax *tracking) {
    /* Because this rax has only keys and no values so we use numnodes. */
    /* 判断 rax 的节点数是否大于 LAZYFREE_THRESHOLD */
    if (tracking->numnodes > LAZYFREE_THRESHOLD) {
        atomicIncr(lazyfree_objects,tracking->numele);
        /* 异步释放 rax */
        bioCreateLazyFreeJob(lazyFreeTrackingTable,1,tracking);
    } else {
        /* 同步释放 rax */
        freeTrackingRadixTree(tracking);
    }
}

/* Free lua_scripts dict, if the dict is huge enough, free it in async way. */
/* 异步释放 lua 脚本，逻辑同上个方法 */
void freeLuaScriptsAsync(dict *lua_scripts) {
    if (dictSize(lua_scripts) > LAZYFREE_THRESHOLD) {
        atomicIncr(lazyfree_objects,dictSize(lua_scripts));
        bioCreateLazyFreeJob(lazyFreeLuaScripts,1,lua_scripts);
    } else {
        dictRelease(lua_scripts);
    }
}

/* Free functions ctx, if the functions ctx contains enough functions, free it in async way. */
/* 异步释放 functionsLibCtx，逻辑同上个方法 */
void freeFunctionsAsync(functionsLibCtx *functions_lib_ctx) {
    if (functionsLibCtxfunctionsLen(functions_lib_ctx) > LAZYFREE_THRESHOLD) {
        atomicIncr(lazyfree_objects,functionsLibCtxfunctionsLen(functions_lib_ctx));
        bioCreateLazyFreeJob(lazyFreeFunctionsCtx,1,functions_lib_ctx);
    } else {
        functionsLibCtxFree(functions_lib_ctx);
    }
}

/* Free replication backlog referencing buffer blocks and rax index. */
/* 异步释放复制缓冲区，主要缓冲区块的数量大于阈值，或者快速索引大于阈值，就异步释放，逻辑同上个方法
 * 由于复制缓冲区主要有 缓冲区块列表和块索引两个结构，所以需要释放两个数据结构 */
void freeReplicationBacklogRefMemAsync(list *blocks, rax *index) {
    /* 只要有一个数据结构的释放代价大于了阈值，就使用异步释放空间 */
    if (listLength(blocks) > LAZYFREE_THRESHOLD ||
        raxSize(index) > LAZYFREE_THRESHOLD)
    {
        atomicIncr(lazyfree_objects,listLength(blocks)+raxSize(index));
        bioCreateLazyFreeJob(lazyFreeReplicationBacklogRefMem,2,blocks,index);
    } else {
        listRelease(blocks);
        raxFree(index);
    }
}
