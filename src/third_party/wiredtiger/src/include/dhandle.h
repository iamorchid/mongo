/*-
 * Copyright (c) 2014-2020 MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

/*
 * Helpers for calling a function with a data handle in session->dhandle then restoring afterwards.
 */
#define WT_WITH_DHANDLE(s, d, e)                        \
    do {                                                \
        WT_DATA_HANDLE *__saved_dhandle = (s)->dhandle; \
        (s)->dhandle = (d);                             \
        e;                                              \
        (s)->dhandle = __saved_dhandle;                 \
    } while (0)

#define WT_WITH_BTREE(s, b, e) WT_WITH_DHANDLE(s, (b)->dhandle, e)

/* Call a function without the caller's data handle, restore afterwards. */
#define WT_WITHOUT_DHANDLE(s, e) WT_WITH_DHANDLE(s, NULL, e)

/*
 * Call a function with the caller's data handle, restore it afterwards in case it is overwritten.
 */
#define WT_SAVE_DHANDLE(s, e) WT_WITH_DHANDLE(s, (s)->dhandle, e)

/* Check if a handle is inactive. */
#define WT_DHANDLE_INACTIVE(dhandle) \
    (F_ISSET(dhandle, WT_DHANDLE_DEAD) || !F_ISSET(dhandle, WT_DHANDLE_EXCLUSIVE | WT_DHANDLE_OPEN))

/* Check if a handle could be reopened. */
#define WT_DHANDLE_CAN_REOPEN(dhandle)                                     \
    (!WT_DHANDLE_INACTIVE(dhandle) && F_ISSET(dhandle, WT_DHANDLE_OPEN) && \
      !F_ISSET(dhandle, WT_DHANDLE_DROPPED))

/* The metadata cursor's data handle. */
#define WT_SESSION_META_DHANDLE(s) (((WT_CURSOR_BTREE *)((s)->meta_cursor))->dhandle)

#define WT_DHANDLE_ACQUIRE(dhandle) (void)__wt_atomic_add32(&(dhandle)->session_ref, 1)

#define WT_DHANDLE_RELEASE(dhandle) (void)__wt_atomic_sub32(&(dhandle)->session_ref, 1)

#define WT_DHANDLE_NEXT(session, dhandle, head, field)                       \
    do {                                                                     \
        WT_ASSERT(session, F_ISSET(session, WT_SESSION_LOCKED_HANDLE_LIST)); \
        if ((dhandle) == NULL)                                               \
            (dhandle) = TAILQ_FIRST(head);                                   \
        else {                                                               \
            WT_DHANDLE_RELEASE(dhandle);                                     \
            (dhandle) = TAILQ_NEXT(dhandle, field);                          \
        }                                                                    \
        if ((dhandle) != NULL)                                               \
            WT_DHANDLE_ACQUIRE(dhandle);                                     \
    } while (0)

/*
 * WT_DATA_HANDLE --
 *	A handle for a generic named data source.
 */
struct __wt_data_handle {
    WT_RWLOCK rwlock; /* Lock for shared/exclusive ops */
    TAILQ_ENTRY(__wt_data_handle) q;
    TAILQ_ENTRY(__wt_data_handle) hashq;

    const char *name;       /* Object name as a URI */
    uint64_t name_hash;     /* Hash of name */
    const char *checkpoint; /* Checkpoint name (or NULL) */
    const char **cfg;       /* Configuration information */
    const char *meta_base;  /* Base metadata configuration */

    /*
     * Sessions holding a connection's data handle will have a non-zero reference count; sessions
     * using a connection's data handle will have a non-zero in-use count. Instances of cached
     * cursors referencing the data handle appear in session_cache_ref.
     */
    uint32_t session_ref;          /* Sessions referencing this handle */
    int32_t session_inuse;         /* Sessions using this handle */
    uint32_t excl_ref;             /* Refs of handle by excl_session */
    uint64_t timeofdeath;          /* Use count went to 0 */
    WT_SESSION_IMPL *excl_session; /* Session with exclusive use, if any */

    WT_DATA_SOURCE *dsrc; /* Data source for this handle */
    void *handle;         /* Generic handle */

    enum { WT_DHANDLE_TYPE_BTREE, WT_DHANDLE_TYPE_TABLE } type;

    bool compact_skip; /* If the handle failed to compact */

    /*
     * Data handles can be closed without holding the schema lock; threads walk the list of open
     * handles, operating on them (checkpoint is the best example). To avoid sources disappearing
     * underneath checkpoint, lock the data handle when closing it.
     */
    WT_SPINLOCK close_lock; /* Lock to close the handle */

    /* Data-source statistics */
    WT_DSRC_STATS *stats[WT_COUNTER_SLOTS];
    WT_DSRC_STATS *stat_array;

/* Flags values over 0xff are reserved for WT_BTREE_* */
/* AUTOMATIC FLAG VALUE GENERATION START */
#define WT_DHANDLE_DEAD 0x01u         /* Dead, awaiting discard */
#define WT_DHANDLE_DISCARD 0x02u      /* Close on release */
#define WT_DHANDLE_DISCARD_KILL 0x04u /* Mark dead on release */
#define WT_DHANDLE_DROPPED 0x08u      /* Handle is dropped */

// [bookmark] WT_DHANDLE_EXCLUSIVE
// 独占访问handle，即获取handle的写锁。
#define WT_DHANDLE_EXCLUSIVE 0x10u    /* Exclusive access */

#define WT_DHANDLE_IS_METADATA 0x20u  /* Metadata handle */

// [bookmark] WT_DHANDLE_LOCK_ONLY
// 到底作为何种锁使用？读锁还是写锁？
// 通过__wt_session_lock_dhandle的分析可以知道，使用WT_DHANDLE_LOCK_ONLY作为flags，既有可能获取
// handle的读锁，也有可能获取handle的写锁，也可能返回EBUSY（取决于当前handle是否open已经其他线程已
// 持有的handle锁）。但如果使用WT_DHANDLE_LOCK_ONLY|WT_DHANDLE_EXCLUSIVE，则获取的锁一定是handle
// 的写锁，或者返回EBUSY。
// 代码中WT_DHANDLE_LOCK_ONLY总是和WT_DHANDLE_EXCLUSIVE一起使用来调用__wt_session_get_dhandle，
// WT_DHANDLE_LOCK_ONLY主要是用来说明不需要open handle（当然，handle被其他线程已经open也没有关系）。
#define WT_DHANDLE_LOCK_ONLY 0x40u    /* Handle only used as a lock */

#define WT_DHANDLE_OPEN 0x80u         /* Handle is open */
                                      /* AUTOMATIC FLAG VALUE GENERATION STOP */
    uint32_t flags;
};
