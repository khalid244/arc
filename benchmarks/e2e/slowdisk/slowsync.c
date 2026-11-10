/*
 * slowsync: make fsync-style durability calls as slow as an HDD.
 *
 * Production ingest pods keep their WAL and stager directories on node-local
 * emptyDirs on spinning disks, where an fsync costs ~10-20 ms and they queue
 * behind each other; a workstation NVMe answers in a few ms. Arc's flush,
 * stage and WAL checkpoint all fsync, so on a laptop the schema-churn window
 * is far narrower than in production.
 *
 * Loaded with DYLD_INSERT_LIBRARIES into the ingest processes only (S12). Every
 * fsync() and fcntl(F_FULLFSYNC / F_BARRIERFSYNC) in the process runs, then
 * holds one process-wide lock for SLOWSYNC_MS milliseconds: one spindle, syncs
 * served one at a time. Nothing else is changed. SLOWSYNC_MS unset or 0 = off.
 *
 * Build: clang -dynamiclib -O2 -o slowsync.dylib slowsync.c
 */
#include <fcntl.h>
#include <pthread.h>
#include <stdarg.h>
#include <stdlib.h>
#include <unistd.h>

#define DYLD_INTERPOSE(_replacement, _replacee)                                   \
    __attribute__((used)) static struct {                                         \
        const void *replacement;                                                  \
        const void *replacee;                                                     \
    } _interpose_##_replacee __attribute__((section("__DATA,__interpose"))) = {   \
        (const void *)(unsigned long)&_replacement, (const void *)(unsigned long)&_replacee};

static pthread_mutex_t spindle = PTHREAD_MUTEX_INITIALIZER;
static long delay_us = -1;

static void slow(void) {
    if (delay_us < 0) {
        const char *s = getenv("SLOWSYNC_MS");
        delay_us = s ? atol(s) * 1000 : 0;
    }
    if (delay_us <= 0)
        return;
    pthread_mutex_lock(&spindle);
    usleep((useconds_t)delay_us);
    pthread_mutex_unlock(&spindle);
}

static int slow_fsync(int fd) {
    int r = fsync(fd);
    slow();
    return r;
}

static int slow_fcntl(int fd, int cmd, ...) {
    va_list ap;
    va_start(ap, cmd);
    void *arg = va_arg(ap, void *);
    va_end(ap);
    int r = fcntl(fd, cmd, arg);
    if (cmd == F_FULLFSYNC || cmd == F_BARRIERFSYNC)
        slow();
    return r;
}

DYLD_INTERPOSE(slow_fsync, fsync)
DYLD_INTERPOSE(slow_fcntl, fcntl)
