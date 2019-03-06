#include "util.h"

#include <fcntl.h>
#include <map>
#include <sched.h>
#include <sys/stat.h>
#include <sys/syscall.h>
#include <sys/types.h>
#include <unistd.h>

// #define BIND_CPU_IN_COMPETITION

namespace polar_race {
    static int LockOrUnlock(int fd, bool lock) {
        errno = 0;
        struct flock f;
        memset(&f, 0, sizeof(f));
        f.l_type = (lock ? F_WRLCK : F_UNLCK);
        f.l_whence = SEEK_SET;
        f.l_start = 0;
        f.l_len = 0;        // Lock/unlock entire file
        return fcntl(fd, F_SETLK, &f);
    }

    int LockFile(const std::string &fname, FileLock **lock) {
        *lock = NULL;
        int result = 0;
        int fd = open(fname.c_str(), O_RDWR | O_CREAT, 0644);

        if (fd < 0) {
            result = errno;
        } else if (LockOrUnlock(fd, true) == -1) {
            result = errno;
            close(fd);
        } else {
            FileLock *fileLock = new FileLock;
            fileLock->mFd = fd;
            fileLock->mName = fname;
            *lock = fileLock;
        }
        return result;
    }

    int UnlockFile(FileLock *lock) {
        int result = 0;
        if (LockOrUnlock(lock->mFd, false) == -1) {
            result = errno;
        }
        close(lock->mFd);
        delete lock;
        return result;
    }

    PerfPoint::PerfPoint(const std::string &name)
            : mName(name),
              mStart(GetCurrentTimeInUs()) {
    }

    PerfPoint::~PerfPoint() {
        uint64_t cost = GetCurrentTimeInUs() - mStart;
        printf("Perf point:%s cost:%lu us\n", mName.c_str(), cost);
    }
}  // namespace polar_race
