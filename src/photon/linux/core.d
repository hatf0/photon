module photon.linux.core;
private:

import std.stdio;
import std.string;
import std.exception;
import std.conv;
import std.array;
import core.thread;
import core.internal.spinlock;
import core.sys.posix.sys.types;
import core.sys.posix.sys.socket;
import core.sys.posix.poll;
import core.sys.posix.netinet.in_;
import core.sys.posix.unistd;
import core.sys.linux.epoll;
import core.sys.linux.timerfd;
import core.stdc.errno;
import core.atomic;
import core.sys.posix.stdlib: abort;
import core.sys.posix.fcntl;
import core.memory;
import core.sys.posix.sys.mman;
import core.sys.posix.pthread;
import core.sys.linux.sys.signalfd;

import photon.linux.support;
import photon.linux.syscalls;
import photon.ds.common;
import photon.ds.intrusive_queue;


shared struct RawEvent {
nothrow:
    this(int init) {
        fd = eventfd(init, 0);
    }

    void waitAndReset() {
        byte[8] bytes = void;
        ssize_t r;
        do {
            r = raw_read(fd, bytes.ptr, 8);
        } while(r < 0 && errno == EINTR);
        r.checked("event reset");
    }
    
    void trigger() { 
        union U {
            ulong cnt;
            ubyte[8] bytes;
        }
        U value;
        value.cnt = 1;
        ssize_t r;
        do {
            r = raw_write(fd, value.bytes.ptr, 8);
        } while(r < 0 && errno == EINTR);
        r.checked("event trigger");
    }
    
    int fd;
}

struct Timer {
nothrow:
    private int timerfd;


    int fd() {
        return timerfd;
    }

    static void ms2ts(timespec *ts, ulong ms)
    {
        ts.tv_sec = ms / 1000;
        ts.tv_nsec = (ms % 1000) * 1000000;
    }

    void arm(int timeout) {
        timespec ts_timeout;
        ms2ts(&ts_timeout, timeout); //convert miliseconds to timespec
        itimerspec its;
        its.it_value = ts_timeout;
        its.it_interval.tv_sec = 0;
        its.it_interval.tv_nsec = 0;
        timerfd_settime(timerfd, 0, &its, null);
    }

    void disam() {
        itimerspec its; // zeros
        timerfd_settime(timerfd, 0, &its, null);
    }

    void dispose() { 
        close(timerfd).checked;
    }
}

Timer timer() nothrow {
    int timerfd = timerfd_create(CLOCK_MONOTONIC, TFD_NONBLOCK | TFD_CLOEXEC).checked;
    interceptFd!(Fcntl.noop)(timerfd);
    return Timer(timerfd);
}

struct AwaitingFiber {
    FiberExt fiber;
    AwaitingFiber* next;
nothrow:
    void scheduleAll(int wakeFd) nothrow
    {
        auto w = &this;
        do {
            fiber.wakeFd = wakeFd;
            fiber.schedule();
            w = w.next;
        } while(w);
    }
}

class FiberExt : Fiber { 
    FiberExt next;
    uint numScheduler;
    int wakeFd; // recieves fd that woken us up
    bool scheduled;
    enum PAGESIZE = 4096;

nothrow:
    this(void function() fn, uint numSched) nothrow {
        super(fn);
        numScheduler = numSched;
    }

    this(void delegate() dg, uint numSched) nothrow {
        super(dg);
        numScheduler = numSched;
    }

    void schedule() nothrow
    {
        if(!scheduled) {
            scheds[numScheduler].queue.push(this);
            scheduled = true;
        }
    }
}

FiberExt currentFiber; //
shared RawEvent termination; // termination event, triggered once last fiber exits
shared int alive; // count of non-terminated Fibers scheduled

struct SchedulerBlock {
    shared IntrusiveQueue!(FiberExt, RawEvent) queue;
    shared uint assigned;
    size_t[2] padding;
}

static assert(SchedulerBlock.sizeof == 64);

package(photon) shared SchedulerBlock[] scheds;

enum int MAX_EVENTS = 500;

package(photon) void schedulerEntry(size_t n)
{
    int tid = gettid();
    cpu_set_t mask;
    CPU_SET(n, &mask);
    sched_setaffinity(tid, mask.sizeof, &mask)
        .checked("sched_setaffinity");
    shared SchedulerBlock* sched = scheds.ptr + n;
    event_loop_fd = cast(int)epoll_create1(0)
        .checked("failed to create event-loop");
    // register termination event
    epoll_event event;
    event.events = EPOLLIN;
    event.data.fd = termination.fd;
    epoll_ctl(event_loop_fd, EPOLL_CTL_ADD, event.data.fd, &event)
        .checked("registering termination event");
    // register queue event
    event.events = EPOLLIN;
    event.data.fd = sched.queue.event.fd;
    logf("Scheduler event fd=%d", sched.queue.event.fd);
    epoll_ctl(event_loop_fd, EPOLL_CTL_ADD, event.data.fd, &event)
        .checked("registering event queue");
    
    ssize_t fdMax = sysconf(_SC_OPEN_MAX).checked;
    descriptors = (cast(Descriptor*) mmap(null, fdMax * Descriptor.sizeof, 
        PROT_READ | PROT_WRITE, MAP_ANON | MAP_PRIVATE, -1, 0)
    )[0..fdMax];

    while (alive > 0) {
        for(;;) {
            FiberExt f = sched.queue.drain();
            if (f is null) break; // drained an empty queue, time to poll events
            do {
                auto next = f.next; // save next, it will be reused on scheduling
                currentFiber = f;
                logf("Fiber %x started", cast(void*)f);
                try {
                    f.scheduled = false;
                    f.call();
                }
                catch (Exception e) {
                    stderr.writeln(e);
                    atomicOp!"-="(alive, 1);
                }
                if (f.state == FiberExt.State.TERM) {
                    logf("Fiber %s terminated", cast(void*)f);
                    atomicOp!"-="(alive, 1);
                }
                f = next;
            } while(f !is null);
        }
        // exit if termination was triggered by other scheduler
        if (processEvents(sched, event_loop_fd) < 0) return;
    }
    termination.trigger();
}

public void spawn(void delegate() func) {
    import std.random;
    uint choice;
    if (scheds.length == 1) choice = 0;
    else {
        uint a = uniform!"[)"(0, cast(uint)scheds.length);
        uint b = uniform!"[)"(0, cast(uint)scheds.length-1);
        if (a == b) b = cast(uint)scheds.length-1;
        uint loadA = scheds[a].assigned;
        uint loadB = scheds[b].assigned;
        if (loadA < loadB) choice = a;
        else choice = b;
    }
    atomicOp!"+="(scheds[choice].assigned, 1);
    atomicOp!"+="(alive, 1);
    auto f = new FiberExt(func, choice);
    logf("Assigned %x -> %d scheduler", cast(void*)f, choice);
    f.schedule();
}

Descriptor[] descriptors;
int event_loop_fd;

enum FdSideState: uint {
    NOT_READY = 0,
    UNCERTAIN = 1,
    READY = 2
}

struct FdSide {
    FdSideState state;
    AwaitingFiber* awaits;
nothrow:
    void enqueue(AwaitingFiber* await) {
        await.next = awaits;
        awaits = await;
    }

    AwaitingFiber* remove(AwaitingFiber* fiber) {
        return removeFromList(awaits, fiber);
    }

    void scheduleAll(int fd) {
        if (awaits) {
            awaits.scheduleAll(fd);
            awaits = null;
        }
    }
}

enum FdState {
    UNINITIALIZED = 0,
    NONBLOCKING = 1,
    THREADPOOL = 2
}

// stateand 2 lists of awaiting fibers
struct Descriptor {
    FdState state;
    FdSide reader;
    FdSide writer;
}

public void startloop()
{
    termination = RawEvent(0);
    import core.cpuid;
    uint threads = threadsPerCPU;
    scheds = new SchedulerBlock[threads];
    foreach(ref sched; scheds) {
        sched.queue = IntrusiveQueue!(FiberExt, RawEvent)(RawEvent(0));
    }
}

int processEvents(shared SchedulerBlock* sched, int event_loop_fd)
{
    epoll_event[MAX_EVENTS] events = void;
    int r;
    do {
        r = epoll_wait(event_loop_fd, events.ptr, MAX_EVENTS, -1);
    } while (r < 0 && errno == EINTR);
    checked(r);
    for (int n = 0; n < r; n++) {
        int fd = events[n].data.fd;
        if (fd == termination.fd) return -1;
        if (fd == sched.queue.event.fd)  // triggered on insert into empty run-queue
            sched.queue.event.waitAndReset();
        auto descriptor = descriptors.ptr + fd;
        // if (descriptor.state == FdState.NONBLOCKING)
        if (events[n].events & EPOLLIN) {
            logf("Read event for fd=%d", fd);
            logf("read state = %d", descriptor.reader.state);
            descriptor.reader.state = FdSideState.READY;
            descriptor.reader.scheduleAll(fd);
        }
        if (events[n].events & EPOLLOUT) {
            logf("Write event for fd=%d", fd);
            logf("write state = %d", descriptor.writer.state);
            descriptor.writer.state = FdSideState.READY;
            descriptor.writer.scheduleAll(fd);                      
        }
    }
    return r;
}


enum Fcntl: int { explicit = 0, msg = MSG_DONTWAIT, sock = SOCK_NONBLOCK, noop = 0xFFFFF }
enum SyscallKind { accept, read, write, connect }

// intercept - a filter for file descriptor, changes flags and register on first use
int interceptFd(Fcntl needsFcntl)(int fd) nothrow {
    logf("Hit interceptFD");
    if (fd < 0 || fd >= descriptors.length) return -1;
    Descriptor* descr = descriptors.ptr + fd;
    if(descr.state == FdState.UNINITIALIZED) {
        logf("First use, registering fd = %s", fd);
        static if(needsFcntl == Fcntl.explicit) {
            int flags = fcntl(fd, F_GETFL, 0);
            fcntl(fd, F_SETFL, flags | O_NONBLOCK).checked;
            logf("Setting FCNTL. %x", cast(void*)currentFiber);
        }
        epoll_event event;
        event.events = EPOLLIN | EPOLLOUT | EPOLLET;
        event.data.fd = fd;
        if (epoll_ctl(event_loop_fd, EPOLL_CTL_ADD, fd, &event) < 0 && errno == EPERM) {
            logf("isSocket = false FD = %s", fd);
            descriptors[fd].state = FdState.THREADPOOL;
        }
        else {
            logf("isSocket = true FD = %s", fd);
            descriptors[fd].state = FdState.NONBLOCKING;
        }
    }
    return 0;
}

void deregisterFd(int fd) nothrow {
    if(fd >= 0 && fd < descriptors.length) {
        auto descriptor = descriptors.ptr + fd;
        descriptor.state = FdState.UNINITIALIZED;
        descriptor.writer.scheduleAll(fd);
        descriptor.writer.state = FdSideState.NOT_READY;
        descriptor.reader.scheduleAll(fd);
        descriptor.reader.state = FdSideState.NOT_READY;
    }
}

ssize_t universalSyscall(size_t ident, string name, SyscallKind kind, Fcntl fcntlStyle, ssize_t ERR, T...)
                        (int fd, T args) {
    if (currentFiber is null) {
        logf("%s PASSTHROUGH FD=%s", name, fd);
        return syscall(ident, fd, args).withErrorno;
    }
    else {
        logf("HOOKED %s FD=%d", name, fd);
        if (interceptFd!(fcntlStyle)(fd) < 0) return withErrorno(-EBADF);
        Descriptor* descriptor = descriptors.ptr + fd;
        if (descriptor.state == FdState.THREADPOOL) {
            logf("%s syscall THREADPOLL FD=%d", name, fd);
            //TODO: offload syscall to thread-pool
            return syscall(ident, fd, args).withErrorno;
        }
        AwaitingFiber await = AwaitingFiber(currentFiber, null);
        // set flags argument if able to avoid direct fcntl calls
        static if (fcntlStyle != Fcntl.explicit)
        {
            args[2] |= fcntlStyle;
        }
        logf("kind:s args:%s", kind, args);
        static if(kind == SyscallKind.accept || kind == SyscallKind.read) {
            for (;;) {
                auto state = descriptor.reader.state;
                logf("%s syscall state is %d. Fiber %x", name, state, cast(void*)currentFiber);
                if (state == FdSideState.NOT_READY) {
                    descriptor.reader.enqueue(&await);
                    FiberExt.yield();
                }
                else {
                    ssize_t resp = syscall(ident, fd, args);
                    static if (kind == SyscallKind.accept) {
                        // for accept we never know if we emptied the queue
                        descriptor.reader.state = FdSideState.UNCERTAIN;
                    }
                    else static if (kind == SyscallKind.read) {
                        if (resp == args[1]) // length is 2nd in (buf, length, ...)
                            descriptor.reader.state = FdSideState.UNCERTAIN;
                        else if (resp >= 0)
                            descriptor.reader.state = FdSideState.NOT_READY;
                    }
                    else static assert(false);
                    if (resp == -EAGAIN || resp == -ERR) {
                        descriptor.reader.state = FdSideState.NOT_READY;
                    }
                    else return withErrorno(resp);
                }
            }
        }
        else static if(kind == SyscallKind.write || kind == SyscallKind.connect) {
            for (;;) {
                if (descriptor.writer.state == FdSideState.NOT_READY) {
                    descriptor.writer.enqueue(&await);
                    FiberExt.yield();
                }
                else {
                    ssize_t resp = syscall(ident, fd, args);
                    if (resp == args[1]) // length is 2nd (buf, length, ...)
                        descriptor.writer.state = FdSideState.UNCERTAIN;
                    else if (resp >= 0)
                        descriptor.writer.state = FdSideState.NOT_READY;
                    else if(resp == -EAGAIN || resp == -ERR) {
                        descriptor.writer.state = FdSideState.NOT_READY;
                    }
                    else return withErrorno(resp);
                }
            }
        }
        assert(0);
    }
}

// ======================================================================================
// SYSCALL wrapper intercepts to override libc surface
// ======================================================================================

extern(C) ssize_t read(int fd, void *buf, size_t count) nothrow
{
    return universalSyscall!(SYS_READ, "READ", SyscallKind.read, Fcntl.explicit, EWOULDBLOCK)
        (fd, cast(size_t)buf, count);
}

extern(C) ssize_t write(int fd, const void *buf, size_t count)
{
    return universalSyscall!(SYS_WRITE, "WRITE", SyscallKind.write, Fcntl.explicit, EWOULDBLOCK)
        (fd, cast(size_t)buf, count);
}

extern(C) ssize_t accept(int sockfd, sockaddr *addr, socklen_t *addrlen)
{
    return universalSyscall!(SYS_ACCEPT, "accept", SyscallKind.accept, Fcntl.explicit, EWOULDBLOCK)
        (sockfd, cast(size_t) addr, cast(size_t) addrlen);    
}

extern(C) ssize_t accept4(int sockfd, sockaddr *addr, socklen_t *addrlen, int flags)
{
    return universalSyscall!(SYS_ACCEPT4, "accept4", SyscallKind.accept, Fcntl.sock, EWOULDBLOCK)
        (sockfd, cast(size_t) addr, cast(size_t) addrlen, flags);
}

extern(C) ssize_t connect(int sockfd, const sockaddr *addr, socklen_t *addrlen)
{
    return universalSyscall!(SYS_CONNECT, "connect", SyscallKind.connect, Fcntl.explicit, EINPROGRESS)
        (sockfd, cast(size_t) addr, cast(size_t) addrlen);
}

extern(C) ssize_t sendto(int sockfd, const void *buf, size_t len, int flags,
                      const sockaddr *dest_addr, socklen_t addrlen)
{
    return universalSyscall!(SYS_SENDTO, "sendto", SyscallKind.write, Fcntl.explicit, EWOULDBLOCK)
        (sockfd, cast(size_t) buf, len, flags, cast(size_t) dest_addr, cast(size_t) addrlen);
}

extern(C) size_t recv(int sockfd, void *buf, size_t len, int flags) nothrow {
    sockaddr_in src_addr;
    src_addr.sin_family = AF_INET;
    src_addr.sin_port = 0;
    src_addr.sin_addr.s_addr = htonl(INADDR_ANY);
    ssize_t addrlen = sockaddr_in.sizeof;
    return recvfrom(sockfd, buf, len, flags, cast(sockaddr*)&src_addr, &addrlen);
}

extern(C) private ssize_t recvfrom(int sockfd, void *buf, size_t len, int flags,
                        sockaddr *src_addr, ssize_t* addrlen) nothrow
{
    return universalSyscall!(SYS_RECVFROM, "RECVFROM", SyscallKind.read, Fcntl.msg, EWOULDBLOCK)
        (sockfd, cast(size_t)buf, len, flags, cast(size_t)src_addr, cast(size_t)addrlen);
}

/*extern(C) private ssize_t poll(pollfd *fds, nfds_t nfds, int timeout)
{
    bool nonBlockingCheck(ref ssize_t result) {
        bool uncertain;
    L_cacheloop:
        foreach (ref fd; fds[0..nfds]) {
            interceptFd!(Fcntl.explicit)(fd.fd);
            fd.revents = 0;
            auto descriptor = descriptors.ptr + fd.fd;
            if (fd.events & POLLIN) {
                auto state = descriptor.readerState;
                logf("Found event %d for reader in select", state);
                switch(state) with(ReaderState) {
                case READY:
                    fd.revents |=  POLLIN;
                    break;
                case EMPTY:
                    break;
                default:
                    uncertain = true;
                    break L_cacheloop;
                }
            }
            if (fd.events & POLLOUT) {
                auto state = descriptor.writerState;
                logf("Found event %d for writer in select", state);
                switch(state) with(WriterState) {
                case READY:
                    fd.revents |= POLLOUT;
                    break;
                case FULL:
                    break;
                default:
                    uncertain = true;
                    break L_cacheloop;
                }
            }
        }
        // fallback to system poll call if descriptor state is uncertain
        if (uncertain) {
            logf("Fallback to system poll, descriptors have uncertain state");
            ssize_t p = raw_poll(fds, nfds, 0);
            if (p != 0) {
                result = p;
                logf("Raw poll returns %d", result);
                return true;
            }
        }
        else {
            ssize_t j = 0;
            foreach (i; 0..nfds) {
                if (fds[i].revents) {
                    fds[j++] = fds[i];
                }
            }
            logf("Using our own event cache: %d events", j);
            if (j > 0) {
                result = cast(ssize_t)j;
                return true;
            }
        }
        return false;
    }
    if (currentFiber is null) {
        logf("POLL PASSTHROUGH!");
        return raw_poll(fds, nfds, timeout);
    }
    else {
        logf("HOOKED POLL %d fds timeout %d", nfds, timeout);
        if (nfds < 0) return -EINVAL.withErrorno;
        if (nfds == 0) {
            if (timeout == 0) return 0;
            shared AwaitingFiber aw = shared(AwaitingFiber)(cast(shared)currentFiber);
            Timer tm = timer();
            descriptors[tm.fd].enqueueReader(&aw);
            scope(exit) tm.dispose();
            tm.arm(timeout);
            logf("Timer fd=%d", tm.fd);
            Fiber.yield();
            logf("Woke up after select %x. WakeFd=%d", cast(void*)currentFiber, currentFiber.wakeFd);
            return 0;
        }
        foreach(ref fd; fds[0..nfds]) {
            if (fd.fd < 0 || fd.fd >= descriptors.length) return -EBADF.withErrorno;
            fd.revents = 0;
        }
        ssize_t result = 0;
        if (timeout <= 0) return raw_poll(fds, nfds, timeout);
        if (nonBlockingCheck(result)) return result;
        shared AwaitingFiber aw = shared(AwaitingFiber)(cast(shared)currentFiber);
        foreach (i; 0..nfds) {
            if (fds[i].events & POLLIN)
                descriptors[fds[i].fd].enqueueReader(&aw);
            else if(fds[i].events & POLLOUT)
                descriptors[fds[i].fd].enqueueWriter(&aw);
        }
        Timer tm = timer();
        scope(exit) tm.dispose();
        tm.arm(timeout);
        descriptors[tm.fd].enqueueReader(&aw);
        Fiber.yield();
        tm.disam();
        atomicStore(descriptors[tm.fd]._readerWaits, cast(shared(AwaitingFiber)*)null);
        foreach (i; 0..nfds) {
            if (fds[i].events & POLLIN)
                descriptors[fds[i].fd].removeReader(&aw);
            else if(fds[i].events & POLLOUT)
                descriptors[fds[i].fd].removeWriter(&aw);
        }
        logf("Woke up after select %x. WakeFD=%d", cast(void*)currentFiber, currentFiber.wakeFd);
        if (currentFiber.wakeFd == tm.fd) return 0;
        else {
            nonBlockingCheck(result);
            return result;
        }
    }
}*/

extern(C) private ssize_t close(int fd) nothrow
{
    logf("HOOKED CLOSE FD=%d", fd);
    deregisterFd(fd);
    return cast(int)withErrorno(syscall(SYS_CLOSE, fd));
}
