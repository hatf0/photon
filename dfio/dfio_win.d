module dfio_win;

version(Windows):

import core.sys.windows.core;
import core.sys.windows.winsock2;
import core.atomic;
import core.stdc.stdio;
import core.stdc.stdlib;
import core.thread;
import std.container.dlist;
import std.exception;
import std.windows.syserror;
import std.random;

//opaque structs
struct UMS_COMPLETION_LIST;
struct UMS_CONTEXT;
struct PROC_THREAD_ATTRIBUTE_LIST;

struct UMS_SCHEDULER_STARTUP_INFO {
    ULONG                      UmsVersion;
    UMS_COMPLETION_LIST*       CompletionList;
    UmsSchedulerProc           SchedulerProc;
    PVOID                      SchedulerParam;
}

struct UMS_CREATE_THREAD_ATTRIBUTES {
  DWORD UmsVersion;
  PVOID UmsContext;
  PVOID UmsCompletionList;
}

enum UMS_SCHEDULER_REASON: uint {
  UmsSchedulerStartup = 0,
  UmsSchedulerThreadBlocked = 1,
  UmsSchedulerThreadYield = 2
}

enum UMS_VERSION =  0x0100;
enum
    PROC_THREAD_ATTRIBUTE_NUMBER = 0x0000FFFF,
    PROC_THREAD_ATTRIBUTE_THREAD = 0x00010000,    // Attribute may be used with thread creation
    PROC_THREAD_ATTRIBUTE_INPUT = 0x00020000,     // Attribute is input only
    PROC_THREAD_ATTRIBUTE_ADDITIVE = 0x00040000;  // Attribute may be "accumulated," e.g. bitmasks, counters, etc.

enum
    ProcThreadAttributeParentProcess                = 0,
    ProcThreadAttributeHandleList                   = 2,
    ProcThreadAttributeGroupAffinity                = 3,
    ProcThreadAttributePreferredNode                = 4,
    ProcThreadAttributeIdealProcessor               = 5,
    ProcThreadAttributeUmsThread                    = 6,
    ProcThreadAttributeMitigationPolicy             = 7;

 enum UMS_THREAD_INFO_CLASS: uint { 
  UmsThreadInvalidInfoClass  = 0,
  UmsThreadUserContext       = 1,
  UmsThreadPriority          = 2,
  UmsThreadAffinity          = 3,
  UmsThreadTeb               = 4,
  UmsThreadIsSuspended       = 5,
  UmsThreadIsTerminated      = 6,
  UmsThreadMaxInfoClass      = 7
}

uint ProcThreadAttributeValue(uint Number, bool Thread, bool Input, bool Additive)
{
    return (Number & PROC_THREAD_ATTRIBUTE_NUMBER) | 
     (Thread != FALSE ? PROC_THREAD_ATTRIBUTE_THREAD : 0) | 
     (Input != FALSE ? PROC_THREAD_ATTRIBUTE_INPUT : 0) | 
     (Additive != FALSE ? PROC_THREAD_ATTRIBUTE_ADDITIVE : 0);
}

enum PROC_THREAD_ATTRIBUTE_UMS_THREAD = ProcThreadAttributeValue(ProcThreadAttributeUmsThread, true, true, false);

extern(Windows) BOOL EnterUmsSchedulingMode(UMS_SCHEDULER_STARTUP_INFO* SchedulerStartupInfo);
extern(Windows) BOOL UmsThreadYield(PVOID SchedulerParam);
extern(Windows) BOOL DequeueUmsCompletionListItems(UMS_COMPLETION_LIST* UmsCompletionList, DWORD WaitTimeOut, UMS_CONTEXT** UmsThreadList);
extern(Windows) UMS_CONTEXT* GetNextUmsListItem(UMS_CONTEXT* UmsContext);
extern(Windows) BOOL ExecuteUmsThread(UMS_CONTEXT* UmsThread);
extern(Windows) BOOL CreateUmsCompletionList(UMS_COMPLETION_LIST** UmsCompletionList);
extern(Windows) BOOL CreateUmsThreadContext(UMS_CONTEXT** lpUmsThread);
extern(Windows) BOOL DeleteUmsThreadContext(UMS_CONTEXT* UmsThread);
extern(Windows) BOOL QueryUmsThreadInformation(
    UMS_CONTEXT*          UmsThread,
    UMS_THREAD_INFO_CLASS UmsThreadInfoClass,
    PVOID                 UmsThreadInformation,
    ULONG                 UmsThreadInformationLength,
    PULONG                ReturnLength
);
extern(Windows) BOOL GetUmsCompletionListEvent(UMS_COMPLETION_LIST* UmsCompletionList, HANDLE* UmsCompletionEvent);

extern(Windows) BOOL InitializeProcThreadAttributeList(
  PROC_THREAD_ATTRIBUTE_LIST* lpAttributeList,
  DWORD                        dwAttributeCount,
  DWORD                        dwFlags,
  PSIZE_T                      lpSize
);

extern(Windows) VOID DeleteProcThreadAttributeList(PROC_THREAD_ATTRIBUTE_LIST* lpAttributeList);
extern(Windows) BOOL UpdateProcThreadAttribute(
  PROC_THREAD_ATTRIBUTE_LIST* lpAttributeList,
  DWORD                        dwFlags,
  DWORD_PTR                    Attribute,
  PVOID                        lpValue,
  SIZE_T                       cbSize,
  PVOID                        lpPreviousValue,
  PSIZE_T                      lpReturnSize
);

extern(Windows) HANDLE CreateRemoteThreadEx(
  HANDLE                       hProcess,
  PSECURITY_ATTRIBUTES        lpThreadAttributes,
  SIZE_T                       dwStackSize,
  LPTHREAD_START_ROUTINE       lpStartAddress,
  LPVOID                       lpParameter,
  DWORD                        dwCreationFlags,
  PROC_THREAD_ATTRIBUTE_LIST*  lpAttributeList,
  LPDWORD                      lpThreadId
);

extern(Windows) BOOL GetQueuedCompletionStatusEx(
  HANDLE             CompletionPort,
  OVERLAPPED_ENTRY*  lpCompletionPortEntries,
  ULONG              ulCount,
  PULONG             ulNumEntriesRemoved,
  DWORD              dwMilliseconds,
  BOOL               fAlertable
);

struct OVERLAPPED_ENTRY {
  ULONG_PTR    lpCompletionKey;
  LPOVERLAPPED lpOverlapped;
  ULONG_PTR    Internal;
  DWORD        dwNumberOfBytesTransferred;
}



enum STACK_SIZE_PARAM_IS_A_RESERVATION = 0x00010000;

alias UmsSchedulerProc = extern(Windows) VOID function(UMS_SCHEDULER_REASON Reason, ULONG_PTR ActivationPayload, PVOID SchedulerParam);

struct RingQueue(T)
{
    T* store;
    size_t length;
    size_t fetch, insert, size;
    
    this(size_t capacity)
    {
        store = cast(T*)malloc(T.sizeof * capacity);
        length = capacity;
        size = 0;
    }

    void push(T ctx)
    {
        store[insert++] = ctx;
        if (insert == length) insert = 0;
        size += 1;
    }

    T pop()
    {
        auto ret = store[fetch++];
        if (fetch == length) fetch = 0;
        size -= 1;
        return ret;
    }
    bool empty(){ return size == 0; }
}

static struct IoState
{
    int result;
    UMS_CONTEXT* ctx;
}

struct SchedulerBlock
{
    UMS_COMPLETION_LIST* completionList;
    RingQueue!(UMS_CONTEXT*) queue; // queue has the number of outstanding threads
    shared uint assigned; // total assigned UMS threads (counter, accessible everywhere)
    IoState[SOCKET] ioWaiters; //used only in this UMS scheduler

    union {
        struct {
            HANDLE completionPort;
            HANDLE event;
        }
        HANDLE[2] handles;
    }

    this(int size)
    {
        queue = RingQueue!(UMS_CONTEXT*)(size);
        wenforce(CreateUmsCompletionList(&completionList), "failed to create UMS completion");
        wenforce(GetUmsCompletionListEvent(completionList, &event), "failed to get event for UMS queue");
        completionPort = CreateIoCompletionPort(cast(HANDLE)INVALID_HANDLE_VALUE, null, 0, 1);
        wenforce(completionPort != INVALID_HANDLE_VALUE, "failed to create I/O completion port");
    }
}

__gshared SchedulerBlock[] scheds;
shared uint activeThreads;
size_t schedNum; // (TLS) number of scheduler

struct Functor
{
	void delegate() func;
    size_t schedNum;
}

void startloop()
{
    import core.cpuid;
    uint threads = threadsPerCPU;
    scheds = new SchedulerBlock[threads];
    foreach (ref sched; scheds)
        sched = SchedulerBlock(100_000);
}


extern(Windows) uint worker(void* func)
{
    auto functor = *cast(Functor*)func;
    schedNum = functor.schedNum;
    functor.func();
    return 0;
}

void spawn(void delegate() func)
{
    ubyte[128] buf = void;
    size_t size = buf.length;
    PROC_THREAD_ATTRIBUTE_LIST* attrList = cast(PROC_THREAD_ATTRIBUTE_LIST*)buf.ptr;
    wenforce(InitializeProcThreadAttributeList(attrList, 1, 0, &size), "failed to initialize proc thread");
    scope(exit) DeleteProcThreadAttributeList(attrList);
    
    UMS_CONTEXT* ctx;
    wenforce(CreateUmsThreadContext(&ctx), "failed to create UMS context");

    // power of 2 random choices:
    size_t a = uniform!"[)"(0, scheds.length);
    size_t b = uniform!"[)"(0, scheds.length);
    uint loadA = scheds[a].assigned; // take into account active queue.size?
    uint loadB = scheds[b].assigned; // ditto
    size_t choice;
    if (loadA < loadB) choice = a;
    else choice = b;
    atomicOp!"+="(scheds[choice].assigned, 1);
    UMS_CREATE_THREAD_ATTRIBUTES umsAttrs;
    umsAttrs.UmsCompletionList = scheds[choice].completionList;
    umsAttrs.UmsContext = ctx;
    umsAttrs.UmsVersion = UMS_VERSION;

    auto fn = new Functor(func, choice);
    wenforce(UpdateProcThreadAttribute(attrList, 0, PROC_THREAD_ATTRIBUTE_UMS_THREAD, &umsAttrs, umsAttrs.sizeof, null, null), "failed to update proc thread");
    HANDLE handle = wenforce(CreateRemoteThreadEx(GetCurrentProcess(), null, 0, &worker, fn, 0, attrList, null), "failed to create thread");
    atomicOp!"+="(activeThreads, 1);
}

void runFibers()
{
    Thread runThread(size_t n){ // damned D lexical capture "semantics"
        auto t = new Thread(() => schedulerEntry(n));
        t.start();
        return t; 
    }
    Thread[] threads = new Thread[scheds.length-1];
    foreach (i; 0..threads.length){
        threads[i] = runThread(i+1);
    }
    schedulerEntry(0);
    foreach (t; threads)
        t.join();
}

import std.format;

void outputToConsole(const(wchar)[] msg)
{
    HANDLE output = GetStdHandle(STD_OUTPUT_HANDLE);
    uint size = cast(uint)msg.length;
    WriteConsole(output, msg.ptr, size, &size, null);
}

void logf(T...)(const(wchar)[] fmt, T args)
{
    debug try {
        import std.algorithm.mutation;
        wchar[256] buf = void;
        wchar[] slice = buf[];
        void writer(const(wchar)[] msg) {
            slice = copy(msg, slice);
        }
        formattedWrite(&writer, fmt, args);
        formattedWrite(&writer, "\n", args);
        formattedWrite(&outputToConsole, buf[0 .. $ - slice.length]);
    }
    catch (Exception e) {
        outputToConsole("ARGH!"w);
    }
}


void schedulerEntry(size_t n)
{
    schedNum = n;
    UMS_SCHEDULER_STARTUP_INFO info;
    info.UmsVersion = UMS_VERSION;
    info.CompletionList = scheds[n].completionList;
    info.SchedulerProc = &umsScheduler;
    info.SchedulerParam = null;
    wenforce(SetThreadAffinityMask(GetCurrentThread(), 1<<n), "failed to set affinity");
    wenforce(EnterUmsSchedulingMode(&info), "failed to enter UMS mode");
}

extern(Windows) VOID umsScheduler(UMS_SCHEDULER_REASON Reason, ULONG_PTR ActivationPayload, PVOID SchedulerParam)
{
    UMS_CONTEXT* ready;
    SchedulerBlock* sched = scheds.ptr + schedNum;
    auto completionList = sched.completionList;
    if (Reason == 2) {
        IoState* state = cast(IoState*)SchedulerParam;
        auto ctx = cast(UMS_CONTEXT*)ActivationPayload;
        logf("Got yield, ctx = %x schedNum: %d state = %s", ctx, schedNum, *state);
        if (state.result != int.min) { // I/O already processed
            ExecuteUmsThread(ctx);
        }
        else
            state.ctx = ctx;
    }
    else if(Reason == 0) {
        logf("Started scheduler schedNum: %d"w, schedNum);
    }
    for (;;)
    {
      if(!DequeueUmsCompletionListItems(completionList, 0, &ready)){
        logf("Failed to dequeue ums workers!"w);
        return;
      }
      auto queue = &sched.queue; // struct, so take a ref
      while (ready != null)
      {
          //logf("Dequeued UMS thread context: %x"w, ready);
          queue.push(ready);
          ready = GetNextUmsListItem(ready);
      }
      while(!queue.empty)
      {
        UMS_CONTEXT* ctx = queue.pop;
        //logf("Fetched thread context from our queue: %x", ctx);
        BOOLEAN terminated;
        uint size;
        if(!QueryUmsThreadInformation(ctx, UMS_THREAD_INFO_CLASS.UmsThreadIsTerminated, &terminated, BOOLEAN.sizeof, &size))
        {
            logf("Query UMS failed: %d"w, GetLastError());
            return;
        }
        if (!terminated)
        {
            auto ret = ExecuteUmsThread(ctx);
            if (ret == ERROR_RETRY) // this UMS thread is locked, try it later
            {
                logf("Need retry!");
                queue.push(ctx);
            }
            else
            {
                logf("Failed to execute thread %x: %d", ctx, GetLastError());
                return;
            }
        }
        else
        {
            logf("Terminated: %x"w, ctx);
            //TODO: delete context or maybe cache them somewhere?
            DeleteUmsThreadContext(ctx);
            atomicOp!"-="(sched.assigned, 1);
            atomicOp!"-="(activeThreads, 1);
        }
      }
      if (activeThreads == 0)
      {
          logf("Shutting down"w);
          return;
      }
      auto wait = WaitForMultipleObjects(2, sched.handles.ptr, FALSE, INFINITE);
      if (wait == WAIT_OBJECT_0) {
        OVERLAPPED_ENTRY[100] entries = void;
        uint count = 0;
        while(GetQueuedCompletionStatusEx(sched.completionPort, entries.ptr, 100, &count, 0, FALSE)) {
            logf("Dequeued I/O schedNum=%d events=%d", schedNum, count);
            foreach (e; entries[0..count]) {
                size_t key = cast(size_t)e.lpCompletionKey;
                auto state = key in sched.ioWaiters;
                state.result = cast(int)e.dwNumberOfBytesTransferred;
                if (state.ctx) { // got into yield before I/O completed
                    queue.push(state.ctx);
                }
            }
            if (count < 100) break;
        }
      }
    }
}

struct WSABUF 
{
    uint length;
    void* buf;
}

extern(Windows) SOCKET WSASocketW(
  int                af,
  int                type,
  int                protocol,
  void*              lpProtocolInfo,
  WORD               g,
  DWORD              dwFlags
);

extern(Windows) int WSARecv(
  SOCKET                             s,
  WSABUF                             *lpBuffers,
  DWORD                              dwBufferCount,
  LPDWORD                            lpNumberOfBytesRecvd,
  LPDWORD                            lpFlags,
  LPWSAOVERLAPPED                    lpOverlapped,
  LPWSAOVERLAPPED_COMPLETION_ROUTINE lpCompletionRoutine
);

extern(Windows) int WSASend(
  SOCKET                             s,
  WSABUF                             *lpBuffers,
  DWORD                              dwBufferCount,
  LPDWORD                            lpNumberOfBytesRecvd,
  LPDWORD                            lpFlags,
  LPWSAOVERLAPPED                    lpOverlapped,
  LPWSAOVERLAPPED_COMPLETION_ROUTINE lpCompletionRoutine
);

enum WSA_FLAG_OVERLAPPED  =  0x01;

extern(Windows) SOCKET socket(int af, int type, int protocol) {
    logf("Intercepted socket!");
    SOCKET s = WSASocketW(af, type, protocol, null, 0, WSA_FLAG_OVERLAPPED);
    registerSocket(s);
    return s;
}

void registerSocket(SOCKET s) {
    auto sched = scheds.ptr + schedNum;
    HANDLE port = sched.completionPort;
    sched.ioWaiters[s] = IoState(int.min, null);
    wenforce(CreateIoCompletionPort(cast(void*)s, port, cast(size_t)s, 0) == port, "failed to register I/O completion");
}

extern(Windows) int recv(SOCKET s, void* buf, int len, int flags) {
    OVERLAPPED overlapped;
    WSABUF wsabuf = WSABUF(cast(uint)len, buf);
    auto sched = scheds.ptr + schedNum;
    auto statePtr = s in sched.ioWaiters;
    *statePtr = IoState(int.min, null);
    int ret = WSARecv(s, &wsabuf, 1, null, cast(uint*)&flags, cast(LPWSAOVERLAPPED)&overlapped, null);
    logf("Got recv %d error: %d", ret, GetLastError());
    UmsThreadYield(statePtr);
    return statePtr.result;
}
