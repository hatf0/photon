module photon.ds.intrusive_queue;

import photon.ds.common;

///
struct IntrusiveQueue(T) {
nothrow:
    T head;
    T tail;

    ///
    bool empty() { return head is null; }

    ///
    void drain(ref IntrusiveQueue!T dest) { 
        dest.head = head;
        dest.tail = tail;
        head = tail = null; 
    }

    ///
    void push(T item) {
        item.next = null;
        if (tail is null) {
            head = tail = item;
        }
        else {
            tail.next = item;
            tail = item;
        }
    }

    ///
    bool tryPop(ref T item) nothrow {
        if (!head) {
            return false;
        }
        else {
            item = head;
            head = head.next;
            if (head is null) tail = null;
            return true;
        }
    }
}

///
shared struct LockedIntrusiveQueue(T, Event, Lock) 
if (is(T : Object)) {
private:
nothrow:
    Lock lock;
    public Event event;
    IntrusiveQueue!T queue;
public:
   
    ///
    this(Event ev, Lock lockDep) {
        event = ev;
        lock = lockDep;
    }

    ///
    void drain(ref IntrusiveQueue!T dest){ 
        lock.lock();
        queue.unsharedRef.drain(dest); 
        lock.unlock();
    }

    ///
    void push(T item) {
        lock.lock();
        const shouldTrigger = queue.unsharedRef.empty;
        queue.unsharedRef.push(item);
        lock.unlock();
        if (shouldTrigger) event.trigger();
    }

    ///
    bool tryPop(ref T item) {
        lock.lock();
        bool r = queue.unsharedRef.tryPop(item);
        lock.unlock();
        return r;
    }
}

shared struct HybridIntrusiveQueue(T, Event, Lock)
if (is(T : Object)) {
private:
nothrow:
    LockedIntrusiveQueue!(T, Event, Lock) input;
    IntrusiveQueue!(T) local;
public:
   
    ///
    this(Event ev, Lock lockDep) {
        input = LockedIntrusiveQueue!(T, Event, Lock)(ev, lockDep);
    }

    ///
    @property Event event(){ return input.event; }

    ///
    void drain(ref IntrusiveQueue!T dest){ 
        IntrusiveQueue!T extra;
        local.unsharedRef.drain(dest);
        input.drain(extra);
        if (dest.empty) {
            dest = extra;
        }
        else if (!extra.empty) {
            dest.tail.next = extra.head;
            dest.tail = extra.tail;
        }
    }

    ///
    void push(T item) {
        input.push(item);
    }
    
    
    ///
    void pushLocal(T item) {
        local.unsharedRef.push(item);
    }

    ///
    bool tryPop(ref T item) {
        if (local.unsharedRef.tryPop(item)) return true;
        input.drain(local.unsharedRef);
        return local.unsharedRef.tryPop(item);
    }

}

alias Seq(T...) = T;

unittest {
    static struct EmptyEvent {
        shared nothrow void trigger(){}
    }

    static struct DummyLock {
        import core.atomic;
        shared int cnt;
        shared nothrow void lock() { atomicOp!"+="(cnt, 1); }
        shared nothrow void unlock() { atomicOp!"-="(cnt, 1); }

        shared nothrow ~this() {
            assert(cnt == 0);
        }
    }
    static class Box(T) {
        Box next;
        T item;
        this(T k) {
            item = k;
        }
    }

    static void testQueue(alias cons, Q)(Q q) {
        q.push(cons(1));
        q.push(cons(2));
        q.push(cons(3));
        Box!int ret;
        q.tryPop(ret);
        assert(ret.item == 1);
        q.tryPop(ret);
        assert(ret.item == 2);

        q.push(cons(4));
        q.tryPop(ret);
        assert(ret.item == 3);
        q.tryPop(ret);
        assert(ret.item == 4);
        q.push(cons(5));

        q.tryPop(ret);
        assert(ret.item == 5);
        assert(q.tryPop(ret) == false);
    }

    testQueue!(i => new Box!int(i))(IntrusiveQueue!(Box!int)());
    testQueue!(i => new Box!int(i))(LockedIntrusiveQueue!(Box!int, EmptyEvent, DummyLock)(EmptyEvent(), DummyLock()));
    testQueue!(i => new Box!int(i))(HybridIntrusiveQueue!(Box!int, EmptyEvent, DummyLock)(EmptyEvent(), DummyLock()));
}
