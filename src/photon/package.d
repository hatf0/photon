module photon;

import core.thread;
version(Windows) public import photon.windows.core;
else version(linux) public import photon.linux.core;
else static assert(false, "Target OS not supported by Photon yet!");

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
