import std.stdio;
import core.sys.posix.unistd : write, _exit;
import core.sys.posix.sys.types;
import std.socket;
import core.stdc.errno;
import core.sys.posix.sys.socket;
import core.sys.posix.fcntl;
import core.thread;
import core.sys.posix.stdlib: abort;
import dfio;

void check(int code) {
    if(code < 0)
        abort();
}

// if this writes say 100 bytes total
void writerReader(int fd1, int fd2) {
    stderr.writefln("<started writerReader, fd1 = %d, fd2 = %d>", fd1, fd2);
    auto s = "simple read write\n";
    write(fd1, s.ptr, s.length).checked;

    stderr.writefln("<midway writerReader, fd1 = %d, fd2 = %d>", fd1, fd2);

    char[100] buf2;
    ssize_t total = 17;
    ssize_t bytes = 0;
    while(bytes < total) {
        ssize_t resp = read(fd2, buf2.ptr + bytes, total - bytes).checked;
        stderr.writefln("read1 resp = %s", resp);
        bytes += resp;
    }

    stderr.writefln("<finished writerReader>");
}

// it must read the exact same amount (in total) that would be 100 bytes
void readerWriter(int fd1, int fd2) {
    stderr.writefln("<started readerWriter, fd1 = %d, fd2 = %d>", fd1, fd2);
    char[100] buf;
    ssize_t total = 17;
    ssize_t bytes = 0;
    while(bytes < total) {
        ssize_t resp = read(fd1, buf.ptr + bytes, total - bytes).checked;
        stderr.writefln("read2 resp = %s", resp);
        bytes += resp;
    }

    stderr.writefln("<midway readerWriter, fd1 = %d, fd2 = %d>", fd1, fd2);

    auto s = "simple read write\n";
    char[] buf2 = s.dup;
    write(fd2, s.ptr, s.length).checked;
    stderr.writefln("<finished readerWriter>");
}

void main() {
    int[2] socks1, socks2;
    startloop();
    check(socketpair(AF_UNIX, SOCK_STREAM, 0, socks1));
    check(socketpair(AF_UNIX, SOCK_STREAM, 0, socks2));
    writeln(socks1);
    writeln(socks2);
    // spawn a thread to run I/O loop
    // spawn thread to write stuff
    auto wr = new Thread(() => writerReader(socks1[0], socks2[0]));
    wr.start();

    // spawn fiber to read stuff
    spawn(() => readerWriter(socks1[1], socks2[1]));
    runUntilCompletion();
    //
    wr.join();
}
