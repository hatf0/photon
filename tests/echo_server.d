module echo_server;

import std.stdio;
import std.socket;
import std.conv;
import std.string;
import std.algorithm;
import std.conv;
import std.format;
import std.range;

import core.thread;

import photon;

// telnet localhost 1337
void server_worker(Socket client) {
    ubyte[1024] buffer;
    scope(exit) {
        client.shutdown(SocketShutdown.BOTH);
        client.close();
    }
    logf("Started server_worker, client = %s", client);
    for(;;) {
        ptrdiff_t received = client.receive(buffer);
        if (received < 0) {
            perror("Error while reading from client");
            return;
        }
        else if (received == 0) { //socket is closed (eof)
            return;
        }
        else {
            logf("Server_worker received:\n<%s>", cast(char[])buffer[0.. received]);
            parser.execute(buffer[0..received]);
        }
        ptrdiff_t sent;
        do {
            ptrdiff_t ret = client.send(buf[sent .. received]);
            if (ret < 0) {
                perror("Error while writing to client");
                return;
            }
            sent += received;
        } while(sent < received);
    } while(keepAlive);
}

void server() {
    Socket server = new TcpSocket();
    server.setOption(SocketOptionLevel.SOCKET, SocketOption.REUSEADDR, true);
    server.bind(new InternetAddress("localhost", 1337));
    server.listen(1000);

    logf("Started server");
    void processClient(Socket client) {
        spawn(() => server_worker(client));
    }
    while(true) {
        logf("Waiting for server.accept()");
        Socket client = server.accept();
        logf("New client accepted %s", client);
        processClient(client);
    }
}

void main() {
    startloop();
    spawn(() => server());
    runFibers();
}
