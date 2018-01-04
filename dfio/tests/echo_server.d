import std.stdio;
import std.socket;
import std.conv;
import core.thread;
import std.string;

import dfio;
// curl -d "value=test"  -X POST localhost:1337


void server_worker(Socket client) {
    char[1024] buffer;
    scope(exit) {
        client.shutdown(SocketShutdown.BOTH);
        client.close();
    }
    logf("Started server_worker, client = %s", client);
    auto received = client.receive(buffer);
    if (received < 0) {
        logf("Error %d", received);
        perror("Error while reading from client");
        return;
    }
    logf("Server_worker received:\n%s", buffer[0.. received]);

    enum header =
        "HTTP/1.0 200 OK\nContent-Type: text/html; charset=utf-8\n\n";

    string response = header ~ to!string(buffer[0..received]) ~ "\n";
    client.send(response);
}

void server() {
    Socket server = new TcpSocket();
    server.setOption(SocketOptionLevel.SOCKET, SocketOption.REUSEADDR, true);
    server.bind(new InternetAddress("localhost", 1337));
    server.listen(1);

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

void client(string toSend) {
    auto request = new TcpSocket();
    request.connect(new InternetAddress("localhost", 1337));
    request.send(toSend.dup) ;

    logf("Sending %s", toSend);

    // TODO timeout?
    char[1024] response;
    long len = request.receive(response);
    if (len < 0){
        perror("Error while reading on client");
        abort();
    }

    logf("Received len = %d", len);
    auto received = response[0..len];

    request.close;

    logf("received: %s", received);
}

void main() {
    startloop();
    spawn(() => server());
    runFibers();
}
