package org.hiwei.election;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;

public class ZkSocket {
    private Socket socket;
    private DataOutputStream don;
    private DataInputStream din;

    public ZkSocket(Socket socket) throws IOException {
        this.socket = socket;
        socket.setKeepAlive(true);
        this.don = new DataOutputStream(socket.getOutputStream());
        this.din = new DataInputStream(socket.getInputStream());
    }

    public DataOutputStream getDon() {
        return don;
    }

    public DataInputStream getDin() {
        return din;
    }

    public Socket getSocket() {
        return socket;
    }
}
