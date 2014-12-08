package ibis.ipl.impl.ib;

import ibis.ipl.ConnectionFailedException;
import ibis.ipl.ReceivePortIdentifier;

import java.io.IOException;
import java.nio.ByteBuffer;

class IbSocket {
    int sockfd = -1;
    WriteChannel out;
    ReadChannel in;

    public IbSocket() {
        // nothing
    }

    public IbSocket(int fd) {
        sockfd = fd;
    }

    synchronized WriteChannel getOutputChannel() throws IOException {
        if (sockfd < 0) {
            return null;
        }
        if (out == null) {
            out = new WriteChannel(sockfd);
        }
        return out;
    }

    synchronized ReadChannel getInputChannel() throws IOException {
        if (sockfd < 0) {
            return null;
        }
        if (in == null) {
            in = new ReadChannel(sockfd);
        }
        return in;
    }

    public void connect(String address, int timeout, ReceivePortIdentifier id)
            throws ConnectionFailedException {
        sockfd = IBCommunication.clientConnect(address, id);
    }

    synchronized void close() throws java.io.IOException {
        if (sockfd < 0) {
            return;
        }
        try {
            IBCommunication.close(sockfd);
        } finally {
            in = null;
            out = null;
            sockfd = -1;
        }
    }
}
