package ibis.ipl.impl.ib;

import ibis.ipl.ConnectionFailedException;
import ibis.ipl.ReceivePortIdentifier;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;

class IbSocket {
    int sockfd = -1;
    WritableByteChannel out;
    ReadableByteChannel in;

    public IbSocket() {
        // nothing
    }

    public IbSocket(int fd) {
        sockfd = fd;
    }

    synchronized WritableByteChannel getOutputChannel() throws IOException {
        if (sockfd < 0) {
            return null;
        }
	if (out == null) {
	    out = new WritableByteChannel() {

                @Override
                public boolean isOpen() {
                    return true;
                }

                @Override
                public void close() throws IOException {
                    // nothing
                }

                @Override
                public int write(ByteBuffer src) throws IOException {
                    int retval = IBCommunication.send(sockfd, src);
                    return retval;
                }
            };
        }
        return out;
    }

    synchronized ReadableByteChannel getInputChannel() throws IOException {
        if (sockfd < 0) {
            return null;
        }
	if (in == null) {
	    in = new ReadableByteChannel() {

                @Override
                public boolean isOpen() {
                    return true;
                }

                @Override
                public void close() throws IOException {
                    // nothing
                }

                @Override
                public int read(ByteBuffer dst) throws IOException {
                    return IBCommunication.receive(sockfd, dst);
                }
            };
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
