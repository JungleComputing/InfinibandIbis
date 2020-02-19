package ibis.ipl.impl.ib;

import java.io.IOException;
import java.nio.channels.ReadableByteChannel;
import java.nio.ByteBuffer;

public class ReadChannel implements ReadableByteChannel {

    private final int sockfd;

    public ReadChannel(int fd) {
        this.sockfd = fd;
    }

    public boolean isOpen() {
        return true;
    }

    public void close() throws IOException {
        // nothing
    }

    public int read(ByteBuffer src) throws IOException {
        int retval = IBCommunication.receive(sockfd, src);
        // System.out.println("Read returns " + retval);
        return retval;
    }

    public void readFully(ByteBuffer src) throws IOException {
        IBCommunication.receiveFull(sockfd, src);
    }
}
