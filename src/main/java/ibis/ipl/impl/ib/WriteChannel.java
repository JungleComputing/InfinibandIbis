package ibis.ipl.impl.ib;

import java.io.IOException;
import java.nio.channels.WritableByteChannel;
import java.nio.ByteBuffer;

public class WriteChannel implements WritableByteChannel {

    private final int sockfd;

    public WriteChannel(int fd) {
        this.sockfd = fd;
    }

    public boolean isOpen() {
        return true;
    }

    public void close() throws IOException {
        // nothing
    }

    public int write(ByteBuffer src) throws IOException {
        int retval = IBCommunication.send(sockfd, src);
        // System.out.println("Write returns " + retval);
        return retval;
    }
}
