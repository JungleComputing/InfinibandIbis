package ibis.ipl.impl.ib;

import ibis.io.Conversion;

import java.io.IOException;
import java.io.InputStream;

public class PInputStream extends InputStream {

    private Conversion conversion = Conversion.loadConversion(false);
    private InputStream[] streams;
    private int currentStream;
    private int leftOver = -1;
    private byte[] tmp = new byte[1];
    private byte[] tmpInt = new byte[4];

    public PInputStream(Rsocket[] sockets) throws IOException {
        streams = new InputStream[sockets.length];
        for (int i = 0; i < sockets.length; i++) {
            streams[i] = sockets[i].getInputStream();
        }
    }

    @Override
    public int read() throws IOException {
        int rc = 0;
        while (rc == 0) {
            rc = this.read(tmp);
        }
        if (rc == -1) {
            return -1;
        }
        return tmp[0] & 255;
    }

    @Override
    public int read(byte[] b) throws IOException {
        return read(b, 0, b.length);
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        if (leftOver == 0) {
            currentStream = (currentStream + 1) % streams.length;
        }
        if (leftOver <= 0) {
            int index = 0;
            while (index < 4) {
                int rc = streams[currentStream].read(tmpInt, index, 4 - index);
                if (rc < 0) {
                    return -1;
                }
                index += rc;
            }
            leftOver = conversion.byte2int(tmpInt, 0);
        }

        int min = Math.min(leftOver, len);
        int rc = streams[currentStream].read(b, off, min);
        if (rc < 0) {
            return -1;
        }
        leftOver -= rc;
        return rc;
    }

    @Override
    public int available() throws IOException {
        int rc = 0;
        for (InputStream in : streams) {
            rc += in.available();
        }
        return rc;
    }

    @Override
    public void close() throws IOException {
        if (streams != null) {
            try {
                for (InputStream i : streams) {
                    i.close();
                }
            } finally {
                streams = null;
            }
        }
    }
}
