package ibis.ipl.impl.ib;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Arrays;

class IbSocket {

    Rsocket[] sockets = null;
    InputStream in;
    OutputStream out;

    IbSocket(Rsocket s) throws IOException {
        sockets = new Rsocket[1];
        sockets[0] = s;
        in = s.getInputStream();
        out = s.getOutputStream();
    }

    public IbSocket(Rsocket[] sockets) throws IOException {
        this.sockets = sockets;
        in = new PInputStream(sockets);
        out = new POutputStream(sockets);
    }

    void setTcpNoDelay(boolean val) throws IOException {
        for (Rsocket socket : sockets) {
            socket.setTcpNoDelay(val);
        }
    }

    java.io.OutputStream getOutputStream() throws IOException {
        if (sockets.length == 1) {
            return sockets[0].getOutputStream();
        }
        return out;
    }

    java.io.InputStream getInputStream() throws IOException {
        if (sockets.length == 1) {
            return sockets[0].getInputStream();
        }
        return in;
    }

    void close() throws java.io.IOException {
        if (sockets == null) {
            return;
        }
        try {
            for (Rsocket socket : sockets) {
                if (socket != null) {
                    socket.close();
                }
            }
        } finally {
            sockets = null;
        }
    }

    @Override
    public String toString() {
        return Arrays.toString(sockets);
    }
}
