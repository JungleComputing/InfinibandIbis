package ibis.ipl.impl.ib;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

class IbSocket {

    Rsocket socket = null;
    InputStream in;
    OutputStream out;

    IbSocket(Rsocket s) throws IOException {
	socket = s;
	in = s.getInputStream();
	out = s.getOutputStream();
    }

    void setTcpNoDelay(boolean val) throws IOException {
	socket.setTcpNoDelay(val);
    }

    java.io.OutputStream getOutputStream() throws IOException {
	return out;
    }

    java.io.InputStream getInputStream() throws IOException {
	return in;
    }

    void close() throws java.io.IOException {
	try {
	    socket.close();
	} finally {
	    socket = null;
	}
    }

    @Override
    public String toString() {
	return socket.toString();
    }
}
