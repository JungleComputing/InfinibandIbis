package ibis.ipl.impl.ib;

import java.io.IOException;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;

class IbSocket {

    Rsocket socket = null;
    WritableByteChannel out;
    ReadableByteChannel in;

    IbSocket(Rsocket s) throws IOException {
	socket = s;
	in = s.getInputChannel();
	out = s.getOutputChannel();
    }

    void setTcpNoDelay(boolean val) throws IOException {
	socket.setTcpNoDelay(val);
    }

    WritableByteChannel getOutputChannel() throws IOException {
	return out;
    }

    ReadableByteChannel getInputChannel() throws IOException {
	return in;
    }

    void close() throws java.io.IOException {
	try {
	    socket.close();
	} finally {
	    socket = null;
	    in = null;
	    out = null;
	}
    }

    @Override
    public String toString() {
	return socket.toString();
    }
}
