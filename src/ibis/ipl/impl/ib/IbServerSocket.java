package ibis.ipl.impl.ib;

class IbServerSocket {

    RserverSocket socket = null;

    IbServerSocket(RserverSocket s) {
	socket = s;
    }

    IbSocket accept() throws java.io.IOException {
	Rsocket s = socket.accept();
	return new IbSocket(s);
    }

    IbSocketAddress getLocalSocketAddress() {
	return new IbSocketAddress(socket.getLocalSocketAddress());
    }

    void close() throws java.io.IOException {
	try {
	    socket.close();
	} finally {
	    socket = null;
	}
    }
}
