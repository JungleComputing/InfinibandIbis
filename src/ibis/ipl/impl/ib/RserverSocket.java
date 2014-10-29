package ibis.ipl.impl.ib;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketAddress;

public class RserverSocket {

    private boolean closed = false;

    private int localport;

    private InetAddress address;

    native void socketBind(InetAddress address, int port) throws IOException;

    native void socketListen(int count) throws IOException;

    native void socketAccept(Rsocket s) throws IOException;

    native void socketClose();

    public Rsocket accept() throws IOException {
	Rsocket s = new Rsocket();
	socketAccept(s);
	return s;
    }

    public SocketAddress getLocalSocketAddress() {
	return new InetSocketAddress(address, localport);
    }

    public void close() {
	socketClose();
	closed = true;
    }

    public void bind(InetSocketAddress local, int backlog) throws IOException {
	if (backlog < 1) {
	    backlog = 50;
	}
	socketBind(local.getAddress(), local.getPort());
	// If successful, socketBind sets address and localport.

	socketListen(backlog);

    }

    public void bind(InetSocketAddress local) throws IOException {
	bind(local, -1);
    }

}
