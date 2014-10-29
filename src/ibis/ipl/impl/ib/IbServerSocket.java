package ibis.ipl.impl.ib;

import ibis.util.IPUtils;

import java.io.DataOutputStream;
import java.net.InetSocketAddress;

class IbServerSocket {

    RserverSocket socket = null;

    IbServerSocket(RserverSocket s) {
        socket = s;
    }

    IbSocket accept() throws java.io.IOException {
        Rsocket s = socket.accept();
        int b = s.getInputStream().read();
        if (b > 1) {
            RserverSocket n = new RserverSocket();
            Rsocket[] result = new Rsocket[b];
            result[0] = s;
            try {
                InetSocketAddress local = new InetSocketAddress(
                        IPUtils.getLocalHostAddress(), 0);
                n.bind(local);
                IbSocketAddress addr = new IbSocketAddress(
                        n.getLocalSocketAddress());
                byte[] baddr = addr.toBytes();
                DataOutputStream d = new DataOutputStream(s.getOutputStream());
                d.writeInt(baddr.length);
                d.write(baddr);
                d.flush();
                for (int i = 1; i < b; i++) {
                    System.out
                            .println("Accept from address " + addr.toString());
                    result[i] = n.accept();
                }
                return new IbSocket(result);
            } finally {
                n.close();
            }
        } else {
            return new IbSocket(s);
        }
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
