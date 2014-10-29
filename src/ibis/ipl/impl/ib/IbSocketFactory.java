package ibis.ipl.impl.ib;

import ibis.ipl.IbisConfigurationException;
import ibis.ipl.impl.IbisIdentifier;
import ibis.util.IPUtils;
import ibis.util.TypedProperties;

import java.io.DataInputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Map;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class IbSocketFactory {

    private static final Logger logger = LoggerFactory
            .getLogger(IbSocketFactory.class);

    IbSocketFactory(TypedProperties properties)
            throws IbisConfigurationException, IOException {
    }

    void setIdent(IbisIdentifier id) {
    }

    IbServerSocket createServerSocket(int port, int backlog, boolean retry,
            Properties properties) throws IOException {
        RserverSocket server = new RserverSocket();
        InetSocketAddress local = new InetSocketAddress(
                IPUtils.getLocalHostAddress(), port);
        server.bind(local, backlog);
        return new IbServerSocket(server);
    }

    IbSocket createClientSocket(IbSocketAddress addr, int timeout,
            boolean fillTimeout, Map<String, String> properties)
            throws IOException {

        int nparallel = 1;
        Rsocket s = new Rsocket();
        s.connect(addr.address, timeout);
        if (properties != null) {
            String np = properties.get("nParallelStreams");
            System.out.println("nParallelStreams = " + np);
            if (np != null) {
                try {
                    nparallel = Integer.parseInt(np);
                } catch (Throwable e) {
                    // ignore
                }
            }
        }
        s.getOutputStream().write(nparallel);
        s.getOutputStream().flush();
        if (nparallel > 1) {
            Rsocket[] result = new Rsocket[nparallel];
            result[0] = s;
            DataInputStream b = new DataInputStream(s.getInputStream());
            int sz = b.readInt();
            byte[] buf = new byte[sz];
            b.readFully(buf);
            addr = new IbSocketAddress(buf);
            for (int i = 1; i < nparallel; i++) {
                result[i] = new Rsocket();
                System.out.println("Connecting to " + addr.toString());
                result[i].connect(addr.address, timeout);
            }
            return new IbSocket(result);
        } else {
            return new IbSocket(s);
        }
    }

    void printStatistics(String s) {
    }
}
