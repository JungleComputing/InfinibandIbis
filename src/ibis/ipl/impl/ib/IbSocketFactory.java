package ibis.ipl.impl.ib;

import ibis.ipl.IbisConfigurationException;
import ibis.ipl.impl.IbisIdentifier;
import ibis.util.IPUtils;
import ibis.util.TypedProperties;

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

	Rsocket s = new Rsocket();
	s.connect(addr.address, timeout);
	return new IbSocket(s);
    }

    void printStatistics(String s) {
    }
}
