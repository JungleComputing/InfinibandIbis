package ibis.ipl.impl.ib;

import ibis.ipl.ConnectionFailedException;
import ibis.ipl.IbisConfigurationException;
import ibis.ipl.ReceivePortIdentifier;
import ibis.ipl.impl.IbisIdentifier;
import ibis.util.TypedProperties;

import java.io.IOException;

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

    IbServerSocket createServerSocket() throws IOException {
	return new IbServerSocket();
    }

    IbSocket createClientSocket(String addr, int timeout, boolean fillTimeout,
	    ReceivePortIdentifier id) throws ConnectionFailedException {

	IbSocket s = new IbSocket();
	s.connect(addr, timeout, id);
	return s;
    }

    void printStatistics(String s) {
    }
}
