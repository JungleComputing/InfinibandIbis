package ibis.ipl.impl.ib;

import ibis.io.IbisIOException;

class IbServerSocket {

    int sockfd = -1;
    String myAddress;

    IbServerSocket() throws IbisIOException {
	sockfd = IBCommunication.serverCreate();
	myAddress = IBCommunication.getSockIP(sockfd);
    }

    IbSocket accept() throws java.io.IOException {
	int fd = IBCommunication.accept(sockfd);
	return new IbSocket(fd);
    }

    String getLocalSocketAddress() {
	return myAddress;
    }

    void close() throws java.io.IOException {
	try {
	    IBCommunication.close(sockfd);
	} finally {
	    sockfd = -1;
	}
    }
}
