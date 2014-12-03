package ibis.ipl.impl.ib;

class IbServerSocket {

    int sockfd = -1;
    String myAddress;

    IbServerSocket() {
	// TODO: create socket and initialize sockfd and myAddress.
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
