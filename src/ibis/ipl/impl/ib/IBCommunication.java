package ibis.ipl.impl.ib;

import ibis.io.IbisIOException;
import ibis.ipl.ConnectionFailedException;
import ibis.ipl.IbisConfigurationException;
import ibis.ipl.ReceivePortIdentifier;
import ibis.util.IPUtils;

import java.io.IOException;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.nio.ByteBuffer;

import cz.adamh.utils.NativeUtils;

public class IBCommunication {

    static {
        try {
            NativeUtils.loadLibraryFromJar("/libibcommunication.so");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static native int clientConnect2(String address, String port);

    public static native int serverCreate2();

    public static native int accept2(int sockfd);

    public static native int send2(int sockfd, ByteBuffer buffer, int offset,
            int size);

    public static native int receive2(int sockfd, ByteBuffer buffer,
            int offset, int size);

    public static native String getPeerIP2(int sockfd);

    public static native String getSockIP2(int sockfd);

    public static native int close2(int sockfd);

    public static String getPeerIP(int sockfd)
            throws IbisConfigurationException {
        String result = getPeerIP2(sockfd);
        if (result.equals("")) {
            throw new IbisConfigurationException("cannot determine ip address");
        }
        return result;
    }

    public static String getSockIP(int sockfd)
            throws IbisConfigurationException {
        String result = getSockIP2(sockfd);
        if (result.equals("")) {
            throw new IbisConfigurationException("cannot determine ip address");
        }
        String port = result.substring(result.lastIndexOf(":") + 1);
        try {
            InetAddress[] addresses = IPUtils.getLocalHostAddresses();
            NetworkInterface[] ifs = new NetworkInterface[addresses.length];
            for (int i = 0; i < ifs.length; i++) {
                ifs[i] = NetworkInterface.getByInetAddress(addresses[i]);
                if (ifs[i] != null) {
                    System.out.println("Address = " + addresses[i]);
                    System.out
                            .println("Interface getName: " + ifs[i].getName());
                    System.out.println("Interface getDisplayname: "
                            + ifs[i].getDisplayName());
                    System.out.println("Interface = " + ifs[i]);
                }
            }
        } catch (Throwable e) {
            throw new IbisConfigurationException("Cannot determine addresses",
                    e);
        }
        // TODO: get IP of Infiniband and use that!
        return result;
    }

    public static int clientConnect(String address, ReceivePortIdentifier id)
            throws ConnectionFailedException {
        int i = address.lastIndexOf(":");
        String a = address.substring(0, i);
        String p = address.substring(i + 1);
        int sockfd = clientConnect2(a, p);
        if (sockfd < 0) {
            throw new ConnectionFailedException("cannot connect to " + address,
                    id);
        }
        return sockfd;
    }

    public synchronized static int accept(int fd) throws IbisIOException {
        int sockfd = accept2(fd);
        if (sockfd < 0) {
            throw new IbisIOException("cannot set up server");
        }
        return sockfd;
    }

    public synchronized static int serverCreate() throws IbisIOException {
        int sockfd = serverCreate2();
        if (sockfd < 0) {
            throw new IbisIOException("cannot set up server");
        }
        return sockfd;
    }

    public static int send(int sockfd, ByteBuffer bb) throws IbisIOException {
        if (sockfd < 0) {
            throw new IbisIOException("invalid socket file descriptor");
        }
        int size = bb.remaining();
        int s = send2(sockfd, bb, bb.position(), size);
        if (s != 0) {
            throw new IbisIOException("Something wrong in send2");
        }
        return size;
    }

    public static int receive(int sockfd, ByteBuffer bb) throws IbisIOException {
        if (sockfd < 0) {
            throw new IbisIOException("invalid socket file descriptor");
        }
        int size = bb.remaining();
        int r = receive2(sockfd, bb, bb.position(), bb.remaining());
        if (r != 0) {
            throw new IbisIOException("Something wrong in receive2");
        }
        return size;
    }

    public static void close(int sockfd) {
        close2(sockfd);
    }

}
