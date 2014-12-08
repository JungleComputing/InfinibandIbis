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

    public static native int clientConnect2(String address, String port) throws IbisIOException;

    public static native int serverCreate2() throws IbisIOException;

    public static native int accept2(int sockfd) throws IbisIOException;

    public static native int send2(int sockfd, ByteBuffer buffer, int offset,
            int size) throws IbisIOException;

    public static native int receive2(int sockfd, ByteBuffer buffer,
            int offset, int size, boolean full) throws IbisIOException;

    public static native String getPeerIP2(int sockfd) throws IbisIOException;

    public static native String getSockIP2(int sockfd) throws IbisIOException;

    public static native int close2(int sockfd) throws IbisIOException;

    public static String getPeerIP(int sockfd)
            throws IbisConfigurationException {
        try {
            String result = getPeerIP2(sockfd);
            if (result.equals("")) {
                throw new IbisConfigurationException("cannot determine ip address");
            }
            return result;
        } catch(IbisIOException e) {
            throw new IbisConfigurationException("cannot determine ip address", e);
        }
    }

    public static String getSockIP(int sockfd)
            throws IbisConfigurationException {
        try {
            String result = getSockIP2(sockfd);
            if (result.equals("")) {
                throw new IbisConfigurationException("cannot determine ip address");
            }
            String addr = result.substring(0, result.lastIndexOf(":"));
            String port = result.substring(result.lastIndexOf(":") + 1);
            InetAddress[] addresses = IPUtils.getLocalHostAddresses();
            NetworkInterface[] ifs = new NetworkInterface[addresses.length];
            for (int i = 0; i < ifs.length; i++) {
                ifs[i] = NetworkInterface.getByInetAddress(addresses[i]);
                if (ifs[i] != null) {
                    /*
                     * System.out.println("Address = " + addresses[i]);
                     * System.out .println("Interface getName: " +
                     * ifs[i].getName());
                     * System.out.println("Interface getDisplayname: " +
                     * ifs[i].getDisplayName());
                     * System.out.println("Interface = " + ifs[i]);
                     */
                    if (ifs[i].getName().startsWith("ib")) {
                        String s = addresses[i].toString();
                        addr = s.substring(s.lastIndexOf("/") + 1);
                        break;
                    }
                }
            }
            return addr + ":" + port;
        } catch (Throwable e) {
            throw new IbisConfigurationException("Cannot determine addresses",
                    e);
        }
    }

    public static int clientConnect(String address, ReceivePortIdentifier id)
            throws ConnectionFailedException {
        int i = address.lastIndexOf(":");
        String a = address.substring(0, i);
        String p = address.substring(i + 1);
        int sockfd;
        try {
            sockfd = clientConnect2(a, p);
        } catch(Throwable e) {
                throw new ConnectionFailedException("cannot connect to " + address,
                        id, e);
        }
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
        bb.position(bb.limit());
        return size;
    }

    public static int receive(int sockfd, ByteBuffer bb) throws IbisIOException {
        if (sockfd < 0) {
            throw new IbisIOException("invalid socket file descriptor");
        }
        int size = bb.remaining();
        if (!bb.isDirect()) {
            throw new IbisIOException("Direct bytebuffer expected");
        }
        int r = receive2(sockfd, bb, bb.position(), size, false);
        if (r < 0) {
            throw new IbisIOException("Something wrong in receive2");
        }
        bb.position(bb.position() + r);
        return r;
    }

    public static void receiveFull(int sockfd, ByteBuffer bb) throws IbisIOException {
        if (sockfd < 0) {
            throw new IbisIOException("invalid socket file descriptor");
        }
        int size = bb.remaining();
        if (!bb.isDirect()) {
            throw new IbisIOException("Direct bytebuffer expected");
        }
        int r = receive2(sockfd, bb, bb.position(), size, true);
        if (r != size) {
            throw new IbisIOException("Something wrong in receive2");
        }
        bb.position(bb.position() + r);
    }

    public static void close(int sockfd) throws IbisIOException {
        close2(sockfd);
    }

}
