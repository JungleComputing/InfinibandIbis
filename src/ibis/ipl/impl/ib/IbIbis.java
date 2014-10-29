/* $Id: IbIbis.java 15337 2014-07-16 14:51:44Z ceriel $ */

package ibis.ipl.impl.ib;

import ibis.io.BufferedArrayInputStream;
import ibis.io.BufferedArrayOutputStream;
import ibis.ipl.AlreadyConnectedException;
import ibis.ipl.CapabilitySet;
import ibis.ipl.ConnectionRefusedException;
import ibis.ipl.ConnectionTimedOutException;
import ibis.ipl.Credentials;
import ibis.ipl.IbisCapabilities;
import ibis.ipl.IbisCreationFailedException;
import ibis.ipl.IbisStarter;
import ibis.ipl.MessageUpcall;
import ibis.ipl.PortMismatchException;
import ibis.ipl.PortType;
import ibis.ipl.ReceivePortConnectUpcall;
import ibis.ipl.RegistryEventHandler;
import ibis.ipl.SendPortDisconnectUpcall;
import ibis.ipl.impl.IbisIdentifier;
import ibis.ipl.impl.ReceivePort;
import ibis.ipl.impl.SendPort;
import ibis.ipl.impl.SendPortIdentifier;
import ibis.util.ThreadPool;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.SocketTimeoutException;
import java.util.HashMap;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class IbIbis extends ibis.ipl.impl.Ibis implements Runnable,
        IbProtocol {

    static final Logger logger = LoggerFactory
            .getLogger("ibis.ipl.impl.ib.IbIbis");

    private IbSocketFactory factory;

    private IbServerSocket systemServer;

    private IbSocketAddress myAddress;

    private boolean quiting = false;

    private HashMap<ibis.ipl.IbisIdentifier, IbSocketAddress> addresses = new HashMap<ibis.ipl.IbisIdentifier, IbSocketAddress>();

    public IbIbis(RegistryEventHandler registryEventHandler,
            IbisCapabilities capabilities, Credentials credentials,
            byte[] applicationTag, PortType[] types, Properties userProperties,
            IbisStarter starter) throws IbisCreationFailedException {
        super(registryEventHandler, capabilities, credentials, applicationTag,
                types, userProperties, starter);

        this.properties.checkProperties("ibis.ipl.impl.ib.", new String[] {},
                null, true);

        factory.setIdent(ident);

        // Create a new accept thread
        ThreadPool.createNew(this, "IbIbis Accept Thread");
    }

    @Override
    protected byte[] getData() throws IOException {

        factory = new IbSocketFactory(properties);

        systemServer = factory.createServerSocket(0, 50, true, null);
        myAddress = systemServer.getLocalSocketAddress();

        if (logger.isInfoEnabled()) {
            logger.info("--> IbIbis: address = " + myAddress);
        }

        return myAddress.toBytes();
    }

    /*
     * // NOTE: this is wrong ? Even though the ibis has left, the
     * IbisIdentifier may still be floating around in the system... We should
     * just have some timeout on the cache entries instead...
     * 
     * public void left(ibis.ipl.IbisIdentifier id) { super.left(id);
     * synchronized(addresses) { addresses.remove(id); } }
     * 
     * public void died(ibis.ipl.IbisIdentifier id) { super.died(id);
     * synchronized(addresses) { addresses.remove(id); } }
     */

    IbSocket connect(IbSendPort sp, ibis.ipl.impl.ReceivePortIdentifier rip,
            int timeout, boolean fillTimeout) throws IOException {

        IbisIdentifier id = (IbisIdentifier) rip.ibisIdentifier();
        String name = rip.name();
        IbSocketAddress idAddr;

        synchronized (addresses) {
            idAddr = addresses.get(id);
            if (idAddr == null) {
                idAddr = new IbSocketAddress(id.getImplementationData());
                addresses.put(id, idAddr);
            }
        }

        long startTime = System.currentTimeMillis();

        if (logger.isDebugEnabled()) {
            logger.debug("--> Creating socket for connection to " + name
                    + " at " + idAddr);
        }

        PortType sendPortType = sp.getPortType();

        do {
            DataOutputStream out = null;
            IbSocket s = null;
            int result = -1;

            sp.printManagementProperties(System.out);

            try {
                s = factory.createClientSocket(idAddr, timeout, fillTimeout,
                        sp.managementProperties());
                s.setTcpNoDelay(true);
                out = new DataOutputStream(new BufferedArrayOutputStream(
                        s.getOutputStream()));

                out.writeUTF(name);
                sp.getIdent().writeTo(out);
                sendPortType.writeTo(out);
                out.flush();

                result = s.getInputStream().read();

                switch (result) {
                case ReceivePort.ACCEPTED:
                    return s;
                case ReceivePort.ALREADY_CONNECTED:
                    throw new AlreadyConnectedException("Already connected",
                            rip);
                case ReceivePort.TYPE_MISMATCH:
                    // Read receiveport type from input, to produce a
                    // better error message.
                    DataInputStream in = new DataInputStream(s.getInputStream());
                    PortType rtp = new PortType(in);
                    CapabilitySet s1 = rtp.unmatchedCapabilities(sendPortType);
                    CapabilitySet s2 = sendPortType.unmatchedCapabilities(rtp);
                    String message = "";
                    if (s1.size() != 0) {
                        message = message
                                + "\nUnmatched receiveport capabilities: "
                                + s1.toString() + ".";
                    }
                    if (s2.size() != 0) {
                        message = message
                                + "\nUnmatched sendport capabilities: "
                                + s2.toString() + ".";
                    }
                    throw new PortMismatchException(
                            "Cannot connect ports of different port types."
                                    + message, rip);
                case ReceivePort.DENIED:
                    throw new ConnectionRefusedException(
                            "Receiver denied connection", rip);
                case ReceivePort.NO_MANY_TO_X:
                    throw new ConnectionRefusedException(
                            "Receiver already has a connection and neither ManyToOne not ManyToMany "
                                    + "is set", rip);
                case ReceivePort.NOT_PRESENT:
                case ReceivePort.DISABLED:
                    // and try again if we did not reach the timeout...
                    if (timeout > 0
                            && System.currentTimeMillis() > startTime + timeout) {
                        throw new ConnectionTimedOutException(
                                "Could not connect", rip);
                    }
                    break;
                case -1:
                    throw new IOException("Encountered EOF in IbIbis.connect");
                default:
                    throw new IOException("Illegal opcode in IbIbis.connect");
                }
            } catch (SocketTimeoutException e) {
                throw new ConnectionTimedOutException("Could not connect", rip);
            } finally {
                if (result != ReceivePort.ACCEPTED) {
                    try {
                        if (out != null) {
                            out.close();
                        }
                    } catch (Throwable e) {
                        // ignored
                    }
                    try {
                        s.close();
                    } catch (Throwable e) {
                        // ignored
                    }
                }
            }
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                // ignore
            }
        } while (true);
    }

    @Override
    protected void quit() {
        try {
            quiting = true;
            // Connect so that the IbIbis thread wakes up.
            factory.createClientSocket(myAddress, 0, false, null);
        } catch (Throwable e) {
            // Ignore
        }
    }

    private void handleConnectionRequest(IbSocket s) throws IOException {

        if (logger.isDebugEnabled()) {
            logger.debug("--> IbIbis got connection request from " + s);
        }

        BufferedArrayInputStream bais = new BufferedArrayInputStream(
                s.getInputStream());

        DataInputStream in = new DataInputStream(bais);
        OutputStream out = s.getOutputStream();

        String name = in.readUTF();
        SendPortIdentifier send = new SendPortIdentifier(in);
        PortType sp = new PortType(in);

        // First, lookup receiveport.
        IbReceivePort rp = (IbReceivePort) findReceivePort(name);

        int result;
        if (rp == null) {
            result = ReceivePort.NOT_PRESENT;
        } else {
            synchronized (rp) {
                result = rp.connectionAllowed(send, sp);
            }
        }

        if (logger.isDebugEnabled()) {
            logger.debug("--> S RP = " + name + ": "
                    + ReceivePort.getString(result));
        }

        out.write(result);
        if (result == ReceivePort.TYPE_MISMATCH) {
            DataOutputStream dout = new DataOutputStream(out);
            rp.getPortType().writeTo(dout);
            dout.flush();
        }
        out.flush();
        if (result == ReceivePort.ACCEPTED) {
            // add the connection to the receiveport.
            rp.connect(send, s, bais);
            if (logger.isDebugEnabled()) {
                logger.debug("--> S connect done ");
            }
        } else {
            out.close();
            in.close();
            s.close();
        }
    }

    public void run() {
        // This thread handles incoming connection request from the
        // connect(IbSendPort) call.

        boolean stop = false;

        while (!stop) {
            IbSocket s = null;

            if (logger.isDebugEnabled()) {
                logger.debug("--> IbIbis doing new accept()");
            }

            try {
                s = systemServer.accept();
                s.setTcpNoDelay(true);
            } catch (Throwable e) {
                /* if the accept itself fails, we have a fatal problem. */
                logger.error("IbIbis:run: got fatal exception in accept! ", e);
                cleanup();
                throw new Error("Fatal: IbIbis could not do an accept", e);
                // This error is thrown in the IbIbis thread, not in a user
                // thread. It kills the thread.
            }

            if (logger.isDebugEnabled()) {
                logger.debug("--> IbIbis through new accept()");
            }

            try {
                if (quiting) {
                    s.close();
                    if (logger.isDebugEnabled()) {
                        logger.debug("--> it is a quit: RETURN");
                    }
                    cleanup();
                    return;
                }

                // This thread will now live on as a connection handler. Start
                // a new accept thread here, and make sure that this thread does
                // not do an accept again, if it ever returns to this loop.
                stop = true;

                try {
                    Thread.currentThread().setName("Connection Handler");
                } catch (Exception e) {
                    // ignore
                }

                ThreadPool.createNew(this, "IbIbis Accept Thread");

                // Try to get the accept thread into an accept call. (Ceriel)
                // Thread.currentThread().yield();
                //
                // Yield is evil. It breaks the whole concept of starting a
                // replacement thread and handling the incoming request
                // ourselves. -- Jason

                handleConnectionRequest(s);
            } catch (Throwable e) {
                try {
                    s.close();
                } catch (Throwable e2) {
                    // ignored
                }
                logger.error("EEK: IbIbis:run: got exception "
                        + "(closing this socket only: ", e);
            }
        }
    }

    private void cleanup() {
        try {
            systemServer.close();
        } catch (Throwable e) {
            // Ignore
        }
    }

    @Override
    protected SendPort doCreateSendPort(PortType tp, String nm,
            SendPortDisconnectUpcall cU, Properties props) throws IOException {
        return new IbSendPort(this, tp, nm, cU, props);
    }

    @Override
    protected ReceivePort doCreateReceivePort(PortType tp, String nm,
            MessageUpcall u, ReceivePortConnectUpcall cU, Properties props)
            throws IOException {
        return new IbReceivePort(this, tp, nm, u, cU, props);
    }

}
