/* $Id: OutputStreamSplitter.java 14649 2012-04-19 11:59:46Z ceriel $ */

package ibis.ipl.impl.ib;

import ibis.util.ThreadPool;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;

/**
 * Contract: write to multiple WriteChannels. When an exception occurs, store it
 * and continue. When the data is written to all channels, throw a single
 * exception that contains all previous exceptions. This way, even when one of
 * the channels dies, the rest will receive the data.
 **/
public final class WriteChannelSplitter extends WriteChannel {

    private static final int MAXTHREADS = 32;

    private boolean removeOnException = false;
    private boolean saveException = false;
    private SplitterException savedException = null;
    private long bytesWritten = 0;

    ArrayList<WriteChannel> out = new ArrayList<WriteChannel>();

    private int numSenders = 0;

    private class Sender implements Runnable {
	ByteBuffer buffer;
	int index;
	int sz;

	Sender(ByteBuffer buffer, int sz, int index) {
	    this.buffer = buffer.duplicate();
	    this.sz = sz;
	    this.index = index;
	}

	@Override
	public void run() {
	    doWrite(buffer, sz, index);
	    finish();
	}
    }

    private class Closer implements Runnable {
	int index;

	Closer(int index) {
	    this.index = index;
	}

	@Override
	public void run() {
	    doClose(index);
	    finish();
	}
    }

    private void doWrite(ByteBuffer buffer, int sz, int index) {
	try {
	    WriteChannel o = out.get(index);
	    if (o != null) {
		while (sz > 0) {
		    int n = o.write(buffer);
		    sz -= n;
		}
	    }
	} catch (IOException e) {
	    addException(e, index);
	}
    }

    private void doClose(int index) {
	try {
	    WriteChannel o = out.get(index);
	    if (o != null) {
		o.close();
	    }
	} catch (IOException e) {
	    addException(e, index);
	}
    }

    private synchronized void finish() {
	numSenders--;
	notifyAll();
    }

    private synchronized void addException(IOException e, int index) {
	if (savedException == null) {
	    savedException = new SplitterException();
	}
	savedException.add(out.get(index), e);
	if (removeOnException) {
	    out.set(index, null);
	}
    }

    public WriteChannelSplitter() {
	super(-1);
    }

    public WriteChannelSplitter(boolean removeOnException, boolean saveException) {
	this();
	this.removeOnException = removeOnException;
	this.saveException = saveException;
    }

    public synchronized void add(WriteChannel s) {
	out.add(s);
    }

    public synchronized void remove(WriteChannel s) throws IOException {

	while (numSenders != 0) {
	    try {
		wait();
	    } catch (Exception e) {
		// Ignored
	    }
	}

	int i = out.indexOf(s);

	if (i == -1) {
	    throw new IOException("Removing unknown stream from splitter.");
	}

	out.remove(i);
    }

    @Override
    public int write(ByteBuffer b) throws IOException {
	int r = b.remaining();
	int sz = out.size();
	bytesWritten += r * sz;
	if (sz > 1) {
	    synchronized (this) {
		while (numSenders != 0) {
		    try {
			wait();
		    } catch (Exception e) {
			// Ignored
		    }
		}
		numSenders++;
	    }
	    for (int i = 1; i < sz; i++) {
		Sender s = new Sender(b, r, i);
		runThread(s, "Splitter sender");
	    }
	}
	doWrite(b, r, 0);
	done();
	return r;
    }

    @Override
    public void close() throws IOException {

	if (out.size() > 1) {
	    synchronized (this) {
		while (numSenders != 0) {
		    try {
			wait();
		    } catch (Exception e) {
			// Ignored
		    }
		}
		numSenders++;
	    }

	    for (int i = 1; i < out.size(); i++) {
		Closer f = new Closer(i);
		runThread(f, "Splitter closer");
	    }
	}
	doClose(0);
	done();
    }

    public long bytesWritten() {
	return bytesWritten;
    }

    public void resetBytesWritten() {
	bytesWritten = 0;
    }

    public SplitterException getExceptions() {
	SplitterException e = savedException;
	savedException = null;
	return e;
    }

    private void runThread(Runnable r, String name) {
	synchronized (this) {
	    while (numSenders >= MAXTHREADS) {
		try {
		    wait();
		} catch (Exception e) {
		    // Ignored
		}
	    }
	    numSenders++;
	}
	ThreadPool.createNew(r, name);
    }

    private void done() throws IOException {
	if (out.size() > 1) {
	    synchronized (this) {
		numSenders--;
		while (numSenders != 0) {
		    try {
			wait();
		    } catch (Exception e) {
			// Ignored
		    }
		}
		notifyAll();
	    }
	}

	if (savedException != null) {
	    synchronized (this) {
		if (removeOnException) {
		    for (int i = 0; i < out.size(); i++) {
			if (out.get(i) == null) {
			    out.remove(i);
			    i--;
			}
		    }
		}

		if (!saveException) {
		    SplitterException e = savedException;
		    savedException = null;
		    throw e;
		}
	    }
	}
    }

    @Override
    public boolean isOpen() {
	// TODO Auto-generated method stub
	return true;
    }
}
