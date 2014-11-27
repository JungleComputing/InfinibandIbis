/* $Id: SplitterException.java 15206 2013-11-27 06:17:28Z ceriel $ */

package ibis.ipl.impl.ib;

import java.io.IOException;
import java.io.PrintStream;
import java.io.PrintWriter;
import java.nio.channels.WritableByteChannel;
import java.util.ArrayList;

public class SplitterException extends IOException {

    /**
     * Generated
     */
    private static final long serialVersionUID = 9005051418523286737L;

    // Transient, because OutputStream is not Serializable. (Thanks, Selmar
    // Smit!)
    // Note that some of the methods here are meaningless after serialization.
    private transient ArrayList<WritableByteChannel> streams = new ArrayList<WritableByteChannel>();

    private ArrayList<Exception> exceptions = new ArrayList<Exception>();

    public SplitterException() {
	// empty constructor
    }

    public void add(WritableByteChannel s, Exception e) {
	if (streams.contains(s)) {
	    System.err.println("AAA, stream was already in splitter exception");
	}

	streams.add(s);
	exceptions.add(e);
    }

    public int count() {
	return streams.size();
    }

    public WritableByteChannel[] getStreams() {
	return streams.toArray(new WritableByteChannel[0]);
    }

    public Exception[] getExceptions() {
	return exceptions.toArray(new Exception[0]);
    }

    public WritableByteChannel getStream(int pos) {
	return streams.get(pos);
    }

    public Exception getException(int pos) {
	return exceptions.get(pos);
    }

    @Override
    public String toString() {
	String res = "got " + exceptions.size() + " exceptions: ";
	for (int i = 0; i < exceptions.size(); i++) {
	    res += "   " + exceptions.get(i) + "\n";
	}

	return res;
    }

    @Override
    public void printStackTrace(PrintStream s) {
	for (int i = 0; i < exceptions.size(); i++) {
	    s.println("Exception: " + exceptions.get(i));
	    (exceptions.get(i)).printStackTrace(s);
	}
    }

    @Override
    public void printStackTrace(PrintWriter s) {
	for (int i = 0; i < exceptions.size(); i++) {
	    s.println("Exception: " + exceptions.get(i));
	    (exceptions.get(i)).printStackTrace(s);
	}
    }

    @Override
    public String getMessage() {
	return toString();
    }
}
