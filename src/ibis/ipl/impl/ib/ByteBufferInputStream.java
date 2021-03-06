/* $Id: BufferedArrayInputStream.java 13086 2011-03-14 11:02:31Z ceriel $ */

package ibis.ipl.impl.ib;

import ibis.io.Constants;
import ibis.io.DataInputStream;
import ibis.io.IOProperties;

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.CharBuffer;
import java.nio.DoubleBuffer;
import java.nio.FloatBuffer;
import java.nio.IntBuffer;
import java.nio.LongBuffer;
import java.nio.ReadOnlyBufferException;
import java.nio.ShortBuffer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This is a complete implementation of <code>DataInputStream</code>. It is
 * built on top of an <code>ReadChannel</code>. There is no need to put any
 * buffering in between. This implementation does all the buffering needed,
 * reading from a direct ByteBuffer.
 */
public final class ByteBufferInputStream extends DataInputStream {

    private static final boolean DEBUG = true;

    private static final Logger logger = LoggerFactory
	    .getLogger(ByteBufferInputStream.class);

    /** The buffer size. */
    private final int BUF_SIZE;

    /** The underlying <code>ReadChannel</code>. */
    private ReadChannel in;

    /** The buffer. */
    private ByteBuffer buffer;

    /** Number of bytes read so far from the underlying layer. */
    private long bytes = 0;

    private byte[] tempBuffer;

    /**
     * Constructor.
     * 
     * @param in
     *            the underlying <code>ReadChannel</code>
     * @param bufSize
     *            the size of the input buffer in bytes
     * @param order
     *            the byte order in which the data is to be received
     */
    public ByteBufferInputStream(ReadChannel in, int bufSize, ByteOrder order) {
	this.in = in;
	BUF_SIZE = bufSize;
	buffer = ByteBuffer.allocateDirect(BUF_SIZE);
	buffer.order(order);
	buffer.limit(0);
	if (DEBUG && logger.isDebugEnabled()) {
	    logger.debug("Creating ByteBufferInputStream " + bufSize);
	}
    }

    /**
     * Constructor.
     * 
     * @param in
     *            the underlying <code>ReadChannel</code>
     * @param order
     *            the byte order in which the data is to be received
     */
    public ByteBufferInputStream(ReadChannel in, ByteOrder order) {
	this(in, IOProperties.BUFFER_SIZE, order);
    }

    @Override
    public long bytesRead() {
	if (DEBUG && logger.isInfoEnabled()) {
	    logger.info("bytesRead returns " + (bytes - buffer.remaining()));
	}
	return bytes - buffer.remaining();
    }

    @Override
    public void resetBytesRead() {
	if (DEBUG && logger.isInfoEnabled()) {
	    logger.info("resetBytesRead called");
	}
	bytes = buffer.remaining();
    }

    private static final int min(int a, int b) {
	return (a > b) ? b : a;
    }

    @Override
    public final int read() throws IOException {
	try {
	    int b = readByte();
	    return (b & 0377);
	} catch (EOFException e) {
	    return -1;
	}
    }

    private final void fillBuffer(int len) throws IOException {

	// This ensures that there are at least 'len' bytes in the buffer.

	int buffered_bytes = buffer.remaining();
	if (buffered_bytes >= len) {
	    return;
	}
	if (buffered_bytes == 0) {
	    buffer.clear();
	    buffer.limit(buffer.position());
	} else if (buffer.position() > BUF_SIZE - len) {
	    // not enough space for "len" more bytes
	    byte[] temp = new byte[buffered_bytes];
	    buffer.get(temp);
	    buffer.position(0);
	    buffer.put(temp);
	    buffer.position(0);
	    buffer.limit(temp.length);
	}
	buffer.mark();
	buffer.position(buffer.limit());

	while (buffered_bytes < len) {
	    buffer.limit(BUF_SIZE);
	    int n = in.read(buffer);
	    if (n <= 0) {
		throw new java.io.EOFException("EOF encountered");
	    }
	    bytes += n;
	    buffered_bytes += n;
	}
	buffer.reset();
	buffer.limit(buffer.position() + buffered_bytes);
	if (DEBUG && logger.isInfoEnabled()) {
	    logger.info("After fillBuffer, bytes = " + bytes + ", remaining = "
		    + buffer.remaining());
	}
    }

    @Override
    public final int available() throws IOException {
	return (buffer.remaining());
    }

    @Override
    public void readArray(boolean[] a, int off, int len) throws IOException {

	if (DEBUG && logger.isDebugEnabled()) {
	    logger.debug("readArray(boolean[" + off + " ... " + (off + len)
		    + "])");
	}

	while (len > 0) {
	    fillBuffer(1);
	    int l = min(len, buffer.remaining());
	    for (int i = 0; i < l; i++) {
		a[off] = buffer.get() == 1;
		off++;
	    }
	    len -= l;
	}
    }

    @Override
    public void readArray(byte[] a, int off, int len) throws IOException {
	if (DEBUG && logger.isDebugEnabled()) {
	    logger.debug("readArray(byte[" + off + " ... " + (off + len) + "])");
	}
	int buffered_bytes = buffer.remaining();
	if (buffered_bytes > 0) {
	    int l = min(len, buffered_bytes);
	    buffer.get(a, off, l);
	    off += l;
	    len -= l;
	}
	while (len > 0) {
	    fillBuffer(min(BUF_SIZE, len));
	    buffered_bytes = buffer.remaining();
	    int l = min(len, buffered_bytes);
	    buffer.get(a, off, l);
	    off += l;
	    len -= l;
	}
    }

    @Override
    public void readArray(short[] a, int off, int len) throws IOException {

	if (DEBUG && logger.isDebugEnabled()) {
	    logger.debug("readArray(short[" + off + " ... " + (off + len)
		    + "])");
	}

	int useable;
	int to_convert = len * Constants.SIZEOF_SHORT;

	while (to_convert > 0) {
	    if (buffer.remaining() > 0) {
		// first, copy the data we do have to 'a' .
		useable = min(buffer.remaining() / Constants.SIZEOF_SHORT, len);
		for (int i = 0; i < useable; i++) {
		    a[off++] = buffer.getShort();
		}
		len -= useable;
		to_convert -= useable * Constants.SIZEOF_SHORT;
	    }
	    if (to_convert > 0) {
		fillBuffer(min(BUF_SIZE, to_convert));
		ShortBuffer s = buffer.asShortBuffer();
		int cnt = min(s.remaining(), len);
		s.get(a, off, cnt);
		len -= cnt;
		off += cnt;
		to_convert -= cnt * Constants.SIZEOF_SHORT;
		buffer.position(buffer.position() + cnt
			* Constants.SIZEOF_SHORT);
	    }
	}
    }

    @Override
    public void readArray(char[] a, int off, int len) throws IOException {

	if (DEBUG && logger.isDebugEnabled()) {
	    logger.debug("readArray(char[" + off + " ... " + (off + len) + "])");
	}

	int useable;
	int to_convert = len * Constants.SIZEOF_CHAR;

	while (to_convert > 0) {
	    if (buffer.remaining() > 0) {
		// first, copy the data we do have to 'a' .
		useable = min(buffer.remaining() / Constants.SIZEOF_CHAR, len);
		for (int i = 0; i < useable; i++) {
		    a[off++] = buffer.getChar();
		}
		len -= useable;
		to_convert -= useable * Constants.SIZEOF_CHAR;
	    }
	    if (to_convert > 0) {
		fillBuffer(min(BUF_SIZE, to_convert));
		CharBuffer s = buffer.asCharBuffer();
		int cnt = min(s.remaining(), len);
		s.get(a, off, cnt);
		len -= cnt;
		off += cnt;
		to_convert -= cnt * Constants.SIZEOF_CHAR;
		buffer.position(buffer.position() + cnt * Constants.SIZEOF_CHAR);
	    }
	}
    }

    @Override
    public void readArray(int[] a, int off, int len) throws IOException {

	if (DEBUG && logger.isDebugEnabled()) {
	    logger.debug("readArray(int[" + off + " ... " + (off + len) + "])");
	}

	int useable;
	int to_convert = len * Constants.SIZEOF_INT;

	while (to_convert > 0) {
	    if (buffer.remaining() > 0) {
		// first, copy the data we do have to 'a' .
		useable = min(buffer.remaining() / Constants.SIZEOF_INT, len);
		for (int i = 0; i < useable; i++) {
		    a[off++] = buffer.getInt();
		}
		len -= useable;
		to_convert -= useable * Constants.SIZEOF_INT;
	    }
	    if (to_convert > 0) {
		fillBuffer(min(BUF_SIZE, to_convert));
		IntBuffer s = buffer.asIntBuffer();
		int cnt = min(s.remaining(), len);
		s.get(a, off, cnt);
		len -= cnt;
		off += cnt;
		to_convert -= cnt * Constants.SIZEOF_INT;
		buffer.position(buffer.position() + cnt * Constants.SIZEOF_INT);
	    }
	}

    }

    @Override
    public void readArray(long[] a, int off, int len) throws IOException {

	if (DEBUG && logger.isDebugEnabled()) {
	    logger.debug("readArray(long[" + off + " ... " + (off + len) + "])");
	}

	int useable;
	int to_convert = len * Constants.SIZEOF_LONG;

	while (to_convert > 0) {
	    if (buffer.remaining() > 0) {
		// first, copy the data we do have to 'a' .
		useable = min(buffer.remaining() / Constants.SIZEOF_LONG, len);
		for (int i = 0; i < useable; i++) {
		    a[off++] = buffer.getLong();
		}
		len -= useable;
		to_convert -= useable * Constants.SIZEOF_LONG;
	    }
	    if (to_convert > 0) {
		fillBuffer(min(BUF_SIZE, to_convert));
		LongBuffer s = buffer.asLongBuffer();
		int cnt = min(s.remaining(), len);
		s.get(a, off, cnt);
		len -= cnt;
		off += cnt;
		to_convert -= cnt * Constants.SIZEOF_LONG;
		buffer.position(buffer.position() + cnt * Constants.SIZEOF_LONG);
	    }
	}
    }

    @Override
    public void readArray(float[] a, int off, int len) throws IOException {

	if (DEBUG && logger.isDebugEnabled()) {
	    logger.debug("readArray(float[" + off + " ... " + (off + len)
		    + "])");
	}

	int useable;
	int to_convert = len * Constants.SIZEOF_FLOAT;

	while (to_convert > 0) {
	    if (buffer.remaining() > 0) {
		// first, copy the data we do have to 'a' .
		useable = min(buffer.remaining() / Constants.SIZEOF_FLOAT, len);
		for (int i = 0; i < useable; i++) {
		    a[off++] = buffer.getFloat();
		}
		len -= useable;
		to_convert -= useable * Constants.SIZEOF_FLOAT;
	    }
	    if (to_convert > 0) {
		fillBuffer(min(BUF_SIZE, to_convert));
		FloatBuffer s = buffer.asFloatBuffer();
		int cnt = min(s.remaining(), len);
		s.get(a, off, cnt);
		len -= cnt;
		off += cnt;
		to_convert -= cnt * Constants.SIZEOF_FLOAT;
		buffer.position(buffer.position() + cnt
			* Constants.SIZEOF_FLOAT);
	    }
	}
    }

    @Override
    public void readArray(double[] a, int off, int len) throws IOException {

	if (DEBUG && logger.isDebugEnabled()) {
	    logger.debug("readArray(double[" + off + " ... " + (off + len)
		    + "])");
	}

	int useable;
	int to_convert = len * Constants.SIZEOF_DOUBLE;

	while (to_convert > 0) {
	    if (buffer.remaining() > 0) {
		// first, copy the data we do have to 'a' .
		useable = min(buffer.remaining() / Constants.SIZEOF_DOUBLE, len);
		for (int i = 0; i < useable; i++) {
		    a[off++] = buffer.getDouble();
		}
		len -= useable;
		to_convert -= useable * Constants.SIZEOF_DOUBLE;
	    }
	    if (to_convert > 0) {
		fillBuffer(min(BUF_SIZE, to_convert));
		DoubleBuffer s = buffer.asDoubleBuffer();
		int cnt = min(s.remaining(), len);
		s.get(a, off, cnt);
		len -= cnt;
		off += cnt;
		to_convert -= cnt * Constants.SIZEOF_DOUBLE;
		buffer.position(buffer.position() + cnt
			* Constants.SIZEOF_DOUBLE);
	    }
	}
    }

    @Override
    public byte readByte() throws IOException {
	fillBuffer(1);
	return buffer.get();
    }

    @Override
    public boolean readBoolean() throws IOException {
	fillBuffer(1);
	return buffer.get() == 1;
    }

    @Override
    public char readChar() throws IOException {
	fillBuffer(Constants.SIZEOF_CHAR);
	return buffer.getChar();
    }

    @Override
    public short readShort() throws IOException {
	fillBuffer(Constants.SIZEOF_SHORT);
	return buffer.getShort();
    }

    @Override
    public int readInt() throws IOException {
	fillBuffer(Constants.SIZEOF_INT);
	return buffer.getInt();
    }

    @Override
    public long readLong() throws IOException {
	fillBuffer(Constants.SIZEOF_LONG);
	return buffer.getLong();
    }

    @Override
    public float readFloat() throws IOException {
	fillBuffer(Constants.SIZEOF_FLOAT);
	return buffer.getFloat();
    }

    @Override
    public double readDouble() throws IOException {
	fillBuffer(Constants.SIZEOF_DOUBLE);
	return buffer.getDouble();
    }

    @Override
    public int read(byte[] b) throws IOException {
	return read(b, 0, b.length);
    }

    @Override
    public int read(byte[] a, int off, int len) throws IOException {
	if (DEBUG && logger.isDebugEnabled()) {
	    logger.debug("read(byte[" + off + " ... " + (off + len) + "])");
	}

	if (len == 0) {
	    return 0;
	}

	int available = buffer.remaining();
	if (available >= len) {
	    // data is already in the buffer.
	    buffer.get(a, off, len);
	    return len;
	}
	int rd = 0;
	if (available != 0) {
	    // first, copy the data we do have to 'a'.
	    buffer.get(a, off, available);
	    off += available;
	    rd = available;
	}
	do {
	    try {
		fillBuffer(1);
	    } catch (java.io.EOFException e) {
		if (rd > 0) {
		    return rd;
		}
		return -1;
	    }
	    int l = min(len - rd, buffer.remaining());
	    buffer.get(a, off, l);
	    off += l;
	    rd += l;
	} while (rd < len);
	return rd;
    }

    @Override
    public void close() throws IOException {
	in.close();
    }

    @Override
    public int bufferSize() {
	return BUF_SIZE;
    }

    @Override
    public void readByteBuffer(ByteBuffer value) throws IOException,
	    ReadOnlyBufferException {

	int position = value.position();
	int len = value.limit() - position;
	int buffered = buffer.remaining();

	if (buffered > 0) {
	    int l = min(len, buffered);
	    if (l == buffered) {
		value.put(buffer);
	    } else if (value.hasArray()) {
		buffer.get(value.array(), value.arrayOffset() + position, l);
		value.position(position + l);
	    } else {
		if (tempBuffer == null) {
		    tempBuffer = new byte[BUF_SIZE];
		}
		buffer.get(tempBuffer, 0, l);
		value.put(tempBuffer, 0, l);
	    }
	    len -= l;
	}
	if (len > 0) {
	    if (value.isDirect()) {
		in.readFully(value);
                if (DEBUG && logger.isDebugEnabled()) {
                    logger.debug("Read " + len + " bytes of a ByteBuffer");
                }
		bytes += len;
		len = 0;
	    } else {
		byte[] b = value.array();
		int off = value.arrayOffset() + value.position();
		readArray(b, off, len);
	    }
	}
	value.position(position);
    }

    public void clear() {
	buffer = null;
    }

    public void setByteOrder(ByteOrder order) {
	buffer.order(order);
    }
}
