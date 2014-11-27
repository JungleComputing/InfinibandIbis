/* $Id: BufferedArrayOutputStream.java 14878 2012-09-04 20:11:40Z ceriel $ */

package ibis.ipl.impl.ib;

import ibis.io.Constants;
import ibis.io.DataOutputStream;
import ibis.io.IOProperties;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.CharBuffer;
import java.nio.DoubleBuffer;
import java.nio.FloatBuffer;
import java.nio.IntBuffer;
import java.nio.LongBuffer;
import java.nio.ShortBuffer;
import java.nio.channels.WritableByteChannel;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This is a complete implementation of <code>DataOutputStream</code>. It is
 * built on top of an <code>WritableByteChannel</code>. There is no need to put
 * any buffering inbetween. This implementation does all the buffering needed.
 */
public final class ByteBufferOutputStream extends DataOutputStream {

    private static final Logger logger = LoggerFactory
	    .getLogger(ByteBufferOutputStream.class);

    private static final boolean DEBUG = IOProperties.DEBUG;

    /** Size of the buffer in which output data is collected. */
    private final int BUF_SIZE;

    /** The underlying <code>WritableByteChannel</code>. */
    private WritableByteChannel out;

    /** The buffer in which output data is collected. */
    private ByteBuffer buffer;

    /** Number of bytes written so far to the underlying layer. */
    private long bytes = 0;

    /**
     * Constructor.
     * 
     * @param out
     *            the underlying <code>OutputStream</code>
     * @param bufSize
     *            the size of the output buffer in bytes
     */
    public ByteBufferOutputStream(WritableByteChannel out, int bufSize) {
	this.out = out;
	BUF_SIZE = bufSize;
	buffer = ByteBuffer.allocateDirect(BUF_SIZE);
	buffer.order(ByteOrder.nativeOrder());
    }

    /**
     * Constructor.
     * 
     * @param out
     *            the underlying <code>OutputStream</code>
     */
    public ByteBufferOutputStream(WritableByteChannel out) {
	this(out, IOProperties.BUFFER_SIZE);
    }

    @Override
    public long bytesWritten() {
	return bytes + buffer.position();
    }

    @Override
    public void resetBytesWritten() {
	bytes = -buffer.position();
    }

    /**
     * Checks if there is space for <code>incr</code> more bytes and if not, the
     * buffer is written to the underlying <code>OutputStream</code>.
     * 
     * @param incr
     *            the space requested
     * @exception IOException
     *                in case of trouble.
     */
    private void flush(int incr) throws IOException {

	int index = buffer.position();
	if (DEBUG && logger.isDebugEnabled()) {
	    logger.debug("flush(" + incr + ") : " + " "
		    + (index + incr >= BUF_SIZE) + " " + (index) + ")");
	}

	if (index + incr > BUF_SIZE) {
	    bytes += index;
	    buffer.limit(index);
	    buffer.position(0);
	    out.write(buffer);
	    buffer.clear();
	}
    }

    @Override
    public void write(int b) throws IOException {
	writeByte((byte) b);
    }

    @Override
    public void writeBoolean(boolean value) throws IOException {
	writeByte(value ? (byte) 1 : (byte) 0);
    }

    @Override
    public void writeByte(byte value) throws IOException {
	flush(1);
	buffer.put(value);
    }

    @Override
    public void writeChar(char value) throws IOException {
	flush(Constants.SIZEOF_CHAR);
	buffer.putChar(value);
    }

    @Override
    public void writeShort(short value) throws IOException {
	flush(Constants.SIZEOF_SHORT);
	buffer.putShort(value);
    }

    @Override
    public void writeInt(int value) throws IOException {
	flush(Constants.SIZEOF_INT);
	buffer.putInt(value);
    }

    @Override
    public void writeLong(long value) throws IOException {
	flush(Constants.SIZEOF_LONG);
	buffer.putLong(value);
    }

    @Override
    public void writeFloat(float value) throws IOException {
	flush(Constants.SIZEOF_FLOAT);
	buffer.putFloat(value);
    }

    @Override
    public void writeDouble(double value) throws IOException {
	flush(Constants.SIZEOF_DOUBLE);
	buffer.putDouble(value);
    }

    @Override
    public void write(byte[] b) throws IOException {
	writeArray(b);
    }

    @Override
    public void write(byte[] b, int off, int len) throws IOException {
	writeArray(b, off, len);
    }

    @Override
    public void writeArray(boolean[] ref, int off, int len) throws IOException {
	if (DEBUG && logger.isDebugEnabled()) {
	    logger.debug("writeArray(boolean[" + off + " ... " + (off + len)
		    + "])");
	}

	do {
	    flush(1);
	    int index = buffer.position();

	    int size = Math.min(BUF_SIZE - index, len);
	    for (int i = off; i < off + size; i++) {
		buffer.put(ref[i] ? (byte) 1 : (byte) 0);
	    }
	    off += size;
	    len -= size;
	} while (len != 0);
    }

    @Override
    public void writeArray(byte[] ref, int off, int len) throws IOException {
	if (DEBUG && logger.isDebugEnabled()) {
	    logger.debug("writeArray(byte[" + off + " ... " + (off + len)
		    + "])");
	}

	int index = buffer.position();

	if (len > (BUF_SIZE - index)) {

	    if (index > 0) {
		flush(len);
	    }
	    if (len >= BUF_SIZE) {
		bytes += len;
		out.write(ByteBuffer.wrap(ref, off, len));
	    } else {
		buffer.put(ref, off, len);
	    }
	} else {
	    buffer.put(ref, off, len);
	}
    }

    @Override
    public void writeArray(char[] ref, int off, int len) throws IOException {
	if (DEBUG && logger.isDebugEnabled()) {
	    logger.debug("writeArray(char[" + off + " ... " + (off + len)
		    + "])");
	}

	do {
	    flush(Constants.SIZEOF_CHAR);
	    int index = buffer.position();

	    int size = Math
		    .min((BUF_SIZE - index) / Constants.SIZEOF_CHAR, len);
	    CharBuffer c = buffer.asCharBuffer();
	    c.put(ref, off, size);
	    buffer.position(buffer.position() + size * Constants.SIZEOF_SHORT);

	    off += size;
	    len -= size;
	} while (len != 0);
    }

    @Override
    public void writeArray(short[] ref, int off, int len) throws IOException {
	if (DEBUG && logger.isDebugEnabled()) {
	    logger.debug("writeArray(short[" + off + " ... " + (off + len)
		    + "])");
	}

	do {
	    flush(Constants.SIZEOF_SHORT);
	    int index = buffer.position();

	    int size = Math.min((BUF_SIZE - index) / Constants.SIZEOF_SHORT,
		    len);
	    ShortBuffer s = buffer.asShortBuffer();
	    s.put(ref, off, size);
	    buffer.position(buffer.position() + size * Constants.SIZEOF_SHORT);

	    off += size;
	    len -= size;
	} while (len != 0);
    }

    @Override
    public void writeArray(int[] ref, int off, int len) throws IOException {
	if (DEBUG && logger.isDebugEnabled()) {
	    logger.debug("writeArray(int[" + off + " ... " + (off + len) + "])");
	}

	do {
	    flush(Constants.SIZEOF_INT);
	    int index = buffer.position();
	    IntBuffer i = buffer.asIntBuffer();

	    int size = Math.min((BUF_SIZE - index) / Constants.SIZEOF_INT, len);
	    i.put(ref, off, size);
	    buffer.position(buffer.position() + size * Constants.SIZEOF_INT);

	    off += size;
	    len -= size;
	} while (len != 0);
    }

    @Override
    public void writeArray(long[] ref, int off, int len) throws IOException {
	if (DEBUG && logger.isDebugEnabled()) {
	    logger.debug("writeArray(long[" + off + " ... " + (off + len)
		    + "])");
	}

	do {
	    flush(Constants.SIZEOF_LONG);
	    int index = buffer.position();
	    LongBuffer l = buffer.asLongBuffer();

	    int size = Math
		    .min((BUF_SIZE - index) / Constants.SIZEOF_LONG, len);
	    l.put(ref, off, size);
	    buffer.position(buffer.position() + size * Constants.SIZEOF_LONG);

	    off += size;
	    len -= size;
	} while (len != 0);
    }

    @Override
    public void writeArray(float[] ref, int off, int len) throws IOException {
	if (DEBUG && logger.isDebugEnabled()) {
	    logger.debug("writeArray(float[" + off + " ... " + (off + len)
		    + "])");
	}
	do {
	    flush(Constants.SIZEOF_FLOAT);
	    int index = buffer.position();
	    FloatBuffer f = buffer.asFloatBuffer();

	    int size = Math.min((BUF_SIZE - index) / Constants.SIZEOF_FLOAT,
		    len);
	    f.put(ref, off, size);
	    buffer.position(buffer.position() + size * Constants.SIZEOF_FLOAT);

	    off += size;
	    len -= size;
	} while (len != 0);
    }

    @Override
    public void writeArray(double[] ref, int off, int len) throws IOException {
	if (DEBUG && logger.isDebugEnabled()) {
	    logger.debug("writeArray(double[" + off + " ... " + (off + len)
		    + "])");
	}

	do {
	    flush(Constants.SIZEOF_DOUBLE);
	    int index = buffer.position();
	    DoubleBuffer d = buffer.asDoubleBuffer();

	    int size = Math.min((BUF_SIZE - index) / Constants.SIZEOF_DOUBLE,
		    len);
	    d.put(ref, off, size);
	    buffer.position(buffer.position() + size * Constants.SIZEOF_DOUBLE);

	    off += size;
	    len -= size;
	} while (len != 0);
    }

    @Override
    public void flush() throws IOException {
	flush(BUF_SIZE + 1); /* Forces flush */
    }

    @Override
    public void finish() {
	// empty
    }

    @Override
    public boolean finished() {
	return true;
    }

    @Override
    public void close() throws IOException {
	flush();
	out.close();
    }

    public void clear() throws IOException {
	flush();
	buffer = null;
    }

    @Override
    public int bufferSize() {
	return BUF_SIZE;
    }

    @Override
    public void writeByteBuffer(ByteBuffer value) throws IOException {
	int len = value.limit() - value.position();
	if (value.isDirect()) {
	    flush();
	    out.write(value);
	    return;
	}
	writeArray(value.array(), value.arrayOffset() + value.position(),
		value.arrayOffset() + value.limit());
    }
}
