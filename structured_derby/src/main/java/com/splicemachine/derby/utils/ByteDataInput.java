package com.splicemachine.derby.utils;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectInputStream;

/**
 * @author Scott Fines
 *         Created on: 4/9/13
 */
public class ByteDataInput implements ObjectInput {
    private final ByteArrayInputStream bais;
    private final ObjectInput wrapper;

    public ByteDataInput(byte[] data) {
        this.bais = new ByteArrayInputStream(data);
        try {
            this.wrapper = new ObjectInputStream(bais);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public void reset() {
        bais.reset();
    }

    @Override
    public Object readObject() throws ClassNotFoundException, IOException {
        return wrapper.readObject();
    }

    @Override
    public int read() throws IOException {
        return wrapper.read();
    }

    @Override
    public int read(byte[] b) throws IOException {
        return wrapper.read(b);
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        return wrapper.read(b, off, len);
    }

    @Override
    public long skip(long n) throws IOException {
        return wrapper.skip(n);
    }

    @Override
    public int available() throws IOException {
        return wrapper.available();
    }

    @Override
    public void close() throws IOException {
        wrapper.close();
    }

    @Override
    public void readFully(byte[] b) throws IOException {
        wrapper.readFully(b);
    }

    @Override
    public void readFully(byte[] b, int off, int len) throws IOException {
        wrapper.readFully(b, off, len);
    }

    @Override
    public int skipBytes(int n) throws IOException {
        return wrapper.skipBytes(n);
    }

    @Override
    public boolean readBoolean() throws IOException {
        return wrapper.readBoolean();
    }

    @Override
    public byte readByte() throws IOException {
        return wrapper.readByte();
    }

    @Override
    public int readUnsignedByte() throws IOException {
        return wrapper.readUnsignedByte();
    }

    @Override
    public short readShort() throws IOException {
        return wrapper.readShort();
    }

    @Override
    public int readUnsignedShort() throws IOException {
        return wrapper.readUnsignedShort();
    }

    @Override
    public char readChar() throws IOException {
        return wrapper.readChar();
    }

    @Override
    public int readInt() throws IOException {
        return wrapper.readInt();
    }

    @Override
    public long readLong() throws IOException {
        return wrapper.readLong();
    }

    @Override
    public float readFloat() throws IOException {
        return wrapper.readFloat();
    }

    @Override
    public double readDouble() throws IOException {
        return wrapper.readDouble();
    }

    @Override
    public String readLine() throws IOException {
        return wrapper.readLine();
    }

    @Override
    public String readUTF() throws IOException {
        return wrapper.readUTF();
    }
}
