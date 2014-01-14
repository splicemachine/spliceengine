package com.splicemachine.utils.kryo;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.UnsafeInput;
import java.io.IOException;
import java.io.ObjectInput;

/**
 * @author Scott Fines
 *         Created on: 8/15/13
 */
public class KryoObjectInput implements ObjectInput {
    private final Input input;
    private final Kryo kryo;
    
    public KryoObjectInput(Input input, Kryo kryo) {
        this.input = input;
        this.kryo = kryo;
    }

    @Override
    public Object readObject() throws ClassNotFoundException, IOException {
        return kryo.readClassAndObject(input);
    }

    @Override
    public int read() throws IOException {
        return input.readByte();
    }

    @Override
    public int read(byte[] b) throws IOException {
        return input.read(b);
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        return input.read(b,off,len);
    }

    @Override
    public long skip(long n) throws IOException {
        return input.skip(n);
    }

    @Override
    public int available() throws IOException {
        return input.available();
    }

    @Override
    public void close() throws IOException {
        input.close();
    }

    @Override
    public void readFully(byte[] b) throws IOException {
        input.readBytes(b);
    }

    @Override
    public void readFully(byte[] b, int off, int len) throws IOException {
        input.readBytes(b,off,len);
    }

    @Override
    public int skipBytes(int n) throws IOException {
        int pos = input.position();
        input.skip(n);
        int newPos = input.position();
        return newPos-pos;
    }

    @Override
    public boolean readBoolean() throws IOException {
        return input.readBoolean();
    }

    @Override
    public byte readByte() throws IOException {
        return input.readByte();
    }

    @Override
    public int readUnsignedByte() throws IOException {
        return input.readByteUnsigned();
    }

    @Override
    public short readShort() throws IOException {
        return input.readShort();
    }

    @Override
    public int readUnsignedShort() throws IOException {
        return input.readShortUnsigned();
    }

    @Override
    public char readChar() throws IOException {
        return input.readChar();
    }

    @Override
    public int readInt() throws IOException {
        return input.readInt();
    }

    @Override
    public long readLong() throws IOException {
        return input.readLong();
    }

    @Override
    public float readFloat() throws IOException {
        return input.readFloat();
    }

    @Override
    public double readDouble() throws IOException {
        return input.readDouble();
    }

    @Override
    public String readLine() throws IOException {
        return input.readString();
    }

    @Override
    public String readUTF() throws IOException {
        return input.readString();
    }
}
