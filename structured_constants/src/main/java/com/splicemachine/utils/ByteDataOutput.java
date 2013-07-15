package com.splicemachine.utils;

import java.io.*;

/**
 * @author Scott Fines
 *         Created on: 4/5/13
 */
public class ByteDataOutput implements ObjectOutput,Closeable {
    private final ByteArrayOutputStream baos;
    private final ObjectOutput dataOutput;

    public ByteDataOutput() {
        baos = new ByteArrayOutputStream();
        try {
            dataOutput = new ObjectOutputStream(baos);
        } catch (IOException e) {
            //should never happen
            throw new RuntimeException(e);
        }
    }

    public byte[] toByteArray() throws IOException{
        dataOutput.flush();
        return baos.toByteArray();
    }

    public void reset() throws IOException{
        dataOutput.flush();
        baos.reset();
    }

    @Override
    public void writeObject(Object obj) throws IOException {
        dataOutput.writeObject(obj);
    }

    @Override
    public void write(int b) throws IOException {
        dataOutput.write(b);
    }

    @Override
    public void write(byte[] b) throws IOException {
        dataOutput.write(b);
    }

    @Override
    public void write(byte[] b, int off, int len) throws IOException {
        dataOutput.write(b, off, len);
    }

    @Override
    public void flush() throws IOException {
        dataOutput.flush();
    }

    @Override
    public void close() throws IOException {
        dataOutput.close();
    }

    @Override
    public void writeBoolean(boolean v) throws IOException {
        dataOutput.writeBoolean(v);
    }

    @Override
    public void writeByte(int v) throws IOException {
        dataOutput.writeByte(v);
    }

    @Override
    public void writeShort(int v) throws IOException {
        dataOutput.writeShort(v);
    }

    @Override
    public void writeChar(int v) throws IOException {
        dataOutput.writeChar(v);
    }

    @Override
    public void writeInt(int v) throws IOException {
        dataOutput.writeInt(v);
    }

    @Override
    public void writeLong(long v) throws IOException {
        dataOutput.writeLong(v);
    }

    @Override
    public void writeFloat(float v) throws IOException {
        dataOutput.writeFloat(v);
    }

    @Override
    public void writeDouble(double v) throws IOException {
        dataOutput.writeDouble(v);
    }

    @Override
    public void writeBytes(String s) throws IOException {
        dataOutput.writeBytes(s);
    }

    @Override
    public void writeChars(String s) throws IOException {
        dataOutput.writeChars(s);
    }

    @Override
    public void writeUTF(String s) throws IOException {
        dataOutput.writeUTF(s);
    }
}
