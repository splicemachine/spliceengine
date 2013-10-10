package com.splicemachine.utils.kryo;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Output;

import java.io.IOException;
import java.io.ObjectOutput;

/**
 * @author Scott Fines
 *         Created on: 8/15/13
 */
public class KryoObjectOutput implements ObjectOutput {
    private final Output output;
    private final Kryo kryo;

    public KryoObjectOutput(Output output, Kryo kryo) {
        this.output = output;
        this.kryo = kryo;
    }

    @Override
    public void writeObject(Object obj) throws IOException {
//        System.out.println("writeObject "+ obj);
        kryo.writeClassAndObject(output,obj);
    }

    @Override
    public void write(int b) throws IOException {
//        System.out.println("write(int) "+ b);
        output.writeInt(b);
    }

    @Override
    public void write(byte[] b) throws IOException {
//        System.out.println("write(byte[]) "+ b);
        output.write(b);
    }

    @Override
    public void write(byte[] b, int off, int len) throws IOException {
//        System.out.println("write(byte[],off,len) "+ b+","+off+","+len);
        output.write(b,off,len);
    }

    @Override
    public void writeBoolean(boolean v) throws IOException {
//        System.out.println("writeBoolean "+ v);
        output.writeBoolean(v);
    }

    @Override
    public void writeByte(int v) throws IOException {
//        System.out.println("writeByte "+ v);
        output.writeByte(v);
    }

    @Override
    public void writeShort(int v) throws IOException {
//        System.out.println("writeShort "+ v);
        output.writeShort(v);
    }

    @Override
    public void writeChar(int v) throws IOException {
//        System.out.println("writeChar "+ v);
        output.writeChar((char)v);
    }

    @Override
    public void writeInt(int v) throws IOException {
//        System.out.println("writeInt "+ v);
        output.writeInt(v);
    }

    @Override
    public void writeLong(long v) throws IOException {
//        System.out.println("writeLong "+ v);
        output.writeLong(v);
    }

    @Override
    public void writeFloat(float v) throws IOException {
//        System.out.println("writeFloat "+ v);
        output.writeFloat(v);
    }

    @Override
    public void writeDouble(double v) throws IOException {
//        System.out.println("writeDouble "+ v);
        output.writeDouble(v);
    }

    @Override
    public void writeBytes(String s) throws IOException {
//        System.out.println("writeBytes(String) "+ s);
        output.writeString(s);
    }

    @Override
    public void writeChars(String s) throws IOException {
//        System.out.println("writeChars(String) "+ s);
        output.writeString(s);
    }

    @Override
    public void writeUTF(String s) throws IOException {
//        System.out.println("writeUTF(String) "+ s);
        output.writeString(s);
    }

    @Override
    public void flush() throws IOException {
//        System.out.println("flush");
        output.flush();
    }

    @Override
    public void close() throws IOException {
        output.close();
    }
}
