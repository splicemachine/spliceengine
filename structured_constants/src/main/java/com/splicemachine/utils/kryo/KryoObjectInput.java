package com.splicemachine.utils.kryo;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;

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
//        System.out.println("readObject:Object");
        return kryo.readClassAndObject(input);
    }

    @Override
    public int read() throws IOException {
//        System.out.println("read:int");
        return input.readByte();
    }

    @Override
    public int read(byte[] b) throws IOException {
//        System.out.println("read(byte[]):int");
        return input.read(b);
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
//        System.out.println("read(byte[],off,len):int");
        return input.read(b,off,len);
    }

    @Override
    public long skip(long n) throws IOException {
//        System.out.println("skip:long");
        return input.skip(n);
    }

    @Override
    public int available() throws IOException {
//        System.out.println("available:int");
        return input.available();
    }

    @Override
    public void close() throws IOException {
        input.close();
    }

    @Override
    public void readFully(byte[] b) throws IOException {
//        System.out.println("readFully(byte[])");
        input.readBytes(b);
    }

    @Override
    public void readFully(byte[] b, int off, int len) throws IOException {
//        System.out.println("readFully(byte[],off,len)");
        input.readBytes(b,off,len);
    }

    @Override
    public int skipBytes(int n) throws IOException {
//        System.out.println("skipBytes(int):int");
        int pos = input.position();
        input.skip(n);
        int newPos = input.position();
        return newPos-pos;
    }

    @Override
    public boolean readBoolean() throws IOException {
//        System.out.println("readBoolean:boolean");
        return input.readBoolean();
    }

    @Override
    public byte readByte() throws IOException {
//        System.out.println("readByte:byte");
        return input.readByte();
    }

    @Override
    public int readUnsignedByte() throws IOException {
//        System.out.println("readUnsignedByte:int");
        return input.readByteUnsigned();
    }

    @Override
    public short readShort() throws IOException {
//        System.out.println("readShort:short");
        return input.readShort();
    }

    @Override
    public int readUnsignedShort() throws IOException {
//        System.out.println("readUnsignedShort:int");
        return input.readShortUnsigned();
    }

    @Override
    public char readChar() throws IOException {
//        System.out.println("readChar:char");
        return input.readChar();
    }

    @Override
    public int readInt() throws IOException {
//        System.out.println("readInt:int");
        return input.readInt();
    }

    @Override
    public long readLong() throws IOException {
//        System.out.println("readLong:long");
        return input.readLong();
    }

    @Override
    public float readFloat() throws IOException {
//        System.out.println("readFloat:float");
        return input.readFloat();
    }

    @Override
    public double readDouble() throws IOException {
//        System.out.println("readDouble:double");
        return input.readDouble();
    }

    @Override
    public String readLine() throws IOException {
//        System.out.println("readLine:String");
        return input.readString();
    }

    @Override
    public String readUTF() throws IOException {
//        System.out.println("readUTF:String");
        return input.readString();
    }
}
