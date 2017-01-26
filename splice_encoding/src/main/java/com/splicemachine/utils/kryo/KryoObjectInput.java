/*
 * Copyright (c) 2012 - 2017 Splice Machine, Inc.
 *
 * This file is part of Splice Machine.
 * Splice Machine is free software: you can redistribute it and/or modify it under the terms of the
 * GNU Affero General Public License as published by the Free Software Foundation, either
 * version 3, or (at your option) any later version.
 * Splice Machine is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU Affero General Public License for more details.
 * You should have received a copy of the GNU Affero General Public License along with Splice Machine.
 * If not, see <http://www.gnu.org/licenses/>.
 */

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
