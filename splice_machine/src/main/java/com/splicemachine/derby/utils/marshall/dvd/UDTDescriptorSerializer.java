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

package com.splicemachine.derby.utils.marshall.dvd;

import org.spark_project.guava.base.Throwables;
import com.splicemachine.EngineDriver;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.services.io.StoredFormatIds;
import com.splicemachine.db.iapi.services.loader.ClassFactory;
import com.splicemachine.db.iapi.sql.conn.LanguageConnectionContext;
import com.splicemachine.db.iapi.types.DataValueDescriptor;
import com.splicemachine.db.impl.jdbc.EmbedConnection;
import com.splicemachine.db.shared.common.udt.UDTBase;
import com.splicemachine.encoding.Encoding;
import com.splicemachine.encoding.MultiFieldDecoder;
import com.splicemachine.encoding.MultiFieldEncoder;

import java.io.*;

/**
 * Created by jyuan on 10/28/15.
 */
public class UDTDescriptorSerializer implements DescriptorSerializer,Closeable {

    public static final DescriptorSerializer INSTANCE = new UDTDescriptorSerializer();


    private ClassFactory cf;

    public static final Factory INSTANCE_FACTORY = new Factory() {
        @Override public DescriptorSerializer newInstance() { return INSTANCE; }

        @Override public boolean applies(DataValueDescriptor dvd) {
            if (dvd == null)
                return false;

            try {
                Object o = dvd.getObject();
                return (o instanceof UDTBase) && applies(dvd.getTypeFormatId());
            } catch (Exception e) {
                throw new RuntimeException(Throwables.getRootCause(e));
            }
        }

        @Override
        public boolean applies(int typeFormatId) {
            switch(typeFormatId){
                case StoredFormatIds.SQL_USERTYPE_ID_V3:
                    return true;
                default:
                    return false;
            }
        }

        @Override public boolean isScalar() { return false; }
        @Override public boolean isFloat() { return false; }
        @Override public boolean isDouble() { return false; }
    };


    @Override
    public void encode(MultiFieldEncoder fieldEncoder, DataValueDescriptor dvd, boolean desc) throws StandardException {
        Object o = dvd.getObject();

        try {
            ByteArrayOutputStream outputBuffer = new ByteArrayOutputStream();
            ObjectOutputStream objectOutputStream = new ObjectOutputStream(outputBuffer);
            objectOutputStream.writeObject(o);
            objectOutputStream.flush();
            fieldEncoder.encodeNextUnsorted(outputBuffer.toByteArray());
            objectOutputStream.close();
        } catch (IOException e) {
            throw StandardException.newException(e.getLocalizedMessage());
        }
    }


    @Override
    public byte[] encodeDirect(DataValueDescriptor dvd, boolean desc) throws StandardException {
        Object o = dvd.getObject();
        try {
            ByteArrayOutputStream outputBuffer = new ByteArrayOutputStream();
            ObjectOutputStream objectOutputStream = new ObjectOutputStream(outputBuffer);
            objectOutputStream.writeObject(o);
            objectOutputStream.flush();
            byte[] bytes = Encoding.encodeBytesUnsorted(outputBuffer.toByteArray());
            objectOutputStream.close();
            return bytes;
        } catch (IOException e) {
            throw StandardException.newException(e.getLocalizedMessage());
        }
    }

    @Override
    public void decode(MultiFieldDecoder fieldDecoder, DataValueDescriptor destDvd, boolean desc) throws StandardException {

        initializeForRead();
        try {
            ByteArrayInputStream input = new ByteArrayInputStream(fieldDecoder.decodeNextBytesUnsorted());
            UDTInputStream inputStream = new UDTInputStream(input, cf);
            Object o = inputStream.readObject();
            destDvd.setValue(o);
            inputStream.close();
        } catch (Exception e) {
            e.printStackTrace();
            throw StandardException.newException(e.getLocalizedMessage());
        }
    }


    @Override
    public void decodeDirect(DataValueDescriptor dvd, byte[] data, int offset, int length, boolean desc) throws StandardException {
        initializeForRead();
        try {
            ByteArrayInputStream input = new ByteArrayInputStream(Encoding.decodeBytesUnsortd(data, offset, length));
            UDTInputStream inputStream = new UDTInputStream(input, cf);
            Object o = inputStream.readObject();
            dvd.setValue(o);
            inputStream.close();
        } catch (Exception e) {
            throw StandardException.newException(e.getLocalizedMessage());
        }
    }

    @Override public boolean isScalarType() { return false; }
    @Override public boolean isFloatType() { return false; }
    @Override public boolean isDoubleType() { return false; }

    @Override
    public void close() throws IOException {
    }

    private void initializeForRead() throws StandardException {
        if (cf == null) {
            EmbedConnection dbConn = (EmbedConnection) EngineDriver.driver().getInternalConnection();
            LanguageConnectionContext lcc = dbConn.getLanguageConnection();
            cf = lcc.getLanguageConnectionFactory().getClassFactory();
        }
    }
}
