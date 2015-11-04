package com.splicemachine.derby.utils.marshall.dvd;

import com.google.common.base.Throwables;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.services.io.StoredFormatIds;
import com.splicemachine.db.iapi.services.loader.ClassFactory;
import com.splicemachine.db.iapi.sql.conn.LanguageConnectionContext;
import com.splicemachine.db.iapi.types.DataValueDescriptor;
import com.splicemachine.db.impl.jdbc.EmbedConnection;
import com.splicemachine.db.shared.common.udt.UDTBase;
import com.splicemachine.derby.hbase.SpliceDriver;
import com.splicemachine.encoding.Encoding;
import com.splicemachine.encoding.MultiFieldDecoder;
import com.splicemachine.encoding.MultiFieldEncoder;

import java.io.*;

/**
 * Created by jyuan on 10/28/15.
 */
public class UDTDescriptorSerializer implements DescriptorSerializer,Closeable {

    public static final DescriptorSerializer INSTANCE = new UDTDescriptorSerializer();

    private ByteArrayOutputStream outputBuffer;
    private ObjectOutputStream objectOutputStream;
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
        initializeForWrite();
        Object o = dvd.getObject();

        try {
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
        initializeForWrite();
        Object o = dvd.getObject();
        try {
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



    private void initializeForWrite() throws StandardException {
        try {
            outputBuffer = new ByteArrayOutputStream();
            objectOutputStream = new ObjectOutputStream(outputBuffer);
        }catch (IOException e) {
            throw StandardException.newException(e.getLocalizedMessage());
        }
    }

    private void initializeForRead() throws StandardException {
        if (cf == null) {
            EmbedConnection dbConn = (EmbedConnection) SpliceDriver.driver().getInternalConnection();
            LanguageConnectionContext lcc = dbConn.getLanguageConnection();
            cf = lcc.getLanguageConnectionFactory().getClassFactory();
        }
    }
}
