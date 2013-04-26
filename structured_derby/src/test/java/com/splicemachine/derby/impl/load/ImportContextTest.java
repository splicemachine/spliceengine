package com.splicemachine.derby.impl.load;

import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

public class ImportContextTest {

    private byte[] serializeToBytes(Externalizable e) throws IOException{

        ByteArrayOutputStream bos = null;
        ObjectOutputStream oos = null;
        byte[] object;

        try{
            bos = new ByteArrayOutputStream();
            oos = new ObjectOutputStream(bos);
            oos.writeObject(e);
            oos.flush();

            object = bos.toByteArray();
        } finally {
            if(oos != null){
                oos.close();
            }

            if(bos != null){
                bos.close();
            }
        }

        return object;
    }

    private Object deserializeObject(byte[] b) throws IOException, ClassNotFoundException {
        ByteArrayInputStream bis = null;
        ObjectInputStream ois = null;
        try{

            bis = new ByteArrayInputStream(b);
            ois = new ObjectInputStream(bis);

            return ois.readObject();

        } finally {
            if(bis != null){
                bis.close();
            }

            if(ois != null){
                ois.close();
            }
        }

    }


    private <T extends Externalizable> T roundTripObject(T obj) throws IOException, ClassNotFoundException {
        return (T) deserializeObject( serializeToBytes(obj));
    }

    @Test
    public void testSerializeImportContext() throws Exception{
        ImportContext.Builder builder = new ImportContext.Builder();
        ImportContext ic = builder.path("/foo")
                                .colDelimiter(",")
                                .destinationTable(1l)
                                .transactionId("1")
                                .byteOffset(100l)
                                .bytesToRead(10)
                                .build();

        ImportContext newIC = roundTripObject(ic);

        Assert.assertEquals("/foo", newIC.getFilePath().toString());
        Assert.assertEquals(",", newIC.getColumnDelimiter());
        Assert.assertEquals(1l, newIC.getTableId());
        Assert.assertEquals("1", newIC.getTransactionId());
        Assert.assertEquals(100l, newIC.getByteOffset());
        Assert.assertEquals(10, newIC.getBytesToRead());
    }

    @Test( expected = NullPointerException.class )
    public void testMissingPath() throws Exception{
        ImportContext.Builder builder = new ImportContext.Builder();
        ImportContext ic = builder.colDelimiter(",")
                .destinationTable(1l)
                .transactionId("1")
                .byteOffset(100l)
                .bytesToRead(10)
                .build();
    }

    @Test( expected = NullPointerException.class )
    public void testMissingDelimiter() throws Exception{
        ImportContext.Builder builder = new ImportContext.Builder();
        ImportContext ic = builder.path("/foo")
                .destinationTable(1l)
                .transactionId("1")
                .byteOffset(100l)
                .bytesToRead(10)
                .build();
    }

    @Test( expected = NullPointerException.class )
    public void testMissingDestinationTable() throws Exception{
        ImportContext.Builder builder = new ImportContext.Builder();
        ImportContext ic = builder.path("/foo")
                .colDelimiter(",")
                .transactionId("1")
                .byteOffset(100l)
                .bytesToRead(10)
                .build();
    }

    @Test( expected = NullPointerException.class )
    public void testMissingTransactionId() throws Exception{
        ImportContext.Builder builder = new ImportContext.Builder();
        ImportContext ic = builder.path("/foo")
                .colDelimiter(",")
                .destinationTable(1l)
                .byteOffset(100l)
                .bytesToRead(10)
                .build();
    }
}
