package com.splicemachine.derby.impl.load;

import com.splicemachine.homeless.SerializationUtils;
import org.junit.Assert;
import org.junit.Test;

public class ImportContextTest {

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

        ImportContext newIC = SerializationUtils.roundTripObject(ic);

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
