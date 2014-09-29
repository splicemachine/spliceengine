package com.splicemachine.derby.impl.sql.execute.operations.export;

import com.google.common.base.Charsets;
import org.apache.derby.iapi.error.StandardException;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class ExportParamsTest {

    @Test
    public void constructor() throws StandardException {

        ExportParams exportParams = new ExportParams("/dir", "local", 42, "ascii", "F", "Q");

        assertEquals("/dir", exportParams.getDirectory());
        assertEquals(ExportFileSystemType.LOCAL, exportParams.getFileSystemType());
        assertEquals(42, exportParams.getReplicationCount());
        assertEquals("ascii", exportParams.getCharacterEncoding());
        assertEquals('F', exportParams.getFieldDelimiter());
        assertEquals('Q', exportParams.getQuoteChar());
    }

    @Test
    public void constructor_testDefaults() throws StandardException {

        ExportParams exportParams = new ExportParams("/dir", null, -1, null, null, null);

        assertEquals("/dir", exportParams.getDirectory());
        assertEquals(ExportFileSystemType.HDFS, exportParams.getFileSystemType());
        assertEquals(1, exportParams.getReplicationCount());
        assertEquals(Charsets.UTF_8.name(), exportParams.getCharacterEncoding());
        assertEquals(',', exportParams.getFieldDelimiter());
        assertEquals('"', exportParams.getQuoteChar());
    }

    @Test
    public void constructor_badExportDirectory() {
        try {
            new ExportParams(null, "local", 1, "UTF-8", ",", null);
            fail();
        } catch (Exception e) {
            assertEquals("Invalid parameter 'export path'='null'.", e.getMessage());
        }
    }

    @Test
    public void constructor_badFileSystemType() {
        try {
            new ExportParams("/dir", "badFileSystemType", 1, "UTF-8", ",", null);
            fail();
        } catch (StandardException e) {
            assertEquals("Invalid parameter 'file system type'='badFileSystemType'.", e.getMessage());
        }
    }

    @Test
    public void constructor_badCharacterEncoding() {
        try {
            new ExportParams("/dir", "local", 1, "NON_EXISTING_CHARSET", ",", null);
            fail();
        } catch (StandardException e) {
            assertEquals("Invalid parameter 'encoding'='NON_EXISTING_CHARSET'.", e.getMessage());
        }
    }

    @Test
    public void constructor_badFieldDelimiter() {
        try {
            new ExportParams("/dir", "LOCAL", 1, "UTF-8", ",,,", null);
            fail();
        } catch (Exception e) {
            assertEquals("Invalid parameter 'field delimiter'=',,,'.", e.getMessage());
        }
    }


    @Test
    public void constructor_badQuoteCharacter() {
        try {
            new ExportParams("/dir", "LOCAL", 1, "UTF-8", ",", "||||");
            fail();
        } catch (Exception e) {
            assertEquals("Invalid parameter 'quote character'='||||'.", e.getMessage());
        }
    }


}