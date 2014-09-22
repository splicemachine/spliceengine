package com.splicemachine.derby.impl.sql.execute.operations.export;

import com.google.common.base.Charsets;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class ExportParamsTest {

    @Test
    public void constructor() {

        ExportParams exportParams = new ExportParams("/dir", "local", 42, "ascii", "F", "Q");

        assertEquals("/dir", exportParams.getDirectory());
        assertEquals(ExportFileSystemType.LOCAL, exportParams.getFileSystemType());
        assertEquals(42, exportParams.getReplicationCount());
        assertEquals("ascii", exportParams.getCharacterEncoding());
        assertEquals('F', exportParams.getFieldDelimiter());
        assertEquals('Q', exportParams.getQuoteChar());
    }

    @Test
    public void constructor_testDefaults() {

        ExportParams exportParams = new ExportParams("/dir", null, -1, null, null, null);

        assertEquals("/dir", exportParams.getDirectory());
        assertEquals(ExportFileSystemType.HDFS, exportParams.getFileSystemType());
        assertEquals(1, exportParams.getReplicationCount());
        assertEquals(Charsets.UTF_8.name(), exportParams.getCharacterEncoding());
        assertEquals(',', exportParams.getFieldDelimiter());
        assertEquals('"', exportParams.getQuoteChar());
    }

}