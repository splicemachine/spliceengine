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

    @Test
    public void constructor_badExportDirectory() {
        try {
            new ExportParams(null, "local", 1, "UTF-8", ",", null);
        } catch (Exception e) {
            assertEquals(" export directory is required", e.getMessage());
        }
    }

    @Test
    public void constructor_badFileSystemType() {
        try {
            new ExportParams("/dir", "badFileSystemType", 1, "UTF-8", ",", null);
        } catch (Exception e) {
            assertEquals(" invalid file system type 'badFileSystemType', valid values are: HDFS, LOCAL", e.getMessage());
        }
    }

    @Test
    public void constructor_badFieldDelimiter() {
        try {
            new ExportParams("/dir", "LOCAL", 1, "UTF-8", ",,,", null);
        } catch (Exception e) {
            assertEquals(" field delimiter must be a single character", e.getMessage());
        }
    }


    @Test
    public void constructor_badQuoteCharacter() {
        try {
            new ExportParams("/dir", "LOCAL", 1, "UTF-8", ",", "||||");
        } catch (Exception e) {
            assertEquals(" quote character must be a single character", e.getMessage());
        }
    }


}