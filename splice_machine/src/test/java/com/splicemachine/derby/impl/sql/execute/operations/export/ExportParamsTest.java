/*
 * Copyright (c) 2012 - 2020 Splice Machine, Inc.
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

package com.splicemachine.derby.impl.sql.execute.operations.export;

import splice.com.google.common.base.Charsets;
import com.splicemachine.db.iapi.error.StandardException;
import org.junit.Test;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class ExportParamsTest {

    @Test
    public void constructor() throws StandardException {

        ExportParams exportParams = new ExportParams("/dir", null, "csv", 42, "ascii", "F", "Q");

        assertEquals("/dir", exportParams.getDirectory());
        assertEquals(ExportFile.COMPRESSION.NONE, exportParams.getCompression());
        assertEquals(42, exportParams.getReplicationCount());
        assertEquals("ascii", exportParams.getCharacterEncoding());
        assertEquals('F', exportParams.getFieldDelimiter());
        assertEquals('Q', exportParams.getQuoteChar());
    }

    @Test
    public void constructor_testDefaults() throws StandardException {

        ExportParams exportParams = new ExportParams("/dir", "BZ2", "csv", -1, null, null, null);

        assertEquals("/dir", exportParams.getDirectory());
        assertEquals(ExportFile.COMPRESSION.BZ2, exportParams.getCompression());
        assertEquals(1, exportParams.getReplicationCount());
        assertEquals(Charsets.UTF_8.name(), exportParams.getCharacterEncoding());
        assertEquals(',', exportParams.getFieldDelimiter());
        assertEquals('"', exportParams.getQuoteChar());
    }

    @Test
    public void constructor_whileSpaceDelimitersAreAllowed() throws StandardException {
        ExportParams exportParams = new ExportParams("/dir", "noNe", "csv", -1, null, " ", " ");
        assertEquals(' ', exportParams.getFieldDelimiter());
        assertEquals(' ', exportParams.getQuoteChar());
        assertEquals(ExportFile.COMPRESSION.NONE, exportParams.getCompression());
    }

    @Test
    public void constructor_usingJavaEscapeSequencesToDesignateArbitraryUnicodeCharactersForDelimiters() throws StandardException {

        ExportParams params1 = new ExportParams("/dir", "BZIP2", "csv", -1, null, "\\t", "\\n");
        assertEquals("\t".charAt(0), params1.getFieldDelimiter());
        assertEquals("\n".charAt(0), params1.getQuoteChar());
        assertEquals(ExportFile.COMPRESSION.BZ2, params1.getCompression());

        ExportParams params2 = new ExportParams("/dir", "gZIP ", "csv", -1, null, "\\u0300", "\\u0400");
        assertEquals('\u0300', params2.getFieldDelimiter());
        assertEquals('\u0400', params2.getQuoteChar());
        assertEquals(ExportFile.COMPRESSION.GZ, params2.getCompression());
    }

    @Test
    public void constructor_badExportDirectory() {
        try {
            new ExportParams(null, "GZIP", "csv", 1, "UTF-8", ",", null);
            fail();
        } catch (Exception e) {
            assertEquals("Invalid parameter 'export path'='null'.", e.getMessage());
        }
    }

    @Test
    public void constructor_badCharacterEncoding() {
        try {
            new ExportParams("/dir", "BZ2", "csv", 1, "NON_EXISTING_CHARSET", ",", null);
            fail();
        } catch (StandardException e) {
            assertEquals("Invalid parameter 'encoding'='NON_EXISTING_CHARSET'.", e.getMessage());
        }
    }

    @Test
    public void constructor_badFieldDelimiter() {
        try {
            new ExportParams("/dir", "GZIP", "csv", 1, "UTF-8", ",,,", null);
            fail();
        } catch (Exception e) {
            assertEquals("Invalid parameter 'field delimiter'=',,,'.", e.getMessage());
        }
    }


    @Test
    public void constructor_badQuoteCharacter() {
        try {
            new ExportParams("/dir", "GZ", "csv", 1, "UTF-8", ",", "||||");
            fail();
        } catch (Exception e) {
            assertEquals("Invalid parameter 'quote character'='||||'.", e.getMessage());
        }
    }


}
