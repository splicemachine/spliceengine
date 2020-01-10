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

package com.splicemachine.derby.impl.load;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

/**
 * Test import column and field separator conversion.
 */
public class HdfsImportCharacterTest {

    @Test
    public void testUnescape() throws Exception {
        // Not supporting unicode yet
        assertEquals("\u0001", ImportUtils.unescape("^A"));
        assertEquals("\u0001", ImportUtils.unescape("^a"));

        assertEquals("\b", ImportUtils.unescape("\\b"));
        assertEquals("\b", ImportUtils.unescape("\b"));
//        assertEquals("\b", ImportUtils.unescape("\\u0008"));

        assertEquals("\t", ImportUtils.unescape("\\t"));
        assertEquals("\t", ImportUtils.unescape("\t"));
//        assertEquals("\t", ImportUtils.unescape("\\u0009"));

        assertEquals("\f", ImportUtils.unescape("\\f"));
        assertEquals("\f", ImportUtils.unescape("\f"));
//        assertEquals("\f", ImportUtils.unescape("\\u000C"));

        assertEquals("\n", ImportUtils.unescape("\\n"));
        assertEquals("\n", ImportUtils.unescape("\n"));
        assertEquals("\n", ImportUtils.unescape("^M"));
        assertEquals("\n", ImportUtils.unescape("^m"));
//        assertEquals("\n", ImportUtils.unescape("\\u000A"));

        assertEquals("\r", ImportUtils.unescape("\\r"));
        assertEquals("\r", ImportUtils.unescape("\r"));
//        assertEquals("\r", ImportUtils.unescape("\\u000D"));

//        assertEquals("\0", ImportUtils.unescape("\\u0000"));
        assertEquals("\"", ImportUtils.unescape("\\\""));
        assertEquals("\"", ImportUtils.unescape("\""));
    }

    @Test
    public void testQuotedInsertColumnList() throws Exception {
        assertEquals("[NULL]", HdfsImport.normalizeIdentifierList("null").toString());
        assertEquals("[COL1, TWO]", HdfsImport.normalizeIdentifierList("col1,two").toString());
        assertEquals("[Col1, Col2]", HdfsImport.normalizeIdentifierList("\"Col1\",\"Col2\"").toString());
        assertEquals("[Col,1, Col,2]", HdfsImport.normalizeIdentifierList("\"Col,1\",\"Col,2\"").toString());
        assertEquals("[Col,One, Col,Two]", HdfsImport.normalizeIdentifierList("\"Col,One\",\"Col,Two\"").toString());
    }

}
