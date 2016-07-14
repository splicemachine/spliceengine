/*
 * Copyright 2012 - 2016 Splice Machine, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
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
        assertEquals("\u0001", HdfsImport.unescape("^A"));
        assertEquals("\u0001", HdfsImport.unescape("^a"));

        assertEquals("\b", HdfsImport.unescape("\\b"));
        assertEquals("\b", HdfsImport.unescape("\b"));
//        assertEquals("\b", HdfsImport.unescape("\\u0008"));

        assertEquals("\t", HdfsImport.unescape("\\t"));
        assertEquals("\t", HdfsImport.unescape("\t"));
//        assertEquals("\t", HdfsImport.unescape("\\u0009"));

        assertEquals("\f", HdfsImport.unescape("\\f"));
        assertEquals("\f", HdfsImport.unescape("\f"));
//        assertEquals("\f", HdfsImport.unescape("\\u000C"));

        assertEquals("\n", HdfsImport.unescape("\\n"));
        assertEquals("\n", HdfsImport.unescape("\n"));
        assertEquals("\n", HdfsImport.unescape("^M"));
        assertEquals("\n", HdfsImport.unescape("^m"));
//        assertEquals("\n", HdfsImport.unescape("\\u000A"));

        assertEquals("\r", HdfsImport.unescape("\\r"));
        assertEquals("\r", HdfsImport.unescape("\r"));
//        assertEquals("\r", HdfsImport.unescape("\\u000D"));

//        assertEquals("\0", HdfsImport.unescape("\\u0000"));
        assertEquals("\"", HdfsImport.unescape("\\\""));
        assertEquals("\"", HdfsImport.unescape("\""));
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
