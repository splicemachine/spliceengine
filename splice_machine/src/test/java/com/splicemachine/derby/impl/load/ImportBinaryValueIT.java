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

import com.splicemachine.derby.test.framework.SpliceSchemaWatcher;
import com.splicemachine.derby.test.framework.SpliceTableWatcher;
import com.splicemachine.derby.test.framework.SpliceUnitTest;
import com.splicemachine.derby.test.framework.SpliceWatcher;
import org.junit.*;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.PreparedStatement;

/**
 *
 * Created by akorotenko on 1/22/16.
 */
public class ImportBinaryValueIT extends SpliceUnitTest {

    protected static SpliceWatcher spliceClassWatcher = new SpliceWatcher();
    public static final String CLASS_NAME = ImportBinaryValueIT.class.getSimpleName().toUpperCase();
    protected static SpliceSchemaWatcher schema = new SpliceSchemaWatcher(CLASS_NAME);

    protected static SpliceTableWatcher blobTale = new SpliceTableWatcher("BLOB_TABLE", schema.schemaName, "(VAL1 BLOB)");

    @ClassRule
    public static TestRule chain = RuleChain.outerRule(spliceClassWatcher)
            .around(schema)
            .around(blobTale);

    @Rule public SpliceWatcher methodWatcher = new SpliceWatcher();

    private static File BADDIR;

    @BeforeClass
    public static void beforeClass() throws Exception {
        BADDIR = SpliceUnitTest.createBadLogDirectory(schema.schemaName);
    }

    @Test
    public void testCannotInsertValueOutOfConstraint() throws Exception{

        String table = blobTale.tableName;
        String file = "field_with_blob.csv";

        String inputFilePath = getResourceDirectory()+"import/"+file;
        Connection conn = methodWatcher.getOrCreateConnection();
        try(PreparedStatement ps = conn.prepareStatement(format("call SYSCS_UTIL.IMPORT_DATA(" +
                        "'%s'," +  // schema name
                        "'%s'," +  // table name
                        "null," +  // insert column list
                        "'%s'," +  // file path
                        "','," +   // column delimiter
                        "null," +  // character delimiter
                        "null," +  // timestamp format
                        "null," +  // date format
                        "null," +  // time format
                        "%d," +    // max bad records
                        "'%s'," +  // bad record dir
                        "'true'," +  // has one line records
                        "null)",   // char set
                schema.schemaName, table, inputFilePath,
                0, BADDIR.getCanonicalPath()))){
            ps.execute();
        }

        Path badFile = Paths.get(BADDIR.getCanonicalPath() + "/" + file + ".bad");

        if (Files.exists(badFile)) {
            Assert.fail("Exception was thrown! See log for more information:" + badFile);
        }
    }
}
