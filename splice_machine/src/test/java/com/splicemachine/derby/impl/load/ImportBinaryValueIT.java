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
