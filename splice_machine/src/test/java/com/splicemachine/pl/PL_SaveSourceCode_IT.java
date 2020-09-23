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

package com.splicemachine.pl;

import com.splicemachine.derby.test.framework.SpliceSchemaWatcher;
import com.splicemachine.derby.test.framework.SpliceWatcher;
import com.splicemachine.derby.test.framework.TestConnection;
import com.splicemachine.test.SerialTest;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.Ignore;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import splice.com.google.common.collect.Lists;

import javax.sql.rowset.serial.SerialBlob;
import java.sql.Blob;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.util.Collection;
import java.util.Properties;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

/**
 * Tests saving/deleting code objects from SYSSOURCECODE table.
 */
@Category(value = {SerialTest.class})
@RunWith(Parameterized.class)
public class PL_SaveSourceCode_IT {

    private static final String SCHEMA = PL_SaveSourceCode_IT.class.getSimpleName();

    @ClassRule
    public static SpliceSchemaWatcher spliceSchemaWatcher = new SpliceSchemaWatcher(SCHEMA);

    @Rule
    public SpliceWatcher methodWatcher = new SpliceWatcher(SCHEMA);

    @Parameterized.Parameters
    public static Collection<Object[]> data() {
        Collection<Object[]> params = Lists.newArrayListWithCapacity(2);
        params.add(new Object[]{"jdbc:splice://localhost:1527/splicedb;user=splice;password=admin"});
        params.add(new Object[]{"jdbc:splice://localhost:1527/splicedb;user=splice;password=admin;useSpark=true"});
        return params;
    }

    private String connectionString;

    public PL_SaveSourceCode_IT(String connecitonString) {
        this.connectionString = connecitonString;
    }

    @Before
    public void createTables() throws Exception {
        Connection conn = new TestConnection(DriverManager.getConnection(connectionString, new Properties()));
        conn.setSchema(SCHEMA.toUpperCase());
        methodWatcher.setConnection(conn);
    }

    @Test
    @Ignore
    public void saveSourceCode() throws Exception {
        // no source code has been saved yet, so following query should return 0 rows
        assertEquals(0, methodWatcher.queryList("SELECT SOURCE_CODE FROM SYS.SYSSOURCECODE").size());

        // save some source code.
        String SCHEMA_NAME = "not_necessarily_a_valid_db_schema";
        String OBJECT_NAME = "identity";
        String OBJECT_TYPE = "SpliceSQLFunction";
        String OBJECT_FORM = "SpliceSQL:1.0@UTF8";
        String SOURCE_CODE = "select ? from sysibm.sysdummy1";
        String DEFINER_NAME = "not_necessarily_a_valid_db_user";
        try (CallableStatement cs = methodWatcher.prepareCall("CALL SYSCS_UTIL.SYSCS_SAVE_SOURCECODE(?,?,?,?,?,?)")) {
            cs.setString(1, SCHEMA_NAME);
            cs.setString(2, OBJECT_NAME);
            cs.setString(3, OBJECT_TYPE);
            cs.setString(4, OBJECT_FORM);
            cs.setString(5, DEFINER_NAME);
            cs.setBlob(6, new SerialBlob(SOURCE_CODE.getBytes()));
            cs.execute();
        }

        // that should have created a row, which we find with the following query
        assertEquals(DEFINER_NAME, methodWatcher.query("SELECT DEFINER_NAME FROM SYS.SYSSOURCECODE"));

        // that same row should contain the same sourcecode that we put into it.
        try (ResultSet rs = methodWatcher.executeQuery("SELECT SOURCE_CODE FROM SYS.SYSSOURCECODE")) {
            rs.next();
            Blob blob = rs.getBlob(1);
            assertEquals(SOURCE_CODE, new String(blob.getBytes(1, (int) blob.length())));
            assertFalse(rs.next());
        }

        // if we call the stored procedure again with null for source code, it should delete the row.
        try (CallableStatement cs = methodWatcher.prepareCall("CALL SYSCS_UTIL.SYSCS_SAVE_SOURCECODE(?,?,?,?,?,?)")) {
            cs.setString(1, SCHEMA_NAME);
            cs.setString(2, OBJECT_NAME);
            cs.setString(3, OBJECT_TYPE);
            cs.setString(4, OBJECT_FORM);
            cs.setString(5, "ignored because next parameter is null");
            cs.setObject(6, null);
            cs.execute();
        }

        // assert that the row has been deleted.
        assertEquals(0, methodWatcher.queryList("SELECT DEFINER_NAME FROM SYS.SYSSOURCECODE").size());
    }
}
