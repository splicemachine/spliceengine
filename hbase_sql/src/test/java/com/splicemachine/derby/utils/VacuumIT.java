/*
 * Copyright (c) 2012 - 2017 Splice Machine, Inc.
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

package com.splicemachine.derby.utils;

import com.splicemachine.db.iapi.sql.dictionary.DataDictionary;
import com.splicemachine.derby.test.framework.SpliceSchemaWatcher;
import com.splicemachine.derby.test.framework.SpliceTableWatcher;
import com.splicemachine.derby.test.framework.SpliceUnitTest;
import com.splicemachine.derby.test.framework.SpliceWatcher;
import com.splicemachine.test.SerialTest;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.util.HashSet;
import java.util.Set;

/**
 * @author Scott Fines
 * Date: 3/19/14
 */
@Category(SerialTest.class)
public class VacuumIT extends SpliceUnitTest{
    public static final String CLASS_NAME = VacuumIT.class.getSimpleName().toUpperCase();
    protected static String TABLE = "T";

	private static final SpliceWatcher spliceClassWatcher = new SpliceWatcher();
	@ClassRule
	public static TestRule classRule = spliceClassWatcher;

	@Rule
	public SpliceWatcher methodRule = new SpliceWatcher();

    @Rule
    public SpliceWatcher methodWatcher = new SpliceWatcher();

    protected static SpliceSchemaWatcher spliceSchemaWatcher = new SpliceSchemaWatcher(CLASS_NAME);
    protected static SpliceTableWatcher spliceTableWatcher = new SpliceTableWatcher(TABLE, spliceSchemaWatcher
            .schemaName, "(name varchar(40), title varchar(40), age int)");

    @ClassRule
    public static TestRule chain = RuleChain.outerRule(spliceClassWatcher)
            .around(spliceSchemaWatcher)
            .around(spliceTableWatcher);

	@Test
	public void testVacuumDoesNotBreakStuff() throws Exception {
        Connection connection = spliceClassWatcher.getOrCreateConnection();
        long[] conglomerateNumber = SpliceAdmin.getConglomNumbers(connection, CLASS_NAME, TABLE);
        String conglomerateString = Long.toString(conglomerateNumber[0]);
        try(PreparedStatement ps = methodWatcher.prepareStatement(String.format("drop table %s.%s", CLASS_NAME, TABLE))){
            ps.execute();
        }

        try(HBaseAdmin admin=new HBaseAdmin(new Configuration())){
            Set<String> beforeTables=getConglomerateSet(admin.listTables());
            try(CallableStatement callableStatement=methodRule.prepareCall("call SYSCS_UTIL.VACUUM()")){
                callableStatement.execute();
            }
            Set<String> afterTables=getConglomerateSet(admin.listTables());
            Assert.assertTrue(beforeTables.contains(conglomerateString));
            Assert.assertFalse(afterTables.contains(conglomerateString));
            Set<String> deletedTables=getDeletedTables(beforeTables,afterTables);
            for(String t : deletedTables){
                long conglom=new Long(t);
                Assert.assertTrue(conglom>=DataDictionary.FIRST_USER_TABLE_NUMBER);
            }
        }
    }

    private Set<String> getConglomerateSet(HTableDescriptor[] tableDescriptors) {
        Set<String> conglomerates = new HashSet<>();
        for (HTableDescriptor descriptor:tableDescriptors) {
            String tableName = descriptor.getNameAsString();
            String[] s = tableName.split(":");
            if (s.length < 2) continue;
            conglomerates.add(s[1]);
        }

        return conglomerates;
    }

    private Set<String> getDeletedTables(Set<String> beforeTables, Set<String> afterTables) {
        for (String t : afterTables) {
            beforeTables.remove(t);
        }
        return beforeTables;
    }
}
