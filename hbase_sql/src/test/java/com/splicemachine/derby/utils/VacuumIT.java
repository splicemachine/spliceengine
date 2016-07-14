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
