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

package com.splicemachine.derby.impl.sql.execute.operations;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.apache.log4j.Logger;
import org.junit.*;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;
import org.junit.runner.Description;
import com.splicemachine.derby.test.framework.SpliceDataWatcher;
import com.splicemachine.derby.test.framework.SpliceSchemaWatcher;
import com.splicemachine.derby.test.framework.SpliceTableWatcher;
import com.splicemachine.derby.test.framework.SpliceUnitTest;
import com.splicemachine.derby.test.framework.SpliceWatcher;

public class OnceOperationIT extends SpliceUnitTest { 
	private static Logger LOG = Logger.getLogger(OnceOperationIT.class);
	protected static SpliceWatcher spliceClassWatcher = new SpliceWatcher();
	public static final String CLASS_NAME = OnceOperationIT.class.getSimpleName().toUpperCase();
	public static final String TABLE1_NAME = "A";
    public static final String TABLE2_NAME = "territories";
    public static final String TABLE3_NAME = "employee_territories";
	protected static SpliceSchemaWatcher spliceSchemaWatcher = new SpliceSchemaWatcher(CLASS_NAME);	
	protected static SpliceTableWatcher spliceTableWatcher1 = new SpliceTableWatcher(TABLE1_NAME,CLASS_NAME,"(k int, l int)");
    protected static SpliceTableWatcher spliceTableWatcher2 = new SpliceTableWatcher(TABLE2_NAME,CLASS_NAME," (territoryid INTEGER NOT NULL,territory_Description VARCHAR(40) NOT NULL,regionid int NOT NULL, primary key (territoryid))");
    protected static SpliceTableWatcher spliceTableWatcher3 = new SpliceTableWatcher(TABLE3_NAME,CLASS_NAME,"(employeeid INTEGER NOT NULL,territoryid INTEGER NOT NULL,primary key (employeeid) )");

    @ClassRule
	public static TestRule chain = RuleChain.outerRule(spliceClassWatcher)
		.around(spliceSchemaWatcher)
		.around(spliceTableWatcher1)
        .around(spliceTableWatcher2)
        .around(spliceTableWatcher3)
		.around(new SpliceDataWatcher(){
			@Override
			protected void starting(Description description) {
				try {
                    PreparedStatement statement = spliceClassWatcher.prepareStatement(String.format("insert into %s.%s values (?, ?)",CLASS_NAME,TABLE1_NAME));
                    statement.setInt(1, 1);
                    statement.setInt(2, 2);
                    statement.execute();
                    statement.setInt(1, 3);
                    statement.setInt(2, 4);
                    statement.execute();
                    statement.setInt(1, 3);
                    statement.setInt(2, 4);
                    statement.execute();

                    statement = spliceClassWatcher.prepareStatement(String.format("insert into %s.%s values (95014,'Cupertino',105)",CLASS_NAME,TABLE2_NAME));
                    statement.execute();

                    statement = spliceClassWatcher.prepareStatement(String.format("insert into %s.%s values (7725070,95014)",CLASS_NAME,TABLE3_NAME));
                    statement.execute();
				} catch (Exception e) {
					throw new RuntimeException(e);
				}
				finally {
					spliceClassWatcher.closeAll();
				}
			}
			
		});
	
	@Rule public SpliceWatcher methodWatcher = new SpliceWatcher();
		
	@Test
	public void testValuesStatement() throws Exception {
		ResultSet rs = methodWatcher.executeQuery(format("values (select k from %s where k = 1)",this.getTableReference(TABLE1_NAME)));
		rs.next();
		Assert.assertNotNull(rs.getInt(1));
	}

	@Test(expected=SQLException.class)
	public void testValuesStatementNonScalarError() throws Exception{
		try {
			ResultSet rs = methodWatcher.executeQuery(format("values (select k from %s where k = 3)",this.getTableReference(TABLE1_NAME)));
			rs.next();
		} catch (SQLException t) {
            t.printStackTrace();
            Assert.assertEquals("Incorrect SQLState returned","21000",t.getSQLState());
            throw t;
		}
	}

    @Test
    /* test case for DB-1208
     */
    public void testTableWithPrimaryKey() throws Exception {
        ResultSet rs = methodWatcher.executeQuery(format("select employeeid from %s a where territoryid = (select territoryid from %s b where a.territoryid = b.territoryid)", this.getTableReference(TABLE3_NAME), this.getTableReference(TABLE2_NAME)));
        rs.next();
        Assert.assertEquals(rs.getLong(1), 7725070);
    }
}
