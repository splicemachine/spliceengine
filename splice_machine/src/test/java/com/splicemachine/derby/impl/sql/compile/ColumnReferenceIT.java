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

package com.splicemachine.derby.impl.sql.compile;

import com.splicemachine.derby.test.framework.SpliceSchemaWatcher;
import com.splicemachine.derby.test.framework.SpliceWatcher;
import junit.framework.Assert;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;

import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * Created by yifuma on 6/18/15.
 */
public class ColumnReferenceIT {
    private static final String SCHEMA = ColumnReferenceIT.class.getSimpleName();

    @ClassRule
    public static SpliceSchemaWatcher spliceSchemaWatcher = new SpliceSchemaWatcher(SCHEMA);

    @Rule
    public SpliceWatcher methodWatcher = new SpliceWatcher(SCHEMA);

    @Test
    public void testColumnReference() throws Exception{
        try{
            methodWatcher.executeUpdate("create table A (TaskId INT NOT NULL, empId Varchar(3) NOT NULL, StartedAt INT NOT NULL, FinishedAt INT NOT NULL, primary key (TaskId))");
        } catch (SQLException e){
            System.out.println("Table A already exist!");
        }

        try{
            methodWatcher.executeUpdate("create table B (TaskId INT NOT NULL references A(TaskId), empId Varchar(3) NOT NULL, StartedAt INT NOT NULL, FinishedAt INT NOT NULL, primary key (TaskId))");
        } catch (SQLException e){
            System.out.println("Table B already exist!");
        }


        String query = "select t.tablename from --Splice-properties joinOrder=fixed\n" +
                "sys.sysschemas s \n" +
                "join sys.systables ti --Splice-properties joinStrategy=NESTEDLOOP\n" +
                "on s.schemaid = ti.schemaid\n" +
                "join sys.SYSCONSTRAINTS ci --Splice-properties joinStrategy=NESTEDLOOP\n" +
                "on ti.tableid = ci.tableid\n" +
                "join sys.sysforeignkeys fk --Splice-properties joinStrategy=SORTMERGE\n" +
                "on FK.KEYCONSTRAINTID = CI.CONSTRAINTID\n" +
                "join sys.SYSCONSTRAINTS c --Splice-properties joinStrategy=NESTEDLOOP\n" +
                "on c.constraintid = fk.constraintid\n" +
                "join sys.systables t --Splice-properties joinStrategy=NESTEDLOOP\n" +
                "on c.tableid = t.tableid\n" +
                "where s.SCHEMANAME='COLUMNREFERENCEIT'\n" +
                "and ti.tablename='A'";

        ResultSet rs = methodWatcher.executeQuery(query);

        Assert.assertEquals("The returned result set is empty!",true, rs.next());

        Assert.assertEquals("The returned result set has the wrong table!", "B", rs.getString(1));

        Assert.assertEquals("The returned result set has more than 1 entry!", false, rs.next());

    }

}
