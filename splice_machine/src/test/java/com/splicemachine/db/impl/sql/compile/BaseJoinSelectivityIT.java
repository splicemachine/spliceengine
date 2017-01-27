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

package com.splicemachine.db.impl.sql.compile;

import com.splicemachine.derby.test.framework.SpliceWatcher;
import com.splicemachine.derby.test.framework.TestConnection;
import com.splicemachine.test_tools.TableCreator;
import org.junit.Assert;

import java.sql.ResultSet;
import java.sql.Statement;

import static com.splicemachine.test_tools.Rows.row;
import static com.splicemachine.test_tools.Rows.rows;

/**
 *
 *
 *
 */
public class BaseJoinSelectivityIT {

    @SuppressWarnings("unchecked")
    public static void createJoinDataSet(SpliceWatcher spliceClassWatcher, String schemaName) throws Exception {
        TestConnection conn = spliceClassWatcher.getOrCreateConnection();
        conn.setSchema(schemaName);
        try{
            TableCreator tc = new TableCreator(conn)
                    .withCreate("create table %s (c1 int not null, c2 varchar(56) not null, c3 timestamp not null, c4 boolean not null)")
                    .withInsert("insert into %s values(?,?,?,?)");

            tc.withTableName("ts_10_spk")
                    .withConstraints("primary key(c1)")
                    .withRows(rows(
                            row(1,"1","1960-01-01 23:03:20",false),
                            row(2,"2","1980-01-01 23:03:20",false),
                            row(3,"3","1985-01-01 23:03:20",false),
                            row(4,"4","1990-01-01 23:03:20",false),
                            row(5,"5","1995-01-01 23:03:20",false),
                            row(6,"6","1995-01-01 23:03:20",false),
                            row(7,"7","1995-01-01 23:03:20",false),
                            row(8,"8","1995-01-01 23:03:20",false),
                            row(9,"9","1995-01-01 23:03:20",false),
                            row(10,"10","1995-01-01 23:03:20",false))).create();

            tc.withTableName("ts_10_mpk")
                    .withConstraints("primary key (c1,c2)")
                    .create();

//                    .withRows(rows(
//                            row(1,"1","1960-01-01 23:03:20",false),
//                            row(2,"2","1980-01-01 23:03:20",false),
//                            row(3,"3","1985-01-01 23:03:20",false),
//                            row(4,"4","1990-01-01 23:03:20",false),
//                            row(5,"5","1995-01-01 23:03:20",false),
//                            row(6,"6","1995-01-01 23:03:20",false),
//                            row(7,"7","1995-01-01 23:03:20",false),
//                            row(8,"8","1995-01-01 23:03:20",false),
//                            row(9,"9","1995-01-01 23:03:20",false),
//                            row(10,"10","1995-01-01 23:03:20",false))).create();

            tc.withTableName("ts_10_npk")
                    .withConstraints(null)
                    .create();
//            new TableCreator(conn)
//                    .withCreate("create table ts_10_npk (c1 int not null, c2 varchar(56) not null, c3 timestamp not null, c4 boolean not null)")
//                    .withInsert("insert into ts_10_npk values(?,?,?,?)")
//                    .withRows(rows(
//                            row(1,"1","1960-01-01 23:03:20",false),
//                            row(2,"2","1980-01-01 23:03:20",false),
//                            row(3,"3","1985-01-01 23:03:20",false),
//                            row(4,"4","1990-01-01 23:03:20",false),
//                            row(5,"5","1995-01-01 23:03:20",false),
//                            row(6,"6","1995-01-01 23:03:20",false),
//                            row(7,"7","1995-01-01 23:03:20",false),
//                            row(8,"8","1995-01-01 23:03:20",false),
//                            row(9,"9","1995-01-01 23:03:20",false),
//                            row(10,"10","1995-01-01 23:03:20",false))).create();

            tc.withTableName("ts_5_spk")
                    .withConstraints("primary key(c1)")
                    .withRows(rows(
                            row(1,"1","1960-01-01 23:03:20",false),
                            row(2,"2","1980-01-01 23:03:20",false),
                            row(3,"3","1985-01-01 23:03:20",false),
                            row(4,"4","1990-01-01 23:03:20",false),
                            row(5,"5","1995-01-01 23:03:20",false))).create();

            tc.withTableName("ts_5_mpk")
                    .withConstraints("primary key(c1,c2)")
                    .create();
//            new TableCreator(conn)
//                    .withCreate("create table ts_5_mpk (c1 int not null, c2 varchar(56) not null, c3 timestamp not null, c4 boolean not null, primary key (c1,c2))")
//                    .withInsert("insert into ts_5_mpk values(?,?,?,?)")
//                    .withRows(rows(
//                            row(1,"1","1960-01-01 23:03:20",false),
//                            row(2,"2","1980-01-01 23:03:20",false),
//                            row(3,"3","1985-01-01 23:03:20",false),
//                            row(4,"4","1990-01-01 23:03:20",false),
//                            row(5,"5","1995-01-01 23:03:20",false))).create();

            tc.withTableName("ts_5_npk")
                    .withConstraints(null)
                    .create();

//            new TableCreator(conn)
//                    .withCreate("create table ts_5_npk (c1 int not null, c2 varchar(56) not null, c3 timestamp not null, c4 boolean not null)")
//                    .withInsert("insert into ts_5_npk values(?,?,?,?)")
//                    .withRows(rows(
//                            row(1,"1","1960-01-01 23:03:20",false),
//                            row(2,"2","1980-01-01 23:03:20",false),
//                            row(3,"3","1985-01-01 23:03:20",false),
//                            row(4,"4","1990-01-01 23:03:20",false),
//                            row(5,"5","1995-01-01 23:03:20",false))).create();

            tc.withTableName("ts_3_spk")
                    .withConstraints("primary key(c1)")
                    .withRows(rows(
                            row(5,"5","1960-01-01 23:03:20",false),
                            row(6,"6","1980-01-01 23:03:20",false),
                            row(7,"7","1985-01-01 23:03:20",false))).create();

            new TableCreator(conn)
                    .withCreate("create table t1(i varchar(30)) --schema="+schemaName)
                    .withIndex("create index t1i on t1(i) --schema="+schemaName)
                    .create();

            new TableCreator(conn)
                    .withCreate("create table t2(j varchar(30))")
                    .withInsert("insert into t2 values(?)")
                    .withIndex("create index t2j on t2(j)")
                    .withRows(rows(
                            row("1"),
                            row("2"),
                            row("3"),
                            row("4")
                    )).create();

            for(int i=0;i<10;i++){
                spliceClassWatcher.executeUpdate("insert into t2 select * from t2");
            }

            conn.createStatement().executeQuery(String.format(
                    "call SYSCS_UTIL.COLLECT_SCHEMA_STATISTICS('%s',false)",
                    schemaName));
            conn.commit();
        }finally{
           conn.reset();
        }
    }

    protected double getTotalCost(String s) {
        double cost = 0.0d;
        String[] strings = s.split(",");
        for(String string : strings) {
            String[] s1 = string.split("=");
            if (s1[0].compareTo("totalCost") == 0) {
                cost = Double.parseDouble(s1[1]);
                break;
            }
        }
        return cost;
    }

    protected void rowContainsQuery(Statement s,
                                    int[] levels,
                                    String query,
                                    String... contains) throws Exception {
        try(ResultSet resultSet = s.executeQuery(query)){
            int i=0;
            int k=0;
            while(resultSet.next()){
                i++;
                for(int level : levels){
                    if(level==i){
                        Assert.assertTrue("failed query: "+query+" -> "+resultSet.getString(1),resultSet.getString(1).contains(contains[k]));
                        k++;
                    }
                }
            }
        }
    }
}