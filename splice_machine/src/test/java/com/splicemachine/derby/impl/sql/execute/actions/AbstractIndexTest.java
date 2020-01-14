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

package com.splicemachine.derby.impl.sql.execute.actions;

import com.splicemachine.derby.test.framework.SpliceIndexWatcher;
import com.splicemachine.derby.test.framework.SpliceTableWatcher;
import com.splicemachine.derby.test.framework.SpliceUnitTest;
import com.splicemachine.derby.test.framework.SpliceWatcher;
import org.junit.Assert;
import org.junit.Rule;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * @author Scott Fines
 *         Created on: 8/4/13
 */
public class AbstractIndexTest extends SpliceUnitTest {

    protected static final String tableSchema = "(a int, b float, c int, d double)";

    private static final float FLOAT_PRECISION = (float) Math.pow(10, -6);
    private static final float DOUBLE_PRECISION = (float) Math.pow(10, -12);

    @Rule public SpliceWatcher methodWatcher = new SpliceWatcher();

    protected final void createIndex(SpliceTableWatcher tableWatcher,String indexName, String indexSchema) throws Exception{
        SpliceIndexWatcher idxWatcher = new SpliceIndexWatcher(
                tableWatcher.tableName,
                tableWatcher.getSchema(),
                indexName,
                tableWatcher.getSchema(),indexSchema);
        idxWatcher.starting(null);
    }

    protected final void createUniqueIndex(SpliceTableWatcher tableWatcher,String indexName, String indexSchema) throws Exception{
        SpliceIndexWatcher idxWatcher = new SpliceIndexWatcher(
                tableWatcher.tableName,
                tableWatcher.getSchema(),
                indexName,
                tableWatcher.getSchema(),indexSchema,true);
        idxWatcher.starting(null);
    }


    protected final void assertColumnDataCorrect(String query,int[] dataNums, ResultSet rs,boolean expectData) throws SQLException {
        try{
            if(!expectData){
                Assert.assertFalse("Rows returned for query " + query, rs.next());
            }else{
                Assert.assertTrue("No Rows returned for query "+ query,rs.next());

                //validate the correct data returned
                int a = rs.getInt(1);
                Assert.assertFalse("No value for a returned for query a=" + dataNums[0], rs.wasNull());
                Assert.assertEquals("Incorrect value for a returned for query "+query ,dataNums[0],a);
                float b = rs.getFloat(2);
                Assert.assertFalse("No value for b returned for query a="+dataNums[1],rs.wasNull());
                Assert.assertEquals("Incorrect value for b returned for query "+query, dataNums[1], b, FLOAT_PRECISION);
                int c = rs.getInt(3);
                Assert.assertFalse("No value for c returned for query a="+dataNums[2],rs.wasNull());
                Assert.assertEquals("Incorrect value for a returned for query "+query , dataNums[2], c);
                double d = rs.getDouble(4);
                Assert.assertFalse("No value for d returned for query a="+dataNums[3],rs.wasNull());
                Assert.assertEquals("Incorrect value for b returned for query "+query , dataNums[3], d, DOUBLE_PRECISION);

                Assert.assertFalse("Too many Rows returned for query "+ query,rs.next());
            }
        }finally{
            rs.close();
        }
    }

    protected final void assertColumnDataCorrect(String query,int i, ResultSet rs,boolean expectData) throws SQLException {
        try{
            if(!expectData){
                Assert.assertFalse("Rows returned for query " + query, rs.next());
            }else{
                Assert.assertTrue("No Rows returned for query "+ query,rs.next());

                //validate the correct data returned
                int a = rs.getInt(1);
                Assert.assertFalse("No value for a returned for query a=" + i, rs.wasNull());
                Assert.assertEquals("Incorrect value for a returned for query "+query ,i,a);
                float b = rs.getFloat(2);
                Assert.assertFalse("No value for b returned for query a="+i,rs.wasNull());
                Assert.assertEquals("Incorrect value for b returned for query "+query, 2 * i, b, FLOAT_PRECISION);
                int c = rs.getInt(3);
                Assert.assertFalse("No value for c returned for query a="+i,rs.wasNull());
                Assert.assertEquals("Incorrect value for a returned for query "+query , 3 * i, c);
                double d = rs.getDouble(4);
                Assert.assertFalse("No value for d returned for query a="+i,rs.wasNull());
                Assert.assertEquals("Incorrect value for b returned for query "+query + i, 4 * i, d, DOUBLE_PRECISION);

                Assert.assertFalse("Too many Rows returned for query "+ query,rs.next());
            }
        }finally{
            rs.close();
        }
    }

    private static final String[] queryFormatStrings = new String[]{
            "select * from %s where a = ?",
            "select * from %s where b = ?",
            "select * from %s where c = ?",
            "select * from %s where d = ?",

            "select * from %s where a = ? and b = ?",
            "select * from %s where a = ? and c = ?",
            "select * from %s where a = ? and d = ?",
            "select * from %s where b = ? and c = ?",
            "select * from %s where b = ? and d = ?",
            "select * from %s where c = ? and d = ?",

            "select * from %s where a = ? and b = ? and c = ?",
            "select * from %s where a = ? and b = ? and d = ?",
            "select * from %s where a = ? and c = ? and d = ?",
            "select * from %s where b = ? and c = ? and d = ?",

            "select * from %s where a = ? and b = ? and c = ? and d = ?"
    };

    private static final int[][] setFields  = new int[][]{
            new int[]{ 1,-1,-1,-1},
            new int[]{-1, 1,-1,-1},
            new int[]{-1,-1, 1,-1},
            new int[]{-1,-1,-1, 1},

            new int[]{ 1, 1,-1,-1},
            new int[]{ 1,-1, 1,-1},
            new int[]{ 1,-1,-1, 1},

            new int[]{-1, 1, 1,-1},
            new int[]{-1, 1,-1, 1},

            new int[]{-1,-1, 1, 1},

            new int[]{ 1, 1, 1,-1},
            new int[]{ 1, 1,-1, 1},
            new int[]{ 1,-1, 1, 1},
            new int[]{-1, 1, 1, 1},

            new int[]{ 1, 1, 1, 1}
    };

    protected final void insertData(int size,String tableName) throws Exception {
        PreparedStatement ps = methodWatcher.prepareStatement("insert into "+ tableName +" (a,b,c,d) values (?,?,?,?)");
        for(int i=0;i<size;i++){
            ps.setInt(1,i);
            ps.setFloat(2,2*i);
            ps.setInt(3,3*i);
            ps.setDouble(4,4*i);
            ps.executeUpdate();
        }
    }

    protected final void assertImportedDataCorrect(String tableName, String fileName) throws Exception{
        PreparedStatement[] statementsToExecute = new PreparedStatement[queryFormatStrings.length];
        for(int i=0;i<queryFormatStrings.length;i++){
            statementsToExecute[i] = methodWatcher.prepareStatement(String.format(queryFormatStrings[i],tableName));
        }
        File file = new File(SpliceUnitTest.getResourceDirectory()+fileName);
        BufferedReader reader = null;
        try{
            reader = new BufferedReader(new FileReader(file));
            String line;
            while((line = reader.readLine())!=null){
                String[] data = line.split(",");
                int[] dataNums = new int[]{
                        Integer.parseInt(data[0]),
                        Integer.parseInt(data[1]),
                        Integer.parseInt(data[2]),
                        Integer.parseInt(data[3])
                };
                for(int i=0;i<statementsToExecute.length;i++){
                    PreparedStatement ps = statementsToExecute[i];
                    int[] numbers = mergeFields(dataNums,setFields[i]);
                    String queryToCheck = appendToStatement(ps, numbers);
                    assertColumnDataCorrect(queryToCheck,dataNums,ps.executeQuery(),true);
                }
            }
        }finally{
            if(reader!=null)
                reader.close();
        }
    }

    private int[] mergeFields(int[] dataNums, int[] setField) {
        return new int[]{
                setField[0]>=0 ? dataNums[0]: -1,
                setField[1]>=0 ? dataNums[1]: -1,
                setField[2]>=0 ? dataNums[2]: -1,
                setField[3]>=0 ? dataNums[3] : -1
        };
    }

    protected final void assertCorrectScan(int size, String tableName) throws Exception{
        int[][] badFieldMultipliers = getBadFieldMultipliers(size);
        PreparedStatement[] statementsToExecute = new PreparedStatement[queryFormatStrings.length];
        for(int i=0;i<queryFormatStrings.length;i++){
            statementsToExecute[i] = methodWatcher.prepareStatement(String.format(queryFormatStrings[i],tableName));
        }

        for(int i=0;i<statementsToExecute.length;i++){
            PreparedStatement statementToCheck = statementsToExecute[i];
            int[] fields = setFields[i];
            for(int num=0;num<size;num++){
                //check good for all set fields
                int[] numbers = getGoodFields(fields, num);
                String queryString = appendToStatement(statementToCheck, numbers);

                assertColumnDataCorrect(queryString,num,statementToCheck.executeQuery(),true);
                if(num==0) continue;

                for(int[] badField:badFieldMultipliers){
                    //check if this badField query applies

                    boolean applies=true;
                    for(int j=0;j<badField.length;j++){
                        if(fields[j]<=0){
                            if(badField[j]> 0 ){
                                //fields[j] is not set, but badField[j] is
                                applies=false;
                                break;
                            }
                        }else if(badField[j] <=0){
                            //fields[j] is set, but badField[j] is not
                            applies=false;
                            break;
                        }
                    }
                    if(!applies)continue;

                    numbers = getBadFields(badField);

                    queryString = appendToStatement(statementToCheck,numbers);
                    assertColumnDataCorrect(queryString,num,statementToCheck.executeQuery(),false);
                }
            }
        }
    }

    private int[] getBadFields(int[] badField) {
        return new int[]{
                badField[0]>0? badField[0]: -1,
                badField[1]>0? 2*badField[1] : -1,
                badField[2]>0? 3*badField[2] : -1,
                badField[3]>0? 4*badField[3] : -1
        };
    }

    private int[] getGoodFields(int[] fields, int num) {
        return new int[]{
                fields[0]>0?num : -1,
                fields[1]>0? 2*num : -1,
                fields[2]>0? 3*num : -1,
                fields[3]>0? 4*num : -1
        };
    }

    private String appendToStatement(PreparedStatement statementToCheck, int[] numbers) throws Exception {
        int nextSpot=1;
        StringBuilder queryString = new StringBuilder();
        if(numbers[0]>=0){
            statementToCheck.setInt(nextSpot,numbers[0]);
            queryString.append("a = ").append(numbers[0]).append(",");
            nextSpot++;
        }
        if(numbers[1]>=0){
            statementToCheck.setFloat(nextSpot,numbers[1]);
            queryString.append("b = ").append(numbers[1]).append(",");
            nextSpot++;
        }
        if(numbers[2]>=0){
            statementToCheck.setInt(nextSpot,numbers[2]);
            queryString.append("c = ").append(numbers[2]).append(",");
            nextSpot++;
        }
        if(numbers[3]>=0){
            statementToCheck.setDouble(nextSpot, numbers[3]);
            queryString.append("d = ").append(numbers[3]).append(",");
        }
        return queryString.toString();
    }

    private int[][] getBadFieldMultipliers(int size) {
        return new int[][]{
                //1 value
                new int[]{size,    -1,    -1,    -1},
                new int[]{  -1,size+1,    -1,    -1},
                new int[]{  -1,    -1,size+1,    -1},
                new int[]{  -1,    -1,    -1,size+1},

                new int[]{     1,size+1,    -1,    -1},
                new int[]{size+1,     1,    -1,    -1},
                new int[]{size+1,size+1,    -1,    -1},

                new int[]{     1,    -1,size+1,    -1},
                new int[]{size+1,    -1,     1,    -1},
                new int[]{size+1,    -1,size+1,    -1},

                new int[]{     1,    -1,    -1,size+1},
                new int[]{size+1,    -1,    -1,     1},
                new int[]{size+1,    -1,    -1,size+1},

                new int[]{    -1,size+1,     1,    -1},
                new int[]{    -1,     1,size+1,    -1},
                new int[]{    -1,size+1,size+1,    -1},

                new int[]{    -1,size+1,    -1,     1},
                new int[]{    -1,     1,    -1,size+1},
                new int[]{    -1,size+1,    -1,size+1},

                new int[]{    -1,    -1,size+1,     1},
                new int[]{    -1,    -1,     1,size+1},
                new int[]{    -1,    -1,size+1,size+1},

                new int[]{size+1,     1,     1,    -1},
                new int[]{     1,size+1,     1,    -1},
                new int[]{     1,     1,size+1,    -1},
                new int[]{size+1,size+1,     1,    -1},
                new int[]{size+1,     1,size+1,    -1},
                new int[]{     1,size+1,size+1,    -1},
                new int[]{size+1,size+1,size+1,    -1},

                new int[]{size+1,     1,    -1,     1},
                new int[]{     1,size+1,    -1,     1},
                new int[]{     1,     1,    -1,size+1},
                new int[]{size+1,size+1,    -1,     1},
                new int[]{size+1,     1,    -1,size+1},
                new int[]{     1,size+1,    -1,size+1},
                new int[]{size+1,size+1,    -1,size+1},

                new int[]{size+1,    -1,     1,     1},
                new int[]{     1,    -1,size+1,     1},
                new int[]{     1,    -1,     1,size+1},
                new int[]{size+1,    -1,size+1,     1},
                new int[]{size+1,    -1,     1,size+1},
                new int[]{     1,    -1,size+1,size+1},
                new int[]{size+1,    -1,size+1,size+1},

                new int[]{size+1,     1,     1,     1},
                new int[]{     1,size+1,     1,     1},
                new int[]{     1,     1,size+1,     1},
                new int[]{     1,     1,     1,size+1},

                new int[]{size+1,size+1,     1,     1},
                new int[]{size+1,     1,size+1,     1},
                new int[]{size+1,     1,     1,size+1},

                new int[]{     1,size+1,size+1,     1},
                new int[]{     1,size+1,     1,size+1},
                new int[]{     1,     1,size+1,size+1},

                new int[]{size+1,size+1,size+1,     1},
                new int[]{size+1,size+1,     1,size+1},
                new int[]{size+1,     1,size+1,size+1},
                new int[]{     1,size+1,size+1,size+1},

                new int[]{size+1,size+1,size+1,size+1}
        };
    }
}
