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

package com.splicemachine.foreignkeys;

import com.splicemachine.derby.test.framework.RuledConnection;
import com.splicemachine.derby.test.framework.SchemaRule;
import com.splicemachine.derby.test.framework.TableRule;
import com.splicemachine.test.SerialTest;
import org.junit.*;
import org.junit.experimental.categories.Category;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

import java.sql.*;

/**
 * @author Scott Fines
 *         Date: 6/27/16
 */
// SPLICE-894 Remove Serial
@Category(value = {SerialTest.class})
public class ForeignKeyMetadataIT{
    private static final String SCHEMA = ForeignKeyMetadataIT.class.getSimpleName().toUpperCase();

    private RuledConnection conn = new RuledConnection(null,true);

    private TableRule p = new TableRule(conn,"P","(a int, b int, CONSTRAINT P_PK PRIMARY KEY(A), CONSTRAINT P_UNIQ UNIQUE(B))");
    private TableRule c1  =new TableRule(conn,"C1","(c int, f int, CONSTRAINT C1_FK FOREIGN KEY (f) REFERENCES P(a))");
    private TableRule c2 = new TableRule(conn,"C2","(g int, h int, CONSTRAINT C2_FK FOREIGN KEY (h) REFERENCES P(b))");

    @Rule public TestRule ruleChain =RuleChain.outerRule(conn)
            .around(new SchemaRule(conn,SCHEMA))
            .around(p.childTable(c1).childTable(c2));


    @Test
    public void testSQLFOREIGNKEYSReturnsCorrectForExportedKeys() throws Exception{
        /*
        CALL SYSIBM.SQLFOREIGNKEYS(?,?,?,?,?,?,?)
         parameter #1: NULL
         parameter #2: SPLICE
         parameter #3: P
         parameter #4:
          parameter #5: NULL
           parameter #6:
            parameter #7: DATATYPE='JDBC';EXPORTEDKEY=1; CURSORHOLD=1
         */
        try(CallableStatement cs = conn.prepareCall("call SYSIBM.SQLFOREIGNKEYS(?,?,?,?,?,?,?)")){
            cs.setNull(1,Types.VARCHAR);
            cs.setString(2,SCHEMA);
            cs.setString(3,"P");
            cs.setString(4,"");
            cs.setNull(5,Types.VARCHAR);
            cs.setString(6,"");
            cs.setString(7,"DATATYPE='JDBC';EXPORTEDKEY=1; CURSORHOLD=1");

            try(ResultSet rs = cs.executeQuery()){
                boolean[] visitedTables = new boolean[]{false,false};
                while(rs.next()){
                    String fkTable = rs.getString("FKTABLE_NAME");
                    Assert.assertFalse("FK Table is null!",rs.wasNull());
                    switch(fkTable){
                        case "C1":
                            Assert.assertFalse("Already visited C1!",visitedTables[0]);
                            validateRow(rs,"P","A","P_PK","C1","F","C1_FK");
                            visitedTables[0] = true;
                            break;
                        case "C2":
                            Assert.assertFalse("Already visited C2!",visitedTables[1]);
                            validateRow(rs, "P","B","P_UNIQ","C2","H","C2_FK");
                            visitedTables[1] = true;
                            break;
                        default:
                            Assert.fail("Unknown child table: "+fkTable);
                    }
                }
                Assert.assertTrue("did not visit C1!",visitedTables[0]);
                Assert.assertTrue("did not visit C2!",visitedTables[0]);
            }
        }
    }

    @Test
    public void testSQLFOREIGNKEYSReturnsCorrectForImportedKeys() throws Exception{
        try(CallableStatement ps=conn.prepareCall("call SYSIBM.SQLFOREIGNKEYS(?,?,?,?,?,?,?)")){
            ps.setString(1,"");
            ps.setNull(2,Types.VARCHAR);
            ps.setString(3,"");
            ps.setNull(4,Types.VARCHAR);
            ps.setString(5,SCHEMA);
            ps.setString(6,"C1");
            ps.setString(7,"DATATYPE='JDBC';IMPORTEDKEY=1; CURSORHOLD=1");

            try(ResultSet rs=ps.executeQuery()){
                Assert.assertTrue("No rows returned!",rs.next());
                validateRow(rs,"P","A","P_PK","C1","F","C1_FK");

                Assert.assertFalse("Too many rows returned!",rs.next());
            }
        }
    }


    @Test
    public void testSimplifiedForeignKeysDoesNotExplode() throws Exception{
        /*
         * Regression test for DB-5424. The constructed query for reproduction
         * is unlikely to return anything (certainly not anything meaningful), so we
         * are really just checking to make sure that the query itself doesn't blow
         * up according to the bug; we won't be checking any other form of correctness
         */

        String sql = "explain SELECT\n"+
                "  1\n"+
                "FROM\n"+
                "  (SELECT\n"+
                "        1 as KEY_SEQ\n"+
                "    FROM\n"+
                "      (SELECT T.TABLEID AS PKTB_ID FROM SYS.SYSTABLES t) AS PKTB (PKTB_ID)\n"+
                "    , SYS.SYSCONSTRAINTS C\n"+
                "  ) AS PKINFO(KEY_SEQ)\n"+
                "  , SYS.SYSCONGLOMERATES CONGLOMS2\n"+
                "  , SYS.SYSCOLUMNS COLS2\n"+
                "WHERE\n"+
                "  PKINFO.KEY_SEQ = CONGLOMS2.DESCRIPTOR.getKeyColumnPosition(COLS2.COLUMNNUMBER)";

        try(Statement s = conn.createStatement()){
           try(ResultSet rs = s.executeQuery(sql)){
               /*
                * We check an arbitrary condition just to force the engine to actually run the query; this avoids
                * situations where we might lazily execute the query (or return before the query is fully completed)
                */
               while(rs.next()){
                   Object o = rs.getObject(1);
                   if(rs.wasNull())
                       Assert.assertNull("Did not return null!",o);
                   else
                       Assert.assertNotNull("returned null!",o);

               }
           }
        }
    }

    /* ****************************************************************************************************************/
    /*private helper methods*/
    private void validateRow(ResultSet rs,
                             String parentTable,String parentColumn, String pConstraint,
                             String childTable, String childColumn, String fkConstraint) throws SQLException{
        String pSchema=rs.getString(2);
        Assert.assertFalse("parent schema was null!",rs.wasNull());
        Assert.assertEquals("Incorrect parent schema!",SCHEMA,pSchema);

        String pTable=rs.getString(3);
        Assert.assertFalse("parent table was null!",rs.wasNull());
        Assert.assertEquals("Incorrect parent table!",parentTable,pTable);

        String pColumn=rs.getString(4);
        Assert.assertFalse("parent column was null!",rs.wasNull());
        Assert.assertEquals("Incorrect parent column!",parentColumn,pColumn);

        String fSchema=rs.getString(6);
        Assert.assertFalse("child schema was null!",rs.wasNull());
        Assert.assertEquals("Incorrect child schema!",SCHEMA,fSchema);

        String fTable=rs.getString(7);
        Assert.assertFalse("child table was null!",rs.wasNull());
        Assert.assertEquals("Incorrect child table!",childTable,fTable);

        String fCol = rs.getString(8);
        Assert.assertFalse("Child column was null!",rs.wasNull());
        Assert.assertEquals("Incorrect child column!",childColumn,fCol);

        String pkName = rs.getString(12);
        Assert.assertFalse("PK name was null!",rs.wasNull());
        Assert.assertEquals("Incorrect parent key name!",pConstraint,pkName);

        String fkName = rs.getString(13);
        Assert.assertFalse("FK name was null!",rs.wasNull());
        Assert.assertEquals("Incorrect foreign key name!",fkConstraint,fkName);

    }
}

