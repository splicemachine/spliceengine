package com.splicemachine.foreignkeys;

import com.splicemachine.derby.test.framework.RuledConnection;
import com.splicemachine.derby.test.framework.SchemaRule;
import org.junit.*;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

import java.sql.*;

/**
 * @author Scott Fines
 *         Date: 6/27/16
 */
public class ForeignKeyMetadataIT{
    private static final String SCHEMA = ForeignKeyMetadataIT.class.getSimpleName();

    private RuledConnection conn = new RuledConnection(null,false);

    @Rule public TestRule ruleChain =RuleChain.outerRule(conn)
            .around(new SchemaRule(conn,SCHEMA));

    @Before
    public void setUp() throws Exception{
        try(Statement s = conn.createStatement()){
            s.execute("drop table if exists C1");
            s.execute("drop table if exists C2");
            s.execute("drop table if exists P");
        }

    }

    @Test
    @Ignore("DB-3536")
    public void testSQLFOREIGNKEYSReturnsCorrect1Table() throws Exception{
        try(Statement s=conn.createStatement()){
            s.executeUpdate("create table P (a int primary key)");
            s.executeUpdate("create table C1 (a int references P(a))");
        }
        try(CallableStatement ps=conn.prepareCall("call SYSIBM.SQLFOREIGNKEYS(?,?,?,?,?,?,?)")){
            ps.setString(1,"");
            ps.setString(2,SCHEMA);
            ps.setString(3,"P");
            ps.setString(4,"");
            ps.setString(5,SCHEMA);
            ps.setString(6,"C1");
            ps.setString(7,"DATATYPE='JDBC';IMPORTEDKEY=1; CURSORHOLD=1");

            try(ResultSet rs=ps.executeQuery()){
                Assert.assertTrue("No rows returned!",rs.next());
                String pSchema=rs.getString(2);
                Assert.assertFalse("parent schema was null!",rs.wasNull());
                Assert.assertEquals("Incorrect parent schema!",SCHEMA,pSchema);

                String pTable=rs.getString(3);
                Assert.assertFalse("parent table was null!",rs.wasNull());
                Assert.assertEquals("Incorrect parent table!","P",pTable);

                String fSchema=rs.getString(5);
                Assert.assertFalse("child schema was null!",rs.wasNull());
                Assert.assertEquals("Incorrect child schema!",SCHEMA,fSchema);

                String fTable=rs.getString(6);
                Assert.assertFalse("child table was null!",rs.wasNull());
                Assert.assertEquals("Incorrect child table!","C1",fTable);
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
}

