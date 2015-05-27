package com.splicemachine.derby.transactions;

import com.splicemachine.derby.test.framework.SpliceSchemaWatcher;
import com.splicemachine.derby.test.framework.SpliceTableWatcher;
import com.splicemachine.derby.test.framework.SpliceWatcher;
import com.splicemachine.derby.test.framework.TestConnection;
import com.splicemachine.pipeline.exception.ErrorState;

import com.splicemachine.test.Transactions;
import org.junit.*;
import org.junit.experimental.categories.Category;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

import java.lang.Exception;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

@Category({Transactions.class})

public class NotExistSubqueryIT{
    public static final SpliceSchemaWatcher schemaWatcher = new SpliceSchemaWatcher("HU");

    private static final SpliceTableWatcher staffTable =  new SpliceTableWatcher("STAFF",schemaWatcher.schemaName,"(EMPNUM VARCHAR(3) NOT NULL, EMPNAME VARCHAR(20), GRADE DECIMAL(4), CITY VARCHAR(15))");
    private static final SpliceTableWatcher projTable = new SpliceTableWatcher("PROJ", schemaWatcher.schemaName, "(PNUM VARCHAR(3) NOT NULL, PNAME VARCHAR(20), PTYPE CHAR(6), BUDGET DECIMAL(9), CITY VARCHAR(15))");
    private static final SpliceTableWatcher worksTable = new SpliceTableWatcher("WORKS", schemaWatcher.schemaName, "(EMPNUM VARCHAR(3) NOT NULL, NUM VARCHAR(3) NOT NULL, HOURS DECIMAL(5))");

    public static final SpliceWatcher classWatcher = new SpliceWatcher();

    @ClassRule
    public static TestRule chain;


    @Before
    public void setupTable() throws Exception {
        try{
            String update1 = "INSERT INTO HU.STAFF VALUES ('E1','Alice',12,'Deale'), ('E2','Betty',10,'Vienna'), " +
                    "('E3','Carmen',13,'Vienna'), ('E4','Don',12,'Deale'), ('E5','Ed',13,'Akron')";

            String update2 = "INSERT INTO HU.PROJ VALUES  ('P1','MXSS','Design',10000,'Deale'), ('P2','CALM','Code',30000,'Vienna')," +
                    "('P3','SDP','Test',30000,'Tampa'), ('P4','SDP','Design',20000,'Deale'), ('P5','IRM','Test',10000,'Vienna')," +
                    " ('P6','PAYR','Design',50000,'Deale')";

            String update3 = "INSERT INTO HU.WORKS VALUES  ('E1','P1',40), ('E1','P2',20), ('E1','P3',80), ('E1','P4',20)," +
                    "('E1','P5',12), ('E1','P6',12), ('E2','P1',40), ('E2','P2',80), ('E3','P2',20), ('E4','P2',20)," +
                    "('E4','P4',40), ('E4','P5',80)";

            classWatcher.executeUpdate(update1);
            classWatcher.executeUpdate(update2);
            classWatcher.executeUpdate(update3);
        } catch(Exception e){
            e.printStackTrace();
        }
    }

    @Test
    @Ignore
    public void testSingleColumnCorrelatedSubquery() throws Exception {

        String query = "SELECT STAFF.EMPNAME FROM STAFF WHERE NOT EXISTS (SELECT * FROM WORKS WHERE STAFF.EMPNUM = WORKS.EMPNUM)";

        ResultSet rs = classWatcher.executeQuery(query);

        /*
            Test whether the result set contains only one entry Ed
         */
        Assert.assertEquals("Result set is empty!", true, rs.next());
        Assert.assertEquals("First column of result set is not Ed!", "Ed", rs.getString("EMPNAME"));
        Assert.assertEquals("Result set has more than 1 entry!", false, rs.next());
    }

    @Test
    @Ignore
    public void testStarCorrelatedSubquery() throws Exception {

        String query = "SELECT * FROM PROJ WHERE NOT EXISTS (SELECT * FROM WORKS where WORKS.PNUM=PROJ.PNUM)";

        ResultSet rs = classWatcher.executeQuery(query);

        /*
            Test whether the result set is empty or not
         */

        Assert.assertEquals("Result set is not empty!", false, rs.next());
    }

    @Test
    @Ignore
    public void testSingleColumnDoublyCorrelatedSubquery() throws Exception {

        String query = "SELECT STAFF.EMPNAME FROM STAFF WHERE NOT EXISTS (SELECT * FROM PROJ WHERE NOT EXISTS (SELECT * FROM WORKS WHERE STAFF.EMPNUM = WORKS.EMPNUM AND WORKS.PNUM=PROJ.PNUM))";

        ResultSet rs = classWatcher.executeQuery(query);

        /*
            Test whether the result set contains only one entry Alice which is the expected behavior

         */

        Assert.assertEquals("Result set is empty!", true, rs.next());
        Assert.assertEquals("First column returned is not Alice!", "Alice", rs.getString("EMPNAME"));
        Assert.assertEquals("Result set has more than 1 entry!", false, rs.next());
    }


}