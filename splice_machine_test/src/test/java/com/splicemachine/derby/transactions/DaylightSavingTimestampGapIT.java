package src.test.java.com.splicemachine.derby.transactions;

import com.splicemachine.derby.test.framework.SpliceSchemaWatcher;
import com.splicemachine.derby.test.framework.SpliceTableWatcher;
import com.splicemachine.derby.test.framework.SpliceWatcher;
import com.splicemachine.derby.test.framework.TestConnection;
import com.splicemachine.derby.transactions.InsertInsertTransactionIT;
import org.junit.*;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

import java.sql.SQLException;
import java.sql.Statement;

/**
 * Created by yifuma on 6/2/15.
 */
public class DaylightSavingTimestampGapIT {
    public static final SpliceSchemaWatcher schemaWatcher = new SpliceSchemaWatcher(DaylightSavingTimestampGapIT.class.getSimpleName().toUpperCase());

    public static final SpliceTableWatcher table = new SpliceTableWatcher("A",schemaWatcher.schemaName,"(a TIMESTAMP)");

    public static final SpliceWatcher classWatcher = new SpliceWatcher();

    @ClassRule
    public static TestRule chain = RuleChain.outerRule(classWatcher)
            .around(schemaWatcher)
            .around(table);

    private static TestConnection conn;

    @BeforeClass
    public static void setUpClass() throws Exception {
        conn = classWatcher.getOrCreateConnection();
    }

    @AfterClass
    public static void tearDownClass() throws Exception {
        conn.close();
    }

    @Test
    @Ignore
    public void testInsertTimestampDuringDaylightSavingGap() throws Exception{

        Statement s = conn.createStatement();

        String sql = "INSERT INTO A VALUES('";

        String insert = "2019-03-10 02:24:11";

        try{

            s.executeUpdate(sql + insert + "')");
        } catch (SQLException e){
            Assert.fail("Unable to insert timestamp " + insert);
        }

        try{

            insert = "2014-03-09 02:45:11";
            s.executeUpdate(sql + insert + "')");
        } catch (SQLException e){
            Assert.fail("Unable to insert timestamp " + insert);
        }

        try{
            insert = "2006-04-02 02:12:33";
            s.executeUpdate(sql + insert + "')");
        } catch (SQLException e){
            Assert.fail("Unable to insert timestamp " + insert);
        }

        try{
            insert = "1990-04-01 02:12:33";
            s.executeUpdate(sql + insert + "')");
        } catch (SQLException e){
            Assert.fail("Unable to insert timestamp " + insert);
        }

        try{
            insert = "1984-04-29 02:12:33";
            s.executeUpdate(sql + insert + "')");
        } catch (SQLException e){
            Assert.fail("Unable to insert timestamp " + insert);
        }

        try{
            insert = "1967-04-30 02:12:33";
            s.executeUpdate(sql + insert + "')");
        } catch (SQLException e){
            Assert.fail("Unable to insert timestamp " + insert);
        }

        try{
            insert = "2061-03-13 02:03:43.715";
            s.executeUpdate(sql + insert + "')");
        } catch (SQLException e){
            Assert.fail("Unable to insert timestamp " + insert);
        }

        try{
            insert = "2077-03-14 02:52:30.712";
            s.executeUpdate(sql + insert + "')");
        } catch (SQLException e){
            Assert.fail("Unable to insert timestamp " + insert);
        }

    }
}
