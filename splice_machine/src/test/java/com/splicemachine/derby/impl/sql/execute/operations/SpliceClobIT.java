package com.splicemachine.derby.impl.sql.execute.operations;

import com.splicemachine.derby.test.framework.SpliceSchemaWatcher;
import com.splicemachine.derby.test.framework.SpliceUnitTest;
import com.splicemachine.derby.test.framework.SpliceWatcher;
import com.splicemachine.derby.test.framework.TestConnection;
import com.splicemachine.test_tools.TableCreator;
import org.junit.*;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

import java.sql.Clob;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

/**
 * Created by jyuan on 12/17/18.
 */
public class SpliceClobIT extends SpliceUnitTest {
    private static final String CLASS_NAME = SpliceClobIT.class.getSimpleName().toUpperCase();
    private static final SpliceWatcher spliceClassWatcher = new SpliceWatcher(CLASS_NAME);
    protected static SpliceSchemaWatcher spliceSchemaWatcher = new SpliceSchemaWatcher(CLASS_NAME);
    private static TestConnection conn;

    @ClassRule
    public static SpliceWatcher methodWatcher = new SpliceWatcher(CLASS_NAME);

    @ClassRule
    public static TestRule chain = RuleChain.outerRule(spliceClassWatcher)
            .around(spliceSchemaWatcher);

    @BeforeClass
    public static void setUp() throws Exception{
        conn = methodWatcher.getOrCreateConnection();
        new TableCreator(conn)
                .withCreate("create table documents (id INT, c CLOB(64 K))")
                .create();
    }

    @Test
    public void testClob() throws Exception {
        String clobString = "A TEST OF CLOB INSERT...";
        PreparedStatement ps = conn.prepareStatement("INSERT INTO documents VALUES (?, ?)");
        Clob clob = conn.createClob();
        clob.setString(1,clobString);
        ps.setInt(1, 100);
        ps.setClob(2, clob);
        ps.execute();

        ps = conn.prepareStatement("SELECT c FROM documents where id=100");
        ResultSet rs = ps.executeQuery();
        while (rs.next()) {
            String s = rs.getString(1);
            Assert.assertEquals(s, clobString, s);
        }
    }

    @Test
    public void testClobWithString() throws Exception {
        PreparedStatement ps = conn.prepareStatement("INSERT INTO documents VALUES (?, ?)");
        String clobString = "Another TEST OF CLOB INSERT...";
        ps.setInt(1, 200);
        ps.setString(2, clobString);
        ps.execute();
        ps = conn.prepareStatement("SELECT c FROM documents where id=200");
        ResultSet rs = ps.executeQuery();
        while (rs.next()) {
            String s = rs.getString(1);
            Assert.assertEquals(s, clobString, s);
        }
    }
}
