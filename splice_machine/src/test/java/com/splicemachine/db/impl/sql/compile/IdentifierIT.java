package com.splicemachine.db.impl.sql.compile;

import com.splicemachine.derby.test.framework.SpliceSchemaWatcher;
import com.splicemachine.derby.test.framework.SpliceUnitTest;
import com.splicemachine.derby.test.framework.SpliceWatcher;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

import java.sql.ResultSet;
import java.sql.SQLException;

import static org.junit.Assert.assertEquals;

public class IdentifierIT extends SpliceUnitTest {

    private static final String SCHEMA_NAME = IdentifierIT.class.getSimpleName().toUpperCase();
    private static final SpliceWatcher spliceClassWatcher = new SpliceWatcher(SCHEMA_NAME);
    private static final SpliceSchemaWatcher spliceSchemaWatcher = new SpliceSchemaWatcher(SCHEMA_NAME);

    @Rule
    public SpliceWatcher methodWatcher = new SpliceWatcher(SCHEMA_NAME);

    @ClassRule
    public static TestRule chain = RuleChain.outerRule(spliceClassWatcher)
            .around(spliceSchemaWatcher);


    @Test
    public void testSpecialCharacter() throws Exception {
        methodWatcher.execute("create table a (i#$@ int)");
        methodWatcher.executeUpdate("insert into a values (1)");
        ResultSet rs = methodWatcher.executeQuery("select * from a");
        rs.next();
        assertEquals(1, rs.getInt("I#$@"));
        rs.close();
        methodWatcher.execute("drop table a");
    }

    @Test
    public void testInvalidSpecialCharacter() throws Exception {
        try {
            methodWatcher.execute("create table a (#$@ int)");
            throw new Exception("Should throw error 42X02");
        } catch (SQLException e) {
            assertEquals("42X02", e.getSQLState());
        }
    }
}
