package com.splicemachine.derby.impl.sql.execute.operations;

import com.splicemachine.db.shared.common.reference.SQLState;
import com.splicemachine.derby.test.framework.SpliceSchemaWatcher;
import com.splicemachine.derby.test.framework.SpliceTableWatcher;
import com.splicemachine.derby.test.framework.SpliceUnitTest;
import com.splicemachine.derby.test.framework.SpliceWatcher;
import com.splicemachine.test_tools.TableCreator;
import com.sun.org.apache.xalan.internal.xsltc.compiler.util.ErrorMsg;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

import java.sql.Connection;

import static com.splicemachine.test_tools.Rows.row;
import static com.splicemachine.test_tools.Rows.rows;
import static org.junit.Assert.assertNotNull;

public class MergeNodeIT
{
    protected static SpliceWatcher spliceClassWatcher = new SpliceWatcher();
    public static final String CLASS_NAME = MergeNodeIT.class.getSimpleName().toUpperCase();

    protected static String TABLE_1 = "A";
    protected static SpliceSchemaWatcher spliceSchemaWatcher = new SpliceSchemaWatcher(CLASS_NAME);
    public SpliceWatcher methodWatcher = new SpliceWatcher(CLASS_NAME);

    @BeforeClass
    public static void beforeClass() throws Exception {
        createDataSet();
    }

    private static void createDataSet() throws Exception {
        Connection conn = spliceClassWatcher.getOrCreateConnection();
        conn.createStatement().execute("CREATE SCHEMA " + spliceSchemaWatcher.schemaName + " IF NOT EXISTS");
        conn.setSchema(spliceSchemaWatcher.schemaName);

        new TableCreator(conn)
                .withCreate("create table A_new ( i integer, j integer)")
                .withInsert("insert into A_new values(?,?,?)")
                .withRows(rows(row(1, 10), row(2, 20), row(3, 30)));

        new TableCreator(conn)
                .withCreate("create table A_dest ( i integer, j integer)")
                .withInsert("insert into A_dest values(?,?,?)")
                .withRows(rows(row(1, 1), row(2, 2), row(3, 3)));
    }

    @Test
    public void testGrammar() throws Exception {
        // when matched UPDATE
        methodWatcher.execute("merge into A_dest dest using A_new src on (src.i = src.j) when matched then update set dest.i = src.j");

        // when matched DELETE
        methodWatcher.execute("merge into A_dest dest using A_new src on (src.i = src.j) when matched then delete");

        // when + AND
        methodWatcher.execute("merge into A_dest dest using A_new src on (src.i = src.j)" +
                " when matched AND dest.i > 0 then update set dest.i = src.j ");

        // when not matched insert
        methodWatcher.execute("merge into A_dest dest using A_new src on (src.i = src.j)" +
                " when NOT matched then INSERT VALUES (5, 5)");

    }

    @Test
    public void testParseError() {
        Connection c = spliceClassWatcher.getOrCreateConnection();
        // when then update -> missing MATCHED / NOT MATCHED
        SpliceUnitTest.assertFailed(c, "merge into A_dest dest using A_new src on (src.i = src.j) " +
                "when then update set dest.i = src.j", SQLState.LANG_SYNTAX_ERROR);

        // not supported syntax: when not matched only with INSERT, not update
        SpliceUnitTest.assertFailed(c, "merge into A_dest dest using A_new src on (src.i = src.j)" +
                " when matched then update set dest.i = src.j " +
                " when not matched then update set dest.i = src.j", SQLState.LANG_SYNTAX_ERROR);
    }
}
