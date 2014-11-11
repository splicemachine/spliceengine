package com.splicemachine.derby.impl.load;

import com.splicemachine.derby.test.framework.SpliceSchemaWatcher;
import com.splicemachine.derby.test.framework.SpliceWatcher;
import com.splicemachine.homeless.TestUtils;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestRule;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.sql.ResultSet;

import static java.lang.String.format;
import static org.junit.Assert.assertEquals;

public class ImportWithDifferentColumnOrderIT {

    public static final String SCHEMA = ImportWithDifferentColumnOrderIT.class.getSimpleName().toUpperCase();

    private static final SpliceWatcher spliceClassWatcher = new SpliceWatcher();
    private static final SpliceSchemaWatcher spliceSchemaWatcher = new SpliceSchemaWatcher(SCHEMA);

    @ClassRule
    public static TestRule chain = RuleChain.outerRule(spliceClassWatcher).around(spliceSchemaWatcher);

    @ClassRule
    public static TemporaryFolder tempFolder = new TemporaryFolder();

    @Rule
    public SpliceWatcher methodWatcher = new SpliceWatcher(SCHEMA);


    @BeforeClass
    public static void createImportFiles() throws FileNotFoundException, UnsupportedEncodingException {

        PrintWriter writer1 = new PrintWriter(new File(tempFolder.getRoot(), "Test1.txt"), "UTF-8");
        writer1.println("yuas,123");
        writer1.println("YifuMa,52");
        writer1.println("PeaceNLove,214");
        writer1.close();

        PrintWriter writer2 = new PrintWriter(new File(tempFolder.getRoot(), "Test2.txt"), "UTF-8");
        writer2.println();
        writer2.println("mvnVSworld, 134,11,sd");
        writer2.println("derbyWins, 97,6,os");
        writer2.println("RadioHeadS,00192,43,ux");
        writer2.close();
    }

    @Test
    public void allColumnsOutOfOrder() throws Exception {
        String path = tempFolder.getRoot() + "/Test1.txt";
        methodWatcher.executeUpdate("create table A (COL1 INT, COL2 VARCHAR(10))");
        methodWatcher.executeQuery(format("call SYSCS_UTIL.IMPORT_DATA('%s','A','COL2,COL1','%s',',',null,null,null,null,0,null)", SCHEMA, path));
        ResultSet rs1 = methodWatcher.executeQuery("select * from A order by col1");
        assertEquals("" +
                "COL1 |   COL2    |\n" +
                "------------------\n" +
                " 52  |  YifuMa   |\n" +
                " 123 |   yuas    |\n" +
                " 214 |PeaceNLove |", TestUtils.FormattedResult.ResultFactory.convert("", rs1, false).toString().trim());
    }

    @Test
    public void onlySomeColumnsOutOfOrder() throws Exception {
        String path = tempFolder.getRoot() + "/Test2.txt";
        methodWatcher.executeUpdate("create table B (COL1 INT, COL2 VARCHAR(10), COL3 INT, COL4 VARCHAR(2))");
        methodWatcher.executeQuery(format("call SYSCS_UTIL.IMPORT_DATA('%s','B','COL2,COL1,COL3,COL4','%s',',',null,null,null,null,0,null)", SCHEMA, path));
        ResultSet rs2 = methodWatcher.executeQuery("select * from B order by col1");
        assertEquals("" +
                "COL1 |   COL2    |COL3 |COL4 |\n" +
                "------------------------------\n" +
                " 97  | derbyWins |  6  | os  |\n" +
                " 134 |mvnVSworld | 11  | sd  |\n" +
                " 192 |RadioHeadS | 43  | ux  |", TestUtils.FormattedResult.ResultFactory.convert("", rs2, false).toString().trim());
    }


}
