package org.apache.derby.impl.sql.execute.operations;


import au.com.bytecode.opencsv.CSVReader;
import au.com.bytecode.opencsv.CSVWriter;
import com.splicemachine.derby.test.framework.DefaultedSpliceWatcher;
import com.splicemachine.derby.test.framework.SpliceDataWatcher;
import com.splicemachine.derby.test.framework.SpliceSchemaWatcher;
import com.splicemachine.derby.test.framework.SpliceWatcher;
import com.splicemachine.homeless.TestUtils;
import org.apache.log4j.Logger;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;
import org.junit.runner.Description;

import java.io.FileReader;
import java.io.FileWriter;
import java.io.Writer;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.List;
import java.util.Map;

@Ignore("Intended for interactive use")
public class QueryPerformanceTest {

    private static Logger LOG = Logger.getLogger(QueryPerformanceTest.class);

    public static final String CLASS_NAME = QueryPerformanceTest.class.getSimpleName().toUpperCase();

    protected static DefaultedSpliceWatcher spliceClassWatcher = new DefaultedSpliceWatcher(CLASS_NAME);
    protected static SpliceSchemaWatcher spliceSchemaWatcher = new SpliceSchemaWatcher(CLASS_NAME);

    @Rule
    public SpliceWatcher methodWatcher = new DefaultedSpliceWatcher(CLASS_NAME);

    @ClassRule
    public static TestRule chain = RuleChain.outerRule(spliceClassWatcher)
            .around(spliceSchemaWatcher)
            .around(new SpliceDataWatcher() {
                @Override
                protected void starting(Description description) {

                    try {
                        System.out.println("Loading data");
                        TestUtils.executeSqlFile(spliceClassWatcher, "/Users/ryan/splice/datasets/msdata_perf/big_item.sql", CLASS_NAME);
                        System.out.println("Finished Loading");
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                }
            });

    @Test
    public void perfTestQuery() throws Exception {


        TestUtils.tableLookupByNumber(methodWatcher);
        System.out.println("Running query....");

        long begin = System.currentTimeMillis();

        for (int i = 0; i < 10; i++) {
            ResultSet rs = methodWatcher.executeQuery(String.format("select * from %s.item", CLASS_NAME));

            int count = 0;

            while (rs.next()) {
                count++;
            }

            if (count != 54000) {
                System.out.println("Count was not 54000, was: " + count);
            }

            rs.close();
        }

        long end = System.currentTimeMillis();

        System.out.println("Took " + Math.ceil((end - begin) / 1000) + " to count all");

    }

    @Test
    public void perfTestQuery2() throws Exception {

        System.out.println("Running query....");

        long begin = System.currentTimeMillis();

        for (int i = 0; i < 20; i++) {
            ResultSet rs = methodWatcher.executeQuery(String.format("select count(*) from %s.item", CLASS_NAME));
            rs.next();
            int count = rs.getInt(1);
            if (count != 54000) {
                System.out.println("Count was not 54000");
            }

            rs.close();
        }

        long end = System.currentTimeMillis();

        System.out.println("Took " + Math.ceil((end - begin) / 1000) + " to count all");

    }

    public static void main(String[] args) throws Exception {
        CSVReader reader = null;
        List<String[]> l = null;
        try {
            reader = new CSVReader(new FileReader("/Users/ryan/splice/msdatasample/item.csv"), ',', '"');
            l = reader.readAll();
        } finally {
            if (reader != null) {
                reader.close();
            }
        }

        Writer w = null;
        CSVWriter writer = null;
        try {
            w = new FileWriter("/Users/ryan/splice/datasets/msdata_perf/big_item.csv");
            writer = new CSVWriter(w, ',', '"');

            for (int i = 1; i <= 150 * l.size(); i++) {
                int idx = i % l.size();
                String[] row = l.get(idx);
                String[] newLine = new String[row.length];

                newLine[0] = String.valueOf(i);
                for (int j = 1; j < row.length; j++) {
                    newLine[j] = row[j];
                }

                writer.writeNext(newLine);
            }

        } finally {
            if (w != null) {
                w.close();
            }
        }

    }
}
