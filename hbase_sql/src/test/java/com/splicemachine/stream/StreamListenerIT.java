package com.splicemachine.stream;

import com.splicemachine.derby.test.framework.*;
import com.splicemachine.test.LongerThanTwoMinutes;
import com.splicemachine.test.SerialTest;
import com.splicemachine.test.SlowTest;
import org.junit.*;
import org.junit.experimental.categories.Category;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;
import org.junit.runner.Description;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;

@Category({LongerThanTwoMinutes.class, SlowTest.class, SerialTest.class})
public class StreamListenerIT {

    private static final String CLASS_NAME = StreamListenerIT.class.getSimpleName().toUpperCase();
    private static final SpliceWatcher spliceClassWatcher = new SpliceWatcher(CLASS_NAME);
    private static final SpliceSchemaWatcher schemaWatcher = new SpliceSchemaWatcher(CLASS_NAME);
    protected static final SpliceTableWatcher A_TABLE = new SpliceTableWatcher("A",schemaWatcher.schemaName,
            "(a1 int)");
    private static final int TABLE_SIZE = 1572864;

    private ExecutorService executor = Executors.newFixedThreadPool(4);

    @ClassRule
    public static TestRule chain = RuleChain.outerRule(spliceClassWatcher)
            .around(schemaWatcher)
            .around(A_TABLE)
            .around(new SpliceDataWatcher() {
                @Override
                protected void starting(Description description) {
                    try {
                        spliceClassWatcher.execute("insert into a values 1,2,3");
                        try (PreparedStatement ps = spliceClassWatcher.prepareStatement("insert into a select a1 + (select count(*) from a) from a")) {
                            for (int i = 0; i < 19; i++) {
                                ps.execute();
                            }
                        }
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    } finally {
                        spliceClassWatcher.closeAll();
                    }
                }
            });

    @Rule
    public SpliceWatcher methodWatcher = new SpliceWatcher(StreamListenerIT.class.getSimpleName().toUpperCase());

    @Test
    public void testThrottlingNotBlocks() throws Exception {

        List<Callable<String>> tasks = new ArrayList<>();

        for (int i =0; i < 24; i++) {
            final int taskIdx = i;
            tasks.add(new Callable<String>() {
                private final int idx = taskIdx;

                @Override
                public String call() throws Exception {
                    String sql = "select * from a --splice-properties useSpark=true" + "\n" +
                            "order by a1";

                    try(Connection connection = spliceClassWatcher.createConnection(); Statement s = connection.createStatement()){
                        s.setQueryTimeout(120);
                        ResultSet rs = s.executeQuery(sql);
                        long counter = 0;
                        while (rs.next()) {
                            rs.getString(1);
                            counter++;
                        }
                        rs.close();
                        if (counter != TABLE_SIZE)
                            return String.format("Counter does not matchL expected %d, received %d", TABLE_SIZE, counter);
                    } catch (Throwable e) {
                        return e.getMessage();
                    }
                    return null;
                }
            });
        }

        List<Future<String>> futureSet = new ArrayList<>();
        for (Callable<String> task : tasks) {
            futureSet.add(executor.submit(task));
        }

        for (Future<String> future : futureSet ) {
            Assert.assertNull(future.get());
        }
    }

}
