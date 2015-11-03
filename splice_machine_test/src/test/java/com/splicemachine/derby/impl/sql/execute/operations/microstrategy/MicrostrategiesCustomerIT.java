package com.splicemachine.derby.impl.sql.execute.operations.microstrategy;

import com.google.common.collect.Sets;
import com.splicemachine.derby.test.framework.SpliceSchemaWatcher;
import com.splicemachine.derby.test.framework.SpliceWatcher;
import com.splicemachine.derby.test.framework.tables.SpliceCustomerTable;
import com.splicemachine.test.suites.MicrostrategiesTests;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.sql.PreparedStatement;
import java.util.List;
import java.util.Set;

import static com.splicemachine.derby.test.framework.SpliceUnitTest.getResourceDirectory;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

@Category(MicrostrategiesTests.class)
public class MicrostrategiesCustomerIT {

    private static final String SCHEMA = MicrostrategiesCustomerIT.class.getSimpleName().toUpperCase();
    @ClassRule
    public static SpliceSchemaWatcher spliceSchemaWatcher = new SpliceSchemaWatcher(SCHEMA);
    @ClassRule
    public static SpliceWatcher spliceClassWatcher = new SpliceWatcher(SCHEMA);
    @Rule
    public SpliceWatcher methodWatcher = new SpliceWatcher(SCHEMA);

    @BeforeClass
    public static void createSharedTableAndImportData() throws Exception {
        spliceClassWatcher.executeUpdate("create table A" + SpliceCustomerTable.CREATE_STRING);
        doImport();
    }

    private static void doImport() throws Exception {
        PreparedStatement ps = spliceClassWatcher.prepareStatement("call SYSCS_UTIL.IMPORT_DATA (?, ?, null,?,',',null,null,null,null,1,null,true,null)");
        ps.setString(1, SCHEMA);
        ps.setString(2, "A");
        ps.setString(3, getResourceDirectory() + "customer_iso.csv");
        ps.executeUpdate();
        ps.close();
    }

    @Test
    public void testRepeatedSelectDistinct() throws Exception {
        for (int i = 1; i <= 10; i++) {
            testSelectDistinct();
            if (i % 3 == 0) {
                // additional imports should not affect select distinct
                doImport();
            }
        }
    }

    @Test
    public void testSelectDistinct() throws Exception {
        List<Integer> allCityIds = methodWatcher.queryList("select distinct cst_city_id from A");
        Set<Integer> uniqueCityIds = Sets.newHashSet(allCityIds);
        assertFalse("No City ids found!", uniqueCityIds.isEmpty());
        assertEquals(allCityIds.size(), uniqueCityIds.size());
        assertEquals(184, uniqueCityIds.size());
    }
}
