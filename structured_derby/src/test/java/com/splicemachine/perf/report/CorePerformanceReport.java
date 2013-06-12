package com.splicemachine.perf.report;

import java.sql.ResultSet;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;
import org.junit.runner.Description;
import com.splicemachine.derby.test.framework.SpliceSchemaWatcher;
import com.splicemachine.derby.test.framework.SpliceUnitTest;
import com.splicemachine.derby.test.framework.SpliceWatcher;
import com.splicemachine.derby.test.framework.tables.SpliceCustomerTable;
import com.splicemachine.derby.test.framework.tables.SpliceOrderLineTable;
//@Ignore
public class CorePerformanceReport extends SpliceUnitTest {
    protected static SpliceWatcher spliceClassWatcher = new SpliceWatcher();
	public static final String CLASS_NAME = CorePerformanceReport.class.getSimpleName().toUpperCase();
	public static final String TABLE_NAME_1 = "CUSTOMER";
	public static final String TABLE_NAME_2 = "ORDER_LINE";	
	public static final String TABLE_NAME_3 = "CUSTOMER_LOAD";
	public static final String TABLE_NAME_4 = "ORDER_LINE_LOAD";	
	public static final String TABLE_NAME_5 = "ORDER_LINE_LOAD10K";	
	
	protected static SpliceSchemaWatcher spliceSchemaWatcher = new SpliceSchemaWatcher(CLASS_NAME);	
	protected static SpliceCustomerTable spliceTableWatcher1 = new SpliceCustomerTable(TABLE_NAME_1,CLASS_NAME) {
		@Override
		protected void starting(Description description) {
			super.starting(description);
			importData(getResourceDirectory()+"customer_iso_10k.csv");
		}	
	};
	protected static SpliceOrderLineTable spliceTableWatcher2 = new SpliceOrderLineTable(TABLE_NAME_2,CLASS_NAME) {
		@Override
		protected void starting(Description description) {
			super.starting(description);
			importData(getResourceDirectory()+"order_line_500k.csv");
		}
	};
	protected static SpliceCustomerTable spliceTableWatcher3 = new SpliceCustomerTable(TABLE_NAME_3,CLASS_NAME); 
	protected static SpliceOrderLineTable spliceTableWatcher4 = new SpliceOrderLineTable(TABLE_NAME_4,CLASS_NAME);
	protected static SpliceOrderLineTable spliceTableWatcher5 = new SpliceOrderLineTable(TABLE_NAME_5,CLASS_NAME) {
		@Override
		protected void starting(Description description) {
			super.starting(description);
			importData(getResourceDirectory()+"order_line_small.csv");
		}
	};

	
	@ClassRule 
	public static TestRule chain = RuleChain.outerRule(spliceClassWatcher)
		.around(spliceSchemaWatcher)
		.around(spliceTableWatcher1)
		.around(spliceTableWatcher2)
		.around(spliceTableWatcher3)
		.around(spliceTableWatcher4)
		.around(spliceTableWatcher5);
	
	@Rule public SpliceWatcher methodWatcher = new SpliceWatcher();
	
	@Test 
	public void load10KCustomerRecords() throws Exception {
		spliceTableWatcher3.importData(getResourceDirectory()+"customer_iso_10k.csv");
	}

	@Test 
	public void load500KOrderLineRecords() throws Exception {
		spliceTableWatcher4.importData(getResourceDirectory()+"order_line_500k.csv");
	}

	@Test
	public void count500KOrderLines() throws Exception {
		ResultSet rs = methodWatcher.executeQuery(format("select count(*) from %s",this.getTableReference(TABLE_NAME_2)));
		while (rs.next()) {
			Assert.assertEquals(500000,rs.getInt(1));
		}
	}
	
	@Test
	public void count500KOrderLinesAgain() throws Exception {
		ResultSet rs = methodWatcher.executeQuery(format("select count(*) from %s",this.getTableReference(TABLE_NAME_2)));
		while (rs.next()) {
			Assert.assertEquals(500000,rs.getInt(1));
		}
	}

	@Test
	public void aggregate10KCustomersByZipCode() throws Exception {
		ResultSet rs = methodWatcher.executeQuery(format("select cst_zipcode, count(*) from %s group by cst_zipcode",this.getTableReference(TABLE_NAME_1)));
		int count = 0;
		while (rs.next()) {
			count++;
		}
		Assert.assertEquals(566, count);
	}	
	@Test
	public void joinAndAggregateByZipcode10KOrderLinesWith10KCustomers() throws Exception {
		ResultSet rs = methodWatcher.executeQuery(format("select cst_zipcode, sum(orl_qty_sold*orl_unit_price) from %s " +
				"left outer join %s on orl_customer_id=cst_id group by cst_zipcode",this.getTableReference(TABLE_NAME_5),this.getTableReference(TABLE_NAME_1)));
		while (rs.next()) {
		}	
	}
	
	@Test
	public void joinAndAggregateByZipcode500KOrderLinesWith10KCustomers() throws Exception {
		ResultSet rs = methodWatcher.executeQuery(format("select cst_zipcode, sum(orl_qty_sold*orl_unit_price) from %s " +
				"left outer join %s on orl_customer_id=cst_id group by cst_zipcode",this.getTableReference(TABLE_NAME_2),this.getTableReference(TABLE_NAME_1)));
		while (rs.next()) {
		}	
	}

    @Test
    public void broadcastJoinAndAggregateByZipcode500KOrderLinesWith10KCustomers() throws Exception {
        ResultSet rs = methodWatcher.executeQuery(format("select cst_zipcode, sum(orl_qty_sold*orl_unit_price) from %s " +
                "left outer join %s --DERBY-PROPERTIES joinStrategy=broadcast \n" +
                "on orl_customer_id=cst_id group by cst_zipcode",this.getTableReference(TABLE_NAME_2),this.getTableReference(TABLE_NAME_1)));
        while (rs.next()) {
        }
    }

    @Test
    public void aggregate500kOrderLines() throws Exception {
        ResultSet rs = methodWatcher.executeQuery(format("select orl_unit_price, orl_unit_cost, orl_discount from  %s group by orl_unit_price, orl_unit_cost, orl_discount",this.getTableReference(TABLE_NAME_2)));
        while (rs.next()) {
        }
    }
}
