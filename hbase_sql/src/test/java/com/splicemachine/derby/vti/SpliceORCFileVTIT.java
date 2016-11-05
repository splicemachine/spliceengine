package com.splicemachine.derby.vti;

import static com.splicemachine.test_tools.Rows.row;
import static com.splicemachine.test_tools.Rows.rows;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

import com.splicemachine.derby.impl.sql.execute.operations.VTIOperationIT;
import com.splicemachine.derby.test.framework.SpliceSchemaWatcher;
import com.splicemachine.derby.test.framework.SpliceUnitTest;
import com.splicemachine.derby.test.framework.SpliceWatcher;
import com.splicemachine.homeless.TestUtils;
import com.splicemachine.test_tools.TableCreator;

public class SpliceORCFileVTIT extends SpliceUnitTest {
	public static final String CLASS_NAME = VTIOperationIT.class
			.getSimpleName().toUpperCase();
	private static final String TABLE_NAME = "EMPLOYEE";

	protected static SpliceWatcher spliceClassWatcher = new SpliceWatcher(
			CLASS_NAME);
	protected static SpliceSchemaWatcher spliceSchemaWatcher = new SpliceSchemaWatcher(
			CLASS_NAME);

	@ClassRule
	public static TestRule chain = RuleChain.outerRule(spliceClassWatcher)
	.around(spliceSchemaWatcher);

	@BeforeClass
	public static void setup() throws Exception {
		setup(spliceClassWatcher);
	}

	@AfterClass
	public static void tearDown() throws Exception {
		// spliceClassWatcher.execute("drop function JDBCTableVTI ");

	}

	private static void setup(SpliceWatcher spliceClassWatcher)
			throws Exception {
		Connection conn = spliceClassWatcher.getOrCreateConnection();
		/*
		 * new TableCreator(conn) .withCreate(
		 * "create table user_orc (id BIGINT, name VARCHAR(100), activity char(1), is_fte BOOLEAN,"
		 * +
		 * " test_tinyint  SMALLINT, test_smallint  SMALLINT, test_int  INT, test_float  FLOAT,"
		 * +
		 * " test_double  DOUBLE, test_decimal DECIMAL(10,0), role      VARCHAR(64), "
		 * + " salary    DECIMAL(8,2), START_DT  DATE, UPDATE_DT TIMESTAMP)")
		 * .create();
		 */

	}

	@Test
	public void testORCFileVTI() throws Exception {
		String location = getResourceDirectory() + "orcVtiFile.in";
		String sql = String
				.format("select * from new com.splicemachine.derby.vti.SpliceORCFileVTI('%s') as b (id bigint,name varchar(128),active char(1), start_dt date, update_dt timestamp )",
						location);
		ResultSet rs = spliceClassWatcher.executeQuery(sql);
		int count = 0;
		while (rs.next()) {
			count++;
		}
		Assert.assertEquals(5, count);
		if (rs != null)
			rs.close();
	}

}
