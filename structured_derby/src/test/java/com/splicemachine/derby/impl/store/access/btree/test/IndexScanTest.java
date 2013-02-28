package com.splicemachine.derby.impl.store.access.btree.test;

import java.sql.ResultSet;
import java.sql.SQLException;

import junit.framework.Assert;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import com.splicemachine.derby.test.SpliceDerbyTest;

@Ignore
public class IndexScanTest extends SpliceDerbyTest {
	@BeforeClass 
	public static void startup() throws Exception {
		startConnection();	
		executeStatement("create table test_jl_a (col1 int, col2 decimal(10,2), col3 varchar(25), col4 char(1), primary key (col1))");
		executeStatement("create index ix_test_jl_a on test_jl_a (col2)");
		executeStatement("create index ix_test_jl_b on test_jl_a (col3)");
		executeStatement("create index ix_test_jl_c on test_jl_a (col4,col2)");
		for (int i =0; i< 100; i++) {
			executeStatement("insert into test_jl_a values(" + i + ", " + (1.02+i)+",'" + i + "','A')");
		}
	}

	@Test
	public void geenrate() {
		
	}
	
	@Test
	public void queryPrimaryKeyEquals() throws SQLException {
		ResultSet resultSet = executeQuery("select * from test_jl_a where col1 = 50");
		int i = 0;
		while (resultSet.next()) {
			i++;
		}	
		resultSet.close();
		Assert.assertTrue(i==1);
	}

	@Test
	public void queryPrimaryKeyGreaterThan() throws SQLException {
		ResultSet resultSet = executeQuery("select * from test_jl_a where col1 > 98");
		int i = 0;
		int row = 0;
		while (resultSet.next()) {
			row = resultSet.getInt(1);
			i++;
		}	
		resultSet.close();
		Assert.assertTrue(row==99);		
		Assert.assertTrue(i==1);
	}

	@Test
	public void queryPrimaryKeyLesserThan() throws SQLException {
		ResultSet resultSet = executeQuery("select * from test_jl_a where col1 < 1");
		int i = 0;
		int row = 0;
		while (resultSet.next()) {
			row = resultSet.getInt(1);
			i++;
		}	
		resultSet.close();
		Assert.assertTrue(row==0);		
		Assert.assertTrue(i==1);
	}
	
	@AfterClass 
	public static void shutdown() throws SQLException {
		executeStatement("drop table test_jl_a");
		stopConnection();		
	}
}
