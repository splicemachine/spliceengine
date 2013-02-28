package com.splicemachine.derby.impl.store.access.btree.test;

import java.sql.ResultSet;
import java.sql.SQLException;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import com.splicemachine.derby.test.SpliceDerbyTest;

public class IndexConglomerateTest extends SpliceDerbyTest {
	@BeforeClass 
	public static void startup() throws Exception {
		startConnection();		
	}

	@AfterClass 
	public static void shutdown() throws SQLException {
		//dropTable() ;
		stopConnection();		
	}
	
	@Test	
	public void createAndDropIndexAndTable() throws SQLException {
		executeStatement("create table test_jl_a (num1 int, num2 int)");
		executeStatement("create index ix_test_jl_a on test_jl_a (num1)");
		executeStatement("drop index ix_test_jl_a");
		executeStatement("drop table test_jl_a");
	}

	@Test
	public void testAddColumn() throws SQLException {
		executeStatement("create table d (num int)");		
		executeStatement("insert into d values(100)");
		
		executeStatement("alter table d add addr varchar(24)");		
		executeStatement("insert into d values(200, 'my new addr')");
		ResultSet rs = executeQuery("select * from d where num=200");
		rs.next(); 
		Assert.assertEquals("my new addr", rs.getString(2));
		
		rs.close();
		executeStatement("drop table d");					
	}
}
