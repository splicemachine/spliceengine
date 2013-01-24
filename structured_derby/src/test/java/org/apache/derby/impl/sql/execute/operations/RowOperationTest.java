package org.apache.derby.impl.sql.execute.operations;

import java.sql.ResultSet;
import java.sql.Statement;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.splicemachine.derby.test.SpliceDerbyTest;

public class RowOperationTest extends SpliceDerbyTest {

	
	@BeforeClass
	public static void setup() throws Exception{
		startConnection();
	}
	
	@AfterClass
	public static void shutdown() throws Exception {
		stopConnection();
	}
	
	@Test
	public void testReturnConstant() throws Exception {
		ResultSet rs = null;
//		try{
//			
//		}
	}
}
