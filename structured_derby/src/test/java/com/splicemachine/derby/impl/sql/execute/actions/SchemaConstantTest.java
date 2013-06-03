package com.splicemachine.derby.impl.sql.execute.actions;

import java.sql.Connection;
import java.sql.ResultSet;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;
import com.splicemachine.derby.test.framework.SpliceSchemaWatcher;
import com.splicemachine.derby.test.framework.SpliceUnitTest;
import com.splicemachine.derby.test.framework.SpliceWatcher;
public class SchemaConstantTest extends SpliceUnitTest {
		public static final String CLASS_NAME = SchemaConstantTest.class.getSimpleName().toUpperCase();
		public static final String SCHEMA1_NAME = CLASS_NAME + "_1";
		public static final String SCHEMA2_NAME = CLASS_NAME + "_2";
		public static final String SCHEMA3_NAME = CLASS_NAME + "_3";
		
		protected static SpliceWatcher spliceClassWatcher = new SpliceWatcher();
		
		@ClassRule 
		public static TestRule chain = RuleChain.outerRule(spliceClassWatcher);
		
		@Rule public SpliceWatcher methodWatcher = new SpliceWatcher();
		
		@Test
	    public void testSchemaCreation() throws Exception{
	    	Connection connection1 = methodWatcher.createConnection();
	    	connection1.setAutoCommit(false);
	    	SpliceSchemaWatcher.executeDrop(SCHEMA1_NAME);    	
	    	connection1.createStatement().execute(String.format("create schema %s",SCHEMA1_NAME));   
	        ResultSet resultSet = connection1.getMetaData().getSchemas(null, SCHEMA1_NAME);
	        Assert.assertTrue("Connection should see its own writes",resultSet.next());
	        connection1.commit();
	        resultSet = connection1.getMetaData().getSchemas(null, SCHEMA1_NAME);
	        Assert.assertTrue("New Transaction cannot see created schema",resultSet.next());
	    }
		
		@Test
	    public void testSchemaCreationIsolation() throws Exception{
	    	Connection connection1 = methodWatcher.createConnection();
	        Connection connection2 = methodWatcher.createConnection();
	    	SpliceSchemaWatcher.executeDrop(SCHEMA2_NAME);
	    	connection1.setAutoCommit(false);
	    	connection2.setAutoCommit(false);
	    	connection1.createStatement().execute(String.format("create schema %s",SCHEMA2_NAME));
	        ResultSet resultSet = connection2.getMetaData().getSchemas(null, SCHEMA2_NAME);
	        Assert.assertFalse("Read Committed Violated",resultSet.next());
	        resultSet = connection1.getMetaData().getSchemas(null, SCHEMA2_NAME);
	        Assert.assertTrue("Connection should see its own writes",resultSet.next());
	        connection1.commit();
	        resultSet = connection2.getMetaData().getSchemas(null, SCHEMA2_NAME);
	        Assert.assertFalse("Read Timestamp Violated",resultSet.next());
	        connection2.commit();
	        resultSet = connection2.getMetaData().getSchemas(null, SCHEMA2_NAME);
	        Assert.assertTrue("New Transaction cannot see created schema",resultSet.next());	        
	    }
		
		@Test
	    public void testSchemaRollbackIsolation() throws Exception{
	    	Connection connection1 = methodWatcher.createConnection();
	        Connection connection2 = methodWatcher.createConnection();
	    	SpliceSchemaWatcher.executeDrop(SCHEMA1_NAME);
	    	connection1.setAutoCommit(false);
	    	connection2.setAutoCommit(false);
	    	connection1.createStatement().execute(String.format("create schema %s",SCHEMA1_NAME));
	        ResultSet resultSet = connection2.getMetaData().getSchemas(null, SCHEMA1_NAME);
	        Assert.assertFalse("Read Committed Violated",resultSet.next());
	        resultSet = connection1.getMetaData().getSchemas(null, SCHEMA1_NAME);
	        Assert.assertTrue("Connection should see its own writes",resultSet.next());
	        connection1.rollback();
	        resultSet = connection2.getMetaData().getSchemas(null, SCHEMA1_NAME);
	        Assert.assertFalse("Read Timestamp Violated",resultSet.next());
	        connection2.commit();
	        resultSet = connection2.getMetaData().getSchemas(null, SCHEMA1_NAME);
	        Assert.assertFalse("New Transaction cannot see rollbacked schema",resultSet.next());	        
	    }

}
