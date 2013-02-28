package com.splicemachine.derby.test;

import java.sql.BatchUpdateException;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.ListIterator;

import org.apache.derbyTesting.junit.JDBC;
import org.apache.log4j.Logger;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Tests for validating Derby Testing Setup
 * 
 */

public class DerbyTestSetupTest extends SpliceDerbyTest {
	private static Logger LOG = Logger.getLogger(DataOperationTest.class);

    public static final String[] GET_TABLES_TABLE = new String[] {"TABLE"};
    /**
     * Constant to pass to DatabaseMetaData.getTables() to fetch
     * just views.
     */
    public static final String[] GET_TABLES_VIEW = new String[] {"VIEW"};

    public static final String[] GET_TABLES_SYNONYM =
            new String[] {"SYNONYM"};

    private static final String[] COMPRESS_DB_OBJECTS =
    {
        "SYS.SYSDEPENDS",
    };

    @BeforeClass 
	public static void startup() throws Exception {
		startConnection();		
	}

    
    /**
     * Compress the objects in the database.
     * 
     * @param conn the db connection
     * @throws SQLException database error
     */
    private static void compressObjects(Connection conn) throws SQLException {
   	 
   	 CallableStatement cs = conn.prepareCall
   	     ("CALL SYSCS_UTIL.SYSCS_INPLACE_COMPRESS_TABLE(?, ?, 1, 1, 1)");
   	 
   	 for (int i = 0; i < COMPRESS_DB_OBJECTS.length; i++)
   	 {
   		 int delim = COMPRESS_DB_OBJECTS[i].indexOf(".");
            cs.setString(1, COMPRESS_DB_OBJECTS[i].substring(0, delim) );
            cs.setString(2, COMPRESS_DB_OBJECTS[i].substring(delim+1) );
            cs.execute();
   	 }
   	 
   	 cs.close();
   	 conn.commit();
    }

    private static boolean sysSequencesExists( Connection conn ) throws SQLException
    {
        PreparedStatement ps = null;
        ResultSet rs =  null;
        try {
            ps = conn.prepareStatement
                (
                 "select count(*) from sys.systables t, sys.sysschemas s\n" +
                 "where t.schemaid = s.schemaid\n" +
                 "and ( cast(s.schemaname as varchar(128)))= 'SYS'\n" +
                 "and ( cast(t.tablename as varchar(128))) = 'SYSSEQUENCES'" );
            rs = ps.executeQuery();
            rs.next();
            return ( rs.getInt( 1 ) > 0 );
        }
        finally
        {
            if ( rs != null ) { rs.close(); }
            if ( ps != null ) { ps.close(); }
        }
    }

	public static void dropSchema(DatabaseMetaData dmd, String schema) throws SQLException
	{		
		Connection conn = dmd.getConnection();
		Assert.assertFalse(conn.getAutoCommit());
		Statement s = dmd.getConnection().createStatement();
        
        // Functions - not supported by JDBC meta data until JDBC 4
        // Need to use the CHAR() function on A.ALIASTYPE
        // so that the compare will work in any schema.
        PreparedStatement psf = conn.prepareStatement(
                "SELECT ALIAS FROM SYS.SYSALIASES A, SYS.SYSSCHEMAS S" +
                " WHERE A.SCHEMAID = S.SCHEMAID " +
                " AND CHAR(A.ALIASTYPE) = ? " +
                " AND S.SCHEMANAME = ?");
        psf.setString(1, "F" );
        psf.setString(2, schema);
        ResultSet rs = psf.executeQuery();
        dropUsingDMD(s, rs, schema, "ALIAS", "FUNCTION");        

		// Procedures
		rs = dmd.getProcedures((String) null,
				schema, (String) null);
		
		dropUsingDMD(s, rs, schema, "PROCEDURE_NAME", "PROCEDURE");
		
		// Views
		rs = dmd.getTables((String) null, schema, (String) null,
                GET_TABLES_VIEW);
		
		dropUsingDMD(s, rs, schema, "TABLE_NAME", "VIEW");
		
		// Tables
		rs = dmd.getTables((String) null, schema, (String) null,
                GET_TABLES_TABLE);
		
		dropUsingDMD(s, rs, schema, "TABLE_NAME", "TABLE");
        
        // At this point there may be tables left due to
        // foreign key constraints leading to a dependency loop.
        // Drop any constraints that remain and then drop the tables.
        // If there are no tables then this should be a quick no-op.
        ResultSet table_rs = dmd.getTables((String) null, schema, (String) null,
                GET_TABLES_TABLE);

        while (table_rs.next()) {
            String tablename = table_rs.getString("TABLE_NAME");
            rs = dmd.getExportedKeys((String) null, schema, tablename);
            while (rs.next()) {
                short keyPosition = rs.getShort("KEY_SEQ");
                if (keyPosition != 1)
                    continue;
                String fkName = rs.getString("FK_NAME");
                // No name, probably can't happen but couldn't drop it anyway.
                if (fkName == null)
                    continue;
                String fkSchema = rs.getString("FKTABLE_SCHEM");
                String fkTable = rs.getString("FKTABLE_NAME");

                String ddl = "ALTER TABLE " +
                    JDBC.escape(fkSchema, fkTable) +
                    " DROP FOREIGN KEY " +
                    JDBC.escape(fkName);
                s.executeUpdate(ddl);
            }
            rs.close();
        }
        table_rs.close();
        conn.commit();
                
        // Tables (again)
        rs = dmd.getTables((String) null, schema, (String) null,
                GET_TABLES_TABLE);        
        dropUsingDMD(s, rs, schema, "TABLE_NAME", "TABLE");

        // drop UDTs
        psf.setString(1, "A" );
        psf.setString(2, schema);
        rs = psf.executeQuery();
        dropUsingDMD(s, rs, schema, "ALIAS", "TYPE");        
        psf.close();
  
        // Synonyms - need work around for DERBY-1790 where
        // passing a table type of SYNONYM fails.
        rs = dmd.getTables((String) null, schema, (String) null,
                GET_TABLES_SYNONYM);
        
        dropUsingDMD(s, rs, schema, "TABLE_NAME", "SYNONYM");
                
        // sequences
        if ( sysSequencesExists( conn ) )
        {
            psf = conn.prepareStatement
                (
                 "SELECT SEQUENCENAME FROM SYS.SYSSEQUENCES A, SYS.SYSSCHEMAS S" +
                 " WHERE A.SCHEMAID = S.SCHEMAID " +
                 " AND S.SCHEMANAME = ?");
            psf.setString(1, schema);
            rs = psf.executeQuery();
            dropUsingDMD(s, rs, schema, "SEQUENCENAME", "SEQUENCE");
            psf.close();
        }

		// Finally drop the schema if it is not APP
		if (!schema.equals("APP")) {
			s.executeUpdate("DROP SCHEMA " + JDBC.escape(schema) + " RESTRICT");
		}
		conn.commit();
		s.close();
	}

	private static void dropUsingDMD(
			Statement s, ResultSet rs, String schema,
			String mdColumn,
			String dropType) throws SQLException
	{
		String dropLeadIn = "DROP " + dropType + " ";
		
        // First collect the set of DROP SQL statements.
        ArrayList ddl = new ArrayList();
		while (rs.next())
		{
            String objectName = rs.getString(mdColumn);
            String raw = dropLeadIn + JDBC.escape(schema, objectName);
            if ( "TYPE".equals( dropType )  || "SEQUENCE".equals( dropType ) ) { raw = raw + " restrict "; }
            ddl.add( raw );
		}
		rs.close();
        if (ddl.isEmpty())
            return;
                
        // Execute them as a complete batch, hoping they will all succeed.
        s.clearBatch();
        int batchCount = 0;
        for (Iterator i = ddl.iterator(); i.hasNext(); )
        {
            Object sql = i.next();
            if (sql != null) {
                s.addBatch(sql.toString());
                batchCount++;
            }
        }

		int[] results;
        boolean hadError;
		try {
		    results = s.executeBatch();
		    Assert.assertNotNull(results);
		    Assert.assertEquals("Incorrect result length from executeBatch",
		    		batchCount, results.length);
            hadError = false;
		} catch (BatchUpdateException batchException) {
			results = batchException.getUpdateCounts();
			Assert.assertNotNull(results);
			Assert.assertTrue("Too many results in BatchUpdateException",
					results.length <= batchCount);
            hadError = true;
		}
		
        // Remove any statements from the list that succeeded.
		boolean didDrop = false;
		for (int i = 0; i < results.length; i++)
		{
			int result = results[i];
			if (result == Statement.EXECUTE_FAILED)
				hadError = true;
			else if (result == Statement.SUCCESS_NO_INFO || result >= 0) {
				didDrop = true;
				ddl.set(i, null);
			}
			else
				Assert.fail("Negative executeBatch status");
		}
        s.clearBatch();
        if (didDrop) {
            // Commit any work we did do.
            s.getConnection().commit();
        }

        // If we had failures drop them as individual statements
        // until there are none left or none succeed. We need to
        // do this because the batch processing stops at the first
        // error. This copes with the simple case where there
        // are objects of the same type that depend on each other
        // and a different drop order will allow all or most
        // to be dropped.
        if (hadError) {
            do {
                hadError = false;
                didDrop = false;
                for (ListIterator i = ddl.listIterator(); i.hasNext();) {
                    Object sql = i.next();
                    if (sql != null) {
                        try {
                            s.executeUpdate(sql.toString());
                            i.set(null);
                            didDrop = true;
                        } catch (SQLException e) {
                            hadError = true;
                        }
                    }
                }
                if (didDrop)
                    s.getConnection().commit();
            } while (hadError && didDrop);
        }
	}

	@Test
	public void testDependencyBug() throws SQLException {
		try {
			conn.setAutoCommit(false);
			DatabaseMetaData dmd = conn.getMetaData();
			dropSchema(dmd,"APP");
			// not sure what we should test for exactly - we haven't made it this far yet
			
		}
		catch (SQLException e) {
			LOG.error("error during create and insert table-"+e.getMessage(), e);
			
		} 
		finally {
		}
	}
	
	@Test
	public void testCompressObjectsBug() throws SQLException {
		try {
			conn.setAutoCommit(false);
			compressObjects(conn);	
			// not sure what we should test for exactly - we haven't made it this far yet
			
		}
		catch (SQLException e) {
			LOG.error("error during create and insert table-"+e.getMessage(), e);
			
		} 
		finally {
		}
	}

	

	@AfterClass 
	public static void shutdown() throws SQLException {
		stopConnection();		
	}
}
