package org.apache.derby.impl.sql.execute.operations;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import org.junit.Assert;
import org.apache.log4j.Logger;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;
import org.junit.runner.Description;
import com.google.common.collect.Lists;
import com.splicemachine.derby.test.framework.SpliceDataWatcher;
import com.splicemachine.derby.test.framework.SpliceSchemaWatcher;
import com.splicemachine.derby.test.framework.SpliceTableWatcher;
import com.splicemachine.derby.test.framework.SpliceUnitTest;
import com.splicemachine.derby.test.framework.SpliceWatcher;

/**
 * @author Scott Fines
 *         Created on: 3/1/13
 */
public class PrimaryKeyTest extends SpliceUnitTest {
    private static final Logger LOG = Logger.getLogger(PrimaryKeyTest.class);
    protected static SpliceWatcher spliceClassWatcher = new SpliceWatcher();
	public static final String CLASS_NAME = PrimaryKeyTest.class.getSimpleName().toUpperCase();
	public static final String TABLE_NAME = "A";
	protected static SpliceSchemaWatcher spliceSchemaWatcher = new SpliceSchemaWatcher(CLASS_NAME);	
	protected static SpliceTableWatcher spliceTableWatcher = new SpliceTableWatcher(TABLE_NAME,CLASS_NAME,"(name varchar(50),val int, PRIMARY KEY(name))");
	protected static String INSERT = String.format("insert into %s.%s (name, val) values (?,?)",CLASS_NAME, TABLE_NAME);
	protected static String SELECT_BY_NAME = String.format("select * from %s.%s where name = ?",CLASS_NAME, TABLE_NAME);
	protected static String SELECT_NAME_BY_NAME = String.format("select name from %s.%s where name = ?",CLASS_NAME, TABLE_NAME);	
	protected static String UPDATE_NAME_BY_NAME = String.format("update %s.%s set name = ? where name = ?",CLASS_NAME, TABLE_NAME);
	protected static String UPDATE_VALUE_BY_NAME = String.format("update %s.%s set val = ? where name = ?",CLASS_NAME, TABLE_NAME);
	
	@ClassRule 
	public static TestRule chain = RuleChain.outerRule(spliceClassWatcher)
		.around(spliceSchemaWatcher)
		.around(spliceTableWatcher)
		.around(new SpliceDataWatcher(){
		@Override
		protected void starting(Description description) {
			try {
				PreparedStatement ps = spliceClassWatcher.prepareStatement(INSERT);
				ps.setString(1,"sfines");
				ps.setInt(2,1);
				ps.executeUpdate();
				ps.setString(1,"jleach");
				ps.setInt(2,2);
				ps.executeUpdate();
				ps.setString(1,"mzweben");
				ps.setInt(2,3);
				ps.executeUpdate();				
				ps.setString(1,"gdavis");
				ps.setInt(2,4);
				ps.executeUpdate();				
			} catch (Exception e) {
				throw new RuntimeException(e);
			}
			finally {
				spliceClassWatcher.closeAll();
			}
		}
		
	});
	@Rule public SpliceWatcher methodWatcher = new SpliceWatcher();


    @Test(expected=SQLException.class,timeout =10000)
    public void cannotInsertDuplicatePks() throws Exception{
        try {
            PreparedStatement ps = methodWatcher.prepareStatement(INSERT);
            ps.setString(1,"sfines");
            ps.setInt(2,1);
            ps.executeUpdate();
            Assert.fail("Did not throw an exception on duplicate records on primary key");
        } catch (SQLException e) {
            PreparedStatement validator = methodWatcher.prepareStatement(SELECT_BY_NAME);
            validator.setString(1,"sfines");
            ResultSet rs = validator.executeQuery();
            int matchCount=0;
            while(rs.next()){
                if("sfines".equalsIgnoreCase(rs.getString(1))){
                    matchCount++;
                }
            }
            Assert.assertEquals("Incorrect number of matching rows found!",1,matchCount);
            throw e;
        }
    }

    @Test()
    public void deleteAndInsertInSameTransaction() throws Exception{
        methodWatcher.getOrCreateConnection().setAutoCommit(false);
        final String name = "sfines";
        final int value = 2;
        ResultSet rs = methodWatcher.executeQuery(format("select * from %s where name = '%s'", this.getTableReference(TABLE_NAME), name));
        List<String> results = Lists.newArrayListWithExpectedSize(1);
        while (rs.next()) {
            String retName = rs.getString(1);
            int val = rs.getInt(2);
            results.add(String.format("name:%s,value:%d", retName, val));
        }
        Assert.assertEquals("Incorrect number of rows returned!", 1, results.size());
        methodWatcher.getStatement().execute(format("delete from %s", this.getTableReference(TABLE_NAME)));
        methodWatcher.getStatement().execute(format("insert into %s (name, val) values ('%s', %s)", this.getTableReference(TABLE_NAME), name, value));
        rs = methodWatcher.executeQuery(format("select * from %s where name = '%s'", this.getTableReference(TABLE_NAME), name));
        results = Lists.newArrayListWithExpectedSize(1);
        while (rs.next()) {
            String retName = rs.getString(1);
            int val = rs.getInt(2);
            results.add(String.format("name:%s,value:%d", retName, val));
        }
        Assert.assertEquals("Incorrect number of rows returned!", 1, results.size());
        methodWatcher.getOrCreateConnection().rollback();
    }

    @Test()
    public void insertAndDeleteInSameTransaction() throws Exception{
        methodWatcher.getOrCreateConnection().setAutoCommit(false);
        final String name = "other";
        final int value = 2;
        ResultSet rs = methodWatcher.executeQuery(format("select * from %s where name = '%s'", this.getTableReference(TABLE_NAME), name));
        List<String> results = Lists.newArrayListWithExpectedSize(1);
        while (rs.next()) {
            String retName = rs.getString(1);
            int val = rs.getInt(2);
            results.add(String.format("name:%s,value:%d", retName, val));
        }
        Assert.assertEquals("Incorrect number of rows returned!", 0, results.size());
        methodWatcher.getStatement().execute(format("insert into %s (name, val) values ('%s', %s)", this.getTableReference(TABLE_NAME), name, value));
        rs = methodWatcher.executeQuery(format("select * from %s where name = '%s'", this.getTableReference(TABLE_NAME), name));
        results = Lists.newArrayListWithExpectedSize(1);
        while (rs.next()) {
            String retName = rs.getString(1);
            int val = rs.getInt(2);
            results.add(String.format("name:%s,value:%d", retName, val));
        }
        Assert.assertEquals("Incorrect number of rows returned!", 1, results.size());
        methodWatcher.getStatement().execute(format("delete from %s where name = '%s'", this.getTableReference(TABLE_NAME), name));
        rs = methodWatcher.executeQuery(format("select * from %s where name = '%s'", this.getTableReference(TABLE_NAME), name));
        results = Lists.newArrayListWithExpectedSize(1);
        while (rs.next()) {
            String retName = rs.getString(1);
            int val = rs.getInt(2);
            results.add(String.format("name:%s,value:%d", retName, val));
        }
        Assert.assertEquals("Incorrect number of rows returned!", 0, results.size());
        methodWatcher.getOrCreateConnection().rollback();
    }

    @Test(expected=SQLException.class,timeout= 10000)
    public void testDuplicateInsertFromSameTable() throws Exception {
        /* Regression test for Bug 419 */
        PreparedStatement ps = methodWatcher.prepareStatement("insert into "+ spliceTableWatcher.toString()+" select * from "+spliceTableWatcher.toString());

        try{
            ps.execute();
        }catch(SQLException sql){
            Assert.assertTrue("Incorrect error returned!",sql.getMessage().contains("23505"));
            throw sql;
        }

    }

    @Test(timeout=10000)
    public void updateKeyColumn() throws Exception{
        PreparedStatement updateStatement = methodWatcher.prepareStatement(UPDATE_NAME_BY_NAME);
        updateStatement.setString(1,"jzhang");
        updateStatement.setString(2,"jleach");
        updateStatement.executeUpdate();
        PreparedStatement validator = methodWatcher.prepareStatement(SELECT_BY_NAME);
        validator.setString(1,"jleach");
        ResultSet rs = validator.executeQuery();
        while(rs.next()){
            Assert.fail("Should have returned nothing");
        }
        validator.setString(1,"jzhang");
        rs = validator.executeQuery();
        int matchCount = 0;
        while(rs.next()){
            if("jzhang".equalsIgnoreCase(rs.getString(1))){
                matchCount++;
                Assert.assertEquals("Column incorrect!",2,rs.getInt(2));
            }
        }
        Assert.assertEquals("Incorrect number of updated rows!",1,matchCount);
    }

    @Test(timeout=10000)
    public void updateNonKeyColumn() throws Exception{
        PreparedStatement updateStatement = methodWatcher.prepareStatement(UPDATE_VALUE_BY_NAME);
        updateStatement.setInt(1,20);
        updateStatement.setString(2,"mzweben");
        Assert.assertEquals(1, updateStatement.executeUpdate());

        PreparedStatement validator = methodWatcher.prepareStatement(SELECT_BY_NAME);
        validator.setString(1,"mzweben");
        ResultSet rs = validator.executeQuery();
        int matchCount =0;
        while(rs.next()){
            if("mzweben".equalsIgnoreCase(rs.getString(1))){
                matchCount++;
                int val = rs.getInt(2);
                Assert.assertEquals("Column incorrect!",20,val);
            }
        }
        Assert.assertEquals("Incorrect number of updated rows!",1,matchCount);
    }

    @Test(timeout=10000)
    public void scanningPrimaryKeyTableWithBaseRowLookup() throws Exception{
        PreparedStatement test = methodWatcher.prepareStatement("select * from "+ spliceTableWatcher+" where name = ?");
        test.setString(1,"sfines");
        ResultSet rs = test.executeQuery();
        Assert.assertTrue("Cannot lookup sfines by primary key", rs.next());
    }

    @Test(timeout=10000)
//    @Ignore("Bug 336")
    public void scanningPrimaryKeyTableByPkOnly() throws Exception{
        ResultSet rs = methodWatcher.executeQuery("select name from "+ spliceTableWatcher+" where name = 'sfines'");
        Assert.assertTrue("Cannot lookup sfines by primary key ",rs.next());
    }

    @Test(timeout=10000)
    public void testCanRetrievePrimaryKeysFromMetadata() throws Exception{
        ResultSet rs = methodWatcher.getOrCreateConnection().getMetaData().getPrimaryKeys(null,CLASS_NAME,TABLE_NAME);
        List<String> results = Lists.newArrayList();
        while(rs.next()){
            String tableCat = rs.getString(1);
            String tableSchem = rs.getString(2);
            String tableName = rs.getString(3);
            String colName = rs.getString(4);
            short keySeq = rs.getShort(5);
            String pkName = rs.getString(6);
            Assert.assertNotNull("No Table name returned",tableName);
            Assert.assertNotNull("No Column name returned",colName);
            Assert.assertNotNull("No Pk Name returned",pkName);
            results.add(String.format("cat:%s,schema:%s,table:%s,column:%s,pk:%s,seqNum:%d",
                    tableCat,tableSchem,tableName,colName,pkName,keySeq));
        }
        Assert.assertTrue("No Pks returned!",results.size()>0);
    }

    @Test(timeout=10000)
    public void testCall() throws Exception{
        PreparedStatement ps = methodWatcher.prepareStatement("SELECT CAST ('' AS VARCHAR(128)) AS TABLE_CAT, " +
                "                   S.SCHEMANAME AS TABLE_SCHEM, T.TABLENAME AS TABLE_NAME, " +
                "                   COLS.COLUMNNAME AS COLUMN_NAME, " +
                "                   CONS.CONSTRAINTNAME AS PK_NAME " +
                "        FROM --DERBY-PROPERTIES joinOrder=FIXED \n " +
                "                        SYS.SYSTABLES T --DERBY-PROPERTIES index='SYSTABLES_INDEX1' \n" +
                "                        , SYS.SYSSCHEMAS S --DERBY-PROPERTIES joinStrategy=NESTEDLOOP, index ='SYSSCHEMAS_INDEX1'  \n" +
                "                        , SYS.SYSCONSTRAINTS CONS --DERBY-PROPERTIES joinStrategy=NESTEDLOOP, index ='SYSCONSTRAINTS_INDEX3'  \n" +
                "                        , SYS.SYSPRIMARYKEYS KEYS \n" +
                "                        , SYS.SYSCONGLOMERATES CONGLOMS --DERBY-PROPERTIES joinStrategy=NESTEDLOOP, index = 'SYSCONGLOMERATES_INDEX1' \n" +
                "                        , SYS.SYSCOLUMNS COLS --DERBY-PROPERTIES joinStrategy=NESTEDLOOP, index ='SYSCOLUMNS_INDEX1' \n" +
                "        WHERE ((1=1) OR ? IS NOT NULL) AND S.SCHEMANAME LIKE ? AND T.TABLENAME=? AND " +
                "                  T.SCHEMAID = S.SCHEMAID AND   " +
                "                  T.TABLEID = COLS.REFERENCEID AND T.TABLEID = CONGLOMS.TABLEID AND " +
                "                  CONS.TABLEID = T.TABLEID AND CONS.TYPE = 'P' AND " +
                "                  CONS.CONSTRAINTID = KEYS.CONSTRAINTID AND " +
                "                  KEYS.CONGLOMERATEID = CONGLOMS.CONGLOMERATEID ");
        ps.setString(1,"%");
        ps.setString(2,CLASS_NAME);
        ps.setString(3,TABLE_NAME);
        ResultSet rs = ps.executeQuery();
        List<String> results = Lists.newArrayList();
        while(rs.next()){
            String tableCat = rs.getString(1);
            String tableSchem = rs.getString(2);
            String tableName = rs.getString(3);
            String colName = rs.getString(4);
            String pkName = rs.getString(5);
            Assert.assertNotNull("No Table name returned",tableName);
            Assert.assertNotNull("No Column name returned",colName);
            Assert.assertNotNull("No Pk Name returned",pkName);
            results.add(String.format("cat:%s,schema:%s,table:%s,column:%s,pk:%s",
                    tableCat,tableSchem,tableName,colName,pkName));
        }
        Assert.assertTrue("No Pks returned!",results.size()>0);
    }

}
