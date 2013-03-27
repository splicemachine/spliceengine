package org.apache.derby.impl.sql.execute.operations;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;

import junit.framework.Assert;

import org.apache.log4j.Logger;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.splicemachine.derby.test.DerbyTestRule;

/**
 * @author Scott Fines
 *         Created on: 3/1/13
 */
public class PrimaryKeyTest {
    private static final Logger LOG = Logger.getLogger(PrimaryKeyTest.class);

    private static final Map<String,String> tableMap = Maps.newHashMap();
    static{
        tableMap.put("t","name varchar(50),val int, PRIMARY KEY(name)");
//        tableMap.put("multi_pk","name varchar(50),fruit varchar(30),val int, PRIMARY KEY(name,fruit)");
    }

    @Rule public static DerbyTestRule rule = new DerbyTestRule(tableMap,LOG);

    @BeforeClass
    public static void startup() throws Exception{
        DerbyTestRule.start();
    }

    @AfterClass
    public static void shutdown() throws Exception{
        DerbyTestRule.shutdown();
    }

    @Test
    public void cannotInsertDuplicatePks() throws Exception{
        /*
         * Test to ensure that duplicate Primary Key combinations are not allowed.
         */
        PreparedStatement ps = rule.prepareStatement("insert into t (name, val) values (?,?)");

        //insert the first entry in the row
        ps.setString(1,"sfines");
        ps.setInt(2,1);
        ps.executeUpdate();

        LOG.info("First insertion completed without error, checking that it succeeded");
        //validate that a single row exists
        PreparedStatement validator = rule.prepareStatement("select * from t where name = ?");
        validator.setString(1,"sfines");
        ResultSet rs = validator.executeQuery();
        int matchCount=0;
        while(rs.next()){
            if("sfines".equalsIgnoreCase(rs.getString(1))){
                matchCount++;
            }
        }
        Assert.assertEquals("Incorrect number of matching rows found!",1,matchCount);

        LOG.info("Attempting second insertion");
        //try and insert it again
        boolean failed=false;
        try{
            ps.executeUpdate();
        }catch(SQLException se){
            LOG.info(se.getMessage());
            failed=true;
        }
        Assert.assertTrue("Insert of duplicate records succeeded!",failed);

        LOG.info("Validating second insertion failed");
        //make sure that it didn't get inserted anyway
        rs = validator.executeQuery();
        matchCount=0;
        while(rs.next()){
            if("sfines".equalsIgnoreCase(rs.getString(1))){
                matchCount++;
            }
        }
        Assert.assertEquals("Incorrect number of matching rows found!",1,matchCount);
        LOG.info("Validation succeeded");
    }

    @Test
    public void canUpdatePrimaryKeyCorrectly() throws Exception{
        PreparedStatement ps = rule.prepareStatement("insert into t (name, val) values (?,?)");

        //insert the first entry in the row
        ps.setString(1,"sfines");
        ps.setInt(2,1);
        ps.executeUpdate();

        LOG.info("First insertion completed without error, checking that it succeeded");
        //validate that a single row exists
        PreparedStatement validator = rule.prepareStatement("select * from t where name = ?");
        validator.setString(1,"sfines");
        ResultSet rs = validator.executeQuery();
        int matchCount=0;
        while(rs.next()){
            if("sfines".equalsIgnoreCase(rs.getString(1))){
                matchCount++;
            }
        }
        rs.close();
        Assert.assertEquals("Incorrect number of matching rows found!",1,matchCount);

        //update it to change the primary key
        PreparedStatement updateStatement = rule.prepareStatement("update t set name = ? where name = ?");
        updateStatement.setString(1,"jzhang");
        updateStatement.setString(2,"sfines");
        updateStatement.executeUpdate();

        LOG.info("Update completed without error, checking success");

        validator = rule.prepareStatement("select * from t where name = ?");
        validator.setString(1,"sfines");
        rs = validator.executeQuery();
        while(rs.next()){
            Assert.fail("Should have returned nothing");
        }
        rs.close();

        validator = rule.prepareStatement("select * from t where name = ?");
        validator.setString(1,"jzhang");
        rs = validator.executeQuery();
        matchCount =0;
        while(rs.next()){
            if("jzhang".equalsIgnoreCase(rs.getString(1))){
                matchCount++;
                int val = rs.getInt(2);
                Assert.assertEquals("Column incorrect!",1,val);
            }
        }
        Assert.assertEquals("Incorrect number of updated rows!",1,matchCount);
    }

    @Test
    public void canUpdateNonPrimaryKeyCorrectly() throws Exception{
        PreparedStatement ps = rule.prepareStatement("insert into t (name, val) values (?,?)");

        //insert the first entry in the row
        ps.setString(1,"sfines");
        ps.setInt(2,1);
        ps.executeUpdate();

        LOG.info("First insertion completed without error, checking that it succeeded");
        //validate that a single row exists
        PreparedStatement validator = rule.prepareStatement("select * from t where name = ?");
        validator.setString(1,"sfines");
        ResultSet rs = validator.executeQuery();
        int matchCount=0;
        while(rs.next()){
            if("sfines".equalsIgnoreCase(rs.getString(1))){
                matchCount++;
            }
        }
        rs.close();
        Assert.assertEquals("Incorrect number of matching rows found!",1,matchCount);

        //update it to change the primary key
        PreparedStatement updateStatement = rule.prepareStatement("update t set val = ? where name = ?");
        updateStatement.setInt(1,20);
        updateStatement.setString(2,"sfines");
        updateStatement.executeUpdate();

        LOG.info("Update completed without error, checking success");

        validator = rule.prepareStatement("select * from t where name = ?");
        validator.setString(1,"sfines");
        rs = validator.executeQuery();
        matchCount =0;
        while(rs.next()){
            if("sfines".equalsIgnoreCase(rs.getString(1))){
                matchCount++;
                int val = rs.getInt(2);
                Assert.assertEquals("Column incorrect!",20,val);
            }
        }
        Assert.assertEquals("Incorrect number of updated rows!",1,matchCount);
    }

    @Test
    public void scanningPrimaryKeyTable() throws Exception{
        PreparedStatement test = rule.prepareStatement("select * from t where name = ?");
        test.setString(1,"sfines");
        ResultSet rs = test.executeQuery();
        if(rs.next());
    }

    @Test
    public void scanningPrimaryKeyTableByPk() throws Exception{
        PreparedStatement test = rule.prepareStatement("select name from t where name = ?");
        test.setString(1,"sfines");
        ResultSet rs = test.executeQuery();
        if(rs.next());
    }

    @Test
    public void testCanRetrievePrimaryKeysFromMetadata() throws Exception{
        ResultSet rs = rule.getConnection().getMetaData().getPrimaryKeys(null,null,"T");
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
        for(String result:results){
            LOG.info(result);
        }

        Assert.assertTrue("No Pks returned!",results.size()>0);
    }

    @Test
    public void testCall() throws Exception{
        PreparedStatement ps = rule.prepareStatement("SELECT CAST ('' AS VARCHAR(128)) AS TABLE_CAT, " +
                "                   S.SCHEMANAME AS TABLE_SCHEM, T.TABLENAME AS TABLE_NAME, " +
                "                   COLS.COLUMNNAME AS COLUMN_NAME, " +
//                "                   CAST (CONGLOMS.DESCRIPTOR.getKeyColumnPosition(COLS.COLUMNNUMBER) AS SMALLINT) AS KEY_SEQ, " +
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
//                "                  (CASE WHEN CONGLOMS.DESCRIPTOR IS NOT NULL THEN " +
//                "                                CONGLOMS.DESCRIPTOR.getKeyColumnPosition(COLS.COLUMNNUMBER) ELSE " +
//                "                                0 END) <> 0 AND " +
                "                  KEYS.CONGLOMERATEID = CONGLOMS.CONGLOMERATEID ");
        ps.setString(1,"%");
        ps.setString(2,"%");
        ps.setString(3,"T");
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
        for(String result:results){
            LOG.info(result);
        }

        Assert.assertTrue("No Pks returned!",results.size()>0);
    }
}
