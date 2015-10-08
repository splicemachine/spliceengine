package com.splicemachine.derby.impl.sql.execute.operations;


import static com.splicemachine.test_tools.Rows.row;
import static com.splicemachine.test_tools.Rows.rows;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;
import org.junit.runner.Description;

import com.splicemachine.derby.management.XPlainTreeNode;
import com.splicemachine.derby.test.framework.SpliceDataWatcher;
import com.splicemachine.derby.test.framework.SpliceSchemaWatcher;
import com.splicemachine.derby.test.framework.SpliceTableWatcher;
import com.splicemachine.derby.test.framework.SpliceUnitTest;
import com.splicemachine.derby.test.framework.SpliceViewWatcher;
import com.splicemachine.derby.test.framework.SpliceWatcher;
import com.splicemachine.derby.test.framework.SpliceXPlainTrace;
import com.splicemachine.derby.test.framework.TestConnection;
import com.splicemachine.homeless.TestUtils;
import com.splicemachine.test_dao.TableDAO;
import com.splicemachine.test_tools.TableCreator;

/**
 *
 * Created by jyuan on 7/30/14.
 */
public class WindowFunctionIT extends SpliceUnitTest {
    private static final String SCHEMA = WindowFunctionIT.class.getSimpleName().toUpperCase();

    private static SpliceWatcher spliceClassWatcher = new SpliceWatcher();
    private static SpliceSchemaWatcher spliceSchemaWatcher = new SpliceSchemaWatcher(SCHEMA);

    private static String empTabDef = "(empnum int, dept int, salary int)";
    private static final String EMPTAB = "EMPTAB";
    private static SpliceTableWatcher empTabTableWatcher = new SpliceTableWatcher(EMPTAB, SCHEMA, empTabDef);

    private static String[] EMPTAB_ROWS = {
        "20,1,75000",
        "70,1,76000",
        "60,1,78000",
        "110,1,53000",
        "50,1,52000",
        "55,1,52000",
        "10,1,50000",
        "90,2,51000",
        "40,2,52000",
        "44,2,52000",
        "49,2,53000",
        "80,3,79000",
        "100,3,55000",
        "120,3,75000",
        "30,3,84000"
    };

    private static String perchacedDef = "(item int, price double, date timestamp)";
    private static final String PURCHASED = "purchased";
    private static SpliceTableWatcher purchacedTableWatcher = new SpliceTableWatcher(PURCHASED, SCHEMA, perchacedDef);

    private static String[] PURCHASED_ROWS = {
        "1, 1.0, '2014-09-08 18:27:48.881'",
        "1, 1.0, '2014-09-08 17:45:15.204'",
        "1, 7.0, '2014-09-08 18:33:46.446'",
        "2, 12.0, '2014-09-08 18:40:15.480'",
        "2, 5.0, '2014-09-08 18:26:51.387'",
        "2, 6.0, '2014-09-08 17:50:17.182'",
        "3, 10.0, '2014-09-08 18:25:42.387'",
        "3, 7.0, '2014-09-08 18:00:44.742'",
        "3, 3.0, '2014-09-08 17:36:55.414'",
        "4, 10.0, '2014-09-08 17:50:17.182'",
        "4, 2.0, '2014-09-08 18:05:47.166'",
        "4, 8.0, '2014-09-08 18:08:04.986'",
        "5, 4.0, '2014-09-08 17:46:26.428'",
        "5, 10.0, '2014-09-08 18:11:23.645'",
        "5, 11.0, '2014-09-08 17:41:56.353'"
    };

    private static String peopleDef = "(PersonID int,FamilyID int,FirstName varchar(10),LastName varchar(25),DOB timestamp)";
    private static final String PEOPLE = "people";
    private static SpliceTableWatcher peopleTableWatcher = new SpliceTableWatcher(PEOPLE, SCHEMA, peopleDef);

    private static String[] PEOPLE_ROWS = {
        "1,1,'Joe','Johnson', '2000-10-23 13:00:00'",
        "2,1,'Jim','Johnson','2001-12-15 05:45:00'",
        "3,2,'Karly','Matthews','2000-05-20 04:00:00'",
        "4,2,'Kacy','Matthews','2000-05-20 04:02:00'",
        "5,2,'Tom','Matthews','2001-09-15 11:52:00'",
        "1,1,'Joe','Johnson', '2000-10-23 13:00:00'",
        "2,1,'Jim','Johnson','2001-12-15 05:45:00'",
        "3,2,'Karly','Matthews','2000-05-20 04:00:00'",
        "5,2,'Tom','Matthews','2001-09-15 11:52:00'",
        "1,1,'Joe','Johnson', '2000-10-23 13:00:00'",
        "2,1,'Jim','Johnson','2001-12-15 05:45:00'"
    };

    private static String empDef = "(EMPNO NUMERIC(4) NOT NULL, ENAME VARCHAR(10), JOB VARCHAR(9), MGR numeric(4), HIREDATE DATE, SAL NUMERIC(7, 2), COMM numeric(7, 2), DEPTNO numeric(2))";
    private static final String EMP = "emp";
    private static SpliceTableWatcher empTableWatcher = new SpliceTableWatcher(EMP, SCHEMA, empDef);

    private static String[] EMP_ROWS = {
        "7369, 'SMITH', 'CLERK',    7902, '1980-12-17', 800, NULL, 20",
        "7499, 'ALLEN', 'SALESMAN', 7698, '1981-02-20', 1600, 300, 30",
        "7521, 'WARD',  'SALESMAN', 7698, '1981-02-22', 1250, 500, 30",
        "7566, 'JONES', 'MANAGER',  7839, '1981-04-02', 2975, NULL, 20",
        "7654, 'MARTIN', 'SALESMAN', 7698,'1981-09-28', 1250, 1400, 30",
        "7698, 'BLAKE', 'MANAGER', 7839,'1981-05-01', 2850, NULL, 30",
        "7782, 'CLARK', 'MANAGER', 7839,'1981-06-09', 2450, NULL, 10",
        "7788, 'SCOTT', 'ANALYST', 7566,'1982-12-09', 3000, NULL, 20",
        "7839, 'KING', 'PRESIDENT', NULL,'1981-11-17', 5000, NULL, 10",
        "7844, 'TURNER', 'SALESMAN', 7698,'1981-09-08', 1500, 0, 30",
        "7876, 'ADAMS', 'CLERK', 7788,'1983-01-12', 1100, NULL, 20",
        "7900, 'JAMES', 'CLERK', 7698,'1981-12-03', 950, NULL, 30",
        "7902, 'FORD', 'ANALYST', 7566,'1981-12-03', 3000, NULL, 20",
        "7934, 'MILLER', 'CLERK', 7782,'1982-01-23', 1300, NULL, 10"
    };

    private static final String YEAR_VIEW = "YEAR_VIEW";
    private static String viewYearDef = String.format("as select to_char(hiredate,'yy') as yr,ename,hiredate from %s.%s group by hiredate,ename",
                                                      SCHEMA, EMP);
    private static SpliceViewWatcher yearView = new SpliceViewWatcher(YEAR_VIEW, SCHEMA, viewYearDef);

    private static String departmentosDef = "(ID INT generated always as identity (START WITH 1, INCREMENT BY 1) PRIMARY KEY, Nome_Dep VARCHAR(200))";
    private static final String DEPARTAMENTOS = "Departamentos";
    private static SpliceTableWatcher departmentosTableWatcher = new SpliceTableWatcher(DEPARTAMENTOS, SCHEMA, departmentosDef);

    private static String[] DEPT_ROWS = {
        "'Vendas'",
        "'IT'",
        "'Recursos Humanos'"
    };

    private static String functionariosDef = "(ID INT generated always as identity (START WITH 1, INCREMENT BY 1) PRIMARY KEY, ID_Dep INT, Nome VARCHAR(200), Salario Numeric(18,2))";
    private static final String FUNCIONARIOS = "Funcionarios";
    private static SpliceTableWatcher functionariosTableWatcher = new SpliceTableWatcher(FUNCIONARIOS, SCHEMA, functionariosDef);

    private static String[] FUNC_ROWS = {
        "1, 'Fabiano', 2000",
        "1, 'Amorim', 2500",
        "1, 'Diego', 9000",
        "2, 'Felipe', 2000",
        "2, 'Ferreira', 2500",
        "2, 'Nogare', 11999",
        "3, 'Laerte', 5000",
        "3, 'Luciano', 23500",
        "3, 'Zavaschi', 13999"
    };

    private static String best_addr_freqDef = "(INDIVIDUAL_ID bigint, RESIDENCE_ID bigint, FILE_ID integer, GENERATION_QUALIFIER varchar(15), FIRST_NAME varchar(15), LAST_NAME varchar(15), US_ADDRESS_LINE_1 varchar(100), US_ADDRESS_LINE_2 varchar(100), US_ADDRESS_LINE_3 varchar(100), US_STATE varchar(100), US_CITY varchar(20), US_ZIP  varchar(10), US_ZIP4 varchar(10), BRAND integer, FREQUENCY bigint, RUS_NCOA_EFFECTIVE_DATE timestamp, RSOURCERECORD_POOL integer, RSOURCERECORD_SUM integer, SOURCE_DATE timestamp, RECORD_ID bigint, RWRNK bigint)";
    private static final String best_addr_freq_NAME = "best_addr_freq";
    private static SpliceTableWatcher spliceTableWatcherBest_addr_freq = new SpliceTableWatcher(best_addr_freq_NAME, SCHEMA, best_addr_freqDef);

    private static final String best_addr_freq_insert = "insert into %s (INDIVIDUAL_ID, RESIDENCE_ID, FILE_ID, GENERATION_QUALIFIER, FIRST_NAME, LAST_NAME, US_ADDRESS_LINE_1, US_ADDRESS_LINE_2, US_ADDRESS_LINE_3, US_STATE, US_CITY, US_ZIP, US_ZIP4, BRAND, FREQUENCY, RUS_NCOA_EFFECTIVE_DATE, RSOURCERECORD_POOL, RSOURCERECORD_SUM, SOURCE_DATE, RECORD_ID, RWRNK) values (%s)";
    private static String[] best_addr_freqRows = {
        "1,  7750, 1, 'a', 'a', 'a',        'Third Street', 'Suite 250', ' ', 'CA', 'San Francisco', '94105', '3333', 222, 1, '1900-01-01 00:00:00.0', 0, 0, NULL, 1, 1",
        "1, 29308, 1, 'a', 'a', 'a',       'Second Street', 'Suite 300', ' ', 'CA', 'San Francisco', '94105', '2222', 222, 1, '1900-01-01 00:00:00.0', 0, 0, NULL, 1, 1",
        "1,  7291, 1, 'a', 'a', 'a',       'Second Street', 'Suite 300', ' ', 'CA', 'San Francisco', '94105', '2222', 222, 1, '1900-01-01 00:00:00.0', 0, 0, NULL, 1, 1",
        "1, 17020, 1, 'a', 'a', 'a',        'Third Street', 'Suite 250', ' ', 'CA', 'San Francisco', '94105', '3333', 222, 1, '1900-01-01 00:00:00.0', 0, 0, NULL, 1, 1",
        "1, 20091, 1, 'a', 'a', 'a',        'Third Street', 'Suite 250', ' ', 'CA', 'San Francisco', '94105', '3333', 222, 1, '1900-01-01 00:00:00.0', 0, 0, NULL, 1, 1",
        "1, 15865, 1, 'a', 'a', 'a',         'Turtle Rock',  '---', ' ', 'CA', 'Irvine', '96789', '5555', 222, 1, '1900-01-01 00:00:00.0', 0, 0, NULL, 1, 1",
        "1, 30459, 1, 'a', 'a', 'a',         'Turtle Rock',  '---', ' ', 'CA', 'Irvine', '96789', '5555', 222, 1, '1900-01-01 00:00:00.0', 0, 0, NULL, 1, 1",
        "1,   570, 1, 'a', 'a', 'a',       'Howard Street', 'Financial District', ' ', 'CA', 'San Francisco', '94105', '1111', 222, 1, '1900-01-01 00:00:00.0', 0, 0, NULL, 1, 1",
        "1, 22471, 1, 'a', 'a', 'a',       'Howard Street', 'Financial District', ' ', 'CA', 'San Francisco', '94105', '1111', 222, 1, '1900-01-01 00:00:00.0', 0, 0, NULL, 1, 1",
        "1,  1337, 1, 'a', 'a', 'a', '19000 Stevens Creek', 'Suite A', ' ', 'CA', 'Cupertino', '95014', '8765', 222, 1, '1900-01-01 00:00:00.0', 0, 0, NULL, 1, 1"
    };

    private static String best_ids_poolDef = "(INDIVIDUAL_ID bigint, RESIDENCE_ID bigint, HOUSEHOLD_ID bigint, FILE_ID integer, BUSINESS_IND boolean, CREATED_BY varchar(100), CREATED_DATE timestamp, JOB_ID integer, BRAND integer, GENERATION_QUALIFIER varchar(15), FIRST_NAME varchar(25), LAST_NAME varchar(25), RSOURCERECORD_POOL integer, RSOURCERECORD_SUM integer, SOURCE_DATE timestamp, RECORD_ID bigint, US_ADDRESS_LINE_1 varchar(100), US_ADDRESS_LINE_2 varchar(100), US_ADDRESS_LINE_3 varchar(100), US_STATE varchar(100), US_CITY varchar(20), US_ZIP  varchar(10), US_ZIP4 varchar(10), RUS_NCOA_EFFECTIVE_DATE timestamp, RUS_NCOA_MOVE_CODE integer, RADDRESS_QUALITY integer, RPRIORITY integer, RINVALID_MOVE_IND integer, ROWRANK bigint)";
    private static final String best_ids_pool_NAME = "best_ids_pool";
    private static SpliceTableWatcher spliceTableWatcherBest_ids_pool = new SpliceTableWatcher(best_ids_pool_NAME, SCHEMA, best_ids_poolDef);

    private static final String best_ids_pool_insert = "insert into %s (INDIVIDUAL_ID ,RESIDENCE_ID ,HOUSEHOLD_ID ,FILE_ID ,BUSINESS_IND ,CREATED_BY ,CREATED_DATE ,JOB_ID ,BRAND ,GENERATION_QUALIFIER ,FIRST_NAME ,LAST_NAME ,RSOURCERECORD_POOL ,RSOURCERECORD_SUM ,SOURCE_DATE ,RECORD_ID ,US_ADDRESS_LINE_1 ,US_ADDRESS_LINE_2 ,US_ADDRESS_LINE_3 ,US_STATE ,US_CITY ,US_ZIP ,US_ZIP4 ,RUS_NCOA_EFFECTIVE_DATE ,RUS_NCOA_MOVE_CODE ,RADDRESS_QUALITY ,RPRIORITY ,RINVALID_MOVE_IND ,ROWRANK ) values (%s)";
    private static String[] best_ids_poolDefRows = {
        "1,  8416, 1, 1, NULL, 'a', NULL, 1, 222, 'a', 'a', 'a', 0, 0, NULL, 1, 'Howard Street', 'Financial District', ' ', 'CA', 'San Francisco', '94105', '1111', '1900-01-01 00:00:00.0', 0, 0, 333, 0, 325",
        "1, 26084, 1, 1, NULL, 'a', NULL, 1, 222, 'a', 'a', 'a', 0, 0, NULL, 1, 'Howard Street', 'Financial District', ' ', 'CA', 'San Francisco', '94105', '1111', '1900-01-01 00:00:00.0', 0, 0, 333, 0, 326",
        "1, 30183, 1, 1, NULL, 'a', NULL, 1, 222, 'a', 'a', 'a', 0, 0, NULL, 1, 'Howard Street', 'Financial District', ' ', 'CA', 'San Francisco', '94105', '1111', '1900-01-01 00:00:00.0', 0, 0, 333, 0, 328",
        "1,  4964, 1, 1, NULL, 'a', NULL, 1, 222, 'a', 'a', 'a', 0, 0, NULL, 1, 'Howard Street', 'Financial District', ' ', 'CA', 'San Francisco', '94105', '1111', '1900-01-01 00:00:00.0', 0, 0, 333, 0, 288",
        "1,  5034, 1, 1, NULL, 'a', NULL, 1, 222, 'a', 'a', 'a', 0, 0, NULL, 1, 'Howard Street', 'Financial District', ' ', 'CA', 'San Francisco', '94105', '1111', '1900-01-01 00:00:00.0', 0, 0, 333, 0, 310",
        "1, 32175, 1, 1, NULL, 'a', NULL, 1, 222, 'a', 'a', 'a', 0, 0, NULL, 1, 'Howard Street', 'Financial District', ' ', 'CA', 'San Francisco', '94105', '1111', '1900-01-01 00:00:00.0', 0, 0, 333, 0, 311",
        "1, 21939, 1, 1, NULL, 'a', NULL, 1, 222, 'a', 'a', 'a', 0, 0, NULL, 1, 'Howard Street', 'Financial District', ' ', 'CA', 'San Francisco', '94105', '1111', '1900-01-01 00:00:00.0', 0, 0, 333, 0, 312",
        "1, 21945, 1, 1, NULL, 'a', NULL, 1, 222, 'a', 'a', 'a', 0, 0, NULL, 1, 'Howard Street', 'Financial District', ' ', 'CA', 'San Francisco', '94105', '1111', '1900-01-01 00:00:00.0', 0, 0, 333, 0, 314",
        "1, 14527, 1, 1, NULL, 'a', NULL, 1, 222, 'a', 'a', 'a', 0, 0, NULL, 1, 'Howard Street', 'Financial District', ' ', 'CA', 'San Francisco', '94105', '1111', '1900-01-01 00:00:00.0', 0, 0, 333, 0, 315",
        "1, 19908, 1, 1, NULL, 'a', NULL, 1, 222, 'a', 'a', 'a', 0, 0, NULL, 1, 'Howard Street', 'Financial District', ' ', 'CA', 'San Francisco', '94105', '1111', '1900-01-01 00:00:00.0', 0, 0, 333, 0, 316"
    };

    private static String empTabHireDateDef = "(empnum int, dept int, salary int, hiredate date)";
    private static final String EMPTAB_HIRE_DATE = "EMPTAB_HIRE_DATE";
    private static SpliceTableWatcher empTabHireDateTableWatcher = new SpliceTableWatcher(EMPTAB_HIRE_DATE, SCHEMA, empTabHireDateDef);

    private static String[] EMPTAB_HIRE_DATE_ROWS = {
        "20,1,75000,'2012-11-11'",
        "70,1,76000,'2012-04-03'",
        "60,1,78000,'2014-03-04'",
        "110,1,53000,'2010-03-20'",
        "50,1,52000,'2011-05-24'",
        "55,1,52000,'2011-10-15'",
        "10,1,50000,'2010-03-20'",
        "90,2,51000,'2012-04-03'",
        "40,2,52000,'2013-06-06'",
        "44,2,52000,'2013-12-20'",
        "49,2,53000,'2012-04-03'",
        "80,3,79000,'2013-04-24'",
        "100,3,55000,'2010-04-12'",
        "120,3,75000,'2012-04-03'",
        "30,3,84000,'2010-08-09'",
    };

    private static final String EMPTAB_NULLS = "EMPTAB_NULLS";
    private static SpliceTableWatcher empTabNullsTableWatcher = new SpliceTableWatcher(EMPTAB_NULLS, SCHEMA, empTabHireDateDef);

    private static String[] EMPTAB_ROWS_NULL = {
        "20,1,75000,'2012-11-11'",
        "70,1,76000,'2012-04-03'",
        "60,1,78000,'2014-03-04'",
        "110,1,53000,'2010-03-20'",
        "50,1,52000,'2011-05-24'",
        "55,1,52000,'2011-10-15'",
        "10,1,50000,'2010-03-20'",
        "90,2,51000,'2012-04-03'",
        "40,2,52000,'2013-06-06'",
        "44,2,52000,'2013-12-20'",
        "49,2,53000,'2012-04-03'",
        "80,3,79000,'2013-04-24'",
        "100,3,55000,'2010-04-12'",
        "120,3,75000,'2012-04-03'",
        "30,3,84000,'2010-08-09'",
        "32,1,null,'2010-08-09'",
        "33,3,null,'2010-08-09'"
    };

    private static String txnDetailDef = "(SOURCE_SALES_INSTANCE_ID BIGINT, TRANSACTION_DT DATE NOT NULL, ORIGINAL_SKU_CATEGORY_ID INTEGER, SALES_AMT DECIMAL(9,2), CUSTOMER_MASTER_ID BIGINT)";
    private static final String TXN_DETAIL = "TXN_DETAIL";
    private static SpliceTableWatcher txnDetailTableWatcher = new SpliceTableWatcher(TXN_DETAIL, SCHEMA, txnDetailDef);

    private static String[] TXN_DETAIL_ROWS = {
        "0,'2013-05-12',44871,329.18,74065939",
        "0,'2013-05-12',44199,35.46,74065939",
        "0,'2013-05-12',44238,395.44,74065939",
        "0,'2013-05-12',44410,1763.41,74065939",
        "0,'2013-05-12',44797,915.97,74065939",
        "0,'2013-05-12',44837,179.88,74065939",
        "0,'2013-05-12',44600,0,74065939",
        "0,'2013-05-12',44880,467.33,74065939"
    };

    private static String allSalesDef = "(years INTEGER, month INTEGER, prd_type_id INTEGER, emp_id INTEGER , amount NUMERIC(8, 2))";
    private static final String ALL_SALES = "ALL_SALES";
    private static SpliceTableWatcher allSalesTableWatcher = new SpliceTableWatcher(ALL_SALES, SCHEMA, allSalesDef);

    private static String[] ALL_SALES_ROWS = {
        "2006,1,1,21,16034.84",
        "2006,2,1,21,15644.65",
        "2006,3,2,21,20167.83",
        "2006,4,2,21,25056.45",
        "2006,5,2,21,NULL",
        "2006,6,1,21,15564.66",
        "2006,7,1,21,15644.65",
        "2006,8,1,21,16434.82",
        "2006,9,1,21,19654.57",
        "2006,10,1,21,21764.19",
        "2006,11,1,21,13026.73",
        "2006,12,2,21,10034.64",
        "2005,1,2,22,16634.84",
        "2005,1,2,21,26034.84",
        "2005,2,1,21,12644.65",
        "2005,3,1,21,NULL",
        "2005,4,1,21,25026.45",
        "2005,5,1,21,17212.66",
        "2005,6,1,21,15564.26",
        "2005,7,2,21,62654.82",
        "2005,8,2,21,26434.82",
        "2005,9,2,21,15644.65",
        "2005,10,2,21,21264.19",
        "2005,11,1,21,13026.73",
        "2005,12,1,21,10032.64"
    };

    @ClassRule
    public static TestRule chain = RuleChain.outerRule(spliceClassWatcher)
                                            .around(spliceSchemaWatcher)
                                            .around(empTabTableWatcher)
                                            .around(new SpliceDataWatcher() {
                                                @Override
                                                protected void starting(Description description) {
                                                    PreparedStatement ps;
                                                    try {
                                                        for (String row : EMPTAB_ROWS) {
                                                            ps = spliceClassWatcher.prepareStatement(
                                                                String.format("insert into %s values (%s)", empTabTableWatcher, row));
                                                            ps.execute();
                                                        }
                                                    } catch (Exception e) {
                                                        throw new RuntimeException(e);
                                                    }
                                                }})
                                            .around(purchacedTableWatcher)
                                            .around(new SpliceDataWatcher() {
                                                @Override
                                                protected void starting(Description description) {
                                                    PreparedStatement ps;
                                                    try {
                                                        for (String row : PURCHASED_ROWS) {
                                                            String sql = String.format("insert into %s values (%s)",
                                                                                       purchacedTableWatcher, row);
//                            System.out.println(sql+";");  // will print insert statements
                                                            ps = spliceClassWatcher.prepareStatement(sql);
                                                            ps.execute();
                                                        }
                                                    } catch (Exception e) {
                                                        throw new RuntimeException(e);
                                                    }
                                                }
                                            })
                                            .around(peopleTableWatcher)
                                            .around(new SpliceDataWatcher() {
                                                @Override
                                                protected void starting(Description description) {
                                                    PreparedStatement ps;
                                                    try {
                                                        for (String row : PEOPLE_ROWS) {
                                                            ps = spliceClassWatcher.prepareStatement(
                                                                String.format("insert into %s values (%s)",
                                                                              peopleTableWatcher, row));
                                                            ps.execute();
                                                        }
                                                    } catch (Exception e) {
                                                        throw new RuntimeException(e);
                                                    }
                                                }
                                            })
                                            .around(empTableWatcher)
                                            .around(new SpliceDataWatcher() {
                                                @Override
                                                protected void starting(Description description) {
                                                    PreparedStatement ps;
                                                    try {
                                                        for (String row : EMP_ROWS) {
                                                            ps = spliceClassWatcher.prepareStatement(
                                                                String.format("insert into %s values (%s)",
                                                                              empTableWatcher, row));
                                                            ps.execute();
                                                        }
                                                    } catch (Exception e) {
                                                        throw new RuntimeException(e);
                                                    }
                                                }
                                            })
                                            .around(yearView)
                                            .around(departmentosTableWatcher)
                                            .around(new SpliceDataWatcher() {
                                                @Override
                                                protected void starting(Description description) {
                                                    PreparedStatement ps;
                                                    try {
                                                        for (String row : DEPT_ROWS) {
                                                            ps = spliceClassWatcher.prepareStatement(
                                                                String.format("insert into %s (Nome_Dep) values (%s)" +
                                                                                  "", departmentosTableWatcher, row));
                                                            ps.execute();
                                                        }
                                                    } catch (Exception e) {
                                                        throw new RuntimeException(e);
                                                    }
                                                }
                                            })
                                            .around(functionariosTableWatcher)
                                            .around(new SpliceDataWatcher() {
                                                @Override
                                                protected void starting(Description description) {
                                                    PreparedStatement ps;
                                                    try {
                                                        for (String row : FUNC_ROWS) {
                                                            ps = spliceClassWatcher.prepareStatement(
                                                                String.format("insert into %s (ID_Dep, Nome, " +
                                                                                  "Salario) values (%s)",
                                                                              functionariosTableWatcher, row));
                                                            ps.execute();
                                                        }
                                                    } catch (Exception e) {
                                                        throw new RuntimeException(e);
                                                    }
                                                }
                                            })
                                            .around(spliceTableWatcherBest_addr_freq)
                                            .around(new SpliceDataWatcher() {
                                                @Override
                                                protected void starting(Description description) {
                                                    PreparedStatement ps;
                                                    try {
                                                        for (String row : best_addr_freqRows) {
                                                            ps = spliceClassWatcher.prepareStatement(
                                                                String.format(best_addr_freq_insert,
                                                                              spliceTableWatcherBest_addr_freq, row));
                                                            ps.execute();
                                                        }
                                                    } catch (Exception e) {
                                                        throw new RuntimeException(e);
                                                    }
                                                }
                                            })
                                            .around(spliceTableWatcherBest_ids_pool)
                                            .around(new SpliceDataWatcher() {
                                                @Override
                                                protected void starting(Description description) {
                                                    PreparedStatement ps;
                                                    try {
                                                        for (String row : best_ids_poolDefRows) {
                                                            ps = spliceClassWatcher.prepareStatement(
                                                                String.format(best_ids_pool_insert,
                                                                              spliceTableWatcherBest_ids_pool, row));
                                                            ps.execute();
                                                        }
                                                    } catch (Exception e) {
                                                        throw new RuntimeException(e);
                                                    }
                                                }
                                            })
                                            .around(empTabHireDateTableWatcher)
                                            .around(empTabNullsTableWatcher)
                                            .around(new SpliceDataWatcher() {
                                                @Override
                                                protected void starting(Description description) {
                                                    PreparedStatement ps;
                                                    try {
                                                        for (String row : EMPTAB_HIRE_DATE_ROWS) {
                                                            String sql = String.format("insert into %s values (%s)", empTabHireDateTableWatcher, row);
                                                            ps = spliceClassWatcher.prepareStatement(sql);
                                                            ps.execute();
                                                        }
                                                    } catch (Exception e) {
                                                        throw new RuntimeException(e);
                                                    }
                                                }
                                            })
                                            .around(new SpliceDataWatcher() {
                                                @Override
                                                protected void starting(Description description) {
                                                    PreparedStatement ps;
                                                    try {
                                                        for (String row : EMPTAB_ROWS_NULL) {
                                                            String sql = String.format("insert into %s values (%s)", empTabNullsTableWatcher, row);
                                                            ps = spliceClassWatcher.prepareStatement(sql);
                                                            ps.execute();
                                                        }
                                                    } catch (Exception e) {
                                                        throw new RuntimeException(e);
                                                    }
                                                }
                                            })
                                            .around(txnDetailTableWatcher)
                                            .around(new SpliceDataWatcher() {
                                                @Override
                                                protected void starting(Description description) {
                                                    PreparedStatement ps;
                                                    try {
                                                        for (String row : TXN_DETAIL_ROWS) {
                                                            ps = spliceClassWatcher.prepareStatement(
                                                                String.format("insert into %s values (%s)", txnDetailTableWatcher, row));
                                                            ps.execute();
                                                        }
                                                    } catch (Exception e) {
                                                        throw new RuntimeException(e);
                                                    }
                                                }
                                            })
                                            .around(allSalesTableWatcher)
                                            .around(new SpliceDataWatcher() {
                                                @Override
                                                protected void starting(Description description) {
                                                    PreparedStatement ps;
                                                    try {
                                                        for (String row : ALL_SALES_ROWS) {
                                                            ps = spliceClassWatcher.prepareStatement(
                                                                String.format("insert into %s values (%s)", allSalesTableWatcher, row));
                                                            ps.execute();
                                                        }
                                                    } catch (Exception e) {
                                                        throw new RuntimeException(e);
                                                    }
                                                }
                                            });

    @Rule
    public SpliceWatcher methodWatcher = new SpliceWatcher();

    @Test
    public void testMinRows2Preceding() throws Exception {
        String sqlText =
            String.format("SELECT empnum,dept,salary," +
                              "min(salary) over (Partition by dept ORDER BY salary ROWS 2 preceding) as minsal " +
                              "from %s order by empnum",
                          this.getTableReference(EMPTAB));

        ResultSet rs = methodWatcher.executeQuery(sqlText);

        String expected =
            "EMPNUM |DEPT |SALARY |MINSAL |\n" +
                "------------------------------\n" +
                "  10   |  1  | 50000 | 50000 |\n" +
                "  20   |  1  | 75000 | 52000 |\n" +
                "  30   |  3  | 84000 | 75000 |\n" +
                "  40   |  2  | 52000 | 51000 |\n" +
                "  44   |  2  | 52000 | 51000 |\n" +
                "  49   |  2  | 53000 | 52000 |\n" +
                "  50   |  1  | 52000 | 50000 |\n" +
                "  55   |  1  | 52000 | 50000 |\n" +
                "  60   |  1  | 78000 | 75000 |\n" +
                "  70   |  1  | 76000 | 53000 |\n" +
                "  80   |  3  | 79000 | 55000 |\n" +
                "  90   |  2  | 51000 | 51000 |\n" +
                "  100  |  3  | 55000 | 55000 |\n" +
                "  110  |  1  | 53000 | 52000 |\n" +
                "  120  |  3  | 75000 | 55000 |";

        assertEquals("\n"+sqlText+"\n", expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
        rs.close();
    }

    @Test
    public void testMaxRowsCurrent2Following()throws Exception{
        String sqlText =
            String.format("SELECT empnum,dept,salary," +
                              "max(salary) over (Partition by dept ORDER BY salary, empnum ROWS BETWEEN CURRENT ROW AND 2 FOLLOWING ) as maxsal " +
                              "from %s order by empnum",
                          this.getTableReference(EMPTAB));

        ResultSet rs = methodWatcher.executeQuery(sqlText);

        String expected =
            "EMPNUM |DEPT |SALARY |MAXSAL |\n" +
                "------------------------------\n" +
                "  10   |  1  | 50000 | 52000 |\n" +
                "  20   |  1  | 75000 | 78000 |\n" +
                "  30   |  3  | 84000 | 84000 |\n" +
                "  40   |  2  | 52000 | 53000 |\n" +
                "  44   |  2  | 52000 | 53000 |\n" +
                "  49   |  2  | 53000 | 53000 |\n" +
                "  50   |  1  | 52000 | 53000 |\n" +
                "  55   |  1  | 52000 | 75000 |\n" +
                "  60   |  1  | 78000 | 78000 |\n" +
                "  70   |  1  | 76000 | 78000 |\n" +
                "  80   |  3  | 79000 | 84000 |\n" +
                "  90   |  2  | 51000 | 52000 |\n" +
                "  100  |  3  | 55000 | 79000 |\n" +
                "  110  |  1  | 53000 | 76000 |\n" +
                "  120  |  3  | 75000 | 84000 |";
        assertEquals("\n"+sqlText+"\n", expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
        rs.close();
    }

    @Test
    public void testFrameRowsUnboundedPrecedingUnboundedFollowing() throws Exception {
        String sqlText =
            String.format("SELECT empnum, dept, salary, " +
                              "max(salary) over (Partition by dept ORDER BY salary, empnum rows between unbounded preceding and unbounded following) as maxsal " +
                              "from %s order by dept",
                          this.getTableReference(EMPTAB));

        ResultSet rs = methodWatcher.executeQuery(sqlText);
        String expected =
            "EMPNUM |DEPT |SALARY |MAXSAL |\n" +
                "------------------------------\n" +
                "  10   |  1  | 50000 | 78000 |\n" +
                "  50   |  1  | 52000 | 78000 |\n" +
                "  55   |  1  | 52000 | 78000 |\n" +
                "  110  |  1  | 53000 | 78000 |\n" +
                "  20   |  1  | 75000 | 78000 |\n" +
                "  70   |  1  | 76000 | 78000 |\n" +
                "  60   |  1  | 78000 | 78000 |\n" +
                "  90   |  2  | 51000 | 53000 |\n" +
                "  40   |  2  | 52000 | 53000 |\n" +
                "  44   |  2  | 52000 | 53000 |\n" +
                "  49   |  2  | 53000 | 53000 |\n" +
                "  100  |  3  | 55000 | 84000 |\n" +
                "  120  |  3  | 75000 | 84000 |\n" +
                "  80   |  3  | 79000 | 84000 |\n" +
                "  30   |  3  | 84000 | 84000 |";
        assertEquals("\n"+sqlText+"\n", expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
        rs.close();
    }

    @Test
    public void testFrameRowsCurrentRowUnboundedFollowing() throws Exception {
        String sqlText =
            String.format("SELECT empnum, dept, salary, " +
                              "max(salary) over (Partition by dept ORDER BY salary rows between current row and unbounded following) as maxsal " +
                              "from %s order by empnum",
                          this.getTableReference(EMPTAB));

        ResultSet rs = methodWatcher.executeQuery(sqlText);
        String expected =
            "EMPNUM |DEPT |SALARY |MAXSAL |\n" +
                "------------------------------\n" +
                "  10   |  1  | 50000 | 78000 |\n" +
                "  20   |  1  | 75000 | 78000 |\n" +
                "  30   |  3  | 84000 | 84000 |\n" +
                "  40   |  2  | 52000 | 53000 |\n" +
                "  44   |  2  | 52000 | 53000 |\n" +
                "  49   |  2  | 53000 | 53000 |\n" +
                "  50   |  1  | 52000 | 78000 |\n" +
                "  55   |  1  | 52000 | 78000 |\n" +
                "  60   |  1  | 78000 | 78000 |\n" +
                "  70   |  1  | 76000 | 78000 |\n" +
                "  80   |  3  | 79000 | 84000 |\n" +
                "  90   |  2  | 51000 | 53000 |\n" +
                "  100  |  3  | 55000 | 84000 |\n" +
                "  110  |  1  | 53000 | 78000 |\n" +
                "  120  |  3  | 75000 | 84000 |";
        assertEquals("\n"+sqlText+"\n", expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
        rs.close();
    }
    @Test
    public void testFrameRowsUnboundedPrecedingCurrentRow() throws Exception {
        String sqlText =
            String.format("SELECT empnum, dept, salary, " +
                              "min(salary) over (Partition by dept ORDER BY salary rows between unbounded preceding and current row) as minsal " +
                              "from %s order by empnum",
                          this.getTableReference(EMPTAB));

        ResultSet rs = methodWatcher.executeQuery(sqlText);
        String expected =
            "EMPNUM |DEPT |SALARY |MINSAL |\n" +
                "------------------------------\n" +
                "  10   |  1  | 50000 | 50000 |\n" +
                "  20   |  1  | 75000 | 50000 |\n" +
                "  30   |  3  | 84000 | 55000 |\n" +
                "  40   |  2  | 52000 | 51000 |\n" +
                "  44   |  2  | 52000 | 51000 |\n" +
                "  49   |  2  | 53000 | 51000 |\n" +
                "  50   |  1  | 52000 | 50000 |\n" +
                "  55   |  1  | 52000 | 50000 |\n" +
                "  60   |  1  | 78000 | 50000 |\n" +
                "  70   |  1  | 76000 | 50000 |\n" +
                "  80   |  3  | 79000 | 55000 |\n" +
                "  90   |  2  | 51000 | 51000 |\n" +
                "  100  |  3  | 55000 | 55000 |\n" +
                "  110  |  1  | 53000 | 50000 |\n" +
                "  120  |  3  | 75000 | 55000 |";
        assertEquals("\n"+sqlText+"\n", expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
        rs.close();
    }

    @Test
    public void testSum() throws Exception {
        String sqlText =
            String.format("SELECT empnum, dept, salary, sum(salary) over (Partition by dept ORDER BY salary) as sumsal " +
                              "from %s order by dept, empnum",
                          this.getTableReference(EMPTAB));

        ResultSet rs = methodWatcher.executeQuery(sqlText);
        String expected =
            "EMPNUM |DEPT |SALARY |SUMSAL |\n" +
                "------------------------------\n" +
                "  10   |  1  | 50000 | 50000 |\n" +
                "  20   |  1  | 75000 |282000 |\n" +
                "  50   |  1  | 52000 |154000 |\n" +
                "  55   |  1  | 52000 |154000 |\n" +
                "  60   |  1  | 78000 |436000 |\n" +
                "  70   |  1  | 76000 |358000 |\n" +
                "  110  |  1  | 53000 |207000 |\n" +
                "  40   |  2  | 52000 |155000 |\n" +
                "  44   |  2  | 52000 |155000 |\n" +
                "  49   |  2  | 53000 |208000 |\n" +
                "  90   |  2  | 51000 | 51000 |\n" +
                "  30   |  3  | 84000 |293000 |\n" +
                "  80   |  3  | 79000 |209000 |\n" +
                "  100  |  3  | 55000 | 55000 |\n" +
                "  120  |  3  | 75000 |130000 |";
        assertEquals("\n"+sqlText+"\n", expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
        rs.close();
    }

    @Test
    public void testSumRowsUnboundedPrecedingUnboundedFollowing() throws Exception {
        String sqlText =
            String.format("SELECT empnum, dept, salary, " +
                              "sum(salary) over (Partition by dept ORDER BY salary rows between unbounded preceding and unbounded following) as sumsal " +
                              "from %s order by dept, empnum",
                          this.getTableReference(EMPTAB));

        ResultSet rs = methodWatcher.executeQuery(sqlText);
        String expected =
            "EMPNUM |DEPT |SALARY |SUMSAL |\n" +
                "------------------------------\n" +
                "  10   |  1  | 50000 |436000 |\n" +
                "  20   |  1  | 75000 |436000 |\n" +
                "  50   |  1  | 52000 |436000 |\n" +
                "  55   |  1  | 52000 |436000 |\n" +
                "  60   |  1  | 78000 |436000 |\n" +
                "  70   |  1  | 76000 |436000 |\n" +
                "  110  |  1  | 53000 |436000 |\n" +
                "  40   |  2  | 52000 |208000 |\n" +
                "  44   |  2  | 52000 |208000 |\n" +
                "  49   |  2  | 53000 |208000 |\n" +
                "  90   |  2  | 51000 |208000 |\n" +
                "  30   |  3  | 84000 |293000 |\n" +
                "  80   |  3  | 79000 |293000 |\n" +
                "  100  |  3  | 55000 |293000 |\n" +
                "  120  |  3  | 75000 |293000 |";
        assertEquals("\n"+sqlText+"\n", expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
        rs.close();
    }

    @Test
    public void testSumRowsCurrentRowUnboundedFollowing() throws Exception {
        String sqlText =
            String.format("SELECT empnum, dept, salary, " +
                              "sum(salary) over (Partition by dept ORDER BY salary, empnum rows between current row and unbounded following) as sumsal " +
                              "from %s order by dept, empnum",
                          this.getTableReference(EMPTAB));

        ResultSet rs = methodWatcher.executeQuery(sqlText);
        String expected =
            "EMPNUM |DEPT |SALARY |SUMSAL |\n" +
                "------------------------------\n" +
                "  10   |  1  | 50000 |436000 |\n" +
                "  20   |  1  | 75000 |229000 |\n" +
                "  50   |  1  | 52000 |386000 |\n" +
                "  55   |  1  | 52000 |334000 |\n" +
                "  60   |  1  | 78000 | 78000 |\n" +
                "  70   |  1  | 76000 |154000 |\n" +
                "  110  |  1  | 53000 |282000 |\n" +
                "  40   |  2  | 52000 |157000 |\n" +
                "  44   |  2  | 52000 |105000 |\n" +
                "  49   |  2  | 53000 | 53000 |\n" +
                "  90   |  2  | 51000 |208000 |\n" +
                "  30   |  3  | 84000 | 84000 |\n" +
                "  80   |  3  | 79000 |163000 |\n" +
                "  100  |  3  | 55000 |293000 |\n" +
                "  120  |  3  | 75000 |238000 |";
        assertEquals("\n"+sqlText+"\n", expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
        rs.close();
    }

    @Test
    public void testSumRows1Preceding1Following() throws Exception {
        String sqlText =
            String.format("SELECT empnum, dept, salary, " +
                              "sum(salary) over (Partition by dept ORDER BY salary, empnum rows between 1 preceding and 1 following) as sumsal from %s",
                          this.getTableReference(EMPTAB));

        ResultSet rs = methodWatcher.executeQuery(sqlText);
        String expected =
            "EMPNUM |DEPT |SALARY |SUMSAL |\n" +
                "------------------------------\n" +
                "  90   |  2  | 51000 |103000 |\n" +
                "  40   |  2  | 52000 |155000 |\n" +
                "  44   |  2  | 52000 |157000 |\n" +
                "  49   |  2  | 53000 |105000 |\n" +
                "  100  |  3  | 55000 |130000 |\n" +
                "  120  |  3  | 75000 |209000 |\n" +
                "  80   |  3  | 79000 |238000 |\n" +
                "  30   |  3  | 84000 |163000 |\n" +
                "  10   |  1  | 50000 |102000 |\n" +
                "  50   |  1  | 52000 |154000 |\n" +
                "  55   |  1  | 52000 |157000 |\n" +
                "  110  |  1  | 53000 |180000 |\n" +
                "  20   |  1  | 75000 |204000 |\n" +
                "  70   |  1  | 76000 |229000 |\n" +
                "  60   |  1  | 78000 |154000 |";
        assertEquals("\n"+sqlText+"\n", expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
        rs.close();
    }

    @Test
    public void testAvg() throws Exception {
        String sqlText =
            String.format("SELECT empnum, dept, salary, " +
                              "avg(salary) over (Partition by dept ORDER BY salary) as avgsal " +
                              "from %s order by dept, empnum",
                          this.getTableReference(EMPTAB));

        ResultSet rs = methodWatcher.executeQuery(sqlText);
        String expected =
            "EMPNUM |DEPT |SALARY |AVGSAL |\n" +
                "------------------------------\n" +
                "  10   |  1  | 50000 | 50000 |\n" +
                "  20   |  1  | 75000 | 56400 |\n" +
                "  50   |  1  | 52000 | 51333 |\n" +
                "  55   |  1  | 52000 | 51333 |\n" +
                "  60   |  1  | 78000 | 62285 |\n" +
                "  70   |  1  | 76000 | 59666 |\n" +
                "  110  |  1  | 53000 | 51750 |\n" +
                "  40   |  2  | 52000 | 51666 |\n" +
                "  44   |  2  | 52000 | 51666 |\n" +
                "  49   |  2  | 53000 | 52000 |\n" +
                "  90   |  2  | 51000 | 51000 |\n" +
                "  30   |  3  | 84000 | 73250 |\n" +
                "  80   |  3  | 79000 | 69666 |\n" +
                "  100  |  3  | 55000 | 55000 |\n" +
                "  120  |  3  | 75000 | 65000 |";
        assertEquals("\n"+sqlText+"\n", expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
        rs.close();
    }

    @Test
    public void testCountStar() throws Exception {

        String sqlText =
            String.format("SELECT empnum, dept, salary, " +
                              "count(*) over (Partition by dept ORDER BY salary) as count from %s order by dept, empnum",
                          this.getTableReference(EMPTAB));

        ResultSet rs = methodWatcher.executeQuery(sqlText);
        String expected =
            "EMPNUM |DEPT |SALARY | COUNT |\n" +
                "------------------------------\n" +
                "  10   |  1  | 50000 |   1   |\n" +
                "  20   |  1  | 75000 |   5   |\n" +
                "  50   |  1  | 52000 |   3   |\n" +
                "  55   |  1  | 52000 |   3   |\n" +
                "  60   |  1  | 78000 |   7   |\n" +
                "  70   |  1  | 76000 |   6   |\n" +
                "  110  |  1  | 53000 |   4   |\n" +
                "  40   |  2  | 52000 |   3   |\n" +
                "  44   |  2  | 52000 |   3   |\n" +
                "  49   |  2  | 53000 |   4   |\n" +
                "  90   |  2  | 51000 |   1   |\n" +
                "  30   |  3  | 84000 |   4   |\n" +
                "  80   |  3  | 79000 |   3   |\n" +
                "  100  |  3  | 55000 |   1   |\n" +
                "  120  |  3  | 75000 |   2   |";
        assertEquals("\n"+sqlText+"\n", expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
        rs.close();
    }

    @Test
    public void testCountRowsCurrentRowUnboundedFollowing() throws Exception {
        String sqlText =
            String.format("SELECT empnum, dept, salary, " +
                              "count(salary) over (Partition by dept ORDER BY salary, empnum rows between current row and unbounded following) as countsal " +
                              "from %s",
                          this.getTableReference(EMPTAB));

        ResultSet rs = methodWatcher.executeQuery(sqlText);
        String expected =
            "EMPNUM |DEPT |SALARY |COUNTSAL |\n" +
                "--------------------------------\n" +
                "  90   |  2  | 51000 |    4    |\n" +
                "  40   |  2  | 52000 |    3    |\n" +
                "  44   |  2  | 52000 |    2    |\n" +
                "  49   |  2  | 53000 |    1    |\n" +
                "  100  |  3  | 55000 |    4    |\n" +
                "  120  |  3  | 75000 |    3    |\n" +
                "  80   |  3  | 79000 |    2    |\n" +
                "  30   |  3  | 84000 |    1    |\n" +
                "  10   |  1  | 50000 |    7    |\n" +
                "  50   |  1  | 52000 |    6    |\n" +
                "  55   |  1  | 52000 |    5    |\n" +
                "  110  |  1  | 53000 |    4    |\n" +
                "  20   |  1  | 75000 |    3    |\n" +
                "  70   |  1  | 76000 |    2    |\n" +
                "  60   |  1  | 78000 |    1    |";
        assertEquals("\n"+sqlText+"\n", expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
        rs.close();
    }

    @Test
    public void testCountRowsUnboundedPrecedingUnboundedFollowing() throws Exception {
        String sqlText =
            String.format("SELECT empnum, dept, salary, " +
                              "count(salary) over (Partition by dept ORDER BY salary rows between unbounded preceding and unbounded following) as sumsal " +
                              "from %s order by salary, dept, empnum",
                          this.getTableReference(EMPTAB));

        ResultSet rs = methodWatcher.executeQuery(sqlText);
        String expected =
            "EMPNUM |DEPT |SALARY |SUMSAL |\n" +
                "------------------------------\n" +
                "  10   |  1  | 50000 |   7   |\n" +
                "  90   |  2  | 51000 |   4   |\n" +
                "  50   |  1  | 52000 |   7   |\n" +
                "  55   |  1  | 52000 |   7   |\n" +
                "  40   |  2  | 52000 |   4   |\n" +
                "  44   |  2  | 52000 |   4   |\n" +
                "  110  |  1  | 53000 |   7   |\n" +
                "  49   |  2  | 53000 |   4   |\n" +
                "  100  |  3  | 55000 |   4   |\n" +
                "  20   |  1  | 75000 |   7   |\n" +
                "  120  |  3  | 75000 |   4   |\n" +
                "  70   |  1  | 76000 |   7   |\n" +
                "  60   |  1  | 78000 |   7   |\n" +
                "  80   |  3  | 79000 |   4   |\n" +
                "  30   |  3  | 84000 |   4   |";
        assertEquals("\n"+sqlText+"\n", expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
        rs.close();
    }

    @Test
    public void testCount() throws Exception {
        String sqlText =
            String.format("SELECT empnum, dept, salary, " +
                              "count(salary) over (Partition by dept) as c from %s order by dept, empnum",
                          this.getTableReference(EMPTAB));

        ResultSet rs = methodWatcher.executeQuery(sqlText);
        String expected =
            "EMPNUM |DEPT |SALARY | C |\n" +
                "--------------------------\n" +
                "  10   |  1  | 50000 | 7 |\n" +
                "  20   |  1  | 75000 | 7 |\n" +
                "  50   |  1  | 52000 | 7 |\n" +
                "  55   |  1  | 52000 | 7 |\n" +
                "  60   |  1  | 78000 | 7 |\n" +
                "  70   |  1  | 76000 | 7 |\n" +
                "  110  |  1  | 53000 | 7 |\n" +
                "  40   |  2  | 52000 | 4 |\n" +
                "  44   |  2  | 52000 | 4 |\n" +
                "  49   |  2  | 53000 | 4 |\n" +
                "  90   |  2  | 51000 | 4 |\n" +
                "  30   |  3  | 84000 | 4 |\n" +
                "  80   |  3  | 79000 | 4 |\n" +
                "  100  |  3  | 55000 | 4 |\n" +
                "  120  |  3  | 75000 | 4 |";
        assertEquals("\n"+sqlText+"\n", expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
        rs.close();
    }

    @Test
    public void testRangeCurrentRowUnboundedFollowing() throws Exception {
        String sqlText =
            String.format("SELECT empnum, dept, salary, " +
                              "sum(salary) over (Partition by dept ORDER BY salary range between current row and unbounded following) " +
                              "from %s order by dept, empnum",
                          this.getTableReference(EMPTAB));

        ResultSet rs = methodWatcher.executeQuery(sqlText);
        String expected =
            "EMPNUM |DEPT |SALARY |   4   |\n" +
                "------------------------------\n" +
                "  10   |  1  | 50000 |436000 |\n" +
                "  20   |  1  | 75000 |229000 |\n" +
                "  50   |  1  | 52000 |386000 |\n" +
                "  55   |  1  | 52000 |386000 |\n" +
                "  60   |  1  | 78000 | 78000 |\n" +
                "  70   |  1  | 76000 |154000 |\n" +
                "  110  |  1  | 53000 |282000 |\n" +
                "  40   |  2  | 52000 |157000 |\n" +
                "  44   |  2  | 52000 |157000 |\n" +
                "  49   |  2  | 53000 | 53000 |\n" +
                "  90   |  2  | 51000 |208000 |\n" +
                "  30   |  3  | 84000 | 84000 |\n" +
                "  80   |  3  | 79000 |163000 |\n" +
                "  100  |  3  | 55000 |293000 |\n" +
                "  120  |  3  | 75000 |238000 |";
        assertEquals("\n"+sqlText+"\n", expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
        rs.close();
    }

    @Test
    public void testRangeUnboundedPrecedingCurrentRow() throws Exception {
        String sqlText =
            String.format("SELECT empnum, dept, salary, " +
                              "sum(salary) over (Partition by dept ORDER BY salary range between unbounded preceding and current row) as sumsal " +
                              "from %s order by dept, empnum",
                          this.getTableReference(EMPTAB));

        ResultSet rs = methodWatcher.executeQuery(sqlText);
        String expected =
            "EMPNUM |DEPT |SALARY |SUMSAL |\n" +
                "------------------------------\n" +
                "  10   |  1  | 50000 | 50000 |\n" +
                "  20   |  1  | 75000 |282000 |\n" +
                "  50   |  1  | 52000 |154000 |\n" +
                "  55   |  1  | 52000 |154000 |\n" +
                "  60   |  1  | 78000 |436000 |\n" +
                "  70   |  1  | 76000 |358000 |\n" +
                "  110  |  1  | 53000 |207000 |\n" +
                "  40   |  2  | 52000 |155000 |\n" +
                "  44   |  2  | 52000 |155000 |\n" +
                "  49   |  2  | 53000 |208000 |\n" +
                "  90   |  2  | 51000 | 51000 |\n" +
                "  30   |  3  | 84000 |293000 |\n" +
                "  80   |  3  | 79000 |209000 |\n" +
                "  100  |  3  | 55000 | 55000 |\n" +
                "  120  |  3  | 75000 |130000 |";
        assertEquals("\n"+sqlText+"\n", expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
        rs.close();
    }

    @Test
    public void testRangeUnboundedPrecedingUnboundedFollowing() throws Exception {
        String sqlText =
            String.format("SELECT empnum, dept, salary, " +
                              "sum(salary) over (Partition by dept ORDER BY salary range between unbounded preceding and unbounded following) as sumsal " +
                              "from %s order by dept, empnum",
                          this.getTableReference(EMPTAB));

        ResultSet rs = methodWatcher.executeQuery(sqlText);
        String expected =
            "EMPNUM |DEPT |SALARY |SUMSAL |\n" +
                "------------------------------\n" +
                "  10   |  1  | 50000 |436000 |\n" +
                "  20   |  1  | 75000 |436000 |\n" +
                "  50   |  1  | 52000 |436000 |\n" +
                "  55   |  1  | 52000 |436000 |\n" +
                "  60   |  1  | 78000 |436000 |\n" +
                "  70   |  1  | 76000 |436000 |\n" +
                "  110  |  1  | 53000 |436000 |\n" +
                "  40   |  2  | 52000 |208000 |\n" +
                "  44   |  2  | 52000 |208000 |\n" +
                "  49   |  2  | 53000 |208000 |\n" +
                "  90   |  2  | 51000 |208000 |\n" +
                "  30   |  3  | 84000 |293000 |\n" +
                "  80   |  3  | 79000 |293000 |\n" +
                "  100  |  3  | 55000 |293000 |\n" +
                "  120  |  3  | 75000 |293000 |";
        assertEquals("\n"+sqlText+"\n", expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
        rs.close();
    }

    @Test
    public void testRowsCurrentRowUnboundedFollowingSortOnResult() throws Exception {
        // had to add empnum to fn order by so that results would be deterministic/repeatable
        String sqlText =
            String.format("SELECT empnum, dept, salary, " +
                              "sum(salary) over (Partition by dept ORDER BY salary, empnum rows between current row and unbounded following) " +
                              "from %s",
                          this.getTableReference(EMPTAB));

        ResultSet rs = methodWatcher.executeQuery(sqlText);
        String expected =
            "EMPNUM |DEPT |SALARY |   4   |\n" +
                "------------------------------\n" +
                "  90   |  2  | 51000 |208000 |\n" +
                "  40   |  2  | 52000 |157000 |\n" +
                "  44   |  2  | 52000 |105000 |\n" +
                "  49   |  2  | 53000 | 53000 |\n" +
                "  100  |  3  | 55000 |293000 |\n" +
                "  120  |  3  | 75000 |238000 |\n" +
                "  80   |  3  | 79000 |163000 |\n" +
                "  30   |  3  | 84000 | 84000 |\n" +
                "  10   |  1  | 50000 |436000 |\n" +
                "  50   |  1  | 52000 |386000 |\n" +
                "  55   |  1  | 52000 |334000 |\n" +
                "  110  |  1  | 53000 |282000 |\n" +
                "  20   |  1  | 75000 |229000 |\n" +
                "  70   |  1  | 76000 |154000 |\n" +
                "  60   |  1  | 78000 | 78000 |";
        assertEquals("\n"+sqlText+"\n", expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
        rs.close();
    }

    @Test
    public void testRowsUnboundedPrecedingCurrentRowSortOnResult() throws Exception {
        String sqlText =
            String.format("SELECT empnum, dept, salary, " +
                              "sum(salary) over (Partition by dept ORDER BY salary, empnum rows between unbounded preceding and current row) as sumsal " +
                              "from %s order by dept, empnum",
                          this.getTableReference(EMPTAB));

        ResultSet rs = methodWatcher.executeQuery(sqlText);
        String expected =
            "EMPNUM |DEPT |SALARY |SUMSAL |\n" +
                "------------------------------\n" +
                "  10   |  1  | 50000 | 50000 |\n" +
                "  20   |  1  | 75000 |282000 |\n" +
                "  50   |  1  | 52000 |102000 |\n" +
                "  55   |  1  | 52000 |154000 |\n" +
                "  60   |  1  | 78000 |436000 |\n" +
                "  70   |  1  | 76000 |358000 |\n" +
                "  110  |  1  | 53000 |207000 |\n" +
                "  40   |  2  | 52000 |103000 |\n" +
                "  44   |  2  | 52000 |155000 |\n" +
                "  49   |  2  | 53000 |208000 |\n" +
                "  90   |  2  | 51000 | 51000 |\n" +
                "  30   |  3  | 84000 |293000 |\n" +
                "  80   |  3  | 79000 |209000 |\n" +
                "  100  |  3  | 55000 | 55000 |\n" +
                "  120  |  3  | 75000 |130000 |";
        assertEquals("\n"+sqlText+"\n", expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
        rs.close();
    }
    @Test
    public void testRowsUnboundedPrecedingUnboundedFollowing() throws Exception {
        String sqlText =
            String.format("SELECT empnum, dept, salary, " +
                              "sum(salary) over (Partition by dept ORDER BY salary rows between unbounded preceding and unbounded following) as sumsal " +
                              "from %s order by dept, empnum",
                          this.getTableReference(EMPTAB));

        ResultSet rs = methodWatcher.executeQuery(sqlText);
        String expected =
            "EMPNUM |DEPT |SALARY |SUMSAL |\n" +
                "------------------------------\n" +
                "  10   |  1  | 50000 |436000 |\n" +
                "  20   |  1  | 75000 |436000 |\n" +
                "  50   |  1  | 52000 |436000 |\n" +
                "  55   |  1  | 52000 |436000 |\n" +
                "  60   |  1  | 78000 |436000 |\n" +
                "  70   |  1  | 76000 |436000 |\n" +
                "  110  |  1  | 53000 |436000 |\n" +
                "  40   |  2  | 52000 |208000 |\n" +
                "  44   |  2  | 52000 |208000 |\n" +
                "  49   |  2  | 53000 |208000 |\n" +
                "  90   |  2  | 51000 |208000 |\n" +
                "  30   |  3  | 84000 |293000 |\n" +
                "  80   |  3  | 79000 |293000 |\n" +
                "  100  |  3  | 55000 |293000 |\n" +
                "  120  |  3  | 75000 |293000 |";
        assertEquals("\n"+sqlText+"\n", expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
        rs.close();
    }

    @Test
    public void testRangeDescOrder() throws Exception {

        {
            String sqlText =
                String.format("SELECT empnum,dept,salary," +
                                  "min(salary) over (Partition by dept ORDER BY salary desc RANGE BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING ) as minsal " +
                                  "from %s order by dept, empnum",
                              this.getTableReference(EMPTAB));

            ResultSet rs = methodWatcher.executeQuery(sqlText);
            String expected =
                "EMPNUM |DEPT |SALARY |MINSAL |\n" +
                    "------------------------------\n" +
                    "  10   |  1  | 50000 | 50000 |\n" +
                    "  20   |  1  | 75000 | 50000 |\n" +
                    "  50   |  1  | 52000 | 50000 |\n" +
                    "  55   |  1  | 52000 | 50000 |\n" +
                    "  60   |  1  | 78000 | 50000 |\n" +
                    "  70   |  1  | 76000 | 50000 |\n" +
                    "  110  |  1  | 53000 | 50000 |\n" +
                    "  40   |  2  | 52000 | 51000 |\n" +
                    "  44   |  2  | 52000 | 51000 |\n" +
                    "  49   |  2  | 53000 | 51000 |\n" +
                    "  90   |  2  | 51000 | 51000 |\n" +
                    "  30   |  3  | 84000 | 55000 |\n" +
                    "  80   |  3  | 79000 | 55000 |\n" +
                    "  100  |  3  | 55000 | 55000 |\n" +
                    "  120  |  3  | 75000 | 55000 |";
            assertEquals("\n"+sqlText+"\n", expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
            rs.close();
        }


        {
            String sqlText =
                String.format("SELECT empnum, dept, salary, " +
                                  "sum(salary) over (Partition by dept ORDER BY salary desc) as sumsal " +
                                  "from %s order by dept, empnum",
                              this.getTableReference(EMPTAB));

            ResultSet rs = methodWatcher.executeQuery(sqlText);
            String expected =
                "EMPNUM |DEPT |SALARY |SUMSAL |\n" +
                    "------------------------------\n" +
                    "  10   |  1  | 50000 |436000 |\n" +
                    "  20   |  1  | 75000 |229000 |\n" +
                    "  50   |  1  | 52000 |386000 |\n" +
                    "  55   |  1  | 52000 |386000 |\n" +
                    "  60   |  1  | 78000 | 78000 |\n" +
                    "  70   |  1  | 76000 |154000 |\n" +
                    "  110  |  1  | 53000 |282000 |\n" +
                    "  40   |  2  | 52000 |157000 |\n" +
                    "  44   |  2  | 52000 |157000 |\n" +
                    "  49   |  2  | 53000 | 53000 |\n" +
                    "  90   |  2  | 51000 |208000 |\n" +
                    "  30   |  3  | 84000 | 84000 |\n" +
                    "  80   |  3  | 79000 |163000 |\n" +
                    "  100  |  3  | 55000 |293000 |\n" +
                    "  120  |  3  | 75000 |238000 |";
            assertEquals("\n"+sqlText+"\n", expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
            rs.close();
        }
    }
    @Test
    public void testSingleRow() throws Exception {
        {
            String sqlText =
                String.format("SELECT empnum, dept, salary, " +
                                  "sum(salary) over (Partition by dept ORDER BY salary range between current row and current row) as sumsal " +
                                  "from %s order by dept, empnum",
                              this.getTableReference(EMPTAB));

            ResultSet rs = methodWatcher.executeQuery(sqlText);
            String expected =
                "EMPNUM |DEPT |SALARY |SUMSAL |\n" +
                    "------------------------------\n" +
                    "  10   |  1  | 50000 | 50000 |\n" +
                    "  20   |  1  | 75000 | 75000 |\n" +
                    "  50   |  1  | 52000 |104000 |\n" +
                    "  55   |  1  | 52000 |104000 |\n" +
                    "  60   |  1  | 78000 | 78000 |\n" +
                    "  70   |  1  | 76000 | 76000 |\n" +
                    "  110  |  1  | 53000 | 53000 |\n" +
                    "  40   |  2  | 52000 |104000 |\n" +
                    "  44   |  2  | 52000 |104000 |\n" +
                    "  49   |  2  | 53000 | 53000 |\n" +
                    "  90   |  2  | 51000 | 51000 |\n" +
                    "  30   |  3  | 84000 | 84000 |\n" +
                    "  80   |  3  | 79000 | 79000 |\n" +
                    "  100  |  3  | 55000 | 55000 |\n" +
                    "  120  |  3  | 75000 | 75000 |";
            assertEquals("\n"+sqlText+"\n", expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
            rs.close();
        }

        {
            String sqlText =
                String.format("SELECT empnum, dept, salary, " +
                                  "sum(salary) over (Partition by dept ORDER BY salary rows between current row and current row) as sumsal " +
                                  "from %s order by dept, empnum",
                              this.getTableReference(EMPTAB));

            ResultSet rs = methodWatcher.executeQuery(sqlText);
            String expected =
                "EMPNUM |DEPT |SALARY |SUMSAL |\n" +
                    "------------------------------\n" +
                    "  10   |  1  | 50000 | 50000 |\n" +
                    "  20   |  1  | 75000 | 75000 |\n" +
                    "  50   |  1  | 52000 | 52000 |\n" +
                    "  55   |  1  | 52000 | 52000 |\n" +
                    "  60   |  1  | 78000 | 78000 |\n" +
                    "  70   |  1  | 76000 | 76000 |\n" +
                    "  110  |  1  | 53000 | 53000 |\n" +
                    "  40   |  2  | 52000 | 52000 |\n" +
                    "  44   |  2  | 52000 | 52000 |\n" +
                    "  49   |  2  | 53000 | 53000 |\n" +
                    "  90   |  2  | 51000 | 51000 |\n" +
                    "  30   |  3  | 84000 | 84000 |\n" +
                    "  80   |  3  | 79000 | 79000 |\n" +
                    "  100  |  3  | 55000 | 55000 |\n" +
                    "  120  |  3  | 75000 | 75000 |";
            assertEquals("\n"+sqlText+"\n", expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
            rs.close();
        }
    }

    @Test
    public void testRowNumberWithinPartiion() throws Exception {
        String sqlText =
            String.format("SELECT empnum, dept, salary, " +
                              "ROW_NUMBER() OVER (partition by dept ORDER BY dept, empnum, salary desc) AS RowNumber " +
                              "FROM %s",
                          this.getTableReference(EMPTAB));

        ResultSet rs = methodWatcher.executeQuery(sqlText);
        String expected =
            "EMPNUM |DEPT |SALARY | ROWNUMBER |\n" +
                "----------------------------------\n" +
                "  40   |  2  | 52000 |     1     |\n" +
                "  44   |  2  | 52000 |     2     |\n" +
                "  49   |  2  | 53000 |     3     |\n" +
                "  90   |  2  | 51000 |     4     |\n" +
                "  30   |  3  | 84000 |     1     |\n" +
                "  80   |  3  | 79000 |     2     |\n" +
                "  100  |  3  | 55000 |     3     |\n" +
                "  120  |  3  | 75000 |     4     |\n" +
                "  10   |  1  | 50000 |     1     |\n" +
                "  20   |  1  | 75000 |     2     |\n" +
                "  50   |  1  | 52000 |     3     |\n" +
                "  55   |  1  | 52000 |     4     |\n" +
                "  60   |  1  | 78000 |     5     |\n" +
                "  70   |  1  | 76000 |     6     |\n" +
                "  110  |  1  | 53000 |     7     |";
        assertEquals("\n"+sqlText+"\n", expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
        rs.close();
    }

    @Test
    public void testRowNumberWithoutPartiion() throws Exception {
        String sqlText =
            String.format("SELECT empnum, dept, salary, ROW_NUMBER() OVER (ORDER BY dept, empnum, salary desc) AS RowNumber " +
                              "FROM %s",
                          this.getTableReference(EMPTAB));

        ResultSet rs = methodWatcher.executeQuery(sqlText);
        String expected =
            "EMPNUM |DEPT |SALARY | ROWNUMBER |\n" +
                "----------------------------------\n" +
                "  10   |  1  | 50000 |     1     |\n" +
                "  20   |  1  | 75000 |     2     |\n" +
                "  50   |  1  | 52000 |     3     |\n" +
                "  55   |  1  | 52000 |     4     |\n" +
                "  60   |  1  | 78000 |     5     |\n" +
                "  70   |  1  | 76000 |     6     |\n" +
                "  110  |  1  | 53000 |     7     |\n" +
                "  40   |  2  | 52000 |     8     |\n" +
                "  44   |  2  | 52000 |     9     |\n" +
                "  49   |  2  | 53000 |    10     |\n" +
                "  90   |  2  | 51000 |    11     |\n" +
                "  30   |  3  | 84000 |    12     |\n" +
                "  80   |  3  | 79000 |    13     |\n" +
                "  100  |  3  | 55000 |    14     |\n" +
                "  120  |  3  | 75000 |    15     |";
        assertEquals("\n"+sqlText+"\n", expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
        rs.close();
    }

    @Test
    public void testRankWithinPartition() throws Exception {
        String sqlText =
            String.format("SELECT empnum, dept, salary, RANK() OVER (PARTITION BY dept ORDER BY dept, empnum, salary desc) AS Rank FROM %s",
                          this.getTableReference(EMPTAB));

        ResultSet rs = methodWatcher.executeQuery(sqlText);
        String expected =
            "EMPNUM |DEPT |SALARY |RANK |\n" +
                "----------------------------\n" +
                "  40   |  2  | 52000 |  1  |\n" +
                "  44   |  2  | 52000 |  2  |\n" +
                "  49   |  2  | 53000 |  3  |\n" +
                "  90   |  2  | 51000 |  4  |\n" +
                "  30   |  3  | 84000 |  1  |\n" +
                "  80   |  3  | 79000 |  2  |\n" +
                "  100  |  3  | 55000 |  3  |\n" +
                "  120  |  3  | 75000 |  4  |\n" +
                "  10   |  1  | 50000 |  1  |\n" +
                "  20   |  1  | 75000 |  2  |\n" +
                "  50   |  1  | 52000 |  3  |\n" +
                "  55   |  1  | 52000 |  4  |\n" +
                "  60   |  1  | 78000 |  5  |\n" +
                "  70   |  1  | 76000 |  6  |\n" +
                "  110  |  1  | 53000 |  7  |";
        assertEquals("\n"+sqlText+"\n", expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
        rs.close();
    }

    @Test
    public void testRankWithoutPartiion() throws Exception {
        String sqlText =
            String.format("SELECT empnum, dept, salary, RANK() OVER (ORDER BY salary desc) AS Rank FROM %s order by dept, empnum",
                          this.getTableReference(EMPTAB));

        ResultSet rs = methodWatcher.executeQuery(sqlText);
        String expected =
            "EMPNUM |DEPT |SALARY |RANK |\n" +
                "----------------------------\n" +
                "  10   |  1  | 50000 | 15  |\n" +
                "  20   |  1  | 75000 |  5  |\n" +
                "  50   |  1  | 52000 | 10  |\n" +
                "  55   |  1  | 52000 | 10  |\n" +
                "  60   |  1  | 78000 |  3  |\n" +
                "  70   |  1  | 76000 |  4  |\n" +
                "  110  |  1  | 53000 |  8  |\n" +
                "  40   |  2  | 52000 | 10  |\n" +
                "  44   |  2  | 52000 | 10  |\n" +
                "  49   |  2  | 53000 |  8  |\n" +
                "  90   |  2  | 51000 | 14  |\n" +
                "  30   |  3  | 84000 |  1  |\n" +
                "  80   |  3  | 79000 |  2  |\n" +
                "  100  |  3  | 55000 |  7  |\n" +
                "  120  |  3  | 75000 |  5  |";
        assertEquals("\n"+sqlText+"\n", expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
        rs.close();
    }

    @Test
    public void testDenseRankWithinPartition() throws Exception {
        String sqlText =
            String.format("SELECT empnum, dept, salary, DENSE_RANK() OVER (PARTITION BY dept ORDER BY salary desc) AS denseank " +
                              "FROM %s order by dept, empnum",
                          this.getTableReference(EMPTAB));

        ResultSet rs = methodWatcher.executeQuery(sqlText);
        String expected =
            "EMPNUM |DEPT |SALARY |DENSEANK |\n" +
                "--------------------------------\n" +
                "  10   |  1  | 50000 |    6    |\n" +
                "  20   |  1  | 75000 |    3    |\n" +
                "  50   |  1  | 52000 |    5    |\n" +
                "  55   |  1  | 52000 |    5    |\n" +
                "  60   |  1  | 78000 |    1    |\n" +
                "  70   |  1  | 76000 |    2    |\n" +
                "  110  |  1  | 53000 |    4    |\n" +
                "  40   |  2  | 52000 |    2    |\n" +
                "  44   |  2  | 52000 |    2    |\n" +
                "  49   |  2  | 53000 |    1    |\n" +
                "  90   |  2  | 51000 |    3    |\n" +
                "  30   |  3  | 84000 |    1    |\n" +
                "  80   |  3  | 79000 |    2    |\n" +
                "  100  |  3  | 55000 |    4    |\n" +
                "  120  |  3  | 75000 |    3    |";
        assertEquals("\n"+sqlText+"\n", expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
        rs.close();
    }

    @Test
    public void testXPlainTrace() throws Exception {
        SpliceXPlainTrace xPlainTrace = new SpliceXPlainTrace();
        TestConnection conn = methodWatcher.getOrCreateConnection();
        conn.setAutoCommit(false);
        long txnId = conn.getCurrentTransactionId();
        try{
            xPlainTrace.setConnection(conn);
            xPlainTrace.turnOnTrace();
            String s = "SELECT empnum, dept, salary, count(salary) over (Partition by dept) as c from %s";
            String sqlText = String.format(s, this.getTableReference(EMPTAB));
            long count = conn.count(sqlText);
            assertEquals(EMPTAB_ROWS.length, count);
            xPlainTrace.turnOffTrace();

            ResultSet rs = conn.query("select * from SYS.SYSSTATEMENTHISTORY where transactionid = " + txnId);
            Assert.assertTrue("XPLAIN does not have a record for this transaction!",rs.next());
            long statementId = rs.getLong("STATEMENTID");
            Assert.assertFalse("No statement id is found!",rs.wasNull());

            XPlainTreeNode operation = xPlainTrace.getOperationTree(statementId);
            Assert.assertTrue(operation.getOperationType().compareToIgnoreCase(SpliceXPlainTrace.PROJECTRESTRICT)==0);
            operation = operation.getChildren().getFirst();
            Assert.assertTrue(operation.getOperationType().compareToIgnoreCase(SpliceXPlainTrace.WINDOW)==0);
            assertEquals(EMPTAB_ROWS.length, operation.getInputRows());
            assertEquals(EMPTAB_ROWS.length, operation.getOutputRows());
            assertEquals(EMPTAB_ROWS.length * 2, operation.getWriteRows());
        }
        catch (Exception e) {
            e.printStackTrace();
            throw e;
        }
        finally{
            conn.rollback();
        }
    }

    @Test
    public void testDenseRankWithoutPartition() throws Exception {
        String sqlText =
            String.format("SELECT empnum, dept, salary, DENSE_RANK() OVER (ORDER BY salary desc) AS denseank " +
                              "FROM %s order by dept, empnum" ,
                          this.getTableReference(EMPTAB));

        ResultSet rs = methodWatcher.executeQuery(sqlText);
        String expected =
            "EMPNUM |DEPT |SALARY |DENSEANK |\n" +
                "--------------------------------\n" +
                "  10   |  1  | 50000 |   10    |\n" +
                "  20   |  1  | 75000 |    5    |\n" +
                "  50   |  1  | 52000 |    8    |\n" +
                "  55   |  1  | 52000 |    8    |\n" +
                "  60   |  1  | 78000 |    3    |\n" +
                "  70   |  1  | 76000 |    4    |\n" +
                "  110  |  1  | 53000 |    7    |\n" +
                "  40   |  2  | 52000 |    8    |\n" +
                "  44   |  2  | 52000 |    8    |\n" +
                "  49   |  2  | 53000 |    7    |\n" +
                "  90   |  2  | 51000 |    9    |\n" +
                "  30   |  3  | 84000 |    1    |\n" +
                "  80   |  3  | 79000 |    2    |\n" +
                "  100  |  3  | 55000 |    6    |\n" +
                "  120  |  3  | 75000 |    5    |";
        assertEquals("\n"+sqlText+"\n", expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
        rs.close();
    }

    @Test
    public void testRowNumber3OrderByCols() throws Exception {
        String sqlText =
            String.format("SELECT empnum, dept, salary, ROW_NUMBER() OVER (ORDER BY dept, salary desc, empnum) AS rownum " +
                              "FROM %s",
                          this.getTableReference(EMPTAB));

        ResultSet rs = methodWatcher.executeQuery(sqlText);
        String expected =
            "EMPNUM |DEPT |SALARY |ROWNUM |\n" +
                "------------------------------\n" +
                "  60   |  1  | 78000 |   1   |\n" +
                "  70   |  1  | 76000 |   2   |\n" +
                "  20   |  1  | 75000 |   3   |\n" +
                "  110  |  1  | 53000 |   4   |\n" +
                "  50   |  1  | 52000 |   5   |\n" +
                "  55   |  1  | 52000 |   6   |\n" +
                "  10   |  1  | 50000 |   7   |\n" +
                "  49   |  2  | 53000 |   8   |\n" +
                "  40   |  2  | 52000 |   9   |\n" +
                "  44   |  2  | 52000 |  10   |\n" +
                "  90   |  2  | 51000 |  11   |\n" +
                "  30   |  3  | 84000 |  12   |\n" +
                "  80   |  3  | 79000 |  13   |\n" +
                "  120  |  3  | 75000 |  14   |\n" +
                "  100  |  3  | 55000 |  15   |";
        assertEquals("\n"+sqlText+"\n", expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
        rs.close();
    }

    @Test
    public void testRank2OrderByCols() throws Exception {
        String sqlText =
            String.format("SELECT empnum, dept, salary, RANK() OVER (ORDER BY dept, salary desc) AS Rank " +
                              "FROM %s order by empnum",
                          this.getTableReference(EMPTAB));

        ResultSet rs = methodWatcher.executeQuery(sqlText);
        String expected =
            "EMPNUM |DEPT |SALARY |RANK |\n" +
                "----------------------------\n" +
                "  10   |  1  | 50000 |  7  |\n" +
                "  20   |  1  | 75000 |  3  |\n" +
                "  30   |  3  | 84000 | 12  |\n" +
                "  40   |  2  | 52000 |  9  |\n" +
                "  44   |  2  | 52000 |  9  |\n" +
                "  49   |  2  | 53000 |  8  |\n" +
                "  50   |  1  | 52000 |  5  |\n" +
                "  55   |  1  | 52000 |  5  |\n" +
                "  60   |  1  | 78000 |  1  |\n" +
                "  70   |  1  | 76000 |  2  |\n" +
                "  80   |  3  | 79000 | 13  |\n" +
                "  90   |  2  | 51000 | 11  |\n" +
                "  100  |  3  | 55000 | 15  |\n" +
                "  110  |  1  | 53000 |  4  |\n" +
                "  120  |  3  | 75000 | 14  |";
        assertEquals("\n"+sqlText+"\n", expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
        rs.close();
    }

    @Test
    public void testDenseRank2OrderByCols() throws Exception {
        String sqlText =
            String.format("SELECT empnum, dept, salary, DENSE_RANK() OVER (ORDER BY dept, salary desc) AS denserank " +
                              "FROM %s order by empnum",
                          this.getTableReference(EMPTAB));

        ResultSet rs = methodWatcher.executeQuery(sqlText);
        String expected =
            "EMPNUM |DEPT |SALARY | DENSERANK |\n" +
                "----------------------------------\n" +
                "  10   |  1  | 50000 |     6     |\n" +
                "  20   |  1  | 75000 |     3     |\n" +
                "  30   |  3  | 84000 |    10     |\n" +
                "  40   |  2  | 52000 |     8     |\n" +
                "  44   |  2  | 52000 |     8     |\n" +
                "  49   |  2  | 53000 |     7     |\n" +
                "  50   |  1  | 52000 |     5     |\n" +
                "  55   |  1  | 52000 |     5     |\n" +
                "  60   |  1  | 78000 |     1     |\n" +
                "  70   |  1  | 76000 |     2     |\n" +
                "  80   |  3  | 79000 |    11     |\n" +
                "  90   |  2  | 51000 |     9     |\n" +
                "  100  |  3  | 55000 |    13     |\n" +
                "  110  |  1  | 53000 |     4     |\n" +
                "  120  |  3  | 75000 |    12     |";
        assertEquals("\n"+sqlText+"\n", expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
        rs.close();
    }

    @Test
    public void testDenseRankWithPartition3OrderByCols_duplicateKey() throws Exception {
        String sqlText =
            String.format("SELECT empnum, dept, salary, DENSE_RANK() OVER (PARTITION BY dept ORDER BY dept, empnum, salary desc) AS DenseRank FROM %s",
                          this.getTableReference(EMPTAB));

        ResultSet rs = methodWatcher.executeQuery(sqlText);
        String expected =
            "EMPNUM |DEPT |SALARY | DENSERANK |\n" +
                "----------------------------------\n" +
                "  40   |  2  | 52000 |     1     |\n" +
                "  44   |  2  | 52000 |     2     |\n" +
                "  49   |  2  | 53000 |     3     |\n" +
                "  90   |  2  | 51000 |     4     |\n" +
                "  30   |  3  | 84000 |     1     |\n" +
                "  80   |  3  | 79000 |     2     |\n" +
                "  100  |  3  | 55000 |     3     |\n" +
                "  120  |  3  | 75000 |     4     |\n" +
                "  10   |  1  | 50000 |     1     |\n" +
                "  20   |  1  | 75000 |     2     |\n" +
                "  50   |  1  | 52000 |     3     |\n" +
                "  55   |  1  | 52000 |     4     |\n" +
                "  60   |  1  | 78000 |     5     |\n" +
                "  70   |  1  | 76000 |     6     |\n" +
                "  110  |  1  | 53000 |     7     |";
        assertEquals("\n"+sqlText+"\n", expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
        rs.close();
    }

    @Test
    public void testDenseRankWithPartition2OrderByCols_duplicateKey() throws Exception {
        String sqlText =
            String.format("SELECT empnum, dept, salary, DENSE_RANK() OVER (PARTITION BY dept ORDER BY dept, salary desc) AS DenseRank " +
                              "FROM %s order by empnum",
                          this.getTableReference(EMPTAB));

        ResultSet rs = methodWatcher.executeQuery(sqlText);
        String expected =
            "EMPNUM |DEPT |SALARY | DENSERANK |\n" +
                "----------------------------------\n" +
                "  10   |  1  | 50000 |     6     |\n" +
                "  20   |  1  | 75000 |     3     |\n" +
                "  30   |  3  | 84000 |     1     |\n" +
                "  40   |  2  | 52000 |     2     |\n" +
                "  44   |  2  | 52000 |     2     |\n" +
                "  49   |  2  | 53000 |     1     |\n" +
                "  50   |  1  | 52000 |     5     |\n" +
                "  55   |  1  | 52000 |     5     |\n" +
                "  60   |  1  | 78000 |     1     |\n" +
                "  70   |  1  | 76000 |     2     |\n" +
                "  80   |  3  | 79000 |     2     |\n" +
                "  90   |  2  | 51000 |     3     |\n" +
                "  100  |  3  | 55000 |     4     |\n" +
                "  110  |  1  | 53000 |     4     |\n" +
                "  120  |  3  | 75000 |     3     |";
        assertEquals("\n"+sqlText+"\n", expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
        rs.close();
    }

    @Test
    public void testDenseRankWithPartition3OrderByCols_KeyColMissingFromSelect() throws Exception {
        String sqlText =
            String.format("SELECT empnum, salary, DENSE_RANK() OVER (PARTITION BY dept ORDER BY dept, empnum, salary desc) AS DenseRank " +
                              "FROM %s",
                          this.getTableReference(EMPTAB));

        ResultSet rs = methodWatcher.executeQuery(sqlText);
        String expected =
            "EMPNUM |SALARY | DENSERANK |\n" +
                "----------------------------\n" +
                "  40   | 52000 |     1     |\n" +
                "  44   | 52000 |     2     |\n" +
                "  49   | 53000 |     3     |\n" +
                "  90   | 51000 |     4     |\n" +
                "  30   | 84000 |     1     |\n" +
                "  80   | 79000 |     2     |\n" +
                "  100  | 55000 |     3     |\n" +
                "  120  | 75000 |     4     |\n" +
                "  10   | 50000 |     1     |\n" +
                "  20   | 75000 |     2     |\n" +
                "  50   | 52000 |     3     |\n" +
                "  55   | 52000 |     4     |\n" +
                "  60   | 78000 |     5     |\n" +
                "  70   | 76000 |     6     |\n" +
                "  110  | 53000 |     7     |";
        assertEquals("\n"+sqlText+"\n", expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
        rs.close();
    }

    @Test
    public void testDenseRankWithoutPartitionOrderby() throws Exception {
        String sqlText =
            String.format("SELECT empnum, dept, salary, DENSE_RANK() OVER (ORDER BY salary desc) AS Rank " +
                              "FROM %s order by empnum",
                          this.getTableReference(EMPTAB));

        ResultSet rs = methodWatcher.executeQuery(sqlText);
        String expected =
            "EMPNUM |DEPT |SALARY |RANK |\n" +
                "----------------------------\n" +
                "  10   |  1  | 50000 | 10  |\n" +
                "  20   |  1  | 75000 |  5  |\n" +
                "  30   |  3  | 84000 |  1  |\n" +
                "  40   |  2  | 52000 |  8  |\n" +
                "  44   |  2  | 52000 |  8  |\n" +
                "  49   |  2  | 53000 |  7  |\n" +
                "  50   |  1  | 52000 |  8  |\n" +
                "  55   |  1  | 52000 |  8  |\n" +
                "  60   |  1  | 78000 |  3  |\n" +
                "  70   |  1  | 76000 |  4  |\n" +
                "  80   |  3  | 79000 |  2  |\n" +
                "  90   |  2  | 51000 |  9  |\n" +
                "  100  |  3  | 55000 |  6  |\n" +
                "  110  |  1  | 53000 |  7  |\n" +
                "  120  |  3  | 75000 |  5  |";
        assertEquals("\n" + sqlText + "\n", expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
        rs.close();
    }

    @Test
    public void testRowNumberWithoutPartitionOrderby() throws Exception {
        // DB-1683
        String sqlText =
            String.format("select PersonID,FamilyID,FirstName,LastName, ROW_NUMBER() over (order by DOB) as Number, dob " +
                              "from %s order by PersonID",
                          this.getTableReference(PEOPLE));

        ResultSet rs = methodWatcher.executeQuery(sqlText);
        String expected =
            "PERSONID |FAMILYID | FIRSTNAME |LASTNAME |NUMBER |         DOB          |\n" +
                "-------------------------------------------------------------------------\n" +
                "    1    |    1    |    Joe    | Johnson |   4   |2000-10-23 13:00:00.0 |\n" +
                "    1    |    1    |    Joe    | Johnson |   5   |2000-10-23 13:00:00.0 |\n" +
                "    1    |    1    |    Joe    | Johnson |   6   |2000-10-23 13:00:00.0 |\n" +
                "    2    |    1    |    Jim    | Johnson |   9   |2001-12-15 05:45:00.0 |\n" +
                "    2    |    1    |    Jim    | Johnson |  10   |2001-12-15 05:45:00.0 |\n" +
                "    2    |    1    |    Jim    | Johnson |  11   |2001-12-15 05:45:00.0 |\n" +
                "    3    |    2    |   Karly   |Matthews |   1   |2000-05-20 04:00:00.0 |\n" +
                "    3    |    2    |   Karly   |Matthews |   2   |2000-05-20 04:00:00.0 |\n" +
                "    4    |    2    |   Kacy    |Matthews |   3   |2000-05-20 04:02:00.0 |\n" +
                "    5    |    2    |    Tom    |Matthews |   7   |2001-09-15 11:52:00.0 |\n" +
                "    5    |    2    |    Tom    |Matthews |   8   |2001-09-15 11:52:00.0 |";
        assertEquals("\n" + sqlText + "\n", expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
        rs.close();
    }

    @Test
    public void testRowNumberWithoutPartitionOrderby_OrderbyColNotInSelect() throws Exception {
        // DB-1683
        String sqlText =
            String.format("select PersonID,FamilyID,FirstName,LastName, ROW_NUMBER() over (order by DOB) as Number " +
                              "from %s order by PersonID",
                          this.getTableReference(PEOPLE));

        ResultSet rs = methodWatcher.executeQuery(sqlText);
        String expected =
            "PERSONID |FAMILYID | FIRSTNAME |LASTNAME |NUMBER |\n" +
                "--------------------------------------------------\n" +
                "    1    |    1    |    Joe    | Johnson |   4   |\n" +
                "    1    |    1    |    Joe    | Johnson |   5   |\n" +
                "    1    |    1    |    Joe    | Johnson |   6   |\n" +
                "    2    |    1    |    Jim    | Johnson |   9   |\n" +
                "    2    |    1    |    Jim    | Johnson |  10   |\n" +
                "    2    |    1    |    Jim    | Johnson |  11   |\n" +
                "    3    |    2    |   Karly   |Matthews |   1   |\n" +
                "    3    |    2    |   Karly   |Matthews |   2   |\n" +
                "    4    |    2    |   Kacy    |Matthews |   3   |\n" +
                "    5    |    2    |    Tom    |Matthews |   7   |\n" +
                "    5    |    2    |    Tom    |Matthews |   8   |";
        assertEquals("\n"+sqlText+"\n", expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
        rs.close();
    }

    @Test
    public void testScalarAggWithOrderBy() throws Exception {
        // DB-1775
        String sqlText =
            String.format("SELECT sum(price) over (Partition by item ORDER BY date) as  sumprice from %s",
                          this.getTableReference(PURCHASED));

        ResultSet rs = methodWatcher.executeQuery(sqlText);
        String expected =
            "SUMPRICE |\n" +
                "----------\n" +
                "  10.0   |\n" +
                "  12.0   |\n" +
                "  20.0   |\n" +
                "   6.0   |\n" +
                "  11.0   |\n" +
                "  23.0   |\n" +
                "   3.0   |\n" +
                "  10.0   |\n" +
                "  20.0   |\n" +
                "   1.0   |\n" +
                "   2.0   |\n" +
                "   9.0   |\n" +
                "  11.0   |\n" +
                "  15.0   |\n" +
                "  25.0   |";
        assertEquals("\n"+sqlText+"\n", expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
        rs.close();
    }

    @Test
    public void testSelectAllColsScalarAggWithOrderBy() throws Exception {
        // DB-1774
        String sqlText =
            String.format("SELECT item, price, sum(price) over (Partition by item ORDER BY date) as sumsal, date " +
                              "from %s",
                          this.getTableReference(PURCHASED));

        ResultSet rs = methodWatcher.executeQuery(sqlText);
        String expected =
            "ITEM | PRICE |SUMSAL |         DATE           |\n" +
                "-----------------------------------------------\n" +
                "  4  | 10.0  | 10.0  |2014-09-08 17:50:17.182 |\n" +
                "  4  |  2.0  | 12.0  |2014-09-08 18:05:47.166 |\n" +
                "  4  |  8.0  | 20.0  |2014-09-08 18:08:04.986 |\n" +
                "  2  |  6.0  |  6.0  |2014-09-08 17:50:17.182 |\n" +
                "  2  |  5.0  | 11.0  |2014-09-08 18:26:51.387 |\n" +
                "  2  | 12.0  | 23.0  |2014-09-08 18:40:15.48  |\n" +
                "  3  |  3.0  |  3.0  |2014-09-08 17:36:55.414 |\n" +
                "  3  |  7.0  | 10.0  |2014-09-08 18:00:44.742 |\n" +
                "  3  | 10.0  | 20.0  |2014-09-08 18:25:42.387 |\n" +
                "  1  |  1.0  |  1.0  |2014-09-08 17:45:15.204 |\n" +
                "  1  |  1.0  |  2.0  |2014-09-08 18:27:48.881 |\n" +
                "  1  |  7.0  |  9.0  |2014-09-08 18:33:46.446 |\n" +
                "  5  | 11.0  | 11.0  |2014-09-08 17:41:56.353 |\n" +
                "  5  |  4.0  | 15.0  |2014-09-08 17:46:26.428 |\n" +
                "  5  | 10.0  | 25.0  |2014-09-08 18:11:23.645 |";
        assertEquals("\n"+sqlText+"\n", expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
        rs.close();
    }

    @Test
    public void testWindowFunctionWithGroupBy() throws Exception {

        String sqlText =
            "select empnum, dept, sum(salary)," +
                "rank() over(partition by dept order by salary desc) rank " +
                "from %s " +
                "group by empnum, dept order by empnum";

        ResultSet rs = methodWatcher.executeQuery(
            String.format(sqlText, this.getTableReference(EMPTAB)));

        String expected =
            "EMPNUM |DEPT |  3   |RANK |\n" +
                "---------------------------\n" +
                "  10   |  1  |50000 |  7  |\n" +
                "  20   |  1  |75000 |  3  |\n" +
                "  30   |  3  |84000 |  1  |\n" +
                "  40   |  2  |52000 |  2  |\n" +
                "  44   |  2  |52000 |  2  |\n" +
                "  49   |  2  |53000 |  1  |\n" +
                "  50   |  1  |52000 |  5  |\n" +
                "  55   |  1  |52000 |  5  |\n" +
                "  60   |  1  |78000 |  1  |\n" +
                "  70   |  1  |76000 |  2  |\n" +
                "  80   |  3  |79000 |  2  |\n" +
                "  90   |  2  |51000 |  4  |\n" +
                "  100  |  3  |55000 |  4  |\n" +
                "  110  |  1  |53000 |  4  |\n" +
                "  120  |  3  |75000 |  3  |";

        assertEquals("\n" + sqlText + "\n", expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
        rs.close();
    }

    @Test
    public void testMaxInOrderBy() throws Exception {
        String sqlText =
            String.format("SELECT empnum, dept, salary, " +
                              "max(salary) over (Partition by dept) as maxsal " +
                              "from %s order by empnum",
                          this.getTableReference(EMPTAB));

        ResultSet rs = methodWatcher.executeQuery(sqlText);

        String expected =
            "EMPNUM |DEPT |SALARY |MAXSAL |\n" +
                "------------------------------\n" +
                "  10   |  1  | 50000 | 78000 |\n" +
                "  20   |  1  | 75000 | 78000 |\n" +
                "  30   |  3  | 84000 | 84000 |\n" +
                "  40   |  2  | 52000 | 53000 |\n" +
                "  44   |  2  | 52000 | 53000 |\n" +
                "  49   |  2  | 53000 | 53000 |\n" +
                "  50   |  1  | 52000 | 78000 |\n" +
                "  55   |  1  | 52000 | 78000 |\n" +
                "  60   |  1  | 78000 | 78000 |\n" +
                "  70   |  1  | 76000 | 78000 |\n" +
                "  80   |  3  | 79000 | 84000 |\n" +
                "  90   |  2  | 51000 | 53000 |\n" +
                "  100  |  3  | 55000 | 84000 |\n" +
                "  110  |  1  | 53000 | 78000 |\n" +
                "  120  |  3  | 75000 | 84000 |";
        assertEquals("\n"+sqlText+"\n", expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
        rs.close();
    }

    @Test
    public void testRankInOrderBy() throws Exception {
        String sqlText =
            String.format("SELECT empnum, dept, salary, " +
                              "rank() over (Partition by dept order by salary) as salrank " +
                              "from %s order by empnum",
                          this.getTableReference(EMPTAB));

        ResultSet rs = methodWatcher.executeQuery(sqlText);

        String expected =
            "EMPNUM |DEPT |SALARY | SALRANK |\n" +
                "--------------------------------\n" +
                "  10   |  1  | 50000 |    1    |\n" +
                "  20   |  1  | 75000 |    5    |\n" +
                "  30   |  3  | 84000 |    4    |\n" +
                "  40   |  2  | 52000 |    2    |\n" +
                "  44   |  2  | 52000 |    2    |\n" +
                "  49   |  2  | 53000 |    4    |\n" +
                "  50   |  1  | 52000 |    2    |\n" +
                "  55   |  1  | 52000 |    2    |\n" +
                "  60   |  1  | 78000 |    7    |\n" +
                "  70   |  1  | 76000 |    6    |\n" +
                "  80   |  3  | 79000 |    3    |\n" +
                "  90   |  2  | 51000 |    1    |\n" +
                "  100  |  3  | 55000 |    1    |\n" +
                "  110  |  1  | 53000 |    4    |\n" +
                "  120  |  3  | 75000 |    2    |";
        assertEquals("\n" + sqlText + "\n", expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
        rs.close();
    }

    @Test
    public void testRankWithAggAsOrderByCol() throws Exception {
        // have to order by ename here because it's the only col with unique values and forces repeatable results
        String sqlText =
            String.format("select sum(sal), " +
                              "rank() over ( order by sum(sal) ) empsal, ename from %s " +
                              "group by ename order by ename",
                          this.getTableReference(EMP));

        ResultSet rs = methodWatcher.executeQuery(sqlText);

        String expected =
            "1    |EMPSAL | ENAME |\n" +
                "-------------------------\n" +
                "1100.00 |   3   | ADAMS |\n" +
                "1600.00 |   8   | ALLEN |\n" +
                "2850.00 |  10   | BLAKE |\n" +
                "2450.00 |   9   | CLARK |\n" +
                "3000.00 |  12   | FORD  |\n" +
                "950.00  |   2   | JAMES |\n" +
                "2975.00 |  11   | JONES |\n" +
                "5000.00 |  14   | KING  |\n" +
                "1250.00 |   4   |MARTIN |\n" +
                "1300.00 |   6   |MILLER |\n" +
                "3000.00 |  12   | SCOTT |\n" +
                "800.00  |   1   | SMITH |\n" +
                "1500.00 |   7   |TURNER |\n" +
                "1250.00 |   4   | WARD  |";
        assertEquals("\n"+sqlText+"\n", expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
        rs.close();
    }

    @Test(expected=SQLException.class)
    public void testRankWithAggAsOrderByColNoGroupBy() throws Exception {
        // expecting an exception here because there's an aggregate with no
        // group by specified for ename column
        String sqlText =
            "select sal, rank() over ( order by sum(sal) ) empsal, ename from %s";

        methodWatcher.executeQuery(
            String.format(sqlText, this.getTableReference(EMP)));
    }

    @Test
    public void testRankWith2AggAsOrderByCol() throws Exception {
        // have to order by ename here because it's the only col with unique values and forces repeatable results
        String sqlText =
            String.format("select sum(sal) as sum_sal, avg(sal) as avg_sal, " +
                              "rank() over ( order by sum(sal), avg(sal) ) empsal, ename " +
                              "from %s group by ename order by ename",
                          this.getTableReference(EMP));

        ResultSet rs = methodWatcher.executeQuery(sqlText);

        String expected =
            "SUM_SAL | AVG_SAL  |EMPSAL | ENAME |\n" +
                "-------------------------------------\n" +
                " 1100.00 |1100.0000 |   3   | ADAMS |\n" +
                " 1600.00 |1600.0000 |   8   | ALLEN |\n" +
                " 2850.00 |2850.0000 |  10   | BLAKE |\n" +
                " 2450.00 |2450.0000 |   9   | CLARK |\n" +
                " 3000.00 |3000.0000 |  12   | FORD  |\n" +
                " 950.00  |950.0000  |   2   | JAMES |\n" +
                " 2975.00 |2975.0000 |  11   | JONES |\n" +
                " 5000.00 |5000.0000 |  14   | KING  |\n" +
                " 1250.00 |1250.0000 |   4   |MARTIN |\n" +
                " 1300.00 |1300.0000 |   6   |MILLER |\n" +
                " 3000.00 |3000.0000 |  12   | SCOTT |\n" +
                " 800.00  |800.0000  |   1   | SMITH |\n" +
                " 1500.00 |1500.0000 |   7   |TURNER |\n" +
                " 1250.00 |1250.0000 |   4   | WARD  |";
        assertEquals("\n"+sqlText+"\n", expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
        rs.close();
    }

    @Test
    public void testMediaForDept() throws Exception {
        // DB-1650, DB-2020
        String sqlText = String.format("SELECT %1$s.Nome_Dep, %2$s.Nome AS Funcionario, %2$s.Salario, " +
                                           "AVG(%2$s.Salario) OVER(PARTITION BY %1$s.Nome_Dep) \"Média por " +
                                           "Departamento\", " +
                                           "%2$s.Salario - AVG(%2$s.Salario) as \"Diferença de Salário\" FROM %2$s " +
                                           "INNER" +
                                           " JOIN %1$s ON %2$s.ID_Dep = %1$s.ID group by %1$s.Nome_Dep," +
                                           "%2$s.Nome, %2$s.Salario ORDER BY 3 DESC, 1",
                                       this.getTableReference(DEPARTAMENTOS), this.getTableReference(FUNCIONARIOS));

        ResultSet rs = methodWatcher.executeQuery(sqlText);

        String expected =
            "NOME_DEP     | FUNCIONARIO | SALARIO |Média por Departamento |Diferença de Salário |\n" +
                "----------------------------------------------------------------------------------------\n" +
                "Recursos Humanos |   Luciano   |23500.00 |      14166.3333       |       0.0000        |\n" +
                "Recursos Humanos |  Zavaschi   |13999.00 |      14166.3333       |       0.0000        |\n" +
                "       IT        |   Nogare    |11999.00 |       5499.6666       |       0.0000        |\n" +
                "     Vendas      |    Diego    | 9000.00 |       4500.0000       |       0.0000        |\n" +
                "Recursos Humanos |   Laerte    | 5000.00 |      14166.3333       |       0.0000        |\n" +
                "       IT        |  Ferreira   | 2500.00 |       5499.6666       |       0.0000        |\n" +
                "     Vendas      |   Amorim    | 2500.00 |       4500.0000       |       0.0000        |\n" +
                "       IT        |   Felipe    | 2000.00 |       5499.6666       |       0.0000        |\n" +
                "     Vendas      |   Fabiano   | 2000.00 |       4500.0000       |       0.0000        |";
        assertEquals("\n"+sqlText+"\n", expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
        rs.close();
    }


    @Test
    public void testConstMinusAvg1ReversedJoinOrder() throws Exception {
        String sqlText = String.format("SELECT %2$s.Salario - AVG(%2$s.Salario) OVER(PARTITION BY " +
                                           "%1$s.Nome_Dep) \"Diferença de Salário\" FROM " +
                                           "%1$s INNER JOIN %2$s " +
                                           "ON %2$s.ID_Dep = %1$s.ID",
                                       this.getTableReference(DEPARTAMENTOS), this.getTableReference(FUNCIONARIOS));

        try(ResultSet rs = methodWatcher.executeQuery(sqlText)){

            String expected=
                "Diferença de Salário |\n"+
                    "----------------------\n"+
                    "     -9166.3333      |\n"+
                    "      9333.6666      |\n"+
                    "      -167.3333      |\n"+
                    "     -2500.0000      |\n"+
                    "     -2000.0000      |\n"+
                    "      4500.0000      |\n"+
                    "     -3499.6666      |\n"+
                    "     -2999.6666      |\n"+
                    "      6499.3333      |";
            assertEquals("\n"+sqlText+"\n",expected,TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
        }
    }
    @Test
    public void testConstMinusAvg1() throws Exception {
        // DB-2124
        String sqlText = String.format("SELECT %2$s.Salario - AVG(%2$s.Salario) OVER(PARTITION BY " +
                                           "%1$s.Nome_Dep) \"Diferença de Salário\" FROM " +
                                           "%2$s INNER JOIN %1$s " +
                                           "ON %2$s.ID_Dep = %1$s.ID",
                                       this.getTableReference(DEPARTAMENTOS), this.getTableReference(FUNCIONARIOS));

        try(ResultSet rs = methodWatcher.executeQuery(sqlText)){

            String expected=
                "Diferença de Salário |\n"+
                    "----------------------\n"+
                    "     -9166.3333      |\n"+
                    "      9333.6666      |\n"+
                    "      -167.3333      |\n"+
                    "     -2500.0000      |\n"+
                    "     -2000.0000      |\n"+
                    "      4500.0000      |\n"+
                    "     -3499.6666      |\n"+
                    "     -2999.6666      |\n"+
                    "      6499.3333      |";
            assertEquals("\n"+sqlText+"\n",expected,TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
        }
    }

    @Test
    public void testConstMinusAvg2() throws Exception {
        // DB-2124
        String sqlText = String.format("SELECT %1$s.Nome_Dep, " +
                                           "%2$s.Nome AS Funcionario, " +
                                           "%2$s.Salario, " +
                                           "AVG(%2$s.Salario) OVER(PARTITION BY %1$s.Nome_Dep) " +
                                           "\"Média por Departamento\", " +
                                           "%2$s.Salario - AVG(%2$s.Salario) OVER(PARTITION BY " +
                                           "%1$s.Nome_Dep) \"Diferença de Salário\" FROM %2$s " +
                                           "INNER JOIN %1$s " +
                                           "ON %2$s.ID_Dep = %1$s.ID ORDER BY 5 DESC",
                                       this.getTableReference(DEPARTAMENTOS), this.getTableReference(FUNCIONARIOS));

        ResultSet rs = methodWatcher.executeQuery(sqlText);

        String expected =
            "NOME_DEP     | FUNCIONARIO | SALARIO |Média por Departamento |Diferença de Salário |\n" +
                "----------------------------------------------------------------------------------------\n" +
                "Recursos Humanos |   Luciano   |23500.00 |      14166.3333       |      9333.6666      |\n" +
                "       IT        |   Nogare    |11999.00 |       5499.6666       |      6499.3333      |\n" +
                "     Vendas      |    Diego    | 9000.00 |       4500.0000       |      4500.0000      |\n" +
                "Recursos Humanos |  Zavaschi   |13999.00 |      14166.3333       |      -167.3333      |\n" +
                "     Vendas      |   Amorim    | 2500.00 |       4500.0000       |     -2000.0000      |\n" +
                "     Vendas      |   Fabiano   | 2000.00 |       4500.0000       |     -2500.0000      |\n" +
                "       IT        |  Ferreira   | 2500.00 |       5499.6666       |     -2999.6666      |\n" +
                "       IT        |   Felipe    | 2000.00 |       5499.6666       |     -3499.6666      |\n" +
                "Recursos Humanos |   Laerte    | 5000.00 |      14166.3333       |     -9166.3333      |";
        assertEquals("\n"+sqlText+"\n", expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
        rs.close();
    }

    @Test
    public void testSumTimesConstDivSum() throws Exception {
        // DB-2086 - identical agg gets removed from aggregates array
        String sqlText = String.format("SELECT SUM(%2$s.Salario) * 100 / " +
                                           "SUM(%2$s.Salario) OVER(PARTITION BY %1$s.Nome_Dep) " +
                                           "\"Média por Departamento\" " +
                                           "FROM %2$s, %1$s GROUP BY %1$s.Nome_Dep",
                                       this.getTableReference(DEPARTAMENTOS), this.getTableReference(FUNCIONARIOS));

        ResultSet rs = methodWatcher.executeQuery(sqlText);

        String expected =
            "Média por Departamento |\n" +
                "------------------------\n" +
                "       3624.9000       |\n" +
                "       3624.9000       |\n" +
                "       3624.9000       |";
        assertEquals("\n"+sqlText+"\n", expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
        rs.close();
    }

    @Test
    @Ignore("DB-2170 - window function over view. (works periodically, why?)")
    public void testDB2170RankOverView() throws Exception {
        String sqlText =
            String.format("select yr, rank() over ( partition by yr order by hiredate ) as EMPRANK, ename," +
                              "hiredate from %s", this.getTableReference(YEAR_VIEW));
        ResultSet rs = methodWatcher.executeQuery(sqlText);
        String expected =
            "YR | EMPRANK | ENAME | HIREDATE  |\n" +
                "----------------------------------\n" +
                "81 |    1    | ALLEN |1981-02-20 |\n" +
                "81 |    2    | WARD  |1981-02-22 |\n" +
                "81 |    3    | JONES |1981-04-02 |\n" +
                "81 |    4    | BLAKE |1981-05-01 |\n" +
                "81 |    5    | CLARK |1981-06-09 |\n" +
                "81 |    6    |TURNER |1981-09-08 |\n" +
                "81 |    7    |MARTIN |1981-09-28 |\n" +
                "81 |    8    | KING  |1981-11-17 |\n" +
                "81 |    9    | FORD  |1981-12-03 |\n" +
                "81 |    9    | JAMES |1981-12-03 |\n" +
                "83 |    1    | ADAMS |1983-01-12 |\n" +
                "82 |    1    |MILLER |1982-01-23 |\n" +
                "82 |    2    | SCOTT |1982-12-09 |\n" +
                "80 |    1    | SMITH |1980-12-17 |";
        assertEquals("\n"+sqlText+"\n", expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
        rs.close();
    }

    @Test
    @Ignore("DB-2170 - window function over view. (works periodically, why?)")
    public void testDB2170RankOverViewMissingKey() throws Exception {
        String sqlText =
            String.format("select rank() over ( partition by yr order by hiredate ) as EMPRANK, ename," +
                              "hiredate from %s", this.getTableReference(YEAR_VIEW));
        ResultSet rs = methodWatcher.executeQuery(sqlText);
        String expected =
            "EMPRANK | ENAME | HIREDATE  |\n" +
                "------------------------------\n" +
                "    1    | ALLEN |1981-02-20 |\n" +
                "    2    | WARD  |1981-02-22 |\n" +
                "    3    | JONES |1981-04-02 |\n" +
                "    4    | BLAKE |1981-05-01 |\n" +
                "    5    | CLARK |1981-06-09 |\n" +
                "    6    |TURNER |1981-09-08 |\n" +
                "    7    |MARTIN |1981-09-28 |\n" +
                "    8    | KING  |1981-11-17 |\n" +
                "    9    | FORD  |1981-12-03 |\n" +
                "    9    | JAMES |1981-12-03 |\n" +
                "    1    | ADAMS |1983-01-12 |\n" +
                "    1    |MILLER |1982-01-23 |\n" +
                "    2    | SCOTT |1982-12-09 |\n" +
                "    1    | SMITH |1980-12-17 |";
        assertEquals("\n"+sqlText+"\n", expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
        rs.close();
    }

    @Test
    @Ignore("DB-2170 - window function over view. (works periodically, why?)")
    public void testDB2170MaxOverView() throws Exception {
        String sqlText =
            String.format("select max(hiredate) over () as maxhiredate, ename," +
                              "hiredate from %s order by ename", this.getTableReference(YEAR_VIEW));
        ResultSet rs = methodWatcher.executeQuery(sqlText);
        String expected =
            "MAXHIREDATE | ENAME | HIREDATE  |\n" +
                "----------------------------------\n" +
                " 1983-01-12  | ADAMS |1983-01-12 |\n" +
                " 1983-01-12  | ALLEN |1981-02-20 |\n" +
                " 1983-01-12  | BLAKE |1981-05-01 |\n" +
                " 1983-01-12  | CLARK |1981-06-09 |\n" +
                " 1983-01-12  | FORD  |1981-12-03 |\n" +
                " 1983-01-12  | JAMES |1981-12-03 |\n" +
                " 1983-01-12  | JONES |1981-04-02 |\n" +
                " 1983-01-12  | KING  |1981-11-17 |\n" +
                " 1983-01-12  |MARTIN |1981-09-28 |\n" +
                " 1983-01-12  |MILLER |1982-01-23 |\n" +
                " 1983-01-12  | SCOTT |1982-12-09 |\n" +
                " 1983-01-12  | SMITH |1980-12-17 |\n" +
                " 1983-01-12  |TURNER |1981-09-08 |\n" +
                " 1983-01-12  | WARD  |1981-02-22 |";
        assertEquals("\n"+sqlText+"\n", expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
        rs.close();
    }

    @Test
    public void testDB2170MaxOverTable() throws Exception {
        String sqlText =
            String.format("select max(hiredate) over () as maxhiredate, ename, hiredate from %s order by ename",
                          this.getTableReference(EMP));

        ResultSet rs = methodWatcher.executeQuery(sqlText);

        String expected =
            "MAXHIREDATE | ENAME | HIREDATE  |\n" +
                "----------------------------------\n" +
                " 1983-01-12  | ADAMS |1983-01-12 |\n" +
                " 1983-01-12  | ALLEN |1981-02-20 |\n" +
                " 1983-01-12  | BLAKE |1981-05-01 |\n" +
                " 1983-01-12  | CLARK |1981-06-09 |\n" +
                " 1983-01-12  | FORD  |1981-12-03 |\n" +
                " 1983-01-12  | JAMES |1981-12-03 |\n" +
                " 1983-01-12  | JONES |1981-04-02 |\n" +
                " 1983-01-12  | KING  |1981-11-17 |\n" +
                " 1983-01-12  |MARTIN |1981-09-28 |\n" +
                " 1983-01-12  |MILLER |1982-01-23 |\n" +
                " 1983-01-12  | SCOTT |1982-12-09 |\n" +
                " 1983-01-12  | SMITH |1980-12-17 |\n" +
                " 1983-01-12  |TURNER |1981-09-08 |\n" +
                " 1983-01-12  | WARD  |1981-02-22 |";
        assertEquals("\n"+sqlText+"\n", expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
        rs.close();
    }

    @Test
    public void testDB2170WithAggOverView() throws Exception {
        String sqlText =
            String.format("select max(hiredate) as maxhiredate, ename,hiredate from %s group by ename, hiredate order by ename",
                          this.getTableReference(YEAR_VIEW));
        ResultSet rs = methodWatcher.executeQuery(sqlText);
        String expected =
            "MAXHIREDATE | ENAME | HIREDATE  |\n" +
                "----------------------------------\n" +
                " 1983-01-12  | ADAMS |1983-01-12 |\n" +
                " 1981-02-20  | ALLEN |1981-02-20 |\n" +
                " 1981-05-01  | BLAKE |1981-05-01 |\n" +
                " 1981-06-09  | CLARK |1981-06-09 |\n" +
                " 1981-12-03  | FORD  |1981-12-03 |\n" +
                " 1981-12-03  | JAMES |1981-12-03 |\n" +
                " 1981-04-02  | JONES |1981-04-02 |\n" +
                " 1981-11-17  | KING  |1981-11-17 |\n" +
                " 1981-09-28  |MARTIN |1981-09-28 |\n" +
                " 1982-01-23  |MILLER |1982-01-23 |\n" +
                " 1982-12-09  | SCOTT |1982-12-09 |\n" +
                " 1980-12-17  | SMITH |1980-12-17 |\n" +
                " 1981-09-08  |TURNER |1981-09-08 |\n" +
                " 1981-02-22  | WARD  |1981-02-22 |";
        assertEquals("\n"+sqlText+"\n", expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
        rs.close();
    }

    @Test @Ignore("DB-3226 window joing order")
    public void testDB2472BufferUnderflow() throws Exception {
        String sqlText =
            String.format("SELECT  BIP.Individual_ID ,BIP.Residence_ID ,BIP.Household_ID ,BIP.FILE_ID ," +
                              "BIP.Record_ID " +
                              ",BAF.Generation_Qualifier ,BAF.First_Name as FName ,BAF.Last_Name as LName ," +
                              "BAF.US_Address_Line_1 " +
                              ",BAF.US_Address_Line_2 ,BAF.US_Address_Line_3 ,BAF.US_State ,BAF.US_City ," +
                              "BAF.US_Zip ,BAF.US_Zip4 " +
                              ",BIP.Brand ,BIP.created_by ,BIP.created_date ,BIP.job_id ," +
                              "RANK() OVER  (PARTITION BY BIP.Individual_ID " +
                              "ORDER BY  BIP.rSourceRecord_sum ASC, BIP.Source_Date DESC, BIP.Record_ID desc, " +
                              "BIP.Individual_ID ASC ) " +
                              "AS rowrnk FROM  %s BAF  INNER JOIN %s BIP ON BAF.individual_id=BIP.individual_id " +
                              "WHERE BAF.rwrnk = 1",
                          this.getTableReference(best_addr_freq_NAME), this.getTableReference(best_ids_pool_NAME));
        ResultSet rs = methodWatcher.executeQuery(sqlText);
//        System.out.println("\n" + sqlText + "\n" + TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
        // Results too large
        assertNotNull("\n" + sqlText + "\n", TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
        rs.close();
    }

    @Test
    public void testLastValueFunction() throws Exception {
        // DB-3920
        String tableName = "emp2";
        String tableRef = SCHEMA+"."+tableName;
        String tableDef = "(EMPNO int, EMPNAME varchar(20), SALARY int, DEPTNO int)";
        new TableDAO(methodWatcher.getOrCreateConnection()).drop(SCHEMA, tableName);

        new TableCreator(methodWatcher.getOrCreateConnection())
            .withCreate(String.format("create table %s %s", tableRef, tableDef))
            .withInsert(String.format("insert into %s (EMPNO, EMPNAME, SALARY, DEPTNO) values (?,?,?,?)", tableRef))
            .withRows(rows(
                row(10, "Bill", 12000, 5), row(11, "Solomon", 10000, 5), row(12, "Susan", 10000, 5),
                row(13, "Wendy", 9000, 1), row(14, "Benjamin", 7500, 1), row(15, "Tom", 7600, 1),
                row(16, "Henry", 8500, 2), row(17, "Robert", 9500, 2), row(18, "Paul", 7700, 2),
                row(19, "Dora", 8500, 3), row(20, "Samuel", 6900, 3), row(21, "Mary", 7500, 3),
                row(22, "Daniel", 6500, 4), row(23, "Ricardo", 7800, 4), row(24, "Mark", 7200, 4)
            ))
        .create();

        String sqlText = format("select empno, salary, deptno, last_value(empno) over(partition by deptno order by " +
                                    "salary asc range between current row and unbounded following) as last_val from " +
                                    "%s order by empno asc", tableRef);
        ResultSet rs = methodWatcher.executeQuery(sqlText);
        // Verified with PostgreSQL App
        String expected =
            "EMPNO |SALARY |DEPTNO |LAST_VAL |\n" +
                "----------------------------------\n" +
                "  10   | 12000 |   5   |   10    |\n" +
                "  11   | 10000 |   5   |   10    |\n" +
                "  12   | 10000 |   5   |   10    |\n" +
                "  13   | 9000  |   1   |   13    |\n" +
                "  14   | 7500  |   1   |   13    |\n" +
                "  15   | 7600  |   1   |   13    |\n" +
                "  16   | 8500  |   2   |   17    |\n" +
                "  17   | 9500  |   2   |   17    |\n" +
                "  18   | 7700  |   2   |   17    |\n" +
                "  19   | 8500  |   3   |   19    |\n" +
                "  20   | 6900  |   3   |   19    |\n" +
                "  21   | 7500  |   3   |   19    |\n" +
                "  22   | 6500  |   4   |   23    |\n" +
                "  23   | 7800  |   4   |   23    |\n" +
                "  24   | 7200  |   4   |   23    |";
        assertEquals("\n"+sqlText+"\n", expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
        rs.close();
    }

    @Test
    public void testFirstValueFunction() throws Exception {
        // DB-3920
        String tableName = "emp3";
        String tableRef = SCHEMA+"."+tableName;
        String tableDef = "(EMPNO int, EMPNAME varchar(20), SALARY int, DEPTNO int)";
        new TableDAO(methodWatcher.getOrCreateConnection()).drop(SCHEMA, tableName);

        new TableCreator(methodWatcher.getOrCreateConnection())
            .withCreate(String.format("create table %s %s", tableRef, tableDef))
            .withInsert(String.format("insert into %s (EMPNO, EMPNAME, SALARY, DEPTNO) values (?,?,?,?)", tableRef))
            .withRows(rows(
                row(10, "Bill", 12000, 5), row(11, "Solomon", 10000, 5), row(12, "Susan", 10000, 5),
                row(13, "Wendy", 9000, 1), row(14, "Benjamin", 7500, 1), row(15, "Tom", 7600, 1),
                row(16, "Henry", 8500, 2), row(17, "Robert", 9500, 2), row(18, "Paul", 7700, 2),
                row(19, "Dora", 8500, 3), row(20, "Samuel", 6900, 3), row(21, "Mary", 7500, 3),
                row(22, "Daniel", 6500, 4), row(23, "Ricardo", 7800, 4), row(24, "Mark", 7200, 4)
            ))
        .create();

        String sqlText = format("select empno, salary, deptno, first_value(empno) over(partition by deptno order by " +
                                    "salary asc rows unbounded preceding) as first_value from " +
                                    "%s order by empno asc", tableRef);
        ResultSet rs = methodWatcher.executeQuery(sqlText);
        // Verified with PostgreSQL App
        String expected =
            "EMPNO |SALARY |DEPTNO | FIRST_VALUE |\n" +
                "--------------------------------------\n" +
                "  10   | 12000 |   5   |     11      |\n" +
                "  11   | 10000 |   5   |     11      |\n" +
                "  12   | 10000 |   5   |     11      |\n" +
                "  13   | 9000  |   1   |     14      |\n" +
                "  14   | 7500  |   1   |     14      |\n" +
                "  15   | 7600  |   1   |     14      |\n" +
                "  16   | 8500  |   2   |     18      |\n" +
                "  17   | 9500  |   2   |     18      |\n" +
                "  18   | 7700  |   2   |     18      |\n" +
                "  19   | 8500  |   3   |     20      |\n" +
                "  20   | 6900  |   3   |     20      |\n" +
                "  21   | 7500  |   3   |     20      |\n" +
                "  22   | 6500  |   4   |     22      |\n" +
                "  23   | 7800  |   4   |     22      |\n" +
                "  24   | 7200  |   4   |     22      |";
        assertEquals("\n"+sqlText+"\n", expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
        rs.close();
    }

    @Test
    public void testLastValueWithAggregateArgument() throws Exception {
        // DB-3920
        String tableName = "all_sales2";
        String tableRef = SCHEMA+"."+tableName;
        String tableDef = "(yr INTEGER, month INTEGER, prd_type_id INTEGER, emp_id INTEGER , amount decimal(8, 2))";
        new TableDAO(methodWatcher.getOrCreateConnection()).drop(SCHEMA, tableName);


        new TableCreator(methodWatcher.getOrCreateConnection())
            .withCreate(String.format("create table %s %s", tableRef, tableDef))
            .withInsert(String.format("insert into %s (yr,MONTH,PRD_TYPE_ID,EMP_ID,AMOUNT) values (?,?,?,?,?)",
                                      tableRef))
            .withRows(rows(
                row(2006, 1, 1, 21, 16034.84), row(2006, 2, 1, 21, 15644.65), row(2006, 3, 2, 21, 20167.83),
                row(2006, 4, 2, 21, 25056.45), row(2006, 5, 2, 21, null), row(2006, 6, 1, 21, 15564.66),
                row(2006, 7, 1, 21, 15644.65), row(2006, 8, 1, 21, 16434.82), row(2006, 9, 1, 21, 19654.57),
                row(2006, 10, 1, 21, 21764.19), row(2006, 11, 1, 21, 13026.73), row(2006, 12, 2, 21, 10034.64),
                row(2005, 1, 2, 22, 16634.84), row(2005, 1, 2, 21, 26034.84), row(2005, 2, 1, 21, 12644.65),
                row(2005, 3, 1, 21, null), row(2005, 4, 1, 21, 25026.45), row(2005, 5, 1, 21, 17212.66),
                row(2005, 6, 1, 21, 15564.26), row(2005, 7, 2, 21, 62654.82), row(2005, 8, 2, 21, 26434.82),
                row(2005, 9, 2, 21, 15644.65), row(2005, 10, 2, 21, 21264.19), row(2005, 11, 1, 21, 13026.73),
                row(2005, 12, 1, 21, 10032.64)
            ))
            .create();

        String sqlText = format("SELECT month, SUM(amount) AS month_amount, " +
                                    "LAST_VALUE(SUM(amount)) OVER (ORDER BY month ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) " +
                                    "AS next_month_amount FROM %s GROUP BY month ORDER BY month", tableRef);
        ResultSet rs = methodWatcher.executeQuery(sqlText);
        // Verified with PostgreSQL App
        String expected =
            "MONTH |MONTH_AMOUNT | NEXT_MONTH_AMOUNT |\n" +
                "------------------------------------------\n" +
                "   1   |  58704.52   |     28289.30      |\n" +
                "   2   |  28289.30   |     20167.83      |\n" +
                "   3   |  20167.83   |     50082.90      |\n" +
                "   4   |  50082.90   |     17212.66      |\n" +
                "   5   |  17212.66   |     31128.92      |\n" +
                "   6   |  31128.92   |     78299.47      |\n" +
                "   7   |  78299.47   |     42869.64      |\n" +
                "   8   |  42869.64   |     35299.22      |\n" +
                "   9   |  35299.22   |     43028.38      |\n" +
                "  10   |  43028.38   |     26053.46      |\n" +
                "  11   |  26053.46   |     20067.28      |\n" +
                "  12   |  20067.28   |     20067.28      |";
        assertEquals("\n"+sqlText+"\n", expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
        rs.close();
    }

    @Test @Ignore("DB-3920: found possible frame processing problem while implementing first_value()")
    public void testFirstValueWithAggregateArgument() throws Exception {
        // DB-3920
        String tableName = "all_sales3";
        String tableRef = SCHEMA+"."+tableName;
        String tableDef = "(yr INTEGER, month INTEGER, prd_type_id INTEGER, emp_id INTEGER , amount decimal(8, 2))";
        new TableDAO(methodWatcher.getOrCreateConnection()).drop(SCHEMA, tableName);


        new TableCreator(methodWatcher.getOrCreateConnection())
            .withCreate(String.format("create table %s %s", tableRef, tableDef))
            .withInsert(String.format("insert into %s (yr,MONTH,PRD_TYPE_ID,EMP_ID,AMOUNT) values (?,?,?,?,?)",
                                      tableRef))
            .withRows(rows(
                row(2006, 1, 1, 21, 16034.84), row(2006, 2, 1, 21, 15644.65), row(2006, 3, 2, 21, 20167.83),
                row(2006, 4, 2, 21, 25056.45), row(2006, 5, 2, 21, null), row(2006, 6, 1, 21, 15564.66),
                row(2006, 7, 1, 21, 15644.65), row(2006, 8, 1, 21, 16434.82), row(2006, 9, 1, 21, 19654.57),
                row(2006, 10, 1, 21, 21764.19), row(2006, 11, 1, 21, 13026.73), row(2006, 12, 2, 21, 10034.64),
                row(2005, 1, 2, 22, 16634.84), row(2005, 1, 2, 21, 26034.84), row(2005, 2, 1, 21, 12644.65),
                row(2005, 3, 1, 21, null), row(2005, 4, 1, 21, 25026.45), row(2005, 5, 1, 21, 17212.66),
                row(2005, 6, 1, 21, 15564.26), row(2005, 7, 2, 21, 62654.82), row(2005, 8, 2, 21, 26434.82),
                row(2005, 9, 2, 21, 15644.65), row(2005, 10, 2, 21, 21264.19), row(2005, 11, 1, 21, 13026.73),
                row(2005, 12, 1, 21, 10032.64)
            ))
            .create();

        String sqlText = format("SELECT month, SUM(amount) AS month_amount, " +
                                    "FIRST_VALUE(SUM(amount)) OVER (ORDER BY month ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) " +
                                    "AS prev_month_amount FROM %s GROUP BY month ORDER BY month", tableRef);
        ResultSet rs = methodWatcher.executeQuery(sqlText);
        // Verified with PostgreSQL App
        String expected =
            "MONTH |MONTH_AMOUNT | PREV_MONTH_AMOUNT |\n" +
                "------------------------------------------\n" +
                "   1   |  58704.52   |     58704.52      |\n" +
                "   2   |  28289.30   |     58704.52      |\n" +
                "   3   |  20167.83   |     28289.30      |\n" +
                "   4   |  50082.90   |     20167.83      |\n" +
                "   5   |  17212.66   |     50082.90      |\n" +
                "   6   |  31128.92   |     17212.66      |\n" +
                "   7   |  78299.47   |     31128.92      |\n" +
                "   8   |  42869.64   |     78299.47      |\n" +
                "   9   |  35299.22   |     42869.64      |\n" +
                "  10   |  43028.38   |     35299.22      |\n" +
                "  11   |  26053.46   |     43028.38      |\n" +
                "  12   |  20067.28   |     26053.46      |";
        assertEquals("\n"+sqlText+"\n", expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
        rs.close();
    }

    @Test
    public void testLeadFunction() throws Exception {
        // DB-3920
        String tableName = "emp3";
        String tableRef = SCHEMA+"."+tableName;
        String tableDef = "(EMPNO int, EMPNAME varchar(20), SALARY int, DEPTNO int)";
        new TableDAO(methodWatcher.getOrCreateConnection()).drop(SCHEMA, tableName);

        new TableCreator(methodWatcher.getOrCreateConnection())
            .withCreate(String.format("create table %s %s", tableRef, tableDef))
            .withInsert(String.format("insert into %s (EMPNO, EMPNAME, SALARY, DEPTNO) values (?,?,?,?)", tableRef))
            .withRows(rows(
                row(10, "Bill", 12000, 5), row(11, "Solomon", 10000, 5), row(12, "Susan", 10000, 5),
                row(13, "Wendy", 9000, 1), row(14, "Benjamin", 7500, 1), row(15, "Tom", 7600, 1),
                row(16, "Henry", 8500, 2), row(17, "Robert", 9500, 2), row(18, "Paul", 7700, 2),
                row(19, "Dora", 8500, 3), row(20, "Samuel", 6900, 3), row(21, "Mary", 7500, 3),
                row(22, "Daniel", 6500, 4), row(23, "Ricardo", 7800, 4), row(24, "Mark", 7200, 4)
            ))
            .create();

        String sqlText = format("select empno, salary, deptno, " +
                                    "LEAD(SALARY) OVER (PARTITION BY DEPTNO ORDER BY SALARY DESC) NEXT_LOWER_SAL from " +
                                    "%s order by deptno, SALARY DESC", tableRef);
        ResultSet rs = methodWatcher.executeQuery(sqlText);
        // Verified with PostgreSQL App
        String expected =
            "EMPNO |SALARY |DEPTNO |NEXT_LOWER_SAL |\n" +
                "----------------------------------------\n" +
                "  13   | 9000  |   1   |     7600      |\n" +
                "  15   | 7600  |   1   |     7500      |\n" +
                "  14   | 7500  |   1   |     NULL      |\n" +
                "  17   | 9500  |   2   |     8500      |\n" +
                "  16   | 8500  |   2   |     7700      |\n" +
                "  18   | 7700  |   2   |     NULL      |\n" +
                "  19   | 8500  |   3   |     7500      |\n" +
                "  21   | 7500  |   3   |     6900      |\n" +
                "  20   | 6900  |   3   |     NULL      |\n" +
                "  23   | 7800  |   4   |     7200      |\n" +
                "  24   | 7200  |   4   |     6500      |\n" +
                "  22   | 6500  |   4   |     NULL      |\n" +
                "  10   | 12000 |   5   |     10000     |\n" +
                "  11   | 10000 |   5   |     10000     |\n" +
                "  12   | 10000 |   5   |     NULL      |";
        assertEquals("\n"+sqlText+"\n", expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
        rs.close();
    }

    @Test
    public void testLagFunction() throws Exception {
        // DB-3920
        String tableName = "emp3";
        String tableRef = SCHEMA+"."+tableName;
        String tableDef = "(EMPNO int, EMPNAME varchar(20), SALARY int, DEPTNO int)";
        new TableDAO(methodWatcher.getOrCreateConnection()).drop(SCHEMA, tableName);

        new TableCreator(methodWatcher.getOrCreateConnection())
            .withCreate(String.format("create table %s %s", tableRef, tableDef))
            .withInsert(String.format("insert into %s (EMPNO, EMPNAME, SALARY, DEPTNO) values (?,?,?,?)", tableRef))
            .withRows(rows(
                row(10, "Bill", 12000, 5), row(11, "Solomon", 10000, 5), row(12, "Susan", 10000, 5),
                row(13, "Wendy", 9000, 1), row(14, "Benjamin", 7500, 1), row(15, "Tom", 7600, 1),
                row(16, "Henry", 8500, 2), row(17, "Robert", 9500, 2), row(18, "Paul", 7700, 2),
                row(19, "Dora", 8500, 3), row(20, "Samuel", 6900, 3), row(21, "Mary", 7500, 3),
                row(22, "Daniel", 6500, 4), row(23, "Ricardo", 7800, 4), row(24, "Mark", 7200, 4)
            ))
            .create();

        String sqlText = format("select empno, salary, deptno, " +
                                    "LAG(SALARY) OVER (PARTITION BY DEPTNO ORDER BY SALARY DESC) NEXT_LOWER_SAL from " +
                                    "%s order by deptno, SALARY DESC", tableRef);
        ResultSet rs = methodWatcher.executeQuery(sqlText);
        // Verified with PostgreSQL App
        String expected =
            "EMPNO |SALARY |DEPTNO |NEXT_LOWER_SAL |\n" +
                "----------------------------------------\n" +
                "  13   | 9000  |   1   |     NULL      |\n" +
                "  15   | 7600  |   1   |     9000      |\n" +
                "  14   | 7500  |   1   |     7600      |\n" +
                "  17   | 9500  |   2   |     NULL      |\n" +
                "  16   | 8500  |   2   |     9500      |\n" +
                "  18   | 7700  |   2   |     8500      |\n" +
                "  19   | 8500  |   3   |     NULL      |\n" +
                "  21   | 7500  |   3   |     8500      |\n" +
                "  20   | 6900  |   3   |     7500      |\n" +
                "  23   | 7800  |   4   |     NULL      |\n" +
                "  24   | 7200  |   4   |     7800      |\n" +
                "  22   | 6500  |   4   |     7200      |\n" +
                "  10   | 12000 |   5   |     NULL      |\n" +
                "  11   | 10000 |   5   |     12000     |\n" +
                "  12   | 10000 |   5   |     10000     |";
        assertEquals("\n"+sqlText+"\n", expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
        rs.close();
    }

    @Test
    public void testLastValueFunctionWithCaseArgument() throws Exception {
        // DB-3920
        String tableName = "emp4";
        String tableRef = SCHEMA+"."+tableName;
        String tableDef = "(EMPNO int, EMPNAME varchar(20), SALARY int, DEPTNO int)";
        new TableDAO(methodWatcher.getOrCreateConnection()).drop(SCHEMA, tableName);

        new TableCreator(methodWatcher.getOrCreateConnection())
            .withCreate(String.format("create table %s %s", tableRef, tableDef))
            .withInsert(String.format("insert into %s (EMPNO, EMPNAME, SALARY, DEPTNO) values (?,?,?,?)", tableRef))
            .withRows(rows(
                row(10, "Bill", 12000, 5), row(11, "Solomon", 10000, 5), row(12, "Susan", 10000, 5),
                row(13, "Wendy", 9000, 1), row(14, "Benjamin", 7500, 1), row(15, "Tom", 7600, 1),
                row(16, "Henry", 8500, 2), row(17, "Robert", 9500, 2), row(18, "Paul", 7700, 2),
                row(19, "Dora", 8500, 3), row(20, "Samuel", 6900, 3), row(21, "Mary", 7500, 3),
                row(22, "Daniel", 6500, 4), row(23, "Ricardo", 7800, 4), row(24, "Mark", 7200, 4)
            ))
            .create();

        String sqlText = format("select empno, salary, deptno, " +
                                "last_value((CASE WHEN empno < 17 THEN 0 WHEN empno >= 17 THEN 1 END)) " +
                                "over(partition by deptno order by salary asc range between current row and unbounded following) as last_val from " +
                                "%s order by empno asc", tableRef);
        ResultSet rs = methodWatcher.executeQuery(sqlText);
        // Verified with PostgreSQL App
        String expected =
            "EMPNO |SALARY |DEPTNO |LAST_VAL |\n" +
                "----------------------------------\n" +
                "  10   | 12000 |   5   |    0    |\n" +
                "  11   | 10000 |   5   |    0    |\n" +
                "  12   | 10000 |   5   |    0    |\n" +
                "  13   | 9000  |   1   |    0    |\n" +
                "  14   | 7500  |   1   |    0    |\n" +
                "  15   | 7600  |   1   |    0    |\n" +
                "  16   | 8500  |   2   |    1    |\n" +
                "  17   | 9500  |   2   |    1    |\n" +
                "  18   | 7700  |   2   |    1    |\n" +
                "  19   | 8500  |   3   |    1    |\n" +
                "  20   | 6900  |   3   |    1    |\n" +
                "  21   | 7500  |   3   |    1    |\n" +
                "  22   | 6500  |   4   |    1    |\n" +
                "  23   | 7800  |   4   |    1    |\n" +
                "  24   | 7200  |   4   |    1    |";
        assertEquals("\n"+sqlText+"\n", expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
        rs.close();
    }

    @Test
    public void testFirstValueFunctionWithCaseArgument() throws Exception {
        // DB-3920
        String tableName = "emp5";
        String tableRef = SCHEMA+"."+tableName;
        String tableDef = "(EMPNO int, EMPNAME varchar(20), SALARY int, DEPTNO int)";
        new TableDAO(methodWatcher.getOrCreateConnection()).drop(SCHEMA, tableName);

        new TableCreator(methodWatcher.getOrCreateConnection())
            .withCreate(String.format("create table %s %s", tableRef, tableDef))
            .withInsert(String.format("insert into %s (EMPNO, EMPNAME, SALARY, DEPTNO) values (?,?,?,?)", tableRef))
            .withRows(rows(
                row(10, "Bill", 12000, 5), row(11, "Solomon", 10000, 5), row(12, "Susan", 10000, 5),
                row(13, "Wendy", 9000, 1), row(14, "Benjamin", 7500, 1), row(15, "Tom", 7600, 1),
                row(16, "Henry", 8500, 2), row(17, "Robert", 9500, 2), row(18, "Paul", 7700, 2),
                row(19, "Dora", 8500, 3), row(20, "Samuel", 6900, 3), row(21, "Mary", 7500, 3),
                row(22, "Daniel", 6500, 4), row(23, "Ricardo", 7800, 4), row(24, "Mark", 7200, 4)
            ))
            .create();

        String sqlText = format("select empno, salary, deptno, " +
                                "first_value((CASE WHEN empno < 17 THEN 1 WHEN empno >= 17 THEN 0 END)) " +
                                "over(partition by deptno order by salary asc rows unbounded preceding) as first_val from " +
                                "%s order by empno asc", tableRef);
        ResultSet rs = methodWatcher.executeQuery(sqlText);
        // Verified with PostgreSQL App
        String expected =
            "EMPNO |SALARY |DEPTNO | FIRST_VAL |\n" +
                "------------------------------------\n" +
                "  10   | 12000 |   5   |     1     |\n" +
                "  11   | 10000 |   5   |     1     |\n" +
                "  12   | 10000 |   5   |     1     |\n" +
                "  13   | 9000  |   1   |     1     |\n" +
                "  14   | 7500  |   1   |     1     |\n" +
                "  15   | 7600  |   1   |     1     |\n" +
                "  16   | 8500  |   2   |     0     |\n" +
                "  17   | 9500  |   2   |     0     |\n" +
                "  18   | 7700  |   2   |     0     |\n" +
                "  19   | 8500  |   3   |     0     |\n" +
                "  20   | 6900  |   3   |     0     |\n" +
                "  21   | 7500  |   3   |     0     |\n" +
                "  22   | 6500  |   4   |     0     |\n" +
                "  23   | 7800  |   4   |     0     |\n" +
                "  24   | 7200  |   4   |     0     |";
        assertEquals("\n"+sqlText+"\n", expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
        rs.close();
    }

    @Test
    public void testLastValueFunctionWithIgnoreNulls() throws Exception {
        // DB-3920
        String tableName = "employees";
        String tableRef = SCHEMA+"."+tableName;
        String tableDef = "(employee_id int, employee_name varchar(10), salary int, department varchar(10), commission int)";
        new TableDAO(methodWatcher.getOrCreateConnection()).drop(SCHEMA, tableName);

        new TableCreator(methodWatcher.getOrCreateConnection())
            .withCreate(String.format("create table %s %s", tableRef, tableDef))
            .withInsert(String.format("insert into %s values (?,?,?,?,?)", tableRef))
            .withRows(rows(
                row(101, "Emp A", 10000, "Sales", null),
                row(102, "Emp B", 20000, "IT", 20),
                row(103, "Emp C", 28000, "IT", 20),
                row(104, "Emp D", 30000, "Support", 5),
                row(105, "Emp E", 32000, "Sales", 10),
                row(106, "Emp F", 20000, "Sales", 5),
                row(107, "Emp G", 12000, "Sales", null),
                row(108, "Emp H", 12000, "Support", null)
            ))
            .create();

        // IGNORE NULLS
        String sqlText = format("SELECT employee_id\n" +
                                    "       ,employee_name\n" +
                                    "       ,department\n" +
                                    "       ,LAST_VALUE(commission IGNORE NULLS) OVER (PARTITION BY department\n" +
                                    "                   ORDER BY employee_id DESC\n" +
                                    "                   ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) " +
                                    "Minimum_Commission\n" +
                                    "FROM %s", tableRef);
        ResultSet rs = methodWatcher.executeQuery(sqlText);
        // Verified with example: http://www.techhoney.com/oracle/function/last_value-function-with-partition-by-clause-in-oracle-sql-plsql/
        String expected =
            "EMPLOYEE_ID | EMPLOYEE_NAME |DEPARTMENT |MINIMUM_COMMISSION |\n" +
                "--------------------------------------------------------------\n" +
                "     107     |     Emp G     |   Sales   |        10         |\n" +
                "     106     |     Emp F     |   Sales   |        10         |\n" +
                "     105     |     Emp E     |   Sales   |        10         |\n" +
                "     101     |     Emp A     |   Sales   |        10         |\n" +
                "     108     |     Emp H     |  Support  |         5         |\n" +
                "     104     |     Emp D     |  Support  |         5         |\n" +
                "     103     |     Emp C     |    IT     |        20         |\n" +
                "     102     |     Emp B     |    IT     |        20         |";
        assertEquals("\n"+sqlText+"\n", expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));

        // RESPECT NULLS
        sqlText = format("SELECT employee_id\n" +
                                    "       ,employee_name\n" +
                                    "       ,department\n" +
                                    "       ,LAST_VALUE(commission) OVER (PARTITION BY department\n" +
                                    "                   ORDER BY employee_id DESC\n" +
                                    "                   ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) " +
                                    "Minimum_Commission\n" +
                                    "FROM %s", tableRef);
        rs = methodWatcher.executeQuery(sqlText);
        // Verified with example: http://www.techhoney.com/oracle/function/last_value-function-with-partition-by-clause-in-oracle-sql-plsql/
        expected =
            "EMPLOYEE_ID | EMPLOYEE_NAME |DEPARTMENT |MINIMUM_COMMISSION |\n" +
                "--------------------------------------------------------------\n" +
                "     107     |     Emp G     |   Sales   |       NULL        |\n" +
                "     106     |     Emp F     |   Sales   |       NULL        |\n" +
                "     105     |     Emp E     |   Sales   |       NULL        |\n" +
                "     101     |     Emp A     |   Sales   |       NULL        |\n" +
                "     108     |     Emp H     |  Support  |         5         |\n" +
                "     104     |     Emp D     |  Support  |         5         |\n" +
                "     103     |     Emp C     |    IT     |        20         |\n" +
                "     102     |     Emp B     |    IT     |        20         |";
        assertEquals("\n"+sqlText+"\n", expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
        rs.close();
    }

    //==================================================================================================================
    // Tests for multiple window functions in one query
    //==================================================================================================================

    @Test
    public void testRankDate() throws Exception {
        String sqlText =
            String.format("SELECT hiredate, dept, rank() OVER (partition by dept ORDER BY hiredate) AS rankhire FROM %s",
                          this.getTableReference(EMPTAB_HIRE_DATE));

        ResultSet rs = methodWatcher.executeQuery(sqlText);
        String expected =
            "HIREDATE  |DEPT |RANKHIRE |\n" +
                "----------------------------\n" +
                "2012-04-03 |  2  |    1    |\n" +
                "2012-04-03 |  2  |    1    |\n" +
                "2013-06-06 |  2  |    3    |\n" +
                "2013-12-20 |  2  |    4    |\n" +
                "2010-04-12 |  3  |    1    |\n" +
                "2010-08-09 |  3  |    2    |\n" +
                "2012-04-03 |  3  |    3    |\n" +
                "2013-04-24 |  3  |    4    |\n" +
                "2010-03-20 |  1  |    1    |\n" +
                "2010-03-20 |  1  |    1    |\n" +
                "2011-05-24 |  1  |    3    |\n" +
                "2011-10-15 |  1  |    4    |\n" +
                "2012-04-03 |  1  |    5    |\n" +
                "2012-11-11 |  1  |    6    |\n" +
                "2014-03-04 |  1  |    7    |";
        assertEquals("\n"+sqlText+"\n", expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
        rs.close();
    }
    @Test
    public void testNullsRankDate() throws Exception {
        String sqlText =
            String.format("SELECT hiredate, dept, " +
                              "rank() OVER (partition by dept ORDER BY hiredate) AS rankhire FROM %s",
                          this.getTableReference(EMPTAB_NULLS));

        ResultSet rs = methodWatcher.executeQuery(sqlText);
        String expected =
            "HIREDATE  |DEPT |RANKHIRE |\n" +
                "----------------------------\n" +
                "2012-04-03 |  2  |    1    |\n" +
                "2012-04-03 |  2  |    1    |\n" +
                "2013-06-06 |  2  |    3    |\n" +
                "2013-12-20 |  2  |    4    |\n" +
                "2010-04-12 |  3  |    1    |\n" +
                "2010-08-09 |  3  |    2    |\n" +
                "2010-08-09 |  3  |    2    |\n" +
                "2012-04-03 |  3  |    4    |\n" +
                "2013-04-24 |  3  |    5    |\n" +
                "2010-03-20 |  1  |    1    |\n" +
                "2010-03-20 |  1  |    1    |\n" +
                "2010-08-09 |  1  |    3    |\n" +
                "2011-05-24 |  1  |    4    |\n" +
                "2011-10-15 |  1  |    5    |\n" +
                "2012-04-03 |  1  |    6    |\n" +
                "2012-11-11 |  1  |    7    |\n" +
                "2014-03-04 |  1  |    8    |";
        assertEquals("\n"+sqlText+"\n", expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
        rs.close();
    }

    @Test
    public void testMultiFunctionSameOverClause() throws Exception {
        String sqlText =
            String.format("SELECT empnum, dept, salary, " +
                              "DENSE_RANK() OVER (PARTITION BY dept ORDER BY salary desc, empnum) AS DenseRank, " +
                              "RANK() OVER (PARTITION BY dept ORDER BY salary desc, empnum) AS Rank, " +
                              "ROW_NUMBER() OVER (PARTITION BY dept ORDER BY salary desc, empnum) AS RowNumber " +
                              "FROM %s",
                          this.getTableReference(EMPTAB_HIRE_DATE));

        ResultSet rs = methodWatcher.executeQuery(sqlText);
        String expected =
            "EMPNUM |DEPT |SALARY | DENSERANK |RANK | ROWNUMBER |\n" +
                "----------------------------------------------------\n" +
                "  49   |  2  | 53000 |     1     |  1  |     1     |\n" +
                "  40   |  2  | 52000 |     2     |  2  |     2     |\n" +
                "  44   |  2  | 52000 |     3     |  3  |     3     |\n" +
                "  90   |  2  | 51000 |     4     |  4  |     4     |\n" +
                "  30   |  3  | 84000 |     1     |  1  |     1     |\n" +
                "  80   |  3  | 79000 |     2     |  2  |     2     |\n" +
                "  120  |  3  | 75000 |     3     |  3  |     3     |\n" +
                "  100  |  3  | 55000 |     4     |  4  |     4     |\n" +
                "  60   |  1  | 78000 |     1     |  1  |     1     |\n" +
                "  70   |  1  | 76000 |     2     |  2  |     2     |\n" +
                "  20   |  1  | 75000 |     3     |  3  |     3     |\n" +
                "  110  |  1  | 53000 |     4     |  4  |     4     |\n" +
                "  50   |  1  | 52000 |     5     |  5  |     5     |\n" +
                "  55   |  1  | 52000 |     6     |  6  |     6     |\n" +
                "  10   |  1  | 50000 |     7     |  7  |     7     |";
        assertEquals("\n"+sqlText+"\n", expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
        rs.close();
    }

    @Test
    public void testNullsMultiFunctionSameOverClause() throws Exception {
        String sqlText =
            String.format("SELECT empnum, dept, salary, " +
                              "DENSE_RANK() OVER (PARTITION BY dept ORDER BY salary desc, empnum) AS DenseRank, " +
                              "RANK() OVER (PARTITION BY dept ORDER BY salary desc, empnum) AS Rank, " +
                              "ROW_NUMBER() OVER (PARTITION BY dept ORDER BY salary desc, empnum) AS RowNumber " +
                              "FROM %s",
                          this.getTableReference(EMPTAB_NULLS));

        ResultSet rs = methodWatcher.executeQuery(sqlText);
        String expected =
            "EMPNUM |DEPT |SALARY | DENSERANK |RANK | ROWNUMBER |\n" +
                "----------------------------------------------------\n" +
                "  49   |  2  | 53000 |     1     |  1  |     1     |\n" +
                "  40   |  2  | 52000 |     2     |  2  |     2     |\n" +
                "  44   |  2  | 52000 |     3     |  3  |     3     |\n" +
                "  90   |  2  | 51000 |     4     |  4  |     4     |\n" +
                "  33   |  3  | NULL  |     1     |  1  |     1     |\n" +
                "  30   |  3  | 84000 |     2     |  2  |     2     |\n" +
                "  80   |  3  | 79000 |     3     |  3  |     3     |\n" +
                "  120  |  3  | 75000 |     4     |  4  |     4     |\n" +
                "  100  |  3  | 55000 |     5     |  5  |     5     |\n" +
                "  32   |  1  | NULL  |     1     |  1  |     1     |\n" +
                "  60   |  1  | 78000 |     2     |  2  |     2     |\n" +
                "  70   |  1  | 76000 |     3     |  3  |     3     |\n" +
                "  20   |  1  | 75000 |     4     |  4  |     4     |\n" +
                "  110  |  1  | 53000 |     5     |  5  |     5     |\n" +
                "  50   |  1  | 52000 |     6     |  6  |     6     |\n" +
                "  55   |  1  | 52000 |     7     |  7  |     7     |\n" +
                "  10   |  1  | 50000 |     8     |  8  |     8     |";
        assertEquals("\n"+sqlText+"\n", expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
        rs.close();
    }

    @Test
    public void testMultiFunctionSamePartitionDifferentOrderBy() throws Exception {
        // DB-1989 - Attempted to encode a value that does not have a scalar type format id
        String sqlText =
            String.format("SELECT empnum, hiredate, dept, salary, " +
                              "DENSE_RANK() OVER (PARTITION BY dept ORDER BY salary desc, hiredate) AS DenseRank, " +
                              "dept, " +
                              "ROW_NUMBER() OVER (PARTITION BY dept ORDER BY hiredate desc) AS RowNumber " +
                              "FROM %s order by hiredate desc, empnum",
                          this.getTableReference(EMPTAB_HIRE_DATE));

        ResultSet rs = methodWatcher.executeQuery(sqlText);
        String expected =
            "EMPNUM | HIREDATE  |DEPT |SALARY | DENSERANK |DEPT | ROWNUMBER |\n" +
                "----------------------------------------------------------------\n" +
                "  60   |2014-03-04 |  1  | 78000 |     1     |  1  |     1     |\n" +
                "  44   |2013-12-20 |  2  | 52000 |     3     |  2  |     1     |\n" +
                "  40   |2013-06-06 |  2  | 52000 |     2     |  2  |     2     |\n" +
                "  80   |2013-04-24 |  3  | 79000 |     2     |  3  |     1     |\n" +
                "  20   |2012-11-11 |  1  | 75000 |     3     |  1  |     2     |\n" +
                "  49   |2012-04-03 |  2  | 53000 |     1     |  2  |     3     |\n" +
                "  70   |2012-04-03 |  1  | 76000 |     2     |  1  |     3     |\n" +
                "  90   |2012-04-03 |  2  | 51000 |     4     |  2  |     4     |\n" +
                "  120  |2012-04-03 |  3  | 75000 |     3     |  3  |     2     |\n" +
                "  55   |2011-10-15 |  1  | 52000 |     6     |  1  |     4     |\n" +
                "  50   |2011-05-24 |  1  | 52000 |     5     |  1  |     5     |\n" +
                "  30   |2010-08-09 |  3  | 84000 |     1     |  3  |     3     |\n" +
                "  100  |2010-04-12 |  3  | 55000 |     4     |  3  |     4     |\n" +
                "  10   |2010-03-20 |  1  | 50000 |     7     |  1  |     7     |\n" +
                "  110  |2010-03-20 |  1  | 53000 |     4     |  1  |     6     |";
        assertEquals("\n"+sqlText+"\n", expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
        rs.close();
    }

    @Test
    public void testNullsMultiFunctionSamePartitionDifferentOrderBy() throws Exception {
        // DB-1989 - Attempted to encode a value that does not have a scalar type format id
        String sqlText =
            String.format("SELECT empnum, hiredate, salary, " +
                              "DENSE_RANK() OVER (PARTITION BY dept ORDER BY hiredate, salary) AS DR_Hire_Sal_By_Dept, " +
                              "dept, " +
                              "ROW_NUMBER() OVER (PARTITION BY dept ORDER BY hiredate, dept) AS RN_Hire_Sal_By_Dept " +
                              "FROM %s order by dept, hiredate",
                          this.getTableReference(EMPTAB_NULLS));

        ResultSet rs = methodWatcher.executeQuery(sqlText);
        String expected =
            "EMPNUM | HIREDATE  |SALARY | DR_HIRE_SAL_BY_DEPT |DEPT | RN_HIRE_SAL_BY_DEPT |\n" +
                "------------------------------------------------------------------------------\n" +
                "  10   |2010-03-20 | 50000 |          1          |  1  |          1          |\n" +
                "  110  |2010-03-20 | 53000 |          2          |  1  |          2          |\n" +
                "  32   |2010-08-09 | NULL  |          3          |  1  |          3          |\n" +
                "  50   |2011-05-24 | 52000 |          4          |  1  |          4          |\n" +
                "  55   |2011-10-15 | 52000 |          5          |  1  |          5          |\n" +
                "  70   |2012-04-03 | 76000 |          6          |  1  |          6          |\n" +
                "  20   |2012-11-11 | 75000 |          7          |  1  |          7          |\n" +
                "  60   |2014-03-04 | 78000 |          8          |  1  |          8          |\n" +
                "  90   |2012-04-03 | 51000 |          1          |  2  |          1          |\n" +
                "  49   |2012-04-03 | 53000 |          2          |  2  |          2          |\n" +
                "  40   |2013-06-06 | 52000 |          3          |  2  |          3          |\n" +
                "  44   |2013-12-20 | 52000 |          4          |  2  |          4          |\n" +
                "  100  |2010-04-12 | 55000 |          1          |  3  |          1          |\n" +
                "  33   |2010-08-09 | NULL  |          2          |  3  |          2          |\n" +
                "  30   |2010-08-09 | 84000 |          3          |  3  |          3          |\n" +
                "  120  |2012-04-03 | 75000 |          4          |  3  |          4          |\n" +
                "  80   |2013-04-24 | 79000 |          5          |  3  |          5          |";
        assertEquals("\n"+sqlText+"\n", expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
        rs.close();
    }

    @Test
    public void testSelectDateMultiFunction() throws Exception {
        // DB-1989 - Attempted to encode a value that does not have a scalar type format id
        String sqlText =
            String.format("SELECT hiredate, dept, " +
                              "DENSE_RANK() OVER (PARTITION BY dept ORDER BY hiredate, salary) AS DenseRank_HireDate_Salary_By_Dept, " +
                              "ROW_NUMBER() OVER (PARTITION BY dept ORDER BY hiredate, dept) AS RowNumber_HireDate_Salary_By_Dept " +
                              "FROM %s order by hiredate, RowNumber_HireDate_Salary_By_Dept",
                          this.getTableReference(EMPTAB_HIRE_DATE));
//        System.out.println(sqlText);
        ResultSet rs = methodWatcher.executeQuery(sqlText);
        String expected =
            "HIREDATE  |DEPT | DENSERANK_HIREDATE_SALARY_BY_DEPT | ROWNUMBER_HIREDATE_SALARY_BY_DEPT |\n" +
                "------------------------------------------------------------------------------------------\n" +
                "2010-03-20 |  1  |                 1                 |                 1                 |\n" +
                "2010-03-20 |  1  |                 2                 |                 2                 |\n" +
                "2010-04-12 |  3  |                 1                 |                 1                 |\n" +
                "2010-08-09 |  3  |                 2                 |                 2                 |\n" +
                "2011-05-24 |  1  |                 3                 |                 3                 |\n" +
                "2011-10-15 |  1  |                 4                 |                 4                 |\n" +
                "2012-04-03 |  2  |                 1                 |                 1                 |\n" +
                "2012-04-03 |  2  |                 2                 |                 2                 |\n" +
                "2012-04-03 |  3  |                 3                 |                 3                 |\n" +
                "2012-04-03 |  1  |                 5                 |                 5                 |\n" +
                "2012-11-11 |  1  |                 6                 |                 6                 |\n" +
                "2013-04-24 |  3  |                 4                 |                 4                 |\n" +
                "2013-06-06 |  2  |                 3                 |                 3                 |\n" +
                "2013-12-20 |  2  |                 4                 |                 4                 |\n" +
                "2014-03-04 |  1  |                 7                 |                 7                 |";
        assertEquals("\n"+sqlText+"\n", expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
        rs.close();
    }

    @Test
    public void testMultiFunctionSamePartitionDifferentOrderBy_WO_hiredate() throws Exception {
        String sqlText = String.format("SELECT empnum, salary, " +
                                           "DENSE_RANK() OVER (PARTITION BY dept ORDER BY salary) AS DenseRank, " +
                                           "dept, " +
                                           "ROW_NUMBER() OVER (PARTITION BY dept ORDER BY dept, empnum) AS RowNumber FROM %s order by empnum",
                                       this.getTableReference(EMPTAB_HIRE_DATE));

        ResultSet rs = methodWatcher.executeQuery(sqlText);
        String expected =
            "EMPNUM |SALARY | DENSERANK |DEPT | ROWNUMBER |\n" +
                "----------------------------------------------\n" +
                "  10   | 50000 |     1     |  1  |     1     |\n" +
                "  20   | 75000 |     4     |  1  |     2     |\n" +
                "  30   | 84000 |     4     |  3  |     1     |\n" +
                "  40   | 52000 |     2     |  2  |     1     |\n" +
                "  44   | 52000 |     2     |  2  |     2     |\n" +
                "  49   | 53000 |     3     |  2  |     3     |\n" +
                "  50   | 52000 |     2     |  1  |     3     |\n" +
                "  55   | 52000 |     2     |  1  |     4     |\n" +
                "  60   | 78000 |     6     |  1  |     5     |\n" +
                "  70   | 76000 |     5     |  1  |     6     |\n" +
                "  80   | 79000 |     3     |  3  |     2     |\n" +
                "  90   | 51000 |     1     |  2  |     4     |\n" +
                "  100  | 55000 |     1     |  3  |     3     |\n" +
                "  110  | 53000 |     3     |  1  |     7     |\n" +
                "  120  | 75000 |     2     |  3  |     4     |";
        assertEquals("\n"+sqlText+"\n", expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
        rs.close();
    }

    @Test
    public void testNullsMultiFunctionSamePartitionDifferentOrderBy_WO_hiredate() throws Exception {
        // Note that, because nulls are sorted last by default in PostgreSQL and
        // we sort nulls first by default, the comparison of ranking function output
        // cannot be compared with PostgreSQL's. Verification of this output is manual.
        String sqlText =
            String.format("SELECT empnum, salary, " +
                              "DENSE_RANK() OVER (PARTITION BY dept ORDER BY salary, empnum) AS DenseRank, " +
                              "dept, " +
                              "ROW_NUMBER() OVER (PARTITION BY dept ORDER BY dept, empnum) AS RowNumber FROM %s order by dept, empnum",
                          this.getTableReference(EMPTAB_NULLS));

        ResultSet rs = methodWatcher.executeQuery(sqlText);
        String expected =
            "EMPNUM |SALARY | DENSERANK |DEPT | ROWNUMBER |\n" +
                "----------------------------------------------\n" +
                "  10   | 50000 |     2     |  1  |     1     |\n" +
                "  20   | 75000 |     6     |  1  |     2     |\n" +
                "  32   | NULL  |     1     |  1  |     3     |\n" +
                "  50   | 52000 |     3     |  1  |     4     |\n" +
                "  55   | 52000 |     4     |  1  |     5     |\n" +
                "  60   | 78000 |     8     |  1  |     6     |\n" +
                "  70   | 76000 |     7     |  1  |     7     |\n" +
                "  110  | 53000 |     5     |  1  |     8     |\n" +
                "  40   | 52000 |     2     |  2  |     1     |\n" +
                "  44   | 52000 |     3     |  2  |     2     |\n" +
                "  49   | 53000 |     4     |  2  |     3     |\n" +
                "  90   | 51000 |     1     |  2  |     4     |\n" +
                "  30   | 84000 |     5     |  3  |     1     |\n" +
                "  33   | NULL  |     1     |  3  |     2     |\n" +
                "  80   | 79000 |     4     |  3  |     3     |\n" +
                "  100  | 55000 |     2     |  3  |     4     |\n" +
                "  120  | 75000 |     3     |  3  |     5     |";
        assertEquals("\n"+sqlText+"\n", expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
        rs.close();
    }

    @Test
    public void testMultiFunctionSamePartitionDifferentOrderBy_MissingKeyColumn() throws Exception {
        // DB-1988 Key column missing from select causes invalid output
        String sqlText =
            String.format("SELECT empnum, salary, " +
                              "DENSE_RANK() OVER (PARTITION BY dept ORDER BY salary) AS DenseRank, " +
                              "ROW_NUMBER() OVER (PARTITION BY dept ORDER BY dept, empnum) AS RowNumber " +
                              "FROM %s order by salary, empnum",
                          this.getTableReference(EMPTAB_HIRE_DATE));
//        System.out.println(sqlText);
        ResultSet rs = methodWatcher.executeQuery(sqlText);
        String expected =
            "EMPNUM |SALARY | DENSERANK | ROWNUMBER |\n" +
                "----------------------------------------\n" +
                "  10   | 50000 |     1     |     1     |\n" +
                "  90   | 51000 |     1     |     4     |\n" +
                "  40   | 52000 |     2     |     1     |\n" +
                "  44   | 52000 |     2     |     2     |\n" +
                "  50   | 52000 |     2     |     3     |\n" +
                "  55   | 52000 |     2     |     4     |\n" +
                "  49   | 53000 |     3     |     3     |\n" +
                "  110  | 53000 |     3     |     7     |\n" +
                "  100  | 55000 |     1     |     3     |\n" +
                "  20   | 75000 |     4     |     2     |\n" +
                "  120  | 75000 |     2     |     4     |\n" +
                "  70   | 76000 |     5     |     6     |\n" +
                "  60   | 78000 |     6     |     5     |\n" +
                "  80   | 79000 |     3     |     2     |\n" +
                "  30   | 84000 |     4     |     1     |";
        assertEquals("\n"+sqlText+"\n", expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
        rs.close();
    }

    @Test
    public void testMultiFunctionInQueryDiffAndSameOverClause() throws Exception {
        String sqlText =
            String.format("SELECT salary, dept, " +
                              "ROW_NUMBER() OVER (ORDER BY salary, dept) AS RowNumber, " +
                              "RANK() OVER (ORDER BY salary desc, dept) AS Rank, " +
                              "DENSE_RANK() OVER (ORDER BY salary desc, dept) AS DenseRank " +
                              "FROM %s order by salary desc, dept",
                          this.getTableReference(EMPTAB_HIRE_DATE));

        ResultSet rs = methodWatcher.executeQuery(sqlText);
        String expected =
            "SALARY |DEPT | ROWNUMBER |RANK | DENSERANK |\n" +
                "--------------------------------------------\n" +
                " 84000 |  3  |    15     |  1  |     1     |\n" +
                " 79000 |  3  |    14     |  2  |     2     |\n" +
                " 78000 |  1  |    13     |  3  |     3     |\n" +
                " 76000 |  1  |    12     |  4  |     4     |\n" +
                " 75000 |  1  |    10     |  5  |     5     |\n" +
                " 75000 |  3  |    11     |  6  |     6     |\n" +
                " 55000 |  3  |     9     |  7  |     7     |\n" +
                " 53000 |  1  |     7     |  8  |     8     |\n" +
                " 53000 |  2  |     8     |  9  |     9     |\n" +
                " 52000 |  1  |     3     | 10  |    10     |\n" +
                " 52000 |  1  |     4     | 10  |    10     |\n" +
                " 52000 |  2  |     5     | 12  |    11     |\n" +
                " 52000 |  2  |     6     | 12  |    11     |\n" +
                " 51000 |  2  |     2     | 14  |    12     |\n" +
                " 50000 |  1  |     1     | 15  |    13     |";
        assertEquals("\n"+sqlText+"\n", expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
        rs.close();
    }

    @Test
    public void testNullsMultiFunctionInQueryDiffAndSameOverClause() throws Exception {
        // Note that, because nulls are sorted last by default in PostgreSQL and
        // we sort nulls first by default, the comparison of ranking function output
        // cannot be compared with PostgreSQL's. Verification of this output is manual.
        String sqlText =
            String.format("SELECT salary, dept, " +
                              "ROW_NUMBER() OVER (ORDER BY salary, dept) AS RowNumber, " +
                              "RANK() OVER (ORDER BY salary desc, dept) AS Rank, " +
                              "DENSE_RANK() OVER (ORDER BY salary desc, dept) AS DenseRank " +
                              "FROM %s order by salary desc, dept",
                          this.getTableReference(EMPTAB_NULLS));

        ResultSet rs = methodWatcher.executeQuery(sqlText);
        String expected =
            "SALARY |DEPT | ROWNUMBER |RANK | DENSERANK |\n" +
                "--------------------------------------------\n" +
                " NULL  |  1  |     1     |  1  |     1     |\n" +
                " NULL  |  3  |     2     |  2  |     2     |\n" +
                " 84000 |  3  |    17     |  3  |     3     |\n" +
                " 79000 |  3  |    16     |  4  |     4     |\n" +
                " 78000 |  1  |    15     |  5  |     5     |\n" +
                " 76000 |  1  |    14     |  6  |     6     |\n" +
                " 75000 |  1  |    12     |  7  |     7     |\n" +
                " 75000 |  3  |    13     |  8  |     8     |\n" +
                " 55000 |  3  |    11     |  9  |     9     |\n" +
                " 53000 |  1  |     9     | 10  |    10     |\n" +
                " 53000 |  2  |    10     | 11  |    11     |\n" +
                " 52000 |  1  |     5     | 12  |    12     |\n" +
                " 52000 |  1  |     6     | 12  |    12     |\n" +
                " 52000 |  2  |     7     | 14  |    13     |\n" +
                " 52000 |  2  |     8     | 14  |    13     |\n" +
                " 51000 |  2  |     4     | 16  |    14     |\n" +
                " 50000 |  1  |     3     | 17  |    15     |";
        assertEquals("\n"+sqlText+"\n", expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
        rs.close();
    }

    @Test @Ignore("Broken as a result of result buffering for lead() and lag(). Looks like a legit error validated by PostgreSQL app.")
    public void testPullFunctionInputColumnUp4Levels() throws Exception {
        // DB-2087 - Kryo exception
        String sqlText =
            String.format("select Transaction_Detail5.SOURCE_SALES_INSTANCE_ID C0, " +
                              "min(Transaction_Detail5.TRANSACTION_DT) over (partition by Transaction_Detail5.ORIGINAL_SKU_CATEGORY_ID) C1, " +
                              "sum(Transaction_Detail5.SALES_AMT) over (partition by Transaction_Detail5.TRANSACTION_DT) C10 " +
                              "from %s AS Transaction_Detail5 " +
                              "where Transaction_Detail5.TRANSACTION_DT between DATE('2010-01-21') " +
                              "and DATE('2013-11-21') and Transaction_Detail5.CUSTOMER_MASTER_ID=74065939",
                          this.getTableReference(TXN_DETAIL));

        ResultSet rs = methodWatcher.executeQuery(sqlText);
        String expected =
            "C0 |    C1     |  C10   |\n" +
                "-------------------------\n" +
                " 0 |2013-05-12 |4086.67 |\n" +
                " 0 |2013-05-12 |4086.67 |\n" +
                " 0 |2013-05-12 |4086.67 |\n" +
                " 0 |2013-05-12 |4086.67 |\n" +
                " 0 |2013-05-12 |4086.67 |\n" +
                " 0 |2013-05-12 |4086.67 |\n" +
                " 0 |2013-05-12 |4086.67 |\n" +
                " 0 |2013-05-12 |4086.67 |";
        assertEquals("\n"+sqlText+"\n", expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
        rs.close();
    }

    @Test
    public void testWindowDefnEquivalent() throws Exception {
        // DB-2165 - NPE, operator null in OverClause.isEquivalent(OrderByCol, OrderByCol)
        String sqlText =
            String.format("SELECT\n" +
                              "prd_type_id, SUM(amount),\n" +
                              "RANK() OVER (ORDER BY SUM(amount) DESC) AS rank,\n" +
                              "DENSE_RANK() OVER (ORDER BY SUM(amount) DESC) AS dense_rank\n" +
                              "FROM %s\n" +
                              "WHERE amount IS NOT NULL\n" +
                              "GROUP BY prd_type_id\n" +
                              "ORDER BY prd_type_id", this.getTableReference(ALL_SALES));

        ResultSet rs = methodWatcher.executeQuery(sqlText);
        String expected =
            "PRD_TYPE_ID |    2     |RANK |DENSE_RANK |\n" +
                "-------------------------------------------\n" +
                "      1      |227276.50 |  1  |     1     |\n" +
                "      2      |223927.08 |  2  |     2     |";
        assertEquals("\n"+sqlText+"\n", expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
        rs.close();
    }

}
