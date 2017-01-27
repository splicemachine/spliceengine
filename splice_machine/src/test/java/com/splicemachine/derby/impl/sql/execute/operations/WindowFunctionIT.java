/*
 * Copyright (c) 2012 - 2017 Splice Machine, Inc.
 *
 * This file is part of Splice Machine.
 * Splice Machine is free software: you can redistribute it and/or modify it under the terms of the
 * GNU Affero General Public License as published by the Free Software Foundation, either
 * version 3, or (at your option) any later version.
 * Splice Machine is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU Affero General Public License for more details.
 * You should have received a copy of the GNU Affero General Public License along with Splice Machine.
 * If not, see <http://www.gnu.org/licenses/>.
 */

package com.splicemachine.derby.impl.sql.execute.operations;

import static com.splicemachine.test_tools.Rows.row;
import static com.splicemachine.test_tools.Rows.rows;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collection;

import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;
import org.junit.runner.Description;

import com.splicemachine.derby.test.framework.SpliceDataWatcher;
import com.splicemachine.derby.test.framework.SpliceSchemaWatcher;
import com.splicemachine.derby.test.framework.SpliceTableWatcher;
import com.splicemachine.derby.test.framework.SpliceUnitTest;
import com.splicemachine.derby.test.framework.SpliceViewWatcher;
import com.splicemachine.derby.test.framework.SpliceWatcher;
import com.splicemachine.homeless.TestUtils;
import com.splicemachine.test_dao.TableDAO;
import com.splicemachine.test_tools.TableCreator;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.spark_project.guava.collect.Lists;

/**
 *
 * Created by jyuan on 7/30/14.
 */
@SuppressWarnings("unchecked")
@RunWith(Parameterized.class)
public class WindowFunctionIT extends SpliceUnitTest {
    private Boolean useSpark;

    @Parameterized.Parameters
    public static Collection<Object[]> data() {
        Collection<Object[]> params = Lists.newArrayListWithCapacity(4);
        params.add(new Object[]{true});
        params.add(new Object[]{false});
        return params;
    }
    public WindowFunctionIT(Boolean useSpark) {
        this.useSpark = useSpark;
    }


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

    private static String perchacedDef = "(item int, price decimal(4,2), date timestamp)";
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

    private static String nestedAggregateWindowFunction = "(a INTEGER, b INTEGER, c INTEGER)";
    private static final String NESTED_AGGREGATION_WF = "NESTED_AGGREGATION_WF";
    private static SpliceTableWatcher nestedAggregateWindowFunctionWatcher = new SpliceTableWatcher(NESTED_AGGREGATION_WF, SCHEMA, nestedAggregateWindowFunction);

    private static String[] NESTED_AGGREGATION_WF_ROWS = {
        "1,2,3",
        "1,2,3",
        "10,3,30",
        "10,3,30",
        "5,4,30",
        "5,4,30",
        "5,6,30",
        "5,6,30",
        "5,7,30",
        "5,7,30"

    };


    private static String nestedAggregateEmptyFunction = "(a INTEGER, b INTEGER, c INTEGER)";
    private static final String EMPTY_TABLE = "NESTED_AGGREGATION_EMPTY_TABLE";
    private static SpliceTableWatcher nestedAggregateEmptyWatcher = new SpliceTableWatcher(EMPTY_TABLE, SCHEMA, nestedAggregateEmptyFunction);


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

    private static String PRODUCT_RATING = "PRODUCT_RATING";
    private static String PRODUCT_RATING_REF = SCHEMA+"."+ PRODUCT_RATING;
    private static String PRODUCT_RATING_DEF = "(productId decimal(10,0), ratingAgencyCode varchar(20), ratingEffDate timestamp, rating varchar(20))";
    private static Iterable<Iterable<Object>> PRODUCT_RATING_ROWS = rows(
                    row(3300d, "Better", "2014-03-07 19:37:30.604", "WDQTAXMXLMTWXQL"),
                    row(4040d, "Best", "2014-08-21 15:48:24.096", "JSKWDFWMTQCVJTS"),
                    row(3300d, "Better", "2014-05-22 10:42:47.834", "QOJYERFYJAYGNIC"),
                    row(4440d, "Dont think about it", "2014-03-11 10:19:44.256", "NIILBXPPMRRKVSG"),
                    row(0001d, "Good", "2014-11-02 03:24:36.044", "TILAXMXGUEYZSEK"),
                    row(3300d, "Better", "2015-01-13 11:01:19.065", "SPIHABCGNPNOOAC"),
                    row(0002d, "Dont think about it", "2014-02-14 04:59:08.36", "JVEZECWKNBLAALU"),
                    row(3300d, "Dont think about it", "2015-08-13 21:03:44.191", "MRUQGZIKYBXHZPQ"),
                    row(4440d, "Dont think about it", "2014-08-05 09:55:23.669", "GLUUGLMTGJRUHSA"),
                    row(5500d, "Good", "2014-07-31 20:33:36.713", "OLSEYNIBUDVHEIF"),
                    row(0001d, "Good", "2014-12-14 05:52:22.326", "UMTHLNRYDFAYWQS"),
                    row(9001d, "Good", "2015-09-27 17:49:39.5", "XHFHMOPWMOJNGOL"),
                    row(4440d, "Run Away", "2014-11-12 06:40:59.685", "VVEEPGAEVWDLBPF"),
                    row(3300d, "Dont think about it", "2014-11-03 05:15:49.851", "TMXIAVAKXCDJYKY"),
                    row(0800d, "Run Away", "2015-01-09 13:29:29.642", "KMCNRDOWZFAEVFS"),
                    row(9001d, "Best", "2014-11-01 21:51:02.199", "JFVQBHBCTVPVZTR"),
                    row(0001d, "Good", "2015-12-04 16:40:43.67", "BLYUEGSMCFCPXJA"),
                    row(6999d, "Dont think about it", "2014-07-30 10:23:12.643", "MRVUOQXPZMVTVXW"),
                    row(3300d, "Poor", "2015-09-02 05:09:32.683", "FFXXMDXCAVYDJTC"),
                    row(9001d, "Dont think about it", "2015-06-11 21:00:40.206", "ILWSWUTLTSCJRSV"),
                    row(0002d, "Run Away", "2014-05-16 22:44:34.056", "ZSFHGWWFCQZZNMO"),
                    row(4440d, "Dont think about it", "2015-01-06 21:26:12.528", "KVPKDRBXWBGATQY"),
                    row(6999d, "Poor", "2015-04-30 05:52:43.627", "LFHHSWUBDDIPKPA"),
                    row(0002d, "Good", "2015-01-22 11:41:01.329", "ZRHSIVRCQUUQZLC"),
                    row(4040d, "Run Away", "2014-06-22 08:12:53.937", "PHAMOZYXPQXWJDF"),
                    row(0001d, "Good", "2015-07-22 04:39:19.453", "XQSJCCCGLIOMMCJ"),
                    row(6999d, "Dont think about it", "2014-09-21 01:20:16.26", "MLBLXWCFKHVWXVU"),
                    row(0800d, "Run Away", "2014-01-04 21:08:27.058", "FWXYVWSYDWYXXBZ"),
                    row(4440d, "Run Away", "2014-04-11 06:29:30.187", "WYREDMPIIZPGXZA"),
                    row(4040d, "Dont think about it", "2015-06-10 12:59:47.54", "LTCOVEBAVEHNCRP"),
                    row(4040d, "Poor", "2014-07-29 18:45:46.257", "EMXGEGPTWXUECLS"),
                    row(0700d, "Poor", "2014-02-07 12:19:36.492", "YPXRMRFFYYPWBUZ"),
                    row(4440d, "Good", "2014-07-27 14:25:56.885", "APGRMQQJCJSTCDB"),
                    row(0001d, "Dont think about it", "2014-02-09 08:12:02.596", "PXVDSMZWANQWGCX"),
                    row(6999d, "Dont think about it", "2015-05-23 16:04:52.561", "HCZIXOTTRASHITI"),
                    row(5500d, "Best", "2015-01-23 18:15:43.723", "JRLYTUTWIZNOAPT"),
                    row(0700d, "Dont think about it", "2014-07-19 08:25:52.238", "UYLCQPSCHURELLY"),
                    row(0001d, "Dont think about it", "2015-01-25 19:47:53.004", "NWAJOBJVSGIAIZU"),
                    row(0700d, "Poor", "2014-08-21 21:27:35.482", "ZKBLWNEPGNMQUTL"),
                    row(4040d, "Run Away", "2015-05-09 01:21:16.513", "YNWDVEUSONYHZXE"),
                    row(9001d, "Poor", "2014-05-22 22:20:20.929", "HZLYPGHDEQORMOH"),
                    row(0800d, "Better", "2015-07-30 11:36:16.128", "UUAYIPTXDNCCBTU"),
                    row(0700d, "Better", "2014-01-27 01:22:55.581", "CHZGQMKFKUXIEAY"),
                    row(0800d, "Run Away", "2015-06-23 20:21:06.677", "DBAEKJQCJBGMOYQ"),
                    row(0800d, "Better", "2015-11-25 00:16:42.154", "ZCQJQILUQRCHWCY"),
                    row(0001d, "Best", "2014-08-29 22:30:41.012", "BIMTVQOBOXOMBNI"),
                    row(0800d, "Run Away", "2014-11-04 19:02:56.001", "ZIMKXUOKMCWCGSZ"),
                    row(6999d, "Dont think about it", "2015-08-03 18:40:21.753", "QZMNXNYRPEKJWBY"),
                    row(0002d, "Good", "2015-10-06 03:44:19.184", "NZRXIDYRZNUVXDU"),
                    row(0002d, "Run Away", "2015-06-30 07:06:13.865", "IAUEYFHYZBFMJFW")
            );

    private static String EMP3 = "EMP3";
    private static String EMP3_REF = SCHEMA+"."+EMP3;
    private static String EMP3_DEF = "(EMPNO int, EMPNAME varchar(20), SALARY int, DEPTNO int)";
    private static Iterable<Iterable<Object>>  EMP3_ROWS = rows(
            row(10, "Bill", 12000, 5), row(11, "Solomon", 10000, 5), row(12, "Susan", 10000, 5),
            row(13, "Wendy", 9000, 1), row(14, "Benjamin", 7500, 1), row(15, "Tom", 7600, 1),
            row(16, "Henry", 8500, 2), row(17, "Robert", 9500, 2), row(18, "Paul", 7700, 2),
            row(19, "Dora", 8500, 3), row(20, "Samuel", 6900, 3), row(21, "Mary", 7500, 3),
            row(22, "Daniel", 6500, 4), row(23, "Ricardo", 7800, 4), row(24, "Mark", 7200, 4));

    private static String ALL_SALES_2 = "ALL_SALES_2";
    private static String ALL_SALES_2_REF = SCHEMA+"."+ALL_SALES_2;
    private static String ALL_SALES_2_DEF = "(yr INTEGER, month INTEGER, prd_type_id INTEGER, emp_id INTEGER , amount decimal(8, 2))";
    private static Iterable<Iterable<Object>>  ALL_SALES_2_ROWS = rows(
            row(2006, 1, 1, 21, 16034.84), row(2006, 2, 1, 21, 15644.65), row(2006, 3, 2, 21, 20167.83),
            row(2006, 4, 2, 21, 25056.45), row(2006, 5, 2, 21, null), row(2006, 6, 1, 21, 15564.66),
            row(2006, 7, 1, 21, 15644.65), row(2006, 8, 1, 21, 16434.82), row(2006, 9, 1, 21, 19654.57),
            row(2006, 10, 1, 21, 21764.19), row(2006, 11, 1, 21, 13026.73), row(2006, 12, 2, 21, 10034.64),
            row(2005, 1, 2, 22, 16634.84), row(2005, 1, 2, 21, 26034.84), row(2005, 2, 1, 21, 12644.65),
            row(2005, 3, 1, 21, null), row(2005, 4, 1, 21, 25026.45), row(2005, 5, 1, 21, 17212.66),
            row(2005, 6, 1, 21, 15564.26), row(2005, 7, 2, 21, 62654.82), row(2005, 8, 2, 21, 26434.82),
            row(2005, 9, 2, 21, 15644.65), row(2005, 10, 2, 21, 21264.19), row(2005, 11, 1, 21, 13026.73),
            row(2005, 12, 1, 21, 10032.64));

    private static String ALL_SALES_3 = "ALL_SALES_3";
    private static String ALL_SALES_3_REF = SCHEMA+"."+ALL_SALES_3;
    private static String ALL_SALES_3_DEF = "(yr INTEGER, month INTEGER, prd_type_id INTEGER, emp_id INTEGER , amount decimal(8, 2))";
    private static Iterable<Iterable<Object>> ALL_SALES_3_ROWS = rows(
            row(2006, 1, 1, 21, 16034.84), row(2006, 2, 1, 21, 15644.65), row(2006, 3, 2, 21, 20167.83),
            row(2006, 4, 2, 21, 25056.45), row(2006, 5, 2, 21, null), row(2006, 6, 1, 21, 15564.66),
            row(2006, 7, 1, 21, 15644.65), row(2006, 8, 1, 21, 16434.82), row(2006, 9, 1, 21, 19654.57),
            row(2006, 10, 1, 21, 21764.19), row(2006, 11, 1, 21, 13026.73), row(2006, 12, 2, 21, 10034.64),
            row(2005, 1, 2, 22, 16634.84), row(2005, 1, 2, 21, 26034.84), row(2005, 2, 1, 21, 12644.65),
            row(2005, 3, 1, 21, null), row(2005, 4, 1, 21, 25026.45), row(2005, 5, 1, 21, 17212.66),
            row(2005, 6, 1, 21, 15564.26), row(2005, 7, 2, 21, 62654.82), row(2005, 8, 2, 21, 26434.82),
            row(2005, 9, 2, 21, 15644.65), row(2005, 10, 2, 21, 21264.19), row(2005, 11, 1, 21, 13026.73),
            row(2005, 12, 1, 21, 10032.64));

    private static String EMP_LEAD_1 = "EMP_LEAD_1";
    private static String EMP_LEAD_1_REF = SCHEMA+"."+EMP_LEAD_1;
    private static String EMP_LEAD_1_DEF = "(EMPNO int, EMPNAME varchar(20), SALARY int, DEPTNO int)";
    private static Iterable<Iterable<Object>> EMP_LEAD_1_ROWS = rows(
            row(10, "Bill", 12000, 5), row(11, "Solomon", 10000, 5), row(12, "Susan", 10000, 5),
            row(13, "Wendy", 9000, 1), row(14, "Benjamin", 7500, 1), row(15, "Tom", 7600, 1),
            row(16, "Henry", 8500, 2), row(17, "Robert", 9500, 2), row(18, "Paul", 7700, 2),
            row(19, "Dora", 8500, 3), row(20, "Samuel", 6900, 3), row(21, "Mary", 7500, 3),
            row(22, "Daniel", 6500, 4), row(23, "Ricardo", 7800, 4), row(24, "Mark", 7200, 4));

    private static String EMP_2 = "EMP2";
    private static String EMP_2_REF = SCHEMA + "." + EMP_2;
    private static String EMP_2_DEF = "(EMPNO int, EMPNAME varchar(20), SALARY int, DEPTNO int)";
    private static Iterable<Iterable<Object>> EMP_2_ROWS = rows(
            row(10, "Bill", 12000, 5), row(11, "Solomon", 10000, 5), row(12, "Susan", 10000, 5),
            row(13, "Wendy", 9000, 1), row(14, "Benjamin", 7500, 1), row(15, "Tom", 7600, 1),
            row(16, "Henry", 8500, 2), row(17, "Robert", 9500, 2), row(18, "Paul", 7700, 2),
            row(19, "Dora", 8500, 3), row(20, "Samuel", 6900, 3), row(21, "Mary", 7500, 3),
            row(22, "Daniel", 6500, 4), row(23, "Ricardo", 7800, 4), row(24, "Mark", 7200, 4));


    private static String EMP_4 = "EMP4";
    private static String EMP_4_REF = SCHEMA + "." + EMP_4;
    private static String EMP_4_DEF = "(EMPNO int, EMPNAME varchar(20), SALARY int, DEPTNO int)";
    private static Iterable<Iterable<Object>> EMP_4_ROWS = rows(
            row(10, "Bill", 12000, 5), row(11, "Solomon", 10000, 5), row(12, "Susan", 10000, 5),
            row(13, "Wendy", 9000, 1), row(14, "Benjamin", 7500, 1), row(15, "Tom", 7600, 1),
            row(16, "Henry", 8500, 2), row(17, "Robert", 9500, 2), row(18, "Paul", 7700, 2),
            row(19, "Dora", 8500, 3), row(20, "Samuel", 6900, 3), row(21, "Mary", 7500, 3),
            row(22, "Daniel", 6500, 4), row(23, "Ricardo", 7800, 4), row(24, "Mark", 7200, 4));



    private static String EMPLOYEES = "EMPLOYEES";
    private static String EMPLOYEES_REF =  SCHEMA+"."+ EMPLOYEES;
    private static String EMPLOYEES_DEF =  "(employee_id int, employee_name varchar(10), salary int, department varchar(10), commission int)";
    private static Iterable<Iterable<Object>> EMPLOYEES_ROWS = rows(
            row(101, "Emp A", 10000, "Sales", null), row(102, "Emp B", 20000, "IT", 20), row(103, "Emp C", 28000, "IT", 20),
            row(104, "Emp D", 30000, "Support", 5), row(105, "Emp E", 32000, "Sales", 10), row(106, "Emp F", 20000, "Sales", 5),
            row(107, "Emp G", 12000, "Sales", null), row(108, "Emp H", 12000, "Support", null));

    private static String EMP5 = "EMP5";
    private static String EMP5_REF = SCHEMA+"."+EMP5;
    private static String EMP5_DEF = "(EMPNO int, EMPNAME varchar(20), SALARY int, DEPTNO int)";
    private static  Iterable<Iterable<Object>> EMP5_ROWS = rows(
            row(10, "Bill", 12000, 5), row(11, "Solomon", 10000, 5), row(12, "Susan", 10000, 5),
            row(13, "Wendy", 9000, 1), row(14, "Benjamin", 7500, 1), row(15, "Tom", 7600, 1),
            row(16, "Henry", 8500, 2), row(17, "Robert", 9500, 2), row(18, "Paul", 7700, 2),
            row(19, "Dora", 8500, 3), row(20, "Samuel", 6900, 3), row(21, "Mary", 7500, 3),
            row(22, "Daniel", 6500, 4), row(23, "Ricardo", 7800, 4), row(24, "Mark", 7200, 4));


    private static String TWOINTS = "TWOINTS";
    private static String TWOINTS_REF = SCHEMA + "." + TWOINTS;
    private static String TWOINTS_DEF = "(a int, b int)";
    private static  Iterable<Iterable<Object>> TWOINTS_ROWS =   rows(
            row(1, null), row(1,15), row(1, 10), row(1, null), row(2, 25), row(2, 10)
    );

    private static String KDW_PAT_SVC_STAGE = "KDW_PAT_SVC_STAGE";
    private static String KDW_PAT_SVC_STAGE_REF = SCHEMA + "." + KDW_PAT_SVC_STAGE;
    private static String KDW_PAT_SVC_STAGE_DEF = "(PRSN_KEY VARCHAR(14), DIAGNOSIS_FOR_STAGING VARCHAR(500), STAGE VARCHAR(255), " +
            "EARLIEST_STAGING_DATE TIMESTAMP, MOST_RECENT_STAGING_DATE TIMESTAMP)";
    private static  Iterable<Iterable<Object>> KDW_PAT_SVC_STAGE_ROWS = rows(
            row("gen key", "Big Long Staging Diagnosis", "The Stage", "2012-02-03 08:42:00", "2016-04-28 08:00:00")
    );



    @ClassRule
    public static TestRule chain = RuleChain.outerRule(spliceClassWatcher)
                                            .around(spliceSchemaWatcher)
                                            .around(empTabTableWatcher)
                                            .around(new SpliceDataWatcher() {
                                                @Override
                                                protected void starting(Description description) {
                                                    try {

                                                        new TableDAO(methodWatcher.getOrCreateConnection()).drop(SCHEMA, PRODUCT_RATING);
                                                        new TableCreator(methodWatcher.getOrCreateConnection())
                                                                .withCreate(String.format("create table %s %s", PRODUCT_RATING_REF, PRODUCT_RATING_DEF))
                                                                .withInsert(String.format("insert into %s (productId, ratingAgencyCode, ratingEffDate, rating) values (?,?,?,?)",
                                                                        PRODUCT_RATING_REF))
                                                                .withRows(PRODUCT_RATING_ROWS)
                                                                .create();


                                                    } catch (Exception e) {
                                                        throw new RuntimeException(e);
                                                    }
                                                }})
                                            .around(new SpliceDataWatcher() {
                                                @Override
                                                protected void starting(Description description) {
                                                    try {
                                                    new TableDAO(methodWatcher.getOrCreateConnection()).drop(SCHEMA, EMP3);
                                                    new TableCreator(methodWatcher.getOrCreateConnection())
                                                            .withCreate(String.format("create table %s %s", EMP3_REF, EMP3_DEF))
                                                            .withInsert(String.format("insert into %s (EMPNO, EMPNAME, SALARY, DEPTNO) values (?,?,?,?)", EMP3_REF))
                                                            .withRows(EMP3_ROWS)
                                                            .create();


                                                    } catch (Exception e) {
                                                        throw new RuntimeException(e);
                                                    }
                                                }})
                                            .around(new SpliceDataWatcher() {
                                                @Override
                                                protected void starting(Description description) {
                                                    try {
                                                        new TableDAO(methodWatcher.getOrCreateConnection()).drop(SCHEMA, ALL_SALES_2);
                                                        new TableCreator(methodWatcher.getOrCreateConnection())
                                                                .withCreate(String.format("create table %s %s", ALL_SALES_2_REF, ALL_SALES_2_DEF))
                                                                .withInsert(String.format("insert into %s (yr,MONTH,PRD_TYPE_ID,EMP_ID,AMOUNT) values (?,?,?,?,?)", ALL_SALES_2_REF))
                                                                .withRows(ALL_SALES_2_ROWS)
                                                                .create();


                                                    } catch (Exception e) {
                                                        throw new RuntimeException(e);
                                                    }
                                                }})
                                        .around(new SpliceDataWatcher() {
                                            @Override
                                            protected void starting(Description description) {
                                                try {
                                                    new TableDAO(methodWatcher.getOrCreateConnection()).drop(SCHEMA, ALL_SALES_3);
                                                    new TableCreator(methodWatcher.getOrCreateConnection())
                                                            .withCreate(String.format("create table %s %s", ALL_SALES_3_REF, ALL_SALES_3_DEF))
                                                            .withInsert(String.format("insert into %s (yr,MONTH,PRD_TYPE_ID,EMP_ID,AMOUNT) values (?,?,?,?,?)",  ALL_SALES_3_REF))
                                                            .withRows(ALL_SALES_3_ROWS)
                                                            .create();


                                                } catch (Exception e) {
                                                    throw new RuntimeException(e);
                                                }
                                            }})
                                        .around(new SpliceDataWatcher() {
                                            @Override
                                            protected void starting(Description description) {
                                                try {

                                                    new TableDAO(methodWatcher.getOrCreateConnection()).drop(SCHEMA, EMP_LEAD_1);
                                                    new TableCreator(methodWatcher.getOrCreateConnection())
                                                            .withCreate(String.format("create table %s %s", EMP_LEAD_1_REF, EMP_LEAD_1_DEF))
                                                            .withInsert(String.format("insert into %s (EMPNO, EMPNAME, SALARY, DEPTNO) values (?,?,?,?)", EMP_LEAD_1_REF))
                                                            .withRows(EMP_LEAD_1_ROWS)
                                                            .create();


                                                } catch (Exception e) {
                                                    throw new RuntimeException(e);
                                                }
                                            }})
                                    .around(new SpliceDataWatcher() {
                                        @Override
                                        protected void starting(Description description) {
                                            try {
                                                new TableDAO(methodWatcher.getOrCreateConnection()).drop(SCHEMA, EMP_2);
                                                new TableCreator(methodWatcher.getOrCreateConnection())
                                                        .withCreate(String.format("create table %s %s", EMP_2_REF, EMP_2_DEF))
                                                        .withInsert(String.format("insert into %s (EMPNO, EMPNAME, SALARY, DEPTNO) values (?,?,?,?)", EMP_2_REF))
                                                        .withRows(EMP_2_ROWS)
                                                        .create();


                                            } catch (Exception e) {
                                                throw new RuntimeException(e);
                                            }
                                        }})
                                        .around(new SpliceDataWatcher() {
                                            @Override
                                            protected void starting(Description description) {
                                                try {
                                                    new TableDAO(methodWatcher.getOrCreateConnection()).drop(SCHEMA, EMP_4);
                                                    new TableCreator(methodWatcher.getOrCreateConnection())
                                                            .withCreate(String.format("create table %s %s", EMP_4_REF, EMP_4_DEF))
                                                            .withInsert(String.format("insert into %s (EMPNO, EMPNAME, SALARY, DEPTNO) values (?,?,?,?)", EMP_4_REF))
                                                            .withRows(EMP_4_ROWS)
                                                            .create();


                                                } catch (Exception e) {
                                                    throw new RuntimeException(e);
                                                }
                                            }})
                                    .around(new SpliceDataWatcher() {
                                        @Override
                                        protected void starting(Description description) {
                                            try {
                                                new TableDAO(methodWatcher.getOrCreateConnection()).drop(SCHEMA, EMPLOYEES);
                                                new TableCreator(methodWatcher.getOrCreateConnection())
                                                        .withCreate(String.format("create table %s %s", EMPLOYEES_REF, EMPLOYEES_DEF))
                                                        .withInsert(String.format("insert into %s (employee_id, employee_name, salary, department, commission) values (?,?,?,?,?)", EMPLOYEES_REF))
                                                        .withRows(EMPLOYEES_ROWS)
                                                        .create();


                                            } catch (Exception e) {
                                                throw new RuntimeException(e);
                                            }
                                        }})
                                        .around(new SpliceDataWatcher() {
                                            @Override
                                            protected void starting(Description description) {
                                                try {
                                                    new TableDAO(methodWatcher.getOrCreateConnection()).drop(SCHEMA, EMP5);
                                                    new TableCreator(methodWatcher.getOrCreateConnection())
                                                            .withCreate(String.format("create table %s %s", EMP5_REF, EMP5_DEF))
                                                            .withInsert(String.format("insert into %s (EMPNO, EMPNAME, SALARY, DEPTNO) values (?,?,?,?)", EMP5_REF))
                                                            .withRows(EMP5_ROWS)
                                                            .create();


                                                } catch (Exception e) {
                                                    throw new RuntimeException(e);
                                                }
                                            }})
                                        .around(new SpliceDataWatcher() {
                                            @Override
                                            protected void starting(Description description) {
                                                try {
                                                    new TableDAO(methodWatcher.getOrCreateConnection()).drop(SCHEMA, EMPLOYEES);
                                                    new TableCreator(methodWatcher.getOrCreateConnection())
                                                            .withCreate(String.format("create table %s %s", EMPLOYEES_REF, EMPLOYEES_DEF))
                                                            .withInsert(String.format("insert into %s values (?,?,?,?,?)", EMPLOYEES_REF))
                                                            .withRows(EMPLOYEES_ROWS)
                                                            .create();


                                                } catch (Exception e) {
                                                    throw new RuntimeException(e);
                                                }
                                            }})
                                        .around(new SpliceDataWatcher() {
                                            @Override
                                            protected void starting(Description description) {
                                                try {
                                                    new TableDAO(methodWatcher.getOrCreateConnection()).drop(SCHEMA, TWOINTS);
                                                    new TableCreator(methodWatcher.getOrCreateConnection())
                                                            .withCreate(String.format("create table %s %s", TWOINTS_REF, TWOINTS_DEF))
                                                            .withInsert(String.format("insert into %s (a, b) values (?,?)", TWOINTS_REF))
                                                            .withRows(TWOINTS_ROWS)
                                                            .create();


                                                } catch (Exception e) {
                                                    throw new RuntimeException(e);
                                                }
                                            }})
                                            .around(new SpliceDataWatcher() {
                                                @Override
                                                protected void starting(Description description) {
                                                    try {
                                                        new TableDAO(methodWatcher.getOrCreateConnection()).drop(SCHEMA, TWOINTS);
                                                        new TableCreator(methodWatcher.getOrCreateConnection())
                                                                .withCreate(String.format("create table %s %s", TWOINTS_REF, TWOINTS_DEF))
                                                                .withInsert(String.format("insert into %s (a, b) values (?,?)", TWOINTS_REF))
                                                                .withRows(TWOINTS_ROWS)
                                                                .create();


                                                    } catch (Exception e) {
                                                        throw new RuntimeException(e);
                                                    }
                                                }})
                                        .around(new SpliceDataWatcher() {
                                            @Override
                                            protected void starting(Description description) {
                                                try {
                                                    new TableDAO(methodWatcher.getOrCreateConnection()).drop(SCHEMA, TWOINTS);
                                                    new TableCreator(methodWatcher.getOrCreateConnection())
                                                            .withCreate(String.format("create table %s %s", TWOINTS_REF, TWOINTS_DEF))
                                                            .withInsert(String.format("insert into %s (a, b) values (?,?)", TWOINTS_REF))
                                                            .withRows(TWOINTS_ROWS)
                                                            .create();


                                                } catch (Exception e) {
                                                    throw new RuntimeException(e);
                                                }
                                            }})
                                            .around(new SpliceDataWatcher() {
                                                @Override
                                                protected void starting(Description description) {
                                                    try {
                                                        new TableDAO(methodWatcher.getOrCreateConnection()).drop(SCHEMA, KDW_PAT_SVC_STAGE);
                                                        new TableCreator(methodWatcher.getOrCreateConnection())
                                                                .withCreate(String.format("create table %s %s", KDW_PAT_SVC_STAGE_REF, KDW_PAT_SVC_STAGE_DEF))
                                                                .withInsert(String.format("insert into %s values (?, ?, ?, ?, ?)", KDW_PAT_SVC_STAGE_REF))
                                                                .withRows(KDW_PAT_SVC_STAGE_ROWS).create();


                                                    } catch (Exception e) {
                                                        throw new RuntimeException(e);
                                                    }
                                                }})
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
                                            .around(new SpliceDataWatcher() {
                                                @Override
                                                protected void starting(Description description) {
                                                    PreparedStatement ps;
                                                    try {
                                                        methodWatcher.setAutoCommit(false);

                                                    } catch (Exception e) {
                                                        throw new RuntimeException(e);
                                                    }
                                                }

                                                @Override
                                                protected void finished(Description description) {
                                                    super.finished(description);
                                                    try {
                                                        methodWatcher.rollback();
                                                        methodWatcher.closeAll();

                                                    } catch (Exception e) {
                                                        throw new RuntimeException(e);
                                                    }
                                                }
                                            })
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
                                            })
                                            .around(nestedAggregateWindowFunctionWatcher)
                                            .around(new SpliceDataWatcher() {
                                                @Override
                                                protected void starting(Description description) {
                                                    PreparedStatement ps;
                                                    try {
                                                        for (String row : NESTED_AGGREGATION_WF_ROWS) {
                                                            ps = spliceClassWatcher.prepareStatement(
                                                                    String.format("insert into %s values (%s)", nestedAggregateWindowFunctionWatcher, row));
                                                            ps.execute();
                                                        }
                                                    } catch (Exception e) {
                                                        throw new RuntimeException(e);
                                                    }
                                                }})
                                            .around(nestedAggregateEmptyWatcher);

    @ClassRule
    public static SpliceWatcher methodWatcher = new SpliceWatcher();


    @Test
    public void testMinRows2Preceding() throws Exception {
        String sqlText =
            String.format("SELECT empnum,dept,salary," +
                              "min(salary) over (Partition by dept ORDER BY salary ROWS 2 preceding) as minsal " +
                              "from %s --SPLICE-PROPERTIES useSpark = %s \n order by empnum",
                          this.getTableReference(EMPTAB),useSpark);

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
                              "from %s --SPLICE-PROPERTIES useSpark = %s \n  order by empnum",
                          this.getTableReference(EMPTAB),useSpark);

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
                              "from %s  --SPLICE-PROPERTIES useSpark = %s \n order by dept",
                          this.getTableReference(EMPTAB),useSpark);

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
                              "from %s --SPLICE-PROPERTIES useSpark = %s \n order by empnum",
                          this.getTableReference(EMPTAB),useSpark);

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
                              "from %s --SPLICE-PROPERTIES useSpark = %s \n  order by empnum",
                          this.getTableReference(EMPTAB), useSpark);

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
                              "from %s --SPLICE-PROPERTIES useSpark = %s \n order by dept, empnum",
                          this.getTableReference(EMPTAB), useSpark);

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
                              "from %s --SPLICE-PROPERTIES useSpark = %s \n order by dept, empnum",
                          this.getTableReference(EMPTAB),useSpark);

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
                              "from %s  --SPLICE-PROPERTIES useSpark = %s \n order by dept, empnum",
                          this.getTableReference(EMPTAB), useSpark);

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
                              "sum(salary) over (Partition by dept ORDER BY salary, empnum rows between 1 preceding " +
                              "and 1 following) as sumsal from %s --SPLICE-PROPERTIES useSpark = %s \n ORDER BY empnum",
                          this.getTableReference(EMPTAB), useSpark);

        ResultSet rs = methodWatcher.executeQuery(sqlText);
        String expected =
            // DB-4600 - added order by to result to make comparison deterministic
            "EMPNUM |DEPT |SALARY |SUMSAL |\n" +
                "------------------------------\n" +
                "  10   |  1  | 50000 |102000 |\n" +
                "  20   |  1  | 75000 |204000 |\n" +
                "  30   |  3  | 84000 |163000 |\n" +
                "  40   |  2  | 52000 |155000 |\n" +
                "  44   |  2  | 52000 |157000 |\n" +
                "  49   |  2  | 53000 |105000 |\n" +
                "  50   |  1  | 52000 |154000 |\n" +
                "  55   |  1  | 52000 |157000 |\n" +
                "  60   |  1  | 78000 |154000 |\n" +
                "  70   |  1  | 76000 |229000 |\n" +
                "  80   |  3  | 79000 |238000 |\n" +
                "  90   |  2  | 51000 |103000 |\n" +
                "  100  |  3  | 55000 |130000 |\n" +
                "  110  |  1  | 53000 |180000 |\n" +
                "  120  |  3  | 75000 |209000 |";
        assertEquals("\n"+sqlText+"\n", expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
        rs.close();
    }

    @Test
    public void testAvg() throws Exception {
        String sqlText =
            String.format("SELECT empnum, dept, salary, " +
                              "avg(salary) over (Partition by dept ORDER BY salary) as avgsal " +
                              "from %s  --SPLICE-PROPERTIES useSpark = %s \n order by dept, empnum",
                          this.getTableReference(EMPTAB), useSpark);

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
                              "count(*) over (Partition by dept ORDER BY salary) as count from %s  --SPLICE-PROPERTIES useSpark = %s \n order by dept, empnum",
                          this.getTableReference(EMPTAB),useSpark);

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
                              "from %s  --SPLICE-PROPERTIES useSpark = %s \n order by empnum",
                          this.getTableReference(EMPTAB), useSpark);

        ResultSet rs = methodWatcher.executeQuery(sqlText);
        // DB-4600 - added order by to result to make comparison deterministic
        String expected =
            "EMPNUM |DEPT |SALARY |COUNTSAL |\n" +
                "--------------------------------\n" +
                "  10   |  1  | 50000 |    7    |\n" +
                "  20   |  1  | 75000 |    3    |\n" +
                "  30   |  3  | 84000 |    1    |\n" +
                "  40   |  2  | 52000 |    3    |\n" +
                "  44   |  2  | 52000 |    2    |\n" +
                "  49   |  2  | 53000 |    1    |\n" +
                "  50   |  1  | 52000 |    6    |\n" +
                "  55   |  1  | 52000 |    5    |\n" +
                "  60   |  1  | 78000 |    1    |\n" +
                "  70   |  1  | 76000 |    2    |\n" +
                "  80   |  3  | 79000 |    2    |\n" +
                "  90   |  2  | 51000 |    4    |\n" +
                "  100  |  3  | 55000 |    4    |\n" +
                "  110  |  1  | 53000 |    4    |\n" +
                "  120  |  3  | 75000 |    3    |";
        assertEquals("\n"+sqlText+"\n", expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
        rs.close();
    }

    @Test
    public void testCountRowsUnboundedPrecedingUnboundedFollowing() throws Exception {
        String sqlText =
            String.format("SELECT empnum, dept, salary, " +
                              "count(salary) over (Partition by dept ORDER BY salary rows between unbounded preceding and unbounded following) as sumsal " +
                              "from %s  --SPLICE-PROPERTIES useSpark = %s \n order by salary, dept, empnum",
                          this.getTableReference(EMPTAB), useSpark);

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
                              "count(salary) over (Partition by dept) as c from %s  --SPLICE-PROPERTIES useSpark = %s \n order by dept, empnum",
                          this.getTableReference(EMPTAB), useSpark);

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
                              "from %s  --SPLICE-PROPERTIES useSpark = %s \n order by dept, empnum",
                          this.getTableReference(EMPTAB), useSpark);

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
                              "from %s  --SPLICE-PROPERTIES useSpark = %s \n order by dept, empnum",
                          this.getTableReference(EMPTAB), useSpark);

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
                              "from %s  --SPLICE-PROPERTIES useSpark = %s \n order by dept, empnum",
                          this.getTableReference(EMPTAB), useSpark);

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
                              "sum(salary) over (Partition by dept ORDER BY salary, empnum rows between current row " +
                              "and unbounded following) from %s  --SPLICE-PROPERTIES useSpark = %s \n  ORDER BY empnum",
                          this.getTableReference(EMPTAB), useSpark);

        ResultSet rs = methodWatcher.executeQuery(sqlText);
        // DB-4600 - added order by to result to make comparison deterministic
        String expected =
                "EMPNUM |DEPT |SALARY |   4   |\n" +
                    "------------------------------\n" +
                    "  10   |  1  | 50000 |436000 |\n" +
                    "  20   |  1  | 75000 |229000 |\n" +
                    "  30   |  3  | 84000 | 84000 |\n" +
                    "  40   |  2  | 52000 |157000 |\n" +
                    "  44   |  2  | 52000 |105000 |\n" +
                    "  49   |  2  | 53000 | 53000 |\n" +
                    "  50   |  1  | 52000 |386000 |\n" +
                    "  55   |  1  | 52000 |334000 |\n" +
                    "  60   |  1  | 78000 | 78000 |\n" +
                    "  70   |  1  | 76000 |154000 |\n" +
                    "  80   |  3  | 79000 |163000 |\n" +
                    "  90   |  2  | 51000 |208000 |\n" +
                    "  100  |  3  | 55000 |293000 |\n" +
                    "  110  |  1  | 53000 |282000 |\n" +
                    "  120  |  3  | 75000 |238000 |";
        assertEquals("\n"+sqlText+"\n", expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
        rs.close();
    }

    @Test
    public void testRowsUnboundedPrecedingCurrentRowSortOnResult() throws Exception {
        String sqlText =
            String.format("SELECT empnum, dept, salary, " +
                              "sum(salary) over (Partition by dept ORDER BY salary, empnum rows between unbounded preceding and current row) as sumsal " +
                              "from %s  --SPLICE-PROPERTIES useSpark = %s \n order by dept, empnum",
                          this.getTableReference(EMPTAB), useSpark);

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
                              "from %s  --SPLICE-PROPERTIES useSpark = %s \n  order by dept, empnum",
                          this.getTableReference(EMPTAB), useSpark);

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
                                  "from %s  --SPLICE-PROPERTIES useSpark = %s \n order by dept, empnum",
                              this.getTableReference(EMPTAB), useSpark);

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
                                  "from %s  --SPLICE-PROPERTIES useSpark = %s \n  order by dept, empnum",
                              this.getTableReference(EMPTAB), useSpark);

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
                                  "from %s  --SPLICE-PROPERTIES useSpark = %s \n order by dept, empnum",
                              this.getTableReference(EMPTAB), useSpark);

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
                                  "from %s  --SPLICE-PROPERTIES useSpark = %s \n order by dept, empnum",
                              this.getTableReference(EMPTAB), useSpark);

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
    public void testRowNumberWithinPartition() throws Exception {
        String sqlText =
            String.format("SELECT empnum, dept, salary, " +
                              "ROW_NUMBER() OVER (partition by dept ORDER BY dept, empnum, salary desc) AS RowNumber " +
                              "FROM %s  --SPLICE-PROPERTIES useSpark = %s \n  ORDER BY empnum",
                          this.getTableReference(EMPTAB), useSpark);

        ResultSet rs = methodWatcher.executeQuery(sqlText);
        // DB-4600 - added order by to result to make comparison deterministic
        String expected =
                "EMPNUM |DEPT |SALARY | ROWNUMBER |\n" +
                    "----------------------------------\n" +
                    "  10   |  1  | 50000 |     1     |\n" +
                    "  20   |  1  | 75000 |     2     |\n" +
                    "  30   |  3  | 84000 |     1     |\n" +
                    "  40   |  2  | 52000 |     1     |\n" +
                    "  44   |  2  | 52000 |     2     |\n" +
                    "  49   |  2  | 53000 |     3     |\n" +
                    "  50   |  1  | 52000 |     3     |\n" +
                    "  55   |  1  | 52000 |     4     |\n" +
                    "  60   |  1  | 78000 |     5     |\n" +
                    "  70   |  1  | 76000 |     6     |\n" +
                    "  80   |  3  | 79000 |     2     |\n" +
                    "  90   |  2  | 51000 |     4     |\n" +
                    "  100  |  3  | 55000 |     3     |\n" +
                    "  110  |  1  | 53000 |     7     |\n" +
                    "  120  |  3  | 75000 |     4     |";
        assertEquals("\n"+sqlText+"\n", expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
        rs.close();
    }

    @Test
    public void testRowNumberWithoutPartiion() throws Exception {
        String sqlText =
            String.format("SELECT empnum, dept, salary, ROW_NUMBER() OVER (ORDER BY dept, empnum, salary desc) AS RowNumber " +
                              "FROM %s  --SPLICE-PROPERTIES useSpark = %s \n ",
                          this.getTableReference(EMPTAB), useSpark);

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
            String.format("SELECT empnum, dept, salary, RANK() OVER (PARTITION BY dept ORDER BY dept, empnum, " +
                              "salary desc) AS Rank FROM %s  --SPLICE-PROPERTIES useSpark = %s \n ORDER BY empnum",
                          this.getTableReference(EMPTAB), useSpark);

        ResultSet rs = methodWatcher.executeQuery(sqlText);
        // DB-4600 - added order by to result to make comparison deterministic
        String expected =
                "EMPNUM |DEPT |SALARY |RANK |\n" +
                    "----------------------------\n" +
                    "  10   |  1  | 50000 |  1  |\n" +
                    "  20   |  1  | 75000 |  2  |\n" +
                    "  30   |  3  | 84000 |  1  |\n" +
                    "  40   |  2  | 52000 |  1  |\n" +
                    "  44   |  2  | 52000 |  2  |\n" +
                    "  49   |  2  | 53000 |  3  |\n" +
                    "  50   |  1  | 52000 |  3  |\n" +
                    "  55   |  1  | 52000 |  4  |\n" +
                    "  60   |  1  | 78000 |  5  |\n" +
                    "  70   |  1  | 76000 |  6  |\n" +
                    "  80   |  3  | 79000 |  2  |\n" +
                    "  90   |  2  | 51000 |  4  |\n" +
                    "  100  |  3  | 55000 |  3  |\n" +
                    "  110  |  1  | 53000 |  7  |\n" +
                    "  120  |  3  | 75000 |  4  |";
        assertEquals("\n"+sqlText+"\n", expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
        rs.close();
    }

    @Test
    public void testRankWithoutPartiion() throws Exception {
        String sqlText =
            String.format("SELECT empnum, dept, salary, RANK() OVER (ORDER BY salary desc) AS Rank FROM %s   --SPLICE-PROPERTIES useSpark = %s \n  order by dept, empnum",
                          this.getTableReference(EMPTAB), useSpark);

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
                              "FROM %s  --SPLICE-PROPERTIES useSpark = %s \n  order by dept, empnum",
                          this.getTableReference(EMPTAB), useSpark);

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
    public void testDenseRankWithoutPartition() throws Exception {
        String sqlText =
            String.format("SELECT empnum, dept, salary, DENSE_RANK() OVER (ORDER BY salary desc) AS denseank " +
                              "FROM %s  --SPLICE-PROPERTIES useSpark = %s \n order by dept, empnum" ,
                          this.getTableReference(EMPTAB), useSpark);

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
                              "FROM %s  --SPLICE-PROPERTIES useSpark = %s \n ",
                          this.getTableReference(EMPTAB), useSpark);

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
                              "FROM %s  --SPLICE-PROPERTIES useSpark = %s \n  order by empnum",
                          this.getTableReference(EMPTAB),useSpark);

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
                              "FROM %s  --SPLICE-PROPERTIES useSpark = %s \n order by empnum",
                          this.getTableReference(EMPTAB),useSpark);

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
            String.format("SELECT empnum, dept, salary, DENSE_RANK() OVER (PARTITION BY dept ORDER BY dept, empnum, salary desc) AS DenseRank FROM %s  --SPLICE-PROPERTIES useSpark = %s \n order by empnum",
                          this.getTableReference(EMPTAB),useSpark);

        ResultSet rs = methodWatcher.executeQuery(sqlText);
        // DB-4600 - added order by to result to make comparison deterministic
        String expected =
                "EMPNUM |DEPT |SALARY | DENSERANK |\n" +
                    "----------------------------------\n" +
                    "  10   |  1  | 50000 |     1     |\n" +
                    "  20   |  1  | 75000 |     2     |\n" +
                    "  30   |  3  | 84000 |     1     |\n" +
                    "  40   |  2  | 52000 |     1     |\n" +
                    "  44   |  2  | 52000 |     2     |\n" +
                    "  49   |  2  | 53000 |     3     |\n" +
                    "  50   |  1  | 52000 |     3     |\n" +
                    "  55   |  1  | 52000 |     4     |\n" +
                    "  60   |  1  | 78000 |     5     |\n" +
                    "  70   |  1  | 76000 |     6     |\n" +
                    "  80   |  3  | 79000 |     2     |\n" +
                    "  90   |  2  | 51000 |     4     |\n" +
                    "  100  |  3  | 55000 |     3     |\n" +
                    "  110  |  1  | 53000 |     7     |\n" +
                    "  120  |  3  | 75000 |     4     |";
        assertEquals("\n"+sqlText+"\n", expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
        rs.close();
    }

    @Test
    public void testDenseRankWithPartition2OrderByCols_duplicateKey() throws Exception {
        String sqlText =
            String.format("SELECT empnum, dept, salary, DENSE_RANK() OVER (PARTITION BY dept ORDER BY dept, salary desc) AS DenseRank " +
                              "FROM %s  --SPLICE-PROPERTIES useSpark = %s \n  order by empnum",
                          this.getTableReference(EMPTAB), useSpark);

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
            String.format("SELECT empnum, salary, DENSE_RANK() OVER (PARTITION BY dept ORDER BY dept, empnum, " +
                              "salary desc) AS DenseRank FROM %s --SPLICE-PROPERTIES useSpark = %s \n  order by empnum",
                          this.getTableReference(EMPTAB),useSpark);

        ResultSet rs = methodWatcher.executeQuery(sqlText);
        // DB-4600 - added order by to result to make comparison deterministic
        String expected =
                "EMPNUM |SALARY | DENSERANK |\n" +
                    "----------------------------\n" +
                    "  10   | 50000 |     1     |\n" +
                    "  20   | 75000 |     2     |\n" +
                    "  30   | 84000 |     1     |\n" +
                    "  40   | 52000 |     1     |\n" +
                    "  44   | 52000 |     2     |\n" +
                    "  49   | 53000 |     3     |\n" +
                    "  50   | 52000 |     3     |\n" +
                    "  55   | 52000 |     4     |\n" +
                    "  60   | 78000 |     5     |\n" +
                    "  70   | 76000 |     6     |\n" +
                    "  80   | 79000 |     2     |\n" +
                    "  90   | 51000 |     4     |\n" +
                    "  100  | 55000 |     3     |\n" +
                    "  110  | 53000 |     7     |\n" +
                    "  120  | 75000 |     4     |";
        assertEquals("\n"+sqlText+"\n", expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
        rs.close();
    }

    @Test
    public void testDenseRankWithoutPartitionOrderby() throws Exception {
        String sqlText =
            String.format("SELECT empnum, dept, salary, DENSE_RANK() OVER (ORDER BY salary desc) AS Rank " +
                              "FROM %s  --SPLICE-PROPERTIES useSpark = %s \n order by empnum",
                          this.getTableReference(EMPTAB), useSpark);

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
                              "from %s  --SPLICE-PROPERTIES useSpark = %s \n order by PersonID",
                          this.getTableReference(PEOPLE), useSpark);

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
                              "from %s  --SPLICE-PROPERTIES useSpark = %s \n order by PersonID",
                          this.getTableReference(PEOPLE), useSpark);

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
            String.format("SELECT sum(price) over (Partition by item ORDER BY date) as  sumprice from %s  --SPLICE-PROPERTIES useSpark = %s \n order by sumprice",
                        this.getTableReference(PURCHASED), useSpark);

        ResultSet rs = methodWatcher.executeQuery(sqlText);
        String expected =
                "SUMPRICE |\n" +
                    "----------\n" +
                    "  1.00   |\n" +
                    "  2.00   |\n" +
                    "  3.00   |\n" +
                    "  6.00   |\n" +
                    "  9.00   |\n" +
                    "  10.00  |\n" +
                    "  10.00  |\n" +
                    "  11.00  |\n" +
                    "  11.00  |\n" +
                    "  12.00  |\n" +
                    "  15.00  |\n" +
                    "  20.00  |\n" +
                    "  20.00  |\n" +
                    "  23.00  |\n" +
                    "  25.00  |";
        assertEquals("\n"+sqlText+"\n", expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
        rs.close();
    }

    @Test
    public void testSelectAllColsScalarAggWithOrderBy() throws Exception {
        // DB-1774
        String sqlText =
                String.format("SELECT item, price, sum(price) over (Partition by item ORDER BY date) as sumsal, date " +
                                "from %s  --SPLICE-PROPERTIES useSpark = %s \n order by item",
                        this.getTableReference(PURCHASED), useSpark);

        ResultSet rs = methodWatcher.executeQuery(sqlText);
        // Validated with PostgreSQL app
        String expected =
                "ITEM | PRICE |SUMSAL |         DATE           |\n" +
                        "-----------------------------------------------\n" +
                "  1  | 1.00  | 1.00  |2014-09-08 17:45:15.204 |\n" +
                "  1  | 1.00  | 2.00  |2014-09-08 18:27:48.881 |\n" +
                "  1  | 7.00  | 9.00  |2014-09-08 18:33:46.446 |\n" +
                "  2  | 6.00  | 6.00  |2014-09-08 17:50:17.182 |\n" +
                "  2  | 5.00  | 11.00 |2014-09-08 18:26:51.387 |\n" +
                "  2  | 12.00 | 23.00 |2014-09-08 18:40:15.48  |\n" +
                "  3  | 3.00  | 3.00  |2014-09-08 17:36:55.414 |\n" +
                "  3  | 7.00  | 10.00 |2014-09-08 18:00:44.742 |\n" +
                "  3  | 10.00 | 20.00 |2014-09-08 18:25:42.387 |\n" +
                "  4  | 10.00 | 10.00 |2014-09-08 17:50:17.182 |\n" +
                "  4  | 2.00  | 12.00 |2014-09-08 18:05:47.166 |\n" +
                "  4  | 8.00  | 20.00 |2014-09-08 18:08:04.986 |\n" +
                "  5  | 11.00 | 11.00 |2014-09-08 17:41:56.353 |\n" +
                "  5  | 4.00  | 15.00 |2014-09-08 17:46:26.428 |\n" +
                "  5  | 10.00 | 25.00 |2014-09-08 18:11:23.645 |";
        assertEquals("\n"+sqlText+"\n", expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
        rs.close();
    }

    @Test
    public void testWindowFunctionWithGroupBy() throws Exception {

        String sqlText =
            "select empnum, dept, sum(salary)," +
                "rank() over(partition by dept order by salary desc) rank " +
                "from %s  --SPLICE-PROPERTIES useSpark = %s \n " +
                "group by empnum, dept order by empnum";

        ResultSet rs = methodWatcher.executeQuery(
            String.format(sqlText, this.getTableReference(EMPTAB), useSpark));

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
                              "from %s  --SPLICE-PROPERTIES useSpark = %s \n order by empnum",
                          this.getTableReference(EMPTAB), useSpark);

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
                              "from %s  --SPLICE-PROPERTIES useSpark = %s \n order by empnum",
                          this.getTableReference(EMPTAB), useSpark);

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
                              "rank() over ( order by sum(sal) ) empsal, ename from %s  --SPLICE-PROPERTIES useSpark = %s \n " +
                              "group by ename order by ename",
                          this.getTableReference(EMP), useSpark);

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
            "select sal, rank() over ( order by sum(sal) ) empsal, ename from %s  --SPLICE-PROPERTIES useSpark = %s \n ";

        methodWatcher.executeQuery(
            String.format(sqlText, this.getTableReference(EMP), useSpark));
    }

    @Test
    public void testRankWith2AggAsOrderByCol() throws Exception {
        // have to order by ename here because it's the only col with unique values and forces repeatable results
        String sqlText =
            String.format("select sum(sal) as sum_sal, avg(sal) as avg_sal, " +
                              "rank() over ( order by sum(sal), avg(sal) ) empsal, ename " +
                              "from %s  --SPLICE-PROPERTIES useSpark = %s \n  group by ename order by ename",
                          this.getTableReference(EMP), useSpark);

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


    // TODO : difference of precision, need to be // FIXME: 10/12/16
    @Test @Ignore
    public void testMediaForDept() throws Exception {
        // DB-1650, DB-2020
        String sqlText = String.format("SELECT %1$s.Nome_Dep, %2$s.Nome AS Funcionario, %2$s.Salario, " +
                                           "AVG(%2$s.Salario) OVER(PARTITION BY %1$s.Nome_Dep) \"Média por " +
                                           "Departamento\", " +
                                           "%2$s.Salario - AVG(%2$s.Salario) as \"Diferença de Salário\" FROM %2$s " +
                                           "INNER" +
                                           " JOIN %1$s ON %2$s.ID_Dep = %1$s.ID group by %1$s.Nome_Dep," +
                                           "%2$s.Nome, %2$s.Salario ORDER BY 3 DESC, 1",
                                       this.getTableReference(DEPARTAMENTOS), this.getTableReference(FUNCIONARIOS), useSpark);

        ResultSet rs = methodWatcher.executeQuery(sqlText);

        String expected =
            "NOME_DEP     | FUNCIONARIO | SALARIO |Média por Departamento |Diferença de Salário |\n" +
                "----------------------------------------------------------------------------------------\n" +
                "Recursos Humanos |   Luciano   |23500.00 |      14166.3333       |       0.0000        |\n" +
                "Recursos Humanos |  Zavaschi   |13999.00 |      14166.3333       |       0.0000        |\n" +
                "       IT        |   Nogare    |11999.00 |       5499.6667       |       0.0000        |\n" +
                "     Vendas      |    Diego    | 9000.00 |       4500.0000       |       0.0000        |\n" +
                "Recursos Humanos |   Laerte    | 5000.00 |      14166.3333       |       0.0000        |\n" +
                "       IT        |  Ferreira   | 2500.00 |       5499.6667       |       0.0000        |\n" +
                "     Vendas      |   Amorim    | 2500.00 |       4500.0000       |       0.0000        |\n" +
                "       IT        |   Felipe    | 2000.00 |       5499.6667       |       0.0000        |\n" +
                "     Vendas      |   Fabiano   | 2000.00 |       4500.0000       |       0.0000        |";
        assertEquals("\n"+sqlText+"\n", expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
        rs.close();
    }


    // TODO : difference of precision, need to be // FIXME: 10/12/16
    @Test @Ignore
    public void testConstMinusAvg1ReversedJoinOrder() throws Exception {
        /*
         * Because the WindowFunction performs a computation, we still have unordered results coming out
         * even though the joined fields are primary keys (and therefore sorted similarly). Thus, we add
         *  a sort order to the query to ensure consistent query results.
         */
        String sqlText = String.format("SELECT %2$s.Salario - AVG(%2$s.Salario) OVER(PARTITION BY " +
                                           "%1$s.Nome_Dep) \"Diferença de Salário\" FROM " +
                                           "%1$s   --SPLICE-PROPERTIES useSpark = %3$s \n INNER JOIN %2$s " +
                                           "ON %2$s.ID_Dep = %1$s.ID order by 1",
                                       this.getTableReference(DEPARTAMENTOS), this.getTableReference(FUNCIONARIOS), useSpark);

        try(ResultSet rs = methodWatcher.executeQuery(sqlText)){

            String expected=
                "Diferença de Salário |\n" +
                    "----------------------\n" +
                    "     -9166.3333      |\n" +
                    "     -3499.6667      |\n" +
                    "     -2999.6667      |\n" +
                    "     -2500.0000      |\n" +
                    "     -2000.0000      |\n" +
                    "      -167.3333      |\n" +
                    "      4500.0000      |\n" +
                    "      6499.3333      |\n" +
                    "      9333.6667      |" ;
            assertEquals("\n"+sqlText+"\n",expected,TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
        }
    }

    // TODO : difference of precision, need to be // FIXME: 10/12/16
    @Test @Ignore
    public void testConstMinusAvg1() throws Exception {
        // DB-2124
        /*
         * We attach an Order by clause to this query to ensure that we get back results in a consistent order,
         * and that our string comparison is stable across platforms and inherent sort orders.
         *
         * Essentially, even though the join fields are both primary keys (and therefore both sorted), the WindowFunction
         * itself destroys the sort order because it's a computation field. Therefore, we must sort to get back results
         * consistently
         */
        String sqlText = String.format("SELECT %2$s.Salario - AVG(%2$s.Salario) OVER(PARTITION BY " +
                                           "%1$s.Nome_Dep) \"Diferença de Salário\" FROM " +
                                           "%2$s --SPLICE-PROPERTIES useSpark = %3$s \n  INNER JOIN %1$s " +
                                           "ON %2$s.ID_Dep = %1$s.ID order by 1",
                                       this.getTableReference(DEPARTAMENTOS), this.getTableReference(FUNCIONARIOS), useSpark);

        try(ResultSet rs = methodWatcher.executeQuery(sqlText)){

            String expected=
                    "Diferença de Salário |\n" +
                        "----------------------\n" +
                        "     -9166.3333      |\n" +
                        "     -3499.6667      |\n" +
                        "     -2999.6667      |\n" +
                        "     -2500.0000      |\n" +
                        "     -2000.0000      |\n" +
                        "      -167.3333      |\n" +
                        "      4500.0000      |\n" +
                        "      6499.3333      |\n" +
                        "      9333.6667      |" ;
            assertEquals("\n"+sqlText+"\n",expected,TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
        }
    }

    // TODO : difference of precision, need to be // FIXME: 10/12/16
    @Test @Ignore
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
                                       this.getTableReference(DEPARTAMENTOS), this.getTableReference(FUNCIONARIOS), useSpark);

        ResultSet rs = methodWatcher.executeQuery(sqlText);
        // validated with PostgreSQL app
        String expected =
            "NOME_DEP     | FUNCIONARIO | SALARIO |Média por Departamento |Diferença de Salário |\n" +
                "----------------------------------------------------------------------------------------\n" +
                "Recursos Humanos |   Luciano   |23500.00 |      14166.3333       |      9333.6667      |\n" +
                "       IT        |   Nogare    |11999.00 |       5499.6667       |      6499.3333      |\n" +
                "     Vendas      |    Diego    | 9000.00 |       4500.0000       |      4500.0000      |\n" +
                "Recursos Humanos |  Zavaschi   |13999.00 |      14166.3333       |      -167.3333      |\n" +
                "     Vendas      |   Amorim    | 2500.00 |       4500.0000       |     -2000.0000      |\n" +
                "     Vendas      |   Fabiano   | 2000.00 |       4500.0000       |     -2500.0000      |\n" +
                "       IT        |  Ferreira   | 2500.00 |       5499.6667       |     -2999.6667      |\n" +
                "       IT        |   Felipe    | 2000.00 |       5499.6667       |     -3499.6667      |\n" +
                "Recursos Humanos |   Laerte    | 5000.00 |      14166.3333       |     -9166.3333      |";
        assertEquals("\n"+sqlText+"\n", expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
        rs.close();
    }

    // TODO : difference of precision, need to be // FIXME: 10/12/16
    @Test @Ignore
    public void testSumTimesConstDivSum() throws Exception {
        // DB-2086 - identical agg gets removed from aggregates array
        String sqlText = String.format("SELECT %1$s.Nome_Dep, SUM(%2$s.Salario) * 100 / " +
                                           "SUM(%2$s.Salario) OVER(PARTITION BY %1$s.Nome_Dep) " +
                                           "\"Média por Departamento\" " +
                                           "FROM %2$s, %1$s  --SPLICE-PROPERTIES useSpark = %3$s \n GROUP BY %1$s.Nome_Dep, %2$s.Salario ORDER BY %1$s.Nome_Dep, %2$s.Salario ",
                                       this.getTableReference(DEPARTAMENTOS), this.getTableReference(FUNCIONARIOS), useSpark);

        ResultSet rs = methodWatcher.executeQuery(sqlText);

        // Verified with PostgreSQL app
        String expected =
            "NOME_DEP     |Média por Departamento |\n" +
                "------------------------------------------\n" +
                "       IT        |        5.8825         |\n" +
                "       IT        |        7.3531         |\n" +
                "       IT        |        7.3531         |\n" +
                "       IT        |        13.2356        |\n" +
                "       IT        |        17.6461        |\n" +
                "       IT        |        20.5873        |\n" +
                "       IT        |        34.5598        |\n" +
                "Recursos Humanos |        5.8825         |\n" +
                "Recursos Humanos |        7.3531         |\n" +
                "Recursos Humanos |        7.3531         |\n" +
                "Recursos Humanos |        13.2356        |\n" +
                "Recursos Humanos |        17.6461        |\n" +
                "Recursos Humanos |        20.5873        |\n" +
                "Recursos Humanos |        34.5598        |\n" +
                "     Vendas      |        5.8825         |\n" +
                "     Vendas      |        7.3531         |\n" +
                "     Vendas      |        7.3531         |\n" +
                "     Vendas      |        13.2356        |\n" +
                "     Vendas      |        17.6461        |\n" +
                "     Vendas      |        20.5873        |\n" +
                "     Vendas      |        34.5598        |";
        assertEquals("\n"+sqlText+"\n", expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
        rs.close();
    }

    @Test
    public void testDB2170RankOverView() throws Exception {
        String sqlText =
            String.format("select yr, rank() over ( partition by yr order by hiredate ) as EMPRANK, ename," +
                              "hiredate from %s   --SPLICE-PROPERTIES useSpark = %s \n  order by yr, EMPRANK, ename", this.getTableReference(YEAR_VIEW), useSpark);
        ResultSet rs = methodWatcher.executeQuery(sqlText);
        String expected =
                "YR | EMPRANK | ENAME | HIREDATE  |\n" +
                        "----------------------------------\n" +
                        "80 |    1    | SMITH |1980-12-17 |\n" +
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
                        "82 |    1    |MILLER |1982-01-23 |\n" +
                "82 |    2    | SCOTT |1982-12-09 |\n" +
                "83 |    1    | ADAMS |1983-01-12 |";
        assertEquals("\n"+sqlText+"\n", expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
        rs.close();
    }

    @Test
    @Ignore("DB-2170 - window function over view. (works periodically, why?)")
    public void testDB2170RankOverViewMissingKey() throws Exception {
        String sqlText =
            String.format("select rank() over ( partition by yr order by hiredate ) as EMPRANK, ename," +
                              "hiredate from %s  --SPLICE-PROPERTIES useSpark = %s \n ", this.getTableReference(YEAR_VIEW), useSpark);
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
    public void testDB2170MaxOverView() throws Exception {
        String sqlText =
            String.format("select max(hiredate)   over () as maxhiredate, ename," +
                              "hiredate from %s --SPLICE-PROPERTIES useSpark = %s \n order by ename", this.getTableReference(YEAR_VIEW), useSpark);
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
            String.format("select max(hiredate) over () as maxhiredate, ename, hiredate from %s   --SPLICE-PROPERTIES useSpark = %s \n order by ename",
                          this.getTableReference(EMP), useSpark);

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
            String.format("select max(hiredate) as maxhiredate, ename,hiredate from %s   --SPLICE-PROPERTIES useSpark = %s \n group by ename, hiredate order by ename",
                          this.getTableReference(YEAR_VIEW), useSpark);
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

    @Test
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
                              "AS rowrnk FROM  %1$s BAF  --SPLICE-PROPERTIES useSpark = %3$s \n  INNER JOIN %2$s BIP ON BAF.individual_id=BIP.individual_id " +
                              "WHERE BAF.rwrnk = 1",
                          this.getTableReference(best_addr_freq_NAME), this.getTableReference(best_ids_pool_NAME), useSpark);
        ResultSet rs = methodWatcher.executeQuery(sqlText);

        // Results too large to compare
        assertNotNull("\n" + sqlText + "\n", TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
        rs.close();
    }

    @Test
    public void testWindowFunctionInCaseStmt() throws Exception {
        // DB-3999 - WF in case stmt not working
        // Added a rank() WF to the select to make sure we still get the ref to the commission col pulled up
        String sqlText = format("SELECT employee_id, rank() over(order by salary), CASE WHEN (commission is not null)" +
                                    " THEN min(salary) over(PARTITION BY department) ELSE -1 END as  minimum_sal" +
                                    " FROM %s   --SPLICE-PROPERTIES useSpark = %s \n order by minimum_sal, employee_id", EMPLOYEES_REF, useSpark);
        ResultSet rs = methodWatcher.executeQuery(sqlText);
        // Verified with PostgreSQL App
        String expected =
            "EMPLOYEE_ID | 2 | MINIMUM_SAL |\n" +
                "--------------------------------\n" +
                "     101     | 1 |     -1      |\n" +
                "     107     | 2 |     -1      |\n" +
                "     108     | 2 |     -1      |\n" +
                "     105     | 8 |    10000    |\n" +
                "     106     | 4 |    10000    |\n" +
                "     104     | 7 |    12000    |\n" +
                "     102     | 4 |    20000    |\n" +
                "     103     | 6 |    20000    |";
        assertEquals("\n"+sqlText+"\n", expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
        rs.close();
    }

    @Test
    public void testLastValueFunction() throws Exception {
        // DB-3920
        String sqlText = format("select empno, salary, deptno, last_value(empno) over(partition by deptno order by " +
                                    "salary asc range between current row and unbounded following) as last_val from  \n " +
                                    "%s  --SPLICE-PROPERTIES useSpark = %s \n order by empno asc", EMP_2_REF, useSpark);
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
        String sqlText = format("select empno, salary, deptno, first_value(empno) over(partition by deptno order by " +
                                    "salary asc, empno rows unbounded preceding) as first_value from  " +
                                    "%s --SPLICE-PROPERTIES useSpark = %s \n order by empno asc", EMP3_REF, useSpark);
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

        String sqlText = format("SELECT month, SUM(amount) AS month_amount, " +
                                    "LAST_VALUE(SUM(amount)) OVER (ORDER BY month ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) " +
                                    "AS next_month_amount FROM %s  --SPLICE-PROPERTIES useSpark = %s \n GROUP BY month ORDER BY month", ALL_SALES_2_REF, useSpark);
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

    @Test
    public void testLeadWithAlias() throws Exception {
        // DB-4757

        // a.ratingEffDate is aliased as effDate - WindowResultSetNode was not recognizing the alias projected out the top
        // to find the column reference of the underlying table column
        String sqlText = format("select a.productId, a.ratingAgencyCode, a.ratingEffDate as effDate, a.rating, LEAD(ratingeffDate)" +
                                    " OVER (PARTITION BY productId, ratingAgencyCode ORDER BY ratingeffDate) \"endDate\" " +
                                    "from %s a --SPLICE-PROPERTIES useSpark = %s \n  order by productId, ratingAgencyCode", PRODUCT_RATING_REF, useSpark);
        ResultSet rs = methodWatcher.executeQuery(sqlText);
        // Verified with PostgreSQL App
        String expected =
            "PRODUCTID | RATINGAGENCYCODE   |        EFFDATE         |    RATING      |        endDate         |\n" +
                "----------------------------------------------------------------------------------------------------\n" +
                "     1     |       Best         |2014-08-29 22:30:41.012 |BIMTVQOBOXOMBNI |         NULL           |\n" +
                "     1     |Dont think about it |2014-02-09 08:12:02.596 |PXVDSMZWANQWGCX |2015-01-25 19:47:53.004 |\n" +
                "     1     |Dont think about it |2015-01-25 19:47:53.004 |NWAJOBJVSGIAIZU |         NULL           |\n" +
                "     1     |       Good         |2014-11-02 03:24:36.044 |TILAXMXGUEYZSEK |2014-12-14 05:52:22.326 |\n" +
                "     1     |       Good         |2014-12-14 05:52:22.326 |UMTHLNRYDFAYWQS |2015-07-22 04:39:19.453 |\n" +
                "     1     |       Good         |2015-07-22 04:39:19.453 |XQSJCCCGLIOMMCJ |2015-12-04 16:40:43.67  |\n" +
                "     1     |       Good         |2015-12-04 16:40:43.67  |BLYUEGSMCFCPXJA |         NULL           |\n" +
                "     2     |Dont think about it |2014-02-14 04:59:08.36  |JVEZECWKNBLAALU |         NULL           |\n" +
                "     2     |       Good         |2015-01-22 11:41:01.329 |ZRHSIVRCQUUQZLC |2015-10-06 03:44:19.184 |\n" +
                "     2     |       Good         |2015-10-06 03:44:19.184 |NZRXIDYRZNUVXDU |         NULL           |\n" +
                "     2     |     Run Away       |2014-05-16 22:44:34.056 |ZSFHGWWFCQZZNMO |2015-06-30 07:06:13.865 |\n" +
                "     2     |     Run Away       |2015-06-30 07:06:13.865 |IAUEYFHYZBFMJFW |         NULL           |\n" +
                "    700    |      Better        |2014-01-27 01:22:55.581 |CHZGQMKFKUXIEAY |         NULL           |\n" +
                "    700    |Dont think about it |2014-07-19 08:25:52.238 |UYLCQPSCHURELLY |         NULL           |\n" +
                "    700    |       Poor         |2014-02-07 12:19:36.492 |YPXRMRFFYYPWBUZ |2014-08-21 21:27:35.482 |\n" +
                "    700    |       Poor         |2014-08-21 21:27:35.482 |ZKBLWNEPGNMQUTL |         NULL           |\n" +
                "    800    |      Better        |2015-07-30 11:36:16.128 |UUAYIPTXDNCCBTU |2015-11-25 00:16:42.154 |\n" +
                "    800    |      Better        |2015-11-25 00:16:42.154 |ZCQJQILUQRCHWCY |         NULL           |\n" +
                "    800    |     Run Away       |2014-01-04 21:08:27.058 |FWXYVWSYDWYXXBZ |2014-11-04 19:02:56.001 |\n" +
                "    800    |     Run Away       |2014-11-04 19:02:56.001 |ZIMKXUOKMCWCGSZ |2015-01-09 13:29:29.642 |\n" +
                "    800    |     Run Away       |2015-01-09 13:29:29.642 |KMCNRDOWZFAEVFS |2015-06-23 20:21:06.677 |\n" +
                "    800    |     Run Away       |2015-06-23 20:21:06.677 |DBAEKJQCJBGMOYQ |         NULL           |\n" +
                "   3300    |      Better        |2014-03-07 19:37:30.604 |WDQTAXMXLMTWXQL |2014-05-22 10:42:47.834 |\n" +
                "   3300    |      Better        |2014-05-22 10:42:47.834 |QOJYERFYJAYGNIC |2015-01-13 11:01:19.065 |\n" +
                "   3300    |      Better        |2015-01-13 11:01:19.065 |SPIHABCGNPNOOAC |         NULL           |\n" +
                "   3300    |Dont think about it |2014-11-03 05:15:49.851 |TMXIAVAKXCDJYKY |2015-08-13 21:03:44.191 |\n" +
                "   3300    |Dont think about it |2015-08-13 21:03:44.191 |MRUQGZIKYBXHZPQ |         NULL           |\n" +
                "   3300    |       Poor         |2015-09-02 05:09:32.683 |FFXXMDXCAVYDJTC |         NULL           |\n" +
                "   4040    |       Best         |2014-08-21 15:48:24.096 |JSKWDFWMTQCVJTS |         NULL           |\n" +
                "   4040    |Dont think about it |2015-06-10 12:59:47.54  |LTCOVEBAVEHNCRP |         NULL           |\n" +
                "   4040    |       Poor         |2014-07-29 18:45:46.257 |EMXGEGPTWXUECLS |         NULL           |\n" +
                "   4040    |     Run Away       |2014-06-22 08:12:53.937 |PHAMOZYXPQXWJDF |2015-05-09 01:21:16.513 |\n" +
                "   4040    |     Run Away       |2015-05-09 01:21:16.513 |YNWDVEUSONYHZXE |         NULL           |\n" +
                "   4440    |Dont think about it |2014-03-11 10:19:44.256 |NIILBXPPMRRKVSG |2014-08-05 09:55:23.669 |\n" +
                "   4440    |Dont think about it |2014-08-05 09:55:23.669 |GLUUGLMTGJRUHSA |2015-01-06 21:26:12.528 |\n" +
                "   4440    |Dont think about it |2015-01-06 21:26:12.528 |KVPKDRBXWBGATQY |         NULL           |\n" +
                "   4440    |       Good         |2014-07-27 14:25:56.885 |APGRMQQJCJSTCDB |         NULL           |\n" +
                "   4440    |     Run Away       |2014-04-11 06:29:30.187 |WYREDMPIIZPGXZA |2014-11-12 06:40:59.685 |\n" +
                "   4440    |     Run Away       |2014-11-12 06:40:59.685 |VVEEPGAEVWDLBPF |         NULL           |\n" +
                "   5500    |       Best         |2015-01-23 18:15:43.723 |JRLYTUTWIZNOAPT |         NULL           |\n" +
                "   5500    |       Good         |2014-07-31 20:33:36.713 |OLSEYNIBUDVHEIF |         NULL           |\n" +
                "   6999    |Dont think about it |2014-07-30 10:23:12.643 |MRVUOQXPZMVTVXW |2014-09-21 01:20:16.26  |\n" +
                "   6999    |Dont think about it |2014-09-21 01:20:16.26  |MLBLXWCFKHVWXVU |2015-05-23 16:04:52.561 |\n" +
                "   6999    |Dont think about it |2015-05-23 16:04:52.561 |HCZIXOTTRASHITI |2015-08-03 18:40:21.753 |\n" +
                "   6999    |Dont think about it |2015-08-03 18:40:21.753 |QZMNXNYRPEKJWBY |         NULL           |\n" +
                "   6999    |       Poor         |2015-04-30 05:52:43.627 |LFHHSWUBDDIPKPA |         NULL           |\n" +
                "   9001    |       Best         |2014-11-01 21:51:02.199 |JFVQBHBCTVPVZTR |         NULL           |\n" +
                "   9001    |Dont think about it |2015-06-11 21:00:40.206 |ILWSWUTLTSCJRSV |         NULL           |\n" +
                "   9001    |       Good         | 2015-09-27 17:49:39.5  |XHFHMOPWMOJNGOL |         NULL           |\n" +
                "   9001    |       Poor         |2014-05-22 22:20:20.929 |HZLYPGHDEQORMOH |         NULL           |";
        assertEquals("\n"+sqlText+"\n", expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
        rs.close();
    }

    @Test @Ignore("DB-3927: found possible frame processing problem while implementing first_value()")
    public void testFirstValueWithAggregateArgument() throws Exception {
        // DB-3927

        String sqlText = format("SELECT month, SUM(amount) AS month_amount, " +
                                    "FIRST_VALUE(SUM(amount)) OVER (ORDER BY month ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) " +
                                    "AS prev_month_amount FROM %s   --SPLICE-PROPERTIES useSpark = %s \n GROUP BY month ORDER BY month", ALL_SALES_3_REF, useSpark);
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
        String sqlText = format("select empno, salary, deptno, " +
                                    "LEAD(SALARY) OVER (PARTITION BY DEPTNO ORDER BY SALARY DESC, EMPNO) NEXT_LOWER_SAL from  " +
                                    "%s   --SPLICE-PROPERTIES useSpark = %s \n  order by deptno, SALARY DESC", EMP_LEAD_1_REF, useSpark);
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
        String sqlText = format("select empno, salary, deptno, " +
                                    "LAG(SALARY) OVER (PARTITION BY DEPTNO ORDER BY SALARY DESC, empno) NEXT_LOWER_SAL from " +
                                    "%s   --SPLICE-PROPERTIES useSpark = %s \n order by deptno, SALARY DESC, empno", EMP3_REF, useSpark);
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
        String sqlText = format("select empno, salary, deptno, " +
                                "last_value((CASE WHEN empno < 17 THEN 0 WHEN empno >= 17 THEN 1 END)) " +
                                "over(partition by deptno order by salary asc range between current row and unbounded following) as last_val from " +
                                "%s   --SPLICE-PROPERTIES useSpark = %s \n  order by empno asc", EMP_4_REF, useSpark);
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
        String sqlText = format("select empno, salary, deptno, " +
                                "first_value((CASE WHEN empno < 17 THEN 1 WHEN empno >= 17 THEN 0 END)) " +
                                "over(partition by deptno order by salary asc rows unbounded preceding) as first_val from " +
                                "%s --SPLICE-PROPERTIES useSpark = %s \n order by empno asc", EMP5_REF, useSpark);
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
        // IGNORE NULLS
        String sqlText = format("SELECT employee_id\n" +
                                    "       ,employee_name\n" +
                                    "       ,department\n" +
                                    "       ,LAST_VALUE(commission IGNORE NULLS) OVER (PARTITION BY department\n" +
                                    "                   ORDER BY employee_id DESC\n" +
                                    "                   ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) " +
                                    "Minimum_Commission\n" +
                                    "FROM %s   --SPLICE-PROPERTIES useSpark = %s \n order by employee_id", EMPLOYEES_REF, useSpark);
        ResultSet rs = methodWatcher.executeQuery(sqlText);
        // Verified with example: http://www.techhoney.com/oracle/function/last_value-function-with-partition-by-clause-in-oracle-sql-plsql/
        // DB-4600 - added order by to result to make comparison deterministic
        String expected =
            "EMPLOYEE_ID | EMPLOYEE_NAME |DEPARTMENT |MINIMUM_COMMISSION |\n" +
                "--------------------------------------------------------------\n" +
                "     101     |     Emp A     |   Sales   |        10         |\n" +
                "     102     |     Emp B     |    IT     |        20         |\n" +
                "     103     |     Emp C     |    IT     |        20         |\n" +
                "     104     |     Emp D     |  Support  |         5         |\n" +
                "     105     |     Emp E     |   Sales   |        10         |\n" +
                "     106     |     Emp F     |   Sales   |        10         |\n" +
                "     107     |     Emp G     |   Sales   |        10         |\n" +
                "     108     |     Emp H     |  Support  |         5         |";
        assertEquals("\n"+sqlText+"\n", expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));

        // RESPECT NULLS
        sqlText = format("SELECT employee_id\n" +
                                    "       ,employee_name\n" +
                                    "       ,department\n" +
                                    "       ,LAST_VALUE(commission) OVER (PARTITION BY department\n" +
                                    "                   ORDER BY employee_id DESC\n" +
                                    "                   ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) " +
                                    "Minimum_Commission\n" +
                             "FROM %s   --SPLICE-PROPERTIES useSpark = %s \n order by employee_id", EMPLOYEES_REF, useSpark);
        rs = methodWatcher.executeQuery(sqlText);
        // Verified with example: http://www.techhoney.com/oracle/function/last_value-function-with-partition-by-clause-in-oracle-sql-plsql/
        // DB-4600 - added order by to result to make comparison deterministic
        expected =
            "EMPLOYEE_ID | EMPLOYEE_NAME |DEPARTMENT |MINIMUM_COMMISSION |\n" +
                "--------------------------------------------------------------\n" +
                "     101     |     Emp A     |   Sales   |       NULL        |\n" +
                "     102     |     Emp B     |    IT     |        20         |\n" +
                "     103     |     Emp C     |    IT     |        20         |\n" +
                "     104     |     Emp D     |  Support  |         5         |\n" +
                "     105     |     Emp E     |   Sales   |       NULL        |\n" +
                "     106     |     Emp F     |   Sales   |       NULL        |\n" +
                "     107     |     Emp G     |   Sales   |       NULL        |\n" +
                "     108     |     Emp H     |  Support  |         5         |";
        assertEquals("\n"+sqlText+"\n", expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
        rs.close();
    }

    @Test
    public void testLeadLagOffsetExtrema() throws Exception {
        // DB-3977, DB-3980 - offset extrema
        String sqlText = format("SELECT a, Lead(b,0) OVER(partition by a order by b) FROM %s   --SPLICE-PROPERTIES useSpark = %s \n ", TWOINTS_REF, useSpark);
        try {
            methodWatcher.executeQuery(sqlText);
            fail("Expected exception because lead(b,0) - offset < 1");
        } catch (SQLException e) {
            // expected
            assertEquals("2201Y", e.getSQLState());
        }

        sqlText = format("SELECT a, Lead(b,%s) OVER(partition by a order by b) FROM %s   --SPLICE-PROPERTIES useSpark = %s \n ", Integer.MAX_VALUE, TWOINTS_REF, useSpark);
        try {
            methodWatcher.executeQuery(sqlText);
            fail("Expected exception because lead(b,0) - offset >= Integer.MAX_VALUE");
        } catch (SQLException e) {
            // expected
            assertEquals("2201Y", e.getSQLState());
        }
    }

    @Test
    public void testLeadLagFirstValueLastValueNoOrderBy() throws Exception {
        // DB-3976 - no order by
        String sqlText = format("SELECT a, first_value(b) OVER(partition by a) FROM %s  --SPLICE-PROPERTIES useSpark = %s \n ", TWOINTS_REF, useSpark);
        methodWatcher.executeQuery(sqlText);

        sqlText = format("SELECT a, lag(b) OVER(partition by a) FROM %s  --SPLICE-PROPERTIES useSpark = %s \n ", TWOINTS_REF, useSpark);
        methodWatcher.executeQuery(sqlText);

        // since it row are returned in arbitrary order when no ORDER BY is specified, the best we can do
        // here is to test that we don't get an exception
//        String expected =
//            "A | 2 |\n" +
//                "--------\n" +
//                " 2 |25 |\n" +
//                " 2 |25 |\n" +
//                " 1 |15 |\n" +
//                " 1 |15 |\n" +
//                " 1 |15 |\n" +
//                " 1 |15 |";
//        assertEquals("\n"+sqlText+"\n", expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
    }

    @Test
    public void testLeadLagDefaultValue() throws Exception {
        // DB-3982 - default not implemented
        String sqlText = format("SELECT a, lag(b, 2, 13) OVER(partition by a order by b) FROM %s  --SPLICE-PROPERTIES useSpark = %s \n ", TWOINTS_REF, useSpark);
        try {
            methodWatcher.executeQuery(sqlText);
            fail("Expected exception \"default\" value not implemented.");
        } catch (SQLException e) {
            assertEquals("2202C", e.getSQLState());
        }
    }

    @Test
    public void testNestedFlattening() throws Exception {
        // DB-5082 - ranking function operand referenced during subquery flattening
       String sqlText = format("select " +
                                    "B.PRSN_KEY, " +
                                    "ROWNUM, " +
                                    "PREV_STAGE_NUM  AS STAGING " +
                                    "FROM ( " +
                                    "        select " +
                                    "        A.PRSN_KEY, " +
                                    "         ROW_NUMBER() OVER (PARTITION BY A.PRSN_KEY ORDER BY " +
                                    "MOST_RECENT_STAGING_DATE DESC) \"ROWNUM\", " +
                                    "         LEAD(STAGE_NUM) OVER (PARTITION BY B.PRSN_KEY ORDER BY " +
                                    "MOST_RECENT_STAGING_DATE DESC ) \"PREV_STAGE_NUM\" " +
                                    "        from ( " +
                                    "                select " +
                                    "                PD.PRSN_KEY, " +
                                    "                 -1  as STAGE_NUM, " +
                                    "                pd.MOST_RECENT_STAGING_DATE " +
                                    "                FROM %s PD   --SPLICE-PROPERTIES useSpark = %s \n " +
                                    "        ) A  " +
                                    ")B where ROWNUM = 1", KDW_PAT_SVC_STAGE_REF, useSpark);
        methodWatcher.executeQuery(sqlText);
    }

    //==================================================================================================================
    // Tests for multiple window functions in one query
    //==================================================================================================================

    @Test
    public void testRankDate() throws Exception {
        String sqlText =
            String.format("SELECT hiredate, dept, rank() OVER (partition by dept ORDER BY hiredate) AS rankhire " +
                              "FROM %s  --SPLICE-PROPERTIES useSpark = %s \n ORDER BY hiredate, dept",
                          this.getTableReference(EMPTAB_HIRE_DATE), useSpark);

        ResultSet rs = methodWatcher.executeQuery(sqlText);
        // DB-4600 - added order by to result to make comparison deterministic
        String expected =
            "HIREDATE  |DEPT |RANKHIRE |\n" +
                "----------------------------\n" +
                "2010-03-20 |  1  |    1    |\n" +
                "2010-03-20 |  1  |    1    |\n" +
                "2010-04-12 |  3  |    1    |\n" +
                "2010-08-09 |  3  |    2    |\n" +
                "2011-05-24 |  1  |    3    |\n" +
                "2011-10-15 |  1  |    4    |\n" +
                "2012-04-03 |  1  |    5    |\n" +
                "2012-04-03 |  2  |    1    |\n" +
                "2012-04-03 |  2  |    1    |\n" +
                "2012-04-03 |  3  |    3    |\n" +
                "2012-11-11 |  1  |    6    |\n" +
                "2013-04-24 |  3  |    4    |\n" +
                "2013-06-06 |  2  |    3    |\n" +
                "2013-12-20 |  2  |    4    |\n" +
                "2014-03-04 |  1  |    7    |";
        assertEquals("\n"+sqlText+"\n", expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
        rs.close();
    }
    @Test
    public void testNullsRankDate() throws Exception {
        String sqlText =
            String.format("SELECT hiredate, dept, " +
                              "rank() OVER (partition by dept ORDER BY hiredate) AS rankhire FROM %s  --SPLICE-PROPERTIES useSpark = %s \n  " +
                              "ORDER BY hiredate, dept",
                          this.getTableReference(EMPTAB_NULLS), useSpark);

        ResultSet rs = methodWatcher.executeQuery(sqlText);
        // DB-4600 - added order by to result to make comparison deterministic
        String expected =
            "HIREDATE  |DEPT |RANKHIRE |\n" +
                "----------------------------\n" +
                "2010-03-20 |  1  |    1    |\n" +
                "2010-03-20 |  1  |    1    |\n" +
                "2010-04-12 |  3  |    1    |\n" +
                "2010-08-09 |  1  |    3    |\n" +
                "2010-08-09 |  3  |    2    |\n" +
                "2010-08-09 |  3  |    2    |\n" +
                "2011-05-24 |  1  |    4    |\n" +
                "2011-10-15 |  1  |    5    |\n" +
                "2012-04-03 |  1  |    6    |\n" +
                "2012-04-03 |  2  |    1    |\n" +
                "2012-04-03 |  2  |    1    |\n" +
                "2012-04-03 |  3  |    4    |\n" +
                "2012-11-11 |  1  |    7    |\n" +
                "2013-04-24 |  3  |    5    |\n" +
                "2013-06-06 |  2  |    3    |\n" +
                "2013-12-20 |  2  |    4    |\n" +
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
                              "FROM  %s  --SPLICE-PROPERTIES useSpark = %s \n  ORDER BY EMPNUM",
                          this.getTableReference(EMPTAB_HIRE_DATE), useSpark);

        ResultSet rs = methodWatcher.executeQuery(sqlText);
        // DB-4600 - added order by to result to make comparison deterministic
        String expected =
            "EMPNUM |DEPT |SALARY | DENSERANK |RANK | ROWNUMBER |\n" +
                "----------------------------------------------------\n" +
                "  10   |  1  | 50000 |     7     |  7  |     7     |\n" +
                "  20   |  1  | 75000 |     3     |  3  |     3     |\n" +
                "  30   |  3  | 84000 |     1     |  1  |     1     |\n" +
                "  40   |  2  | 52000 |     2     |  2  |     2     |\n" +
                "  44   |  2  | 52000 |     3     |  3  |     3     |\n" +
                "  49   |  2  | 53000 |     1     |  1  |     1     |\n" +
                "  50   |  1  | 52000 |     5     |  5  |     5     |\n" +
                "  55   |  1  | 52000 |     6     |  6  |     6     |\n" +
                "  60   |  1  | 78000 |     1     |  1  |     1     |\n" +
                "  70   |  1  | 76000 |     2     |  2  |     2     |\n" +
                "  80   |  3  | 79000 |     2     |  2  |     2     |\n" +
                "  90   |  2  | 51000 |     4     |  4  |     4     |\n" +
                "  100  |  3  | 55000 |     4     |  4  |     4     |\n" +
                "  110  |  1  | 53000 |     4     |  4  |     4     |\n" +
                "  120  |  3  | 75000 |     3     |  3  |     3     |";
        assertEquals("\n"+sqlText+"\n", expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
        rs.close();
    }

    //TODO : next version of Spark will support it
    @Test @Ignore
    public void testNullsMultiFunctionSameOverClause() throws Exception {
        String sqlText =
            String.format("SELECT empnum, dept, salary, " +
//                              "DENSE_RANK() OVER (PARTITION BY dept ORDER BY salary desc, empnum) AS DenseRank " +
//                              "RANK() OVER (PARTITION BY dept ORDER BY salary desc, empnum) AS Rank, " +
                              "ROW_NUMBER() OVER (PARTITION BY dept ORDER BY salary desc, empnum) AS RowNumber " +
                              "FROM %s  --SPLICE-PROPERTIES useSpark = %s \n ORDER BY dept, salary, EMPNUM",
                          this.getTableReference(EMPTAB_NULLS), useSpark);

        ResultSet rs = methodWatcher.executeQuery(sqlText);
        // DB-4600 - added order by to result to make comparison deterministic
        String expected =
            "EMPNUM |DEPT |SALARY | DENSERANK |RANK | ROWNUMBER |\n" +
                "----------------------------------------------------\n" +
                "  10   |  1  | 50000 |     8     |  8  |     8     |\n" +
                "  20   |  1  | 75000 |     4     |  4  |     4     |\n" +
                "  30   |  3  | 84000 |     2     |  2  |     2     |\n" +
                "  32   |  1  | NULL  |     1     |  1  |     1     |\n" +
                "  33   |  3  | NULL  |     1     |  1  |     1     |\n" +
                "  40   |  2  | 52000 |     2     |  2  |     2     |\n" +
                "  44   |  2  | 52000 |     3     |  3  |     3     |\n" +
                "  49   |  2  | 53000 |     1     |  1  |     1     |\n" +
                "  50   |  1  | 52000 |     6     |  6  |     6     |\n" +
                "  55   |  1  | 52000 |     7     |  7  |     7     |\n" +
                "  60   |  1  | 78000 |     2     |  2  |     2     |\n" +
                "  70   |  1  | 76000 |     3     |  3  |     3     |\n" +
                "  80   |  3  | 79000 |     3     |  3  |     3     |\n" +
                "  90   |  2  | 51000 |     4     |  4  |     4     |\n" +
                "  100  |  3  | 55000 |     5     |  5  |     5     |\n" +
                "  110  |  1  | 53000 |     5     |  5  |     5     |\n" +
                "  120  |  3  | 75000 |     4     |  4  |     4     |";
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
                              "FROM %s   --SPLICE-PROPERTIES useSpark = %s \n order by hiredate desc, empnum",
                          this.getTableReference(EMPTAB_HIRE_DATE), useSpark);

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
                              "FROM %s   --SPLICE-PROPERTIES useSpark = %s \n order by dept, hiredate",
                          this.getTableReference(EMPTAB_NULLS), useSpark);

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
                              "FROM %s   --SPLICE-PROPERTIES useSpark = %s \n order by hiredate, RowNumber_HireDate_Salary_By_Dept",
                          this.getTableReference(EMPTAB_HIRE_DATE), useSpark);

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
                                           "ROW_NUMBER() OVER (PARTITION BY dept ORDER BY dept, empnum) AS RowNumber FROM %s " +
                                           "  --SPLICE-PROPERTIES useSpark = %s \n  order by empnum",
                                       this.getTableReference(EMPTAB_HIRE_DATE),useSpark);

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
                              "ROW_NUMBER() OVER (PARTITION BY dept ORDER BY dept, empnum) AS RowNumber FROM %s   --SPLICE-PROPERTIES useSpark = %s \n order by dept, empnum",
                          this.getTableReference(EMPTAB_NULLS), useSpark);

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
                              "FROM %s   --SPLICE-PROPERTIES useSpark = %s \n order by salary, empnum",
                          this.getTableReference(EMPTAB_HIRE_DATE), useSpark);
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
                              "ROW_NUMBER() OVER (ORDER BY salary desc, dept) AS RowNumber, " +
                              "RANK() OVER (ORDER BY salary desc, dept) AS Rank, " +
                              "DENSE_RANK() OVER (ORDER BY salary desc, dept) AS DenseRank " +
                              "FROM %s   --SPLICE-PROPERTIES useSpark = %s \n order by salary desc, dept",
                          this.getTableReference(EMPTAB_HIRE_DATE), useSpark);

        ResultSet rs = methodWatcher.executeQuery(sqlText);
        // Validated with PostgreSQL app
        String expected =
            "SALARY |DEPT | ROWNUMBER |RANK | DENSERANK |\n" +
                "--------------------------------------------\n" +
                " 84000 |  3  |     1     |  1  |     1     |\n" +
                " 79000 |  3  |     2     |  2  |     2     |\n" +
                " 78000 |  1  |     3     |  3  |     3     |\n" +
                " 76000 |  1  |     4     |  4  |     4     |\n" +
                " 75000 |  1  |     5     |  5  |     5     |\n" +
                " 75000 |  3  |     6     |  6  |     6     |\n" +
                " 55000 |  3  |     7     |  7  |     7     |\n" +
                " 53000 |  1  |     8     |  8  |     8     |\n" +
                " 53000 |  2  |     9     |  9  |     9     |\n" +
                " 52000 |  1  |    10     | 10  |    10     |\n" +
                " 52000 |  1  |    11     | 10  |    10     |\n" +
                " 52000 |  2  |    12     | 12  |    11     |\n" +
                " 52000 |  2  |    13     | 12  |    11     |\n" +
                " 51000 |  2  |    14     | 14  |    12     |\n" +
                " 50000 |  1  |    15     | 15  |    13     |";
        assertEquals("\n"+sqlText+"\n", expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
        rs.close();
    }

    // TODO : Spark will support that next version
    @Test @Ignore
    public void testNullsMultiFunctionInQueryDiffAndSameOverClause() throws Exception {
        String sqlText =
            String.format("SELECT salary, dept, " +
                              "ROW_NUMBER() OVER (ORDER BY salary desc, dept) AS RowNumber, " +
                              "RANK() OVER (ORDER BY salary desc, dept) AS Rank, " +
                              "DENSE_RANK() OVER (ORDER BY salary desc, dept) AS DenseRank " +
                              "FROM %s   --SPLICE-PROPERTIES useSpark = %s \n  order by salary desc, dept",
                          this.getTableReference(EMPTAB_NULLS), useSpark);

        ResultSet rs = methodWatcher.executeQuery(sqlText);
        // Validated in PostgreSQL app
        String expected =
            "SALARY |DEPT | ROWNUMBER |RANK | DENSERANK |\n" +
                "--------------------------------------------\n" +
                " NULL  |  1  |     1     |  1  |     1     |\n" +
                " NULL  |  3  |     2     |  2  |     2     |\n" +
                " 84000 |  3  |     3     |  3  |     3     |\n" +
                " 79000 |  3  |     4     |  4  |     4     |\n" +
                " 78000 |  1  |     5     |  5  |     5     |\n" +
                " 76000 |  1  |     6     |  6  |     6     |\n" +
                " 75000 |  1  |     7     |  7  |     7     |\n" +
                " 75000 |  3  |     8     |  8  |     8     |\n" +
                " 55000 |  3  |     9     |  9  |     9     |\n" +
                " 53000 |  1  |    10     | 10  |    10     |\n" +
                " 53000 |  2  |    11     | 11  |    11     |\n" +
                " 52000 |  1  |    12     | 12  |    12     |\n" +
                " 52000 |  1  |    13     | 12  |    12     |\n" +
                " 52000 |  2  |    14     | 14  |    13     |\n" +
                " 52000 |  2  |    15     | 14  |    13     |\n" +
                " 51000 |  2  |    16     | 16  |    14     |\n" +
                " 50000 |  1  |    17     | 17  |    15     |";
        assertEquals("\n"+sqlText+"\n", expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
        rs.close();
    }

    @Test
    public void testPullFunctionInputColumnUp4Levels() throws Exception {
        // DB-2087 - Kryo exception
        String sqlText =
            String.format("select Transaction_Detail5.SOURCE_SALES_INSTANCE_ID C0, " +
                              "min(Transaction_Detail5.TRANSACTION_DT) over (partition by Transaction_Detail5.ORIGINAL_SKU_CATEGORY_ID) C1, " +
                              "sum(Transaction_Detail5.SALES_AMT) over (partition by Transaction_Detail5.TRANSACTION_DT) C10 " +
                              "from %s AS Transaction_Detail5   --SPLICE-PROPERTIES useSpark = %s \n  " +
                              "where Transaction_Detail5.TRANSACTION_DT between DATE('2010-01-21') " +
                              "and DATE('2013-11-21') and Transaction_Detail5.CUSTOMER_MASTER_ID=74065939",
                          this.getTableReference(TXN_DETAIL), useSpark);

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
                              "FROM %s\n   --SPLICE-PROPERTIES useSpark = %s \n " +
                              "WHERE amount IS NOT NULL\n" +
                              "GROUP BY prd_type_id\n" +
                              "ORDER BY prd_type_id", this.getTableReference(ALL_SALES), useSpark);

        ResultSet rs = methodWatcher.executeQuery(sqlText);
        String expected =
            "PRD_TYPE_ID |    2     |RANK |DENSE_RANK |\n" +
                "-------------------------------------------\n" +
                "      1      |227276.50 |  1  |     1     |\n" +
                "      2      |223927.08 |  2  |     2     |";
        assertEquals("\n"+sqlText+"\n", expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
        rs.close();
    }

    @Test
    public void testNestedAggregateFunction() throws Exception {
        // SPLICE-969 -  Test a simple use case of nested aggregate within a window functions
        String sqlText =
                String.format("select  sum(sum(b)) over (partition by b)  as revenue\n" +
                        "from %s\n" +
                        "group by c order by revenue", this.getTableReference(NESTED_AGGREGATION_WF));

        ResultSet rs = methodWatcher.executeQuery(sqlText);
        String expected =
                "REVENUE |\n" +
                        "----------\n" +
                        "    4    |\n" +
                        "   40    |";
        assertEquals("\n"+sqlText+"\n", expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
        rs.close();
    }

    @Test
    public void testNestedAggregatWithEmptyTableFunction() throws Exception {
        // SPLICE-969 -  Test a simple use case of nested aggregate within a window functions
        String sqlText =
                String.format("select  sum(sum(b)) over (partition by b)  as revenue\n" +
                        "from %s\n" +
                        "group by  c order by revenue", this.getTableReference(EMPTY_TABLE));

        ResultSet rs = methodWatcher.executeQuery(sqlText);
        String expected = "";
        assertEquals("\n"+sqlText+"\n", expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
        rs.close();
    }

    @Test
    public void testWindowColFromSubqry_1() throws Exception {
        // SPLICE_1005: NPE with window function when used with the subquery aggregate column
        String sqlText =
                String.format("SELECT rank_col, rank() over (order by rank_col asc) rnk \n" +
                                "from (select dept dept_sk, avg(salary) rank_col \n" +
                                "   from %s ss1 --SPLICE-PROPERTIES useSpark = %s \n" +
                                "   group by dept)V1",
                        this.getTableReference(EMPTAB),useSpark);

        ResultSet rs = methodWatcher.executeQuery(sqlText);
        String expected =
                "RANK_COL | RNK |\n" +
                "----------------\n" +
                "  52000  |  1  |\n" +
                "  62285  |  2  |\n" +
                "  73250  |  3  |";

        assertEquals("\n"+sqlText+"\n", expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
        rs.close();
    }

    @Test
    public void testWindowColFromSubqry_2() throws Exception {
        // SPLICE_1005: NPE with window function when used with the subquery aggregate column
        String sqlText =
                String.format(
                        "select rank() over (order by rnk asc) new_rnk \n" +
                        "  from (select rank() over (order by rank_col asc) rnk \n" +
                        "    from (select dept dept_sk, avg(salary) rank_col \n" +
                        "      from %s ss1 --SPLICE-PROPERTIES useSpark = %s \n" +
                        "      group by dept) V1) V2",
                        this.getTableReference(EMPTAB),useSpark);

        ResultSet rs = methodWatcher.executeQuery(sqlText);
        String expected =
                "NEW_RNK |\n" +
                "----------\n" +
                "    1    |\n" +
                "    2    |\n" +
                "    3    |";

        assertEquals("\n"+sqlText+"\n", expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
        rs.close();
    }

}
