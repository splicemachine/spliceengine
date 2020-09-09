/*
 * Copyright (c) 2012 - 2020 Splice Machine, Inc.
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

import com.splicemachine.derby.test.framework.*;
import com.splicemachine.homeless.TestUtils;
import com.splicemachine.test.LongerThanTwoMinutes;
import com.splicemachine.test_dao.TableDAO;
import com.splicemachine.test_tools.TableCreator;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import splice.com.google.common.collect.Lists;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collection;

import static com.splicemachine.test_tools.Rows.row;
import static com.splicemachine.test_tools.Rows.rows;
import static org.junit.Assert.*;

/**
 *
 * Created by jyuan on 7/30/14.
 */
@SuppressWarnings("unchecked")
@RunWith(Parameterized.class)
@Category(LongerThanTwoMinutes.class)
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

    private static String tableADef = "(c1 int, c2 int, c3 int)";
    private static final String tableA = "A";
    private static SpliceTableWatcher tableAWatcher = new SpliceTableWatcher(tableA, SCHEMA, tableADef);

    private static String tableBDef = "(a1 bigint generated always as identity (start with 1, increment by 1),\n" +
                                      "                 b1 timestamp)";
    private static final String tableB = "B";
    private static SpliceTableWatcher tableBWatcher = new SpliceTableWatcher(tableB, SCHEMA, tableBDef);

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
            row(25, "Jack", null, 5), row(26, "Tom", null, 5), row(27, "Apple", null, 5), row(28, "Dan", 15000, 5), row(29, "Ken", null, 5),
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
            .around(nestedAggregateEmptyWatcher)
            .around(tableAWatcher)
            .around(new SpliceDataWatcher() {
                @Override
                protected void starting(Description description) {
                    PreparedStatement ps;
                    try {
                        ps = spliceClassWatcher.prepareStatement(
                                String.format("insert into %s values (?,?,?)", tableAWatcher));
                        for (int i = 0; i < 200; ++i) {
                            for (int j = 0; j < 90; ++j) {
                                ps.setInt(1, i);
                                ps.setInt(2, j);
                                ps.setInt(3, 10);
                                ps.addBatch();
                            }
                        }
                        ps.executeBatch();
                    }
                    catch (Exception e) {

                    }
                }
            })
            .around(tableBWatcher)
            .around(new SpliceDataWatcher() {
        @Override
        protected void starting(Description description) {
            PreparedStatement ps;
            try {
                String sqlText = String.format("insert into %s(b1) values(timestamp(CURRENT_DATE))", tableBWatcher);
                methodWatcher.executeUpdate(sqlText);
                sqlText = String.format("insert into %s(b1) select b1 from %s", tableBWatcher, tableBWatcher);

                for (int i = 0; i < 8; ++i) {
                    methodWatcher.executeUpdate(sqlText);
                }
            }
            catch (Exception e) {
                fail("Problem setting up table B.");
            }
        }
    });

    @ClassRule
    public static SpliceWatcher methodWatcher = new SpliceWatcher();


    @Test
    public void testSumWithMoreThan101Rows() throws Exception {
        String sqlText =
        String.format("select rn, sum(rn) over (order by rn) sm from (select a1 rn from %s --SPLICE-PROPERTIES useSpark=%s \n" +
        " ) a", this.getTableReference(tableB),useSpark);

        ResultSet rs = methodWatcher.executeQuery(sqlText);

        String expected =
        "RN  | SM   |\n" +
        "------------\n" +
        " 1  |  1   |\n" +
        " 2  |  3   |\n" +
        " 3  |  6   |\n" +
        " 4  | 10   |\n" +
        " 5  | 15   |\n" +
        " 6  | 21   |\n" +
        " 7  | 28   |\n" +
        " 8  | 36   |\n" +
        " 9  | 45   |\n" +
        "10  | 55   |\n" +
        "11  | 66   |\n" +
        "12  | 78   |\n" +
        "13  | 91   |\n" +
        "14  | 105  |\n" +
        "15  | 120  |\n" +
        "16  | 136  |\n" +
        "17  | 153  |\n" +
        "18  | 171  |\n" +
        "19  | 190  |\n" +
        "20  | 210  |\n" +
        "21  | 231  |\n" +
        "22  | 253  |\n" +
        "23  | 276  |\n" +
        "24  | 300  |\n" +
        "25  | 325  |\n" +
        "26  | 351  |\n" +
        "27  | 378  |\n" +
        "28  | 406  |\n" +
        "29  | 435  |\n" +
        "30  | 465  |\n" +
        "31  | 496  |\n" +
        "32  | 528  |\n" +
        "33  | 561  |\n" +
        "34  | 595  |\n" +
        "35  | 630  |\n" +
        "36  | 666  |\n" +
        "37  | 703  |\n" +
        "38  | 741  |\n" +
        "39  | 780  |\n" +
        "40  | 820  |\n" +
        "41  | 861  |\n" +
        "42  | 903  |\n" +
        "43  | 946  |\n" +
        "44  | 990  |\n" +
        "45  |1035  |\n" +
        "46  |1081  |\n" +
        "47  |1128  |\n" +
        "48  |1176  |\n" +
        "49  |1225  |\n" +
        "50  |1275  |\n" +
        "51  |1326  |\n" +
        "52  |1378  |\n" +
        "53  |1431  |\n" +
        "54  |1485  |\n" +
        "55  |1540  |\n" +
        "56  |1596  |\n" +
        "57  |1653  |\n" +
        "58  |1711  |\n" +
        "59  |1770  |\n" +
        "60  |1830  |\n" +
        "61  |1891  |\n" +
        "62  |1953  |\n" +
        "63  |2016  |\n" +
        "64  |2080  |\n" +
        "65  |2145  |\n" +
        "66  |2211  |\n" +
        "67  |2278  |\n" +
        "68  |2346  |\n" +
        "69  |2415  |\n" +
        "70  |2485  |\n" +
        "71  |2556  |\n" +
        "72  |2628  |\n" +
        "73  |2701  |\n" +
        "74  |2775  |\n" +
        "75  |2850  |\n" +
        "76  |2926  |\n" +
        "77  |3003  |\n" +
        "78  |3081  |\n" +
        "79  |3160  |\n" +
        "80  |3240  |\n" +
        "81  |3321  |\n" +
        "82  |3403  |\n" +
        "83  |3486  |\n" +
        "84  |3570  |\n" +
        "85  |3655  |\n" +
        "86  |3741  |\n" +
        "87  |3828  |\n" +
        "88  |3916  |\n" +
        "89  |4005  |\n" +
        "90  |4095  |\n" +
        "91  |4186  |\n" +
        "92  |4278  |\n" +
        "93  |4371  |\n" +
        "94  |4465  |\n" +
        "95  |4560  |\n" +
        "96  |4656  |\n" +
        "97  |4753  |\n" +
        "98  |4851  |\n" +
        "99  |4950  |\n" +
        "100 |5050  |\n" +
        "101 |5151  |\n" +
        "102 |5253  |\n" +
        "103 |5356  |\n" +
        "104 |5460  |\n" +
        "105 |5565  |\n" +
        "106 |5671  |\n" +
        "107 |5778  |\n" +
        "108 |5886  |\n" +
        "109 |5995  |\n" +
        "110 |6105  |\n" +
        "111 |6216  |\n" +
        "112 |6328  |\n" +
        "113 |6441  |\n" +
        "114 |6555  |\n" +
        "115 |6670  |\n" +
        "116 |6786  |\n" +
        "117 |6903  |\n" +
        "118 |7021  |\n" +
        "119 |7140  |\n" +
        "120 |7260  |\n" +
        "121 |7381  |\n" +
        "122 |7503  |\n" +
        "123 |7626  |\n" +
        "124 |7750  |\n" +
        "125 |7875  |\n" +
        "126 |8001  |\n" +
        "127 |8128  |\n" +
        "128 |8256  |\n" +
        "129 |8385  |\n" +
        "130 |8515  |\n" +
        "131 |8646  |\n" +
        "132 |8778  |\n" +
        "133 |8911  |\n" +
        "134 |9045  |\n" +
        "135 |9180  |\n" +
        "136 |9316  |\n" +
        "137 |9453  |\n" +
        "138 |9591  |\n" +
        "139 |9730  |\n" +
        "140 |9870  |\n" +
        "141 |10011 |\n" +
        "142 |10153 |\n" +
        "143 |10296 |\n" +
        "144 |10440 |\n" +
        "145 |10585 |\n" +
        "146 |10731 |\n" +
        "147 |10878 |\n" +
        "148 |11026 |\n" +
        "149 |11175 |\n" +
        "150 |11325 |\n" +
        "151 |11476 |\n" +
        "152 |11628 |\n" +
        "153 |11781 |\n" +
        "154 |11935 |\n" +
        "155 |12090 |\n" +
        "156 |12246 |\n" +
        "157 |12403 |\n" +
        "158 |12561 |\n" +
        "159 |12720 |\n" +
        "160 |12880 |\n" +
        "161 |13041 |\n" +
        "162 |13203 |\n" +
        "163 |13366 |\n" +
        "164 |13530 |\n" +
        "165 |13695 |\n" +
        "166 |13861 |\n" +
        "167 |14028 |\n" +
        "168 |14196 |\n" +
        "169 |14365 |\n" +
        "170 |14535 |\n" +
        "171 |14706 |\n" +
        "172 |14878 |\n" +
        "173 |15051 |\n" +
        "174 |15225 |\n" +
        "175 |15400 |\n" +
        "176 |15576 |\n" +
        "177 |15753 |\n" +
        "178 |15931 |\n" +
        "179 |16110 |\n" +
        "180 |16290 |\n" +
        "181 |16471 |\n" +
        "182 |16653 |\n" +
        "183 |16836 |\n" +
        "184 |17020 |\n" +
        "185 |17205 |\n" +
        "186 |17391 |\n" +
        "187 |17578 |\n" +
        "188 |17766 |\n" +
        "189 |17955 |\n" +
        "190 |18145 |\n" +
        "191 |18336 |\n" +
        "192 |18528 |\n" +
        "193 |18721 |\n" +
        "194 |18915 |\n" +
        "195 |19110 |\n" +
        "196 |19306 |\n" +
        "197 |19503 |\n" +
        "198 |19701 |\n" +
        "199 |19900 |\n" +
        "200 |20100 |\n" +
        "201 |20301 |\n" +
        "202 |20503 |\n" +
        "203 |20706 |\n" +
        "204 |20910 |\n" +
        "205 |21115 |\n" +
        "206 |21321 |\n" +
        "207 |21528 |\n" +
        "208 |21736 |\n" +
        "209 |21945 |\n" +
        "210 |22155 |\n" +
        "211 |22366 |\n" +
        "212 |22578 |\n" +
        "213 |22791 |\n" +
        "214 |23005 |\n" +
        "215 |23220 |\n" +
        "216 |23436 |\n" +
        "217 |23653 |\n" +
        "218 |23871 |\n" +
        "219 |24090 |\n" +
        "220 |24310 |\n" +
        "221 |24531 |\n" +
        "222 |24753 |\n" +
        "223 |24976 |\n" +
        "224 |25200 |\n" +
        "225 |25425 |\n" +
        "226 |25651 |\n" +
        "227 |25878 |\n" +
        "228 |26106 |\n" +
        "229 |26335 |\n" +
        "230 |26565 |\n" +
        "231 |26796 |\n" +
        "232 |27028 |\n" +
        "233 |27261 |\n" +
        "234 |27495 |\n" +
        "235 |27730 |\n" +
        "236 |27966 |\n" +
        "237 |28203 |\n" +
        "238 |28441 |\n" +
        "239 |28680 |\n" +
        "240 |28920 |\n" +
        "241 |29161 |\n" +
        "242 |29403 |\n" +
        "243 |29646 |\n" +
        "244 |29890 |\n" +
        "245 |30135 |\n" +
        "246 |30381 |\n" +
        "247 |30628 |\n" +
        "248 |30876 |\n" +
        "249 |31125 |\n" +
        "250 |31375 |\n" +
        "251 |31626 |\n" +
        "252 |31878 |\n" +
        "253 |32131 |\n" +
        "254 |32385 |\n" +
        "255 |32640 |\n" +
        "256 |32896 |";

        assertEquals("\n"+sqlText+"\n", expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
        rs.close();

        String.format("select rn, sum(rn) over (order by rn) sm from (select row_number() over (order by a1) rn from %s --SPLICE-PROPERTIES useSpark=%s \n" +
        " ) a", this.getTableReference(tableB),useSpark);

        rs = methodWatcher.executeQuery(sqlText);
        assertEquals("\n"+sqlText+"\n", expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
        rs.close();
    }
    @Test
    public void testSumWithMoreThan101RowsAndExpressionInOverClause() throws Exception {
        String sqlText =
        String.format("select rn, sum(rn) over (partition by rn/10 order by rn) sm from (select a1 rn from %s --SPLICE-PROPERTIES useSpark=%s \n" +
        " ) a order by rn", this.getTableReference(tableB),useSpark);

        ResultSet rs = methodWatcher.executeQuery(sqlText);

        String expected =
        "RN  | SM  |\n" +
        "-----------\n" +
        " 1  |  1  |\n" +
        " 2  |  3  |\n" +
        " 3  |  6  |\n" +
        " 4  | 10  |\n" +
        " 5  | 15  |\n" +
        " 6  | 21  |\n" +
        " 7  | 28  |\n" +
        " 8  | 36  |\n" +
        " 9  | 45  |\n" +
        "10  | 10  |\n" +
        "11  | 21  |\n" +
        "12  | 33  |\n" +
        "13  | 46  |\n" +
        "14  | 60  |\n" +
        "15  | 75  |\n" +
        "16  | 91  |\n" +
        "17  | 108 |\n" +
        "18  | 126 |\n" +
        "19  | 145 |\n" +
        "20  | 20  |\n" +
        "21  | 41  |\n" +
        "22  | 63  |\n" +
        "23  | 86  |\n" +
        "24  | 110 |\n" +
        "25  | 135 |\n" +
        "26  | 161 |\n" +
        "27  | 188 |\n" +
        "28  | 216 |\n" +
        "29  | 245 |\n" +
        "30  | 30  |\n" +
        "31  | 61  |\n" +
        "32  | 93  |\n" +
        "33  | 126 |\n" +
        "34  | 160 |\n" +
        "35  | 195 |\n" +
        "36  | 231 |\n" +
        "37  | 268 |\n" +
        "38  | 306 |\n" +
        "39  | 345 |\n" +
        "40  | 40  |\n" +
        "41  | 81  |\n" +
        "42  | 123 |\n" +
        "43  | 166 |\n" +
        "44  | 210 |\n" +
        "45  | 255 |\n" +
        "46  | 301 |\n" +
        "47  | 348 |\n" +
        "48  | 396 |\n" +
        "49  | 445 |\n" +
        "50  | 50  |\n" +
        "51  | 101 |\n" +
        "52  | 153 |\n" +
        "53  | 206 |\n" +
        "54  | 260 |\n" +
        "55  | 315 |\n" +
        "56  | 371 |\n" +
        "57  | 428 |\n" +
        "58  | 486 |\n" +
        "59  | 545 |\n" +
        "60  | 60  |\n" +
        "61  | 121 |\n" +
        "62  | 183 |\n" +
        "63  | 246 |\n" +
        "64  | 310 |\n" +
        "65  | 375 |\n" +
        "66  | 441 |\n" +
        "67  | 508 |\n" +
        "68  | 576 |\n" +
        "69  | 645 |\n" +
        "70  | 70  |\n" +
        "71  | 141 |\n" +
        "72  | 213 |\n" +
        "73  | 286 |\n" +
        "74  | 360 |\n" +
        "75  | 435 |\n" +
        "76  | 511 |\n" +
        "77  | 588 |\n" +
        "78  | 666 |\n" +
        "79  | 745 |\n" +
        "80  | 80  |\n" +
        "81  | 161 |\n" +
        "82  | 243 |\n" +
        "83  | 326 |\n" +
        "84  | 410 |\n" +
        "85  | 495 |\n" +
        "86  | 581 |\n" +
        "87  | 668 |\n" +
        "88  | 756 |\n" +
        "89  | 845 |\n" +
        "90  | 90  |\n" +
        "91  | 181 |\n" +
        "92  | 273 |\n" +
        "93  | 366 |\n" +
        "94  | 460 |\n" +
        "95  | 555 |\n" +
        "96  | 651 |\n" +
        "97  | 748 |\n" +
        "98  | 846 |\n" +
        "99  | 945 |\n" +
        "100 | 100 |\n" +
        "101 | 201 |\n" +
        "102 | 303 |\n" +
        "103 | 406 |\n" +
        "104 | 510 |\n" +
        "105 | 615 |\n" +
        "106 | 721 |\n" +
        "107 | 828 |\n" +
        "108 | 936 |\n" +
        "109 |1045 |\n" +
        "110 | 110 |\n" +
        "111 | 221 |\n" +
        "112 | 333 |\n" +
        "113 | 446 |\n" +
        "114 | 560 |\n" +
        "115 | 675 |\n" +
        "116 | 791 |\n" +
        "117 | 908 |\n" +
        "118 |1026 |\n" +
        "119 |1145 |\n" +
        "120 | 120 |\n" +
        "121 | 241 |\n" +
        "122 | 363 |\n" +
        "123 | 486 |\n" +
        "124 | 610 |\n" +
        "125 | 735 |\n" +
        "126 | 861 |\n" +
        "127 | 988 |\n" +
        "128 |1116 |\n" +
        "129 |1245 |\n" +
        "130 | 130 |\n" +
        "131 | 261 |\n" +
        "132 | 393 |\n" +
        "133 | 526 |\n" +
        "134 | 660 |\n" +
        "135 | 795 |\n" +
        "136 | 931 |\n" +
        "137 |1068 |\n" +
        "138 |1206 |\n" +
        "139 |1345 |\n" +
        "140 | 140 |\n" +
        "141 | 281 |\n" +
        "142 | 423 |\n" +
        "143 | 566 |\n" +
        "144 | 710 |\n" +
        "145 | 855 |\n" +
        "146 |1001 |\n" +
        "147 |1148 |\n" +
        "148 |1296 |\n" +
        "149 |1445 |\n" +
        "150 | 150 |\n" +
        "151 | 301 |\n" +
        "152 | 453 |\n" +
        "153 | 606 |\n" +
        "154 | 760 |\n" +
        "155 | 915 |\n" +
        "156 |1071 |\n" +
        "157 |1228 |\n" +
        "158 |1386 |\n" +
        "159 |1545 |\n" +
        "160 | 160 |\n" +
        "161 | 321 |\n" +
        "162 | 483 |\n" +
        "163 | 646 |\n" +
        "164 | 810 |\n" +
        "165 | 975 |\n" +
        "166 |1141 |\n" +
        "167 |1308 |\n" +
        "168 |1476 |\n" +
        "169 |1645 |\n" +
        "170 | 170 |\n" +
        "171 | 341 |\n" +
        "172 | 513 |\n" +
        "173 | 686 |\n" +
        "174 | 860 |\n" +
        "175 |1035 |\n" +
        "176 |1211 |\n" +
        "177 |1388 |\n" +
        "178 |1566 |\n" +
        "179 |1745 |\n" +
        "180 | 180 |\n" +
        "181 | 361 |\n" +
        "182 | 543 |\n" +
        "183 | 726 |\n" +
        "184 | 910 |\n" +
        "185 |1095 |\n" +
        "186 |1281 |\n" +
        "187 |1468 |\n" +
        "188 |1656 |\n" +
        "189 |1845 |\n" +
        "190 | 190 |\n" +
        "191 | 381 |\n" +
        "192 | 573 |\n" +
        "193 | 766 |\n" +
        "194 | 960 |\n" +
        "195 |1155 |\n" +
        "196 |1351 |\n" +
        "197 |1548 |\n" +
        "198 |1746 |\n" +
        "199 |1945 |\n" +
        "200 | 200 |\n" +
        "201 | 401 |\n" +
        "202 | 603 |\n" +
        "203 | 806 |\n" +
        "204 |1010 |\n" +
        "205 |1215 |\n" +
        "206 |1421 |\n" +
        "207 |1628 |\n" +
        "208 |1836 |\n" +
        "209 |2045 |\n" +
        "210 | 210 |\n" +
        "211 | 421 |\n" +
        "212 | 633 |\n" +
        "213 | 846 |\n" +
        "214 |1060 |\n" +
        "215 |1275 |\n" +
        "216 |1491 |\n" +
        "217 |1708 |\n" +
        "218 |1926 |\n" +
        "219 |2145 |\n" +
        "220 | 220 |\n" +
        "221 | 441 |\n" +
        "222 | 663 |\n" +
        "223 | 886 |\n" +
        "224 |1110 |\n" +
        "225 |1335 |\n" +
        "226 |1561 |\n" +
        "227 |1788 |\n" +
        "228 |2016 |\n" +
        "229 |2245 |\n" +
        "230 | 230 |\n" +
        "231 | 461 |\n" +
        "232 | 693 |\n" +
        "233 | 926 |\n" +
        "234 |1160 |\n" +
        "235 |1395 |\n" +
        "236 |1631 |\n" +
        "237 |1868 |\n" +
        "238 |2106 |\n" +
        "239 |2345 |\n" +
        "240 | 240 |\n" +
        "241 | 481 |\n" +
        "242 | 723 |\n" +
        "243 | 966 |\n" +
        "244 |1210 |\n" +
        "245 |1455 |\n" +
        "246 |1701 |\n" +
        "247 |1948 |\n" +
        "248 |2196 |\n" +
        "249 |2445 |\n" +
        "250 | 250 |\n" +
        "251 | 501 |\n" +
        "252 | 753 |\n" +
        "253 |1006 |\n" +
        "254 |1260 |\n" +
        "255 |1515 |\n" +
        "256 |1771 |";

        assertEquals("\n"+sqlText+"\n", expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
        rs.close();

        String.format("select rn, sum(rn) over (order by rn) sm from (select row_number() over (order by a1) rn from %s --SPLICE-PROPERTIES useSpark=%s \n" +
        " ) a", this.getTableReference(tableB),useSpark);

        rs = methodWatcher.executeQuery(sqlText);
        assertEquals("\n"+sqlText+"\n", expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
        rs.close();
    }

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

        sqlText =
        String.format("SELECT empnum,dept,salary," +
        "min(salary) over (Partition by CASE WHEN dept=1 then 1 else 2 END ORDER BY salary ROWS 2 preceding) as minsal " +
        "from %s --SPLICE-PROPERTIES useSpark = %s \n order by empnum",
        this.getTableReference(EMPTAB),useSpark);

        rs = methodWatcher.executeQuery(sqlText);

        expected =
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
        "  100  |  3  | 55000 | 52000 |\n" +
        "  110  |  1  | 53000 | 52000 |\n" +
        "  120  |  3  | 75000 | 53000 |";

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

        // The following makes the window size 1 row.
        sqlText =
        String.format("SELECT empnum,dept,salary," +
        "max(salary) over (Partition by ln(ABS(salary/1000)) ORDER BY salary, empnum ROWS BETWEEN CURRENT ROW AND 2 FOLLOWING ) as maxsal " +
        "from %s --SPLICE-PROPERTIES useSpark = %s \n  order by empnum",
        this.getTableReference(EMPTAB),useSpark);

        rs = methodWatcher.executeQuery(sqlText);

        expected =
        "EMPNUM |DEPT |SALARY |MAXSAL |\n" +
        "------------------------------\n" +
        "  10   |  1  | 50000 | 50000 |\n" +
        "  20   |  1  | 75000 | 75000 |\n" +
        "  30   |  3  | 84000 | 84000 |\n" +
        "  40   |  2  | 52000 | 52000 |\n" +
        "  44   |  2  | 52000 | 52000 |\n" +
        "  49   |  2  | 53000 | 53000 |\n" +
        "  50   |  1  | 52000 | 52000 |\n" +
        "  55   |  1  | 52000 | 52000 |\n" +
        "  60   |  1  | 78000 | 78000 |\n" +
        "  70   |  1  | 76000 | 76000 |\n" +
        "  80   |  3  | 79000 | 79000 |\n" +
        "  90   |  2  | 51000 | 51000 |\n" +
        "  100  |  3  | 55000 | 55000 |\n" +
        "  110  |  1  | 53000 | 53000 |\n" +
        "  120  |  3  | 75000 | 75000 |";
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

        sqlText =
        String.format("SELECT empnum, dept, salary, " +
        "max(salary) over (Partition by substr(cast (empnum as CHAR(7)),1,1) ORDER BY salary, empnum rows between unbounded preceding and unbounded following) as maxsal " +
        "from %s  --SPLICE-PROPERTIES useSpark = %s \n order by empnum",
        this.getTableReference(EMPTAB),useSpark);

        rs = methodWatcher.executeQuery(sqlText);
        expected =
        "EMPNUM |DEPT |SALARY |MAXSAL |\n" +
        "------------------------------\n" +
        "  10   |  1  | 50000 | 75000 |\n" +
        "  20   |  1  | 75000 | 75000 |\n" +
        "  30   |  3  | 84000 | 84000 |\n" +
        "  40   |  2  | 52000 | 53000 |\n" +
        "  44   |  2  | 52000 | 53000 |\n" +
        "  49   |  2  | 53000 | 53000 |\n" +
        "  50   |  1  | 52000 | 52000 |\n" +
        "  55   |  1  | 52000 | 52000 |\n" +
        "  60   |  1  | 78000 | 78000 |\n" +
        "  70   |  1  | 76000 | 76000 |\n" +
        "  80   |  3  | 79000 | 79000 |\n" +
        "  90   |  2  | 51000 | 51000 |\n" +
        "  100  |  3  | 55000 | 75000 |\n" +
        "  110  |  1  | 53000 | 75000 |\n" +
        "  120  |  3  | 75000 | 75000 |";
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

        sqlText =
        String.format("SELECT empnum, dept, salary, " +
        "max(salary) over (Partition by MOD(cast ((trim(cast(empnum as char(7))) || trim(cast(salary as char(5))) || trim(cast(dept as char(1)))) as int),3) ORDER BY salary rows between current row and unbounded following) as maxsal " +
        "from %s --SPLICE-PROPERTIES useSpark = %s \n order by empnum",
        this.getTableReference(EMPTAB),useSpark);

        rs = methodWatcher.executeQuery(sqlText);
        expected =
        "EMPNUM |DEPT |SALARY |MAXSAL |\n" +
        "------------------------------\n" +
        "  10   |  1  | 50000 | 78000 |\n" +
        "  20   |  1  | 75000 | 84000 |\n" +
        "  30   |  3  | 84000 | 84000 |\n" +
        "  40   |  2  | 52000 | 78000 |\n" +
        "  44   |  2  | 52000 | 55000 |\n" +
        "  49   |  2  | 53000 | 55000 |\n" +
        "  50   |  1  | 52000 | 78000 |\n" +
        "  55   |  1  | 52000 | 84000 |\n" +
        "  60   |  1  | 78000 | 78000 |\n" +
        "  70   |  1  | 76000 | 84000 |\n" +
        "  80   |  3  | 79000 | 84000 |\n" +
        "  90   |  2  | 51000 | 55000 |\n" +
        "  100  |  3  | 55000 | 55000 |\n" +
        "  110  |  1  | 53000 | 55000 |\n" +
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

        sqlText =
        String.format("select PersonID,FamilyID,FirstName,LastName, ROW_NUMBER() over (partition by length(FirstName) order by DOB) as Number, dob " +
        "from %s  --SPLICE-PROPERTIES useSpark = %s \n order by PersonID",
        this.getTableReference(PEOPLE), useSpark);

        rs = methodWatcher.executeQuery(sqlText);
        expected =
        "PERSONID |FAMILYID | FIRSTNAME |LASTNAME |NUMBER |         DOB          |\n" +
        "-------------------------------------------------------------------------\n" +
        "    1    |    1    |    Joe    | Johnson |   1   |2000-10-23 13:00:00.0 |\n" +
        "    1    |    1    |    Joe    | Johnson |   2   |2000-10-23 13:00:00.0 |\n" +
        "    1    |    1    |    Joe    | Johnson |   3   |2000-10-23 13:00:00.0 |\n" +
        "    2    |    1    |    Jim    | Johnson |   6   |2001-12-15 05:45:00.0 |\n" +
        "    2    |    1    |    Jim    | Johnson |   7   |2001-12-15 05:45:00.0 |\n" +
        "    2    |    1    |    Jim    | Johnson |   8   |2001-12-15 05:45:00.0 |\n" +
        "    3    |    2    |   Karly   |Matthews |   1   |2000-05-20 04:00:00.0 |\n" +
        "    3    |    2    |   Karly   |Matthews |   2   |2000-05-20 04:00:00.0 |\n" +
        "    4    |    2    |   Kacy    |Matthews |   1   |2000-05-20 04:02:00.0 |\n" +
        "    5    |    2    |    Tom    |Matthews |   4   |2001-09-15 11:52:00.0 |\n" +
        "    5    |    2    |    Tom    |Matthews |   5   |2001-09-15 11:52:00.0 |";
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

        sqlText =
        String.format("select PersonID,FamilyID,FirstName,LastName, ROW_NUMBER() over ( partition by " +
        "length(trim(ucase(lcase(ucase(firstname)))) || trim(substr(lastname, 1, 5))) order by DOB) as Number " +
        "from %s  --SPLICE-PROPERTIES useSpark = %s \n order by PersonID",
        this.getTableReference(PEOPLE), useSpark);

        rs = methodWatcher.executeQuery(sqlText);
        expected =
        "PERSONID |FAMILYID | FIRSTNAME |LASTNAME |NUMBER |\n" +
        "--------------------------------------------------\n" +
        "    1    |    1    |    Joe    | Johnson |   1   |\n" +
        "    1    |    1    |    Joe    | Johnson |   2   |\n" +
        "    1    |    1    |    Joe    | Johnson |   3   |\n" +
        "    2    |    1    |    Jim    | Johnson |   6   |\n" +
        "    2    |    1    |    Jim    | Johnson |   7   |\n" +
        "    2    |    1    |    Jim    | Johnson |   8   |\n" +
        "    3    |    2    |   Karly   |Matthews |   1   |\n" +
        "    3    |    2    |   Karly   |Matthews |   2   |\n" +
        "    4    |    2    |   Kacy    |Matthews |   1   |\n" +
        "    5    |    2    |    Tom    |Matthews |   4   |\n" +
        "    5    |    2    |    Tom    |Matthews |   5   |";
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

        sqlText =
        String.format("SELECT item, date, sum(price) over (Partition by extract(hour from date) ORDER BY date) as  sumprice from %s  --SPLICE-PROPERTIES useSpark = %s \n order by date, item",
        this.getTableReference(PURCHASED), useSpark);

        rs = methodWatcher.executeQuery(sqlText);
        expected =
        "ITEM |         DATE           |SUMPRICE |\n" +
        "-----------------------------------------\n" +
        "  3  |2014-09-08 17:36:55.414 |  3.00   |\n" +
        "  5  |2014-09-08 17:41:56.353 |  14.00  |\n" +
        "  1  |2014-09-08 17:45:15.204 |  15.00  |\n" +
        "  5  |2014-09-08 17:46:26.428 |  19.00  |\n" +
        "  2  |2014-09-08 17:50:17.182 |  35.00  |\n" +
        "  4  |2014-09-08 17:50:17.182 |  35.00  |\n" +
        "  3  |2014-09-08 18:00:44.742 |  7.00   |\n" +
        "  4  |2014-09-08 18:05:47.166 |  9.00   |\n" +
        "  4  |2014-09-08 18:08:04.986 |  17.00  |\n" +
        "  5  |2014-09-08 18:11:23.645 |  27.00  |\n" +
        "  3  |2014-09-08 18:25:42.387 |  37.00  |\n" +
        "  2  |2014-09-08 18:26:51.387 |  42.00  |\n" +
        "  1  |2014-09-08 18:27:48.881 |  43.00  |\n" +
        "  1  |2014-09-08 18:33:46.446 |  50.00  |\n" +
        "  2  |2014-09-08 18:40:15.48  |  62.00  |";
        assertEquals("\n"+sqlText+"\n", expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
        rs.close();
    }

    @Test
    public void testSelectAllColsScalarAggWithOrderBy() throws Exception {
        // DB-1774
        String sqlText =
                String.format("SELECT item, price, sum(price) over (Partition by item ORDER BY date) as sumsal, date " +
                                "from %s  --SPLICE-PROPERTIES useSpark = %s \n order by item, price, date",
                        this.getTableReference(PURCHASED), useSpark);

        ResultSet rs = methodWatcher.executeQuery(sqlText);
        // Validated with PostgreSQL app
        String expected =
                "ITEM | PRICE |SUMSAL |         DATE           |\n" +
                "-----------------------------------------------\n" +
                "  1  | 1.00  | 1.00  |2014-09-08 17:45:15.204 |\n" +
                "  1  | 1.00  | 2.00  |2014-09-08 18:27:48.881 |\n" +
                "  1  | 7.00  | 9.00  |2014-09-08 18:33:46.446 |\n" +
                "  2  | 5.00  | 11.00 |2014-09-08 18:26:51.387 |\n" +
                "  2  | 6.00  | 6.00  |2014-09-08 17:50:17.182 |\n" +
                "  2  | 12.00 | 23.00 |2014-09-08 18:40:15.48  |\n" +
                "  3  | 3.00  | 3.00  |2014-09-08 17:36:55.414 |\n" +
                "  3  | 7.00  | 10.00 |2014-09-08 18:00:44.742 |\n" +
                "  3  | 10.00 | 20.00 |2014-09-08 18:25:42.387 |\n" +
                "  4  | 2.00  | 12.00 |2014-09-08 18:05:47.166 |\n" +
                "  4  | 8.00  | 20.00 |2014-09-08 18:08:04.986 |\n" +
                "  4  | 10.00 | 10.00 |2014-09-08 17:50:17.182 |\n" +
                "  5  | 4.00  | 15.00 |2014-09-08 17:46:26.428 |\n" +
                "  5  | 10.00 | 25.00 |2014-09-08 18:11:23.645 |\n" +
                "  5  | 11.00 | 11.00 |2014-09-08 17:41:56.353 |";
        assertEquals("\n"+sqlText+"\n", expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
        rs.close();

        // Repeated expression in the OVER clause.
        sqlText =
        String.format("SELECT item, price, sum(price) over (Partition by TIMESTAMPADD(SQL_TSI_HOUR, 6, date), " +
        "TIMESTAMPADD(SQL_TSI_HOUR, 6, date)) as sumsal, TIMESTAMPADD(SQL_TSI_HOUR, 6, date)" +
        "from %s  --SPLICE-PROPERTIES useSpark = %s \n order by item, price, date",
        this.getTableReference(PURCHASED), useSpark);

        rs = methodWatcher.executeQuery(sqlText);
        expected =
        "ITEM | PRICE |SUMSAL |           4            |\n" +
        "-----------------------------------------------\n" +
        "  1  | 1.00  | 1.00  |2014-09-08 23:45:15.204 |\n" +
        "  1  | 1.00  | 1.00  |2014-09-09 00:27:48.881 |\n" +
        "  1  | 7.00  | 7.00  |2014-09-09 00:33:46.446 |\n" +
        "  2  | 5.00  | 5.00  |2014-09-09 00:26:51.387 |\n" +
        "  2  | 6.00  | 16.00 |2014-09-08 23:50:17.182 |\n" +
        "  2  | 12.00 | 12.00 |2014-09-09 00:40:15.48  |\n" +
        "  3  | 3.00  | 3.00  |2014-09-08 23:36:55.414 |\n" +
        "  3  | 7.00  | 7.00  |2014-09-09 00:00:44.742 |\n" +
        "  3  | 10.00 | 10.00 |2014-09-09 00:25:42.387 |\n" +
        "  4  | 2.00  | 2.00  |2014-09-09 00:05:47.166 |\n" +
        "  4  | 8.00  | 8.00  |2014-09-09 00:08:04.986 |\n" +
        "  4  | 10.00 | 16.00 |2014-09-08 23:50:17.182 |\n" +
        "  5  | 4.00  | 4.00  |2014-09-08 23:46:26.428 |\n" +
        "  5  | 10.00 | 10.00 |2014-09-09 00:11:23.645 |\n" +
        "  5  | 11.00 | 11.00 |2014-09-08 23:41:56.353 |";
        assertEquals("\n"+sqlText+"\n", expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
        rs.close();
    }

    @Test
    public void testWindowFunctionWithGroupBy() throws Exception {

        String sqlText =
                "select empnum, dept, sum(salary)," +
                        "rank() over(partition by dept order by sum(salary) desc) rank " +
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

        sqlText =
        String.format("select sum(sal), " +
        "rank() over ( order by substr(ename, 1,1) ) empsal, ename from %s  --SPLICE-PROPERTIES useSpark = %s \n " +
        "group by ename order by ename",
        this.getTableReference(EMP), useSpark);

        rs = methodWatcher.executeQuery(sqlText);

        expected =
        "1    |EMPSAL | ENAME |\n" +
        "-------------------------\n" +
        "1100.00 |   1   | ADAMS |\n" +
        "1600.00 |   1   | ALLEN |\n" +
        "2850.00 |   3   | BLAKE |\n" +
        "2450.00 |   4   | CLARK |\n" +
        "3000.00 |   5   | FORD  |\n" +
        "950.00  |   6   | JAMES |\n" +
        "2975.00 |   6   | JONES |\n" +
        "5000.00 |   8   | KING  |\n" +
        "1250.00 |   9   |MARTIN |\n" +
        "1300.00 |   9   |MILLER |\n" +
        "3000.00 |  11   | SCOTT |\n" +
        "800.00  |  11   | SMITH |\n" +
        "1500.00 |  13   |TURNER |\n" +
        "1250.00 |  14   | WARD  |";
        assertEquals("\n"+sqlText+"\n", expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
        rs.close();

        sqlText =
        String.format("select sal, max(sal) over (partition by substr(ename, 1,1) order by " +
                      "substr(ename, 1,2) desc) empsal, ename from %s  --SPLICE-PROPERTIES useSpark = %s \n " +
                      "order by ename",
                      this.getTableReference(EMP), useSpark);
        rs = methodWatcher.executeQuery(sqlText);

        expected =
        "SAL   |EMPSAL  | ENAME |\n" +
        "--------------------------\n" +
        "1100.00 |1600.00 | ADAMS |\n" +
        "1600.00 |1600.00 | ALLEN |\n" +
        "2850.00 |2850.00 | BLAKE |\n" +
        "2450.00 |2450.00 | CLARK |\n" +
        "3000.00 |3000.00 | FORD  |\n" +
        "950.00  |2975.00 | JAMES |\n" +
        "2975.00 |2975.00 | JONES |\n" +
        "5000.00 |5000.00 | KING  |\n" +
        "1250.00 |1300.00 |MARTIN |\n" +
        "1300.00 |1300.00 |MILLER |\n" +
        "3000.00 |3000.00 | SCOTT |\n" +
        "800.00  |800.00  | SMITH |\n" +
        "1500.00 |1500.00 |TURNER |\n" +
        "1250.00 |1250.00 | WARD  |";
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

        sqlText =
        String.format("select sum(sal) as sum_sal, avg(sal) as avg_sal," +
        "rank() over ( order by sum(sal) + avg(sal) ) empsal, ename " +
        "from %s  --SPLICE-PROPERTIES useSpark = %s \n  group by ename order by ename",
        this.getTableReference(EMP), useSpark);

        rs = methodWatcher.executeQuery(sqlText);

        expected =
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

        sqlText =
        String.format("select sum(sal) as sum_sal, avg(sal) as avg_sal," +
        "rank() over ( order by deptno - avg(sal) ) empsal, ename " +
        "from %s  --SPLICE-PROPERTIES useSpark = %s \n  group by deptno, ename order by ename",
        this.getTableReference(EMP), useSpark);

        rs = methodWatcher.executeQuery(sqlText);

        expected =
        "SUM_SAL | AVG_SAL  |EMPSAL | ENAME |\n" +
        "-------------------------------------\n" +
        " 1100.00 |1100.0000 |  12   | ADAMS |\n" +
        " 1600.00 |1600.0000 |   7   | ALLEN |\n" +
        " 2850.00 |2850.0000 |   5   | BLAKE |\n" +
        " 2450.00 |2450.0000 |   6   | CLARK |\n" +
        " 3000.00 |3000.0000 |   2   | FORD  |\n" +
        " 950.00  |950.0000  |  13   | JAMES |\n" +
        " 2975.00 |2975.0000 |   4   | JONES |\n" +
        " 5000.00 |5000.0000 |   1   | KING  |\n" +
        " 1250.00 |1250.0000 |  10   |MARTIN |\n" +
        " 1300.00 |1300.0000 |   9   |MILLER |\n" +
        " 3000.00 |3000.0000 |   2   | SCOTT |\n" +
        " 800.00  |800.0000  |  14   | SMITH |\n" +
        " 1500.00 |1500.0000 |   8   |TURNER |\n" +
        " 1250.00 |1250.0000 |  10   | WARD  |";
        assertEquals("\n"+sqlText+"\n", expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
        rs.close();

        sqlText =
        String.format("select sal, min(sal) over ( partition by substr(ename,1,1) order by substr(ename,1,1),sal ) minsal," +
        "ename from %s  --SPLICE-PROPERTIES useSpark = %s \n  order by ename",
        this.getTableReference(EMP), useSpark);

        rs = methodWatcher.executeQuery(sqlText);

        expected =
        "SAL   |MINSAL  | ENAME |\n" +
        "--------------------------\n" +
        "1100.00 |1100.00 | ADAMS |\n" +
        "1600.00 |1100.00 | ALLEN |\n" +
        "2850.00 |2850.00 | BLAKE |\n" +
        "2450.00 |2450.00 | CLARK |\n" +
        "3000.00 |3000.00 | FORD  |\n" +
        "950.00  |950.00  | JAMES |\n" +
        "2975.00 |950.00  | JONES |\n" +
        "5000.00 |5000.00 | KING  |\n" +
        "1250.00 |1250.00 |MARTIN |\n" +
        "1300.00 |1250.00 |MILLER |\n" +
        "3000.00 |800.00  | SCOTT |\n" +
        "800.00  |800.00  | SMITH |\n" +
        "1500.00 |1500.00 |TURNER |\n" +
        "1250.00 |1250.00 | WARD  |";
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

        sqlText =
        String.format("select yr, rank() over ( partition by extract(month from hiredate) order by hiredate ) as EMPRANK, ename," +
        "hiredate from %s   --SPLICE-PROPERTIES useSpark = %s \n  order by hiredate, ename", this.getTableReference(YEAR_VIEW), useSpark);
        rs = methodWatcher.executeQuery(sqlText);
        expected =
        "YR | EMPRANK | ENAME | HIREDATE  |\n" +
        "----------------------------------\n" +
        "80 |    1    | SMITH |1980-12-17 |\n" +
        "81 |    1    | ALLEN |1981-02-20 |\n" +
        "81 |    2    | WARD  |1981-02-22 |\n" +
        "81 |    1    | JONES |1981-04-02 |\n" +
        "81 |    1    | BLAKE |1981-05-01 |\n" +
        "81 |    1    | CLARK |1981-06-09 |\n" +
        "81 |    1    |TURNER |1981-09-08 |\n" +
        "81 |    2    |MARTIN |1981-09-28 |\n" +
        "81 |    1    | KING  |1981-11-17 |\n" +
        "81 |    2    | FORD  |1981-12-03 |\n" +
        "81 |    2    | JAMES |1981-12-03 |\n" +
        "82 |    1    |MILLER |1982-01-23 |\n" +
        "82 |    4    | SCOTT |1982-12-09 |\n" +
        "83 |    2    | ADAMS |1983-01-12 |";
        assertEquals("\n"+sqlText+"\n", expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
        rs.close();

        sqlText =
        String.format("select a. yr, rank() over ( partition by extract(month from b.hiredate) order by b.hiredate ) as EMPRANK, a.ename," +
        "b.hiredate from %s a left outer join %s b   --SPLICE-PROPERTIES useSpark = %s \n  on a.ename = b.ename and b.ename <> 'JAMES' order by a.hiredate, a.ename",
        this.getTableReference(YEAR_VIEW), this.getTableReference(YEAR_VIEW), useSpark);
        rs = methodWatcher.executeQuery(sqlText);
        expected =
        "YR | EMPRANK | ENAME | HIREDATE  |\n" +
        "----------------------------------\n" +
        "80 |    1    | SMITH |1980-12-17 |\n" +
        "81 |    1    | ALLEN |1981-02-20 |\n" +
        "81 |    2    | WARD  |1981-02-22 |\n" +
        "81 |    1    | JONES |1981-04-02 |\n" +
        "81 |    1    | BLAKE |1981-05-01 |\n" +
        "81 |    1    | CLARK |1981-06-09 |\n" +
        "81 |    1    |TURNER |1981-09-08 |\n" +
        "81 |    2    |MARTIN |1981-09-28 |\n" +
        "81 |    1    | KING  |1981-11-17 |\n" +
        "81 |    2    | FORD  |1981-12-03 |\n" +
        "81 |    1    | JAMES |   NULL    |\n" +
        "82 |    1    |MILLER |1982-01-23 |\n" +
        "82 |    3    | SCOTT |1982-12-09 |\n" +
        "83 |    2    | ADAMS |1983-01-12 |";
        assertEquals("\n"+sqlText+"\n", expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
        rs.close();
    }

    @Test
    public void testDB2170RankOverViewMissingKey() throws Exception {
        String sqlText =
                String.format("select rank() over ( partition by yr order by hiredate ) as EMPRANK, ename," +
                        "hiredate from %s  --SPLICE-PROPERTIES useSpark = %s \n  order by hiredate, ename ", this.getTableReference(YEAR_VIEW), useSpark);
        ResultSet rs = methodWatcher.executeQuery(sqlText);
        String expected =
                "EMPRANK | ENAME | HIREDATE  |\n" +
                "------------------------------\n" +
                "    1    | SMITH |1980-12-17 |\n" +
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
                "    1    |MILLER |1982-01-23 |\n" +
                "    2    | SCOTT |1982-12-09 |\n" +
                "    1    | ADAMS |1983-01-12 |";
        assertEquals("\n"+sqlText+"\n", expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
        rs.close();

        sqlText =
        String.format("select rank() over ( partition by extract(year from hiredate) order by hiredate ) as EMPRANK, ename," +
        "hiredate from %s  --SPLICE-PROPERTIES useSpark = %s \n  order by hiredate, ename ", this.getTableReference(YEAR_VIEW), useSpark);
        rs = methodWatcher.executeQuery(sqlText);
        expected =
        "EMPRANK | ENAME | HIREDATE  |\n" +
        "------------------------------\n" +
        "    1    | SMITH |1980-12-17 |\n" +
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
        "    1    |MILLER |1982-01-23 |\n" +
        "    2    | SCOTT |1982-12-09 |\n" +
        "    1    | ADAMS |1983-01-12 |";
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

        sqlText =
        String.format("select max(extract (year from hiredate)) over (partition by extract(month from hiredate))" +
        "as maxhiredate, ename," +
        "hiredate from %s --SPLICE-PROPERTIES useSpark = %s \n order by extract(month from hiredate), ename", this.getTableReference(YEAR_VIEW), useSpark);

        rs = methodWatcher.executeQuery(sqlText);
        expected =
        "MAXHIREDATE | ENAME | HIREDATE  |\n" +
        "----------------------------------\n" +
        "    1983     | ADAMS |1983-01-12 |\n" +
        "    1983     |MILLER |1982-01-23 |\n" +
        "    1981     | ALLEN |1981-02-20 |\n" +
        "    1981     | WARD  |1981-02-22 |\n" +
        "    1981     | JONES |1981-04-02 |\n" +
        "    1981     | BLAKE |1981-05-01 |\n" +
        "    1981     | CLARK |1981-06-09 |\n" +
        "    1981     |MARTIN |1981-09-28 |\n" +
        "    1981     |TURNER |1981-09-08 |\n" +
        "    1981     | KING  |1981-11-17 |\n" +
        "    1982     | FORD  |1981-12-03 |\n" +
        "    1982     | JAMES |1981-12-03 |\n" +
        "    1982     | SCOTT |1982-12-09 |\n" +
        "    1982     | SMITH |1980-12-17 |";
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

        sqlText = format("SELECT employee_id, rank() over(order by salary), CASE WHEN (commission is not null)" +
           " THEN min(salary) over(PARTITION BY CASE WHEN department = 'Sales' THEN 1 ELSE 0 END) ELSE -1 END as  minimum_sal" +
           " FROM %s   --SPLICE-PROPERTIES useSpark = %s \n order by minimum_sal, employee_id", EMPLOYEES_REF, useSpark);
        rs = methodWatcher.executeQuery(sqlText);
        // Verified with PostgreSQL App
        expected =
        "EMPLOYEE_ID | 2 | MINIMUM_SAL |\n" +
        "--------------------------------\n" +
        "     101     | 1 |     -1      |\n" +
        "     107     | 2 |     -1      |\n" +
        "     108     | 2 |     -1      |\n" +
        "     105     | 8 |    10000    |\n" +
        "     106     | 4 |    10000    |\n" +
        "     102     | 4 |    12000    |\n" +
        "     103     | 6 |    12000    |\n" +
        "     104     | 7 |    12000    |";
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

        sqlText = format("select empno, salary, deptno, last_value(empno) over(partition by deptno/3 order by " +
        "salary+empno*1000 asc range between current row and unbounded following) as last_val from  \n " +
        "%s  --SPLICE-PROPERTIES useSpark = %s \n order by empno asc", EMP_2_REF, useSpark);
        rs = methodWatcher.executeQuery(sqlText);
        // Verified with PostgreSQL App
        expected =
        "EMPNO |SALARY |DEPTNO |LAST_VAL |\n" +
        "----------------------------------\n" +
        "  10   | 12000 |   5   |   24    |\n" +
        "  11   | 10000 |   5   |   24    |\n" +
        "  12   | 10000 |   5   |   24    |\n" +
        "  13   | 9000  |   1   |   17    |\n" +
        "  14   | 7500  |   1   |   17    |\n" +
        "  15   | 7600  |   1   |   17    |\n" +
        "  16   | 8500  |   2   |   17    |\n" +
        "  17   | 9500  |   2   |   17    |\n" +
        "  18   | 7700  |   2   |   17    |\n" +
        "  19   | 8500  |   3   |   24    |\n" +
        "  20   | 6900  |   3   |   24    |\n" +
        "  21   | 7500  |   3   |   24    |\n" +
        "  22   | 6500  |   4   |   24    |\n" +
        "  23   | 7800  |   4   |   24    |\n" +
        "  24   | 7200  |   4   |   24    |";
        assertEquals("\n"+sqlText+"\n", expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
        rs.close();
    }

    @Test
    public void testFirstValueFunction() throws Exception {
        // DB-3920
        String sqlText = format("select empno, salary, deptno, first_value(empno) over(partition by deptno order by " +
                "salary asc, empno rows unbounded preceding) as first_value from  " +
                "%s --SPLICE-PROPERTIES useSpark = %s \n where empno < 25 order by empno asc", EMP3_REF, useSpark);
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
    public void testFirstValueFunctionIgnoreNulls() throws Exception {
        // test default setting, which is the same as respect nulls
        String sqlText = format("select deptno, empno, salary, first_value(salary) over(partition by deptno order by " +
                "empno desc) as first_value from  " +
                "%s --SPLICE-PROPERTIES useSpark = %s \n order by deptno, empno desc", EMP3_REF, useSpark);
        ResultSet rs = methodWatcher.executeQuery(sqlText);
        String expected =
                "DEPTNO | EMPNO |SALARY | FIRST_VALUE |\n" +
                        "--------------------------------------\n" +
                        "   1   |  15   | 7600  |    7600     |\n" +
                        "   1   |  14   | 7500  |    7600     |\n" +
                        "   1   |  13   | 9000  |    7600     |\n" +
                        "   2   |  18   | 7700  |    7700     |\n" +
                        "   2   |  17   | 9500  |    7700     |\n" +
                        "   2   |  16   | 8500  |    7700     |\n" +
                        "   3   |  21   | 7500  |    7500     |\n" +
                        "   3   |  20   | 6900  |    7500     |\n" +
                        "   3   |  19   | 8500  |    7500     |\n" +
                        "   4   |  24   | 7200  |    7200     |\n" +
                        "   4   |  23   | 7800  |    7200     |\n" +
                        "   4   |  22   | 6500  |    7200     |\n" +
                        "   5   |  29   | NULL  |    NULL     |\n" +
                        "   5   |  28   | 15000 |    NULL     |\n" +
                        "   5   |  27   | NULL  |    NULL     |\n" +
                        "   5   |  26   | NULL  |    NULL     |\n" +
                        "   5   |  25   | NULL  |    NULL     |\n" +
                        "   5   |  12   | 10000 |    NULL     |\n" +
                        "   5   |  11   | 10000 |    NULL     |\n" +
                        "   5   |  10   | 12000 |    NULL     |";
        assertEquals("\n"+sqlText+"\n", expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
        rs.close();

        // test explicit respect nulls
        sqlText = format("select deptno, empno, salary, first_value(salary respect nulls) over(partition by deptno order by " +
                "empno desc) as first_value from  " +
                "%s --SPLICE-PROPERTIES useSpark = %s \n order by deptno, empno desc", EMP3_REF, useSpark);
        rs = methodWatcher.executeQuery(sqlText);
        assertEquals("\n"+sqlText+"\n", expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
        rs.close();

        //test UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING, which should get the same result as above
        sqlText = format("select deptno, empno, salary, first_value(salary respect nulls) over(partition by deptno order by " +
                "empno desc rows BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as first_value from  " +
                "%s --SPLICE-PROPERTIES useSpark = %s \n order by deptno, empno desc", EMP3_REF, useSpark);
        rs = methodWatcher.executeQuery(sqlText);
        assertEquals("\n"+sqlText+"\n", expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
        rs.close();

        sqlText = format("select deptno, empno, salary, first_value(salary respect nulls) over(partition by abs(deptno) order by " +
        "empno desc rows BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as first_value from  " +
        "%s --SPLICE-PROPERTIES useSpark = %s \n order by deptno, empno desc", EMP3_REF, useSpark);
        rs = methodWatcher.executeQuery(sqlText);
        assertEquals("\n"+sqlText+"\n", expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
        rs.close();

        // test ignore nulls
        expected =
                "DEPTNO | EMPNO |SALARY | FIRST_VALUE |\n" +
                        "--------------------------------------\n" +
                        "   1   |  15   | 7600  |    7600     |\n" +
                        "   1   |  14   | 7500  |    7600     |\n" +
                        "   1   |  13   | 9000  |    7600     |\n" +
                        "   2   |  18   | 7700  |    7700     |\n" +
                        "   2   |  17   | 9500  |    7700     |\n" +
                        "   2   |  16   | 8500  |    7700     |\n" +
                        "   3   |  21   | 7500  |    7500     |\n" +
                        "   3   |  20   | 6900  |    7500     |\n" +
                        "   3   |  19   | 8500  |    7500     |\n" +
                        "   4   |  24   | 7200  |    7200     |\n" +
                        "   4   |  23   | 7800  |    7200     |\n" +
                        "   4   |  22   | 6500  |    7200     |\n" +
                        "   5   |  29   | NULL  |    NULL     |\n" +
                        "   5   |  28   | 15000 |    15000    |\n" +
                        "   5   |  27   | NULL  |    15000    |\n" +
                        "   5   |  26   | NULL  |    15000    |\n" +
                        "   5   |  25   | NULL  |    15000    |\n" +
                        "   5   |  12   | 10000 |    15000    |\n" +
                        "   5   |  11   | 10000 |    15000    |\n" +
                        "   5   |  10   | 12000 |    15000    |";
        sqlText = format("select deptno, empno, salary, first_value(salary ignore nulls) over(partition by deptno order by " +
                "empno desc) as first_value from  " +
                "%s --SPLICE-PROPERTIES useSpark = %s \n order by deptno, empno desc", EMP3_REF, useSpark);
        rs = methodWatcher.executeQuery(sqlText);
        assertEquals("\n"+sqlText+"\n", expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
        rs.close();

        sqlText = format("select deptno, empno, salary, first_value(salary ignore nulls) over(partition by abs(deptno) order by " +
        "empno desc) as first_value from  " +
        "%s --SPLICE-PROPERTIES useSpark = %s \n order by deptno, empno desc", EMP3_REF, useSpark);
        rs = methodWatcher.executeQuery(sqlText);
        assertEquals("\n"+sqlText+"\n", expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
        rs.close();
    }

    @Test
    public void testFirstValueFunctionWithFrameClause() throws Exception {
        // test default setting, which is the same as respect nulls
        String sqlText = format("select deptno, empno, salary, first_value(salary) over(partition by deptno order by " +
                "empno desc rows between 1 preceding and 1 following) as first_value from  " +
                "%s --SPLICE-PROPERTIES useSpark = %s \n order by deptno, empno desc", EMP3_REF, useSpark);
        ResultSet rs = methodWatcher.executeQuery(sqlText);
        String expected =
                "DEPTNO | EMPNO |SALARY | FIRST_VALUE |\n" +
                        "--------------------------------------\n" +
                        "   1   |  15   | 7600  |    7600     |\n" +
                        "   1   |  14   | 7500  |    7600     |\n" +
                        "   1   |  13   | 9000  |    7500     |\n" +
                        "   2   |  18   | 7700  |    7700     |\n" +
                        "   2   |  17   | 9500  |    7700     |\n" +
                        "   2   |  16   | 8500  |    9500     |\n" +
                        "   3   |  21   | 7500  |    7500     |\n" +
                        "   3   |  20   | 6900  |    7500     |\n" +
                        "   3   |  19   | 8500  |    6900     |\n" +
                        "   4   |  24   | 7200  |    7200     |\n" +
                        "   4   |  23   | 7800  |    7200     |\n" +
                        "   4   |  22   | 6500  |    7800     |\n" +
                        "   5   |  29   | NULL  |    NULL     |\n" +
                        "   5   |  28   | 15000 |    NULL     |\n" +
                        "   5   |  27   | NULL  |    15000    |\n" +
                        "   5   |  26   | NULL  |    NULL     |\n" +
                        "   5   |  25   | NULL  |    NULL     |\n" +
                        "   5   |  12   | 10000 |    NULL     |\n" +
                        "   5   |  11   | 10000 |    10000    |\n" +
                        "   5   |  10   | 12000 |    10000    |";
        assertEquals("\n"+sqlText+"\n", expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
        rs.close();

        // test explicit respect nulls
        sqlText = format("select deptno, empno, salary, first_value(salary respect nulls) over(partition by deptno order by " +
                "empno desc rows between 1 preceding and 1 following) as first_value from  " +
                "%s --SPLICE-PROPERTIES useSpark = %s \n order by deptno, empno desc", EMP3_REF, useSpark);
        rs = methodWatcher.executeQuery(sqlText);
        assertEquals("\n"+sqlText+"\n", expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
        rs.close();

        sqlText = format("select deptno, empno, salary, first_value(-salary) over(partition by ln(deptno) order by " +
        "empno desc rows between 1 preceding and 1 following) as first_value from  " +
        "%s --SPLICE-PROPERTIES useSpark = %s \n order by deptno, empno desc", EMP3_REF, useSpark);
        rs = methodWatcher.executeQuery(sqlText);
        expected =
        "DEPTNO | EMPNO |SALARY | FIRST_VALUE |\n" +
        "--------------------------------------\n" +
        "   1   |  15   | 7600  |    -7600    |\n" +
        "   1   |  14   | 7500  |    -7600    |\n" +
        "   1   |  13   | 9000  |    -7500    |\n" +
        "   2   |  18   | 7700  |    -7700    |\n" +
        "   2   |  17   | 9500  |    -7700    |\n" +
        "   2   |  16   | 8500  |    -9500    |\n" +
        "   3   |  21   | 7500  |    -7500    |\n" +
        "   3   |  20   | 6900  |    -7500    |\n" +
        "   3   |  19   | 8500  |    -6900    |\n" +
        "   4   |  24   | 7200  |    -7200    |\n" +
        "   4   |  23   | 7800  |    -7200    |\n" +
        "   4   |  22   | 6500  |    -7800    |\n" +
        "   5   |  29   | NULL  |    NULL     |\n" +
        "   5   |  28   | 15000 |    NULL     |\n" +
        "   5   |  27   | NULL  |   -15000    |\n" +
        "   5   |  26   | NULL  |    NULL     |\n" +
        "   5   |  25   | NULL  |    NULL     |\n" +
        "   5   |  12   | 10000 |    NULL     |\n" +
        "   5   |  11   | 10000 |   -10000    |\n" +
        "   5   |  10   | 12000 |   -10000    |";
        assertEquals("\n"+sqlText+"\n", expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
        rs.close();

        // test ignore nulls
        expected =
                "DEPTNO | EMPNO |SALARY | FIRST_VALUE |\n" +
                        "--------------------------------------\n" +
                        "   1   |  15   | 7600  |    7600     |\n" +
                        "   1   |  14   | 7500  |    7600     |\n" +
                        "   1   |  13   | 9000  |    7500     |\n" +
                        "   2   |  18   | 7700  |    7700     |\n" +
                        "   2   |  17   | 9500  |    7700     |\n" +
                        "   2   |  16   | 8500  |    9500     |\n" +
                        "   3   |  21   | 7500  |    7500     |\n" +
                        "   3   |  20   | 6900  |    7500     |\n" +
                        "   3   |  19   | 8500  |    6900     |\n" +
                        "   4   |  24   | 7200  |    7200     |\n" +
                        "   4   |  23   | 7800  |    7200     |\n" +
                        "   4   |  22   | 6500  |    7800     |\n" +
                        "   5   |  29   | NULL  |    15000    |\n" +
                        "   5   |  28   | 15000 |    15000    |\n" +
                        "   5   |  27   | NULL  |    15000    |\n" +
                        "   5   |  26   | NULL  |    NULL     |\n" +
                        "   5   |  25   | NULL  |    10000    |\n" +
                        "   5   |  12   | 10000 |    10000    |\n" +
                        "   5   |  11   | 10000 |    10000    |\n" +
                        "   5   |  10   | 12000 |    10000    |";
        sqlText = format("select deptno, empno, salary, first_value(salary ignore nulls) over(partition by deptno order by " +
                "empno desc rows between 1 preceding and 1 following) as first_value from  " +
                "%s --SPLICE-PROPERTIES useSpark = %s \n order by deptno, empno desc", EMP3_REF, useSpark);
        rs = methodWatcher.executeQuery(sqlText);
        assertEquals("\n"+sqlText+"\n", expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
        rs.close();
    }

    @Test
    public void testLastValueFunctionIgnoreNulls() throws Exception {
        // test default setting, which is the same as respect nulls
        String sqlText = format("select deptno, empno, salary, last_value(salary) over(partition by deptno order by " +
                "empno) as last_value from  " +
                "%s --SPLICE-PROPERTIES useSpark = %s \n order by deptno, empno", EMP3_REF, useSpark);
        ResultSet rs = methodWatcher.executeQuery(sqlText);
        String expected =
                "DEPTNO | EMPNO |SALARY |LAST_VALUE |\n" +
                        "------------------------------------\n" +
                        "   1   |  13   | 9000  |   9000    |\n" +
                        "   1   |  14   | 7500  |   7500    |\n" +
                        "   1   |  15   | 7600  |   7600    |\n" +
                        "   2   |  16   | 8500  |   8500    |\n" +
                        "   2   |  17   | 9500  |   9500    |\n" +
                        "   2   |  18   | 7700  |   7700    |\n" +
                        "   3   |  19   | 8500  |   8500    |\n" +
                        "   3   |  20   | 6900  |   6900    |\n" +
                        "   3   |  21   | 7500  |   7500    |\n" +
                        "   4   |  22   | 6500  |   6500    |\n" +
                        "   4   |  23   | 7800  |   7800    |\n" +
                        "   4   |  24   | 7200  |   7200    |\n" +
                        "   5   |  10   | 12000 |   12000   |\n" +
                        "   5   |  11   | 10000 |   10000   |\n" +
                        "   5   |  12   | 10000 |   10000   |\n" +
                        "   5   |  25   | NULL  |   NULL    |\n" +
                        "   5   |  26   | NULL  |   NULL    |\n" +
                        "   5   |  27   | NULL  |   NULL    |\n" +
                        "   5   |  28   | 15000 |   15000   |\n" +
                        "   5   |  29   | NULL  |   NULL    |";
        assertEquals("\n"+sqlText+"\n", expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
        rs.close();

        // test explicit respect nulls
        sqlText = format("select deptno, empno, salary, last_value(salary respect nulls) over(partition by deptno order by " +
                "empno) as last_value from  " +
                "%s --SPLICE-PROPERTIES useSpark = %s \n order by deptno, empno", EMP3_REF, useSpark);
        rs = methodWatcher.executeQuery(sqlText);
        assertEquals("\n"+sqlText+"\n", expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
        rs.close();

        // test ignore nulls
        expected =
                "DEPTNO | EMPNO |SALARY |LAST_VALUE |\n" +
                        "------------------------------------\n" +
                        "   1   |  13   | 9000  |   9000    |\n" +
                        "   1   |  14   | 7500  |   7500    |\n" +
                        "   1   |  15   | 7600  |   7600    |\n" +
                        "   2   |  16   | 8500  |   8500    |\n" +
                        "   2   |  17   | 9500  |   9500    |\n" +
                        "   2   |  18   | 7700  |   7700    |\n" +
                        "   3   |  19   | 8500  |   8500    |\n" +
                        "   3   |  20   | 6900  |   6900    |\n" +
                        "   3   |  21   | 7500  |   7500    |\n" +
                        "   4   |  22   | 6500  |   6500    |\n" +
                        "   4   |  23   | 7800  |   7800    |\n" +
                        "   4   |  24   | 7200  |   7200    |\n" +
                        "   5   |  10   | 12000 |   12000   |\n" +
                        "   5   |  11   | 10000 |   10000   |\n" +
                        "   5   |  12   | 10000 |   10000   |\n" +
                        "   5   |  25   | NULL  |   10000   |\n" +
                        "   5   |  26   | NULL  |   10000   |\n" +
                        "   5   |  27   | NULL  |   10000   |\n" +
                        "   5   |  28   | 15000 |   15000   |\n" +
                        "   5   |  29   | NULL  |   15000   |";
        sqlText = format("select deptno, empno, salary, last_value(salary ignore nulls) over(partition by deptno order by " +
                "empno) as last_value from  " +
                "%s --SPLICE-PROPERTIES useSpark = %s \n order by deptno, empno", EMP3_REF, useSpark);
        rs = methodWatcher.executeQuery(sqlText);
        assertEquals("\n"+sqlText+"\n", expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
        rs.close();
    }

    @Test
    public void testLastValueFunctionWithFrameClause() throws Exception {
        // test default setting, which is the same as respect nulls
        String sqlText = format("select deptno, empno, salary, last_value(salary) over(partition by deptno order by " +
                "empno rows between 1 preceding and 1 following) as last_value from  " +
                "%s --SPLICE-PROPERTIES useSpark = %s \n order by deptno, empno", EMP3_REF, useSpark);
        ResultSet rs = methodWatcher.executeQuery(sqlText);
        String expected =
                "DEPTNO | EMPNO |SALARY |LAST_VALUE |\n" +
                        "------------------------------------\n" +
                        "   1   |  13   | 9000  |   7500    |\n" +
                        "   1   |  14   | 7500  |   7600    |\n" +
                        "   1   |  15   | 7600  |   7600    |\n" +
                        "   2   |  16   | 8500  |   9500    |\n" +
                        "   2   |  17   | 9500  |   7700    |\n" +
                        "   2   |  18   | 7700  |   7700    |\n" +
                        "   3   |  19   | 8500  |   6900    |\n" +
                        "   3   |  20   | 6900  |   7500    |\n" +
                        "   3   |  21   | 7500  |   7500    |\n" +
                        "   4   |  22   | 6500  |   7800    |\n" +
                        "   4   |  23   | 7800  |   7200    |\n" +
                        "   4   |  24   | 7200  |   7200    |\n" +
                        "   5   |  10   | 12000 |   10000   |\n" +
                        "   5   |  11   | 10000 |   10000   |\n" +
                        "   5   |  12   | 10000 |   NULL    |\n" +
                        "   5   |  25   | NULL  |   NULL    |\n" +
                        "   5   |  26   | NULL  |   NULL    |\n" +
                        "   5   |  27   | NULL  |   15000   |\n" +
                        "   5   |  28   | 15000 |   NULL    |\n" +
                        "   5   |  29   | NULL  |   NULL    |";
        assertEquals("\n"+sqlText+"\n", expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
        rs.close();

        // test explicit respect nulls
        sqlText = format("select deptno, empno, salary, last_value(salary respect nulls) over(partition by deptno order by " +
                "empno rows between 1 preceding and 1 following) as last_value from  " +
                "%s --SPLICE-PROPERTIES useSpark = %s \n order by deptno, empno", EMP3_REF, useSpark);
        rs = methodWatcher.executeQuery(sqlText);
        assertEquals("\n"+sqlText+"\n", expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
        rs.close();

        // test ignore nulls
        expected =
                "DEPTNO | EMPNO |SALARY |LAST_VALUE |\n" +
                        "------------------------------------\n" +
                        "   1   |  13   | 9000  |   7500    |\n" +
                        "   1   |  14   | 7500  |   7600    |\n" +
                        "   1   |  15   | 7600  |   7600    |\n" +
                        "   2   |  16   | 8500  |   9500    |\n" +
                        "   2   |  17   | 9500  |   7700    |\n" +
                        "   2   |  18   | 7700  |   7700    |\n" +
                        "   3   |  19   | 8500  |   6900    |\n" +
                        "   3   |  20   | 6900  |   7500    |\n" +
                        "   3   |  21   | 7500  |   7500    |\n" +
                        "   4   |  22   | 6500  |   7800    |\n" +
                        "   4   |  23   | 7800  |   7200    |\n" +
                        "   4   |  24   | 7200  |   7200    |\n" +
                        "   5   |  10   | 12000 |   10000   |\n" +
                        "   5   |  11   | 10000 |   10000   |\n" +
                        "   5   |  12   | 10000 |   10000   |\n" +
                        "   5   |  25   | NULL  |   10000   |\n" +
                        "   5   |  26   | NULL  |   NULL    |\n" +
                        "   5   |  27   | NULL  |   15000   |\n" +
                        "   5   |  28   | 15000 |   15000   |\n" +
                        "   5   |  29   | NULL  |   15000   |";
        sqlText = format("select deptno, empno, salary, last_value(salary ignore nulls) over(partition by deptno order by " +
                "empno rows between 1 preceding and 1 following) as last_value from  " +
                "%s --SPLICE-PROPERTIES useSpark = %s \n order by deptno, empno", EMP3_REF, useSpark);
        rs = methodWatcher.executeQuery(sqlText);
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

        sqlText = format("SELECT month, SUM(amount) AS month_amount, " +
        "LAST_VALUE(SUM(amount)) OVER (ORDER BY mod(month,7)+month ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) " +
        "AS next_month_amount FROM %s  --SPLICE-PROPERTIES useSpark = %s \n GROUP BY month ORDER BY month", ALL_SALES_2_REF, useSpark);
        rs = methodWatcher.executeQuery(sqlText);
        // Verified with PostgreSQL App
        expected =
        "MONTH |MONTH_AMOUNT | NEXT_MONTH_AMOUNT |\n" +
        "------------------------------------------\n" +
        "   1   |  58704.52   |     28289.30      |\n" +
        "   2   |  28289.30   |     20167.83      |\n" +
        "   3   |  20167.83   |     78299.47      |\n" +
        "   4   |  50082.90   |     42869.64      |\n" +
        "   5   |  17212.66   |     35299.22      |\n" +
        "   6   |  31128.92   |     43028.38      |\n" +
        "   7   |  78299.47   |     50082.90      |\n" +
        "   8   |  42869.64   |     17212.66      |\n" +
        "   9   |  35299.22   |     31128.92      |\n" +
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

        sqlText = format("select a.productId, a.ratingAgencyCode, a.ratingEffDate as effDate, a.rating, LEAD(ratingeffDate)" +
        " OVER (PARTITION BY extract(year from a.ratingEffDate) + length(ratingAgencyCode) ORDER BY ratingeffDate) \"endDate\" " +
        "from %s a --SPLICE-PROPERTIES useSpark = %s \n  order by productId, ratingAgencyCode, effdate", PRODUCT_RATING_REF, useSpark);
        rs = methodWatcher.executeQuery(sqlText);
        // Verified with PostgreSQL App
        expected =
        "PRODUCTID | RATINGAGENCYCODE   |        EFFDATE         |    RATING      |        endDate         |\n" +
        "----------------------------------------------------------------------------------------------------\n" +
        "     1     |       Best         |2014-08-29 22:30:41.012 |BIMTVQOBOXOMBNI |2014-11-01 21:51:02.199 |\n" +
        "     1     |Dont think about it |2014-02-09 08:12:02.596 |PXVDSMZWANQWGCX |2014-02-14 04:59:08.36  |\n" +
        "     1     |Dont think about it |2015-01-25 19:47:53.004 |NWAJOBJVSGIAIZU |2015-05-23 16:04:52.561 |\n" +
        "     1     |       Good         |2014-11-02 03:24:36.044 |TILAXMXGUEYZSEK |2014-12-14 05:52:22.326 |\n" +
        "     1     |       Good         |2014-12-14 05:52:22.326 |UMTHLNRYDFAYWQS |         NULL           |\n" +
        "     1     |       Good         |2015-07-22 04:39:19.453 |XQSJCCCGLIOMMCJ |2015-09-02 05:09:32.683 |\n" +
        "     1     |       Good         |2015-12-04 16:40:43.67  |BLYUEGSMCFCPXJA |         NULL           |\n" +
        "     2     |Dont think about it |2014-02-14 04:59:08.36  |JVEZECWKNBLAALU |2014-03-11 10:19:44.256 |\n" +
        "     2     |       Good         |2015-01-22 11:41:01.329 |ZRHSIVRCQUUQZLC |2015-01-23 18:15:43.723 |\n" +
        "     2     |       Good         |2015-10-06 03:44:19.184 |NZRXIDYRZNUVXDU |2015-12-04 16:40:43.67  |\n" +
        "     2     |     Run Away       |2014-05-16 22:44:34.056 |ZSFHGWWFCQZZNMO |2014-06-22 08:12:53.937 |\n" +
        "     2     |     Run Away       |2015-06-30 07:06:13.865 |IAUEYFHYZBFMJFW |         NULL           |\n" +
        "    700    |      Better        |2014-01-27 01:22:55.581 |CHZGQMKFKUXIEAY |2014-03-07 19:37:30.604 |\n" +
        "    700    |Dont think about it |2014-07-19 08:25:52.238 |UYLCQPSCHURELLY |2014-07-30 10:23:12.643 |\n" +
        "    700    |       Poor         |2014-02-07 12:19:36.492 |YPXRMRFFYYPWBUZ |2014-05-22 22:20:20.929 |\n" +
        "    700    |       Poor         |2014-08-21 21:27:35.482 |ZKBLWNEPGNMQUTL |2014-08-29 22:30:41.012 |\n" +
        "    800    |      Better        |2015-07-30 11:36:16.128 |UUAYIPTXDNCCBTU |2015-11-25 00:16:42.154 |\n" +
        "    800    |      Better        |2015-11-25 00:16:42.154 |ZCQJQILUQRCHWCY |         NULL           |\n" +
        "    800    |     Run Away       |2014-01-04 21:08:27.058 |FWXYVWSYDWYXXBZ |2014-04-11 06:29:30.187 |\n" +
        "    800    |     Run Away       |2014-11-04 19:02:56.001 |ZIMKXUOKMCWCGSZ |2014-11-12 06:40:59.685 |\n" +
        "    800    |     Run Away       |2015-01-09 13:29:29.642 |KMCNRDOWZFAEVFS |2015-05-09 01:21:16.513 |\n" +
        "    800    |     Run Away       |2015-06-23 20:21:06.677 |DBAEKJQCJBGMOYQ |2015-06-30 07:06:13.865 |\n" +
        "   3300    |      Better        |2014-03-07 19:37:30.604 |WDQTAXMXLMTWXQL |2014-05-22 10:42:47.834 |\n" +
        "   3300    |      Better        |2014-05-22 10:42:47.834 |QOJYERFYJAYGNIC |         NULL           |\n" +
        "   3300    |      Better        |2015-01-13 11:01:19.065 |SPIHABCGNPNOOAC |2015-07-30 11:36:16.128 |\n" +
        "   3300    |Dont think about it |2014-11-03 05:15:49.851 |TMXIAVAKXCDJYKY |         NULL           |\n" +
        "   3300    |Dont think about it |2015-08-13 21:03:44.191 |MRUQGZIKYBXHZPQ |         NULL           |\n" +
        "   3300    |       Poor         |2015-09-02 05:09:32.683 |FFXXMDXCAVYDJTC | 2015-09-27 17:49:39.5  |\n" +
        "   4040    |       Best         |2014-08-21 15:48:24.096 |JSKWDFWMTQCVJTS |2014-08-21 21:27:35.482 |\n" +
        "   4040    |Dont think about it |2015-06-10 12:59:47.54  |LTCOVEBAVEHNCRP |2015-06-11 21:00:40.206 |\n" +
        "   4040    |       Poor         |2014-07-29 18:45:46.257 |EMXGEGPTWXUECLS |2014-07-31 20:33:36.713 |\n" +
        "   4040    |     Run Away       |2014-06-22 08:12:53.937 |PHAMOZYXPQXWJDF |2014-11-04 19:02:56.001 |\n" +
        "   4040    |     Run Away       |2015-05-09 01:21:16.513 |YNWDVEUSONYHZXE |2015-06-23 20:21:06.677 |\n" +
        "   4440    |Dont think about it |2014-03-11 10:19:44.256 |NIILBXPPMRRKVSG |2014-07-19 08:25:52.238 |\n" +
        "   4440    |Dont think about it |2014-08-05 09:55:23.669 |GLUUGLMTGJRUHSA |2014-09-21 01:20:16.26  |\n" +
        "   4440    |Dont think about it |2015-01-06 21:26:12.528 |KVPKDRBXWBGATQY |2015-01-25 19:47:53.004 |\n" +
        "   4440    |       Good         |2014-07-27 14:25:56.885 |APGRMQQJCJSTCDB |2014-07-29 18:45:46.257 |\n" +
        "   4440    |     Run Away       |2014-04-11 06:29:30.187 |WYREDMPIIZPGXZA |2014-05-16 22:44:34.056 |\n" +
        "   4440    |     Run Away       |2014-11-12 06:40:59.685 |VVEEPGAEVWDLBPF |         NULL           |\n" +
        "   5500    |       Best         |2015-01-23 18:15:43.723 |JRLYTUTWIZNOAPT |2015-04-30 05:52:43.627 |\n" +
        "   5500    |       Good         |2014-07-31 20:33:36.713 |OLSEYNIBUDVHEIF |2014-08-21 15:48:24.096 |\n" +
        "   6999    |Dont think about it |2014-07-30 10:23:12.643 |MRVUOQXPZMVTVXW |2014-08-05 09:55:23.669 |\n" +
        "   6999    |Dont think about it |2014-09-21 01:20:16.26  |MLBLXWCFKHVWXVU |2014-11-03 05:15:49.851 |\n" +
        "   6999    |Dont think about it |2015-05-23 16:04:52.561 |HCZIXOTTRASHITI |2015-06-10 12:59:47.54  |\n" +
        "   6999    |Dont think about it |2015-08-03 18:40:21.753 |QZMNXNYRPEKJWBY |2015-08-13 21:03:44.191 |\n" +
        "   6999    |       Poor         |2015-04-30 05:52:43.627 |LFHHSWUBDDIPKPA |2015-07-22 04:39:19.453 |\n" +
        "   9001    |       Best         |2014-11-01 21:51:02.199 |JFVQBHBCTVPVZTR |2014-11-02 03:24:36.044 |\n" +
        "   9001    |Dont think about it |2015-06-11 21:00:40.206 |ILWSWUTLTSCJRSV |2015-08-03 18:40:21.753 |\n" +
        "   9001    |       Good         | 2015-09-27 17:49:39.5  |XHFHMOPWMOJNGOL |2015-10-06 03:44:19.184 |\n" +
        "   9001    |       Poor         |2014-05-22 22:20:20.929 |HZLYPGHDEQORMOH |2014-07-27 14:25:56.885 |";
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
                "%s   --SPLICE-PROPERTIES useSpark = %s \n where empno < 25 order by deptno, SALARY DESC, empno", EMP3_REF, useSpark);
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
                "         LEAD(STAGE_NUM) OVER (PARTITION BY A.PRSN_KEY ORDER BY " +
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
                        "  30   |2010-08-09 | 84000 |          2          |  3  |          2          |\n" +
                        "  33   |2010-08-09 | NULL  |          3          |  3  |          3          |\n" +
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
                        "  10   | 50000 |     1     |  1  |     1     |\n" +
                        "  20   | 75000 |     5     |  1  |     2     |\n" +
                        "  32   | NULL  |     8     |  1  |     3     |\n" +
                        "  50   | 52000 |     2     |  1  |     4     |\n" +
                        "  55   | 52000 |     3     |  1  |     5     |\n" +
                        "  60   | 78000 |     7     |  1  |     6     |\n" +
                        "  70   | 76000 |     6     |  1  |     7     |\n" +
                        "  110  | 53000 |     4     |  1  |     8     |\n" +
                        "  40   | 52000 |     2     |  2  |     1     |\n" +
                        "  44   | 52000 |     3     |  2  |     2     |\n" +
                        "  49   | 53000 |     4     |  2  |     3     |\n" +
                        "  90   | 51000 |     1     |  2  |     4     |\n" +
                        "  30   | 84000 |     4     |  3  |     1     |\n" +
                        "  33   | NULL  |     5     |  3  |     2     |\n" +
                        "  80   | 79000 |     3     |  3  |     3     |\n" +
                        "  100  | 55000 |     1     |  3  |     4     |\n" +
                        "  120  | 75000 |     2     |  3  |     5     |";
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
                        "group by c,b order by revenue", this.getTableReference(NESTED_AGGREGATION_WF));

        ResultSet rs = methodWatcher.executeQuery(sqlText);
        String expected =
                "REVENUE |\n" +
                        "----------\n" +
                        "    4    |\n" +
                        "    6    |\n" +
                        "    8    |\n" +
                        "   12    |\n" +
                        "   14    |";
        assertEquals("\n"+sqlText+"\n", expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
        rs.close();
    }

    @Test
    public void testNestedAggregatWithEmptyTableFunction() throws Exception {
        // SPLICE-969 -  Test a simple use case of nested aggregate within a window functions
        String sqlText =
                String.format("select  sum(sum(b)) over (partition by b)  as revenue\n" +
                        "from %s\n" +
                        "group by  c, b order by revenue", this.getTableReference(EMPTY_TABLE));

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

    @Test
    public void testWindowFnWithSubqAlias() throws Exception {
        String sqlText =
                String.format(
                        "select distinct " +
                                "sum(d1.c6) over (partition by d1.c7) as aggr_sum " +
                                ",d1.c7 " +
                                "from (select mgr as c7, sal c6 from %s  --SPLICE-PROPERTIES useSpark = %s \n" +
                                ") as d1 order by 1",
                        this.getTableReference(EMP),useSpark);

        ResultSet rs = methodWatcher.executeQuery(sqlText);
        String expected =
                "AGGR_SUM | C7  |\n"+
                        "----------------\n" +
                        " 800.00  |7902 |\n" +
                        " 1100.00 |7788 |\n" +
                        " 1300.00 |7782 |\n" +
                        " 5000.00 |NULL |\n" +
                        " 6000.00 |7566 |\n" +
                        " 6550.00 |7698 |\n" +
                        " 8275.00 |7839 |";

        assertEquals("\n"+sqlText+"\n", expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
        rs.close();
    }

    @Test
    public void testManyPartitionsOnControl() throws Exception{
        String sqlText =
                String.format("select c1, c2, c3, row_number() over (partition by c1, c2 order by c1, c2) from %s " +
                        "--splice-properties useSpark=false", this.getTableReference(tableA));

        ResultSet rs = methodWatcher.executeQuery(sqlText);
        // Just to make sure the query can execute in control mode without throwing a StackOverflowError.
        assert rs.next();
    }

    @Test
    public void testSerDeForWindowFunctionOverNLJ() throws Exception {
        String sqlText =
                String.format("select max(X.salary) over (partition by X.deptno) from --splice-properties joinOrder=fixed\n" +
                        "%s as X, %s as Y " +
                        "--splice-properties joinStrategy=nestedloop, useSpark=%s\n" +
                        "where X.empno=Y.empno", EMP_2_REF, EMP_2_REF, this.useSpark);

        String expected = "1   |\n" +
                "-------\n" +
                "12000 |\n" +
                "12000 |\n" +
                "12000 |\n" +
                "7800  |\n" +
                "7800  |\n" +
                "7800  |\n" +
                "8500  |\n" +
                "8500  |\n" +
                "8500  |\n" +
                "9000  |\n" +
                "9000  |\n" +
                "9000  |\n" +
                "9500  |\n" +
                "9500  |\n" +
                "9500  |";
        ResultSet rs = methodWatcher.executeQuery(sqlText);
        assertEquals(expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
        rs.close();

    }

    @Test
    public void testCaseExpressionWithColumnsfromMultipleInstanceOfSameTable() throws Exception {
        String sqlText =
                String.format("select X.empname, X.salary, X.deptno, case when dept.Nome_Dep is null then dept_def.Nome_Dep else dept.Nome_Dep end as deptName," +
                        "max(X.salary) over (partition by X.deptno), rank() over (partition by X.deptno order by X.salary) from " +
                        "%1$s as X --splice-properties useSpark=%3$s\n" +
                        "          left join %2$s as dept on X.deptno= dept.id " +
                        "          left join %2$s as dept_def on dept_def.id=1" +
                        "order by X.deptno, X.empname", EMP_2_REF, this.getTableReference(DEPARTAMENTOS), this.useSpark);

        String expected = "EMPNAME |SALARY |DEPTNO |    DEPTNAME     |  5   | 6 |\n" +
                "-------------------------------------------------------\n" +
                "Benjamin | 7500  |   1   |     Vendas      |9000  | 1 |\n" +
                "   Tom   | 7600  |   1   |     Vendas      |9000  | 2 |\n" +
                "  Wendy  | 9000  |   1   |     Vendas      |9000  | 3 |\n" +
                "  Henry  | 8500  |   2   |       IT        |9500  | 2 |\n" +
                "  Paul   | 7700  |   2   |       IT        |9500  | 1 |\n" +
                " Robert  | 9500  |   2   |       IT        |9500  | 3 |\n" +
                "  Dora   | 8500  |   3   |Recursos Humanos |8500  | 3 |\n" +
                "  Mary   | 7500  |   3   |Recursos Humanos |8500  | 2 |\n" +
                " Samuel  | 6900  |   3   |Recursos Humanos |8500  | 1 |\n" +
                " Daniel  | 6500  |   4   |     Vendas      |7800  | 1 |\n" +
                "  Mark   | 7200  |   4   |     Vendas      |7800  | 2 |\n" +
                " Ricardo | 7800  |   4   |     Vendas      |7800  | 3 |\n" +
                "  Bill   | 12000 |   5   |     Vendas      |12000 | 3 |\n" +
                " Solomon | 10000 |   5   |     Vendas      |12000 | 1 |\n" +
                "  Susan  | 10000 |   5   |     Vendas      |12000 | 1 |";
        ResultSet rs = methodWatcher.executeQuery(sqlText);
        assertEquals(expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
        rs.close();
    }

    @Test
    public void testMaxWindowFunctionWithNulls() throws Exception {
        String sqlText =
                String.format("select years, month, max(amount) over (partition by years order by month rows between unbounded preceding and current row) X\n" +
                        "from %s --splice-properties useSpark=%s\n" +
                        "where years=2005 and emp_id=21 order by years, month", this.getTableReference(ALL_SALES), useSpark);

        ResultSet rs = methodWatcher.executeQuery(sqlText);
        String expected =
                "YEARS | MONTH |    X    |\n" +
                        "--------------------------\n" +
                        " 2005  |   1   |26034.84 |\n" +
                        " 2005  |   2   |26034.84 |\n" +
                        " 2005  |   3   |26034.84 |\n" +
                        " 2005  |   4   |26034.84 |\n" +
                        " 2005  |   5   |26034.84 |\n" +
                        " 2005  |   6   |26034.84 |\n" +
                        " 2005  |   7   |62654.82 |\n" +
                        " 2005  |   8   |62654.82 |\n" +
                        " 2005  |   9   |62654.82 |\n" +
                        " 2005  |  10   |62654.82 |\n" +
                        " 2005  |  11   |62654.82 |\n" +
                        " 2005  |  12   |62654.82 |";
        assertEquals("\n"+sqlText+"\n", expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
        rs.close();
    }

    @Test
    public void testMinWindowFunctionWithNulls() throws Exception {
        String sqlText =
                String.format("select years, month, min(amount) over (partition by years order by month rows between unbounded preceding and current row) X\n" +
                        "from %s --splice-properties useSpark=%s\n" +
                        "where years=2005 and emp_id=21 order by years, month", this.getTableReference(ALL_SALES), useSpark);

        ResultSet rs = methodWatcher.executeQuery(sqlText);
        String expected =
                "YEARS | MONTH |    X    |\n" +
                        "--------------------------\n" +
                        " 2005  |   1   |26034.84 |\n" +
                        " 2005  |   2   |12644.65 |\n" +
                        " 2005  |   3   |12644.65 |\n" +
                        " 2005  |   4   |12644.65 |\n" +
                        " 2005  |   5   |12644.65 |\n" +
                        " 2005  |   6   |12644.65 |\n" +
                        " 2005  |   7   |12644.65 |\n" +
                        " 2005  |   8   |12644.65 |\n" +
                        " 2005  |   9   |12644.65 |\n" +
                        " 2005  |  10   |12644.65 |\n" +
                        " 2005  |  11   |12644.65 |\n" +
                        " 2005  |  12   |10032.64 |";
        assertEquals("\n"+sqlText+"\n", expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
        rs.close();
    }
}
