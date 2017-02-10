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

package com.splicemachine.derby.impl.sql.execute.tester;

import com.splicemachine.derby.test.framework.*;
import com.splicemachine.test.SlowTest;
import org.junit.*;
import org.junit.experimental.categories.Category;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;
import org.junit.runner.Description;

import java.sql.ResultSet;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;


@Category(SlowTest.class)
public class DataTypeCorrectnessIT extends SpliceUnitTest {

    private static boolean done;

    private static final String CLASS_NAME = DataTypeCorrectnessIT.class.getSimpleName().toUpperCase();
    private static final SpliceWatcher spliceClassWatcher = new SpliceWatcher(CLASS_NAME);

    private static String TABLE_1 = "A", TABLE_2 = "B", TABLE_3 = "C", TABLE_4 = "D", TABLE_5 = "E", TABLE_6 = "F",
            TABLE_7 = "G", TABLE_8 = "H", TABLE_9 = "I", TABLE_10 = "J", TABLE_11 = "K", TABLE_12 = "L", TABLE_13 = "M",
            TABLE_14 = "N", TABLE_15 = "O", TABLE_16 = "P", TABLE_17 = "Q", TABLE_18 = "R";

    private static String TABLE_30 = "AA", TABLE_31 = "BB", TABLE_32 = "CC", TABLE_33 = "DD", TABLE_34 = "EE",
            TABLE_35 = "FF", TABLE_36 = "GG", TABLE_37 = "HH", TABLE_38 = "II", TABLE_39 = "JJ", TABLE_40 = "KK",
            TABLE_41 = "LL";

    private static String TABLE_50 = "AAA";
    private static String TABLE_51 = "BBB";


    String[] tables = new String[]{TABLE_1, TABLE_2, TABLE_3, TABLE_4, TABLE_5, TABLE_6, TABLE_7, TABLE_8, TABLE_9, TABLE_10, TABLE_11, TABLE_12, TABLE_13, TABLE_14, TABLE_15, TABLE_16, TABLE_17, TABLE_18};
    String[] bTables = new String[]{TABLE_30, TABLE_31, TABLE_32, TABLE_33, TABLE_34, TABLE_35, TABLE_36, TABLE_37, TABLE_38, TABLE_39, TABLE_40, TABLE_41, TABLE_50, TABLE_51};

    private static SpliceSchemaWatcher spliceSchemaWatcher = new SpliceSchemaWatcher(CLASS_NAME);
    private static SpliceTableWatcher spliceTableWatcher1 = new SpliceTableWatcher(TABLE_1, spliceSchemaWatcher.schemaName, "(boolean1 boolean, boolean2 boolean,boolean3 boolean, smallint1 smallint primary key, smallint2 smallint,smallint3 smallint, integer1 integer,integer2 integer,integer3 integer, bigint1 bigint,bigint2 bigint,bigint3 bigint,decimal1 decimal,decimal2 decimal,decimal3 decimal,real1 real, real2 real,real3 real,double1 double,double2 double,double3 double,float1 float,float2 float,float3 float,char1 char(10),char2 char(10),char3 char(10),varchar1 varchar(100),varchar2 varchar(100),varchar3 varchar(100))");
    private static SpliceTableWatcher spliceTableWatcher2 = new SpliceTableWatcher(TABLE_2, spliceSchemaWatcher.schemaName, "(boolean1 boolean, boolean2 boolean,boolean3 boolean, smallint1 smallint, smallint2 smallint,smallint3 smallint, integer1 integer  primary key,integer2 integer,integer3 integer, bigint1 bigint,bigint2 bigint,bigint3 bigint,decimal1 decimal,decimal2 decimal,decimal3 decimal,real1 real, real2 real,real3 real,double1 double,double2 double,double3 double,float1 float,float2 float,float3 float,char1 char(10),char2 char(10),char3 char(10),varchar1 varchar(100),varchar2 varchar(100),varchar3 varchar(100))");
    private static SpliceTableWatcher spliceTableWatcher3 = new SpliceTableWatcher(TABLE_3, spliceSchemaWatcher.schemaName, "(boolean1 boolean, boolean2 boolean,boolean3 boolean, smallint1 smallint, smallint2 smallint,smallint3 smallint, integer1 integer,integer2 integer,integer3 integer, bigint1 bigint primary key,bigint2 bigint,bigint3 bigint,decimal1 decimal,decimal2 decimal,decimal3 decimal,real1 real, real2 real,real3 real,double1 double,double2 double,double3 double,float1 float,float2 float,float3 float,char1 char(10),char2 char(10),char3 char(10),varchar1 varchar(100),varchar2 varchar(100),varchar3 varchar(100))");
    private static SpliceTableWatcher spliceTableWatcher4 = new SpliceTableWatcher(TABLE_4, spliceSchemaWatcher.schemaName, "(boolean1 boolean, boolean2 boolean,boolean3 boolean, smallint1 smallint, smallint2 smallint,smallint3 smallint, integer1 integer,integer2 integer,integer3 integer, bigint1 bigint,bigint2 bigint,bigint3 bigint,decimal1 decimal primary key,decimal2 decimal,decimal3 decimal,real1 real, real2 real,real3 real,double1 double,double2 double,double3 double,float1 float,float2 float,float3 float,char1 char(10),char2 char(10),char3 char(10),varchar1 varchar(100),varchar2 varchar(100),varchar3 varchar(100))");
    private static SpliceTableWatcher spliceTableWatcher5 = new SpliceTableWatcher(TABLE_5, spliceSchemaWatcher.schemaName, "(boolean1 boolean, boolean2 boolean,boolean3 boolean, smallint1 smallint, smallint2 smallint,smallint3 smallint, integer1 integer,integer2 integer,integer3 integer, bigint1 bigint,bigint2 bigint,bigint3 bigint,decimal1 decimal,decimal2 decimal,decimal3 decimal,real1 real primary key, real2 real,real3 real,double1 double,double2 double,double3 double,float1 float,float2 float,float3 float,char1 char(10),char2 char(10),char3 char(10),varchar1 varchar(100),varchar2 varchar(100),varchar3 varchar(100))");
    private static SpliceTableWatcher spliceTableWatcher6 = new SpliceTableWatcher(TABLE_6, spliceSchemaWatcher.schemaName, "(boolean1 boolean, boolean2 boolean,boolean3 boolean, smallint1 smallint, smallint2 smallint,smallint3 smallint, integer1 integer,integer2 integer,integer3 integer, bigint1 bigint,bigint2 bigint,bigint3 bigint,decimal1 decimal,decimal2 decimal,decimal3 decimal,real1 real, real2 real,real3 real,double1 double primary key,double2 double,double3 double,float1 float,float2 float,float3 float,char1 char(10),char2 char(10),char3 char(10),varchar1 varchar(100),varchar2 varchar(100),varchar3 varchar(100))");
    private static SpliceTableWatcher spliceTableWatcher7 = new SpliceTableWatcher(TABLE_7, spliceSchemaWatcher.schemaName, "(boolean1 boolean, boolean2 boolean,boolean3 boolean, smallint1 smallint, smallint2 smallint,smallint3 smallint, integer1 integer,integer2 integer,integer3 integer, bigint1 bigint,bigint2 bigint,bigint3 bigint,decimal1 decimal,decimal2 decimal,decimal3 decimal,real1 real, real2 real,real3 real,double1 double,double2 double,double3 double,float1 float primary key,float2 float,float3 float,char1 char(10),char2 char(10),char3 char(10),varchar1 varchar(100),varchar2 varchar(100),varchar3 varchar(100))");
    private static SpliceTableWatcher spliceTableWatcher8 = new SpliceTableWatcher(TABLE_8, spliceSchemaWatcher.schemaName, "(boolean1 boolean, boolean2 boolean,boolean3 boolean, smallint1 smallint, smallint2 smallint,smallint3 smallint, integer1 integer,integer2 integer,integer3 integer, bigint1 bigint,bigint2 bigint,bigint3 bigint,decimal1 decimal,decimal2 decimal,decimal3 decimal,real1 real, real2 real,real3 real,double1 double,double2 double,double3 double,float1 float,float2 float,float3 float,char1 char(10) primary key,char2 char(10),char3 char(10),varchar1 varchar(100),varchar2 varchar(100),varchar3 varchar(100))");
    private static SpliceTableWatcher spliceTableWatcher9 = new SpliceTableWatcher(TABLE_9, spliceSchemaWatcher.schemaName, "(boolean1 boolean, boolean2 boolean,boolean3 boolean, smallint1 smallint, smallint2 smallint,smallint3 smallint, integer1 integer,integer2 integer,integer3 integer, bigint1 bigint,bigint2 bigint,bigint3 bigint,decimal1 decimal,decimal2 decimal,decimal3 decimal,real1 real, real2 real,real3 real,double1 double,double2 double,double3 double,float1 float,float2 float,float3 float,char1 char(10),char2 char(10),char3 char(10),varchar1 varchar(100)  primary key,varchar2 varchar(100),varchar3 varchar(100))");
    private static SpliceTableWatcher spliceTableWatcher10 = new SpliceTableWatcher(TABLE_10, spliceSchemaWatcher.schemaName, "(boolean1 boolean, boolean2 boolean,boolean3 boolean, smallint1 smallint, smallint2 smallint,smallint3 smallint, integer1 integer,integer2 integer,integer3 integer, bigint1 bigint,bigint2 bigint,bigint3 bigint,decimal1 decimal,decimal2 decimal,decimal3 decimal,real1 real, real2 real,real3 real,double1 double,double2 double,double3 double,float1 float,float2 float,float3 float,char1 char(10),char2 char(10),char3 char(10),varchar1 varchar(100),varchar2 varchar(100),varchar3 varchar(100), PRIMARY KEY (boolean1,smallint1))");
    private static SpliceTableWatcher spliceTableWatcher11 = new SpliceTableWatcher(TABLE_11, spliceSchemaWatcher.schemaName, "(boolean1 boolean, boolean2 boolean,boolean3 boolean, smallint1 smallint, smallint2 smallint,smallint3 smallint, integer1 integer,integer2 integer,integer3 integer, bigint1 bigint,bigint2 bigint,bigint3 bigint,decimal1 decimal,decimal2 decimal,decimal3 decimal,real1 real, real2 real,real3 real,double1 double,double2 double,double3 double,float1 float,float2 float,float3 float,char1 char(10),char2 char(10),char3 char(10),varchar1 varchar(100),varchar2 varchar(100),varchar3 varchar(100), PRIMARY KEY (boolean1,smallint1,integer1))");
    private static SpliceTableWatcher spliceTableWatcher12 = new SpliceTableWatcher(TABLE_12, spliceSchemaWatcher.schemaName, "(boolean1 boolean, boolean2 boolean,boolean3 boolean, smallint1 smallint, smallint2 smallint,smallint3 smallint, integer1 integer,integer2 integer,integer3 integer, bigint1 bigint,bigint2 bigint,bigint3 bigint,decimal1 decimal,decimal2 decimal,decimal3 decimal,real1 real, real2 real,real3 real,double1 double,double2 double,double3 double,float1 float,float2 float,float3 float,char1 char(10),char2 char(10),char3 char(10),varchar1 varchar(100),varchar2 varchar(100),varchar3 varchar(100), PRIMARY KEY (boolean1,smallint1,integer1, bigint1))");
    private static SpliceTableWatcher spliceTableWatcher13 = new SpliceTableWatcher(TABLE_13, spliceSchemaWatcher.schemaName, "(boolean1 boolean, boolean2 boolean,boolean3 boolean, smallint1 smallint, smallint2 smallint,smallint3 smallint, integer1 integer,integer2 integer,integer3 integer, bigint1 bigint,bigint2 bigint,bigint3 bigint,decimal1 decimal,decimal2 decimal,decimal3 decimal,real1 real, real2 real,real3 real,double1 double,double2 double,double3 double,float1 float,float2 float,float3 float,char1 char(10),char2 char(10),char3 char(10),varchar1 varchar(100),varchar2 varchar(100),varchar3 varchar(100), PRIMARY KEY (boolean1,smallint1,integer1, bigint1,decimal1))");
    private static SpliceTableWatcher spliceTableWatcher14 = new SpliceTableWatcher(TABLE_14, spliceSchemaWatcher.schemaName, "(boolean1 boolean, boolean2 boolean,boolean3 boolean, smallint1 smallint, smallint2 smallint,smallint3 smallint, integer1 integer,integer2 integer,integer3 integer, bigint1 bigint,bigint2 bigint,bigint3 bigint,decimal1 decimal,decimal2 decimal,decimal3 decimal,real1 real, real2 real,real3 real,double1 double,double2 double,double3 double,float1 float,float2 float,float3 float,char1 char(10),char2 char(10),char3 char(10),varchar1 varchar(100),varchar2 varchar(100),varchar3 varchar(100), PRIMARY KEY (boolean1,smallint1,integer1, bigint1,decimal1,real1))");
    private static SpliceTableWatcher spliceTableWatcher15 = new SpliceTableWatcher(TABLE_15, spliceSchemaWatcher.schemaName, "(boolean1 boolean, boolean2 boolean,boolean3 boolean, smallint1 smallint, smallint2 smallint,smallint3 smallint, integer1 integer,integer2 integer,integer3 integer, bigint1 bigint,bigint2 bigint,bigint3 bigint,decimal1 decimal,decimal2 decimal,decimal3 decimal,real1 real, real2 real,real3 real,double1 double,double2 double,double3 double,float1 float,float2 float,float3 float,char1 char(10),char2 char(10),char3 char(10),varchar1 varchar(100),varchar2 varchar(100),varchar3 varchar(100), PRIMARY KEY (boolean1,smallint1,integer1, bigint1,decimal1,real1,double1))");
    private static SpliceTableWatcher spliceTableWatcher16 = new SpliceTableWatcher(TABLE_16, spliceSchemaWatcher.schemaName, "(boolean1 boolean, boolean2 boolean,boolean3 boolean, smallint1 smallint, smallint2 smallint,smallint3 smallint, integer1 integer,integer2 integer,integer3 integer, bigint1 bigint,bigint2 bigint,bigint3 bigint,decimal1 decimal,decimal2 decimal,decimal3 decimal,real1 real, real2 real,real3 real,double1 double,double2 double,double3 double,float1 float,float2 float,float3 float,char1 char(10),char2 char(10),char3 char(10),varchar1 varchar(100),varchar2 varchar(100),varchar3 varchar(100), PRIMARY KEY (boolean1,smallint1,integer1, bigint1,decimal1,real1,double1,float1))");
    private static SpliceTableWatcher spliceTableWatcher17 = new SpliceTableWatcher(TABLE_17, spliceSchemaWatcher.schemaName, "(boolean1 boolean, boolean2 boolean,boolean3 boolean, smallint1 smallint, smallint2 smallint,smallint3 smallint, integer1 integer,integer2 integer,integer3 integer, bigint1 bigint,bigint2 bigint,bigint3 bigint,decimal1 decimal,decimal2 decimal,decimal3 decimal,real1 real, real2 real,real3 real,double1 double,double2 double,double3 double,float1 float,float2 float,float3 float,char1 char(10),char2 char(10),char3 char(10),varchar1 varchar(100),varchar2 varchar(100),varchar3 varchar(100), PRIMARY KEY (boolean1,smallint1,integer1, bigint1,decimal1,real1,double1,float1,char1))");
    private static SpliceTableWatcher spliceTableWatcher18 = new SpliceTableWatcher(TABLE_18, spliceSchemaWatcher.schemaName, "(boolean1 boolean, boolean2 boolean,boolean3 boolean, smallint1 smallint, smallint2 smallint,smallint3 smallint, integer1 integer,integer2 integer,integer3 integer, bigint1 bigint,bigint2 bigint,bigint3 bigint,decimal1 decimal,decimal2 decimal,decimal3 decimal,real1 real, real2 real,real3 real,double1 double,double2 double,double3 double,float1 float,float2 float,float3 float,char1 char(10),char2 char(10),char3 char(10),varchar1 varchar(100),varchar2 varchar(100),varchar3 varchar(100), PRIMARY KEY (boolean1,smallint1,integer1, bigint1,decimal1,real1,double1,float1,char1,varchar1))");

    private static SpliceTableWatcher spliceTableWatcher30 = new SpliceTableWatcher(TABLE_30, spliceSchemaWatcher.schemaName, " ( lvarchar1 long varchar, lvarchar2 long varchar , lvarchar3 long varchar , charforbitdata1 char(8) for bit data,charforbitdata2 char(8) for bit data,charforbitdata3 char(8) for bit data,varcharforbitdata1 varchar(1024) for bit data,varcharforbitdata2 varchar(1024) for bit data,varcharforbitdata3 varchar(1024) for bit data, longvarcharforbitdata1 long varchar for bit data, longvarcharforbitdata2 long varchar for bit data, longvarcharforbitdata3 long varchar for bit data,date1 date,date2 date,date3 date,time1 time,time2 time,time3 time,timestamp1 timestamp,timestamp2 timestamp,timestamp3 timestamp)");
    private static SpliceTableWatcher spliceTableWatcher31 = new SpliceTableWatcher(TABLE_31, spliceSchemaWatcher.schemaName, " ( lvarchar1 long varchar, lvarchar2 long varchar , lvarchar3 long varchar , charforbitdata1 char(8) for bit data,charforbitdata2 char(8) for bit data,charforbitdata3 char(8) for bit data,varcharforbitdata1 varchar(1024) for bit data,varcharforbitdata2 varchar(1024) for bit data,varcharforbitdata3 varchar(1024) for bit data, longvarcharforbitdata1 long varchar for bit data, longvarcharforbitdata2 long varchar for bit data, longvarcharforbitdata3 long varchar for bit data,date1 date,date2 date,date3 date,time1 time,time2 time,time3 time,timestamp1 timestamp,timestamp2 timestamp,timestamp3 timestamp)");
    private static SpliceTableWatcher spliceTableWatcher32 = new SpliceTableWatcher(TABLE_32, spliceSchemaWatcher.schemaName, " ( lvarchar1 long varchar , lvarchar2 long varchar , lvarchar3 long varchar , charforbitdata1 char(8) for bit data  primary key,charforbitdata2 char(8) for bit data,charforbitdata3 char(8) for bit data,varcharforbitdata1 varchar(1024) for bit data,varcharforbitdata2 varchar(1024) for bit data,varcharforbitdata3 varchar(1024) for bit data, longvarcharforbitdata1 long varchar for bit data, longvarcharforbitdata2 long varchar for bit data, longvarcharforbitdata3 long varchar for bit data,date1 date,date2 date,date3 date,time1 time,time2 time,time3 time,timestamp1 timestamp,timestamp2 timestamp,timestamp3 timestamp)");
    private static SpliceTableWatcher spliceTableWatcher33 = new SpliceTableWatcher(TABLE_33, spliceSchemaWatcher.schemaName, " ( lvarchar1 long varchar , lvarchar2 long varchar , lvarchar3 long varchar , charforbitdata1 char(8) for bit data,charforbitdata2 char(8) for bit data,charforbitdata3 char(8) for bit data,varcharforbitdata1 varchar(1024) for bit data  primary key,varcharforbitdata2 varchar(1024) for bit data,varcharforbitdata3 varchar(1024) for bit data, longvarcharforbitdata1 long varchar for bit data, longvarcharforbitdata2 long varchar for bit data, longvarcharforbitdata3 long varchar for bit data,date1 date,date2 date,date3 date,time1 time,time2 time,time3 time,timestamp1 timestamp,timestamp2 timestamp,timestamp3 timestamp)");
    private static SpliceTableWatcher spliceTableWatcher35 = new SpliceTableWatcher(TABLE_35, spliceSchemaWatcher.schemaName, " ( lvarchar1 long varchar , lvarchar2 long varchar , lvarchar3 long varchar , charforbitdata1 char(8) for bit data,charforbitdata2 char(8) for bit data,charforbitdata3 char(8) for bit data,varcharforbitdata1 varchar(1024) for bit data,varcharforbitdata2 varchar(1024) for bit data,varcharforbitdata3 varchar(1024) for bit data, longvarcharforbitdata1 long varchar for bit data, longvarcharforbitdata2 long varchar for bit data, longvarcharforbitdata3 long varchar for bit data,date1 date,date2 date,date3 date,time1 time,time2 time,time3 time,timestamp1 timestamp,timestamp2 timestamp,timestamp3 timestamp, PRIMARY KEY (charforbitdata1))");
    private static SpliceTableWatcher spliceTableWatcher34 = new SpliceTableWatcher(TABLE_34, spliceSchemaWatcher.schemaName, " ( lvarchar1 long varchar , lvarchar2 long varchar , lvarchar3 long varchar , charforbitdata1 char(8) for bit data,charforbitdata2 char(8) for bit data,charforbitdata3 char(8) for bit data,varcharforbitdata1 varchar(1024) for bit data,varcharforbitdata2 varchar(1024) for bit data,varcharforbitdata3 varchar(1024) for bit data, longvarcharforbitdata1 long varchar for bit data, longvarcharforbitdata2 long varchar for bit data, longvarcharforbitdata3 long varchar for bit data,date1 date,date2 date,date3 date,time1 time,time2 time,time3 time,timestamp1 timestamp,timestamp2 timestamp,timestamp3 timestamp)");
    private static SpliceTableWatcher spliceTableWatcher36 = new SpliceTableWatcher(TABLE_36, spliceSchemaWatcher.schemaName, " ( lvarchar1 long varchar , lvarchar2 long varchar , lvarchar3 long varchar , charforbitdata1 char(8) for bit data,charforbitdata2 char(8) for bit data,charforbitdata3 char(8) for bit data,varcharforbitdata1 varchar(1024) for bit data,varcharforbitdata2 varchar(1024) for bit data,varcharforbitdata3 varchar(1024) for bit data, longvarcharforbitdata1 long varchar for bit data, longvarcharforbitdata2 long varchar for bit data, longvarcharforbitdata3 long varchar for bit data,date1 date,date2 date,date3 date,time1 time,time2 time,time3 time,timestamp1 timestamp,timestamp2 timestamp,timestamp3 timestamp, PRIMARY KEY (charforbitdata1,varcharforbitdata1))");
    private static SpliceTableWatcher spliceTableWatcher37 = new SpliceTableWatcher(TABLE_37, spliceSchemaWatcher.schemaName, " ( lvarchar1 long varchar , lvarchar2 long varchar , lvarchar3 long varchar , charforbitdata1 char(8) for bit data,charforbitdata2 char(8) for bit data,charforbitdata3 char(8) for bit data,varcharforbitdata1 varchar(1024) for bit data,varcharforbitdata2 varchar(1024) for bit data,varcharforbitdata3 varchar(1024) for bit data, longvarcharforbitdata1 long varchar for bit data, longvarcharforbitdata2 long varchar for bit data, longvarcharforbitdata3 long varchar for bit data,date1 date,date2 date,date3 date,time1 time,time2 time,time3 time,timestamp1 timestamp,timestamp2 timestamp,timestamp3 timestamp, PRIMARY KEY (charforbitdata1,varcharforbitdata1,date1))");
    private static SpliceTableWatcher spliceTableWatcher38 = new SpliceTableWatcher(TABLE_38, spliceSchemaWatcher.schemaName, " ( lvarchar1 long varchar , lvarchar2 long varchar , lvarchar3 long varchar , charforbitdata1 char(8) for bit data,charforbitdata2 char(8) for bit data,charforbitdata3 char(8) for bit data,varcharforbitdata1 varchar(1024) for bit data,varcharforbitdata2 varchar(1024) for bit data,varcharforbitdata3 varchar(1024) for bit data, longvarcharforbitdata1 long varchar for bit data, longvarcharforbitdata2 long varchar for bit data, longvarcharforbitdata3 long varchar for bit data,date1 date,date2 date,date3 date,time1 time,time2 time,time3 time,timestamp1 timestamp,timestamp2 timestamp,timestamp3 timestamp, PRIMARY KEY (charforbitdata1,varcharforbitdata1,date1,time1))");
    private static SpliceTableWatcher spliceTableWatcher39 = new SpliceTableWatcher(TABLE_39, spliceSchemaWatcher.schemaName, " ( lvarchar1 long varchar , lvarchar2 long varchar , lvarchar3 long varchar , charforbitdata1 char(8) for bit data,charforbitdata2 char(8) for bit data,charforbitdata3 char(8) for bit data,varcharforbitdata1 varchar(1024) for bit data,varcharforbitdata2 varchar(1024) for bit data,varcharforbitdata3 varchar(1024) for bit data, longvarcharforbitdata1 long varchar for bit data, longvarcharforbitdata2 long varchar for bit data, longvarcharforbitdata3 long varchar for bit data,date1 date,date2 date,date3 date,time1 time,time2 time,time3 time,timestamp1 timestamp,timestamp2 timestamp,timestamp3 timestamp)");
    private static SpliceTableWatcher spliceTableWatcher40 = new SpliceTableWatcher(TABLE_40, spliceSchemaWatcher.schemaName, " ( lvarchar1 long varchar , lvarchar2 long varchar , lvarchar3 long varchar , charforbitdata1 char(8) for bit data,charforbitdata2 char(8) for bit data,charforbitdata3 char(8) for bit data,varcharforbitdata1 varchar(1024) for bit data,varcharforbitdata2 varchar(1024) for bit data,varcharforbitdata3 varchar(1024) for bit data, longvarcharforbitdata1 long varchar for bit data, longvarcharforbitdata2 long varchar for bit data, longvarcharforbitdata3 long varchar for bit data,date1 date,date2 date,date3 date,time1 time,time2 time,time3 time,timestamp1 timestamp,timestamp2 timestamp,timestamp3 timestamp, PRIMARY KEY (varcharforbitdata1,date1,time1,timestamp1))");
    private static SpliceTableWatcher spliceTableWatcher41 = new SpliceTableWatcher(TABLE_41, spliceSchemaWatcher.schemaName, " ( lvarchar1 long varchar , lvarchar2 long varchar , lvarchar3 long varchar , charforbitdata1 char(8) for bit data,charforbitdata2 char(8) for bit data,charforbitdata3 char(8) for bit data,varcharforbitdata1 varchar(1024) for bit data,varcharforbitdata2 varchar(1024) for bit data,varcharforbitdata3 varchar(1024) for bit data, longvarcharforbitdata1 long varchar for bit data, longvarcharforbitdata2 long varchar for bit data, longvarcharforbitdata3 long varchar for bit data,date1 date,date2 date,date3 date,time1 time,time2 time,time3 time,timestamp1 timestamp,timestamp2 timestamp,timestamp3 timestamp, PRIMARY KEY ( charforbitdata1,varcharforbitdata1,date1,time1,timestamp1))");


    private static SpliceTableWatcher spliceTableWatcher50 = new SpliceTableWatcher(TABLE_50, spliceSchemaWatcher.schemaName, "(boolean1 boolean, boolean2 boolean,boolean3 boolean, smallint1 smallint, smallint2 smallint,smallint3 smallint, integer1 integer,integer2 integer,integer3 integer, bigint1 bigint,bigint2 bigint,bigint3 bigint,decimal1 decimal,decimal2 decimal,decimal3 decimal,real1 real, real2 real,real3 real,double1 double,double2 double,double3 double,float1 float,float2 float,float3 float,char1 char(10),char2 char(10),char3 char(10),varchar1 varchar(100),varchar2 varchar(100),varchar3 varchar(100),lvarchar1 long varchar, lvarchar2 long varchar , lvarchar3 long varchar , charforbitdata1 char(8) for bit data,charforbitdata2 char(8) for bit data,charforbitdata3 char(8) for bit data,varcharforbitdata1 varchar(1024) for bit data,varcharforbitdata2 varchar(1024) for bit data,varcharforbitdata3 varchar(1024) for bit data, longvarcharforbitdata1 long varchar for bit data, longvarcharforbitdata2 long varchar for bit data, longvarcharforbitdata3 long varchar for bit data,date1 date,date2 date,date3 date,time1 time,time2 time,time3 time,timestamp1 timestamp,timestamp2 timestamp,timestamp3 timestamp)");
    private static SpliceTableWatcher spliceTableWatcher51 = new SpliceTableWatcher(TABLE_51, spliceSchemaWatcher.schemaName, "(boolean1 boolean, boolean2 boolean,boolean3 boolean, smallint1 smallint, smallint2 smallint,smallint3 smallint, integer1 integer,integer2 integer,integer3 integer, bigint1 bigint,bigint2 bigint,bigint3 bigint,decimal1 decimal,decimal2 decimal,decimal3 decimal,real1 real, real2 real,real3 real,double1 double,double2 double,double3 double,float1 float,float2 float,float3 float,char1 char(10),char2 char(10),char3 char(10),varchar1 varchar(100),varchar2 varchar(100),varchar3 varchar(100),lvarchar1 long varchar, lvarchar2 long varchar , lvarchar3 long varchar , charforbitdata1 char(8) for bit data,charforbitdata2 char(8) for bit data,charforbitdata3 char(8) for bit data,varcharforbitdata1 varchar(1024) for bit data,varcharforbitdata2 varchar(1024) for bit data,varcharforbitdata3 varchar(1024) for bit data, longvarcharforbitdata1 long varchar for bit data, longvarcharforbitdata2 long varchar for bit data, longvarcharforbitdata3 long varchar for bit data,date1 date,date2 date,date3 date,time1 time,time2 time,time3 time,timestamp1 timestamp,timestamp2 timestamp,timestamp3 timestamp, PRIMARY KEY (boolean1,smallint1,integer1, bigint1,decimal1,real1,double1,float1,char1,varchar1))");


    @Rule
    public SpliceWatcher methodWatcher = new SpliceWatcher();


    @ClassRule
    public static TestRule chain = RuleChain.outerRule(spliceClassWatcher)
            .around(spliceSchemaWatcher)
            .around(spliceTableWatcher1)
            .around(spliceTableWatcher2)
            .around(spliceTableWatcher3)
            .around(spliceTableWatcher4)
            .around(spliceTableWatcher5)
            .around(spliceTableWatcher6)
            .around(spliceTableWatcher7)
            .around(spliceTableWatcher8)
            .around(spliceTableWatcher9)
            .around(spliceTableWatcher10)
            .around(spliceTableWatcher11)
            .around(spliceTableWatcher12)
            .around(spliceTableWatcher13)
            .around(spliceTableWatcher14)
            .around(spliceTableWatcher15)
            .around(spliceTableWatcher16)
            .around(spliceTableWatcher17)
            .around(spliceTableWatcher18)
            .around(spliceTableWatcher30)
            .around(spliceTableWatcher31)
            .around(spliceTableWatcher32)
            .around(spliceTableWatcher33)
            .around(spliceTableWatcher34)
            .around(spliceTableWatcher35)
            .around(spliceTableWatcher36)
            .around(spliceTableWatcher37)
            .around(spliceTableWatcher38)
            .around(spliceTableWatcher39)
            .around(spliceTableWatcher40)
            .around(spliceTableWatcher41)
            .around(spliceTableWatcher50)
            .around(spliceTableWatcher51)
            .around(new SpliceDataWatcher() {
                @Override
                protected void starting(Description description) {
                    try {
                        for (char tableName = 'A'; tableName <= 'R'; tableName++) {
                            spliceClassWatcher.executeUpdate("insert into " + tableName +
                                    " VALUES(true,true,false, 1,2,3, 1,2,3, 1,2,3, 1,2,3, 1.0,2.0,3.0, 1.0,2.0,3.0, 1.0,2.0,3.0, 'a','c','d', 'a','c','d')");
                            spliceClassWatcher.executeUpdate("insert into " + tableName +
                                    " VALUES(true,false,true, 2,4,2, 2,4,2, 2,4,2, 2,4,2, 2.0,4.0,2.0, 2.0,4.0,2.0, 2.0,4.0,2.0, 'b','b','b', 'b','b','b')");
                            spliceClassWatcher.executeUpdate("insert into " + tableName +
                                    " VALUES(false,true,true, 3,2,1, 3,1,2, 3,1,2, 3,3,1, 3.0,3.0,1.0, 3.0,3.0,1.0, 3.0,3.0,1.0, 'c','a','e', 'c','a','e')");
                        }

                        spliceClassWatcher.prepareStatement("CREATE INDEX TABLE1 ON " + TABLE_1 + " (smallint3)").execute();
                        spliceClassWatcher.prepareStatement("CREATE INDEX TABLE2 ON " + TABLE_2 + " (boolean3,smallint3)").execute();
                        spliceClassWatcher.prepareStatement("CREATE INDEX TABLE3 ON " + TABLE_3 + " (boolean3,smallint3,integer3)").execute();
                        spliceClassWatcher.prepareStatement("CREATE INDEX TABLE4 ON " + TABLE_4 + " (boolean3,smallint3,integer3,bigint3)").execute();
                        spliceClassWatcher.prepareStatement("CREATE INDEX TABLE5 ON " + TABLE_5 + " (boolean3,smallint3,integer3,bigint3,decimal3)").execute();
                        spliceClassWatcher.prepareStatement("CREATE INDEX TABLE6 ON " + TABLE_6 + " (boolean3,smallint3,integer3,bigint3,decimal3,real3)").execute();
                        spliceClassWatcher.prepareStatement("CREATE INDEX TABLE7 ON " + TABLE_7 + " (boolean3,smallint3,integer3,bigint3,decimal3,real3,double3)").execute();
                        spliceClassWatcher.prepareStatement("CREATE INDEX TABLE8 ON " + TABLE_8 + " (boolean3,smallint3,integer3,bigint3,decimal3,real3,double3,float3)").execute();
                        spliceClassWatcher.prepareStatement("CREATE INDEX TABLE9 ON " + TABLE_9 + " (boolean3,smallint3,integer3,bigint3,decimal3,real3,double3,float3,char3)").execute();
                        spliceClassWatcher.prepareStatement("CREATE INDEX TABLE10 ON " + TABLE_10 + " (boolean3,smallint3,integer3,bigint3,decimal3,real3,double3,float3,char3,varchar3)").execute();
                        spliceClassWatcher.prepareStatement("CREATE INDEX TABLE11 ON " + TABLE_11 + " (boolean3,smallint3,integer3,bigint3,decimal3,real3,double3,float3,char3,varchar3)").execute();
                        spliceClassWatcher.prepareStatement("CREATE INDEX TABLE12 ON " + TABLE_12 + " (boolean3,smallint3,integer3,bigint3,decimal3,real3,double3,float3,char3,varchar3)").execute();
                        spliceClassWatcher.prepareStatement("CREATE INDEX TABLE13 ON " + TABLE_13 + " (boolean3,smallint3,integer3,bigint3,decimal3,real3,double3,float3,char3,varchar3)").execute();
                        spliceClassWatcher.prepareStatement("CREATE INDEX TABLE14 ON " + TABLE_14 + " (boolean3,smallint3,integer3,bigint3,decimal3,real3,double3,float3,char3,varchar3)").execute();
                        spliceClassWatcher.prepareStatement("CREATE INDEX TABLE15 ON " + TABLE_15 + " (boolean3,smallint3,integer3,bigint3,decimal3,real3,double3,float3,char3,varchar3)").execute();
                        spliceClassWatcher.prepareStatement("CREATE INDEX TABLE16 ON " + TABLE_16 + " (boolean3,smallint3,integer3,bigint3,decimal3,real3,double3,float3,char3,varchar3)").execute();
                        spliceClassWatcher.prepareStatement("CREATE INDEX TABLE17 ON " + TABLE_17 + " (boolean3,smallint3,integer3,bigint3,decimal3,real3,double3,float3,char3,varchar3)").execute();
                        spliceClassWatcher.prepareStatement("CREATE INDEX TABLE18 ON " + TABLE_18 + " (boolean3,smallint3,integer3,bigint3,decimal3,real3,double3,float3,char3,varchar3)").execute();
                        spliceClassWatcher.prepareStatement("CREATE INDEX TABLE30 ON " + TABLE_30 + " (charforbitdata3)").execute();
                        spliceClassWatcher.prepareStatement("CREATE INDEX TABLE31 ON " + TABLE_31 + " (varcharforbitdata3)").execute();
                        spliceClassWatcher.prepareStatement("CREATE INDEX TABLE35 ON " + TABLE_35 + " (charforbitdata3)").execute();
                        spliceClassWatcher.prepareStatement("CREATE INDEX TABLE36 ON " + TABLE_36 + " (charforbitdata3,varcharforbitdata3)").execute();
                        spliceClassWatcher.prepareStatement("CREATE INDEX TABLE37 ON " + TABLE_37 + " (charforbitdata3,varcharforbitdata3,date3)").execute();
                        spliceClassWatcher.prepareStatement("CREATE INDEX TABLE38 ON " + TABLE_38 + " (charforbitdata3,varcharforbitdata3,date3,time3)").execute();
                        spliceClassWatcher.prepareStatement("CREATE INDEX TABLE39 ON " + TABLE_39 + " (charforbitdata3,varcharforbitdata3,date3,time3,timestamp2)").execute();
                        spliceClassWatcher.prepareStatement("CREATE INDEX TABLE41 ON " + TABLE_41 + " (varcharforbitdata3,date3,time3,timestamp2)").execute();
                        spliceClassWatcher.prepareStatement("CREATE INDEX TABLE50 ON " + TABLE_50 + " (boolean3,smallint3,integer3,bigint3,decimal3,real3,double3,float3,char3,varchar3,varcharforbitdata3,date3,time3,timestamp2)").execute();
                        spliceClassWatcher.prepareStatement("CREATE INDEX TABLE51 ON " + TABLE_51 + " (boolean3,smallint3,integer3,bigint3,decimal3,real3,double3,float3,char3,varchar3,varcharforbitdata3,date3,time3,timestamp2)").execute();

                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    } finally {
                        spliceClassWatcher.closeAll();
                    }
                }

            });

    @Before
    public void setUp() throws Exception {
        if (!done) {
            try {
                for (String btable : bTables) {
                    if (btable.equals(TABLE_50) || btable.equals(TABLE_51)) continue;
                    String i1 = "insert into " + CLASS_NAME + "." + btable + "(lvarchar1,lvarchar2,lvarchar3"
                            + ",charforbitdata1,charforbitdata2,charforbitdata3"
                            + ",varcharforbitdata1,varcharforbitdata2,varcharforbitdata3"
                            + ",longvarcharforbitdata1,longvarcharforbitdata2,longvarcharforbitdata3"
                            + ",date1,date2,date3"
                            + ",time1,time2,time3"
                            + ",timestamp1,timestamp2,timestamp3) "
                            + "values ('aaaaaaaaaaaaaaaaaaaaaaaaaa','aaaaaaaaaaaaaaaaaaaaaaaaaa','aaaaaaaaaaaaaaaaaaaaaaaaaa'"
                            + ",X'ABCDEF',X'ABCDEF',X'ABCDEF'"
                            + ",X'1234abcdef',X'1234abcdef',X'1234abcdef'"
                            + ",X'1234abcdef',X'1234abcdef',X'1234abcdef'"
                            + ",'2014-05-01','2014-05-01','2014-05-01'"
                            + ",'05:05:05','05:05:05','05:05:05'"
                            + ",'2014-05-01 00:00:00','2014-05-01 00:00:00','2014-05-01 00:00:00')";

                    String i2 = "insert into " + CLASS_NAME + "." + btable + "(lvarchar1,lvarchar2,lvarchar3"
                            + ",charforbitdata1,charforbitdata2,charforbitdata3"
                            + ",varcharforbitdata1,varcharforbitdata2,varcharforbitdata3"
                            + ",longvarcharforbitdata1,longvarcharforbitdata2,longvarcharforbitdata3"
                            + ",date1,date2,date3"
                            + ",time1,time2,time3"
                            + ",timestamp1,timestamp2,timestamp3) "
                            + "values ('bbbbbbbbbbbbbbbbbbbbbbbbbb','bbbbbbbbbbbbbbbbbbbbbbbbbb','bbbbbbbbbbbbbbbbbbbbbbbbbb'"
                            + ",X'BCDEFA',X'BCDEFA',X'BCDEFA'"
                            + ",X'234abcdef1',X'234abcdef1',X'234abcdef1'"
                            + ",X'234abcdef1',X'234abcdef1',X'234abcdef1'"
                            + ",'2014-05-02','2014-05-02','2014-05-02'"
                            + ",'06:06:06','06:06:06','06:06:06'"
                            + ",'2014-05-02 00:00:00','2014-05-02 00:00:00','2014-05-02 00:00:00')";

                    String i3 = "insert into " + CLASS_NAME + "." + btable + "(lvarchar1,lvarchar2,lvarchar3"
                            + ",charforbitdata1,charforbitdata2,charforbitdata3"
                            + ",varcharforbitdata1,varcharforbitdata2,varcharforbitdata3"
                            + ",longvarcharforbitdata1,longvarcharforbitdata2,longvarcharforbitdata3"
                            + ",date1,date2,date3"
                            + ",time1,time2,time3"
                            + ",timestamp1,timestamp2,timestamp3) "
                            + "values ('cccccccccccccccccccccccccc','cccccccccccccccccccccccccc','cccccccccccccccccccccccccc'"
                            + ",X'CDEFAB',X'CDEFAB',X'CDEFAB'"
                            + ",X'34abcdef12',X'34abcdef12',X'34abcdef12'"
                            + ",X'34abcdef12',X'34abcdef12',X'34abcdef12'"
                            + ",'2014-05-03','2014-05-03','2014-05-03'"
                            + ",'07:07:07','07:07:07','07:07:07'"
                            + ",'2014-05-03 00:00:00','2014-05-03 00:00:00','2014-05-03 00:00:00')";
                    methodWatcher.executeUpdate(i1);
                    methodWatcher.executeUpdate(i2);
                    methodWatcher.executeUpdate(i3);
                }
                String ii1 = "insert into " + CLASS_NAME + "." + TABLE_50 + "(boolean1, boolean2,boolean3"
                        + ",smallint1, smallint2,smallint3"
                        + ",integer1,integer2,integer3"
                        + ",bigint1,bigint2,bigint3"
                        + ",decimal1,decimal2,decimal3"
                        + ",real1, real2,real3"
                        + ",double1,double2,double3"
                        + ",float1,float2,float3"
                        + ",char1,char2,char3"
                        + ",varchar1,varchar2,varchar3"
                        + ",lvarchar1,lvarchar2,lvarchar3"
                        + ",charforbitdata1,charforbitdata2,charforbitdata3"
                        + ",varcharforbitdata1,varcharforbitdata2,varcharforbitdata3"
                        + ",longvarcharforbitdata1,longvarcharforbitdata2,longvarcharforbitdata3"
                        + ",date1,date2,date3"
                        + ",time1,time2,time3"
                        + ",timestamp1,timestamp2,timestamp3) "
                        + "values ('true','true','false', 1,2,3, 1,2,3, 1,2,3, 1,2,3, 1.0,2.0,3.0, 1.0,2.0,3.0, 1.0,2.0,3.0, 'a','c','d', 'a','c','d',"
                        + "'aaaaaaaaaaaaaaaaaaaaaaaaaa','aaaaaaaaaaaaaaaaaaaaaaaaaa','aaaaaaaaaaaaaaaaaaaaaaaaaa'"
                        + ",X'ABCDEF',X'ABCDEF',X'ABCDEF'"
                        + ",X'1234abcdef',X'1234abcdef',X'1234abcdef'"
                        + ",X'1234abcdef',X'1234abcdef',X'1234abcdef'"
                        + ",'2014-05-01','2014-05-01','2014-05-01'"
                        + ",'05:05:05','05:05:05','05:05:05'"
                        + ",'2014-05-01 00:00:00','2014-05-01 00:00:00','2014-05-01 00:00:00')";

                String ii2 = "insert into " + CLASS_NAME + "." + TABLE_50 + "(boolean1, boolean2,boolean3"
                        + ",smallint1, smallint2,smallint3"
                        + ",integer1,integer2,integer3"
                        + ",bigint1,bigint2,bigint3"
                        + ",decimal1,decimal2,decimal3"
                        + ",real1, real2,real3"
                        + ",double1,double2,double3"
                        + ",float1,float2,float3"
                        + ",char1,char2,char3"
                        + ",varchar1,varchar2,varchar3"
                        + ",lvarchar1,lvarchar2,lvarchar3"
                        + ",charforbitdata1,charforbitdata2,charforbitdata3"
                        + ",varcharforbitdata1,varcharforbitdata2,varcharforbitdata3"
                        + ",longvarcharforbitdata1,longvarcharforbitdata2,longvarcharforbitdata3"
                        + ",date1,date2,date3"
                        + ",time1,time2,time3"
                        + ",timestamp1,timestamp2,timestamp3) "
                        + "values ('true','false','true', 2,4,2, 2,4,2, 2,4,2, 2,4,2, 2.0,4.0,2.0, 2.0,4.0,2.0, 2.0,4.0,2.0, 'b','b','b', 'b','b','b',"
                        + "'bbbbbbbbbbbbbbbbbbbbbbbbbb','bbbbbbbbbbbbbbbbbbbbbbbbbb','bbbbbbbbbbbbbbbbbbbbbbbbbb'"
                        + ",X'BCDEFA',X'BCDEFA',X'BCDEFA'"
                        + ",X'234abcdef1',X'234abcdef1',X'234abcdef1'"
                        + ",X'234abcdef1',X'234abcdef1',X'234abcdef1'"
                        + ",'2014-05-02','2014-05-02','2014-05-02'"
                        + ",'06:06:06','06:06:06','06:06:06'"
                        + ",'2014-05-02 00:00:00','2014-05-02 00:00:00','2014-05-02 00:00:00')";

                String ii3 = "insert into " + CLASS_NAME + "." + TABLE_50 + "(boolean1,boolean2,boolean3"
                        + ",smallint1, smallint2,smallint3"
                        + ",integer1,integer2,integer3"
                        + ",bigint1,bigint2,bigint3"
                        + ",decimal1,decimal2,decimal3"
                        + ",real1, real2,real3"
                        + ",double1,double2,double3"
                        + ",float1,float2,float3"
                        + ",char1,char2,char3"
                        + ",varchar1,varchar2,varchar3"
                        + ",lvarchar1,lvarchar2,lvarchar3"
                        + ",charforbitdata1,charforbitdata2,charforbitdata3"
                        + ",varcharforbitdata1,varcharforbitdata2,varcharforbitdata3"
                        + ",longvarcharforbitdata1,longvarcharforbitdata2,longvarcharforbitdata3"
                        + ",date1,date2,date3"
                        + ",time1,time2,time3"
                        + ",timestamp1,timestamp2,timestamp3) "
                        + "values ('false','true','true', 3,2,1, 3,1,2, 3,1,2, 3,3,1, 3.0,3.0,1.0, 3.0,3.0,1.0, 3.0,3.0,1.0, 'c','a','e', 'c','a','e',"
                        + "'cccccccccccccccccccccccccc','cccccccccccccccccccccccccc','cccccccccccccccccccccccccc'"
                        + ",X'CDEFAB',X'CDEFAB',X'CDEFAB'"
                        + ",X'34abcdef12',X'34abcdef12',X'34abcdef12'"
                        + ",X'34abcdef12',X'34abcdef12',X'34abcdef12'"
                        + ",'2014-05-03','2014-05-03','2014-05-03'"
                        + ",'07:07:07','07:07:07','07:07:07'"
                        + ",'2014-05-03 00:00:00','2014-05-03 00:00:00','2014-05-03 00:00:00')";
                methodWatcher.executeUpdate(ii1);
                methodWatcher.executeUpdate(ii2);
                methodWatcher.executeUpdate(ii3);
                ii1 = "insert into " + CLASS_NAME + "." + TABLE_51 + "(boolean1, boolean2,boolean3"
                        + ",smallint1, smallint2,smallint3"
                        + ",integer1,integer2,integer3"
                        + ",bigint1,bigint2,bigint3"
                        + ",decimal1,decimal2,decimal3"
                        + ",real1, real2,real3"
                        + ",double1,double2,double3"
                        + ",float1,float2,float3"
                        + ",char1,char2,char3"
                        + ",varchar1,varchar2,varchar3"
                        + ",lvarchar1,lvarchar2,lvarchar3"
                        + ",charforbitdata1,charforbitdata2,charforbitdata3"
                        + ",varcharforbitdata1,varcharforbitdata2,varcharforbitdata3"
                        + ",longvarcharforbitdata1,longvarcharforbitdata2,longvarcharforbitdata3"
                        + ",date1,date2,date3"
                        + ",time1,time2,time3"
                        + ",timestamp1,timestamp2,timestamp3) "
                        + "values ('true','true','false', 1,2,3, 1,2,3, 1,2,3, 1,2,3, 1.0,2.0,3.0, 1.0,2.0,3.0, 1.0,2.0,3.0, 'a','c','d', 'a','c','d',"
                        + "'aaaaaaaaaaaaaaaaaaaaaaaaaa','aaaaaaaaaaaaaaaaaaaaaaaaaa','aaaaaaaaaaaaaaaaaaaaaaaaaa'"
                        + ",X'ABCDEF',X'ABCDEF',X'ABCDEF'"
                        + ",X'1234abcdef',X'1234abcdef',X'1234abcdef'"
                        + ",X'1234abcdef',X'1234abcdef',X'1234abcdef'"
                        + ",'2014-05-01','2014-05-01','2014-05-01'"
                        + ",'05:05:05','05:05:05','05:05:05'"
                        + ",'2014-05-01 00:00:00','2014-05-01 00:00:00','2014-05-01 00:00:00')";

                ii2 = "insert into " + CLASS_NAME + "." + TABLE_51 + "(boolean1, boolean2,boolean3"
                        + ",smallint1, smallint2,smallint3"
                        + ",integer1,integer2,integer3"
                        + ",bigint1,bigint2,bigint3"
                        + ",decimal1,decimal2,decimal3"
                        + ",real1, real2,real3"
                        + ",double1,double2,double3"
                        + ",float1,float2,float3"
                        + ",char1,char2,char3"
                        + ",varchar1,varchar2,varchar3"
                        + ",lvarchar1,lvarchar2,lvarchar3"
                        + ",charforbitdata1,charforbitdata2,charforbitdata3"
                        + ",varcharforbitdata1,varcharforbitdata2,varcharforbitdata3"
                        + ",longvarcharforbitdata1,longvarcharforbitdata2,longvarcharforbitdata3"
                        + ",date1,date2,date3"
                        + ",time1,time2,time3"
                        + ",timestamp1,timestamp2,timestamp3) "
                        + "values ('true','false','true', 2,4,2, 2,4,2, 2,4,2, 2,4,2, 2.0,4.0,2.0, 2.0,4.0,2.0, 2.0,4.0,2.0, 'b','b','b', 'b','b','b',"
                        + "'bbbbbbbbbbbbbbbbbbbbbbbbbb','bbbbbbbbbbbbbbbbbbbbbbbbbb','bbbbbbbbbbbbbbbbbbbbbbbbbb'"
                        + ",X'BCDEFA',X'BCDEFA',X'BCDEFA'"
                        + ",X'234abcdef1',X'234abcdef1',X'234abcdef1'"
                        + ",X'234abcdef1',X'234abcdef1',X'234abcdef1'"
                        + ",'2014-05-02','2014-05-02','2014-05-02'"
                        + ",'06:06:06','06:06:06','06:06:06'"
                        + ",'2014-05-02 00:00:00','2014-05-02 00:00:00','2014-05-02 00:00:00')";

                ii3 = "insert into " + CLASS_NAME + "." + TABLE_51 + "(boolean1,boolean2,boolean3"
                        + ",smallint1, smallint2,smallint3"
                        + ",integer1,integer2,integer3"
                        + ",bigint1,bigint2,bigint3"
                        + ",decimal1,decimal2,decimal3"
                        + ",real1, real2,real3"
                        + ",double1,double2,double3"
                        + ",float1,float2,float3"
                        + ",char1,char2,char3"
                        + ",varchar1,varchar2,varchar3"
                        + ",lvarchar1,lvarchar2,lvarchar3"
                        + ",charforbitdata1,charforbitdata2,charforbitdata3"
                        + ",varcharforbitdata1,varcharforbitdata2,varcharforbitdata3"
                        + ",longvarcharforbitdata1,longvarcharforbitdata2,longvarcharforbitdata3"
                        + ",date1,date2,date3"
                        + ",time1,time2,time3"
                        + ",timestamp1,timestamp2,timestamp3) "
                        + "values ('false','true','true', 3,2,1, 3,1,2, 3,1,2, 3,3,1, 3.0,3.0,1.0, 3.0,3.0,1.0, 3.0,3.0,1.0, 'c','a','e', 'c','a','e',"
                        + "'cccccccccccccccccccccccccc','cccccccccccccccccccccccccc','cccccccccccccccccccccccccc'"
                        + ",X'CDEFAB',X'CDEFAB',X'CDEFAB'"
                        + ",X'34abcdef12',X'34abcdef12',X'34abcdef12'"
                        + ",X'34abcdef12',X'34abcdef12',X'34abcdef12'"
                        + ",'2014-05-03','2014-05-03','2014-05-03'"
                        + ",'07:07:07','07:07:07','07:07:07'"
                        + ",'2014-05-03 00:00:00','2014-05-03 00:00:00','2014-05-03 00:00:00')";
                methodWatcher.executeUpdate(ii1);
                methodWatcher.executeUpdate(ii2);
                methodWatcher.executeUpdate(ii3);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            done = true;
        }
    }

    private void runAndTestQueryRI(String query, int lookFor, String field) throws Exception {
        ResultSet rs = methodWatcher.executeQuery(query);
        if (rs.next()) {
            assertEquals(lookFor, rs.getInt(field));
        }
        rs.close();
    }

    private void runAndTestQueryRI3(String query, int lf[], String field) throws Exception {
        int pos = 0;
        ResultSet rs = methodWatcher.executeQuery(query);
        while (rs.next()) {
            assertEquals(lf[pos++], rs.getInt(field));
        }
        rs.close();
    }

    private void runAndTestQueryRD(String query, double lookFor, String field) throws Exception {
        ResultSet rs = methodWatcher.executeQuery(query);
        if (rs.next()) {
            assertEquals(lookFor, rs.getDouble(field), 0.0001);
        }
        rs.close();
    }

    private void runAndTestQueryRS(String query, String expectedValue, String column) throws Exception {
        ResultSet rs = methodWatcher.executeQuery(query);
        if (rs.next()) {
            assertEquals(expectedValue, rs.getString(column));
        }
        rs.close();
    }

    private void runAndTestQueryRB(String query, boolean lookFor, String field) throws Exception {
        ResultSet rs = methodWatcher.executeQuery(query);
        if (rs.next()) {
            assertEquals(lookFor, rs.getInt(field) == 1);
        }
        rs.close();
    }

    private void runAndTestQueryR3(String query, int lookFor1, int lookFor2, int lookFor3, String field) throws Exception {
        ResultSet rs = methodWatcher.executeQuery(query);
        while (rs.next()) {
            assertTrue(rs.getInt(field) == lookFor1 || rs.getInt(field) == lookFor2 || rs.getInt(field) == lookFor3);
        }
        rs.close();
    }

    private void runAndTestQueryJ2(String query, int A1[][], String B1[][], String C1[][]) throws Exception {
        ResultSet rs = methodWatcher.executeQuery(query);
        while (rs.next()) {
            int r1 = rs.getInt(C1[0][0]);
            int r2 = rs.getInt(C1[0][1]);
            int r3 = rs.getInt(C1[1][0]);
            int r4 = rs.getInt(C1[1][1]);
            String c1 = rs.getString(C1[2][0]);
            String c2 = rs.getString(C1[2][1]);
            String c3 = rs.getString(C1[3][0]);
            String c4 = rs.getString(C1[3][1]);
            assertTrue(A1[0][0] == r1 && A1[1][0] == r2 && A1[2][0] == r3 && A1[3][0] == r4 && B1[0][0].equals(c1.trim()) && B1[1][0].equals(c2.trim()) && B1[2][0].equals(c3.trim()) && B1[3][0].equals(c4.trim()) || A1[0][1] == r1 && A1[1][1] == r2 && A1[2][1] == r3 && A1[3][1] == r4 && B1[0][1].equals(c1.trim()) && B1[1][1].equals(c2.trim()) && B1[2][1].equals(c3.trim()) && B1[3][1].equals(c4.trim()));
        }
        rs.close();
    }

    private void runAndTestQueryJ3(String query, int A1[][], String B1[][], String C1[][]) throws Exception {
        ResultSet rs = methodWatcher.executeQuery(query);
        while (rs.next()) {
            int r1 = rs.getInt(C1[0][0]);
            int r2 = rs.getInt(C1[0][1]);
            int r3 = rs.getInt(C1[1][0]);
            int r4 = rs.getInt(C1[1][1]);
            String c1 = rs.getString(C1[2][0]);
            String c2 = rs.getString(C1[2][1]);
            String c3 = rs.getString(C1[3][0]);
            String c4 = rs.getString(C1[3][1]);
            assertTrue(A1[0][0] == r1 && A1[1][0] == r2 && A1[2][0] == r3 && A1[3][0] == r4 && B1[0][0].equals(c1.trim()) && B1[1][0].equals(c2.trim()) && B1[2][0].equals(c3.trim()) && B1[3][0].equals(c4.trim()) || A1[0][1] == r1 && A1[1][1] == r2 && A1[2][1] == r3 && A1[3][1] == r4 && B1[0][1].equals(c1.trim()) && B1[1][1].equals(c2.trim()) && B1[2][1].equals(c3.trim()) && B1[3][1].equals(c4.trim()) || A1[0][2] == r1 && A1[1][2] == r2 && A1[2][2] == r3 && A1[3][2] == r4 && B1[0][2].equals(c1.trim()) && B1[1][2].equals(c2.trim()) && B1[2][2].equals(c3.trim()) && B1[3][2].equals(c4.trim()));
        }
        rs.close();
    }

    private void runAndTestQueryR3D(String query, double lookfor1, double lookfor2, double lookfor3, String field) throws Exception {
        ResultSet rs = methodWatcher.executeQuery(query);
        while (rs.next()) {
            Double returnVal = rs.getDouble(field);
            assertTrue(Math.abs(lookfor1 - returnVal) < 0.0001 || Math.abs(lookfor2 - returnVal) < 0.0001 || Math.abs(lookfor3 - returnVal) < 0.0001);
        }
        rs.close();
    }

    private void runAndTestQueryR3S(String query, String lookfor1, String lookfor2, String lookfor3, String field) throws Exception {
        ResultSet rs = methodWatcher.executeQuery(query);
        while (rs.next()) {
            assertTrue(rs.getString(field).equals(lookfor1) || rs.getString(field).equals(lookfor2) || rs.getString(field).equals(lookfor3));
        }
        rs.close();
    }

    @Test
    public void testBoolean() throws Exception {
        try {
            for (String table : tables) {
                runAndTestQueryRB("select * from " + CLASS_NAME + "." + table + " where boolean1 = true", true, "boolean1");
                runAndTestQueryRB("select count(*) as retval from " + CLASS_NAME + "." + table + " group by boolean1 having boolean1 = false", true, "retval");
                runAndTestQueryRB("select * from " + CLASS_NAME + "." + table + " where boolean2 = true", true, "boolean2");
                runAndTestQueryRB("select count(*) as retval from " + CLASS_NAME + "." + table + " group by boolean2 having boolean2 = false", true, "retval");
                runAndTestQueryRB("select * from " + CLASS_NAME + "." + table + " where boolean3 = true", true, "boolean3");
                runAndTestQueryRB("select count(*) as retval from " + CLASS_NAME + "." + table + " group by boolean3 having boolean3 = false", true, "retval");
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    public void testSmallInt() throws Exception {
        for (String table : tables) {
            runAndTestQueryRI("select * from " + CLASS_NAME + "." + table + " where smallint1 = 1", 1, "smallint1");
            runAndTestQueryR3("select * from " + CLASS_NAME + "." + table + " where smallint1 between 1 and 3", 1, 2, 3, "smallint1");
            runAndTestQueryRI("select count(*) as retval from " + CLASS_NAME + "." + table + " group by smallint1 having smallint1 = 2", 1, "retval");
            runAndTestQueryRI("select * from " + CLASS_NAME + "." + table + " where smallint2 = 1", 3, "smallint2");
            runAndTestQueryR3("select * from " + CLASS_NAME + "." + table + " where smallint2 between 1 and 3", 1, 2, 3, "smallint2");
            runAndTestQueryRI("select count(*) as retval from " + CLASS_NAME + "." + table + " group by smallint2 having smallint2 = 2", 2, "retval");
            runAndTestQueryRI("select * from " + CLASS_NAME + "." + table + " where smallint3 = 1", 1, "smallint3");
            runAndTestQueryR3("select * from " + CLASS_NAME + "." + table + " where smallint3 between 1 and 3", 1, 2, 3, "smallint3");
            runAndTestQueryRI("select count(*) as retval from " + CLASS_NAME + "." + table + " group by smallint3 having smallint3 = 2", 1, "retval");
            runAndTestQueryRI("select count(*) as retval from " + CLASS_NAME + "." + table + " where smallint1 < 1", 0, "retval");
            runAndTestQueryRI("select count(*) as retval from " + CLASS_NAME + "." + table + " where smallint1 < 2", 1, "retval");
            runAndTestQueryRI("select count(*) as retval from " + CLASS_NAME + "." + table + " where smallint1 < 3", 2, "retval");
            runAndTestQueryRI("select count(*) as retval from " + CLASS_NAME + "." + table + " where smallint1 < 4", 3, "retval");
            runAndTestQueryRI("select count(*) as retval from " + CLASS_NAME + "." + table + " where smallint2 < 1", 0, "retval");
            runAndTestQueryRI("select count(*) as retval from " + CLASS_NAME + "." + table + " where smallint2 < 2", 0, "retval");
            runAndTestQueryRI("select count(*) as retval from " + CLASS_NAME + "." + table + " where smallint2 < 3", 2, "retval");
            runAndTestQueryRI("select count(*) as retval from " + CLASS_NAME + "." + table + " where smallint2 < 4", 2, "retval");
            runAndTestQueryRI("select count(*) as retval from " + CLASS_NAME + "." + table + " where smallint3 < 1", 0, "retval");
            runAndTestQueryRI("select count(*) as retval from " + CLASS_NAME + "." + table + " where smallint3 < 2", 1, "retval");
            runAndTestQueryRI("select count(*) as retval from " + CLASS_NAME + "." + table + " where smallint3 < 3", 2, "retval");
            runAndTestQueryRI("select count(*) as retval from " + CLASS_NAME + "." + table + " where smallint3 < 4", 3, "retval");
            runAndTestQueryRI("select count(*) as retval from " + CLASS_NAME + "." + table + " where smallint1 <= 1", 1, "retval");
            runAndTestQueryRI("select count(*) as retval from " + CLASS_NAME + "." + table + " where smallint1 <= 2", 2, "retval");
            runAndTestQueryRI("select count(*) as retval from " + CLASS_NAME + "." + table + " where smallint1 <= 3", 3, "retval");
            runAndTestQueryRI("select count(*) as retval from " + CLASS_NAME + "." + table + " where smallint1 <= 4", 3, "retval");
            runAndTestQueryRI("select count(*) as retval from " + CLASS_NAME + "." + table + " where smallint2 <= 1", 0, "retval");
            runAndTestQueryRI("select count(*) as retval from " + CLASS_NAME + "." + table + " where smallint2 <= 2", 2, "retval");
            runAndTestQueryRI("select count(*) as retval from " + CLASS_NAME + "." + table + " where smallint2 <= 3", 2, "retval");
            runAndTestQueryRI("select count(*) as retval from " + CLASS_NAME + "." + table + " where smallint2 <= 4", 3, "retval");
            runAndTestQueryRI("select count(*) as retval from " + CLASS_NAME + "." + table + " where smallint3 <= 1", 1, "retval");
            runAndTestQueryRI("select count(*) as retval from " + CLASS_NAME + "." + table + " where smallint3 <= 2", 2, "retval");
            runAndTestQueryRI("select count(*) as retval from " + CLASS_NAME + "." + table + " where smallint3 <= 3", 3, "retval");
            runAndTestQueryRI("select count(*) as retval from " + CLASS_NAME + "." + table + " where smallint3 <= 4", 3, "retval");
            runAndTestQueryRI("select count(*) as retval from " + CLASS_NAME + "." + table + " where smallint1 > 0", 3, "retval");
            runAndTestQueryRI("select count(*) as retval from " + CLASS_NAME + "." + table + " where smallint1 > 1", 2, "retval");
            runAndTestQueryRI("select count(*) as retval from " + CLASS_NAME + "." + table + " where smallint1 > 2", 1, "retval");
            runAndTestQueryRI("select count(*) as retval from " + CLASS_NAME + "." + table + " where smallint1 > 3", 0, "retval");
            runAndTestQueryRI("select count(*) as retval from " + CLASS_NAME + "." + table + " where smallint2 > 0", 3, "retval");
            runAndTestQueryRI("select count(*) as retval from " + CLASS_NAME + "." + table + " where smallint2 > 1", 3, "retval");
            runAndTestQueryRI("select count(*) as retval from " + CLASS_NAME + "." + table + " where smallint2 > 2", 1, "retval");
            runAndTestQueryRI("select count(*) as retval from " + CLASS_NAME + "." + table + " where smallint2 > 3", 1, "retval");
            runAndTestQueryRI("select count(*) as retval from " + CLASS_NAME + "." + table + " where smallint3 > 0", 3, "retval");
            runAndTestQueryRI("select count(*) as retval from " + CLASS_NAME + "." + table + " where smallint3 > 1", 2, "retval");
            runAndTestQueryRI("select count(*) as retval from " + CLASS_NAME + "." + table + " where smallint3 > 2", 1, "retval");
            runAndTestQueryRI("select count(*) as retval from " + CLASS_NAME + "." + table + " where smallint3 > 3", 0, "retval");
            runAndTestQueryRI("select count(*) as retval from " + CLASS_NAME + "." + table + " where smallint1 >= 0", 3, "retval");
            runAndTestQueryRI("select count(*) as retval from " + CLASS_NAME + "." + table + " where smallint1 >= 1", 3, "retval");
            runAndTestQueryRI("select count(*) as retval from " + CLASS_NAME + "." + table + " where smallint1 >= 2", 2, "retval");
            runAndTestQueryRI("select count(*) as retval from " + CLASS_NAME + "." + table + " where smallint1 >= 3", 1, "retval");
            runAndTestQueryRI("select count(*) as retval from " + CLASS_NAME + "." + table + " where smallint1 >= 4", 0, "retval");
            runAndTestQueryRI("select count(*) as retval from " + CLASS_NAME + "." + table + " where smallint2 >= 0", 3, "retval");
            runAndTestQueryRI("select count(*) as retval from " + CLASS_NAME + "." + table + " where smallint2 >= 1", 3, "retval");
            runAndTestQueryRI("select count(*) as retval from " + CLASS_NAME + "." + table + " where smallint2 >= 2", 3, "retval");
            runAndTestQueryRI("select count(*) as retval from " + CLASS_NAME + "." + table + " where smallint2 >= 3", 1, "retval");
            runAndTestQueryRI("select count(*) as retval from " + CLASS_NAME + "." + table + " where smallint2 >= 4", 1, "retval");
            runAndTestQueryRI("select count(*) as retval from " + CLASS_NAME + "." + table + " where smallint3 >= 0", 3, "retval");
            runAndTestQueryRI("select count(*) as retval from " + CLASS_NAME + "." + table + " where smallint3 >= 1", 3, "retval");
            runAndTestQueryRI("select count(*) as retval from " + CLASS_NAME + "." + table + " where smallint3 >= 2", 2, "retval");
            runAndTestQueryRI("select count(*) as retval from " + CLASS_NAME + "." + table + " where smallint3 >= 3", 1, "retval");
            runAndTestQueryRI("select count(*) as retval from " + CLASS_NAME + "." + table + " where smallint3 >= 4", 0, "retval");
        }

    }

    @Test
    public void testInteger() throws Exception {

        for (String table : tables) {
            runAndTestQueryRI("select * from " + CLASS_NAME + "." + table + " where integer1 = 1", 1, "integer1");
            runAndTestQueryR3("select * from " + CLASS_NAME + "." + table + " where integer1 between 1 and 3", 1, 2, 3, "integer1");
            runAndTestQueryRI("select count(*) as retval from " + CLASS_NAME + "." + table + " group by integer1 having integer1 = 2", 1, "retval");
            runAndTestQueryRI("select * from " + CLASS_NAME + "." + table + " where integer2 = 1", 1, "integer2");
            runAndTestQueryR3("select * from " + CLASS_NAME + "." + table + " where integer2 between 1 and 3", 1, 2, 3, "integer2");
            runAndTestQueryRI("select count(*) as retval from " + CLASS_NAME + "." + table + " group by integer2 having integer2 = 2", 1, "retval");
            runAndTestQueryRI("select * from " + CLASS_NAME + "." + table + " where integer3 = 1", 1, "integer3");
            runAndTestQueryR3("select * from " + CLASS_NAME + "." + table + " where integer3 between 1 and 3", 1, 2, 3, "integer3");
            runAndTestQueryRI("select count(*) as retval from " + CLASS_NAME + "." + table + " group by integer3 having integer3 = 2", 2, "retval");
            runAndTestQueryRI("select count(*) as retval from " + CLASS_NAME + "." + table + " where integer1 < 1", 0, "retval");
            runAndTestQueryRI("select count(*) as retval from " + CLASS_NAME + "." + table + " where integer1 < 2", 1, "retval");
            runAndTestQueryRI("select count(*) as retval from " + CLASS_NAME + "." + table + " where integer1 < 3", 2, "retval");
            runAndTestQueryRI("select count(*) as retval from " + CLASS_NAME + "." + table + " where integer1 < 4", 3, "retval");
            runAndTestQueryRI("select count(*) as retval from " + CLASS_NAME + "." + table + " where integer2 < 1", 0, "retval");
            runAndTestQueryRI("select count(*) as retval from " + CLASS_NAME + "." + table + " where integer2 < 2", 1, "retval");
            runAndTestQueryRI("select count(*) as retval from " + CLASS_NAME + "." + table + " where integer2 < 3", 2, "retval");
            runAndTestQueryRI("select count(*) as retval from " + CLASS_NAME + "." + table + " where integer2 < 4", 2, "retval");
            runAndTestQueryRI("select count(*) as retval from " + CLASS_NAME + "." + table + " where integer3 < 1", 0, "retval");
            runAndTestQueryRI("select count(*) as retval from " + CLASS_NAME + "." + table + " where integer3 < 2", 0, "retval");
            runAndTestQueryRI("select count(*) as retval from " + CLASS_NAME + "." + table + " where integer3 < 3", 2, "retval");
            runAndTestQueryRI("select count(*) as retval from " + CLASS_NAME + "." + table + " where integer3 < 4", 3, "retval");
            runAndTestQueryRI("select count(*) as retval from " + CLASS_NAME + "." + table + " where integer1 <= 1", 1, "retval");
            runAndTestQueryRI("select count(*) as retval from " + CLASS_NAME + "." + table + " where integer1 <= 2", 2, "retval");
            runAndTestQueryRI("select count(*) as retval from " + CLASS_NAME + "." + table + " where integer1 <= 3", 3, "retval");
            runAndTestQueryRI("select count(*) as retval from " + CLASS_NAME + "." + table + " where integer1 <= 4", 3, "retval");
            runAndTestQueryRI("select count(*) as retval from " + CLASS_NAME + "." + table + " where integer2 <= 1", 1, "retval");
            runAndTestQueryRI("select count(*) as retval from " + CLASS_NAME + "." + table + " where integer2 <= 2", 2, "retval");
            runAndTestQueryRI("select count(*) as retval from " + CLASS_NAME + "." + table + " where integer2 <= 3", 2, "retval");
            runAndTestQueryRI("select count(*) as retval from " + CLASS_NAME + "." + table + " where integer2 <= 4", 3, "retval");
            runAndTestQueryRI("select count(*) as retval from " + CLASS_NAME + "." + table + " where integer3 <= 1", 0, "retval");
            runAndTestQueryRI("select count(*) as retval from " + CLASS_NAME + "." + table + " where integer3 <= 2", 2, "retval");
            runAndTestQueryRI("select count(*) as retval from " + CLASS_NAME + "." + table + " where integer3 <= 3", 3, "retval");
            runAndTestQueryRI("select count(*) as retval from " + CLASS_NAME + "." + table + " where integer3 <= 4", 3, "retval");
            runAndTestQueryRI("select count(*) as retval from " + CLASS_NAME + "." + table + " where integer1 > 0", 3, "retval");
            runAndTestQueryRI("select count(*) as retval from " + CLASS_NAME + "." + table + " where integer1 > 1", 2, "retval");
            runAndTestQueryRI("select count(*) as retval from " + CLASS_NAME + "." + table + " where integer1 > 2", 1, "retval");
            runAndTestQueryRI("select count(*) as retval from " + CLASS_NAME + "." + table + " where integer1 > 3", 0, "retval");
            runAndTestQueryRI("select count(*) as retval from " + CLASS_NAME + "." + table + " where integer2 > 0", 3, "retval");
            runAndTestQueryRI("select count(*) as retval from " + CLASS_NAME + "." + table + " where integer2 > 1", 2, "retval");
            runAndTestQueryRI("select count(*) as retval from " + CLASS_NAME + "." + table + " where integer2 > 2", 1, "retval");
            runAndTestQueryRI("select count(*) as retval from " + CLASS_NAME + "." + table + " where integer2 > 3", 1, "retval");
            runAndTestQueryRI("select count(*) as retval from " + CLASS_NAME + "." + table + " where integer3 > 0", 3, "retval");
            runAndTestQueryRI("select count(*) as retval from " + CLASS_NAME + "." + table + " where integer3 > 1", 3, "retval");
            runAndTestQueryRI("select count(*) as retval from " + CLASS_NAME + "." + table + " where integer3 > 2", 1, "retval");
            runAndTestQueryRI("select count(*) as retval from " + CLASS_NAME + "." + table + " where integer3 > 3", 0, "retval");
            runAndTestQueryRI("select count(*) as retval from " + CLASS_NAME + "." + table + " where integer1 >= 0", 3, "retval");
            runAndTestQueryRI("select count(*) as retval from " + CLASS_NAME + "." + table + " where integer1 >= 1", 3, "retval");
            runAndTestQueryRI("select count(*) as retval from " + CLASS_NAME + "." + table + " where integer1 >= 2", 2, "retval");
            runAndTestQueryRI("select count(*) as retval from " + CLASS_NAME + "." + table + " where integer1 >= 3", 1, "retval");
            runAndTestQueryRI("select count(*) as retval from " + CLASS_NAME + "." + table + " where integer1 >= 4", 0, "retval");
            runAndTestQueryRI("select count(*) as retval from " + CLASS_NAME + "." + table + " where integer2 >= 0", 3, "retval");
            runAndTestQueryRI("select count(*) as retval from " + CLASS_NAME + "." + table + " where integer2 >= 1", 3, "retval");
            runAndTestQueryRI("select count(*) as retval from " + CLASS_NAME + "." + table + " where integer2 >= 2", 2, "retval");
            runAndTestQueryRI("select count(*) as retval from " + CLASS_NAME + "." + table + " where integer2 >= 3", 1, "retval");
            runAndTestQueryRI("select count(*) as retval from " + CLASS_NAME + "." + table + " where integer2 >= 4", 1, "retval");
            runAndTestQueryRI("select count(*) as retval from " + CLASS_NAME + "." + table + " where integer3 >= 0", 3, "retval");
            runAndTestQueryRI("select count(*) as retval from " + CLASS_NAME + "." + table + " where integer3 >= 1", 3, "retval");
            runAndTestQueryRI("select count(*) as retval from " + CLASS_NAME + "." + table + " where integer3 >= 2", 3, "retval");
            runAndTestQueryRI("select count(*) as retval from " + CLASS_NAME + "." + table + " where integer3 >= 3", 1, "retval");
            runAndTestQueryRI("select count(*) as retval from " + CLASS_NAME + "." + table + " where integer3 >= 4", 0, "retval");
        }

    }

    @Test
    public void testBigInt() throws Exception {

        for (String table : tables) {
            runAndTestQueryRI("select * from " + CLASS_NAME + "." + table + " where bigint1 = 1", 1, "bigint1");
            runAndTestQueryR3("select * from " + CLASS_NAME + "." + table + " where bigint1 between 1 and 3", 1, 2, 3, "bigint1");
            runAndTestQueryRI("select count(*) as retval from " + CLASS_NAME + "." + table + " group by bigint1 having bigint1 = 2", 1, "retval");
            runAndTestQueryRI("select * from " + CLASS_NAME + "." + table + " where bigint2 = 1", 1, "bigint2");
            runAndTestQueryR3("select * from " + CLASS_NAME + "." + table + " where bigint2 between 1 and 3", 1, 2, 3, "bigint2");
            runAndTestQueryRI("select count(*) as retval from " + CLASS_NAME + "." + table + " group by bigint2 having bigint2 = 2", 1, "retval");
            runAndTestQueryRI("select * from " + CLASS_NAME + "." + table + " where bigint3 = 1", 1, "bigint3");
            runAndTestQueryR3("select * from " + CLASS_NAME + "." + table + " where bigint3 between 1 and 3", 1, 2, 3, "bigint3");
            runAndTestQueryRI("select count(*) as retval from " + CLASS_NAME + "." + table + " group by bigint3 having bigint3 = 2", 2, "retval");
            runAndTestQueryRI("select count(*) as retval from " + CLASS_NAME + "." + table + " where bigint1 < 1", 0, "retval");
            runAndTestQueryRI("select count(*) as retval from " + CLASS_NAME + "." + table + " where bigint1 < 2", 1, "retval");
            runAndTestQueryRI("select count(*) as retval from " + CLASS_NAME + "." + table + " where bigint1 < 3", 2, "retval");
            runAndTestQueryRI("select count(*) as retval from " + CLASS_NAME + "." + table + " where bigint1 < 4", 3, "retval");
            runAndTestQueryRI("select count(*) as retval from " + CLASS_NAME + "." + table + " where bigint2 < 1", 0, "retval");
            runAndTestQueryRI("select count(*) as retval from " + CLASS_NAME + "." + table + " where bigint2 < 2", 1, "retval");
            runAndTestQueryRI("select count(*) as retval from " + CLASS_NAME + "." + table + " where bigint2 < 3", 2, "retval");
            runAndTestQueryRI("select count(*) as retval from " + CLASS_NAME + "." + table + " where bigint2 < 4", 2, "retval");
            runAndTestQueryRI("select count(*) as retval from " + CLASS_NAME + "." + table + " where bigint3 < 1", 0, "retval");
            runAndTestQueryRI("select count(*) as retval from " + CLASS_NAME + "." + table + " where bigint3 < 2", 0, "retval");
            runAndTestQueryRI("select count(*) as retval from " + CLASS_NAME + "." + table + " where bigint3 < 3", 2, "retval");
            runAndTestQueryRI("select count(*) as retval from " + CLASS_NAME + "." + table + " where bigint3 < 4", 3, "retval");
            runAndTestQueryRI("select count(*) as retval from " + CLASS_NAME + "." + table + " where bigint1 <= 1", 1, "retval");
            runAndTestQueryRI("select count(*) as retval from " + CLASS_NAME + "." + table + " where bigint1 <= 2", 2, "retval");
            runAndTestQueryRI("select count(*) as retval from " + CLASS_NAME + "." + table + " where bigint1 <= 3", 3, "retval");
            runAndTestQueryRI("select count(*) as retval from " + CLASS_NAME + "." + table + " where bigint1 <= 4", 3, "retval");
            runAndTestQueryRI("select count(*) as retval from " + CLASS_NAME + "." + table + " where bigint2 <= 1", 1, "retval");
            runAndTestQueryRI("select count(*) as retval from " + CLASS_NAME + "." + table + " where bigint2 <= 2", 2, "retval");
            runAndTestQueryRI("select count(*) as retval from " + CLASS_NAME + "." + table + " where bigint2 <= 3", 2, "retval");
            runAndTestQueryRI("select count(*) as retval from " + CLASS_NAME + "." + table + " where bigint2 <= 4", 3, "retval");
            runAndTestQueryRI("select count(*) as retval from " + CLASS_NAME + "." + table + " where bigint3 <= 1", 0, "retval");
            runAndTestQueryRI("select count(*) as retval from " + CLASS_NAME + "." + table + " where bigint3 <= 2", 2, "retval");
            runAndTestQueryRI("select count(*) as retval from " + CLASS_NAME + "." + table + " where bigint3 <= 3", 3, "retval");
            runAndTestQueryRI("select count(*) as retval from " + CLASS_NAME + "." + table + " where bigint3 <= 4", 3, "retval");
            runAndTestQueryRI("select count(*) as retval from " + CLASS_NAME + "." + table + " where bigint1 > 0", 3, "retval");
            runAndTestQueryRI("select count(*) as retval from " + CLASS_NAME + "." + table + " where bigint1 > 1", 2, "retval");
            runAndTestQueryRI("select count(*) as retval from " + CLASS_NAME + "." + table + " where bigint1 > 2", 1, "retval");
            runAndTestQueryRI("select count(*) as retval from " + CLASS_NAME + "." + table + " where bigint1 > 3", 0, "retval");
            runAndTestQueryRI("select count(*) as retval from " + CLASS_NAME + "." + table + " where bigint2 > 0", 3, "retval");
            runAndTestQueryRI("select count(*) as retval from " + CLASS_NAME + "." + table + " where bigint2 > 1", 2, "retval");
            runAndTestQueryRI("select count(*) as retval from " + CLASS_NAME + "." + table + " where bigint2 > 2", 1, "retval");
            runAndTestQueryRI("select count(*) as retval from " + CLASS_NAME + "." + table + " where bigint2 > 3", 1, "retval");
            runAndTestQueryRI("select count(*) as retval from " + CLASS_NAME + "." + table + " where bigint3 > 0", 3, "retval");
            runAndTestQueryRI("select count(*) as retval from " + CLASS_NAME + "." + table + " where bigint3 > 1", 3, "retval");
            runAndTestQueryRI("select count(*) as retval from " + CLASS_NAME + "." + table + " where bigint3 > 2", 1, "retval");
            runAndTestQueryRI("select count(*) as retval from " + CLASS_NAME + "." + table + " where bigint3 > 3", 0, "retval");
            runAndTestQueryRI("select count(*) as retval from " + CLASS_NAME + "." + table + " where bigint1 >= 0", 3, "retval");
            runAndTestQueryRI("select count(*) as retval from " + CLASS_NAME + "." + table + " where bigint1 >= 1", 3, "retval");
            runAndTestQueryRI("select count(*) as retval from " + CLASS_NAME + "." + table + " where bigint1 >= 2", 2, "retval");
            runAndTestQueryRI("select count(*) as retval from " + CLASS_NAME + "." + table + " where bigint1 >= 3", 1, "retval");
            runAndTestQueryRI("select count(*) as retval from " + CLASS_NAME + "." + table + " where bigint1 >= 4", 0, "retval");
            runAndTestQueryRI("select count(*) as retval from " + CLASS_NAME + "." + table + " where bigint2 >= 0", 3, "retval");
            runAndTestQueryRI("select count(*) as retval from " + CLASS_NAME + "." + table + " where bigint2 >= 1", 3, "retval");
            runAndTestQueryRI("select count(*) as retval from " + CLASS_NAME + "." + table + " where bigint2 >= 2", 2, "retval");
            runAndTestQueryRI("select count(*) as retval from " + CLASS_NAME + "." + table + " where bigint2 >= 3", 1, "retval");
            runAndTestQueryRI("select count(*) as retval from " + CLASS_NAME + "." + table + " where bigint2 >= 4", 1, "retval");
            runAndTestQueryRI("select count(*) as retval from " + CLASS_NAME + "." + table + " where bigint3 >= 0", 3, "retval");
            runAndTestQueryRI("select count(*) as retval from " + CLASS_NAME + "." + table + " where bigint3 >= 1", 3, "retval");
            runAndTestQueryRI("select count(*) as retval from " + CLASS_NAME + "." + table + " where bigint3 >= 2", 3, "retval");
            runAndTestQueryRI("select count(*) as retval from " + CLASS_NAME + "." + table + " where bigint3 >= 3", 1, "retval");
            runAndTestQueryRI("select count(*) as retval from " + CLASS_NAME + "." + table + " where bigint3 >= 4", 0, "retval");
        }

    }

    @Test
    public void testDecimal() throws Exception {

        for (String table : tables) {
            runAndTestQueryRD("select * from " + CLASS_NAME + "." + table + " where decimal1 = 1", 1.0, "decimal1");
            runAndTestQueryR3D("select * from " + CLASS_NAME + "." + table + " where decimal1 between 1.0 and 3.0", 1.0, 2.0, 3.0, "decimal1");
            runAndTestQueryRI("select count(*) as retval from " + CLASS_NAME + "." + table + " group by decimal1 having decimal1 = 2.0", 1, "retval");
            runAndTestQueryRD("select * from " + CLASS_NAME + "." + table + " where decimal2 = 1.0", 1.0, "decimal2");
            runAndTestQueryR3D("select * from " + CLASS_NAME + "." + table + " where decimal2 between 1 and 3", 1.0, 2.0, 3.0, "decimal2");
            runAndTestQueryRI("select count(*) as retval from " + CLASS_NAME + "." + table + " group by decimal2 having decimal2 = 2.0", 1, "retval");
            runAndTestQueryRD("select * from " + CLASS_NAME + "." + table + " where decimal3 = 1.0", 1.0, "decimal3");
            runAndTestQueryR3D("select * from " + CLASS_NAME + "." + table + " where decimal3 between 1 and 3", 1.0, 2.0, 3.0, "decimal3");
            runAndTestQueryRI("select count(*) as retval from " + CLASS_NAME + "." + table + " group by decimal3 having decimal3 = 2.0", 1, "retval");
            runAndTestQueryRI("select count(*) as retval from " + CLASS_NAME + "." + table + " group by decimal1 having decimal1 < 2.0", 1, "retval");
            runAndTestQueryRI("select count(*) as retval from " + CLASS_NAME + "." + table + " group by decimal2 having decimal2 < 2.0", 1, "retval");
            runAndTestQueryRI("select count(*) as retval from " + CLASS_NAME + "." + table + " group by decimal3 having decimal3 < 2.0", 1, "retval");
            runAndTestQueryRI("select count(*) as retval from " + CLASS_NAME + "." + table + " group by decimal1 having decimal1 <= 2.0", 1, "retval");
            runAndTestQueryRI("select count(*) as retval from " + CLASS_NAME + "." + table + " group by decimal2 having decimal2 <= 2.0", 1, "retval");
            runAndTestQueryRI("select count(*) as retval from " + CLASS_NAME + "." + table + " group by decimal3 having decimal3 <= 2.0", 1, "retval");
            runAndTestQueryRI("select count(*) as retval from " + CLASS_NAME + "." + table + " group by decimal1 having decimal1 > 2.0", 1, "retval");
            runAndTestQueryRI("select count(*) as retval from " + CLASS_NAME + "." + table + " group by decimal2 having decimal2 > 2.0", 1, "retval");
            runAndTestQueryRI("select count(*) as retval from " + CLASS_NAME + "." + table + " group by decimal3 having decimal3 > 2.0", 1, "retval");
            runAndTestQueryRI("select count(*) as retval from " + CLASS_NAME + "." + table + " group by decimal1 having decimal1 >= 2.0", 1, "retval");
            runAndTestQueryRI("select count(*) as retval from " + CLASS_NAME + "." + table + " group by decimal2 having decimal2 >= 2.0", 1, "retval");
            runAndTestQueryRI("select count(*) as retval from " + CLASS_NAME + "." + table + " group by decimal3 having decimal3 >= 2.0", 1, "retval");
        }

    }

    @Test
    public void testReal() throws Exception {

        for (String table : tables) {
            runAndTestQueryRD("select * from " + CLASS_NAME + "." + table + " where real1 = 1", 1.0, "real1");
            runAndTestQueryR3D("select * from " + CLASS_NAME + "." + table + " where real1 between 1.5 and 2.5", 1.5, 2.0, 2.5, "real1");
            runAndTestQueryRI("select count(*) as retval from " + CLASS_NAME + "." + table + " group by real1 having real1 = 2.0", 1, "retval");
            runAndTestQueryRD("select * from " + CLASS_NAME + "." + table + " where real2 = 1", 1.0, "real2");
            runAndTestQueryR3D("select * from " + CLASS_NAME + "." + table + " where real2 between 1.5 and 2.5", 1.5, 2.0, 2.5, "real2");
            runAndTestQueryRI("select count(*) as retval from " + CLASS_NAME + "." + table + " group by real2 having real2 = 2.0", 1, "retval");
            runAndTestQueryRD("select * from " + CLASS_NAME + "." + table + " where real3 = 1", 1.0, "real3");
            runAndTestQueryR3D("select * from " + CLASS_NAME + "." + table + " where real3 between 1.5 and 2.5", 1.5, 2.0, 2.5, "real3");
            runAndTestQueryRI("select count(*) as retval from " + CLASS_NAME + "." + table + " group by real3 having real3 = 2.0", 1, "retval");
            runAndTestQueryRI("select count(*) as retval from " + CLASS_NAME + "." + table + " group by real1 having real1 < 2.0", 1, "retval");
            runAndTestQueryRI("select count(*) as retval from " + CLASS_NAME + "." + table + " group by real2 having real2 < 2.0", 1, "retval");
            runAndTestQueryRI("select count(*) as retval from " + CLASS_NAME + "." + table + " group by real3 having real3 < 2.0", 1, "retval");
            runAndTestQueryRI("select count(*) as retval from " + CLASS_NAME + "." + table + " group by real1 having real1 <= 2.0", 1, "retval");
            runAndTestQueryRI("select count(*) as retval from " + CLASS_NAME + "." + table + " group by real2 having real2 <= 2.0", 1, "retval");
            runAndTestQueryRI("select count(*) as retval from " + CLASS_NAME + "." + table + " group by real3 having real3 <= 2.0", 1, "retval");
            runAndTestQueryRI("select count(*) as retval from " + CLASS_NAME + "." + table + " group by real1 having real1 > 2.0", 1, "retval");
            runAndTestQueryRI("select count(*) as retval from " + CLASS_NAME + "." + table + " group by real2 having real2 > 2.0", 1, "retval");
            runAndTestQueryRI("select count(*) as retval from " + CLASS_NAME + "." + table + " group by real3 having real3 > 2.0", 1, "retval");
            runAndTestQueryRI("select count(*) as retval from " + CLASS_NAME + "." + table + " group by real1 having real1 >= 2.0", 1, "retval");
            runAndTestQueryRI("select count(*) as retval from " + CLASS_NAME + "." + table + " group by real2 having real2 >= 2.0", 1, "retval");
            runAndTestQueryRI("select count(*) as retval from " + CLASS_NAME + "." + table + " group by real3 having real3 >= 2.0", 1, "retval");
        }

    }

    @Test
    public void testDouble() throws Exception {

        for (String table : tables) {
            runAndTestQueryRD("select * from " + CLASS_NAME + "." + table + " where double1 = 1", 1.0, "double1");
            runAndTestQueryR3D("select * from " + CLASS_NAME + "." + table + " where double1 between 1.5 and 2.5", 1.5, 2.0, 2.5, "double1");
            runAndTestQueryRI("select count(*) as retval from " + CLASS_NAME + "." + table + " group by double1 having double1 = 2.0", 1, "retval");
            runAndTestQueryRD("select * from " + CLASS_NAME + "." + table + " where double2 = 1", 1.0, "double2");
            runAndTestQueryR3D("select * from " + CLASS_NAME + "." + table + " where double2 between 1.5 and 2.5", 1.5, 2.0, 2.5, "double2");
            runAndTestQueryRI("select count(*) as retval from " + CLASS_NAME + "." + table + " group by double2 having double2 = 2.0", 1, "retval");
            runAndTestQueryRD("select * from " + CLASS_NAME + "." + table + " where double3 = 1", 1.0, "double3");
            runAndTestQueryR3D("select * from " + CLASS_NAME + "." + table + " where double3 between 1.5 and 2.5", 1.5, 2.0, 2.5, "double3");
            runAndTestQueryRI("select count(*) as retval from " + CLASS_NAME + "." + table + " group by double3 having double3 = 2.0", 1, "retval");
            runAndTestQueryRI("select count(*) as retval from " + CLASS_NAME + "." + table + " group by double1 having double1 < 2.0", 1, "retval");
            runAndTestQueryRI("select count(*) as retval from " + CLASS_NAME + "." + table + " group by double2 having double2 < 2.0", 1, "retval");
            runAndTestQueryRI("select count(*) as retval from " + CLASS_NAME + "." + table + " group by double3 having double3 < 2.0", 1, "retval");
            runAndTestQueryRI("select count(*) as retval from " + CLASS_NAME + "." + table + " group by double1 having double1 <= 2.0", 1, "retval");
            runAndTestQueryRI("select count(*) as retval from " + CLASS_NAME + "." + table + " group by double2 having double2 <= 2.0", 1, "retval");
            runAndTestQueryRI("select count(*) as retval from " + CLASS_NAME + "." + table + " group by double3 having double3 <= 2.0", 1, "retval");
            runAndTestQueryRI("select count(*) as retval from " + CLASS_NAME + "." + table + " group by double1 having double1 > 2.0", 1, "retval");
            runAndTestQueryRI("select count(*) as retval from " + CLASS_NAME + "." + table + " group by double2 having double2 > 2.0", 1, "retval");
            runAndTestQueryRI("select count(*) as retval from " + CLASS_NAME + "." + table + " group by double3 having double3 > 2.0", 1, "retval");
            runAndTestQueryRI("select count(*) as retval from " + CLASS_NAME + "." + table + " group by double1 having double1 >= 2.0", 1, "retval");
            runAndTestQueryRI("select count(*) as retval from " + CLASS_NAME + "." + table + " group by double2 having double2 >= 2.0", 1, "retval");
            runAndTestQueryRI("select count(*) as retval from " + CLASS_NAME + "." + table + " group by double3 having double3 >= 2.0", 1, "retval");
        }

    }

    @Test
    public void testFloat() throws Exception {

        for (String table : tables) {
            runAndTestQueryRD("select * from " + CLASS_NAME + "." + table + " where float1 = 1", 1.0, "float1");
            runAndTestQueryR3D("select * from " + CLASS_NAME + "." + table + " where float1 between 1.5 and 2.5", 1.5, 2.0, 2.5, "float1");
            runAndTestQueryRI("select count(*) as retval from " + CLASS_NAME + "." + table + " group by float1 having float1 = 2.0", 1, "retval");
            runAndTestQueryRD("select * from " + CLASS_NAME + "." + table + " where float2 = 1", 1.0, "float2");
            runAndTestQueryR3D("select * from " + CLASS_NAME + "." + table + " where float2 between 1.5 and 2.5", 1.5, 2.0, 2.5, "float2");
            runAndTestQueryRI("select count(*) as retval from " + CLASS_NAME + "." + table + " group by float2 having float2 = 2.0", 1, "retval");
            runAndTestQueryRD("select * from " + CLASS_NAME + "." + table + " where float3 = 1", 1.0, "float3");
            runAndTestQueryR3D("select * from " + CLASS_NAME + "." + table + " where float3 between 1.5 and 2.5", 1.5, 2.0, 2.5, "float3");
            runAndTestQueryRI("select count(*) as retval from " + CLASS_NAME + "." + table + " group by float3 having float3 = 2.0", 1, "retval");
            runAndTestQueryRI("select count(*) as retval from " + CLASS_NAME + "." + table + " group by float1 having float1 < 2.0", 1, "retval");
            runAndTestQueryRI("select count(*) as retval from " + CLASS_NAME + "." + table + " group by float2 having float2 < 2.0", 1, "retval");
            runAndTestQueryRI("select count(*) as retval from " + CLASS_NAME + "." + table + " group by float3 having float3 < 2.0", 1, "retval");
            runAndTestQueryRI("select count(*) as retval from " + CLASS_NAME + "." + table + " group by float1 having float1 <= 2.0", 1, "retval");
            runAndTestQueryRI("select count(*) as retval from " + CLASS_NAME + "." + table + " group by float2 having float2 <= 2.0", 1, "retval");
            runAndTestQueryRI("select count(*) as retval from " + CLASS_NAME + "." + table + " group by float3 having float3 <= 2.0", 1, "retval");
            runAndTestQueryRI("select count(*) as retval from " + CLASS_NAME + "." + table + " group by float1 having float1 > 2.0", 1, "retval");
            runAndTestQueryRI("select count(*) as retval from " + CLASS_NAME + "." + table + " group by float2 having float2 > 2.0", 1, "retval");
            runAndTestQueryRI("select count(*) as retval from " + CLASS_NAME + "." + table + " group by float3 having float3 > 2.0", 1, "retval");
            runAndTestQueryRI("select count(*) as retval from " + CLASS_NAME + "." + table + " group by float1 having float1 >= 2.0", 1, "retval");
            runAndTestQueryRI("select count(*) as retval from " + CLASS_NAME + "." + table + " group by float2 having float2 >= 2.0", 1, "retval");
            runAndTestQueryRI("select count(*) as retval from " + CLASS_NAME + "." + table + " group by float3 having float3 >= 2.0", 1, "retval");
        }

    }

    @Test
    public void testChar() throws Exception {

        for (String table : tables) {
            runAndTestQueryRS("select * from " + CLASS_NAME + "." + table + " where char1 = 'a'", "a         ", "char1");
            runAndTestQueryR3S("select * from " + CLASS_NAME + "." + table + " where char1 between 'a' and 'c'", "a         ", "b         ", "c         ", "char1");
            runAndTestQueryRI("select count(*) as retval from " + CLASS_NAME + "." + table + " group by char1 having char1 = 'b'", 1, "retval");
            runAndTestQueryRS("select * from " + CLASS_NAME + "." + table + " where char2 = 'a'", "a         ", "char2");
            runAndTestQueryR3S("select * from " + CLASS_NAME + "." + table + " where char2 between 'a' and 'c'", "a         ", "b         ", "c         ", "char2");
            runAndTestQueryRI("select count(*) as retval from " + CLASS_NAME + "." + table + " group by char2 having char2 = 'b'", 1, "retval");
            runAndTestQueryRS("select * from " + CLASS_NAME + "." + table + " where char3 = 'a'", "a         ", "char3");
            runAndTestQueryR3S("select * from " + CLASS_NAME + "." + table + " where char3 between 'a' and 'c'", "a         ", "b         ", "c         ", "char3");

            runAndTestQueryRI("select count(*) as retval from " + CLASS_NAME + "." + table + " group by char3 having char3 = 'b'", 1, "retval");
            runAndTestQueryRI("select count(*) as retval from " + CLASS_NAME + "." + table + " group by char1 having char1 < 'b'", 1, "retval");
            runAndTestQueryRI("select count(*) as retval from " + CLASS_NAME + "." + table + " group by char2 having char2 < 'b'", 1, "retval");
            runAndTestQueryRI("select count(*) as retval from " + CLASS_NAME + "." + table + " group by char3 having char3 < 'b'", 1, "retval");
            runAndTestQueryRI("select count(*) as retval from " + CLASS_NAME + "." + table + " group by char1 having char1 <= 'b'", 1, "retval");
            runAndTestQueryRI("select count(*) as retval from " + CLASS_NAME + "." + table + " group by char2 having char2 <= 'b'", 1, "retval");
            runAndTestQueryRI("select count(*) as retval from " + CLASS_NAME + "." + table + " group by char3 having char3 <= 'b'", 1, "retval");
            runAndTestQueryRI("select count(*) as retval from " + CLASS_NAME + "." + table + " group by char1 having char1 > 'b'", 1, "retval");
            runAndTestQueryRI("select count(*) as retval from " + CLASS_NAME + "." + table + " group by char2 having char2 > 'b'", 1, "retval");
            runAndTestQueryRI("select count(*) as retval from " + CLASS_NAME + "." + table + " group by char3 having char3 > 'b'", 1, "retval");
            runAndTestQueryRI("select count(*) as retval from " + CLASS_NAME + "." + table + " group by char1 having char1 >= 'b'", 1, "retval");
            runAndTestQueryRI("select count(*) as retval from " + CLASS_NAME + "." + table + " group by char2 having char2 >= 'b'", 1, "retval");
            runAndTestQueryRI("select count(*) as retval from " + CLASS_NAME + "." + table + " group by char3 having char3 >= 'b'", 1, "retval");
        }

    }

    @Test
    public void testVarChar() throws Exception {

        for (String table : tables) {
            runAndTestQueryRS("select * from " + CLASS_NAME + "." + table + " where varchar1 = 'a'", "a", "varchar1");
            runAndTestQueryR3S("select * from " + CLASS_NAME + "." + table + " where varchar1 between 'a' and 'c'", "a", "b", "c", "varchar1");
            runAndTestQueryRI("select count(*) as retval from " + CLASS_NAME + "." + table + " group by varchar1 having varchar1 = 'b'", 1, "retval");
            runAndTestQueryRS("select * from " + CLASS_NAME + "." + table + " where varchar2 = 'a'", "a", "varchar2");
            runAndTestQueryR3S("select * from " + CLASS_NAME + "." + table + " where varchar2 between 'a' and 'c'", "a", "b", "c", "varchar2");
            runAndTestQueryRI("select count(*) as retval from " + CLASS_NAME + "." + table + " group by varchar2 having varchar2 = 'b'", 1, "retval");
            runAndTestQueryRS("select * from " + CLASS_NAME + "." + table + " where varchar3 = 'a'", "a", "varchar3");
            runAndTestQueryR3S("select * from " + CLASS_NAME + "." + table + " where varchar3 between 'a' and 'c'", "a", "b", "c", "varchar3");
            runAndTestQueryRI("select count(*) as retval from " + CLASS_NAME + "." + table + " group by varchar3 having varchar3 = 'b'", 1, "retval");
            runAndTestQueryRI("select count(*) as retval from " + CLASS_NAME + "." + table + " group by varchar1 having varchar1 < 'b'", 1, "retval");
            runAndTestQueryRI("select count(*) as retval from " + CLASS_NAME + "." + table + " group by varchar2 having varchar2 < 'b'", 1, "retval");
            runAndTestQueryRI("select count(*) as retval from " + CLASS_NAME + "." + table + " group by varchar3 having varchar3 < 'b'", 1, "retval");
            runAndTestQueryRI("select count(*) as retval from " + CLASS_NAME + "." + table + " group by varchar1 having varchar1 <= 'b'", 1, "retval");
            runAndTestQueryRI("select count(*) as retval from " + CLASS_NAME + "." + table + " group by varchar2 having varchar2 <= 'b'", 1, "retval");
            runAndTestQueryRI("select count(*) as retval from " + CLASS_NAME + "." + table + " group by varchar3 having varchar3 <= 'b'", 1, "retval");
            runAndTestQueryRI("select count(*) as retval from " + CLASS_NAME + "." + table + " group by varchar1 having varchar1 > 'b'", 1, "retval");
            runAndTestQueryRI("select count(*) as retval from " + CLASS_NAME + "." + table + " group by varchar2 having varchar2 > 'b'", 1, "retval");
            runAndTestQueryRI("select count(*) as retval from " + CLASS_NAME + "." + table + " group by varchar3 having varchar3 > 'b'", 1, "retval");
            runAndTestQueryRI("select count(*) as retval from " + CLASS_NAME + "." + table + " group by varchar1 having varchar1 >= 'b'", 1, "retval");
            runAndTestQueryRI("select count(*) as retval from " + CLASS_NAME + "." + table + " group by varchar2 having varchar2 >= 'b'", 1, "retval");
            runAndTestQueryRI("select count(*) as retval from " + CLASS_NAME + "." + table + " group by varchar3 having varchar3 >= 'b'", 1, "retval");
        }

    }

    @Test
    public void testFieldLongVarChars() throws Exception {

        for (String bTable : bTables) {
            runAndTestQueryRS("select * from " + CLASS_NAME + "." + bTable + " where CAST(lvarchar1 AS VARCHAR(128)) = 'aaaaaaaaaaaaaaaaaaaaaaaaaa'", "aaaaaaaaaaaaaaaaaaaaaaaaaa", "lvarchar1");
            runAndTestQueryR3S("select * from " + CLASS_NAME + "." + bTable + " where CAST(lvarchar1 as varchar(128)) between 'aaaaaaaaaaaaaaaaaaaaaaaaaa' and 'cccccccccccccccccccccccccc'", "aaaaaaaaaaaaaaaaaaaaaaaaaa", "bbbbbbbbbbbbbbbbbbbbbbbbbb", "cccccccccccccccccccccccccc", "lvarchar1");
            runAndTestQueryRI("select count(*) as retval from " + CLASS_NAME + "." + bTable + " where CAST(lvarchar1 as varchar(128)) = 'aaaaaaaaaaaaaaaaaaaaaaaaaa' or CAST(lvarchar1 as varchar(128)) = 'bbbbbbbbbbbbbbbbbbbbbbbbbb' ", 2, "retval");
            runAndTestQueryRS("select * from " + CLASS_NAME + "." + bTable + " where CAST(lvarchar2 AS VARCHAR(128)) = 'aaaaaaaaaaaaaaaaaaaaaaaaaa'", "aaaaaaaaaaaaaaaaaaaaaaaaaa", "lvarchar2");
            runAndTestQueryR3S("select * from " + CLASS_NAME + "." + bTable + " where CAST(lvarchar2 as varchar(128)) between 'aaaaaaaaaaaaaaaaaaaaaaaaaa' and 'cccccccccccccccccccccccccc'", "aaaaaaaaaaaaaaaaaaaaaaaaaa", "bbbbbbbbbbbbbbbbbbbbbbbbbb", "cccccccccccccccccccccccccc", "lvarchar2");
            runAndTestQueryRI("select count(*) as retval from " + CLASS_NAME + "." + bTable + " where CAST(lvarchar2 as varchar(128)) = 'aaaaaaaaaaaaaaaaaaaaaaaaaa' or CAST(lvarchar2 as varchar(128)) = 'bbbbbbbbbbbbbbbbbbbbbbbbbb' ", 2, "retval");
            runAndTestQueryRS("select * from " + CLASS_NAME + "." + bTable + " where CAST(lvarchar3 AS VARCHAR(128)) = 'aaaaaaaaaaaaaaaaaaaaaaaaaa'", "aaaaaaaaaaaaaaaaaaaaaaaaaa", "lvarchar3");
            runAndTestQueryR3S("select * from " + CLASS_NAME + "." + bTable + " where CAST(lvarchar3 as varchar(128)) between 'aaaaaaaaaaaaaaaaaaaaaaaaaa' and 'cccccccccccccccccccccccccc'", "aaaaaaaaaaaaaaaaaaaaaaaaaa", "bbbbbbbbbbbbbbbbbbbbbbbbbb", "cccccccccccccccccccccccccc", "lvarchar3");
            runAndTestQueryRI("select count(*) as retval from " + CLASS_NAME + "." + bTable + " where CAST(lvarchar3 as varchar(128)) = 'aaaaaaaaaaaaaaaaaaaaaaaaaa' or CAST(lvarchar3 as varchar(128)) = 'bbbbbbbbbbbbbbbbbbbbbbbbbb' ", 2, "retval");
            runAndTestQueryRI("select count(*) as retval from " + CLASS_NAME + "." + bTable + " where CAST(lvarchar1 as varchar(128)) < 'aaaaaaaaaaaaaaaaaaaaaaaaaa' or CAST(lvarchar1 as varchar(128)) < 'bbbbbbbbbbbbbbbbbbbbbbbbbb' ", 1, "retval");
            runAndTestQueryRI("select count(*) as retval from " + CLASS_NAME + "." + bTable + " where CAST(lvarchar2 as varchar(128)) < 'aaaaaaaaaaaaaaaaaaaaaaaaaa' or CAST(lvarchar2 as varchar(128)) < 'bbbbbbbbbbbbbbbbbbbbbbbbbb' ", 1, "retval");
            runAndTestQueryRI("select count(*) as retval from " + CLASS_NAME + "." + bTable + " where CAST(lvarchar3 as varchar(128)) < 'aaaaaaaaaaaaaaaaaaaaaaaaaa' or CAST(lvarchar3 as varchar(128)) < 'bbbbbbbbbbbbbbbbbbbbbbbbbb' ", 1, "retval");
            runAndTestQueryRI("select count(*) as retval from " + CLASS_NAME + "." + bTable + " where CAST(lvarchar1 as varchar(128)) <= 'aaaaaaaaaaaaaaaaaaaaaaaaaa' or CAST(lvarchar1 as varchar(128)) <= 'bbbbbbbbbbbbbbbbbbbbbbbbbb' ", 2, "retval");
            runAndTestQueryRI("select count(*) as retval from " + CLASS_NAME + "." + bTable + " where CAST(lvarchar2 as varchar(128)) <= 'aaaaaaaaaaaaaaaaaaaaaaaaaa' or CAST(lvarchar2 as varchar(128)) <= 'bbbbbbbbbbbbbbbbbbbbbbbbbb' ", 2, "retval");
            runAndTestQueryRI("select count(*) as retval from " + CLASS_NAME + "." + bTable + " where CAST(lvarchar3 as varchar(128)) <= 'aaaaaaaaaaaaaaaaaaaaaaaaaa' or CAST(lvarchar3 as varchar(128)) <= 'bbbbbbbbbbbbbbbbbbbbbbbbbb' ", 2, "retval");
            runAndTestQueryRI("select count(*) as retval from " + CLASS_NAME + "." + bTable + " where CAST(lvarchar1 as varchar(128)) > 'aaaaaaaaaaaaaaaaaaaaaaaaaa' or CAST(lvarchar1 as varchar(128)) > 'bbbbbbbbbbbbbbbbbbbbbbbbbb' ", 2, "retval");
            runAndTestQueryRI("select count(*) as retval from " + CLASS_NAME + "." + bTable + " where CAST(lvarchar2 as varchar(128)) > 'aaaaaaaaaaaaaaaaaaaaaaaaaa' or CAST(lvarchar2 as varchar(128)) > 'bbbbbbbbbbbbbbbbbbbbbbbbbb' ", 2, "retval");
            runAndTestQueryRI("select count(*) as retval from " + CLASS_NAME + "." + bTable + " where CAST(lvarchar3 as varchar(128)) > 'aaaaaaaaaaaaaaaaaaaaaaaaaa' or CAST(lvarchar3 as varchar(128)) > 'bbbbbbbbbbbbbbbbbbbbbbbbbb' ", 2, "retval");
            runAndTestQueryRI("select count(*) as retval from " + CLASS_NAME + "." + bTable + " where CAST(lvarchar1 as varchar(128)) >= 'aaaaaaaaaaaaaaaaaaaaaaaaaa' or CAST(lvarchar1 as varchar(128)) >= 'bbbbbbbbbbbbbbbbbbbbbbbbbb' ", 3, "retval");
            runAndTestQueryRI("select count(*) as retval from " + CLASS_NAME + "." + bTable + " where CAST(lvarchar2 as varchar(128)) >= 'aaaaaaaaaaaaaaaaaaaaaaaaaa' or CAST(lvarchar2 as varchar(128)) >= 'bbbbbbbbbbbbbbbbbbbbbbbbbb' ", 3, "retval");
            runAndTestQueryRI("select count(*) as retval from " + CLASS_NAME + "." + bTable + " where CAST(lvarchar3 as varchar(128)) >= 'aaaaaaaaaaaaaaaaaaaaaaaaaa' or CAST(lvarchar3 as varchar(128)) >= 'bbbbbbbbbbbbbbbbbbbbbbbbbb' ", 3, "retval");
            runAndTestQueryRI("select count(*) as retval from " + CLASS_NAME + "." + bTable + " where CAST(lvarchar1 as varchar(128)) < 'aaaaaaaaaaaaaaaaaaaaaaaaaa' and CAST(lvarchar1 as varchar(128)) < 'bbbbbbbbbbbbbbbbbbbbbbbbbb' ", 0, "retval");
            runAndTestQueryRI("select count(*) as retval from " + CLASS_NAME + "." + bTable + " where CAST(lvarchar2 as varchar(128)) < 'aaaaaaaaaaaaaaaaaaaaaaaaaa' and CAST(lvarchar2 as varchar(128)) < 'bbbbbbbbbbbbbbbbbbbbbbbbbb' ", 0, "retval");
            runAndTestQueryRI("select count(*) as retval from " + CLASS_NAME + "." + bTable + " where CAST(lvarchar3 as varchar(128)) < 'aaaaaaaaaaaaaaaaaaaaaaaaaa' and CAST(lvarchar3 as varchar(128)) < 'bbbbbbbbbbbbbbbbbbbbbbbbbb' ", 0, "retval");
            runAndTestQueryRI("select count(*) as retval from " + CLASS_NAME + "." + bTable + " where CAST(lvarchar1 as varchar(128)) <= 'aaaaaaaaaaaaaaaaaaaaaaaaaa' and CAST(lvarchar1 as varchar(128)) <= 'bbbbbbbbbbbbbbbbbbbbbbbbbb' ", 1, "retval");
            runAndTestQueryRI("select count(*) as retval from " + CLASS_NAME + "." + bTable + " where CAST(lvarchar2 as varchar(128)) <= 'aaaaaaaaaaaaaaaaaaaaaaaaaa' and CAST(lvarchar2 as varchar(128)) <= 'bbbbbbbbbbbbbbbbbbbbbbbbbb' ", 1, "retval");
            runAndTestQueryRI("select count(*) as retval from " + CLASS_NAME + "." + bTable + " where CAST(lvarchar3 as varchar(128)) <= 'aaaaaaaaaaaaaaaaaaaaaaaaaa' and CAST(lvarchar3 as varchar(128)) <= 'bbbbbbbbbbbbbbbbbbbbbbbbbb' ", 1, "retval");
            runAndTestQueryRI("select count(*) as retval from " + CLASS_NAME + "." + bTable + " where CAST(lvarchar1 as varchar(128)) > 'aaaaaaaaaaaaaaaaaaaaaaaaaa' and CAST(lvarchar1 as varchar(128)) > 'bbbbbbbbbbbbbbbbbbbbbbbbbb' ", 1, "retval");
            runAndTestQueryRI("select count(*) as retval from " + CLASS_NAME + "." + bTable + " where CAST(lvarchar2 as varchar(128)) > 'aaaaaaaaaaaaaaaaaaaaaaaaaa' and CAST(lvarchar2 as varchar(128)) > 'bbbbbbbbbbbbbbbbbbbbbbbbbb' ", 1, "retval");
            runAndTestQueryRI("select count(*) as retval from " + CLASS_NAME + "." + bTable + " where CAST(lvarchar3 as varchar(128)) > 'aaaaaaaaaaaaaaaaaaaaaaaaaa' and CAST(lvarchar3 as varchar(128)) > 'bbbbbbbbbbbbbbbbbbbbbbbbbb' ", 1, "retval");
            runAndTestQueryRI("select count(*) as retval from " + CLASS_NAME + "." + bTable + " where CAST(lvarchar1 as varchar(128)) >= 'aaaaaaaaaaaaaaaaaaaaaaaaaa' and CAST(lvarchar1 as varchar(128)) >= 'bbbbbbbbbbbbbbbbbbbbbbbbbb' ", 2, "retval");
            runAndTestQueryRI("select count(*) as retval from " + CLASS_NAME + "." + bTable + " where CAST(lvarchar2 as varchar(128)) >= 'aaaaaaaaaaaaaaaaaaaaaaaaaa' and CAST(lvarchar2 as varchar(128)) >= 'bbbbbbbbbbbbbbbbbbbbbbbbbb' ", 2, "retval");
            runAndTestQueryRI("select count(*) as retval from " + CLASS_NAME + "." + bTable + " where CAST(lvarchar3 as varchar(128)) >= 'aaaaaaaaaaaaaaaaaaaaaaaaaa' and CAST(lvarchar3 as varchar(128)) >= 'bbbbbbbbbbbbbbbbbbbbbbbbbb' ", 2, "retval");
        }

    }

    @Test
    public void testFieldCharForBITData() throws Exception {

        runAndTestQueryRI("select count(*) as retval from " + CLASS_NAME + "." + "CC" + " where charforbitdata1 = X'bcdefa2020202020' or charforbitdata1 = X'cdefab2020202020' ", 2, "retval");
//        for (String bTable : bTables) {
//            runAndTestQueryRS("select * from " + CLASS_NAME + "." + bTable + " where charforbitdata1 = X'bcdefa2020202020'", "bcdefa2020202020", "charforbitdata1");
//            runAndTestQueryR3S("select * from " + CLASS_NAME + "." + bTable + " where charforbitdata1 between X'abcdef2020202020' and X'cdefab2020202020'", "abcdef2020202020", "bcdefa2020202020", "cdefab2020202020", "charforbitdata1");
//            System.out.println(bTable);
//            runAndTestQueryRI("select count(*) as retval from " + CLASS_NAME + "." + bTable + " where charforbitdata1 = X'bcdefa2020202020' or charforbitdata1 = X'cdefab2020202020' ", 2, "retval");
//            runAndTestQueryRS("select * from " + CLASS_NAME + "." + bTable + " where charforbitdata2 = X'bcdefa2020202020'", "bcdefa2020202020", "charforbitdata2");
//            runAndTestQueryR3S("select * from " + CLASS_NAME + "." + bTable + " where charforbitdata2 between X'abcdef2020202020' and X'cdefab2020202020'", "abcdef2020202020", "bcdefa2020202020", "cdefab2020202020", "charforbitdata2");
//            runAndTestQueryRI("select count(*) as retval from " + CLASS_NAME + "." + bTable + " where charforbitdata2 = X'bcdefa2020202020' or charforbitdata2 = X'cdefab2020202020' ", 2, "retval");
//            runAndTestQueryRS("select * from " + CLASS_NAME + "." + bTable + " where charforbitdata3 = X'bcdefa2020202020'", "bcdefa2020202020", "charforbitdata3");
//            runAndTestQueryR3S("select * from " + CLASS_NAME + "." + bTable + " where charforbitdata3 between X'abcdef2020202020' and X'cdefab2020202020'", "abcdef2020202020", "bcdefa2020202020", "cdefab2020202020", "charforbitdata3");
//            runAndTestQueryRI("select count(*) as retval from " + CLASS_NAME + "." + bTable + " where charforbitdata3 = X'bcdefa2020202020' or charforbitdata3 = X'cdefab2020202020' ", 2, "retval");
//            runAndTestQueryRI("select count(*) as retval from " + CLASS_NAME + "." + bTable + " where charforbitdata1 > X'abcdef2020202020' or charforbitdata1 < X'cdefab2020202020' ", 3, "retval");
//            runAndTestQueryRI("select count(*) as retval from " + CLASS_NAME + "." + bTable + " where charforbitdata1 > X'abcdef2020202020' or charforbitdata1 <= X'cdefab2020202020' ", 3, "retval");
//            runAndTestQueryRI("select count(*) as retval from " + CLASS_NAME + "." + bTable + " where charforbitdata1 >= X'abcdef2020202020' or charforbitdata1 <= X'cdefab2020202020' ", 3, "retval");
//            runAndTestQueryRI("select count(*) as retval from " + CLASS_NAME + "." + bTable + " where charforbitdata1 > X'abcdef2020202020' and charforbitdata1 < X'cdefab2020202020' ", 1, "retval");
//            runAndTestQueryRI("select count(*) as retval from " + CLASS_NAME + "." + bTable + " where charforbitdata1 > X'abcdef2020202020' and charforbitdata1 <= X'cdefab2020202020' ", 2, "retval");
//            runAndTestQueryRI("select count(*) as retval from " + CLASS_NAME + "." + bTable + " where charforbitdata1 >= X'abcdef2020202020' and charforbitdata1 <= X'cdefab2020202020' ", 3, "retval");
//            runAndTestQueryRI("select count(*) as retval from " + CLASS_NAME + "." + bTable + " where charforbitdata2 > X'abcdef2020202020' or charforbitdata3 < X'cdefab2020202020' ", 3, "retval");
//            runAndTestQueryRI("select count(*) as retval from " + CLASS_NAME + "." + bTable + " where charforbitdata2 > X'abcdef2020202020' or charforbitdata3 <= X'cdefab2020202020' ", 3, "retval");
//            runAndTestQueryRI("select count(*) as retval from " + CLASS_NAME + "." + bTable + " where charforbitdata2 >= X'abcdef2020202020' or charforbitdata3 <= X'cdefab2020202020' ", 3, "retval");
//            runAndTestQueryRI("select count(*) as retval from " + CLASS_NAME + "." + bTable + " where charforbitdata2 > X'abcdef2020202020' and charforbitdata3 < X'cdefab2020202020' ", 1, "retval");
//            runAndTestQueryRI("select count(*) as retval from " + CLASS_NAME + "." + bTable + " where charforbitdata2 > X'abcdef2020202020' and charforbitdata3 <= X'cdefab2020202020' ", 2, "retval");
//            runAndTestQueryRI("select count(*) as retval from " + CLASS_NAME + "." + bTable + " where charforbitdata2 >= X'abcdef2020202020' and charforbitdata3 <= X'cdefab2020202020' ", 3, "retval");
//            runAndTestQueryRI("select count(*) as retval from " + CLASS_NAME + "." + bTable + " where charforbitdata3 > X'abcdef2020202020' or charforbitdata3 < X'cdefab2020202020' ", 3, "retval");
//            runAndTestQueryRI("select count(*) as retval from " + CLASS_NAME + "." + bTable + " where charforbitdata3 > X'abcdef2020202020' or charforbitdata3 <= X'cdefab2020202020' ", 3, "retval");
//            runAndTestQueryRI("select count(*) as retval from " + CLASS_NAME + "." + bTable + " where charforbitdata3 >= X'abcdef2020202020' or charforbitdata3 <= X'cdefab2020202020' ", 3, "retval");
//            runAndTestQueryRI("select count(*) as retval from " + CLASS_NAME + "." + bTable + " where charforbitdata3 > X'abcdef2020202020' and charforbitdata3 < X'cdefab2020202020' ", 1, "retval");
//            runAndTestQueryRI("select count(*) as retval from " + CLASS_NAME + "." + bTable + " where charforbitdata3 > X'abcdef2020202020' and charforbitdata3 <= X'cdefab2020202020' ", 2, "retval");
//            runAndTestQueryRI("select count(*) as retval from " + CLASS_NAME + "." + bTable + " where charforbitdata3 >= X'abcdef2020202020' and charforbitdata3 <= X'cdefab2020202020' ", 3, "retval");
//
//        }

    }

    @Test
    public void testFieldVarcharForBITData() throws Exception {

        for (String bTable : bTables) {
            runAndTestQueryRS("select * from " + CLASS_NAME + "." + bTable + " where varcharforbitdata1 = X'234abcdef1'", "234abcdef1", "varcharforbitdata1");
            runAndTestQueryR3S("select * from " + CLASS_NAME + "." + bTable + " where varcharforbitdata1 between X'1234abcdef' and X'cdefab2020202020'", "1234abcdef", "234abcdef1", "34abcdef12", "varcharforbitdata1");
            runAndTestQueryRI("select count(*) as retval from " + CLASS_NAME + "." + bTable + " where varcharforbitdata1 = X'234abcdef1' or varcharforbitdata1 = X'34abcdef12' ", 2, "retval");
            runAndTestQueryRS("select * from " + CLASS_NAME + "." + bTable + " where varcharforbitdata2 = X'234abcdef1'", "234abcdef1", "varcharforbitdata2");
            runAndTestQueryR3S("select * from " + CLASS_NAME + "." + bTable + " where varcharforbitdata2 between X'1234abcdef' and X'34abcdef12'", "1234abcdef", "234abcdef1", "34abcdef12", "varcharforbitdata2");
            runAndTestQueryRI("select count(*) as retval from " + CLASS_NAME + "." + bTable + " where varcharforbitdata2 = X'234abcdef1' or varcharforbitdata2 = X'34abcdef12' ", 2, "retval");
            runAndTestQueryRS("select * from " + CLASS_NAME + "." + bTable + " where varcharforbitdata3 = X'234abcdef1'", "234abcdef1", "varcharforbitdata3");
            runAndTestQueryR3S("select * from " + CLASS_NAME + "." + bTable + " where varcharforbitdata3 between X'1234abcdef' and X'34abcdef12'", "1234abcdef", "234abcdef1", "34abcdef12", "varcharforbitdata3");
            runAndTestQueryRI("select count(*) as retval from " + CLASS_NAME + "." + bTable + " where varcharforbitdata3 = X'234abcdef1' or varcharforbitdata3 = X'34abcdef12' ", 2, "retval");
            runAndTestQueryRI("select count(*) as retval from " + CLASS_NAME + "." + bTable + " where varcharforbitdata1 > X'1234abcdef' or varcharforbitdata1 < X'34abcdef12' ", 3, "retval");
            runAndTestQueryRI("select count(*) as retval from " + CLASS_NAME + "." + bTable + " where varcharforbitdata1 > X'1234abcdef' or varcharforbitdata1 <= X'34abcdef12' ", 3, "retval");
            runAndTestQueryRI("select count(*) as retval from " + CLASS_NAME + "." + bTable + " where varcharforbitdata1 >= X'1234abcdef' or varcharforbitdata1 <= X'34abcdef12' ", 3, "retval");
            runAndTestQueryRI("select count(*) as retval from " + CLASS_NAME + "." + bTable + " where varcharforbitdata2 > X'1234abcdef' or varcharforbitdata2 < X'34abcdef12' ", 3, "retval");
            runAndTestQueryRI("select count(*) as retval from " + CLASS_NAME + "." + bTable + " where varcharforbitdata2 > X'1234abcdef' or varcharforbitdata2 <= X'34abcdef12' ", 3, "retval");
            runAndTestQueryRI("select count(*) as retval from " + CLASS_NAME + "." + bTable + " where varcharforbitdata2 >= X'1234abcdef' or varcharforbitdata2 <= X'34abcdef12' ", 3, "retval");
            runAndTestQueryRI("select count(*) as retval from " + CLASS_NAME + "." + bTable + " where varcharforbitdata3 > X'1234abcdef' or varcharforbitdata3 < X'34abcdef12' ", 3, "retval");
            runAndTestQueryRI("select count(*) as retval from " + CLASS_NAME + "." + bTable + " where varcharforbitdata3 > X'1234abcdef' or varcharforbitdata3 <= X'34abcdef12' ", 3, "retval");
            runAndTestQueryRI("select count(*) as retval from " + CLASS_NAME + "." + bTable + " where varcharforbitdata3 >= X'1234abcdef' or varcharforbitdata3 <= X'34abcdef12' ", 3, "retval");
            runAndTestQueryRI("select count(*) as retval from " + CLASS_NAME + "." + bTable + " where varcharforbitdata1 > X'1234abcdef' and varcharforbitdata1 < X'34abcdef12' ", 1, "retval");
            runAndTestQueryRI("select count(*) as retval from " + CLASS_NAME + "." + bTable + " where varcharforbitdata1 > X'1234abcdef' and varcharforbitdata1 <= X'34abcdef12' ", 2, "retval");
            runAndTestQueryRI("select count(*) as retval from " + CLASS_NAME + "." + bTable + " where varcharforbitdata1 >= X'1234abcdef' and varcharforbitdata1 <= X'34abcdef12' ", 3, "retval");
            runAndTestQueryRI("select count(*) as retval from " + CLASS_NAME + "." + bTable + " where varcharforbitdata2 > X'1234abcdef' and varcharforbitdata2 < X'34abcdef12' ", 1, "retval");
            runAndTestQueryRI("select count(*) as retval from " + CLASS_NAME + "." + bTable + " where varcharforbitdata2 > X'1234abcdef' and varcharforbitdata2 <= X'34abcdef12' ", 2, "retval");
            runAndTestQueryRI("select count(*) as retval from " + CLASS_NAME + "." + bTable + " where varcharforbitdata2 >= X'1234abcdef' and varcharforbitdata2 <= X'34abcdef12' ", 3, "retval");
            runAndTestQueryRI("select count(*) as retval from " + CLASS_NAME + "." + bTable + " where varcharforbitdata3 > X'1234abcdef' and varcharforbitdata3 < X'34abcdef12' ", 1, "retval");
            runAndTestQueryRI("select count(*) as retval from " + CLASS_NAME + "." + bTable + " where varcharforbitdata3 > X'1234abcdef' and varcharforbitdata3 <= X'34abcdef12' ", 2, "retval");
            runAndTestQueryRI("select count(*) as retval from " + CLASS_NAME + "." + bTable + " where varcharforbitdata3 >= X'1234abcdef' and varcharforbitdata3 <= X'34abcdef12' ", 3, "retval");
        }

    }

    @Test
    public void testFieldLongVarcharForBITData() throws Exception {

        for (String bTable : bTables) {
            runAndTestQueryRS("select * from " + CLASS_NAME + "." + bTable + " where CAST(longvarcharforbitdata1 AS  varchar(1024) for bit data) = X'234abcdef1'", "234abcdef1", "longvarcharforbitdata1");
            runAndTestQueryR3S("select * from " + CLASS_NAME + "." + bTable + " where CAST(longvarcharforbitdata1 AS  varchar(1024) for bit data) between X'1234abcdef' and X'34abcdef12'", "1234abcdef", "234abcdef1", "34abcdef12", "longvarcharforbitdata1");
            runAndTestQueryRI("select count(*) as retval from " + CLASS_NAME + "." + bTable + " where CAST(longvarcharforbitdata1 AS  varchar(1024) for bit data) = X'234abcdef1' or CAST(longvarcharforbitdata1 AS  varchar(1024) for bit data) = X'34abcdef12' ", 2, "retval");
            runAndTestQueryRS("select * from " + CLASS_NAME + "." + bTable + " where CAST(longvarcharforbitdata2 AS  varchar(1024) for bit data) = X'234abcdef1'", "234abcdef1", "longvarcharforbitdata2");
            runAndTestQueryR3S("select * from " + CLASS_NAME + "." + bTable + " where CAST(longvarcharforbitdata2 AS  varchar(1024) for bit data) between X'1234abcdef' and X'34abcdef12'", "1234abcdef", "234abcdef1", "34abcdef12", "longvarcharforbitdata2");
            runAndTestQueryRI("select count(*) as retval from " + CLASS_NAME + "." + bTable + " where CAST(longvarcharforbitdata2 AS  varchar(1024) for bit data) = X'234abcdef1' or CAST(longvarcharforbitdata2 AS  varchar(1024) for bit data) = X'34abcdef12' ", 2, "retval");
            runAndTestQueryRS("select * from " + CLASS_NAME + "." + bTable + " where CAST(longvarcharforbitdata3 AS  varchar(1024) for bit data) = X'234abcdef1'", "234abcdef1", "longvarcharforbitdata3");
            runAndTestQueryR3S("select * from " + CLASS_NAME + "." + bTable + " where CAST(longvarcharforbitdata3 AS  varchar(1024) for bit data) between X'1234abcdef' and X'34abcdef12'", "1234abcdef", "234abcdef1", "34abcdef12", "longvarcharforbitdata3");
            runAndTestQueryRI("select count(*) as retval from " + CLASS_NAME + "." + bTable + " where CAST(longvarcharforbitdata3 AS  varchar(1024) for bit data) = X'234abcdef1' or CAST(longvarcharforbitdata3 AS  varchar(1024) for bit data) = X'34abcdef12' ", 2, "retval");
            runAndTestQueryRI("select count(*) as retval from " + CLASS_NAME + "." + bTable + " where CAST(longvarcharforbitdata1 AS  varchar(1024) for bit data) > X'1234abcdef' or CAST(longvarcharforbitdata1 AS  varchar(1024) for bit data) < X'34abcdef12' ", 3, "retval");
            runAndTestQueryRI("select count(*) as retval from " + CLASS_NAME + "." + bTable + " where CAST(longvarcharforbitdata1 AS  varchar(1024) for bit data) > X'1234abcdef' or CAST(longvarcharforbitdata1 AS  varchar(1024) for bit data) <= X'34abcdef12' ", 3, "retval");
            runAndTestQueryRI("select count(*) as retval from " + CLASS_NAME + "." + bTable + " where CAST(longvarcharforbitdata1 AS  varchar(1024) for bit data) >= X'1234abcdef' or CAST(longvarcharforbitdata1 AS  varchar(1024) for bit data) <= X'34abcdef12' ", 3, "retval");
            runAndTestQueryRI("select count(*) as retval from " + CLASS_NAME + "." + bTable + " where CAST(longvarcharforbitdata2 AS  varchar(1024) for bit data) > X'1234abcdef' or CAST(longvarcharforbitdata2 AS  varchar(1024) for bit data) < X'34abcdef12' ", 3, "retval");
            runAndTestQueryRI("select count(*) as retval from " + CLASS_NAME + "." + bTable + " where CAST(longvarcharforbitdata2 AS  varchar(1024) for bit data) > X'1234abcdef' or CAST(longvarcharforbitdata2 AS  varchar(1024) for bit data) <= X'34abcdef12' ", 3, "retval");
            runAndTestQueryRI("select count(*) as retval from " + CLASS_NAME + "." + bTable + " where CAST(longvarcharforbitdata2 AS  varchar(1024) for bit data) >= X'1234abcdef' or CAST(longvarcharforbitdata2 AS  varchar(1024) for bit data) <= X'34abcdef12' ", 3, "retval");
            runAndTestQueryRI("select count(*) as retval from " + CLASS_NAME + "." + bTable + " where CAST(longvarcharforbitdata3 AS  varchar(1024) for bit data) > X'1234abcdef' or CAST(longvarcharforbitdata3 AS  varchar(1024) for bit data) < X'34abcdef12' ", 3, "retval");
            runAndTestQueryRI("select count(*) as retval from " + CLASS_NAME + "." + bTable + " where CAST(longvarcharforbitdata3 AS  varchar(1024) for bit data) > X'1234abcdef' or CAST(longvarcharforbitdata3 AS  varchar(1024) for bit data) <= X'34abcdef12' ", 3, "retval");
            runAndTestQueryRI("select count(*) as retval from " + CLASS_NAME + "." + bTable + " where CAST(longvarcharforbitdata3 AS  varchar(1024) for bit data) >= X'1234abcdef' or CAST(longvarcharforbitdata3 AS  varchar(1024) for bit data) <= X'34abcdef12' ", 3, "retval");
            runAndTestQueryRI("select count(*) as retval from " + CLASS_NAME + "." + bTable + " where CAST(longvarcharforbitdata1 AS  varchar(1024) for bit data) > X'1234abcdef' and CAST(longvarcharforbitdata1 AS  varchar(1024) for bit data) < X'34abcdef12' ", 1, "retval");
            runAndTestQueryRI("select count(*) as retval from " + CLASS_NAME + "." + bTable + " where CAST(longvarcharforbitdata1 AS  varchar(1024) for bit data) > X'1234abcdef' and CAST(longvarcharforbitdata1 AS  varchar(1024) for bit data) <= X'34abcdef12' ", 2, "retval");
            runAndTestQueryRI("select count(*) as retval from " + CLASS_NAME + "." + bTable + " where CAST(longvarcharforbitdata1 AS  varchar(1024) for bit data) >= X'1234abcdef' and CAST(longvarcharforbitdata1 AS  varchar(1024) for bit data) <= X'34abcdef12' ", 3, "retval");
            runAndTestQueryRI("select count(*) as retval from " + CLASS_NAME + "." + bTable + " where CAST(longvarcharforbitdata2 AS  varchar(1024) for bit data) > X'1234abcdef' and CAST(longvarcharforbitdata2 AS  varchar(1024) for bit data) < X'34abcdef12' ", 1, "retval");
            runAndTestQueryRI("select count(*) as retval from " + CLASS_NAME + "." + bTable + " where CAST(longvarcharforbitdata2 AS  varchar(1024) for bit data) > X'1234abcdef' and CAST(longvarcharforbitdata2 AS  varchar(1024) for bit data) <= X'34abcdef12' ", 2, "retval");
            runAndTestQueryRI("select count(*) as retval from " + CLASS_NAME + "." + bTable + " where CAST(longvarcharforbitdata2 AS  varchar(1024) for bit data) >= X'1234abcdef' and CAST(longvarcharforbitdata2 AS  varchar(1024) for bit data) <= X'34abcdef12' ", 3, "retval");
            runAndTestQueryRI("select count(*) as retval from " + CLASS_NAME + "." + bTable + " where CAST(longvarcharforbitdata3 AS  varchar(1024) for bit data) > X'1234abcdef' and CAST(longvarcharforbitdata3 AS  varchar(1024) for bit data) < X'34abcdef12' ", 1, "retval");
            runAndTestQueryRI("select count(*) as retval from " + CLASS_NAME + "." + bTable + " where CAST(longvarcharforbitdata3 AS  varchar(1024) for bit data) > X'1234abcdef' and CAST(longvarcharforbitdata3 AS  varchar(1024) for bit data) <= X'34abcdef12' ", 2, "retval");
            runAndTestQueryRI("select count(*) as retval from " + CLASS_NAME + "." + bTable + " where CAST(longvarcharforbitdata3 AS  varchar(1024) for bit data) >= X'1234abcdef' and CAST(longvarcharforbitdata3 AS  varchar(1024) for bit data) <= X'34abcdef12' ", 3, "retval");
        }

    }

    @Test
    public void testFieldDate() throws Exception {

        for (String bTable : bTables) {
            runAndTestQueryRS("select * from " + CLASS_NAME + "." + bTable + " where date1 = '2014-05-02'", "2014-05-02", "date1");
            runAndTestQueryR3S("select * from " + CLASS_NAME + "." + bTable + " where date1 between '2014-05-01' and '2014-05-03'", "2014-05-01", "2014-05-02", "2014-05-03", "date1");
            runAndTestQueryRI("select count(*) as retval from " + CLASS_NAME + "." + bTable + " where date1 = '2014-05-02' or date1 = '2014-05-03' ", 2, "retval");
            runAndTestQueryRS("select * from " + CLASS_NAME + "." + bTable + " where date2 = '2014-05-02'", "2014-05-02", "date2");
            runAndTestQueryR3S("select * from " + CLASS_NAME + "." + bTable + " where date2 between '2014-05-01' and '2014-05-03'", "2014-05-01", "2014-05-02", "2014-05-03", "date2");
            runAndTestQueryRI("select count(*) as retval from " + CLASS_NAME + "." + bTable + " where date2 = '2014-05-02' or date2 = '2014-05-03' ", 2, "retval");
            runAndTestQueryRS("select * from " + CLASS_NAME + "." + bTable + " where date3 = '2014-05-02'", "2014-05-02", "date3");
            runAndTestQueryR3S("select * from " + CLASS_NAME + "." + bTable + " where date3 between '2014-05-01' and '2014-05-03'", "2014-05-01", "2014-05-02", "2014-05-03", "date3");
            runAndTestQueryRI("select count(*) as retval from " + CLASS_NAME + "." + bTable + " where date3 = '2014-05-02' or date3 = '2014-05-03' ", 2, "retval");
            runAndTestQueryRI("select count(*) as retval from " + CLASS_NAME + "." + bTable + " where date1 > '2014-05-01' or date1 < '2014-05-03' ", 3, "retval");
            runAndTestQueryRI("select count(*) as retval from " + CLASS_NAME + "." + bTable + " where date1 > '2014-05-01' or date1 <= '2014-05-03' ", 3, "retval");
            runAndTestQueryRI("select count(*) as retval from " + CLASS_NAME + "." + bTable + " where date1 >= '2014-05-01' or date1 <= '2014-05-03' ", 3, "retval");
            runAndTestQueryRI("select count(*) as retval from " + CLASS_NAME + "." + bTable + " where date2 > '2014-05-01' or date2 < '2014-05-03' ", 3, "retval");
            runAndTestQueryRI("select count(*) as retval from " + CLASS_NAME + "." + bTable + " where date2 > '2014-05-01' or date2 <= '2014-05-03' ", 3, "retval");
            runAndTestQueryRI("select count(*) as retval from " + CLASS_NAME + "." + bTable + " where date2 >= '2014-05-01' or date2 <= '2014-05-03' ", 3, "retval");
            runAndTestQueryRI("select count(*) as retval from " + CLASS_NAME + "." + bTable + " where date3 > '2014-05-01' or date3 < '2014-05-03' ", 3, "retval");
            runAndTestQueryRI("select count(*) as retval from " + CLASS_NAME + "." + bTable + " where date3 > '2014-05-01' or date3 <= '2014-05-03' ", 3, "retval");
            runAndTestQueryRI("select count(*) as retval from " + CLASS_NAME + "." + bTable + " where date3 >= '2014-05-01' or date3 <= '2014-05-03' ", 3, "retval");
            runAndTestQueryRI("select count(*) as retval from " + CLASS_NAME + "." + bTable + " where date1 > '2014-05-01' and date1 < '2014-05-03' ", 1, "retval");
            runAndTestQueryRI("select count(*) as retval from " + CLASS_NAME + "." + bTable + " where date1 > '2014-05-01' and date1 <= '2014-05-03' ", 2, "retval");
            runAndTestQueryRI("select count(*) as retval from " + CLASS_NAME + "." + bTable + " where date1 >= '2014-05-01' and date1 <= '2014-05-03' ", 3, "retval");
            runAndTestQueryRI("select count(*) as retval from " + CLASS_NAME + "." + bTable + " where date2 > '2014-05-01' and date2 < '2014-05-03' ", 1, "retval");
            runAndTestQueryRI("select count(*) as retval from " + CLASS_NAME + "." + bTable + " where date2 > '2014-05-01' and date2 <= '2014-05-03' ", 2, "retval");
            runAndTestQueryRI("select count(*) as retval from " + CLASS_NAME + "." + bTable + " where date2 >= '2014-05-01' and date2 <= '2014-05-03' ", 3, "retval");
            runAndTestQueryRI("select count(*) as retval from " + CLASS_NAME + "." + bTable + " where date3 > '2014-05-01' and date3 < '2014-05-03' ", 1, "retval");
            runAndTestQueryRI("select count(*) as retval from " + CLASS_NAME + "." + bTable + " where date3 > '2014-05-01' and date3 <= '2014-05-03' ", 2, "retval");
            runAndTestQueryRI("select count(*) as retval from " + CLASS_NAME + "." + bTable + " where date3 >= '2014-05-01' and date3 <= '2014-05-03' ", 3, "retval");
        }

    }

    @Test
    public void testFieldTime() throws Exception {

        for (String bTable : bTables) {
            runAndTestQueryRS("select * from " + CLASS_NAME + "." + bTable + " where time1 = '06:06:06'", "06:06:06", "time1");
            runAndTestQueryR3S("select * from " + CLASS_NAME + "." + bTable + " where time1 between '05:05:05' and '07:07:07'", "05:05:05", "06:06:06", "07:07:07", "time1");
            runAndTestQueryRI("select count(*) as retval from " + CLASS_NAME + "." + bTable + " where time1 = '06:06:06' or time1 = '07:07:07' ", 2, "retval");
            runAndTestQueryRS("select * from " + CLASS_NAME + "." + bTable + " where time2 = '06:06:06'", "06:06:06", "time2");
            runAndTestQueryR3S("select * from " + CLASS_NAME + "." + bTable + " where time2 between '05:05:05' and '07:07:07'", "05:05:05", "06:06:06", "07:07:07", "time2");
            runAndTestQueryRI("select count(*) as retval from " + CLASS_NAME + "." + bTable + " where time2 = '06:06:06' or time2 = '07:07:07' ", 2, "retval");
            runAndTestQueryRS("select * from " + CLASS_NAME + "." + bTable + " where time3 = '06:06:06'", "06:06:06", "time3");
            runAndTestQueryR3S("select * from " + CLASS_NAME + "." + bTable + " where time3 between '05:05:05' and '07:07:07'", "05:05:05", "06:06:06", "07:07:07", "time3");
            runAndTestQueryRI("select count(*) as retval from " + CLASS_NAME + "." + bTable + " where time3 = '06:06:06' or time3 = '07:07:07' ", 2, "retval");
            runAndTestQueryRI("select count(*) as retval from " + CLASS_NAME + "." + bTable + " where time1 > '05:05:05' or time1 < '07:07:07' ", 3, "retval");
            runAndTestQueryRI("select count(*) as retval from " + CLASS_NAME + "." + bTable + " where time1 > '05:05:05' or time1 <= '07:07:07' ", 3, "retval");
            runAndTestQueryRI("select count(*) as retval from " + CLASS_NAME + "." + bTable + " where time1 >= '05:05:05' or time1 <= '07:07:07' ", 3, "retval");
            runAndTestQueryRI("select count(*) as retval from " + CLASS_NAME + "." + bTable + " where time2 > '05:05:05' or time2 < '07:07:07' ", 3, "retval");
            runAndTestQueryRI("select count(*) as retval from " + CLASS_NAME + "." + bTable + " where time2 > '05:05:05' or time2 <= '07:07:07' ", 3, "retval");
            runAndTestQueryRI("select count(*) as retval from " + CLASS_NAME + "." + bTable + " where time2 >= '05:05:05' or time2 <= '07:07:07' ", 3, "retval");
            runAndTestQueryRI("select count(*) as retval from " + CLASS_NAME + "." + bTable + " where time3 > '05:05:05' or time3 < '07:07:07' ", 3, "retval");
            runAndTestQueryRI("select count(*) as retval from " + CLASS_NAME + "." + bTable + " where time3 > '05:05:05' or time3 <= '07:07:07' ", 3, "retval");
            runAndTestQueryRI("select count(*) as retval from " + CLASS_NAME + "." + bTable + " where time3 >= '05:05:05' or time3 <= '07:07:07' ", 3, "retval");
            runAndTestQueryRI("select count(*) as retval from " + CLASS_NAME + "." + bTable + " where time1 > '05:05:05' and time1 < '07:07:07' ", 1, "retval");
            runAndTestQueryRI("select count(*) as retval from " + CLASS_NAME + "." + bTable + " where time1 > '05:05:05' and time1 <= '07:07:07' ", 2, "retval");
            runAndTestQueryRI("select count(*) as retval from " + CLASS_NAME + "." + bTable + " where time1 >= '05:05:05' and time1 <= '07:07:07' ", 3, "retval");
            runAndTestQueryRI("select count(*) as retval from " + CLASS_NAME + "." + bTable + " where time2 > '05:05:05' and time2 < '07:07:07' ", 1, "retval");
            runAndTestQueryRI("select count(*) as retval from " + CLASS_NAME + "." + bTable + " where time2 > '05:05:05' and time2 <= '07:07:07' ", 2, "retval");
            runAndTestQueryRI("select count(*) as retval from " + CLASS_NAME + "." + bTable + " where time2 >= '05:05:05' and time2 <= '07:07:07' ", 3, "retval");
            runAndTestQueryRI("select count(*) as retval from " + CLASS_NAME + "." + bTable + " where time3 > '05:05:05' and time3 < '07:07:07' ", 1, "retval");
            runAndTestQueryRI("select count(*) as retval from " + CLASS_NAME + "." + bTable + " where time3 > '05:05:05' and time3 <= '07:07:07' ", 2, "retval");
            runAndTestQueryRI("select count(*) as retval from " + CLASS_NAME + "." + bTable + " where time3 >= '05:05:05' and time3 <= '07:07:07' ", 3, "retval");
        }

    }

    @Test
    public void testFieldTimestamp() throws Exception {
        for (String bTable : bTables) {
            runAndTestQueryRS("select * from " + CLASS_NAME + "." + bTable + " where timestamp1 = '2014-05-02 00:00:00'", "2014-05-02 00:00:00.0", "timestamp1");
            runAndTestQueryR3S("select * from " + CLASS_NAME + "." + bTable + " where timestamp1 between '2014-05-01 00:00:00' and '2014-05-01 00:00:00'", "2014-05-01 00:00:00.0", "2014-05-02 00:00:00.0", "2014-05-03 00:00:00.0", "timestamp1");
            runAndTestQueryRI("select count(*) as retval from " + CLASS_NAME + "." + bTable + " where timestamp1 = '2014-05-02 00:00:00' or timestamp1 = '2014-05-01 00:00:00' ", 2, "retval");
            runAndTestQueryRS("select * from " + CLASS_NAME + "." + bTable + " where timestamp2 = '2014-05-02 00:00:00'", "2014-05-02 00:00:00.0", "timestamp2");
            runAndTestQueryR3S("select * from " + CLASS_NAME + "." + bTable + " where timestamp2 between '2014-05-01 00:00:00' and '2014-05-03 00:00:00'", "2014-05-01 00:00:00.0", "2014-05-02 00:00:00.0", "2014-05-03 00:00:00.0", "timestamp2");
            runAndTestQueryRI("select count(*) as retval from " + CLASS_NAME + "." + bTable + " where timestamp2 = '2014-05-02 00:00:00' or timestamp2 = '2014-05-03 00:00:00' ", 2, "retval");
            runAndTestQueryRS("select * from " + CLASS_NAME + "." + bTable + " where timestamp3 = '2014-05-02 00:00:00'", "2014-05-02 00:00:00.0", "timestamp3");
            runAndTestQueryR3S("select * from " + CLASS_NAME + "." + bTable + " where timestamp3 between '2014-05-01 00:00:00' and '2014-05-03 00:00:00'", "2014-05-01 00:00:00.0", "2014-05-02 00:00:00.0", "2014-05-03 00:00:00.0", "timestamp3");
            runAndTestQueryRI("select count(*) as retval from " + CLASS_NAME + "." + bTable + " where timestamp3 = '2014-05-02 00:00:00' or timestamp3 = '2014-05-03 00:00:00' ", 2, "retval");
            runAndTestQueryRI("select count(*) as retval from " + CLASS_NAME + "." + bTable + " where timestamp1 >= '2014-05-01 00:00:00' or timestamp1 <= '2014-05-03 00:00:00' ", 3, "retval");
            runAndTestQueryRI("select count(*) as retval from " + CLASS_NAME + "." + bTable + " where timestamp1 > '2014-05-01 00:00:00' or timestamp1 < '2014-05-03 00:00:00' ", 3, "retval");
            runAndTestQueryRI("select count(*) as retval from " + CLASS_NAME + "." + bTable + " where timestamp1 > '2014-05-01 00:00:00' or timestamp1 <= '2014-05-03 00:00:00' ", 3, "retval");
            runAndTestQueryRI("select count(*) as retval from " + CLASS_NAME + "." + bTable + " where timestamp2 > '2014-05-01 00:00:00' or timestamp2 < '2014-05-03 00:00:00' ", 3, "retval");
            runAndTestQueryRI("select count(*) as retval from " + CLASS_NAME + "." + bTable + " where timestamp2 >= '2014-05-01 00:00:00' or timestamp2 <= '2014-05-03 00:00:00' ", 3, "retval");
            runAndTestQueryRI("select count(*) as retval from " + CLASS_NAME + "." + bTable + " where timestamp2 > '2014-05-01 00:00:00' or timestamp2 <= '2014-05-03 00:00:00' ", 3, "retval");
            runAndTestQueryRI("select count(*) as retval from " + CLASS_NAME + "." + bTable + " where timestamp3 > '2014-05-01 00:00:00' or timestamp3 < '2014-05-03 00:00:00' ", 3, "retval");
            runAndTestQueryRI("select count(*) as retval from " + CLASS_NAME + "." + bTable + " where timestamp3 >= '2014-05-01 00:00:00' or timestamp3 <= '2014-05-03 00:00:00' ", 3, "retval");
            runAndTestQueryRI("select count(*) as retval from " + CLASS_NAME + "." + bTable + " where timestamp3 > '2014-05-01 00:00:00' or timestamp3 <= '2014-05-03 00:00:00' ", 3, "retval");
            runAndTestQueryRI("select count(*) as retval from " + CLASS_NAME + "." + bTable + " where timestamp1 > '2014-05-01 00:00:00' and timestamp1 < '2014-05-03 00:00:00' ", 1, "retval");
            runAndTestQueryRI("select count(*) as retval from " + CLASS_NAME + "." + bTable + " where timestamp1 >= '2014-05-01 00:00:00' and timestamp1 <= '2014-05-03 00:00:00' ", 3, "retval");
            runAndTestQueryRI("select count(*) as retval from " + CLASS_NAME + "." + bTable + " where timestamp1 > '2014-05-01 00:00:00' and timestamp1 <= '2014-05-03 00:00:00' ", 2, "retval");
            runAndTestQueryRI("select count(*) as retval from " + CLASS_NAME + "." + bTable + " where timestamp2 > '2014-05-01 00:00:00' and timestamp2 < '2014-05-03 00:00:00' ", 1, "retval");
            runAndTestQueryRI("select count(*) as retval from " + CLASS_NAME + "." + bTable + " where timestamp2 >= '2014-05-01 00:00:00' and timestamp2 <= '2014-05-03 00:00:00' ", 3, "retval");
            runAndTestQueryRI("select count(*) as retval from " + CLASS_NAME + "." + bTable + " where timestamp2 > '2014-05-01 00:00:00' and timestamp2 <= '2014-05-03 00:00:00' ", 2, "retval");
            runAndTestQueryRI("select count(*) as retval from " + CLASS_NAME + "." + bTable + " where timestamp3 > '2014-05-01 00:00:00' and timestamp3 < '2014-05-03 00:00:00' ", 1, "retval");
            runAndTestQueryRI("select count(*) as retval from " + CLASS_NAME + "." + bTable + " where timestamp3 >= '2014-05-01 00:00:00' and timestamp3 <= '2014-05-03 00:00:00' ", 3, "retval");
            runAndTestQueryRI("select count(*) as retval from " + CLASS_NAME + "." + bTable + " where timestamp3 > '2014-05-01 00:00:00' and timestamp3 <= '2014-05-03 00:00:00' ", 2, "retval");
        }
    }

    @Test
    public void testIntegerChar() throws Exception {
        int A1[][] = {{3, 3}, {2, 3}, {2, 2}, {2, 2}};
        String B1[][] = {{"c", "c"}, {"b", "c"}, {"e", "e"}, {"b", "e"}};
        String C1[][] = {{"a1", "b1"}, {"a3", "b3"}, {"a1c", "b1c"}, {"a3c", "b3c"}};
        for (int i = 0; i < tables.length - 1; i++) {
            String query = "select a.integer1 as a1, a.integer3 as a3,b.integer1 as b1,b.integer3 as b3,a.char1 as a1c,a.char3 as a3c,b.char1 as b1c,b.char3 as b3c  from " + CLASS_NAME + "." + tables[i] + " as a, " + CLASS_NAME + "." + tables[i + 1] + " as b where (a.integer1 > b.integer3) or cast(a.integer1 as char(50)) > b.char3";
            runAndTestQueryJ2(query, A1, B1, C1);
        }
    }

    @Test
    public void testBigintVarchar() throws Exception {
        int A1[][] = {{3, 3}, {2, 3}, {2, 2}, {2, 2}};
        String B1[][] = {{"c", "c"}, {"b", "c"}, {"e", "e"}, {"b", "e"}};
        String C1[][] = {{"a1", "b1"}, {"a3", "b3"}, {"a1c", "b1c"}, {"a3c", "b3c"}};
        for (int i = 0; i < tables.length - 1; i++) {
            String query = "select a.bigint1 as a1, a.bigint3 as a3,b.bigint1 as b1,b.bigint3 as b3,a.varchar1 as a1c,a.varchar3 as a3c,b.varchar1 as b1c,b.varchar3 as b3c  from " + CLASS_NAME + "." + tables[i] + " as a, " + CLASS_NAME + "." + tables[i + 1] + " as b where (a.bigint1 > b.bigint3) or cast(a.bigint1 as char(50)) > b.varchar3";
            runAndTestQueryJ2(query, A1, B1, C1);
        }
    }

    @Test
    public void testSmallintVarchar() throws Exception {
        int A1[][] = {{2, 3, 3}, {3, 2, 3}, {2, 1, 1}, {1, 2, 1}};
        String B1[][] = {{"b", "c", "c"}, {"c", "b", "c"}, {"b", "e", "e"}, {"e", "b", "e"}};
        String C1[][] = {{"a1", "b1"}, {"a3", "b3"}, {"a1c", "b1c"}, {"a3c", "b3c"}};
        for (int i = 0; i < tables.length - 1; i++) {
            String query = "select a.smallint1 as a1, a.smallint3 as a3,b.smallint1 as b1,b.smallint3 as b3,a.varchar1 as a1c,a.varchar3 as a3c,b.varchar1 as b1c,b.varchar3 as b3c  from " + CLASS_NAME + "." + tables[i] + " as a, " + CLASS_NAME + "." + tables[i + 1] + " as b where (a.smallint1 > b.smallint3) or cast(a.smallint1 as char(50)) > b.varchar3";
            runAndTestQueryJ3(query, A1, B1, C1);
        }
    }

    @Test
    public void testJoinsSmallint() throws Exception {
        int lf[] = {1, 1, 1};
        String query = "select count(*) as retval from DataTypeCorrectnessIT.AAA as a join DataTypeCorrectnessIT.AAA as b  on a.smallint1=b.smallint1 group by a.timestamp1,b.time2 having cast(a.timestamp1 as time) <= b.time2";
        runAndTestQueryRI3(query, lf, "retval");
        query = "select count(*) as retval from DataTypeCorrectnessIT.AAA as a join DataTypeCorrectnessIT.BBB as b  on a.smallint1=b.smallint1 group by a.timestamp1,b.time2 having cast(a.timestamp1 as time) <= b.time2";
        runAndTestQueryRI3(query, lf, "retval");
        query = "select count(*) as retval from DataTypeCorrectnessIT.BBB as a join DataTypeCorrectnessIT.BBB as b  on a.smallint1=b.smallint1 group by a.timestamp1,b.time2 having cast(a.timestamp1 as time) <= b.time2";
        runAndTestQueryRI3(query, lf, "retval");
    }

    @Test
    public void testJoinsBigint() throws Exception {
        int lf[] = {1, 1, 1};
        String query = "select count(*) as retval from DataTypeCorrectnessIT.AAA as a join DataTypeCorrectnessIT.AAA as b  on a.bigint1=b.bigint1 group by a.timestamp1,b.time2 having cast(a.timestamp1 as time) <= b.time2";
        runAndTestQueryRI3(query, lf, "retval");
        query = "select count(*) as retval from DataTypeCorrectnessIT.AAA as a join DataTypeCorrectnessIT.BBB as b  on a.bigint1=b.bigint1 group by a.timestamp1,b.time2 having cast(a.timestamp1 as time) <= b.time2";
        runAndTestQueryRI3(query, lf, "retval");
        query = "select count(*) as retval from DataTypeCorrectnessIT.BBB as a join DataTypeCorrectnessIT.BBB as b  on a.bigint1=b.bigint1 group by a.timestamp1,b.time2 having cast(a.timestamp1 as time) <= b.time2";
        runAndTestQueryRI3(query, lf, "retval");
    }

    @Test
    public void testJoinsInteger() throws Exception {
        int lf[] = {1, 1, 1};
        String query = "select count(*) as retval from DataTypeCorrectnessIT.AAA as a join DataTypeCorrectnessIT.AAA as b  on a.integer1=b.integer1 group by a.timestamp1,b.time2 having cast(a.timestamp1 as time) <= b.time2";
        runAndTestQueryRI3(query, lf, "retval");
        query = "select count(*) as retval from DataTypeCorrectnessIT.AAA as a join DataTypeCorrectnessIT.BBB as b  on a.integer1=b.integer1 group by a.timestamp1,b.time2 having cast(a.timestamp1 as time) <= b.time2";
        runAndTestQueryRI3(query, lf, "retval");
        query = "select count(*) as retval from DataTypeCorrectnessIT.BBB as a join DataTypeCorrectnessIT.BBB as b  on a.integer1=b.integer1 group by a.timestamp1,b.time2 having cast(a.timestamp1 as time) <= b.time2";
        runAndTestQueryRI3(query, lf, "retval");
    }

    @Test
    public void testJoinsDecimal() throws Exception {
        int lf[] = {1, 1, 1};
        String query = "select count(*) as retval from DataTypeCorrectnessIT.AAA as a join DataTypeCorrectnessIT.AAA as b  on a.decimal1=b.decimal1 group by a.timestamp1,b.time2 having cast(a.timestamp1 as time) <= b.time2";
        runAndTestQueryRI3(query, lf, "retval");
        query = "select count(*) as retval from DataTypeCorrectnessIT.AAA as a join DataTypeCorrectnessIT.BBB as b  on a.decimal1=b.decimal1 group by a.timestamp1,b.time2 having cast(a.timestamp1 as time) <= b.time2";
        runAndTestQueryRI3(query, lf, "retval");
        query = "select count(*) as retval from DataTypeCorrectnessIT.BBB as a join DataTypeCorrectnessIT.BBB as b  on a.decimal1=b.decimal1 group by a.timestamp1,b.time2 having cast(a.timestamp1 as time) <= b.time2";
        runAndTestQueryRI3(query, lf, "retval");
    }

    @Test
    public void testJoinsDouble() throws Exception {
        int lf[] = {1, 1, 1};
        String query = "select count(*) as retval from DataTypeCorrectnessIT.AAA as a join DataTypeCorrectnessIT.AAA as b  on a.double1=b.double1 group by a.timestamp1,b.time2 having cast(a.timestamp1 as time) <= b.time2";
        runAndTestQueryRI3(query, lf, "retval");
        query = "select count(*) as retval from DataTypeCorrectnessIT.AAA as a join DataTypeCorrectnessIT.BBB as b  on a.double1=b.double1 group by a.timestamp1,b.time2 having cast(a.timestamp1 as time) <= b.time2";
        runAndTestQueryRI3(query, lf, "retval");
        query = "select count(*) as retval from DataTypeCorrectnessIT.BBB as a join DataTypeCorrectnessIT.BBB as b  on a.double1=b.double1 group by a.timestamp1,b.time2 having cast(a.timestamp1 as time) <= b.time2";
        runAndTestQueryRI3(query, lf, "retval");
    }

    @Test
    public void testJoinsFloat() throws Exception {
        int lf[] = {1, 1, 1};
        String query = "select count(*) as retval from DataTypeCorrectnessIT.AAA as a join DataTypeCorrectnessIT.AAA as b  on a.float1=b.float1 group by a.timestamp1,b.time2 having cast(a.timestamp1 as time) <= b.time2";
        runAndTestQueryRI3(query, lf, "retval");
        query = "select count(*) as retval from DataTypeCorrectnessIT.AAA as a join DataTypeCorrectnessIT.BBB as b  on a.float1=b.float1 group by a.timestamp1,b.time2 having cast(a.timestamp1 as time) <= b.time2";
        runAndTestQueryRI3(query, lf, "retval");
        query = "select count(*) as retval from DataTypeCorrectnessIT.BBB as a join DataTypeCorrectnessIT.BBB as b  on a.float1=b.float1 group by a.timestamp1,b.time2 having cast(a.timestamp1 as time) <= b.time2";
        runAndTestQueryRI3(query, lf, "retval");
    }

    @Test
    public void testJoinsDate() throws Exception {
        int lf[] = {1, 1, 1};
        String query = "select count(*) as retval from DataTypeCorrectnessIT.AAA as a join DataTypeCorrectnessIT.AAA as b  on a.date1=b.date1 group by a.timestamp1,b.time2 having cast(a.timestamp1 as time) <= b.time2";
        runAndTestQueryRI3(query, lf, "retval");
        query = "select count(*) as retval from DataTypeCorrectnessIT.AAA as a join DataTypeCorrectnessIT.BBB as b  on a.date1=b.date1 group by a.timestamp1,b.time2 having cast(a.timestamp1 as time) <= b.time2";
        runAndTestQueryRI3(query, lf, "retval");
        query = "select count(*) as retval from DataTypeCorrectnessIT.BBB as a join DataTypeCorrectnessIT.BBB as b  on a.date1=b.date1 group by a.timestamp1,b.time2 having cast(a.timestamp1 as time) <= b.time2";
        runAndTestQueryRI3(query, lf, "retval");
    }

    @Test
    public void testJoinsTime() throws Exception {
        int lf[] = {1, 1, 1};
        String query = "select count(*) as retval from DataTypeCorrectnessIT.AAA as a join DataTypeCorrectnessIT.AAA as b  on a.time1=b.time1 group by a.timestamp1,b.time2 having cast(a.timestamp1 as time) <= b.time2";
        runAndTestQueryRI3(query, lf, "retval");
        query = "select count(*) as retval from DataTypeCorrectnessIT.AAA as a join DataTypeCorrectnessIT.BBB as b  on a.time1=b.time1 group by a.timestamp1,b.time2 having cast(a.timestamp1 as time) <= b.time2";
        runAndTestQueryRI3(query, lf, "retval");
        query = "select count(*) as retval from DataTypeCorrectnessIT.BBB as a join DataTypeCorrectnessIT.BBB as b  on a.time1=b.time1 group by a.timestamp1,b.time2 having cast(a.timestamp1 as time) <= b.time2";
        runAndTestQueryRI3(query, lf, "retval");
    }

}