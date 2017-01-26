/*
 * This file is part of Splice Machine.
 * Splice Machine is free software: you can redistribute it and/or modify it under the terms of the
 * GNU Affero General Public License as published by the Free Software Foundation, either
 * version 3, or (at your option) any later version.
 * Splice Machine is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU Affero General Public License for more details.
 * You should have received a copy of the GNU Affero General Public License along with Splice Machine.
 * If not, see <http://www.gnu.org/licenses/>.
 *
 * Some parts of this source code are based on Apache Derby, and the following notices apply to
 * Apache Derby:
 *
 * Apache Derby is a subproject of the Apache DB project, and is licensed under
 * the Apache License, Version 2.0 (the "License"); you may not use these files
 * except in compliance with the License. You may obtain a copy of the License at:
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 * Splice Machine, Inc. has modified the Apache Derby code in this file.
 *
 * All such Splice Machine modifications are Copyright 2012 - 2017 Splice Machine, Inc.,
 * and are licensed to you under the GNU Affero General Public License.
 */
package com.splicemachine.dbTesting.system.optimizer;
/**
 *
 * Class StaticValues: A location to store all the common static values used in 
 * this test
 *
 */
public class StaticValues {
    public static String clientURL="jdbc:splice://localhost:1527/testdb;create=true";
	public static String clientClass="com.splicemachine.db.jdbc.ClientDriver";

    public static String embedURL="jdbc:splice:testdb;create=true";
	public static String embedClass="com.splicemachine.db.jdbc.EmbeddedDriver";

    public static int NUM_OF_ROWS=1000; //Total number of rows expected in each table
    public static int NUM_OF_TABLES=64; //Total number of tables to be created
    public static int ITER=2; 			//Number of iterations of each query

    public static String queryFile="query.list"; //File name that contains the custom queries
    //SCHEMA OBJECTS
    public static String DROP_TABLE="DROP TABLE ";
    public static String CREATE_TABLE="CREATE TABLE ";
    public static String TABLE_NAME="MYTABLE";
    public static String TABLE_COLS="(col1 INT primary key, col2 VARCHAR(100),col3 VARCHAR(100),col4 VARCHAR(30),col5 VARCHAR(30),col6 varchar(30),col7 VARCHAR(40), col8 INT, col9 timestamp)";
    public static String CREATE_VIEW="CREATE VIEW ";
    public static String VIEW1_COLS="col1, col2, col3, col4, col5, col6, col7 from ";
    public static String VIEW2_COLS="col1, col2, col3, col4, col5, col6, col7, col8, col9 from ";

    //INSERT
    public static String INSERT_TABLE="INSERT INTO ";
    public static String INSERT_VALUES=" VALUES(?,?,?,?,?,?,?, ?, ?) ";

    public static void init(){
        //TODO Load from property file
    }
}
