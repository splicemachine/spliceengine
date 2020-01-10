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
 * All such Splice Machine modifications are Copyright 2012 - 2020 Splice Machine, Inc.,
 * and are licensed to you under the GNU Affero General Public License.
 */
package com.splicemachine.dbTesting.system.sttest.utils;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import com.splicemachine.db.tools.JDBCDisplayUtil;

/**
 * creates database and builds single user table with indexes
 */
public class Setup {
	public static boolean doit(String dbURL) throws Throwable {
		Connection conn = null;
		Statement s = null;
		ResultSet rs = null;
		boolean finished = false;
		System.out.println("dbsetup start");
		try {
			conn = DriverManager.getConnection(dbURL + ";create=true");
			conn.setAutoCommit(false);
			conn.setHoldability(ResultSet.CLOSE_CURSORS_AT_COMMIT);
		} catch (SQLException se) {
			System.out.println("connect failed for " + dbURL);
			JDBCDisplayUtil.ShowException(System.out, se);
			return (false);
		}
		try {
			s = conn.createStatement();
			rs = s.executeQuery("select tablename from sys.systables "
					+ " where tablename = 'DATATYPES'");
			if (rs.next()) {
				rs.close();
				System.out.println("table 'DATATYPES' already exists");
				finished = true;
			}
		} catch (SQLException se) {
			System.out.println("create table: FAIL -- unexpected exception:");
			JDBCDisplayUtil.ShowException(System.out, se);
			return (false);
		}
		if (finished == false)
			try {
				System.out.println("creating table 'DATATYPES'");
				s
				.execute("create table Datatypes ("
						+ "id int not null,"
						+ "t_char char(100),"
						+ "t_blob blob(100K),"
						+ "t_clob clob(100K),"
						+ "t_date date,"
						+ "t_decimal decimal,"
						+ "t_decimal_nn decimal(10,10),"
						+ "t_double double precision,"
						+ "t_float float,"
						+ "t_int int,"
						+ "t_longint bigint,"
						+ "t_numeric_large numeric(31,0),"
						+ "t_real real,"
						+ "t_smallint smallint,"
						+ "t_time time,"
						+ "t_timestamp timestamp,"
						+ "t_varchar varchar(100),"
						+ "serialkey bigint generated always as identity (start with 1,increment by 1),"
						+ "unique (serialkey), " + "unique (id))");
				s.execute("create index t_char_ind on Datatypes ( t_char)");
				s.execute("create index t_date_ind on Datatypes ( t_date)");
				s
				.execute("create index t_decimal_ind on Datatypes ( t_decimal)");
				s
				.execute("create index t_decimal_nn_ind on Datatypes ( t_decimal_nn)");
				s.execute("create index t_double_ind on Datatypes ( t_double)");
				s.execute("create index t_float_ind on Datatypes ( t_float)");
				s.execute("create index t_int_ind on Datatypes ( t_int)");
				s
				.execute("create index t_longint_ind on Datatypes ( t_longint)");
				s
				.execute("create index t_numeric_larg_ind on Datatypes ( t_numeric_large)");
				s.execute("create index t_real_ind on Datatypes ( t_real)");
				s
				.execute("create index t_smallint_ind on Datatypes ( t_smallint)");
				s.execute("create index t_time_ind on Datatypes ( t_time)");
				s
				.execute("create index t_timestamp_ind on Datatypes ( t_timestamp)");
				s
				.execute("create index t_varchar_ind on Datatypes ( t_varchar)");
				conn.commit();
				conn.close();
			} catch (SQLException se) {
				System.out
				.println("create table: FAIL -- unexpected exception:");
				JDBCDisplayUtil.ShowException(System.out, se);
				return (false);
			}
			return (true);
	}
}
