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

/**
 *
 * Package for Splice VTI background.
 *
 * Splice's VTI matches derby except for the expectation that each VTIFunction implement DatasetProvider.
 *
 * @see com.splicemachine.derby.vti.iapi.DatasetProvider
 *
 *
 * We support both derby style and non-derby style (in line code) functions.
 *
 * Here is an example of a derby style implementation.
 *
 *
 * create function JDBCTableVTI(conn varchar(32672), s varchar(1024), t varchar(1024))
 * returns table (
 * name varchar(56),
 * id bigint,
 * salary numeric(9,2),
 * ranking int
 * )
 * language java
 * parameter style SPLICE_JDBC_RESULT_SET
 * no sql
 * external name 'com.splicemachine.derby.vti.SpliceJDBCVTI.getJDBCTableVTI'";
 *
 * The VTIOperation expects SpliceJDBCVTI to implement DatasetProvider
 *
 *
 * Here is an example of a non-derby style vti (dynamic)
 *
 * select * from new com.splicemachine.derby.vti.SpliceFileVTI(
 * '/Users/jleach/Documents/workspace/spliceengine/cdh5.4.1/splice_machine_test/src/test/test-data/vtiConversion.in’,’’,’,’) as b (c1 varchar(128), c2 varchar(128)
 * , c3 varchar(128), c4 varchar(128), c5 varchar(128), c6 varchar(128))
 *
 *
 * @see com.splicemachine.derby.vti.iapi
 *
 */
package com.splicemachine.derby.vti;