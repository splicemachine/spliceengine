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

package com.splicemachine.db.impl.tools.ij;

import com.splicemachine.db.iapi.tools.i18n.LocalizedResource;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.util.Vector;
import java.util.List;

/**
 * This is an empty impl for reuse of code.
 *
 */
abstract class ijResultImpl implements ijResult {
	public boolean isConnection() { return false; }
	public boolean isStatement() { return false; }
	public boolean isResultSet() throws SQLException { return false; }
	public boolean isUpdateCount() throws SQLException { return false; }
	public boolean isNextRowOfResultSet() { return false; }
	public boolean isVector() { return false; }
	public boolean isMulti() { return false; }
	public boolean isException() { return false; }
	public boolean isMultipleResultSetResult(){ return false; }
	public boolean isUnsupportedCommand() { return false; }
	public boolean hasWarnings() throws SQLException { return getSQLWarnings()!=null; }

	public Connection getConnection() { return null; }
	public Statement getStatement() { return null; }
	public int getUpdateCount() throws SQLException { return -1; }
	public ResultSet getResultSet() throws SQLException { return null; }
	public List getMultipleResultSets() { return null; }
	public ResultSet getNextRowOfResultSet() { return null; }
	public Vector getVector() { return null; }
	public SQLException getException() { return null; }

	public int[] getColumnDisplayList() { return null; }
	public int[] getColumnWidthList() { return null; }

	public void closeStatement() throws SQLException { }

	public abstract SQLWarning getSQLWarnings() throws SQLException;
	public abstract void clearSQLWarnings() throws SQLException;


	public String toString() {
		if (isConnection()) return LocalizedResource.getMessage("IJ_Con0",getConnection().toString());
		if (isStatement()) return LocalizedResource.getMessage("IJ_Stm0",getStatement().toString());
		if (isNextRowOfResultSet()) return LocalizedResource.getMessage("IJ_Row0",getNextRowOfResultSet().toString());
		if (isVector()) return LocalizedResource.getMessage("IJ_Vec0",getVector().toString());
		if (isMulti()) return LocalizedResource.getMessage("IJ_Mul0",getVector().toString());
		if (isException()) return LocalizedResource.getMessage("IJ_Exc0",getException().toString());
		if (isMultipleResultSetResult())
			return LocalizedResource.getMessage("IJ_MRS0",
										getMultipleResultSets().toString());
		try {
			if (isResultSet()) return LocalizedResource.getMessage("IJ_Rse0",getStatement().toString());
		} catch(SQLException se) {
		}
		return LocalizedResource.getMessage("IJ_Unkn0",this.getClass().getName());
	}
}
