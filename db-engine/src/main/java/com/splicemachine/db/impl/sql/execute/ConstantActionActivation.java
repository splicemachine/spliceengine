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

package com.splicemachine.db.impl.sql.execute;

import com.splicemachine.db.iapi.error.StandardException;

import com.splicemachine.db.iapi.sql.ResultSet;

import java.util.Vector;

/**
	A pre-compiled activation that supports a single ResultSet with
	a single constant action. All the execution logic is contained
	in the constant action.
    <P>
    At compile time for DDL statements this class will be picked
    as the implementation of Activation. The language PreparedStatement
    will contain the ConstantAction created at compiled time.
    At execute time this class then fetches a language ResultSet using
    ResultSetFactory.getDDLResultSet and executing the ResultSet
    will invoke the execute on the ConstantAction.

 */
public final class ConstantActionActivation extends BaseActivation
{

	public int getExecutionCount() { return 0;}
	public void setExecutionCount(int count) {}

	public Vector getRowCountCheckVector() {return null;}
	public void setRowCountCheckVector(Vector v) {}

	public int getStalePlanCheckInterval() { return Integer.MAX_VALUE; }
	public void setStalePlanCheckInterval(int count) {}

	public ResultSet execute() throws StandardException {

		throwIfClosed("execute");
		startExecution();

		if (resultSet == null)
			resultSet = getResultSetFactory().getDDLResultSet(this);
		return resultSet;
	}
	public void postConstructor(){}

    public void materialize() throws StandardException {}
}
