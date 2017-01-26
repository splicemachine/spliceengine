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

package com.splicemachine.db.catalog;

import com.splicemachine.db.iapi.db.Factory;
import com.splicemachine.db.impl.sql.execute.TriggerExecutionContext;

import java.io.InputStream;
import java.io.Reader;
import java.sql.*;

/**
 * Provides information about about a a set of new rows created by a
 * trigger action. 
 * 
 * <p>
 * This class implements only JDBC 1.2, not JDBC 2.0.  You cannot
 * compile this class with JDK1.2, since it implements only the
 * JDBC 1.2 ResultSet interface and not the JDBC 2.0 ResultSet
 * interface.  You can only use this class in a JDK 1.2 runtime 
 * environment if no JDBC 2.0 calls are made against it.
 *
 */
public final class TriggerNewTransitionRows extends com.splicemachine.db.vti.UpdatableVTITemplate
{

	private ResultSet resultSet;

	/**
	 * Construct a VTI on the trigger's new row set.
	 * The new row set is the after image of the rows
	 * that are changed by the trigger.  For a trigger
	 * on a delete, this throws an exception.
	 * For a trigger on an update, this is the rows after
	 * they are updated.  For an insert, this is the rows
	 * that are inserted.
	 *
	 * @exception SQLException thrown if no trigger active
	 */
	public TriggerNewTransitionRows() throws SQLException
	{
		initializeResultSet();
	}

	private ResultSet initializeResultSet() throws SQLException {
		if (resultSet != null)
			resultSet.close();
		
		TriggerExecutionContext tec = Factory.getTriggerExecutionContext();
		if (tec == null)
		{
			throw new SQLException("There are no active triggers", "38000");
		}
        // JC - TEC no longer returns ResultSets, it returns ExecRows. This class is not used.
//		resultSet = tec.getNewRowSet();

		if (resultSet == null)
		{
			throw new SQLException("There is no new transition rows result set for this trigger", "38000");
		}
		return resultSet;
	}
    
    public ResultSetMetaData getMetaData() throws SQLException
    {
        return resultSet.getMetaData();
    }

    @Override
    public void setRowId(int parameterIndex, RowId x) throws SQLException {

    }

    @Override
    public void setNString(int parameterIndex, String value) throws SQLException {

    }

    @Override
    public void setNCharacterStream(int parameterIndex, Reader value, long length) throws SQLException {

    }

    @Override
    public void setNClob(int parameterIndex, NClob value) throws SQLException {

    }

    @Override
    public void setClob(int parameterIndex, Reader reader, long length) throws SQLException {

    }

    @Override
    public void setBlob(int parameterIndex, InputStream inputStream, long length) throws SQLException {

    }

    @Override
    public void setNClob(int parameterIndex, Reader reader, long length) throws SQLException {

    }

    @Override
    public void setSQLXML(int parameterIndex, SQLXML xmlObject) throws SQLException {

    }

    @Override
    public void setAsciiStream(int parameterIndex, InputStream x, long length) throws SQLException {

    }

    @Override
    public void setBinaryStream(int parameterIndex, InputStream x, long length) throws SQLException {

    }

    @Override
    public void setCharacterStream(int parameterIndex, Reader reader, long length) throws SQLException {

    }

    @Override
    public void setAsciiStream(int parameterIndex, InputStream x) throws SQLException {

    }

    @Override
    public void setBinaryStream(int parameterIndex, InputStream x) throws SQLException {

    }

    @Override
    public void setCharacterStream(int parameterIndex, Reader reader) throws SQLException {

    }

    @Override
    public void setNCharacterStream(int parameterIndex, Reader value) throws SQLException {

    }

    @Override
    public void setClob(int parameterIndex, Reader reader) throws SQLException {

    }

    @Override
    public void setBlob(int parameterIndex, InputStream inputStream) throws SQLException {

    }

    @Override
    public void setNClob(int parameterIndex, Reader reader) throws SQLException {

    }

    public ResultSet executeQuery() throws SQLException {
	   //DERBY-4095. Need to reinititialize ResultSet on 
       //executeQuery, in case it was closed in a NESTEDLOOP join.
       return initializeResultSet();
   }
    
   public int getResultSetConcurrency() {
        return ResultSet.CONCUR_READ_ONLY;
   }

    @Override
    public boolean isClosed() throws SQLException {
        return false;
    }

    @Override
    public void setPoolable(boolean poolable) throws SQLException {

    }

    @Override
    public boolean isPoolable() throws SQLException {
        return false;
    }

    @Override
    public void closeOnCompletion() throws SQLException {

    }

    @Override
    public boolean isCloseOnCompletion() throws SQLException {
        return false;
    }

    public void close() throws SQLException {
       resultSet.close();
   }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        return null;
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return false;
    }
}
