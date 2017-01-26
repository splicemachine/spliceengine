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
package com.splicemachine.dbTesting.functionTests.util;

import java.io.InputStream;
import java.io.Reader;
import java.sql.ResultSet;
import java.sql.SQLException;


/**
 * A very simple, read-only, sample VTI.
 * <p>
 * This VTI is incomplete and has its quirks - it is intended for basic testing
 * only! Supported getters:
 * <ul> <li>getString</li>
 *      <li>getInt</li>
 * </ul>
 */
public class SampleVTI  {

    private static final String[][] oneColData = new String[][] {
            {"one"}, {"two"}, {"three"}, {"four"}, {"five"}
        };

    /** Returns a sample VTI that is empty (has zero rows). */
    public static ResultSet emptySampleVTI() {
        throw new UnsupportedOperationException("splice");
    }

    /**
     * Returns a sample VTI with the some test data.
     *
     *@return A result set with a single column with string data (five rows).
     */
    public static ResultSet oneColSampleVTI() {
        throw new UnsupportedOperationException("splice");
    }

    public static String[][] oneColSampleVTIData() {
        return (String[][])oneColData.clone();
    }

    private final String[][] data;
    private final int rows;
    private final int cols;
    private int index = -1;
    private boolean wasNull;
    private boolean closed;

    private SampleVTI(String[][] data) {
        this.data = data;
        this.rows = data.length;
        this.cols = rows == 0 ? 0 : data[0].length;
    }

    private String getColumn(int columnIndex)
            throws SQLException {
        if (closed) {
            throw new SQLException("result set closed");
        }
        if (columnIndex < 1 || columnIndex > cols) {
            throw new SQLException("column value out of range");
        }
        String val = data[index][columnIndex -1];
        wasNull = val == null;
        return val;
    }

    //@Override
    public boolean next() throws SQLException {
        if (closed) {
            throw new SQLException("result set closed");
        }
        return ++index < rows;
    }

    //@Override
    public void close() throws SQLException {
        this.closed = true;
    }

    //@Override
    public String getString(int columnIndex)
            throws SQLException {
        return getColumn(columnIndex);
    }

    //@Override
    public int getInt(int columnIndex)
            throws SQLException {
        String raw = getColumn(columnIndex);
        if (wasNull) {
            raw = "0";
        }
        try {
            return Integer.parseInt(raw);
        } catch (NumberFormatException nfe) {
            throw new SQLException("cannot get value as int");
        }
    }

    //@Override
    public boolean wasNull() {
        return wasNull;
    }

    public int getHoldability() throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    public boolean isClosed() throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    public void updateNString(int columnIndex, String nString) throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    public void updateNString(String columnLabel, String nString) throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    public String getNString(int columnIndex) throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    public String getNString(String columnLabel) throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    public Reader getNCharacterStream(int columnIndex) throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    public Reader getNCharacterStream(String columnLabel) throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    public void updateNCharacterStream(int columnIndex, Reader x, long length) throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    public void updateNCharacterStream(String columnLabel, Reader reader, long length) throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    public void updateAsciiStream(int columnIndex, InputStream x, long length) throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    public void updateBinaryStream(int columnIndex, InputStream x, long length) throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    public void updateCharacterStream(int columnIndex, Reader x, long length) throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    public void updateAsciiStream(String columnLabel, InputStream x, long length) throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    public void updateBinaryStream(String columnLabel, InputStream x, long length) throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    public void updateCharacterStream(String columnLabel, Reader reader, long length) throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    public void updateBlob(int columnIndex, InputStream inputStream, long length) throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    public void updateBlob(String columnLabel, InputStream inputStream, long length) throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    public void updateClob(int columnIndex, Reader reader, long length) throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    public void updateClob(String columnLabel, Reader reader, long length) throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    public void updateNClob(int columnIndex, Reader reader, long length) throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    public void updateNClob(String columnLabel, Reader reader, long length) throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    public void updateNCharacterStream(int columnIndex, Reader x) throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    public void updateNCharacterStream(String columnLabel, Reader reader) throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    public void updateAsciiStream(int columnIndex, InputStream x) throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    public void updateBinaryStream(int columnIndex, InputStream x) throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    public void updateCharacterStream(int columnIndex, Reader x) throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    public void updateAsciiStream(String columnLabel, InputStream x) throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    public void updateBinaryStream(String columnLabel, InputStream x) throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    public void updateCharacterStream(String columnLabel, Reader reader) throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    public void updateBlob(int columnIndex, InputStream inputStream) throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    public void updateBlob(String columnLabel, InputStream inputStream) throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    public void updateClob(int columnIndex, Reader reader) throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    public void updateClob(String columnLabel, Reader reader) throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    public void updateNClob(int columnIndex, Reader reader) throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    public void updateNClob(String columnLabel, Reader reader) throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    public Object getObject(int columnIndex, Class type) throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    public Object getObject(String columnLabel, Class type) throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }
}
