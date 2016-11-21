/*
 * ddlUtils is a subproject of the Apache DB project, and is licensed under
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
 * Splice Machine, Inc. has modified this file.
 *
 * All Splice Machine modifications are Copyright 2012 - 2016 Splice Machine, Inc.,
 * and are licensed to you under the License; you may not use this file except in
 * compliance with the License.
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */

package org.apache.ddlutils.platform;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;

/**
 * Describes a column in a metadata result set.
 *
 * @version $Revision: $
 */
public class MetaDataColumnDescriptor {
    /**
     * The name of the column.
     */
    private String _columnName;
    /**
     * The jdbc type to read from the result set.
     */
    private int _jdbcType;
    /**
     * The default value if the column is not present in the result set.
     */
    private Object _defaultValue;

    /**
     * Creates a new descriptor instance.
     *
     * @param columnName The name of the column
     * @param jdbcType   The jdbc type for reading from the result set, one of
     *                   VARCHAR, INTEGER, TINYINT, BIT
     */
    public MetaDataColumnDescriptor(String columnName, int jdbcType) {
        this(columnName, jdbcType, null);
    }

    /**
     * Creates a new descriptor instance.
     *
     * @param columnName   The name of the column
     * @param jdbcType     The jdbc type for reading from the result set, one of
     *                     VARCHAR, INTEGER, TINYINT, BIT
     * @param defaultValue The default value if the column is not present in the result set
     */
    public MetaDataColumnDescriptor(String columnName, int jdbcType, Object defaultValue) {
        _columnName = columnName.toUpperCase();
        _jdbcType = jdbcType;
        _defaultValue = defaultValue;
    }

    /**
     * Returns the name.
     *
     * @return The name
     */
    public String getName() {
        return _columnName;
    }

    /**
     * Returns the default value.
     *
     * @return The default value
     */
    public Object getDefaultValue() {
        return _defaultValue;
    }

    /**
     * Returns the jdbc type to read from the result set.
     *
     * @return The jdbc type
     */
    public int getJdbcType() {
        return _jdbcType;
    }

    /**
     * Reads the column from the result set.
     *
     * @param resultSet The result set
     * @return The column value or the default value if the column is not present in the result set
     */
    public Object readColumn(ResultSet resultSet) throws SQLException {
        Object result = null;
        try {
            switch (_jdbcType) {
                case Types.BIT:
                    result = new Boolean(resultSet.getBoolean(_columnName));
                    break;
                case Types.INTEGER:
                    result = new Integer(resultSet.getInt(_columnName));
                    break;
                case Types.TINYINT:
                    result = new Short(resultSet.getShort(_columnName));
                    break;
                default:
                    result = resultSet.getString(_columnName);
                    break;
            }
            if (resultSet.wasNull()) {
                result = null;
            }
        } catch (SQLException ex) {
            if (isColumnInResultSet(resultSet)) {
                String msg = "Column name: \"" + _columnName + "\" Type: " + _jdbcType + " => " + resultSet.getObject
                    (_columnName);
                throw new SQLException(msg, ex);
            } else {
                result = _defaultValue;
            }
        }
        return result;
    }

    /**
     * Determines whether a value for the specified column is present in the given result set.
     *
     * @param resultSet The result set
     * @return <code>true</code> if the column is present in the result set
     */
    private boolean isColumnInResultSet(ResultSet resultSet) throws SQLException {
        ResultSetMetaData metaData = resultSet.getMetaData();

        for (int idx = 1; idx <= metaData.getColumnCount(); idx++) {
            if (_columnName.equals(metaData.getColumnName(idx).toUpperCase())) {
                return true;
            }
        }
        return false;
    }
}
