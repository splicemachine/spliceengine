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

import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.regex.Pattern;

/**
 * Wrapper class for database meta data that stores additional info.
 */
public class DatabaseMetaDataWrapper {
    /**
     * Matches the characters not allowed in search strings.
     */
    private final Pattern searchStringPattern = Pattern.compile("[_%]");
    /**
     * The database meta data.
     */
    private DatabaseMetaData _metaData;
    /**
     * The catalog to acess in the database.
     */
    private String _catalog;
    /**
     * The schema(s) to acess in the database.
     */
    private String _schemaPattern;
    /**
     * The table types to process.
     */
    private String[] _tableTypes;

    /**
     * Returns the database meta data.
     *
     * @return The meta data
     */
    public DatabaseMetaData getMetaData() {
        return _metaData;
    }

    /**
     * Sets the database meta data.
     *
     * @param metaData The meta data
     */
    public void setMetaData(DatabaseMetaData metaData) {
        _metaData = metaData;
    }

    /**
     * Returns the catalog in the database to read.
     *
     * @return The catalog
     */
    public String getCatalog() {
        return _catalog;
    }

    /**
     * Sets the catalog in the database to read.
     *
     * @param catalog The catalog
     */
    public void setCatalog(String catalog) {
        _catalog = catalog;
    }

    /**
     * Returns the schema in the database to read.
     *
     * @return The schema
     */
    public String getSchemaPattern() {
        return _schemaPattern;
    }

    /**
     * Sets the schema in the database to read.
     *
     * @param schema The schema
     */
    public void setSchemaPattern(String schema) {
        _schemaPattern = schema;
    }

    /**
     * Returns the table types to recognize.
     *
     * @return The table types
     */
    public String[] getTableTypes() {
        if (_tableTypes == null) {
            return null;
        } else {
            String[] result = new String[_tableTypes.length];

            System.arraycopy(_tableTypes, 0, result, 0, _tableTypes.length);
            return result;
        }
    }

    /**
     * Sets the table types to recognize.
     *
     * @param types The table types
     */
    public void setTableTypes(String[] types) {
        if (types == null) {
            _tableTypes = null;
        } else {
            _tableTypes = new String[types.length];

            System.arraycopy(types, 0, _tableTypes, 0, types.length);
        }
    }

    /**
     * Escape a string literal so that it can be used as a search pattern.
     *
     * @param literalString The string to escape.
     * @return A string that can be properly used as a search string.
     * @throws SQLException If an error occurred retrieving the meta data
     */
    public String escapeForSearch(String literalString) throws SQLException {
        String escape = getMetaData().getSearchStringEscape();

        if (escape == null || escape.isEmpty()) {
            // No escape string, so nothing to do...
            return literalString;
        } else {
            // with Java 5, we would just use Matcher.quoteReplacement
            StringBuilder quotedEscape = new StringBuilder();

            for (int idx = 0; idx < escape.length(); idx++) {
                char c = escape.charAt(idx);

                switch (c) {
                    case '\\':
                        quotedEscape.append("\\\\");
                        break;
                    case '$':
                        quotedEscape.append("\\$");
                        break;
                    default:
                        quotedEscape.append(c);
                }
            }
            quotedEscape.append("$0");

            return searchStringPattern.matcher(literalString).replaceAll(quotedEscape.toString());
        }
    }

    /**
     * Convenience method to return the schema meta data using the configured catalog,
     * schema pattern.
     *
     * @param schemaNamePattern The pattern identifying for which schemas to return info
     * @return The schema meta data
     * @throws SQLException If an error occurred retrieving the meta data
     */
    public ResultSet getSchemas(String schemaNamePattern) throws SQLException {
        return getMetaData().getSchemas(getCatalog(), schemaNamePattern);
    }

    /**
     * Convenience method to return the table meta data using the configured catalog,
     * schema pattern and table types.
     *
     * @param tableNamePattern The pattern identifying for which tables to return info
     * @return The table meta data
     * @throws SQLException If an error occurred retrieving the meta data
     * @see DatabaseMetaData#getTables(java.lang.String, java.lang.String, java.lang.String, java.lang.String[])
     */
    public ResultSet getTables(String tableNamePattern) throws SQLException {
        return getMetaData().getTables(getCatalog(), getSchemaPattern(), tableNamePattern, getTableTypes());
    }

    /**
     * Convenience method to return the column meta data using the configured catalog and
     * schema pattern.
     *
     * @param tableNamePattern  The pattern identifying for which tables to return info
     * @param columnNamePattern The pattern identifying for which columns to return info
     * @return The column meta data
     * @throws SQLException If an error occurred retrieving the meta data
     * @see DatabaseMetaData#getColumns(java.lang.String, java.lang.String, java.lang.String, java.lang.String)
     */
    public ResultSet getColumns(String tableNamePattern, String columnNamePattern) throws SQLException {
        return getMetaData().getColumns(getCatalog(), getSchemaPattern(), tableNamePattern, columnNamePattern);
    }

    /**
     * Convenience method to return the primary key meta data using the configured catalog and
     * schema pattern.
     *
     * @param tableNamePattern The pattern identifying for which tables to return info
     * @return The primary key meta data
     * @throws SQLException If an error occurred retrieving the meta data
     * @see DatabaseMetaData#getPrimaryKeys(java.lang.String, java.lang.String, java.lang.String)
     */
    public ResultSet getPrimaryKeys(String tableNamePattern) throws SQLException {
        return getMetaData().getPrimaryKeys(getCatalog(), getSchemaPattern(), tableNamePattern);
    }

    /**
     * Convenience method to return the foreign key meta data using the configured catalog and
     * schema pattern.
     *
     * @param tableNamePattern The pattern identifying for which tables to return info
     * @return The foreign key meta data
     * @throws SQLException If an error occurred retrieving the meta data
     * @see DatabaseMetaData#getImportedKeys(java.lang.String, java.lang.String, java.lang.String)
     */
    public ResultSet getForeignKeys(String tableNamePattern) throws SQLException {
        return getMetaData().getImportedKeys(getCatalog(), getSchemaPattern(), tableNamePattern);
    }

    /**
     * Convenience method to return the index meta data using the configured catalog and
     * schema pattern.
     *
     * @param tableNamePattern The pattern identifying for which tables to return info
     * @param unique           Whether to return only indices for unique values
     * @param approximate      Whether the result is allowed to reflect approximate or out of data values
     * @return The index meta data
     * @throws SQLException If an error occurred retrieving the meta data
     * @see DatabaseMetaData#getIndexInfo(java.lang.String, java.lang.String, java.lang.String, boolean, boolean)
     */
    public ResultSet getIndices(String tableNamePattern, boolean unique, boolean approximate) throws SQLException {
        return getMetaData().getIndexInfo(getCatalog(), getSchemaPattern(), tableNamePattern, unique, approximate);
    }
}
