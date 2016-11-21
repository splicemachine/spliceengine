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

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.text.Collator;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.collections.map.ListOrderedMap;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.ddlutils.Platform;
import org.apache.ddlutils.PlatformInfo;
import org.apache.ddlutils.model.CascadeActionEnum;
import org.apache.ddlutils.model.Column;
import org.apache.ddlutils.model.Database;
import org.apache.ddlutils.model.ForeignKey;
import org.apache.ddlutils.model.Index;
import org.apache.ddlutils.model.IndexColumn;
import org.apache.ddlutils.model.NonUniqueIndex;
import org.apache.ddlutils.model.Reference;
import org.apache.ddlutils.model.Schema;
import org.apache.ddlutils.model.Table;
import org.apache.ddlutils.model.TableType;
import org.apache.ddlutils.model.UniqueIndex;
import org.apache.ddlutils.util.StringUtilsExt;

/**
 * An utility class to create a Database model from a live database.
 *
 * @version $Revision: 1002945 $
 */
public class JdbcModelReader {
    /**
     * The Log to which logging calls will be made.
     */
    private final Log _log = LogFactory.getLog(getClass());

    /**
     * The descriptors for the relevant columns in the table meta data.
     */
    private final List<MetaDataColumnDescriptor> _columnsForTable;
    /**
     * The descriptors for the relevant columns in the table column meta data.
     */
    private final List<MetaDataColumnDescriptor> _columnsForColumn;
    /**
     * The descriptors for the relevant columns in the primary key meta data.
     */
    private final List<MetaDataColumnDescriptor> _columnsForPK;
    /**
     * The descriptors for the relevant columns in the foreign key meta data.
     */
    private final List<MetaDataColumnDescriptor> _columnsForFK;
    /**
     * The descriptors for the relevant columns in the index meta data.
     */
    private final List<MetaDataColumnDescriptor> _columnsForIndex;
    /**
     * The active connection while reading a database model.
     */
    protected Connection _connection;
    /**
     * The platform that this model reader belongs to.
     */
    private Platform _platform;
    /**
     * Contains default column sizes (minimum sizes that a JDBC-compliant db must support).
     */
    private HashMap<Integer, String> _defaultSizes = new HashMap<>();
    /**
     * The default database catalog to read.
     */
    private String _defaultCatalogPattern = "%";
    /**
     * The default database schema(s) to read.
     */
    private String _defaultSchemaPattern = "%";
    /**
     * The default pattern for reading all tables.
     */
    private String _defaultTablePattern = "%";
    /**
     * The default pattern for reading all columns.
     */
    private String _defaultColumnPattern;

    /**
     * System table types
     * @see TableType
     */
    public static final String[] SYS_TABLE_TYPES = {"SYSTEM TABLE"};

    /**
     * All non system table types
     * @see TableType
     */
    public static final String[] ALL_NON_SYS_TABLE_TYPES = {"TABLE", "VIEW", "GLOBAL TEMPORARY", "LOCAL TEMPORARY", "ALIAS", "SYNONYM"};

    /**
     * All table types
     * @see TableType
     */
    public static final String[] ALL_TABLE_TYPES = {"SYSTEM TABLE", "TABLE", "VIEW", "GLOBAL TEMPORARY", "LOCAL TEMPORARY", "ALIAS", "SYNONYM"};

    /**
     * The table types to recognize per default - {@link #ALL_NON_SYS_TABLE_TYPES all table types} - but can be set by caller.
     */
    private String[] _defaultTableTypes;

    /**
     * Creates a new model reader instance.
     *
     * @param platform The platform this builder belongs to
     */
    public JdbcModelReader(Platform platform) {
        _defaultTableTypes = new String[ALL_NON_SYS_TABLE_TYPES.length];

        System.arraycopy(ALL_NON_SYS_TABLE_TYPES, 0, _defaultTableTypes, 0, ALL_NON_SYS_TABLE_TYPES.length);

        _platform = platform;

        _defaultSizes.put(Types.CHAR, "254");
        _defaultSizes.put(Types.VARCHAR, "254");
        _defaultSizes.put(Types.LONGVARCHAR, "254");
        _defaultSizes.put(Types.BINARY, "254");
        _defaultSizes.put(Types.VARBINARY, "254");
        _defaultSizes.put(Types.LONGVARBINARY, "254");
        _defaultSizes.put(Types.INTEGER, "32");
        _defaultSizes.put(Types.BIGINT, "64");
        _defaultSizes.put(Types.REAL, "7,0");
        _defaultSizes.put(Types.FLOAT, "15,0");
        _defaultSizes.put(Types.DOUBLE, "15,0");
        _defaultSizes.put(Types.DECIMAL, "15,15");
        _defaultSizes.put(Types.NUMERIC, "15,15");

        _columnsForTable = initColumnsForTable();
        _columnsForColumn = initColumnsForColumn();
        _columnsForPK = initColumnsForPK();
        _columnsForFK = initColumnsForFK();
        _columnsForIndex = initColumnsForIndex();
    }

    /**
     * Returns the log used by the model reader.
     *
     * @return The log
     */
    protected Log getLog() {
        return _log;
    }

    /**
     * Returns the platform that this model reader belongs to.
     *
     * @return The platform
     */
    public Platform getPlatform() {
        return _platform;
    }

    /**
     * Returns the platform specific settings.
     *
     * @return The platform settings
     */
    public PlatformInfo getPlatformInfo() {
        return _platform.getPlatformInfo();
    }

    /**
     * Returns descriptors for the columns that shall be read from the result set when
     * reading the meta data for a table. Note that the columns are read in the order
     * defined by this list.<br/>
     * Redefine this method if you want more columns or a different order.
     *
     * @return The descriptors for the result set columns
     */
    protected List<MetaDataColumnDescriptor> initColumnsForTable() {
        List<MetaDataColumnDescriptor> result = new ArrayList<>();

        result.add(new MetaDataColumnDescriptor("TABLE_NAME", Types.VARCHAR));
        result.add(new MetaDataColumnDescriptor("TABLE_TYPE", Types.VARCHAR, "UNKNOWN"));
        result.add(new MetaDataColumnDescriptor("TABLE_CAT", Types.VARCHAR));
        result.add(new MetaDataColumnDescriptor("TABLE_SCHEM", Types.VARCHAR));
        result.add(new MetaDataColumnDescriptor("REMARKS", Types.VARCHAR));

        return result;
    }

    /**
     * Returns descriptors for the columns that shall be read from the result set when
     * reading the meta data for table columns. Note that the columns are read in the order
     * defined by this list.<br/>
     * Redefine this method if you want more columns or a different order.
     *
     * @return The map column name -> descriptor for the result set columns
     */
    protected List<MetaDataColumnDescriptor> initColumnsForColumn() {
        List<MetaDataColumnDescriptor> result = new ArrayList<>();

        // As suggested by Alexandre Borgoltz, we're reading the COLUMN_DEF first because Oracle
        // has problems otherwise (it seemingly requires a LONG column to be the first to be read)
        // See also DDLUTILS-29
        result.add(new MetaDataColumnDescriptor("COLUMN_DEF", Types.VARCHAR));
        // we're also reading the table name so that a model reader impl can filter manually
        result.add(new MetaDataColumnDescriptor("TABLE_NAME", Types.VARCHAR));
        result.add(new MetaDataColumnDescriptor("COLUMN_NAME", Types.VARCHAR));
        result.add(new MetaDataColumnDescriptor("DATA_TYPE", Types.INTEGER, Types.OTHER));
        result.add(new MetaDataColumnDescriptor("NUM_PREC_RADIX", Types.INTEGER, 10));
        result.add(new MetaDataColumnDescriptor("DECIMAL_DIGITS", Types.INTEGER, 0));
        result.add(new MetaDataColumnDescriptor("COLUMN_SIZE", Types.VARCHAR));
        result.add(new MetaDataColumnDescriptor("IS_NULLABLE", Types.VARCHAR, "YES"));
        result.add(new MetaDataColumnDescriptor("REMARKS", Types.VARCHAR));
        result.add(new MetaDataColumnDescriptor("ORDINAL_POSITION", Types.INTEGER));

        return result;
    }

    /**
     * Returns descriptors for the columns that shall be read from the result set when
     * reading the meta data for primary keys. Note that the columns are read in the order
     * defined by this list.<br/>
     * Redefine this method if you want more columns or a different order.
     *
     * @return The map column name -> descriptor for the result set columns
     */
    protected List<MetaDataColumnDescriptor> initColumnsForPK() {
        List<MetaDataColumnDescriptor> result = new ArrayList<>();

        result.add(new MetaDataColumnDescriptor("COLUMN_NAME", Types.VARCHAR));
        // we're also reading the table name so that a model reader impl can filter manually
        result.add(new MetaDataColumnDescriptor("TABLE_NAME", Types.VARCHAR));
        // the name of the primary key is currently only interesting to the pk index name resolution
        result.add(new MetaDataColumnDescriptor("PK_NAME", Types.VARCHAR));

        return result;
    }

    /**
     * Returns descriptors for the columns that shall be read from the result set when
     * reading the meta data for foreign keys originating from a table. Note that the
     * columns are read in the order defined by this list.<br/>
     * Redefine this method if you want more columns or a different order.
     *
     * @return The map column name -> descriptor for the result set columns
     */
    protected List<MetaDataColumnDescriptor> initColumnsForFK() {
        List<MetaDataColumnDescriptor> result = new ArrayList<>();

        result.add(new MetaDataColumnDescriptor("PKTABLE_NAME", Types.VARCHAR));
        // we're also reading the table name so that a model reader impl can filter manually
        result.add(new MetaDataColumnDescriptor("FKTABLE_NAME", Types.VARCHAR));
        result.add(new MetaDataColumnDescriptor("KEY_SEQ", Types.TINYINT, (short) 0));
        result.add(new MetaDataColumnDescriptor("FK_NAME", Types.VARCHAR));
        result.add(new MetaDataColumnDescriptor("UPDATE_RULE", Types.TINYINT));
        result.add(new MetaDataColumnDescriptor("DELETE_RULE", Types.TINYINT));
        result.add(new MetaDataColumnDescriptor("PKCOLUMN_NAME", Types.VARCHAR));
        result.add(new MetaDataColumnDescriptor("FKCOLUMN_NAME", Types.VARCHAR));

        return result;
    }

    /**
     * Returns descriptors for the columns that shall be read from the result set when
     * reading the meta data for indices. Note that the columns are read in the order
     * defined by this list.<br/>
     * Redefine this method if you want more columns or a different order.
     *
     * @return The map column name -> descriptor for the result set columns
     */
    protected List<MetaDataColumnDescriptor> initColumnsForIndex() {
        List<MetaDataColumnDescriptor> result = new ArrayList<>();

        result.add(new MetaDataColumnDescriptor("INDEX_NAME", Types.VARCHAR));
        // we're also reading the table name so that a model reader impl can filter manually
        result.add(new MetaDataColumnDescriptor("TABLE_NAME", Types.VARCHAR));
        result.add(new MetaDataColumnDescriptor("NON_UNIQUE", Types.BIT, Boolean.TRUE));
        result.add(new MetaDataColumnDescriptor("ORDINAL_POSITION", Types.TINYINT, (short) 0));
        result.add(new MetaDataColumnDescriptor("COLUMN_NAME", Types.VARCHAR));
        result.add(new MetaDataColumnDescriptor("TYPE", Types.VARCHAR));

        return result;
    }

    /**
     * Returns the catalog(s) in the database to read per default.
     *
     * @return The default catalog(s)
     */
    public String getDefaultCatalogPattern() {
        return _defaultCatalogPattern;
    }

    /**
     * Sets the catalog(s) in the database to read per default.
     *
     * @param catalogPattern The catalog(s)
     */
    public void setDefaultCatalogPattern(String catalogPattern) {
        _defaultCatalogPattern = catalogPattern;
    }

    /**
     * Returns the schema(s) in the database to read per default.
     *
     * @return The default schema(s)
     */
    public String getDefaultSchemaPattern() {
        return _defaultSchemaPattern;
    }

    /**
     * Sets the schema(s) in the database to read per default.
     *
     * @param schemaPattern The schema(s)
     */
    public void setDefaultSchemaPattern(String schemaPattern) {
        _defaultSchemaPattern = schemaPattern;
    }

    /**
     * Returns the default pattern to read the relevant tables from the database.
     *
     * @return The table pattern
     */
    public String getDefaultTablePattern() {
        return _defaultTablePattern;
    }

    /**
     * Sets the default pattern to read the relevant tables from the database.
     *
     * @param tablePattern The table pattern
     */
    public void setDefaultTablePattern(String tablePattern) {
        _defaultTablePattern = tablePattern;
    }

    /**
     * Returns the default pattern to read the relevant columns from the database.
     *
     * @return The column pattern
     */
    public String getDefaultColumnPattern() {
        return _defaultColumnPattern;
    }

    /**
     * Sets the default pattern to read the relevant columns from the database.
     *
     * @param columnPattern The column pattern
     */
    public void setDefaultColumnPattern(String columnPattern) {
        _defaultColumnPattern = columnPattern;
    }

    /**
     * Returns the table types to recognize per default.
     *
     * @return The default table types
     */
    public String[] getDefaultTableTypes() {
        if (_defaultTableTypes == null) {
            return null;
        } else {
            String[] result = new String[_defaultTableTypes.length];

            System.arraycopy(_defaultTableTypes, 0, result, 0, _defaultTableTypes.length);
            return result;
        }
    }

    /**
     * Sets the table types to recognize per default. Typical types are "TABLE", "VIEW",
     * "SYSTEM TABLE", "GLOBAL TEMPORARY", "LOCAL TEMPORARY", "ALIAS", "SYNONYM".
     *
     * @param types The table types
     */
    public void setDefaultTableTypes(String[] types) {
        if (types == null) {
            _defaultTableTypes = null;
        } else {
            _defaultTableTypes = new String[types.length];

            System.arraycopy(types, 0, types, 0, types.length);
        }
    }

    /**
     * Returns the descriptors for the columns to be read from the table meta data result set.
     *
     * @return The column descriptors
     */
    protected List<MetaDataColumnDescriptor> getColumnsForTable() {
        return _columnsForTable;
    }

    /**
     * Returns the descriptors for the columns to be read from the column meta data result set.
     *
     * @return The column descriptors
     */
    protected List<MetaDataColumnDescriptor> getColumnsForColumn() {
        return _columnsForColumn;
    }

    /**
     * Returns the descriptors for the columns to be read from the primary key meta data result set.
     *
     * @return The column descriptors
     */
    protected List<MetaDataColumnDescriptor> getColumnsForPK() {
        return _columnsForPK;
    }

    /**
     * Returns the descriptors for the columns to be read from the foreign key meta data result set.
     *
     * @return The column descriptors
     */
    protected List<MetaDataColumnDescriptor> getColumnsForFK() {
        return _columnsForFK;
    }

    /**
     * Returns the descriptors for the columns to be read from the index meta data result set.
     *
     * @return The column descriptors
     */
    protected List<MetaDataColumnDescriptor> getColumnsForIndex() {
        return _columnsForIndex;
    }

    /**
     * Returns the active connection. Note that this is only set during a call to
     * {@link #getDatabase(Connection, String, String, String, String[])}.
     *
     * @return The connection or <code>null</code> if there is no active connection
     */
    protected Connection getConnection() {
        return _connection;
    }

    /**
     * Reads the database model from the given connection.
     *
     * @param connection The connection
     * @param name       The name of the resulting database; <code>null</code> when the default name (the catalog)
     *                   is desired which might be <code>null</code> itself though
     * @return The database model
     */
    public Database getDatabase(Connection connection, String name) throws SQLException {
        return getDatabase(connection, name, null, null, null);
    }

    /**
     * Reads the database model from the given connection.
     *
     * @param connection    The connection
     * @param name          The name of the resulting database; <code>null</code> when the default name (the catalog)
     *                      is desired which might be <code>null</code> itself though
     * @param catalog       The catalog to acess in the database; use <code>null</code> for the default value
     * @param schemaPattern The schema to access in the database; use <code>null</code> for the default value (all)
     * @param tableTypes    The table types to process; use <code>null</code> or an empty list for the default ones
     * @return The database model
     */
    public Database getDatabase(Connection connection, String name, String catalog, String schemaPattern, String[] tableTypes)
        throws SQLException {
        Database db = new Database();

        if (name == null) {
            try {
                db.setName(connection.getCatalog());
                if (catalog == null) {
                    catalog = db.getName();
                }
            } catch (Exception ex) {
                _log.info("Cannot determine the catalog name from connection.", ex);
            }
        } else {
            db.setName(name);
        }
        try {
            _connection = connection;
            for (Schema schema : readSchemas(catalog, schemaPattern)) {
                db.addSchema(schema);
                db.addTablesIntoSchema(schema, readTables(catalog, schema, tableTypes));
                // Note that we do this here instead of in readTable since platforms may redefine the
                // readTable method whereas it is highly unlikely that this method gets redefined
                if (getPlatform().isForeignKeysSorted()) {
                    sortForeignKeys(db);
                }
            }
            addUsers(db);
            addRoles(db);
            addPermissions(db);
        } finally {
            _connection = null;
        }
        db.initialize();
        return db;
    }

    protected void addUsers(Database db) throws SQLException {
        // Optional method. Default implementation does nothing.
    }

    protected void addRoles(Database db) throws SQLException {
        // Optional method. Default implementation does nothing.
    }

    protected void addPermissions(Database db) throws SQLException {
        // Optional method. Default implementation does nothing.
    }

    protected Iterable<? extends Schema> readSchemas(String catalog, String schemaPattern) throws SQLException {
        ResultSet schemaData = null;

        try {
            DatabaseMetaDataWrapper metaData = new DatabaseMetaDataWrapper();

            metaData.setMetaData(_connection.getMetaData());
            metaData.setCatalog(catalog == null ? getDefaultCatalogPattern() : catalog);
            metaData.setSchemaPattern(schemaPattern == null ? getDefaultSchemaPattern() : schemaPattern);

            schemaData = metaData.getSchemas(schemaPattern);

            List<Schema> schemas = new ArrayList<>();

            while (schemaData.next()) {
                schemas.add(new Schema(schemaData.getString(1), "", ""));
            }

            Collections.sort(schemas);
            return schemas;
        } finally {
            closeResultSet(schemaData);
        }
    }

    /**
     * Reads the tables from the database metadata for the given schema.
     *
     * @param catalog    The catalog to acess in the database; use <code>null</code> for the default value
     * @param schema     The schema to access in the database; non- <code>null</code>
     * @param tableTypes The table types to process; use <code>null</code> or an empty list for the default ones
     * @return The tables
     */
    protected Collection<Table> readTables(String catalog, Schema schema, String[] tableTypes) throws SQLException {
        ResultSet tableData = null;
        boolean lookingForViews = false;
        String[] interestedTableTypes;
        if (tableTypes == null || tableTypes.length == 0) {
            interestedTableTypes = getDefaultTableTypes();
        } else {
            interestedTableTypes = new String[tableTypes.length];
            System.arraycopy(tableTypes, 0, interestedTableTypes, 0, tableTypes.length);
        }
        lookingForViews = StringUtilsExt.contains(TableType.VIEW.type(), interestedTableTypes);
        if (lookingForViews) {
            interestedTableTypes = StringUtilsExt.remove(TableType.VIEW.type(), interestedTableTypes);
        }

        try {
            DatabaseMetaDataWrapper metaData = new DatabaseMetaDataWrapper();

            metaData.setMetaData(_connection.getMetaData());
            metaData.setCatalog(catalog == null ? getDefaultCatalogPattern() : catalog);
            metaData.setSchemaPattern(schema.getSchemaName());
            metaData.setTableTypes(interestedTableTypes);

            tableData = metaData.getTables(getDefaultTablePattern());

            List<Table> tables = new ArrayList<>();

            while (tableData.next()) {
                Map values = readColumns(tableData, getColumnsForTable());
                Table table = readTable(metaData, schema, values);

                if (table != null && (lookingForViews && ! table.getType().equals(TableType.VIEW))) {
                    tables.add(table);
                }
            }

            final Collator collator = Collator.getInstance();

            Collections.sort(tables, new Comparator<Table>() {
                public int compare(Table obj1, Table obj2) {
                    return collator.compare(obj1.getQualifiedName().toUpperCase(), obj2.getQualifiedName().toUpperCase());
                }
            });

            // Add views after table dependencies
            if (lookingForViews) {
                metaData.setTableTypes(new String[] {TableType.VIEW.type()});
                tableData = metaData.getTables(getDefaultTablePattern());
                while (tableData.next()) {
                    Map values = readColumns(tableData, getColumnsForTable());
                    Table table = readTable(metaData, schema, values);
                    if (table != null) {
                        table.setViewDefinition(readViewDefinition(table));
                        tables.add(table);
                    }
                }
            }

            return tables;
        } finally {
            closeResultSet(tableData);
        }
    }

    /**
     * Reads the next table from the meta data.
     *
     * @param metaData The database meta data
     * @param schema   The schema in which the table resides.
     * @param values   The table metadata values as defined by {@link #getColumnsForTable()}  @return The table or
     *                 <code>null</code> if the result set row did not contain a valid table
     */
    protected Table readTable(DatabaseMetaDataWrapper metaData, Schema schema, Map values) throws SQLException {
        String tableName = (String) values.get("TABLE_NAME");
        Table table = null;

        if ((tableName != null) && (tableName.length() > 0)) {
            table = new Table();

            table.setName(tableName);
            table.setType(TableType.of((String) values.get("TABLE_TYPE")));
            table.setCatalog((String) values.get("TABLE_CAT"));
            table.setSchema(schema);
            table.setDescription((String) values.get("REMARKS"));
            setTableInternalId(table);

            table.addColumns(readColumns(metaData, table));
            table.addForeignKeys(readForeignKeys(metaData, tableName));
            table.addIndices(readIndices(metaData, tableName));

            Collection primaryKeys = readPrimaryKeyNames(metaData, tableName);

            for (Object primaryKey : primaryKeys) {
                table.findColumn((String) primaryKey, true).setPrimaryKey(true);
            }

            if (getPlatformInfo().isSystemIndicesReturned()) {
                removeSystemIndices(metaData, table);
            }

            // opportunity to post process the created table
            postProcessTable(table);
        }
        return table;
    }

    protected void postProcessTable(Table table) {
        // must be overridden by a specific reader
    }

    protected void setTableInternalId(Table table) throws SQLException {
        // must be overridden by a specific reader
    }

    /**
     * Read the view definition. The result will be the SQL string that created the given view.
     * @param view the view for which to record the definition
     * @return The SQL string that created the given view.
     */
    protected String readViewDefinition(Table view) throws SQLException {
        // must be overridden by a specific reader
        return "";
    }

    /**
     * Removes system indices (generated by the database for primary and foreign keys)
     * from the table.
     *
     * @param metaData The database meta data
     * @param table    The table
     */
    protected void removeSystemIndices(DatabaseMetaDataWrapper metaData, Table table) throws SQLException {
        removeInternalPrimaryKeyIndex(metaData, table);

        for (int fkIdx = 0; fkIdx < table.getForeignKeyCount(); fkIdx++) {
            removeInternalForeignKeyIndex(metaData, table, table.getForeignKey(fkIdx));
        }
    }

    /**
     * Tries to remove the internal index for the table's primary key.
     *
     * @param metaData The database meta data
     * @param table    The table
     */
    protected void removeInternalPrimaryKeyIndex(DatabaseMetaDataWrapper metaData, Table table) throws SQLException {
        Column[] pks = table.getPrimaryKeyColumns();
        List<String> columnNames = new ArrayList<>();

        for (int columnIdx = 0; columnIdx < pks.length; columnIdx++) {
            columnNames.add(pks[columnIdx].getName());
        }

        for (int indexIdx = 0; indexIdx < table.getIndexCount(); ) {
            Index index = table.getIndex(indexIdx);

            if (index.isUnique() && matches(index, columnNames) &&
                isInternalPrimaryKeyIndex(metaData, table, index)) {
                table.removeIndex(indexIdx);
            } else {
                indexIdx++;
            }
        }
    }

    /**
     * Tries to remove the internal index for the given foreign key.
     *
     * @param metaData The database meta data
     * @param table    The table where the table is defined
     * @param fk       The foreign key
     */
    protected void removeInternalForeignKeyIndex(DatabaseMetaDataWrapper metaData, Table table, ForeignKey fk) throws
        SQLException {
        List<String> columnNames = new ArrayList<>();
        boolean mustBeUnique = !getPlatformInfo().isSystemForeignKeyIndicesAlwaysNonUnique();

        for (int columnIdx = 0; columnIdx < fk.getReferenceCount(); columnIdx++) {
            String name = fk.getReference(columnIdx).getLocalColumnName();
            Column localColumn = table.findColumn(name,
                                                  getPlatform().isDelimitedIdentifierModeOn());

            if (mustBeUnique && !localColumn.isPrimaryKey()) {
                mustBeUnique = false;
            }
            columnNames.add(name);
        }

        for (int indexIdx = 0; indexIdx < table.getIndexCount(); ) {
            Index index = table.getIndex(indexIdx);

            if ((!mustBeUnique || index.isUnique()) && matches(index, columnNames) &&
                isInternalForeignKeyIndex(metaData, table, fk, index)) {
                fk.setAutoIndexPresent(true);
                table.removeIndex(indexIdx);
            } else {
                indexIdx++;
            }
        }
    }

    /**
     * Checks whether the given index matches the column list.
     *
     * @param index              The index
     * @param columnsToSearchFor The names of the columns that the index should be for
     * @return <code>true</code> if the index matches the columns
     */
    protected boolean matches(Index index, List<String> columnsToSearchFor) {
        if (index.getColumnCount() != columnsToSearchFor.size()) {
            return false;
        }
        for (int columnIdx = 0; columnIdx < index.getColumnCount(); columnIdx++) {
            if (!columnsToSearchFor.get(columnIdx).equals(index.getColumn(columnIdx).getName())) {
                return false;
            }
        }
        return true;
    }

    /**
     * Tries to determine whether the index is the internal database-generated index
     * for the given table's primary key.
     * Note that only unique indices with the correct columns are fed to this method.
     * Redefine this method for specific platforms if there are better ways
     * to determine internal indices.
     *
     * @param metaData The database meta data
     * @param table    The table owning the index
     * @param index    The index to check
     * @return <code>true</code> if the index seems to be an internal primary key one
     */
    protected boolean isInternalPrimaryKeyIndex(DatabaseMetaDataWrapper metaData, Table table, Index index) throws SQLException {
        return false;
    }

    /**
     * Tries to determine whether the index is the internal database-generated index
     * for the given foreign key.
     * Note that only non-unique indices with the correct columns are fed to this method.
     * Redefine this method for specific platforms if there are better ways
     * to determine internal indices.
     *
     * @param metaData The database meta data
     * @param table    The table owning the index and foreign key
     * @param fk       The foreign key
     * @param index    The index to check
     * @return <code>true</code> if the index seems to be an internal primary key one
     */
    protected boolean isInternalForeignKeyIndex(DatabaseMetaDataWrapper metaData, Table table, ForeignKey fk, Index index)
        throws SQLException {
        return false;
    }

    /**
     * Reads the column definitions for the indicated table.
     *
     * @param metaData  The database meta data
     * @param table The the table
     * @return The columns
     */
    protected Collection readColumns(DatabaseMetaDataWrapper metaData, Table table) throws SQLException {
        ResultSet columnData = null;

        try {
            columnData = metaData.getColumns(metaData.escapeForSearch(table.getName()), getDefaultColumnPattern());

            List<Column> columns = new ArrayList<>();

            while (columnData.next()) {
                Map<String, Object> values = readColumns(columnData, getColumnsForColumn());
                if (table.hasInternalId()) {
                    values.put("SCHEMA_ID", table.getSchema().getSchemaId());
                    values.put("TABLE_ID", table.getInternalId());
                }

                columns.add(readColumn(metaData, values));
            }
            return columns;
        } finally {
            closeResultSet(columnData);
        }
    }

    /**
     * Extracts a column definition from the result set.
     *
     * @param metaData The database meta data
     * @param values   The column meta data values as defined by {@link #getColumnsForColumn()}
     * @return The column
     */
    protected Column readColumn(DatabaseMetaDataWrapper metaData, Map<String,Object> values) throws SQLException {
        Column column = new Column();

        column.setName((String) values.get("COLUMN_NAME"));
        column.setDefaultValue((String) values.get("COLUMN_DEF"));
        column.setTypeCode((Integer) values.get("DATA_TYPE"));
        column.setOrdinalPosition((Integer) values.get("ORDINAL_POSITION"));
        column.setSchemaId((String) values.get("SCHEMA_ID"));
        column.setTableId((String) values.get("TABLE_ID"));

        Integer precision = (Integer) values.get("NUM_PREC_RADIX");

        if (precision != null) {
            column.setPrecisionRadix(precision);
        }

        String size = (String) values.get("COLUMN_SIZE");

        if (size == null) {
            size = _defaultSizes.get(column.getTypeCode());
        }
        // we're setting the size after the precision and radix in case
        // the database prefers to return them in the size value
        column.setSize(size);

        Integer scale = (Integer) values.get("DECIMAL_DIGITS");

        if (scale != null) {
            // if there is a scale value, set it after the size (which probably did not contain
            // a scale specification)
            column.setScale(scale);
        }
        column.setRequired("NO".equalsIgnoreCase(((String) values.get("IS_NULLABLE")).trim()));

        String description = (String) values.get("REMARKS");

        if (!org.apache.ddlutils.util.StringUtilsExt.isEmpty(description)) {
            column.setDescription(description);
        }
        return column;
    }

    /**
     * Retrieves the names of the columns that make up the primary key for a given table.
     *
     * @param metaData  The database meta data
     * @param tableName The name of the table from which to retrieve PK information
     * @return The primary key column names
     */
    protected Collection readPrimaryKeyNames(DatabaseMetaDataWrapper metaData, String tableName) throws SQLException {
        List<String> pks = new ArrayList<>();
        ResultSet pkData = null;

        try {
            pkData = metaData.getPrimaryKeys(metaData.escapeForSearch(tableName));
            while (pkData.next()) {
                Map values = readColumns(pkData, getColumnsForPK());

                pks.add(readPrimaryKeyName(metaData, values));
            }
        } finally {
            closeResultSet(pkData);
        }
        return pks;
    }

    /**
     * Extracts a primary key name from the result set.
     *
     * @param metaData The database meta data
     * @param values   The primary key meta data values as defined by {@link #getColumnsForPK()}
     * @return The primary key name
     */
    protected String readPrimaryKeyName(DatabaseMetaDataWrapper metaData, Map values) throws SQLException {
        return (String) values.get("COLUMN_NAME");
    }

    /**
     * Retrieves the foreign keys of the indicated table.
     *
     * @param metaData  The database meta data
     * @param tableName The name of the table from which to retrieve FK information
     * @return The foreign keys
     */
    protected Collection readForeignKeys(DatabaseMetaDataWrapper metaData, String tableName) throws SQLException {
        Map fks = new ListOrderedMap();
        ResultSet fkData = null;

        try {
            fkData = metaData.getForeignKeys(metaData.escapeForSearch(tableName));

            while (fkData.next()) {
                Map values = readColumns(fkData, getColumnsForFK());

                readForeignKey(metaData, values, fks);
            }
        } finally {
            closeResultSet(fkData);
        }
        return fks.values();
    }

    /**
     * Reads the next foreign key spec from the result set.
     *
     * @param metaData The database meta data
     * @param values   The foreign key meta data as defined by {@link #getColumnsForFK()}
     * @param knownFks The already read foreign keys for the current table
     */
    protected void readForeignKey(DatabaseMetaDataWrapper metaData, Map values, Map knownFks) throws SQLException {
        String fkName = (String) values.get("FK_NAME");
        ForeignKey fk = (ForeignKey) knownFks.get(fkName);

        if (fk == null) {
            fk = new ForeignKey(fkName);
            fk.setForeignTableName((String) values.get("PKTABLE_NAME"));

            CascadeActionEnum onUpdateAction = convertAction((Short) values.get("UPDATE_RULE"));
            CascadeActionEnum onDeleteAction = convertAction((Short) values.get("DELETE_RULE"));

            if (onUpdateAction == null) {
                onUpdateAction = getPlatformInfo().getDefaultOnUpdateAction();
            }
            if (onDeleteAction == null) {
                onDeleteAction = getPlatformInfo().getDefaultOnDeleteAction();
            }

            fk.setOnUpdate(onUpdateAction);
            fk.setOnDelete(onDeleteAction);
            knownFks.put(fkName, fk);
        }

        Reference ref = new Reference();

        ref.setForeignColumnName((String) values.get("PKCOLUMN_NAME"));
        ref.setLocalColumnName((String) values.get("FKCOLUMN_NAME"));
        if (values.containsKey("KEY_SEQ")) {
            ref.setSequenceValue(((Short) values.get("KEY_SEQ")).intValue());
        }
        fk.addReference(ref);
    }

    /**
     * Converts the JDBC action value (one of the <code>importKey</code> constants in the
     * {@link DatabaseMetaData} class) to a {@link CascadeActionEnum}.
     *
     * @param jdbcActionValue The jdbc action value
     * @return The enum value
     */
    protected CascadeActionEnum convertAction(Short jdbcActionValue) {
        CascadeActionEnum action = null;

        if (jdbcActionValue != null) {
            switch (jdbcActionValue) {
                case DatabaseMetaData.importedKeyCascade:
                    action = CascadeActionEnum.CASCADE;
                    break;
                case DatabaseMetaData.importedKeySetNull:
                    action = CascadeActionEnum.SET_NULL;
                    break;
                case DatabaseMetaData.importedKeySetDefault:
                    action = CascadeActionEnum.SET_DEFAULT;
                    break;
                case DatabaseMetaData.importedKeyRestrict:
                    action = CascadeActionEnum.RESTRICT;
                    break;
            }
        }
        return action;
    }

    /**
     * Determines the indices for the indicated table.
     *
     * @param metaData  The database meta data
     * @param tableName The name of the table
     * @return The list of indices
     */
    protected Collection readIndices(DatabaseMetaDataWrapper metaData, String tableName) throws SQLException {
        Map indices = new ListOrderedMap();
        ResultSet indexData = null;

        try {
            indexData = metaData.getIndices(metaData.escapeForSearch(tableName), false, false);

            while (indexData.next()) {
                Map values = readColumns(indexData, getColumnsForIndex());

                readIndex(metaData, values, indices);
            }
        } finally {
            closeResultSet(indexData);
        }
        return indices.values();
    }

    /**
     * Reads the next index spec from the result set.
     *
     * @param metaData     The database meta data
     * @param values       The index meta data as defined by {@link #getColumnsForIndex()}
     * @param knownIndices The already read indices for the current table
     */
    protected void readIndex(DatabaseMetaDataWrapper metaData, Map values, Map knownIndices) throws SQLException {
        String indexType = (String) values.get("TYPE");

        // we're ignoring statistic indices
//        if ((indexType != null) && (indexType.shortValue() == DatabaseMetaData.tableIndexStatistic))
//        {
//        	return;
//        }

        String indexName = (String) values.get("INDEX_NAME");

        if (indexName != null) {
            Index index = (Index) knownIndices.get(indexName);

            if (index == null) {
                if ((Boolean) values.get("NON_UNIQUE")) {
                    index = new NonUniqueIndex();
                } else {
                    index = new UniqueIndex();
                }

                index.setName(indexName);
                knownIndices.put(indexName, index);
            }

            IndexColumn indexColumn = new IndexColumn();

            indexColumn.setName((String) values.get("COLUMN_NAME"));
            if (values.containsKey("ORDINAL_POSITION")) {
                indexColumn.setOrdinalPosition(((Short) values.get("ORDINAL_POSITION")).intValue());
            }
            index.addColumn(indexColumn);
        }
    }

    /**
     * Reads the indicated columns from the result set.
     *
     * @param resultSet         The result set
     * @param columnDescriptors The dscriptors of the columns to read
     * @return The read values keyed by the column name
     */
    protected Map<String, Object> readColumns(ResultSet resultSet, List<MetaDataColumnDescriptor> columnDescriptors) throws SQLException {
        Map<String, Object> values = new HashMap<>();

        for (MetaDataColumnDescriptor descriptor : columnDescriptors) {
            values.put(descriptor.getName(), descriptor.readColumn(resultSet));
        }
        return values;
    }

    /**
     * Helper method that determines the auto increment status for the given columns via the
     * {@link ResultSetMetaData#isAutoIncrement(int)} method.
     *
     * @param table          The table
     * @param columnsToCheck The columns to check (e.g. the primary key columns)
     */
    protected void determineAutoIncrementFromResultSetMetaData(Table table, Column[] columnsToCheck) throws SQLException {
        if ((columnsToCheck == null) || (columnsToCheck.length == 0)) {
            return;
        }

        StringBuilder query = new StringBuilder();

        query.append("SELECT ");
        for (int idx = 0; idx < columnsToCheck.length; idx++) {
            if (idx > 0) {
                query.append(",");
            }
            if (getPlatform().isDelimitedIdentifierModeOn()) {
                query.append(getPlatformInfo().getDelimiterToken());
            }
            query.append(columnsToCheck[idx].getName());
            if (getPlatform().isDelimitedIdentifierModeOn()) {
                query.append(getPlatformInfo().getDelimiterToken());
            }
        }
        query.append(" FROM ");
        if (getPlatform().isDelimitedIdentifierModeOn()) {
            query.append(getPlatformInfo().getDelimiterToken());
        }
        query.append(table.getName());
        if (getPlatform().isDelimitedIdentifierModeOn()) {
            query.append(getPlatformInfo().getDelimiterToken());
        }
        query.append(" WHERE 1 = 0");

        Statement stmt = null;

        try {
            stmt = getConnection().createStatement();

            ResultSet rs = stmt.executeQuery(query.toString());
            ResultSetMetaData rsMetaData = rs.getMetaData();

            for (int idx = 0; idx < columnsToCheck.length; idx++) {
                if (rsMetaData.isAutoIncrement(idx + 1)) {
                    columnsToCheck[idx].setAutoIncrement(true);
                }
            }
        } finally {
            closeStatement(stmt);
        }
    }

    /**
     * Sorts the foreign keys in the tables of the model.
     *
     * @param model The model
     */
    protected void sortForeignKeys(Database model) {
        for (Schema schema : model.getSchemas()) {
            for (Table table : schema.getTables()) {
                table.sortForeignKeys(getPlatform().isDelimitedIdentifierModeOn());
            }
        }
    }

    /**
     * Replaces a specific character sequence in the given text with the character sequence
     * whose escaped version it is.
     *
     * @param text      The text
     * @param unescaped The unescaped string, e.g. "'"
     * @param escaped   The escaped version, e.g. "''"
     * @return The resulting text
     */
    protected String unescape(String text, String unescaped, String escaped) {
        String result = text;

        // we need special handling if the single quote is escaped via a double single quote
        if (result != null) {
            if (escaped.equals("''")) {
                if ((result.length() > 2) && result.startsWith("'") && result.endsWith("'")) {
                    result = "'" + StringUtils.replace(result.substring(1, result.length() - 1), escaped, unescaped) + "'";
                } else {
                    result = StringUtils.replace(result, escaped, unescaped);
                }
            } else {
                result = StringUtils.replace(result, escaped, unescaped);
            }
        }
        return result;
    }

    /**
     * Tries to find the schema to which the given table belongs.
     *
     * @param connection    The database connection
     * @param schemaPattern The schema pattern to limit the schemas to search in
     * @param table         The table to search for
     * @return The schema name or <code>null</code> if the schema of the table
     * could not be found
     * @deprecated Will be removed once full schema support is in place
     */
    public String determineSchemaOf(Connection connection, String schemaPattern, Table table) throws SQLException {
        ResultSet tableData = null;
        ResultSet columnData = null;

        try {
            DatabaseMetaDataWrapper metaData = new DatabaseMetaDataWrapper();

            metaData.setMetaData(connection.getMetaData());
            metaData.setCatalog(getDefaultCatalogPattern());
            metaData.setSchemaPattern(schemaPattern == null ? getDefaultSchemaPattern() : schemaPattern);
            metaData.setTableTypes(getDefaultTableTypes());

            String tablePattern = table.getName();

            if (getPlatform().isDelimitedIdentifierModeOn()) {
                tablePattern = tablePattern.toUpperCase();
            }

            tableData = metaData.getTables(metaData.escapeForSearch(tablePattern));

            boolean found = false;
            String schema = null;

            while (!found && tableData.next()) {
                Map values = readColumns(tableData, getColumnsForTable());
                String tableName = (String) values.get("TABLE_NAME");

                if ((tableName != null) && (tableName.length() > 0)) {
                    schema = (String) values.get("TABLE_SCHEM");
                    columnData = metaData.getColumns(metaData.escapeForSearch(tableName), getDefaultColumnPattern());
                    found = true;

                    while (found && columnData.next()) {
                        values = readColumns(columnData, getColumnsForColumn());

                        if (table.findColumn((String) values.get("COLUMN_NAME"),
                                             getPlatform().isDelimitedIdentifierModeOn()) == null) {
                            found = false;
                        }
                    }
                    columnData.close();
                    columnData = null;
                }
            }
            return found ? schema : null;
        } finally {
            closeResultSet(columnData);
            closeResultSet(tableData);
        }
    }

    /**
     * Closes the given result set. Any exceptions are logged on warn level but otherwise swallowed.
     *
     * @param resultSet The result set
     */
    protected void closeResultSet(ResultSet resultSet) {
        if (resultSet != null) {
            try {
                resultSet.close();
            } catch (SQLException ex) {
                _log.warn("Error while closing result set", ex);
            }
        }
    }

    /**
     * Closes the given statement. Any exceptions are logged on warn level but otherwise swallowed.
     *
     * @param stmt The statement
     */
    protected void closeStatement(Statement stmt) {
        if (stmt != null) {
            try {
                stmt.close();
            } catch (SQLException ex) {
                _log.warn("Error while closing statement", ex);
            }
        }
    }
}
