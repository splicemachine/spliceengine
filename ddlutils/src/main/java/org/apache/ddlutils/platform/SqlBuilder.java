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

import java.io.IOException;
import java.io.Writer;
import java.rmi.server.UID;
import java.sql.Types;
import java.text.DateFormat;
import java.text.NumberFormat;
import java.util.Iterator;
import java.util.Locale;
import java.util.Map;

import com.sun.istack.internal.Nullable;
import org.apache.commons.collections.map.ListOrderedMap;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.ddlutils.DdlUtilsException;
import org.apache.ddlutils.Platform;
import org.apache.ddlutils.PlatformInfo;
import org.apache.ddlutils.model.CascadeActionEnum;
import org.apache.ddlutils.model.CheckConstraint;
import org.apache.ddlutils.model.Column;
import org.apache.ddlutils.model.Database;
import org.apache.ddlutils.model.ForeignKey;
import org.apache.ddlutils.model.Index;
import org.apache.ddlutils.model.IndexColumn;
import org.apache.ddlutils.model.ModelException;
import org.apache.ddlutils.model.Role;
import org.apache.ddlutils.model.Schema;
import org.apache.ddlutils.model.Table;
import org.apache.ddlutils.model.TableType;
import org.apache.ddlutils.model.TypeMap;
import org.apache.ddlutils.model.User;

/**
 * This class is a collection of Strategy methods for creating the DDL required to create and drop
 * databases and tables.
 * <p/>
 * It is hoped that just a single implementation of this class, for each database should make creating DDL
 * for each physical database fairly straightforward.
 * <p/>
 * An implementation of this class can always delegate down to some templating technology such as Velocity if
 * it requires. Though often that can be quite complex when attempting to reuse code across many databases.
 * Hopefully only a small amount code needs to be changed on a per database basis.
 *
 * @version $Revision: 893941 $
 */
public abstract class SqlBuilder {
    /**
     * The placeholder for the size value in the native type spec.
     */
    protected static final String SIZE_PLACEHOLDER = "{0}";
    /**
     * The line separator for in between sql commands.
     */
    private static final String LINE_SEPARATOR = System.getProperty("line.separator", "\n");
    /**
     * The Log to which logging calls will be made.
     */
    protected final Log _log = LogFactory.getLog(SqlBuilder.class);

    /**
     * The platform that this builder belongs to.
     */
    private Platform _platform;
    /**
     * The current Writer used to output the SQL to.
     */
    private Writer _writer;
    /**
     * The indentation used to indent commands.
     */
    private String _indent = "    ";
    /**
     * An optional locale specification for number and date formatting.
     */
    private String _valueLocale;
    /**
     * The date formatter.
     */
    private DateFormat _valueDateFormat;
    /**
     * The date time formatter.
     */
    private DateFormat _valueTimeFormat;
    /**
     * The number formatter.
     */
    private NumberFormat _valueNumberFormat;
    /**
     * Helper object for dealing with default values.
     */
    private DefaultValueHelper _defaultValueHelper = new DefaultValueHelper();
    /**
     * The character sequences that need escaping.
     */
    private Map _charSequencesToEscape = new ListOrderedMap();
    /**
     * Append statement terminator to statements. Default is <code>true</code>
     */
    private boolean terminateStatements = true;

    //
    // Configuration
    //                

    /**
     * Creates a new sql builder.
     *
     * @param platform The plaftform this builder belongs to
     */
    public SqlBuilder(Platform platform) {
        _platform = platform;
    }

    /**
     * Returns the platform object.
     *
     * @return The platform
     */
    public Platform getPlatform() {
        return _platform;
    }

    /**
     * Returns the platform info object.
     *
     * @return The info object
     */
    public PlatformInfo getPlatformInfo() {
        return _platform.getPlatformInfo();
    }

    /**
     * Returns the writer that the DDL is printed to.
     *
     * @return The writer
     */
    public Writer getWriter() {
        return _writer;
    }

    /**
     * Sets the writer for printing the DDL to.
     *
     * @param writer The writer
     */
    public void setWriter(Writer writer) {
        _writer = writer;
    }

    /**
     * Wheter to use the terminator char to terminate statements. Default is <code>true</code>
     * @param terminateStatements use statement terminator char
     */
    public void setTerminateStatements(boolean terminateStatements) {
        this.terminateStatements = terminateStatements;
    }

    /**
     * Returns the default value helper.
     *
     * @return The default value helper
     */
    public DefaultValueHelper getDefaultValueHelper() {
        return _defaultValueHelper;
    }

    /**
     * Returns the string used to indent the SQL.
     *
     * @return The indentation string
     */
    public String getIndent() {
        return _indent;
    }

    /**
     * Sets the string used to indent the SQL.
     *
     * @param indent The indentation string
     */
    public void setIndent(String indent) {
        _indent = indent;
    }

    /**
     * Returns the locale that is used for number and date formatting
     * (when printing default values and in generates insert/update/delete
     * statements).
     *
     * @return The locale or <code>null</code> if default formatting is used
     */
    public String getValueLocale() {
        return _valueLocale;
    }

    /**
     * Sets the locale that is used for number and date formatting
     * (when printing default values and in generates insert/update/delete
     * statements).
     *
     * @param localeStr The new locale or <code>null</code> if default formatting
     *                  should be used; Format is "language[_country[_variant]]"
     */
    public void setValueLocale(String localeStr) {
        if (localeStr != null) {
            int sepPos = localeStr.indexOf('_');
            String language = null;
            String country = null;
            String variant = null;

            if (sepPos > 0) {
                language = localeStr.substring(0, sepPos);
                country = localeStr.substring(sepPos + 1);
                sepPos = country.indexOf('_');
                if (sepPos > 0) {
                    variant = country.substring(sepPos + 1);
                    country = country.substring(0, sepPos);
                }
            } else {
                language = localeStr;
            }
            if (language != null) {
                Locale locale = null;

                if (variant != null) {
                    locale = new Locale(language, country, variant);
                } else if (country != null) {
                    locale = new Locale(language, country);
                } else {
                    locale = new Locale(language);
                }

                _valueLocale = localeStr;
                setValueDateFormat(DateFormat.getDateInstance(DateFormat.SHORT, locale));
                setValueTimeFormat(DateFormat.getTimeInstance(DateFormat.SHORT, locale));
                setValueNumberFormat(NumberFormat.getNumberInstance(locale));
                return;
            }
        }
        _valueLocale = null;
        setValueDateFormat(null);
        setValueTimeFormat(null);
        setValueNumberFormat(null);
    }

    /**
     * Returns the format object for formatting dates in the specified locale.
     *
     * @return The date format object or null if no locale is set
     */
    protected DateFormat getValueDateFormat() {
        return _valueDateFormat;
    }

    /**
     * Sets the format object for formatting dates in the specified locale.
     *
     * @param format The date format object
     */
    protected void setValueDateFormat(DateFormat format) {
        _valueDateFormat = format;
    }

    /**
     * Returns the format object for formatting times in the specified locale.
     *
     * @return The time format object or null if no locale is set
     */
    protected DateFormat getValueTimeFormat() {
        return _valueTimeFormat;
    }

    /**
     * Sets the date format object for formatting times in the specified locale.
     *
     * @param format The time format object
     */
    protected void setValueTimeFormat(DateFormat format) {
        _valueTimeFormat = format;
    }

    /**
     * Returns the format object for formatting numbers in the specified locale.
     *
     * @return The number format object or null if no locale is set
     */
    protected NumberFormat getValueNumberFormat() {
        return _valueNumberFormat;
    }

    /**
     * Returns a new date format object for formatting numbers in the specified locale.
     * Platforms can override this if necessary.
     *
     * @param format The number format object
     */
    protected void setValueNumberFormat(NumberFormat format) {
        _valueNumberFormat = format;
    }

    /**
     * Adds a char sequence that needs escaping, and its escaped version.
     *
     * @param charSequence   The char sequence
     * @param escapedVersion The escaped version
     */
    protected void addEscapedCharSequence(String charSequence, String escapedVersion) {
        _charSequencesToEscape.put(charSequence, escapedVersion);
    }

    /**
     * Returns the maximum number of characters that a table name can have.
     * This method is intended to give platform specific builder implementations
     * more control over the maximum length.
     *
     * @return The number of characters, or -1 if not limited
     */
    public int getMaxTableNameLength() {
        return getPlatformInfo().getMaxTableNameLength();
    }

    /**
     * Returns the maximum number of characters that a column name can have.
     * This method is intended to give platform specific builder implementations
     * more control over the maximum length.
     *
     * @return The number of characters, or -1 if not limited
     */
    public int getMaxColumnNameLength() {
        return getPlatformInfo().getMaxColumnNameLength();
    }

    /**
     * Returns the maximum number of characters that a constraint name can have.
     * This method is intended to give platform specific builder implementations
     * more control over the maximum length.
     *
     * @return The number of characters, or -1 if not limited
     */
    public int getMaxConstraintNameLength() {
        return getPlatformInfo().getMaxConstraintNameLength();
    }

    /**
     * Returns the maximum number of characters that a foreign key name can have.
     * This method is intended to give platform specific builder implementations
     * more control over the maximum length.
     *
     * @return The number of characters, or -1 if not limited
     */
    public int getMaxForeignKeyNameLength() {
        return getPlatformInfo().getMaxForeignKeyNameLength();
    }

    //
    // public interface
    //

    /**
     * Outputs the DDL required to drop and (re)create all tables in the database model.
     *
     * @param database The database model
     */
    public void createTables(Database database) throws IOException {
        createTables(database, null, true);
    }

    /**
     * Outputs the DDL required to drop (if requested) and (re)create all tables in the database model.
     *
     * @param database   The database
     * @param dropTables Whether to drop tables before creating them
     */
    public void createTables(Database database, boolean dropTables) throws IOException {
        createTables(database, null, dropTables);
    }

    /**
     * Outputs the DDL required to drop (if requested) and (re)create all tables in the database model.
     *
     * @param database   The database
     * @param params     The parameters used in the creation, may be null
     * @param dropTables Whether to drop tables before creating them
     */
    public void createTables(Database database, @Nullable CreationParameters params, boolean dropTables) throws IOException {
        if (dropTables) {
            dropAuth(database);
            dropTables(database);
            dropSchemas(database);
        }

        for (Schema schema : database.getSchemas()) {
            writeSchemaComment(schema);
            createSchema(database, schema);
            setSchema(schema);
            for (Table table : schema.getTablesNotOfType(t -> t == TableType.VIEW)) {
                writeTableComment(table);
                createTable(database, table, params == null ? null : params.getParametersFor(table));
            }
            for (Table table : schema.getTablesOfType(t -> t == TableType.VIEW)) {
                writeTableComment(table);
                createView(database, table, params == null ? null : params.getParametersFor(table));
            }
        }

        // we're writing the external foreignkeys last to ensure that all referenced tables are already defined
        createForeignKeys(database);
        // create users, roles, authorization permissions
        createAuth(database);
    }

    protected void dropAuth(Database database) throws IOException {
        dropPermissions(database);
        dropRoles(database);
        dropUsers(database);
    }

    protected void dropUsers(Database database) throws IOException {
        // Optional method. Default implementation does nothing.
    }

    protected void dropPermissions(Database database) throws IOException {
        // Optional method. Default implementation does nothing.
    }

    protected void dropRoles(Database database) throws IOException {
        // Optional method. Default implementation does nothing.
    }

    protected void createAuth(Database database) throws IOException {
        createUsers(database);
        createRoles(database);
        createPermissions(database);
    }

    protected void createRoles(Database database) throws IOException {
        // Optional method. Default implementation does nothing.
    }

    protected void createUsers(Database database) throws IOException {
        // Optional method. Default implementation does nothing.
    }

    protected void createPermissions(Database database) throws IOException {
        // Optional method. Default implementation does nothing.
    }

    /**
     * Outputs the DDL to create the given temporary table. Per default this is simply
     * a call to {@link #createTable(Database, Table, Map)}.
     *
     * @param database   The database model
     * @param table      The table
     * @param parameters Additional platform-specific parameters for the table creation
     */
    protected void createTemporaryTable(Database database, Table table, Map parameters) throws IOException {
        createTable(database, table, parameters);
    }

    /**
     * Outputs the DDL to drop the given temporary table. Per default this is simply
     * a call to {@link #dropTable(Table)}.
     *
     * @param database The database model
     * @param table    The table
     */
    protected void dropTemporaryTable(Database database, Table table) throws IOException {
        dropTable(table);
    }

    /**
     * Writes a statement that copies the data from the source to the target table. Note
     * that this copies only those columns that are in both tables.
     * Database-specific implementations might redefine this method though it usually
     * suffices to redefine the {@link #writeCastExpression(Column, Column)} method.
     *
     * @param sourceTable The source table
     * @param targetTable The target table
     */
    protected void copyData(Table sourceTable, Table targetTable) throws IOException {
        ListOrderedMap columns = new ListOrderedMap();

        for (int idx = 0; idx < sourceTable.getColumnCount(); idx++) {
            Column sourceColumn = sourceTable.getColumn(idx);
            Column targetColumn = targetTable.findColumn(sourceColumn.getName(),
                                                         getPlatform().isDelimitedIdentifierModeOn());


            if (targetColumn != null) {
                columns.put(sourceColumn, targetColumn);
            }
        }

        print("INSERT INTO ");
        printIdentifier(getTableName(targetTable));
        print(" (");
        for (Iterator columnIt = columns.keySet().iterator(); columnIt.hasNext(); ) {
            printIdentifier(getColumnName((Column) columnIt.next()));
            if (columnIt.hasNext()) {
                print(",");
            }
        }
        print(") SELECT ");
        for (Iterator columnsIt = columns.entrySet().iterator(); columnsIt.hasNext(); ) {
            Map.Entry entry = (Map.Entry) columnsIt.next();

            writeCastExpression((Column) entry.getKey(),
                                (Column) entry.getValue());
            if (columnsIt.hasNext()) {
                print(",");
            }
        }
        print(" FROM ");
        printIdentifier(getTableName(sourceTable));
        printEndOfStatement();
    }

    /**
     * Writes a cast expression that converts the value of the source column to the data type
     * of the target column. Per default, simply the name of the source column is written
     * thereby assuming that any casts happen implicitly.
     *
     * @param sourceColumn The source column
     * @param targetColumn The target column
     */
    protected void writeCastExpression(Column sourceColumn, Column targetColumn) throws IOException {
        printIdentifier(getColumnName(sourceColumn));
    }

    /**
     * Compares the two strings.
     *
     * @param string1     The first string
     * @param string2     The second string
     * @param caseMatters Whether case matters in the comparison
     * @return <code>true</code> if the string are equal
     */
    protected boolean areEqual(String string1, String string2, boolean caseMatters) {
        return (caseMatters && string1.equals(string2)) ||
            (!caseMatters && string1.equalsIgnoreCase(string2));
    }

    /**
     * Outputs the DDL to create the table along with any non-external constraints as well
     * as with external primary keys and indices (but not foreign keys).
     *
     * @param database The database model
     * @param table    The table
     */
    public void createTable(Database database, Table table) throws IOException {
        createTable(database, table, null);
    }

    /**
     * Outputs the DDL to create the view.
     *
     * @param database   The database model
     * @param view      The view
     * @param parameters Additional platform-specific parameters for the table creation
     */
    public void createView(Database database, Table view, Map parameters) throws IOException {
        print(view.getViewDefinition());
        printEndOfStatement();
    }

    /**
     * Outputs the DDL to create the table along with any non-external constraints as well
     * as with external primary keys and indices (but not foreign keys).
     *
     * @param database   The database model
     * @param table      The table
     * @param parameters Additional platform-specific parameters for the table creation
     */
    public void createTable(Database database, Table table, Map parameters) throws IOException {
        writeTableCreationStmt(database, table, parameters);
        writeTableCreationStmtEnding(table, parameters);

        if (!getPlatformInfo().isPrimaryKeyEmbedded()) {
            createPrimaryKey(table, table.getPrimaryKeyColumns());
        }
        if (!getPlatformInfo().isIndicesEmbedded()) {
            createIndexes(table);
        }
    }

    /**
     * Outputs the DDL to create the table along with any non-external constraints as well
     * as with external primary keys and indices (but not foreign keys).
     *
     * @param database The database (catalog)
     * @param schema   The database schema
     */
    public void createSchema(Database database, Schema schema) throws IOException {
        // TODO: JC - create catalog?
        print("CREATE SCHEMA ");
        printIdentifier(schema.getSchemaName());
        if (schema.getAuthorizationId() != null && !schema.getAuthorizationId().isEmpty()) {
            print(" AUTHORIZATION ");
            printIdentifier(schema.getAuthorizationId());
        }
        printEndOfStatement();
    }

    /**
     * Enter the schema
     * @param schema the schema to set
     */
    public void setSchema(Schema schema) throws IOException {
        print("SET SCHEMA ");
        printIdentifier(schema.getSchemaName());
        printEndOfStatement();
    }

    /**
     * Writes the primary key constraints of the table as alter table statements.
     *
     * @param table             The table
     * @param primaryKeyColumns The primary key columns
     */
    public void createPrimaryKey(Table table, Column[] primaryKeyColumns) throws IOException {
        if ((primaryKeyColumns.length > 0) && shouldGeneratePrimaryKeys(primaryKeyColumns)) {
            print("ALTER TABLE ");
            printlnIdentifier(getTableName(table));
            printIndent();
            print("ADD CONSTRAINT ");
            printIdentifier(getConstraintName(null, table, "PK", null));
            print(" ");
            writePrimaryKeyStmt(table, primaryKeyColumns);
            printEndOfStatement();
        }
    }

    /**
     * Writes the indexes for the given table using external index creation statements.
     *
     * @param table The table
     */
    public void createIndexes(Table table) throws IOException {
        for (int idx = 0; idx < table.getIndexCount(); idx++) {
            Index index = table.getIndex(idx);

            if (!index.isUnique() && !getPlatformInfo().isIndicesSupported()) {
                throw new ModelException("Platform does not support non-unique indices");
            }
            createIndex(table, index);
        }
    }

    /**
     * Writes the given index for the table using an external index creation statement.
     *
     * @param table The table
     * @param index The index
     */
    public void createIndex(Table table, Index index) throws IOException {
        if (!getPlatformInfo().isIndicesSupported()) {
            throw new DdlUtilsException("This platform does not support indexes");
        } else if (index.getName() == null) {
            _log.warn("Cannot write unnamed index " + index);
        } else {
            print("CREATE");
            if (index.isUnique()) {
                print(" UNIQUE");
            }
            print(" INDEX ");
            printIdentifier(getIndexName(index));
            print(" ON ");
            printIdentifier(getTableName(table));
            print(" (");

            for (int idx = 0; idx < index.getColumnCount(); idx++) {
                IndexColumn idxColumn = index.getColumn(idx);
                Column col = table.findColumn(idxColumn.getName());

                if (col == null) {
                    // would get null pointer on next line anyway, so throw exception
                    throw new ModelException("Invalid column '" + idxColumn.getName() + "' on index " + index.getName() + " for" +
                                                 " table " + table.getQualifiedName());
                }
                if (idx > 0) {
                    print(", ");
                }
                printIdentifier(getColumnName(col));
            }

            print(")");
            printEndOfStatement();
        }
    }

    /**
     * Creates the external foreignkey creation statements for all tables in the database.
     *
     * @param database The database
     */
    public void createForeignKeys(Database database) throws IOException {
        for (Schema schema : database.getSchemas()) {
            for (Table table : schema.getTables()) {
                createForeignKeys(database, table);
            }
        }
    }

    /**
     * Creates external foreignkey creation statements if necessary.
     *
     * @param database The database model
     * @param table    The table
     */
    public void createForeignKeys(Database database, Table table) throws IOException {
        for (int idx = 0; idx < table.getForeignKeyCount(); idx++) {
            createForeignKey(database, table, table.getForeignKey(idx));
        }
    }

    /**
     * Writes a single foreign key constraint using a alter table statement.
     *
     * @param database   The database model
     * @param table      The table
     * @param foreignKey The foreign key
     */
    public void createForeignKey(Database database, Table table, ForeignKey foreignKey) throws IOException {
        if (getPlatformInfo().isForeignKeysEmbedded()) {
            throw new DdlUtilsException("This platform does not supported the external creation of foreign keys");
        } else if (foreignKey.getForeignTableName() == null) {
            _log.warn("Foreign key table is null for key " + foreignKey);
        } else {
            writeTableAlterStmt(table);

            print("ADD CONSTRAINT ");
            printIdentifier(getForeignKeyName(table, foreignKey));
            print(" FOREIGN KEY (");
            writeLocalReferences(foreignKey);
            print(") REFERENCES ");
            printIdentifier(getTableName(database.findTable(table.getSchema(), foreignKey.getForeignTableName(), false)));
            print(" (");
            writeForeignReferences(foreignKey);
            print(")");
            writeForeignKeyOnDeleteAction(table, foreignKey);
            writeForeignKeyOnUpdateAction(table, foreignKey);
            printEndOfStatement();
        }
    }

    /**
     * Prints the SQL for adding a column to a table.
     *
     * @param model     The database model
     * @param table     The table
     * @param newColumn The new column
     */
    public void addColumn(Database model, Table table, Column newColumn) throws IOException {
        print("ALTER TABLE ");
        printlnIdentifier(getTableName(table));
        printIndent();
        print("ADD COLUMN ");
        writeColumn(table, newColumn);
        printEndOfStatement();
    }

    /**
     * Outputs the DDL required to drop the database.
     *
     * @param database The database
     */
    public void dropTables(Database database) throws IOException {
        // We're making the assumption here that foreign keys to not exist across schema.
        // While there's nothing preventing that, NOT following this assumption will make it
        // impossible to determine dependency order.
        for (Schema schema : database.getSchemas()) {
            // we're dropping the external foreignkeys first
            for (Table table : schema.getTablesInReverseOrder()) {

                if ((table.getQualifiedName() != null) &&
                    (table.getQualifiedName().length() > 0)) {
                    dropForeignKeys(table);
                }
            }

            // Next we drop the tables in reverse order to avoid referencial problems
            // TODO: It might be more useful to either (or both)
            //       * determine an order in which the tables can be dropped safely (via the foreignkeys)
            //       * alter the tables first to drop the internal foreignkeys
            for (Table table : schema.getTablesInReverseOrder()) {

                if ((table.getQualifiedName() != null) &&
                    (table.getQualifiedName().length() > 0)) {
                    writeTableComment(table);
                    dropTable(table);
                }
            }
        }
    }

    public void dropSchemas(Database model) throws IOException {
        for (Schema schema : model.getSchemas()) {
            writeSchemaComment(schema);
            dropSchema(schema);
        }
    }

    protected void dropSchema(Schema schema) throws IOException {
        print("DROP SCHEMA ");
        printIdentifier(schema.getSchemaName());
        printEndOfStatement();
    }

    /**
     * Outputs the DDL required to drop the given table. This method also
     * drops foreign keys to the table.
     *
     * @param database The database
     * @param table    The table
     */
    public void dropTable(Database database, Table table) throws IOException {
        for (Schema schema : database.getSchemas()) {
            // we're dropping the foreignkeys to the table first
            for (Table otherTable : schema.getTablesInReverseOrder()) {
                ForeignKey[] fks = otherTable.getForeignKeys();

                for (int fkIdx = 0; (fks != null) && (fkIdx < fks.length); fkIdx++) {
                    if (fks[fkIdx].getForeignTable().equals(table)) {
                        dropForeignKey(otherTable, fks[fkIdx]);
                    }
                }
            }
        }
        // and the foreign keys from the table
        dropForeignKeys(table);

        writeTableComment(table);
        dropTable(table);
    }

    /**
     * Outputs the DDL to drop the table. Note that this method does not drop
     * foreign keys to this table. Use {@link #dropTable(Database, Table)}
     * if you want that.
     *
     * @param table The table to drop
     */
    public void dropTable(Table table) throws IOException {
        print("DROP "+table.getType()+" "+(table.getType().equals(TableType.TABLE) ? ifExists() : ""));
        printIdentifier(getTableName(table));
        printEndOfStatement();
    }

    protected String ifExists() {
        return "";
    }

    /**
     * Creates external foreignkey drop statements.
     *
     * @param table The table
     */
    public void dropForeignKeys(Table table) throws IOException {
        for (int idx = 0; idx < table.getForeignKeyCount(); idx++) {
            dropForeignKey(table, table.getForeignKey(idx));
        }
    }

    /**
     * Generates the statement to drop a foreignkey constraint from the database using an
     * alter table statement.
     *
     * @param table      The table
     * @param foreignKey The foreign key
     */
    public void dropForeignKey(Table table, ForeignKey foreignKey) throws IOException {
        writeTableAlterStmt(table);
        print("DROP CONSTRAINT ");
        printIdentifier(getForeignKeyName(table, foreignKey));
        printEndOfStatement();
    }

    /**
     * Creates the SQL for inserting an object into the specified table.
     * If values are given then a concrete insert statement is created, otherwise an
     * insert statement usable in a prepared statement is build.
     *
     * @param table           The table
     * @param columnValues    The columns values indexed by the column names
     * @param genPlaceholders Whether to generate value placeholders for a
     *                        prepared statement
     * @return The insertion sql
     */
    public String getInsertSql(Table table, Map columnValues, boolean genPlaceholders) {
        StringBuilder buffer = new StringBuilder("INSERT INTO ");
        boolean addComma = false;

        buffer.append(getDelimitedIdentifier(getTableName(table)));
        buffer.append(" (");

        for (int idx = 0; idx < table.getColumnCount(); idx++) {
            Column column = table.getColumn(idx);

            if (columnValues.containsKey(column.getName())) {
                if (addComma) {
                    buffer.append(", ");
                }
                buffer.append(getDelimitedIdentifier(column.getName()));
                addComma = true;
            }
        }
        buffer.append(") VALUES (");
        if (genPlaceholders) {
            addComma = false;
            for (int idx = 0; idx < columnValues.size(); idx++) {
                if (addComma) {
                    buffer.append(", ");
                }
                buffer.append("?");
                addComma = true;
            }
        } else {
            addComma = false;
            for (int idx = 0; idx < table.getColumnCount(); idx++) {
                Column column = table.getColumn(idx);

                if (columnValues.containsKey(column.getName())) {
                    if (addComma) {
                        buffer.append(", ");
                    }
                    buffer.append(getValueAsString(column, columnValues.get(column.getName())));
                    addComma = true;
                }
            }
        }
        buffer.append(")");
        return buffer.toString();
    }

    /**
     * Creates the SQL for updating an object in the specified table.
     * If values are given then a concrete update statement is created, otherwise an
     * update statement usable in a prepared statement is build.
     *
     * @param table           The table
     * @param columnValues    Contains the values for the columns to update, and should also
     *                        contain the primary key values to identify the object to update
     *                        in case <code>genPlaceholders</code> is <code>false</code>
     * @param genPlaceholders Whether to generate value placeholders for a
     *                        prepared statement (both for the pk values and the object values)
     * @return The update sql
     */
    public String getUpdateSql(Table table, Map columnValues, boolean genPlaceholders) {
        StringBuilder buffer = new StringBuilder("UPDATE ");
        boolean addSep = false;

        buffer.append(getDelimitedIdentifier(getTableName(table)));
        buffer.append(" SET ");

        for (int idx = 0; idx < table.getColumnCount(); idx++) {
            Column column = table.getColumn(idx);

            if (!column.isPrimaryKey() && columnValues.containsKey(column.getName())) {
                if (addSep) {
                    buffer.append(", ");
                }
                buffer.append(getDelimitedIdentifier(column.getName()));
                buffer.append(" = ");
                if (genPlaceholders) {
                    buffer.append("?");
                } else {
                    buffer.append(getValueAsString(column, columnValues.get(column.getName())));
                }
                addSep = true;
            }
        }
        buffer.append(" WHERE ");
        addSep = false;
        for (int idx = 0; idx < table.getColumnCount(); idx++) {
            Column column = table.getColumn(idx);

            if (column.isPrimaryKey() && columnValues.containsKey(column.getName())) {
                if (addSep) {
                    buffer.append(" AND ");
                }
                buffer.append(getDelimitedIdentifier(column.getName()));
                buffer.append(" = ");
                if (genPlaceholders) {
                    buffer.append("?");
                } else {
                    buffer.append(getValueAsString(column, columnValues.get(column.getName())));
                }
                addSep = true;
            }
        }
        return buffer.toString();
    }

    /**
     * Creates the SQL for updating an object in the specified table.
     * If values are given then a concrete update statement is created, otherwise an
     * update statement usable in a prepared statement is build.
     *
     * @param table           The table
     * @param oldColumnValues Contains the column values to identify the row to update
     * @param newColumnValues Contains the values for the columns to update
     * @param genPlaceholders Whether to generate value placeholders for a
     *                        prepared statement (both for the pk values and the object values)
     * @return The update sql
     */
    public String getUpdateSql(Table table, Map oldColumnValues, Map newColumnValues, boolean genPlaceholders) {
        StringBuilder buffer = new StringBuilder("UPDATE ");
        boolean addSep = false;

        buffer.append(getDelimitedIdentifier(getTableName(table)));
        buffer.append(" SET ");

        for (int idx = 0; idx < table.getColumnCount(); idx++) {
            Column column = table.getColumn(idx);

            if (newColumnValues.containsKey(column.getName())) {
                if (addSep) {
                    buffer.append(", ");
                }
                buffer.append(getDelimitedIdentifier(column.getName()));
                buffer.append(" = ");
                if (genPlaceholders) {
                    buffer.append("?");
                } else {
                    buffer.append(getValueAsString(column, newColumnValues.get(column.getName())));
                }
                addSep = true;
            }
        }
        buffer.append(" WHERE ");
        addSep = false;
        for (int idx = 0; idx < table.getColumnCount(); idx++) {
            Column column = table.getColumn(idx);

            if (oldColumnValues.containsKey(column.getName())) {
                if (addSep) {
                    buffer.append(" AND ");
                }
                buffer.append(getDelimitedIdentifier(column.getName()));
                buffer.append(" = ");
                if (genPlaceholders) {
                    buffer.append("?");
                } else {
                    buffer.append(getValueAsString(column, oldColumnValues.get(column.getName())));
                }
                addSep = true;
            }
        }
        return buffer.toString();
    }

    /**
     * Creates the SQL for deleting an object from the specified table. Depending on
     * the value of <code>genPlaceholders</code>, the generated SQL will contain
     * prepared statement place holders or concrete values. Only those primary key
     * columns wil be used that are present in the given map. If the map is null or
     * completely empty, then the SQL will not have a WHERE clause. The SQL will contain
     * the columns in the order defined in the table.
     *
     * @param table           The table
     * @param pkValues        The primary key columns to use, and optionally their values
     * @param genPlaceholders Whether to generate value placeholders for a
     *                        prepared statement
     * @return The delete sql
     */
    public String getDeleteSql(Table table, Map pkValues, boolean genPlaceholders) {
        StringBuilder buffer = new StringBuilder("DELETE FROM ");
        boolean addSep = false;

        buffer.append(getDelimitedIdentifier(getTableName(table)));
        if ((pkValues != null) && !pkValues.isEmpty()) {
            buffer.append(" WHERE ");

            Column[] pkCols = table.getPrimaryKeyColumns();

            for (int pkColIdx = 0; pkColIdx < pkCols.length; pkColIdx++) {
                Column column = pkCols[pkColIdx];

                if (pkValues.containsKey(column.getName())) {
                    if (addSep) {
                        buffer.append(" AND ");
                    }
                    buffer.append(getDelimitedIdentifier(column.getName()));
                    buffer.append(" = ");
                    if (genPlaceholders) {
                        buffer.append("?");
                    } else {
                        buffer.append(getValueAsString(column, pkValues.get(column.getName())));
                    }
                    addSep = true;
                }
            }
        }
        return buffer.toString();
    }

    /**
     * Generates the string representation of the given value.
     *
     * @param column The column
     * @param value  The value
     * @return The string representation
     */
    protected String getValueAsString(Column column, Object value) {
        if (value == null) {
            return "NULL";
        }

        StringBuilder result = new StringBuilder();

        // TODO: Handle binary types (BINARY, VARBINARY, LONGVARBINARY, BLOB)
        switch (column.getTypeCode()) {
            case Types.DATE:
                result.append(getPlatformInfo().getValueQuoteToken());
                if (!(value instanceof String) && (getValueDateFormat() != null)) {
                    // TODO: Can the format method handle java.sql.Date properly ?
                    result.append(getValueDateFormat().format(value));
                } else {
                    result.append(value.toString());
                }
                result.append(getPlatformInfo().getValueQuoteToken());
                break;
            case Types.TIME:
                result.append(getPlatformInfo().getValueQuoteToken());
                if (!(value instanceof String) && (getValueTimeFormat() != null)) {
                    // TODO: Can the format method handle java.sql.Date properly ?
                    result.append(getValueTimeFormat().format(value));
                } else {
                    result.append(value.toString());
                }
                result.append(getPlatformInfo().getValueQuoteToken());
                break;
            case Types.TIMESTAMP:
                result.append(getPlatformInfo().getValueQuoteToken());
                result.append(value.toString());
                result.append(getPlatformInfo().getValueQuoteToken());
                break;
            case Types.REAL:
            case Types.NUMERIC:
            case Types.FLOAT:
            case Types.DOUBLE:
            case Types.DECIMAL:
                result.append(getPlatformInfo().getValueQuoteToken());
                if (!(value instanceof String) && (getValueNumberFormat() != null)) {
                    result.append(getValueNumberFormat().format(value));
                } else {
                    result.append(value.toString());
                }
                result.append(getPlatformInfo().getValueQuoteToken());
                break;
            default:
                result.append(getPlatformInfo().getValueQuoteToken());
                result.append(escapeStringValue(value.toString()));
                result.append(getPlatformInfo().getValueQuoteToken());
                break;
        }
        return result.toString();
    }

    /**
     * Generates the SQL for querying the id that was created in the last insertion
     * operation. This is obviously only useful for pk fields that are auto-incrementing.
     * A database that does not support this, will return <code>null</code>.
     *
     * @param table The table
     * @return The sql, or <code>null</code> if the database does not support this
     */
    public String getSelectLastIdentityValues(Table table) {
        // No default possible as the databases are quite different in this respect
        return null;
    }

    //
    // implementation methods that may be overridden by specific database builders
    //

    /**
     * Generates a version of the name that has at most the specified
     * length.
     *
     * @param name          The original name
     * @param desiredLength The desired maximum length
     * @return The shortened version
     */
    public String shortenName(String name, int desiredLength) {
        if (name == null) {
            return null;
        }

        // TODO: Find an algorithm that generates unique names
        int originalLength = name.length();

        if ((desiredLength <= 0) || (originalLength <= desiredLength)) {
            return name;
        }

        int delta = originalLength - desiredLength;
        int startCut = desiredLength / 2;

        StringBuilder result = new StringBuilder();

        result.append(name.substring(0, startCut));
        if (((startCut == 0) || (name.charAt(startCut - 1) != '_')) &&
            ((startCut + delta + 1 == originalLength) || (name.charAt(startCut + delta + 1) != '_'))) {
            // just to make sure that there isn't already a '_' right before or right
            // after the cutting place (which would look odd with an aditional one)
            result.append("_");
        }
        result.append(name.substring(startCut + delta + 1, originalLength));
        return result.toString();
    }

    /**
     * Outputs a comment for the schema.
     *
     * @param schema The schema
     */
    protected void writeSchemaComment(Schema schema) throws IOException {
        printComment("-----------------------------------------------------------------------");
        printComment(" SCHEMA "+schema.getSchemaName());
        printComment("-----------------------------------------------------------------------");
        println();
    }

    /**
     * Outputs a comment for the user.
     *
     * @param user The database user
     */
    protected void writeUserComment(User user) throws IOException {
        printComment("-----------------------------------------------------------------------");
        printComment(" USER "+user.getName());
        printComment("-----------------------------------------------------------------------");
        println();
    }

    /**
     * Outputs a comment for the role.
     *
     * @param role The database role
     */
    protected void writeRoleComment(Role role) throws IOException {
        printComment("-----------------------------------------------------------------------");
        printComment(" ROLE "+role.getName());
        printComment("-----------------------------------------------------------------------");
        println();
    }

    /**
     * Returns the table name. This method takes care of length limitations imposed by some databases.
     *
     * @param table The table
     * @return The table name
     */
    public String getTableName(Table table) {
        return shortenName(table.getQualifiedName(), getMaxTableNameLength());
    }

    /**
     * Outputs a comment for the table.
     *
     * @param table The table
     */
    protected void writeTableComment(Table table) throws IOException {
        printComment("-----------------------------------------------------------------------");
        printComment(table.getType()+" "+getTableName(table));
        printComment("-----------------------------------------------------------------------");
        println();
    }

    /**
     * Generates the first part of the ALTER TABLE statement including the
     * table name.
     *
     * @param table The table being altered
     */
    protected void writeTableAlterStmt(Table table) throws IOException {
        print("ALTER TABLE ");
        printlnIdentifier(getTableName(table));
        printIndent();
    }

    /**
     * Writes the table creation statement without the statement end.
     *
     * @param database   The model
     * @param table      The table
     * @param parameters Additional platform-specific parameters for the table creation
     */
    protected void writeTableCreationStmt(Database database, Table table, Map parameters) throws IOException {
        print("CREATE TABLE ");
        printlnIdentifier(getTableName(table));
        println("(");

        writeColumns(table);
        writeCheckConstraint(table);

        if (getPlatformInfo().isPrimaryKeyEmbedded()) {
            writeEmbeddedPrimaryKeysStmt(table);
        }
        if (getPlatformInfo().isForeignKeysEmbedded()) {
            writeEmbeddedForeignKeysStmt(database, table);
        }
        if (getPlatformInfo().isIndicesEmbedded()) {
            writeEmbeddedIndicesStmt(table);
        }
        println();
        print(")");
    }

    /**
     * Writes the end of the table creation statement. Per default,
     * only the end of the statement is written, but this can be changed
     * in subclasses.
     *
     * @param table      The table
     * @param parameters Additional platform-specific parameters for the table creation
     */
    protected void writeTableCreationStmtEnding(Table table, Map parameters) throws IOException {
        printEndOfStatement();
    }

    /**
     * Writes the check constraint of the given table, if any.
     *
     * @param table The table
     */
    protected void writeCheckConstraint(Table table) throws IOException {
        if (table.hasCheckConstraint()) {
            CheckConstraint checkConstraint = table.getCheckConstraint();
            println(",");
            printIndent();
            writeCheckConstraint(checkConstraint);
            println();
        }
    }

    protected void writeCheckConstraint(CheckConstraint checkConstraint) throws IOException {
        print("CONSTRAINT "+checkConstraint.getConstraintName()+" CHECK "+checkConstraint.getCheckDefinition());
    }

    /**
     * Writes the columns of the given table.
     *
     * @param table The table
     */
    protected void writeColumns(Table table) throws IOException {
        for (int idx = 0; idx < table.getColumnCount(); idx++) {
            printIndent();
            writeColumn(table, table.getColumn(idx));
            if (idx < table.getColumnCount() - 1) {
                println(",");
            }
        }
    }

    /**
     * Returns the column name. This method takes care of length limitations imposed by some databases.
     *
     * @param column The column
     * @return The column name
     */
    protected String getColumnName(Column column) throws IOException {
        return shortenName(column.getName(), getMaxColumnNameLength());
    }

    /**
     * Outputs the DDL for the specified column.
     *
     * @param table  The table containing the column
     * @param column The column
     */
    protected void writeColumn(Table table, Column column) throws IOException {
        //see comments in columnsDiffer about null/"" defaults
        printIdentifier(getColumnName(column));
        print(" ");
        print(getSqlType(column));
        writeColumnDefaultValueStmt(table, column);
        if (column.isRequired()) {
            print(" ");
            writeColumnNotNullableStmt();
        } else if (getPlatformInfo().isNullAsDefaultValueRequired() &&
            getPlatformInfo().hasNullDefault(column.getTypeCode())) {
            print(" ");
            writeColumnNullableStmt();
        }
        if (column.isAutoIncrement() && !getPlatformInfo().isDefaultValueUsedForIdentitySpec()) {
            if (!getPlatformInfo().isNonPrimaryKeyIdentityColumnsSupported() && !column.isPrimaryKey()) {
                throw new ModelException("Column " + column.getName() + " in table " + table.getQualifiedName() + " is " +
                                             "auto-incrementing but not a primary key column, which is not supported by the " +
                                             "platform");
            }
            print(" ");
            writeColumnAutoIncrementStmt(table, column);
        }
        if (column.hasCheckConstraint()) {
            CheckConstraint checkConstraint = column.getCheckConstraint();
            print(" ");
            writeCheckConstraint(checkConstraint);
        }
    }

    /**
     * Returns the full SQL type specification (including size and precision/scale) for the
     * given column.
     *
     * @param column The column
     * @return The full SQL type string including the size
     */
    protected String getSqlType(Column column) {
        return getSqlType(column, getNativeType(column));
    }

    /**
     * Returns the full SQL type specification (including size and precision/scale) for the
     * given column.
     *
     * @param column     The column
     * @param nativeType Overrides the native type of the column; can include the size placeholder
     * @return The full SQL type string including the size
     */
    protected String getSqlType(Column column, String nativeType) {
        int sizePos = nativeType.indexOf(SIZE_PLACEHOLDER);
        StringBuilder sqlType = new StringBuilder();

        sqlType.append(sizePos >= 0 ? nativeType.substring(0, sizePos) : nativeType);

        String sizeSpec = getSizeSpec(column);

        if (sizeSpec != null && !sizeSpec.isEmpty()) {
            sqlType.append("(");
            sqlType.append(sizeSpec);
            sqlType.append(")");
        }
        sqlType.append(sizePos >= 0 ? nativeType.substring(sizePos + SIZE_PLACEHOLDER.length()) : "");

        return sqlType.toString();
    }

    /**
     * Returns the database-native type for the given column.
     *
     * @param column The column
     * @return The native type
     */
    protected String getNativeType(Column column) {
        String nativeType = getPlatformInfo().getNativeType(column.getTypeCode());

        return nativeType == null ? column.getType() : nativeType;
    }

    /**
     * Returns the bare database-native type for the given column without any size specifies.
     *
     * @param column The column
     * @return The native type
     */
    protected String getBareNativeType(Column column) {
        String nativeType = getNativeType(column);
        int sizePos = nativeType.indexOf(SIZE_PLACEHOLDER);

        return sizePos >= 0 ? nativeType.substring(0, sizePos) : nativeType;
    }

    /**
     * Returns the size specification for the given column. If the column is of a type that has size
     * or precision and scale, and no size is defined for the column itself, then the default size
     * or precision/scale for that type and platform is used instead.
     *
     * @param column The column
     * @return The size spec
     */
    protected String getSizeSpec(Column column) {
        StringBuilder result = new StringBuilder();
        Object sizeSpec = column.getSize();

        if (sizeSpec == null) {
            sizeSpec = getPlatformInfo().getDefaultSize(column.getTypeCode());
        }
        if (sizeSpec != null) {
            if (getPlatformInfo().hasSize(column.getTypeCode())) {
                result.append(sizeSpec.toString());
            } else if (getPlatformInfo().hasPrecisionAndScale(column.getTypeCode())) {
                result.append(column.getSizeAsInt());
                result.append(",");
                result.append(column.getScale());
            }
        }
        return result.toString();
    }

    /**
     * Returns the native default value for the column.
     *
     * @param column The column
     * @return The native default value
     */
    protected String getNativeDefaultValue(Column column) {
        return column.getDefaultValue();
    }

    /**
     * Escapes the necessary characters in given string value.
     *
     * @param value The value
     * @return The corresponding string with the special characters properly escaped
     */
    protected String escapeStringValue(String value) {
        String result = value;

        for (Iterator it = _charSequencesToEscape.entrySet().iterator(); it.hasNext(); ) {
            Map.Entry entry = (Map.Entry) it.next();

            result = StringUtils.replace(result, (String) entry.getKey(), (String) entry.getValue());
        }
        return result;
    }

    /**
     * Determines whether the given default spec is a non-empty spec that shall be used in a DEFAULT
     * expression. E.g. if the spec is an empty string and the type is a numeric type, then it is
     * no valid default value whereas if it is a string type, then it is valid.
     *
     * @param defaultSpec The default value spec
     * @param typeCode    The JDBC type code
     * @return <code>true</code> if the default value spec is valid
     */
    protected boolean isValidDefaultValue(String defaultSpec, int typeCode) {
        return (defaultSpec != null) &&
            ((defaultSpec.length() > 0) ||
                (!TypeMap.isNumericType(typeCode) && !TypeMap.isDateTimeType(typeCode)));
    }

    /**
     * Prints the default value stmt part for the column.
     *
     * @param table  The table
     * @param column The column
     */
    protected void writeColumnDefaultValueStmt(Table table, Column column) throws IOException {
        Object parsedDefault = column.getParsedDefaultValue();

        if (parsedDefault != null) {
            if (!getPlatformInfo().isDefaultValuesForLongTypesSupported() &&
                ((column.getTypeCode() == Types.LONGVARBINARY) || (column.getTypeCode() == Types.LONGVARCHAR))) {
                throw new ModelException("The platform does not support default values for LONGVARCHAR or LONGVARBINARY columns");
            }
            // we write empty default value strings only if the type is not a numeric or date/time type
            if (isValidDefaultValue(column.getDefaultValue(), column.getTypeCode())) {
                print(" DEFAULT ");
                writeColumnDefaultValue(table, column);
            }
        } else if (getPlatformInfo().isDefaultValueUsedForIdentitySpec() && column.isAutoIncrement()) {
            print(" DEFAULT ");
            writeColumnDefaultValue(table, column);
        }
    }

    /**
     * Prints the default value of the column.
     *
     * @param table  The table
     * @param column The column
     */
    protected void writeColumnDefaultValue(Table table, Column column) throws IOException {
        printDefaultValue(getNativeDefaultValue(column), column.getTypeCode());
    }

    /**
     * Prints the default value of the column.
     *
     * @param defaultValue The default value
     * @param typeCode     The type code to write the default value for
     */
    protected void printDefaultValue(Object defaultValue, int typeCode) throws IOException {
        if (defaultValue != null) {
            boolean shouldUseQuotes = !TypeMap.isNumericType(typeCode);

            if (shouldUseQuotes) {
                // characters are only escaped when within a string literal 
                print(getPlatformInfo().getValueQuoteToken());
                print(escapeStringValue(defaultValue.toString()));
                print(getPlatformInfo().getValueQuoteToken());
            } else {
                print(defaultValue.toString());
            }
        }
    }

    /**
     * Prints that the column is an auto increment column.
     *
     * @param table  The table
     * @param column The column
     */
    protected void writeColumnAutoIncrementStmt(Table table, Column column) throws IOException {
        print("IDENTITY");
    }

    /**
     * Prints that a column is nullable.
     */
    protected void writeColumnNullableStmt() throws IOException {
        print("NULL");
    }

    /**
     * Prints that a column is not nullable.
     */
    protected void writeColumnNotNullableStmt() throws IOException {
        print("NOT NULL");
    }

    /**
     * Compares the current column in the database with the desired one.
     * Type, nullability, size, scale, default value, and precision radix are
     * the attributes checked.  Currently default values are compared, and
     * null and empty string are considered equal.
     *
     * @param currentColumn The current column as it is in the database
     * @param desiredColumn The desired column
     * @return <code>true</code> if the column specifications differ
     */
    protected boolean columnsDiffer(Column currentColumn, Column desiredColumn) {
        //The createColumn method leaves off the default clause if column.getDefaultValue()
        //is null.  mySQL interprets this as a default of "" or 0, and thus the columns
        //are always different according to this method.  alterDatabase will generate
        //an alter statement for the column, but it will be the exact same definition
        //as before.  In order to avoid this situation I am ignoring the comparison
        //if the desired default is null.  In order to "un-default" a column you'll
        //have to have a default="" or default="0" in the schema xml.
        //If this is bad for other databases, it is recommended that the createColumn
        //method use a "DEFAULT NULL" statement if that is what is needed.
        //A good way to get this would be to require a defaultValue="<NULL>" in the
        //schema xml if you really want null and not just unspecified.

        String desiredDefault = desiredColumn.getDefaultValue();
        String currentDefault = currentColumn.getDefaultValue();
        boolean defaultsEqual = (desiredDefault == null) || desiredDefault.equals(currentDefault);
        boolean sizeMatters = getPlatformInfo().hasSize(currentColumn.getTypeCode()) &&
            (desiredColumn.getSize() != null);

        // We're comparing the jdbc type that corresponds to the native type for the
        // desired type, in order to avoid repeated altering of a perfectly valid column
        return (getPlatformInfo().getTargetJdbcType(desiredColumn.getTypeCode()) != currentColumn.getTypeCode()) ||
            (desiredColumn.isRequired() != currentColumn.isRequired()) ||
            (sizeMatters && !StringUtils.equals(desiredColumn.getSize(), currentColumn.getSize())) ||
            !defaultsEqual;
    }

    /**
     * Returns the name to be used for the given foreign key. If the foreign key has no
     * specified name, this method determines a unique name for it. The name will also
     * be shortened to honor the maximum identifier length imposed by the platform.
     *
     * @param table The table for whith the foreign key is defined
     * @param fk    The foreign key
     * @return The name
     */
    public String getForeignKeyName(Table table, ForeignKey fk) {
        String fkName = fk.getName();
        boolean needsName = (fkName == null) || (fkName.length() == 0);

        if (needsName) {
            StringBuilder name = new StringBuilder();

            for (int idx = 0; idx < fk.getReferenceCount(); idx++) {
                name.append(fk.getReference(idx).getLocalColumnName());
                name.append("_");
            }
            name.append(fk.getForeignTableName());
            fkName = getConstraintName(null, table, "FK", name.toString());
        }
        fkName = shortenName(fkName, getMaxForeignKeyNameLength());

        if (needsName) {
            _log.warn("Encountered a foreign key in table " + table.getQualifiedName() + " that has no name. " +
                          "DdlUtils will use the auto-generated and shortened name " + fkName + " instead.");
        }

        return fkName;
    }

    /**
     * Returns the constraint name. This method takes care of length limitations imposed by some databases.
     *
     * @param prefix     The constraint prefix, can be <code>null</code>
     * @param table      The table that the constraint belongs to
     * @param secondPart The second name part, e.g. the name of the constraint column
     * @param suffix     The constraint suffix, e.g. a counter (can be <code>null</code>)
     * @return The constraint name
     */
    public String getConstraintName(String prefix, Table table, String secondPart, String suffix) {
        StringBuilder result = new StringBuilder();

        if (prefix != null) {
            result.append(prefix);
            result.append("_");
        }
        result.append(table.getQualifiedName());
        result.append("_");
        result.append(secondPart);
        if (suffix != null) {
            result.append("_");
            result.append(suffix);
        }
        return shortenName(result.toString(), getMaxConstraintNameLength());
    }

    /**
     * Writes the primary key constraints of the table inside its definition.
     *
     * @param table The table
     */
    protected void writeEmbeddedPrimaryKeysStmt(Table table) throws IOException {
        Column[] primaryKeyColumns = table.getPrimaryKeyColumns();

        if ((primaryKeyColumns.length > 0) && shouldGeneratePrimaryKeys(primaryKeyColumns)) {
            printStartOfEmbeddedStatement();
            writePrimaryKeyStmt(table, primaryKeyColumns);
        }
    }

    /**
     * Determines whether we should generate a primary key constraint for the given
     * primary key columns.
     *
     * @param primaryKeyColumns The pk columns
     * @return <code>true</code> if a pk statement should be generated for the columns
     */
    protected boolean shouldGeneratePrimaryKeys(Column[] primaryKeyColumns) {
        return true;
    }

    /**
     * Writes a primary key statement for the given columns.
     *
     * @param table             The table
     * @param primaryKeyColumns The primary columns
     */
    protected void writePrimaryKeyStmt(Table table, Column[] primaryKeyColumns) throws IOException {
        print("PRIMARY KEY (");
        for (int idx = 0; idx < primaryKeyColumns.length; idx++) {
            printIdentifier(getColumnName(primaryKeyColumns[idx]));
            if (idx < primaryKeyColumns.length - 1) {
                print(", ");
            }
        }
        print(")");
    }

    /**
     * Returns the index name. This method takes care of length limitations imposed by some databases.
     *
     * @param index The index
     * @return The index name
     */
    public String getIndexName(Index index) {
        return shortenName(index.getName(), getMaxConstraintNameLength());
    }

    /**
     * Writes the indexes embedded within the create table statement.
     *
     * @param table The table
     */
    protected void writeEmbeddedIndicesStmt(Table table) throws IOException {
        if (getPlatformInfo().isIndicesSupported()) {
            for (int idx = 0; idx < table.getIndexCount(); idx++) {
                printStartOfEmbeddedStatement();
                writeEmbeddedIndexCreateStmt(table, table.getIndex(idx));
            }
        }
    }

    /**
     * Writes the given embedded index of the table.
     *
     * @param table The table
     * @param index The index
     */
    protected void writeEmbeddedIndexCreateStmt(Table table, Index index) throws IOException {
        if ((index.getName() != null) && (index.getName().length() > 0)) {
            print(" CONSTRAINT ");
            printIdentifier(getIndexName(index));
        }
        if (index.isUnique()) {
            print(" UNIQUE");
        } else {
            print(" INDEX ");
        }
        print(" (");

        for (int idx = 0; idx < index.getColumnCount(); idx++) {
            IndexColumn idxColumn = index.getColumn(idx);
            Column col = table.findColumn(idxColumn.getName());

            if (col == null) {
                // would get null pointer on next line anyway, so throw exception
                throw new ModelException("Invalid column '" + idxColumn.getName() + "' on index " + index.getName() + " for " +
                                             "table " + table.getQualifiedName());
            }
            if (idx > 0) {
                print(", ");
            }
            printIdentifier(getColumnName(col));
        }

        print(")");
    }

    /**
     * Generates the statement to drop a non-embedded index from the database.
     *
     * @param table The table the index is on
     * @param index The index to drop
     */
    public void dropIndex(Table table, Index index) throws IOException {
        if (getPlatformInfo().isAlterTableForDropUsed()) {
            writeTableAlterStmt(table);
        }
        print("DROP INDEX ");
        printIdentifier(getIndexName(index));
        if (!getPlatformInfo().isAlterTableForDropUsed()) {
            print(" ON ");
            printIdentifier(getTableName(table));
        }
        printEndOfStatement();
    }


    /**
     * Writes the foreign key constraints inside a create table () clause.
     *
     * @param database The database model
     * @param table    The table
     */
    protected void writeEmbeddedForeignKeysStmt(Database database, Table table) throws IOException {
        for (int idx = 0; idx < table.getForeignKeyCount(); idx++) {
            ForeignKey foreignKey = table.getForeignKey(idx);

            if (foreignKey.getForeignTableName() == null) {
                _log.warn("Foreign key table is null for key " + foreignKey);
            } else {
                printStartOfEmbeddedStatement();
                if (getPlatformInfo().isEmbeddedForeignKeysNamed()) {
                    print("CONSTRAINT ");
                    printIdentifier(getForeignKeyName(table, foreignKey));
                    print(" ");
                }
                print("FOREIGN KEY (");
                writeLocalReferences(foreignKey);
                print(") REFERENCES ");
                printIdentifier(getTableName(database.findTable(table.getSchema(), foreignKey.getForeignTableName(), false)));
                print(" (");
                writeForeignReferences(foreignKey);
                print(")");
                writeForeignKeyOnDeleteAction(table, foreignKey);
                writeForeignKeyOnUpdateAction(table, foreignKey);
            }
        }
    }

    /**
     * Writes a list of local references for the given foreign key.
     *
     * @param key The foreign key
     */
    protected void writeLocalReferences(ForeignKey key) throws IOException {
        for (int idx = 0; idx < key.getReferenceCount(); idx++) {
            if (idx > 0) {
                print(", ");
            }
            printIdentifier(key.getReference(idx).getLocalColumnName());
        }
    }

    /**
     * Writes a list of foreign references for the given foreign key.
     *
     * @param key The foreign key
     */
    protected void writeForeignReferences(ForeignKey key) throws IOException {
        for (int idx = 0; idx < key.getReferenceCount(); idx++) {
            if (idx > 0) {
                print(", ");
            }
            printIdentifier(key.getReference(idx).getForeignColumnName());
        }
    }

    /**
     * Writes the onDelete action for the given foreign key.
     *
     * @param table      The table
     * @param foreignKey The foreignkey
     */
    protected void writeForeignKeyOnDeleteAction(Table table, ForeignKey foreignKey) throws IOException {
        CascadeActionEnum action = foreignKey.getOnDelete();

        if (!getPlatformInfo().isActionSupportedForOnDelete(action)) {
            if (getPlatform().isDefaultOnDeleteActionUsedIfUnsupported()) {
                _log.info("The platform does not support the " + action + " action for onDelete; using " + getPlatformInfo()
                    .getDefaultOnDeleteAction() + " instead");
                action = getPlatformInfo().getDefaultOnDeleteAction();
            } else {
                throw new ModelException("The platform does not support the action '" + action +
                                             "' for onDelete in foreign key in table " + table.getQualifiedName());
            }
        }
        if (action != getPlatformInfo().getDefaultOnDeleteAction()) {
            print(" ON DELETE ");
            switch (action.getValue()) {
                case CascadeActionEnum.VALUE_CASCADE:
                    print("CASCADE");
                    break;
                case CascadeActionEnum.VALUE_SET_NULL:
                    print("SET NULL");
                    break;
                case CascadeActionEnum.VALUE_SET_DEFAULT:
                    print("SET DEFAULT");
                    break;
                case CascadeActionEnum.VALUE_RESTRICT:
                    print("RESTRICT");
                    break;
                case CascadeActionEnum.VALUE_NONE:
                    print("NO ACTION");
                    break;
                default:
                    throw new ModelException("Unsupported cascade value '" + action +
                                                 "' for onDelete in foreign key in table " + table.getQualifiedName());
            }
        }
    }

    /**
     * Writes the onDelete action for the given foreign key.
     *
     * @param table      The table
     * @param foreignKey The foreignkey
     */
    protected void writeForeignKeyOnUpdateAction(Table table, ForeignKey foreignKey) throws IOException {
        CascadeActionEnum action = foreignKey.getOnUpdate();

        if (!getPlatformInfo().isActionSupportedForOnUpdate(action)) {
            if (getPlatform().isDefaultOnUpdateActionUsedIfUnsupported()) {
                _log.info("The platform does not support the " + action + " action for onUpdate; using " + getPlatformInfo()
                    .getDefaultOnUpdateAction() + " instead");
                action = getPlatformInfo().getDefaultOnUpdateAction();
            } else {
                throw new ModelException("The platform does not support the action '" + action +
                                             "' for onUpdate in foreign key in table " + table.getQualifiedName());
            }
        }
        if (action != getPlatformInfo().getDefaultOnUpdateAction()) {
            print(" ON UPDATE ");
            switch (action.getValue()) {
                case CascadeActionEnum.VALUE_CASCADE:
                    print("CASCADE");
                    break;
                case CascadeActionEnum.VALUE_SET_NULL:
                    print("SET NULL");
                    break;
                case CascadeActionEnum.VALUE_SET_DEFAULT:
                    print("SET DEFAULT");
                    break;
                case CascadeActionEnum.VALUE_RESTRICT:
                    print("RESTRICT");
                    break;
                case CascadeActionEnum.VALUE_NONE:
                    print("NO ACTION");
                    break;
                default:
                    throw new ModelException("Unsupported cascade value '" + action +
                                                 "' for onUpdate in foreign key in table " + table.getQualifiedName());
            }
        }
    }

    //
    // Helper methods
    //

    /**
     * Prints an SQL comment to the current stream.
     *
     * @param text The comment text
     */
    protected void printComment(String text) throws IOException {
        if (getPlatform().isSqlCommentsOn()) {
            print(getPlatformInfo().getCommentPrefix());
            // Some databases insist on a space after the prefix
            print(" ");
            print(text);
            print(" ");
            print(getPlatformInfo().getCommentSuffix());
            println();
        }
    }

    /**
     * Prints the start of an embedded statement.
     */
    protected void printStartOfEmbeddedStatement() throws IOException {
        println(",");
        printIndent();
    }

    /**
     * Prints the end of statement text, which is typically a semi colon followed by
     * a carriage return.
     */
    protected void printEndOfStatement() throws IOException {
        // TODO: It might make sense to use a special writer which stores the individual
        //       statements separately (the end of a statement is identified by this method)
        if (terminateStatements) {
            println(getPlatformInfo().getSqlCommandDelimiter());
        }
        println();
    }

    /**
     * Prints a newline.
     */
    protected void println() throws IOException {
        print(LINE_SEPARATOR);
    }

    /**
     * Prints some text.
     *
     * @param text The text to print
     */
    protected void print(String text) throws IOException {
        _writer.write(text);
    }

    /**
     * Returns the delimited version of the identifier (if configured).
     *
     * @param identifier The identifier
     * @return The delimited version of the identifier unless the platform is configured
     * to use undelimited identifiers; in that case, the identifier is returned unchanged
     */
    protected String getDelimitedIdentifier(String identifier) {
        if (getPlatform().isDelimitedIdentifierModeOn()) {
            return getPlatformInfo().getDelimiterToken() + identifier + getPlatformInfo().getDelimiterToken();
        } else {
            return identifier;
        }
    }

    /**
     * Prints the given identifier. For most databases, this will
     * be a delimited identifier.
     *
     * @param identifier The identifier
     */
    protected void printIdentifier(String identifier) throws IOException {
        print(getDelimitedIdentifier(identifier));
    }

    /**
     * Prints the given identifier followed by a newline. For most databases, this will
     * be a delimited identifier.
     *
     * @param identifier The identifier
     */
    protected void printlnIdentifier(String identifier) throws IOException {
        println(getDelimitedIdentifier(identifier));
    }

    /**
     * Prints some text followed by a newline.
     *
     * @param text The text to print
     */
    protected void println(String text) throws IOException {
        print(text);
        println();
    }

    /**
     * Prints the characters used to indent SQL.
     */
    protected void printIndent() throws IOException {
        print(getIndent());
    }

    /**
     * Creates a reasonably unique identifier only consisting of hexadecimal characters and underscores.
     * It looks like <code>d578271282b42fce__2955b56e_107df3fbc96__8000</code> and is 48 characters long.
     *
     * @return The identifier
     */
    protected String createUniqueIdentifier() {
        return new UID().toString().replace(':', '_').replace('-', '_');
    }

    /**
     * Create SQL statements to export data from the given model.
     * @param model the model to export.
     * @param exportRoot location of the root of the export directory
     * @param params The parameters used for export
     */
    public void createExportSQL(Database model, String exportRoot, Map<String, String> params) throws IOException {
        // Optional method. Default implementation does nothing.
    }

    /**
     * Create SQL statements to import data into the given model.
     * @param model the model to import.
     * @param exportRoot location of the root of the export directory
     * @param params The parameters used for import
     */
    public void createImportSQL(Database model, String exportRoot, Map<String, String> params) throws IOException {
        // Optional method. Default implementation does nothing.
    }
}
