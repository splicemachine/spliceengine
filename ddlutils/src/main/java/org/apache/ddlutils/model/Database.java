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

package org.apache.ddlutils.model;

import java.io.Serializable;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import org.apache.commons.beanutils.DynaBean;
import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.commons.lang.builder.HashCodeBuilder;
import org.apache.ddlutils.dynabean.DynaClassCache;
import org.apache.ddlutils.dynabean.SqlDynaClass;
import org.apache.ddlutils.dynabean.SqlDynaException;

/**
 * Represents the database model, ie. the tables in the database. It also
 * contains the corresponding dyna classes for creating dyna beans for the
 * objects stored in the tables.
 *
 * @version $Revision: 636151 $
 */
public class Database implements Serializable {
    /**
     * Unique ID for serialization purposes.
     */
    private static final long serialVersionUID = -3160443396757573868L;

    /**
     * The name of the database model.
     */
    private String _name;
    /**
     * The method for generating primary keys (currently ignored).
     */
    private String _idMethod;
    /**
     * The version of the model.
     */
    private String _version;
    /** database users */
    private Map<String,User> _users = new HashMap<>();
    /** database roles */
    private Map<String,Role> _roles = new HashMap<>();
    /** database permissions */
    private Map<String,Permission> _permissions = new HashMap<>();
    /**
     * The schemas.
     */
    private Map<String, Schema> _schemas = new HashMap<>();
    /**
     * The dyna class cache for this model.
     */
    private transient DynaClassCache _dynaClassCache = null;

    /**
     * Creates an empty model without a name.
     */
    public Database() {
    }

    /**
     * Creates an empty model with the given name.
     *
     * @param name The name
     */
    public Database(String name) {
        _name = name;
    }

//    /**
//     * Adds all tables from the other database to this database.
//     * Note that the other database is not changed.
//     *
//     * @param otherDb The other database model
//     */
//    public void mergeWith(Database otherDb) throws ModelException
//    {
//        CloneHelper cloneHelper = new CloneHelper();
//
//        for (int tableIdx = 0; tableIdx < otherDb.getTableCount(); tableIdx++)
//        {
//            Table table = otherDb.getTable(tableIdx);
//
//            if (findTable(table.getName()) != null)
//            {
//                // TODO: It might make more sense to log a warning and overwrite the table (or merge them) ?
//                throw new ModelException("Cannot merge the models because table "+table.getQualifiedName()+" already defined
// in this model");
//            }
//            else
//            {
//                addTable(cloneHelper.clone(table, true, false, this, true));
//            }
//        }
//        for (int tableIdx = 0; tableIdx < otherDb.getTableCount(); tableIdx++)
//        {
//            Table otherTable = otherDb.getTable(tableIdx);
//            Table localTable = findTable(otherTable.getName());
//
//            for (int fkIdx = 0; fkIdx < otherTable.getForeignKeyCount(); fkIdx++)
//            {
//                ForeignKey fk = otherTable.getForeignKey(fkIdx);
//
//                localTable.addForeignKey(cloneHelper.clone(fk, localTable, this, false));
//            }
//        }
//    }

    /**
     * Returns the name of this database model.
     *
     * @return The name
     */
    public String getName() {
        return _name;
    }

    /**
     * Sets the name of this database model.
     *
     * @param name The name
     */
    public void setName(String name) {
        _name = name;
    }

    /**
     * Returns the version of this database model.
     *
     * @return The version
     */
    public String getVersion() {
        return _version;
    }

    /**
     * Sets the version of this database model.
     *
     * @param version The version
     */
    public void setVersion(String version) {
        _version = version;
    }

    /**
     * Returns the method for generating primary key values.
     *
     * @return The method
     */
    public String getIdMethod() {
        return _idMethod;
    }

    /**
     * Sets the method for generating primary key values. Note that this
     * value is ignored by DdlUtils and only for compatibility with Torque.
     *
     * @param idMethod The method
     */
    public void setIdMethod(String idMethod) {
        _idMethod = idMethod;
    }

    /**
     * Add a user.
     * @param user a database user
     */
    public void addUser(User user) {
        if (user != null && (user.getName() != null && ! user.getName().isEmpty())) {
            // user names are case sensitive
            _users.put(user.getName(), user);
        }
    }

    /**
     * Find a user.
     * @param userName a database user's user name
     */
    public User findUser(String userName) {
        if (userName != null && ! userName.isEmpty()) {
            // user names are case sensitive
            return _users.get(userName);
        }
        return null;
    }

    /**
     * Get the database users.
     * @return users
     */
    public Collection<User> getUsers() {
        return new ArrayList<>(_users.values());
    }

    /**
     * Add a role.
     * @param role a database role
     */
    public void addRole(Role role) {
        if (role != null && (role.getName() != null && ! role.getName().isEmpty())) {
            // user names are case sensitive
            _roles.put(role.getName(), role);
        }
    }

    /**
     * Find a role.
     * @param roleName a database user's role name
     */
    public Role findRole(String roleName) {
        if (roleName != null && ! roleName.isEmpty()) {
            // role names are case sensitive
            return _roles.get(roleName);
        }
        return null;
    }

    /**
     * Get the database roles.
     * @return roles
     */
    public Collection<Role> getRoles() {
        return new ArrayList<>(_roles.values());
    }

    /**
     * Add a permission.
     * @param permission a database role
     */
    public void addPermission(Permission permission) {
        if (permission != null && (permission.getId() != null && ! permission.getId().isEmpty())) {
            // user names are case sensitive
            _permissions.put(permission.getId(), permission);
        }
    }

    /**
     * Find a permission.
     * @param permissionId a database user's role name
     */
    public Permission findPermission(String permissionId) {
        if (permissionId != null && ! permissionId.isEmpty()) {
            // permission ids are case sensitive
            return _permissions.get(permissionId);
        }
        return null;
    }

    /**
     * Get the database permissions.
     * @return permissions
     */
    public Collection<Permission> getPermissions() {
        return new ArrayList<>(_permissions.values());
    }

    /**
     * Find the database actor. Could be user or role.
     * @param actorName name of the actor.
     * @return the actor or null if not found.
     */
    public Actor findActor(String actorName) {
        // could be user or role
        Actor actor = findUser(actorName);
        if (actor == null) {
            actor = findRole(actorName);
        }
        return actor;
    }

    /**
     * Returns the number of schemas in this model.
     *
     * @return The number of schemas
     */
    public int getSchemaCount() {
        return _schemas.size();
    }

    /**
     * Returns the schemas in this model.
     *
     * @return The schemas
     */
    public List<Schema> getSchemas() {
        return new ArrayList<>(_schemas.values());
    }

    /**
     * Adds a schema.
     *
     * @param schema The schema to add
     */
    public void addSchema(Schema schema) {
        if (schema != null) {
            _schemas.put(schema.getSchemaName().toUpperCase(), schema);
        }
    }

    /**
     * Find the given schema by name in the database model.
     * @param schemaName schema to find
     * @return the schema or <code>null</code> if not found.
     */
    public Schema findSchema(String schemaName) {
        return _schemas.get(schemaName.toUpperCase());
    }

    /**
     * Find the given schema in the database model.
     * @param schema schema to find
     * @return the schema or <code>null</code> if not found.
     */
    public Schema findSchema(Schema schema) {
        return _schemas.get(schema.getSchemaName().toUpperCase());
    }

    /**
     * Removes the given table from the given schema.
     *
     * @param schema the schema in which to add tables. Can't be null.
     * @param table  The table to add
     */
    public void removeTableFromSchema(Schema schema, Table table) {
        if (schema == null) throw new IllegalArgumentException("Schema cannot be null.");
        Schema target = _schemas.get(schema.getSchemaName().toUpperCase());
        if (target != null) {
            target.removeTable(table);
        }
    }

    /**
     * Adds the given table to the given schema.
     *
     * @param schema the schema in which to add tables. Can't be null.
     * @param table  The table to add
     */
    public void addTableIntoSchema(Schema schema, Table table) {
        if (schema == null) throw new IllegalArgumentException("Schema cannot be null.");
        Schema target = _schemas.get(schema.getSchemaName().toUpperCase());
        if (target != null) {
            target.addTable(table);
        }
    }

    /**
     * Adds the given tables to the given schema.
     *
     * @param schema the schema in which to add tables. Can't be null.
     * @param tables The tables to add
     */
    public void addTablesIntoSchema(Schema schema, Collection<Table> tables) {
        if (schema == null) throw new IllegalArgumentException("Schema cannot be null.");
        Schema target = _schemas.get(schema.getSchemaName().toUpperCase());
        if (target != null) {
            for (Table table : tables) {
                target.addTable(table);
            }
        }
    }

//    /**
//     * Returns the number of tables in this model.
//     *
//     * @return The number of tables
//     */
//    public int getTableCount()
//    {
//        return _tables.size();
//    }
//
//    /**
//     * Returns the tables in this model.
//     *
//     * @return The tables
//     */
//    public Table[] getTables()
//    {
//        return (Table[])_tables.toArray(new Table[_tables.size()]);
//    }
//
//    /**
//     * Returns the table at the specified position.
//     *
//     * @param idx The index of the table
//     * @return The table
//     */
//    public Table getTable(int idx)
//    {
//        return (Table)_tables.get(idx);
//    }
//
//    /**
//     * Adds a table.
//     *
//     * @param table The table to add
//     */
//    public void addTable(Table table)
//    {
//        if (table != null)
//        {
//            _tables.add(table);
//        }
//    }
//
//    /**
//     * Adds a table at the specified position.
//     *
//     * @param idx   The index where to insert the table
//     * @param table The table to add
//     */
//    public void addTable(int idx, Table table)
//    {
//        if (table != null)
//        {
//            _tables.add(idx, table);
//        }
//    }
//
//    /**
//     * Adds the given tables.
//     *
//     * @param tables The tables to add
//     */
//    public void addTables(Collection tables)
//    {
//        for (Iterator it = tables.iterator(); it.hasNext();)
//        {
//            addTable((Table)it.next());
//        }
//    }
//
//    /**
//     * Removes the given table. This method does not check whether there are foreign keys to the table.
//     *
//     * @param table The table to remove
//     */
//    public void removeTable(Table table)
//    {
//        if (table != null)
//        {
//            _tables.remove(table);
//        }
//    }
//
//    /**
//     * Removes the indicated table. This method does not check whether there are foreign keys to the table.
//     *
//     * @param idx The index of the table to remove
//     */
//    public void removeTable(int idx)
//    {
//        _tables.remove(idx);
//    }
//
//    /**
//     * Removes the given tables. This method does not check whether there are foreign keys to the tables.
//     *
//     * @param tables The tables to remove
//     */
//    public void removeTables(Table[] tables)
//    {
//        _tables.removeAll(Arrays.asList(tables));
//    }
//
//    /**
//     * Removes all but the given tables. This method does not check whether there are foreign keys to the
//     * removed tables.
//     *
//     * @param tables The tables to keep
//     */
//    public void removeAllTablesExcept(Table[] tables)
//    {
//        ArrayList allTables = new ArrayList(_tables);
//
//        allTables.removeAll(Arrays.asList(tables));
//        _tables.removeAll(allTables);
//    }

    // Helper methods

    /**
     * Initializes the model by establishing the relationships between elements in this model encoded
     * eg. in foreign keys etc. Also checks that the model elements are valid (table and columns have
     * a name, foreign keys rference existing tables etc.)
     */
    public void initialize() throws ModelException {
        // we have to setup
        // * target tables in foreign keys
        // * columns in foreign key references
        // * columns in indices
        // * columns in uniques
        HashSet<String> namesOfProcessedTables = new HashSet<>();
        HashSet<String> namesOfProcessedColumns = new HashSet<>();
        HashSet<String> namesOfProcessedFks = new HashSet<>();
        HashSet<String> namesOfProcessedIndices = new HashSet<>();
        int tableIdx = 0;

        if ((getName() == null) || (getName().length() == 0)) {
            throw new ModelException("The database model has no name");
        }

        for (Schema schema : _schemas.values()) {
            for (Table curTable : schema.getTables()) {

                if ((curTable.getName() == null) || (curTable.getName().length() == 0)) {
                    throw new ModelException("The table nr. " + tableIdx + " has no name");
                }
                if (namesOfProcessedTables.contains(curTable.getQualifiedName())) {
                    throw new ModelException("There are multiple tables with the name " + curTable.getQualifiedName());
                }
                namesOfProcessedTables.add(curTable.getQualifiedName());

                namesOfProcessedColumns.clear();
                namesOfProcessedFks.clear();
                namesOfProcessedIndices.clear();


                for (int idx = 0; idx < curTable.getColumnCount(); idx++) {
                    Column column = curTable.getColumn(idx);

                    if ((column.getName() == null) || (column.getName().length() == 0)) {
                        throw new ModelException("The column nr. " + idx + " in table " + curTable.getQualifiedName() + " has " +
                                                     "no name");

                    }
                    if (namesOfProcessedColumns.contains(column.getName())) {
                        throw new ModelException("There are multiple columns with the name " + column.getName() + " in the " +
                                                     "table " + curTable.getQualifiedName());
                    }
                    namesOfProcessedColumns.add(column.getName());

                    if ((column.getType() == null) || (column.getType().length() == 0)) {
                        throw new ModelException("The column nr. " + idx + " in table " + curTable.getQualifiedName() + " has " +
                                                     "no type");
                    }
                    if ((column.getTypeCode() == Types.OTHER) && !"OTHER".equalsIgnoreCase(column.getType())) {
                        throw new ModelException("The column nr. " + idx + " in table " + curTable.getQualifiedName() + " has " +
                                                     "an unknown type " + column.getType());
                    }
                    namesOfProcessedColumns.add(column.getName());
                }

                for (int idx = 0; idx < curTable.getForeignKeyCount(); idx++) {
                    ForeignKey fk = curTable.getForeignKey(idx);
                    String fkName = (fk.getName() == null ? "" : fk.getName());
                    String fkDesc = (fkName.length() == 0 ? "nr. " + idx : fkName);

                    if (fkName.length() > 0) {
                        if (namesOfProcessedFks.contains(fkName)) {
                            throw new ModelException("There are multiple foreign keys in table " + curTable.getQualifiedName()
                                                         + " with the name " + fkName);

                        }
                        namesOfProcessedFks.add(fkName);
                    }

                    if (fk.getForeignTable() == null) {
                        Table targetTable = findTable(schema, fk.getForeignTableName(), true);

                        if (targetTable == null) {
                            throw new ModelException("The foreignkey " + fkDesc + " in table " + curTable.getQualifiedName() +
                                                         " references the undefined table " + fk.getForeignTableName());
                        } else {
                            fk.setForeignTable(targetTable);
                        }
                    }
                    if (fk.getReferenceCount() == 0) {
                        throw new ModelException("The foreignkey " + fkDesc + " in table " + curTable.getQualifiedName() + " " +
                                                     "does not have any references");
                    }
                    for (int refIdx = 0; refIdx < fk.getReferenceCount(); refIdx++) {
                        Reference ref = fk.getReference(refIdx);

                        if (ref.getLocalColumn() == null) {
                            Column localColumn = curTable.findColumn(ref.getLocalColumnName(), true);

                            if (localColumn == null) {
                                throw new ModelException("The foreignkey " + fkDesc + " in table " + curTable.getQualifiedName
                                    () + " references the undefined local column " + ref.getLocalColumnName());
                            } else {
                                ref.setLocalColumn(localColumn);
                            }
                        }
                        if (ref.getForeignColumn() == null) {
                            Column foreignColumn = fk.getForeignTable().findColumn(ref.getForeignColumnName(), true);

                            if (foreignColumn == null) {
                                throw new ModelException("The foreignkey " + fkDesc + " in table " + curTable.getQualifiedName
                                    () + " references the undefined local column " + ref.getForeignColumnName() + " in table "
                                                             + fk.getForeignTable().getName());
                            } else {
                                ref.setForeignColumn(foreignColumn);
                            }
                        }
                    }
                }

                for (int idx = 0; idx < curTable.getIndexCount(); idx++) {
                    Index index = curTable.getIndex(idx);
                    String indexName = (index.getName() == null ? "" : index.getName());
                    String indexDesc = (indexName.length() == 0 ? "nr. " + idx : indexName);

                    if (indexName.length() > 0) {
                        if (namesOfProcessedIndices.contains(indexName)) {
                            throw new ModelException("There are multiple indices in table " + curTable.getQualifiedName() + " " +
                                                         "with the name " + indexName);
                        }
                        namesOfProcessedIndices.add(indexName);
                    }
                    if (index.getColumnCount() == 0) {
                        throw new ModelException("The index " + indexDesc + " in table " + curTable.getQualifiedName() + " does" +
                                                     " not have any columns");
                    }

                    for (int indexColumnIdx = 0; indexColumnIdx < index.getColumnCount(); indexColumnIdx++) {
                        IndexColumn indexColumn = index.getColumn(indexColumnIdx);
                        Column column = curTable.findColumn(indexColumn.getName(), true);

                        if (column == null) {
                            throw new ModelException("The index " + indexDesc + " in table " + curTable.getQualifiedName() + " " +
                                                         "references the undefined column " + indexColumn.getName());
                        } else {
                            indexColumn.setColumn(column);
                        }
                    }
                }
            }
        }
    }
//
//    /**
//     * Finds the table with the specified name, using case insensitive matching.
//     * Note that this method is not called getTable to avoid introspection
//     * problems.
//     *
//     * @param name The name of the table to find
//     * @return The table or <code>null</code> if there is no such table
//     */
//    public Table findTable(String name)
//    {
//        return findTable(, name, false);
//    }

    /**
     * Finds the table with the specified name, using case insensitive matching.
     * Note that this method is not called getTable) to avoid introspection
     * problems.
     *
     * @param schemaName    The name of the schema in which to look.
     * @param tableName     The name of the table to find
     * @param caseSensitive Whether case matters for the names
     * @return The table or <code>null</code> if there is no such table
     */
    public Table findTable(String schemaName, String tableName, boolean caseSensitive) {
        Schema target = _schemas.get(schemaName.toUpperCase());
        return findTable(target, tableName, caseSensitive);
    }

    /**
     * Finds the table with the specified name, using case insensitive matching.
     * Note that this method is not called getTable) to avoid introspection
     * problems.
     *
     * @param schema        The schema in which to look.
     * @param tableName     The name of the table to find
     * @param caseSensitive Whether case matters for the names
     * @return The table or <code>null</code> if there is no such table
     */
    public Table findTable(Schema schema, String tableName, boolean caseSensitive) {
        Schema target = _schemas.get(schema.getSchemaName().toUpperCase());

        if (target != null) {
            for (Table table : target.getTables()) {

                if (caseSensitive) {
                    if (table.getName().equals(tableName)) {
                        return table;
                    }
                } else {
                    if (table.getName().equalsIgnoreCase(tableName)) {
                        return table;
                    }
                }
            }
        }
        return null;
    }

//    /**
//     * Returns the indicated tables.
//     *
//     * @param tableNames    The names of the tables
//     * @param caseSensitive Whether the case of the table names matters
//     * @return The tables
//     */
//    public Table[] findTables(String[] tableNames, boolean caseSensitive)
//    {
//        ArrayList tables = new ArrayList();
//
//        if (tableNames != null)
//        {
//            for (int idx = 0; idx < tableNames.length; idx++)
//            {
//                Table table = findTable(tableNames[idx], caseSensitive);
//
//                if (table != null)
//                {
//                    tables.add(table);
//                }
//            }
//        }
//        return (Table[])tables.toArray(new Table[tables.size()]);
//    }

//    /**
//     * Finds the tables whose names match the given regular expression.
//     *
//     * @param tableNameRegExp The table name regular expression
//     * @param caseSensitive   Whether the case of the table names matters; if not, then the regular expression should
//     *                        assume that the table names are all-uppercase
//     * @return The tables
//     * @throws PatternSyntaxException If the regular expression is invalid
//     */
//    public Table[] findTables(String tableNameRegExp, boolean caseSensitive) throws PatternSyntaxException
//    {
//        ArrayList tables = new ArrayList();
//
//        if (tableNameRegExp != null)
//        {
//            Pattern pattern = Pattern.compile(tableNameRegExp);
//
//            for (Iterator tableIt = _tables.iterator(); tableIt.hasNext();)
//            {
//                Table  table     = (Table)tableIt.next();
//                String tableName = table.getName();
//
//                if (!caseSensitive)
//                {
//                    tableName = tableName.toUpperCase();
//                }
//                if (pattern.matcher(tableName).matches())
//                {
//                    tables.add(table);
//                }
//            }
//        }
//        return (Table[])tables.toArray(new Table[tables.size()]);
//    }

    /**
     * Returns the dyna class cache. If none is available yet, a new one will be created.
     *
     * @return The dyna class cache
     */
    private DynaClassCache getDynaClassCache() {
        if (_dynaClassCache == null) {
            _dynaClassCache = new DynaClassCache();
        }
        return _dynaClassCache;
    }

    /**
     * Resets the dyna class cache. This should be done for instance when a column
     * has been added or removed to a table.
     */
    public void resetDynaClassCache() {
        _dynaClassCache = null;
    }

    /**
     * Returns the {@link org.apache.ddlutils.dynabean.SqlDynaClass} for the given table name. If the it does not
     * exist yet, a new one will be created based on the Table definition.
     *
     * @param tableName The name of the table to create the bean for
     * @return The <code>SqlDynaClass</code> for the indicated table or <code>null</code>
     * if the model contains no such table
     */
    public SqlDynaClass getDynaClassFor(String schemaName, String tableName) {
        Table table = findTable(schemaName, tableName, false);

        return table != null ? getDynaClassCache().getDynaClass(table) : null;
    }

    /**
     * Returns the {@link org.apache.ddlutils.dynabean.SqlDynaClass} for the given dyna bean.
     *
     * @param bean The dyna bean
     * @return The <code>SqlDynaClass</code> for the given bean
     */
    public SqlDynaClass getDynaClassFor(DynaBean bean) {
        return getDynaClassCache().getDynaClass(bean);
    }

    /**
     * Creates a new dyna bean for the given table.
     *
     * @param table The table to create the bean for
     * @return The new dyna bean
     */
    public DynaBean createDynaBeanFor(Table table) throws SqlDynaException {
        return getDynaClassCache().createNewInstance(table);
    }

//    /**
//     * Convenience method that combines {@link #createDynaBeanFor(Table)} and
//     * {@link #findTable(Schema, String, boolean)}.
//     *
//     * @param tableName     The name of the table to create the bean for
//     * @param caseSensitive Whether case matters for the names
//     * @return The new dyna bean
//     */
//    public DynaBean createDynaBeanFor(String tableName, boolean caseSensitive) throws SqlDynaException
//    {
//        return getDynaClassCache().createNewInstance(findTable(schema, tableName, caseSensitive));
//    }

    /**
     * {@inheritDoc}
     */
    public int hashCode() {
        return new HashCodeBuilder(17, 37).append(_name)
                                          .append(_schemas)
                                          .toHashCode();
    }

    /**
     * {@inheritDoc}
     */
    public boolean equals(Object obj) {
        if (obj instanceof Database) {
            Database other = (Database) obj;

            // Note that this compares case sensitive
            return new EqualsBuilder().append(_name, other._name)
                                      .append(_schemas, other._schemas)
                                      .isEquals();
        } else {
            return false;
        }
    }

    /**
     * {@inheritDoc}
     */
    public String toString() {
        StringBuffer result = new StringBuffer();

        result.append("Database [name=");
        result.append(getName());
        result.append("; ");
        result.append(getSchemaCount());
        result.append(" schemas]");

        return result.toString();
    }

//    /**
//     * Returns a verbose string representation of this database.
//     *
//     * @return The string representation
//     */
//    public String toVerboseString()
//    {
//        StringBuffer result = new StringBuffer();
//
//        result.append("Database [");
//        result.append(getName());
//        result.append("] tables:");
//        for (int idx = 0; idx < getTableCount(); idx++)
//        {
//            result.append(" ");
//            result.append(getTable(idx).toVerboseString());
//        }
//
//        return result.toString();
//    }
}
