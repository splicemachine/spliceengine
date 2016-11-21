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

package org.apache.ddlutils.alteration;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.ddlutils.PlatformInfo;
import org.apache.ddlutils.model.CloneHelper;
import org.apache.ddlutils.model.Column;
import org.apache.ddlutils.model.Database;
import org.apache.ddlutils.model.ForeignKey;
import org.apache.ddlutils.model.Index;
import org.apache.ddlutils.model.Schema;
import org.apache.ddlutils.model.Table;
import org.apache.ddlutils.util.StringUtilsExt;

/**
 * Compares two database models and creates change objects that express how to
 * adapt the first model so that it becomes the second one. Neither of the models
 * are changed in the process, however, it is also assumed that the models do not
 * change in between.
 *
 * @version $Revision: $
 */
public class ModelComparator {
    /**
     * The log for this comparator.
     */
    private final Log _log = LogFactory.getLog(ModelComparator.class);

    /**
     * The platform information.
     */
    private PlatformInfo _platformInfo;
    /**
     * The predicate that defines which changes are supported by the platform.
     */
    private TableDefinitionChangesPredicate _tableDefCangePredicate;
    /**
     * The object clone helper.
     */
    private CloneHelper _cloneHelper = new CloneHelper();
    /**
     * Whether comparison is case sensitive.
     */
    private boolean _caseSensitive;
    /**
     * Whether the comparator should generate {@link PrimaryKeyChange} objects.
     */
    private boolean _generatePrimaryKeyChanges = true;
    /**
     * Whether {@link RemoveColumnChange} objects for primary key columns are enough or
     * additional primary key change objects are necessary.
     */
    private boolean _canDropPrimaryKeyColumns = true;

    /**
     * Creates a new model comparator object.
     *
     * @param platformInfo            The platform info
     * @param tableDefChangePredicate The predicate that defines whether tables changes are supported
     *                                by the platform or not; all changes are supported if this is null
     * @param caseSensitive           Whether comparison is case sensitive
     */
    public ModelComparator(PlatformInfo platformInfo,
                           TableDefinitionChangesPredicate tableDefChangePredicate,
                           boolean caseSensitive) {
        _platformInfo = platformInfo;
        _caseSensitive = caseSensitive;
        _tableDefCangePredicate = tableDefChangePredicate;
    }

    /**
     * Specifies whether the comparator should generate {@link PrimaryKeyChange} objects or a
     * pair of {@link RemovePrimaryKeyChange} and {@link AddPrimaryKeyChange} objects instead.
     * The default value is <code>true</code>.
     *
     * @param generatePrimaryKeyChanges Whether to create {@link PrimaryKeyChange} objects
     */
    public void setGeneratePrimaryKeyChanges(boolean generatePrimaryKeyChanges) {
        _generatePrimaryKeyChanges = generatePrimaryKeyChanges;
    }

    /**
     * Specifies whether the {@link RemoveColumnChange} are fine even for primary key columns.
     * If the platform cannot drop primary key columns, set this to <code>false</code> and the
     * comparator will create additional primary key changes.
     * The default value is <code>true</code>.
     *
     * @param canDropPrimaryKeyColumns Whether {@link RemoveColumnChange} objecs for primary
     *                                 key columns are ok
     */
    public void setCanDropPrimaryKeyColumns(boolean canDropPrimaryKeyColumns) {
        _canDropPrimaryKeyColumns = canDropPrimaryKeyColumns;
    }

    /**
     * Returns the info object for the platform.
     *
     * @return The platform info object
     */
    protected PlatformInfo getPlatformInfo() {
        return _platformInfo;
    }

    /**
     * Determines whether comparison should be case sensitive.
     *
     * @return <code>true</code> if case matters
     */
    protected boolean isCaseSensitive() {
        return _caseSensitive;
    }

    /**
     * Compares the two models and returns the changes necessary to create the second
     * model from the first one.
     *
     * @param sourceModel The source model
     * @param targetModel The target model
     * @return The changes
     */
    public List compare(Database sourceModel, Database targetModel) {
        Database intermediateModel = _cloneHelper.clone(sourceModel);

        return compareModels(sourceModel, intermediateModel, targetModel);
    }

    /**
     * Compares the given source and target models and creates change objects to get from
     * the source to the target one. These changes will be applied to the given
     * intermediate model (the other two won't be changed), so that it will be equal to
     * the target model after this model has finished.
     *
     * @param sourceModel       The source model
     * @param intermediateModel The intermediate model to apply the changes to
     * @param targetModel       The target model
     * @return The changes
     */
    protected List compareModels(Database sourceModel,
                                 Database intermediateModel,
                                 Database targetModel) {
        ArrayList changes = new ArrayList();

        changes.addAll(checkForRemovedForeignKeys(sourceModel, intermediateModel, targetModel));
        changes.addAll(checkForRemovedTables(sourceModel, intermediateModel, targetModel));

        for (Schema schema : intermediateModel.getSchemas()) {
            for (Table intermediateTable : schema.getTables()) {
                Table sourceTable = sourceModel.findTable(schema, intermediateTable.getName(), _caseSensitive);
                Table targetTable = targetModel.findTable(schema, intermediateTable.getName(), _caseSensitive);
                List tableChanges = compareTables(sourceModel, sourceTable,
                                                  intermediateModel, intermediateTable,
                                                  targetModel, targetTable);

                changes.addAll(tableChanges);
            }
        }

        changes.addAll(checkForAddedTables(sourceModel, intermediateModel, targetModel));
        changes.addAll(checkForAddedForeignKeys(sourceModel, intermediateModel, targetModel));
        return changes;
    }

    /**
     * Creates change objects for foreign keys that are present in the given source model but are no longer in the target
     * model, and applies them to the given intermediate model.
     *
     * @param sourceModel       The source model
     * @param intermediateModel The intermediate model to apply the changes to
     * @param targetModel       The target model
     * @return The changes
     */
    protected List checkForRemovedForeignKeys(Database sourceModel,
                                              Database intermediateModel,
                                              Database targetModel) {
        List changes = new ArrayList();

        for (Schema schema : intermediateModel.getSchemas()) {
            for (Table intermediateTable : schema.getTables()) {
                Table targetTable = targetModel.findTable(schema, intermediateTable.getName(), _caseSensitive);
                ForeignKey[] intermediateFks = intermediateTable.getForeignKeys();

                // Dropping foreign keys from tables to be removed might not be necessary, but some databases might require it
                for (int fkIdx = 0; fkIdx < intermediateFks.length; fkIdx++) {
                    ForeignKey sourceFk = intermediateFks[fkIdx];
                    ForeignKey targetFk = targetTable == null ? null : findCorrespondingForeignKey(targetTable, sourceFk);

                    if (targetFk == null) {
                        if (_log.isInfoEnabled()) {
                            _log.info("Foreign key " + sourceFk + " needs to be removed from table " + intermediateTable
                                .getName());
                        }

                        RemoveForeignKeyChange fkChange = new RemoveForeignKeyChange(intermediateTable, sourceFk);

                        changes.add(fkChange);
                        fkChange.apply(intermediateModel, _caseSensitive);
                    }
                }
            }
        }
        return changes;
    }

    /**
     * Creates change objects for foreign keys that are not present in the given source model but are in the target
     * model, and applies them to the given intermediate model.
     *
     * @param sourceModel       The source model
     * @param intermediateModel The intermediate model to apply the changes to
     * @param targetModel       The target model
     * @return The changes
     */
    protected List checkForAddedForeignKeys(Database sourceModel,
                                            Database intermediateModel,
                                            Database targetModel) {
        List changes = new ArrayList();

        for (Schema schema : targetModel.getSchemas()) {
            for (Table targetTable : schema.getTables()) {
                Table intermediateTable = intermediateModel.findTable(schema, targetTable.getName(), _caseSensitive);

                for (int fkIdx = 0; fkIdx < targetTable.getForeignKeyCount(); fkIdx++) {
                    ForeignKey targetFk = targetTable.getForeignKey(fkIdx);
                    ForeignKey intermediateFk = findCorrespondingForeignKey(intermediateTable, targetFk);

                    if (intermediateFk == null) {
                        if (_log.isInfoEnabled()) {
                            _log.info("Foreign key " + targetFk + " needs to be added to table " + intermediateTable.getName());
                        }

                        intermediateFk = _cloneHelper.clone(targetFk, intermediateTable, intermediateModel, _caseSensitive);

                        AddForeignKeyChange fkChange = new AddForeignKeyChange(intermediateTable, intermediateFk);

                        changes.add(fkChange);
                        fkChange.apply(intermediateModel, _caseSensitive);
                    }
                }
            }
        }
        return changes;
    }

    /**
     * Creates change objects for tables that are present in the given source model but are no longer in the target
     * model, and applies them to the given intermediate model.
     *
     * @param sourceModel       The source model
     * @param intermediateModel The intermediate model to apply the changes to
     * @param targetModel       The target model
     * @return The changes
     */
    protected List checkForRemovedTables(Database sourceModel,
                                         Database intermediateModel,
                                         Database targetModel) {
        List changes = new ArrayList();

        for (Schema schema : intermediateModel.getSchemas()) {
            for (Table intermediateTable : schema.getTables()) {
                Table targetTable = targetModel.findTable(schema, intermediateTable.getName(), _caseSensitive);

                if (targetTable == null) {
                    if (_log.isInfoEnabled()) {
                        _log.info("Table " + intermediateTable.getName() + " needs to be removed");
                    }

                    RemoveTableChange tableChange = new RemoveTableChange(intermediateTable);

                    changes.add(tableChange);
                    tableChange.apply(intermediateModel, _caseSensitive);
                }
            }
        }
        return changes;
    }

    /**
     * Creates change objects for tables that are not present in the given source model but are in the target
     * model, and applies them to the given intermediate model.
     *
     * @param sourceModel       The source model
     * @param intermediateModel The intermediate model to apply the changes to
     * @param targetModel       The target model
     * @return The changes
     */
    protected List checkForAddedTables(Database sourceModel,
                                       Database intermediateModel,
                                       Database targetModel) {
        List changes = new ArrayList();

        for (Schema schema : targetModel.getSchemas()) {
            for (Table targetTable : schema.getTables()) {
                Table intermediateTable = intermediateModel.findTable(schema, targetTable.getName(), _caseSensitive);

                if (intermediateTable == null) {
                    if (_log.isInfoEnabled()) {
                        _log.info("Table " + targetTable.getName() + " needs to be added");
                    }

                    // we're using a clone of the target table, and remove all foreign
                    // keys as these will be added later
                    intermediateTable = _cloneHelper.clone(targetTable, true, false, intermediateModel, _caseSensitive);

                    AddTableChange tableChange = new AddTableChange(intermediateTable);

                    changes.add(tableChange);
                    tableChange.apply(intermediateModel, _caseSensitive);
                }
            }
        }
        return changes;
    }

    /**
     * Compares the two tables and returns the changes necessary to create the second
     * table from the first one.
     *
     * @param sourceModel       The source model
     * @param sourceTable       The source table
     * @param intermediateModel The intermediate model to which the changes will be applied incrementally
     * @param intermediateTable The table corresponding to the source table in the intermediate model
     * @param targetModel       The target model which contains the target table
     * @param targetTable       The target table
     * @return The changes
     */
    protected List compareTables(Database sourceModel,
                                 Table sourceTable,
                                 Database intermediateModel,
                                 Table intermediateTable,
                                 Database targetModel,
                                 Table targetTable) {
        ArrayList changes = new ArrayList();

        changes.addAll(checkForRemovedIndexes(sourceModel, sourceTable, intermediateModel, intermediateTable, targetModel,
                                              targetTable));

        ArrayList tableDefinitionChanges = new ArrayList();
        Table tmpTable = _cloneHelper.clone(intermediateTable, true, false, intermediateModel, _caseSensitive);

        tableDefinitionChanges.addAll(checkForRemovedColumns(sourceModel, sourceTable, intermediateModel, intermediateTable,
                                                             targetModel, targetTable));
        tableDefinitionChanges.addAll(checkForChangeOfColumnOrder(sourceModel, sourceTable, intermediateModel,
                                                                  intermediateTable, targetModel, targetTable));
        tableDefinitionChanges.addAll(checkForChangedColumns(sourceModel, sourceTable, intermediateModel, intermediateTable,
                                                             targetModel, targetTable));
        tableDefinitionChanges.addAll(checkForAddedColumns(sourceModel, sourceTable, intermediateModel, intermediateTable,
                                                           targetModel, targetTable));
        tableDefinitionChanges.addAll(checkForPrimaryKeyChanges(sourceModel, sourceTable, intermediateModel, intermediateTable,
                                                                targetModel, targetTable));

        // TOOD: check for foreign key changes (on delete/on update)
        if (!tableDefinitionChanges.isEmpty()) {
            if ((_tableDefCangePredicate == null) || _tableDefCangePredicate.areSupported(tmpTable, tableDefinitionChanges)) {
                changes.addAll(tableDefinitionChanges);
            } else {
                // we need to recreate the table; for this to work we need to remove foreign keys to and from the table
                // however, we don't have to add them back here as there is a check for added foreign keys/indexes
                // later on anyways
                // we also don't have to drop indexes on the original table

                ForeignKey[] fks = intermediateTable.getForeignKeys();

                for (int fkIdx = 0; fkIdx < fks.length; fkIdx++) {
                    RemoveForeignKeyChange fkChange = new RemoveForeignKeyChange(intermediateTable, fks[fkIdx]);

                    changes.add(fkChange);
                    fkChange.apply(intermediateModel, _caseSensitive);
                }
                for (Schema schema : intermediateModel.getSchemas()) {
                    for (Table curTable : schema.getTables()) {

                        if (curTable != intermediateTable) {
                            ForeignKey[] curFks = curTable.getForeignKeys();

                            for (int fkIdx = 0; fkIdx < curFks.length; fkIdx++) {
                                if ((_caseSensitive && curFks[fkIdx].getForeignTableName().equals(intermediateTable.getName())) ||
                                    (!_caseSensitive && curFks[fkIdx].getForeignTableName().equalsIgnoreCase(intermediateTable
                                                                                                                 .getName()))) {
                                    RemoveForeignKeyChange fkChange = new RemoveForeignKeyChange(curTable, curFks[fkIdx]);

                                    changes.add(fkChange);
                                    fkChange.apply(intermediateModel, _caseSensitive);
                                }
                            }
                        }
                    }
                }

                RecreateTableChange tableChange = new RecreateTableChange(intermediateTable,
                                                                          new ArrayList(tableDefinitionChanges));

                changes.add(tableChange);
                tableChange.apply(intermediateModel, _caseSensitive);
            }
        }

        changes.addAll(checkForAddedIndexes(sourceModel, sourceTable, intermediateModel, intermediateTable, targetModel,
                                            targetTable));

        return changes;
    }

    /**
     * Returns the names of the columns in the intermediate table corresponding to the given column objects.
     *
     * @param columns           The column objects
     * @param intermediateTable The intermediate table
     * @return The column names
     */
    protected String[] getIntermediateColumnNamesFor(Column[] columns, Table intermediateTable) {
        String[] result = new String[columns.length];

        for (int idx = 0; idx < columns.length; idx++) {
            result[idx] = intermediateTable.findColumn(columns[idx].getName(), _caseSensitive).getName();
        }
        return result;
    }

    /**
     * Creates change objects for indexes that are present in the given source table but are no longer in the target
     * table, and applies them to the given intermediate model.
     *
     * @param sourceModel       The source model
     * @param sourceTable       The source table
     * @param intermediateModel The intermediate model to apply the changes to
     * @param intermediateTable The table from the intermediate model corresponding to the source table
     * @param targetModel       The target model
     * @param targetTable       The target table
     * @return The changes
     */
    protected List checkForRemovedIndexes(Database sourceModel,
                                          Table sourceTable,
                                          Database intermediateModel,
                                          Table intermediateTable,
                                          Database targetModel,
                                          Table targetTable) {
        List changes = new ArrayList();
        Index[] indexes = intermediateTable.getIndices();

        for (int indexIdx = 0; indexIdx < indexes.length; indexIdx++) {
            Index sourceIndex = indexes[indexIdx];
            Index targetIndex = findCorrespondingIndex(targetTable, sourceIndex);

            if (targetIndex == null) {
                if (_log.isInfoEnabled()) {
                    _log.info("Index " + sourceIndex.getName() + " needs to be removed from table " + intermediateTable.getName
                        ());
                }

                RemoveIndexChange change = new RemoveIndexChange(intermediateTable, sourceIndex);

                changes.add(change);
                change.apply(intermediateModel, _caseSensitive);
            }
        }
        return changes;
    }

    /**
     * Creates change objects for indexes that are not present in the given source table but are in the target
     * table, and applies them to the given intermediate model.
     *
     * @param sourceModel       The source model
     * @param sourceTable       The source table
     * @param intermediateModel The intermediate model to apply the changes to
     * @param intermediateTable The table from the intermediate model corresponding to the source table
     * @param targetModel       The target model
     * @param targetTable       The target table
     * @return The changes
     */
    protected List checkForAddedIndexes(Database sourceModel,
                                        Table sourceTable,
                                        Database intermediateModel,
                                        Table intermediateTable,
                                        Database targetModel,
                                        Table targetTable) {
        List changes = new ArrayList();

        for (int indexIdx = 0; indexIdx < targetTable.getIndexCount(); indexIdx++) {
            Index targetIndex = targetTable.getIndex(indexIdx);
            Index intermediateIndex = findCorrespondingIndex(intermediateTable, targetIndex);
            Index sourceIndex = findCorrespondingIndex(sourceTable, targetIndex);

            if ((sourceIndex == null) && (intermediateIndex == null)) {
                if (_log.isInfoEnabled()) {
                    _log.info("Index " + targetIndex.getName() + " needs to be created for table " + intermediateTable.getName());
                }

                Index clonedIndex = _cloneHelper.clone(targetIndex, intermediateTable, _caseSensitive);
                AddIndexChange change = new AddIndexChange(intermediateTable, clonedIndex);

                changes.add(change);
                change.apply(intermediateModel, _caseSensitive);
            }
        }
        return changes;
    }

    /**
     * Checks for changes in the column order between the given source and target table, creates change objects for these and
     * applies them to the given intermediate model.
     *
     * @param sourceModel       The source model
     * @param sourceTable       The source table
     * @param intermediateModel The intermediate model to apply the changes to
     * @param intermediateTable The table from the intermediate model corresponding to the source table
     * @param targetModel       The target model
     * @param targetTable       The target table
     * @return The changes
     */
    protected List checkForChangeOfColumnOrder(Database sourceModel,
                                               Table sourceTable,
                                               Database intermediateModel,
                                               Table intermediateTable,
                                               Database targetModel,
                                               Table targetTable) {
        List changes = new ArrayList();
        List targetOrder = new ArrayList();
        int numChangedPKs = 0;

        for (int columnIdx = 0; columnIdx < targetTable.getColumnCount(); columnIdx++) {
            Column targetColumn = targetTable.getColumn(columnIdx);
            Column sourceColumn = intermediateTable.findColumn(targetColumn.getName(), _caseSensitive);

            if (sourceColumn != null) {
                targetOrder.add(sourceColumn);
            }
        }

        HashMap newPositions = new HashMap();

        for (int columnIdx = 0; columnIdx < intermediateTable.getColumnCount(); columnIdx++) {
            Column sourceColumn = intermediateTable.getColumn(columnIdx);
            int targetIdx = targetOrder.indexOf(sourceColumn);

            if ((targetIdx >= 0) && (targetIdx != columnIdx)) {
                newPositions.put(sourceColumn.getName(), new Integer(targetIdx));
                if (sourceColumn.isPrimaryKey()) {
                    numChangedPKs++;
                }
            }
        }

        if (!newPositions.isEmpty()) {
            ColumnOrderChange change = new ColumnOrderChange(intermediateTable, newPositions);

            change.apply(intermediateModel, _caseSensitive);
            if (numChangedPKs > 1) {
                // create pk change that only covers the order change
                // fortunately, the order change will have adjusted the pk order already
                changes.add(new PrimaryKeyChange(intermediateTable,
                                                 getIntermediateColumnNamesFor(intermediateTable.getPrimaryKeyColumns(),
                                                                               intermediateTable)));
            }
            changes.add(change);
        }
        return changes;
    }

    /**
     * Creates change objects for columns that are present in the given source table but are no longer in the target
     * table, and applies them to the given intermediate model.
     *
     * @param sourceModel       The source model
     * @param sourceTable       The source table
     * @param intermediateModel The intermediate model to apply the changes to
     * @param intermediateTable The table from the intermediate model corresponding to the source table
     * @param targetModel       The target model
     * @param targetTable       The target table
     * @return The changes
     */
    protected List checkForRemovedColumns(Database sourceModel,
                                          Table sourceTable,
                                          Database intermediateModel,
                                          Table intermediateTable,
                                          Database targetModel,
                                          Table targetTable) {
        // if the platform does not support dropping pk columns, then the pk handling above will
        // generate appropriate pk changes

        List changes = new ArrayList();
        Column[] columns = intermediateTable.getColumns();

        for (int columnIdx = 0; columnIdx < columns.length; columnIdx++) {
            Column sourceColumn = columns[columnIdx];
            Column targetColumn = targetTable.findColumn(sourceColumn.getName(), _caseSensitive);

            if (targetColumn == null) {
                if (_log.isInfoEnabled()) {
                    _log.info("Column " + sourceColumn.getName() + " needs to be removed from table " + intermediateTable
                        .getName());
                }

                RemoveColumnChange change = new RemoveColumnChange(intermediateTable, sourceColumn.getName());

                changes.add(change);
                change.apply(intermediateModel, _caseSensitive);
            }
        }
        return changes;
    }

    /**
     * Creates change objects for columns that are not present in the given source table but are in the target
     * table, and applies them to the given intermediate model.
     *
     * @param sourceModel       The source model
     * @param sourceTable       The source table
     * @param intermediateModel The intermediate model to apply the changes to
     * @param intermediateTable The table from the intermediate model corresponding to the source table
     * @param targetModel       The target model
     * @param targetTable       The target table
     * @return The changes
     */
    protected List checkForAddedColumns(Database sourceModel,
                                        Table sourceTable,
                                        Database intermediateModel,
                                        Table intermediateTable,
                                        Database targetModel,
                                        Table targetTable) {
        List changes = new ArrayList();

        for (int columnIdx = 0; columnIdx < targetTable.getColumnCount(); columnIdx++) {
            Column targetColumn = targetTable.getColumn(columnIdx);
            Column sourceColumn = intermediateTable.findColumn(targetColumn.getName(), _caseSensitive);

            if (sourceColumn == null) {
                String prevColumn = (columnIdx > 0 ? intermediateTable.getColumn(columnIdx - 1).getName() : null);
                String nextColumn = (columnIdx < intermediateTable.getColumnCount() ? intermediateTable.getColumn(columnIdx)
                                                                                                       .getName() : null);
                Column clonedColumn = _cloneHelper.clone(targetColumn, false);
                AddColumnChange change = new AddColumnChange(intermediateTable, clonedColumn, prevColumn, nextColumn);

                changes.add(change);
                change.apply(intermediateModel, _caseSensitive);
            }
        }
        return changes;
    }

    /**
     * Creates change objects for columns that have a different in the given source and target table, and applies them
     * to the given intermediate model.
     *
     * @param sourceModel       The source model
     * @param sourceTable       The source table
     * @param intermediateModel The intermediate model to apply the changes to
     * @param intermediateTable The table from the intermediate model corresponding to the source table
     * @param targetModel       The target model
     * @param targetTable       The target table
     * @return The changes
     */
    protected List checkForChangedColumns(Database sourceModel,
                                          Table sourceTable,
                                          Database intermediateModel,
                                          Table intermediateTable,
                                          Database targetModel,
                                          Table targetTable) {
        List changes = new ArrayList();

        for (int columnIdx = 0; columnIdx < targetTable.getColumnCount(); columnIdx++) {
            Column targetColumn = targetTable.getColumn(columnIdx);
            Column sourceColumn = intermediateTable.findColumn(targetColumn.getName(), _caseSensitive);

            if (sourceColumn != null) {
                ColumnDefinitionChange change = compareColumns(intermediateTable, sourceColumn, targetTable, targetColumn);

                if (change != null) {
                    changes.add(change);
                    change.apply(intermediateModel, _caseSensitive);
                }
            }
        }
        return changes;
    }

    /**
     * Creates change objects for primary key differences (primary key added/removed/changed), and applies them to the given
     * intermediate model.
     *
     * @param sourceModel       The source model
     * @param sourceTable       The source table
     * @param intermediateModel The intermediate model to apply the changes to
     * @param intermediateTable The table from the intermediate model corresponding to the source table
     * @param targetModel       The target model
     * @param targetTable       The target table
     * @return The changes
     */
    protected List checkForPrimaryKeyChanges(Database sourceModel,
                                             Table sourceTable,
                                             Database intermediateModel,
                                             Table intermediateTable,
                                             Database targetModel,
                                             Table targetTable) {
        List changes = new ArrayList();
        Column[] sourcePK = sourceTable.getPrimaryKeyColumns();
        Column[] curPK = intermediateTable.getPrimaryKeyColumns();
        Column[] targetPK = targetTable.getPrimaryKeyColumns();

        if ((curPK.length == 0) && (targetPK.length > 0)) {
            if (_log.isInfoEnabled()) {
                _log.info("A primary key needs to be added to the table " + intermediateTable.getName());
            }

            AddPrimaryKeyChange change = new AddPrimaryKeyChange(intermediateTable, getIntermediateColumnNamesFor(targetPK,
                                                                                                                  intermediateTable));

            changes.add(change);
            change.apply(intermediateModel, _caseSensitive);
        } else if ((targetPK.length == 0) && (curPK.length > 0)) {
            if (_log.isInfoEnabled()) {
                _log.info("The primary key needs to be removed from the table " + intermediateTable.getName());
            }

            RemovePrimaryKeyChange change = new RemovePrimaryKeyChange(intermediateTable);

            changes.add(change);
            change.apply(intermediateModel, _caseSensitive);
        } else {
            boolean changePK = false;

            if ((curPK.length != targetPK.length) || (!_canDropPrimaryKeyColumns && sourcePK.length > targetPK.length)) {
                changePK = true;
            } else if ((curPK.length > 0) && (targetPK.length > 0)) {
                for (int pkColumnIdx = 0; (pkColumnIdx < curPK.length) && !changePK; pkColumnIdx++) {
                    if (!StringUtilsExt.equals(curPK[pkColumnIdx].getName(), targetPK[pkColumnIdx].getName(), _caseSensitive)) {
                        changePK = true;
                    }
                }
            }
            if (changePK) {
                if (_log.isInfoEnabled()) {
                    _log.info("The primary key of table " + intermediateTable.getName() + " needs to be changed");
                }
                if (_generatePrimaryKeyChanges) {
                    PrimaryKeyChange change = new PrimaryKeyChange(intermediateTable,
                                                                   getIntermediateColumnNamesFor(targetPK, intermediateTable));

                    changes.add(change);
                    change.apply(intermediateModel, changePK);
                } else {
                    RemovePrimaryKeyChange removePKChange = new RemovePrimaryKeyChange(intermediateTable);
                    AddPrimaryKeyChange addPKChange = new AddPrimaryKeyChange(intermediateTable,
                                                                              getIntermediateColumnNamesFor(targetPK,
                                                                                                            intermediateTable));

                    changes.add(removePKChange);
                    changes.add(addPKChange);
                    removePKChange.apply(intermediateModel, _caseSensitive);
                    addPKChange.apply(intermediateModel, _caseSensitive);
                }
            }
        }
        return changes;
    }

    /**
     * Compares the two columns and returns the change necessary to create the second
     * column from the first one if they differe.
     *
     * @param sourceTable  The source table which contains the source column
     * @param sourceColumn The source column
     * @param targetTable  The target table which contains the target column
     * @param targetColumn The target column
     * @return The change or <code>null</code> if the columns are the same
     */
    protected ColumnDefinitionChange compareColumns(Table sourceTable,
                                                    Column sourceColumn,
                                                    Table targetTable,
                                                    Column targetColumn) {
        if (ColumnDefinitionChange.isChanged(getPlatformInfo(), sourceColumn, targetColumn)) {
            Column newColumnDef = _cloneHelper.clone(sourceColumn, true);
            int targetTypeCode = _platformInfo.getTargetJdbcType(targetColumn.getTypeCode());
            boolean sizeMatters = _platformInfo.hasSize(targetTypeCode);
            boolean scaleMatters = _platformInfo.hasPrecisionAndScale(targetTypeCode);

            newColumnDef.setTypeCode(targetColumn.getTypeCode());
            newColumnDef.setSize(sizeMatters || scaleMatters ? targetColumn.getSize() : null);
            newColumnDef.setAutoIncrement(targetColumn.isAutoIncrement());
            newColumnDef.setRequired(targetColumn.isRequired());
            newColumnDef.setDescription(targetColumn.getDescription());
            newColumnDef.setDefaultValue(targetColumn.getDefaultValue());
            return new ColumnDefinitionChange(sourceTable, sourceColumn.getName(), newColumnDef);
        } else {
            return null;
        }
    }

    /**
     * Searches in the given table for a corresponding foreign key. If the given key
     * has no name, then a foreign key to the same table with the same columns (but not
     * necessarily in the same order) is searched. If the given key has a name, then the
     * corresponding key also needs to have the same name, or no name at all, but not a
     * different one.
     *
     * @param table The table to search in
     * @param fk    The original foreign key
     * @return The corresponding foreign key if found
     */
    protected ForeignKey findCorrespondingForeignKey(Table table, ForeignKey fk) {
        for (int fkIdx = 0; fkIdx < table.getForeignKeyCount(); fkIdx++) {
            ForeignKey curFk = table.getForeignKey(fkIdx);

            if ((_caseSensitive && fk.equals(curFk)) ||
                (!_caseSensitive && fk.equalsIgnoreCase(curFk))) {
                return curFk;
            }
        }
        return null;
    }

    /**
     * Searches in the given table for a corresponding index. If the given index
     * has no name, then a index to the same table with the same columns in the
     * same order is searched. If the given index has a name, then the a corresponding
     * index also needs to have the same name, or no name at all, but not a different one.
     *
     * @param table The table to search in
     * @param index The original index
     * @return The corresponding index if found
     */
    protected Index findCorrespondingIndex(Table table, Index index) {
        for (int indexIdx = 0; indexIdx < table.getIndexCount(); indexIdx++) {
            Index curIndex = table.getIndex(indexIdx);

            if ((_caseSensitive && index.equals(curIndex)) ||
                (!_caseSensitive && index.equalsIgnoreCase(curIndex))) {
                return curIndex;
            }
        }
        return null;
    }
}
