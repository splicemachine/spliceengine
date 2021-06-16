/*
 * Copyright (c) 2012 - 2020 Splice Machine, Inc.
 *
 * This file is part of Splice Machine.
 * Splice Machine is free software: you can redistribute it and/or modify it under the terms of the
 * GNU Affero General Public License as published by the Free Software Foundation, either
 * version 3, or (at your option) any later version.
 * Splice Machine is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU Affero General Public License for more details.
 * You should have received a copy of the GNU Affero General Public License along with Splice Machine.
 * If not, see <http://www.gnu.org/licenses/>.
 */

package com.splicemachine.derby.impl.sql.execute.actions;

import com.splicemachine.db.catalog.AliasInfo;
import com.splicemachine.db.catalog.Dependable;
import com.splicemachine.db.catalog.DependableFinder;
import com.splicemachine.db.catalog.UUID;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.reference.SQLState;
import com.splicemachine.db.iapi.sql.Activation;
import com.splicemachine.db.iapi.sql.PreparedStatement;
import com.splicemachine.db.iapi.sql.ResultSet;
import com.splicemachine.db.iapi.sql.StatementType;
import com.splicemachine.db.iapi.sql.conn.LanguageConnectionContext;
import com.splicemachine.db.iapi.sql.depend.DependencyManager;
import com.splicemachine.db.iapi.sql.depend.Dependent;
import com.splicemachine.db.iapi.sql.dictionary.*;
import com.splicemachine.db.iapi.sql.execute.ConstantAction;
import com.splicemachine.db.iapi.store.access.TransactionController;
import com.splicemachine.db.impl.services.uuid.BasicUUID;
import com.splicemachine.db.impl.sql.execute.GenericConstantActionFactory;
import com.splicemachine.db.impl.sql.execute.GenericExecutionFactory;
import com.splicemachine.ddl.DDLMessage;
import com.splicemachine.derby.impl.store.access.SpliceTransactionManager;
import com.splicemachine.protobuf.ProtoUtil;
import com.splicemachine.utils.SpliceLogUtils;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;
import splice.com.google.common.collect.Iterables;

import java.util.*;

/**
 *    This class  describes actions that are ALWAYS performed for a
 *    DROP SCHEMA Statement at Execution time.
 *
 */

public class DropSchemaConstantOperation extends DDLConstantOperation {
    private static final Logger LOG = LogManager.getLogger(DropSchemaConstantOperation.class);
    private final String schemaName;
    private final int dropBehavior;
    private final UUID dbId;
    /**
     *    Make the ConstantAction for a DROP SCHEMA statement.
     *
     * @param    schemaName            Schema name.
     * @param dbId
     *
     */
    public DropSchemaConstantOperation(UUID dbId, String schemaName, int dropBehavior) {
        this.dbId = dbId;
        this.schemaName = schemaName;
        this.dropBehavior = dropBehavior;
    }

    public    String    toString() {
        return "DROP SCHEMA " + schemaName + (dropBehavior== StatementType.DROP_CASCADE?" CASCADE" :" RESTRICT");
    }

    /**
     *    This is the guts of the Execution-time logic for DROP TABLE.
     *
     *    @see ConstantAction#executeConstantAction
     *
     * @exception StandardException        Thrown on failure
     */
    public void executeConstantAction( Activation activation ) throws StandardException {
        LanguageConnectionContext lcc = activation.getLanguageConnectionContext();
        DataDictionary dd = lcc.getDataDictionary();
        DependencyManager dm = dd.getDependencyManager();

        /*
         * Inform the data dictionary that we are about to write to it.
         * There are several calls to data dictionary "get" methods here
         * that might be done in "read" mode in the data dictionary, but
         * it seemed safer to do this whole operation in "write" mode.
         *
         * We tell the data dictionary we're done writing at the end of
         * the transaction.
         */
        dd.startWriting(lcc);
        SpliceTransactionManager tc = (SpliceTransactionManager)lcc.getTransactionExecute();
        SchemaDescriptor sd = dd.getSchemaDescriptor(dbId, schemaName, tc, true);

        // drop all objects in the schema first
        if (dropBehavior == StatementType.DROP_CASCADE) {
            dropAllSchemaObjects(sd, lcc, tc, activation);
        }

        /* Invalidate dependencies remotely */
        DDLMessage.DDLChange ddlChange = ProtoUtil.createDropSchema(
                tc.getActiveStateTxn().getTxnId(),
                (BasicUUID)sd.getDatabaseId(),
                schemaName,
                (BasicUUID)sd.getUUID());
        // Run locally first to capture any errors.
        dm.invalidateFor(sd, DependencyManager.DROP_SCHEMA, lcc);
        // Run Remotely
        notifyMetadataChange(tc, ddlChange);

        dd.dropAllSchemaPermDescriptors(sd.getObjectID(),tc);
        sd.drop(lcc, activation);
    }

    public String getScopeName() {
        return String.format("Drop Schema %s", schemaName);
    }

    private void dropAllSchemaObjects(SchemaDescriptor sd, LanguageConnectionContext lcc, TransactionController tc, Activation activation) throws StandardException {
        DataDictionary dd = lcc.getDataDictionary();
        String schemaId = sd.getUUID().toString();

        // We drop all schema objects in waves. First, we look at triggers, tables, and descriptors that depend on tables
        // and store each descriptor in a bucket so that each bucket contains descriptors that depend on the previous bucket
        // drop views, aliases and tables need to be considered together due to the dependencies among them
        // views could be defined on other views/tables/aliases, and aliases could be on tables/views
        // get all the table/view/alias and their dependents in the dependencyBucketing
        dropObjectsAndDependencies(
                Iterables.concat(dd.getTriggersInSchema(schemaId), dd.getTablesInSchema(sd)),
                sd, lcc, tc, activation);

        // Drop the remaining aliases
        dropObjectsAndDependencies(
                dd.getAliasesInSchema(schemaId),
                sd, lcc, tc, activation);

        // Drop remaining sequences
        dropObjectsAndDependencies(
                dd.getSequencesInSchema(schemaId),
                sd, lcc, tc, activation);

        // drop files
        ArrayList<FileInfoDescriptor> fileList = dd.getFilesInSchema(schemaId);
        for (FileInfoDescriptor fileDescriptor: fileList) {
            executeUpdate(lcc, String.format("CALL SQLJ.REMOVE_JAR('\"%s\".\"%s\"', 0)", sd.getSchemaName(), fileDescriptor.getDescriptorName()));
        }

    }

    private void dropBucket(TransactionController tc, Activation activation, Map<UUID, DDLConstantOperation> dropOperations, List<UUID> dropBucket) throws StandardException {
        // Construct grouped DDL notification for remote RS cache invalidation
        long txnId = ((SpliceTransactionManager) tc).getActiveStateTxn().getTxnId();
        // Send invalidation messages 1000 by 1000 to avoid exceeding the zookeeper limit
        for (int i = 0; i < dropBucket.size(); i += 1000) {
            List<DDLMessage.DDLChange> ddlChanges = new ArrayList<>();
            List<UUID> subBucket = dropBucket.subList(i, Math.min(dropBucket.size(), i + 1000));
            for (UUID uuid : subBucket) {
                ddlChanges.addAll(dropOperations.get(uuid).generateDDLChanges(txnId, activation));
            }
            notifyMetadataChanges(tc, ddlChanges);
        }

        // Drop everything
        for (UUID uuid: dropBucket) {
            dropOperations.get(uuid).executeConstantAction(activation, false);
        }
    }

    private void dropObjectsAndDependencies(
            Iterable<? extends TupleDescriptor> descriptors,
            SchemaDescriptor sd,
            LanguageConnectionContext lcc,
            TransactionController tc,
            Activation activation) throws StandardException {
        DataDictionary dd = lcc.getDataDictionary();
        DependencyBucketing<UUID> dependencyBucketing = new DependencyBucketing<>();
        Map<UUID, TupleDescriptor> dropMap = new HashMap<>();
        Map<UUID, DDLConstantOperation> dropOperations = new HashMap<>();
        for (TupleDescriptor td: descriptors) {
            if (!dropMap.containsKey(td.getUUID())) {
                getDependencies(td, sd, dependencyBucketing, dropMap, dd);
            }
        }
        for (TupleDescriptor tupleDescriptor: dropMap.values()) {
            if (!dropOperations.containsKey(tupleDescriptor.getUUID())) {
                ConstantAction action = getDropConstantAction(tupleDescriptor, sd, lcc, tc, activation);
                if (action != null) {
                    dropOperations.put(tupleDescriptor.getUUID(), (DDLConstantOperation) action);
                }
            }
        }

        // Drop independent tuple descriptors one wave at a time
        for (List<UUID> dropBucket : dependencyBucketing.getBuckets()) {
            dropBucket(tc, activation, dropOperations, dropBucket);
        }
    }

    private ConstantAction getDropConstantAction(TupleDescriptor tupleDescriptor, SchemaDescriptor sd, LanguageConnectionContext lcc, TransactionController tc, Activation activation) throws StandardException {
        DataDictionary dd = lcc.getDataDictionary();
        GenericExecutionFactory execFactory = (GenericExecutionFactory) lcc.getLanguageConnectionFactory().getExecutionFactory();
        GenericConstantActionFactory constantActionFactory = execFactory.getConstantActionFactory();

        // check the type of the descriptor
        if (tupleDescriptor instanceof TableDescriptor) {
            TableDescriptor td = (TableDescriptor)tupleDescriptor;
            switch (td.getTableType()) {
                case TableDescriptor.BASE_TABLE_TYPE:
                case TableDescriptor.EXTERNAL_TYPE:
                case TableDescriptor.LOCAL_TEMPORARY_TABLE_TYPE:
                    return constantActionFactory.getDropTableConstantAction(td.getQualifiedName(),
                            td.getName(),
                            td.getSchemaDescriptor(),
                            td.getHeapConglomerateId(),
                            td.getUUID(), StatementType.DROP_DEFAULT);
                case TableDescriptor.VIEW_TYPE:
                    return constantActionFactory.getDropViewConstantAction(td.getQualifiedName(),
                            td.getName(),
                            td.getSchemaDescriptor());
                case TableDescriptor.SYNONYM_TYPE:
                    return constantActionFactory.getDropAliasConstantAction(td.getSchemaDescriptor(),
                            td.getName(),
                            AliasInfo.ALIAS_NAME_SPACE_SYNONYM_AS_CHAR);
                default:
                    assert true : "we should not be dropping table types reaching here";
                    return null;
            }
        } else if (tupleDescriptor instanceof ViewDescriptor) {
            TableDescriptor viewTableDescriptor = dd.getTableDescriptor(((ViewDescriptor) tupleDescriptor).getObjectID());
            return constantActionFactory.getDropViewConstantAction(viewTableDescriptor.getQualifiedName(),
                    viewTableDescriptor.getName(),
                    viewTableDescriptor.getSchemaDescriptor());
        } else if (tupleDescriptor instanceof AliasDescriptor) {
            AliasDescriptor aliasDescriptor = (AliasDescriptor)tupleDescriptor;
            return constantActionFactory.getDropAliasConstantAction(sd,
                    aliasDescriptor.getName(),
                    aliasDescriptor.getNameSpace());
        } else if (tupleDescriptor instanceof SequenceDescriptor) {
            SequenceDescriptor sequenceDescriptor = (SequenceDescriptor) tupleDescriptor;
            return constantActionFactory.getDropSequenceConstantAction(sd,sequenceDescriptor.getDescriptorName());
        } else if (tupleDescriptor instanceof TriggerDescriptor) {
            TriggerDescriptor triggerDescriptor = (TriggerDescriptor)tupleDescriptor;
            return constantActionFactory.getDropTriggerConstantAction(sd,triggerDescriptor.getDescriptorName(), triggerDescriptor.getTableDescriptor().getUUID());
        } else {
            assert false:"should not reach here!";
            return null;
        }
    }

    private void walkDependencyTree(TupleDescriptor tupleDescriptor,
                                    SchemaDescriptor sd,
                                    DependencyBucketing<UUID> dependencyBucketing,
                                    Map<UUID, TupleDescriptor> dropMap,
                                    HashSet<UUID> ancestors,
                                    DataDictionary dd) throws StandardException {

        // check dependency in sys.sysdepends
        String providerID = null;
        if (tupleDescriptor instanceof TableDescriptor) {
            // TableDescriptor could be for a view or alias
            if (((TableDescriptor) tupleDescriptor).isSynonymDescriptor()) {
                // we need to get the alias UUID for dependency check
                AliasDescriptor aliasDescriptor = dd.getAliasDescriptor(sd.getUUID().toString(), ((TableDescriptor) tupleDescriptor).getName(), AliasInfo.ALIAS_TYPE_SYNONYM_AS_CHAR, null);
                if (aliasDescriptor != null) {
                    providerID = aliasDescriptor.getUUID().toString();
                }
            } else {
                providerID = tupleDescriptor.getUUID().toString();
            }
        } else if (tupleDescriptor instanceof ViewDescriptor ||
                tupleDescriptor instanceof AliasDescriptor ||
                tupleDescriptor instanceof TriggerDescriptor) {
            providerID = tupleDescriptor.getUUID().toString();
        } else {
            return;
        }

        List<DependencyDescriptor> providersDescriptorList = dd.getProvidersDescriptorList(providerID);
        List<Dependent> storedList = getDependencyDescriptorList(providersDescriptorList, dd);
        for (int j = 0; j < storedList.size(); j++) {
            TupleDescriptor dependentTupleDescriptor = (TupleDescriptor)storedList.get(j);
            // check whether the dependent objects are within the same schema
            checkSchema(sd, tupleDescriptor, dependentTupleDescriptor, dd);

            // only add table/view/alias to the pendingDropMap, the other dependencies are secondary and can be dropped
            // at the time the primary objects(table/view/alias) are dropped
            if (dependentTupleDescriptor instanceof TableDescriptor ||
                    dependentTupleDescriptor instanceof ViewDescriptor ||
                    dependentTupleDescriptor instanceof AliasDescriptor ||
                    dependentTupleDescriptor instanceof TriggerDescriptor) {
                // ignore self-dependency
                if (dependentTupleDescriptor.getUUID().equals(tupleDescriptor.getUUID()))
                    continue;

                // check cyclic dependency
                if (ancestors.contains(dependentTupleDescriptor.getUUID())) {
                    throw StandardException.newException(SQLState.LANG_CYCLIC_DEPENDENCY_DETECTED,
                            sd.getSchemaName(), tupleDescriptor.getDescriptorName(), dependentTupleDescriptor.getDescriptorName());
                }
                dependencyBucketing.addDependency(dependentTupleDescriptor.getUUID(), tupleDescriptor.getUUID());
                dropMap.putIfAbsent(dependentTupleDescriptor.getUUID(), dependentTupleDescriptor);
                ancestors.add(dependentTupleDescriptor.getUUID());
                walkDependencyTree(dependentTupleDescriptor, sd, dependencyBucketing, dropMap, ancestors, dd);
                ancestors.remove(dependentTupleDescriptor.getUUID());
            }
        }


        // check FK constraints for TableDescriptor
        if (tupleDescriptor instanceof TableDescriptor) {
            ConstraintDescriptorList cdl = dd.getConstraintDescriptors((TableDescriptor)tupleDescriptor);
            for (int i = 0; i < cdl.size(); i++) {
                ConstraintDescriptor cd = cdl.elementAt(i);
                if (cd instanceof ReferencedKeyConstraintDescriptor) {
                    // get the FK tables that depend on it
                    providersDescriptorList = dd.getProvidersDescriptorList(cd.getObjectID().toString());
                    storedList = getDependencyDescriptorList(providersDescriptorList, dd);
                    for (int j = 0; j < storedList.size(); j++) {
                        if (storedList.get(j) instanceof ForeignKeyConstraintDescriptor) {
                            // get the corresponding table and check if it is in the same schema
                            ForeignKeyConstraintDescriptor foreignKeyConstraintDescriptor = (ForeignKeyConstraintDescriptor) storedList.get(j);
                            TableDescriptor dependentTableDescriptor = foreignKeyConstraintDescriptor.getTableDescriptor();

                            // ignore self-dependency
                            if (dependentTableDescriptor.getUUID().equals(tupleDescriptor.getUUID()))
                                continue;

                            checkSchema(sd, tupleDescriptor, dependentTableDescriptor, dd);

                            // check cyclic dependency
                            if (ancestors.contains(dependentTableDescriptor.getUUID())) {
                                throw StandardException.newException(SQLState.LANG_CYCLIC_DEPENDENCY_DETECTED,
                                        sd.getSchemaName(), tupleDescriptor.getDescriptorName(), dependentTableDescriptor.getDescriptorName());
                            }
                            dependencyBucketing.addDependency(dependentTableDescriptor.getUUID(), tupleDescriptor.getUUID());
                            dropMap.putIfAbsent(dependentTableDescriptor.getUUID(), dependentTableDescriptor);
                            ancestors.add(dependentTableDescriptor.getUUID());
                            walkDependencyTree(dependentTableDescriptor, sd, dependencyBucketing, dropMap, ancestors, dd);
                            ancestors.remove(dependentTableDescriptor.getUUID());
                        }
                    }
                }
            }
        }
    }

    private void getDependencies(TupleDescriptor td, SchemaDescriptor sd, DependencyBucketing<UUID> dependencyBucketing, Map<UUID, TupleDescriptor> dropMap, DataDictionary dd) throws StandardException {
        // the dependency among the objects is supposed to be a DAG, we will walk through it using pre-order tree traversal from the root
        // keep a hashset of the path to the root, if during the traversal, we get a child which also appears as an ancestor on the path
        // to the root, then there is a cyclic dependency
        HashSet<UUID> ancestors = new HashSet<>();
        dependencyBucketing.addSingleNode(td.getUUID());
        dropMap.putIfAbsent(td.getUUID(), td);
        ancestors.add(td.getUUID());
        walkDependencyTree(td, sd, dependencyBucketing, dropMap, ancestors, dd);
        ancestors.remove(td.getUUID());
    }


    private List<Dependent> getDependencyDescriptorList(List<DependencyDescriptor> storedList,
                                                          DataDictionary dd) throws StandardException {
        List<Dependent> returnList = new ArrayList<>();
        if (storedList.isEmpty()) {
            return returnList;
        }

       /* borrowed some code from BaseDependentcyManger.getDependencyDescriptorList() */
        for (DependencyDescriptor aStoredList : storedList) {
            DependableFinder finder = aStoredList.getDependentFinder();
            Dependent tempD = (Dependent) finder.getDependable(dd, aStoredList.getUUID());
            returnList.add(tempD);
        }
        return returnList;
    }

    private void checkSchema(SchemaDescriptor sd,
                             TupleDescriptor providerDescriptor,
                             TupleDescriptor dependentDescriptor,
                             DataDictionary dd) throws StandardException {

        String objectType = ((Dependable)providerDescriptor).getClassType();

        if (dependentDescriptor instanceof TableDescriptor) {
            if (!((TableDescriptor)dependentDescriptor).getSchemaDescriptor().getUUID().equals(sd.getUUID())) {
                throw StandardException.newException(SQLState.LANG_PROVIDER_HAS_EXTERNAL_DEPENDENCY,
                        sd.getSchemaName(), objectType, providerDescriptor.getDescriptorName());
            }
        } else if (dependentDescriptor instanceof ViewDescriptor) {
            ViewDescriptor viewDescriptor = (ViewDescriptor)dependentDescriptor;
            TableDescriptor viewTdDescriptor = dd.getTableDescriptor(viewDescriptor.getObjectID());
            if (!viewTdDescriptor.getSchemaDescriptor().getUUID().equals(sd.getUUID())) {
                throw StandardException.newException(SQLState.LANG_PROVIDER_HAS_EXTERNAL_DEPENDENCY,
                        sd.getSchemaName(), objectType, providerDescriptor.getDescriptorName());
            }
        } else if (dependentDescriptor instanceof AliasDescriptor) {
            if (!((AliasDescriptor) dependentDescriptor).getSchemaDescriptor().getUUID().equals(sd.getUUID())) {
                throw StandardException.newException(SQLState.LANG_PROVIDER_HAS_EXTERNAL_DEPENDENCY,
                        sd.getSchemaName(), objectType, providerDescriptor.getDescriptorName());
            }
        }
    }

    protected static void executeUpdate(LanguageConnectionContext lcc, String dropStmt) throws StandardException {
        SpliceLogUtils.trace(LOG, "executeUpdate with statement {%s}", dropStmt);
        PreparedStatement ps = lcc.prepareInternalStatement(dropStmt);

        ResultSet rs = ps.executeSubStatement(lcc, true, 0L);
        rs.close();
    }

}
