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
import com.splicemachine.derby.ddl.DDLUtils;
import com.splicemachine.derby.impl.store.access.SpliceTransactionManager;
import com.splicemachine.protobuf.ProtoUtil;
import com.splicemachine.utils.SpliceLogUtils;
import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

/**
 *    This class  describes actions that are ALWAYS performed for a
 *    DROP SCHEMA Statement at Execution time.
 *
 */

public class DropSchemaConstantOperation extends DDLConstantOperation {
    private static final Logger LOG = Logger.getLogger(DropSchemaConstantOperation.class);
    private final String schemaName;
    private final int dropBehavior;
    private final UUID dbId;
    /**
     *    Make the ConstantAction for a DROP SCHEMA statement.
     *
     *    @param    schemaName            Schema name.
     *
     */
    public DropSchemaConstantOperation(String schemaName, int dropBehavior) {
        this(null, schemaName, dropBehavior);
    }

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
        tc.prepareDataDictionaryChange(DDLUtils.notifyMetadataChange(ddlChange));

        dd.dropAllSchemaPermDescriptors(sd.getObjectID(),tc);
        sd.drop(lcc, activation);
    }

    public String getScopeName() {
        return String.format("Drop Schema %s", schemaName);
    }

    private void dropAllSchemaObjects(SchemaDescriptor sd, LanguageConnectionContext lcc, TransactionController tc, Activation activation) throws StandardException {
        DataDictionary dd = lcc.getDataDictionary();

        // drop views, aliases and tables need to be considered together due to the dependencies among them
        // views could be defined on other views/tables/aliases, and aliases could be on tables/views

        //get all the table/view/alias
        ArrayList<TupleDescriptor> tableList = dd.getTablesInSchema(sd);

        // get all the table/view/alias and their dependents in the pendingDropList
        ArrayList<TupleDescriptor> pendingDropList = new ArrayList<>();
        HashSet<UUID> droppedList = new HashSet<>();
        for (TupleDescriptor td: tableList) {
            getDependenciesForTable(td, sd, pendingDropList, dd);
        }

        // drop the objects in the pendingDropList in the reverse order
        for (int i=pendingDropList.size()-1; i>=0; i--) {
            TupleDescriptor tupleDescriptor = pendingDropList.get(i);
            // drop each table if it hasn't been dropped
            if (tupleDescriptor.getUUID() != null) {
                if (!droppedList.contains(tupleDescriptor.getUUID())) {
                    dropObject(tupleDescriptor, sd, lcc, tc, activation);
                    droppedList.add(tupleDescriptor.getUUID());
                }
            }
        }

        // drop aliases - we still need to look at alias for other objects like procedures, functions
        ArrayList<AliasDescriptor> aliasList = dd.getAliasesInSchema(sd.getUUID().toString());
        for (AliasDescriptor aliasDescriptor: aliasList) {
            // we'are done with aliases for table/view already, so no need to check here
            if (aliasDescriptor.getAliasType() != AliasInfo.ALIAS_NAME_SPACE_SYNONYM_AS_CHAR)
                dropObject(aliasDescriptor, sd, lcc, tc, activation);
        }

        // drop sequences
        ArrayList<SequenceDescriptor> sequenceList = dd.getSequencesInSchema(sd.getUUID().toString());
        for (SequenceDescriptor sequenceDescriptor: sequenceList) {
            dropObject(sequenceDescriptor, sd, lcc, tc, activation);
        }

        // drop triggers
        ArrayList<TriggerDescriptor> triggerList = dd.getTriggersInSchema(sd.getUUID().toString());
        for (TriggerDescriptor triggerDescriptor: triggerList) {
            dropObject(triggerDescriptor, sd, lcc, tc, activation);
        }

        // drop files
        ArrayList<FileInfoDescriptor> fileList = dd.getFilesInSchema(sd.getUUID().toString());
        for (FileInfoDescriptor fileDescriptor: fileList) {
            executeUpdate(lcc, String.format("CALL SQLJ.REMOVE_JAR('\"%s\".\"%s\"', 0)", sd.getSchemaName(), fileDescriptor.getDescriptorName()));
        }

    }

    private void dropObject(TupleDescriptor tupleDescriptor, SchemaDescriptor sd, LanguageConnectionContext lcc, TransactionController tc, Activation activation) throws StandardException {
        DataDictionary dd = lcc.getDataDictionary();
        GenericExecutionFactory execFactory = (GenericExecutionFactory) lcc.getLanguageConnectionFactory().getExecutionFactory();
        GenericConstantActionFactory constantActionFactory = execFactory.getConstantActionFactory();
        ConstantAction action;

        // check the type of the descriptor
        if (tupleDescriptor instanceof TableDescriptor) {
            TableDescriptor td = (TableDescriptor)tupleDescriptor;
            switch (td.getTableType()) {
                case TableDescriptor.BASE_TABLE_TYPE:
                case TableDescriptor.EXTERNAL_TYPE:
                case TableDescriptor.LOCAL_TEMPORARY_TABLE_TYPE:
                    action = constantActionFactory.getDropTableConstantAction(td.getQualifiedName(),
                            td.getName(),
                            td.getSchemaDescriptor(),
                            td.getHeapConglomerateId(),
                            td.getUUID(), StatementType.DROP_DEFAULT);
                    action.executeConstantAction(activation);
                    break;
                case TableDescriptor.VIEW_TYPE:
                    action = constantActionFactory.getDropViewConstantAction(td.getQualifiedName(),
                            td.getName(),
                            td.getSchemaDescriptor());
                    action.executeConstantAction(activation);
                    break;
                case TableDescriptor.SYNONYM_TYPE:
                    action = constantActionFactory.getDropAliasConstantAction(td.getSchemaDescriptor(),
                            td.getName(),
                            AliasInfo.ALIAS_NAME_SPACE_SYNONYM_AS_CHAR);
                    action.executeConstantAction(activation);
                    break;
                default:
                    assert true : "we should not be dropping table types reaching here";
            }
        } else if (tupleDescriptor instanceof ViewDescriptor) {
            TableDescriptor viewTableDescriptor = dd.getTableDescriptor(((ViewDescriptor) tupleDescriptor).getObjectID());
            action = constantActionFactory.getDropViewConstantAction(viewTableDescriptor.getQualifiedName(),
                    viewTableDescriptor.getName(),
                    viewTableDescriptor.getSchemaDescriptor());
            action.executeConstantAction(activation);
        } else if (tupleDescriptor instanceof AliasDescriptor) {
            AliasDescriptor aliasDescriptor = (AliasDescriptor)tupleDescriptor;
            action = constantActionFactory.getDropAliasConstantAction(sd,
                    aliasDescriptor.getName(),
                    aliasDescriptor.getNameSpace());
            action.executeConstantAction(activation);
        } else if (tupleDescriptor instanceof SequenceDescriptor) {
            SequenceDescriptor sequenceDescriptor = (SequenceDescriptor) tupleDescriptor;
            action = constantActionFactory.getDropSequenceConstantAction(sd,sequenceDescriptor.getDescriptorName());
            action.executeConstantAction(activation);
        } else if (tupleDescriptor instanceof TriggerDescriptor) {
            TriggerDescriptor triggerDescriptor = (TriggerDescriptor)tupleDescriptor;
            action = constantActionFactory.getDropTriggerConstantAction(sd,triggerDescriptor.getDescriptorName(), triggerDescriptor.getTableDescriptor().getUUID());
            action.executeConstantAction(activation);
        } else {
            assert false:"should not reach here!";
        }
    }

    private void walkDependencyTree(TupleDescriptor tupleDescriptor,
                                    SchemaDescriptor sd,
                                    ArrayList<TupleDescriptor> pendingDropList,
                                    HashSet<UUID> ancestors,
                                    DataDictionary dd) throws StandardException {

        // check dependency in sys.sysdepends
        String providerID = null;
        if (tupleDescriptor instanceof TableDescriptor) {
            // TableDescriptor could be for a view or alias
            if (((TableDescriptor) tupleDescriptor).getTableType() == TableDescriptor.SYNONYM_TYPE) {
                // we need to get the alias UUID for dependency check
                AliasDescriptor aliasDescriptor = dd.getAliasDescriptor(sd.getUUID().toString(), ((TableDescriptor) tupleDescriptor).getName(), AliasInfo.ALIAS_TYPE_SYNONYM_AS_CHAR);
                if (aliasDescriptor != null)
                    providerID = aliasDescriptor.getUUID().toString();
            } else {
                providerID = tupleDescriptor.getUUID().toString();
            }
        } else if (tupleDescriptor instanceof ViewDescriptor) {
            providerID = tupleDescriptor.getUUID().toString();
        } else if (tupleDescriptor instanceof AliasDescriptor) {
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

            // only add table/view/alias to the pendingDropList, the other dependencies are secondary and can be dropped
            // at the time the primary objects(table/view/alias) are dropped
            if (dependentTupleDescriptor instanceof TableDescriptor ||
                    dependentTupleDescriptor instanceof ViewDescriptor ||
                    dependentTupleDescriptor instanceof AliasDescriptor) {
                // ignore self-dependency
                if (dependentTupleDescriptor.getUUID().equals(tupleDescriptor.getUUID()))
                    continue;

                // check cyclic dependency
                if (ancestors.contains(dependentTupleDescriptor.getUUID())) {
                    throw StandardException.newException(SQLState.LANG_CYCLIC_DEPENDENCY_DETECTED,
                            sd.getSchemaName(), tupleDescriptor.getDescriptorName(), dependentTupleDescriptor.getDescriptorName());
                }
                pendingDropList.add(dependentTupleDescriptor);
                ancestors.add(dependentTupleDescriptor.getUUID());
                walkDependencyTree(dependentTupleDescriptor, sd, pendingDropList, ancestors, dd);
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
                            pendingDropList.add(dependentTableDescriptor);
                            ancestors.add(dependentTableDescriptor.getUUID());
                            walkDependencyTree(dependentTableDescriptor, sd, pendingDropList, ancestors, dd);
                            ancestors.remove(dependentTableDescriptor.getUUID());
                        }
                    }
                }
            }
        }
    }

    private void getDependenciesForTable(TupleDescriptor td, SchemaDescriptor sd, ArrayList<TupleDescriptor> pendingDropList, DataDictionary dd) throws StandardException {
        // the dependency among the objects is supposed to be a DAG, we will walk through it using pre-order tree traversal from the root
        // keep a hashset of the path to the root, if during the traversal, we get a child which also appears as an ancestor on the path
        // to the root, then there is a cyclic dependency
        HashSet<UUID> ancestors = new HashSet<>();
        pendingDropList.add(td);
        ancestors.add(td.getUUID());
        walkDependencyTree(td, sd, pendingDropList, ancestors, dd);
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
