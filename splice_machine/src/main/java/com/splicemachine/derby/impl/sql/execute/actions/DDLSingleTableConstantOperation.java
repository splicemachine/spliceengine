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

import com.splicemachine.db.catalog.UUID;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.services.sanity.SanityManager;
import com.splicemachine.db.iapi.sql.Activation;
import com.splicemachine.db.iapi.sql.conn.LanguageConnectionContext;
import com.splicemachine.db.iapi.sql.dictionary.*;
import com.splicemachine.db.iapi.sql.execute.ConstantAction;
import com.splicemachine.db.iapi.store.access.ConglomerateController;
import com.splicemachine.db.iapi.store.access.TransactionController;
import com.splicemachine.db.impl.services.uuid.BasicUUID;
import com.splicemachine.ddl.DDLMessage;
import com.splicemachine.derby.ddl.DDLChangeType;
import com.splicemachine.derby.ddl.DDLUtils;
import com.splicemachine.derby.impl.job.fk.FkJobSubmitter;
import com.splicemachine.derby.impl.store.access.SpliceTransactionManager;
import com.splicemachine.protobuf.ProtoUtil;
import com.splicemachine.si.api.txn.Txn;
import com.splicemachine.si.api.txn.TxnView;
import com.splicemachine.utils.SpliceLogUtils;
import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public abstract class DDLSingleTableConstantOperation extends DDLConstantOperation {
	private static final Logger LOG = Logger.getLogger(DDLSingleTableConstantOperation.class);
	protected UUID tableId;

	DDLSingleTableConstantOperation(){}
	/**
	 * constructor
	 *
	 * @param tableId the target table
	 */
	DDLSingleTableConstantOperation(UUID tableId) {
		super();
		this.tableId = tableId;
	}

	/**
	 * Drop the constraint corresponding to the received descriptor.
	 * If in doing so we also drop a backing conglomerate that is
	 * shared by other constraints/indexes, then we have to create
	 * a new conglomerate to fill the gap.
	 *
	 * This method exists here as a "utility" method for the various
	 * constant actions that may drop constraints in one way or
	 * another (there are several that do).
	 *
	 * @param consDesc ConstraintDescriptor for the constraint to drop
	 * @param activation Activation used when creating a new backing
	 *  index (if a new backing index is needed)
	 * @param lcc LanguageConnectionContext used for dropping
	 * @param clearDeps Whether or not to clear dependencies when
	 *   dropping the constraint
	 */
	void dropConstraint(ConstraintDescriptor consDesc,Activation activation, LanguageConnectionContext lcc,
		boolean clearDeps) throws StandardException {
		SpliceLogUtils.trace(LOG, "dropConstraint %s", consDesc);
		dropConstraint(consDesc, null, null, activation, lcc, clearDeps);
	}

	/**
	 * See "dropConstraint(...") above.
	 *
	 * @param skipCreate Optional TableDescriptor.  If non-null
	 *  then we will skip the "create new conglomerate" processing
	 *  *IF* the constraint that we drop came from the table
	 *  described by skipCreate.
	 */
	void dropConstraint(ConstraintDescriptor consDesc,TableDescriptor skipCreate, Activation activation,
		LanguageConnectionContext lcc, boolean clearDeps) throws StandardException {
		SpliceLogUtils.trace(LOG, "dropConstraint %s",consDesc);
		dropConstraint(consDesc, skipCreate,null, activation, lcc, clearDeps);
	}

	/**
	 * See "dropConstraint(...") above.
	 *
	 * @param newConglomActions Optional List.  If non-null then
	 *  for each ConglomerateDescriptor for which we skip the
	 *  "create new conglomerate" processing we will add a
	 *  ConstantAction to this list.  The constant action can
	 *  then be executed later (esp. by the caller) to create the
	 *  new conglomerate, if needed.  If this argument is null and
	 *  we skip creation of a new conglomerate, the new conglomerate
	 *  is effectively ignored (which may be fine in some cases--
	 *  ex. when dropping a table).
	 */
	void dropConstraint(ConstraintDescriptor consDesc,TableDescriptor skipCreate, List<ConstantAction> newConglomActions,
		Activation activation, LanguageConnectionContext lcc, boolean clearDeps) throws StandardException {
		SpliceLogUtils.trace(LOG, "dropConstraint %s",consDesc);
		/* Get the properties on the old backing conglomerate before
		 * dropping the constraint, since we can't get them later if
		 * dropping the constraint causes us to drop the backing
		 * conglomerate.
		 */
		Properties ixProps = null;
		if (consDesc instanceof KeyConstraintDescriptor) {
			ixProps = new Properties();
			loadIndexProperties(lcc, ((KeyConstraintDescriptor)consDesc)
					.getIndexConglomerateDescriptor(lcc.getDataDictionary()),ixProps);
		}

		ConglomerateDescriptor newBackingConglomCD = consDesc.drop(lcc, clearDeps);

        if (consDesc.getConstraintType() == DataDictionary.FOREIGNKEY_CONSTRAINT) {
            DataDictionary dd = lcc.getDataDictionary();
            ForeignKeyConstraintDescriptor d = (ForeignKeyConstraintDescriptor) consDesc;
            TransactionController tc = lcc.getTransactionExecute();
            final ReferencedKeyConstraintDescriptor referencedConstraint = d.getReferencedConstraint();
            new FkJobSubmitter(dd, (SpliceTransactionManager) tc, referencedConstraint, consDesc, DDLChangeType.DROP_FOREIGN_KEY,lcc).submit();
        }

        if (consDesc.getConstraintType() == DataDictionary.UNIQUE_CONSTRAINT) {
            DataDictionary dd = lcc.getDataDictionary();
            ReferencedKeyConstraintDescriptor keyConsDesc = (ReferencedKeyConstraintDescriptor)consDesc;
            ConglomerateDescriptor indexConglom = keyConsDesc.getIndexConglomerateDescriptor(dd);
            String indexName = indexConglom.getObjectName();
            TableDescriptor td = consDesc.getTableDescriptor();
            String schemaName = td.getSchemaName();
            dropIndex(td, indexConglom, (SpliceTransactionManager)lcc.getTransactionExecute(),lcc, schemaName, indexName);
        }
		/* If we don't need a new conglomerate then there's nothing
		 * else to do.
		 */
		if (newBackingConglomCD == null)
			return;

		/* Only create the new conglomerate if it is NOT for the table
		 * described by skipCreate.
		 */
		if ((skipCreate != null) && skipCreate.getUUID().equals(
				consDesc.getTableDescriptor().getUUID())) {
			/* We're skipping the "create new conglom" phase; if we have
			 * a list in which to store the ConstantAction, then store it;
			 * otherwise, the new conglomerate is effectively ignored.
			 */
			if (newConglomActions != null) {
				newConglomActions.add(getConglomReplacementAction(newBackingConglomCD,
						consDesc.getTableDescriptor(), ixProps));
			}
		}
		else {
			executeConglomReplacement(
				getConglomReplacementAction(newBackingConglomCD,
					consDesc.getTableDescriptor(), ixProps),
				activation);
		}
	}

	/**
	 * Similar to dropConstraint(...) above, except this method
	 * drops a conglomerate directly instead of going through
	 * a ConstraintDescriptor.
	 *
	 * @param congDesc ConglomerateDescriptor for the conglom to drop
	 * @param td TableDescriptor for the table on which congDesc exists
	 * @param activation Activation used when creating a new backing
	 *  index (if a new backing index is needed)
	 * @param lcc LanguageConnectionContext used for dropping
	 */
	protected void dropConglomerate(ConglomerateDescriptor congDesc, TableDescriptor td,
																	Activation activation, LanguageConnectionContext lcc) throws StandardException {
		SpliceLogUtils.trace(LOG, "dropConglomerate %s with table descriptor %s",congDesc, td);
		dropConglomerate(congDesc, td, false, null, activation, lcc);
	}

	/**
	 * See "dropConglomerate(...)" above.
	 *	
	 * @param skipCreate If true then we will skip the "create
	 *  new conglomerate" processing for the dropped conglom.
	 * @param newConglomActions Optional List.  If non-null then
	 *  for each ConglomerateDescriptor for which we skip the
	 *  "create new conglomerate" processing we will add a
	 *  ConstantAction to this list.  The constant action can
	 *  then be executed later (esp. by the caller) to create the
	 *  new conglomerate, if needed.  If this argument is null and
	 *  we skip creation of a new conglomerate, the new conglomerate
	 *  is effectively ignored (which may be fine in some cases--
	 *  ex. when dropping a table).
	 */
	void dropConglomerate(ConglomerateDescriptor congDesc, TableDescriptor td,boolean skipCreate, 
			List<ConstantAction> newConglomActions, Activation activation, LanguageConnectionContext lcc) throws StandardException {
		SpliceLogUtils.trace(LOG, "dropConglomerate %s with table descriptor %s",congDesc, td);
		// Get the properties on the old index before dropping.
		Properties ixProps = new Properties();
		loadIndexProperties(lcc, congDesc, ixProps);
		// Drop the conglomerate.
		ConglomerateDescriptor newBackingConglomCD = congDesc.drop(lcc, td);

		/* If we don't need a new conglomerate then there's nothing
		 * else to do.
		 */
		if (newBackingConglomCD == null)
			return;

		if (skipCreate) {
			/* We're skipping the "create new conglom" phase; if we have
			 * a list in which to store the ConstantAction, then store it;
			 * otherwise, the new conglomerate is effectively ignored.
			 */
			if (newConglomActions != null) {
				newConglomActions.add(getConglomReplacementAction(newBackingConglomCD, td, ixProps));
			}
		}
		else {
			executeConglomReplacement(getConglomReplacementAction(newBackingConglomCD, td, ixProps), activation);
		}
	}
    
    /**
     * Recreate backing index of unique constraint.
     *
     * It first drops the existing index and creates it again with 
     * uniqueness set to false and uniqueWhenNotNull set to true. It reuses
     * the uuid so there is no need to update ConstraintDescriptor.
     *
     * @param cd            ConglomerateDescritor to recreate
     * @param td            TableDescriptor for table on which congDesc exists
     * @param activation    Activation used when creating a new backing index 
     *                      (if a new backing index is needed)
     * @param lcc           LanguageConnectionContext used for dropping
     *
     * @throws StandardException
     */
    void recreateUniqueConstraintBackingIndexAsUniqueWhenNotNull(
    ConglomerateDescriptor cd, TableDescriptor td,
    Activation activation, LanguageConnectionContext lcc) throws StandardException  {
		SpliceLogUtils.trace(LOG, "recreateUniqueConstraintBackingIndexAsUniqueWhenNotNull %s with table descriptor %s",cd, td);
        //get index property
        Properties prop = new Properties ();
        loadIndexProperties(lcc, cd, prop);
        ArrayList<ConstantAction> list = new ArrayList<>();
        // drop the existing index.
        dropConglomerate(cd, td, false, list, activation, lcc);
        String [] cols = cd.getColumnNames();
        if (cols == null) {
            //column list wasn't stored in conglomerateDescriptor
            //fetch is from table descriptor
            int [] pos = cd.getIndexDescriptor().baseColumnPositions();
            cols       = new String [pos.length];
            for (int i = 0; i < cols.length; i++) {
                cols[i] = td.getColumnDescriptor(pos[i]).getColumnName();
            }
        }
        
        //create new index action
        CreateIndexConstantOperation action =
                new CreateIndexConstantOperation(
                        false,          // not part of create table 
                        false,          // not unique
                        true,           // create as unique when not null index
                        cd.getIndexDescriptor().indexType(), 
                        td.getSchemaName(), 
                        cd.getConglomerateName(), td.getName(), td.getUUID(),
                        cols,
                        cd.getIndexDescriptor().getIndexColumnTypes(),
                        cd.getIndexDescriptor().isAscending(),
                        true, cd.getUUID(), false, false, false,false,false,0,
                        null,null,null,null,null,null,null,
                        cd.getIndexDescriptor().getExprTexts(),
                        cd.getIndexDescriptor().getExprBytecode(),
                        cd.getIndexDescriptor().getGeneratedClassNames(),
                        prop);

        //create index
        action.executeConstantAction(activation);
    }

	/**
	 * Get any table properties that exist for the received
	 * index descriptor.
	 */
	private void loadIndexProperties(LanguageConnectionContext lcc,
		ConglomerateDescriptor congDesc, Properties ixProps) throws StandardException {
		SpliceLogUtils.trace(LOG, "loadIndexProperties %s",congDesc);
	   	ConglomerateController cc = 
		   	lcc.getTransactionExecute().openConglomerate(
			   	congDesc.getConglomerateNumber(),
			   	false,
			   	TransactionController.OPENMODE_FORUPDATE,
			   	TransactionController.MODE_TABLE,
			   	TransactionController.ISOLATION_SERIALIZABLE);
		cc.getInternalTablePropertySet(ixProps);
		cc.close();
	}

	/**
	 * Create a ConstantAction which, when executed, will create a
	 * new conglomerate whose attributes match those of the received
	 * ConglomerateDescriptor.
	 *
	 * @param srcCD Descriptor describing what the replacement
	 *   physical conglomerate should look like
	 * @param td Table descriptor for the table to which srcCD belongs
	 * @param properties Properties from the old (dropped) conglom
	 *  that should be "forwarded" to the new (replacement) conglom.
	 */
	ConstantAction getConglomReplacementAction(ConglomerateDescriptor srcCD,
		TableDescriptor td, Properties properties) throws StandardException {
		SpliceLogUtils.trace(LOG, "getConglomReplacementAction %s",srcCD);
		/* Re-use CreateIndexActionConstantAction to do the work
		 * of creating a new conglomerate.  The big difference
		 * between creating an _index_ and creating an index
		 * _conglomerate_ is that we don't need to create a new
		 * ConglomerateDescriptor in the latter case.  Use of the
		 * following constructor dictates that we want to create
		 * a _conglomerate_ only--i.e. that no new conglomerate
		 * descriptor is necessary.
		 */
		return new CreateIndexConstantOperation(srcCD, td, properties);
	}

	/**
	 * Execute the received ConstantAction, which will create a
	 * new physical conglomerate (or find an existing physical
	 * conglomerate that is "sharable") to replace some dropped
	 * physical conglomerate.  Then find any conglomerate descriptors
	 * which still reference the dropped physical conglomerate and
	 * update them all to have a conglomerate number that points
	 * to the conglomerate created by the ConstantAction.
	 *
	 * This method is called as part of DROP processing to handle
	 * cases where a physical conglomerate that was shared by
	 * multiple descriptors is dropped--in which case a new physical
	 * conglomerate must be created to support the remaining
	 * descriptors.
	 *
	 * @param replaceConglom Constant action which, when executed,
	 *  will either create a new conglomerate or find an existing
	 *  one that satisfies the ConstantAction's requirements.
	 * @param activation Activation used when creating the conglom
	 */
	void executeConglomReplacement(ConstantAction replaceConglom,Activation activation) throws StandardException {
		SpliceLogUtils.trace(LOG, "executeConglomReplacement %s",replaceConglom);
		CreateIndexConstantOperation replaceConglomAction = (CreateIndexConstantOperation)replaceConglom;
		LanguageConnectionContext lcc = activation.getLanguageConnectionContext();
		DataDictionary dd = lcc.getDataDictionary();
		// Create the new (replacement) backing conglomerate...
		replaceConglomAction.executeConstantAction(activation);
		/* Find all conglomerate descriptors that referenced the
		 * old backing conglomerate and update them to have the
		 * conglomerate number for the new backing conglomerate.
		 */
		ConglomerateDescriptor [] congDescs =
			dd.getConglomerateDescriptors(replaceConglomAction.getReplacedConglomNumber());

		if (SanityManager.DEBUG) {
			/* There should be at least one descriptor requiring
			 * an updated conglomerate number--namely, the one
			 * corresponding to "srcCD" for which the constant
			 * action was created (see getConglomReplacementAction()
			 * above). There may be others, as well.
			 */
			if (congDescs.length < 1) {
				SanityManager.THROWASSERT(
					"Should have found at least one conglomerate " +
					"descriptor that needs an updated conglomerate " +
					"number (due to a dropped index), but only " +
					"found " + congDescs.length);
			}
		}

		dd.updateConglomerateDescriptor(congDescs,
			replaceConglomAction.getCreatedConglomNumber(),lcc.getTransactionExecute());
	}

    protected void dropIndex(TableDescriptor td, ConglomerateDescriptor conglomerateDescriptor,
                             SpliceTransactionManager userTxnManager, LanguageConnectionContext lcc,
                             String schemaName, String indexName) throws StandardException {
        final long tableConglomId = td.getHeapConglomerateId();
        final long indexConglomId = conglomerateDescriptor.getConglomerateNumber();
        TxnView uTxn = userTxnManager.getRawTransaction().getActiveStateTxn();
        //get the top-most transaction, that's the actual user transaction
        TransactionController tc = lcc.getTransactionExecute();
        TxnView t = uTxn;
        while(t.getTxnId()!= Txn.ROOT_TRANSACTION.getTxnId()){
            uTxn = t;
            t = uTxn.getParentTxnView();
        }
        final TxnView userTxn = uTxn;
        DDLMessage.DDLChange ddlChange = ProtoUtil.createDropIndex(indexConglomId, tableConglomId, userTxn.getTxnId(), (BasicUUID) tableId,schemaName,indexName);
        tc.prepareDataDictionaryChange(DDLUtils.notifyMetadataChange(ddlChange));
    }
}

