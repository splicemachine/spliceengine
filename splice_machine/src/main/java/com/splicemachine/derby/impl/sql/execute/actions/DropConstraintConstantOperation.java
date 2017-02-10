/*
 * Copyright (c) 2012 - 2017 Splice Machine, Inc.
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

import com.splicemachine.derby.ddl.DDLChangeType;
import com.splicemachine.derby.impl.job.fk.FkJobSubmitter;
import com.splicemachine.derby.impl.store.access.SpliceTransactionManager;
import org.apache.log4j.Logger;

import com.splicemachine.db.catalog.UUID;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.reference.SQLState;
import com.splicemachine.db.iapi.sql.Activation;
import com.splicemachine.db.iapi.sql.StatementType;
import com.splicemachine.db.iapi.sql.conn.LanguageConnectionContext;
import com.splicemachine.db.iapi.sql.depend.DependencyManager;
import com.splicemachine.db.iapi.sql.dictionary.ConglomerateDescriptor;
import com.splicemachine.db.iapi.sql.dictionary.ConstraintDescriptor;
import com.splicemachine.db.iapi.sql.dictionary.ConstraintDescriptorList;
import com.splicemachine.db.iapi.sql.dictionary.DataDictionary;
import com.splicemachine.db.iapi.sql.dictionary.ForeignKeyConstraintDescriptor;
import com.splicemachine.db.iapi.sql.dictionary.ReferencedKeyConstraintDescriptor;
import com.splicemachine.db.iapi.sql.dictionary.SchemaDescriptor;
import com.splicemachine.db.iapi.sql.dictionary.TableDescriptor;
import com.splicemachine.db.iapi.sql.execute.ConstantAction;
import com.splicemachine.db.iapi.store.access.TransactionController;
import com.splicemachine.utils.SpliceLogUtils;

/**
 *	This class describes actions that are ALWAYS performed for a drop constraint at Execution time.
 */
public class DropConstraintConstantOperation extends ConstraintConstantOperation {

    private static final Logger LOG = Logger.getLogger(DropConstraintConstantOperation.class);

    private final boolean cascade;
	private final String constraintSchemaName;
    private final int verifyType;
    private long indexConglomerateId = -1;

	// CONSTRUCTORS

	/**
	 *	Make one of these puppies.
	 *
	 *  @param constraintName	Constraint name.
	 *  @param constraintSchemaName		the schema that constraint lives in.
	 *  @param tableName		Table name.
	 *	@param tableId			UUID of table.
	 *  @param tableSchemaName				the schema that table lives in.
	 *  @param indexAction		IndexConstantAction for constraint (if necessary)
	 *  @param behavior			the drop behavior (e.g. StatementType.DROP_CASCADE)
	 */
	public DropConstraintConstantOperation(
		               String				constraintName,
					   String				constraintSchemaName,
		               String				tableName,
					   UUID					tableId,
					   String				tableSchemaName,
					   ConstantAction indexAction,
					   int					behavior,
                       int                  verifyType) {
		super(constraintName, DataDictionary.DROP_CONSTRAINT, tableName, tableId, tableSchemaName, indexAction);
		this.cascade = (behavior == StatementType.DROP_CASCADE);
		this.constraintSchemaName = constraintSchemaName;
        this.verifyType = verifyType;
	}

    /**
     * Only available after {@link #executeConstantAction(Activation)}, get the
     * conglomerate ID associated with this constraint, or -1 if no index conglomerate
     * is associated.
     * @return the associated index conglomerate ID for this constraint, or -1 if no
     * index is associated.
     */
    public long getIndexConglomerateId() {
        return indexConglomerateId;
    }

    @Override
	public String toString() {
		if (constraintName == null)
			return "DROP PRIMARY KEY";
		String ss = constraintSchemaName == null ? schemaName : constraintSchemaName;
		return "DROP CONSTRAINT " + ss + "." + constraintName;
	}

	/**
	 *	This is the guts of the Execution-time logic for DROP CONSTRAINT.
	 *
	 *	@see ConstantAction#executeConstantAction
	 *
	 * @exception StandardException		Thrown on failure
	 */
    @Override
	public void executeConstantAction(Activation activation ) throws StandardException {
		SpliceLogUtils.trace(LOG, "executeConstantAction");
		ConstraintDescriptor		conDesc;
		TableDescriptor				td;

		LanguageConnectionContext lcc = activation.getLanguageConnectionContext();
		DataDictionary dd = lcc.getDataDictionary();
		DependencyManager dm = dd.getDependencyManager();
		TransactionController tc = lcc.getTransactionExecute();


		/*
		** Inform the data dictionary that we are about to write to it.
		** There are several calls to data dictionary "get" methods here
		** that might be done in "read" mode in the data dictionary, but
		** it seemed safer to do this whole operation in "write" mode.
		**
		** We tell the data dictionary we're done writing at the end of
		** the transaction.
		*/
		dd.startWriting(lcc);

		td = dd.getTableDescriptor(tableId);

		if (td == null)
		{
			throw StandardException.newException(SQLState.LANG_TABLE_NOT_FOUND_DURING_EXECUTION, tableName);
		}

		/* Table gets locked in AlterTableConstantAction */

		/*
		** If the schema descriptor is null, then
		** we must have just read ourselves in.  
		** So we will get the corresponding schema
		** descriptor from the data dictionary.
		*/

		SchemaDescriptor tdSd = td.getSchemaDescriptor();
		SchemaDescriptor constraintSd = 
			constraintSchemaName == null ? tdSd : dd.getSchemaDescriptor(constraintSchemaName, tc, true);


		/* Get the constraint descriptor for the index, along
		 * with an exclusive row lock on the row in sys.sysconstraints
		 * in order to ensure that no one else compiles against the
		 * index.
		 */
        if (constraintName == null)  // this means "alter table drop primary key"
            conDesc = dd.getConstraintDescriptors(td).getPrimaryKey();
        else
			conDesc = dd.getConstraintDescriptorByName(td, constraintSd, constraintName, true);

		// Error if constraint doesn't exist
		if (conDesc == null)
		{
			String errorName = constraintName == null ? "PRIMARY KEY" :
								(constraintSd.getSchemaName() + "."+ constraintName);

			throw StandardException.newException(SQLState.LANG_DROP_NON_EXISTENT_CONSTRAINT,
						errorName,
						td.getQualifiedName());
		}
        if (conDesc instanceof ReferencedKeyConstraintDescriptor) {
            this.indexConglomerateId = getIndexConglomerateId((ReferencedKeyConstraintDescriptor)conDesc, td, dd);
        }

        switch (verifyType) {
            case DataDictionary.UNIQUE_CONSTRAINT:
                if (conDesc.getConstraintType() != verifyType)
                    throw StandardException.newException(SQLState.LANG_DROP_CONSTRAINT_TYPE,
                            constraintName, "UNIQUE");
                break;

            case DataDictionary.CHECK_CONSTRAINT:
                if (conDesc.getConstraintType() != verifyType)
                    throw StandardException.newException(SQLState.LANG_DROP_CONSTRAINT_TYPE,
                            constraintName, "CHECK");
                break;

            case DataDictionary.FOREIGNKEY_CONSTRAINT:
            if( conDesc.getConstraintType() != verifyType)
                throw StandardException.newException(SQLState.LANG_DROP_CONSTRAINT_TYPE,
                                                     constraintName, "FOREIGN KEY");
                break;
        }

		boolean cascadeOnRefKey = (cascade && conDesc instanceof ReferencedKeyConstraintDescriptor);
		if (!cascadeOnRefKey)
		{
			dm.invalidateFor(conDesc, DependencyManager.DROP_CONSTRAINT, lcc);
		}

		/*
		** If we had a primary/unique key and it is drop cascade,
		** drop all the referencing keys now.  We MUST do this AFTER
		** dropping the referenced key because otherwise we would
		** be repeatedly changing the reference count of the referenced
		** key and generating unnecessary I/O.
		*/
		dropConstraint(conDesc, activation, lcc, !cascadeOnRefKey);

        if(conDesc.getConstraintType() == DataDictionary.FOREIGNKEY_CONSTRAINT) {
            ForeignKeyConstraintDescriptor d = (ForeignKeyConstraintDescriptor)conDesc;
            final ReferencedKeyConstraintDescriptor referencedConstraint = d.getReferencedConstraint();
            new FkJobSubmitter(dd, (SpliceTransactionManager) tc, referencedConstraint, conDesc, DDLChangeType.DROP_FOREIGN_KEY,lcc).submit();
        }


		if (cascadeOnRefKey) 
		{
			ForeignKeyConstraintDescriptor fkcd;
			ReferencedKeyConstraintDescriptor cd;
			ConstraintDescriptorList cdl;

			cd = (ReferencedKeyConstraintDescriptor)conDesc;
			cdl = cd.getForeignKeyConstraints(ReferencedKeyConstraintDescriptor.ALL);
			int cdlSize = cdl.size();

			for(int index = 0; index < cdlSize; index++)
			{
				fkcd = (ForeignKeyConstraintDescriptor) cdl.elementAt(index);
				dm.invalidateFor(fkcd, DependencyManager.DROP_CONSTRAINT, lcc);
				dropConstraint(fkcd, activation, lcc, true);
			}
	
			/*
			** We told dropConstraintAndIndex not to
			** remove our dependencies, so send an invalidate,
			** and drop the dependencies.
			*/
			dm.invalidateFor(conDesc, DependencyManager.DROP_CONSTRAINT, lcc);
			dm.clearDependencies(lcc, conDesc);
		}
	}

    /**
     * Find the index conglomerate identifier (aka, conglomerate number) for the given index constraint descriptor
     * in the given table descriptor.
     * @param indexConstraint the index constraint for which to find the index conglomerate id.
     * @param tableDescriptor the table descriptor in which the index conglomerate is expected
     *                        to reside.
     * @param dd data directory, used to look up conglomerates.
     * @return the index conglomerate number associated with the given index constraint or -1 if not found.
     * @throws StandardException
     */
    private long getIndexConglomerateId(ReferencedKeyConstraintDescriptor indexConstraint,
                                        TableDescriptor tableDescriptor, DataDictionary dd) throws StandardException {
        ConglomerateDescriptor indexConglomerate = indexConstraint.getIndexConglomerateDescriptor(dd);
        String indexName = indexConglomerate.getConglomerateName();
        if (indexName != null) {
            for (ConglomerateDescriptor conglomerateDescriptor : tableDescriptor.getConglomerateDescriptorList()) {
                if (conglomerateDescriptor.isIndex() && indexName.equals(conglomerateDescriptor.getConglomerateName())) {
                    return conglomerateDescriptor.getConglomerateNumber();
                }
            }
        }
        return -1;
    }

	public String getScopeName() {
		return String.format("Drop Constraint %s (Table %s)", constraintName, tableName);
	}

}
