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

import java.util.Enumeration;
import java.util.Hashtable;
import com.splicemachine.db.catalog.UUID;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.services.io.FormatableBitSet;
import com.splicemachine.db.iapi.services.sanity.SanityManager;
import com.splicemachine.db.iapi.sql.Activation;
import com.splicemachine.db.iapi.sql.conn.LanguageConnectionContext;
import com.splicemachine.db.iapi.sql.depend.DependencyManager;
import com.splicemachine.db.iapi.sql.dictionary.CheckConstraintDescriptor;
import com.splicemachine.db.iapi.sql.dictionary.ConstraintDescriptor;
import com.splicemachine.db.iapi.sql.dictionary.ConstraintDescriptorList;
import com.splicemachine.db.iapi.sql.dictionary.DataDictionary;
import com.splicemachine.db.iapi.sql.dictionary.ForeignKeyConstraintDescriptor;
import com.splicemachine.db.iapi.sql.dictionary.IndexRowGenerator;
import com.splicemachine.db.iapi.sql.dictionary.ReferencedKeyConstraintDescriptor;
import com.splicemachine.db.iapi.sql.dictionary.TableDescriptor;
import com.splicemachine.db.iapi.sql.execute.ConstantAction;
import com.splicemachine.db.iapi.sql.execute.ExecIndexRow;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.iapi.store.access.ConglomerateController;
import com.splicemachine.db.iapi.store.access.TransactionController;
import com.splicemachine.db.iapi.types.RowLocation;

public class SetConstraintsConstantOperation extends DDLConstantOperation {
	private boolean enable;
	private	boolean	unconditionallyEnforce;
	/*
	** For the following fields, never access directly, always
	** get the constraint descript list via the private
	** method getConstraintDescriptorList() defined herein.
	*/
	private ConstraintDescriptorList cdl;
	private UUID[] cuuids;
	private UUID[] tuuids;

	// CONSTRUCTORS
	/**
	 *Boilerplate
	 *
	 * @param cdl						ConstraintDescriptorList
	 * @param enable					true == turn them on, false == turn them off
	 * @param unconditionallyEnforce	Replication sets this to true at
	 *									the end of REFRESH. This forces us
	 *									to run the included foreign key constraints even
	 *									if they're already marked ENABLED.
	 */
	public SetConstraintsConstantOperation (ConstraintDescriptorList cdl,boolean enable,
		boolean	unconditionallyEnforce) {
		this.cdl = cdl;
		this.enable = enable;
		this.unconditionallyEnforce = unconditionallyEnforce;
	}

	public	String	toString() {
		return "SET CONSTRAINTS";
	}

	/**
	 *	This is the guts of the Execution-time logic for DROP CONSTRAINT.
	 *
	 *	@see ConstantAction#executeConstantAction
	 *
	 * @exception StandardException		Thrown on failure
	 */
    @Override
	public void executeConstantAction( Activation activation ) throws StandardException
	{
		ConstraintDescriptor		cd;
		TableDescriptor				td;
		ConstraintDescriptorList	tmpCdl;
		boolean						enforceThisConstraint;

		LanguageConnectionContext lcc = activation.getLanguageConnectionContext();
		DataDictionary dd = lcc.getDataDictionary();
		DependencyManager dm = dd.getDependencyManager();
		TransactionController tc = lcc.getTransactionExecute();

		tmpCdl = getConstraintDescriptorList(dd);

		int[] enabledCol = new int[1];
		enabledCol[0] = ConstraintDescriptor.SYSCONSTRAINTS_STATE_FIELD;
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

		/*
		** Callback to rep subclass
		*/
		publishToTargets(activation);

		boolean skipFKs = false;

		/*
		** If the constraint list is empty, then we are getting
		** all constraints.  In this case, don't bother going
		** after referencing keys (foreign keys) when we are 
		** disabling a referenced key (pk or unique key) since
		** we know we'll hit it eventually.	
		*/
		if (tmpCdl == null)
		{
			skipFKs = true;
			tmpCdl = dd.getConstraintDescriptors((TableDescriptor)null);
		}
	
		Hashtable checkConstraintTables = null;
		int cdlSize = tmpCdl.size();
		for (int index = 0; index < cdlSize; index++)
		{
			cd = tmpCdl.elementAt(index);

			/*	
			** We are careful to enable this constraint before trying
			** to enable constraints that reference it.  Similarly,
			** we disabled constraints that reference us before we
			** disable ourselves, to make sure everything works ok.
			*/
			if (unconditionallyEnforce) 
			{ 
				enforceThisConstraint = true; 
			}
			else 
			{ 
				enforceThisConstraint = (enable && !cd.isEnabled()); 
			}

			if (enforceThisConstraint)
			{
				if (cd instanceof ForeignKeyConstraintDescriptor)
				{
					validateFKConstraint((ForeignKeyConstraintDescriptor)cd, dd, tc, lcc);
				}
				/*
				** For check constraints, we build up a list of check constriants
				** by table descriptor.  Once we have collected them all, we
				** execute them in a single query per table descriptor.
				*/
				else if (cd instanceof CheckConstraintDescriptor)
				{
					td = cd.getTableDescriptor();

					if (checkConstraintTables == null)
					{
						checkConstraintTables = new Hashtable(10);
					}

					ConstraintDescriptorList tabCdl = (ConstraintDescriptorList)
												checkConstraintTables.get(td.getUUID());
					if (tabCdl == null)
					{
						tabCdl = new ConstraintDescriptorList();
						checkConstraintTables.put(td.getUUID(), tabCdl);
					}
					tabCdl.add(cd);
				}
				/*
				** If we are enabling a constraint, we need to issue
				** the invalidation on the underlying table rather than
				** the constraint we are enabling.  This is because
				** stmts that were compiled against a disabled constraint
				** have no depedency on that disabled constriant.
				*/
				dm.invalidateFor(cd.getTableDescriptor(), 
									DependencyManager.SET_CONSTRAINTS_ENABLE, lcc);
				cd.setEnabled();
				dd.updateConstraintDescriptor(cd, 
											cd.getUUID(), 
											enabledCol, 
											tc);
			}
	
			/*
			** If we are dealing with a referenced constraint, then
			** we find all of the constraints that reference this constraint.
			** Turn them on/off based on what we are doing to this
			** constraint.
			*/
			if (!skipFKs &&
				(cd instanceof ReferencedKeyConstraintDescriptor))
			{
				ForeignKeyConstraintDescriptor fkcd;
				ReferencedKeyConstraintDescriptor refcd;
				ConstraintDescriptorList fkcdl;
	
				refcd = (ReferencedKeyConstraintDescriptor)cd;
				fkcdl = refcd.getForeignKeyConstraints(ReferencedKeyConstraintDescriptor.ALL);

				int fkcdlSize = fkcdl.size();
				for (int inner = 0; inner < fkcdlSize; inner++)
				{
					fkcd = (ForeignKeyConstraintDescriptor) fkcdl.elementAt(inner);	
					if (enable && !fkcd.isEnabled())
					{
						dm.invalidateFor(fkcd.getTableDescriptor(), 
									DependencyManager.SET_CONSTRAINTS_ENABLE, lcc);
						validateFKConstraint(fkcd, dd, tc, lcc);
						fkcd.setEnabled();
						dd.updateConstraintDescriptor(fkcd, 
								fkcd.getUUID(), 
								enabledCol, 
								tc);
					}
					else if (!enable && fkcd.isEnabled())
					{
						dm.invalidateFor(fkcd, DependencyManager.SET_CONSTRAINTS_DISABLE,
										 lcc);
						fkcd.setDisabled();
						dd.updateConstraintDescriptor(fkcd, 
								fkcd.getUUID(), 
								enabledCol, 
								tc);
					}
				}
			}
	
			if (!enable && cd.isEnabled())
			{
				dm.invalidateFor(cd, DependencyManager.SET_CONSTRAINTS_DISABLE,
								 lcc);
				cd.setDisabled();
				dd.updateConstraintDescriptor(cd, 
												cd.getUUID(), 
												enabledCol, 
												tc);
			}
		}

		validateAllCheckConstraints(lcc, checkConstraintTables);
	}

	private void validateAllCheckConstraints(LanguageConnectionContext lcc, Hashtable ht)
		throws StandardException
	{
		ConstraintDescriptorList	cdl;
		ConstraintDescriptor		cd = null;
		TableDescriptor				td;
		StringBuffer				text;
		StringBuffer				constraintNames;

		if (ht == null)
		{
			return;
		}

		for (Enumeration e = ht.elements(); e.hasMoreElements(); )
		{
		
			cdl = (ConstraintDescriptorList) e.nextElement();
			text = null;
			constraintNames = null;

			/*
			** Build up the text of all the constraints into one big
			** predicate.  Also, we unfortunately have to build up a big
			** comma separated list of constraint names in case
			** there is an error (we are favoring speed over a very
			** explicit check constraint xxxx failed error message).
			*/
			int cdlSize = cdl.size();
			for (int index = 0; index < cdlSize; index++)
			{
				cd = (ConstraintDescriptor) cdl.elementAt(index);
				if (text == null)
				{
					text = new StringBuffer("(").append(cd.getConstraintText()).append(") ");
					constraintNames = new StringBuffer(cd.getConstraintName());
				}
				else
				{
					text.append(" AND (").append(cd.getConstraintText()).append(") ");
					constraintNames.append(", ").append(cd.getConstraintName());
				}
			}

			if (SanityManager.DEBUG)
			{
				SanityManager.ASSERT(text != null, "internal error, badly built hastable");
			}

			ConstraintConstantOperation.validateConstraint(
												constraintNames.toString(),
												text.toString(),
												cd.getTableDescriptor(),
												lcc, true);
		}
	}

	/*
	**
	*/
	private void validateFKConstraint
	(
		ForeignKeyConstraintDescriptor	fk,
		DataDictionary					dd,
		TransactionController			tc,
        LanguageConnectionContext       lcc
	)
		throws StandardException
	{
		/*
		** Construct a template row 
		*/
		IndexRowGenerator irg = fk.getIndexConglomerateDescriptor(dd).getIndexDescriptor();	
		ExecIndexRow indexTemplateRow = irg.getIndexRowTemplate();
		TableDescriptor td = fk.getTableDescriptor();
		ExecRow baseRow = td.getEmptyExecRow();
		irg.getIndexRow(baseRow, getRowLocation(dd, td, tc), 
								indexTemplateRow, (FormatableBitSet)null);

		/*
		** The moment of truth
		*/
		ConstraintConstantOperation.validateFKConstraint(tc, dd, fk, 
							fk.getReferencedConstraint(), indexTemplateRow, lcc);
	}
			
	/*
	** Get a row location template.  Note that we are assuming that
	** the same row location can be used for all tables participating
	** in the fk.  For example, if there are multiple foreign keys,
	** we are using the row location of one of the tables and assuming
	** that it is the right shape for all tables.  Currently, this
	** is a legitimate assumption.
	*/
	private RowLocation getRowLocation
	(
		DataDictionary			dd, 
		TableDescriptor			td,
		TransactionController	tc
	) 
		throws StandardException
	{
		RowLocation 			rl; 
		ConglomerateController 	heapCC = null;

		long tableId = td.getHeapConglomerateId();
		heapCC = 
            tc.openConglomerate(
                tableId, false, 0, tc.MODE_RECORD, tc.ISOLATION_READ_COMMITTED);
		try
		{
			rl = heapCC.newRowLocationTemplate();
		}
		finally
		{
			heapCC.close();
		}

		return rl;
	}
		
	/*
	** Wrapper for constraint descriptor list -- always use
	** this to get the constriant descriptor list.  It is
	** used to hide serialization.
	*/
	private ConstraintDescriptorList getConstraintDescriptorList(DataDictionary dd)
		throws StandardException
	{
		if (cdl != null)
		{
			return cdl;
		}
		if (tuuids == null)
		{
			return null;
		}

		/*
		** Reconstitute the cdl from the uuids
		*/
		cdl = new ConstraintDescriptorList();

		for (int i = 0; i < tuuids.length; i++)
		{
			TableDescriptor td = dd.getTableDescriptor(tuuids[i]);
			if (SanityManager.DEBUG)
			{
				if (td == null)
				{
					SanityManager.THROWASSERT("couldn't locate table descriptor "+
						"in SET CONSTRAINTS for uuid "+tuuids[i]);
				}
			}

			ConstraintDescriptor cd = dd.getConstraintDescriptorById(td, cuuids[i]);

			if (SanityManager.DEBUG)
			{
				if (cd == null)
				{
					SanityManager.THROWASSERT("couldn't locate constraint descriptor "+
						" in SET CONSTRAINTS for uuid "+cuuids[i]);
				}
			}

			cdl.add(cd);
		}
		return cdl;
	}
		
	///////////////////////////////////////////////
	//
	// MISC
	//
	///////////////////////////////////////////////

	/**
	 * Do the work of publishing any this action to any
	 * replication targets.  On a non-replicated source,
	 * this is a no-op.
	 *
	 * @param activation the activation
	 *
	 * @exception StandardException on error
	 */
	protected void publishToTargets(Activation activation)
		throws StandardException
	{
	}
}

