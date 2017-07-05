/*
 * This file is part of Splice Machine.
 * Splice Machine is free software: you can redistribute it and/or modify it under the terms of the
 * GNU Affero General Public License as published by the Free Software Foundation, either
 * version 3, or (at your option) any later version.
 * Splice Machine is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU Affero General Public License for more details.
 * You should have received a copy of the GNU Affero General Public License along with Splice Machine.
 * If not, see <http://www.gnu.org/licenses/>.
 *
 * Some parts of this source code are based on Apache Derby, and the following notices apply to
 * Apache Derby:
 *
 * Apache Derby is a subproject of the Apache DB project, and is licensed under
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
 * Splice Machine, Inc. has modified the Apache Derby code in this file.
 *
 * All such Splice Machine modifications are Copyright 2012 - 2017 Splice Machine, Inc.,
 * and are licensed to you under the GNU Affero General Public License.
 */

package com.splicemachine.db.impl.sql.execute;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import com.splicemachine.db.catalog.UUID;

import com.splicemachine.db.iapi.error.StandardException;

import com.splicemachine.db.iapi.services.sanity.SanityManager;

import com.splicemachine.db.iapi.sql.Activation;
import com.splicemachine.db.iapi.sql.conn.LanguageConnectionContext;
import com.splicemachine.db.iapi.sql.dictionary.ConglomerateDescriptor;
import com.splicemachine.db.iapi.sql.dictionary.ConstraintDescriptor;
import com.splicemachine.db.iapi.sql.dictionary.KeyConstraintDescriptor;
import com.splicemachine.db.iapi.sql.dictionary.DataDictionary;
import com.splicemachine.db.iapi.sql.dictionary.TableDescriptor;
import com.splicemachine.db.iapi.sql.execute.ConstantAction;
import com.splicemachine.db.iapi.store.access.ConglomerateController;
import com.splicemachine.db.iapi.store.access.TransactionController;

/**
 * Abstract class that has actions that are across
 * all DDL actions that are tied to a table.  An example
 * of DDL that affects a table is CREATE INDEX or
 * DROP VIEW.  An example of DDL that does not affect
 * a table is CREATE STATEMENT or DROP SCHEMA.
 *
 */
abstract class DDLSingleTableConstantAction extends DDLConstantAction 
{
	protected UUID					tableId;

	
	/**
	 * constructor
	 *
	 * @param tableId the target table
	 */
	DDLSingleTableConstantAction(UUID tableId)
	{
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
	void dropConstraint(ConstraintDescriptor consDesc,
		Activation activation, LanguageConnectionContext lcc,
		boolean clearDeps) throws StandardException
	{
		dropConstraint(consDesc, (TableDescriptor)null,
			(List)null, activation, lcc, clearDeps);
	}

	/**
	 * See "dropConstraint(...") above.
	 *
	 * @param skipCreate Optional TableDescriptor.  If non-null
	 *  then we will skip the "create new conglomerate" processing
	 *  *IF* the constraint that we drop came from the table
	 *  described by skipCreate.
	 */
	void dropConstraint(ConstraintDescriptor consDesc,
		TableDescriptor skipCreate, Activation activation,
		LanguageConnectionContext lcc, boolean clearDeps)
		throws StandardException
	{
		dropConstraint(consDesc, skipCreate,
			(List)null, activation, lcc, clearDeps);
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
	void dropConstraint(ConstraintDescriptor consDesc,
		TableDescriptor skipCreate, List newConglomActions,
		Activation activation, LanguageConnectionContext lcc,
		boolean clearDeps) throws StandardException
	{
		/* Get the properties on the old backing conglomerate before
		 * dropping the constraint, since we can't get them later if
		 * dropping the constraint causes us to drop the backing
		 * conglomerate.
		 */
		Properties ixProps = null;
		if (consDesc instanceof KeyConstraintDescriptor)
		{
			ixProps = new Properties();
			loadIndexProperties(lcc,
				((KeyConstraintDescriptor)consDesc)
					.getIndexConglomerateDescriptor(lcc.getDataDictionary()),
				ixProps);
		}

		ConglomerateDescriptor newBackingConglomCD = consDesc.drop(lcc, clearDeps);

		/* If we don't need a new conglomerate then there's nothing
		 * else to do.
		 */
		if (newBackingConglomCD == null)
			return;

		/* Only create the new conglomerate if it is NOT for the table
		 * described by skipCreate.
		 */
		if ((skipCreate != null) &&
			skipCreate.getUUID().equals(
				consDesc.getTableDescriptor().getUUID()))
		{
			/* We're skipping the "create new conglom" phase; if we have
			 * a list in which to store the ConstantAction, then store it;
			 * otherwise, the new conglomerate is effectively ignored.
			 */
			if (newConglomActions != null)
			{
				newConglomActions.add(
					getConglomReplacementAction(newBackingConglomCD,
						consDesc.getTableDescriptor(), ixProps));
			}
		}
		else
		{
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
	void dropConglomerate(
		ConglomerateDescriptor congDesc, TableDescriptor td,
		Activation activation, LanguageConnectionContext lcc)
		throws StandardException
	{
		dropConglomerate(congDesc, td,
			false, (List)null, activation, lcc);
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
	void dropConglomerate(
		ConglomerateDescriptor congDesc, TableDescriptor td,
		boolean skipCreate, List newConglomActions,
		Activation activation, LanguageConnectionContext lcc)
		throws StandardException
	{
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

		if (skipCreate)
		{
			/* We're skipping the "create new conglom" phase; if we have
			 * a list in which to store the ConstantAction, then store it;
			 * otherwise, the new conglomerate is effectively ignored.
			 */
			if (newConglomActions != null)
			{
				newConglomActions.add(
					getConglomReplacementAction(
						newBackingConglomCD, td, ixProps));
			}
		}
		else
		{
			executeConglomReplacement(
				getConglomReplacementAction(newBackingConglomCD, td, ixProps),
				activation);
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
    ConglomerateDescriptor      cd,
    TableDescriptor             td,
    Activation                  activation, 
    LanguageConnectionContext   lcc) 
        throws StandardException 
    {
        //get index property
        Properties prop = new Properties ();
        loadIndexProperties(lcc, cd, prop);
        ArrayList list = new ArrayList();

        // drop the existing index.
        dropConglomerate(cd, td, false, list, activation, lcc);


        String [] cols = cd.getColumnNames();
        if (cols == null) 
        {
            //column list wasn't stored in conglomerateDescriptor
            //fetch is from table descriptor
            int [] pos = cd.getIndexDescriptor().baseColumnPositions();
            cols       = new String [pos.length];

            for (int i = 0; i < cols.length; i++) 
            {
                cols[i] = td.getColumnDescriptor(pos[i]).getColumnName();
            }
        }
        
        //create new index action
        CreateIndexConstantAction action =
                new CreateIndexConstantAction(
                        false,          // not part of create table 
                        false,          // not unique
                        true,           // create as unique when not null index
                        cd.getIndexDescriptor().indexType(), 
                        td.getSchemaName(), 
                        cd.getConglomerateName(), td.getName(), td.getUUID(),
                        cols, cd.getIndexDescriptor().isAscending(),
                        true, cd.getUUID(), prop);

        //create index
        action.executeConstantAction(activation);
    }

	/**
	 * Get any table properties that exist for the received
	 * index descriptor.
	 */
	private void loadIndexProperties(LanguageConnectionContext lcc,
		ConglomerateDescriptor congDesc, Properties ixProps)
		throws StandardException
	{
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
		TableDescriptor td, Properties properties) throws StandardException
	{
		/* Re-use CreateIndexActionConstantAction to do the work
		 * of creating a new conglomerate.  The big difference
		 * between creating an _index_ and creating an index
		 * _conglomerate_ is that we don't need to create a new
		 * ConglomerateDescriptor in the latter case.  Use of the
		 * following constructor dictates that we want to create
		 * a _conglomerate_ only--i.e. that no new conglomerate
		 * descriptor is necessary.
		 */
		return new CreateIndexConstantAction(srcCD, td, properties);
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
	void executeConglomReplacement(ConstantAction replaceConglom,
		Activation activation) throws StandardException
	{
		CreateIndexConstantAction replaceConglomAction =
			(CreateIndexConstantAction)replaceConglom;

		LanguageConnectionContext lcc =
			activation.getLanguageConnectionContext();

		DataDictionary dd = lcc.getDataDictionary();

		// Create the new (replacment) backing conglomerate...
		replaceConglomAction.executeConstantAction(activation);

		/* Find all conglomerate descriptors that referenced the
		 * old backing conglomerate and update them to have the
		 * conglomerate number for the new backing conglomerate.
		 */
		ConglomerateDescriptor [] congDescs =
			dd.getConglomerateDescriptors(
				replaceConglomAction.getReplacedConglomNumber());

		if (SanityManager.DEBUG)
		{
			/* There should be at least one descriptor requiring
			 * an updated conglomerate number--namely, the one
			 * corresponding to "srcCD" for which the constant
			 * action was created (see getConglomReplacementAction()
			 * above). There may be others, as well.
			 */
			if (congDescs.length < 1)
			{
				SanityManager.THROWASSERT(
					"Should have found at least one conglomerate " +
					"descriptor that needs an updated conglomerate " +
					"number (due to a dropped index), but only " +
					"found " + congDescs.length);
			}
		}

		dd.updateConglomerateDescriptor(congDescs,
			replaceConglomAction.getCreatedConglomNumber(),
			lcc.getTransactionExecute());

    }
}
