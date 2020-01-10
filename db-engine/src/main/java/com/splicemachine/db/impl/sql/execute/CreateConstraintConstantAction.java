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
 * All such Splice Machine modifications are Copyright 2012 - 2020 Splice Machine, Inc.,
 * and are licensed to you under the GNU Affero General Public License.
 */

package com.splicemachine.db.impl.sql.execute;

import com.splicemachine.db.iapi.services.sanity.SanityManager;


import com.splicemachine.db.catalog.UUID;
import com.splicemachine.db.iapi.services.uuid.UUIDFactory;
import com.splicemachine.db.catalog.types.ReferencedColumnsDescriptorImpl;

import com.splicemachine.db.iapi.error.StandardException;

import com.splicemachine.db.iapi.sql.conn.LanguageConnectionContext;

import com.splicemachine.db.iapi.sql.dictionary.DDUtils;
import com.splicemachine.db.iapi.sql.dictionary.ColumnDescriptor;
import com.splicemachine.db.iapi.sql.dictionary.ConglomerateDescriptor;
import com.splicemachine.db.iapi.sql.dictionary.ConstraintDescriptor;
import com.splicemachine.db.iapi.sql.dictionary.DataDescriptorGenerator;
import com.splicemachine.db.iapi.sql.dictionary.DataDictionary;
import com.splicemachine.db.iapi.sql.dictionary.ForeignKeyConstraintDescriptor;
import com.splicemachine.db.iapi.sql.dictionary.ReferencedKeyConstraintDescriptor;
import com.splicemachine.db.iapi.sql.dictionary.SchemaDescriptor;
import com.splicemachine.db.iapi.sql.dictionary.TableDescriptor;

import com.splicemachine.db.iapi.reference.SQLState;

import com.splicemachine.db.iapi.sql.depend.DependencyManager;
import com.splicemachine.db.iapi.sql.depend.Provider;
import com.splicemachine.db.iapi.sql.depend.ProviderInfo;

import com.splicemachine.db.iapi.sql.execute.ConstantAction;

import com.splicemachine.db.iapi.sql.Activation;

import com.splicemachine.db.iapi.store.access.TransactionController;
import com.splicemachine.db.iapi.services.loader.ClassFactory;

/**
 *	This class  describes actions that are ALWAYS performed for a
 *	constraint creation at Execution time.
 *
 *	@version 0.1
 */

public class CreateConstraintConstantAction extends ConstraintConstantAction
{
    private final boolean forCreateTable;
    
	private String[]		columnNames;
	private	String			constraintText;

	private ConstraintInfo	otherConstraintInfo;
	private	ClassFactory	cf;

	/*
	** Is this constraint to be created as enabled or not.
	** The only way to create a disabled constraint is by
	** publishing a disabled constraint.
	*/
	private boolean			enabled;

	private ProviderInfo[] providerInfo;

	// CONSTRUCTORS

	/**
	 *	Make one of these puppies.
	 *
	 *  @param constraintName	Constraint name.
	 *  @param constraintType	Constraint type.
     *  @param forCreateTable   Constraint is being added for a CREATE TABLE
	 *  @param tableName		Table name.
	 *	@param tableId			UUID of table.
	 *  @param schemaName		the schema that table and constraint lives in.
	 *  @param columnNames		String[] for column names
	 *  @param indexAction		IndexConstantAction for constraint (if necessary)
	 *  @param constraintText	Text for check constraint
	 *  RESOLVE - the next parameter should go away once we use UUIDs
	 *			  (Generated constraint names will be based off of uuids)
	 *	@param enabled			Should the constraint be created as enabled 
	 *							(enabled == true), or disabled (enabled == false).
	 *	@param otherConstraint 	information about the constraint that this references
	 *  @param providerInfo Information on all the Providers
	 */
	public CreateConstraintConstantAction(
		               String	constraintName,
					   int		constraintType,
                       boolean  forCreateTable,
		               String	tableName,
					   UUID		tableId,
					   String	schemaName,
					   String[]	columnNames,
					   ConstantAction indexAction,
					   String	constraintText,
					   boolean	enabled,
				       ConstraintInfo	otherConstraint,
					   ProviderInfo[] providerInfo)
	{
		super(constraintName, constraintType, tableName, 
			  tableId, schemaName, indexAction);
        this.forCreateTable = forCreateTable;
		this.columnNames = columnNames;
		this.constraintText = constraintText;
		this.enabled = enabled;
		this.otherConstraintInfo = otherConstraint;
		this.providerInfo = providerInfo;
	}

	// INTERFACE METHODS


	/**
	 *	This is the guts of the Execution-time logic for CREATE CONSTRAINT.
	 *  <P>
	 *  A constraint is represented as:
	 *  <UL>
	 *  <LI> ConstraintDescriptor.
	 *  </UL>
	 *  If a backing index is required then the index will
	 *  be created through an CreateIndexConstantAction setup
	 *  by the compiler.
	 *  <BR>
	 *  Dependencies are created as:
	 *  <UL>
	 *  <LI> ConstraintDescriptor depends on all the providers collected
     *  at compile time and passed into the constructor.
	 *  <LI> For a FOREIGN KEY constraint ConstraintDescriptor depends
     *  on the ConstraintDescriptor for the referenced constraints
     *  and the privileges required to create the constraint.
	 *  </UL>

	 *  @see ConstraintDescriptor
	 *  @see CreateIndexConstantAction
	 *	@see ConstantAction#executeConstantAction
	 *
	 * @exception StandardException		Thrown on failure
	 */
	public void	executeConstantAction( Activation activation )
						throws StandardException
	{
		ConglomerateDescriptor		conglomDesc = null;
		ConglomerateDescriptor[]	conglomDescs = null;
		ConstraintDescriptor		conDesc = null;
		TableDescriptor				td = null;
		UUID						indexId = null;
		String						uniqueName;
		String						backingIndexName;

		/* RESOLVE - blow off not null constraints for now (and probably for ever) */
		if (constraintType == DataDictionary.NOTNULL_CONSTRAINT)
		{
			return;
		}

		LanguageConnectionContext lcc = activation.getLanguageConnectionContext();
		DataDictionary dd = lcc.getDataDictionary();
		DependencyManager dm = dd.getDependencyManager();
		TransactionController tc = lcc.getTransactionExecute();

		cf = lcc.getLanguageConnectionFactory().getClassFactory();

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

		/* Table gets locked in AlterTableConstantAction */

		/*
		** If the schema descriptor is null, then
		** we must have just read ourselves in.  
		** So we will get the corresponding schema
		** descriptor from the data dictionary.
		*/

		SchemaDescriptor sd = dd.getSchemaDescriptor(schemaName, tc, true);
		
		/* Try to get the TableDescriptor from
		 * the Activation. We will go to the
		 * DD if not there. (It should always be
		 * there except when in a target.)
		 */
		td = activation.getDDLTableDescriptor();

		if (td == null)
		{
			/* tableId will be non-null if adding a
			 * constraint to an existing table.
			 */
			if (tableId != null)
			{
				td = dd.getTableDescriptor(tableId);
			}
			else
			{
				td = dd.getTableDescriptor(tableName, sd, tc);
			}

			if (td == null)
			{
				throw StandardException.newException(SQLState.LANG_TABLE_NOT_FOUND_DURING_EXECUTION, tableName);
			}
			activation.setDDLTableDescriptor(td);
		}

		/* Generate the UUID for the backing index.  This will become the
		 * constraint's name, if no name was specified.
		 */
		UUIDFactory uuidFactory = dd.getUUIDFactory();
        
        indexId = manageIndexAction(td,uuidFactory,activation);

		UUID constraintId = uuidFactory.createUUID();

		/* Now, lets create the constraint descriptor */
		DataDescriptorGenerator ddg = dd.getDataDescriptorGenerator();
		switch (constraintType)
		{
			case DataDictionary.PRIMARYKEY_CONSTRAINT:
				conDesc = ddg.newPrimaryKeyConstraintDescriptor(
								td, constraintName,
								false, //deferable,
								false, //initiallyDeferred,
								genColumnPositions(td, false), //int[],
								constraintId, 
								indexId, 
								sd,
								enabled,
								0				// referenceCount
								);
				dd.addConstraintDescriptor(conDesc, tc);
				break;

			case DataDictionary.UNIQUE_CONSTRAINT:
				conDesc = ddg.newUniqueConstraintDescriptor(
								td, constraintName,
								false, //deferable,
								false, //initiallyDeferred,
								genColumnPositions(td, false), //int[],
								constraintId, 
								indexId, 
								sd,
								enabled,
								0				// referenceCount
								);
				dd.addConstraintDescriptor(conDesc, tc);
				break;

			case DataDictionary.CHECK_CONSTRAINT:
				conDesc = ddg.newCheckConstraintDescriptor(
								td, constraintName,
								false, //deferable,
								false, //initiallyDeferred,
								constraintId, 
								constraintText, 
								new ReferencedColumnsDescriptorImpl(genColumnPositions(td, false)), //int[],
								sd,
								enabled
								);
				dd.addConstraintDescriptor(conDesc, tc);
				storeConstraintDependenciesOnPrivileges
					(activation, conDesc, null, providerInfo);
				break;

			case DataDictionary.FOREIGNKEY_CONSTRAINT:
				ReferencedKeyConstraintDescriptor referencedConstraint = DDUtils.locateReferencedConstraint
					( dd, td, constraintName, columnNames, otherConstraintInfo );
				DDUtils.validateReferentialActions(dd, td, constraintName, otherConstraintInfo,columnNames);
				
				conDesc = ddg.newForeignKeyConstraintDescriptor(
								td, constraintName,
								false, //deferable,
								false, //initiallyDeferred,
								genColumnPositions(td, false), //int[],
								constraintId,
								indexId,
								sd,
								referencedConstraint,
								enabled,
								otherConstraintInfo.getReferentialActionDeleteRule(),
								otherConstraintInfo.getReferentialActionUpdateRule()
								);

				// try to create the constraint first, because it
				// is expensive to do the bulk check, find obvious
				// errors first
				dd.addConstraintDescriptor(conDesc, tc);

				/* No need to do check if we're creating a 
				 * table.
				 */
				if ( (! forCreateTable) && 
					 dd.activeConstraint( conDesc ) )
				{
					validateFKConstraint(tc, 
										 dd, 
										 (ForeignKeyConstraintDescriptor)conDesc, 
										 referencedConstraint,
										 ((CreateIndexConstantAction)indexAction).getIndexTemplateRow());
				}
				
				/* Create stored dependency on the referenced constraint */
				dm.addDependency(conDesc, referencedConstraint, lcc.getContextManager());
				/**
				 * It is problematic to make the FK constraint depend on a privileges or role definition,
				 * as when the depended privilege or role is dropped, the FK constraint will be dropped too
				 *

				 //store constraint's dependency on REFERENCES privileges in the dependeny system
				storeConstraintDependenciesOnPrivileges
					(activation,
					 conDesc,
					 referencedConstraint.getTableId(),
					 providerInfo);
				 */
				break;

			default:
				if (SanityManager.DEBUG)
				{
					SanityManager.THROWASSERT("contraintType (" + constraintType + 
						") has unexpected value");
				}
				break;
		}

		/* Create stored dependencies for each provider */
		if (providerInfo != null)
		{
            for (ProviderInfo aProviderInfo : providerInfo) {
                Provider provider = null;

				/* We should always be able to find the Provider */
                provider = (Provider) aProviderInfo.
                        getDependableFinder().
                        getDependable(dd,
                                aProviderInfo.getObjectId());

                dm.addDependency(conDesc, provider, lcc.getContextManager());
            }
		}

		/* Finally, invalidate off of the table descriptor(s)
		 * to ensure that any dependent statements get
		 * re-compiled.
		 */
		if (! forCreateTable)
		{
			dm.invalidateFor(td, DependencyManager.CREATE_CONSTRAINT, lcc);
		}
		if (constraintType == DataDictionary.FOREIGNKEY_CONSTRAINT)
		{
			if (SanityManager.DEBUG)
			{
				SanityManager.ASSERT(conDesc != null,
					"conDesc expected to be non-null");

				if (! (conDesc instanceof ForeignKeyConstraintDescriptor))
				{
					SanityManager.THROWASSERT(
						"conDesc expected to be instance of ForeignKeyConstraintDescriptor, not " +
						conDesc.getClass().getName());
				}
			}
			dm.invalidateFor(
				((ForeignKeyConstraintDescriptor)conDesc).
					getReferencedConstraint().
						getTableDescriptor(),
				DependencyManager.CREATE_CONSTRAINT, lcc);
		}
	}

    protected UUID manageIndexAction(TableDescriptor td,
                                     UUIDFactory uuidFactory,
                                     Activation activation) throws StandardException{

        /* Create the index, if there's one for this constraint */
        ConglomerateDescriptor[] conglomDescs;
        String backingIndexName;
        ConglomerateDescriptor conglomDesc = null;
	IndexConstantAction iAction;
        if (indexAction instanceof IndexConstantAction)
        {
	    iAction = (IndexConstantAction)indexAction;
            if ( iAction.getIndexName() == null )
            {
				/* Set the index name */
                backingIndexName =  uuidFactory.createUUID().toString();
                iAction.setIndexName(backingIndexName);
            }
            else { backingIndexName = iAction.getIndexName(); }


			/* Create the index */
            indexAction.executeConstantAction(activation);

			/* Get the conglomerate descriptor for the backing index */
            conglomDescs = td.getConglomerateDescriptors();

            for (ConglomerateDescriptor conglomDesc1 : conglomDescs) {
                conglomDesc = conglomDesc1;

				/* Check for conglomerate being an index first, since
                 * name is null for heap.
				 */
                if (conglomDesc.isIndex() &&
                        backingIndexName.equals(conglomDesc.getConglomerateName())) {
                    break;
                }
            }

            if (SanityManager.DEBUG)
            {
                SanityManager.ASSERT(conglomDesc != null,
                        "conglomDesc is expected to be non-null after search for backing index");
                SanityManager.ASSERT(conglomDesc.isIndex(),
                        "conglomDesc is expected to be indexable after search for backing index");
                SanityManager.ASSERT(conglomDesc.getConglomerateName().equals(backingIndexName),
                        "conglomDesc name expected to be the same as backing index name after search for backing index");
            }

            return conglomDesc.getUUID();
        }
        return null;
    }

	/**
	 * Is the constant action for a foreign key
	 *
	 * @return true/false
	 */
	boolean isForeignKeyConstraint()
	{ 
		return (constraintType == DataDictionary.FOREIGNKEY_CONSTRAINT);
	}

	/**
	 * Generate an array of column positions for the column list in
	 * the constraint.
	 *
	 * @param td	The TableDescriptor for the table in question
	 * @param columnsMustBeOrderable	true for primaryKey and unique constraints
	 *
	 * @return int[]	The column positions.
	 */
	public int[] genColumnPositions(TableDescriptor td,
									 boolean columnsMustBeOrderable)
		throws StandardException
	{
		int[] baseColumnPositions;

		// Translate the base column names to column positions
		baseColumnPositions = new int[columnNames.length];
		for (int i = 0; i < columnNames.length; i++)
		{
			ColumnDescriptor columnDescriptor;

			// Look up the column in the data dictionary
			columnDescriptor = td.getColumnDescriptor(columnNames[i]);
			if (columnDescriptor == null)
			{
				throw StandardException.newException(SQLState.LANG_COLUMN_NOT_FOUND_IN_TABLE, 
															columnNames[i],
															tableName);
			}

			// Don't allow a column to be created on a non-orderable type
			// (for primaryKey and unique constraints)
			if ( columnsMustBeOrderable &&
				 ( ! columnDescriptor.getType().getTypeId().orderable(
															cf))
			   )
			{
				throw StandardException.newException(SQLState.LANG_COLUMN_NOT_ORDERABLE_DURING_EXECUTION, 
					columnDescriptor.getType().getTypeId().getSQLTypeName());
			}

			// Remember the position in the base table of each column
			baseColumnPositions[i] = columnDescriptor.getPosition();
		}

		return baseColumnPositions;
	}
	///////////////////////////////////////////////////////////////////////
	//
	//	ACCESSORS
	//
	///////////////////////////////////////////////////////////////////////



	/**
	  *	Get the text defining this constraint.
	  *
	  *	@return	constraint text
	  */
    String	getConstraintText() { return constraintText; }

	public String toString()
	{
		// Do not put this under SanityManager.DEBUG - it is needed for
		// error reporting.
		StringBuilder strbuf = new StringBuilder();
		strbuf.append("CREATE CONSTRAINT ").append(constraintName);
		strbuf.append("\n=========================\n");

		if (columnNames == null)
		{
			strbuf.append("columnNames == null\n");
		}
		else
		{
			for (int ix=0; ix < columnNames.length; ix++)
			{
				strbuf.append("\n\tcol[").append(ix).append("]").append(columnNames[ix]);
			}
		}
		
		strbuf.append("\n");
		strbuf.append(constraintText);
		strbuf.append("\n");
		if (otherConstraintInfo != null)
		{
			strbuf.append(otherConstraintInfo.toString());
		}
		strbuf.append("\n");
		return strbuf.toString();
	}
}
