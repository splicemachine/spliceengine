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

import com.splicemachine.db.iapi.sql.StatementType;
import com.splicemachine.derby.ddl.DDLChangeType;
import com.splicemachine.derby.impl.job.fk.FkJobSubmitter;
import com.splicemachine.derby.impl.store.access.SpliceTransactionManager;
import com.splicemachine.db.catalog.UUID;
import com.splicemachine.db.catalog.types.ReferencedColumnsDescriptorImpl;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.reference.SQLState;
import com.splicemachine.db.iapi.services.loader.ClassFactory;
import com.splicemachine.db.iapi.services.sanity.SanityManager;
import com.splicemachine.db.iapi.services.uuid.UUIDFactory;
import com.splicemachine.db.iapi.sql.Activation;
import com.splicemachine.db.iapi.sql.conn.LanguageConnectionContext;
import com.splicemachine.db.iapi.sql.depend.DependencyManager;
import com.splicemachine.db.iapi.sql.depend.Provider;
import com.splicemachine.db.iapi.sql.depend.ProviderInfo;
import com.splicemachine.db.iapi.sql.dictionary.ColumnDescriptor;
import com.splicemachine.db.iapi.sql.dictionary.ConglomerateDescriptor;
import com.splicemachine.db.iapi.sql.dictionary.ConstraintDescriptor;
import com.splicemachine.db.iapi.sql.dictionary.DDUtils;
import com.splicemachine.db.iapi.sql.dictionary.DataDescriptorGenerator;
import com.splicemachine.db.iapi.sql.dictionary.DataDictionary;
import com.splicemachine.db.iapi.sql.dictionary.ForeignKeyConstraintDescriptor;
import com.splicemachine.db.iapi.sql.dictionary.ReferencedKeyConstraintDescriptor;
import com.splicemachine.db.iapi.sql.dictionary.SchemaDescriptor;
import com.splicemachine.db.iapi.sql.dictionary.TableDescriptor;
import com.splicemachine.db.iapi.sql.execute.ConstantAction;
import com.splicemachine.db.iapi.store.access.TransactionController;
import com.splicemachine.db.impl.sql.execute.ConstraintInfo;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.apache.log4j.Logger;

import com.splicemachine.utils.SpliceLogUtils;

public class CreateConstraintConstantOperation extends ConstraintConstantOperation {
	private static final Logger LOG = Logger.getLogger(CreateConstraintConstantOperation.class);
    private final boolean forCreateTable;
	public String[] columnNames;
	private	String constraintText;
	private ConstraintInfo otherConstraintInfo;
	private	ClassFactory cf;
	/*
	** Is this constraint to be created as enabled or not.
	** The only way to create a disabled constraint is by
	** publishing a disabled constraint.
	*/
	private boolean enabled;
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
	@SuppressFBWarnings(value = "EI_EXPOSE_REP2",justification = "Intentional")
	public CreateConstraintConstantOperation(
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
					   ProviderInfo[] providerInfo) {
		super(constraintName, constraintType, tableName, tableId, schemaName, indexAction);
		SpliceLogUtils.trace(LOG, "CreateConstraintConstantOperation for %s", constraintName);
        this.forCreateTable = forCreateTable;
		this.columnNames = columnNames;
		this.constraintText = constraintText;
		this.enabled = enabled;
		this.otherConstraintInfo = otherConstraint;
		this.providerInfo = providerInfo;
	}

	@Override
	public void validateSupported() throws StandardException{
		if(constraintType!=DataDictionary.FOREIGNKEY_CONSTRAINT) return; //nothing to validate

		/*
		 * DB-2224/2225/3985. SpliceMachine does not support ON DELETE ... rules when creating foreign
		 * keys, until DB-2224 (ON DELETE CASCADE) and DB-2225 (ON DELETE SET NULL) are implemented. This
		 * little block throws the proper error message rather than attempt to create a potentially
		 * invalid table.
		 */
		switch(otherConstraintInfo.getReferentialActionDeleteRule()){
			case StatementType.RA_CASCADE:
				throw StandardException.newException(SQLState.NOT_IMPLEMENTED, "ON DELETE CASCADE");
			case StatementType.RA_SETNULL:
				throw StandardException.newException(SQLState.NOT_IMPLEMENTED, "ON DELETE SET NULL");
		}
	}

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
	 *  @see CreateIndexConstantOperation
	 *	@see ConstantAction#executeConstantAction
	 *
	 * @exception StandardException		Thrown on failure
	 */
    @Override
	public void executeConstantAction( Activation activation ) throws StandardException {
		SpliceLogUtils.trace(LOG, "executeConstantAction");
		ConglomerateDescriptor		conglomDesc = null;
		ConglomerateDescriptor[]	conglomDescs = null;
		ConstraintDescriptor		conDesc = null;
		TableDescriptor				td = null;
		UUID						indexId = null;
		String						uniqueName;
		String						backingIndexName;
		/* RESOLVE - blow off not null constraints for now (and probably for ever) */
		// XXX TODO Was this us or derby? - JL
		if (constraintType == DataDictionary.NOTNULL_CONSTRAINT) {
			return;
		}
		LanguageConnectionContext lcc = activation.getLanguageConnectionContext();
		DataDictionary dd = lcc.getDataDictionary();
		DependencyManager dm = dd.getDependencyManager();
		TransactionController tc = lcc.getTransactionExecute();
		cf = lcc.getLanguageConnectionFactory().getClassFactory();
		SchemaDescriptor sd = dd.getSchemaDescriptor(schemaName, tc, true);
		
		td = activation.getDDLTableDescriptor();

		if (td == null) {
			/* tableId will be non-null if adding a
			 * constraint to an existing table.
			 */
			td = tableId!=null?dd.getTableDescriptor(tableId):dd.getTableDescriptor(tableName, sd, tc);
			if (td == null)
				throw StandardException.newException(SQLState.LANG_TABLE_NOT_FOUND_DURING_EXECUTION, tableName);
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
		switch (constraintType) {
			case DataDictionary.PRIMARYKEY_CONSTRAINT:
				if (td.getTableType() == TableDescriptor.EXTERNAL_TYPE)
					throw StandardException.newException(SQLState.EXTERNAL_TABLES_NO_PRIMARY_KEYS,td.getName());
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
				if (td.getTableType() == TableDescriptor.EXTERNAL_TYPE)
					throw StandardException.newException(SQLState.EXTERNAL_TABLES_NO_UNIQUE_CONSTRAINTS,td.getName());
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
				if (td.getTableType() == TableDescriptor.EXTERNAL_TYPE)
					throw StandardException.newException(SQLState.EXTERNAL_TABLES_NO_CHECK_CONSTRAINTS,td.getName());

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
				if (td.getTableType() == TableDescriptor.EXTERNAL_TYPE)
					throw StandardException.newException(SQLState.EXTERNAL_TABLES_NO_REFERENCE_CONSTRAINTS,td.getName());
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
										 ((CreateIndexConstantOperation)indexAction).getIndexTemplateRow(), lcc);
				}
				
				/* Create stored dependency on the referenced constraint */
				dm.addDependency(conDesc, referencedConstraint, lcc.getContextManager());
				//store constraint's dependency on REFERENCES privileges in the dependency system
				storeConstraintDependenciesOnPrivileges
					(activation,
					 conDesc,
					 referencedConstraint.getTableId(),
					 providerInfo);


                // Use the task framework to add FK Write handler on remote nodes.
                new FkJobSubmitter(dd, (SpliceTransactionManager) tc, referencedConstraint, conDesc, DDLChangeType.ADD_FOREIGN_KEY,lcc).submit();

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
		if (providerInfo != null) {
            for (ProviderInfo aProviderInfo : providerInfo) {
                /* We should always be able to find the Provider */
                Provider provider = (Provider) aProviderInfo.
                        getDependableFinder().getDependable(dd, aProviderInfo.getObjectId());
                dm.addDependency(conDesc, provider, lcc.getContextManager());
            }
		}

		/* Finally, invalidate off of the table descriptor(s)
		 * to ensure that any dependent statements get
		 * re-compiled.
		 */
		if (! forCreateTable) {
			dm.invalidateFor(td, DependencyManager.CREATE_CONSTRAINT, lcc);
		}
		if (constraintType == DataDictionary.FOREIGNKEY_CONSTRAINT) {
			if (SanityManager.DEBUG) {
				SanityManager.ASSERT(conDesc != null,
					"conDesc expected to be non-null");

				if (! (conDesc instanceof ForeignKeyConstraintDescriptor)) {
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
		SpliceLogUtils.trace(LOG, "manageIndexAction with table %s",td);
        /* Create the index, if there's one for this constraint */
        ConglomerateDescriptor[] conglomDescs;
        String backingIndexName;
        ConglomerateDescriptor conglomDesc = null;
        IndexConstantOperation iAction;
        if (indexAction instanceof IndexConstantOperation) {
            iAction = (IndexConstantOperation)indexAction;
            if ( iAction.getIndexName() == null ) {
				/* Set the index name */
                backingIndexName =  uuidFactory.createUUID().toString();
                iAction.setIndexName(backingIndexName);
            }
            else {
                backingIndexName = iAction.getIndexName();
            }
			/* Create the index */
            indexAction.executeConstantAction(activation);

			/* Get the conglomerate descriptor for the backing index */
            conglomDescs = td.getConglomerateDescriptors();

            for (int index = 0; index < conglomDescs.length; index++) {
                conglomDesc = conglomDescs[index];

				/* Check for conglomerate being an index first, since
				 * name is null for heap.
				 */
                if (conglomDesc.isIndex() && backingIndexName.equals(conglomDesc.getConglomerateName()))
                    break;
            }

            if (SanityManager.DEBUG) {
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
    boolean isForeignKeyConstraint() {
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
    public int[] genColumnPositions(TableDescriptor td, boolean columnsMustBeOrderable) throws StandardException {
        int[] baseColumnPositions;
        // Translate the base column names to column positions
        baseColumnPositions = new int[columnNames.length];
        for (int i = 0; i < columnNames.length; i++) {
            ColumnDescriptor columnDescriptor;

            // Look up the column in the data dictionary
            columnDescriptor = td.getColumnDescriptor(columnNames[i]);
            if (columnDescriptor == null) {
                throw StandardException.newException(SQLState.LANG_COLUMN_NOT_FOUND_IN_TABLE,
                        columnNames[i],tableName);
            }

			// Don't allow a column to be created on a non-orderable type
			// (for primaryKey and unique constraints)
			if ( columnsMustBeOrderable && ( ! columnDescriptor.getType().getTypeId().orderable(cf)))
				throw StandardException.newException(SQLState.LANG_COLUMN_NOT_ORDERABLE_DURING_EXECUTION, 
					columnDescriptor.getType().getTypeId().getSQLTypeName());

			// Remember the position in the base table of each column
			baseColumnPositions[i] = columnDescriptor.getPosition();
		}
		return baseColumnPositions;
	}

	/**
	  *	Get the text defining this constraint.
	  *
	  *	@return	constraint text
	  */
    String	getConstraintText() { 
    	return constraintText; 
    }

    @Override
	public String toString() {
		StringBuilder strbuf = new StringBuilder();
		strbuf.append( "CREATE CONSTRAINT " + constraintName );
		strbuf.append("\n=========================\n");
		if (columnNames == null)
			strbuf.append("columnNames == null\n");
		else {
			for (int ix=0; ix < columnNames.length; ix++) {
				strbuf.append("\n\tcol["+ix+"]"+ columnNames[ix]);
			}
		}
		strbuf.append("\n");
		strbuf.append(constraintText);
		strbuf.append("\n");
		if (otherConstraintInfo != null) {
			strbuf.append(otherConstraintInfo.toString());
		}
		strbuf.append("\n");
		return strbuf.toString();
	}

	public String getScopeName() {
		return String.format("Create Constraint %s (Table %s)", constraintName, tableName);
	}
}
