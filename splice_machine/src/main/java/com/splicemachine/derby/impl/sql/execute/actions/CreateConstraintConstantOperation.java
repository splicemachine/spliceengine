package com.splicemachine.derby.impl.sql.execute.actions;

import com.splicemachine.derby.impl.job.fk.CreateFkJobSubmitter;
import com.splicemachine.derby.impl.store.access.SpliceTransactionManager;
import org.apache.derby.catalog.UUID;
import org.apache.derby.catalog.types.ReferencedColumnsDescriptorImpl;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.reference.SQLState;
import org.apache.derby.iapi.services.loader.ClassFactory;
import org.apache.derby.iapi.services.sanity.SanityManager;
import org.apache.derby.iapi.services.uuid.UUIDFactory;
import org.apache.derby.iapi.sql.Activation;
import org.apache.derby.iapi.sql.conn.LanguageConnectionContext;
import org.apache.derby.iapi.sql.depend.DependencyManager;
import org.apache.derby.iapi.sql.depend.Provider;
import org.apache.derby.iapi.sql.depend.ProviderInfo;
import org.apache.derby.iapi.sql.dictionary.ColumnDescriptor;
import org.apache.derby.iapi.sql.dictionary.ConglomerateDescriptor;
import org.apache.derby.iapi.sql.dictionary.ConstraintDescriptor;
import org.apache.derby.iapi.sql.dictionary.DDUtils;
import org.apache.derby.iapi.sql.dictionary.DataDescriptorGenerator;
import org.apache.derby.iapi.sql.dictionary.DataDictionary;
import org.apache.derby.iapi.sql.dictionary.ForeignKeyConstraintDescriptor;
import org.apache.derby.iapi.sql.dictionary.ReferencedKeyConstraintDescriptor;
import org.apache.derby.iapi.sql.dictionary.SchemaDescriptor;
import org.apache.derby.iapi.sql.dictionary.TableDescriptor;
import org.apache.derby.iapi.sql.execute.ConstantAction;
import org.apache.derby.iapi.store.access.TransactionController;
import org.apache.derby.impl.sql.execute.ConstraintInfo;
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
										 ((CreateIndexConstantOperation)indexAction).getIndexTemplateRow(), lcc);
				}
				
				/* Create stored dependency on the referenced constraint */
				dm.addDependency(conDesc, referencedConstraint, lcc.getContextManager());
				//store constraint's dependency on REFERENCES privileges in the dependeny system
				storeConstraintDependenciesOnPrivileges
					(activation,
					 conDesc,
					 referencedConstraint.getTableId(),
					 providerInfo);


                // Use the task framework to add FK Write handler on remote nodes.
                new CreateFkJobSubmitter(dd, (SpliceTransactionManager) tc, referencedConstraint).submit();

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
			for (int ix = 0; ix < providerInfo.length; ix++) {
				Provider provider = null;
				/* We should always be able to find the Provider */
					provider = (Provider) providerInfo[ix].
						getDependableFinder().getDependable(dd, providerInfo[ix].getObjectId());
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
				strbuf.append("\n\tcol["+ix+"]"+columnNames[ix].toString());
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
}
