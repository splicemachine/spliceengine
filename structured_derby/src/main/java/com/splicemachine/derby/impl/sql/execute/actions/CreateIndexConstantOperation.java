package com.splicemachine.derby.impl.sql.execute.actions;

import java.io.IOException;
import java.util.Arrays;
import java.util.Properties;

import com.google.common.io.Closeables;
import com.splicemachine.si.api.TransactionManager;
import org.apache.derby.catalog.UUID;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.reference.SQLState;
import org.apache.derby.iapi.services.io.FormatableBitSet;
import org.apache.derby.iapi.services.loader.ClassFactory;
import org.apache.derby.iapi.services.sanity.SanityManager;
import org.apache.derby.iapi.sql.Activation;
import org.apache.derby.iapi.sql.conn.LanguageConnectionContext;
import org.apache.derby.iapi.sql.depend.DependencyManager;
import org.apache.derby.iapi.sql.dictionary.ColumnDescriptor;
import org.apache.derby.iapi.sql.dictionary.ColumnDescriptorList;
import org.apache.derby.iapi.sql.dictionary.ConglomerateDescriptor;
import org.apache.derby.iapi.sql.dictionary.ConglomerateDescriptorList;
import org.apache.derby.iapi.sql.dictionary.ConstraintDescriptor;
import org.apache.derby.iapi.sql.dictionary.DataDescriptorGenerator;
import org.apache.derby.iapi.sql.dictionary.DataDictionary;
import org.apache.derby.iapi.sql.dictionary.IndexRowGenerator;
import org.apache.derby.iapi.sql.dictionary.SchemaDescriptor;
import org.apache.derby.iapi.sql.dictionary.TableDescriptor;
import org.apache.derby.iapi.sql.execute.ConstantAction;
import org.apache.derby.iapi.sql.execute.ExecIndexRow;
import org.apache.derby.iapi.sql.execute.ExecRow;
import org.apache.derby.iapi.store.access.AccessFactoryGlobals;
import org.apache.derby.iapi.store.access.ColumnOrdering;
import org.apache.derby.iapi.store.access.ConglomerateController;
import org.apache.derby.iapi.store.access.GroupFetchScanController;
import org.apache.derby.iapi.store.access.RowLocationRetRowSource;
import org.apache.derby.iapi.store.access.TransactionController;
import org.apache.derby.iapi.types.DataTypeDescriptor;
import org.apache.derby.iapi.types.DataValueDescriptor;
import org.apache.derby.iapi.types.RowLocation;
import org.apache.derby.iapi.types.TypeId;
import org.apache.derby.impl.services.daemon.IndexStatisticsDaemonImpl;
import org.apache.derby.impl.sql.execute.IndexColumnOrder;
import org.apache.derby.impl.sql.execute.RowUtil;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.log4j.Logger;

import com.splicemachine.derby.ddl.DDLChange;
import com.splicemachine.derby.ddl.TentativeIndexDesc;
import com.splicemachine.derby.impl.store.access.SpliceAccessManager;
import com.splicemachine.derby.utils.Exceptions;
import com.splicemachine.derby.utils.SpliceUtils;
import com.splicemachine.si.api.HTransactorFactory;
import com.splicemachine.si.impl.TransactionId;
import com.splicemachine.utils.SpliceLogUtils;


public class CreateIndexConstantOperation extends IndexConstantOperation {
    private static final Logger LOG = Logger.getLogger(CreateIndexConstantOperation.class);
    /**
     * Is this for a CREATE TABLE, i.e. it is
     * for a constraint declared in a CREATE TABLE
     * statement that requires a backing index.
     */
    private final boolean forCreateTable;
	private boolean			unique;
	private boolean			uniqueWithDuplicateNulls;
	private String			indexType;
	private String[]		columnNames;
	private boolean[]		isAscending;
	private boolean			isConstraint;
	private UUID			conglomerateUUID;
	private Properties		properties;
    private IndexRowGenerator irg;
	private ExecRow indexTemplateRow;

	/** Conglomerate number for the conglomerate created by this
	 * constant action; -1L if this constant action has not been
	 * executed.  If this constant action doesn't actually create
	 * a new conglomerate--which can happen if it finds an existing
	 * conglomerate that satisfies all of the criteria--then this
	 * field will hold the conglomerate number of whatever existing
	 * conglomerate was found.
	 */
	private long conglomId;

	/** Conglomerate number of the physical conglomerate that we
	 * will "replace" using this constant action.  That is, if
	 * the purpose of this constant action is to create a new physical
	 * conglomerate to replace a dropped physical conglomerate, then
	 * this field holds the conglomerate number of the dropped physical
	 * conglomerate. If -1L then we are not replacing a conglomerate,
	 * we're simply creating a new index (and backing physical
	 * conglomerate) as normal.
	 */
	private long droppedConglomNum;

	// CONSTRUCTORS
	/**
     * 	Make the ConstantAction to create an index.
     *
     * @param forCreateTable                Being executed within a CREATE TABLE
     *                                      statement
     * @param unique		                True means it will be a unique index
     * @param uniqueWithDuplicateNulls      True means index check and disallow
     *                                      any duplicate key if key has no
     *                                      column with a null value.  If any
     *                                      column in the key has a null value,
     *                                      no checking is done and insert will
     *                                      always succeed.
     * @param indexType	                    type of index (BTREE, for example)
     * @param schemaName	                schema that table (and index)
     *                                      lives in.
     * @param indexName	                    Name of the index
     * @param tableName	                    Name of table the index will be on
     * @param tableId		                UUID of table
     * @param columnNames	                Names of the columns in the index,
     *                                      in order
     * @param isAscending	                Array of booleans telling asc/desc
     *                                      on each column
     * @param isConstraint	                TRUE if index is backing up a
     *                                      constraint, else FALSE
     * @param conglomerateUUID	            ID of conglomerate
     * @param properties	                The optional properties list
     *                                      associated with the index.
     */

	public CreateIndexConstantOperation(
            boolean         forCreateTable,
            boolean			unique,
            boolean			uniqueWithDuplicateNulls,
            String			indexType,
            String			schemaName,
            String			indexName,
            String			tableName,
            UUID			tableId,
            String[]		columnNames,
            boolean[]		isAscending,
            boolean			isConstraint,
            UUID			conglomerateUUID,
            Properties		properties) {
		super(tableId, indexName, tableName, schemaName);
		SpliceLogUtils.trace(LOG, "CreateIndexConstantOperation for table %s.%s with index named %s for columns %s",schemaName,tableName,indexName,Arrays.toString(columnNames));
        this.forCreateTable             = forCreateTable;
		this.unique                     = unique;
		this.uniqueWithDuplicateNulls   = uniqueWithDuplicateNulls;
		this.indexType                  = indexType;
		this.columnNames                = columnNames;
		this.isAscending                = isAscending;
		this.isConstraint               = isConstraint;
		this.conglomerateUUID           = conglomerateUUID;
		this.properties                 = properties;
		this.conglomId                  = -1L;
		this.droppedConglomNum          = -1L;
	}

	/**
	 * Make a ConstantAction that creates a new physical conglomerate
	 * based on index information stored in the received descriptors.
	 * Assumption is that the received ConglomerateDescriptor is still
	 * valid (meaning it has corresponding entries in the system tables
	 * and it describes some constraint/index that has _not_ been
	 * dropped--though the physical conglomerate underneath has).
	 *
	 * This constructor is used in cases where the physical conglomerate
	 * for an index has been dropped but the index still exists. That
	 * can happen if multiple indexes share a physical conglomerate but
	 * then the conglomerate is dropped as part of "drop index" processing
	 * for one of the indexes. (Note that "indexes" here includes indexes
	 * which were created to back constraints.) In that case we have to
	 * create a new conglomerate to satisfy the remaining sharing indexes,
	 * so that's what we're here for.  See ConglomerateDescriptor.drop()
	 * for details on when that is necessary.
	 */
	CreateIndexConstantOperation(ConglomerateDescriptor srcCD, TableDescriptor td, Properties properties) {
		super(td.getUUID(),srcCD.getConglomerateName(), td.getName(), td.getSchemaName());
		SpliceLogUtils.trace(LOG, "CreateIndexConstantOperation for conglomerate {%s} and table {%s} with properties {%s}", srcCD,td,properties);
		this.forCreateTable = false;

		/* We get here when a conglomerate has been dropped and we
		 * need to create (or find) another one to fill its place.
		 * At this point the received conglomerate descriptor still
		 * references the old (dropped) conglomerate, so we can
		 * pull the conglomerate number from there.
		 */
		this.droppedConglomNum = srcCD.getConglomerateNumber();

		/* Plug in the rest of the information from the received
		 * descriptors.
		 */
		IndexRowGenerator irg = srcCD.getIndexDescriptor();
		this.unique = irg.isUnique();
		this.uniqueWithDuplicateNulls = irg.isUniqueWithDuplicateNulls();
		this.indexType = irg.indexType();
		this.columnNames = srcCD.getColumnNames();
		this.isAscending = irg.isAscending();
		this.isConstraint = srcCD.isConstraint();
		this.conglomerateUUID = srcCD.getUUID();
		this.properties = properties;
		this.conglomId = -1L;

		/* The ConglomerateDescriptor may not know the names of
		 * the columns it includes.  If that's true (which seems
		 * to be the more common case) then we have to build the
		 * list of ColumnNames ourselves.
		 */
		if (columnNames == null) {
			int [] baseCols = irg.baseColumnPositions();
			columnNames = new String[baseCols.length];
			ColumnDescriptorList colDL = td.getColumnDescriptorList();
			for (int i = 0; i < baseCols.length; i++) {
				columnNames[i] = colDL.elementAt(baseCols[i]-1).getColumnName();
			}
		}
	}



	public	String	toString() {
		return "CREATE INDEX " + indexName;
	}

	/**
	 *	This is the guts of the Execution-time logic for
     *  creating an index.
     *
     *  <P>
     *  A index is represented as:
     *  <UL>
     *  <LI> ConglomerateDescriptor.
     *  </UL>
     *  No dependencies are created.
   	 *
     *  @see ConglomerateDescriptor
     *  @see SchemaDescriptor
	 *	@see ConstantAction#executeConstantAction
	 *
	 * @exception StandardException		Thrown on failure
	 */
	public void executeConstantAction( Activation activation ) throws StandardException {
		SpliceLogUtils.trace(LOG, "executeConstantActivation with activation %s",activation);
		TableDescriptor 			td;
		UUID 						toid;
		ColumnDescriptor			columnDescriptor;
		int[]						baseColumnPositions;
		IndexRowGenerator			indexRowGenerator = null;
		ExecRow[]					baseRows;
		ExecIndexRow[]				indexRows;
		ExecRow[]					compactBaseRows;
		GroupFetchScanController    scan;
		RowLocationRetRowSource	    rowSource;
		long						sortId;
		int							maxBaseColumnPosition = -1;

		LanguageConnectionContext lcc = activation.getLanguageConnectionContext();
		DataDictionary dd = lcc.getDataDictionary();
		DependencyManager dm = dd.getDependencyManager();
        TransactionController parent = lcc.getTransactionExecute();
        TransactionController tc = lcc.getTransactionExecute().startNestedUserTransaction(false, true);
        dd.startWriting(lcc);
		SchemaDescriptor sd = dd.getSchemaDescriptor(schemaName, tc, true) ;
        ConglomerateDescriptor existingIndex = dd.getConglomerateDescriptor(indexName, sd, false);
        if (existingIndex != null) {
            throw StandardException.newException(SQLState.LANG_OBJECT_ALREADY_EXISTS_IN_OBJECT,
                    existingIndex.getDescriptorType(),
                    existingIndex.getDescriptorName(),
                    sd.getDescriptorType(),
                    sd.getDescriptorName());
        }
		td = activation.getDDLTableDescriptor();
		if (td == null) {
			td = tableId != null?dd.getTableDescriptor(tableId):dd.getTableDescriptor(tableName, sd, tc);
		}
		if (td == null)
			throw StandardException.newException(SQLState.LANG_CREATE_INDEX_NO_TABLE, indexName, tableName);

		if (td.getTableType() == TableDescriptor.SYSTEM_TABLE_TYPE) {
			throw StandardException.newException(SQLState.LANG_CREATE_SYSTEM_INDEX_ATTEMPTED, indexName, tableName);
		}
        try {

            // invalidate any prepared statements that
            // depended on this table (including this one)
            if (! forCreateTable)
                dm.invalidateFor(td, DependencyManager.CREATE_INDEX, lcc);

            SpliceLogUtils.trace(LOG, "Translation Base Column Names");
            // Translate the base column names to column positions
            baseColumnPositions = new int[columnNames.length];
            for (int i = 0; i < columnNames.length; i++) {
                // Look up the column in the data dictionary
                columnDescriptor = td.getColumnDescriptor(columnNames[i]);
                if (columnDescriptor == null) {
                    throw StandardException.newException(SQLState.LANG_COLUMN_NOT_FOUND_IN_TABLE, columnNames[i],tableName);
                }

                TypeId typeId = columnDescriptor.getType().getTypeId();

                // Don't allow a column to be created on a non-orderable type
                ClassFactory cf = lcc.getLanguageConnectionFactory().getClassFactory();
                boolean isIndexable = typeId.orderable(cf);

                if (isIndexable && typeId.userType()) {
                    String userClass = typeId.getCorrespondingJavaTypeName();

                    // Don't allow indexes to be created on classes that
                    // are loaded from the database. This is because recovery
                    // won't be able to see the class and it will need it to
                    // run the compare method.
                    try {
                        if (cf.isApplicationClass(cf.loadApplicationClass(userClass)))
                            isIndexable = false;
                    } catch (ClassNotFoundException cnfe) {
                        // shouldn't happen as we just check the class is orderable
                        isIndexable = false;
                    }
                }

                if (!isIndexable)
                    throw StandardException.newException(SQLState.LANG_COLUMN_NOT_ORDERABLE_DURING_EXECUTION, typeId.getSQLTypeName());

                // Remember the position in the base table of each column
                baseColumnPositions[i] = columnDescriptor.getPosition();

                if (maxBaseColumnPosition < baseColumnPositions[i])
                    maxBaseColumnPosition = baseColumnPositions[i];
            }

            /* The code below tries to determine if the index that we're about
             * to create can "share" a conglomerate with an existing index.
             * If so, we will use a single physical conglomerate--namely, the
             * one that already exists--to support both indexes. I.e. we will
             * *not* create a new conglomerate as part of this constant action.
             */

            // check if we have similar indices already for this table
            ConglomerateDescriptor[] congDescs = td.getConglomerateDescriptors();
            boolean shareExisting = false;
            for (int i = 0; i < congDescs.length; i++) {
                ConglomerateDescriptor cd = congDescs[i];
                if ( ! cd.isIndex())
                    continue;

                if (droppedConglomNum == cd.getConglomerateNumber()) {
                    /* We can't share with any conglomerate descriptor
                     * whose conglomerate number matches the dropped
                     * conglomerate number, because that descriptor's
                     * backing conglomerate was dropped, as well.  If
                     * we're going to share, we have to share with a
                     * descriptor whose backing physical conglomerate
                     * is still around.
                     */
                    continue;
                }

                IndexRowGenerator irg = cd.getIndexDescriptor();
                int[] bcps = irg.baseColumnPositions();
                boolean[] ia = irg.isAscending();
                int j = 0;

                /* The conditions which allow an index to share an existing
                 * conglomerate are as follows:
                 *
                 * 1. the set of columns (both key and include columns) and their
                 *  order in the index is the same as that of an existing index AND
                 *
                 * 2. the ordering attributes are the same AND
                 *
                 * 3. one of the following is true:
                 *    a) the existing index is unique, OR
                 *    b) the existing index is non-unique with uniqueWhenNotNulls
                 *       set to TRUE and the index being created is non-unique, OR
                 *    c) both the existing index and the one being created are
                 *       non-unique and have uniqueWithDuplicateNulls set to FALSE.
                 */
                boolean possibleShare = (irg.isUnique() || !unique) &&
                    (bcps.length == baseColumnPositions.length);

                //check if existing index is non unique and uniqueWithDuplicateNulls
                //is set to true (backing index for unique constraint)
                if (possibleShare && !irg.isUnique ()) {
                    /* If the existing index has uniqueWithDuplicateNulls set to
                     * TRUE it can be shared by other non-unique indexes; otherwise
                     * the existing non-unique index has uniqueWithDuplicateNulls
                     * set to FALSE, which means the new non-unique conglomerate
                     * can only share if it has uniqueWithDuplicateNulls set to
                     * FALSE, as well.
                     */
                    possibleShare = (irg.isUniqueWithDuplicateNulls() || ! uniqueWithDuplicateNulls);
                }

                if (possibleShare && indexType.equals(irg.indexType())) {
                    for (; j < bcps.length; j++) {
                        if ((bcps[j] != baseColumnPositions[j]) || (ia[j] != isAscending[j]))
                            break;
                    }
                }

                if (j == baseColumnPositions.length) {	// share
                    /*
                     * Don't allow users to create a duplicate index. Allow if being done internally
                     * for a constraint
                     */
                    if (!isConstraint) {
                        activation.addWarning(StandardException.newWarning(SQLState.LANG_INDEX_DUPLICATE,cd.getConglomerateName()));
                        return;
                    }

                    /* Sharing indexes share the physical conglomerate
                     * underneath, so pull the conglomerate number from
                     * the existing conglomerate descriptor.
                     */
                    conglomId = cd.getConglomerateNumber();

                    /* We create a new IndexRowGenerator because certain
                     * attributes--esp. uniqueness--may be different between
                     * the index we're creating and the conglomerate that
                     * already exists.  I.e. even though we're sharing a
                     * conglomerate, the new index is not necessarily
                     * identical to the existing conglomerate. We have to
                     * keep track of that info so that if we later drop
                     * the shared physical conglomerate, we can figure out
                     * what this index (the one we're creating now) is
                     * really supposed to look like.
                     */
                    indexRowGenerator =
                        new IndexRowGenerator(
                            indexType, unique, uniqueWithDuplicateNulls,
                            baseColumnPositions,
                            isAscending,
                            baseColumnPositions.length);

                    //DERBY-655 and DERBY-1343
                    // Sharing indexes will have unique logical conglomerate UUIDs.
                    conglomerateUUID = dd.getUUIDFactory().createUUID();
                    shareExisting = true;
                    break;
                }
            }

            /* If we have a droppedConglomNum then the index we're about to
             * "create" already exists--i.e. it has an index descriptor and
             * the corresponding information is already in the system catalogs.
             * The only thing we're missing, then, is the physical conglomerate
             * to back the index (because the old conglomerate was dropped).
             */
            boolean alreadyHaveConglomDescriptor = (droppedConglomNum > -1L);

            /* If this index already has an essentially same one, we share the
             * conglomerate with the old one, and just simply add a descriptor
             * entry into SYSCONGLOMERATES--unless we already have a descriptor,
             * in which case we don't even need to do that.
             */
            DataDescriptorGenerator ddg = dd.getDataDescriptorGenerator();
            if (shareExisting && !alreadyHaveConglomDescriptor) {
                ConglomerateDescriptor cgd =
                    ddg.newConglomerateDescriptor(conglomId, indexName, true,
                                              indexRowGenerator, isConstraint,
                                              conglomerateUUID, td.getUUID(), sd.getUUID() );
                dd.addDescriptor(cgd, sd, DataDictionary.SYSCONGLOMERATES_CATALOG_NUM, false, tc);
                // add newly added conglomerate to the list of conglomerate
                // descriptors in the td.
                ConglomerateDescriptorList cdl = td.getConglomerateDescriptorList();
                cdl.add(cgd);
                // can't just return yet, need to get member "indexTemplateRow"
                // because create constraint may use it

            }

            // Describe the properties of the index to the store using Properties
            // RESOLVE: The following properties assume a BTREE index.
            Properties	indexProperties;

            if (properties != null) {
                indexProperties = properties;
            }
            else {
                indexProperties = new Properties();
            }

            SpliceLogUtils.trace(LOG, "Here XXX");

            // Tell it the conglomerate id of the base table
            indexProperties.put("baseConglomerateId",Long.toString(td.getHeapConglomerateId()));

            // All indexes are unique because they contain the RowLocation.
            // The number of uniqueness columns must include the RowLocation
            // if the user did not specify a unique index.
            indexProperties.put("nUniqueColumns",
                                       Integer.toString(unique ? baseColumnPositions.length :
                                                                baseColumnPositions.length + 1));

            if (uniqueWithDuplicateNulls) {
                // Derby made the distinction between "unique" and "uniqueWithDuplicateNulls"
                // with different behavior for each -- IndexDescriptor.isUnique() returned
                // false when IndexDescriptor.isUniqueWithDuplicateNulls() returned true,
                // for instance.
                // We don't make such a distinction.  Unique is always unique even when
                // uniqueWithDuplicateNulls is true.
                //
                indexProperties.put("uniqueWithDuplicateNulls", Boolean.toString(true));
                unique = true;
            }
            // By convention, the row location column is the last column
            indexProperties.put("rowLocationColumn", Integer.toString(baseColumnPositions.length));

            // For now, all columns are key fields, including the RowLocation
            indexProperties.put("nKeyFields",Integer.toString(baseColumnPositions.length + 1));

            // For now, assume that all index columns are ordered columns
            if (! shareExisting) {
                if (dd.checkVersion(DataDictionary.DD_VERSION_DERBY_10_4, null)) {
                    indexRowGenerator = new IndexRowGenerator(
                                                indexType,
                                                unique,
                                                uniqueWithDuplicateNulls,
                                                baseColumnPositions,
                                                isAscending,
                                                baseColumnPositions.length);
                }
                else {
                    indexRowGenerator = new IndexRowGenerator(
                                                indexType,
                                                unique,
                                                baseColumnPositions,
                                                isAscending,
                                                baseColumnPositions.length);
                }
            }

            /* Now add the rows from the base table to the conglomerate.
             * We do this by scanning the base table and inserting the
             * rows into a sorter before inserting from the sorter
             * into the index.  This gives us better performance
             * and a more compact index.
             */

            rowSource = null;
            sortId = 0;
            boolean needToDropSort = false;	// set to true once the sorter is created

            /* bulkFetchSIze will be 16 (for now) unless
             * we are creating the table in which case it
             * will be 1.  Too hard to remove scan when
             * creating index on new table, so minimize
             * work where we can.
             */
            int bulkFetchSize = (forCreateTable) ? 1 : 16;
            int numColumns = td.getNumberOfColumns();
            int approximateRowSize = 0;

            // Create the FormatableBitSet for mapping the partial to full base row
            FormatableBitSet bitSet = new FormatableBitSet(numColumns+1);
            for (int index = 0; index < baseColumnPositions.length; index++) {
                bitSet.set(baseColumnPositions[index]);
            }
            FormatableBitSet zeroBasedBitSet = RowUtil.shift(bitSet, 1);

            // Start by opening a full scan on the base table.
            scan = tc.openGroupFetchScan(
                                td.getHeapConglomerateId(),
                                false,	// hold
                                0,	// open base table read only
                                TransactionController.MODE_TABLE,
                                TransactionController.ISOLATION_SERIALIZABLE,
                                zeroBasedBitSet,    // all fields as objects
                                (DataValueDescriptor[]) null,	// startKeyValue
                                0,		// not used when giving null start posn.
                                null,	// qualifier
                                (DataValueDescriptor[]) null,	// stopKeyValue
                                0);		// not used when giving null stop posn.

            // Create an array to put base row template
            baseRows = new ExecRow[bulkFetchSize];
            indexRows = new ExecIndexRow[bulkFetchSize];
            compactBaseRows = new ExecRow[bulkFetchSize];

            ColumnOrdering[] order;
            try {
                // Create the array of base row template
                for (int i = 0; i < bulkFetchSize; i++) {
                    // create a base row template
                    baseRows[i] = activation.getExecutionFactory().getValueRow(maxBaseColumnPosition);

                    // create an index row template
                    indexRows[i] = indexRowGenerator.getIndexRowTemplate();

                    // create a compact base row template
                    compactBaseRows[i] = activation.getExecutionFactory().getValueRow(
                                                        baseColumnPositions.length);
                }

                indexTemplateRow = indexRows[0];

                // Fill the partial row with nulls of the correct type
                ColumnDescriptorList cdl = td.getColumnDescriptorList();
                int					 cdlSize = cdl.size();
                for (int index = 0, numSet = 0; index < cdlSize; index++) {
                    if (! zeroBasedBitSet.get(index)) {
                        continue;
                    }
                    numSet++;
                    ColumnDescriptor cd = (ColumnDescriptor) cdl.elementAt(index);
                    DataTypeDescriptor dts = cd.getType();

                    for (int i = 0; i < bulkFetchSize; i++) {
                        // Put the column in both the compact and sparse base rows
                        baseRows[i].setColumn(index + 1,dts.getNull());
                        compactBaseRows[i].setColumn(numSet,baseRows[i].getColumn(index + 1));
                    }
                    // Calculate the approximate row size for the index row
                    approximateRowSize += dts.getTypeId().getApproximateLengthInBytes(dts);
                }

                // Get an array of RowLocation template
                RowLocation rl[] = new RowLocation[bulkFetchSize];
                for (int i = 0; i < bulkFetchSize; i++) {
                    rl[i] = scan.newRowLocationTemplate();
                    // Get an index row based on the base row
                    indexRowGenerator.getIndexRow(compactBaseRows[i], rl[i], indexRows[i], bitSet);
                }

                /* now that we got indexTemplateRow, done for sharing index
                 */
                if (shareExisting) // Sharing leaves...
                    return;

                /* For non-unique indexes, we order by all columns + the RID.
                 * For unique indexes, we just order by the columns.
                 * We create a unique index observer for unique indexes
                 * so that we can catch duplicate key.
                 * We create a basic sort observer for non-unique indexes
                 * so that we can reuse the wrappers during an external
                 * sort.
                 */
                int             numColumnOrderings;
                Properties      sortProperties = null;
                if (unique || uniqueWithDuplicateNulls) {
                    // if the index is a constraint, use constraintname in
                    // possible error message
                    String indexOrConstraintName = indexName;
                    if  (conglomerateUUID != null) {
                        ConglomerateDescriptor cd = dd.getConglomerateDescriptor(conglomerateUUID);
                        if ((isConstraint) &&  (cd != null && cd.getUUID() != null && td != null)) {
                            ConstraintDescriptor conDesc = dd.getConstraintDescriptor(td, cd.getUUID());
                            indexOrConstraintName = conDesc.getConstraintName();
                        }
                    }

                    if (unique) {
                        numColumnOrderings = baseColumnPositions.length;
                    }
                    else {
                        // unique with duplicate nulls allowed.
                        numColumnOrderings = baseColumnPositions.length + 1;

                        // tell transaction controller to use the unique with
                        // duplicate nulls sorter, when making createSort() call.
                        sortProperties = new Properties();
                        sortProperties.put(
                            AccessFactoryGlobals.IMPL_TYPE,
                            AccessFactoryGlobals.SORT_UNIQUEWITHDUPLICATENULLS_EXTERNAL);
                        //use sort operator which treats nulls unequal
                    }
                }
                else {
                    numColumnOrderings = baseColumnPositions.length + 1;
                }

                order= new ColumnOrdering[numColumnOrderings];
                for (int i=0; i < numColumnOrderings; i++) {
                    order[i] = new IndexColumnOrder(i, unique || i < numColumnOrderings - 1 ? isAscending[i] : true);
                }

                conglomId = tc.createConglomerate(indexType, indexTemplateRow.getRowArray(), order, indexRowGenerator.getColumnCollationIds(
                            td.getColumnDescriptorList()), indexProperties, TransactionController.IS_DEFAULT);

            }
            finally {
                /* close the table scan */
                if (scan != null)
                    scan.close();
            }

            ConglomerateController indexController = tc.openConglomerate(conglomId, false, 0, TransactionController.MODE_TABLE,TransactionController.ISOLATION_SERIALIZABLE);

            // Check to make sure that the conglomerate can be used as an index
            if ( ! indexController.isKeyed()) {
                indexController.close();
                throw StandardException.newException(SQLState.LANG_NON_KEYED_INDEX, indexName,indexType);
            }
            indexController.close();

            //
            // Create a conglomerate descriptor with the conglomId filled
            // in and add it--if we don't have one already.
            //
            SpliceLogUtils.trace(LOG,"Huh?");

            if (!alreadyHaveConglomDescriptor) {
                SpliceLogUtils.trace(LOG,"! Conglom Descriptor");

                ConglomerateDescriptor cgd = ddg.newConglomerateDescriptor(conglomId, indexName, true, indexRowGenerator, isConstraint, conglomerateUUID, td.getUUID(), sd.getUUID() );
                dd.addDescriptor(cgd, sd,DataDictionary.SYSCONGLOMERATES_CATALOG_NUM, false, tc);

                // add newly added conglomerate to the list of conglomerate
                // descriptors in the td.
                ConglomerateDescriptorList cdl = td.getConglomerateDescriptorList();
								//noinspection unchecked
								cdl.add(cgd);

                /* Since we created a new conglomerate descriptor, load
                 * its UUID into the corresponding field, to ensure that
                 * it is properly set in the StatisticsDescriptor created
                 * below.
                 */
                conglomerateUUID = cgd.getUUID();
            }

            boolean[] descColumns = new boolean[isAscending.length];
            for(int i=0;i<isAscending.length;i++){
               descColumns[i] = !isAscending[i];
            }

            // Perform tentative DDL change
            TransactionId tentativeTransaction;
            TransactionManager transactor = HTransactorFactory.getTransactionManager();
            try {
                tentativeTransaction = transactor.beginTransaction();
            } catch (IOException e) {
                LOG.error("Couldn't start transaction for tentative DDL operation");
                throw Exceptions.parseException(e);
            }
            TentativeIndexDesc tentativeIndexDesc = new TentativeIndexDesc(conglomId, td.getHeapConglomerateId(),
                    baseColumnPositions, unique,
                    uniqueWithDuplicateNulls,
                    SpliceUtils.bitSetFromBooleanArray(descColumns));
            DDLChange ddlChange = new DDLChange(tentativeTransaction.getTransactionIdString(),
                    DDLChange.TentativeType.CREATE_INDEX);
            ddlChange.setTentativeDDLDesc(tentativeIndexDesc);
            ddlChange.setParentTransactionId(tc.getActiveStateTxIdString());

            notifyMetadataChange(ddlChange);

            final long tableConglomId = td.getHeapConglomerateId();
            HTableInterface table = SpliceAccessManager.getHTable(Long.toString(tableConglomId).getBytes());
            try{
                // Add the indexes to the exisiting regions
                createIndex(activation, ddlChange, table, td);

                TransactionId indexTransaction = getIndexTransaction(parent, tc, tentativeTransaction, transactor,tableConglomId);

                populateIndex(activation, baseColumnPositions, descColumns, tableConglomId, table, indexTransaction, tentativeIndexDesc);
                //only commit the index transaction if the job actually completed
                transactor.commit(indexTransaction);
            }finally{
                Closeables.closeQuietly(table);
            }
        } catch (StandardException se) {
            tc.abort();
            throw se;
        } catch (Throwable t) {
            tc.abort();
            throw Exceptions.parseException(t);
        }
        try {
            HTransactorFactory.getTransactionManager().commit(new TransactionId(getTransactionId(tc)));
        } catch (IOException e) {
            throw Exceptions.parseException(e);
        }
    }




    /**
     * Determines if a statistics entry is to be added for the index.
     * <p>
     * As an optimization, it may be better to not write a statistics entry to
     * SYS.SYSSTATISTICS. If it isn't needed by Derby as part of query
     * optimization there is no reason to spend resources keeping the
     * statistics up to date.
     *
     * @param dd the data dictionary
     * @param irg the index row generator
     * @param numRows the number of rows in the index
     * @return {@code true} if statistics should be written to
     *      SYS.SYSSTATISTICS, {@code false} otherwise.
     * @throws StandardException if accessing the data dictionary fails
     */
    @SuppressWarnings("UnusedDeclaration")
		private boolean addStatistics(DataDictionary dd,IndexRowGenerator irg,long numRows) throws StandardException {
    	SpliceLogUtils.trace(LOG, "addStatistics for index %s",irg.getIndexDescriptor());
        boolean add = (numRows > 0);
        if (dd.checkVersion(DataDictionary.DD_VERSION_DERBY_10_9, null) &&
                // This horrible piece of code will hopefully go away soon!
               ((IndexStatisticsDaemonImpl)dd.getIndexStatsRefresher(false)).skipDisposableStats) {
            if (add && irg.isUnique() && irg.numberOfOrderedColumns() == 1) {
                // Do not add statistics for single-column unique indexes.
                add = false;
            }
        }
        return add;
    }

	public ExecRow getIndexTemplateRow() {
		return indexTemplateRow;
	}

	/**
	 * Get the conglomerate number for the conglomerate that was
	 * created by this constant action.  Will return -1L if the
	 * constant action has not yet been executed.  This is used
	 * for updating conglomerate descriptors which share a
	 * conglomerate that has been dropped, in which case those
	 * "sharing" descriptors need to point to the newly-created
	 * conglomerate (the newly-created conglomerate replaces
	 * the dropped one).
	 */
	public long getCreatedConglomNumber() {
		if (SanityManager.DEBUG) {
			if (conglomId == -1L) {
				SanityManager.THROWASSERT(
					"Called getCreatedConglomNumber() on a CreateIndex" +
					"ConstantAction before the action was executed.");
			}
		}
		return conglomId;
	}

	/**
	 * If the purpose of this constant action was to "replace" a
	 * dropped physical conglomerate, then this method returns the
	 * conglomerate number of the dropped conglomerate.  Otherwise
	 * this method will end up returning -1.
	 */
	public long getReplacedConglomNumber() {
		return droppedConglomNum;
	}

	/**
	 * Get the UUID for the conglomerate descriptor that was created
	 * (or re-used) by this constant action.
	 */
	public UUID getCreatedUUID() {
		return conglomerateUUID;
	}

    @Override
    protected boolean waitsForConcurrentTransactions() {
        return true;
    }
}
