package com.splicemachine.derby.impl.sql.execute.actions;

import com.google.common.io.Closeables;
import com.splicemachine.derby.ddl.DDLChange;
import com.splicemachine.derby.ddl.DDLChangeType;
import com.splicemachine.derby.ddl.TentativeIndexDesc;
import com.splicemachine.derby.impl.store.access.SpliceAccessManager;
import com.splicemachine.derby.impl.store.access.SpliceTransaction;
import com.splicemachine.derby.impl.store.access.SpliceTransactionManager;
import com.splicemachine.derby.impl.store.access.hbase.HBaseRowLocation;
import com.splicemachine.derby.utils.Exceptions;
import com.splicemachine.derby.utils.SpliceUtils;
import com.splicemachine.primitives.BooleanArrays;
import com.splicemachine.si.api.TransactionLifecycle;
import com.splicemachine.si.api.Txn;
import com.splicemachine.si.api.TxnView;
import com.splicemachine.utils.SpliceLogUtils;
import org.apache.derby.catalog.IndexDescriptor;
import org.apache.derby.catalog.UUID;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.reference.SQLState;
import org.apache.derby.iapi.services.io.FormatableBitSet;
import org.apache.derby.iapi.services.sanity.SanityManager;
import org.apache.derby.iapi.sql.Activation;
import org.apache.derby.iapi.sql.PreparedStatement;
import org.apache.derby.iapi.sql.ResultSet;
import org.apache.derby.iapi.sql.StatementType;
import org.apache.derby.iapi.sql.conn.LanguageConnectionContext;
import org.apache.derby.iapi.sql.depend.DependencyManager;
import org.apache.derby.iapi.sql.dictionary.*;
import org.apache.derby.iapi.sql.execute.ConstantAction;
import org.apache.derby.iapi.sql.execute.ExecIndexRow;
import org.apache.derby.iapi.sql.execute.ExecRow;
import org.apache.derby.iapi.store.access.*;
import org.apache.derby.iapi.types.DataValueDescriptor;
import org.apache.derby.iapi.types.RowLocation;
import org.apache.derby.impl.sql.execute.ColumnInfo;
import org.apache.derby.impl.sql.execute.IndexColumnOrder;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Properties;

/**
 *	This class  describes actions that are ALWAYS performed for an
 *	ALTER TABLE Statement at Execution time.
 *
 */

public class AlterTableConstantOperation extends IndexConstantOperation implements RowLocationRetRowSource {
    private static final Logger LOG = Logger.getLogger(AlterTableConstantOperation.class);
    // copied from constructor args and stored locally.
    protected SchemaDescriptor			sd;
    protected String						tableName;
    protected UUID						schemaId;
    protected ColumnInfo[]				columnInfo;
    private	    ConstraintConstantOperation[]	constraintActions;
    private	    char						lockGranularity;
    protected long						tableConglomerateId;
    protected int						    behavior;

    protected String						indexNameForStatistics;

    private     int						    numIndexes;
    private     long[]					    indexConglomerateNumbers;
    private     ConglomerateController	    compressHeapCC;
    private     ExecIndexRow[]			    indexRows;
    private	    GroupFetchScanController    compressHeapGSC;
    protected IndexRowGenerator[]		    compressIRGs;
    private     int						    droppedColumnPosition;
    private     ColumnOrdering[][]		    ordering;
    private     int[][]		                collation;

    private	TableDescriptor 		        td;

    // CONSTRUCTORS
    private LanguageConnectionContext lcc;
    private DataDictionary dd;
    private DependencyManager dm;
    private TransactionController tc;
    private Activation activation;

    /**
     *	Make the AlterAction for an ALTER TABLE statement.
     *
     * @param sd              descriptor for the table's schema.
     *  @param tableName          Name of table.
     * @param tableId            UUID of table
     * @param tableConglomerateId  heap conglomerate number of table
     * @param columnInfo          Information on all the columns in the table.
     * @param constraintActions  ConstraintConstantAction[] for constraints
     * @param lockGranularity      The lock granularity.
     * @param behavior            drop behavior for dropping column
     * @param indexNameForStatistics  Will name the index whose statistics
     */
    public AlterTableConstantOperation(
            SchemaDescriptor sd,
            String tableName,
            UUID tableId,
            long tableConglomerateId,
            ColumnInfo[] columnInfo,
            ConstantAction[] constraintActions,
            char lockGranularity,
            int behavior,
            String indexNameForStatistics) {
        super(tableId);
        if (LOG.isTraceEnabled())
            SpliceLogUtils.trace(LOG, "instantiating AlterTableConstantOperation for table {%s.%s} with ColumnInfo {%s} and constraintActions {%s}",sd!=null?sd.getSchemaName():"default",tableName, Arrays.toString(columnInfo), Arrays.toString(constraintActions));
        this.sd                     = sd;
        this.tableName              = tableName;
        this.tableConglomerateId    = tableConglomerateId;
        this.columnInfo             = columnInfo;
        this.constraintActions      = (ConstraintConstantOperation[]) constraintActions;
        this.lockGranularity        = lockGranularity;
        this.behavior               = behavior;
        this.indexNameForStatistics = indexNameForStatistics;
        if (SanityManager.DEBUG)
            SanityManager.ASSERT(sd != null, "schema descriptor is null");
    }

    public	String	toString() {
        return "ALTER TABLE " + tableName;
    }

    /**
     * Run this constant action.
     *
     * @param activation the activation in which to run the action
     * @throws StandardException if an error happens during execution
     * of the action
     */
    @Override
    public void executeConstantAction(Activation activation) throws StandardException {
        SpliceLogUtils.trace(LOG, "executeConstantAction with activation %s",activation);

        TransactionController userTxnController = activation.getTransactionController();
        SpliceTransactionManager child = (SpliceTransactionManager)userTxnController.startNestedUserTransaction(false,false);
        ((SpliceTransaction)child.getRawTransaction()).elevate(Long.toString(tableConglomerateId).getBytes());
        lcc = activation.getLanguageConnectionContext();
        tc = child;
        lcc.pushNestedTransaction(child);
        try {
            //body is executing inside of a child transaction now
            executeConstantActionBody(activation);
        } catch(StandardException se){
            child.abort();
            throw se;
        } finally {
            lcc.popNestedTransaction();
            clearState();
        }
        child.commit();
    }

    /**
     * @see RowSource#getValidColumns
     */
    public FormatableBitSet getValidColumns() {
        SpliceLogUtils.trace(LOG, "getValidColumns");
        // All columns are valid
        return null;
    }

    /*private helper methods*/
    protected void executeConstantActionBody(Activation activation) throws StandardException {
        SpliceLogUtils.trace(LOG, "executeConstantActionBody with activation %s",activation);

        // Save references to the main structures we need.
        this.activation = activation;
        lcc = activation.getLanguageConnectionContext();
        dd = lcc.getDataDictionary();
        dm = dd.getDependencyManager();
        tc = lcc.getTransactionExecute();
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

        // now do the real work

        // get an exclusive lock of the heap, to avoid deadlock on rows of
        // SYSCOLUMNS etc datadictionary tables and phantom table
        // descriptor, in which case table shape could be changed by a
        // concurrent thread doing add/drop column.

        // older version (or at target) has to get td first, potential deadlock
        getTableDescriptor();

        dm.invalidateFor(td, DependencyManager.ALTER_TABLE, lcc);

        // Save the TableDescriptor off in the Activation
        activation.setDDLTableDescriptor(td);

		   /*
		    ** If the schema descriptor is null, then we must have just read
        ** ourselves in.  So we will get the corresponding schema descriptor 
        ** from the data dictionary.
		    */
        if (sd == null) {
            sd = getAndCheckSchemaDescriptor(dd, schemaId, "ALTER TABLE");
        }
		
		    /* Prepare all dependents to invalidate.  (This is their chance
		     * to say that they can't be invalidated.  For example, an open
		     * cursor referencing a table/view that the user is attempting to
		     * alter.) If no one objects, then invalidate any dependent objects.
		     */
        //TODO -sf- do we need to invalidate twice?
        dm.invalidateFor(td, DependencyManager.ALTER_TABLE, lcc);

        int numRows = 0;
        // adjust dependencies on user defined types
        adjustUDTDependencies( lcc, dd, td, columnInfo, false );

        executeConstraintActions(activation, td,numRows);
        adjustLockGranularity();
    }

    protected void adjustLockGranularity() throws StandardException {
        // Are we changing the lock granularity?
        if (lockGranularity != '\0') {
            if (SanityManager.DEBUG) {
                if (lockGranularity != 'T' && lockGranularity != 'R') {
                    SanityManager.THROWASSERT("lockGranularity expected to be 'T'or 'R', not " + lockGranularity);
                }
            }
            // update the TableDescriptor
            td.setLockGranularity(lockGranularity);
            // update the DataDictionary
            dd.updateLockGranularity(td, sd, lockGranularity, tc);
        }
    }

    protected void executeConstraintActions(Activation activation,TableDescriptor td, int numRows) throws StandardException {
        if(constraintActions==null) return; //no constraints to apply, so nothing to do
        boolean tableScanned = numRows>=0;
        if(numRows<0)
            numRows = 0;


        for (int conIndex = 0; conIndex < constraintActions.length; conIndex++) {
            ConstraintConstantOperation cca = constraintActions[conIndex];
            if (cca instanceof CreateConstraintConstantOperation) {
                int constraintType = cca.getConstraintType();

					      /* Some constraint types require special checking:
					       *   Check		 - table must be empty, for now
					       *   Primary Key - table cannot already have a primary key
					       */
                switch (constraintType) {
                    case DataDictionary.PRIMARYKEY_CONSTRAINT:
                        // Check to see if a constraint of the same type
                        // already exists
                        ConstraintDescriptorList cdl = dd.getConstraintDescriptors(td);
                        if (cdl.getPrimaryKey() != null) {
                            throw StandardException.newException(SQLState.LANG_ADD_PRIMARY_KEY_FAILED1, td.getQualifiedName());
                        }

                        if (!tableScanned) {
                            tableScanned = true;
                            numRows = getSemiRowCount(tc,td);
                        }
                        break;
                    case DataDictionary.CHECK_CONSTRAINT:
                        if (!tableScanned){
                            tableScanned = true;
                            numRows = getSemiRowCount(tc,td);
                        }
                        if (numRows > 0){
								            /*
								            ** We are assuming that there will only be one
								            ** check constraint that we are adding, so it
								            ** is ok to do the check now rather than try
								            ** to lump together several checks.
								            */
                            ConstraintConstantOperation.validateConstraint(
                                    cca.getConstraintName(), ((CreateConstraintConstantOperation)cca).getConstraintText(),
                                    td, lcc, true);
                        }
                        break;
                }
            } else {
                if (SanityManager.DEBUG) {
                    if (!(cca instanceof DropConstraintConstantOperation)) {
                        SanityManager.THROWASSERT("constraintActions[" + conIndex + "] expected to be instanceof " +
                                "DropConstraintConstantOperation not " +cca.getClass().getName());
                    }
                }
            }
            constraintActions[conIndex].executeConstantAction(activation);
        }
    }

    private void getTableDescriptor() throws StandardException {
        if (tableConglomerateId == 0) {
            td = dd.getTableDescriptor(tableId);
            if (td == null) {
                throw StandardException.newException(SQLState.LANG_TABLE_NOT_FOUND_DURING_EXECUTION, tableName);
            }
            tableConglomerateId = td.getHeapConglomerateId();
        }

        // XXX - TODO JL CANNOT LOCK lockTableForDDL(tc, tableConglomerateId, true);

        td = dd.getTableDescriptor(tableId);
        if (td == null) {
            throw StandardException.newException(SQLState.LANG_TABLE_NOT_FOUND_DURING_EXECUTION, tableName);
        }
    }

    /**
     * Clear the state of this constant action.
     */
    private void clearState() {
        SpliceLogUtils.trace(LOG, "clearState");
        // DERBY-3009: executeConstantAction() stores some of its state in
        // instance variables instead of local variables for convenience.
        // These variables should be cleared after the execution of the
        // constant action has completed, so that the objects they reference
        // can be garbage collected.
        td = null;
        lcc = null;
        dd = null;
        dm = null;
        tc = null;
        activation = null;
    }

    protected void updateAllIndexes(long newHeapConglom, DataDictionary dd,TableDescriptor td, TransactionController tc) throws StandardException {
        /*
         * Update all of the indexes on a table when doing a bulk insert
         * on an empty table.
         */
        SpliceLogUtils.trace(LOG, "updateAllIndexes on new heap conglom %d",newHeapConglom);
        long[] newIndexCongloms = new long[numIndexes];
        for (int index = 0; index < numIndexes; index++) {
            updateIndex(newHeapConglom, activation, tc, td,index, newIndexCongloms);
        }
    }

    protected void updateIndex(long newHeapConglom, Activation activation,
                               TransactionController tc, TableDescriptor td, int index, long[] newIndexCongloms)
            throws StandardException {
        SpliceLogUtils.trace(LOG, "updateIndex on new heap conglom %d for index %d with newIndexCongloms %s",
                newHeapConglom, index, Arrays.toString(newIndexCongloms));

        // Get the ConglomerateDescriptor for the index
        ConglomerateDescriptor cd = td.getConglomerateDescriptor(indexConglomerateNumbers[index]);

        // Build the properties list for the new conglomerate
        ConglomerateController indexCC =
                tc.openConglomerate(
                        indexConglomerateNumbers[index],
                        false,
                        TransactionController.OPENMODE_FORUPDATE,
                        TransactionController.MODE_TABLE,
                        TransactionController.ISOLATION_SERIALIZABLE);
        Properties properties = getIndexProperties(newHeapConglom, index, cd, indexCC);


        // We can finally drain the sorter and rebuild the index
        // Populate the index.


        DataValueDescriptor[] rowArray = indexRows[index].getRowArray();
        ColumnOrdering[] columnOrder = ordering[index];
        int[] collationIds = collation[index];

        doIndexUpdate(dd,td,tc, index, newIndexCongloms, cd, properties, false,rowArray,columnOrder,collationIds);
        try{
            // Populate indexes
            IndexRowGenerator indexDescriptor = cd.getIndexDescriptor();
            boolean[] isAscending = indexDescriptor.isAscending();
            int[] baseColumnPositions = indexDescriptor.baseColumnPositions();
            boolean unique = indexDescriptor.isUnique();
            boolean uniqueWithDuplicateNulls = indexDescriptor.isUniqueWithDuplicateNulls();
            boolean[] descColumns = BooleanArrays.not(isAscending);

            byte[] writeTable = Long.toString(newHeapConglom).getBytes();
            TxnView parentTxn = ((SpliceTransactionManager) tc).getActiveStateTxn();
            Txn tentativeTransaction;
            try {
                tentativeTransaction = TransactionLifecycle.getLifecycleManager().beginChildTransaction(parentTxn, Txn.IsolationLevel.SNAPSHOT_ISOLATION,writeTable);
            } catch (IOException e) {
                LOG.error("Couldn't start transaction for tentative DDL operation");
                throw Exceptions.parseException(e);
            }
            TentativeIndexDesc tentativeIndexDesc = new TentativeIndexDesc(newIndexCongloms[index], newHeapConglom,
                    baseColumnPositions, unique,
                    uniqueWithDuplicateNulls,
                    SpliceUtils.bitSetFromBooleanArray(descColumns));
            DDLChange ddlChange = new DDLChange(tentativeTransaction,
                    DDLChangeType.CREATE_INDEX);
            ddlChange.setTentativeDDLDesc(tentativeIndexDesc);
            ddlChange.setParentTxn(((SpliceTransactionManager)tc).getActiveStateTxn());

            notifyMetadataChange(ddlChange);

            HTableInterface table = SpliceAccessManager.getHTable(writeTable);
            try{
                // Add the indexes to the exisiting regions
                createIndex(activation, ddlChange, table, td);

                Txn indexTransaction = getIndexTransaction(tc, tc, tentativeTransaction, newHeapConglom);

                populateIndex(activation, baseColumnPositions, descColumns,
                        newHeapConglom, table,tc,
                        indexTransaction, tentativeTransaction.getCommitTimestamp(),tentativeIndexDesc);
                //only commit the index transaction if the job actually completed
                indexTransaction.commit();
            }finally{
                Closeables.closeQuietly(table);
            }
        }catch (Throwable t) {
            throw Exceptions.parseException(t);
        }

		/* Update the DataDictionary
		 *
		 * Update sys.sysconglomerates with new conglomerate #, we need to
		 * update all (if any) duplicate index entries sharing this same
		 * conglomerate.
		 */
        activation.getLanguageConnectionContext().getDataDictionary().updateConglomerateDescriptor(
                td.getConglomerateDescriptors(indexConglomerateNumbers[index]),
                newIndexCongloms[index],
                tc);

        // Drop the old conglomerate
        tc.dropConglomerate(indexConglomerateNumbers[index]);
    }

    protected void doIndexUpdate(DataDictionary dd,
                                TableDescriptor td,
                                TransactionController tc,
                                 int index, long[] newIndexCongloms,
                                 ConglomerateDescriptor cd, Properties properties,
                                 boolean statisticsExist,
                                 DataValueDescriptor[] rowArray,
                                 ColumnOrdering[] columnOrder,
                                 int[] collationIds) throws StandardException {
//        RowLocationRetRowSource cCount;
//        sorters[index].completedInserts();
//        sorters[index] = null;

//        if (td.statisticsExist(cd)) {
//            cCount = new CardinalityCounter( tc.openSortRowSource(sortIds[index]));
//
//            statisticsExist = true;
//        } else {
//            cCount = new CardinalityCounter( tc.openSortRowSource(sortIds[index]));
//        }

        newIndexCongloms[index] =
                tc.createAndLoadConglomerate(
                        "BTREE",
                        rowArray,
                        columnOrder,
                        collationIds,
                        properties,
                        TransactionController.IS_DEFAULT,
                        null,
                        null);

        //For an index, if the statistics already exist, then drop them.
        //The statistics might not exist for an index if the index was
        //created when the table was empty.
        //
        //For all alter table actions, including ALTER TABLE COMPRESS,
        //for both kinds of indexes (ie. one with preexisting statistics
        //and with no statistics), create statistics for them if the table
        //is not empty.
        if (statisticsExist)
            dd.dropStatisticsDescriptors(td.getUUID(), cd.getUUID(), tc);

//        long numRows;
//        if ((numRows = ((CardinalityCounter)cCount).getRowCount()) > 0) {
//            long[] c = ((CardinalityCounter)cCount).getCardinality();
//            for (int i = 0; i < c.length; i++) {
//                StatisticsDescriptor statDesc =
//                        new StatisticsDescriptor(
//                                dd,
//                                dd.getUUIDFactory().createUUID(),
//                                cd.getUUID(),
//                                td.getUUID(),
//                                "I",
//                                new StatisticsImpl(numRows, c[i]),
//                                i + 1);
//
//                dd.addDescriptor(
//                        statDesc,
//                        null,   // no parent descriptor
//                        DataDictionary.SYSSTATISTICS_CATALOG_NUM,
//                        true,   // no error on duplicate.
//                        tc);
//            }
//        }
    }

    private Properties getIndexProperties(long newHeapConglom, int index, ConglomerateDescriptor cd, ConglomerateController indexCC) throws StandardException {
        // Get the properties on the old index
        Properties properties = new Properties();
        indexCC.getInternalTablePropertySet(properties);

		    /* Create the properties that language supplies when creating the
		     * the index.  (The store doesn't preserve these.)
		     */
        int indexRowLength = indexRows[index].nColumns();
        properties.put("baseConglomerateId", Long.toString(newHeapConglom));
        if (cd.getIndexDescriptor().isUnique()) {
            properties.put( "nUniqueColumns", Integer.toString(indexRowLength - 1));
        } else {
            properties.put( "nUniqueColumns", Integer.toString(indexRowLength));
        }
        if(cd.getIndexDescriptor().isUniqueWithDuplicateNulls()) {
            properties.put( "uniqueWithDuplicateNulls", Boolean.toString(true));
        }
        properties.put( "rowLocationColumn", Integer.toString(indexRowLength - 1));
        properties.put( "nKeyFields", Integer.toString(indexRowLength));

        indexCC.close();
        return properties;
    }


    /**
     * Get info on the indexes on the table being compressed.
     *
     * @exception StandardException		Thrown on error
     */
    protected int getAffectedIndexes(TableDescriptor td) throws StandardException {
        SpliceLogUtils.trace(LOG, "getAffectedIndexes");

        IndexLister	indexLister = td.getIndexLister( );

		/* We have to get non-distinct index row generaters and conglom numbers
		 * here and then compress it to distinct later because drop column
		 * will need to change the index descriptor directly on each index
		 * entry in SYSCONGLOMERATES, on duplicate indexes too.
		 */
        compressIRGs = indexLister.getIndexRowGenerators();
        numIndexes = compressIRGs.length;
        indexConglomerateNumbers = indexLister.getIndexConglomerateNumbers();
        ExecRow emptyHeapRow = td.getEmptyExecRow();
        if(numIndexes > 0) {
            indexRows = new ExecIndexRow[numIndexes];
            ordering  = new ColumnOrdering[numIndexes][];
            collation = new int[numIndexes][];

            for (int index = 0; index < numIndexes; index++) {
                IndexRowGenerator curIndex = compressIRGs[index];
                RowLocation rl = new HBaseRowLocation(); //TODO -sf- don't explicitly depend on this
                // create a single index row template for each index
                indexRows[index] = curIndex.getIndexRowTemplate();
                curIndex.getIndexRow(emptyHeapRow,
                        rl,
                        indexRows[index],
                        null);
				        /* For non-unique indexes, we order by all columns + the RID.
				         * For unique indexes, we just order by the columns.
				         * No need to try to enforce uniqueness here as
				         * index should be valid.
				         */
                int[] baseColumnPositions = curIndex.baseColumnPositions();

                boolean[] isAscending = curIndex.isAscending();

                int numColumnOrderings = baseColumnPositions.length + 1;
                ordering[index] = new ColumnOrdering[numColumnOrderings];

                for (int ii =0; ii < numColumnOrderings - 1; ii++) {
                    ordering[index][ii] = new IndexColumnOrder(ii, isAscending[ii]);
                }
                ordering[index][numColumnOrderings - 1] = new IndexColumnOrder(numColumnOrderings - 1);
                collation[index] = curIndex.getColumnCollationIds(td.getColumnDescriptorList());
            }
        }
//
////        if (! (compressTable))		// then it's drop column
////        {
//            ArrayList newCongloms = new ArrayList();
//            for (int i = 0; i < compressIRGs.length; i++)
//            {
//                int[] baseColumnPositions = compressIRGs[i].baseColumnPositions();
//                int j;
//                for (j = 0; j < baseColumnPositions.length; j++)
//                    if (baseColumnPositions[j] == droppedColumnPosition) break;
//                if (j == baseColumnPositions.length)	// not related
//                    continue;
//
//                if (baseColumnPositions.length == 1 ||
//                        (behavior == StatementType.DROP_CASCADE && compressIRGs[i].isUnique()))
//                {
//                    numIndexes--;
//					/* get first conglomerate with this conglom number each time
//					 * and each duplicate one will be eventually all dropped
//					 */
//                    ConglomerateDescriptor cd = td.getConglomerateDescriptor
//                            (indexConglomerateNumbers[i]);
//
//                    dropConglomerate(cd, td, true, newCongloms, activation,
//                            activation.getLanguageConnectionContext());
//
//                    compressIRGs[i] = null;		// mark it
//                    continue;
//                }
//                // give an error for unique index on multiple columns including
//                // the column we are to drop (restrict), such index is not for
//                // a constraint, because constraints have already been handled
//                if (compressIRGs[i].isUnique())
//                {
//                    ConglomerateDescriptor cd = td.getConglomerateDescriptor
//                            (indexConglomerateNumbers[i]);
//                    throw StandardException.newException(SQLState.LANG_PROVIDER_HAS_DEPENDENT_OBJECT,
//                            dm.getActionString(DependencyManager.DROP_COLUMN),
//                            columnInfo[0].name, "UNIQUE INDEX",
//                            cd.getConglomerateName() );
//                }
//            }
//
//			/* If there are new backing conglomerates which must be
//			 * created to replace a dropped shared conglomerate
//			 * (where the shared conglomerate was dropped as part
//			 * of a "drop conglomerate" call above), then create
//			 * them now.  We do this *after* dropping all dependent
//			 * conglomerates because we don't want to waste time
//			 * creating a new conglomerate if it's just going to be
//			 * dropped again as part of another "drop conglomerate"
//			 * call.
//			 */
//            createNewBackingCongloms(newCongloms, indexConglomerateNumbers);
//
//            IndexRowGenerator[] newIRGs = new IndexRowGenerator[numIndexes];
//            long[] newIndexConglomNumbers = new long[numIndexes];
//
//            for (int i = 0, j = 0; i < numIndexes; i++, j++)
//            {
//                while (compressIRGs[j] == null)
//                    j++;
//
//                int[] baseColumnPositions = compressIRGs[j].baseColumnPositions();
//                newIRGs[i] = compressIRGs[j];
//                newIndexConglomNumbers[i] = indexConglomerateNumbers[j];
//
//                boolean[] isAscending = compressIRGs[j].isAscending();
//                boolean reMakeArrays = false;
//                int size = baseColumnPositions.length;
//                for (int k = 0; k < size; k++)
//                {
//                    if (baseColumnPositions[k] > droppedColumnPosition)
//                        baseColumnPositions[k]--;
//                    else if (baseColumnPositions[k] == droppedColumnPosition)
//                    {
//                        baseColumnPositions[k] = 0;		// mark it
//                        reMakeArrays = true;
//                    }
//                }
//                if (reMakeArrays)
//                {
//                    size--;
//                    int[] newBCP = new int[size];
//                    boolean[] newIsAscending = new boolean[size];
//                    for (int k = 0, step = 0; k < size; k++)
//                    {
//                        if (step == 0 && baseColumnPositions[k + step] == 0)
//                            step++;
//                        newBCP[k] = baseColumnPositions[k + step];
//                        newIsAscending[k] = isAscending[k + step];
//                    }
//                    IndexDescriptor id = compressIRGs[j].getIndexDescriptor();
//                    id.setBaseColumnPositions(newBCP);
//                    id.setIsAscending(newIsAscending);
//                    id.setNumberOfOrderedColumns(id.numberOfOrderedColumns() - 1);
//                }
//            }
//            compressIRGs = newIRGs;
//            indexConglomerateNumbers = newIndexConglomNumbers;
////        }
//
//		/* Now we are done with updating each index descriptor entry directly
//		 * in SYSCONGLOMERATES (for duplicate index as well), from now on, our
//		 * work should apply ONLY once for each real conglomerate, so we
//		 * compress any duplicate indexes now.
//		 */
//        Object[] compressIndexResult =
//                compressIndexArrays(indexConglomerateNumbers, compressIRGs);
//
//        if (compressIndexResult != null)
//        {
//            indexConglomerateNumbers = (long[]) compressIndexResult[1];
//            compressIRGs = (IndexRowGenerator[]) compressIndexResult[2];
//            numIndexes = indexConglomerateNumbers.length;
//        }
//
//        getIndexedColumns(index,td);
        return numIndexes;
    }

    private FormatableBitSet getIndexedColumns(int index,TableDescriptor td) {
        FormatableBitSet indexedCols = new FormatableBitSet(getIndexedColumnSize(td));
//        for (int index = 0; index < numIndexes; index++) {
            int[] colIds = compressIRGs[index].getIndexDescriptor().baseColumnPositions();

            for (int colId : colIds) {
                indexedCols.set(colId);
            }
        return indexedCols;
//        }
    }

    protected int getIndexedColumnSize(TableDescriptor td) {
        return td.getNumberOfColumns();
    }

    /**
     * Iterate through the received list of CreateIndexConstantActions and
     * execute each one, It's possible that one or more of the constant
     * actions in the list has been rendered "unneeded" by the time we get
     * here (because the index that the constant action was going to create
     * is no longer needed), so we have to check for that.
     *
     * @param newConglomActions Potentially empty list of constant actions
     *   to execute, if still needed
     * @param ixCongNums Optional array of conglomerate numbers; if non-null
     *   then any entries in the array which correspond to a dropped physical
     *   conglomerate (as determined from the list of constant actions) will
     *   be updated to have the conglomerate number of the newly-created
     *   physical conglomerate.
     */
    private void createNewBackingCongloms(ArrayList newConglomActions, long [] ixCongNums) throws StandardException {
        SpliceLogUtils.trace(LOG, "createNewBackingCongloms");
        for (Object newConglomAction : newConglomActions) {
            CreateIndexConstantOperation ca =
                    (CreateIndexConstantOperation) newConglomAction;

            if (dd.getConglomerateDescriptor(ca.getCreatedUUID()) == null) {
        /* Conglomerate descriptor was dropped after
				 * being selected as the source for a new
				 * conglomerate, so don't create the new
				 * conglomerate after all.  Either we found
				 * another conglomerate descriptor that can
				 * serve as the source for the new conglom,
				 * or else we don't need a new conglomerate
				 * at all because all constraints/indexes
				 * which shared it had a dependency on the
				 * dropped column and no longer exist.
				 */
                continue;
            }

            executeConglomReplacement(ca, activation);
            long oldCongNum = ca.getReplacedConglomNumber();
            long newCongNum = ca.getCreatedConglomNumber();

			/* The preceding call to executeConglomReplacement updated all
			 * relevant ConglomerateDescriptors with the new conglomerate
			 * number *WITHIN THE DATA DICTIONARY*.  But the table
			 * descriptor that we have will not have been updated.
			 * There are two approaches to syncing the table descriptor
			 * with the dictionary: 1) refetch the table descriptor from
			 * the dictionary, or 2) update the table descriptor directly.
			 * We choose option #2 because the caller of this method (esp.
			 * getAffectedIndexes()) has pointers to objects from the
			 * table descriptor as it was before we entered this method.
			 * It then changes data within those objects, with the
			 * expectation that, later, those objects can be used to
			 * persist the changes to disk.  If we change the table
			 * descriptor here the objects that will get persisted to
			 * disk (from the table descriptor) will *not* be the same
			 * as the objects that were updated--so we'll lose the updates
			 * and that will in turn cause other problems.  So we go with
			 * option #2 and just change the existing TableDescriptor to
			 * reflect the fact that the conglomerate number has changed.
			 */
            ConglomerateDescriptor[] tdCDs =
                    td.getConglomerateDescriptors(oldCongNum);

            for (ConglomerateDescriptor tdCD : tdCDs)
                tdCD.setConglomerateNumber(newCongNum);

			/* If we received a list of index conglomerate numbers
			 * then they are the "old" numbers; see if any of those
			 * numbers should now be updated to reflect the new
			 * conglomerate, and if so, update them.
			 */
            if (ixCongNums != null) {
                for (int j = 0; j < ixCongNums.length; j++) {
                    if (ixCongNums[j] == oldCongNum)
                        ixCongNums[j] = newCongNum;
                }
            }
        }
    }

    // RowSource interface


    /**
     * @see RowSource#getNextRowFromRowSource
     * @exception StandardException on error
     */
    public DataValueDescriptor[] getNextRowFromRowSource() throws StandardException {
        return null;
//        SpliceLogUtils.trace(LOG, "getNextRowFromRowSource");
//        currentRow = null;
//        // Time for a new bulk fetch?
//        if ((! doneScan) &&
//                (currentCompressRow == bulkFetchSize || !validRow[currentCompressRow]))
//        {
//            int bulkFetched;
//
//            bulkFetched = compressHeapGSC.fetchNextGroup(baseRowArray, compressRL);
//
//            doneScan = (bulkFetched != bulkFetchSize);
//            currentCompressRow = 0;
//            for (int index = 0; index < bulkFetched; index++)
//            {
//                validRow[index] = true;
//            }
//            for (int index = bulkFetched; index < bulkFetchSize; index++)
//            {
//                validRow[index] = false;
//            }
//        }
//
//        if (validRow[currentCompressRow])
//        {
////            if (compressTable)
////            {
////                currentRow = baseRow[currentCompressRow];
////            }
////            else
////            {
//                if (currentRow == null)
//                {
//                    currentRow =
//                            activation.getExecutionFactory().getValueRow(
//                                    baseRowArray[currentCompressRow].length - 1);
//                }
//
//                for (int i = 0; i < currentRow.nColumns(); i++)
//                {
//                    currentRow.setColumn(
//                            i + 1,
//                            i < droppedColumnPosition - 1 ?
//                                    baseRow[currentCompressRow].getColumn(i+1) :
//                                    baseRow[currentCompressRow].getColumn(i+1+1));
//                }
////            }
//            currentCompressRow++;
//        }
//
//        if (currentRow != null)
//        {
//			/* Let the target preprocess the row.  For now, this
//			 * means doing an in place clone on any indexed columns
//			 * to optimize cloning and so that we don't try to drain
//			 * a stream multiple times.
//			 */
//            if (compressIRGs.length > 0)
//            {
//				/* Do in-place cloning of all of the key columns */
//                currentRow =  currentRow.getClone(indexedCols);
//            }
//
//            return currentRow.getRowArray();
//        }

//        return null;
    }

    /**
     * @see RowSource#needsToClone
     */
    public boolean needsToClone() {
        SpliceLogUtils.trace(LOG, "needsToClone");
        return(true);
    }

    /**
     * @see RowSource#closeRowSource
     */
    public void closeRowSource() {
        SpliceLogUtils.trace(LOG, "closeRowSource");
        // Do nothing here - actual work will be done in close()
    }


    // RowLocationRetRowSource interface

    /**
     * @see RowLocationRetRowSource#needsRowLocation
     */
    public boolean needsRowLocation() {
        SpliceLogUtils.trace(LOG, "needsRowLocation");
        // Only true if table has indexes
        return (numIndexes > 0);
    }

    /**
     * @see RowLocationRetRowSource#rowLocation
     * @exception StandardException on error
     */
    public void rowLocation(RowLocation rl)
            throws StandardException
    {
//        SpliceLogUtils.trace(LOG, "rowLocation");
//		/* Set up sorters, etc. if 1st row and there are indexes */
//        if (compressIRGs.length > 0)
//        {
//            objectifyStreamingColumns();
//
//			/* Put the row into the indexes.  If sequential,
//			 * then we only populate the 1st sorter when compressing
//			 * the heap.
//			 */
//            int maxIndex = compressIRGs.length;
//            if (maxIndex > 1 && sequential)
//            {
//                maxIndex = 1;
//            }
//            for (int index = 0; index < maxIndex; index++)
//            {
//                insertIntoSorter(index, rl);
//            }
//        }
    }

    protected void	cleanUp() throws StandardException {
        if (compressHeapCC != null) {
            compressHeapCC.close();
            compressHeapCC = null;
        }

        if (compressHeapGSC != null) {
            closeBulkFetchScan();
        }

        // Close each sorter
//        if (sorters != null) {
//            for (int index = 0; index < compressIRGs.length; index++) {
//                if (sorters[index] != null) {
//                    sorters[index].completedInserts();
//                }
//                sorters[index] = null;
//            }
//        }

//        if (needToDropSort != null) {
//            for (int index = 0; index < needToDropSort.length; index++) {
//                if (needToDropSort[index]) {
//                    tc.dropSort(sortIds[index]);
//                    needToDropSort[index] = false;
//                }
//            }
//        }
    }

    protected TableDescriptor getTableDescriptor(DataDictionary dd) throws StandardException {
        TableDescriptor td;
        td = dd.getTableDescriptor(tableId);
        if (td == null) {
            throw StandardException.newException(SQLState.LANG_TABLE_NOT_FOUND_DURING_EXECUTION, tableName);
        }
        if (tableConglomerateId == 0) {
            tableConglomerateId = td.getHeapConglomerateId();
        }
        return td;
    }

    // class implementation

    /**
     * Return the "semi" row count of a table.  We are only interested in
     * whether the table has 0, 1 or > 1 rows.
     *
     *
     * @return Number of rows (0, 1 or > 1) in table.
     *
     * @exception StandardException		Thrown on failure
     */
    protected int getSemiRowCount(TransactionController tc,TableDescriptor td) throws StandardException {
        SpliceLogUtils.trace(LOG, "getSemiRowCount");
        int			   numRows = 0;

        ScanController sc = tc.openScan(td.getHeapConglomerateId(),
                false,	// hold
                0,	    // open read only
                TransactionController.MODE_TABLE,
                TransactionController.ISOLATION_SERIALIZABLE,
                RowUtil.EMPTY_ROW_BITSET, // scanColumnList
                null,	// start position
                ScanController.GE,      // startSearchOperation
                null, // scanQualifier
                null, //stop position - through last row
                ScanController.GT);     // stopSearchOperation

        while (sc.next()) {
            numRows++;
            // We're only interested in whether the table has 0, 1 or > 1 rows
            if (numRows == 2) {
                break;
            }
        }
        sc.close();

        return numRows;
    }

    protected static void executeUpdate(LanguageConnectionContext lcc, String updateStmt) throws StandardException {
        SpliceLogUtils.trace(LOG, "executeUpdate with statement {%s}",updateStmt);
        PreparedStatement ps = lcc.prepareInternalStatement(updateStmt);

        // This is a substatement; for now, we do not set any timeout
        // for it. We might change this behaviour later, by linking
        // timeout to its parent statement's timeout settings.
        ResultSet rs = ps.executeSubStatement(lcc, true, 0L);
        rs.close();
    }

    private void closeBulkFetchScan() throws StandardException {
        compressHeapGSC.close();
        compressHeapGSC = null;
    }

    /**
     * Get rid of duplicates from a set of index conglomerate numbers and
     * index descriptors.
     *
     * @param	indexCIDS	array of index conglomerate numbers
     * @param	irgs		array of index row generaters
     *
     * @return value:		If no duplicates, returns NULL; otherwise,
     *						a size-3 array of objects, first element is an
     *						array of duplicates' indexes in the input arrays;
     *						second element is the compact indexCIDs; third
     *						element is the compact irgs.
     */
    private Object[] compressIndexArrays( long[] indexCIDS, IndexRowGenerator[] irgs) {
        SpliceLogUtils.trace(LOG, "compressIndexArrays");
		/* An efficient way to compress indexes.  From one end of workSpace,
		 * we save unique conglom IDs; and from the other end we save
		 * duplicate indexes' indexes.  We save unique conglom IDs so that
		 * we can do less amount of comparisons.  This is efficient in
		 * space as well.  No need to use hash table.
		 */
        long[] workSpace = new long[indexCIDS.length];
        int j = 0, k = indexCIDS.length - 1;
        for (int i = 0; i < indexCIDS.length; i++) {
            int m;
            for (m = 0; m < j; m++){		// look up our unique set
                if (indexCIDS[i] == workSpace[m]){	// it's a duplicate
                    workSpace[k--] = i;		// save dup index's index
                    break;
                }
            }
            if (m == j)
                workSpace[j++] = indexCIDS[i];	// save unique conglom id
        }

        if(j>=indexCIDS.length) return null; //no duplicates

        long[] newIndexCIDS = new long[j];
        IndexRowGenerator[] newIrgs = new IndexRowGenerator[j];
        int[] duplicateIndexes = new int[indexCIDS.length - j];
        k = 0;
        // do everything in one loop
        for (int m = 0, n = indexCIDS.length - 1; m < indexCIDS.length; m++) {
            // we already gathered our indexCIDS and duplicateIndexes
            if (m < j)
                newIndexCIDS[m] = workSpace[m];
            else
                duplicateIndexes[indexCIDS.length - m - 1] = (int) workSpace[m];

            // stack up our irgs, indexSCOCIs, indexDCOCIs
            if ((n >= j) && (m == (int) workSpace[n]))
                n--;
            else {
                newIrgs[k] = irgs[m];
                k++;
            }
        }

        // construct return value
        Object[] returnValue = new Object[3]; // [indexSCOCIs == null ? 3 : 5];
        returnValue[0] = duplicateIndexes;
        returnValue[1] = newIndexCIDS;
        returnValue[2] = newIrgs;
        return returnValue;
    }
}
