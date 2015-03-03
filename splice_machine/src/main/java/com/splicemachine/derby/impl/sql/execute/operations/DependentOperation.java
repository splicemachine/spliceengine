package com.splicemachine.derby.impl.sql.execute.operations;

import java.io.IOException;
import java.util.List;
import java.util.Properties;
import java.util.Vector;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.reference.SQLState;
import com.splicemachine.db.iapi.services.i18n.MessageService;
import com.splicemachine.db.iapi.services.io.FormatableBitSet;
import com.splicemachine.db.iapi.services.loader.GeneratedMethod;
import com.splicemachine.db.iapi.services.sanity.SanityManager;
import com.splicemachine.db.iapi.sql.Activation;
import com.splicemachine.db.iapi.sql.execute.CursorResultSet;
import com.splicemachine.db.iapi.sql.execute.ExecIndexRow;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.iapi.sql.execute.TemporaryRowHolder;
import com.splicemachine.db.iapi.store.access.ConglomerateController;
import com.splicemachine.db.iapi.store.access.DynamicCompiledOpenConglomInfo;
import com.splicemachine.db.iapi.store.access.Qualifier;
import com.splicemachine.db.iapi.store.access.ScanController;
import com.splicemachine.db.iapi.store.access.StaticCompiledOpenConglomInfo;
import com.splicemachine.db.iapi.store.access.TransactionController;
import com.splicemachine.db.iapi.types.RowLocation;

import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.iapi.sql.execute.SpliceRuntimeContext;
import com.splicemachine.derby.impl.sql.execute.IndexRow;

/**
 * 
 * XXX -- TODO Not Ready yet
 * 
 * DependentResultSet should be used by only ON DELETE CASCADE/ON DELETE SET NULL ref
 * actions implementation to gather the rows from the dependent tables.  
 * Idea is to scan the foreign key index for the rows in 
 * the source table matelized temporary result set. Scanning of foreign key index gives us the 
 * rows that needs to be deleted on dependent tables. Using the row location 
 * we got from the index , base row is fetched.
*/
public class DependentOperation extends ScanOperation {


	ConglomerateController heapCC;
	RowLocation	baseRowLocation;  // base row location we got from the index
	ExecRow indexRow; //templeate to fetch the index row
	IndexRow indexQualifierRow; // template for the index qualifier row
	ScanController indexSC;  // Index Scan Controller
	StaticCompiledOpenConglomInfo  indexScoci;
	DynamicCompiledOpenConglomInfo indexDcoci;
	int numFkColumns;
	boolean isOpen; // source result set is opened or not
	boolean deferred;
	TemporaryRowHolderOperation source; // Current parent table result set
	TransactionController tc;
	String parentResultSetId;
	int[] fkColArray;
	RowLocation rowLocation;
    TemporaryRowHolder[] sourceRowHolders;
	TemporaryRowHolderOperation[] sourceResultSets;
	int[] sourceOpened;
	int    sArrayIndex;
	Vector sVector;


    protected ScanController scanController;
	protected boolean		scanControllerOpened;
	protected boolean		isKeyed;
	protected boolean		firstScan = true;
	protected ExecIndexRow	startPosition;
	protected ExecIndexRow	stopPosition;

    // set in constructor and not altered during
    // life of object.
	protected long conglomId;
    protected DynamicCompiledOpenConglomInfo heapDcoci;
    protected StaticCompiledOpenConglomInfo heapScoci;
	protected GeneratedMethod resultRowAllocator;
	protected GeneratedMethod startKeyGetter;
	protected int startSearchOperator;
	protected GeneratedMethod stopKeyGetter;
	protected int stopSearchOperator;
	protected Qualifier[][] qualifiers;
	public String tableName;
	public String userSuppliedOptimizerOverrides;
	public String indexName;
	protected boolean runTimeStatisticsOn;
	public int rowsPerRead;
	public boolean forUpdate;
	private boolean sameStartStopPosition;

	// Run time statistics
	private Properties scanProperties;
	public String startPositionString;
	public String stopPositionString;
	public boolean isConstraint;
	public boolean coarserLock;
	public boolean oneRowScan;
	protected long	rowsThisScan;

    protected static final String NAME = DependentOperation.class.getSimpleName().replaceAll("Operation","");

	@Override
	public String getName() {
			return NAME;
	}

	
	//
    // class interface
    //
	public DependentOperation(
		long conglomId,
		StaticCompiledOpenConglomInfo scoci, 
		Activation activation, 
		GeneratedMethod resultRowAllocator, 
		int resultSetNumber,
		GeneratedMethod startKeyGetter, int startSearchOperator,
		GeneratedMethod stopKeyGetter, int stopSearchOperator,
		boolean sameStartStopPosition,
		String qualifiersField,
		String tableName,
		String userSuppliedOptimizerOverrides,
		String indexName,
		boolean isConstraint,
		boolean forUpdate,
		int colRefItem,
		int lockMode,
		boolean tableLocked,
		int isolationLevel,		// ignored
		int rowsPerRead,
		boolean oneRowScan,
		double optimizerEstimatedRowCount,
		double optimizerEstimatedCost,
		String parentResultSetId, 
		long fkIndexConglomId,
		int fkColArrayItem,
		int rltItem
		)	throws StandardException
	{
		  //Because the scan for the tables in this result set are done
		  //internally for delete cascades, isolation should be set to
		  //REPEATABLE READ irrespective what the user level isolation
		  //level is.
		super(conglomId,activation, resultSetNumber,startKeyGetter, startSearchOperator,
				stopKeyGetter, stopSearchOperator, sameStartStopPosition, qualifiersField,
			 resultRowAllocator,lockMode, tableLocked,
			  TransactionController.ISOLATION_REPEATABLE_READ,
              colRefItem, -1, false, // Add Index Ref: This is junk JL
			  optimizerEstimatedRowCount, optimizerEstimatedCost);

		/* Static info created at compile time and can be shared across
		 * instances of the plan.
		 * Dynamic info created on 1st instantiation of this ResultSet as
		 * it cannot be shared.
		 */
        this.heapScoci = scoci;
        heapDcoci = activation.getTransactionController().getDynamicCompiledConglomInfo(conglomId);

		if (SanityManager.DEBUG) {
			SanityManager.ASSERT( activation!=null, "table scan must get activation context");
			SanityManager.ASSERT( resultRowAllocator!= null, "table scan must get row allocator");
			if (sameStartStopPosition)
			{
				SanityManager.ASSERT(stopKeyGetter == null,
					"stopKeyGetter expected to be null when sameStartStopPosition is true");
			}
		}

        this.resultRowAllocator = resultRowAllocator;

		this.startKeyGetter = startKeyGetter;
		this.startSearchOperator = startSearchOperator;
		this.stopKeyGetter = stopKeyGetter;
		this.stopSearchOperator = stopSearchOperator;
		this.sameStartStopPosition = sameStartStopPosition;
		this.qualifiers = qualifiers;
		this.tableName = tableName;
		this.userSuppliedOptimizerOverrides = userSuppliedOptimizerOverrides;
		this.indexName = "On Foreign Key";  // RESOLVE , get actual indexName;
		this.isConstraint = isConstraint;
		this.forUpdate = forUpdate;
		this.rowsPerRead = rowsPerRead;
		this.oneRowScan = oneRowScan;
		
		runTimeStatisticsOn = (activation != null &&
							   activation.getLanguageConnectionContext().getRunTimeStatisticsMode());

		tc = activation.getTransactionController();
		//values required to scan the forein key index.
		indexDcoci = tc.getDynamicCompiledConglomInfo(fkIndexConglomId);
		indexScoci = tc.getStaticCompiledConglomInfo(fkIndexConglomId);
		
		this.parentResultSetId = parentResultSetId;
		this.fkColArray = (int[])(activation.getPreparedStatement().
						getSavedObject(fkColArrayItem));

		this.rowLocation = (RowLocation)(activation.getPreparedStatement().
										 getSavedObject(rltItem));
		numFkColumns = fkColArray.length;
		indexQualifierRow = new IndexRow(numFkColumns);
		recordConstructorTime(); 
	}


	/**
	 * Get a scan controller positioned using searchRow as
	 * the start/stop position.  The assumption is that searchRow
	 * is of the same format as the index being opened. 
	 * @param searchRow			the row to match
	 * @exception StandardException on error
	 */


	private ScanController openIndexScanController(ExecRow searchRow)	throws StandardException
	{
		setupQualifierRow(searchRow);
		indexSC = tc.openCompiledScan(
					  false,                       				// hold 
					  TransactionController.OPENMODE_FORUPDATE, // update only
                      lockMode,									// lock Mode
					  isolationLevel,                           //isolation level
                      (FormatableBitSet)null, 							// retrieve all fields
                      indexQualifierRow.getRowArray(),    		// startKeyValue
                      ScanController.GE,            			// startSearchOp
                      null,                         			// qualifier
                      indexQualifierRow.getRowArray(),    		// stopKeyValue
                      ScanController.GT,             			// stopSearchOp 
					  indexScoci,
					  indexDcoci
                      );

		return indexSC;

	}

	
	//reopen the scan with a differnt search row
	private void reopenIndexScanController(ExecRow searchRow)	throws   StandardException
	{

		setupQualifierRow(searchRow);
		indexSC.reopenScan(
						indexQualifierRow.getRowArray(),    	// startKeyValue
						ScanController.GE,            		// startSearchOp
						null,                         		// qualifier
						indexQualifierRow.getRowArray(), 		// stopKeyValue
						ScanController.GT             		// stopSearchOp 
						);
	}

	
	/*
	** Do reference copy for the qualifier row.  No cloning.
	** So we cannot get another row until we are done with
	** this one.
	*/
	private void setupQualifierRow(ExecRow searchRow)
	{
		Object[] indexColArray = indexQualifierRow.getRowArray();
		Object[] baseColArray = searchRow.getRowArray();

		for (int i = 0; i < numFkColumns; i++)
		{
			indexColArray[i] = baseColArray[fkColArray[i] - 1];
		}
	}


	private void  openIndexScan(ExecRow searchRow) throws StandardException
	{

		if (indexSC == null)
		{
			indexSC =  openIndexScanController(searchRow);
			//create a template for the index row
			indexRow = indexQualifierRow.getClone();
			indexRow.setColumn(numFkColumns + 1, rowLocation.cloneValue(false));

		}else
		{
			reopenIndexScanController(searchRow);
		}
	}


	/**
	  Fetch a row from the index scan.

	  @return The row or null. Note that the next call to fetch will
	  replace the columns in the returned row.
	  @exception StandardException Ooops
	  */
	private ExecRow fetchIndexRow()
		 throws StandardException
	{ 
		if (!indexSC.fetchNext(indexRow.getRowArray()))
		{
			return null;
		}
		return indexRow;
	}

	

	/**
	  Fetch the base row corresponding to the current index row

	  @return The base row row or null.
	  @exception StandardException Ooops
	  */
	private ExecRow fetchBaseRow() throws StandardException {
        ExecRow candidate = scanInformation.getResultRow();
        FormatableBitSet accessedCols = scanInformation.getAccessedColumns();
		if (currentRow == null) {
			currentRow = operationInformation.compactRow(candidate, accessedCols, isKeyed);
		} 

		baseRowLocation = (RowLocation) indexRow.getColumn(indexRow.getRowArray().length);
		boolean base_row_exists = 
            heapCC.fetch(
                baseRowLocation, candidate.getRowArray(),accessedCols);

        if (SanityManager.DEBUG)
        {
            SanityManager.ASSERT(base_row_exists, "base row disappeared.");
        }

		return currentRow;
	}
	

	ExecRow searchRow = null; //the current row we are searching for

	//this function will return an index row on dependent table 
	@Override
	public ExecRow nextRow(SpliceRuntimeContext spliceRuntimeContext) throws StandardException
	{
		
		if (searchRow == null) {
			//we are searching for a row first time
			if((searchRow = getNextParentRow())!=null)
			   openIndexScan(searchRow);
		}	
	
		ExecRow currentIndexRow = null;
	    while(searchRow != null) {
			//get if the current search row has  more 
			//than one row in the dependent tables
			currentIndexRow = fetchIndexRow();

			if(currentIndexRow !=null)
				break;
			if((searchRow = getNextParentRow())!=null)
			   openIndexScan(searchRow);
		}

		if(currentIndexRow!= null) {
			return fetchBaseRow();
		}else {
			return currentIndexRow;
		}
		
		
	}


	//this function will return the rows from the parent result sets 
	private ExecRow	getNextParentRow() throws StandardException 
	{

		ExecRow cRow;
		TemporaryRowHolder rowHolder;

		if(sourceOpened[sArrayIndex] == 0)
		{
			rowHolder = sourceRowHolders[sArrayIndex];
			source = (TemporaryRowHolderOperation)rowHolder.getResultSet();
			source.open(); //open the cursor result set
			sourceOpened[sArrayIndex] = -1;
			sourceResultSets[sArrayIndex] = source;
		}

		if(sourceOpened[sArrayIndex] == 1)
		{
			source = sourceResultSets[sArrayIndex];
			source.reStartScan(sourceRowHolders[sArrayIndex].getTemporaryConglomId(),
							  sourceRowHolders[sArrayIndex].getPositionIndexConglomId());
			sourceOpened[sArrayIndex] = -1;
			
		}

		if(sVector.size() > sourceRowHolders.length)
		{
			addNewSources();
		}

		cRow = source.getNextRow();
		while(cRow == null &&  (sArrayIndex+1) <  sourceRowHolders.length)
		{

			//opening the next source;
			sArrayIndex++;
			if(sourceOpened[sArrayIndex] == 0)
			{
				rowHolder = sourceRowHolders[sArrayIndex];
				source = (TemporaryRowHolderOperation)rowHolder.getResultSet();
				source.open(); //open the cursor result set
				sourceOpened[sArrayIndex] = -1;
				sourceResultSets[sArrayIndex] = source;
			}

			if(sourceOpened[sArrayIndex] == 1)
			{
				source = sourceResultSets[sArrayIndex];
				source.reStartScan(sourceRowHolders[sArrayIndex].getTemporaryConglomId(),
								  sourceRowHolders[sArrayIndex].getPositionIndexConglomId());
				sourceOpened[sArrayIndex] = -1;
			}
		
			cRow = source.getNextRow();
		}

		if(cRow == null)
		{
			//which means no source has any more  currently rows.
			sArrayIndex = 0;
			//mark all the sources to  restartScan.
			for(int i =0 ; i < sourceOpened.length ; i++)
				sourceOpened[i] = 1;
		}
		
		return cRow;
	}



	/*
	** Open the heap Conglomerate controller
	**
	** @param transaction controller will open one if null
	*/
	public ConglomerateController openHeapConglomerateController()
		throws StandardException
	{
		return tc.openCompiledConglomerate(
                    false,
				    TransactionController.OPENMODE_FORUPDATE,
					lockMode,
					isolationLevel,
					heapScoci,
					heapDcoci);
	}




	/**
	  Close the all the opens we did in this result set.
	  */
	public void close()
					throws StandardException, IOException {
		//save the information for the runtime stastics
		// This is where we get the scan properties for the reference index scans
		if (runTimeStatisticsOn) {
			startPositionString = printStartPosition();
			stopPositionString = printStopPosition();
			scanProperties = getScanProperties();
		}

		if (indexSC != null)  {
			indexSC.close();
			indexSC = null;
		}

		if ( heapCC != null ) {
			heapCC.close();
			heapCC = null;
		}
			super.close();
			source.close();

	}

	public void	finish() throws StandardException {
		if (source != null)
			source.finish();
	}

	public void open() throws StandardException, IOException {
        super.open();
        if(source!=null)source.open();
		initIsolationLevel();
		sVector = activation.getParentResultSet(parentResultSetId);
		int size = sVector.size();
		sourceRowHolders = new TemporaryRowHolder[size];
		sourceOpened = new int[size];
		sourceResultSets = new TemporaryRowHolderOperation[size];
		for(int i = 0 ; i < size ; i++)
		{
			sourceRowHolders[i] = (TemporaryRowHolder)sVector.elementAt(i);
			sourceOpened[i] = 0;
		}

		//open the table scan
		heapCC = openHeapConglomerateController();
	}


	private void addNewSources()
	{
		int size = sVector.size();
		TemporaryRowHolder[] tsourceRowHolders = new TemporaryRowHolder[size];
		int[] tsourceOpened = new int[size];
		TemporaryRowHolderOperation[] tsourceResultSets = new TemporaryRowHolderOperation[size];
		
		//copy the source we have now
		System.arraycopy(sourceRowHolders, 0, tsourceRowHolders, 0 , sourceRowHolders.length);
		System.arraycopy(sourceOpened, 0, tsourceOpened , 0 ,sourceOpened.length);
		System.arraycopy(sourceResultSets , 0, tsourceResultSets ,0 ,sourceResultSets.length);

		//copy the new sources
		for(int i = sourceRowHolders.length; i < size ; i++)
		{
			tsourceRowHolders[i] = (TemporaryRowHolder)sVector.elementAt(i);
			tsourceOpened[i] = 0;
		}

		sourceRowHolders = tsourceRowHolders;
		sourceOpened = tsourceOpened ;
		sourceResultSets = tsourceResultSets;
	}



	/**
	 * Can we get instantaneous locks when getting share row
	 * locks at READ COMMITTED.
	 */
	boolean canGetInstantaneousLocks()
	{
		return false;
	}

	//Cursor result set information.
	public RowLocation getRowLocation() throws StandardException
	{
		return baseRowLocation;
	}

	public ExecRow getCurrentRow() throws StandardException 
	{
		return currentRow;
	}


	public Properties getScanProperties()
	{
		if (scanProperties == null)
		{
			scanProperties = new Properties();
		}
		try
		{
			if (indexSC != null)
			{
				indexSC.getScanInfo().getAllScanInfo(scanProperties);
				/* Did we get a coarser lock due to
				 * a covering lock, lock escalation
				 * or configuration?
				 */
				coarserLock = indexSC.isTableLocked() && 
					(lockMode == TransactionController.MODE_RECORD);
			}
		}
		catch(StandardException se)
		{
				// ignore
		}

		return scanProperties;
	}

	public String printStartPosition()
	{
		return printPosition(ScanController.GE, indexQualifierRow);
	}

	public String printStopPosition()
	{
		return printPosition(ScanController.GT, indexQualifierRow);
	}


	/**
	 * Return a start or stop positioner as a String.
	 *
	 * If we already generated the information, then use
	 * that.  Otherwise, invoke the activation to get it.
	 */
	private String printPosition(int searchOperator, ExecIndexRow positioner)
	{
		String idt = "";
		String output = "";

		String searchOp = null;
		switch (searchOperator)
		{
			case ScanController.GE:
				searchOp = ">=";
				break;

			case ScanController.GT:
				searchOp = ">";
				break;

			default:
				if (SanityManager.DEBUG)
				{
					SanityManager.THROWASSERT("Unknown search operator " +
												searchOperator);
				}

				// NOTE: This does not have to be internationalized because
				// this code should never be reached.
				searchOp = "unknown value (" + searchOperator + ")";
				break;
		}

		if(positioner !=null)
		{
			output = output + "\t" +
				MessageService.getTextMessage(
										  SQLState.LANG_POSITIONER,
										  searchOp,
										  String.valueOf(positioner.nColumns())) +
				"\n";

			output = output + "\t" +
				MessageService.getTextMessage(
											  SQLState.LANG_ORDERED_NULL_SEMANTICS) +
				"\n";
			boolean colSeen = false;
			for (int position = 0; position < positioner.nColumns(); position++)
			{
				if (positioner.areNullsOrdered(position))
				{
					output = output + position + " ";
					colSeen = true;
				}

				if (colSeen && position == positioner.nColumns() - 1) {
					output = output +  "\n";
				}
			}
		}
	
		return output;
	}


	/**
	 * Return an array of Qualifiers as a String
	 */
	public String printQualifiers()
	{
		//There are no qualifiers in thie result set for index scans.
		String idt = "";
		return idt + MessageService.getTextMessage(SQLState.LANG_NONE);
	}


	@Override
	public List<NodeType> getNodeTypes() {
		throw new RuntimeException("Not Implemented Yet");
	}


	@Override
	public List<SpliceOperation> getSubOperations() {
		throw new RuntimeException("Not Implemented Yet");
	}
	
	@Override
	public long getTimeSpent(int type)
	{
		return constructorTime + openTime + nextTime + closeTime;
	}
}





