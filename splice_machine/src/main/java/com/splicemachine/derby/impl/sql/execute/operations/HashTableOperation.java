package com.splicemachine.derby.impl.sql.execute.operations;

import com.google.common.base.Strings;

import com.splicemachine.db.iapi.services.loader.GeneratedMethod;
import com.splicemachine.db.iapi.services.sanity.SanityManager;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.iapi.sql.Activation;
import com.splicemachine.db.iapi.sql.execute.NoPutResultSet;
import com.splicemachine.db.iapi.store.access.Qualifier;
import com.splicemachine.db.iapi.store.access.BackingStoreHashtable;
import com.splicemachine.db.iapi.services.io.FormatableArrayHolder;
import com.splicemachine.db.iapi.services.io.FormatableIntHolder;
import com.splicemachine.db.catalog.types.ReferencedColumnsDescriptorImpl;

import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.iapi.sql.execute.SpliceRuntimeContext;

import java.util.Arrays;
import java.util.List;
import java.util.Properties;

/**
 * Builds a hash table on the underlying result set tree.
 *
 */
public class HashTableOperation extends SpliceBaseOperation  {
	/* Run time statistics variables */
	public long restrictionTime;
	public long projectionTime;
	public int  hashtableSize;
	public Properties scanProperties;

    // set in constructor and not altered during
    // life of object.
    public SpliceOperation source;
    public GeneratedMethod singleTableRestriction;
	public Qualifier[][] nextQualifiers;
    private GeneratedMethod projection;
	private int[]			projectMapping;
	private boolean runTimeStatsOn;
	private ExecRow			mappedResultRow;
	public boolean reuseResult;
	public int[]			keyColumns;
	private boolean			removeDuplicates;
	private long			maxInMemoryRowCount;
    private	int				initialCapacity;
    private	float			loadFactor;
	private boolean			skipNullKeyColumns;
	// Variable for managing next() logic on hash entry
	private boolean		firstNext = true;
	private int			numFetchedOnNext;
	private int			entryVectorSize;
	private List		entryVector;
	private boolean hashTableBuilt;
	private boolean firstIntoHashtable = true;
	private ExecRow nextCandidate;
	private ExecRow projRow;
	private BackingStoreHashtable ht;
    public NoPutResultSet[] subqueryTrackingArray;

    protected static final String NAME = HashTableOperation.class.getSimpleName().replaceAll("Operation","");

	@Override
	public String getName() {
			return NAME;
	}

    
    //
    // class interface
    //
    public HashTableOperation(SpliceOperation s,
					Activation a,
					GeneratedMethod str,
					String equijoinQualifiers,
					GeneratedMethod p,
					int resultSetNumber,
					int mapRefItem,
					boolean reuseResult,
					int keyColItem,
					boolean removeDuplicates,
					long maxInMemoryRowCount,
					int	initialCapacity,
					float loadFactor,
					boolean skipNullKeyColumns,
				    double optimizerEstimatedRowCount,
					double optimizerEstimatedCost) 
		throws StandardException
	{
		super(a, resultSetNumber, optimizerEstimatedRowCount, optimizerEstimatedCost);
     
		source = s;
		// source expected to be non-null, mystery stress test bug
		// - sometimes get NullPointerException in openCore().
		if (SanityManager.DEBUG)
		{
			SanityManager.ASSERT(source != null,
				"HTRS(), source expected to be non-null");
		}
        singleTableRestriction = str;
//		this.nextQualifiers = nextQualifiers;
        projection = p;
		projectMapping = ((ReferencedColumnsDescriptorImpl) a.getPreparedStatement().getSavedObject(mapRefItem)).getReferencedColumnPositions();
		FormatableArrayHolder fah = (FormatableArrayHolder) a.getPreparedStatement().getSavedObject(keyColItem);
		FormatableIntHolder[] fihArray = (FormatableIntHolder[]) fah.getArray(FormatableIntHolder.class);
		keyColumns = new int[fihArray.length];
		for (int index = 0; index < fihArray.length; index++)
		{
			keyColumns[index] = fihArray[index].getInt();
		}

		this.reuseResult = reuseResult;
		this.removeDuplicates = removeDuplicates;
		this.maxInMemoryRowCount = maxInMemoryRowCount;
		this.initialCapacity = initialCapacity;
		this.loadFactor = loadFactor;
		this.skipNullKeyColumns = skipNullKeyColumns;

		// Allocate a result row if all of the columns are mapped from the source
		if (projection == null)
		{
			mappedResultRow = activation.getExecutionFactory().getValueRow(projectMapping.length);
		}
		recordConstructorTime(); 
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
    public int[] getRootAccessedCols(long tableNumber) throws StandardException {
        return ((SpliceOperation)source).getRootAccessedCols(tableNumber);
    }

    @Override
    public boolean isReferencingTable(long tableNumber) {
        return ((SpliceOperation)source).isReferencingTable(tableNumber);
    }

    @Override
	public ExecRow nextRow(SpliceRuntimeContext spliceRuntimeContext) throws StandardException {
		throw new RuntimeException("Not Implemented Yet");
	}

//	@Override
//	public long getTimeSpent(int type)
//	{
//		long totTime = constructorTime + openTime + nextTime + closeTime;
//
//		if (type == CURRENT_RESULTSET_ONLY)
//			return	totTime - source.getTimeSpent(ENTIRE_RESULTSET_TREE);
//		else
//			return totTime;
//	}

    @Override
    public String prettyPrint(int indentLevel) {
        String indent = "\n"+ Strings.repeat("\t",indentLevel);

        return new StringBuilder("HashTable:")
                .append(indent).append("resultSetNumber:").append(resultSetNumber)
                .append(indent).append("keyColumns:").append(Arrays.toString(keyColumns))
                .append(indent).append("projectMapping:").append(Arrays.toString(projectMapping))
                .append(indent).append("source:").append(((SpliceOperation)source).prettyPrint(indentLevel+1))
                .toString();
    }

	@Override
	public String getOptimizerOverrides(SpliceRuntimeContext ctx){
		return source.getOptimizerOverrides(ctx);
	}
}