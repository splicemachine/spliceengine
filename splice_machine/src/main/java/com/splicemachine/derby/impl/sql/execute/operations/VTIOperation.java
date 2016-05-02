
package com.splicemachine.derby.impl.sql.execute.operations;

import com.splicemachine.db.catalog.TypeDescriptor;
import com.splicemachine.db.iapi.services.loader.ClassInspector;
import com.splicemachine.db.iapi.services.sanity.SanityManager;
import com.splicemachine.db.iapi.sql.execute.CursorResultSet;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.iapi.sql.execute.NoPutResultSet;
import com.splicemachine.db.iapi.sql.Activation;
import com.splicemachine.db.iapi.sql.ParameterValueSet;
import com.splicemachine.db.iapi.types.TypeId;
import com.splicemachine.db.iapi.types.DataTypeDescriptor;
import com.splicemachine.db.iapi.types.DataValueDescriptor;
import com.splicemachine.db.iapi.types.VariableSizeDataValue;
import com.splicemachine.db.iapi.sql.execute.ExecutionContext;
import com.splicemachine.db.iapi.store.access.Qualifier;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.services.loader.GeneratedMethod;
import com.splicemachine.db.iapi.types.RowLocation;
import com.splicemachine.db.iapi.reference.SQLState;
import com.splicemachine.db.iapi.services.io.FormatableBitSet;
import com.splicemachine.db.iapi.services.io.FormatableHashtable;
import com.splicemachine.db.vti.IFastPath;
import com.splicemachine.db.vti.VTIEnvironment;
import com.splicemachine.db.vti.Restriction;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.iapi.sql.execute.SpliceRuntimeContext;

import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.ResultSetMetaData;
import java.util.List;


/**
 */
public class VTIOperation extends SpliceBaseOperation implements VTIEnvironment {
	/* Run time statistics variables */
	public int rowsReturned;
	public String javaClassName;

    private boolean next;
	private ClassInspector classInspector;
    private GeneratedMethod row;
    private GeneratedMethod constructor;
	private PreparedStatement userPS;
	private ResultSet userVTI;
	private ExecRow allocatedRow;
	private FormatableBitSet referencedColumns;
	private boolean version2;
	private boolean reuseablePs;
	private boolean isTarget;
	private FormatableHashtable compileTimeConstants;
	private int ctcNumber;

	private boolean pushedProjection;
	private IFastPath	fastPath;

	private Qualifier[][]	pushedQualifiers;

	private boolean[] runtimeNullableColumn;

	private boolean isDerbyStyleTableFunction;

    private final TypeDescriptor returnType;

    private DataTypeDescriptor[]    returnColumnTypes;

    private String[] vtiProjection;
    private Restriction vtiRestriction;

	/**
		Specified isolation level of SELECT (scan). If not set or
		not application, it will be set to ExecutionContext.UNSPECIFIED_ISOLATION_LEVEL
	*/
	private int scanIsolationLevel = ExecutionContext.UNSPECIFIED_ISOLATION_LEVEL;

    protected static final String NAME = VTIOperation.class.getSimpleName().replaceAll("Operation","");

	@Override
	public String getName() {
			return NAME;
	}

	
    //
    // class interface
    //
   public VTIOperation(Activation activation, GeneratedMethod row, int resultSetNumber,
				 GeneratedMethod constructor,
				 String javaClassName,
				 String pushedQualifiers,
				 int erdNumber,
				 boolean version2, boolean reuseablePs,
				 int ctcNumber,
				 boolean isTarget,
				 int scanIsolationLevel,
			     double optimizerEstimatedRowCount,
				 double optimizerEstimatedCost,
				 boolean isDerbyStyleTableFunction,
                 int returnTypeNumber,
                 int vtiProjectionNumber,
                 int vtiRestrictionNumber
                 ) 
		throws StandardException
	{
		super(activation, resultSetNumber, 
			  optimizerEstimatedRowCount, optimizerEstimatedCost);
        this.row = row;
		this.constructor = constructor;
		this.javaClassName = javaClassName;
		this.version2 = version2;
		this.reuseablePs = reuseablePs;
		this.isTarget = isTarget;
//		this.pushedQualifiers = pushedQualifiers;
		this.scanIsolationLevel = scanIsolationLevel;
		this.isDerbyStyleTableFunction = isDerbyStyleTableFunction;

        this.returnType = returnTypeNumber == -1 ? null :
            (TypeDescriptor)
            activation.getPreparedStatement().getSavedObject(returnTypeNumber);

        this.vtiProjection = vtiProjectionNumber == -1 ? null :
            (String[])
            activation.getPreparedStatement().getSavedObject(vtiProjectionNumber);

        this.vtiRestriction = vtiRestrictionNumber == -1 ? null :
            (Restriction)
            activation.getPreparedStatement().getSavedObject(vtiRestrictionNumber);

		if (erdNumber != -1)
		{
			this.referencedColumns = (FormatableBitSet)(activation.getPreparedStatement().
								getSavedObject(erdNumber));
		}

		this.ctcNumber = ctcNumber;
		compileTimeConstants = (FormatableHashtable) (activation.getPreparedStatement().
								getSavedObject(ctcNumber));

    }

	//
	// ResultSet interface (leftover from NoPutResultSet)
	//


    @Override
    public void open() throws StandardException, IOException {
        super.open();
    }

    /**
     * Clone the restriction for a Restricted VTI, filling in parameter values
     * as necessary.
     */
    private Restriction cloneRestriction( Activation activation ) throws StandardException
    {
        if ( vtiRestriction == null ) { return null; }
        else { return cloneRestriction( activation, vtiRestriction ); }
    }
    private Restriction cloneRestriction( Activation activation, Restriction original )
        throws StandardException
    {
        if ( original instanceof Restriction.AND)
        {
            Restriction.AND and = (Restriction.AND) original;
            
            return new Restriction.AND
                (
                 cloneRestriction( activation, and.getLeftChild() ),
                 cloneRestriction( activation, and.getRightChild() )
                 );
        }
        else if ( original instanceof Restriction.OR)
        {
            Restriction.OR or = (Restriction.OR) original;
            
            return new Restriction.OR
                (
                 cloneRestriction( activation, or.getLeftChild() ),
                 cloneRestriction( activation, or.getRightChild() )
                 );
        }
        else if ( original instanceof Restriction.ColumnQualifier)
        {
            Restriction.ColumnQualifier cq = (Restriction.ColumnQualifier) original;
            Object originalConstant = cq.getConstantOperand();
            Object newConstant;

            if ( originalConstant ==  null ) { newConstant = null; }
            else if ( originalConstant instanceof int[] )
            {
                int parameterNumber = ((int[]) originalConstant)[ 0 ];
                ParameterValueSet pvs = activation.getParameterValueSet();

                newConstant = pvs.getParameter( parameterNumber ).getObject();
            }
            else { newConstant = originalConstant; }
           
            return new Restriction.ColumnQualifier
                (
                 cq.getColumnName(),
                 cq.getComparisonOperator(),
                 newConstant
                 );
        }
        else
        {
            throw StandardException.newException( SQLState.NOT_IMPLEMENTED, original.getClass().getName() );
        }
    }

	private boolean[] setNullableColumnList() throws SQLException, StandardException {

		if (runtimeNullableColumn != null)
			return runtimeNullableColumn;

		// Derby-style table functions return SQL rows which don't have not-null
		// constraints bound to them
		if ( isDerbyStyleTableFunction )
		{
		    int         count = getAllocatedRow().nColumns() + 1;
            
		    runtimeNullableColumn = new boolean[ count ];
		    for ( int i = 0; i < count; i++ )   { runtimeNullableColumn[ i ] = true; }
            
		    return runtimeNullableColumn;
		}

		if (userVTI == null)
			return null;

		ResultSetMetaData rsmd = userVTI.getMetaData();
		boolean[] nullableColumn = new boolean[rsmd.getColumnCount() + 1];
		for (int i = 1; i <  nullableColumn.length; i++) {
			nullableColumn[i] = rsmd.isNullable(i) != ResultSetMetaData.columnNoNulls;
		}

		return runtimeNullableColumn = nullableColumn;
	}

	/**
	 * If the VTI is a version2 vti that does not
	 * need to be instantiated multiple times then
	 * we simply close the current ResultSet and 
	 * create a new one via a call to 
	 * PreparedStatement.executeQuery().
	 *
	 * @see NoPutResultSet#openCore
	 * @exception StandardException thrown if cursor finished.
	 */
	public void reopenCore() throws StandardException, IOException {
		if (reuseablePs)
		{
			/* close the user ResultSet.
			 */
			if (userVTI != null)
			{
				try
				{
					userVTI.close();
					userVTI = userPS.executeQuery();

					/* Save off the target VTI */
					if (isTarget)
					{
						activation.setTargetVTI(userVTI);
					}
				} catch (SQLException se)
				{
					throw StandardException.unexpectedUserException(se);
				}
			}
		}
		else
		{
			close();
            open();
		}
	}

	/**
     * If open and not returned yet, returns the row
     * after plugging the parameters into the expressions.
	 *
	 * @exception StandardException thrown on failure.
     */
	@Override
	public ExecRow nextRow(SpliceRuntimeContext spliceRuntimeContext) throws StandardException {
        throw StandardException.newException( SQLState.NOT_IMPLEMENTED, this.getClass().getName());
	}

	/**
	 * @exception StandardException thrown on error
	 */
	public void	close() throws StandardException
	{
        throw StandardException.newException( SQLState.NOT_IMPLEMENTED, this.getClass().getName());
	}

	public void finish() throws StandardException {
        throw StandardException.newException( SQLState.NOT_IMPLEMENTED, this.getClass().getName());
	}

	//
	// CursorResultSet interface
	//

	/**
	 * This is not operating against a stored table,
	 * so it has no row location to report.
	 *
	 * @see CursorResultSet
	 *
	 * @return a null.
	 */
	public RowLocation getRowLocation() {
		if (SanityManager.DEBUG)
			SanityManager.THROWASSERT("RowResultSet used in positioned update/delete");
		return null;
	}

	/**
	 * This is not used in positioned update and delete,
	 * so just return a null.
	 *
	 * @see CursorResultSet
	 *
	 * @return a null.
	 */
	public ExecRow getCurrentRow() {
		if (SanityManager.DEBUG)
			SanityManager.THROWASSERT("RowResultSet used in positioned update/delete");
		return null;
	}

	// Class implementation

	/**
	 * Return the GeneratedMethod for instantiating the VTI.
	 *
	 * @return The  GeneratedMethod for instantiating the VTI.
	 */
	GeneratedMethod getVTIConstructor()
	{
		return constructor;
	}

	boolean isReuseablePs() {
		return reuseablePs;
	}


	/**
	 * Cache the ExecRow for this result set.
	 *
	 * @return The cached ExecRow for this ResultSet
	 *
	 * @exception StandardException thrown on failure.
	 */
	private ExecRow getAllocatedRow()
		throws StandardException
	{
		if (allocatedRow == null)
		{
			allocatedRow = (ExecRow) row.invoke(activation);
		}

		return allocatedRow;
	}

	private int[] getProjectedColList() {

		FormatableBitSet refs = referencedColumns;
		int size = refs.size();
		int arrayLen = 0;
		for (int i = 0; i < size; i++) {
			if (refs.isSet(i))
				arrayLen++;
		}

		int[] colList = new int[arrayLen];
		int offset = 0;
		for (int i = 0; i < size; i++) {
			if (refs.isSet(i))
				colList[offset++] = i + 1;
		}

		return colList;
	}
	/**
	 * @exception StandardException thrown on failure to open
	 */
	public void populateFromResultSet(ExecRow row)
		throws StandardException
	{
		try
		{
            DataTypeDescriptor[]    columnTypes = null;
            if ( isDerbyStyleTableFunction )
            {
                    columnTypes = getReturnColumnTypes();
            }

			boolean[] nullableColumn = setNullableColumnList();
			DataValueDescriptor[] columns = row.getRowArray();
			// ExecRows are 0-based, ResultSets are 1-based
			int rsColNumber = 1;
			for (int index = 0; index < columns.length; index++)
			{
				// Skip over unreferenced columns
				if (referencedColumns != null && (! referencedColumns.get(index)))
				{
					if (!pushedProjection)
						rsColNumber++;

					continue;
				}

				columns[index].setValueFromResultSet(
									userVTI, rsColNumber, 
									/* last parameter is whether or
									 * not the column is nullable
									 */
									nullableColumn[rsColNumber]);
				rsColNumber++;

                // for Derby-style table functions, coerce the value coming out
                // of the ResultSet to the declared SQL type of the return
                // column
                if ( isDerbyStyleTableFunction )
                {
                    DataTypeDescriptor  dtd = columnTypes[ index ];
                    DataValueDescriptor dvd = columns[ index ];

                    cast( dtd, dvd );
                }

            }

		} catch (StandardException se) {
			throw se;
		}
		catch (Throwable t)
		{
			throw StandardException.unexpectedUserException(t);
		}
	}

	public final int getScanIsolationLevel() {
		return scanIsolationLevel;
	}

	/*
	** VTIEnvironment
	*/
	public final boolean isCompileTime() {
		return false;
	}

	public final String getOriginalSQL() {
		return activation.getPreparedStatement().getSource();
	}

	public final int getStatementIsolationLevel() {
		return ExecutionContext.CS_TO_JDBC_ISOLATION_LEVEL_MAP[getScanIsolationLevel()];
	}


	public final void setSharedState(String key, java.io.Serializable value) {
		if (key == null)
			return;

		if (compileTimeConstants == null) {

			Object[] savedObjects = activation.getPreparedStatement().getSavedObjects();

			synchronized (savedObjects) {

				compileTimeConstants = (FormatableHashtable) savedObjects[ctcNumber];
				if (compileTimeConstants == null) {
					compileTimeConstants = new FormatableHashtable();
					savedObjects[ctcNumber] = compileTimeConstants;
				}
			}
		}

		if (value == null)
			compileTimeConstants.remove(key);
		else
			compileTimeConstants.put(key, value);


	}

	public Object getSharedState(String key) {
		if ((key == null) || (compileTimeConstants == null))
			return null;

		return compileTimeConstants.get(key);
	}

    /**
     * <p>
     * Get the types of the columns returned by a Derby-style table function.
     * </p>
     */
    private DataTypeDescriptor[]    getReturnColumnTypes()
        throws StandardException
    {
        if ( returnColumnTypes == null )
        {
            TypeDescriptor[] columnTypes = returnType.getRowTypes();
            int                         count = columnTypes.length;

            returnColumnTypes = new DataTypeDescriptor[ count ];
            for ( int i = 0; i < count; i++ )
            {
                returnColumnTypes[ i ] = DataTypeDescriptor.getType( columnTypes[ i ] );
            }
        }

        return returnColumnTypes;
    }

    /**
     * <p>
     * Cast the value coming out of the user-coded ResultSet. The
     * rules are described in CastNode.getDataValueConversion().
     * </p>
     */
    private void    cast( DataTypeDescriptor dtd, DataValueDescriptor dvd )
        throws StandardException
    {
        TypeId      typeID = dtd.getTypeId();

        if ( !typeID.isBlobTypeId() && !typeID.isClobTypeId() )
        {
            if ( typeID.isLongVarcharTypeId() ) { castLongvarchar( dtd, dvd ); }
            else if ( typeID.isLongVarbinaryTypeId() ) { castLongvarbinary( dtd, dvd ); }
            else if ( typeID.isDecimalTypeId() ) { castDecimal( dtd, dvd ); }
            else
            {
                Object      o = dvd.getObject();

                dvd.setObjectForCast( o, true, typeID.getCorrespondingJavaTypeName() );

                if ( typeID.variableLength() )
                {
                    VariableSizeDataValue   vsdv = (VariableSizeDataValue) dvd;
                    int                                 width;
                    if ( typeID.isNumericTypeId() ) { width = dtd.getPrecision(); }
                    else { width = dtd.getMaximumWidth(); }
            
                    vsdv.setWidth( width, dtd.getScale(), false );
                }
            }

        }

    }

    /**
     * <p>
     * Truncate long varchars to the legal maximum.
     * </p>
     */
    private void    castLongvarchar( DataTypeDescriptor dtd, DataValueDescriptor dvd )
        throws StandardException
    {
        if ( dvd.getLength() > TypeId.LONGVARCHAR_MAXWIDTH )
        {
            dvd.setValue( dvd.getString().substring( 0, TypeId.LONGVARCHAR_MAXWIDTH ) );
        }
    }
    
    /**
     * <p>
     * Truncate long varbinary values to the legal maximum.
     * </p>
     */
    private void    castLongvarbinary( DataTypeDescriptor dtd, DataValueDescriptor dvd )
        throws StandardException
    {
        if ( dvd.getLength() > TypeId.LONGVARBIT_MAXWIDTH )
        {
            byte[]  original = dvd.getBytes();
            byte[]  result = new byte[ TypeId.LONGVARBIT_MAXWIDTH ];

            System.arraycopy( original, 0, result, 0, TypeId.LONGVARBIT_MAXWIDTH );
            
            dvd.setValue( result );
        }
    }
    
    /**
     * <p>
     * Set the correct precision and scale for a decimal value.
     * </p>
     */
    private void    castDecimal( DataTypeDescriptor dtd, DataValueDescriptor dvd )
        throws StandardException
    {
        VariableSizeDataValue   vsdv = (VariableSizeDataValue) dvd;
            
        vsdv.setWidth( dtd.getPrecision(), dtd.getScale(), false );
    }

	@Override
	public List<NodeType> getNodeTypes(){
		throw new UnsupportedOperationException(StandardException.newException( SQLState.NOT_IMPLEMENTED, this.getClass().getName()));
	}

	@Override
	public List<SpliceOperation> getSubOperations() {
		throw new UnsupportedOperationException(StandardException.newException( SQLState.NOT_IMPLEMENTED, this.getClass().getName()));
	}

	@Override
    public int[] getRootAccessedCols(long tableNumber) {
        return null;
    }

    @Override
    public boolean isReferencingTable(long tableNumber) {
        return false;
    }

    @Override
	public long getTimeSpent(int type)
	{
		return constructorTime + openTime + nextTime + closeTime;
	}

    @Override
    public String prettyPrint(int indentLevel) {
        return "VTIOperation";
    }

	@Override
	public String getOptimizerOverrides(SpliceRuntimeContext ctx){
		return null;
	}
}