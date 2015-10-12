
package com.splicemachine.derby.impl.sql.execute.operations;

import com.splicemachine.db.catalog.TypeDescriptor;
import com.splicemachine.db.iapi.services.loader.ClassInspector;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
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
import com.splicemachine.db.iapi.reference.SQLState;
import com.splicemachine.db.iapi.services.io.FormatableBitSet;
import com.splicemachine.db.iapi.services.io.FormatableHashtable;
import com.splicemachine.db.vti.IFastPath;
import com.splicemachine.db.vti.Restriction;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperationContext;
import com.splicemachine.derby.impl.SpliceMethod;
import com.splicemachine.derby.stream.iapi.DataSet;
import com.splicemachine.derby.stream.iapi.DataSetProcessor;
import com.splicemachine.derby.vti.iapi.DatasetProvider;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Collections;
import java.util.List;

/*

create function hmm()
returns table
(
    id int,
    taxPayerID varchar( 50 ),
    firstName varchar( 50 ),
    lastName varchar( 50 )
)
language java
parameter style SPLICE_JDBC_RESULT_SET
no sql
external name 'com.splicemachine.derby.vti.SpliceTestVTI.getSpliceTestVTI'
 */

/*
create function hmm(filename varchar(32672))
        returns table
        (
        row varchar(32672)
        )
        language java
        parameter style SPLICE_JDBC_RESULT_SET
        no sql
        external name 'com.splicemachine.derby.vti.SpliceFileVTI.getSpliceFileVTI';
        */

/*
create function hmm2(filename varchar(32672), characterDelimiter varchar(1), columnDelimiter varchar(1), numberofColumns int)
        returns table
        (
        col1 varchar(50),
        col2 varchar(50),
        col3 varchar(50)
        )
        language java
        parameter style SPLICE_JDBC_RESULT_SET
        no sql
        external name 'com.splicemachine.derby.vti.SpliceFileVTI.getSpliceFileVTI';

        */


/*



String fileName, String characterDelimiter, String columnDelimiter, int numberOfColumns

 */




/**
 */
public class VTIOperation extends SpliceBaseOperation {
	/* Run time statistics variables */
	public int rowsReturned;
	public String javaClassName;

    private boolean next;
	private ClassInspector classInspector;
    private SpliceMethod<ExecRow> row;
    private String rowMethodName;
    private SpliceMethod<DatasetProvider> constructor;
    private String constructorMethodName;

	private PreparedStatement userPS;
	private DatasetProvider userVTI;
	private ExecRow allocatedRow;
	private FormatableBitSet referencedColumns;
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
        this.rowMethodName = row.getMethodName();
		this.constructorMethodName = constructor.getMethodName();
		this.javaClassName = javaClassName;
		this.isTarget = isTarget;
	//	this.pushedQualifiers = pushedQualifiers;
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
        init(SpliceOperationContext.newContext(activation));
    }

    @Override
    public void init(SpliceOperationContext context) throws StandardException {
        this.activation = context.getActivation();
        this.row = (rowMethodName==null)? null: new SpliceMethod<ExecRow>(rowMethodName,activation);
        this.constructor = (constructorMethodName==null)? null: new SpliceMethod<DatasetProvider>(constructorMethodName,activation);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        super.readExternal(in);
        rowMethodName = in.readUTF();
        constructorMethodName = in.readUTF();
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        super.writeExternal(out);
        out.writeUTF(rowMethodName);
        out.writeUTF(constructorMethodName);
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
/*
		ResultSetMetaData rsmd = userVTI.getMetaData();
		boolean[] nullableColumn = new boolean[rsmd.getColumnCount() + 1];
		for (int i = 1; i <  nullableColumn.length; i++) {
			nullableColumn[i] = rsmd.isNullable(i) != ResultSetMetaData.columnNoNulls;
		}
*/
		return new boolean[0];
	}


	/**
	 * Cache the ExecRow for this result set.
	 *
	 * @return The cached ExecRow for this ResultSet
	 *
	 * @exception StandardException thrown on failure.
	 */
	private ExecRow getAllocatedRow() throws StandardException {
		if (allocatedRow == null)
			allocatedRow = (ExecRow) row.invoke();
		return allocatedRow;
	}

    /**
     * Cache the ExecRow for this result set.
     *
     * @return The cached ExecRow for this ResultSet
     *
     * @exception StandardException thrown on failure.
     */
    private DatasetProvider getDataSetProvider() throws StandardException {
        if (userVTI == null)
            userVTI = constructor.invoke();
        return userVTI;
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
    public <Op extends SpliceOperation> DataSet<LocatedRow> getDataSet(DataSetProcessor dsp) throws StandardException {
        return getDataSetProvider().getDataSet(this, dsp,getAllocatedRow());
    }

    @Override
    public List<SpliceOperation> getSubOperations() {
        return Collections.EMPTY_LIST;
    }
}