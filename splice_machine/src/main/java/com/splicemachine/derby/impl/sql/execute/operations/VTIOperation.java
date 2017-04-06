
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

package com.splicemachine.derby.impl.sql.execute.operations;

import com.splicemachine.db.catalog.TypeDescriptor;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.iapi.sql.Activation;
import com.splicemachine.db.iapi.types.DataTypeDescriptor;
import com.splicemachine.db.iapi.sql.execute.ExecutionContext;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.services.loader.GeneratedMethod;
import com.splicemachine.db.iapi.services.io.FormatableBitSet;
import com.splicemachine.db.iapi.services.io.FormatableHashtable;
import com.splicemachine.db.vti.Restriction;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperationContext;
import com.splicemachine.derby.impl.SpliceMethod;
import com.splicemachine.derby.stream.iapi.DataSet;
import com.splicemachine.derby.stream.iapi.DataSetProcessor;
import com.splicemachine.derby.vti.SpliceFileVTI;
import com.splicemachine.derby.vti.iapi.DatasetProvider;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
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
	public String javaClassName;
    private SpliceMethod<ExecRow> row;
    private String rowMethodName;
    private SpliceMethod<DatasetProvider> constructor;
    private String constructorMethodName;
	public DatasetProvider userVTI;
	private ExecRow allocatedRow;
	private FormatableBitSet referencedColumns;
	private boolean isTarget;
	private FormatableHashtable compileTimeConstants;
	private int ctcNumber;
	private boolean[] runtimeNullableColumn;
	private boolean isDerbyStyleTableFunction;
    private  TypeDescriptor returnType;
    private DataTypeDescriptor[]    returnColumnTypes;
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


    public VTIOperation() {

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
        init();
    }

    @Override
    public void init(SpliceOperationContext context) throws StandardException, IOException {
        super.init(context);
        this.activation = context.getActivation();
        this.row = (rowMethodName==null)? null: new SpliceMethod<ExecRow>(rowMethodName,activation);
        this.constructor = (constructorMethodName==null)? null: new SpliceMethod<DatasetProvider>(constructorMethodName,activation);
        this.userVTI = constructor==null?null:constructor.invoke();
    }

    @Override
    public ExecRow getExecRowDefinition() throws StandardException {
        return getAllocatedRow();
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        super.readExternal(in);
        javaClassName = in.readUTF();
        rowMethodName = in.readUTF();
        constructorMethodName = in.readUTF();
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        super.writeExternal(out);
        out.writeUTF(javaClassName);
        out.writeUTF(rowMethodName);
        out.writeUTF(constructorMethodName);
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
    public DatasetProvider getDataSetProvider() throws StandardException {
        return userVTI;
    }


    public final int getScanIsolationLevel() {
		return scanIsolationLevel;
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
    public String prettyPrint(int indentLevel) {
        return "VTIOperation";
    }

    @Override
    public DataSet<LocatedRow> getDataSet(DataSetProcessor dsp) throws StandardException {
        return getDataSetProvider().getDataSet(this, dsp,getAllocatedRow());
    }

    @Override
    public List<SpliceOperation> getSubOperations() {
        return Collections.emptyList();
    }

    @Override
    public String getVTIFileName() {
        if (userVTI instanceof SpliceFileVTI)
            return ((SpliceFileVTI) userVTI).getFileName();
        return null;
    }
    
    @Override
    public String getScopeName() {
        return "VTIOperation" + (userVTI != null ? " (" + userVTI.getClass().getSimpleName() + ")" : "");
    }
}