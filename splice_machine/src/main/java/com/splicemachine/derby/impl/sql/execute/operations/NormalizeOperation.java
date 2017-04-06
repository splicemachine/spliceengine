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

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.services.sanity.SanityManager;
import com.splicemachine.db.iapi.sql.Activation;
import com.splicemachine.db.iapi.sql.ResultColumnDescriptor;
import com.splicemachine.db.iapi.sql.ResultDescription;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.iapi.types.DataTypeDescriptor;
import com.splicemachine.db.iapi.types.DataValueDescriptor;
import com.splicemachine.db.shared.common.reference.SQLState;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperationContext;
import com.splicemachine.derby.stream.function.NormalizeFunction;
import com.splicemachine.derby.stream.iapi.DataSet;
import com.splicemachine.derby.stream.iapi.DataSetProcessor;
import com.splicemachine.derby.stream.iapi.OperationContext;
import com.splicemachine.utils.SpliceLogUtils;
import org.apache.log4j.Logger;
import org.spark_project.guava.base.Strings;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.sql.Types;
import java.util.Collections;
import java.util.List;

public class NormalizeOperation extends SpliceBaseOperation{
    private static final long serialVersionUID=2l;
    private static final Logger LOG=Logger.getLogger(NormalizeOperation.class);
    public SpliceOperation source;
    private ExecRow normalizedRow;
    private int numCols;
    private int startCol;
    private boolean forUpdate;
    private int erdNumber;

    private DataValueDescriptor[] cachedDestinations;

    private ResultDescription resultDescription;

    private DataTypeDescriptor[] desiredTypes;

    protected static final String NAME=NormalizeOperation.class.getSimpleName().replaceAll("Operation","");

    @Override
    public String getName(){
        return NAME;
    }


    public NormalizeOperation(){
        super();
        SpliceLogUtils.trace(LOG,"instantiating without parameters");
    }

    public NormalizeOperation(SpliceOperation source,
                              Activation activation,
                              int resultSetNumber,
                              int erdNumber,
                              double optimizerEstimatedRowCount,
                              double optimizerEstimatedCost,
                              boolean forUpdate) throws StandardException{
        super(activation,resultSetNumber,optimizerEstimatedRowCount,optimizerEstimatedCost);
        this.source=source;
        this.erdNumber=erdNumber;
        this.forUpdate=forUpdate;
        init();
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException,
            ClassNotFoundException{
        super.readExternal(in);
        forUpdate=in.readBoolean();
        erdNumber=in.readInt();
        source=(SpliceOperation)in.readObject();
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException{
        super.writeExternal(out);
        out.writeBoolean(forUpdate);
        out.writeInt(erdNumber);
        out.writeObject(source);
    }

    @Override
    public void init(SpliceOperationContext context) throws StandardException, IOException{
        super.init(context);
        source.init(context);
        this.resultDescription=
                (ResultDescription)activation.getPreparedStatement().getSavedObject(erdNumber);
        numCols=resultDescription.getColumnCount();
        normalizedRow=activation.getExecutionFactory().getValueRow(numCols);
        cachedDestinations=new DataValueDescriptor[numCols];
        startCol=computeStartColumn(forUpdate,resultDescription);
    }

    private int computeStartColumn(boolean forUpdate,
                                   ResultDescription resultDescription){
        int count=resultDescription.getColumnCount();
        return forUpdate?((count-1)/2)+1:1;
    }

    @Override
    public List<SpliceOperation> getSubOperations(){
        return Collections.singletonList(source);
    }

    @Override
    public SpliceOperation getLeftOperation(){
        return source;
    }

    @Override
    public ExecRow getExecRowDefinition(){
        try{
            return getFromResultDescription(resultDescription);
        }catch(StandardException e){
            SpliceLogUtils.logAndThrowRuntime(LOG,e);
            return null;
        }
    }

    @Override
    public int[] getRootAccessedCols(long tableNumber) throws StandardException{
        return source.getRootAccessedCols(tableNumber);
    }

    @Override
    public boolean isReferencingTable(long tableNumber){
        return source.isReferencingTable(tableNumber);
    }

    public ExecRow normalizeRow(ExecRow sourceRow,boolean requireNotNull) throws StandardException{
        int colCount=resultDescription.getColumnCount();
        for(int i=1;i<=colCount;i++){
            DataValueDescriptor sourceCol=sourceRow.getColumn(i);
            if(sourceCol!=null){
                DataValueDescriptor normalizedCol;
                if(i<startCol){
                    normalizedCol=sourceCol;
                }else{
                    normalizedCol=normalizeColumn(getDesiredType(i),sourceRow,i,
                            getCachedDesgination(i),resultDescription,requireNotNull);
                }
                normalizedRow.setColumn(i,normalizedCol);
            }
        }
        return normalizedRow;
    }

    private static DataValueDescriptor normalize(DataValueDescriptor source,
                                                 DataTypeDescriptor destType,DataValueDescriptor cachedDest,boolean requireNonNull)
            throws StandardException{
        if(SanityManager.DEBUG){
            if(cachedDest!=null){
                if(!destType.getTypeId().isUserDefinedTypeId()){
                    String t1=destType.getTypeName();
                    String t2=cachedDest.getTypeName();
                    if(!t1.equals(t2)){

                        if(!(((t1.equals("DECIMAL") || t1.equals("NUMERIC"))
                                && (t2.equals("DECIMAL") || t2.equals("NUMERIC"))) ||
                                (t1.startsWith("INT") && t2.startsWith("INT"))))  //INT/INTEGER

                            SanityManager.THROWASSERT(
                                    "Normalization of "+t2+" being asked to convert to "+t1);
                    }
                }
            }else{
                SanityManager.THROWASSERT("cachedDest is null");
            }
        }

        if(source.isNull()){
            if(requireNonNull && !destType.isNullable())
                throw StandardException.newException(com.splicemachine.db.iapi.reference.SQLState.LANG_NULL_INTO_NON_NULL,"");

            cachedDest.setToNull();
        }else{

            int jdbcId=destType.getJDBCTypeId();

            cachedDest.normalize(destType,source);
            //doing the following check after normalize so that normalize method would get called on long varchs and long varbinary
            //Need normalize to be called on long varchar for bug 5592 where we need to enforce a lenght limit in db2 mode
            if((jdbcId==Types.LONGVARCHAR) || (jdbcId==Types.LONGVARBINARY)){
                // special case for possible streams
                if(source.getClass()==cachedDest.getClass())
                    return source;
            }
        }
        return cachedDest;
    }

    private DataValueDescriptor normalizeColumn(DataTypeDescriptor desiredType,
                                                ExecRow sourceRow,int sourceColPos,DataValueDescriptor cachedDesgination,
                                                ResultDescription resultDescription,boolean requireNotNull) throws StandardException{
        DataValueDescriptor sourceCol=sourceRow.getColumn(sourceColPos);
        try{
            return normalize(sourceCol,desiredType,cachedDesgination,requireNotNull);
        }catch(StandardException se){
            if(se.getMessageId().startsWith(SQLState.LANG_NULL_INTO_NON_NULL)){
                ResultColumnDescriptor colDesc=resultDescription.getColumnDescriptor(sourceColPos);
                throw StandardException.newException(SQLState.LANG_NULL_INTO_NON_NULL,colDesc.getName());
            }
            throw se;
        }
    }

    private DataValueDescriptor getCachedDesgination(int col) throws StandardException{
        int index=col-1;
        if(cachedDestinations[index]==null){
            DataValueDescriptor dvd=getDesiredType(col).getNull();
//						int formatId = dvd.getTypeFormatId();
            cachedDestinations[index]=dvd;
//						cachedDestinations[index] = LazyDataValueFactory.getLazyNull(formatId);
        }
        return cachedDestinations[index];
    }

    private DataTypeDescriptor getDesiredType(int i){
        if(desiredTypes==null)
            desiredTypes=fetchResultTypes(resultDescription);
        return desiredTypes[i-1];
    }

    private DataTypeDescriptor[] fetchResultTypes(
            ResultDescription resultDescription){
        int colCount=resultDescription.getColumnCount();
        DataTypeDescriptor[] result=new DataTypeDescriptor[colCount];
        for(int i=1;i<=colCount;i++){
            ResultColumnDescriptor colDesc=resultDescription.getColumnDescriptor(i);
            DataTypeDescriptor dtd=colDesc.getType();

            result[i-1]=dtd;
        }
        return result;
    }

    @Override
    public String toString(){
        return String.format("NormalizeOperation {resultSetNumber=%d, source=%s}",resultSetNumber,source);
    }

    public SpliceOperation getSource(){
        return this.source;
    }

    @Override
    public String prettyPrint(int indentLevel){
        String indent="\n"+ Strings.repeat("\t", indentLevel);

        return new StringBuilder("Normalize:")
                .append(indent).append("resultSetNumber:").append(resultSetNumber)
                .append(indent).append("numCols:").append(numCols)
                .append(indent).append("startCol:").append(startCol)
                .append(indent).append("erdNumber:").append(erdNumber)
                .append(indent).append("source:").append(source.prettyPrint(indentLevel+1))
                .toString();
    }

    @SuppressWarnings("unchecked")
    @Override
    public DataSet<LocatedRow> getDataSet(DataSetProcessor dsp) throws StandardException{
        DataSet<LocatedRow> sourceSet=source.getDataSet(dsp);
        OperationContext operationContext=dsp.createOperationContext(this);
        operationContext.pushScope();
        try{
            return sourceSet.flatMap(new NormalizeFunction(operationContext),true);
        }finally{
            operationContext.popScope();
        }
    }

    @Override
    public String getVTIFileName(){
        return getSubOperations().get(0).getVTIFileName();
    }

}
