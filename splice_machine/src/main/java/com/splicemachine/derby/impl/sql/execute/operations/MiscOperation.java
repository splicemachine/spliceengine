/*
 * Copyright (c) 2012 - 2020 Splice Machine, Inc.
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

import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.iapi.types.SQLInteger;
import com.splicemachine.db.impl.sql.execute.ValueRow;
import com.splicemachine.derby.stream.iapi.ScopeNamed;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.Activation;
import com.splicemachine.derby.stream.iapi.DataSet;
import com.splicemachine.derby.stream.iapi.DataSetProcessor;
import com.splicemachine.pipeline.Exceptions;
import com.splicemachine.utils.SpliceLogUtils;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;

/**
 * This is a wrapper class which invokes the Execution-time logic for
 * Misc statements. The real Execution-time logic lives inside the
 * executeConstantAction() method. Note that when re-using the
 * language result set tree across executions (DERBY-827) it is not
 * possible to store the ConstantAction as a member variable, because
 * a re-prepare of the statement will invalidate the stored
 * ConstantAction. Re-preparing a statement does not create a new
 * Activation unless the GeneratedClass has changed, so the existing
 * result set tree may survive a re-prepare.
 *
 * @author jessiezhang
 */

@SuppressFBWarnings(value="SE_NO_SUITABLE_CONSTRUCTOR_FOR_EXTERNALIZATION", justification="Serializing this is a mistake,"+
        "but we inherit externalizability from the SpliceBaseOperation")
public class MiscOperation extends NoRowsOperation{
    private static final Logger LOG=Logger.getLogger(MiscOperation.class);
    protected static final String NAME=MiscOperation.class.getSimpleName().replaceAll("Operation","");

    public MiscOperation(){}
    @Override
    public String getName(){
        return NAME;
    }


    /**
     * Construct a MiscResultSet
     *
     * @param activation Describes run-time environment.
     */
    public MiscOperation(Activation activation) throws StandardException{
        super(activation);
    }

    @Override
    public void close() throws StandardException{
        super.close();
        SpliceLogUtils.trace(LOG,"close for miscRowProvider, isOpen=%s",isOpen);
        if(!isOpen)
            return;
        try{
            int staLength=(subqueryTrackingArray==null)?0:subqueryTrackingArray.length;

            for(int index=0;index<staLength;index++){
                if(subqueryTrackingArray[index]==null || subqueryTrackingArray[index].isClosed())
                    continue;

                subqueryTrackingArray[index].close();
            }

            isOpen=false;
            if(activation.isSingleExecution())
                activation.close();
        }catch(Exception e){
            SpliceLogUtils.error(LOG,e);
            throw Exceptions.parseException(e);
        }
    }

    @Override
    public String toString(){
        return "ConstantActionOperation";
    }

    @Override
    public String prettyPrint(int indentLevel){
        return "ConstantAction"+super.prettyPrint(indentLevel);
    }

    @Override
    public int[] getRootAccessedCols(long tableNumber){
        return null;
    }

    @Override
    public boolean isReferencingTable(long tableNumber){
        return false;
    }

    @Override
    public DataSet<ExecRow> getDataSet(DataSetProcessor dsp) throws StandardException{
        if (!isOpen)
            throw new IllegalStateException("Operation is not open");

        if (dsp.isSparkExplain())
            return dsp.getEmpty();

        setup();

        activation.getConstantAction().executeConstantAction(activation);

        ValueRow valueRow=new ValueRow(1);
        valueRow.setColumn(1,new SQLInteger((int)activation.getRowsSeen()));

        // TODO (wjk): consider using ControlDataSetProcessor explicitly
        // for actions which, like CreateIndexConstantOperation, do their own scope
        // push and which therefore don't really need to have MiscOperation do it.
        //
        // dsp = EngineDriver.driver().processorFactory().localProcessor(activation, null);
        String name=null;
        if(activation.getConstantAction() instanceof ScopeNamed){
            name=(((ScopeNamed)activation.getConstantAction()).getScopeName());
        }else{
            name=StringUtils.join(
                    StringUtils.splitByCharacterTypeCamelCase(
                            activation.getConstantAction().getClass().getSimpleName().
                                    replace("Operation","").replace("Constant","")),' ');
        }
        return dsp.singleRowDataSet(valueRow,name);
    }
}
