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

package com.splicemachine.derby.impl.sql.execute.operations.framework;

import org.spark_project.guava.collect.Lists;
import com.splicemachine.db.impl.sql.GenericStorablePreparedStatement;
import com.splicemachine.db.impl.sql.execute.AggregatorInfo;
import com.splicemachine.db.impl.sql.execute.AggregatorInfoList;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperationContext;
import com.splicemachine.derby.impl.SpliceMethod;
import com.splicemachine.derby.impl.sql.execute.operations.iapi.AggregateContext;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.services.loader.ClassFactory;
import com.splicemachine.db.iapi.sql.Activation;
import com.splicemachine.db.iapi.sql.conn.LanguageConnectionContext;
import com.splicemachine.db.iapi.sql.execute.ExecIndexRow;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.List;

/**
 * @author Scott Fines
 *         Created on: 11/4/13
 */
public class DerbyAggregateContext implements AggregateContext {
    private static final long serialVersionUID = 1l;
    private String rowAllocatorMethodName;
    private int aggregateItem;
    private Activation activation;
    private SpliceMethod<ExecRow> rowAllocator;
    private ExecIndexRow sortTemplateRow;
    private ExecIndexRow sourceExecIndexRow;
    private SpliceGenericAggregator[] aggregates;
    private SpliceGenericAggregator[] distinctAggs;
    private SpliceGenericAggregator[] nonDistinctAggs;

    public DerbyAggregateContext() { }

    public DerbyAggregateContext(String rowAllocatorMethodName, int aggregateItem) {
        this.rowAllocatorMethodName = rowAllocatorMethodName;
        this.aggregateItem = aggregateItem;
    }

    @Override
    public void init(SpliceOperationContext context) throws StandardException {
        this.activation = context.getActivation();
        GenericStorablePreparedStatement statement = context.getPreparedStatement();
        LanguageConnectionContext lcc = context.getLanguageConnectionContext();
        AggregatorInfoList aggInfoList = (AggregatorInfoList)statement.getSavedObject(aggregateItem);
        this.aggregates = buildAggregates(aggInfoList,false,lcc);

        this.rowAllocator = (rowAllocatorMethodName==null)? null: new SpliceMethod<ExecRow>(rowAllocatorMethodName,activation);
    }

    @Override
    @SuppressFBWarnings(value = "EI_EXPOSE_REP",justification = "Intentional")
    public SpliceGenericAggregator[] getAggregators() throws StandardException {
        return aggregates;
    }

    @Override
    public ExecIndexRow getSortTemplateRow() throws StandardException {
        if(sortTemplateRow==null){
            sortTemplateRow = activation.getExecutionFactory().getIndexableRow(rowAllocator.invoke());
        }
        return sortTemplateRow;
    }

    @Override
    public ExecIndexRow getSourceIndexRow() {
        if(sourceExecIndexRow==null){
            sourceExecIndexRow = activation.getExecutionFactory().getIndexableRow(sortTemplateRow);
        }
        return sourceExecIndexRow;
    }

    @Override
    @SuppressFBWarnings(value = "EI_EXPOSE_REP",justification = "Intentional")
    public SpliceGenericAggregator[] getDistinctAggregators() {
        if(distinctAggs==null){
            List<SpliceGenericAggregator> distinctAggList = Lists.newArrayListWithExpectedSize(0);
            for(SpliceGenericAggregator agg:aggregates){
                if(agg.isDistinct())
                    distinctAggList.add(agg);
            }
            distinctAggs = new SpliceGenericAggregator[distinctAggList.size()];
            distinctAggList.toArray(distinctAggs);
        }

        return distinctAggs;
    }

    @Override
    @SuppressFBWarnings(value = "EI_EXPOSE_REP",justification = "Intentional")
    public SpliceGenericAggregator[] getNonDistinctAggregators() {
        if(nonDistinctAggs==null){
            List<SpliceGenericAggregator> nonDistinctAggList = Lists.newArrayListWithExpectedSize(0);
            for(SpliceGenericAggregator agg:aggregates){
                if(!agg.isDistinct())
                    nonDistinctAggList.add(agg);
            }
            nonDistinctAggs = new SpliceGenericAggregator[nonDistinctAggList.size()];
            nonDistinctAggList.toArray(nonDistinctAggs);
        }

        return nonDistinctAggs;
    }

    /*****************************************************************************************/
    /*private helper functions*/
    private SpliceGenericAggregator[] buildAggregates(AggregatorInfoList list,
                                                      boolean eliminateDistincts,
                                                      LanguageConnectionContext lcc) {
        List<SpliceGenericAggregator> tmpAggregators = Lists.newArrayList();
        ClassFactory cf = lcc.getLanguageConnectionFactory().getClassFactory();
        int count = list.size();
        for (int i = 0; i < count; i++) {
            AggregatorInfo aggInfo =list.get(i);
            if (! (eliminateDistincts && aggInfo.isDistinct())){
                tmpAggregators.add(new SpliceGenericAggregator(aggInfo, cf));
            }
        }
        SpliceGenericAggregator[] aggregators =
                                    new SpliceGenericAggregator[tmpAggregators.size()];
        tmpAggregators.toArray(aggregators);
        return aggregators;
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeBoolean(rowAllocatorMethodName!=null);
        if(rowAllocatorMethodName!=null)
            out.writeUTF(rowAllocatorMethodName);

        out.writeInt(aggregateItem);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        if(in.readBoolean())
            this.rowAllocatorMethodName = in.readUTF();
        else
            this.rowAllocatorMethodName = null;

        this.aggregateItem = in.readInt();
    }
}
