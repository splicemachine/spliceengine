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

package com.splicemachine.derby.stream.function;

import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.impl.sql.execute.operations.LocatedRow;
import com.splicemachine.derby.impl.sql.execute.operations.window.WindowAggregator;
import com.splicemachine.derby.stream.iapi.OperationContext;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

/**
 * Created by jleach on 4/24/15.
 */
public class WindowFinisherFunction extends SpliceFunction<SpliceOperation, LocatedRow, LocatedRow>{
    protected WindowAggregator[] aggregates;
    protected SpliceOperation op;

    public WindowFinisherFunction(){
        super();
    }

    protected boolean initialized=false;

    @SuppressFBWarnings(value="EI_EXPOSE_REP2", justification="Intentional")
    public WindowFinisherFunction(OperationContext<SpliceOperation> operationContext,WindowAggregator[] aggregates){
        super(operationContext);
        this.aggregates=aggregates;
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException{
        super.writeExternal(out);
        out.writeInt(aggregates.length);
        for(int i=0;i<aggregates.length;i++){
            out.writeObject(aggregates[i]);
        }
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException{
        super.readExternal(in);
        aggregates=new WindowAggregator[in.readInt()];
        for(int i=0;i<aggregates.length;i++){
            aggregates[i]=(WindowAggregator)in.readObject();
        }
    }

    @Override
    public LocatedRow call(LocatedRow locatedRow) throws Exception{
        if(!initialized){
            op=getOperation();
            initialized=true;
        }
        for(WindowAggregator aggregator : aggregates){
//                if (aggregator.initialize(locatedRow.getRow())) {
//                    aggregator.accumulate(locatedRow.getRow(), locatedRow.getRow());
//                }
            aggregator.finish(locatedRow.getRow());
        }
        op.setCurrentLocatedRow(locatedRow);
        return locatedRow;
    }
}
