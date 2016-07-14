/*
 * Copyright 2012 - 2016 Splice Machine, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
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
