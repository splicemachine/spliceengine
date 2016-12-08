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
 *
 */

package com.splicemachine.compactions;

import com.splicemachine.si.impl.server.Compactor;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.util.LongAccumulator;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

/**
 * @author Scott Fines
 *         Date: 12/1/16
 */
public class SparkAccumulator implements Compactor.Accumulator,Externalizable{
    private LongAccumulator cellsProcessed;
    private LongAccumulator cellsDeleted;
    private LongAccumulator rowsProcessed;
    private LongAccumulator rowsDeleted;
    private LongAccumulator cellsCommitted;
    private LongAccumulator cellsRolledback;

    public SparkAccumulator(){
    }

    public SparkAccumulator(JavaSparkContext sparkContext){
        this.cellsProcessed = sparkContext.sc().longAccumulator("cells processed");
        this.cellsDeleted = sparkContext.sc().longAccumulator("cells deleted");
        this.rowsProcessed = sparkContext.sc().longAccumulator("rows processed");
        this.rowsDeleted = sparkContext.sc().longAccumulator("rows deleted");
        this.cellsCommitted = sparkContext.sc().longAccumulator("cells committed");
        this.cellsRolledback = sparkContext.sc().longAccumulator("cells rolled back");
    }

    @Override public void cellDeleted(){ cellsDeleted.add(1L); }
    @Override public void cellProcessed(){ cellsProcessed.add(1L); }
    @Override public void rowProcessed(){ rowsProcessed.add(1L); }
    @Override public void rowDeleted(){ rowsDeleted.add(1L); }
    @Override public void cellCommitted(){ cellsCommitted.add(1L); }
    @Override public void cellRolledback(){ cellsRolledback.add(1L); }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException{
       out.writeObject(cellsProcessed);
        out.writeObject(cellsDeleted);
        out.writeObject(rowsProcessed);
        out.writeObject(rowsDeleted);
        out.writeObject(cellsCommitted);
        out.writeObject(cellsRolledback);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException{
        this.cellsProcessed = (LongAccumulator)in.readObject();
        this.cellsDeleted = (LongAccumulator)in.readObject();
        this.rowsProcessed = (LongAccumulator)in.readObject();
        this.rowsDeleted = (LongAccumulator)in.readObject();
        this.cellsCommitted = (LongAccumulator)in.readObject();
        this.cellsRolledback = (LongAccumulator)in.readObject();
    }
}
