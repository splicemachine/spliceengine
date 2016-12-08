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

package com.splicemachine.si.impl.server;

import org.apache.hadoop.hbase.Cell;

import java.io.IOException;
import java.util.List;

/**
 * @author Scott Fines
 *         Date: 11/23/16
 */
public interface Compactor{

    void compact(List<Cell> input,
                 List<Cell> destination) throws IOException;

    interface Accumulator{
        void cellDeleted();
        void cellProcessed();
        void rowProcessed();
        void rowDeleted();
        void cellCommitted();
        void cellRolledback();
    }

    Accumulator NoOpAccumulator = new Accumulator(){
        @Override public void cellDeleted(){ }
        @Override public void cellProcessed(){ }
        @Override public void rowProcessed(){ }
        @Override public void rowDeleted(){ }
        @Override public void cellCommitted(){ }
        @Override public void cellRolledback(){ }
    };
}
