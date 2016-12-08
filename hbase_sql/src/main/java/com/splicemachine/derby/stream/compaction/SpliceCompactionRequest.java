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

package com.splicemachine.derby.stream.compaction;

import com.splicemachine.si.impl.server.Compactor;
import org.apache.hadoop.hbase.regionserver.StoreFile;
import org.apache.hadoop.hbase.regionserver.compactions.CompactionRequest;

import java.util.Collection;

/**
 * @author Scott Fines
 *         Date: 12/1/16
 */
public class SpliceCompactionRequest extends CompactionRequest{
    final Compactor.Accumulator accumulator;

    public SpliceCompactionRequest(Compactor.Accumulator accumulator){
        this.accumulator=accumulator;
    }

    public SpliceCompactionRequest(Collection<StoreFile> files,Compactor.Accumulator accumulator){
        super(files);
        this.accumulator=accumulator;
    }

    public Compactor.Accumulator accumulator(){
        return accumulator;
    }
}
