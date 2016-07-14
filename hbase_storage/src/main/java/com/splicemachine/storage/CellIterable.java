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

package com.splicemachine.storage;

import com.google.common.base.Function;
import org.sparkproject.guava.collect.Iterators;
import org.apache.hadoop.hbase.Cell;
import java.util.Iterator;

/**
 * @author Scott Fines
 *         Date: 12/16/15
 */
public class CellIterable implements Iterable<DataCell>{
    private Iterable<Cell> delegate;

    private HCell wrapper = new HCell();
    private Function<Cell, DataCell> transform=new Function<Cell, DataCell>(){
        @Override
        public DataCell apply(Cell input){
            wrapper.set(input);
            return wrapper;
        }
    };
    public CellIterable(Iterable<Cell> delegate){
        this.delegate=delegate;
    }

    @Override
    public Iterator<DataCell> iterator(){
        return Iterators.transform(delegate.iterator(),transform);
    }
}
