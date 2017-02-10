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

package com.splicemachine.storage;

import org.spark_project.guava.base.Function;
import org.spark_project.guava.collect.Iterators;
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
