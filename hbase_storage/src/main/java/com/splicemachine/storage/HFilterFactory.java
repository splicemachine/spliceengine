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

import com.splicemachine.derby.hbase.AllocatedFilter;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;

/**
 * @author Scott Fines
 *         Date: 12/18/15
 */
public class HFilterFactory implements DataFilterFactory{

    public static final DataFilterFactory INSTANCE=new HFilterFactory();

    private HFilterFactory(){}

    @Override
    public DataFilter singleColumnEqualsValueFilter(byte[] family,byte[] qualifier,byte[] value){
        SingleColumnValueFilter scvf = new SingleColumnValueFilter(family,qualifier,CompareFilter.CompareOp.EQUAL,value);
        return new HFilterWrapper(scvf);
    }

    @Override
    public DataFilter allocatedFilter(byte[] localAddress){
        return new HFilterWrapper(new AllocatedFilter(localAddress));
    }
}
