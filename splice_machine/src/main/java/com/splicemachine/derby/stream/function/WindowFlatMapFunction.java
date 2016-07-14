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
import scala.Tuple2;

import java.util.Iterator;

/**
 * Created by jleach on 4/28/15.
 */
public class WindowFlatMapFunction<Op extends SpliceOperation,K,V,U> extends SpliceFlatMapFunction<Op,Iterator<Tuple2<K, V>>, U> {


    @Override
    public Iterable<U> call(Iterator<Tuple2<K, V>> sourceIterator) throws Exception {
        return null;
    }
}
