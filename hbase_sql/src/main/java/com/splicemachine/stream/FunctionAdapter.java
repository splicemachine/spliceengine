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

package com.splicemachine.stream;

import scala.Function1;
import scala.collection.Iterator;
import scala.reflect.ClassTag;
import scala.runtime.AbstractFunction1;

import java.io.Serializable;

/**
 * Created by dgomezferro on 6/1/16.
 */
public class FunctionAdapter extends AbstractFunction1<Iterator<String>, String> implements Serializable {

    private final static ClassTag<String> tag = scala.reflect.ClassTag$.MODULE$.apply(String.class);

    @Override
    public String apply(scala.collection.Iterator<String> it) {
        String result = it.next();
        // This iterator is what's returned on ResultStreamer.call(), which is a list of one element
        assert !it.hasNext();
        return result;
    }
}
