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

package com.splicemachine.stream;

import org.apache.spark.TaskContext;
import scala.collection.Iterator;
import scala.reflect.ClassTag;
import scala.runtime.AbstractFunction1;
import scala.runtime.AbstractFunction2;

import java.io.Serializable;

/**
 * Created by dgomezferro on 6/1/16.
 */
public class FunctionAdapter extends AbstractFunction1<Iterator<StreamerResult>, StreamerResult> implements Serializable {

    @Override
    public StreamerResult apply(scala.collection.Iterator<StreamerResult> it) {
        StreamerResult result = it.next();
        // This iterator is what's returned on ResultStreamer.call(), which is a list of one element
        assert !it.hasNext();
        return result;
    }
}
