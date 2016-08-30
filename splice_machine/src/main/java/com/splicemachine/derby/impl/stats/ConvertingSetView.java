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

package com.splicemachine.derby.impl.stats;

import com.google.common.base.Function;
import org.sparkproject.guava.collect.Iterators;
import java.util.AbstractSet;
import java.util.Iterator;
import java.util.Set;

/**
 * @author Scott Fines
 *         Date: 2/27/15
 */
public class ConvertingSetView<T,E> extends AbstractSet<E> {
    private final Set<? extends T> baseValue;
    private final Function<T,E> conversionFunction;

    public ConvertingSetView(Set<? extends T> baseValue, Function<T, E> conversionFunction) {
        this.baseValue = baseValue;
        this.conversionFunction = conversionFunction;
    }

    @Override
    public Iterator<E> iterator() {
        return Iterators.transform(baseValue.iterator(), conversionFunction);
    }

    @Override public int size() { return baseValue.size(); }
}
