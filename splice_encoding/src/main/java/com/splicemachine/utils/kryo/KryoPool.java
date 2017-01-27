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

package com.splicemachine.utils.kryo;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.util.DefaultClassResolver;
import com.esotericsoftware.kryo.util.MapReferenceResolver;

/**
 * Simple Pool of Kryo objects that allows a core of Kryo objects to remain
 * available for re-use, while requiring only a single thread access to a Kryo instance
 * at a time. If the pool is exhausted, then this will create new Kryo objects.
 *
 * @author Scott Fines
 * Created on: 8/15/13
 */
public class KryoPool extends AbstractKryoPool {
    public KryoPool(int poolSize) {
        super(poolSize);
    }

    @Override
    public Kryo newInstance() {
        Kryo next = new Kryo(new DefaultClassResolver(),new MapReferenceResolver());
        if(kryoRegistry!=null)
            kryoRegistry.register(next);
        return next;
    }
}
