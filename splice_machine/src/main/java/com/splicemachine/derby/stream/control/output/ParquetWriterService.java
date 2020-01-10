/*
 * Copyright (c) 2012 - 2020 Splice Machine, Inc.
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
 *
 */

package com.splicemachine.derby.stream.control.output;

import com.splicemachine.db.iapi.error.StandardException;

import java.util.Iterator;
import java.util.ServiceLoader;

public class ParquetWriterService {
    private static volatile ParquetWriterFactory factory;

    public static ParquetWriterFactory getFactory() {
        ParquetWriterFactory result = factory;
        if (result == null) {
            result = loadFactory();
        }
        return result;
    }

    private static synchronized ParquetWriterFactory loadFactory() {
        if (factory != null)
            return factory;

        ServiceLoader<ParquetWriterFactory> serviceLoader = ServiceLoader.load(ParquetWriterFactory.class);
        Iterator<ParquetWriterFactory> it = serviceLoader.iterator();
        if (!it.hasNext()) {
            throw new IllegalStateException("No ParquetWriterFactory found!");
        }
        factory = it.next();
        return factory;
    }
}
