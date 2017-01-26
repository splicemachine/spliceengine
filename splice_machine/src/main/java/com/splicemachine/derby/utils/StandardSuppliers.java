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

package com.splicemachine.derby.utils;

import com.splicemachine.db.iapi.error.StandardException;

/**
 * @author Scott Fines
 *         Created on: 10/31/13
 */
public class StandardSuppliers {

    private StandardSuppliers(){}

    private static final StandardSupplier EMPTY_SUPPLIER = new StandardSupplier() {
        @Override
        public Object get() throws StandardException {
            return null;
        }
    };

    @SuppressWarnings("unchecked")
    public static <T>StandardSupplier<T> emptySupplier(){
        return (StandardSupplier<T>) EMPTY_SUPPLIER;
    }
}
