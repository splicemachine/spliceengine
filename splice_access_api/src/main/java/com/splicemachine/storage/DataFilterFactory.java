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
 */

package com.splicemachine.storage;

/**
 * Factory for creating different DataFilters. Each architecture is expected to provide an architecture
 * specific version of this.
 *
 * @author Scott Fines
 *         Date: 12/16/15
 */
public interface DataFilterFactory{
    /**
     * Filter rows based on whether or not the value at the specified family and qualifier is <em>equal</em>
     * to the specified value.
     *
     * @param family the family to check
     * @param qualifier the qualifier of the column to check
     * @param value the value to check
     * @return a DataFilter which performed the equality check.
     */
    DataFilter singleColumnEqualsValueFilter(byte[] family,byte[] qualifier,byte[] value);

    DataFilter allocatedFilter(byte[] localAddress);
}
