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
