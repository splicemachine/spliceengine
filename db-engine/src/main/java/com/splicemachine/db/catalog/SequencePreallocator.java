/*
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
 * Some parts of this source code are based on Apache Derby, and the following notices apply to
 * Apache Derby:
 *
 * Apache Derby is a subproject of the Apache DB project, and is licensed under
 * the Apache License, Version 2.0 (the "License"); you may not use these files
 * except in compliance with the License. You may obtain a copy of the License at:
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 * Splice Machine, Inc. has modified the Apache Derby code in this file.
 *
 * All such Splice Machine modifications are Copyright 2012 - 2020 Splice Machine, Inc.,
 * and are licensed to you under the GNU Affero General Public License.
 */

package com.splicemachine.db.catalog;

/**
 * <p>
 * Logic to determine how many values to pre-allocate for a sequence.
 * By default, Derby boosts concurrency by pre-allocating ranges of numbers for sequences.
 * During orderly database shutdown, the unused numbers are reclaimed so that shutdown will
 * not create holes in the sequences.  However, holes may appear if the application fails to shut
 * down its databases before the JVM exits.
 * </p>
 *
 * <p>
 * Logic in this class is called every time Derby needs to pre-allocate a new range of sequence
 * values. Users can override Derby's default behavior by writing their own implementation of this
 * interface and then setting the following Derby property:
 * </p>
 *
 * <pre>
 *  -Dderby.language.sequence.preallocator=com.acme.MySequencePreallocator
 * </pre>
 *
 * <p>
 * Classes which implement this interface must also provide a public 0-arg constructor so
 * that Derby can instantiate them. Derby will instantiate a SequencePreallocator for every sequence.
 * </p>
 *
 */
public  interface   SequencePreallocator
{
    /**
     * <p>
     * This method returns the size of the next pre-allocated range for the specified
     * sequence. Names are case-sensitive, as specified in CREATE SEQUENCE
     * and CREATE TABLE statements.
     * </p>
     *
     * @param schemaName Name of schema holding the sequence.
     * @param sequenceName Specific name of the sequence.
     */
    int nextRangeSize
    (
            String schemaName,
            String sequenceName
    );
    
}



