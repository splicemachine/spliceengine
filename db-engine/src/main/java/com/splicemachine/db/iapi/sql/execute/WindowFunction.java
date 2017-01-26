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
 * All such Splice Machine modifications are Copyright 2012 - 2017 Splice Machine, Inc.,
 * and are licensed to you under the GNU Affero General Public License.
 */

package com.splicemachine.db.iapi.sql.execute;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.services.io.Formatable;
import com.splicemachine.db.iapi.services.io.FormatableHashtable;
import com.splicemachine.db.iapi.services.loader.ClassFactory;
import com.splicemachine.db.iapi.types.DataTypeDescriptor;
import com.splicemachine.db.iapi.types.DataValueDescriptor;

/**
 * Apply function over one or more arguments.
 *
 * @author Jeff Cunningham
 *         Date: 8/4/14
 */
public interface WindowFunction extends ExecAggregator, Formatable {

    /**
     Set's up the aggregate for processing.

     @param  classFactory Database-specific class factory.
     @param  windowFunctionName  For builtin functions, this is a SQL name like RANK.
     @param  returnDataType  The type returned by the getResult() method.
     @param functionSpecificArgs function arguments specific to each window function.
     */
    WindowFunction setup(ClassFactory classFactory,
                        String windowFunctionName,
                        DataTypeDescriptor returnDataType,
                        FormatableHashtable functionSpecificArgs);


    /**
     * Apply the function to the given dvd[].<br/>
     * Function result can be acquired by calling {@link #getResult()}.
     *
     * @param valueDescriptors the column value or column values used to apply the function.
     *                         If a function aggregates of just one column, like a traditional
     *                         aggregate function, then it should just used the DVD at
     *                         <code>valueDescriptors[0]</code>. In other words, it's up to
     *                         the discretion of the function as to what to input and what to
     *                         output.
     */
    void accumulate(DataValueDescriptor[] valueDescriptors) throws StandardException;

    /**
     * Produces the result to be returned by the query.
     * The last processing of the aggregate.
     *
     * @exception StandardException on error
     */
    DataValueDescriptor getResult() throws StandardException;

    /**
     * Call to clear any accumulated state.
     */
    void reset();

    /**
     * Factory method to create a new instance of the particular splice-side
     * window function.  Assumes
     * {@link #setup(com.splicemachine.db.iapi.services.loader.ClassFactory, String,
     * com.splicemachine.db.iapi.types.DataTypeDescriptor, FormatableHashtable) setup}
     * has been called first.
     * @return a configured new instance of this instance.
     */
    WindowFunction newWindowFunction();
}
