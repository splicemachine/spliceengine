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

package com.splicemachine.db.impl.sql.compile;

import com.splicemachine.db.catalog.AliasInfo;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.reference.SQLState;
import com.splicemachine.db.iapi.services.compiler.MethodBuilder;
import com.splicemachine.db.iapi.services.loader.ClassFactory;
import com.splicemachine.db.iapi.services.loader.ClassInspector;
import com.splicemachine.db.iapi.services.sanity.SanityManager;
import com.splicemachine.db.iapi.sql.compile.AggregateDefinition;
import com.splicemachine.db.iapi.sql.compile.C_NodeTypes;
import com.splicemachine.db.iapi.sql.compile.CompilerContext;
import com.splicemachine.db.iapi.sql.dictionary.AliasDescriptor;
import com.splicemachine.db.iapi.sql.dictionary.DataDictionary;
import com.splicemachine.db.iapi.sql.dictionary.SchemaDescriptor;
import com.splicemachine.db.iapi.types.DataTypeDescriptor;
import com.splicemachine.db.iapi.types.DataValueDescriptor;

import java.util.List;

import static com.splicemachine.db.iapi.sql.compile.AggregateDefinition.FunctionType;
import static com.splicemachine.db.iapi.sql.compile.AggregateDefinition.fromString;

/**
 * A special Aggregate Node for STRING_AGG that handles the second argument.
 */

public class StringAggregateNode extends AggregateNode {
    private DataValueDescriptor parameter;
    private OrderByList orderByList;

    /**
     * Intializer.  Used for user defined and internally defined aggregates.
     * Called when binding a StaticMethodNode that we realize is an aggregate.
     *
     * @param operand       the value expression for the aggregate
     * @param uadClass      the class name for user aggregate definition for the aggregate
     *                      or the Class for the internal aggregate type.
     * @param distinct      boolean indicating whether this is distinct
     *                      or not.
     * @param aggregateName the name of the aggregate from the user's perspective,
     *                      e.g. MAX
     * @param separator     separator used for STRING_AGG
     * @param orderByList   columns specifying the order that concatenation should happen in
     * @throws StandardException on error
     */
    public void init
    (
            Object operand,
            Object uadClass,
            Object distinct,
            Object aggregateName,
            Object separator,
            Object orderByList
    ) throws StandardException {
        super.init(operand, uadClass, distinct, aggregateName);
        if (separator != null && separator instanceof ConstantNode) {
            parameter = ((ConstantNode) separator).getValue();
        }
        this.orderByList = (OrderByList) orderByList;
    }

    public DataValueDescriptor getParameter() {
        return parameter;
    }

}
