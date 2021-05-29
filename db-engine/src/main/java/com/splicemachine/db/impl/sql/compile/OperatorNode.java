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

import java.lang.reflect.Modifier;
import com.splicemachine.db.iapi.services.classfile.VMOpcode;
import com.splicemachine.db.iapi.services.compiler.LocalField;
import com.splicemachine.db.iapi.services.compiler.MethodBuilder;
import com.splicemachine.db.iapi.types.SqlXmlUtil;

/**
 * Abstract base-class for the various operator nodes: UnaryOperatorNode,
 * BinaryOperatorNode and TernarnyOperatorNode.
 */
public abstract class OperatorNode extends ValueNode {

    /**
     * <p>
     * Generate code that pushes an SqlXmlUtil instance onto the stack. The
     * instance will be created and cached in the activation's constructor, so
     * that we don't need to create a new instance for every row.
     * </p>
     *
     * <p>
     * If the {@code xmlQuery} parameter is non-null, there will also be code
     * that compiles the query when the SqlXmlUtil instance is created.
     * </p>
     *
     * @param acb builder for the class in which the generated code lives
     * @param mb builder for the method that implements this operator
     * @param xmlQuery the XML query to be executed by the operator, or
     * {@code null} if this isn't an XMLEXISTS or XMLQUERY operator
     * @param xmlOpName the name of the operator (ignored if {@code xmlQuery}
     * is {@code null})
     */
    static void pushSqlXmlUtil(
            ExpressionClassBuilder acb, MethodBuilder mb,
            String xmlQuery, String xmlOpName) {

        // Create a field in which the instance can be cached.
        LocalField sqlXmlUtil = acb.newFieldDeclaration(
                Modifier.PRIVATE | Modifier.FINAL, SqlXmlUtil.class.getName());

        // Add code that creates the SqlXmlUtil instance in the constructor.
        MethodBuilder constructor = acb.getConstructor();
        constructor.pushNewStart(SqlXmlUtil.class.getName());
        constructor.pushNewComplete(0);
        constructor.putField(sqlXmlUtil);

        // Compile the query, if one is specified.
        if (xmlQuery == null) {
            // No query. The SqlXmlUtil instance is still on the stack. Pop it
            // to restore the initial state of the stack.
            constructor.pop();
        } else {
            // Compile the query. This will consume the SqlXmlUtil instance
            // and leave the stack in its initial state.
            constructor.push(xmlQuery);
            constructor.push(xmlOpName);
            constructor.callMethod(
                    VMOpcode.INVOKEVIRTUAL, SqlXmlUtil.class.getName(),
                    "compileXQExpr", "void", 2);
        }

        // Read the cached value and push it onto the stack in the method
        // generated for the operator.
        mb.getField(sqlXmlUtil);
    }
}
