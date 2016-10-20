/*
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
 * Splice Machine, Inc. has modified this file.
 *
 * All Splice Machine modifications are Copyright 2012 - 2016 Splice Machine, Inc.,
 * and are licensed to you under the License; you may not use this file except in
 * compliance with the License.
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 */
package com.splicemachine.db.impl.ast;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.compile.Visitable;
import com.splicemachine.db.impl.sql.compile.FromTable;
import com.splicemachine.db.impl.sql.compile.WindowFunctionNode;
import org.apache.log4j.Logger;

import static com.splicemachine.db.iapi.sql.compile.CompilerContext.DataSetProcessorType.FORCED_SPARK;

import java.util.List;



/**
 * Created by jfilali on 10/10/16.
 * This visitor will force the dataSetProcessor To Spark if one the operator
 * is a window function.
 * Currently window function will be supported by spark only
 */
public class WindowFunctionVisitor extends AbstractSpliceVisitor{

    public static List<FromTable> getSourceNode(WindowFunctionNode node) throws StandardException {
        return CollectingVisitorBuilder.forClass(FromTable.class).collect(node);
    }

    private static Logger LOG = Logger.getLogger(WindowFunctionVisitor.class);

    @Override
    public Visitable visit(WindowFunctionNode node) throws StandardException {
        getSourceNode(node)
        .forEach( source -> source.setDataSetProcessorType(FORCED_SPARK));

        LOG.info("Window Function can only be executed on top of Spark, " +
                "therefore dataSetProcessorType has been set to FORCED_SPARK");

        return node;
    }

}
