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

package com.splicemachine.db.impl.sql.compile.aggregatepush;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.services.context.ContextManager;
import com.splicemachine.db.iapi.sql.compile.C_NodeTypes;
import com.splicemachine.db.iapi.sql.compile.NodeFactory;
import com.splicemachine.db.impl.sql.compile.*;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by yxia on 4/17/17.
 */
public class AggregatePushingContext {
    // the SelectNode where original aggregates reside in
    private SelectNode selectNode;
    // the list of SingleTableAggrPushContext
    private List<SingleTableAggrPushContext> ctxList = null;

    AggregatePushingContext(SelectNode node) {
        selectNode = node;
        ctxList = new ArrayList<>();
    }

    public void computeDuplicationFactors() throws StandardException {
        for (SingleTableAggrPushContext aContext : ctxList) {
            computeDuplicationFactor(aContext);
        }
        return;
    }


    public void computeDuplicationFactor(SingleTableAggrPushContext context) throws StandardException {
        ContextManager cm = selectNode.getContextManager();
        NodeFactory nf = selectNode.getNodeFactory();
        if (!context.isDuplicateSensitive())
            return;

        ValueNode duplicationFactor = null;
        for (int i=0; i<ctxList.size(); i++) {
            SingleTableAggrPushContext aContext = ctxList.get(i);
            if (aContext == context)
                continue;

            if (!aContext.isPushed())
                continue;
            ResultColumn countColumn = aContext.getFromSubquery().getResultColumns().
                    elementAt(aContext.getCountColId()-1);
            ColumnReference countNode = (ColumnReference)nf.getNode(C_NodeTypes.COLUMN_REFERENCE,
                                                                    countColumn.getName(),
                                                                    null,
                                                                    cm);
            if (duplicationFactor == null)
                duplicationFactor = countNode;
            else {
                duplicationFactor = (ValueNode) nf.getNode(C_NodeTypes.BINARY_TIMES_OPERATOR_NODE,
                                                           duplicationFactor,
                                                           countNode,
                                                           cm);
            }
        }
        context.setDuplicationFactor(duplicationFactor);

        return;
    }

    public void addContext(SingleTableAggrPushContext context) {
        ctxList.add(context);
        return;
    }

    public int getNumOfContexts() {
        if (ctxList == null)
            return 0;
        return ctxList.size();
    }

    public SingleTableAggrPushContext getContext(int i) {
        if (ctxList == null || i >= ctxList.size())
            return null;
        return ctxList.get(i);
    }
}
