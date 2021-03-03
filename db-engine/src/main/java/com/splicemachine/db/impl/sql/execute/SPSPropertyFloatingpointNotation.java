/*
 * Copyright (c) 2012 - 2020 Splice Machine, Inc.
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

package com.splicemachine.db.impl.sql.execute;

import com.splicemachine.db.catalog.UUID;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.compile.CompilerContext;
import com.splicemachine.db.iapi.sql.compile.Node;
import com.splicemachine.db.impl.sql.compile.StatementNode;
import com.splicemachine.db.impl.sql.compile.ValueNode;

import java.sql.Types;

/**
 * An SPS property that governs floating-point fields in a query
 */
public class SPSPropertyFloatingpointNotation extends SPSProperty {

    protected SPSPropertyFloatingpointNotation(UUID uuid, String name) {
        super(uuid, name);
    }

    @Override
    protected void checkAndAddDependency(final Node node, CompilerContext cc) throws StandardException {
        if(node instanceof ValueNode && ((ValueNode)node).getTypeServices() != null) {
            int nodeType = ((ValueNode)node).getTypeServices().getJDBCTypeId();
            if(nodeType == Types.DOUBLE || nodeType == Types.FLOAT || nodeType == Types.REAL) {
                cc.createDependency(this);
            }
        }
    }
}
