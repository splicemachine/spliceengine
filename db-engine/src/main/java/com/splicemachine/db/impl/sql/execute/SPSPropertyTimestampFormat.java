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
import com.splicemachine.db.iapi.types.DataTypeDescriptor;
import com.splicemachine.db.impl.sql.compile.CastNode;
import com.splicemachine.db.impl.sql.compile.StatementNode;
import com.splicemachine.db.impl.sql.compile.ValueNode;

import java.sql.Types;

/**
 * An SPS property that governs timestamp field(s) in a query.
 */
final class SPSPropertyTimestampFormat extends SPSProperty {

    protected SPSPropertyTimestampFormat(final UUID uuid, final String name) {
        super(uuid, name);
    }

    protected void checkAndAddDependency(final Node node, CompilerContext cc) throws StandardException {
        if(node instanceof CastNode) {
            CastNode castNode = (CastNode)node;
            ValueNode castOperand = castNode.getCastOperand();
            if(castOperand != null
               && castOperand.getTypeServices() != null
               && castOperand.getTypeServices().getJDBCTypeId() == Types.TIMESTAMP
               && DataTypeDescriptor.isCharacterType(castNode.getTypeServices().getJDBCTypeId())) {
                cc.createDependency(this);
            }
        }
    }
}
