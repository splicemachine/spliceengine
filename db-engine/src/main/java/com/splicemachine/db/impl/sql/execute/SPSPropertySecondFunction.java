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
import com.splicemachine.db.impl.sql.compile.CollectNodesVisitor;
import com.splicemachine.db.impl.sql.compile.SecondFunctionNode;
import com.splicemachine.db.impl.sql.compile.StatementNode;

public class SPSPropertySecondFunction extends SPSProperty{

    protected SPSPropertySecondFunction(UUID uuid, String name) {
        super(uuid, name);
    }

    @Override
    protected void checkAndAddDependency(final StatementNode statementNode, CompilerContext cc) throws StandardException {
        assert statementNode != null;
        assert cc != null;
        final CollectNodesVisitor v = new CollectNodesVisitor(SecondFunctionNode.class);
        statementNode.accept(v);
        if(!v.getList().isEmpty()) {
            cc.createDependency(this);
        }
    }
}
