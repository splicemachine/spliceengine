/*
 * Copyright (c) 2012 - 2017 Splice Machine, Inc.
 *
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

package com.splicemachine.si.impl.data;

import com.splicemachine.kvpair.KVPair;
import com.splicemachine.si.api.data.OperationStatusFactory;
import com.splicemachine.si.api.server.ConstraintChecker;
import com.splicemachine.storage.DataResult;
import com.splicemachine.storage.MutationStatus;

import java.io.IOException;

/**
 * @author Scott Fines
 *         Date: 12/16/15
 */
public class NoOpConstraintChecker implements ConstraintChecker{
    private final OperationStatusFactory opFactory;

    public NoOpConstraintChecker(OperationStatusFactory opFactory){
        this.opFactory=opFactory;
    }

    @Override
    public MutationStatus checkConstraint(KVPair mutation,DataResult existingRow) throws IOException{
        return opFactory.success();
    }
}
