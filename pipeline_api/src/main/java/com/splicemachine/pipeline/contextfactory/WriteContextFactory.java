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

package com.splicemachine.pipeline.contextfactory;

import com.splicemachine.access.api.ServerControl;
import com.splicemachine.ddl.DDLMessage;
import com.splicemachine.pipeline.context.WriteContext;
import com.splicemachine.pipeline.writehandler.SharedCallBufferFactory;
import com.splicemachine.si.api.txn.TxnView;
import java.io.IOException;

/**
 * @author Scott Fines
 *         Created on: 4/30/13
 */
public interface WriteContextFactory<T> {

    WriteContext create(SharedCallBufferFactory indexSharedCallBuffer,
                        TxnView txn,
                        T key,
                        ServerControl env) throws IOException, InterruptedException;

    WriteContext create(SharedCallBufferFactory indexSharedCallBuffer,
                        TxnView txn,
                        T key,
                        int expectedWrites,
                        boolean skipIndexWrites,
                        ServerControl env) throws IOException, InterruptedException;

    /**
     * Creates a context that only updates side effects.
     */
    WriteContext createPassThrough(SharedCallBufferFactory indexSharedCallBuffer,
                                   TxnView txn,
                                   T key,
                                   int expectedWrites,
                                   ServerControl env) throws IOException, InterruptedException;

    void addDDLChange(DDLMessage.DDLChange ddlChange);

    void close();

    void prepare();

    boolean hasDependentWrite(TxnView txn) throws IOException, InterruptedException;

}
