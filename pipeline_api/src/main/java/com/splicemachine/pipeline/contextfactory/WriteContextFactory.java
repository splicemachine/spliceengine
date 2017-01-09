/*
 * Copyright 2012 - 2016 Splice Machine, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */

package com.splicemachine.pipeline.contextfactory;

import com.splicemachine.access.api.ServerControl;
import com.splicemachine.ddl.DDLMessage;
import com.splicemachine.pipeline.context.WriteContext;
import com.splicemachine.pipeline.writehandler.SharedCallBufferFactory;
import com.splicemachine.si.api.txn.Txn;
import java.io.IOException;

/**
 * @author Scott Fines
 *         Created on: 4/30/13
 */
public interface WriteContextFactory<T> {

    WriteContext create(SharedCallBufferFactory indexSharedCallBuffer,
                        Txn txn,
                        T key,
                        ServerControl env) throws IOException, InterruptedException;

    WriteContext create(SharedCallBufferFactory indexSharedCallBuffer,
                        Txn txn,
                        T key,
                        int expectedWrites,
                        boolean skipIndexWrites,
                        ServerControl env) throws IOException, InterruptedException;

    /**
     * Creates a context that only updates side effects.
     */
    WriteContext createPassThrough(SharedCallBufferFactory indexSharedCallBuffer,
                                   Txn txn,
                                   T key,
                                   int expectedWrites,
                                   ServerControl env) throws IOException, InterruptedException;

    void addDDLChange(DDLMessage.DDLChange ddlChange);

    void close();

    void prepare();

    boolean hasDependentWrite(Txn txn) throws IOException, InterruptedException;

}
