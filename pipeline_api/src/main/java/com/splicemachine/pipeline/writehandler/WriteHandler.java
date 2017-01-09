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

package com.splicemachine.pipeline.writehandler;

import com.splicemachine.pipeline.context.WriteContext;
import com.splicemachine.storage.Record;
import java.io.IOException;

/**
 * Simple Interface for Handling Writes for a giving write pipeline (WriteContext).
 * 
 * @author Scott Fines
 * Created on: 4/30/13
 */
public interface WriteHandler {

	/**
	 * Process the mutation with the given handler
	 */
    void next(Record mutation, WriteContext ctx);

    /**
     * Flush the writes with the given handler.  This method assumes possible asynchronous underlying calls.
     */
    void flush(WriteContext ctx) throws IOException;

    /**
     * This closes the writes with the given handler.  It will need to wait for all underlying calls to finish or
     * throw exceptions.
     */
    void close(WriteContext ctx) throws IOException;

}
