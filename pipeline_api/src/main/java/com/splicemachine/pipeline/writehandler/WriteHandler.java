/*
 * Copyright (c) 2012 - 2020 Splice Machine, Inc.
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

package com.splicemachine.pipeline.writehandler;

import com.splicemachine.kvpair.KVPair;
import com.splicemachine.pipeline.context.WriteContext;

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
    void next(KVPair mutation, WriteContext ctx);

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
