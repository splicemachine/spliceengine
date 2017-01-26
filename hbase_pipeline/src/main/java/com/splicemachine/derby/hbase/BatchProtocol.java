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

package com.splicemachine.derby.hbase;

import org.apache.hadoop.hbase.coprocessor.CoprocessorService;
import java.io.IOException;

/**
 * Protocol for implementing Batch mutations as coprocessor execs.
 *
 * @author Scott Fines
 * Created on: 3/11/13
 */
public interface BatchProtocol extends CoprocessorService {

    /**
     * Apply all the Mutations to N regions in a single, synchronous, N bulk operations.
     *
     * @param bulkWrites the mutations to apply
     * @throws IOException if something goes wrong applying the mutation
     */
    byte[] bulkWrites(byte[] bulkWrites) throws IOException;

}
