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
