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

package com.splicemachine.pipeline;

import java.io.Closeable;
import java.io.IOException;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.storage.Record;

/**
 * Transformer interface for taking a base row and transforming it to another representation.
 * 
 *
 */
public interface RowTransformer extends Closeable  {

    /**
     * Transform source ExecRow to a target KVPair.<br/>
     * This method is used during the populate phase of alter table, when copying
     * existing rows from the source conglomerate to the new target.
     * @param row a row from the original table.
     * @return the transformed KVPair of the row to be inserted into the new conglomerate.
     * @throws StandardException
     * @throws IOException
     */
    Record transform(ExecRow row) throws StandardException, IOException;

    /**
     * Transform a source KVPair to a target KVPair, where a KVPair represents a
     * row in a table that's being altered. The table may have been altered by
     * adding or removing a column, or constraint.<br/>
     * This method is used during the intercept phase of alter table.
     * @param kvPair the row to transform.
     * @return the transformed row.
     * @throws StandardException
     * @throws IOException
     */
    Record transform(Record kvPair) throws StandardException, IOException;

}
