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

package com.splicemachine.pipeline;

import java.io.Closeable;
import java.io.IOException;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.kvpair.KVPair;

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
    KVPair transform(ExecRow row) throws StandardException, IOException;

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
    KVPair transform(KVPair kvPair) throws StandardException, IOException;

}
