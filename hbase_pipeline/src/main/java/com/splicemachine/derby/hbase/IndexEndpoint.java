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

import java.io.IOException;
import com.splicemachine.pipeline.client.BulkWrites;
import com.splicemachine.pipeline.client.BulkWritesResult;

public interface IndexEndpoint {

    /**
     * Perform the actual bulk writes.
     * The logic is as follows: Determine whether the writes are independent or dependent.  Get a "permit" for the writes.
     * And then perform the writes through the region's write pipeline.
     *
     * @param bulkWrites the bulks writes to perform on the region
     * @return the results of the bulk write operation
     * @throws IOException
     */
	BulkWritesResult bulkWrite(BulkWrites bulkWrites) throws IOException;

	SpliceIndexEndpoint getBaseIndexEndpoint();
}
