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

package com.splicemachine.storage;

import com.splicemachine.metrics.TimeView;
import com.splicemachine.storage.DataCell;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.util.List;

/**
 * @author Scott Fines
 *         Date: 12/14/15
 */
public interface DataScanner extends AutoCloseable{

    @Nonnull List<DataCell> next(int limit) throws IOException;

    TimeView getReadTime();

    long getBytesOutput();

    long getRowsFiltered();

    long getRowsVisited();

    @Override void close() throws IOException;

    Partition getPartition();
}
