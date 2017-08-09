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

package com.splicemachine.derby.ddl;

import java.io.IOException;

import com.splicemachine.derby.impl.sql.execute.operations.scanner.TableScannerBuilder;
import com.splicemachine.pipeline.RowTransformer;
import com.splicemachine.pipeline.writehandler.WriteHandler;

/**
 * Interface describing descriptors used for DDL operations that enforce transactionality
 * when altering tables by using the intercept/populate method.
 * <p/>
 * These descriptors can be thought of as factories that create components to facilitate
 * transforming original table rows to altered table rows and writing them to the new
 * conglomerate.
 *
 * @author Jeff Cunningham
 *         Date: 3/27/15
 */
public interface TransformingDDLDescriptor extends TentativeDDLDesc{

    /**
     * Create a RowTransformer that can intercept rows from the original table and write them
     * to the altered table.
     * @return the appropriate RowTransformer
     * @throws IOException
     */
    RowTransformer createRowTransformer() throws IOException;

    /**
     * Create a WriteHandler that will use a RowTransformer to map and transform rows from
     * the original table to the altered table.
     * @param transformer the RowTransformer to use
     * @return the appropriate WriteHandler
     * @throws IOException
     */
    WriteHandler createWriteHandler(RowTransformer transformer) throws IOException;

    /**
     * A TableScannerBuilder gets passed in. Set key/row ordering as appropriate for the
     * TentativeDDL type and return.<br/>
     * This builder creates the table scanner that will read, decode and return an ExecRow
     * for every row in the source conglomerate.
     * @param builder the builder to specialize
     * @return the specialized builder
     * @throws IOException
     */
    TableScannerBuilder setScannerBuilderProperties(TableScannerBuilder builder) throws IOException;
}
