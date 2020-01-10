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

package com.splicemachine.derby.ddl;

import java.io.IOException;

import com.splicemachine.derby.impl.sql.execute.operations.scanner.TableScannerBuilder;
import com.splicemachine.derby.utils.marshall.KeyEncoder;
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
     * Create a RowTransformer that can map rows from the original table
     * to the altered table.
     * @param keyEncoder the key encoder to use when not working with PKs - no rowKey munging.
     * @return the appropriate RowTransformer
     * @throws IOException
     */
    RowTransformer createRowTransformer(KeyEncoder keyEncoder) throws IOException;

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
