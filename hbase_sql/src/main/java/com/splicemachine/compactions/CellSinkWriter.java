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
 *
 */

package com.splicemachine.compactions;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.regionserver.StoreFile;
import org.apache.hadoop.hbase.regionserver.compactions.Compactor;

import java.io.Closeable;
import java.io.IOException;

/**
 * @author Scott Fines
 *         Date: 12/12/16
 */
public class CellSinkWriter implements StoreFileWriter{
    private Compactor.CellSink writer;

    public CellSinkWriter(Compactor.CellSink writer){
        super();
        this.writer=writer;
    }

    @Override
    public void close() throws IOException{
        if(writer instanceof Closeable)
            ((Closeable)writer).close();
        else if(writer instanceof StoreFile.Writer)
            ((StoreFile.Writer)writer).close();
        else throw new IllegalStateException("Unknown writer!");
    }

    @Override
    public Path getPath(){
        throw new UnsupportedOperationException();
    }

    @Override
    public void append(Cell cell) throws IOException{
        writer.append(cell);
    }

}
