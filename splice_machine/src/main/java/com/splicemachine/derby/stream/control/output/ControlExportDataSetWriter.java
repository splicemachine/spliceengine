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

package com.splicemachine.derby.stream.control.output;

import com.splicemachine.access.api.DistributedFileSystem;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.types.SQLInteger;
import com.splicemachine.db.iapi.types.SQLLongint;
import com.splicemachine.db.impl.sql.execute.ValueRow;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.impl.sql.execute.operations.LocatedRow;
import com.splicemachine.derby.impl.sql.execute.operations.export.ExportFile;
import com.splicemachine.derby.impl.sql.execute.operations.export.ExportOperation;
import com.splicemachine.derby.stream.control.ControlDataSet;
import com.splicemachine.derby.stream.function.SpliceFunction2;
import com.splicemachine.derby.stream.iapi.DataSet;
import com.splicemachine.derby.stream.iapi.TableWriter;
import com.splicemachine.derby.stream.output.DataSetWriter;
import com.splicemachine.derby.stream.output.ExportDataSetWriterBuilder;
import com.splicemachine.si.api.txn.TxnView;
import com.splicemachine.si.impl.driver.SIDriver;
import org.apache.commons.collections.iterators.SingletonIterator;
import org.apache.log4j.Logger;
import java.io.OutputStream;
import java.nio.file.StandardOpenOption;
import java.util.Iterator;
import java.util.zip.GZIPOutputStream;

/**
 * @author Scott Fines
 *         Date: 1/8/16
 */
public class ControlExportDataSetWriter<V> implements DataSetWriter{
    private final String path;
    private final SpliceFunction2<? extends SpliceOperation, OutputStream, Iterator<V>, Integer> exportFunction;
    private final DataSet<V> dataSet;
    private static final Logger LOG = Logger.getLogger(ControlExportDataSetWriter.class);

    public ControlExportDataSetWriter(String path,
                                      SpliceFunction2<? extends SpliceOperation, OutputStream, Iterator<V>, Integer> exportFunction,
                                      DataSet<V> dataSet){

        this.path=path;
        this.exportFunction=exportFunction;
        this.dataSet = dataSet;
    }

    @Override
    public DataSet<LocatedRow> write() throws StandardException{
        Integer count;
        String extension = ".csv";
        SpliceOperation operation=exportFunction.getOperation();
        boolean isCompressed = path.endsWith(".gz");
        if(!isCompressed && operation instanceof ExportOperation){
            ExportOperation op=(ExportOperation)exportFunction.getOperation();
            isCompressed=op.getExportParams().isCompression();
            if(isCompressed){
                extension+=".gz";
            }
        }
        try{
            final DistributedFileSystem dfs=SIDriver.driver().fileSystem();
            dfs.createDirectory(path,false);
            // The 'part-r-00000' naming convention is what spark uses so we are consistent on control side
            try(OutputStream fileOut =dfs.newOutputStream(path /*directory*/,"part-r-00000"+extension/*file*/,StandardOpenOption.CREATE)){
                OutputStream toWrite=fileOut;
                if(isCompressed){
                    toWrite=new GZIPOutputStream(fileOut);
                }
                count=exportFunction.call(toWrite,dataSet.toLocalIterator());
            }
            dfs.touchFile(path, ExportFile.SUCCESS_FILE);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        ValueRow valueRow = new ValueRow(2);
        valueRow.setColumn(1,new SQLLongint(count));
        valueRow.setColumn(2,new SQLInteger(0));
        return new ControlDataSet<>(new SingletonIterator(new LocatedRow(valueRow)));
    }

    @Override
    public void setTxn(TxnView childTxn){
        throw new UnsupportedOperationException("IMPLEMENT");
    }

    @Override
    public TableWriter getTableWriter(){
       return null;
    }

    @Override
    public TxnView getTxn(){
        throw new UnsupportedOperationException("IMPLEMENT");
    }

    @Override
    public byte[] getDestinationTable(){
        throw new UnsupportedOperationException("IMPLEMENT");
    }

    public static class Builder<V> implements ExportDataSetWriterBuilder{
        private String directory;
        private SpliceFunction2<? extends SpliceOperation,OutputStream,Iterator<V>,Integer> exportFunction;
        private final DataSet<V> dataSet;

        public Builder(DataSet<V> dataSet){
            this.dataSet=dataSet;
        }

        @Override
        public ExportDataSetWriterBuilder directory(String directory){
            this.directory = directory;
            return this;
        }

        @Override
        public /*<Op extends SpliceOperation>*/ ExportDataSetWriterBuilder exportFunction(SpliceFunction2/*<Op,OutputStream,Iterator<V>, Integer>*/ exportFunction){
            this.exportFunction = exportFunction;
            return this;
        }


        @Override
        public DataSetWriter build(){
            return new ControlExportDataSetWriter<>(directory,exportFunction,dataSet);
        }
    }
}
