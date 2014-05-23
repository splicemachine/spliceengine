package com.splicemachine.derby.utils;

import com.splicemachine.derby.iapi.sql.execute.SpliceNoPutResultSet;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.iapi.sql.execute.SpliceRuntimeContext;
import com.splicemachine.derby.iapi.sql.execute.StandardCloseable;
import com.splicemachine.metrics.IOStats;
import com.splicemachine.metrics.Metrics;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.sql.execute.ExecRow;
import org.apache.derby.iapi.sql.execute.NoPutResultSet;
import com.splicemachine.stats.Metrics;
import com.splicemachine.stats.TimeView;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.sql.execute.ExecRow;
import org.apache.derby.iapi.sql.execute.NoPutResultSet;
import com.splicemachine.derby.iapi.sql.execute.SpliceNoPutResultSet;
import com.splicemachine.derby.iapi.sql.execute.SpliceRuntimeContext;

import java.io.IOException;
import java.util.Iterator;
import java.util.concurrent.Callable;

/**
 * @author Scott Fines
 *         Created on: 11/2/13
 */
public class StandardIterators {

    private StandardIterators() {
    }

    public static <T> MeasuredStandardIterator<T> wrap(Iterable<T> data){
        return new IteratorStandardIterator<T>(data.iterator());
    }

    public static MeasuredStandardIterator<ExecRow> wrap(SpliceOperation op) {
        return new SpliceOpStandardIterator(op);
    }

    public static MeasuredStandardIterator<ExecRow> wrap(SpliceNoPutResultSet NPRS){
        return new ResultSetStandardIterator(NPRS);
    }

    public static <T> StandardIterator<T> wrap(Callable<T> callable) {
        return new CallableStandardIterator<T>(callable);
    }

    public static <T> StandardIterator<T> wrap(Callable<T> callable, StandardCloseable c) {
        return new CallableStandardIterator<T>(callable, c);
    }

    private static class IteratorStandardIterator<T> implements MeasuredStandardIterator<T>{
        private final Iterator<T> delegate;

        private IteratorStandardIterator(Iterator<T> delegate) {
            this.delegate = delegate;
        }

        @Override
        public void open() throws StandardException, IOException {
        }//no-op

        @Override
        public void close() throws StandardException, IOException {
        } //no-op

        @Override
        public T next(SpliceRuntimeContext spliceRuntimeContext) throws
                                                                 StandardException,
                                                                 IOException {
            if (!delegate.hasNext())
                return null;
            return delegate.next();
        }

        @Override
        public TimeView getRemoteReadTime() {
            return Metrics.noOpTimeView();
        }

        @Override
        public long getRemoteBytesRead(){
            return 0;
        }

        @Override
        public long getRemoteRowsRead(){
            return 0;
        }
    }

    private static class SpliceOpStandardIterator implements MeasuredStandardIterator<ExecRow> {
        private final SpliceOperation op;

        private SpliceOpStandardIterator(SpliceOperation op) {
            this.op = op;
        }

        @Override
        public void open() throws StandardException, IOException {
            op.open();
        }

        @Override
        public ExecRow next(SpliceRuntimeContext ctx) throws StandardException,
                                                             IOException {
            return op.nextRow(ctx);
        }

        @Override
        public void close() throws StandardException, IOException {
            op.close();
        }

        @Override
        public TimeView getRemoteReadTime() {
            return Metrics.noOpTimeView();
        };

        @Override
        public long getRemoteBytesRead(){
            return 0;
        };

        @Override
        public long getRemoteRowsRead(){
            return 0;
        };
    }

    private static class SpliceResultSetStandardIterator implements MeasuredStandardIterator<ExecRow> {
        private final SpliceNoPutResultSet noPut;

        private SpliceResultSetStandardIterator(SpliceNoPutResultSet noPut) {
            this.noPut = noPut;
        }

        @Override
        public void open() throws StandardException, IOException {
            noPut.open();
            noPut.openCore();
        }

        @Override
        public void close() throws StandardException{
            noPut.close();
        }

        @Override
        public ExecRow next(SpliceRuntimeContext spliceRuntimeContext) throws
                                                                       StandardException,
                                                                       IOException {
            return noPut.getNextRowCore();
        }

        @Override
        public TimeView getRemoteReadTime() {
            return Metrics.noOpTimeView();
        };

        @Override
        public long getRemoteBytesRead(){
            return 0;
        };

        @Override
        public long getRemoteRowsRead(){
            return 0;
        };

    }
    private static class ResultSetStandardIterator implements MeasuredStandardIterator<ExecRow>{
        private final SpliceNoPutResultSet noPut;

        private ResultSetStandardIterator(SpliceNoPutResultSet noPut){
            this.noPut = noPut;
        }

        @Override
        public void open() throws StandardException, IOException {
            noPut.open();
            noPut.openCore();
        }

        @Override
        public ExecRow next(SpliceRuntimeContext spliceRuntimeContext) throws
                                                                       StandardException,
                                                                       IOException {
            return noPut.getNextRowCore();
        }

        @Override
        public void close() throws StandardException, IOException {
            noPut.close();
        }

        @Override
        public TimeView getRemoteReadTime() {
            return noPut.getRemoteReadTime();
        }

        @Override
        public long getRemoteBytesRead() {
            return noPut.getRemoteBytesRead();
        }

        @Override
        public long getRemoteRowsRead() {
            return noPut.getRemoteRowsRead();
        }
    }

    public static class CallableStandardIterator<T> implements StandardIterator<T> {
        private final Callable<T> callable;
        private final StandardCloseable c;

        public CallableStandardIterator(Callable<T> callable) {
            this.callable = callable;
            this.c = null;
        }

        public CallableStandardIterator(Callable<T> callable, StandardCloseable c) {
            this.callable = callable;
            this.c = c;
        }

        @Override
        public void open() {
        }

        @Override
        public void close() throws StandardException, IOException {
            if (c != null) {
                c.close();
            }
        }

        @Override
        public T next(SpliceRuntimeContext spliceRuntimeContext) throws
                                                                 StandardException,
                                                                 IOException {
            try {
                return callable.call();
            } catch (StandardException se) {
                throw se;
            } catch (IOException ioe) {
                throw ioe;
            } catch (Exception e) {
                throw Exceptions.parseException(e);
            }
        }
    }
}
