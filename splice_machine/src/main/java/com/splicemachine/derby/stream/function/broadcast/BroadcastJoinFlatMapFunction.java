package com.splicemachine.derby.stream.function.broadcast;

import com.google.common.base.Function;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.Iterators;
import com.google.common.collect.PeekingIterator;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.impl.sql.execute.BaseActivation;
import com.splicemachine.derby.hbase.SpliceDriver;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.impl.sql.JoinTable;
import com.splicemachine.derby.impl.sql.execute.operations.BroadcastJoinCache;
import com.splicemachine.derby.impl.sql.execute.operations.BroadcastJoinOperation;
import com.splicemachine.derby.impl.sql.execute.operations.LocatedRow;
import com.splicemachine.derby.stream.function.SpliceFlatMapFunction;
import com.splicemachine.derby.stream.iapi.DataSetProcessor;
import com.splicemachine.derby.stream.iapi.OperationContext;
import com.splicemachine.derby.stream.iterator.merge.AbstractMergeJoinIterator;
import com.splicemachine.derby.stream.utils.StreamUtils;
import com.splicemachine.stream.Stream;
import com.splicemachine.stream.Streams;
import scala.Tuple2;

import javax.annotation.Nullable;
import java.io.Closeable;
import java.io.IOException;
import java.util.Iterator;
import java.util.concurrent.Callable;

/**
 * Created by dgomezferro on 11/4/15.
 */
public class BroadcastJoinFlatMapFunction extends AbstractBroadcastJoinFlatMapFunction<LocatedRow, Tuple2<ExecRow, Tuple2<LocatedRow, LocatedRow>>> {

    public BroadcastJoinFlatMapFunction() {
    }

    public BroadcastJoinFlatMapFunction(OperationContext operationContext) {
        super(operationContext);
    }

    @Override
    public Iterable<Tuple2<ExecRow, Tuple2<LocatedRow, LocatedRow>>> call(final Iterator<LocatedRow> locatedRows, final JoinTable joinTable) {
        Iterable<Tuple2<ExecRow, Tuple2<LocatedRow, LocatedRow>>> result = FluentIterable.from(new Iterable<LocatedRow>() {
            @Override
            public Iterator<LocatedRow> iterator() {
                return locatedRows;
            }
        }).transformAndConcat(
                new Function<LocatedRow, Iterable<Tuple2<ExecRow, Tuple2<LocatedRow, LocatedRow>>>>() {
                    @Nullable
                    @Override
                    public Iterable<Tuple2<ExecRow, Tuple2<LocatedRow, LocatedRow>>> apply(@Nullable final LocatedRow left) {
                        Iterable<ExecRow> inner = new Iterable<ExecRow>() {
                            @Override
                            public Iterator<ExecRow> iterator() {
                                try {
                                    return joinTable.fetchInner(left.getRow());
                                } catch (Exception e) {
                                    throw new RuntimeException(e);
                                }
                            }
                        };
                        return FluentIterable.from(inner).transform(
                                new Function<ExecRow, Tuple2<ExecRow, Tuple2<LocatedRow, LocatedRow>>>() {
                                    @Nullable
                                    @Override
                                    public Tuple2<ExecRow, Tuple2<LocatedRow, LocatedRow>> apply(@Nullable ExecRow right) {
                                        return new Tuple2(left.getRow(), new Tuple2(left, new LocatedRow(right)));
                                    }
                                });
                    }
                });
        return result;
    }
}
