package com.splicemachine.derby.stream.function.broadcast;

import com.google.common.base.Function;
import com.google.common.collect.FluentIterable;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.impl.sql.JoinTable;
import com.splicemachine.derby.impl.sql.execute.operations.LocatedRow;
import com.splicemachine.derby.stream.function.SpliceFlatMapFunction;
import com.splicemachine.derby.stream.iapi.OperationContext;
import scala.Tuple2;

import javax.annotation.Nullable;
import java.util.Iterator;

/**
 * Created by dgomezferro on 11/6/15.
 */
public class CogroupBroadcastJoinFunction extends AbstractBroadcastJoinFlatMapFunction<LocatedRow, Tuple2<LocatedRow,Iterable<LocatedRow>>> {
    public CogroupBroadcastJoinFunction(OperationContext operationContext) {
    }

    @Override
    protected Iterable<Tuple2<LocatedRow, Iterable<LocatedRow>>> call(final Iterator<LocatedRow> locatedRows, final JoinTable joinTable) {
        Iterable<Tuple2<LocatedRow, Iterable<LocatedRow>>> result = FluentIterable.from(new Iterable<LocatedRow>() {
            @Override
            public Iterator<LocatedRow> iterator() {
                return locatedRows;
            }
        }).transform(
                new Function<LocatedRow, Tuple2<LocatedRow, Iterable<LocatedRow>>>() {
                    @Nullable
                    @Override
                    public Tuple2<LocatedRow, Iterable<LocatedRow>> apply(@Nullable final LocatedRow left) {
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
                        return new Tuple2(left, inner);
                    }
                });
        return result;
    }
}
