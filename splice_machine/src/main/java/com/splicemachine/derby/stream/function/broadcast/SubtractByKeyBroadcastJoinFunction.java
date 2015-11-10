package com.splicemachine.derby.stream.function.broadcast;

import com.google.common.base.Function;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.derby.impl.sql.JoinTable;
import com.splicemachine.derby.impl.sql.execute.operations.LocatedRow;
import com.splicemachine.derby.stream.iapi.OperationContext;
import org.sparkproject.guava.base.Predicate;
import org.sparkproject.guava.collect.FluentIterable;
import scala.Tuple2;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.Iterator;

/**
 * Created by dgomezferro on 11/6/15.
 */
public class SubtractByKeyBroadcastJoinFunction extends AbstractBroadcastJoinFlatMapFunction<LocatedRow, LocatedRow> {
    public SubtractByKeyBroadcastJoinFunction() {
    }

    public SubtractByKeyBroadcastJoinFunction(OperationContext operationContext) {
        super(operationContext);
    }

    @Override
    protected Iterable<LocatedRow> call(final Iterator<LocatedRow> locatedRows, final JoinTable joinTable) {
        Iterable<LocatedRow> result = FluentIterable.from(new Iterable<LocatedRow>() {
            @Override
            public Iterator<LocatedRow> iterator() {
                return locatedRows;
            }
        }).filter(new Predicate<LocatedRow>() {
            @Override
            public boolean apply(@Nullable LocatedRow locatedRow) {
                try {
                    boolean rowsOnRight = joinTable.fetchInner(locatedRow.getRow()).hasNext();
                    return !rowsOnRight;
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
        });
        return result;
    }
}
