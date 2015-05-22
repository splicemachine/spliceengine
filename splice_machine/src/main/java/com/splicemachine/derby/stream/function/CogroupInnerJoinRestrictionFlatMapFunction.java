package com.splicemachine.derby.stream.function;

import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.impl.sql.execute.operations.JoinOperation;
import com.splicemachine.derby.impl.sql.execute.operations.LocatedRow;
import com.splicemachine.derby.stream.iapi.OperationContext;
import scala.Tuple2;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Set;

/**
 * Created by jleach on 4/30/15.
 */
public class CogroupInnerJoinRestrictionFlatMapFunction<Op extends SpliceOperation> extends SpliceJoinFlatMapFunction<Op, Tuple2<ExecRow,Tuple2<Iterable<LocatedRow>,Iterable<LocatedRow>>>,LocatedRow> {
    protected InnerJoinRestrictionFlatMapFunction innerJoinRestrictionFlatMapFunction;
    protected LocatedRow leftRow;
    public CogroupInnerJoinRestrictionFlatMapFunction() {
        super();
    }

    public CogroupInnerJoinRestrictionFlatMapFunction(OperationContext<Op> operationContext) {
        super(operationContext);
    }

    @Override
    public Iterable<LocatedRow> call(Tuple2<ExecRow,Tuple2<Iterable<LocatedRow>, Iterable<LocatedRow>>> tuple) throws Exception {
        checkInit();
        Set<LocatedRow> rightSide = Sets.newHashSet(tuple._2._2); // Memory Issue, HashSet ?
        Iterable<LocatedRow> returnRows = new ArrayList();
        Iterator<LocatedRow> it = tuple._2._1.iterator();
        while (it.hasNext()) {
            returnRows = Iterables.concat(returnRows, innerJoinRestrictionFlatMapFunction.call(new Tuple2(it.next(),rightSide)));
        }
        return returnRows;
    }
    @Override
    protected void checkInit() {
        if (!initialized)
            innerJoinRestrictionFlatMapFunction = new InnerJoinRestrictionFlatMapFunction(operationContext);
        super.checkInit();
    }

}