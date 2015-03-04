package com.splicemachine.derby.impl.ast;

import com.google.common.base.Function;
import com.google.common.collect.Iterables;
import com.splicemachine.db.iapi.sql.compile.JoinStrategy;
import com.splicemachine.db.impl.sql.compile.*;

import javax.annotation.Nullable;
import java.util.List;

import static java.lang.String.format;

/**
 * @author P Trolard
 *         Date: 17/09/2013
 */
public class JoinInfo {
    public final JoinStrategy strategy;
    public final boolean userSuppliedStrategy;
    public final boolean isSystemTable;
    public final boolean isEquiJoin;
    public final boolean hasRightIndex;
    public final List<Predicate> joinPredicates;
    public final List<Predicate> otherPredicates;
    public final List<ResultSetNode> rightNodes;
    public final List<ResultSetNode> rightLeaves;
    public final int rightSingleRegionSize;

    public JoinInfo(JoinStrategy strategy,
                    boolean userSuppliedStrategy, boolean isSystemTable,
                    boolean isEquiJoin, boolean hasRightIndex,
                    List<Predicate> joinPredicates, List<Predicate> otherPredicates,
                    List<ResultSetNode> rightNodes, List<ResultSetNode> rightLeaves,
                    int rightSingleRegionSize){
        this.strategy = strategy;
        this.userSuppliedStrategy = userSuppliedStrategy;
        this.isSystemTable = isSystemTable;
        this.isEquiJoin = isEquiJoin;
        this.hasRightIndex = hasRightIndex;
        this.joinPredicates = joinPredicates;
        this.otherPredicates = otherPredicates;
        this.rightNodes = rightNodes;
        this.rightLeaves = rightLeaves;
        this.rightSingleRegionSize = rightSingleRegionSize;
    }


    public String toString(){
        return format("{" +
                          "strategy=%s, " +
                          "userSuppliedStrategy=%s, " +
                          "isSystemTable=%s, " +
                          "isEquijoin=%s, " +
                          "hasRightIndex=%s, " +
                          "joinPreds=%s, " +
                          "otherPreds=%s, " +
                          "rightNodes=%s, " +
                          "rightLeaves=%s, " +
                          "rightSingleRegionSize=%s",
                         strategy, userSuppliedStrategy, isSystemTable,
                         isEquiJoin, hasRightIndex,
                         Iterables.transform(joinPredicates, PredicateUtils.predToString),
                         Iterables.transform(otherPredicates, PredicateUtils.predToString),
                         Iterables.transform(rightNodes, className),
                         Iterables.transform(rightLeaves, className),
                         rightSingleRegionSize);
    }

    public static Function<Object, String> className = new Function<Object, String>() {
        @Override
        public String apply(@Nullable Object o) {
            return o == null ? "" : o.getClass().getSimpleName();
        }
    };

}
