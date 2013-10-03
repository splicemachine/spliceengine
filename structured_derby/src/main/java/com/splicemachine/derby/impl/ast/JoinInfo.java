package com.splicemachine.derby.impl.ast;

import com.google.common.base.Function;
import com.google.common.collect.Iterables;
import org.apache.derby.iapi.sql.compile.JoinStrategy;
import org.apache.derby.impl.sql.compile.Predicate;
import org.apache.derby.impl.sql.compile.ResultSetNode;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;

/**
 * @author P Trolard
 *         Date: 17/09/2013
 */
public class JoinInfo {
    public final JoinStrategy strategy;
    public final boolean userSuppliedStrategy;
    public final boolean isSystemTable;
    public final boolean isEquiJoin;
    public final boolean rightEquiJoinColIsPK;
    public final boolean hasRightIndex;
    public final List<Predicate> joinPredicates;
    public final List<Predicate> otherPredicates;
    public final List<ResultSetNode> rightNodes;
    public final List<ResultSetNode> rightLeaves;

    public JoinInfo(JoinStrategy strategy,
                    boolean userSuppliedStrategy, boolean isSystemTable,
                    boolean isEquiJoin, boolean rightEquiJoinColIsPK, boolean hasRightIndex,
                    List<Predicate> joinPredicates, List<Predicate> otherPredicates,
                    List<ResultSetNode> rightNodes, List<ResultSetNode> rightLeaves){
        this.strategy = strategy;
        this.userSuppliedStrategy = userSuppliedStrategy;
        this.isSystemTable = isSystemTable;
        this.isEquiJoin = isEquiJoin;
        this.rightEquiJoinColIsPK = rightEquiJoinColIsPK;
        this.hasRightIndex = hasRightIndex;
        this.joinPredicates = joinPredicates;
        this.otherPredicates = otherPredicates;
        this.rightNodes = rightNodes;
        this.rightLeaves = rightLeaves;
    }


    public String toString(){
        return String.format("{" +
                "strategy=%s, " +
                "userSuppliedStrategy=%s, " +
                "isSystemTable=%s, " +
                "isEquijoin=%s, " +
                "rightEquiJoinColIsPk=%s, " +
                "hasRightIndex=%s, " +
                "joinPreds=%s, " +
                "otherPreds=%s, " +
                "rightNodes=%s, " +
                "rightLeaves=%s",
                strategy, userSuppliedStrategy, isSystemTable,
                isEquiJoin, rightEquiJoinColIsPK, hasRightIndex,
                Iterables.transform(joinPredicates, predToString),
                Iterables.transform(otherPredicates, predToString),
                Iterables.transform(rightNodes, className),
                Iterables.transform(rightLeaves, className)
        );

    }

    public static Function<Predicate,String> predToString = new Function<Predicate,String>(){
        @Override
        public String apply(@Nullable Predicate predicate) {
            return predicate == null ? null : predicate.binaryRelOpColRefsToString();
        }
    };

    public static Function<Object, String> className = new Function<Object, String>() {
        @Override
        public String apply(@Nullable Object o) {
            return o == null ? "" : o.getClass().getSimpleName();
        }
    };

}
