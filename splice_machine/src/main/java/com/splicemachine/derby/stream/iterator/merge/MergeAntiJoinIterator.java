package com.splicemachine.derby.stream.iterator.merge;

import com.splicemachine.derby.impl.sql.execute.operations.LocatedRow;
import com.splicemachine.derby.impl.sql.execute.operations.MergeJoinOperation;
import org.apache.log4j.Logger;
import org.sparkproject.guava.common.collect.PeekingIterator;
import java.util.Iterator;

public class MergeAntiJoinIterator extends AbstractMergeJoinIterator {
    private static final Logger LOG = Logger.getLogger(MergeAntiJoinIterator.class);
    /**
     * MergeJoinRows constructor. Note that keys for left & right sides
     * are the join keys on which each side is sorted (not all of the
     * join keys).
     *
     * @param leftRS        Iterator for left side rows
     * @param rightRS       Iterator for right side rows
     * @param leftKeys      Join key(s) on which left side is sorted
     * @param rightKeys     Join Key(s) on which right side is sorted
     */
    public MergeAntiJoinIterator(Iterator<LocatedRow> leftRS,
                                 PeekingIterator<LocatedRow> rightRS,
                                 int[] leftKeys, int[] rightKeys,
                                 MergeJoinOperation mergeJoinOperation) {
        super(leftRS,rightRS,leftKeys,rightKeys,mergeJoinOperation);
    }

    @Override
    public boolean hasNext() {
        try {
            while (leftRS.hasNext()) {
                left = leftRS.next();
                currentRightIterator = rightsForLeft(left.getRow());
                boolean returnedRows = false;
                while (currentRightIterator.hasNext()) {
                    currentLocatedRow = mergeRows(left, currentRightIterator.next());
                    if (mergeJoinOperation.getRestriction().apply(currentLocatedRow.getRow())) {
                        returnedRows = true;
                        break;
                    }
                }
                if (!returnedRows) {
                    currentLocatedRow = mergeRows(left, null);
                    return true;
                }
            }
            return false;
        }  catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

}