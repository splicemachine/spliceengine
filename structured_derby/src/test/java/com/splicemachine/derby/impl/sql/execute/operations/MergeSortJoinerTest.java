package com.splicemachine.derby.impl.sql.execute.operations;

import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.collect.Collections2;
import com.google.common.collect.Lists;
import com.splicemachine.derby.impl.sql.execute.ValueRow;
import com.splicemachine.derby.utils.JoinSideExecRow;
import com.splicemachine.derby.utils.StandardIterator;
import com.splicemachine.derby.utils.StandardIterators;
import com.splicemachine.derby.utils.StandardSupplier;
import com.splicemachine.encoding.Encoding;
import com.splicemachine.stats.Metrics;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.sql.execute.ExecRow;
import org.apache.derby.iapi.types.DataValueDescriptor;
import org.apache.derby.iapi.types.SQLInteger;
import org.junit.Assert;
import org.junit.Test;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.*;

/**
 * @author Scott Fines
 * Created on: 10/29/13
 */
@SuppressWarnings("ConstantConditions")
public class MergeSortJoinerTest {
    private static final Predicate<Integer> noOpIntPredicate = new Predicate<Integer>() {
        @Override
        public boolean apply(@Nullable Integer input) {
            return false;
        }
    };

    private static Predicate<Integer> evenIntPredicate = new Predicate<Integer>() {
        @Override
        public boolean apply(@Nullable Integer input) {
            return input % 2 == 0;
        }
    };

    private static final Predicate<JoinSideExecRow> noOpExecRowPredicate = new Predicate<JoinSideExecRow>() {
        @Override
        public boolean apply(@Nullable JoinSideExecRow input) {
            return true;
        }
    };
    private static final Predicate<JoinSideExecRow> evenExecRowPredicate = new Predicate<JoinSideExecRow>() {
        @Override
        public boolean apply(@Nullable JoinSideExecRow input) {
            try {
                return input.getRow().getColumn(1).getInt() % 2 == 0;
            } catch (StandardException e) {
                throw new RuntimeException(e);
            }
        }
    };

    @Test
    public void testCanJoinRowsCorrectlyMultipleRightRows() throws Exception {
          /*
         * Test that it works in the easy case of Right,Right,Left,Left,Right,Right...
         */
        int numRightRows = 3;
        int numLeftRows= 5;
        List<ExecRow> rightRows = Lists.newArrayListWithCapacity(numRightRows);
        List<ExecRow> leftRows = Lists.newArrayListWithCapacity(numLeftRows);

        populateBaseData(numRightRows, numLeftRows, rightRows, leftRows,evenIntPredicate,noOpIntPredicate);

        testCorrectness(rightRows, leftRows, noOpExecRowPredicate, noOpExecRowPredicate,null,false);
    }

    @Test
    public void testCanJoinRowsCorrectly() throws Exception {
        /*
         * Test that it works in the easy case of Right,Right,Left,Left,Right,Right...
         */
        int numRightRows = 3;
        int numLeftRows= 5;
        List<ExecRow> rightRows = Lists.newArrayListWithCapacity(numRightRows);
        List<ExecRow> leftRows = Lists.newArrayListWithCapacity(numLeftRows);

        populateBaseData(numRightRows, numLeftRows, rightRows, leftRows, noOpIntPredicate, noOpIntPredicate);

        testCorrectness(rightRows, leftRows,noOpExecRowPredicate,noOpExecRowPredicate,null,false);
    }

    @Test
    public void testCanInnerJoinWithEmptyLeftRows() throws Exception {
         /*
         * Test that it works in the easy case of Right,Right,Left,Left,Right,Right...
         */
        int numRightRows = 3;
        int numLeftRows= 5;
        List<ExecRow> rightRows = Lists.newArrayListWithCapacity(numRightRows);
        List<ExecRow> leftRows = Lists.newArrayListWithCapacity(numLeftRows);

        populateBaseData(numRightRows, numLeftRows, rightRows, leftRows,noOpIntPredicate,noOpIntPredicate);

        testCorrectness(rightRows, leftRows, evenExecRowPredicate,noOpExecRowPredicate,null,false);
    }

    @Test
    public void testCanInnerJoinWithEmptyRightRows() throws Exception {
        int numRightRows = 3;
        int numLeftRows= 5;
        List<ExecRow> rightRows = Lists.newArrayListWithCapacity(numRightRows);
        List<ExecRow> leftRows = Lists.newArrayListWithCapacity(numLeftRows);

        populateBaseData(numRightRows, numLeftRows, rightRows, leftRows,noOpIntPredicate,noOpIntPredicate);
        ExecRow emptyRightRow = new ValueRow(2);
        emptyRightRow.setRowArray(new DataValueDescriptor[]{new SQLInteger(),new SQLInteger()});

        testCorrectness(rightRows, leftRows, noOpExecRowPredicate,evenExecRowPredicate,emptyRightRow,false);
    }

    @Test
    public void testCanOuterJoinWithEmptyRightRows() throws Exception {
        int numRightRows = 3;
        int numLeftRows= 5;
        List<ExecRow> rightRows = Lists.newArrayListWithCapacity(numRightRows);
        List<ExecRow> leftRows = Lists.newArrayListWithCapacity(numLeftRows);

        populateBaseData(numRightRows, numLeftRows, rightRows, leftRows,noOpIntPredicate,noOpIntPredicate);
        ExecRow emptyRightRow = new ValueRow(2);
        emptyRightRow.setRowArray(new DataValueDescriptor[]{new SQLInteger(),new SQLInteger()});

        testCorrectness(rightRows, leftRows, noOpExecRowPredicate,evenExecRowPredicate,emptyRightRow,true);
    }

    /*private helper methods*/
    private void assertReturnedRowsCorrect(List<ExecRow> correctResults, List<ExecRow> actualAnswers) {
        Assert.assertEquals("Incorrect join row size!", correctResults.size(), actualAnswers.size());
        for(int i=0;i<correctResults.size();i++){
            ExecRow correct = correctResults.get(i);
            ExecRow actual = actualAnswers.get(i);
            Assert.assertArrayEquals("Incorrect row result!", correct.getRowArray(), actual.getRowArray());
        }
    }

    private List<ExecRow> populateFromJoiner(List<ExecRow> correctResults, Joiner joiner) throws StandardException, IOException {
        List<ExecRow> joinedAnswers = Lists.newArrayListWithExpectedSize(correctResults.size());
        ExecRow row;
        do{
            row = joiner.nextRow();
            if(row==null) continue;

            joinedAnswers.add(row.getClone());
        }while(row!=null);
        return joinedAnswers;
    }

    private StandardIterator<JoinSideExecRow> getCollectionScanner(List<JoinSideExecRow> joinedRows) throws StandardException, IOException {
        final List<JoinSideExecRow> rowsToEmit = Lists.newArrayList(joinedRows);
        StandardIterator<JoinSideExecRow> scanner = StandardIterators.wrap(rowsToEmit);
        return scanner;
    }

    private class MergeSortTestData {
        private final List<ExecRow> rightRows;
        private final List<ExecRow> leftRows;
        private final ExecRow mergedRowTemplate;
        private List<JoinSideExecRow> joinedRows;
        private List<ExecRow> correctResults;
        private final Predicate<JoinSideExecRow> leftRowFilter;
        private final Predicate<JoinSideExecRow> rightRowFilter;
        private final ExecRow emptyRightRow;
        private final boolean outer;

        public MergeSortTestData(List<ExecRow> rightRows,
                                 List<ExecRow> leftRows,
                                 Predicate<JoinSideExecRow> leftRowFilter,
                                 Predicate<JoinSideExecRow> rightRowFilter,
                                 ExecRow emptyRightRow,
                                 boolean outer) throws StandardException {
            this.rightRows = rightRows;
            this.leftRows = leftRows;
            this.leftRowFilter = leftRowFilter;
            this.rightRowFilter = rightRowFilter;
            this.emptyRightRow = emptyRightRow;
            this.outer = outer;

            mergedRowTemplate = new ValueRow(4);
            mergedRowTemplate.setRowArray(new DataValueDescriptor[]{
                    new SQLInteger(),
                    new SQLInteger(),
                    new SQLInteger(),
                    new SQLInteger()
            });
            build();
        }

        public ExecRow getMergedRowTemplate() {
            return mergedRowTemplate;
        }

        public List<JoinSideExecRow> getJoinedRows() {
            return joinedRows;
        }

        public List<ExecRow> getCorrectResults() {
            return correctResults;
        }

        public void build() throws StandardException {
            List<JoinSideExecRow> rightJoinSideRows = Lists.newArrayList(Collections2.transform(rightRows, new Function<ExecRow, JoinSideExecRow>() {
                @Override
                public JoinSideExecRow apply(@Nullable ExecRow input) {
                    try {
                        @SuppressWarnings("ConstantConditions") byte[] hash = Encoding.encode(input.getColumn(1).getInt());
                        return new JoinSideExecRow(input, JoinUtils.JoinSide.RIGHT, hash);
                    } catch (StandardException e) {
                        throw new RuntimeException(e);
                    }
                }
            }));

            List<JoinSideExecRow> leftJoinSideRows = Lists.newArrayList(Collections2.transform(leftRows, new Function<ExecRow, JoinSideExecRow>() {
                @Override
                public JoinSideExecRow apply(@Nullable ExecRow input) {
                    try {
                        @SuppressWarnings("ConstantConditions") byte[] hash = Encoding.encode(input.getColumn(1).getInt());
                        return new JoinSideExecRow(input, JoinUtils.JoinSide.LEFT, hash);
                    } catch (StandardException e) {
                        throw new RuntimeException(e);
                    }
                }
            }));


            //do the sort part of the join
            //put 1 right, then all matching left, then one right, then all matching left, etc.
            joinedRows = Lists.newArrayListWithCapacity(rightJoinSideRows.size() + leftJoinSideRows.size());
            correctResults = Lists.newArrayListWithExpectedSize(rightJoinSideRows.size() * leftJoinSideRows.size());
            addToJoinRows(leftJoinSideRows, rightJoinSideRows,leftRowFilter,rightRowFilter);


            sortCorrectResults();
        }

        private void sortCorrectResults() {
            Collections.sort(correctResults, new Comparator<ExecRow>() {
                @Override
                public int compare(ExecRow o1, ExecRow o2) {
                    DataValueDescriptor[] o1Row = o1.getRowArray();
                    DataValueDescriptor[] o2Row = o2.getRowArray();
                    int i;
                    for (i = 0; i < o1Row.length; i++) {
                        if (o2Row.length <= i) {
                            return 1; //sort longer rows after shorter ones
                        }
                        try {
                            int compare = o1Row[i].compare(o2Row[i]);
                            if (compare != 0) return compare;
                        } catch (StandardException e) {
                            throw new RuntimeException(e);
                        }
                    }
                    if (i < o2Row.length) return -1; //sort longer rows after shorter ones
                    return 0;
                }
            });
        }

        private void addToJoinRows(List<JoinSideExecRow> leftJoinSideRows,
                                   List<JoinSideExecRow> rightRowsToFilter,
                                   Predicate<JoinSideExecRow> leftSidePredicate,
                                   Predicate<JoinSideExecRow> rightSidePredicate) throws StandardException {
            Iterator<JoinSideExecRow> rightSideIterator = rightRowsToFilter.iterator();
            while(rightSideIterator.hasNext()){
                List<JoinSideExecRow> rightRowsForAnswer = Lists.newArrayList();
                JoinSideExecRow rightSideJoinRow = rightSideIterator.next();
                rightSideIterator.remove();
                if(rightSidePredicate.apply(rightSideJoinRow))
                    joinedRows.add(rightSideJoinRow);
                rightRowsForAnswer.add(rightSideJoinRow);
                final byte[] hash = rightSideJoinRow.getHash();
                while(rightSideIterator.hasNext()){
                    JoinSideExecRow nextRightRow = rightSideIterator.next();
                    if(nextRightRow.sameHash(hash)){
                        if(rightSidePredicate.apply(nextRightRow))
                            joinedRows.add(nextRightRow);
                        rightRowsForAnswer.add(nextRightRow);
                        rightSideIterator.remove();
                    }
                }
                //reset the iterator for the next row
                rightSideIterator = rightRowsToFilter.iterator();

                //always add the left side to joinedRows
                Collection<JoinSideExecRow> filteredLeftSideRows = Collections2.filter(Collections2.filter(leftJoinSideRows, leftSidePredicate), new Predicate<JoinSideExecRow>() {
                    @Override
                    public boolean apply(@Nullable JoinSideExecRow input) {
                        return input.sameHash(hash);
                    }
                });
                joinedRows.addAll(filteredLeftSideRows);

                /*
                 * add the correct output rows.
                 * If the right side is present, then join it to the left side manually.
                 * If the right side is not present, but outer is true, then add the left side, with emptyExecRow as the right side
                 */
                for(JoinSideExecRow rightRow:rightRowsForAnswer){
                    boolean addLeft = false;
                    if(rightSidePredicate.apply(rightRow)){
                        //right side is included
                        mergedRowTemplate.setColumn(3,rightRow.getRow().getColumn(1).cloneValue(true));
                        mergedRowTemplate.setColumn(4,rightRow.getRow().getColumn(2).cloneValue(true));
                        addLeft=true;
                    }else if(outer){
                        //right side isn't included, but outer is true
                        mergedRowTemplate.setColumn(3, emptyRightRow.getColumn(1).cloneValue(true));
                        mergedRowTemplate.setColumn(4, emptyRightRow.getColumn(2).cloneValue(true));
                        addLeft=true;
                    }
                    if(!addLeft) continue;

                    correctResults.addAll(Collections2.transform(filteredLeftSideRows,new Function<JoinSideExecRow, ExecRow>() {
                        @Override
                        public ExecRow apply(@Nullable JoinSideExecRow input) {
                            ExecRow row = mergedRowTemplate.getClone();
                            try {
                                //noinspection ConstantConditions
                                row.setColumn(1,input.getRow().getColumn(1).cloneValue(true));
                                row.setColumn(2, input.getRow().getColumn(2).cloneValue(true));
                            } catch (StandardException e) {
                                throw new RuntimeException(e);
                            }
                            return row;
                        }
                    }));
                }
            }
        }
    }

    private void populateBaseData(int numRightRows,
                                  int numLeftRows,
                                  List<ExecRow> rightRows,
                                  List<ExecRow> leftRows,
                                  Predicate<Integer> rightRowDuplicatePredicate,
                                  Predicate<Integer> leftRowDuplicatePredicate) throws StandardException {
        ExecRow rightRow = new ValueRow(2);
        rightRow.setColumn(1,new SQLInteger());
        rightRow.setColumn(2,new SQLInteger());
        ExecRow leftRow = new ValueRow(2);
        leftRow.setColumn(1,new SQLInteger());
        leftRow.setColumn(2,new SQLInteger());

        for(int rightCount=0;rightCount<numRightRows;rightCount++){
            rightRow.getColumn(1).setValue(rightCount);
            rightRow.getColumn(2).setValue(2*rightCount);
            rightRows.add(rightRow.getClone());
            if(rightRowDuplicatePredicate.apply(rightCount)){
                rightRow.getColumn(2).setValue(3*rightCount);
                rightRows.add(rightRow.getClone());
            }
            for(int leftCount=0;leftCount<numLeftRows;leftCount++){
                leftRow.getColumn(1).setValue(rightCount);
                leftRow.getColumn(2).setValue(leftCount);
                leftRows.add(leftRow.getClone());
                if(leftRowDuplicatePredicate.apply(leftCount)){
                    leftRow.getColumn(2).setValue(2*leftCount);
                    leftRows.add(leftRow.getClone());
                }
            }
        }
    }

    private void testCorrectness(List<ExecRow> rightRows,
                                 List<ExecRow> leftRows,
                                 Predicate<JoinSideExecRow> leftPredicate,
                                 Predicate<JoinSideExecRow> rightPredicate,
                                 final ExecRow emptyRightRow,
                                 boolean outer) throws StandardException, IOException {
        MergeSortTestData mergeSortTestData = new MergeSortTestData(rightRows,
                leftRows,leftPredicate,rightPredicate,emptyRightRow,outer);
        List<JoinSideExecRow> joinedRows = mergeSortTestData.getJoinedRows();
        ExecRow mergedRowTemplate = mergeSortTestData.getMergedRowTemplate();
        List<ExecRow> correctResults = mergeSortTestData.getCorrectResults();


        //now for the actual test

        StandardIterator<JoinSideExecRow> scanner = getCollectionScanner(joinedRows);
        StandardIterators.StandardIteratorIterator<JoinSideExecRow> bridgeIterator = StandardIterators.asIter(scanner);
        IJoinRowsIterator<ExecRow> joinRows = new MergeSortJoinRows(bridgeIterator);

        Joiner joiner;
        if(outer){
            joiner = new Joiner(joinRows, mergedRowTemplate,false,2,2,false,false,new StandardSupplier<ExecRow>() {
                @Override
                public ExecRow get() throws StandardException {
                    return emptyRightRow;
                }
            }){
                @Override
                protected boolean shouldMergeEmptyRow(boolean noRecordsFound) {
                    return noRecordsFound;
                }
            };
        }else
            joiner = new Joiner(joinRows, mergedRowTemplate,false,2,2,false, false,null);

        List<ExecRow> joinedAnswers = populateFromJoiner(correctResults, joiner);
        assertReturnedRowsCorrect(correctResults, joinedAnswers);
    }

}
