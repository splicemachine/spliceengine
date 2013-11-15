package com.splicemachine.derby.impl.sql.execute.operations;

import com.google.common.base.Function;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.splicemachine.derby.impl.sql.execute.ValueRow;
import com.splicemachine.derby.utils.JoinSideExecRow;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.sql.execute.ExecRow;
import org.apache.derby.iapi.types.SQLVarchar;
import org.apache.hadoop.hbase.util.Pair;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import java.util.*;

/**
 * @author P Trolard
 *         Date: 14/11/2013
 */
public class MergeSortJoinRowsTest {

    static final List<String> testdata = Arrays.asList("R1a",
                                          "R2b", "R3b", "R4b", "R5b", "L1b", "L2b",
                                          "R6c", "L3c", "L4c", "L5c",
                                          "L6d", "L7d",
                                          "R7e",
                                          "R8f", "L8f");

    static final List<JoinSideExecRow> source = new ArrayList<JoinSideExecRow>(testdata.size());

    static {
        for (String rowdata: testdata){
            ValueRow row = new ValueRow(1);
            row.setColumn(1, new SQLVarchar(rowdata));
            JoinSideExecRow sideRow = new JoinSideExecRow(
                    row,
                    rowdata.charAt(0) == 'R' ? JoinUtils.JoinSide.RIGHT : JoinUtils.JoinSide.LEFT,
                    rowdata.substring(2).getBytes());
            source.add(sideRow);
        }
    }

    static final Function<ExecRow,String> execRowToString = new Function<ExecRow, String>() {
        @Override
        public String apply(ExecRow row) {
            try {
                return row.getColumn(1).toString();
            } catch (StandardException e) {
                throw new RuntimeException(e);
            }
        }
    };

    static final List<Pair<String,List<String>>> expectedStrings = Arrays.asList(
        new Pair<String,List<String>>("L1b", Arrays.asList("R2b", "R3b", "R4b", "R5b")),
        new Pair<String,List<String>>("L2b", Arrays.asList("R2b", "R3b", "R4b", "R5b")),
        new Pair<String,List<String>>("L3c", Arrays.asList("R6c")),
        new Pair<String,List<String>>("L4c", Arrays.asList("R6c")),
        new Pair<String,List<String>>("L5c", Arrays.asList("R6c")),
        new Pair<String,List<String>>("L6d", Collections.EMPTY_LIST),
        new Pair<String,List<String>>("L7d", Collections.EMPTY_LIST),
        new Pair<String,List<String>>("L8f", Arrays.asList("R8f")));


    @Test
    public void testDataTest() throws Exception {
        for (JoinSideExecRow r: source){
            System.out.println(r.getRow().getColumn(1).getString());
        }
    }

    @Test
    @Ignore
    public void testMergeSortJoinRowsConsuming() throws Exception {

        MergeSortJoinRows pairs = new MergeSortJoinRows(source.iterator());
        List<Pair<String, List<String>>> stringRows = Lists.transform(Lists.newArrayList((Iterator) pairs),
                new Function<Pair<ExecRow, Iterator<ExecRow>>, Pair<String, List<String>>>() {
                    @Override
                    public Pair<String, List<String>> apply(Pair<ExecRow, Iterator<ExecRow>> input) {
                        return new Pair<String, List<String>>(execRowToString.apply(input.getFirst()),
                                Lists.transform(Lists.newArrayList(input.getSecond()), execRowToString));
                    }
                });

        Assert.assertTrue("Realizing whole collection from iterator breaks because iterator " +
                "returns a mutable array for the right rows",
                !Arrays.equals(expectedStrings.toArray(),  stringRows.toArray()));
    }

    @Test
    public void testMergeSortJoinRowsIterating() throws Exception {

        MergeSortJoinRows pairs = new MergeSortJoinRows(source.iterator());
        int pos = 0;
        for (Pair<ExecRow,Iterator<ExecRow>> p: pairs){
            Assert.assertEquals(expectedStrings.get(pos).getFirst(), execRowToString.apply(p.getFirst()));
            Assert.assertEquals(expectedStrings.get(pos).getSecond(),
                    Lists.transform(Lists.newArrayList(p.getSecond()), execRowToString));
            pos++;
        }
    }
}
