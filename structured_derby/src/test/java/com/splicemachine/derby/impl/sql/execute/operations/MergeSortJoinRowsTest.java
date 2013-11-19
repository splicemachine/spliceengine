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

    static final Function<String,JoinSideExecRow> toRow = new Function<String, JoinSideExecRow>() {
        @Override
        public JoinSideExecRow apply(String rowdata) {
            ValueRow row = new ValueRow(1);
            row.setColumn(1, new SQLVarchar(rowdata));
            return new JoinSideExecRow(row,
                        rowdata.charAt(0) == 'R' ? JoinUtils.JoinSide.RIGHT : JoinUtils.JoinSide.LEFT,
                        rowdata.substring(2).getBytes());
        }
    };

    static final List<JoinSideExecRow> source = Lists.transform(testdata, toRow);

    static final Function<ExecRow,String> execRowToString = new Function<ExecRow, String>() {
        @Override
        public String apply(ExecRow row) {
            try {
                return row.getColumn(1).toString();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    };

    static final List<Pair<String,List<String>>> expectedStrings = Arrays.asList(
        Pair.newPair("L1b", Arrays.asList("R2b", "R3b", "R4b", "R5b")),
        Pair.newPair("L2b", Arrays.asList("R2b", "R3b", "R4b", "R5b")),
        Pair.newPair("L3c", Arrays.asList("R6c")),
        Pair.newPair("L4c", Arrays.asList("R6c")),
        Pair.newPair("L5c", Arrays.asList("R6c")),
        Pair.newPair("L6d", (List<String>)Collections.EMPTY_LIST),
        Pair.newPair("L7d", (List<String>)Collections.EMPTY_LIST),
        Pair.newPair("L8f", Arrays.asList("R8f")));


    public void testDataTest() throws Exception {
        for (JoinSideExecRow r: source){
            System.out.println(r.getRow().getColumn(1).getString());
        }
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
