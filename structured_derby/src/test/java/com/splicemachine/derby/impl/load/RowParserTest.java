package com.splicemachine.derby.impl.load;

import com.google.common.base.Function;
import com.google.common.collect.Lists;
import com.splicemachine.derby.utils.ErrorState;
import com.splicemachine.derby.utils.test.ImportDataType;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.sql.execute.ExecRow;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import javax.annotation.Nullable;
import java.util.*;

/**
 * @author Scott Fines
 * Created on: 9/26/13
 */
@SuppressWarnings("deprecation")
@RunWith(Parameterized.class)
public class RowParserTest {

    @Parameterized.Parameters
    public static Collection<Object[]> data() {
        Collection<Object[]> dataTypes = Lists.newArrayList();

        for(ImportDataType dataType:ImportDataType.values()){
            dataTypes.add(new Object[]{Arrays.asList(dataType)});
        }

        //do combinations of 2

        for(ImportDataType dataType:ImportDataType.values()){
            for(ImportDataType secondPosType:ImportDataType.values()){
                dataTypes.add(new Object[]{Arrays.asList(dataType,secondPosType)});
            }
        }
//
        for(ImportDataType dataType:ImportDataType.values()){
            for(ImportDataType secondPosType:ImportDataType.values()){
                for(ImportDataType thirdPosType:ImportDataType.values()){
                    dataTypes.add(new Object[]{Arrays.asList(dataType,secondPosType,thirdPosType)});
                }
            }
        }

        return dataTypes;
    }

    private final List<ImportDataType> dataTypes;

    public RowParserTest(List<ImportDataType> dataTypes) {
        this.dataTypes = dataTypes;
    }

    @Test
    public void testProperlyParsesType() throws Exception {
        Random random = new Random(0l);

        final ExecRow row = getExecRow();

        final RowParser parser = getRowParser(row);

        final ColumnContext[] columnCtxs = new ColumnContext[dataTypes.size()];
        for(int i=0;i<columnCtxs.length;i++){
            ImportDataType dataType = dataTypes.get(i);
            ColumnContext ctx = new ColumnContext.Builder()
                    .columnNumber(i)
                    .columnType(dataType.getJdbcType())
                    .nullable(true).build();
            columnCtxs[i] = ctx;
        }
        final List<String[]> rows = Lists.newArrayListWithCapacity(10);
        List<ExecRow> correctRows = Lists.newArrayListWithCapacity(10);
        for(int i=0;i<10;i++){
            int colPos=1;
            String[] line = new String[dataTypes.size()];
            for(ImportDataType importDataType:dataTypes){
                Object o = importDataType.newObject(random);
                importDataType.setNext(row.getColumn(colPos),o);
                line[colPos-1] = importDataType.toString(o);
                colPos++;
            }
            correctRows.add(row.getClone());
            rows.add(line);
        }

        List<ExecRow> parsedRows = Lists.newArrayList(Lists.transform(rows,new Function<String[], ExecRow>() {
            @Override
            public ExecRow apply(@Nullable String[] input) {
                try {
                    return parser.process(input,columnCtxs).getClone();
                } catch (StandardException e) {
                    throw new RuntimeException(e);
                }
            }
        }));

        assertExecRowsEqual(correctRows, parsedRows);
    }

    private RowParser getRowParser(ExecRow row) {
        String tsFormat = "yyyy-MM-dd HH:mm:ss.SSS";
        String dFormat = "yyyy-MM-dd";
        String tFormat = "HH:mm:ss";
        return new RowParser(row,dFormat,tFormat,tsFormat);
    }

    private void assertExecRowsEqual(List<ExecRow> correctRows, List<ExecRow> parsedRows) throws StandardException {
        Comparator<ExecRow> comparator = ImportTestUtils.columnComparator(1);
        Collections.sort(correctRows, comparator);
        Collections.sort(parsedRows,comparator);

        for(int i=0;i<parsedRows.size();i++){
            ExecRow correctRow = correctRows.get(i);
            ExecRow parsedRow = parsedRows.get(i);
            for(int dvdPos=1;dvdPos<=correctRow.nColumns();dvdPos++){
                Assert.assertEquals("Incorrect column at position " + dvdPos,
                        correctRow.getColumn(dvdPos),
                        parsedRow.getColumn(dvdPos));
            }
        }
    }

    @Test
    public void testParseEmptyInFirstPosition() throws Exception {
        /*
         * Tests that we can parse an empty string as null in the first element
         * of the string[] and still get a correct array
         */
        Random random = new Random(0l);

        final ExecRow row = getExecRow();

        final RowParser parser = getRowParser(row);

        final ColumnContext[] columnCtxs = new ColumnContext[dataTypes.size()];
        for(int i=0;i<columnCtxs.length;i++){
            ImportDataType dataType = dataTypes.get(i);
            ColumnContext ctx = new ColumnContext.Builder().columnType(dataType.getJdbcType()).nullable(true).build();
            columnCtxs[i] = ctx;
        }
        final List<String[]> rows = Lists.newArrayListWithCapacity(10);
        List<ExecRow> correctRows = Lists.newArrayListWithCapacity(10);
        for(int i=0;i<10;i++){
            int colPos=1;
            String[] line = new String[dataTypes.size()];
            for(ImportDataType importDataType:dataTypes){
                if(colPos!=1){
                    Object o = importDataType.newObject(random);
                    importDataType.setNext(row.getColumn(colPos),o);
                    line[colPos-1] = importDataType.toString(o);
                }else{
                    row.getColumn(colPos).setToNull();
                    line[colPos-1] = "";
                }
                colPos++;
            }
            correctRows.add(row.getClone());
            rows.add(line);
        }

        List<ExecRow> parsedRows = Lists.newArrayList(Lists.transform(rows,new Function<String[], ExecRow>() {
            @Override
            public ExecRow apply(@Nullable String[] input) {
                try {
                    return parser.process(input,columnCtxs).getClone();
                } catch (StandardException e) {
                    throw new RuntimeException(e);
                }
            }
        }));
        assertExecRowsEqual(correctRows, parsedRows);
    }

    @Test
    public void testParseWhitespaceInFirstPosition() throws Exception {
        /*
         * Tests that we can parse a whitespace string as null in the first element
         * of the string[] and still get a correct array (if the type is NOT VARCHAR or CHAR)
         */
        if(ImportDataType.VARCHAR.equals(dataTypes.get(0))||
                ImportDataType.CHAR.equals(dataTypes.get(0)))
            return;

        Random random = new Random(0l);

        final ExecRow row = getExecRow();

        final RowParser parser = getRowParser(row);

        final ColumnContext[] columnCtxs = new ColumnContext[dataTypes.size()];
        for(int i=0;i<columnCtxs.length;i++){
            ImportDataType dataType = dataTypes.get(i);
            ColumnContext ctx = new ColumnContext.Builder().columnType(dataType.getJdbcType()).nullable(true).build();
            columnCtxs[i] = ctx;
        }
        final List<String[]> rows = Lists.newArrayListWithCapacity(10);
        List<ExecRow> correctRows = Lists.newArrayListWithCapacity(10);
        for(int i=0;i<10;i++){
            int colPos=1;
            String[] line = new String[dataTypes.size()];
            for(ImportDataType importDataType:dataTypes){
                if(colPos!=1){
                    Object o = importDataType.newObject(random);
                    importDataType.setNext(row.getColumn(colPos),o);
                    line[colPos-1] = importDataType.toString(o);
                }else{
                    row.getColumn(colPos).setToNull();
                    line[colPos-1] = " ";
                }
                colPos++;
            }
            correctRows.add(row.getClone());
            rows.add(line);
        }

        List<ExecRow> parsedRows = Lists.newArrayList(Lists.transform(rows,new Function<String[], ExecRow>() {
            @Override
            public ExecRow apply(@Nullable String[] input) {
                try {
                    return parser.process(input,columnCtxs).getClone();
                } catch (StandardException e) {
                    throw new RuntimeException(e);
                }
            }
        }));
        assertExecRowsEqual(correctRows, parsedRows);
    }

    @Test(expected = StandardException.class)
    public void testParseNullIntoNonNullFails() throws Throwable {
        /*
         * Tests that we can parse a whitespace string as null in the first element
         * of the string[] and still get a correct array (if the type is NOT VARCHAR or CHAR)
         */

        Random random = new Random(0l);

        final ExecRow row = getExecRow();

        final RowParser parser = getRowParser(row);

        final ColumnContext[] columnCtxs = new ColumnContext[dataTypes.size()];
        for(int i=0;i<columnCtxs.length;i++){
            ImportDataType dataType = dataTypes.get(i);
            if(i==0){
            columnCtxs[i] = new ColumnContext.Builder()
                    .columnType(dataType.getJdbcType())
                    .nullable(false).build();
            }else{
                columnCtxs[i] = new ColumnContext.Builder()
                        .columnType(dataType.getJdbcType())
                        .nullable(true).build();
            }
        }
        final List<String[]> rows = Lists.newArrayListWithCapacity(10);
        List<ExecRow> correctRows = Lists.newArrayListWithCapacity(10);
        for(int i=0;i<10;i++){
            int colPos=1;
            String[] line = new String[dataTypes.size()];
            for(ImportDataType importDataType:dataTypes){
                if(colPos!=1){
                    Object o = importDataType.newObject(random);
                    importDataType.setNext(row.getColumn(colPos),o);
                    line[colPos-1] = importDataType.toString(o);
                }else{
                    row.getColumn(colPos).setToNull();
                    line[colPos-1] = "";
                }
                colPos++;
            }
            correctRows.add(row.getClone());
            rows.add(line);
        }

        try{
            Lists.newArrayList(Lists.transform(rows,new Function<String[], ExecRow>() {
                @Override
                public ExecRow apply(@Nullable String[] input) {
                    try {
                        return parser.process(input,columnCtxs).getClone();
                    } catch (StandardException e) {
                        Assert.assertEquals("Incorrect sql state!", ErrorState.LANG_NULL_INTO_NON_NULL.getSqlState(),e.getSqlState());
                        throw new RuntimeException(e);
                    }
                }
            }));
        }catch(RuntimeException re){
            Throwable t =  re.getCause();
            if(t!=null)
                throw t;
        }
        Assert.fail("Did not get any errors for type "+ dataTypes.get(0));
    }

    private ExecRow getExecRow() {
        final ExecRow row = new com.splicemachine.derby.impl.sql.execute.ValueRow(dataTypes.size());
        for(int i=0;i<dataTypes.size();i++){
            row.setColumn(i+1,dataTypes.get(i).getDataValueDescriptor());
        }
        return row;
    }
}
