package com.splicemachine.derby.impl.load;

import com.google.common.base.Function;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.splicemachine.derby.utils.ErrorState;
import com.splicemachine.derby.utils.test.TestingDataType;
import com.splicemachine.hbase.KVPair;
import com.splicemachine.hbase.writer.WriteResult;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.sql.execute.ExecRow;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import javax.annotation.Nullable;
import java.io.IOException;
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

//				dataTypes.add(new Object[]{Arrays.asList(TestingDataType.INTEGER)});
        for(TestingDataType dataType: TestingDataType.values()){
            dataTypes.add(new Object[]{Arrays.asList(dataType)});
        }

        //do combinations of 2

//        dataTypes.add(new Object[]{Arrays.asList(TestingDataType.BOOLEAN,TestingDataType.BOOLEAN)});
        for(TestingDataType dataType: TestingDataType.values()){
            for(TestingDataType secondPosType: TestingDataType.values()){
                dataTypes.add(new Object[]{Arrays.asList(dataType,secondPosType)});
            }
        }
        for(TestingDataType dataType: TestingDataType.values()){
            for(TestingDataType secondPosType: TestingDataType.values()){
                for(TestingDataType thirdPosType: TestingDataType.values()){
                    dataTypes.add(new Object[]{Arrays.asList(dataType,secondPosType,thirdPosType)});
                }
            }
        }

        return dataTypes;
    }

    private final List<TestingDataType> dataTypes;

    public RowParserTest(List<TestingDataType> dataTypes) {
        this.dataTypes = dataTypes;
    }

    @Test
    public void testProperlyParsesType() throws Exception {
        System.out.println(dataTypes);
        Random random = new Random(0l);

        final ExecRow row = getExecRow();

        final RowParser parser = getRowParser(row);

        final ColumnContext[] columnCtxs = new ColumnContext[dataTypes.size()];
        for(int i=0;i<columnCtxs.length;i++){
            TestingDataType dataType = dataTypes.get(i);
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
            for(TestingDataType testingDataType :dataTypes){
                Object o = testingDataType.newObject(random);
                testingDataType.setNext(row.getColumn(colPos),o);
                line[colPos-1] = testingDataType.toString(o);
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
			return getRowParser(row,FailAlwaysReporter.INSTANCE);
		}

    private RowParser getRowParser(ExecRow row,ImportErrorReporter errorReporter) {
        String tsFormat = "yyyy-MM-dd HH:mm:ss.SSS";
        String dFormat = "yyyy-MM-dd";
        String tFormat = "HH:mm:ss";
        return new RowParser(row,dFormat,tFormat,tsFormat,errorReporter);
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
        System.out.println(dataTypes);
        Random random = new Random(0l);

        final ExecRow row = getExecRow();

        final RowParser parser = getRowParser(row);

        final ColumnContext[] columnCtxs = new ColumnContext[dataTypes.size()];
        for(int i=0;i<columnCtxs.length;i++){
            TestingDataType dataType = dataTypes.get(i);
            ColumnContext ctx = new ColumnContext.Builder()
                    .columnType(dataType.getJdbcType())
                    .nullable(true).columnNumber(i).build();
            columnCtxs[i] = ctx;
        }
        final List<String[]> rows = Lists.newArrayListWithCapacity(10);
        List<ExecRow> correctRows = Lists.newArrayListWithCapacity(10);
        for(int i=0;i<10;i++){
            int colPos=1;
            String[] line = new String[dataTypes.size()];
            for(TestingDataType testingDataType :dataTypes){
                if(colPos!=1){
                    Object o = testingDataType.newObject(random);
                    testingDataType.setNext(row.getColumn(colPos),o);
                    line[colPos-1] = testingDataType.toString(o);
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
        if(TestingDataType.VARCHAR.equals(dataTypes.get(0))
                || TestingDataType.CHAR.equals(dataTypes.get(0))
                || TestingDataType.LAZYVARCHAR.equals(dataTypes.get(0)) )
            return;

        Random random = new Random(0l);

        final ExecRow row = getExecRow();

        final RowParser parser = getRowParser(row);

        final ColumnContext[] columnCtxs = new ColumnContext[dataTypes.size()];
        for(int i=0;i<columnCtxs.length;i++){
            TestingDataType dataType = dataTypes.get(i);
            ColumnContext ctx = new ColumnContext.Builder()
                    .columnType(dataType.getJdbcType())
                    .nullable(true).columnNumber(i).build();
            columnCtxs[i] = ctx;
        }
        final List<String[]> rows = Lists.newArrayListWithCapacity(10);
        List<ExecRow> correctRows = Lists.newArrayListWithCapacity(10);
        for(int i=0;i<10;i++){
            int colPos=1;
            String[] line = new String[dataTypes.size()];
            for(TestingDataType testingDataType :dataTypes){
                if(colPos!=1){
                    Object o = testingDataType.newObject(random);
                    testingDataType.setNext(row.getColumn(colPos),o);
                    line[colPos-1] = testingDataType.toString(o);
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

		@Test
		public void testParseNullIntoNonNullRecordsBadRow() throws Throwable {
        /*
         * Tests that we can parse a whitespace string as null in the first element
         * of the string[] and still get a correct array (if the type is NOT VARCHAR or CHAR)
         */
				Random random = new Random(0l);

				final ExecRow row = getExecRow();

				final Set<String> badRows = Sets.newHashSet();
				final RowParser parser = getRowParser(row, new ImportErrorReporter() {
						@Override
						public boolean reportError(KVPair kvPair, WriteResult result) {
								Assert.fail("How did a KVPair get created?!");
								return false;
						}

						@Override
						public boolean reportError(String row, WriteResult result) {
								Assert.assertFalse("Bad Row reported multiple times!",badRows.contains(row));
								badRows.add(row);
								return true;
						}

						@Override public void close() throws IOException {  }

						@Override public long errorsReported() { return 0; }
				});

				final ColumnContext[] columnCtxs = new ColumnContext[dataTypes.size()];
				for(int i=0;i<columnCtxs.length;i++){
						TestingDataType dataType = dataTypes.get(i);
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
				for(int i=0;i<1;i++){
						int colPos=1;
						String[] line = new String[dataTypes.size()];
						for(TestingDataType testingDataType :dataTypes){
								if(colPos!=1){
										Object o = testingDataType.newObject(random);
										testingDataType.setNext(row.getColumn(colPos),o);
										line[colPos-1] = testingDataType.toString(o);
								}else{
										row.getColumn(colPos).setToNull();
										line[colPos-1] = "";
								}
								colPos++;
						}
						correctRows.add(row.getClone());
						rows.add(line);
				}

				Lists.newArrayList(Lists.transform(rows, new Function<String[], ExecRow>() {
						@Override
						public ExecRow apply(@Nullable String[] input) {
								try {
										ExecRow process = parser.process(input, columnCtxs);
										return process !=null? process.getClone(): null;
								} catch (StandardException e) {
										throw new RuntimeException(e);
								}
						}
				}));
				Assert.assertEquals("Incorrect number of failed rows!",1,badRows.size());
		}

		@Test(expected = StandardException.class)
    public void testParseNullIntoNonNullFailsWithTooManyErrors() throws Throwable {
        /*
         * Tests that we can parse a whitespace string as null in the first element
         * of the string[] and still get a correct array (if the type is NOT VARCHAR or CHAR)
         */

        Random random = new Random(0l);

        final ExecRow row = getExecRow();

        final RowParser parser = getRowParser(row,new ImportErrorReporter() {
						@Override public boolean reportError(KVPair kvPair, WriteResult result) { return false; }
						@Override public boolean reportError(String row, WriteResult result) { return false; }
						@Override public void close() throws IOException {  }

						@Override public long errorsReported() { return 0; }
				});

        final ColumnContext[] columnCtxs = new ColumnContext[dataTypes.size()];
        for(int i=0;i<columnCtxs.length;i++){
            TestingDataType dataType = dataTypes.get(i);
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
            for(TestingDataType testingDataType :dataTypes){
                if(colPos!=1){
                    Object o = testingDataType.newObject(random);
                    testingDataType.setNext(row.getColumn(colPos),o);
                    line[colPos-1] = testingDataType.toString(o);
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
                        Assert.assertEquals("Incorrect sql state!", ErrorState.LANG_IMPORT_TOO_MANY_BAD_RECORDS.getSqlState(),e.getSqlState());
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
            TestingDataType dataType = dataTypes.get(i);
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
            for(TestingDataType testingDataType :dataTypes){
                if(colPos!=1){
                    Object o = testingDataType.newObject(random);
                    testingDataType.setNext(row.getColumn(colPos),o);
                    line[colPos-1] = testingDataType.toString(o);
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
