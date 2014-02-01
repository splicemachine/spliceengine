package com.splicemachine.derby.impl.load;

import com.google.common.collect.Lists;
import com.splicemachine.derby.impl.sql.execute.ValueRow;
import org.apache.derby.iapi.sql.execute.ExecRow;
import org.apache.derby.iapi.types.DataValueDescriptor;
import org.apache.derby.iapi.types.SQLTimestamp;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.sql.Timestamp;
import java.sql.Types;
import java.util.Collection;

/**
 * Tests around RowParser's Date Time format.
 *
 * @author Scott Fines
 * Created on: 9/30/13
 */
@RunWith(Parameterized.class)
public class RowParserDateTimeTest {

		@Parameterized.Parameters
		public static Collection<Object[]> data() {
				Collection<Object[]> data = Lists.newArrayList();

				data.add(new Object[]{"yyyy/MM/dd HH:mm:ss", "2010/01/01 00:00:00", new Timestamp(2010-1900,0,1,0,0,0,0)});
				data.add(new Object[]{"MM/dd/yyyy HH:mm:ss", "01/01/2010 00:00:00", new Timestamp(2010-1900,0,1,0,0,0,0)});
				data.add(new Object[]{"dd/MM/yyyy HH:mm:ss", "01/01/2010 00:00:00", new Timestamp(2010-1900,0,1,0,0,0,0)});
				data.add(new Object[]{"yyyy-MM-dd HH:mm:ss", "2010-01-01 00:00:00", new Timestamp(2010-1900,0,1,0,0,0,0)});
				data.add(new Object[]{"yyyy-MM-dd HH:mm:ssZ", "2010-01-01 00:00:00-05", new Timestamp(2009-1900,11,31,23,0,0,0)});
				data.add(new Object[]{"yyyy-MM-dd HH:mm:ssZ", "2010-01-01 00:00:00-0500", new Timestamp(2009-1900,11,31,23,0,0,0)});
				data.add(new Object[]{"yyyy-MM-dd HH:mm:ssZ", "2010-01-01 00:00:00-0430", new Timestamp(2009-1900,11,31,22,30,0,0)});
				data.add(new Object[]{"yyyy-MM-dd HH:mm:ss.SSSZ", "2010-01-01 00:00:00.000-0430", new Timestamp(2009-1900,11,31,22,30,0,0)});
				data.add(new Object[]{"yyyy-MM-dd HH:mm:ss.SSSZ", "2010-01-01 00:00:00.000-0415", new Timestamp(2009-1900,11,31,22,15,0,0)});
				data.add(new Object[]{"yyyy-MM-dd HH:mm:ss.SSSZ", "2010-01-01 00:00:00.000-0405", new Timestamp(2009-1900,11,31,22,05,0,0)});

				return data;
		}

		private final String timestampFormat;
		private final String testData;
		private final Timestamp correctDate;

		public RowParserDateTimeTest(String timestampFormat, String testData,Timestamp correctDate) {
				this.timestampFormat = timestampFormat;
				this.testData = testData;
				this.correctDate = correctDate;
		}

		@Test
		public void testCanParseDateTimeCorrectly() throws Exception {
				ExecRow row = new ValueRow(1);
				DataValueDescriptor dvd = new SQLTimestamp();
				row.setColumn(1,dvd);

				RowParser parser = new RowParser(row,null,null,timestampFormat);
				ColumnContext ctx = new ColumnContext.Builder()
								.columnType(Types.TIMESTAMP)
								.nullable(true)
								.build();
				String[] testLine = new String[]{testData};
				ExecRow processed = parser.process(testLine,new ColumnContext[]{ctx});

				Assert.assertEquals("Incorrect parsed timestamp!", correctDate,processed.getColumn(1).getTimestamp(null));
		}

		@Ignore
		@Test
    public void testCanParseTwoDateFieldsCorrectlyWithSpecifiedFormat() throws Exception {
        ExecRow testRow = new ValueRow(1);
        DataValueDescriptor testDvd = new SQLTimestamp();
        testRow.setColumn(1,testDvd);

        RowParser parser = new RowParser(testRow,null,null,"MM/dd/yyyy HH:mm:ss");

        ColumnContext ctx = new ColumnContext.Builder()
                                    .columnType(Types.TIMESTAMP)
                .nullable(true)
                .build();
        String[] testLine1 = new String[]{"01/01/2013 00:00:00"};
        ExecRow processed = parser.process(testLine1,new ColumnContext[]{ctx});

        Assert.assertEquals("Incorrect parsed timestamp!",new Timestamp(2013-1900,0,1,0,0,0,0),processed.getColumn(1).getTimestamp(null));

        testLine1 = new String[]{"02/01/2013 00:00:00"};
        processed = parser.process(testLine1,new ColumnContext[]{ctx});

        Assert.assertEquals("Incorrect parsed timestamp!",new Timestamp(2013-1900,1,1,0,0,0,0),processed.getColumn(1).getTimestamp(null));
        
        
        testLine1 = new String[]{"2013-02-01 00:00:00-00"};
        parser = new RowParser(testRow,null,null,"yyyy-MM-dd HH:mm:ssZ");
        Assert.assertEquals("Incorrect parsed timestamp!",new Timestamp(2013-1900,1,1,0,0,0,0),processed.getColumn(1).getTimestamp(null));
        testLine1 = new String[]{"2013-02-01 00:00:00.00-00"};
        parser = new RowParser(testRow,null,null,"yyyy-MM-dd HH:mm:ss.SSZ");
        Assert.assertEquals("Incorrect parsed timestamp!",new Timestamp(2013-1900,1,1,0,0,0,0),processed.getColumn(1).getTimestamp(null));
        testLine1 = new String[]{"2013-02-01 00:00:00.000-00"};
        parser = new RowParser(testRow,null,null,"yyyy-MM-dd HH:mm:ss.SSSZ");
        Assert.assertEquals("Incorrect parsed timestamp!",new Timestamp(2013-1900,1,1,0,0,0,0),processed.getColumn(1).getTimestamp(null));

        RowParser parser1 = new RowParser(testRow,null,null,"MM/dd/yyyy HH:mm:ss@1");      
        ColumnContext ctx1 = new ColumnContext.Builder()
                                    .columnType(Types.TIMESTAMP)
                .nullable(true)
                .build();
        testLine1 = new String[]{"01/01/2013 00:00:00"};
        ExecRow processed1 = parser1.process(testLine1,new ColumnContext[]{ctx1});
        Assert.assertEquals("Incorrect parsed timestamp!",new Timestamp(2013-1900,0,1,0,0,0,0),processed1.getColumn(1).getTimestamp(null)); 
    }
}
