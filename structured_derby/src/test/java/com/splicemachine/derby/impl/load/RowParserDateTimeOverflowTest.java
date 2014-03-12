package com.splicemachine.derby.impl.load;

import java.sql.Types;
import org.apache.derby.iapi.sql.execute.ExecRow;
import org.apache.derby.iapi.types.DataValueDescriptor;
import org.apache.derby.iapi.types.SQLDate;
import org.junit.Test;
import com.splicemachine.derby.impl.sql.execute.ValueRow;

public class RowParserDateTimeOverflowTest {
	@Test(expected=Exception.class)
	public void testCanParseDateTimeOverflowCorrectly() throws Exception {
			ExecRow row = new ValueRow(1);
			DataValueDescriptor dvd = new SQLDate();
			row.setColumn(1,dvd);

			RowParser parser = new RowParser(row,null,null,"yyyy-MM-dd",FailAlwaysReporter.INSTANCE);
			ColumnContext ctx = new ColumnContext.Builder()
							.columnType(Types.TIMESTAMP)
							.nullable(true)
							.build();
			String[] testLine = new String[]{"2001-30-04"};
			ExecRow processed = parser.process(testLine,new ColumnContext[]{ctx});
	}
	
}
