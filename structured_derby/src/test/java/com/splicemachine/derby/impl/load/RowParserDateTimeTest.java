package com.splicemachine.derby.impl.load;

import com.splicemachine.derby.impl.sql.execute.ValueRow;
import org.apache.derby.iapi.sql.execute.ExecRow;
import org.apache.derby.iapi.types.DataValueDescriptor;
import org.apache.derby.iapi.types.SQLTimestamp;
import org.junit.Assert;
import org.junit.Test;

import java.sql.Timestamp;
import java.sql.Types;

/**
 * Tests around RowParser's Date Time format.
 *
 * @author Scott Fines
 * Created on: 9/30/13
 */
public class RowParserDateTimeTest {

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
        
    }
}
