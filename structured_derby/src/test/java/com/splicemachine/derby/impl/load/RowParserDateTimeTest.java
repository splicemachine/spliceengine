package com.splicemachine.derby.impl.load;

import com.google.common.collect.Lists;
import com.splicemachine.derby.impl.sql.execute.ValueRow;
import org.apache.derby.iapi.sql.execute.ExecRow;
import org.apache.derby.iapi.types.DataValueDescriptor;
import org.apache.derby.iapi.types.SQLTimestamp;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.sql.Timestamp;
import java.sql.Types;
import java.text.SimpleDateFormat;
import java.util.Collection;
import java.util.Date;
import java.util.TimeZone;

/**
 * Tests around RowParser's Date Time format.
 *
 * We want to make sure that we test parsing for all manner of different time zones. Otherwise,
 * we may do something that works for common Tzs, but is all kinds of screwed up when trying to run in,
 * say, Nepal, or India.
 *
 * @author Scott Fines
 * Created on: 9/30/13
 */
@SuppressWarnings("deprecation")
@RunWith(Parameterized.class)
public class RowParserDateTimeTest {

		private static final String[] formats = new String[]{
						"yyyy/MM/dd HH:mm:ss",
						"MM/dd/yyyy HH:mm:ss",
						"dd/MM/yyyy HH:mm:ss",
						"yyyy-MM-dd HH:mm:ss",
		};
		private static final String[] tzFormats = new String[]{
						"yyyy-MM-dd HH:mm:ssZ",
						"yyyy-MM-dd HH:mm:ssZ",
						"yyyy-MM-dd HH:mm:ssZ",
						"yyyy-MM-dd HH:mm:ss.SSSZ",
						"yyyy-MM-dd HH:mm:ss.SSSZ",
						"yyyy-MM-dd HH:mm:ss.SSSZ"
		};
		private static final TimeZone[] goofyTimeZones = new TimeZone[]{
						TimeZone.getTimeZone("America/Caracas"), // off by 30 minutes
						TimeZone.getTimeZone("Asia/Kathmandu"), //off by 15 or 45 minutes
						TimeZone.getDefault(), //default time zone
		};
		@Parameterized.Parameters
		public static Collection<Object[]> data() {
				Collection<Object[]> data = Lists.newArrayList();

				long time = System.currentTimeMillis();
				long timeToUse = 1000*(time/1000);
				Date date = new Date(time);
				Timestamp ts = new Timestamp(timeToUse);
				for(String format:formats){
						SimpleDateFormat sdf = new SimpleDateFormat(format);
						String text = sdf.format(date);
						data.add(new Object[]{format,text,ts});
				}

				//add specific timezone fields

				//pick Caracas because it has a crazy-ass offset of -430 for most of the year
				Date value = new Date(System.currentTimeMillis());
				for(TimeZone timeZone:goofyTimeZones){
						for(String tzFormat:tzFormats){
								long t = value.getTime();
								if(!tzFormat.matches(".*\\.S.*"))
										t = 1000*(t/1000);

								ts = new Timestamp(t);
								SimpleDateFormat sdf = new SimpleDateFormat(tzFormat);
								sdf.setTimeZone(timeZone);
								String text = sdf.format(value);
								data.add(new Object[]{tzFormat,text,ts});
						}
				}

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
}
