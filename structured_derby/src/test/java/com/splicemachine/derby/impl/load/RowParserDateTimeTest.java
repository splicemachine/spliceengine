package com.splicemachine.derby.impl.load;

import com.google.common.collect.Lists;

import org.apache.derby.iapi.sql.execute.ExecRow;
import org.apache.derby.iapi.types.DataValueDescriptor;
import org.apache.derby.iapi.types.SQLTimestamp;
import org.apache.derby.impl.sql.execute.ValueRow;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.sql.Timestamp;
import java.sql.Types;
import java.text.SimpleDateFormat;
import java.util.*;

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

		private static final String[] secondsOnlyFormats = new String[]{
						"yyyy/MM/dd HH:mm:ss",
						"MM/dd/yyyy HH:mm:ss",
						"dd/MM/yyyy HH:mm:ss",
						"yyyy-MM-dd HH:mm:ss",
		};

		private static final String[] millisFormats = new String[]{
						"yyyy/MM/dd HH:mm:ss.SSS",
						"yyyy/MM/dd HH:mm:ss.SS",
		};
		private static final String[] tzSecondsOnlyFormats = new String[]{
						"yyyy-MM-dd HH:mm:ssZ",
						"yyyy/MM/dd HH:mm:ssZ",
						"dd/MM/yyyy HH:mm:ssZ",
						"yyyy-MM-dd HH:mm:ssZ",
		};
		private static final String[] tzMillisFormats = new String[]{
						"yyyy-MM-dd HH:mm:ss.SSSZ",
						"yyyy/MM/dd HH:mm:ss.SSSZ",
						"dd/MM/yyyy HH:mm:ss.SSSZ",
						"yyyy-MM-dd HH:mm:ss.SSSZ",
		};
		private static final TimeZone[] goofyTimeZones = new TimeZone[]{
						TimeZone.getTimeZone("America/Caracas"), // off by 30 minutes
						TimeZone.getTimeZone("Asia/Kathmandu"), //off by 15 or 45 minutes
						TimeZone.getDefault(), //default time zone
		};
		@Parameterized.Parameters
		public static Collection<Object[]> data() {
				Collection<Object[]> data = Lists.newArrayList();

				addTimestamp(data, Timestamp.valueOf("2014-04-24 12:23:32.417"));
				addTimestamp(data,Timestamp.valueOf("2014-04-24 12:34:46.655"));
				addTimestamp(data,Timestamp.valueOf("2014-04-24 12:36:23.701"));
				addTimestamp(data,Timestamp.valueOf("2013-04-21 09:21:24.980"));
				long time = System.currentTimeMillis();
				addTimestamp(data,new Timestamp(time));

				return data;
		}

		protected static void addTimestamp(Collection<Object[]> data, Timestamp ts) {
				for(String format: secondsOnlyFormats){
						SimpleDateFormat sdf = new SimpleDateFormat(format);
						String text = sdf.format(ts);
						data.add(new Object[]{format,text,ts,true});
				}
				for(String format: millisFormats){
						SimpleDateFormat sdf = new SimpleDateFormat(format);
						String text = sdf.format(ts);
						data.add(new Object[]{format,text,ts,false});
				}
				for(TimeZone tz:goofyTimeZones){
						for(String format:tzSecondsOnlyFormats){
								SimpleDateFormat sdf = new SimpleDateFormat(format);
								sdf.setTimeZone(tz);
								String text = sdf.format(ts);
								data.add(new Object[]{format,text,ts,true});
						}
						for(String format:tzMillisFormats){
								SimpleDateFormat sdf = new SimpleDateFormat(format);
								sdf.setTimeZone(tz);
								String text = sdf.format(ts);
								data.add(new Object[]{format,text,ts,false});
						}
				}
		}

		private final String timestampFormat;
		private final String testData;
		private final Timestamp correctDate;
		private final boolean truncateMillis;

		public RowParserDateTimeTest(String timestampFormat, String testData,Timestamp correctDate,boolean truncateMillis) {
				this.timestampFormat = timestampFormat;
				this.testData = testData;
				this.correctDate = correctDate;
				this.truncateMillis = truncateMillis;
		}

		@Test
		public void testCanParseDateTimeCorrectly() throws Exception {
				System.out.println(correctDate);
				ExecRow row = new ValueRow(1);
				DataValueDescriptor dvd = new SQLTimestamp();
				row.setColumn(1,dvd);

				RowParser parser = new RowParser(row,null,null,timestampFormat,FailAlwaysReporter.INSTANCE);
				ColumnContext ctx = new ColumnContext.Builder()
								.columnType(Types.TIMESTAMP)
								.nullable(true)
								.build();
				String[] testLine = new String[]{testData};
				ExecRow processed = parser.process(testLine,new ColumnContext[]{ctx});

				Timestamp actual = processed.getColumn(1).getTimestamp(null);
				long correctTime = correctDate.getTime();
				if(truncateMillis){
						correctTime=1000*(correctTime/1000);
				}
				Assert.assertEquals("Incorrect parsed timestamp!", correctTime,actual.getTime());
		}
}
