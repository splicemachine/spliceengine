package com.splicemachine.derby.impl.load;

import com.splicemachine.constants.SpliceConstants;
import com.splicemachine.stats.IOStats;
import com.splicemachine.stats.Metrics;
import com.splicemachine.stats.TimeView;
import com.splicemachine.stats.Timer;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;

import java.util.Set;

/**
 * @author Scott Fines
 *         Date: 2/12/14
 */
public class FileImportReaderTest {

		@Test
		public void testPerformance() throws Exception {
				FileImportReader reader = new FileImportReader();
				ImportContext ctx = new ImportContext.Builder().
								path("/Users/scottfines/workspace/customer/radiumone/data/PROBLEM/download/data/radiumone/small/019220_0.gz")
								.destinationTable(1184l)
								.colDelimiter(",")
								.transactionId("12")
								.recordStats().build();

				reader.setup(FileSystem.get(SpliceConstants.config),ctx);

				Timer globalTimer = Metrics.newTimer();
				String[] data = null;
//				int lineCount = 0;
				globalTimer.startTiming();
				while((data = reader.nextRow())!=null){
//					lineCount++;
				}
				globalTimer.stopTiming();
//				System.out.printf("Ignore: %d%n",lineCount);

				IOStats stats = reader.getStats();
				System.out.printf("Rows: %d%n",stats.getRows());
				System.out.printf("MB: %f%n",stats.getBytes()/(1024*1024d));

				TimeView time = stats.getTime();


				printTimer("Total",globalTimer.getTime(),0);
				printTimer("Read",time,stats.getRows());
		}

		protected void printTimer(String prefix,TimeView time, long numEvents) {
				double conversion = 1000*1000*1000d;
				double wallTimeS = time.getWallClockTime()/conversion;
				System.out.printf("%s:WallTime:%f%n",prefix,wallTimeS);
				double cpuTimeS = time.getCpuTime()/conversion;
				System.out.printf("%s:CpuTime:%f%n",prefix,cpuTimeS);
				double userTimeS = time.getUserTime()/conversion;
				System.out.printf("%s:UserTime:%f%n",prefix,userTimeS);
				System.out.printf("%s:NumEvents:%d%n",prefix, numEvents);
				System.out.printf("----------------------%n%n");
		}
}
