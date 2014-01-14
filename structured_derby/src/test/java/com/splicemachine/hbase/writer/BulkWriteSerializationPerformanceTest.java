package com.splicemachine.hbase.writer;

import com.carrotsearch.hppc.ObjectArrayList;
import com.esotericsoftware.kryo.Kryo;
import com.splicemachine.derby.hbase.SpliceDriver;
import com.splicemachine.utils.ByteDataOutput;
import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Histogram;
import com.yammer.metrics.core.MetricName;
import com.yammer.metrics.core.Timer;
import com.yammer.metrics.reporting.ConsoleReporter;

import java.io.IOException;
import java.util.Random;
import java.util.concurrent.TimeUnit;

/**
 * Compares performance of raw Java serialization with that of BulkWrite.toBytes()
 * in terms of both size of resulting byte[] and amount of time taken to write it.
 *
 * @author Scott Fines
 * Date: 12/10/13
 */
public class BulkWriteSerializationPerformanceTest {

		public static void main(String...args) throws Exception{
				System.out.println("Java Serialization");
				timeJavaSerialization(1000, 1000, 10, 100, 0f, 0f);
				System.out.println("-------------------------------------------\n");
				System.out.println("Kryo Serialization");
				timeKryoSerialization(1000, 1000, 10, 100, 0f, 0f);
		}

		private static void timeKryoSerialization(int numIterations,
																							int rowsPerIteration,
																							int keySize,
																							int rowSize,
																							float percentDeletes,
																							float percentUpdates) throws IOException {
				Random random = new Random(System.currentTimeMillis());

				Kryo kryo = SpliceDriver.getKryoPool().get(); //initialize kryo to avoid the timing cost
				Histogram sizeHistogram =Metrics.newHistogram(new MetricName("com.splicemachine", "test", "kryoByteSize"));
				sizeHistogram.clear();
				Timer timer = Metrics.newTimer(BulkWrite.class, "javaSerializationTime");
				timer.clear();
				for(int i=0;i<numIterations;i++){
						BulkWrite write = buildBulkWrite(rowsPerIteration, keySize, rowSize, random, percentDeletes, percentUpdates);
						long start = System.nanoTime();
						byte[] bytes = write.toBytes();
						long end = System.nanoTime();
						timer.update(end-start,TimeUnit.NANOSECONDS);
						sizeHistogram.update(bytes.length);
				}

				ConsoleReporter consoleReporter = new ConsoleReporter(System.out);
				System.out.println("Serialization Time:");
				consoleReporter.processTimer(new MetricName("test", "test", "serializationTime"), timer, System.out);
				System.out.println("Serialized Size(bytes):");
				consoleReporter.processHistogram(new MetricName("test","test","byteSize"),sizeHistogram,System.out);
		}

		private static void timeJavaSerialization(int numIterations,
																										 int rowsPerIteration,
																										 int keySize,
																										 int rowSize,
																										 float percentDeletes,
																										 float percentUpdates) throws IOException {
				Random random = new Random(System.currentTimeMillis());

				Histogram sizeHistogram =Metrics.newHistogram(new MetricName("com.splicemachine", "test", "javaByteSize"));
				Timer timer = Metrics.newTimer(BulkWrite.class, "kryoSerializationTime");
				ByteDataOutput bdo  = new ByteDataOutput();
				for(int i=0;i<numIterations;i++){
						BulkWrite write = buildBulkWrite(rowsPerIteration, keySize, rowSize, random, percentDeletes, percentUpdates);
						long start = System.nanoTime();
						bdo.reset();
						bdo.writeObject(write);
						long end = System.nanoTime();
						timer.update(end-start,TimeUnit.NANOSECONDS);
						sizeHistogram.update(bdo.toByteArray().length);
				}

				ConsoleReporter consoleReporter = new ConsoleReporter(System.out);
				System.out.println("Serialization Time:");
				consoleReporter.processTimer(new MetricName("test", "test", "serializationTime"), timer, System.out);
				System.out.println("Serialized Size(bytes):");
				consoleReporter.processHistogram(new MetricName("test","test","byteSize"),sizeHistogram,System.out);

		}

		private static BulkWrite buildBulkWrite(int rowsPerIteration,
																						int keySize,
																						int rowSize,
																						Random random,
																						float percentDeletes,
																						float percentUpdates) {
				ObjectArrayList<KVPair> kvPairs = new ObjectArrayList<KVPair>(rowsPerIteration);
				float updateThreshold = percentDeletes+percentUpdates;
				for(int i=0;i<rowsPerIteration;i++){
						byte[] nextKey = new byte[keySize];
						random.nextBytes(nextKey);
						byte[] nextRow = new byte[rowSize];
						random.nextBytes(nextRow);

						float typeFloat = random.nextFloat();
						KVPair.Type type;
						if(typeFloat<percentDeletes){
							type = KVPair.Type.DELETE;
						}else if(typeFloat < updateThreshold)
								type = KVPair.Type.UPDATE;
						else
							type = KVPair.Type.INSERT;

						kvPairs.add(new KVPair(nextKey,nextRow,type));
				}
				String txnId = Integer.toString(random.nextInt(2000000000));
				return new BulkWrite(kvPairs,txnId,new byte[]{});
		}
}
