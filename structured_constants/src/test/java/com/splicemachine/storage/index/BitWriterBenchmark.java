package com.splicemachine.storage.index;

import com.splicemachine.storage.BitWriter;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

/**
 * @author Scott Fines
 * Date: 2/10/14
 */
@State(Scope.Thread)
public class BitWriterBenchmark {

		BitWriter bitWriter;
		byte[] data;

		@Setup(Level.Invocation)
		public void prepare(){
				int numBits = 1000000;
				int numBytes =1;
				numBits-=4;
				if(numBits>0){
						numBytes+=numBits/7;
						if(numBits%7!=0)
								numBytes++;
				}

				data = new byte[numBytes];
				bitWriter = new BitWriter(data,0,data.length,5,true);

		}

		@TearDown(Level.Invocation)
		public void teardown(){
				bitWriter = null;
		}

		@GenerateMicroBenchmark
		public byte[] encode(){
				bitWriter.set(3);
				return data;
		}

		public static void main(String...args) throws Exception{
				Options opt = new OptionsBuilder()
								.include(".*" + BitWriterBenchmark.class.getSimpleName() + ".*")
								.warmupIterations(5)
								.measurementIterations(5)
								.forks(1)
								.jvmArgs("-ea")
								.build();

				new Runner(opt).run();
		}
}
