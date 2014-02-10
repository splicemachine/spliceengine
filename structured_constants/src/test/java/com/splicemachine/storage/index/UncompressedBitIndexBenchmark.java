package com.splicemachine.storage.index;

import com.carrotsearch.hppc.BitSet;
import org.openjdk.jmh.annotations.GenerateMicroBenchmark;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

/**
 * @author Scott Fines
 *         Date: 2/10/14
 */
@State(Scope.Thread)
public class UncompressedBitIndexBenchmark {

		BitIndex index;

		@Setup
		public void prepare(){
				BitSet setFields = new BitSet(4);
				setFields.set(0,4);
				BitSet scalarFields = new BitSet(0);
				BitSet floatFields = new BitSet(0);
				BitSet doubleFields = new BitSet(0);

				index = BitIndexing.uncompressedBitMap(setFields,scalarFields,floatFields,doubleFields);
		}

		@GenerateMicroBenchmark
		public byte[] benchmarkEncode(){
				return index.encode();
		}

		@GenerateMicroBenchmark
		public int benchmarkSize(){
				return index.encodedSize();
		}

		public static void main(String...args) throws Exception{
				Options opt = new OptionsBuilder()
								.include(".*" + UncompressedBitIndexBenchmark.class.getSimpleName() + ".*")
								.warmupIterations(10)
								.measurementIterations(5)
								.forks(1)
								.jvmArgs("-ea")
								.build();

				new Runner(opt).run();
		}
}
