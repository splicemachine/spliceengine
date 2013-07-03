package com.splicemachine.spark;


import java.util.ArrayList;
import java.util.List;
import org.apache.derby.iapi.sql.execute.ExecRow;
import org.apache.derby.iapi.types.DataValueDescriptor;
import org.apache.derby.iapi.types.SQLVarchar;
import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import com.splicemachine.derby.impl.sql.execute.ValueRow;
import scala.Option;
import scala.Tuple2;
import spark.api.java.JavaPairRDD;
import spark.api.java.JavaSparkContext;

public class JoinTests {
	private static Logger LOG = Logger.getLogger(JoinTests.class);
	protected static List<Tuple2<ExecRow,ExecRow>> stringBasedRowsSet1 = new ArrayList<Tuple2<ExecRow,ExecRow>>();
	protected static List<Tuple2<ExecRow,ExecRow>> stringBasedRowsSet2 = new ArrayList<Tuple2<ExecRow,ExecRow>>();
	protected static JavaSparkContext ctx = new JavaSparkContext("local", "JavaWordCount");		   
	protected static int[] keyValues = {1};
	
	
	@BeforeClass
	public static void setup() {
		stringBasedRowsSet1.add(populateData(keyValues,"John","Leach"));
		stringBasedRowsSet1.add(populateData(keyValues,"John","Lockhead"));
		stringBasedRowsSet1.add(populateData(keyValues,"Monte","Zweben"));
		stringBasedRowsSet1.add(populateData(keyValues,"Rich","Rheimer"));
		stringBasedRowsSet1.add(populateData(keyValues,"Gene","Davis"));
		stringBasedRowsSet1.add(populateData(keyValues,"Scott","Fines"));
		stringBasedRowsSet2.add(populateData(keyValues,"John","Leach"));
		stringBasedRowsSet2.add(populateData(keyValues,"John","Lockhead"));
		stringBasedRowsSet2.add(populateData(keyValues,"Monte","Zweben"));
		stringBasedRowsSet2.add(populateData(keyValues,"Rich","Rheimer"));
		stringBasedRowsSet2.add(populateData(keyValues,"Gene","Davis"));		
	}

	private static Tuple2<ExecRow,ExecRow> populateData(int[] keyColumns, String...values) {
		String[] string = new String[keyColumns.length];
		for (int i = 0; i < keyColumns.length; i++) {
			string[i] = values[keyColumns[i]];
		}
		return new Tuple2<ExecRow,ExecRow>(generateStringValueRow(string),generateStringValueRow(values));
	}
	
	private static ValueRow generateStringValueRow(String... string) {
		ValueRow row = new ValueRow(string.length);
		for (int i = 0; i < string.length; i++) {
			row.setColumn(i+1, new SQLVarchar(string[i]));
		}
		return row;		
	}

	private static ValueRow generateValueRow(DataValueDescriptor... dvd) {
		ValueRow row = new ValueRow(dvd.length);
		for (int i = 0; i < dvd.length; i++) {
			row.setColumn(i+1, dvd[i]);
		}
		return row;		
	}

	@Test
	public void testcartesionJoin() {
		JavaPairRDD<ExecRow, ExecRow> row1 = ctx.parallelizePairs(stringBasedRowsSet1);
		JavaPairRDD<ExecRow, ExecRow> row2 = ctx.parallelizePairs(stringBasedRowsSet2);
		JavaPairRDD<Tuple2<ExecRow, ExecRow>,Tuple2<ExecRow, ExecRow>> results = row1.cartesian(row2);
		List<Tuple2<Tuple2<ExecRow, ExecRow>, Tuple2<ExecRow, ExecRow>>> data = results.collect();
		for (Tuple2<Tuple2<ExecRow, ExecRow>, Tuple2<ExecRow,ExecRow>> row: data) {
			System.out.println(row);
		}
		Assert.assertEquals(30,results.count());
	}

	@Test
	public void testLeftOuterJoin() {
		JavaPairRDD<ExecRow, ExecRow> row1 = ctx.parallelizePairs(stringBasedRowsSet1);
		JavaPairRDD<ExecRow, ExecRow> row2 = ctx.parallelizePairs(stringBasedRowsSet2);
		JavaPairRDD<ExecRow, Tuple2<ExecRow, Option<ExecRow>>> test = row1.leftOuterJoin(row2);
		List<Tuple2<ExecRow, Tuple2<ExecRow, Option<ExecRow>>>> values = test.collect();
		for (Tuple2<ExecRow, Tuple2<ExecRow, Option<ExecRow>>> row : values) {
			System.out.println(row);
		}
		Assert.assertEquals(6, test.count());
	}

	@Test
	public void testRightOuterJoin() {
		JavaPairRDD<ExecRow, ExecRow> row1 = ctx.parallelizePairs(stringBasedRowsSet1);
		JavaPairRDD<ExecRow, ExecRow> row2 = ctx.parallelizePairs(stringBasedRowsSet2);
		JavaPairRDD<ExecRow, Tuple2<Option<ExecRow>, ExecRow>> test = row1.rightOuterJoin(row2);
		List<Tuple2<ExecRow, Tuple2<Option<ExecRow>, ExecRow>>> values = test.collect();
		for (Tuple2<ExecRow, Tuple2<Option<ExecRow>, ExecRow>> row : values) {
			System.out.println(row);
		}
		Assert.assertEquals(5, test.count());
	}

	@Test
	@Ignore
	public void testInnerJoin() {
		JavaPairRDD<ExecRow, ExecRow> row1 = ctx.parallelizePairs(stringBasedRowsSet1);
		JavaPairRDD<ExecRow, ExecRow> row2 = ctx.parallelizePairs(stringBasedRowsSet2);
		JavaPairRDD<ExecRow, Tuple2<ExecRow, ExecRow>> test = row1.join(row2);
		List<Tuple2<ExecRow, Tuple2<ExecRow, ExecRow>>> values = test.collect();
		for (Tuple2<ExecRow, Tuple2<ExecRow, ExecRow>> row : values) {
			System.out.println(row);
		}
		Assert.assertEquals(5, test.count());
	}

	
	@Test
	public void testUnionAll() {
		JavaPairRDD<ExecRow, ExecRow> row1 = ctx.parallelizePairs(stringBasedRowsSet1);
		JavaPairRDD<ExecRow, ExecRow> row2 = ctx.parallelizePairs(stringBasedRowsSet2);
		JavaPairRDD<ExecRow, ExecRow> results = row1.union(row2);
		Assert.assertEquals(11,results.count());
	}

	@Test
	public void testSortedUnionAll() {
		JavaPairRDD<ExecRow, ExecRow> row1 = ctx.parallelizePairs(stringBasedRowsSet1);
		JavaPairRDD<ExecRow, ExecRow> row2 = ctx.parallelizePairs(stringBasedRowsSet2);
		JavaPairRDD<ExecRow, ExecRow> results = row1.union(row2).sortByKey();
		for (Tuple2<ExecRow, ExecRow> row : results.collect()) {
			System.out.println(row);
		}

		Assert.assertEquals(11,results.count());
	}

	@Test
	@Ignore
	public void testUnion() {
		JavaPairRDD<ExecRow, ExecRow> row1 = ctx.parallelizePairs(stringBasedRowsSet1);
		JavaPairRDD<ExecRow, ExecRow> row2 = ctx.parallelizePairs(stringBasedRowsSet2);
		JavaPairRDD<ExecRow, ExecRow> results = row1.union(row2).distinct().sortByKey();
		for (Tuple2<ExecRow, ExecRow> row: results.collect()) {
			System.out.println(row);
		}
		//Assert.assertEquals(6,results.count());
	}

}
