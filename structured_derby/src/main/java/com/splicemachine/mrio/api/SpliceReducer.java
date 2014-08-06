package com.splicemachine.mrio.api;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.mapred.RawKeyValueIterator;
import org.apache.hadoop.mapreduce.ReduceContext;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.TaskAttemptID;

/** 
 * Reduces a set of intermediate values which share a key to a smaller set of
 * values.  
 * 
 * <p><code>Reducer</code> implementations 
 * can access the {@link Configuration} for the job via the 
 * {@link JobContext#getConfiguration()} method.</p>

 * <p><code>Reducer</code> has 3 primary phases:</p>
 * <ol>
 *   <li>
 *   
 *   <h4 id="Shuffle">Shuffle</h4>
 *   
 *   <p>The <code>Reducer</code> copies the sorted output from each 
 *   {@link Mapper} using HTTP across the network.</p>
 *   </li>
 *   
 *   <li>
 *   <h4 id="Sort">Sort</h4>
 *   
 *   <p>The framework merge sorts <code>Reducer</code> inputs by 
 *   <code>key</code>s 
 *   (since different <code>Mapper</code>s may have output the same key).</p>
 *   
 *   <p>The shuffle and sort phases occur simultaneously i.e. while outputs are
 *   being fetched they are merged.</p>
 *      
 *   <h5 id="SecondarySort">SecondarySort</h5>
 *   
 *   <p>To achieve a secondary sort on the values returned by the value 
 *   iterator, the application should extend the key with the secondary
 *   key and define a grouping comparator. The keys will be sorted using the
 *   entire key, but will be grouped using the grouping comparator to decide
 *   which keys and values are sent in the same call to reduce.The grouping 
 *   comparator is specified via 
 *   {@link Job#setGroupingComparatorClass(Class)}. The sort order is
 *   controlled by 
 *   {@link Job#setSortComparatorClass(Class)}.</p>
 *   
 *   
 *   For example, say that you want to find duplicate web pages and tag them 
 *   all with the url of the "best" known example. You would set up the job 
 *   like:
 *   <ul>
 *     <li>Map Input Key: url</li>
 *     <li>Map Input Value: document</li>
 *     <li>Map Output Key: document checksum, url pagerank</li>
 *     <li>Map Output Value: url</li>
 *     <li>Partitioner: by checksum</li>
 *     <li>OutputKeyComparator: by checksum and then decreasing pagerank</li>
 *     <li>OutputValueGroupingComparator: by checksum</li>
 *   </ul>
 *   </li>
 *   
 *   <li>   
 *   <h4 id="Reduce">Reduce</h4>
 *   
 *   <p>In this phase the 
 *   {@link #reduce(Object, Iterable, Context)}
 *   method is called for each <code>&lt;key, (collection of values)></code> in
 *   the sorted inputs.</p>
 *   <p>The output of the reduce task is typically written to a 
 *   {@link RecordWriter} via 
 *   {@link Context#write(Object, Object)}.</p>
 *   </li>
 * </ol>
 * 
 * <p>The output of the <code>Reducer</code> is <b>not re-sorted</b>.</p>
 * 
 * <p>Example:</p>
 * <p><blockquote><pre>
 * public class IntSumReducer<Key> extends Reducer<Key,IntWritable,
 *                                                 Key,IntWritable> {
 *   private IntWritable result = new IntWritable();
 * 
 *   public void reduce(Key key, Iterable<IntWritable> values, 
 *                      Context context) throws IOException {
 *     int sum = 0;
 *     for (IntWritable val : values) {
 *       sum += val.get();
 *     }
 *     result.set(sum);
 *     context.collect(key, result);
 *   }
 * }
 * </pre></blockquote></p>
 * 
 * @see Mapper
 * @see Partitioner
 */
public class SpliceReducer<KEYIN,VALUEIN,KEYOUT,VALUEOUT> extends Reducer<KEYIN,VALUEIN,KEYOUT,VALUEOUT>{

  /**
   * The <code>Context</code> passed on to the {@link Reducer} implementations.
   */
	private SQLUtil sqlUtil = SQLUtil.getInstance();
  
  /**
   * Advanced application writers can use the 
   * {@link #run(org.apache.hadoop.mapreduce.Reducer.Context)} method to
   * control how the reduce task works.
   */
  @Override
  public void run(Context context){
	  Connection childConn = null;
	  try
		{
			childConn = sqlUtil.createConn();
			sqlUtil.disableAutoCommit(childConn);
			String txsID = sqlUtil.getTransactionID(childConn);
			String taskID = context.getTaskAttemptID().getTaskID().toString();
			context.getConfiguration().set(taskID, txsID);
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	  
	  try {
		super.setup(context);
		while (context.nextKey()) {
	      reduce(context.getCurrentKey(), context.getValues(), context);
	    }
	    System.out.println("------"+context.getStatus());
	    super.cleanup(context);
	    commit(childConn);
	} catch (IOException e1) {
		// TODO Auto-generated catch block
		System.out.println("IOException, rolling back child transaction");
		rollback(childConn);
	} catch (InterruptedException e1) {
		// TODO Auto-generated catch block
		System.out.println("InterruptedException, rolling back child transaction");
		rollback(childConn);
	} 
  }
  
  public void commit(Connection conn)
  {
	  try {
		conn.commit();
	} catch (SQLException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	}
  }
  
  public void rollback(Connection conn)
  {
	  try {
		conn.rollback();
	} catch (SQLException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	}
  }
}