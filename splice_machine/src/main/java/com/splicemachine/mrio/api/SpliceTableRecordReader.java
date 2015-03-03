package com.splicemachine.mrio.api;

import java.io.IOException;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
/**
 * Iterate over an HBase table data, return (ImmutableBytesWritable, Result)
 * pairs.
 */
public class SpliceTableRecordReader
extends RecordReader<ImmutableBytesWritable, ExecRow> {

  private SpliceTableRecordReaderImp recordReaderImpl = null;
  Configuration conf = null;
  
  public SpliceTableRecordReader(Configuration conf){
	  super();
	  this.conf = conf;
	  this.recordReaderImpl = new SpliceTableRecordReaderImp(conf);
  }
  
  /**
   * Restart from survivable exceptions by creating a new scanner.
   *
   * @param firstRow  The first row to start at.
   * @throws IOException When restarting fails.
   */
  public void restart(byte[] firstRow) throws IOException {
    this.recordReaderImpl.restart(firstRow);
  }


  /**
   * Sets the HBase table.
   *
   * @param htable  The {@link HTable} to scan.
   */
  public void setHTable(HTable htable) {
    this.recordReaderImpl.setHTable(htable);
  }

  /**
   * Sets the scan defining the actual details like columns etc.
   *
   * @param scan  The scan to set.
   */
  public void setScan(Scan scan) {
    this.recordReaderImpl.setScan(scan);
  }

  /**
   * Closes the split.
   *
   * @see org.apache.hadoop.mapreduce.RecordReader#close()
   */
  @Override
  public void close() {
    this.recordReaderImpl.close();
  }

  /**
   * Returns the current key.
   *
   * @return The current key.
   * @throws IOException
   * @throws InterruptedException When the job is aborted.
   * @see org.apache.hadoop.mapreduce.RecordReader#getCurrentKey()
   */
  @Override
  public ImmutableBytesWritable getCurrentKey() throws IOException,
      InterruptedException {
    return this.recordReaderImpl.getCurrentKey();
  }

  /**
   * Returns the current value.
   *
   * @return The current value.
   * @throws IOException When the value is faulty.
   * @throws InterruptedException When the job is aborted.
   * @see org.apache.hadoop.mapreduce.RecordReader#getCurrentValue()
   */
  @Override
  public ExecRow getCurrentValue() throws IOException, InterruptedException {
    try {
		return this.recordReaderImpl.getCurrentValue();
	} catch (StandardException e) {
		e.printStackTrace();
		throw new IOException(e);
	}
  }

  /**
   * Initializes the reader.
   *
   * @param inputsplit  The split to work with.
   * @param context  The current task context.
   * @throws IOException When setting up the reader fails.
   * @throws InterruptedException When the job is aborted.
   * @see org.apache.hadoop.mapreduce.RecordReader#initialize(
   *   org.apache.hadoop.mapreduce.InputSplit,
   *   org.apache.hadoop.mapreduce.TaskAttemptContext)
   */
  @Override
  public void initialize(InputSplit inputsplit,
      TaskAttemptContext context) throws IOException,
      InterruptedException {
    this.recordReaderImpl.initialize(inputsplit, context);
  }

  /**
   * Positions the record reader to the next record.
   *
   * @return <code>true</code> if there was another record.
   * @throws IOException When reading the record failed.
   * @throws InterruptedException When the job was aborted.
   * @see org.apache.hadoop.mapreduce.RecordReader#nextKeyValue()
   */
  @Override
  public boolean nextKeyValue() throws IOException, InterruptedException {
    try {
		return this.recordReaderImpl.nextKeyValue();
	} catch (StandardException e) {
		e.printStackTrace();
		throw new IOException(e);
	}
   
  }

  	public void init() throws IOException {
	    this.recordReaderImpl.init();
	  }
  
  /**
   * The current progress of the record reader through its data.
   *
   * @return A number between 0.0 and 1.0, the fraction of the data read.
   * @see org.apache.hadoop.mapreduce.RecordReader#getProgress()
   */
  @Override
  public float getProgress() {
    return this.recordReaderImpl.getProgress();
  }
  
}
