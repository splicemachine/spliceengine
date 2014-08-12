package com.splicemachine.mrio.sample;



import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.SpliceHConnection;
import org.apache.hadoop.util.ProgramDriver;

public class Driver {

	public static void main(String[] args) throws Throwable {
	    ProgramDriver pgd = new ProgramDriver();
	    SpliceHConnection shc = new SpliceHConnection(new Configuration(), false);
	    
	    pgd.addClass(WordCount.NAME, WordCount.class, "WordCount");
	    pgd.driver(args);
	  }
}
