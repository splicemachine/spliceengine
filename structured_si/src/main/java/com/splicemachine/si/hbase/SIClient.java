package com.splicemachine.si.hbase;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import com.splicemachine.impl.si.txn.Transaction;

public class SIClient {
	protected HashMap<HRegionLocation,List<Mutation>> federatedMutations = new HashMap<HRegionLocation,List<Mutation>>();
	protected HTable htable;
	protected Transaction tranaction;
	public SIClient () {
		
	}
	
	public SIClient(HTableInterface htableInterface) {
		htable = ((HTable) htableInterface);
	}
	
	public void put(final Put put) throws IOException {
	   doPut(Arrays.asList(put));
	}

	public void put(final List<Put> puts) throws IOException {
	  doPut(puts);
	}
	
	private void doPut(final List<Put> puts) throws IOException {
	    for (Put put : puts) {
	    	HRegionLocation regionLocation = htable.getRegionLocation(put.getRow(), false);
	    	if (federatedMutations.containsKey(regionLocation)) {
	    		federatedMutations.get(regionLocation).add(put);
	    	} else {	    		
	    		federatedMutations.put(regionLocation, Arrays.asList((Mutation)put));
	    	}
	  }
	}
	/*
	public void flushMutations() throws IOException, Throwable  {
		for (final HRegionLocation regionLocation : federatedPuts.keySet()) {
			htable.coprocessorExec(SIWriteProtocol.class, regionLocation.getRegionInfo().getStartKey(), regionLocation.getRegionInfo().getEndKey(), new Batch.Call<SIWriteProtocol,SIWriteResponse>(){
					
					
					@Override
					public SIWriteResponse call(SIWriteProtocol instance) throws IOException {
						
						return instance.puts(federatedPuts.get(regionLocation));
					}
				},
				new Batch.Callback<SIWriteResponse>() {

					@Override
					public void update(byte[] region, byte[] row,SIWriteResponse result) {
						
					}
					
				});	
		}
	}
	*/

}
