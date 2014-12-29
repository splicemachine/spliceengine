package com.splicemachine.pipeline.impl;

import com.carrotsearch.hppc.ObjectArrayList;
import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

/**
 * @author Scott Fines
 * Created on: 8/8/13
 */
public class BulkWritesResult implements Externalizable {
	private ObjectArrayList<BulkWriteResult> bulkWriteResults;
	
	
	public BulkWritesResult() {
		this.bulkWriteResults = new ObjectArrayList<>();
    }

	public BulkWritesResult(ObjectArrayList<BulkWriteResult> bulkWriteResults){
		this.bulkWriteResults = bulkWriteResults;
	}

    public void addResult(BulkWriteResult result) {
    	bulkWriteResults.add(result);
    }

    public ObjectArrayList<BulkWriteResult> getBulkWriteResults() {
		return bulkWriteResults;
	}

	public void setBulkWriteResults(ObjectArrayList<BulkWriteResult> bulkWriteResults) {
		this.bulkWriteResults = bulkWriteResults;
	}

	@Override
    public String toString() {
        StringBuilder sb = new StringBuilder("BulkWritesResult{");
        		int isize = bulkWriteResults.size();
        		for (int i = 0; i<isize; i++) {
        			sb.append(bulkWriteResults.get(i));
        			if (i!=isize-1)
        				sb.append(",");
        		}
        		return sb.toString();
    }

		@Override
		public void writeExternal(final ObjectOutput out) throws IOException {
				out.writeInt(bulkWriteResults.size());
				Object[] buffer = bulkWriteResults.buffer;
				int size = bulkWriteResults.size();
				for(int i=0;i<size;i++){
					out.writeObject(buffer[i]);
				}
		}

		@Override
		public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
				int size = in.readInt();
				bulkWriteResults = new ObjectArrayList<>(size);
				for(int i=0;i<size;i++){
					bulkWriteResults.add((BulkWriteResult) in.readObject());
				}
		}

}
