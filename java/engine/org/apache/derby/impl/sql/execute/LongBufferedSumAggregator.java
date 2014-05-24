package org.apache.derby.impl.sql.execute;

import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.reference.SQLState;
import org.apache.derby.iapi.sql.execute.ExecAggregator;
import org.apache.derby.iapi.types.DataValueDescriptor;
import org.apache.derby.iapi.types.SQLLongint;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

/**
 * @author Scott Fines
 *         Date: 5/15/14
 */
public class LongBufferedSumAggregator extends SumAggregator {

		private final long[] buffer;
		private final int length;
		private int position;


		private long sum = 0;

		public LongBufferedSumAggregator(int bufferSize) {
				int s = 1;
				while(s<bufferSize){
						s<<=1;
				}
				buffer = new long[s];
				this.length = s-1;
				position = 0;
		}

		@Override
		protected void accumulate(DataValueDescriptor addend) throws StandardException {
				buffer[position] = addend.getLong();
				incrementPosition();
		}


		@Override
		public void merge(ExecAggregator addend) throws StandardException {
				if(addend==null) return; //treat null entries as zero
				//In Splice, we should never see a different type of an ExecAggregator
				long otherSum = ((LongBufferedSumAggregator)addend).sum;
				buffer[position] = otherSum;
				incrementPosition();
		}

		@Override
		public void writeExternal(ObjectOutput out) throws IOException {
				//Need to sum up all the intermediate values before serializing
				if(position!=0){
						try {
								sum(position);
						} catch (StandardException e) {
								throw new IOException(e);
						}
						position=0;
				}
				out.writeBoolean(eliminatedNulls);
				out.writeLong(sum);
		}

		@Override
		public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
				this.eliminatedNulls = in.readBoolean();
				this.sum = in.readLong();
		}

		@Override
		public DataValueDescriptor getResult() throws StandardException {
				if (value == null) {
						value = new SQLLongint();
				}
				if(position!=0){
						sum(position);
						position=0;
				}
				value.setValue(sum);
				return value;
		}

		/**
		 * Can only be safely called after first calling getResult();
		 * e.g. after GenericAggregator.finish() has been called
		 * @return the current sum;
		 */
		public long getSum(){
				assert position==0: "There are entries still to be buffered!";
				return sum;
		}

		public void init(long sum,boolean eliminatedNulls){
				this.sum = sum;
				this.eliminatedNulls = eliminatedNulls;
		}

		@Override
		public ExecAggregator newAggregator() {
				return new LongBufferedSumAggregator(64);
		}

		/**
		 * Update this aggregator to an aggregator which does not suffer from the same overflows.
		 *
		 * @return an aggregator which can contain larger numbers
		 * @throws StandardException if something goes wrong.
		 */
		public SumAggregator upgrade() throws StandardException {
				DoubleBufferedSumAggregator aggregator = new DoubleBufferedSumAggregator(buffer.length);
				aggregator.init(sum,eliminatedNulls);
				for(int i=0;i<position;i++){
						aggregator.addDirect(buffer[i]);
				}
				return aggregator;
		}

		private boolean sum(int bufferLength) throws StandardException {
				long oldSum = sum;
				long l = buffer[0];
				boolean isNeg = (l<0);
				boolean signsDiffer = (sum <0)!=isNeg;
				/*
				 * We use a temporary local variable so that overflowing the sum will not
				 * corrupt our internal state (i.e. we can try to merge values, but if we overflow,
				 * then we won't have affected the actual sum value, so it'll still be useable).
				 */
				long newSum = sum;
				newSum+=l;
				for (int i=1;i<bufferLength;i++) {
						l = buffer[i];
						newSum += l;
						signsDiffer =  signsDiffer || ((l<0) != isNeg);
				}
						/*
						 * overflow can only occur if all addends have the same sign.
						 */
				if(!signsDiffer && (newSum<0)!=(oldSum<0)){
						throw StandardException.newException(SQLState.LANG_OUTSIDE_RANGE_FOR_DATATYPE,"BIGINT");
				}
				sum = newSum;
				return signsDiffer;
		}

		private void incrementPosition() throws StandardException {
				position = (position+1) & length;
				if(position==0){
						sum(buffer.length);
				}
		}
}
