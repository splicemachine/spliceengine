package org.apache.derby.impl.sql.execute;

import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.sql.execute.ExecAggregator;
import org.apache.derby.iapi.types.DataValueDescriptor;
import org.apache.derby.iapi.types.SQLDecimal;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.math.BigDecimal;

/**
 * @author Scott Fines
 *         Date: 5/16/14
 */
public class DecimalBufferedSumAggregator extends SumAggregator {
		private final BigDecimal[] buffer;
		private final int length;
		private int position;

		private BigDecimal sum = BigDecimal.ZERO;

		public DecimalBufferedSumAggregator(int bufferSize) {
				int s = 1;
				while(s<bufferSize){
						s<<=1;
				}
				buffer = new BigDecimal[s];
				this.length = s-1;
				position = 0;
		}

		@Override
		protected void accumulate(DataValueDescriptor addend) throws StandardException {
				buffer[position] = (BigDecimal)addend.getObject();
				incrementPosition();
		}


		@Override
		public void merge(ExecAggregator addend) throws StandardException {
				if(addend==null) return; //treat null entries as zero
				//In Splice, we should never see a different type of an ExecAggregator
				BigDecimal otherSum = ((DecimalBufferedSumAggregator)addend).sum;
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
				out.writeObject(sum);
		}

		@Override
		public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
				this.eliminatedNulls = in.readBoolean();
				this.sum = (BigDecimal)in.readObject();
		}

		@Override
		public DataValueDescriptor getResult() throws StandardException {
				if (value == null) {
						value = new SQLDecimal();
				}
				if(position!=0){
						sum(position);
						position=0;
				}
				value.setBigDecimal(sum);
				return value;
		}

		/**
		 * Can only be safely called after first calling getResult();
		 * e.g. after GenericAggregator.finish() has been called
		 * @return the current sum;
		 */
		public BigDecimal getSum(){
				assert position==0: "There are entries still to be buffered!";
				return sum;
		}

		public void init(BigDecimal sum,boolean eliminatedNulls){
				this.sum = sum;
				this.eliminatedNulls = eliminatedNulls;
		}

		@Override
		public ExecAggregator newAggregator() {
				return new DecimalBufferedSumAggregator(buffer.length);
		}

		private void sum(int bufferLength) throws StandardException {
				for (int i=0;i<bufferLength;i++) {
						BigDecimal l = buffer[i];
						sum=sum.add(l);
				}
		}

		private void incrementPosition() throws StandardException {
				position = (position+1) & length;
				if(position==0){
						sum(buffer.length);
				}
		}

		public void addDirect(BigDecimal bigDecimal) throws StandardException {
				buffer[position] = bigDecimal;
				incrementPosition();
		}
}
