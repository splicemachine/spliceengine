package org.apache.derby.impl.sql.execute;

import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.sql.execute.ExecAggregator;
import org.apache.derby.iapi.types.DataValueDescriptor;
import org.apache.derby.iapi.types.NumberDataType;
import org.apache.derby.iapi.types.SQLDouble;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

/**
 * @author Scott Fines
 *         Date: 5/15/14
 */
public class DoubleBufferedSumAggregator extends SumAggregator{

		private final double[] buffer;
		private final int length;
		private int position;

		private double sum = 0d;

		public DoubleBufferedSumAggregator(int bufferSize) {
				int s = 1;
				while(s<bufferSize){
						s<<=1;
				}
				buffer = new double[s];
				this.length = s-1;
				position = 0;
		}

		@Override
		protected void accumulate(DataValueDescriptor addend) throws StandardException {
				buffer[position] = addend.getDouble();
				incrementPosition();
		}


		@Override
		public void merge(ExecAggregator addend) throws StandardException {
				if(addend==null) return; //treat null entries as zero
				//In Splice, we should never see a different type of an ExecAggregator
				double otherSum = ((DoubleBufferedSumAggregator)addend).sum;
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
				out.writeDouble(sum);
		}

		@Override
		public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
				this.eliminatedNulls = in.readBoolean();
				this.sum = in.readDouble();
		}

		@Override
		public DataValueDescriptor getResult() throws StandardException {
				if (value == null) {
						value = new SQLDouble();
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
		public double getSum(){
				assert position==0: "There are entries still to be buffered!";
				return sum;
		}

		public void init(double sum,boolean eliminatedNulls){
				this.sum = sum;
				this.eliminatedNulls = eliminatedNulls;
		}

		@Override
		public ExecAggregator newAggregator() {
				return new DoubleBufferedSumAggregator(buffer.length);
		}

		private void sum(int bufferLength) throws StandardException {
				for (int i=0;i<bufferLength;i++) {
						double l = buffer[i];
						sum += l;
				}
				//normalize the sum to ensure it remains valid
				sum = NumberDataType.normalizeDOUBLE(sum);
		}

		private void incrementPosition() throws StandardException {
				position = (position+1) & length;
				if(position==0){
						sum(buffer.length);
				}
		}
}
