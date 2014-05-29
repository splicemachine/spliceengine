package org.apache.derby.impl.sql.execute;

import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.sql.execute.ExecAggregator;
import org.apache.derby.iapi.types.DataValueDescriptor;
import org.apache.derby.iapi.types.NumberDataType;
import org.apache.derby.iapi.types.SQLReal;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

/**
 * @author Scott Fines
 *         Date: 5/16/14
 */
public class FloatBufferedSumAggregator extends SumAggregator {
		private final float[] buffer;
		private final int length;
		private int position;

		private float sum = 0f;
		private boolean isNull = true;

		public FloatBufferedSumAggregator(int bufferSize) {
				int s = 1;
				while(s<bufferSize){
						s<<=1;
				}
				buffer = new float[s];
				this.length = s-1;
				position = 0;
		}

		@Override
		protected void accumulate(DataValueDescriptor addend) throws StandardException {
				buffer[position] = addend.getFloat();
				incrementPosition();
		}


		@Override
		public void merge(ExecAggregator addend) throws StandardException {
				if(addend==null) return; //treat null entries as zero
				//In Splice, we should never see a different type of an ExecAggregator
				float otherSum = ((FloatBufferedSumAggregator)addend).sum;
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
				out.writeBoolean(isNull);
				out.writeFloat(sum);
		}

		@Override
		public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
				this.eliminatedNulls = in.readBoolean();
				this.isNull = in.readBoolean();
				this.sum = in.readFloat();
		}

		@Override
		public DataValueDescriptor getResult() throws StandardException {
				if (value == null) {
						value = new SQLReal();
				}
				if(isNull){
						value.setToNull();
						return value;
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
		public float getSum(){
				assert position==0: "There are entries still to be buffered!";
				return sum;
		}

		public void init(float sum,boolean eliminatedNulls){
				this.sum = sum;
				this.eliminatedNulls = eliminatedNulls;
		}

		@Override
		public ExecAggregator newAggregator() {
				return new FloatBufferedSumAggregator(buffer.length);
		}

		private void sum(int bufferLength) throws StandardException {
				float newSum = sum;
				for (int i=0;i<bufferLength;i++) {
						float l = buffer[i];
						newSum += l;
				}
				//normalize the sum to ensure it remains valid
				sum = NumberDataType.normalizeREAL(newSum);
		}

		private void incrementPosition() throws StandardException {
				isNull=false;
				position = (position+1) & length;
				if(position==0){
						sum(buffer.length);
				}
		}

		public SumAggregator upgrade() throws StandardException {
				DoubleBufferedSumAggregator agg = new DoubleBufferedSumAggregator(buffer.length);
				agg.init(sum,eliminatedNulls);
				for(int i=0;i<position;i++){
						agg.addDirect(buffer[i]);
				}
				return agg;
		}
}
