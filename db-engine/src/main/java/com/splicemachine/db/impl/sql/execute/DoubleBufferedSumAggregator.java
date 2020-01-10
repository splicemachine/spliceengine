/*
 * This file is part of Splice Machine.
 * Splice Machine is free software: you can redistribute it and/or modify it under the terms of the
 * GNU Affero General Public License as published by the Free Software Foundation, either
 * version 3, or (at your option) any later version.
 * Splice Machine is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU Affero General Public License for more details.
 * You should have received a copy of the GNU Affero General Public License along with Splice Machine.
 * If not, see <http://www.gnu.org/licenses/>.
 *
 * Some parts of this source code are based on Apache Derby, and the following notices apply to
 * Apache Derby:
 *
 * Apache Derby is a subproject of the Apache DB project, and is licensed under
 * the Apache License, Version 2.0 (the "License"); you may not use these files
 * except in compliance with the License. You may obtain a copy of the License at:
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 * Splice Machine, Inc. has modified the Apache Derby code in this file.
 *
 * All such Splice Machine modifications are Copyright 2012 - 2020 Splice Machine, Inc.,
 * and are licensed to you under the GNU Affero General Public License.
 */

package com.splicemachine.db.impl.sql.execute;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.execute.ExecAggregator;
import com.splicemachine.db.iapi.types.DataValueDescriptor;
import com.splicemachine.db.iapi.types.NumberDataType;
import com.splicemachine.db.iapi.types.SQLDouble;
import org.apache.commons.lang3.mutable.MutableDouble;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.math.BigDecimal;
import java.util.Arrays;
import java.util.Map;
import java.util.TreeMap;

/**
 * @author Scott Fines
 *         Date: 5/15/14
 */
public class DoubleBufferedSumAggregator extends SumAggregator{

		private final double[] buffer;
		private final int length;
		private int position;

        private java.util.TreeMap<Integer, MutableDouble> sumTree;
        private boolean isNull = true;

        public DoubleBufferedSumAggregator(int bufferSize) {
        	this(bufferSize, null);
		}
		public DoubleBufferedSumAggregator(int bufferSize, TreeMap<Integer, MutableDouble> tree) {
			    if (tree == null)
                    sumTree = new TreeMap<>();
			    else
					sumTree = tree;
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

		public void addDirect(double l) throws StandardException {
				buffer[position] = l;
				incrementPosition();
		}

		@Override
		public void merge(ExecAggregator addend) throws StandardException {
			if(addend==null) return; //treat null entries as zero
				//In Splice, we should never see a different type of an ExecAggregator
			DoubleBufferedSumAggregator other = (DoubleBufferedSumAggregator)addend;

            MutableDouble item;
            if (!other.sumTree.isEmpty())
                isNull = false;

            for (Map.Entry<Integer, MutableDouble> s : other.sumTree.entrySet()) {
                if ((item = sumTree.get(s.getKey())) != null)
                    item.setValue(s.getValue().doubleValue() + item.doubleValue());
                else
                    sumTree.put(s.getKey(), new MutableDouble(s.getValue()));
            }

            for (int i = 0; i< other.position;i++) {
                buffer[position] = other.buffer[i];
                incrementPosition();
            }
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
				out.writeObject(sumTree);
		}

		@Override
		public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
				this.eliminatedNulls = in.readBoolean();
				this.isNull = in.readBoolean();
				this.sumTree = (TreeMap<Integer, MutableDouble>) in.readObject();
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
				if (sumTree.isEmpty())
                    value.setToNull();
				else
				    value.setValue(collectFinalSum());
				return value;
		}

		private double collectFinalSum() throws StandardException {
			double tempSum = 0.0d;
			for (Map.Entry<Integer, MutableDouble> s : sumTree.entrySet()) {
				tempSum += s.getValue().doubleValue();
			}
			tempSum = NumberDataType.normalizeDOUBLE(tempSum);
			return tempSum;
		}

		public void init(double sum,boolean eliminatedNulls) throws StandardException {
        	    if (sum != 0.0d)
			        addDirect(sum);
				this.eliminatedNulls = eliminatedNulls;
				this.isNull = false;
		}

		@Override
		public ExecAggregator newAggregator() {
				return new DoubleBufferedSumAggregator(buffer.length);
		}

                private void sum(int bufferLength) throws StandardException { 
                    for (int i=0;i<bufferLength;i++) {
                        // Want to combine the big numbers first, so make
                        // the key the negative of the exponent, as the
                        // iterator will traverse the nodes in ascending
                        // order of the keys.
                        int ix = -java.lang.Math.getExponent(buffer[i]);
                        MutableDouble entry = sumTree.get(ix);
                        if (entry == null)
                            sumTree.put(ix, new MutableDouble(buffer[i]));
                        else
                            entry.setValue(entry.doubleValue() + buffer[i]);
                    }
					position = 0;
		}

		private void incrementPosition() throws StandardException {
				int newposition = (position+1) & length;
				if(newposition==0){
						sum(buffer.length);
				}
				position = newposition;
				isNull = false;
		}

      public String toString() {
         String bufferInfo = isNull ? null : (position < 25 && position > 0 ? 
                                                Arrays.toString(Arrays.copyOfRange(buffer, 0, position))
                                                : String.format("%s buffered", position));
         try {
			 return "DoubleBufferedSumAggregator: " + (isNull ? "NULL" :
				 String.format("{ sum=%s buffer=%s }", collectFinalSum(), bufferInfo));
		 }
         catch (StandardException e) {
         	return "Overflow";
		 }
      }

}
