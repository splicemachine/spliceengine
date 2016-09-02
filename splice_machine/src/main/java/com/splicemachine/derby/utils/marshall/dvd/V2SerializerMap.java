/*
 * Copyright 2012 - 2016 Splice Machine, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */

package com.splicemachine.derby.utils.marshall.dvd;

import com.splicemachine.SpliceKryoRegistry;

/**
 * @author Scott Fines
 *         Date: 4/7/14
 */
public class V2SerializerMap extends V1SerializerMap {

        public static final V2SerializerMap SPARSE_MAP = new V2SerializerMap(true);
		public static final V2SerializerMap DENSE_MAP = new V2SerializerMap(false);

		public static final String VERSION = "2.0";

		public static V2SerializerMap instance(boolean sparse){
				return sparse? SPARSE_MAP: DENSE_MAP;
		}

		public V2SerializerMap(boolean sparse) {
				super(sparse);
		}

		@Override
		protected void populateFactories(boolean sparse) {
				factories[0]  = NullDescriptorSerializer.nullFactory(BooleanDescriptorSerializer.INSTANCE_FACTORY,sparse);
				factories[1]  = NullDescriptorSerializer.nullFactory(ScalarDescriptorSerializer.INSTANCE_FACTORY,sparse);
				factories[2]  = NullDescriptorSerializer.floatChecker(RealDescriptorSerializer.INSTANCE_FACTORY, sparse);
				factories[3]  = NullDescriptorSerializer.doubleChecker(DoubleDescriptorSerializer.INSTANCE_FACTORY, sparse);
				factories[4]  = NullDescriptorSerializer.nullFactory(StringDescriptorSerializer.INSTANCE_FACTORY, sparse);
				factories[5]  = NullDescriptorSerializer.nullFactory(KryoDescriptorSerializer.newFactory(SpliceKryoRegistry.getInstance()),sparse);
				factories[6]  = NullDescriptorSerializer.nullFactory(DateDescriptorSerializer.INSTANCE_FACTORY,sparse);
				factories[7]  = NullDescriptorSerializer.nullFactory(TimeDescriptorSerializer.INSTANCE_FACTORY,sparse);
				factories[8]  = NullDescriptorSerializer.nullFactory(TimestampV2DescriptorSerializer.INSTANCE_FACTORY,sparse);
				factories[9]  = NullDescriptorSerializer.nullFactory(UnsortedBinaryDescriptorSerializer.INSTANCE_FACTORY,sparse);
				factories[10] = NullDescriptorSerializer.nullFactory(DecimalDescriptorSerializer.INSTANCE_FACTORY,sparse);
				factories[11] = NullDescriptorSerializer.nullFactory(RefDescriptorSerializer.INSTANCE_FACTORY, sparse);
				factories[12] = NullDescriptorSerializer.nullFactory(UDTDescriptorSerializer.INSTANCE_FACTORY, sparse);

			    eagerFactories[0]  = NullDescriptorSerializer.nullFactory(BooleanDescriptorSerializer.INSTANCE_FACTORY,sparse);
				eagerFactories[1]  = NullDescriptorSerializer.nullFactory(ScalarDescriptorSerializer.INSTANCE_FACTORY,sparse);
				eagerFactories[2]  = NullDescriptorSerializer.floatChecker(RealDescriptorSerializer.INSTANCE_FACTORY, sparse);
				eagerFactories[3]  = NullDescriptorSerializer.doubleChecker(DoubleDescriptorSerializer.INSTANCE_FACTORY, sparse);
				eagerFactories[4]  = NullDescriptorSerializer.nullFactory(StringDescriptorSerializer.INSTANCE_FACTORY, sparse);
				eagerFactories[5]  = NullDescriptorSerializer.nullFactory(KryoDescriptorSerializer.newFactory(SpliceKryoRegistry.getInstance()),sparse);
				eagerFactories[6]  = NullDescriptorSerializer.nullFactory(DateDescriptorSerializer.INSTANCE_FACTORY,sparse);
				eagerFactories[7]  = NullDescriptorSerializer.nullFactory(TimeDescriptorSerializer.INSTANCE_FACTORY,sparse);
				eagerFactories[8]  = NullDescriptorSerializer.nullFactory(TimestampV2DescriptorSerializer.INSTANCE_FACTORY,sparse);
				eagerFactories[9]  = NullDescriptorSerializer.nullFactory(UnsortedBinaryDescriptorSerializer.INSTANCE_FACTORY,sparse);
				eagerFactories[10] = NullDescriptorSerializer.nullFactory(DecimalDescriptorSerializer.INSTANCE_FACTORY,sparse);
				eagerFactories[11] = NullDescriptorSerializer.nullFactory(RefDescriptorSerializer.INSTANCE_FACTORY,sparse);
				eagerFactories[12] = NullDescriptorSerializer.nullFactory(UDTDescriptorSerializer.INSTANCE_FACTORY, sparse);

		}
}
