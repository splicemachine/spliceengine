package com.splicemachine.derby.utils.marshall.dvd;

import com.splicemachine.SpliceKryoRegistry;

/**
 * @author Scott Fines
 *         Date: 4/7/14
 */
public class V2SerializerMap extends V1SerializerMap {
		public V2SerializerMap(boolean sparse) {
				super(sparse);
		}


		public static final V2SerializerMap SPARSE_MAP = new V2SerializerMap(true);
		public static final V2SerializerMap DENSE_MAP = new V2SerializerMap(false);

		private static final String tableVersion = "2.0";

		public static V2SerializerMap instance(boolean sparse){
				return sparse? SPARSE_MAP: DENSE_MAP;
		}

		@Override
		protected void populateFactories(boolean sparse) {
				factories[0]  = NullDescriptorSerializer.nullFactory(BooleanDescriptorSerializer.INSTANCE_FACTORY,sparse);
				factories[1]  = NullDescriptorSerializer.nullFactory(LazyDescriptorSerializer.singletonFactory(ScalarDescriptorSerializer.INSTANCE_FACTORY, tableVersion),sparse);
				factories[2]  = NullDescriptorSerializer.floatChecker(RealDescriptorSerializer.INSTANCE_FACTORY,sparse);
				factories[3]  = NullDescriptorSerializer.doubleChecker(LazyDescriptorSerializer.singletonFactory(DoubleDescriptorSerializer.INSTANCE_FACTORY, tableVersion),sparse);
				factories[4]  = NullDescriptorSerializer.nullFactory(LazyDescriptorSerializer.singletonFactory(StringDescriptorSerializer.INSTANCE_FACTORY, tableVersion),sparse);
				factories[5]  = NullDescriptorSerializer.nullFactory(KryoDescriptorSerializer.newFactory(SpliceKryoRegistry.getInstance()),sparse);
				factories[6]  = NullDescriptorSerializer.nullFactory(DateDescriptorSerializer.INSTANCE_FACTORY,sparse);
				factories[7]  = NullDescriptorSerializer.nullFactory(TimeDescriptorSerializer.INSTANCE_FACTORY,sparse);
				factories[8]  = NullDescriptorSerializer.nullFactory(LazyTimeValuedSerializer.newFactory(TimestampV2DescriptorSerializer.INSTANCE_FACTORY,tableVersion),sparse);
				factories[9]  = NullDescriptorSerializer.nullFactory(SortedBinaryDescriptorSerializer.INSTANCE_FACTORY,sparse);
				factories[10] = NullDescriptorSerializer.nullFactory(LazyDescriptorSerializer.singletonFactory(DecimalDescriptorSerializer.INSTANCE_FACTORY,tableVersion),sparse);

				eagerFactories[0]  = NullDescriptorSerializer.nullFactory(BooleanDescriptorSerializer.INSTANCE_FACTORY,sparse);
				eagerFactories[1]  = NullDescriptorSerializer.nullFactory(ScalarDescriptorSerializer.INSTANCE_FACTORY,sparse);
				eagerFactories[2]  = NullDescriptorSerializer.floatChecker(RealDescriptorSerializer.INSTANCE_FACTORY,sparse);
				eagerFactories[3]  = NullDescriptorSerializer.doubleChecker(DoubleDescriptorSerializer.INSTANCE_FACTORY,sparse);
				eagerFactories[4]  = NullDescriptorSerializer.nullFactory(StringDescriptorSerializer.INSTANCE_FACTORY,sparse);
				eagerFactories[5]  = NullDescriptorSerializer.nullFactory(KryoDescriptorSerializer.newFactory(SpliceKryoRegistry.getInstance()),sparse);
				eagerFactories[6]  = NullDescriptorSerializer.nullFactory(DateDescriptorSerializer.INSTANCE_FACTORY,sparse);
				eagerFactories[7]  = NullDescriptorSerializer.nullFactory(TimeDescriptorSerializer.INSTANCE_FACTORY,sparse);
				eagerFactories[8]  = NullDescriptorSerializer.nullFactory(TimestampV2DescriptorSerializer.INSTANCE_FACTORY,sparse);
				eagerFactories[9]  = NullDescriptorSerializer.nullFactory(SortedBinaryDescriptorSerializer.INSTANCE_FACTORY,sparse);
				eagerFactories[10] = NullDescriptorSerializer.nullFactory(DecimalDescriptorSerializer.INSTANCE_FACTORY,sparse);
		}
}
