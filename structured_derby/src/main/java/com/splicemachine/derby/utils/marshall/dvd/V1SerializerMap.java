package com.splicemachine.derby.utils.marshall.dvd;

import com.splicemachine.SpliceKryoRegistry;
import org.apache.derby.iapi.sql.execute.ExecRow;
import org.apache.derby.iapi.types.DataValueDescriptor;

/**
 * @author Scott Fines
 * Date: 4/3/14
 */
public class V1SerializerMap implements SerializerMap {
		protected final DescriptorSerializer.Factory[] factories;
		protected final DescriptorSerializer.Factory[] eagerFactories;

		private static final String version = "1.0";

		protected void populateFactories(boolean sparse){
				factories[0]  = NullDescriptorSerializer.nullFactory(BooleanDescriptorSerializer.INSTANCE_FACTORY,sparse);
				factories[1]  = NullDescriptorSerializer.nullFactory(LazyDescriptorSerializer.singletonFactory(ScalarDescriptorSerializer.INSTANCE_FACTORY,version),sparse);
				factories[2]  = NullDescriptorSerializer.floatChecker(RealDescriptorSerializer.INSTANCE_FACTORY,sparse);
				factories[3]  = NullDescriptorSerializer.doubleChecker(LazyDescriptorSerializer.singletonFactory(DoubleDescriptorSerializer.INSTANCE_FACTORY,version),sparse);
				factories[4]  = NullDescriptorSerializer.nullFactory(LazyDescriptorSerializer.singletonFactory(StringDescriptorSerializer.INSTANCE_FACTORY,version),sparse);
				factories[5]  = NullDescriptorSerializer.nullFactory(KryoDescriptorSerializer.newFactory(SpliceKryoRegistry.getInstance()),sparse);
				factories[6]  = NullDescriptorSerializer.nullFactory(DateDescriptorSerializer.INSTANCE_FACTORY,sparse);
				factories[7]  = NullDescriptorSerializer.nullFactory(TimeDescriptorSerializer.INSTANCE_FACTORY,sparse);
				factories[8]  = NullDescriptorSerializer.nullFactory(LazyTimeValuedSerializer.newFactory(TimestampV1DescriptorSerializer.INSTANCE_FACTORY,version),sparse);
				factories[9]  = NullDescriptorSerializer.nullFactory(SortedBinaryDescriptorSerializer.INSTANCE_FACTORY,sparse);
				factories[10] = NullDescriptorSerializer.nullFactory(LazyDescriptorSerializer.singletonFactory(DecimalDescriptorSerializer.INSTANCE_FACTORY,version),sparse);

				eagerFactories[0]  = NullDescriptorSerializer.nullFactory(BooleanDescriptorSerializer.INSTANCE_FACTORY,sparse);
				eagerFactories[1]  = NullDescriptorSerializer.nullFactory(ScalarDescriptorSerializer.INSTANCE_FACTORY,sparse);
				eagerFactories[2]  = NullDescriptorSerializer.floatChecker(RealDescriptorSerializer.INSTANCE_FACTORY,sparse);
				eagerFactories[3]  = NullDescriptorSerializer.doubleChecker(DoubleDescriptorSerializer.INSTANCE_FACTORY,sparse);
				eagerFactories[4]  = NullDescriptorSerializer.nullFactory(StringDescriptorSerializer.INSTANCE_FACTORY,sparse);
				eagerFactories[5]  = NullDescriptorSerializer.nullFactory(KryoDescriptorSerializer.newFactory(SpliceKryoRegistry.getInstance()),sparse);
				eagerFactories[6]  = NullDescriptorSerializer.nullFactory(DateDescriptorSerializer.INSTANCE_FACTORY,sparse);
				eagerFactories[7]  = NullDescriptorSerializer.nullFactory(TimeDescriptorSerializer.INSTANCE_FACTORY,sparse);
				eagerFactories[8]  = NullDescriptorSerializer.nullFactory(TimestampV1DescriptorSerializer.INSTANCE_FACTORY,sparse);
				eagerFactories[9]  = NullDescriptorSerializer.nullFactory(SortedBinaryDescriptorSerializer.INSTANCE_FACTORY,sparse);
				eagerFactories[10] = NullDescriptorSerializer.nullFactory(DecimalDescriptorSerializer.INSTANCE_FACTORY,sparse);
		}

		public static final SerializerMap SPARSE_MAP = new V1SerializerMap(true);
		public static final SerializerMap DENSE_MAP = new V1SerializerMap(false);

		public static SerializerMap instance(boolean sparse){
				return sparse? SPARSE_MAP: DENSE_MAP;
		}

		public V1SerializerMap(boolean sparse) {
				this.factories = new DescriptorSerializer.Factory[11];
				this.eagerFactories = new DescriptorSerializer.Factory[11];
				populateFactories(sparse);
		}

		@Override
		public DescriptorSerializer getSerializer(DataValueDescriptor dvd) {
				if(dvd==null) return new NullDescriptorSerializer(null,true);
				return getSerializer(dvd.getTypeFormatId());
		}

		@Override
		public DescriptorSerializer getSerializer(int typeFormatId) {
				for(DescriptorSerializer.Factory factory: factories){
						if(factory.applies(typeFormatId))
								return factory.newInstance();
				}
				throw new IllegalStateException("Unable to find serializer for type format id "+ typeFormatId);
		}

		@Override
		public DescriptorSerializer[] getSerializers(ExecRow row) {
				assert row!=null :"Cannot get serializers for null array!";
				DataValueDescriptor[] dvds = row.getRowArray();
				return getSerializers(dvds);
		}

		@Override
		public DescriptorSerializer[] getSerializers(DataValueDescriptor[] dvds) {
				DescriptorSerializer[] serializers = new DescriptorSerializer[dvds.length];
				for(int i=0;i<dvds.length;i++){
						serializers[i] = getSerializer(dvds[i]);
				}
				return serializers;
		}

		@Override
		public DescriptorSerializer getEagerSerializer(int typeFormatId) {
				for(DescriptorSerializer.Factory factory: eagerFactories){
						if(factory.applies(typeFormatId))
								return factory.newInstance();
				}
				throw new IllegalStateException("Unable to find serializer for type format id "+ typeFormatId);
		}
}
