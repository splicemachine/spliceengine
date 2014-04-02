package com.splicemachine.derby.utils.marshall.dvd;

import com.splicemachine.SpliceKryoRegistry;
import org.apache.derby.iapi.sql.execute.ExecRow;
import org.apache.derby.iapi.types.DataValueDescriptor;

/**
 * @author Scott Fines
 * Date: 4/3/14
 */
public class V1SerializerMap implements SerializerMap {
		private final DescriptorSerializer.Factory[] v1Factories;

		private void populateFactories(boolean sparse){
				v1Factories[0]  = NullDescriptorSerializer.nullFactory(BooleanDescriptorSerializer.INSTANCE_FACTORY,sparse);
				v1Factories[1]  = NullDescriptorSerializer.nullFactory(LazyDescriptorSerializer.singletonFactory(ScalarDescriptorSerializer.INSTANCE_FACTORY),sparse);
				v1Factories[2]  = NullDescriptorSerializer.floatChecker(RealDescriptorSerializer.INSTANCE_FACTORY,sparse);
				v1Factories[3]  = NullDescriptorSerializer.doubleChecker(LazyDescriptorSerializer.singletonFactory(DoubleDescriptorSerializer.INSTANCE_FACTORY),sparse);
				v1Factories[4]  = NullDescriptorSerializer.nullFactory(LazyDescriptorSerializer.singletonFactory(StringDescriptorSerializer.INSTANCE_FACTORY),sparse);
				v1Factories[5]  = NullDescriptorSerializer.nullFactory(KryoDescriptorSerializer.newFactory(SpliceKryoRegistry.getInstance()),sparse);
				v1Factories[6]  = NullDescriptorSerializer.nullFactory(DateDescriptorSerializer.INSTANCE_FACTORY,sparse);
				v1Factories[7]  = NullDescriptorSerializer.nullFactory(TimeDescriptorSerializer.INSTANCE_FACTORY,sparse);
				v1Factories[8]  = NullDescriptorSerializer.nullFactory(LazyTimeValuedSerializer.newFactory(TimestampV1DescriptorSerializer.INSTANCE_FACTORY),sparse);
				v1Factories[9]  = NullDescriptorSerializer.nullFactory(SortedBinaryDescriptorSerializer.INSTANCE_FACTORY,sparse);
				v1Factories[10] = NullDescriptorSerializer.nullFactory(LazyDescriptorSerializer.singletonFactory(DecimalDescriptorSerializer.INSTANCE_FACTORY),sparse);
		}

		public static final SerializerMap SPARSE_MAP = new V1SerializerMap(true);
		public static final SerializerMap DENSE_MAP = new V1SerializerMap(false);

		public static SerializerMap instance(boolean sparse){
				return sparse? SPARSE_MAP: DENSE_MAP;
		}

		public V1SerializerMap(boolean sparse) {
				this.v1Factories = new DescriptorSerializer.Factory[11];
				populateFactories(sparse);
		}

		@Override
		public DescriptorSerializer getSerializer(DataValueDescriptor dvd) {
				if(dvd==null) return new NullDescriptorSerializer(null,true);
				return getSerializer(dvd.getTypeFormatId());
		}

		@Override
		public DescriptorSerializer getSerializer(int typeFormatId) {
				for(DescriptorSerializer.Factory factory:v1Factories){
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
}
