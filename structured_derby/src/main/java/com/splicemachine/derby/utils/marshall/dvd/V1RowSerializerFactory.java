package com.splicemachine.derby.utils.marshall.dvd;

import com.splicemachine.SpliceKryoRegistry;
import org.apache.derby.iapi.sql.execute.ExecRow;
import org.apache.derby.iapi.types.DataValueDescriptor;

/**
 * Represents the encoding used for V1 tables.
 *
 * @author Scott Fines
 * Date: 4/2/14
 */
public class V1RowSerializerFactory implements RowSerializer.Factory{

		private final DescriptorSerializer.Factory[] v1Factories;

		private static final String version = "1.0";
		private void populateFactories(boolean sparse){
				v1Factories[0]  = NullDescriptorSerializer.nullFactory(BooleanDescriptorSerializer.INSTANCE_FACTORY,sparse);
				v1Factories[1]  = NullDescriptorSerializer.nullFactory(LazyDescriptorSerializer.singletonFactory(ScalarDescriptorSerializer.INSTANCE_FACTORY,version),sparse);
				v1Factories[2]  = NullDescriptorSerializer.floatChecker(RealDescriptorSerializer.INSTANCE_FACTORY,sparse);
				v1Factories[3]  = NullDescriptorSerializer.doubleChecker(LazyDescriptorSerializer.singletonFactory(DoubleDescriptorSerializer.INSTANCE_FACTORY,version),sparse);
				v1Factories[4]  = NullDescriptorSerializer.nullFactory(LazyDescriptorSerializer.singletonFactory(StringDescriptorSerializer.INSTANCE_FACTORY,version),sparse);
				v1Factories[5]  = NullDescriptorSerializer.nullFactory(KryoDescriptorSerializer.newFactory(SpliceKryoRegistry.getInstance()),sparse);
				v1Factories[6]  = NullDescriptorSerializer.nullFactory(DateDescriptorSerializer.INSTANCE_FACTORY,sparse);
				v1Factories[7]  = NullDescriptorSerializer.nullFactory(TimeDescriptorSerializer.INSTANCE_FACTORY,sparse);
				v1Factories[8]  = NullDescriptorSerializer.nullFactory(LazyTimeValuedSerializer.newFactory(TimestampV1DescriptorSerializer.INSTANCE_FACTORY,version),sparse);
				v1Factories[9]  = NullDescriptorSerializer.nullFactory(SortedBinaryDescriptorSerializer.INSTANCE_FACTORY,sparse);
				v1Factories[10] = NullDescriptorSerializer.nullFactory(LazyDescriptorSerializer.singletonFactory(DecimalDescriptorSerializer.INSTANCE_FACTORY,version),sparse);
		}

		public static final RowSerializer.Factory SPARSE_FACTORY = new V1RowSerializerFactory(true);
		public static final RowSerializer.Factory DENSE_FACTORY = new V1RowSerializerFactory(false);

		public static RowSerializer.Factory instance(boolean sparse){
				return sparse? SPARSE_FACTORY: DENSE_FACTORY;
		}

		private V1RowSerializerFactory(boolean sparse){
				v1Factories = new DescriptorSerializer.Factory[23];
				populateFactories(sparse);
		}

		@Override
		public RowSerializer forRow(ExecRow row, int[] columnMap, boolean[] columnSortOrder) {
				DescriptorSerializer[] serializers = getDescriptors(row);
				return new RowSerializer(serializers,columnMap,columnSortOrder);
		}


		@Override
		public EntryRowSerializer entryRow(ExecRow template, int[] keyColumns, boolean[] keySortOrder) {
				return new EntryRowSerializer(getDescriptors(template),keyColumns,keySortOrder);
		}


		@Override
		public DescriptorSerializer[] getDescriptors(ExecRow row) {
				DataValueDescriptor[] dvds = row.getRowArray();
				DescriptorSerializer[] serializers = new DescriptorSerializer[dvds.length];
				for(int i=0;i<dvds.length;i++){
						serializers[i] = getSerializer(dvds[i]);
				}
				return serializers;
		}

		private DescriptorSerializer getSerializer(DataValueDescriptor dvd) {
				if(dvd==null) return new NullDescriptorSerializer(null,true);
				int typeFormatId = dvd.getTypeFormatId();
				for(DescriptorSerializer.Factory serializerFactory:v1Factories){
					if(serializerFactory.applies(typeFormatId))
							return serializerFactory.newInstance();
				}
				//we couldn't find a serializer! Programmer error, blow up
				throw new IllegalStateException("Unable to find serializer for type format id "+ typeFormatId);
		}
}
