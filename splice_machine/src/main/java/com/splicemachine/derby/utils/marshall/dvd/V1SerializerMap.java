/*
 * Copyright (c) 2012 - 2020 Splice Machine, Inc.
 *
 * This file is part of Splice Machine.
 * Splice Machine is free software: you can redistribute it and/or modify it under the terms of the
 * GNU Affero General Public License as published by the Free Software Foundation, either
 * version 3, or (at your option) any later version.
 * Splice Machine is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU Affero General Public License for more details.
 * You should have received a copy of the GNU Affero General Public License along with Splice Machine.
 * If not, see <http://www.gnu.org/licenses/>.
 */

package com.splicemachine.derby.utils.marshall.dvd;

import com.splicemachine.SpliceKryoRegistry;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.iapi.types.DataValueDescriptor;

/**
 * @author Scott Fines
 * Date: 4/3/14
 */
public class V1SerializerMap implements SerializerMap,TypeProvider {
		protected final DescriptorSerializer.Factory[] factories;
		protected final DescriptorSerializer.Factory[] eagerFactories;

		public static final String VERSION = "1.0";

		protected void populateFactories(boolean sparse){
				factories[0]  = NullDescriptorSerializer.nullFactory(BooleanDescriptorSerializer.INSTANCE_FACTORY,sparse);
				factories[1]  = NullDescriptorSerializer.nullFactory(ScalarDescriptorSerializer.INSTANCE_FACTORY,sparse);
				factories[2]  = NullDescriptorSerializer.floatChecker(RealDescriptorSerializer.INSTANCE_FACTORY, sparse);
				factories[3]  = NullDescriptorSerializer.doubleChecker(DoubleDescriptorSerializer.INSTANCE_FACTORY, sparse);
				factories[4]  = NullDescriptorSerializer.nullFactory(StringDescriptorSerializer.INSTANCE_FACTORY, sparse);
				factories[5]  = NullDescriptorSerializer.nullFactory(KryoDescriptorSerializer.newFactory(SpliceKryoRegistry.getInstance()),sparse);
				factories[6]  = NullDescriptorSerializer.nullFactory(DateDescriptorSerializer.INSTANCE_FACTORY,sparse);
				factories[7]  = NullDescriptorSerializer.nullFactory(TimeDescriptorSerializer.INSTANCE_FACTORY,sparse);
				factories[8]  = NullDescriptorSerializer.nullFactory(TimestampV1DescriptorSerializer.INSTANCE_FACTORY,sparse);
				factories[9]  = NullDescriptorSerializer.nullFactory(UnsortedBinaryDescriptorSerializer.INSTANCE_FACTORY,sparse);
				factories[10] = NullDescriptorSerializer.nullFactory(DecimalDescriptorSerializer.INSTANCE_FACTORY,sparse);
				factories[11] = NullDescriptorSerializer.nullFactory(RefDescriptorSerializer.INSTANCE_FACTORY, sparse);
				factories[12] = NullDescriptorSerializer.nullFactory(UDTDescriptorSerializer.INSTANCE_FACTORY, sparse);
				eagerFactories[0]  = NullDescriptorSerializer.nullFactory(BooleanDescriptorSerializer.INSTANCE_FACTORY, sparse);
				eagerFactories[1]  = NullDescriptorSerializer.nullFactory(ScalarDescriptorSerializer.INSTANCE_FACTORY,sparse);
				eagerFactories[2]  = NullDescriptorSerializer.floatChecker(RealDescriptorSerializer.INSTANCE_FACTORY, sparse);
				eagerFactories[3]  = NullDescriptorSerializer.doubleChecker(DoubleDescriptorSerializer.INSTANCE_FACTORY, sparse);
				eagerFactories[4]  = NullDescriptorSerializer.nullFactory(StringDescriptorSerializer.INSTANCE_FACTORY, sparse);
				eagerFactories[5]  = NullDescriptorSerializer.nullFactory(KryoDescriptorSerializer.newFactory(SpliceKryoRegistry.getInstance()),sparse);
				eagerFactories[6]  = NullDescriptorSerializer.nullFactory(DateDescriptorSerializer.INSTANCE_FACTORY,sparse);
				eagerFactories[7]  = NullDescriptorSerializer.nullFactory(TimeDescriptorSerializer.INSTANCE_FACTORY,sparse);
				eagerFactories[8]  = NullDescriptorSerializer.nullFactory(TimestampV1DescriptorSerializer.INSTANCE_FACTORY,sparse);
				eagerFactories[9]  = NullDescriptorSerializer.nullFactory(UnsortedBinaryDescriptorSerializer.INSTANCE_FACTORY,sparse);
				eagerFactories[10] = NullDescriptorSerializer.nullFactory(DecimalDescriptorSerializer.INSTANCE_FACTORY,sparse);
				eagerFactories[11] = NullDescriptorSerializer.nullFactory(RefDescriptorSerializer.INSTANCE_FACTORY,sparse);
			    eagerFactories[12] = NullDescriptorSerializer.nullFactory(UDTDescriptorSerializer.INSTANCE_FACTORY, sparse);
		}

		public static final V1SerializerMap SPARSE_MAP = new V1SerializerMap(true);
		public static final V1SerializerMap DENSE_MAP = new V1SerializerMap(false);

		public static V1SerializerMap instance(boolean sparse){
				return sparse? SPARSE_MAP: DENSE_MAP;
		}

		public V1SerializerMap(boolean sparse) {
				this.factories = new DescriptorSerializer.Factory[13];
				this.eagerFactories = new DescriptorSerializer.Factory[13];
				populateFactories(sparse);
		}

		@Override
		public DescriptorSerializer getSerializer(DataValueDescriptor dvd) {
				if(dvd==null) return new NullDescriptorSerializer(null,true);
                for(DescriptorSerializer.Factory factory: factories){
                    if(factory.applies(dvd))
                        return factory.newInstance();
                }
                throw new IllegalStateException("Unable to find serializer for type format id "+ dvd.getTypeFormatId());
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

		@Override
		public DescriptorSerializer[] getSerializers(int[] typeFormatIds) {
				DescriptorSerializer[] serializers = new DescriptorSerializer[typeFormatIds.length];
				for(int i=0;i<typeFormatIds.length;i++){
						serializers[i] = getSerializer(typeFormatIds[i]);
				}
				return serializers;
		}

		@Override
		public boolean isScalar(int typeFormatId) {
				for(DescriptorSerializer.Factory factory:eagerFactories){
						if(factory.applies(typeFormatId))
								return factory.isScalar();
				}
				throw new IllegalStateException("Unable to find serializer for type format id "+ typeFormatId);
		}

		@Override
		public boolean isFloat(int typeFormatId) {
				for(DescriptorSerializer.Factory factory:eagerFactories){
						if(factory.applies(typeFormatId))
								return factory.isFloat();
				}
				throw new IllegalStateException("Unable to find serializer for type format id "+ typeFormatId);
		}

		@Override
		public boolean isDouble(int typeFormatId) {
				for(DescriptorSerializer.Factory factory:eagerFactories){
						if(factory.applies(typeFormatId))
								return factory.isDouble();
				}
				throw new IllegalStateException("Unable to find serializer for type format id "+ typeFormatId);
		}
}
