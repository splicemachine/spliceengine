/*
 * Copyright (c) 2012 - 2019 Splice Machine, Inc.
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

package com.splicemachine.derby.impl;

import com.carrotsearch.hppc.BitSet;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.esotericsoftware.kryo.serializers.DefaultSerializers;
import com.esotericsoftware.kryo.serializers.FieldSerializer;
import com.esotericsoftware.kryo.serializers.MapSerializer;
import com.google.common.collect.ArrayListMultimap;
import com.splicemachine.EngineDriver;
import com.splicemachine.SpliceKryoRegistry;
import com.splicemachine.db.catalog.types.*;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.services.io.*;
import com.splicemachine.db.iapi.sql.dictionary.IndexRowGenerator;
import com.splicemachine.db.iapi.sql.dictionary.SchemaDescriptor;
import com.splicemachine.db.iapi.stats.ColumnStatisticsImpl;
import com.splicemachine.db.iapi.stats.ColumnStatisticsMerge;
import com.splicemachine.db.iapi.stats.FakeColumnStatisticsImpl;
import com.splicemachine.db.iapi.types.*;
import com.splicemachine.db.impl.services.uuid.BasicUUID;
import com.splicemachine.db.impl.sql.*;
import com.splicemachine.db.impl.sql.catalog.DDColumnDependableFinder;
import com.splicemachine.db.impl.sql.catalog.DD_Version;
import com.splicemachine.db.impl.sql.catalog.DDdependableFinder;
import com.splicemachine.db.impl.sql.catalog.ManagedCache;
import com.splicemachine.db.impl.sql.compile.*;
import com.splicemachine.db.impl.sql.execute.*;
import com.splicemachine.db.impl.store.access.PC_XenaVersion;
import com.splicemachine.derby.ddl.*;
import com.splicemachine.derby.impl.kryo.SparkValueRowSerializer;
import com.splicemachine.derby.impl.sql.execute.actions.DeleteConstantOperation;
import com.splicemachine.derby.impl.sql.execute.actions.InsertConstantOperation;
import com.splicemachine.derby.impl.sql.execute.actions.UpdateConstantOperation;
import com.splicemachine.derby.impl.sql.execute.operations.*;
import com.splicemachine.derby.impl.sql.execute.operations.export.ExportOperation;
import com.splicemachine.derby.impl.sql.execute.operations.framework.DerbyAggregateContext;
import com.splicemachine.derby.impl.sql.execute.operations.groupedaggregate.DerbyGroupedAggregateContext;
import com.splicemachine.derby.impl.store.access.btree.IndexConglomerate;
import com.splicemachine.derby.impl.store.access.hbase.HBaseConglomerate;
import com.splicemachine.derby.serialization.ActivationSerializer;
import com.splicemachine.derby.serialization.SpliceObserverInstructions;
import com.splicemachine.derby.stream.ActivationHolder;
import com.splicemachine.derby.stream.control.BadRecordsRecorder;
import com.splicemachine.derby.stream.function.*;
import com.splicemachine.derby.stream.function.broadcast.AbstractBroadcastJoinFlatMapFunction;
import com.splicemachine.derby.stream.spark.*;
import com.splicemachine.derby.utils.kryo.DataValueDescriptorSerializer;
import com.splicemachine.derby.utils.kryo.ListDataTypeSerializer;
import com.splicemachine.derby.utils.kryo.SimpleObjectSerializer;
import com.splicemachine.kvpair.KVPair;
import com.splicemachine.pipeline.client.BulkWrite;
import com.splicemachine.stream.ResultStreamer;
import com.splicemachine.stream.StreamProtocol;
import com.splicemachine.utils.ByteSlice;
import com.splicemachine.utils.kryo.ExternalizableSerializer;
import com.splicemachine.utils.kryo.KryoObjectInput;
import com.splicemachine.utils.kryo.KryoObjectOutput;
import com.splicemachine.utils.kryo.KryoPool;
import de.javakaffee.kryoserializers.ArraysAsListSerializer;
import de.javakaffee.kryoserializers.UUIDSerializer;
import de.javakaffee.kryoserializers.UnmodifiableCollectionsSerializer;
import org.apache.commons.lang3.mutable.MutableDouble;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.serializer.KryoRegistrator;
import org.spark_project.guava.base.Optional;

import java.io.IOException;
import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.*;

import static com.splicemachine.SpliceKryoRegistry.getClassFromString;

/**
 *
 * Maps serializable entities to a Kryo Serializer, so that
 * they may be more efficiently serialized than raw Java serialization.
 *
 * https://github.com/EsotericSoftware/kryo
 *
 * This registry differs from SpliceKryoRegistry in only being used for
 * transient data serialization. This means we can evolve the serialization
 * format without problems.
 *
 * @author Scott Fines
 * Created on: 8/15/13
 */
public class SpliceSparkKryoRegistrator implements KryoRegistrator, KryoPool.KryoRegistry {
    //ExternalizableSerializers are stateless, no need to create more than we need
    private static final ExternalizableSerializer EXTERNALIZABLE_SERIALIZER = ExternalizableSerializer.INSTANCE;
    private static final UnmodifiableCollectionsSerializer UNMODIFIABLE_COLLECTIONS_SERIALIZER = new UnmodifiableCollectionsSerializer();

    private static volatile KryoPool spliceKryoPool;


    public static KryoPool getInstance(){
        KryoPool kp = spliceKryoPool;
        if(kp==null){
            synchronized(SpliceKryoRegistry.class){
                kp = spliceKryoPool;
                if(kp==null){
                    EngineDriver driver = EngineDriver.driver();
                    if(driver==null){
                        kp = new KryoPool(1000);
                    }else{
                        int kpSize = driver.getConfiguration().getKryoPoolSize();
                        kp = spliceKryoPool = new KryoPool(kpSize);
                    }
                    kp.setKryoRegistry(new SpliceSparkKryoRegistrator());
                }
            }
        }
        return kp;
    }

    @Override
    public void register(Kryo instance) {
        registerClasses(instance);
    }

    @Override
    public void registerClasses(Kryo instance) {
//    	instance.setReferences(false);
        instance.setRegistrationRequired(false); //TODO set to true

        instance.register(ValueRow.class, new SparkValueRowSerializer<ValueRow>(){
            @Override
            protected ValueRow newType(int size) {
                return new ValueRow(size);
            }
        });
        instance.register(IndexValueRow.class, new SparkValueRowSerializer<IndexValueRow>(){
            @Override
            protected IndexValueRow newType(int size) {
                return new IndexValueRow(new ValueRow(size));
            }
        });
        instance.register(GenericStorablePreparedStatement.class,EXTERNALIZABLE_SERIALIZER);
        instance.register(DataTypeDescriptor.class,EXTERNALIZABLE_SERIALIZER);
        instance.register(TypeDescriptorImpl.class,EXTERNALIZABLE_SERIALIZER);
        instance.register(BaseTypeIdImpl.class,EXTERNALIZABLE_SERIALIZER);
        instance.register(UserDefinedTypeIdImpl.class,EXTERNALIZABLE_SERIALIZER);
        instance.register(DecimalTypeIdImpl.class,EXTERNALIZABLE_SERIALIZER);
        instance.register(RowMultiSetImpl.class,EXTERNALIZABLE_SERIALIZER);
        instance.register(RoutineAliasInfo.class,EXTERNALIZABLE_SERIALIZER);
        instance.register(SparkOperationContext.class,EXTERNALIZABLE_SERIALIZER);

        instance.register(IndexConglomerate.class,EXTERNALIZABLE_SERIALIZER);
        instance.register(HBaseConglomerate.class,EXTERNALIZABLE_SERIALIZER);

        instance.register(FormatableBitSet.class,EXTERNALIZABLE_SERIALIZER);

        instance.register(CountAggregator.class,EXTERNALIZABLE_SERIALIZER);
        instance.register(SumAggregator.class,EXTERNALIZABLE_SERIALIZER);
        instance.register(AvgAggregator.class,EXTERNALIZABLE_SERIALIZER);
        instance.register(MaxMinAggregator.class,EXTERNALIZABLE_SERIALIZER);

        instance.register(SQLDecimal.class,new DataValueDescriptorSerializer<SQLDecimal>(){
            @Override
            protected void writeValue(Kryo kryo, Output output, SQLDecimal object) throws StandardException {
                kryo.writeObjectOrNull(output, object.getObject(), BigDecimal.class);
            }
            @Override
            protected void readValue(Kryo kryo, Input input, SQLDecimal dvd) throws StandardException {
                dvd.setBigDecimal(kryo.readObjectOrNull(input, BigDecimal.class));
            }
        });
        instance.register(SQLDouble.class,new DataValueDescriptorSerializer<SQLDouble>(){
            @Override
            protected void writeValue(Kryo kryo, Output output, SQLDouble object) throws StandardException {
                output.writeDouble(object.getDouble());
            }
            @Override
            protected void readValue(Kryo kryo, Input input, SQLDouble dvd) throws StandardException {
                dvd.setValue(input.readDouble());
            }
        });
        instance.register(SQLInteger.class,new DataValueDescriptorSerializer<SQLInteger>() {
            @Override
            protected void writeValue(Kryo kryo, Output output, SQLInteger object) {
                output.writeInt(object.getInt());
            }
            @Override
            protected void readValue(Kryo kryo, Input input, SQLInteger dvd) {
                dvd.setValue(input.readInt());
            }
        });
        instance.register(SQLVarchar.class,new DataValueDescriptorSerializer<SQLVarchar>() {
            @Override
            protected void writeValue(Kryo kryo, Output output, SQLVarchar object) throws StandardException {
                output.writeString(object.getString());
            }
            @Override
            protected void readValue(Kryo kryo, Input input, SQLVarchar dvd) {
                dvd.setValue(input.readString());
            }
        });
        instance.register(SQLLongvarchar.class,new DataValueDescriptorSerializer<SQLLongvarchar>() {
            @Override
            protected void writeValue(Kryo kryo, Output output, SQLLongvarchar object) throws StandardException {
                output.writeString(object.getString());
            }
            @Override
            protected void readValue(Kryo kryo, Input input, SQLLongvarchar dvd) {
                dvd.setValue(input.readString());
            }
        });
        instance.register(SQLChar.class,new DataValueDescriptorSerializer<SQLChar>() {
            @Override
            protected void writeValue(Kryo kryo, Output output, SQLChar object) throws StandardException {
                output.writeString(object.getString());
            }
            @Override
            protected void readValue(Kryo kryo, Input input, SQLChar dvd) {
                dvd.setValue(input.readString());
            }
        });
        instance.register(SQLBoolean.class,new DataValueDescriptorSerializer<SQLBoolean>() {
            @Override
            protected void writeValue(Kryo kryo, Output output, SQLBoolean object) throws StandardException {
                output.writeBoolean(object.getBoolean());
            }

            @Override
            protected void readValue(Kryo kryo, Input input, SQLBoolean dvd) throws StandardException {
                dvd.setValue(input.readBoolean());
            }
        });
        instance.register(SQLBit.class,new DataValueDescriptorSerializer<SQLBit>() {
            @Override
            protected void writeValue(Kryo kryo, Output output, SQLBit object) throws StandardException {
                byte[] data = object.getBytes();
                output.writeInt(data.length);
                output.write(data);
            }

            @Override
            protected void readValue(Kryo kryo, Input input, SQLBit dvd) throws StandardException {
                byte[] data = new byte[input.readInt()];
                //noinspection ResultOfMethodCallIgnored
                input.read(data);
                dvd.setValue(data);
            }
        });
        instance.register(SQLVarbit.class,new DataValueDescriptorSerializer<SQLVarbit>() {
            @Override
            protected void writeValue(Kryo kryo, Output output, SQLVarbit object) throws StandardException {
                byte[] data = object.getBytes();
                output.writeInt(data.length);
                output.write(data);
            }

            @Override
            protected void readValue(Kryo kryo, Input input, SQLVarbit dvd) throws StandardException {
                byte[] data = new byte[input.readInt()];
                //noinspection ResultOfMethodCallIgnored
                input.read(data);
                dvd.setValue(data);
            }
        });
        instance.register(SQLLongVarbit.class,new DataValueDescriptorSerializer<SQLLongVarbit>() {
            @Override
            protected void writeValue(Kryo kryo, Output output, SQLLongVarbit object) throws StandardException {
                byte[] data = object.getBytes();
                output.writeInt(data.length);
                output.write(data);
            }

            @Override
            protected void readValue(Kryo kryo, Input input, SQLLongVarbit dvd) throws StandardException {
                byte[] data = new byte[input.readInt()];
                //noinspection ResultOfMethodCallIgnored
                input.read(data);
                dvd.setValue(data);
            }
        });

        instance.register(SQLDate.class,new DataValueDescriptorSerializer<SQLDate>() {
            @Override
            protected void writeValue(Kryo kryo, Output output, SQLDate object) throws StandardException {
                output.writeLong(object.getDate((Calendar) null).getTime());
            }

            @Override
            protected void readValue(Kryo kryo, Input input, SQLDate dvd) throws StandardException {
                dvd.setValue(new Date(input.readLong()));
            }
        });
        instance.register(SQLTime.class,new DataValueDescriptorSerializer<SQLTime>(){
            @Override
            protected void writeValue(Kryo kryo, Output output, SQLTime object) throws StandardException {
                output.writeLong(object.getTime(null).getTime());
            }

            @Override
            protected void readValue(Kryo kryo, Input input, SQLTime dvd) throws StandardException {
                dvd.setValue(new Time(input.readLong()));
            }
        });
        instance.register(SQLTimestamp.class,new DataValueDescriptorSerializer<SQLTimestamp>(){
            @Override
            protected void writeValue(Kryo kryo, Output output, SQLTimestamp object) throws StandardException {
                output.writeLong(object.getTimestamp(null).getTime());
            }

            @Override
            protected void readValue(Kryo kryo, Input input, SQLTimestamp dvd) throws StandardException {
                dvd.setValue(new Timestamp(input.readLong()));
            }
        });
        instance.register(SQLSmallint.class,new DataValueDescriptorSerializer<SQLSmallint>() {
            @Override
            protected void writeValue(Kryo kryo, Output output, SQLSmallint object) throws StandardException {
                output.writeShort(object.getShort());
            }
            @Override
            protected void readValue(Kryo kryo, Input input, SQLSmallint dvd) throws StandardException {
                dvd.setValue(input.readShort());
            }
        });
        instance.register(SQLTinyint.class,new DataValueDescriptorSerializer<SQLTinyint>() {
            @Override
            protected void writeValue(Kryo kryo, Output output, SQLTinyint object) throws StandardException {
                output.writeByte(object.getByte());
            }
            @Override
            protected void readValue(Kryo kryo, Input input, SQLTinyint dvd) throws StandardException {
                dvd.setValue(input.readByte());
            }
        });

        instance.register(SQLLongint.class,new DataValueDescriptorSerializer<SQLLongint>() {
            @Override
            protected void writeValue(Kryo kryo, Output output, SQLLongint object) throws StandardException {
                output.writeLong(object.getLong());
            }
            @Override
            protected void readValue(Kryo kryo, Input input, SQLLongint dvd) throws StandardException {
                dvd.setValue(input.readLong());
            }
        });
        instance.register(SQLReal.class, new DataValueDescriptorSerializer<SQLReal>() {
            @Override
            protected void writeValue(Kryo kryo, Output output, SQLReal object) throws StandardException {
                output.writeFloat(object.getFloat());
            }

            @Override
            protected void readValue(Kryo kryo, Input input, SQLReal dvd) throws StandardException {
                dvd.setValue(input.readFloat());
            }
        });
        instance.register(SQLRef.class,EXTERNALIZABLE_SERIALIZER);
        instance.register(ListDataType.class, new ListDataTypeSerializer<ListDataType>() {
            @Override
            protected void writeValue(Kryo kryo, Output output, ListDataType object) throws StandardException {
                int forLim = object.getLength();
                output.writeInt(forLim);
                for (int i = 0; i < forLim; i++) {
                    boolean dvdPresent = (object.getDVD(i) != null);
                    output.writeBoolean(dvdPresent);
                    if (dvdPresent)
                        kryo.writeClassAndObject(output, object.getDVD(i));
                }
            }
    
            @Override
            protected void readValue(Kryo kryo, Input input, ListDataType dvd) {
                int forLim = input.readInt();
                dvd.setLength(forLim);
                for (int i = 0; i < forLim; i++) {
                    DataValueDescriptor inputDVD;
                    if (input.readBoolean()) {
                        inputDVD = (DataValueDescriptor) kryo.readClassAndObject(input);
                        try {
                            dvd.setFrom(inputDVD, i);
                        } catch (StandardException e) {
                            throw new RuntimeException(e);
                        }
                    }
                }
            }
        });
        //register Activation-related classes
        instance.register(ActivationSerializer.ArrayFieldStorage.class,EXTERNALIZABLE_SERIALIZER);
        instance.register(ActivationSerializer.DataValueStorage.class,EXTERNALIZABLE_SERIALIZER);
        instance.register(ActivationSerializer.ExecRowStorage.class,EXTERNALIZABLE_SERIALIZER);
        instance.register(ActivationSerializer.SerializableStorage.class,EXTERNALIZABLE_SERIALIZER);
        instance.register(ActivationSerializer.CachedOpFieldStorage.class,EXTERNALIZABLE_SERIALIZER);

        instance.register(SpliceObserverInstructions.class,EXTERNALIZABLE_SERIALIZER);
        instance.register(SpliceObserverInstructions.ActivationContext.class,EXTERNALIZABLE_SERIALIZER);
        instance.register(GenericParameterValueSet.class,EXTERNALIZABLE_SERIALIZER);
        instance.register(GenericParameter.class,EXTERNALIZABLE_SERIALIZER);
        instance.register(SchemaDescriptor.class,EXTERNALIZABLE_SERIALIZER);
        instance.register(ProjectRestrictOperation.class, EXTERNALIZABLE_SERIALIZER);
        instance.register(TableScanOperation.class, EXTERNALIZABLE_SERIALIZER);
        instance.register(ScanOperation.class, EXTERNALIZABLE_SERIALIZER);
        instance.register(GroupedAggregateOperation.class, EXTERNALIZABLE_SERIALIZER);
        instance.register(DistinctScanOperation.class, EXTERNALIZABLE_SERIALIZER);
        instance.register(DistinctScalarAggregateOperation.class, EXTERNALIZABLE_SERIALIZER);
        instance.register(IndexRowToBaseRowOperation.class, EXTERNALIZABLE_SERIALIZER);
        instance.register(SortOperation.class, EXTERNALIZABLE_SERIALIZER);
        instance.register(UnionOperation.class, EXTERNALIZABLE_SERIALIZER);
        instance.register(UpdateOperation.class, EXTERNALIZABLE_SERIALIZER);
        instance.register(UpdateConstantOperation.class, EXTERNALIZABLE_SERIALIZER);
        instance.register(InsertOperation.class, EXTERNALIZABLE_SERIALIZER);
        instance.register(DeleteOperation.class, EXTERNALIZABLE_SERIALIZER);
        instance.register(MergeSortJoinOperation.class, EXTERNALIZABLE_SERIALIZER);
        instance.register(NestedLoopJoinOperation.class, EXTERNALIZABLE_SERIALIZER);
        instance.register(NestedLoopLeftOuterJoinOperation.class, EXTERNALIZABLE_SERIALIZER);
        instance.register(MergeSortLeftOuterJoinOperation.class, EXTERNALIZABLE_SERIALIZER);
        instance.register(ScalarAggregateOperation.class, EXTERNALIZABLE_SERIALIZER);
        instance.register(NormalizeOperation.class, EXTERNALIZABLE_SERIALIZER);
        instance.register(AnyOperation.class, EXTERNALIZABLE_SERIALIZER);
        instance.register(RowOperation.class, EXTERNALIZABLE_SERIALIZER);
        instance.register(OnceOperation.class, EXTERNALIZABLE_SERIALIZER);
        instance.register(BroadcastJoinOperation.class, EXTERNALIZABLE_SERIALIZER);
        instance.register(BroadcastLeftOuterJoinOperation.class, EXTERNALIZABLE_SERIALIZER);
        instance.register(DerbyOperationInformation.class,EXTERNALIZABLE_SERIALIZER);
        instance.register(DerbyScanInformation.class,EXTERNALIZABLE_SERIALIZER);
        instance.register(CachedOperation.class,EXTERNALIZABLE_SERIALIZER);
        instance.register(CallStatementOperation.class,EXTERNALIZABLE_SERIALIZER);
        instance.register(ExplainOperation.class,EXTERNALIZABLE_SERIALIZER);
        instance.register(ExportOperation.class,EXTERNALIZABLE_SERIALIZER);

        instance.register(PC_XenaVersion.class,EXTERNALIZABLE_SERIALIZER);
        instance.register(BasicUUID.class,EXTERNALIZABLE_SERIALIZER);
        instance.register(UUID.class, new UUIDSerializer());
        instance.register(IndexDescriptorImpl.class,EXTERNALIZABLE_SERIALIZER);
        instance.register(FormatableHashtable.class,EXTERNALIZABLE_SERIALIZER);
        instance.register(FormatableIntHolder.class,EXTERNALIZABLE_SERIALIZER);
        instance.register(FormatableIntHolder[].class);
        instance.register(DD_Version.class,EXTERNALIZABLE_SERIALIZER);
        instance.register(DDdependableFinder.class,EXTERNALIZABLE_SERIALIZER);
        instance.register(DDColumnDependableFinder.class,EXTERNALIZABLE_SERIALIZER);
        instance.register(byte[].class);
        instance.register(boolean[].class);
        instance.register(int[].class);
        instance.register(double[].class);
        instance.register(float[].class);
        instance.register(long[].class);
        instance.register(short[].class);
        instance.register(Object[].class);
        instance.register(scala.collection.mutable.WrappedArray.ofRef.class);
        instance.register(Collections.emptyList().getClass());
        instance.register(Collections.unmodifiableList(new LinkedList()).getClass(), UNMODIFIABLE_COLLECTIONS_SERIALIZER);
        instance.register(Collections.unmodifiableList(new ArrayList()).getClass(), UNMODIFIABLE_COLLECTIONS_SERIALIZER);
        instance.register(CursorInfo.class,EXTERNALIZABLE_SERIALIZER);
        instance.register(GenericResultDescription.class,EXTERNALIZABLE_SERIALIZER);
        instance.register(GenericColumnDescriptor.class,EXTERNALIZABLE_SERIALIZER);
        instance.register(ReferencedColumnsDescriptorImpl.class,EXTERNALIZABLE_SERIALIZER);
//        instance.register(ErrorTransport.class,EXTERNALIZABLE_SERIALIZER);
        instance.register(DefaultInfoImpl.class,EXTERNALIZABLE_SERIALIZER);

        instance.register(BigDecimal.class);
        instance.register(HashMap.class);
        instance.register(TreeMap.class);
        instance.register(ArrayList.class);
        instance.register(LinkedList.class);

        instance.register(FormatableArrayHolder.class,EXTERNALIZABLE_SERIALIZER);
        instance.register(IndexColumnOrder.class,EXTERNALIZABLE_SERIALIZER);
        instance.register(HBaseRowLocation.class,EXTERNALIZABLE_SERIALIZER);
        instance.register(AggregatorInfoList.class,EXTERNALIZABLE_SERIALIZER);
        instance.register(AggregatorInfo.class,EXTERNALIZABLE_SERIALIZER);
        instance.register(UserType.class,EXTERNALIZABLE_SERIALIZER);
        instance.register(InsertConstantOperation.class,EXTERNALIZABLE_SERIALIZER);
        instance.register(DerbyDMLWriteInfo.class,EXTERNALIZABLE_SERIALIZER);
        instance.register(DeleteConstantOperation.class,EXTERNALIZABLE_SERIALIZER);
        instance.register(FormatableLongHolder.class,EXTERNALIZABLE_SERIALIZER);
        instance.register(FormatableInstanceGetter.class,EXTERNALIZABLE_SERIALIZER);
        instance.register(IndexRowGenerator.class,EXTERNALIZABLE_SERIALIZER);
        instance.register(MultiProbeDerbyScanInformation.class,EXTERNALIZABLE_SERIALIZER);
        instance.register(MultiProbeTableScanOperation.class,EXTERNALIZABLE_SERIALIZER);
        instance.register(DistinctGroupedAggregateOperation.class,EXTERNALIZABLE_SERIALIZER);
        instance.register(SQLClob.class,new DataValueDescriptorSerializer<SQLClob>() {
            @Override
            protected void writeValue(Kryo kryo, Output output, SQLClob object) throws StandardException {
                output.writeString(object.getString());
            }
            @Override
            protected void readValue(Kryo kryo, Input input, SQLClob dvd) {
                dvd.setValue(input.readString());
            }
        });
        instance.register(SQLBlob.class,new DataValueDescriptorSerializer<SQLBlob>() {
            @Override
            protected void writeValue(Kryo kryo, Output output, SQLBlob object) throws StandardException {
                byte[] data = object.getBytes();
                output.writeInt(data.length);
                output.write(data);
            }

            @Override
            protected void readValue(Kryo kryo, Input input, SQLBlob dvd) throws StandardException {
                byte[] data = new byte[input.readInt()];
                //noinspection ResultOfMethodCallIgnored
                input.read(data);
                dvd.setValue(data);
            }
        });
        instance.register(SynonymAliasInfo.class, EXTERNALIZABLE_SERIALIZER);
        instance.register(CursorTableReference.class, EXTERNALIZABLE_SERIALIZER);
        instance.register(FKInfo.class, EXTERNALIZABLE_SERIALIZER);
        instance.register(DerbyAggregateContext.class,EXTERNALIZABLE_SERIALIZER);
        instance.register(DerbyGroupedAggregateContext.class,EXTERNALIZABLE_SERIALIZER);
        instance.register(LastIndexKeyOperation.class,EXTERNALIZABLE_SERIALIZER);
        instance.register(MergeJoinOperation.class, EXTERNALIZABLE_SERIALIZER);

        instance.register(AggregateAliasInfo.class, EXTERNALIZABLE_SERIALIZER);
        instance.register(UserDefinedAggregator.class, EXTERNALIZABLE_SERIALIZER);
        instance.register(BulkWrite.class,EXTERNALIZABLE_SERIALIZER);
        instance.register(KVPair.class,EXTERNALIZABLE_SERIALIZER);
        instance.register(SpliceStddevPop.class,EXTERNALIZABLE_SERIALIZER);
        instance.register(SpliceStddevSamp.class,EXTERNALIZABLE_SERIALIZER);
        instance.register(SpliceUDAVariance.class,EXTERNALIZABLE_SERIALIZER);
        instance.register(Properties.class, new MapSerializer());

//        instance.register(com.splicemachine.derby.client.sql.execute.ValueRow.class,EXTERNALIZABLE_SERIALIZER);
//        instance.register(com.splicemachine.derby.client.sql.execute.IndexRow.class,EXTERNALIZABLE_SERIALIZER);
        instance.register(IndexRow.class,
                new SparkValueRowSerializer<IndexRow>(){
                    @Override
                    public void write(Kryo kryo, Output output, IndexRow object) {
                        super.write(kryo, output, object);
                        boolean[] orderedNulls = object.getOrderedNulls();
                        output.writeInt(orderedNulls.length);
                        for (boolean orderedNull : orderedNulls) {
                            output.writeBoolean(orderedNull);
                        }
                    }

                    @Override
                    public IndexRow read(Kryo kryo,
                                         Input input,
                                         Class<IndexRow> type) {
                        IndexRow row = super.read(kryo, input, type);
                        boolean[] orderedNulls = new boolean[input.readInt()];
                        for(int i=0;i<orderedNulls.length;i++){
                            orderedNulls[i] = input.readBoolean();
                        }
                        row.setOrderedNulls(orderedNulls);
                        return row;
                    }

                    @Override
                    protected IndexRow newType(int size) {
                        return IndexRow.createRaw(size);
                    }
                });

//        instance.register(ActivationSerializer.OperationResultSetStorage.class,EXTERNALIZABLE_SERIALIZER);
        instance.register(ByteSlice.class,EXTERNALIZABLE_SERIALIZER);
        instance.register(TriggerExecutionContext.class,EXTERNALIZABLE_SERIALIZER);
        instance.register(TriggerExecutionStack.class,EXTERNALIZABLE_SERIALIZER);
        instance.register(TentativeDropColumnDesc.class,new FieldSerializer(instance,TentativeDropColumnDesc.class));
        instance.register(TentativeAddColumnDesc.class,new FieldSerializer(instance,TentativeAddColumnDesc.class));
        instance.register(TentativeAddConstraintDesc.class,new FieldSerializer(instance,TentativeAddConstraintDesc.class));
        instance.register(TentativeDropPKConstraintDesc.class,new FieldSerializer(instance,TentativeDropPKConstraintDesc.class));
        instance.register(BitSet.class,new Serializer<BitSet>() {
            @Override
            public void write(Kryo kryo, Output output, BitSet object) {
                output.writeInt(object.wlen);
                long[] bits = object.bits;
                for(int i=0;i<object.wlen;i++){
                    output.writeLong(bits[i]);
                }
            }

            @Override
            public BitSet read(Kryo kryo, Input input, Class<BitSet> type) {
                int wlen = input.readInt();
                long[] bits = new long[wlen];
                for(int i=0;i<wlen;i++){
                    bits[i] = input.readLong();
                }
                return new BitSet(bits,wlen);
            }
        });
        instance.register(DDLChangeType.class,new DefaultSerializers.EnumSerializer(DDLChangeType.class));
        instance.register(ColumnInfo.class,EXTERNALIZABLE_SERIALIZER);
        instance.register(ColumnInfo[].class);
        instance.register(MergeLeftOuterJoinOperation.class, EXTERNALIZABLE_SERIALIZER);
        instance.register(DataValueDescriptor[].class);
        instance.register(AbstractSpliceFunction.class,EXTERNALIZABLE_SERIALIZER);
        instance.register(AggregateFinisherFunction.class,EXTERNALIZABLE_SERIALIZER);
        instance.register(AntiJoinFunction.class,EXTERNALIZABLE_SERIALIZER);
        instance.register(AntiJoinRestrictionFlatMapFunction.class,EXTERNALIZABLE_SERIALIZER);
        instance.register(CogroupAntiJoinRestrictionFlatMapFunction.class,EXTERNALIZABLE_SERIALIZER);



        instance.register(CogroupInnerJoinRestrictionFlatMapFunction.class,EXTERNALIZABLE_SERIALIZER);
        instance.register(CogroupLeftOuterJoinRestrictionFlatMapFunction.class,EXTERNALIZABLE_SERIALIZER);
        instance.register(ColumnComparator.class,EXTERNALIZABLE_SERIALIZER);
        instance.register(GroupedAggregateRollupFlatMapFunction.class,EXTERNALIZABLE_SERIALIZER);

        instance.register(InnerJoinFunction.class,EXTERNALIZABLE_SERIALIZER);
        instance.register(InnerJoinRestrictionFlatMapFunction.class,EXTERNALIZABLE_SERIALIZER);
        instance.register(InnerJoinRestrictionFunction.class,EXTERNALIZABLE_SERIALIZER);
        instance.register(JoinRestrictionPredicateFunction.class,EXTERNALIZABLE_SERIALIZER);
        instance.register(KeyerFunction.class,EXTERNALIZABLE_SERIALIZER);
        instance.register(MergeAllAggregatesFunction.class,EXTERNALIZABLE_SERIALIZER);
        instance.register(MergeAllAggregatesFlatMapFunction.class,EXTERNALIZABLE_SERIALIZER);
        instance.register(MergeNonDistinctAggregatesFunction.class,EXTERNALIZABLE_SERIALIZER);
        instance.register(MergeNonDistinctAggregatesFunctionForMixedRows.class,EXTERNALIZABLE_SERIALIZER);
        instance.register(MergeAllAggregatesFunctionForMixedRows.class,EXTERNALIZABLE_SERIALIZER);
        instance.register(DistinctAggregatesPrepareFunction.class,EXTERNALIZABLE_SERIALIZER);
        instance.register(StitchMixedRowFunction.class,EXTERNALIZABLE_SERIALIZER);
        instance.register(StitchMixedRowFlatMapFunction.class,EXTERNALIZABLE_SERIALIZER);
        instance.register(DistinctAggregateKeyCreation.class,EXTERNALIZABLE_SERIALIZER);

        instance.register(MergeWindowFunction.class,EXTERNALIZABLE_SERIALIZER);
        instance.register(NLJAntiJoinFunction.class,EXTERNALIZABLE_SERIALIZER);
        instance.register(NLJInnerJoinFunction.class,EXTERNALIZABLE_SERIALIZER);
        instance.register(NLJOneRowInnerJoinFunction.class,EXTERNALIZABLE_SERIALIZER);
        instance.register(NLJOuterJoinFunction.class,EXTERNALIZABLE_SERIALIZER);
        instance.register(OuterJoinFunction.class,EXTERNALIZABLE_SERIALIZER);

        instance.register(OuterJoinPairFunction.class,EXTERNALIZABLE_SERIALIZER);
        instance.register(LeftOuterJoinRestrictionFlatMapFunction.class,EXTERNALIZABLE_SERIALIZER);
        instance.register(ProjectRestrictMapFunction.class,EXTERNALIZABLE_SERIALIZER);
        instance.register(ProjectRestrictPredicateFunction.class,EXTERNALIZABLE_SERIALIZER);
        instance.register(RowComparator.class,EXTERNALIZABLE_SERIALIZER);
        instance.register(RowOperationFunction.class,EXTERNALIZABLE_SERIALIZER);
        instance.register(ScalarAggregateFlatMapFunction.class,EXTERNALIZABLE_SERIALIZER);
        instance.register(SetCurrentLocatedRowFunction.class,EXTERNALIZABLE_SERIALIZER);

        instance.register(SpliceFlatMapFunction.class,EXTERNALIZABLE_SERIALIZER);
        instance.register(SpliceFunction.class,EXTERNALIZABLE_SERIALIZER);
        instance.register(SpliceFunction2.class,EXTERNALIZABLE_SERIALIZER);
        instance.register(SpliceJoinFlatMapFunction.class,EXTERNALIZABLE_SERIALIZER);
        instance.register(SpliceJoinFunction.class,EXTERNALIZABLE_SERIALIZER);
        instance.register(SplicePairFunction.class,EXTERNALIZABLE_SERIALIZER);
        instance.register(SplicePredicateFunction.class,EXTERNALIZABLE_SERIALIZER);
        instance.register(TableScanTupleMapFunction.class,EXTERNALIZABLE_SERIALIZER);
        instance.register(TableScanPredicateFunction.class,EXTERNALIZABLE_SERIALIZER);

        instance.register(UpdateNoOpPredicateFunction.class,EXTERNALIZABLE_SERIALIZER);
        instance.register(WindowFinisherFunction.class,EXTERNALIZABLE_SERIALIZER);
        instance.register(WindowOperation.class,EXTERNALIZABLE_SERIALIZER);
        instance.register(ScrollInsensitiveOperation.class, EXTERNALIZABLE_SERIALIZER);
        instance.register(IndexRowReaderBuilder.class, EXTERNALIZABLE_SERIALIZER);
        instance.register(VTIOperation.class, EXTERNALIZABLE_SERIALIZER);
        instance.register(StandardException.class, new Serializer<StandardException>() {
            @Override
            public void write(Kryo kryo, Output output, StandardException e) {
                Object[] arguments = e.getArguments();
                if (arguments != null) {
                    output.writeInt(arguments.length);
                } else {
                    output.writeInt(0);
                }
                for (Object arg : arguments) {
                    kryo.writeClassAndObject(output, arg);
                }
                output.writeString(e.getMessageId());
            }

            @Override
            public StandardException read(Kryo kryo, Input input, Class aClass) {
                int args = input.readInt();
                Object[] arguments = null;
                if (args > 0) {
                    arguments = new Object[args];
                    for (int i = 0; i < args; ++i) {
                        arguments[i] = kryo.readClassAndObject(input);
                    }
                }
                String messageId = input.readString();
                return StandardException.newException(messageId, arguments);
            }
        });
        instance.register(ArrayListMultimap.class, new Serializer<ArrayListMultimap>() {
            @Override
            public void write(Kryo kryo, Output output, ArrayListMultimap map) {
                output.writeInt(map.size());
                for (Map.Entry e : (Collection<Map.Entry>) map.entries()) {
                    kryo.writeClassAndObject(output, e.getKey());
                    kryo.writeClassAndObject(output, e.getValue());
                }
            }

            @Override
            public ArrayListMultimap read(Kryo kryo, Input input, Class aClass) {
                int size = input.readInt();
                ArrayListMultimap map = ArrayListMultimap.create();
                for (int i = 0; i < size; ++i) {
                    map.put(kryo.readClassAndObject(input), kryo.readClassAndObject(input));
                }
                return map;
            }
        });

        instance.register(ActivationHolder.class,EXTERNALIZABLE_SERIALIZER);
        instance.register(HBasePartitioner.class,EXTERNALIZABLE_SERIALIZER);
        instance.register(RowPartition.class, EXTERNALIZABLE_SERIALIZER);
        instance.register(HalfMergeSortJoinOperation.class,EXTERNALIZABLE_SERIALIZER);
        instance.register(HalfMergeSortLeftOuterJoinOperation.class,EXTERNALIZABLE_SERIALIZER);
        instance.register(RowToLocatedRowFunction.class,EXTERNALIZABLE_SERIALIZER);
        instance.register(LocatedRowToRowFunction.class,EXTERNALIZABLE_SERIALIZER);
        instance.register(CachedOperation.CacheFunction.class,EXTERNALIZABLE_SERIALIZER);
        instance.register(CountJoinedLeftFunction.class,EXTERNALIZABLE_SERIALIZER);
        instance.register(CountJoinedRightFunction.class,EXTERNALIZABLE_SERIALIZER);
        instance.register(CountProducedFunction.class,EXTERNALIZABLE_SERIALIZER);
        instance.register(CountReadFunction.class,EXTERNALIZABLE_SERIALIZER);
        instance.register(EmptyFunction.class,EXTERNALIZABLE_SERIALIZER);
        instance.register(HTableScanTupleFunction.class,EXTERNALIZABLE_SERIALIZER);
        instance.register(IndexToBaseRowFilterPredicateFunction.class,EXTERNALIZABLE_SERIALIZER);
        instance.register(IndexTransformFunction.class,EXTERNALIZABLE_SERIALIZER);
        instance.register(InnerJoinNullFilterFunction.class,EXTERNALIZABLE_SERIALIZER);
        instance.register(MapFunction.class,EXTERNALIZABLE_SERIALIZER);
        instance.register(RowTransformFunction.class,EXTERNALIZABLE_SERIALIZER);
        instance.register(TxnViewDecoderFunction.class,EXTERNALIZABLE_SERIALIZER);
        instance.register(CountWriteFunction.class,EXTERNALIZABLE_SERIALIZER);
        instance.register(SQLArray.class,EXTERNALIZABLE_SERIALIZER);
        instance.register(SparkFlatMapFunction.class,EXTERNALIZABLE_SERIALIZER);
        instance.register(ExternalizableFlatMapFunction.class,EXTERNALIZABLE_SERIALIZER);
        instance.register(RowCountOperation.class,EXTERNALIZABLE_SERIALIZER);
        instance.register(SpliceBaseOperation.class,EXTERNALIZABLE_SERIALIZER);
        instance.register(MergeStatisticsHolder.class,EXTERNALIZABLE_SERIALIZER);
        instance.register(ColumnStatisticsMerge.class,EXTERNALIZABLE_SERIALIZER);
        instance.register(ColumnStatisticsImpl.class,EXTERNALIZABLE_SERIALIZER);
        instance.register(StreamProtocol.Init.class,EXTERNALIZABLE_SERIALIZER);
        instance.register(StreamProtocol.Limit.class,EXTERNALIZABLE_SERIALIZER);
        instance.register(StreamProtocol.ConfirmClose.class,EXTERNALIZABLE_SERIALIZER);
        instance.register(StreamProtocol.Continue.class,EXTERNALIZABLE_SERIALIZER);
        instance.register(StreamProtocol.RequestClose.class,EXTERNALIZABLE_SERIALIZER);
        instance.register(StreamProtocol.Skip.class,EXTERNALIZABLE_SERIALIZER);
        instance.register(StreamProtocol.Skipped.class,EXTERNALIZABLE_SERIALIZER);
        instance.register(SparkSpliceFunctionWrapper.class,EXTERNALIZABLE_SERIALIZER);
        instance.register(SparkSpliceFunctionWrapper2.class,EXTERNALIZABLE_SERIALIZER);
        instance.register(ResultStreamer.class,EXTERNALIZABLE_SERIALIZER);
        instance.register(JoinOperation.class,EXTERNALIZABLE_SERIALIZER);
        instance.register(DMLWriteOperation.class,EXTERNALIZABLE_SERIALIZER);
        instance.register(MiscOperation.class,EXTERNALIZABLE_SERIALIZER);
        instance.register(CrossJoinOperation.class,EXTERNALIZABLE_SERIALIZER);
        instance.register(FormatableProperties.class,EXTERNALIZABLE_SERIALIZER);
        instance.register(BadRecordsRecorder.class,EXTERNALIZABLE_SERIALIZER);
        instance.register(ManagedCache.class,EXTERNALIZABLE_SERIALIZER);

        Serializer<Optional> optionalSerializer = new Serializer<Optional>() {
            @Override
            public void write(Kryo kryo, Output output, Optional object) {
                if (object.isPresent()) {
                    output.writeBoolean(false);
                    kryo.writeClassAndObject(output, object.get());
                } else {
                    output.writeBoolean(true);
                }
            }

            @Override
            public Optional read(Kryo kryo, Input input, Class<Optional> type) {
                boolean isNull = input.readBoolean();
                if (isNull) {
                    return Optional.absent();
                } else {
                    return Optional.of(kryo.readClassAndObject(input));
                }
            }
        };

        instance.register(Optional.class, optionalSerializer);
        instance.register(Optional.absent().getClass(), optionalSerializer);
        instance.register(Optional.of("").getClass(), optionalSerializer);
        instance.register(SelfReferenceOperation.class,new Serializer<SelfReferenceOperation>(){
            @Override
            public void write(Kryo kryo,Output output,SelfReferenceOperation object){
                try{
                    object.writeExternalWithoutChild(new KryoObjectOutput(output,kryo));
                }catch(IOException e){
                    throw new RuntimeException(e);
                }
            }

            @Override
            public SelfReferenceOperation read(Kryo kryo,Input input,Class type){
                SelfReferenceOperation selfReferenceOperation=new SelfReferenceOperation();
                try{
                    selfReferenceOperation.readExternal(new KryoObjectInput(input,kryo));
                }catch(IOException|ClassNotFoundException e){
                    throw new RuntimeException(e);
                }
                return selfReferenceOperation;
            }
        });

        instance.register(RecursiveUnionOperation.class,new Serializer<RecursiveUnionOperation>(){
            @Override
            public void write(Kryo kryo,Output output,RecursiveUnionOperation object){
                try{
                    object.writeExternal(new KryoObjectOutput(output,kryo));
                }catch(IOException e){
                    throw new RuntimeException(e);
                }
            }

            @Override
            public RecursiveUnionOperation read(Kryo kryo,Input input,Class type){
                RecursiveUnionOperation recursiveUnionOperation=new RecursiveUnionOperation();
                try{
                    recursiveUnionOperation.readExternal(new KryoObjectInput(input,kryo));
                    recursiveUnionOperation.setRecursiveUnionReference(recursiveUnionOperation);
                }catch(IOException|ClassNotFoundException e){
                    throw new RuntimeException(e);
                }
                return recursiveUnionOperation;
            }
        });

        instance.register(org.apache.commons.lang3.mutable.MutableDouble.class,
                          new SimpleObjectSerializer<MutableDouble>() {
            @Override
            protected void writeValue(Kryo kryo, Output output, MutableDouble object) throws RuntimeException {
                output.writeDouble(object.doubleValue());
            }

            @Override
            protected void readValue(Kryo kryo, Input input, MutableDouble object) throws RuntimeException {
                object.setValue(input.readDouble());
            }
        });

        instance.register(LongBufferedSumAggregator.class,new Serializer<LongBufferedSumAggregator>(){
            @Override
            public void write(Kryo kryo,Output output,LongBufferedSumAggregator object){
                try{
                    object.writeExternal(new KryoObjectOutput(output,kryo));
                }catch(IOException e){
                    throw new RuntimeException(e);
                }
            }

            @Override
            public LongBufferedSumAggregator read(Kryo kryo,Input input,Class type){
                LongBufferedSumAggregator aggregator=new LongBufferedSumAggregator(64); //todo -sf- make configurable
                try{
                    aggregator.readExternal(new KryoObjectInput(input,kryo));
                }catch(IOException|ClassNotFoundException e){
                    throw new RuntimeException(e);
                }
                return aggregator;
            }
        });

        instance.register(DoubleBufferedSumAggregator.class,new Serializer<DoubleBufferedSumAggregator>(){
            @Override
            public void write(Kryo kryo,Output output,DoubleBufferedSumAggregator object){
                try{
                    object.writeExternal(new KryoObjectOutput(output,kryo));
                }catch(IOException e){
                    throw new RuntimeException(e);
                }
            }

            @Override
            public DoubleBufferedSumAggregator read(Kryo kryo,Input input,Class type){
                DoubleBufferedSumAggregator aggregator=new DoubleBufferedSumAggregator(64); //todo -sf- make configurable
                try{
                    aggregator.readExternal(new KryoObjectInput(input,kryo));
                }catch(IOException|ClassNotFoundException e){
                    throw new RuntimeException(e);
                }
                return aggregator;
            }
        });

        instance.register(DecimalBufferedSumAggregator.class,new Serializer<DecimalBufferedSumAggregator>(){
            @Override
            public void write(Kryo kryo,Output output,DecimalBufferedSumAggregator object){
                try{
                    object.writeExternal(new KryoObjectOutput(output,kryo));
                }catch(IOException e){
                    throw new RuntimeException(e);
                }
            }

            @Override
            public DecimalBufferedSumAggregator read(Kryo kryo,Input input,Class type){
                DecimalBufferedSumAggregator aggregator=new DecimalBufferedSumAggregator(64); //todo -sf- make configurable
                try{
                    aggregator.readExternal(new KryoObjectInput(input,kryo));
                }catch(IOException|ClassNotFoundException e){
                    throw new RuntimeException(e);
                }
                return aggregator;
            }
        });

        instance.register(getClassFromString("java.util.Arrays$ArrayList"),
                          new ArraysAsListSerializer());

        instance.register(SparkColumnReference.class,EXTERNALIZABLE_SERIALIZER);
        instance.register(SparkConstantExpression.class,EXTERNALIZABLE_SERIALIZER);
        instance.register(SparkLogicalOperator.class,EXTERNALIZABLE_SERIALIZER);
        instance.register(SparkRelationalOperator.class,EXTERNALIZABLE_SERIALIZER);
        instance.register(SparkArithmeticOperator.class,EXTERNALIZABLE_SERIALIZER);
        instance.register(SparkCastNode.class,EXTERNALIZABLE_SERIALIZER);
        instance.register(SignalOperation.class,EXTERNALIZABLE_SERIALIZER);
        instance.register(SetOperation.class,EXTERNALIZABLE_SERIALIZER);
        instance.register(CogroupFullOuterJoinRestrictionFlatMapFunction.class,EXTERNALIZABLE_SERIALIZER);
        instance.register(AbstractBroadcastJoinFlatMapFunction.class,EXTERNALIZABLE_SERIALIZER);
        instance.register(LeftAntiJoinRestrictionFlatMapFunction.class,EXTERNALIZABLE_SERIALIZER);
        instance.register(BroadcastFullOuterJoinOperation.class,EXTERNALIZABLE_SERIALIZER);
        instance.register(MergeSortFullOuterJoinOperation.class,EXTERNALIZABLE_SERIALIZER);
        instance.register(FakeColumnStatisticsImpl.class,EXTERNALIZABLE_SERIALIZER);
    }
}
