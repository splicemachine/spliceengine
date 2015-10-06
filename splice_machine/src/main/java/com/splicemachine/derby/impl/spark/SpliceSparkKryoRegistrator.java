package com.splicemachine.derby.impl.spark;

import com.carrotsearch.hppc.BitSet;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.esotericsoftware.kryo.serializers.DefaultSerializers;
import com.esotericsoftware.kryo.serializers.FieldSerializer;
import com.esotericsoftware.kryo.serializers.MapSerializer;

import com.google.common.collect.ArrayListMultimap;
import com.splicemachine.derby.ddl.DDLChangeType;
import com.splicemachine.derby.ddl.TentativeAddColumnDesc;
import com.splicemachine.derby.ddl.TentativeAddConstraintDesc;
import com.splicemachine.derby.ddl.TentativeDropColumnDesc;
import com.splicemachine.derby.ddl.TentativeDropPKConstraintDesc;
import com.splicemachine.derby.ddl.TentativeIndexDesc;
import com.splicemachine.derby.hbase.ActivationSerializer;
import com.splicemachine.derby.hbase.SpliceObserverInstructions;
import com.splicemachine.derby.iapi.sql.execute.SpliceRuntimeContext;
import com.splicemachine.derby.impl.job.altertable.AlterTableTask;
import com.splicemachine.derby.impl.job.ZkTask;
import com.splicemachine.derby.impl.job.altertable.PopulateConglomerateTask;
import com.splicemachine.derby.impl.job.index.CreateIndexTask;
import com.splicemachine.derby.impl.job.index.PopulateIndexTask;
import com.splicemachine.derby.impl.load.ColumnContext;
import com.splicemachine.derby.impl.load.FileImportReader;
import com.splicemachine.derby.impl.load.ImportContext;
import com.splicemachine.derby.impl.load.ImportTask;
import com.splicemachine.derby.impl.load.spark.ImportResult;
import com.splicemachine.derby.impl.spark.kryo.SparkValueRowSerializer;
import com.splicemachine.derby.impl.sql.execute.LazyDataValueDescriptor;
import com.splicemachine.derby.impl.sql.execute.LazyNumberDataValueDescriptor;
import com.splicemachine.derby.impl.sql.execute.LazyStringDataValueDescriptor;
import com.splicemachine.derby.impl.sql.execute.LazyTimestampDataValueDescriptor;
import com.splicemachine.derby.impl.sql.execute.actions.DeleteConstantOperation;
import com.splicemachine.derby.impl.sql.execute.actions.InsertConstantOperation;
import com.splicemachine.derby.impl.sql.execute.actions.UpdateConstantOperation;
import com.splicemachine.derby.impl.sql.execute.operations.*;
import com.splicemachine.derby.impl.sql.execute.operations.framework.DerbyAggregateContext;
import com.splicemachine.derby.impl.sql.execute.operations.groupedaggregate.DerbyGroupedAggregateContext;
import com.splicemachine.derby.impl.store.access.btree.IndexConglomerate;
import com.splicemachine.derby.impl.store.access.hbase.HBaseConglomerate;
import com.splicemachine.derby.impl.store.access.hbase.HBaseRowLocation;
import com.splicemachine.derby.stats.TaskStats;
import com.splicemachine.derby.stream.function.*;
import com.splicemachine.derby.stream.spark.SparkOperationContext;
import com.splicemachine.derby.utils.kryo.DataValueDescriptorSerializer;
import com.splicemachine.hbase.KVPair;
import com.splicemachine.job.ErrorTransport;
import com.splicemachine.job.TaskStatus;
import com.splicemachine.pipeline.impl.BulkWrite;
import com.splicemachine.utils.ByteSlice;
import com.splicemachine.utils.kryo.ExternalizableSerializer;

import de.javakaffee.kryoserializers.UnmodifiableCollectionsSerializer;
import com.splicemachine.db.catalog.types.*;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.services.io.*;
import com.splicemachine.db.iapi.sql.dictionary.IndexRowGenerator;
import com.splicemachine.db.iapi.sql.dictionary.SchemaDescriptor;
import com.splicemachine.db.iapi.store.raw.ContainerKey;
import com.splicemachine.db.iapi.types.*;
import com.splicemachine.db.impl.services.uuid.BasicUUID;
import com.splicemachine.db.impl.sql.*;
import com.splicemachine.db.impl.sql.catalog.DDColumnDependableFinder;
import com.splicemachine.db.impl.sql.catalog.DD_Version;
import com.splicemachine.db.impl.sql.catalog.DDdependableFinder;
import com.splicemachine.db.impl.sql.execute.*;
import com.splicemachine.db.impl.store.access.PC_XenaVersion;
import org.apache.spark.scheduler.CompressedMapStatus;
import org.apache.spark.serializer.KryoRegistrator;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.*;

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
public class SpliceSparkKryoRegistrator implements KryoRegistrator {
    //ExternalizableSerializers are stateless, no need to create more than we need
    private static final ExternalizableSerializer EXTERNALIZABLE_SERIALIZER = new ExternalizableSerializer();
    private static final UnmodifiableCollectionsSerializer UNMODIFIABLE_COLLECTIONS_SERIALIZER = new UnmodifiableCollectionsSerializer();

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
        instance.register(LazyDataValueDescriptor.class,EXTERNALIZABLE_SERIALIZER);
        instance.register(LazyNumberDataValueDescriptor.class,EXTERNALIZABLE_SERIALIZER);
        instance.register(LazyStringDataValueDescriptor.class,EXTERNALIZABLE_SERIALIZER);
        instance.register(TaskStatus.class,EXTERNALIZABLE_SERIALIZER);


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
        instance.register(SpliceRuntimeContext.class,EXTERNALIZABLE_SERIALIZER);

        instance.register(ProjectRestrictOperation.class, EXTERNALIZABLE_SERIALIZER);
        instance.register(TableScanOperation.class, EXTERNALIZABLE_SERIALIZER);
        instance.register(BulkTableScanOperation.class, EXTERNALIZABLE_SERIALIZER);
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

        instance.register(PC_XenaVersion.class,EXTERNALIZABLE_SERIALIZER);
        instance.register(BasicUUID.class,EXTERNALIZABLE_SERIALIZER);
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
        instance.register(Collections.emptyList().getClass());
        instance.register(Collections.unmodifiableList(new LinkedList()).getClass(), UNMODIFIABLE_COLLECTIONS_SERIALIZER);
        instance.register(Collections.unmodifiableList(new ArrayList()).getClass(), UNMODIFIABLE_COLLECTIONS_SERIALIZER);
        instance.register(CursorInfo.class,EXTERNALIZABLE_SERIALIZER);
        instance.register(GenericResultDescription.class,EXTERNALIZABLE_SERIALIZER);
        instance.register(GenericColumnDescriptor.class,EXTERNALIZABLE_SERIALIZER);
        instance.register(ReferencedColumnsDescriptorImpl.class,EXTERNALIZABLE_SERIALIZER);
        instance.register(ErrorTransport.class,EXTERNALIZABLE_SERIALIZER);
        instance.register(DefaultInfoImpl.class,EXTERNALIZABLE_SERIALIZER);

        instance.register(BigDecimal.class);
        instance.register(HashMap.class);
        instance.register(TreeMap.class);
        instance.register(ArrayList.class);
        instance.register(LinkedList.class);

        instance.register(FormatableArrayHolder.class,EXTERNALIZABLE_SERIALIZER);
        instance.register(IndexColumnOrder.class,EXTERNALIZABLE_SERIALIZER);
        instance.register(HBaseRowLocation.class,EXTERNALIZABLE_SERIALIZER);
        instance.register(TaskStats.class,EXTERNALIZABLE_SERIALIZER);
        instance.register(AggregatorInfoList.class,EXTERNALIZABLE_SERIALIZER);
        instance.register(AggregatorInfo.class,EXTERNALIZABLE_SERIALIZER);
        instance.register(UserType.class,EXTERNALIZABLE_SERIALIZER);
        instance.register(InsertConstantOperation.class,EXTERNALIZABLE_SERIALIZER);
        instance.register(DerbyDMLWriteInfo.class,EXTERNALIZABLE_SERIALIZER);
        instance.register(DeleteConstantOperation.class,EXTERNALIZABLE_SERIALIZER);
        instance.register(FormatableLongHolder.class,EXTERNALIZABLE_SERIALIZER);
        instance.register(FormatableInstanceGetter.class,EXTERNALIZABLE_SERIALIZER);
        instance.register(SpliceRuntimeContext.Path.class,EXTERNALIZABLE_SERIALIZER);
        instance.register(IndexRowGenerator.class,EXTERNALIZABLE_SERIALIZER);
        instance.register(MultiProbeDerbyScanInformation.class,EXTERNALIZABLE_SERIALIZER);
        instance.register(MultiProbeTableScanOperation.class,EXTERNALIZABLE_SERIALIZER);
        instance.register(DistinctGroupedAggregateOperation.class,EXTERNALIZABLE_SERIALIZER);



        instance.register(ContainerKey.class,new Serializer<ContainerKey>() {
            @Override
            public void write(Kryo kryo, Output output, ContainerKey object) {
                output.writeLong(object.getContainerId());
                output.writeLong(object.getSegmentId());
            }

            @Override
            public ContainerKey read(Kryo kryo, Input input, Class<ContainerKey> type) {
                long containerId = input.readLong();
                long segmentId = input.readLong();
                return new ContainerKey(segmentId,containerId);
            }
        });

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
        instance.register(SpliceStddevPop.class);
        instance.register(SpliceStddevSamp.class);
        instance.register(Properties.class, new MapSerializer());

//        instance.register(com.splicemachine.derby.impl.sql.execute.ValueRow.class,EXTERNALIZABLE_SERIALIZER);
//        instance.register(com.splicemachine.derby.impl.sql.execute.IndexRow.class,EXTERNALIZABLE_SERIALIZER);
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

        instance.register(LazyTimestampDataValueDescriptor.class,EXTERNALIZABLE_SERIALIZER);
       // instance.register(ActivationSerializer.OperationResultSetStorage.class,EXTERNALIZABLE_SERIALIZER);
        instance.register(ByteSlice.class,EXTERNALIZABLE_SERIALIZER);
				instance.register(ZkTask.class,EXTERNALIZABLE_SERIALIZER);
				//instance.register(SinkTask.class,EXTERNALIZABLE_SERIALIZER);
				instance.register(CreateIndexTask.class,EXTERNALIZABLE_SERIALIZER);
				instance.register(PopulateIndexTask.class,EXTERNALIZABLE_SERIALIZER);
				instance.register(ImportTask.class,EXTERNALIZABLE_SERIALIZER);
				instance.register(ImportContext.class,EXTERNALIZABLE_SERIALIZER);
				instance.register(ColumnContext.class,EXTERNALIZABLE_SERIALIZER);
				instance.register(FileImportReader.class,EXTERNALIZABLE_SERIALIZER);
				instance.register(TriggerExecutionContext.class,EXTERNALIZABLE_SERIALIZER);
				instance.register(TriggerExecutionStack.class,EXTERNALIZABLE_SERIALIZER);
				instance.register(TentativeIndexDesc.class,new FieldSerializer(instance,TentativeIndexDesc.class));
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
				instance.register(AlterTableTask.class,EXTERNALIZABLE_SERIALIZER);
				instance.register(PopulateConglomerateTask.class,EXTERNALIZABLE_SERIALIZER);
				instance.register(ColumnInfo.class,EXTERNALIZABLE_SERIALIZER);
				instance.register(ColumnInfo[].class);
				instance.register(MergeLeftOuterJoinOperation.class, EXTERNALIZABLE_SERIALIZER);
        instance.register(DataValueDescriptor[].class);
        instance.register(LocatedRow.class,EXTERNALIZABLE_SERIALIZER);
        instance.register(AbstractSpliceFunction.class,EXTERNALIZABLE_SERIALIZER);
        instance.register(AggregateFinisherFunction.class,EXTERNALIZABLE_SERIALIZER);
        instance.register(AntiJoinFunction.class,EXTERNALIZABLE_SERIALIZER);
        instance.register(AntiJoinRestrictionFlatMapFunction.class,EXTERNALIZABLE_SERIALIZER);
        instance.register(CogroupAntiJoinRestrictionFlatMapFunction.class,EXTERNALIZABLE_SERIALIZER);



        instance.register(CogroupInnerJoinRestrictionFlatMapFunction.class,EXTERNALIZABLE_SERIALIZER);
        instance.register(CogroupOuterJoinRestrictionFlatMapFunction.class,EXTERNALIZABLE_SERIALIZER);
        instance.register(ColumnComparator.class,EXTERNALIZABLE_SERIALIZER);
        instance.register(FetchWithOffsetFunction.class,EXTERNALIZABLE_SERIALIZER);
        instance.register(GroupedAggregateRollupFlatMapFunction.class,EXTERNALIZABLE_SERIALIZER);

        instance.register(InnerJoinFunction.class,EXTERNALIZABLE_SERIALIZER);
        instance.register(InnerJoinRestrictionFlatMapFunction.class,EXTERNALIZABLE_SERIALIZER);
        instance.register(InnerJoinRestrictionFunction.class,EXTERNALIZABLE_SERIALIZER);
        instance.register(InsertPairFunction.class,EXTERNALIZABLE_SERIALIZER);

        instance.register(JoinRestrictionPredicateFunction.class,EXTERNALIZABLE_SERIALIZER);
        instance.register(KeyerFunction.class,EXTERNALIZABLE_SERIALIZER);
        instance.register(LocatedRowToRowLocationFunction.class,EXTERNALIZABLE_SERIALIZER);
        instance.register(MergeAllAggregatesFunction.class,EXTERNALIZABLE_SERIALIZER);
        instance.register(MergeNonDistinctAggregatesFunction.class,EXTERNALIZABLE_SERIALIZER);


        instance.register(MergeWindowFunction.class,EXTERNALIZABLE_SERIALIZER);
        instance.register(NLJAntiJoinFunction.class,EXTERNALIZABLE_SERIALIZER);
        instance.register(NLJInnerJoinFunction.class,EXTERNALIZABLE_SERIALIZER);
        instance.register(NLJOneRowInnerJoinFunction.class,EXTERNALIZABLE_SERIALIZER);
        instance.register(NLJOuterJoinFunction.class,EXTERNALIZABLE_SERIALIZER);
        instance.register(OuterJoinFunction.class,EXTERNALIZABLE_SERIALIZER);

        instance.register(OuterJoinPairFunction.class,EXTERNALIZABLE_SERIALIZER);
        instance.register(OuterJoinRestrictionFlatMapFunction.class,EXTERNALIZABLE_SERIALIZER);
        instance.register(ProjectRestrictFlatMapFunction.class,EXTERNALIZABLE_SERIALIZER);
        instance.register(RowComparator.class,EXTERNALIZABLE_SERIALIZER);
        instance.register(RowOperationFunction.class,EXTERNALIZABLE_SERIALIZER);
        instance.register(ScalarAggregateFunction.class,EXTERNALIZABLE_SERIALIZER);
        instance.register(SetCurrentLocatedRowFunction.class,EXTERNALIZABLE_SERIALIZER);

        instance.register(SpliceFlatMap2Function.class,EXTERNALIZABLE_SERIALIZER);
        instance.register(SpliceFlatMapFunction.class,EXTERNALIZABLE_SERIALIZER);
        instance.register(SpliceFunction.class,EXTERNALIZABLE_SERIALIZER);
        instance.register(SpliceFunction2.class,EXTERNALIZABLE_SERIALIZER);
        instance.register(SpliceJoinFlatMapFunction.class,EXTERNALIZABLE_SERIALIZER);
        instance.register(SpliceJoinFunction.class,EXTERNALIZABLE_SERIALIZER);
        instance.register(SplicePairFunction.class,EXTERNALIZABLE_SERIALIZER);
        instance.register(SplicePredicateFunction.class,EXTERNALIZABLE_SERIALIZER);
        instance.register(TableScanTupleFunction.class,EXTERNALIZABLE_SERIALIZER);

        instance.register(UpdateNoOpPredicateFunction.class,EXTERNALIZABLE_SERIALIZER);
        instance.register(WindowFinisherFunction.class,EXTERNALIZABLE_SERIALIZER);
        instance.register(WindowFlatMapFunction.class,EXTERNALIZABLE_SERIALIZER);
        instance.register(ScrollInsensitiveOperation.class, EXTERNALIZABLE_SERIALIZER);
        instance.register(IndexRowReaderBuilder.class, EXTERNALIZABLE_SERIALIZER);
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
        instance.register(ImportResult.class);
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
    }
}