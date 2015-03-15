package com.splicemachine.derby.impl.spark;

import com.carrotsearch.hppc.BitSet;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Registration;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.esotericsoftware.kryo.serializers.DefaultSerializers;
import com.esotericsoftware.kryo.serializers.FieldSerializer;
import com.esotericsoftware.kryo.serializers.MapSerializer;
import com.splicemachine.constants.SpliceConstants;
import com.splicemachine.derby.ddl.DDLChangeType;
import com.splicemachine.derby.ddl.TentativeDropColumnDesc;
import com.splicemachine.derby.ddl.TentativeIndexDesc;
import com.splicemachine.derby.hbase.ActivationSerializer;
import com.splicemachine.derby.hbase.SpliceObserverInstructions;
import com.splicemachine.derby.iapi.sql.execute.SpliceRuntimeContext;
import com.splicemachine.derby.impl.job.AlterTable.DropColumnTask;
import com.splicemachine.derby.impl.job.AlterTable.LoadConglomerateTask;
import com.splicemachine.derby.impl.job.ZkTask;
import com.splicemachine.derby.impl.job.index.CreateIndexTask;
import com.splicemachine.derby.impl.job.index.PopulateIndexTask;
import com.splicemachine.derby.impl.job.operation.SinkTask;
import com.splicemachine.derby.impl.load.ColumnContext;
import com.splicemachine.derby.impl.load.FileImportReader;
import com.splicemachine.derby.impl.load.ImportContext;
import com.splicemachine.derby.impl.load.ImportTask;
import com.splicemachine.derby.impl.spark.kryo.SparkValueRowSerializer;
import com.splicemachine.derby.impl.sql.execute.*;
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
import com.splicemachine.derby.utils.kryo.DataValueDescriptorSerializer;
import com.splicemachine.hbase.KVPair;
import com.splicemachine.job.ErrorTransport;
import com.splicemachine.job.TaskStatus;
import com.splicemachine.pipeline.impl.BulkWrite;
import com.splicemachine.utils.ByteSlice;
import com.splicemachine.utils.kryo.ExternalizableSerializer;
import com.splicemachine.utils.kryo.KryoPool;
import com.twitter.chill.TraversableSerializer;
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
import com.splicemachine.db.impl.sql.execute.IndexRow;
import com.splicemachine.db.impl.store.access.PC_XenaVersion;
import org.apache.spark.SerializableWritable;
import org.apache.spark.scheduler.CompressedMapStatus;
import org.apache.spark.scheduler.MapStatus;
import org.apache.spark.storage.BlockManagerId;
import org.apache.spark.storage.StorageLevel;
import scala.Tuple2;
import scala.collection.mutable.ArrayBuffer;
import scala.collection.mutable.WrappedArray;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
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
public class SpliceSparkKryoRegistry implements KryoPool.KryoRegistry{
    //ExternalizableSerializers are stateless, no need to create more than we need
    private static final ExternalizableSerializer EXTERNALIZABLE_SERIALIZER = new ExternalizableSerializer();
    private static final UnmodifiableCollectionsSerializer UNMODIFIABLE_COLLECTIONS_SERIALIZER = new UnmodifiableCollectionsSerializer();

		private static final KryoPool spliceKryoPool;
		static{
				spliceKryoPool = new KryoPool(SpliceConstants.kryoPoolSize);
				spliceKryoPool.setKryoRegistry(new SpliceSparkKryoRegistry());
		}

		public static KryoPool getInstance(){
				return spliceKryoPool;
		}

    @Override
    public void register(Kryo instance) {
    	instance.setReferences(false);
        instance.setRegistrationRequired(false); //TODO set to true

        instance.register(ValueRow.class, new SparkValueRowSerializer<ValueRow>(){
						@Override
						protected ValueRow newType(int size) {
								return new ValueRow(size);
						}
				},10);
        instance.register(GenericStorablePreparedStatement.class,EXTERNALIZABLE_SERIALIZER,11);
        instance.register(DataTypeDescriptor.class,EXTERNALIZABLE_SERIALIZER,18);
        instance.register(TypeDescriptorImpl.class,EXTERNALIZABLE_SERIALIZER,12);
        instance.register(BaseTypeIdImpl.class,EXTERNALIZABLE_SERIALIZER,13);
        instance.register(UserDefinedTypeIdImpl.class,EXTERNALIZABLE_SERIALIZER,14);
        instance.register(DecimalTypeIdImpl.class,EXTERNALIZABLE_SERIALIZER,15);
        instance.register(RowMultiSetImpl.class,EXTERNALIZABLE_SERIALIZER,16);
        instance.register(RoutineAliasInfo.class,EXTERNALIZABLE_SERIALIZER,17);

        instance.register(IndexConglomerate.class,EXTERNALIZABLE_SERIALIZER,19);
        instance.register(HBaseConglomerate.class,EXTERNALIZABLE_SERIALIZER,20);

        instance.register(FormatableBitSet.class,EXTERNALIZABLE_SERIALIZER,21);

        instance.register(CountAggregator.class,EXTERNALIZABLE_SERIALIZER,22);
        instance.register(SumAggregator.class,EXTERNALIZABLE_SERIALIZER,23);
        instance.register(AvgAggregator.class,EXTERNALIZABLE_SERIALIZER,24);
        instance.register(MaxMinAggregator.class,EXTERNALIZABLE_SERIALIZER,25);

        instance.register(SQLDecimal.class,new DataValueDescriptorSerializer<SQLDecimal>(){
            @Override
            protected void writeValue(Kryo kryo, Output output, SQLDecimal object) throws StandardException {
                kryo.writeObjectOrNull(output, object.getObject(), BigDecimal.class);
            }
            @Override
            protected void readValue(Kryo kryo, Input input, SQLDecimal dvd) throws StandardException {
                dvd.setBigDecimal(kryo.readObjectOrNull(input, BigDecimal.class));
            }
        },26);
        instance.register(SQLDouble.class,new DataValueDescriptorSerializer<SQLDouble>(){
            @Override
            protected void writeValue(Kryo kryo, Output output, SQLDouble object) throws StandardException {
                output.writeDouble(object.getDouble());
            }
            @Override
            protected void readValue(Kryo kryo, Input input, SQLDouble dvd) throws StandardException {
                dvd.setValue(input.readDouble());
            }
        },27);
        instance.register(SQLInteger.class,new DataValueDescriptorSerializer<SQLInteger>() {
            @Override
            protected void writeValue(Kryo kryo, Output output, SQLInteger object) {
                output.writeInt(object.getInt());
            }
            @Override
            protected void readValue(Kryo kryo, Input input, SQLInteger dvd) {
                dvd.setValue(input.readInt());
            }
        },28);
        instance.register(SQLVarchar.class,new DataValueDescriptorSerializer<SQLVarchar>() {
            @Override
            protected void writeValue(Kryo kryo, Output output, SQLVarchar object) throws StandardException {
                output.writeString(object.getString());
            }
            @Override
            protected void readValue(Kryo kryo, Input input, SQLVarchar dvd) {
                dvd.setValue(input.readString());
            }
        },29);
        instance.register(SQLLongvarchar.class,new DataValueDescriptorSerializer<SQLLongvarchar>() {
            @Override
            protected void writeValue(Kryo kryo, Output output, SQLLongvarchar object) throws StandardException {
                output.writeString(object.getString());
            }
            @Override
            protected void readValue(Kryo kryo, Input input, SQLLongvarchar dvd) {
                dvd.setValue(input.readString());
            }
        },30);
        instance.register(SQLChar.class,new DataValueDescriptorSerializer<SQLChar>() {
            @Override
            protected void writeValue(Kryo kryo, Output output, SQLChar object) throws StandardException {
                output.writeString(object.getString());
            }
            @Override
            protected void readValue(Kryo kryo, Input input, SQLChar dvd) {
                dvd.setValue(input.readString());
            }
        },31);
        instance.register(SQLBoolean.class,new DataValueDescriptorSerializer<SQLBoolean>() {
            @Override
            protected void writeValue(Kryo kryo, Output output, SQLBoolean object) throws StandardException {
                output.writeBoolean(object.getBoolean());
            }

            @Override
            protected void readValue(Kryo kryo, Input input, SQLBoolean dvd) throws StandardException {
                dvd.setValue(input.readBoolean());
            }
        },32);
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
        },33);
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
        },34);
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
        },35);

        instance.register(SQLDate.class,new DataValueDescriptorSerializer<SQLDate>() {
            @Override
            protected void writeValue(Kryo kryo, Output output, SQLDate object) throws StandardException {
                output.writeLong(object.getDate((Calendar) null).getTime());
            }

            @Override
            protected void readValue(Kryo kryo, Input input, SQLDate dvd) throws StandardException {
                dvd.setValue(new Date(input.readLong()));
            }
        },36);
        instance.register(SQLTime.class,new DataValueDescriptorSerializer<SQLTime>(){
            @Override
            protected void writeValue(Kryo kryo, Output output, SQLTime object) throws StandardException {
                output.writeLong(object.getTime(null).getTime());
            }

            @Override
            protected void readValue(Kryo kryo, Input input, SQLTime dvd) throws StandardException {
                dvd.setValue(new Time(input.readLong()));
            }
        },37);
        instance.register(SQLTimestamp.class,new DataValueDescriptorSerializer<SQLTimestamp>(){
            @Override
            protected void writeValue(Kryo kryo, Output output, SQLTimestamp object) throws StandardException {
                output.writeLong(object.getTimestamp(null).getTime());
            }

            @Override
            protected void readValue(Kryo kryo, Input input, SQLTimestamp dvd) throws StandardException {
                dvd.setValue(new Timestamp(input.readLong()));
            }
        },38);
        instance.register(SQLSmallint.class,new DataValueDescriptorSerializer<SQLSmallint>() {
            @Override
            protected void writeValue(Kryo kryo, Output output, SQLSmallint object) throws StandardException {
                output.writeShort(object.getShort());
            }
            @Override
            protected void readValue(Kryo kryo, Input input, SQLSmallint dvd) throws StandardException {
                dvd.setValue(input.readShort());
            }
        },39);
        instance.register(SQLTinyint.class,new DataValueDescriptorSerializer<SQLTinyint>() {
            @Override
            protected void writeValue(Kryo kryo, Output output, SQLTinyint object) throws StandardException {
                output.writeByte(object.getByte());
            }
            @Override
            protected void readValue(Kryo kryo, Input input, SQLTinyint dvd) throws StandardException {
                dvd.setValue(input.readByte());
            }
        },40);

        instance.register(SQLLongint.class,new DataValueDescriptorSerializer<SQLLongint>() {
            @Override
            protected void writeValue(Kryo kryo, Output output, SQLLongint object) throws StandardException {
                output.writeLong(object.getLong());
            }
            @Override
            protected void readValue(Kryo kryo, Input input, SQLLongint dvd) throws StandardException {
                dvd.setValue(input.readLong());
            }
        },41);
        instance.register(SQLReal.class, new DataValueDescriptorSerializer<SQLReal>() {
            @Override
            protected void writeValue(Kryo kryo, Output output, SQLReal object) throws StandardException {
                output.writeFloat(object.getFloat());
            }

            @Override
            protected void readValue(Kryo kryo, Input input, SQLReal dvd) throws StandardException {
                dvd.setValue(input.readFloat());
            }
        },42);
        instance.register(SQLRef.class,EXTERNALIZABLE_SERIALIZER,43);
        instance.register(LazyDataValueDescriptor.class,EXTERNALIZABLE_SERIALIZER,44);
        instance.register(LazyNumberDataValueDescriptor.class,EXTERNALIZABLE_SERIALIZER,45);
        instance.register(LazyStringDataValueDescriptor.class,EXTERNALIZABLE_SERIALIZER,46);
        instance.register(TaskStatus.class,EXTERNALIZABLE_SERIALIZER,47);


        //register Activation-related classes
        instance.register(ActivationSerializer.ArrayFieldStorage.class,EXTERNALIZABLE_SERIALIZER,48);
        instance.register(ActivationSerializer.DataValueStorage.class,EXTERNALIZABLE_SERIALIZER,49);
        instance.register(ActivationSerializer.ExecRowStorage.class,EXTERNALIZABLE_SERIALIZER,50);
        instance.register(ActivationSerializer.SerializableStorage.class,EXTERNALIZABLE_SERIALIZER,51);
        instance.register(ActivationSerializer.CachedOpFieldStorage.class,EXTERNALIZABLE_SERIALIZER,151);

        instance.register(SpliceObserverInstructions.class,EXTERNALIZABLE_SERIALIZER,52);
        instance.register(SpliceObserverInstructions.ActivationContext.class,EXTERNALIZABLE_SERIALIZER,53);
        instance.register(GenericParameterValueSet.class,EXTERNALIZABLE_SERIALIZER,54);
        instance.register(GenericParameter.class,EXTERNALIZABLE_SERIALIZER,55);
        instance.register(SchemaDescriptor.class,EXTERNALIZABLE_SERIALIZER,56);
        instance.register(SpliceRuntimeContext.class,EXTERNALIZABLE_SERIALIZER,57);

        instance.register(ProjectRestrictOperation.class, EXTERNALIZABLE_SERIALIZER,58);
        instance.register(TableScanOperation.class, EXTERNALIZABLE_SERIALIZER,59);
        instance.register(BulkTableScanOperation.class, EXTERNALIZABLE_SERIALIZER,60);
        instance.register(GroupedAggregateOperation.class, EXTERNALIZABLE_SERIALIZER,61);
        instance.register(DistinctScanOperation.class, EXTERNALIZABLE_SERIALIZER,62);
        instance.register(DistinctScalarAggregateOperation.class, EXTERNALIZABLE_SERIALIZER,63);
        instance.register(IndexRowToBaseRowOperation.class, EXTERNALIZABLE_SERIALIZER,64);
        instance.register(SortOperation.class, EXTERNALIZABLE_SERIALIZER,65);
        instance.register(UnionOperation.class, EXTERNALIZABLE_SERIALIZER,66);
        instance.register(UpdateOperation.class, EXTERNALIZABLE_SERIALIZER,67);
        instance.register(UpdateConstantOperation.class, EXTERNALIZABLE_SERIALIZER,68);
        instance.register(InsertOperation.class, EXTERNALIZABLE_SERIALIZER,69);
        instance.register(DeleteOperation.class, EXTERNALIZABLE_SERIALIZER,70);
        instance.register(MergeSortJoinOperation.class, EXTERNALIZABLE_SERIALIZER,71);
        instance.register(NestedLoopJoinOperation.class, EXTERNALIZABLE_SERIALIZER,72);
        instance.register(NestedLoopLeftOuterJoinOperation.class, EXTERNALIZABLE_SERIALIZER,73);
        instance.register(MergeSortLeftOuterJoinOperation.class, EXTERNALIZABLE_SERIALIZER,74);
        instance.register(ScalarAggregateOperation.class, EXTERNALIZABLE_SERIALIZER,75);
        instance.register(NormalizeOperation.class, EXTERNALIZABLE_SERIALIZER,76);
        instance.register(AnyOperation.class, EXTERNALIZABLE_SERIALIZER,77);
        instance.register(RowOperation.class, EXTERNALIZABLE_SERIALIZER,78);
        instance.register(OnceOperation.class, EXTERNALIZABLE_SERIALIZER,79);
        instance.register(BroadcastJoinOperation.class, EXTERNALIZABLE_SERIALIZER,81);
        instance.register(BroadcastLeftOuterJoinOperation.class, EXTERNALIZABLE_SERIALIZER,82);
        instance.register(DerbyOperationInformation.class,EXTERNALIZABLE_SERIALIZER,83);
        instance.register(DerbyScanInformation.class,EXTERNALIZABLE_SERIALIZER,89);
        instance.register(CachedOperation.class,EXTERNALIZABLE_SERIALIZER,150);

        instance.register(PC_XenaVersion.class,EXTERNALIZABLE_SERIALIZER,84);
        instance.register(BasicUUID.class,EXTERNALIZABLE_SERIALIZER,85);
        instance.register(IndexDescriptorImpl.class,EXTERNALIZABLE_SERIALIZER,86);
        instance.register(FormatableHashtable.class,EXTERNALIZABLE_SERIALIZER,87);
        instance.register(FormatableIntHolder.class,EXTERNALIZABLE_SERIALIZER,88);
        instance.register(FormatableIntHolder[].class,91);
        instance.register(DD_Version.class,EXTERNALIZABLE_SERIALIZER,90);
        instance.register(DDdependableFinder.class,EXTERNALIZABLE_SERIALIZER,92);
        instance.register(DDColumnDependableFinder.class,EXTERNALIZABLE_SERIALIZER,93);
        instance.register(byte[].class,94);
        instance.register(boolean[].class,95);
        instance.register(int[].class,96);
        instance.register(double[].class,97);
        instance.register(float[].class,98);
        instance.register(long[].class,99);
        instance.register(Collections.emptyList().getClass(),100);
        instance.register(Collections.unmodifiableList(new LinkedList()).getClass(), UNMODIFIABLE_COLLECTIONS_SERIALIZER, 152);
        instance.register(Collections.unmodifiableList(new ArrayList()).getClass(), UNMODIFIABLE_COLLECTIONS_SERIALIZER, 153);
        instance.register(CursorInfo.class,EXTERNALIZABLE_SERIALIZER,101);
        instance.register(GenericResultDescription.class,EXTERNALIZABLE_SERIALIZER,102);
        instance.register(GenericColumnDescriptor.class,EXTERNALIZABLE_SERIALIZER,103);
        instance.register(ReferencedColumnsDescriptorImpl.class,EXTERNALIZABLE_SERIALIZER,104);
        instance.register(ErrorTransport.class,EXTERNALIZABLE_SERIALIZER,105);
        instance.register(DefaultInfoImpl.class,EXTERNALIZABLE_SERIALIZER,106);

        instance.register(BigDecimal.class,107);
        instance.register(HashMap.class,108);
        instance.register(TreeMap.class,109);
        instance.register(ArrayList.class,110);
        instance.register(LinkedList.class,111);

        instance.register(FormatableArrayHolder.class,EXTERNALIZABLE_SERIALIZER,112);
        instance.register(IndexColumnOrder.class,EXTERNALIZABLE_SERIALIZER,113);
        instance.register(HBaseRowLocation.class,EXTERNALIZABLE_SERIALIZER,114);
        instance.register(TaskStats.class,EXTERNALIZABLE_SERIALIZER,115);
        instance.register(AggregatorInfoList.class,EXTERNALIZABLE_SERIALIZER,117);
        instance.register(AggregatorInfo.class,EXTERNALIZABLE_SERIALIZER,118);
        instance.register(UserType.class,EXTERNALIZABLE_SERIALIZER,119);
        instance.register(InsertConstantOperation.class,EXTERNALIZABLE_SERIALIZER,120);
        instance.register(DerbyDMLWriteInfo.class,EXTERNALIZABLE_SERIALIZER,121);
        instance.register(DeleteConstantOperation.class,EXTERNALIZABLE_SERIALIZER,122);
        instance.register(FormatableLongHolder.class,EXTERNALIZABLE_SERIALIZER,123);
        instance.register(FormatableInstanceGetter.class,EXTERNALIZABLE_SERIALIZER,124);
        instance.register(SpliceRuntimeContext.Path.class,EXTERNALIZABLE_SERIALIZER,125);
        instance.register(IndexRowGenerator.class,EXTERNALIZABLE_SERIALIZER,126);
        instance.register(MultiProbeDerbyScanInformation.class,EXTERNALIZABLE_SERIALIZER,127);
        instance.register(MultiProbeTableScanOperation.class,EXTERNALIZABLE_SERIALIZER,128);
        instance.register(DistinctGroupedAggregateOperation.class,EXTERNALIZABLE_SERIALIZER,129);



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
        },130);

        instance.register(SQLClob.class,new DataValueDescriptorSerializer<SQLClob>() {
            @Override
            protected void writeValue(Kryo kryo, Output output, SQLClob object) throws StandardException {
                output.writeString(object.getString());
            }
            @Override
            protected void readValue(Kryo kryo, Input input, SQLClob dvd) {
                dvd.setValue(input.readString());
            }
        },131);
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
        },132);
        instance.register(SynonymAliasInfo.class, EXTERNALIZABLE_SERIALIZER,133);
        instance.register(CursorTableReference.class, EXTERNALIZABLE_SERIALIZER,134);
        instance.register(FKInfo.class, EXTERNALIZABLE_SERIALIZER,135);
        instance.register(DerbyAggregateContext.class,EXTERNALIZABLE_SERIALIZER,136);
        instance.register(DerbyGroupedAggregateContext.class,EXTERNALIZABLE_SERIALIZER,137);
        instance.register(LastIndexKeyOperation.class,EXTERNALIZABLE_SERIALIZER,138);
        instance.register(MergeJoinOperation.class, EXTERNALIZABLE_SERIALIZER,139);

        instance.register(AggregateAliasInfo.class, EXTERNALIZABLE_SERIALIZER,140);
        instance.register(UserDefinedAggregator.class, EXTERNALIZABLE_SERIALIZER,141);
        instance.register(BulkWrite.class,EXTERNALIZABLE_SERIALIZER,142);
        instance.register(KVPair.class,EXTERNALIZABLE_SERIALIZER,143);
        instance.register(SpliceStddevPop.class,144);
        instance.register(SpliceStddevSamp.class,145);
        instance.register(Properties.class, new MapSerializer(), 146);

//        instance.register(com.splicemachine.derby.impl.sql.execute.ValueRow.class,EXTERNALIZABLE_SERIALIZER,147);
//        instance.register(com.splicemachine.derby.impl.sql.execute.IndexRow.class,EXTERNALIZABLE_SERIALIZER,148);
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
                },149);

        instance.register(LazyTimestampDataValueDescriptor.class,EXTERNALIZABLE_SERIALIZER,154);
        instance.register(ActivationSerializer.OperationResultSetStorage.class,EXTERNALIZABLE_SERIALIZER,155);
        instance.register(ByteSlice.class,EXTERNALIZABLE_SERIALIZER,156);
				instance.register(ZkTask.class,EXTERNALIZABLE_SERIALIZER,157);
				instance.register(SinkTask.class,EXTERNALIZABLE_SERIALIZER,158);
				instance.register(CreateIndexTask.class,EXTERNALIZABLE_SERIALIZER,159);
				instance.register(PopulateIndexTask.class,EXTERNALIZABLE_SERIALIZER,160);
				instance.register(ImportTask.class,EXTERNALIZABLE_SERIALIZER,161);
				instance.register(ImportContext.class,EXTERNALIZABLE_SERIALIZER,162);
				instance.register(ColumnContext.class,EXTERNALIZABLE_SERIALIZER,163);
				instance.register(FileImportReader.class,EXTERNALIZABLE_SERIALIZER,164);
				instance.register(TentativeIndexDesc.class,new FieldSerializer(instance,TentativeIndexDesc.class),166);
				instance.register(TentativeDropColumnDesc.class,new FieldSerializer(instance,TentativeDropColumnDesc.class),167);
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
				},168);
				instance.register(DDLChangeType.class,new DefaultSerializers.EnumSerializer(DDLChangeType.class),169);
				instance.register(DropColumnTask.class,EXTERNALIZABLE_SERIALIZER,170);
				instance.register(ColumnInfo.class,EXTERNALIZABLE_SERIALIZER,171);
				instance.register(ColumnInfo[].class,172);
				instance.register(LoadConglomerateTask.class,EXTERNALIZABLE_SERIALIZER,173);
				instance.register(MergeLeftOuterJoinOperation.class, EXTERNALIZABLE_SERIALIZER,174);
        instance.register(DataValueDescriptor[].class,175);
        instance.register(Tuple2.class, new Serializer<Tuple2>() {
            @Override
            public Tuple2 read(Kryo kryo, Input input, Class type) {
                Object first = kryo.readClassAndObject(input);
                Object second = kryo.readClassAndObject(input);
                return new Tuple2(first, second);
            }

            @Override
            public void write(Kryo kryo, Output output, Tuple2 object) {
                kryo.writeClassAndObject(output, object._1());
                kryo.writeClassAndObject(output, object._2());
            }
        }, 176);
        instance.register(MapStatus.class, 177);
        instance.register(StorageLevel.class, 178);
    //        instance.register(PutBlock.class, 179);
    //        instance.register(GotBlock.class, 180);
        instance.register(BlockManagerId.class, 181);
        instance.register(IndexValueRow.class, new SparkValueRowSerializer<IndexValueRow>() {
            @Override
            protected IndexValueRow newType(int size) {
                return new IndexValueRow(new ValueRow(size));
            }
        },182);
        instance.register(scala.Array.class, 183);
        instance.register(WrappedArray.class, 184);
        instance.register(WrappedArray.ofRef.class, new Serializer<WrappedArray.ofRef>() {
            @Override
            public WrappedArray.ofRef read(Kryo kryo, Input input, Class type) {
                Object[] array = kryo.readObject(input, Object[].class);
                return new WrappedArray.ofRef(array);
            }

            @Override
            public void write(Kryo kryo, Output output, WrappedArray.ofRef object) {
                kryo.writeObject(output, object.array());
            }
        }, 185);
        instance.register(ArrayBuffer.class, new TraversableSerializer(false, ArrayBuffer.canBuildFrom()), 186);
        instance.register(ArrayBuffer[].class, 187);
        Registration register = instance.register(SerializableWritable.class, new Serializer<SerializableWritable>() {
            @Override
            public void write(Kryo kryo, Output output, SerializableWritable object) {
                try {
                    ObjectOutputStream oos = new ObjectOutputStream(output);
                    oos.writeObject(object);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }

            @Override
            public SerializableWritable read(Kryo kryo, Input input, Class type) {
                ObjectInputStream ois = null;
                SerializableWritable sw = null;
                try {
                    ois = new ObjectInputStream(input);
                    sw = (SerializableWritable) ois.readObject();
                } catch (IOException e) {
                    throw new RuntimeException(e);
                } catch (ClassNotFoundException e) {
                    throw new RuntimeException(e);
                }
                return sw;
            }
        }, 188);
//        instance.register(WrappedArray.ofBoolean.class, new Serializer<WrappedArray.ofBoolean>() {
//            @Override
//            public WrappedArray.ofBoolean read(Kryo kryo, Input input, Class type) {
//                boolean[] array = kryo.readObject(input, Object[].class);
//                return new WrappedArray.ofBoolean(array);
//            }
//
//            @Override
//            public void write(Kryo kryo, Output output, WrappedArray.ofBoolean object) {
//                kryo.writeObject(output, object.array());
//            }
//        },  186);
//        instance.register(WrappedArray.ofByte.class, 187);
//        instance.register(WrappedArray.ofChar.class, 188);
//        instance.register(WrappedArray.ofDouble.class, 189);
//        instance.register(WrappedArray.ofFloat.class, 190);
//        instance.register(WrappedArray.ofInt.class, 191);
//        instance.register(WrappedArray.ofLong.class, 192);
//        instance.register(WrappedArray.ofShort.class, 193);
//        instance.register(WrappedArray.ofUnit.class, 194);
        instance.register(Object[].class,189);
        instance.register(CompressedMapStatus.class,190);
    }
}