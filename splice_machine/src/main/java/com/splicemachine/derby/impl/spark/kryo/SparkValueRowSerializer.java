package com.splicemachine.derby.impl.spark.kryo;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.splicemachine.derby.utils.SpliceUtils;
import com.splicemachine.derby.utils.marshall.BareKeyHash;
import com.splicemachine.derby.utils.marshall.DataHash;
import com.splicemachine.derby.utils.marshall.KeyHashDecoder;
import com.splicemachine.derby.utils.marshall.dvd.DescriptorSerializer;
import com.splicemachine.derby.utils.marshall.dvd.VersionedSerializers;
import com.splicemachine.utils.IntArrays;
import com.splicemachine.utils.SpliceLogUtils;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.services.io.StoredFormatIds;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.iapi.types.DataValueDescriptor;
import com.splicemachine.db.iapi.types.DataValueFactoryImpl;
import com.splicemachine.db.iapi.types.SQLDecimal;
import org.apache.log4j.Logger;

import java.util.Arrays;
import java.util.concurrent.ExecutionException;

/**
 * @author Scott Fines
 * Created on: 10/10/13
 */
public abstract class SparkValueRowSerializer<T extends ExecRow> extends Serializer<T> {
    private static Logger LOG = Logger.getLogger(SparkValueRowSerializer.class);
    private LoadingCache<IntArray, DescriptorSerializer[]> serializersCache = CacheBuilder.newBuilder().maximumSize(10).build(
            new CacheLoader<IntArray, DescriptorSerializer[]>() {
                @Override
                public DescriptorSerializer[] load(IntArray key) {
                    return VersionedSerializers.latestVersion(false).getSerializers(key.array);
                }
            }
    );
    private LoadingCache<IntArray, DataValueDescriptor[]> templatesCache = CacheBuilder.newBuilder().maximumSize(10).build(
            new CacheLoader<IntArray, DataValueDescriptor[]>() {
                @Override
                public DataValueDescriptor[] load(IntArray key) {
                    return getRowTemplate(key.array);
                }
            }
    );

    @Override
    public void write(Kryo kryo, Output output, T object) {
        int[] formatIds = SpliceUtils.getFormatIds(object.getRowArray());
        DataHash encoder = getEncoder(formatIds);
        output.writeInt(formatIds.length, true);
        for (int formatId : formatIds) {
            output.writeInt(formatId, true);
        }

        encoder.setRow(object);
        try {
            byte[] encoded = encoder.encode();
            output.writeInt(encoded.length, true);
            output.writeBytes(encoded);
        } catch (Exception e) {
            SpliceLogUtils.logAndThrowRuntime(LOG, "Exception while serializing row " + object, e);
        }
    }

    private static DataValueDescriptor[] getRowTemplate(int[] formatIds) {
        DataValueDescriptor[] row = new DataValueDescriptor[formatIds.length];
        int i = 0;
        for (int formatId : formatIds) {
            // TODO Handle collation ids and DECIMAL
            if (formatId == StoredFormatIds.SQL_DECIMAL_ID) {
                row[i] = new SQLDecimal();
            } else {
                row[i] = DataValueFactoryImpl.getNullDVDWithUCS_BASICcollation(formatId);
            }
            ++i;
        }
        return row;
    }

    @Override
    public T read(Kryo kryo, Input input, Class<T> type) {
        int size = input.readInt(true);

        T instance = newType(size);

        int[] formatIds = new int[size];
        for (int i = 0; i < size; ++i) {
            formatIds[i] = input.readInt(true);
        }
        DataValueDescriptor[] rowTemplate;
        try {
            rowTemplate = getClone(templatesCache.get(new IntArray(formatIds)));
        } catch (ExecutionException e) {
            LOG.error("Error loading template from cache", e);
            rowTemplate = getRowTemplate(formatIds);
        }
        instance.setRowArray(rowTemplate);

        KeyHashDecoder decoder = getEncoder(formatIds).getDecoder();
        int length = input.readInt(true);
        int position = input.position();

        byte[] buffer = input.getBuffer();
        if (position + length < buffer.length) {
            decoder.set(input.getBuffer(), position, length);
            input.setPosition(position + length);
        } else {
            byte[] toDecode = input.readBytes(length);
            decoder.set(toDecode, 0, length);
        }
        try {
            decoder.decode(instance);
        } catch (StandardException e) {
            SpliceLogUtils.logAndThrowRuntime(LOG, "Exception while deserializing row with template " + instance, e);
        }
        return instance;
    }

    private static DataValueDescriptor[] getClone(DataValueDescriptor[] dataValueDescriptors) {
        DataValueDescriptor[] result =  new DataValueDescriptor[dataValueDescriptors.length];
        for (int i = 0; i < dataValueDescriptors.length; ++i) {
            result[i] = dataValueDescriptors[i].getNewNull();
        }
        return result;
    }

    protected abstract T newType(int size);


    private DataHash getEncoder(int[] formatIds) {
        int[] rowColumns = IntArrays.count(formatIds.length);
        DescriptorSerializer[] serializers;
        try {
            serializers = serializersCache.get(new IntArray(formatIds));
        } catch (ExecutionException e) {
            LOG.error("Error loading serializers from serializersCache", e);
            serializers = VersionedSerializers.latestVersion(false).getSerializers(formatIds);
        }

        return BareKeyHash.encoder(rowColumns, null, serializers);
    }

    private static class IntArray {
        private final int[] array;

        IntArray(int[] array) {
            this.array = array;
        }

        @Override
        public int hashCode() {
            return Arrays.hashCode(array);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            IntArray intArray = (IntArray) o;

            if (!Arrays.equals(array, intArray.array)) return false;

            return true;
        }
    }

}