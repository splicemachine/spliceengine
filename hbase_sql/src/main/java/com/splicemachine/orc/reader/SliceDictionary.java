package com.splicemachine.orc.reader;

import io.airlift.slice.Slice;
import org.apache.parquet.column.Dictionary;
import org.apache.parquet.column.Encoding;
import org.apache.parquet.io.api.Binary;
import org.apache.spark.memory.MemoryMode;
import org.apache.spark.sql.execution.vectorized.ColumnVector;
import org.apache.spark.sql.types.DataTypes;

import java.util.Arrays;


/**
 *
 * This is a hack, using parquet dictionary for ORC?
 *
 *
 */
public class SliceDictionary extends Dictionary {
    Slice[] slice;
    public SliceDictionary(Slice[] slice) {
        // Encoding Does not matter for us...
        super(null);
        this.slice = slice;
    }

    @Override
    public Encoding getEncoding() {
        return super.getEncoding();
    }

    @Override
    public Binary decodeToBinary(int id) {
        return Binary.fromConstantByteBuffer(slice[id].toByteBuffer());
    }


    @Override
    public int decodeToInt(int id) {
        return super.decodeToInt(id);
    }

    @Override
    public long decodeToLong(int id) {
        return super.decodeToLong(id);
    }

    @Override
    public float decodeToFloat(int id) {
        return super.decodeToFloat(id);
    }

    @Override
    public double decodeToDouble(int id) {
        return super.decodeToDouble(id);
    }

    @Override
    public boolean decodeToBoolean(int id) {
        return super.decodeToBoolean(id);
    }

    @Override
    public int getMaxId() {
        return 0;
    }

    public int size() {
        return slice.length;
    }

    @Override
    public boolean equals(Object obj) {
        assert obj != null;
        return Arrays.equals(slice,((SliceDictionary) obj).slice);
    }
}
