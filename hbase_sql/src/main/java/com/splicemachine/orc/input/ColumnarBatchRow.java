package com.splicemachine.orc.input;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.types.Decimal;
import org.apache.spark.sql.types.DecimalType;
import org.apache.spark.sql.types.StructType;
import scala.collection.Seq;
import scala.collection.immutable.Map;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Date;
import java.sql.Timestamp;
import java.util.List;

/**
 * Created by jleach on 3/22/17.
 */
public class ColumnarBatchRow implements Row {
    InternalRow row;
    StructType structType;
    public ColumnarBatchRow(InternalRow row, StructType structType) {
        this.row = row;
        this.structType = structType;
    }
    @Override
    public int size() {
        return structType.size();
    }

    @Override
    public Timestamp getTimestamp(int i) {
        return new Timestamp(row.getLong(i));
    }

    @Override
    public Object get(int i) {
        return row.get(i,structType.fields()[i].dataType());
    }

    @Override
    public boolean anyNull() {
        return row.anyNull();
    }

    @Override
    public <T> List<T> getList(int i) {
        throw new UnsupportedOperationException("dsfd");
    }

    @Override
    public double getDouble(int i) {
        return row.getDouble(i);
    }

    @Override
    public Seq<Object> toSeq() {
        return row.toSeq(structType);
    }

    @Override
    public <T> T getAs(String fieldName) {
        throw new UnsupportedOperationException("dsfd");
    }

    @Override
    public <T> T getAs(int i) {
        throw new UnsupportedOperationException("dsfd");
    }

    @Override
    public int length() {
        return structType.length();
    }

    @Override
    public <T> Map<String, T> getValuesMap(Seq<String> fieldNames) {
        throw new UnsupportedOperationException("dsfd");
    }

    @Override
    public float getFloat(int i) {
        return row.getFloat(i);
    }

    @Override
    public long getLong(int i) {
        return row.getLong(i);
    }

    @Override
    public <K, V> scala.collection.Map<K, V> getMap(int i) {
        throw new UnsupportedOperationException("dsfd");
    }

    @Override
    public Row copy() {
        return new ColumnarBatchRow(row.copy(),structType);
    }

    @Override
    public <K, V> java.util.Map<K, V> getJavaMap(int i) {
/*        Function1<Object,Object> function = CatalystTypeConverters.createToScalaConverter(structType.fields()[i].dataType());
        ((scala.collection.Map)function.apply(row.getMap(i))).
        return row.getMap(i);
        */
        throw new UnsupportedOperationException("dsfd");
    }

    @Override
    public byte getByte(int i) {
        return row.getByte(i);
    }

    @Override
    public BigDecimal getDecimal(int i) {
        if (isNullAt(i)) {
            return null;
        }
        int precision = ((DecimalType) structType.fields()[i].dataType()).precision();
        int scale = ((DecimalType) structType.fields()[i].dataType()).scale();
        if (precision <= Decimal.MAX_LONG_DIGITS()) {
            return Decimal.apply(getLong(i), precision, scale).toJavaBigDecimal();
        } else {
            byte[] bytes = row.getBinary(i);
            BigInteger bigInteger = new BigInteger(bytes);
            BigDecimal javaDecimal = new BigDecimal(bigInteger, scale);
            return Decimal.apply(javaDecimal, precision, scale).toJavaBigDecimal();
        }
    }

    @Override
    public boolean getBoolean(int i) {
        return row.getBoolean(i);
    }

    @Override
    public <T> Seq<T> getSeq(int i) {
        throw new UnsupportedOperationException("j");
    }

    @Override
    public short getShort(int i) {
        return row.getShort(i);
    }

    @Override
    public Object apply(int i) {
        throw new RuntimeException("Not Implemented Yet");
//        return super.apply(i);
    }

    @Override
    public Row getStruct(int i) {
        StructType secondaryStruct = ((StructType) structType.fields()[i].dataType());
        return new ColumnarBatchRow(row.getStruct(i,secondaryStruct.size()),secondaryStruct);
    }

    @Override
    public String mkString(String start, String sep, String end) {
        return toSeq().mkString(start, sep, end);
    }

    @Override
    public String mkString(String sep) {
        return toSeq().mkString(sep);
    }

    @Override
    public String mkString() {
        return toSeq().mkString();
    }

    @Override
    public Date getDate(int i) {
        return new Date(row.getInt(i));
    }

    @Override
    public StructType schema() {
        return structType;
    }

    @Override
    public int getInt(int i) {
        return row.getInt(i);
    }

    @Override
    public boolean isNullAt(int i) {
        return row.isNullAt(i);
    }

    @Override
    public int fieldIndex(String name) {
        return structType.fieldIndex(name);
    }

    @Override
    public String getString(int i) {
        return row.getString(i);
    }


}
