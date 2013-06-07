package com.splicemachine.derby.impl.sql.execute;

import com.splicemachine.derby.impl.sql.execute.serial.DVDSerializer;
import com.splicemachine.utils.SpliceLogUtils;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.services.io.ArrayInputStream;
import org.apache.derby.iapi.services.io.FormatableBitSet;
import org.apache.derby.iapi.types.BooleanDataValue;
import org.apache.derby.iapi.types.DataTypeDescriptor;
import org.apache.derby.iapi.types.DataValueDescriptor;
import org.apache.derby.iapi.types.SQLBoolean;
import org.apache.derby.iapi.types.StringDataValue;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.sql.*;
import java.util.Calendar;

public abstract class LazyDataValueDescriptor implements DataValueDescriptor {

    private static Logger LOG = Logger.getLogger(LazyDataValueDescriptor.class);

    protected byte[] dvdBytes;
    protected DVDSerializer DVDSerializer;
    protected boolean deserialized;

    public LazyDataValueDescriptor(){

    }

    public LazyDataValueDescriptor(DataValueDescriptor dvd, DVDSerializer DVDSerializer){
        this.setDvd(dvd);
        this.DVDSerializer = DVDSerializer;
    }


    protected abstract DataValueDescriptor getDvd();

    protected abstract void setDvd(DataValueDescriptor dvd);


    public void initForDeserialization(byte[] bytes){
        this.dvdBytes = bytes;
        getDvd().setToNull();
        deserialized = false;
    }

    public boolean isSerialized(){
        return dvdBytes != null;
    }

    public boolean isDeserialized(){
        return getDvd() != null && deserialized;
    }

    protected void forceDeserialization()  {
        if( !isDeserialized() && isSerialized()){
            try{
                DVDSerializer.deserialize(dvdBytes, getDvd());
                deserialized = true;
            }catch(Exception e){
                SpliceLogUtils.error(LOG, "Error lazily deserializing bytes", e);
            }
        }
    }

    protected void forceSerialization(){
        if(!isSerialized()){
            try{
                dvdBytes = DVDSerializer.serialize(getDvd());
            }catch(Exception e){
                SpliceLogUtils.error(LOG, "Error serializing DataValueDescriptor to bytes", e);
            }
        }
    }

    protected void resetForSerialization(){
        dvdBytes = null;
    }

    @Override
    public int getLength() throws StandardException {
        forceDeserialization();
        return getDvd().getLength();
    }

    @Override
    public String getString() throws StandardException {
        forceDeserialization();
        return getDvd().getString();
    }

    @Override
    public String getTraceString() throws StandardException {
        forceDeserialization();
        return getDvd().getTraceString();
    }

    @Override
    public boolean getBoolean() throws StandardException {
        forceDeserialization();
        return getDvd().getBoolean();
    }

    @Override
    public byte getByte() throws StandardException {
        forceDeserialization();
        return getDvd().getByte();
    }

    @Override
    public short getShort() throws StandardException {
        forceDeserialization();
        return getDvd().getShort();
    }

    @Override
    public int getInt() throws StandardException {
        forceDeserialization();
        return getDvd().getInt();
    }

    @Override
    public long getLong() throws StandardException {
        forceDeserialization();
        return getDvd().getLong();
    }

    @Override
    public float getFloat() throws StandardException {
        forceDeserialization();
        return getDvd().getFloat();
    }

    @Override
    public double getDouble() throws StandardException {
        forceDeserialization();
        return getDvd().getDouble();
    }

    @Override
    public int typeToBigDecimal() throws StandardException {
        forceDeserialization();
        return getDvd().typeToBigDecimal();
    }

    @Override
    public byte[] getBytes() throws StandardException {
        forceSerialization();
        return dvdBytes;
    }

    @Override
    public Date getDate(Calendar cal) throws StandardException {
        forceDeserialization();
        return getDvd().getDate(cal);
    }

    @Override
    public Time getTime(Calendar cal) throws StandardException {
        forceDeserialization();
        return getDvd().getTime(cal);
    }

    @Override
    public Timestamp getTimestamp(Calendar cal) throws StandardException {
        forceDeserialization();
        return getDvd().getTimestamp(cal);
    }

    @Override
    public Object getObject() throws StandardException {
        forceDeserialization();
        return getDvd().getObject();
    }

    @Override
    public InputStream getStream() throws StandardException {
        forceDeserialization();
        return getDvd().getStream();
    }

    @Override
    public boolean hasStream() {
        forceDeserialization();
        return getDvd().hasStream();
    }

    @Override
    public DataValueDescriptor recycle() {
        return null;
    }

    @Override
    public void setValueFromResultSet(ResultSet resultSet, int colNumber, boolean isNullable) throws StandardException, SQLException {
        resetForSerialization();
        getDvd().setValueFromResultSet(resultSet, colNumber, isNullable);
    }

    @Override
    public void setInto(PreparedStatement ps, int position) throws SQLException, StandardException {
        resetForSerialization();
        getDvd().setInto(ps, position);
    }

    @Override
    public void setInto(ResultSet rs, int position) throws SQLException, StandardException {
        getDvd().setInto(rs, position);
    }

    @Override
    public void setValue(int theValue) throws StandardException {
        resetForSerialization();
        getDvd().setValue(theValue);
    }

    @Override
    public void setValue(double theValue) throws StandardException {
        resetForSerialization();
        getDvd().setValue(theValue);
    }

    @Override
    public void setValue(float theValue) throws StandardException {
        resetForSerialization();
        getDvd().setValue(theValue);
    }

    @Override
    public void setValue(short theValue) throws StandardException {
        resetForSerialization();
        getDvd().setValue(theValue);
    }

    @Override
    public void setValue(long theValue) throws StandardException {
        resetForSerialization();
        getDvd().setValue(theValue);
    }

    @Override
    public void setValue(byte theValue) throws StandardException {
        resetForSerialization();
        getDvd().setValue(theValue);
    }

    @Override
    public void setValue(boolean theValue) throws StandardException {
        resetForSerialization();
        getDvd().setValue(theValue);
    }

    @Override
    public void setValue(Object theValue) throws StandardException {
        resetForSerialization();
        getDvd().setValue(theValue);
    }

    @Override
    public void setValue(byte[] theValue) throws StandardException {
        resetForSerialization();
        getDvd().setValue(theValue);
    }

    @Override
    public void setBigDecimal(Number bigDecimal) throws StandardException {
        resetForSerialization();
        getDvd().setValue(bigDecimal);
    }

    @Override
    public void setValue(String theValue) throws StandardException {
        resetForSerialization();
        getDvd().setValue(theValue);
    }

    @Override
    public void setValue(Blob theValue) throws StandardException {
        resetForSerialization();
        getDvd().setValue(theValue);
    }

    @Override
    public void setValue(Clob theValue) throws StandardException {
        resetForSerialization();
        getDvd().setValue(theValue);
    }

    @Override
    public void setValue(Time theValue) throws StandardException {
        resetForSerialization();
        getDvd().setValue(theValue);
    }

    @Override
    public void setValue(Time theValue, Calendar cal) throws StandardException {
        resetForSerialization();
        getDvd().setValue(theValue, cal);
    }

    @Override
    public void setValue(Timestamp theValue) throws StandardException {
        resetForSerialization();
        getDvd().setValue(theValue);
    }

    @Override
    public void setValue(Timestamp theValue, Calendar cal) throws StandardException {
        resetForSerialization();
        getDvd().setValue(theValue, cal);
    }

    @Override
    public void setValue(Date theValue) throws StandardException {
        resetForSerialization();
        getDvd().setValue(theValue);
    }

    @Override
    public void setValue(Date theValue, Calendar cal) throws StandardException {
        resetForSerialization();
        getDvd().setValue(theValue, cal);
    }

    @Override
    public void setValue(DataValueDescriptor theValue) throws StandardException {
        resetForSerialization();
        getDvd().setValue(theValue);
    }

    @Override
    public void setToNull() {
        resetForSerialization();
        getDvd().setToNull();
    }

    @Override
    public void normalize(DataTypeDescriptor dtd, DataValueDescriptor source) throws StandardException {
        resetForSerialization();
        getDvd().normalize(dtd, source);
    }

    @Override
    public BooleanDataValue isNullOp() {
        forceDeserialization();
        return getDvd().isNullOp();
    }

    @Override
    public BooleanDataValue isNotNull() {
        forceDeserialization();
        return getDvd().isNotNull();
    }

    @Override
    public String getTypeName() {
        return getDvd().getTypeName();
    }

    @Override
    public void setObjectForCast(Object value, boolean instanceOfResultType, String resultTypeClassName) throws StandardException {
        resetForSerialization();
        getDvd().setObjectForCast(value, instanceOfResultType, resultTypeClassName);
    }

    @Override
    public void readExternalFromArray(ArrayInputStream ais) throws IOException, ClassNotFoundException {
        resetForSerialization();
        getDvd().readExternalFromArray(ais);
    }

    @Override
    public int typePrecedence() {
        return getDvd().typePrecedence();
    }

    protected DataValueDescriptor unwrap(DataValueDescriptor dvd){

        DataValueDescriptor unwrapped = null;

        if(dvd instanceof LazyDataValueDescriptor){
            LazyDataValueDescriptor ldvd = (LazyDataValueDescriptor) dvd;
            ldvd.forceDeserialization();
            unwrapped = ldvd.getDvd();
        }else{
            unwrapped = dvd;
        }

        return unwrapped;
    }

    @Override
    public BooleanDataValue equals(DataValueDescriptor left, DataValueDescriptor right) throws StandardException {
        return SQLBoolean.truthValue(left, right, left.compare(right) == 0);
    }

    @Override
    public BooleanDataValue notEquals(DataValueDescriptor left, DataValueDescriptor right) throws StandardException {
        return SQLBoolean.truthValue(left, right, left.compare(right) != 0);
    }

    @Override
    public BooleanDataValue lessThan(DataValueDescriptor left, DataValueDescriptor right) throws StandardException {
        return SQLBoolean.truthValue(left, right, left.compare(right) < 0);
    }

    @Override
    public BooleanDataValue greaterThan(DataValueDescriptor left, DataValueDescriptor right) throws StandardException {
        return SQLBoolean.truthValue(left, right, left.compare(right) > 0);
    }

    @Override
    public BooleanDataValue lessOrEquals(DataValueDescriptor left, DataValueDescriptor right) throws StandardException {
        return SQLBoolean.truthValue(left, right, left.compare(right) <= 0);
    }

    @Override
    public BooleanDataValue greaterOrEquals(DataValueDescriptor left, DataValueDescriptor right) throws StandardException {
        return SQLBoolean.truthValue(left, right, left.compare(right) >= 0);
    }

    @Override
    public DataValueDescriptor coalesce(DataValueDescriptor[] list, DataValueDescriptor returnValue) throws StandardException {
        forceDeserialization();
        return getDvd().coalesce(list, returnValue);
    }

    @Override
    public BooleanDataValue in(DataValueDescriptor left, DataValueDescriptor[] inList, boolean orderedList) throws StandardException {
        forceDeserialization();
        return getDvd().in(left, inList, orderedList);
    }

    @Override
    public int compare(DataValueDescriptor other) throws StandardException {

        int result;

        boolean thisIsNull = this.isNull();
        boolean otherIsNull = other.isNull();

       if(thisIsNull || otherIsNull){
           if(thisIsNull && otherIsNull){
               result = 0;
           }else if(thisIsNull){
               result = 1;
           }else{
               result = -1;
           }
       }else if(other.isLazy() && this.isSameType(other)){
           result = Bytes.compareTo(this.getBytes(), other.getBytes());
       }else{
           forceDeserialization();
           result = getDvd().compare(other);
       }

        return result;
    }

    private boolean isSameType(DataValueDescriptor dvd){
        return this.getTypeFormatId() == dvd.getTypeFormatId();
    }

    @Override
    public int compare(DataValueDescriptor other, boolean nullsOrderedLow) throws StandardException {

        int result;

        boolean isThisNull = this.isNull();
        boolean isOtherNull = other.isNull();

        if( isThisNull || isOtherNull){

            if(isThisNull && isOtherNull){

                result = 0;

            }else if( isThisNull ){

                result = nullsOrderedLow ? -1 : 1;

            }else{

                result = nullsOrderedLow ? 1 : -1;

            }

        } else {
            result = compare(other);
        }

        return result;
    }

    @Override
    public boolean compare(int op, DataValueDescriptor other, boolean orderedNulls, boolean unknownRV) throws StandardException {

        return compareResultWithOperator(op, compare(other));

    }

    @Override
    public boolean compare(int op, DataValueDescriptor other, boolean orderedNulls, boolean nullsOrderedLow, boolean unknownRV) throws StandardException {

        return compareResultWithOperator(op, compare(other, nullsOrderedLow));

    }

    private boolean compareResultWithOperator(int op, int compareResult){

        boolean result;

        switch(op)
        {
            case ORDER_OP_LESSTHAN:
                result = (compareResult < 0);   // this <  other
                break;
            case ORDER_OP_EQUALS:
                result = (compareResult == 0);  // this == other
                break;
            case ORDER_OP_LESSOREQUALS:
                result = (compareResult <= 0);  // this <= other
                break;
            case ORDER_OP_GREATERTHAN:
                result = (compareResult > 0);   // this > other
                break;
            case ORDER_OP_GREATEROREQUALS:
                result = (compareResult >= 0);  // this >= other
                break;
            default:
                result = false;
        }

        return result;
    }

    @Override
    public void setValue(InputStream theStream, int valueLength) throws StandardException {
        resetForSerialization();
        getDvd().setValue(theStream, valueLength);
    }

    @Override
    public void checkHostVariable(int declaredLength) throws StandardException {
        getDvd().checkHostVariable(declaredLength);
    }

    @Override
    public int estimateMemoryUsage() {
        forceDeserialization();
        return getDvd().estimateMemoryUsage();
    }

    @Override
    public boolean isLazy() {
        return true;
    }

    @Override
    public boolean isNull() {

        boolean isWrappedDvdNull = getDvd().isNull();

        boolean result;

        if(isWrappedDvdNull){

            result = dvdBytes == null || dvdBytes.length == 0;

        }else{

            result = false;

        }

        return result;

    }

    @Override
    public void restoreToNull() {
        getDvd().restoreToNull();
        resetForSerialization();
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {

        out.writeBoolean(getDvd() != null);

        if(getDvd() != null){
            out.writeUTF(getDvd().getClass().getCanonicalName());
        }

        out.writeBoolean(dvdBytes != null);

        if(isSerialized()){
            out.writeObject(new FormatableBitSet(dvdBytes));
        }

        out.writeUTF(DVDSerializer.getClass().getCanonicalName());

        out.writeBoolean(deserialized);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {

        if(in.readBoolean()){
            DataValueDescriptor dvd = (DataValueDescriptor) createClassInstance(in.readUTF());

            if(dvd instanceof StringDataValue){
                setDvd((StringDataValue) dvd);
            }else{
                setDvd(dvd);
            }

            getDvd().setToNull();
        }

        if(in.readBoolean()){
            FormatableBitSet fbs = (FormatableBitSet) in.readObject();
            dvdBytes = fbs.getByteArray();
        }

        DVDSerializer = (DVDSerializer) createClassInstance(in.readUTF());

        deserialized = in.readBoolean();
    }

    protected Object createClassInstance(String className) throws IOException {
        try{
            return Class.forName(className).newInstance();
        }catch (Exception e){
            throw new IOException("Error Instantiating Class: " + className, e);
        }
    }

    @Override
    public int getTypeFormatId() {
        return getDvd().getTypeFormatId();
    }

    @Override
    public int hashCode() {
        forceDeserialization();
        return getDvd().hashCode();
    }

    @Override
    public boolean equals(Object o) {

        forceDeserialization();

        boolean result = false;

        if(getDvd() == null && o instanceof LazyDataValueDescriptor){

            result = Bytes.equals(dvdBytes, ((LazyDataValueDescriptor) o).dvdBytes);

        }else if (getDvd() != null && o instanceof DataValueDescriptor ){

            result = getDvd().equals(unwrap( (DataValueDescriptor) o));

        }

        return result;

    }

    protected DVDSerializer getDVDSerializer(){
        return DVDSerializer;
    }
}

