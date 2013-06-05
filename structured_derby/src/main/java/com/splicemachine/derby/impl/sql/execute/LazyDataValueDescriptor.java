package com.splicemachine.derby.impl.sql.execute;

import com.splicemachine.derby.impl.sql.execute.serial.DVDSerializer;
import com.splicemachine.utils.SpliceLogUtils;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.services.io.ArrayInputStream;
import org.apache.derby.iapi.services.io.FormatableBitSet;
import org.apache.derby.iapi.types.BooleanDataValue;
import org.apache.derby.iapi.types.DataTypeDescriptor;
import org.apache.derby.iapi.types.DataValueDescriptor;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.sql.*;
import java.util.Calendar;

public class LazyDataValueDescriptor implements DataValueDescriptor {

    private static Logger LOG = Logger.getLogger(LazyDataValueDescriptor.class);

    private DataValueDescriptor dvd;

    protected byte[] dvdBytes;
    protected DVDSerializer DVDSerializer;
    protected boolean deserialized;

    public LazyDataValueDescriptor(){

    }

    public LazyDataValueDescriptor(DataValueDescriptor dvd, DVDSerializer DVDSerializer){
        this.setDvd(dvd);
        this.DVDSerializer = DVDSerializer;
    }

    public void initForDeserialization(byte[] bytes){
        this.dvdBytes = bytes;
        dvd.setToNull();
        deserialized = false;
    }

    public boolean isSerialized(){
        return dvdBytes != null;
    }

    public boolean isDeserialized(){
        return dvd != null && deserialized;
    }

    protected void forceDeserialization()  {
        if( !isDeserialized() && isSerialized()){
            try{
                DVDSerializer.deserialize(dvdBytes, dvd);
                deserialized = true;
            }catch(Exception e){
                SpliceLogUtils.error(LOG, "Error lazily deserializing bytes", e);
            }
        }
    }

    protected void forceSerialization(){
        if(!isSerialized()){
            try{
                dvdBytes = DVDSerializer.serialize(dvd);
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
        return dvd.getLength();
    }

    @Override
    public String getString() throws StandardException {
        forceDeserialization();
        return dvd.getString();
    }

    @Override
    public String getTraceString() throws StandardException {
        forceDeserialization();
        return dvd.getTraceString();
    }

    @Override
    public boolean getBoolean() throws StandardException {
        forceDeserialization();
        return dvd.getBoolean();
    }

    @Override
    public byte getByte() throws StandardException {
        forceDeserialization();
        return dvd.getByte();
    }

    @Override
    public short getShort() throws StandardException {
        forceDeserialization();
        return dvd.getShort();
    }

    @Override
    public int getInt() throws StandardException {
        forceDeserialization();
        return dvd.getInt();
    }

    @Override
    public long getLong() throws StandardException {
        forceDeserialization();
        return dvd.getLong();
    }

    @Override
    public float getFloat() throws StandardException {
        forceDeserialization();
        return dvd.getFloat();
    }

    @Override
    public double getDouble() throws StandardException {
        forceDeserialization();
        return dvd.getDouble();
    }

    @Override
    public int typeToBigDecimal() throws StandardException {
        forceDeserialization();
        return dvd.typeToBigDecimal();
    }

    @Override
    public byte[] getBytes() throws StandardException {
        forceSerialization();
        return dvdBytes;
    }

    @Override
    public Date getDate(Calendar cal) throws StandardException {
        forceDeserialization();
        return dvd.getDate(cal);
    }

    @Override
    public Time getTime(Calendar cal) throws StandardException {
        forceDeserialization();
        return dvd.getTime(cal);
    }

    @Override
    public Timestamp getTimestamp(Calendar cal) throws StandardException {
        forceDeserialization();
        return dvd.getTimestamp(cal);
    }

    @Override
    public Object getObject() throws StandardException {
        forceDeserialization();
        return dvd.getObject();
    }

    @Override
    public InputStream getStream() throws StandardException {
        forceDeserialization();
        return dvd.getStream();
    }

    @Override
    public boolean hasStream() {
        forceDeserialization();
        return dvd.hasStream();
    }

    @Override
    public DataValueDescriptor cloneHolder() {
        return new LazyDataValueDescriptor(dvd.cloneHolder(), DVDSerializer);
    }

    @Override
    public DataValueDescriptor cloneValue(boolean forceMaterialization) {
        return new LazyDataValueDescriptor(dvd.cloneValue(forceMaterialization), DVDSerializer);
    }

    @Override
    public DataValueDescriptor recycle() {
        return null;
    }

    @Override
    public DataValueDescriptor getNewNull() {
        return new LazyDataValueDescriptor(dvd.getNewNull(), DVDSerializer);
    }

    @Override
    public void setValueFromResultSet(ResultSet resultSet, int colNumber, boolean isNullable) throws StandardException, SQLException {
        resetForSerialization();
        dvd.setValueFromResultSet(resultSet, colNumber, isNullable);
    }

    @Override
    public void setInto(PreparedStatement ps, int position) throws SQLException, StandardException {
        resetForSerialization();
        dvd.setInto(ps, position);
    }

    @Override
    public void setInto(ResultSet rs, int position) throws SQLException, StandardException {
        dvd.setInto(rs, position);
    }

    @Override
    public void setValue(int theValue) throws StandardException {
        resetForSerialization();
        dvd.setValue(theValue);
    }

    @Override
    public void setValue(double theValue) throws StandardException {
        resetForSerialization();
        dvd.setValue(theValue);
    }

    @Override
    public void setValue(float theValue) throws StandardException {
        resetForSerialization();
        dvd.setValue(theValue);
    }

    @Override
    public void setValue(short theValue) throws StandardException {
        resetForSerialization();
        dvd.setValue(theValue);
    }

    @Override
    public void setValue(long theValue) throws StandardException {
        resetForSerialization();
        dvd.setValue(theValue);
    }

    @Override
    public void setValue(byte theValue) throws StandardException {
        resetForSerialization();
        dvd.setValue(theValue);
    }

    @Override
    public void setValue(boolean theValue) throws StandardException {
        resetForSerialization();
        dvd.setValue(theValue);
    }

    @Override
    public void setValue(Object theValue) throws StandardException {
        resetForSerialization();
        dvd.setValue(theValue);
    }

    @Override
    public void setValue(byte[] theValue) throws StandardException {
        resetForSerialization();
        dvd.setValue(theValue);
    }

    @Override
    public void setBigDecimal(Number bigDecimal) throws StandardException {
        resetForSerialization();
        dvd.setValue(bigDecimal);
    }

    @Override
    public void setValue(String theValue) throws StandardException {
        resetForSerialization();
        dvd.setValue(theValue);
    }

    @Override
    public void setValue(Blob theValue) throws StandardException {
        resetForSerialization();
        dvd.setValue(theValue);
    }

    @Override
    public void setValue(Clob theValue) throws StandardException {
        resetForSerialization();
        dvd.setValue(theValue);
    }

    @Override
    public void setValue(Time theValue) throws StandardException {
        resetForSerialization();
        dvd.setValue(theValue);
    }

    @Override
    public void setValue(Time theValue, Calendar cal) throws StandardException {
        resetForSerialization();
        dvd.setValue(theValue, cal);
    }

    @Override
    public void setValue(Timestamp theValue) throws StandardException {
        resetForSerialization();
        dvd.setValue(theValue);
    }

    @Override
    public void setValue(Timestamp theValue, Calendar cal) throws StandardException {
        resetForSerialization();
        dvd.setValue(theValue, cal);
    }

    @Override
    public void setValue(Date theValue) throws StandardException {
        resetForSerialization();
        dvd.setValue(theValue);
    }

    @Override
    public void setValue(Date theValue, Calendar cal) throws StandardException {
        resetForSerialization();
        dvd.setValue(theValue, cal);
    }

    @Override
    public void setValue(DataValueDescriptor theValue) throws StandardException {
        resetForSerialization();
        dvd.setValue(theValue);
    }

    @Override
    public void setToNull() {
        resetForSerialization();
        dvd.setToNull();
    }

    @Override
    public void normalize(DataTypeDescriptor dtd, DataValueDescriptor source) throws StandardException {
        resetForSerialization();
        dvd.normalize(dtd, source);
    }

    @Override
    public BooleanDataValue isNullOp() {
        forceDeserialization();
        return dvd.isNullOp();
    }

    @Override
    public BooleanDataValue isNotNull() {
        forceDeserialization();
        return dvd.isNotNull();
    }

    @Override
    public String getTypeName() {
        return dvd.getTypeName();
    }

    @Override
    public void setObjectForCast(Object value, boolean instanceOfResultType, String resultTypeClassName) throws StandardException {
        resetForSerialization();
        dvd.setObjectForCast(value, instanceOfResultType, resultTypeClassName);
    }

    @Override
    public void readExternalFromArray(ArrayInputStream ais) throws IOException, ClassNotFoundException {
        resetForSerialization();
        dvd.readExternalFromArray(ais);
    }

    @Override
    public int typePrecedence() {
        return dvd.typePrecedence();
    }

    protected DataValueDescriptor unwrap(DataValueDescriptor dvd){

        DataValueDescriptor unwrapped = null;

        if(dvd instanceof LazyDataValueDescriptor){
            LazyDataValueDescriptor ldvd = (LazyDataValueDescriptor) dvd;
            ldvd.forceDeserialization();
            unwrapped = ldvd.dvd;
        }else{
            unwrapped = dvd;
        }

        return unwrapped;
    }

    @Override
    public BooleanDataValue equals(DataValueDescriptor left, DataValueDescriptor right) throws StandardException {
        forceDeserialization();
        return dvd.equals(unwrap(left), unwrap(right));
    }

    @Override
    public BooleanDataValue notEquals(DataValueDescriptor left, DataValueDescriptor right) throws StandardException {
        forceDeserialization();
        return dvd.notEquals(unwrap(left), unwrap(right));
    }

    @Override
    public BooleanDataValue lessThan(DataValueDescriptor left, DataValueDescriptor right) throws StandardException {
        forceDeserialization();
        return dvd.lessThan(unwrap(left), unwrap(right));
    }

    @Override
    public BooleanDataValue greaterThan(DataValueDescriptor left, DataValueDescriptor right) throws StandardException {
        forceDeserialization();
        return dvd.greaterThan(unwrap(left), unwrap(right));
    }

    @Override
    public BooleanDataValue lessOrEquals(DataValueDescriptor left, DataValueDescriptor right) throws StandardException {
        forceDeserialization();
        return dvd.lessOrEquals(unwrap(left), unwrap(right));
    }

    @Override
    public BooleanDataValue greaterOrEquals(DataValueDescriptor left, DataValueDescriptor right) throws StandardException {
        forceDeserialization();
        return dvd.greaterOrEquals(unwrap(left), unwrap(right));
    }

    @Override
    public DataValueDescriptor coalesce(DataValueDescriptor[] list, DataValueDescriptor returnValue) throws StandardException {
        forceDeserialization();
        return dvd.coalesce(list, returnValue);
    }

    @Override
    public BooleanDataValue in(DataValueDescriptor left, DataValueDescriptor[] inList, boolean orderedList) throws StandardException {
        forceDeserialization();
        return dvd.in(left, inList, orderedList);
    }

    @Override
    public int compare(DataValueDescriptor other) throws StandardException {
        forceDeserialization();
        if (this.getTypeFormatId() == other.getTypeFormatId())
        	return Bytes.compareTo(this.getBytes(), other.getBytes());
        return dvd.compare(unwrap(other));
    }

    @Override
    public int compare(DataValueDescriptor other, boolean nullsOrderedLow) throws StandardException {
        forceDeserialization();

        return dvd.compare(unwrap(other), nullsOrderedLow);
    }

    @Override
    public boolean compare(int op, DataValueDescriptor other, boolean orderedNulls, boolean unknownRV) throws StandardException {
        forceDeserialization();

        return dvd.compare(op, unwrap(other), orderedNulls, unknownRV);
    }

    @Override
    public boolean compare(int op, DataValueDescriptor other, boolean orderedNulls, boolean nullsOrderedLow, boolean unknownRV) throws StandardException {
        forceDeserialization();

        return dvd.compare(op, unwrap(other), orderedNulls, nullsOrderedLow, unknownRV);
    }

    @Override
    public void setValue(InputStream theStream, int valueLength) throws StandardException {
        resetForSerialization();
        dvd.setValue(theStream, valueLength);
    }

    @Override
    public void checkHostVariable(int declaredLength) throws StandardException {
        dvd.checkHostVariable(declaredLength);
    }

    @Override
    public int estimateMemoryUsage() {
        forceDeserialization();
        return dvd.estimateMemoryUsage();
    }

    @Override
    public boolean isNull() {
        forceDeserialization();
        return dvd == null || dvd.isNull();
    }

    @Override
    public void restoreToNull() {
        dvd.restoreToNull();
        resetForSerialization();
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {

        out.writeBoolean(dvd != null);

        if(dvd != null){
            out.writeObject(dvd);
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
            setDvd((DataValueDescriptor) in.readObject());
        }

        if(in.readBoolean()){
            FormatableBitSet fbs = (FormatableBitSet) in.readObject();
            dvdBytes = fbs.getByteArray();
        }

        try{
            DVDSerializer = (DVDSerializer) Class.forName(in.readUTF()).newInstance();
        }catch(Exception e){
            throw new RuntimeException("Error deserializing serialization class", e);
        }

        deserialized = in.readBoolean();
    }

    @Override
    public int getTypeFormatId() {
        forceDeserialization();
        return dvd.getTypeFormatId();
    }

    protected DataValueDescriptor getDvd() {
        return dvd;
    }

    protected void setDvd(DataValueDescriptor dvd) {
        this.dvd = dvd;
    }

    @Override
    public int hashCode() {
        forceDeserialization();
        return dvd.hashCode();
    }

    @Override
    public boolean equals(Object o) {

        forceDeserialization();

        boolean result = false;

        if(dvd == null && o instanceof LazyDataValueDescriptor){

            result = Bytes.equals(dvdBytes, ((LazyDataValueDescriptor) o).dvdBytes);

        }else if (dvd != null && o instanceof DataValueDescriptor ){

            result = dvd.equals(unwrap( (DataValueDescriptor) o));

        }

        return result;

    }

    protected DVDSerializer getDVDSerializer(){
        return DVDSerializer;
    }
}
