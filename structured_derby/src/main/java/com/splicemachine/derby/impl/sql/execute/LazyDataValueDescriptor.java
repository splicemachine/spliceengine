package com.splicemachine.derby.impl.sql.execute;

import com.splicemachine.derby.impl.sql.execute.serial.DVDSerializer;
import com.splicemachine.derby.utils.Exceptions;
import com.splicemachine.utils.SpliceLogUtils;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.services.io.ArrayInputStream;
import org.apache.derby.iapi.types.BooleanDataValue;
import org.apache.derby.iapi.types.DataTypeDescriptor;
import org.apache.derby.iapi.types.DataValueDescriptor;
import org.apache.derby.iapi.types.SQLBoolean;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.nio.ByteBuffer;
import java.sql.*;
import java.util.Calendar;

/**
 * Lazy subclass of DataValueDescriptor.  Holds a byte array representing the data value
 * and the DVDSerializer for converting to/from bytes.  There is also some duplication
 * in the variables below and in the subclasses. See the specific variables below for
 * rationale.
 */
public abstract class LazyDataValueDescriptor implements DataValueDescriptor {
    private static final long serialVersionUID = 3l;
    private static Logger LOG = Logger.getLogger(LazyDataValueDescriptor.class);

    //Same as the dvd used in the subclasses, another reference is kept here
    //to avoid a covariant getter call on the subclass (slows down performance)
    DataValueDescriptor dvd = null;

    /*
     * One or the other is always non-null, but not both
     */
    protected byte[] dvdBytes;

    protected DVDSerializer dvdSerializer;
    protected boolean deserialized;
    protected boolean descendingOrder;

    //Sort of a cached return value for the isNull() call of the DataValueDescriptor
    //The isNull() method is hit very hard here and in derby, this makes that call much faster
    protected boolean isNull = false;

    //Also the cached dvd.getTypeFormat(), avoids the double method invocation when calling
    //this.getTypeFormatId
    protected int typeFormatId;

    public LazyDataValueDescriptor(){

    }

    public LazyDataValueDescriptor(DataValueDescriptor dvd, DVDSerializer dvdSerializer){
       init(dvd, dvdSerializer);
    }

    public void setDescendingOrder(boolean descendingOrder){
        assert isNull();
        this.descendingOrder = descendingOrder;
    }

    protected void init(DataValueDescriptor dvd, DVDSerializer dvdSerializer){
        this.dvd = dvd;
        typeFormatId = dvd.getTypeFormatId();
        updateNullFlag();
        deserialized = ! dvd.isNull();
        this.dvdSerializer = dvdSerializer;
    }

    protected void updateNullFlag(){
        isNull = dvd.isNull() && (dvdBytes == null || dvdBytes.length == 0) ;
    }

    public void initForDeserialization(byte[] bytes){
        this.dvdBytes = bytes;
        dvd.setToNull();
        deserialized = false;
        updateNullFlag();
    }

    public void initForDeserialization(byte[] bytes,boolean desc){
        this.dvdBytes = bytes;
        descendingOrder = desc;
        dvd.setToNull();
        deserialized = false;
        isNull = bytes==null || bytes.length==0;
        descendingOrder = desc;
    }

    public boolean isSerialized(){
        return dvdBytes!=null;
    }

    public boolean isDeserialized(){
        return deserialized;
    }

    protected void forceDeserialization()  {
        if( !isDeserialized() && isSerialized()){
            try{
                dvdSerializer.deserialize(dvdBytes,dvd,descendingOrder);
                deserialized=true;
            }catch(Exception e){
                SpliceLogUtils.error(LOG, "Error lazily deserializing bytes", e);
            }
        }
    }

    protected void forceSerialization(){
        if(!isSerialized()){
            try{
                dvdBytes = dvdSerializer.serialize(dvd);
                descendingOrder=false;
            }catch(Exception e){
                SpliceLogUtils.error(LOG, "Error serializing DataValueDescriptor to bytes", e);
            }
        }
    }

    protected void resetForSerialization(){
        dvdBytes = null;
        deserialized = true;
        updateNullFlag();
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
    public DataValueDescriptor recycle() {
        restoreToNull();
        return this;
    }

    @Override
    public void setValueFromResultSet(ResultSet resultSet, int colNumber, boolean isNullable) throws StandardException, SQLException {
        dvd.setValueFromResultSet(resultSet, colNumber, isNullable);
        resetForSerialization();
    }

    @Override
    public void setInto(PreparedStatement ps, int position) throws SQLException, StandardException {
        dvd.setInto(ps, position);
        resetForSerialization();
    }

    @Override
    public void setInto(ResultSet rs, int position) throws SQLException, StandardException {
        dvd.setInto(rs, position);
    }

    @Override
    public void setValue(int theValue) throws StandardException {
        dvd.setValue(theValue);
        resetForSerialization();
    }

    @Override
    public void setValue(double theValue) throws StandardException {
        dvd.setValue(theValue);
        resetForSerialization();
    }

    @Override
    public void setValue(float theValue) throws StandardException {
        dvd.setValue(theValue);
        resetForSerialization();
    }

    @Override
    public void setValue(short theValue) throws StandardException {
        dvd.setValue(theValue);
        resetForSerialization();
    }

    @Override
    public void setValue(long theValue) throws StandardException {
        dvd.setValue(theValue);
        resetForSerialization();
    }

    @Override
    public void setValue(byte theValue) throws StandardException {
        dvd.setValue(theValue);
        resetForSerialization();
    }

    @Override
    public void setValue(boolean theValue) throws StandardException {
        dvd.setValue(theValue);
        resetForSerialization();
    }

    @Override
    public void setValue(Object theValue) throws StandardException {
        dvd.setValue(theValue);
        resetForSerialization();
    }

    @Override
    public void setValue(byte[] theValue) throws StandardException {
        dvd.setValue(theValue);
        resetForSerialization();
    }

    @Override
    public void setBigDecimal(Number bigDecimal) throws StandardException {
        dvd.setBigDecimal(bigDecimal);
        resetForSerialization();
    }

    @Override
    public void setValue(String theValue) throws StandardException {
        dvd.setValue(theValue);
        resetForSerialization();
    }

    @Override
    public void setValue(Blob theValue) throws StandardException {
        dvd.setValue(theValue);
        resetForSerialization();
    }

    @Override
    public void setValue(Clob theValue) throws StandardException {
        dvd.setValue(theValue);
        resetForSerialization();
    }

    @Override
    public void setValue(Time theValue) throws StandardException {
        dvd.setValue(theValue);
        resetForSerialization();
    }

    @Override
    public void setValue(Time theValue, Calendar cal) throws StandardException {
        dvd.setValue(theValue, cal);
        resetForSerialization();
    }

    @Override
    public void setValue(Timestamp theValue) throws StandardException {
        dvd.setValue(theValue);
        resetForSerialization();
    }

    @Override
    public void setValue(Timestamp theValue, Calendar cal) throws StandardException {
        dvd.setValue(theValue, cal);
        resetForSerialization();
    }

    @Override
    public void setValue(Date theValue) throws StandardException {
        dvd.setValue(theValue);
        resetForSerialization();
    }

    @Override
    public void setValue(Date theValue, Calendar cal) throws StandardException {
        dvd.setValue(theValue, cal);
        resetForSerialization();
    }

    @Override
    public void setValue(DataValueDescriptor theValue) throws StandardException {
        dvd.setValue(theValue);
        resetForSerialization();
    }

    @Override
    public void setToNull() {
        dvd.setToNull();
        resetForSerialization();
    }

    @Override
    public void normalize(DataTypeDescriptor dtd, DataValueDescriptor source) throws StandardException {
        dvd.normalize(dtd, source);
        resetForSerialization();
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
        dvd.setObjectForCast(value, instanceOfResultType, resultTypeClassName);
        resetForSerialization();
    }

    @Override
    public void readExternalFromArray(ArrayInputStream ais) throws IOException, ClassNotFoundException {
        dvd.readExternalFromArray(ais);
        resetForSerialization();
    }

    @Override
    public int typePrecedence() {
        return dvd.typePrecedence();
    }

    protected DataValueDescriptor unwrap(DataValueDescriptor dvd){

        DataValueDescriptor unwrapped;

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
        return dvd.coalesce(list, returnValue);
    }

    @Override
    public BooleanDataValue in(DataValueDescriptor left, DataValueDescriptor[] inList, boolean orderedList) throws StandardException {
        forceDeserialization();
        return dvd.in(left, inList, orderedList);
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
           result = dvd.compare(other);
       }

        return result;
    }

    private boolean isSameType(DataValueDescriptor dvd){
        return typeFormatId == dvd.getTypeFormatId();
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
        dvd.setValue(theStream, valueLength);
        resetForSerialization();
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
    public boolean isLazy() {
        return true;
    }

    @Override
    public boolean isNull() {
        return isNull;
    }

    @Override
    public void restoreToNull() {
        dvd.restoreToNull();
        resetForSerialization();
    }

    protected void writeDvdBytes(ObjectOutput out) throws IOException {
        if(!isSerialized()){
            forceSerialization();
        }

        byte[] bytes = null;

        try{
            bytes = getBytes();
        }catch(StandardException e){
            throw new IOException("Error reading bytes from DVD", e);
        }

        out.writeBoolean(bytes != null);

        if(bytes != null){
            out.writeInt(bytes.length);
            out.write(bytes);
        }
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {

        out.writeInt(typeFormatId);
        out.writeBoolean(dvd != null);
        writeDvdBytes(out);

    }

    protected void readDvdBytes(ObjectInput in) throws IOException, ClassNotFoundException {
        if(in.readBoolean()){

            int numBytes = in.readInt();

            dvdBytes = new byte[numBytes];
            in.readFully(dvdBytes);
        }
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {

        DataValueDescriptor externalDVD = null;

        int typeId = in.readInt();

        if(in.readBoolean()){
            externalDVD = createNullDVD(typeId);
        }
        readDvdBytes(in);

        init(externalDVD, LazyDataValueFactory.getDVDSerializer(typeId));
    }

    protected DataValueDescriptor createNullDVD(int typeId) throws IOException {

        DataValueDescriptor externalDVD;

        try{
            externalDVD = LazyDataValueFactory.getLazyNull(typeId);
        }catch(StandardException e){
            throw new IOException("Error creating Null DataValueDescriptor", e);
        }

        return externalDVD;
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
        return typeFormatId;
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

        if(o instanceof DataValueDescriptor){

            DataValueDescriptor otherDVD = (DataValueDescriptor) o;

            if(otherDVD.isLazy()){
                LazyDataValueDescriptor ldvd = (LazyDataValueDescriptor) otherDVD;

                if(dvdBytes!=null && ldvd.dvdBytes!=null){
                    return dvdBytes.equals(ldvd.dvdBytes);
                } else {
                    result = dvd.equals(ldvd.dvd);
                }
            } else{
                forceDeserialization();
                result = dvd.equals(otherDVD);
            }
        }

        return result;
    }

    protected DVDSerializer getDVDSerializer(){
        return dvdSerializer;
    }

    public byte[] getBytes(boolean desc) throws StandardException{
        byte[] bytes = getBytes();
        byte[] retBytes = new byte[bytes.length];
        System.arraycopy(bytes,0,retBytes,0,bytes.length);
        if(desc && !this.descendingOrder){
            //need to convert to descending order
            for(int i=0;i<retBytes.length;i++){
                retBytes[i] ^=0xff;
            }
        }else if(!desc && this.descendingOrder){
            //need to convert to ascending order
            for(int i=0;i<retBytes.length;i++){
                retBytes[i] ^=0xff;
            }
        }
        return retBytes;
    }
}

