package com.splicemachine.derby.impl.sql.execute;

import com.splicemachine.derby.utils.marshall.dvd.DescriptorSerializer;
import com.splicemachine.derby.utils.marshall.dvd.TimeValuedSerializer;
import com.splicemachine.derby.utils.marshall.dvd.VersionedSerializers;
import com.splicemachine.encoding.MultiFieldEncoder;
import com.splicemachine.utils.ByteSlice;
import com.splicemachine.utils.SpliceLogUtils;

import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.services.io.ArrayInputStream;
import org.apache.derby.iapi.types.BooleanDataValue;
import org.apache.derby.iapi.types.DataTypeDescriptor;
import org.apache.derby.iapi.types.DataValueDescriptor;
import org.apache.derby.iapi.types.DataValueFactoryImpl.Format;
import org.apache.derby.iapi.types.SQLBoolean;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;
import org.joda.time.DateTime;

import javax.management.Descriptor;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInput;
import java.io.ObjectOutput;
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
	protected ByteSlice bytes;

//    protected DVDSerializer dvdSerializer;
    protected boolean deserialized;
    protected boolean descendingOrder;

    //Sort of a cached return value for the isNull() call of the DataValueDescriptor
    //The isNull() method is hit very hard here and in derby, this makes that call much faster
    protected boolean isNull = false;

    //Also the cached dvd.getTypeFormat(), avoids the double method invocation when calling
    //this.getTypeFormatId
    protected int typeFormatId;
		protected String tableVersion;
		protected DescriptorSerializer serializer;

		public LazyDataValueDescriptor(){

    }

    public LazyDataValueDescriptor(DataValueDescriptor dvd){
       init(dvd);
    }

		protected void init(DataValueDescriptor dvd){
        this.dvd = dvd;
        typeFormatId = dvd.getTypeFormatId();
        updateNullFlag();
        deserialized = ! dvd.isNull();
    }

    protected void updateNullFlag(){
        isNull = dvd.isNull() && (bytes == null || bytes.length() == 0) ;
    }

		public void initForDeserialization(String tableVersion,byte[] bytes,int offset,int length, boolean desc){
				if(this.bytes==null)
						this.bytes = new ByteSlice();
				this.bytes.set(bytes,offset,length);
				dvd.setToNull();
				deserialized = false;
				updateNullFlag();
				this.descendingOrder = desc;
				this.tableVersion = tableVersion;
				this.serializer = VersionedSerializers.forVersion(tableVersion,true).getEagerSerializer(dvd.getTypeFormatId());
		}

		public boolean isSerialized(){
				return bytes!=null && bytes.length()>0;
    }

    public boolean isDeserialized(){
        return deserialized;
    }

    protected void forceDeserialization()  {
        if( !isDeserialized() && isSerialized()){
            try{
								if(serializer==null)
										serializer = VersionedSerializers.forVersion(tableVersion,true).getEagerSerializer(typeFormatId);
								serializer.decodeDirect(dvd, bytes.array(), bytes.offset(), bytes.length(), descendingOrder);
                deserialized=true;
            }catch(Exception e){
                SpliceLogUtils.error(LOG, "Error lazily deserializing bytes", e);
            }
        }
    }

		protected void forceSerialization(){
			forceSerialization(descendingOrder,tableVersion);
		}
    protected void forceSerialization(boolean desc,String destinationVersion){
        if(!isSerialized()){
            try{
								if(bytes==null)
										bytes = new ByteSlice();
								if(serializer==null)
										serializer = VersionedSerializers.forVersion(destinationVersion,true).getSerializer(typeFormatId);
								byte[] serialize = serializer.encodeDirect(dvd, desc);
								bytes.set(serialize,0,serialize.length);
                descendingOrder=desc;
								tableVersion = destinationVersion;
            }catch(Exception e){
                SpliceLogUtils.error(LOG, "Error serializing DataValueDescriptor to bytes", e);
            }
        }else if(desc!=descendingOrder){
						byte[] data = bytes.getByteCopy();
						for(int i=0;i<data.length;i++){
								data[i]^=0xff;
						}
						bytes.set(data,0,data.length);
						descendingOrder = desc;
				}else if(tableVersion==null || !tableVersion.equals(destinationVersion)){
						/*
						 * If our table versions don't match, forcibly deserialize then reserialize with the proper version
						 */
						forceDeserialization();
						if(bytes!=null)
								bytes.reset();
						serializer = null;
						tableVersion = destinationVersion;
						forceSerialization(desc,destinationVersion);
				}
    }

    protected void resetForSerialization(){
				if(bytes!=null)
						bytes.reset();
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
				return bytes.getByteCopy();
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
				if(cal!=null && serializer !=null && serializer instanceof TimeValuedSerializer)
						((TimeValuedSerializer)serializer).setCalendar(cal);
        forceDeserialization();
        return dvd.getTimestamp(cal);
    }

    @Override
    public DateTime getDateTime() throws StandardException {
        forceDeserialization();
        return dvd.getDateTime();
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
    	return false;
//        forceDeserialization();
//       return dvd.hasStream();
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
    public void setValue(DateTime theValue) throws StandardException {
        dvd.setValue(theValue);
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
					 LazyDataValueDescriptor lOther = (LazyDataValueDescriptor)other;
					 if(deserialized){
							 if(!lOther.isDeserialized())
									 lOther.forceDeserialization();
							 result =dvd.compare(lOther.dvd);
					 }else if(lOther.isDeserialized()){
							 forceDeserialization();
							 result = dvd.compare(lOther.dvd);
					 }else
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

		@Override
    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeInt(typeFormatId);
        boolean isN = isNull();
        out.writeBoolean(isN);
        if(!isN){
            if(!isSerialized())
                forceSerialization();
            byte[] bytes;
            try {
                bytes = getBytes();
            } catch (StandardException e) {
                throw new IOException(e);
            }

            out.writeInt(bytes.length);
            out.write(bytes);
						out.writeUTF(tableVersion);
        }
    }

		@Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        typeFormatId = in.readInt();
        if(!in.readBoolean()){
						byte[] data = new byte[in.readInt()];
						in.readFully(data);
						if(bytes==null)
								bytes = new ByteSlice();
						bytes.set(data,0,data.length);
						tableVersion = in.readUTF();
						this.serializer = VersionedSerializers.forVersion(tableVersion,true).getEagerSerializer(typeFormatId);
        }else{
            isNull = true;
        }

        DataValueDescriptor externalDVD= createNullDVD(typeFormatId);
        init(externalDVD);
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

                if(bytes!=null && ldvd.bytes!=null
                        && descendingOrder == ldvd.descendingOrder){
										return bytes.equals(ldvd.bytes);
                    //return dvdBytes.equals(ldvd.dvdBytes);
                } else {
                    ldvd.forceDeserialization();
                    result = dvd.equals(ldvd.dvd);
                }
            } else{
                result = dvd.equals(otherDVD);
            }
        }

        return result;
    }

    public byte[] getBytes(boolean desc,String tableVersion) throws StandardException{
				forceSerialization(desc,tableVersion);
				return getBytes();
    }

		public void encodeInto(MultiFieldEncoder fieldEncoder, boolean desc,String destinationVersion) {
				forceSerialization(desc,destinationVersion);
				fieldEncoder.setRawBytes(bytes.array(),bytes.offset(),bytes.length());
		}

		public void setSerializer(DescriptorSerializer serializer) {
				this.serializer = serializer;
		}

		@Override
		public Format getFormat() {
			return dvd.getFormat();
		}
		
}

