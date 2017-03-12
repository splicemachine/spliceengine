/*
 * Copyright 2012 - 2016 Splice Machine, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */

package com.splicemachine.derby.impl.sql.execute.dvd;

import com.splicemachine.access.util.ByteComparisons;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.services.io.ArrayInputStream;
import com.splicemachine.db.iapi.services.sanity.SanityManager;
import com.splicemachine.db.iapi.types.*;
import com.splicemachine.db.iapi.types.DataValueFactoryImpl.Format;
import com.splicemachine.derby.utils.marshall.dvd.DescriptorSerializer;
import com.splicemachine.derby.utils.marshall.dvd.TimeValuedSerializer;
import com.splicemachine.derby.utils.marshall.dvd.VersionedSerializers;
import com.splicemachine.encoding.MultiFieldEncoder;
import com.splicemachine.primitives.Bytes;
import com.splicemachine.utils.SpliceLogUtils;
import com.yahoo.sketches.theta.UpdateSketch;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.apache.log4j.Logger;
import org.joda.time.DateTime;

import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.sql.*;
import java.util.Arrays;
import java.util.Calendar;

/**
 * Lazy subclass of DataValueDescriptor.  Holds a byte array representing the data value
 * and the DVDSerializer for converting to/from bytes.  There is also some duplication
 * in the variables below and in the subclasses. See the specific variables below for
 * rationale.
 */
public abstract class LazyDataValueDescriptor<DVD extends DataValueDescriptor>
        extends NullValueData implements DataValueDescriptor, Comparable {
    private static final long serialVersionUID=3l;
    private static Logger LOG=Logger.getLogger(LazyDataValueDescriptor.class);

    //Same as the dvd used in the subclasses, another reference is kept here
    //to avoid a covariant getter call on the subclass (slows down performance)
    DVD dvd=null;

    /*
     * One or the other is always non-null, but not both
     */
    protected byte[] bytes;
    protected int offset;
    protected int length;

    //    protected DVDSerializer dvdSerializer;
    protected volatile boolean deserialized;
    protected boolean descendingOrder;

//    //Sort of a cached return value for the isNull() call of the DataValueDescriptor
//    //The isNull() method is hit very hard here and in derby, this makes that call much faster
//    protected boolean isNull=true;

    //Also the cached dvd.getTypeFormat(), avoids the double method invocation when calling
    //this.getTypeFormatId
//    protected int typeFormatId;
    protected String tableVersion;
    protected DescriptorSerializer serializer;

    // Used to avoid concurrent attempts to deserialize
    private Object deserLock = new Object();

    public LazyDataValueDescriptor(){
    }

    public LazyDataValueDescriptor(DVD dvd){
        init(dvd);
    }
    protected void init(DVD dvd){
        this.dvd=dvd;
        deserialized=dvd!=null && !dvd.isNull();
        if (dvd == null) {
            if (deserialized)
                isNull = true;
            // else the isNull value has been set already
        } else
            isNull = dvd.isNull();
    }

    public void initForDeserialization(LazyDataValueDescriptor ldvd) {
        initForDeserialization(ldvd.tableVersion, ldvd.serializer,
                ldvd.bytes, ldvd.offset, ldvd.length, ldvd.descendingOrder);

    }

    @SuppressFBWarnings(value = "EI_EXPOSE_REP2",justification = "Intentional")
    public void initForDeserialization(String tableVersion,
                                       DescriptorSerializer serializer,
                                       byte[] bytes,int offset,int length,boolean desc){
        this.bytes = bytes;
        this.offset = offset;
        this.length = length;
        if (dvd != null) {
            ((NullValueData)dvd).setIsNull(true);
        }

        deserialized=false;
        this.descendingOrder=desc;
        this.serializer=serializer;
        this.tableVersion=tableVersion;
        isNull = evaluateNull();
    }

    public boolean isSerialized(){
        return bytes!=null && length>0;
    }

    public boolean isDeserialized(){
        return deserialized;
    }

    protected void createNewDescriptor() {
        dvd = newDescriptor();
        if (dvd == null || dvd.isNull())
            isNull = bytes == null || length == 0;
        else isNull = false;
    }

    private Object initLock = new Object();
    private volatile boolean hasDescriptor = false;

    protected void forceDeserialization(){
        if (!hasDescriptor) {
            synchronized (initLock) {
                if (!hasDescriptor && dvd == null) {
                    createNewDescriptor();
                    hasDescriptor = true;
                }
            }
        }
        if(isSerialized()&&!isDeserialized()){
            synchronized (deserLock) {
                // double checked locking on volatile
                if (!isDeserialized()) {
                    try {
                        serializer.decodeDirect(dvd, bytes, offset, length, descendingOrder);
                        deserialized = true;
                    } catch (Exception e) {
                        SpliceLogUtils.error(LOG, String.format("Error lazily deserializing %s from bytes with serializer %s",
                                this.getClass().getSimpleName(), serializer == null ? "NULL" : serializer.getClass().getSimpleName()), e);
                    }
                }
            }
        }
        isNull = evaluateNull();
    }

    protected abstract DVD newDescriptor();

    protected void forceSerialization(){
        forceSerialization(descendingOrder,tableVersion);
    }

    void forceSerialization(boolean desc,String destinationVersion){
        if(!isSerialized()){
            try{
                if(dvd==null){
                    //if we don't have a dvd, then there's nothing to encode
                    bytes=new byte[]{};
                }else{
                    if(serializer==null)
                        serializer=VersionedSerializers.forVersion(destinationVersion,true).getSerializer(getTypeFormatId());
                    bytes=serializer.encodeDirect(dvd,desc);
                }
                offset=0;
                length=bytes.length;

                descendingOrder=desc;
                tableVersion=destinationVersion;
            }catch(Exception e){
                SpliceLogUtils.error(LOG,String.format("Error serializing %s to bytes with serializer %s",
                        this.getClass().getSimpleName(),
                        serializer==null?"NULL":
                                serializer.getClass().getSimpleName()),e);
            }
        }else if(desc!=descendingOrder){
            bytes = reverseBytes();
            offset=0;
            descendingOrder=desc;
        }else if(tableVersion==null || !tableVersion.equals(destinationVersion)){
            /*
             * If our table versions don't match, forcibly deserialize then reserialize with the proper version
			 */
            forceDeserialization();
            length=0;
            bytes=null;
            serializer=null;
            tableVersion=destinationVersion;
            forceSerialization(desc,destinationVersion);
        }
        isNull = evaluateNull();
    }


    protected void resetForSerialization(){
        length=0;
        bytes=null;
        offset=0;
        deserialized=true;
        if(dvd==null||dvd.isNull())
            isNull = true;
        else isNull = false;
    }

    @Override
    public int getLength() throws StandardException{
        forceDeserialization();
        return dvd.getLength();
    }

    @Override
    public String getString() throws StandardException{
        forceDeserialization();
        /*
         * forceDeserialization should ensure dvd is not null but does not
         * for test LazyTimestampTest.testCanSerializeNullsCorrectly
         * so we are leaving this null check in for now
         */
        if (dvd == null)
            return null;
        return dvd.getString();
    }

    @Override
    public String getTraceString() throws StandardException{
        forceDeserialization();
        return dvd.getTraceString();
    }

    @Override
    public boolean getBoolean() throws StandardException{
        forceDeserialization();
        return dvd.getBoolean();
    }

    @Override
    public byte getByte() throws StandardException{
        forceDeserialization();
        return dvd.getByte();
    }

    @Override
    public short getShort() throws StandardException{
        forceDeserialization();
        return dvd.getShort();
    }

    @Override
    public int getInt() throws StandardException{
        forceDeserialization();
        return dvd.getInt();
    }

    @Override
    public long getLong() throws StandardException{
        forceDeserialization();
        return dvd.getLong();
    }

    @Override
    public float getFloat() throws StandardException{
        forceDeserialization();
        return dvd.getFloat();
    }

    @Override
    public double getDouble() throws StandardException{
        forceDeserialization();
        return dvd.getDouble();
    }

    @Override
    public int typeToBigDecimal() throws StandardException{
        forceDeserialization();
        return dvd.typeToBigDecimal();
    }

    @Override
    public byte[] getBytes() throws StandardException{
        forceSerialization();
        byte[] d = new byte[length];
        System.arraycopy(bytes,offset,d,0,length);
        return d;
    }

    @Override
    public Date getDate(Calendar cal) throws StandardException{
        forceDeserialization();
        return dvd.getDate(cal);
    }

    @Override
    public Time getTime(Calendar cal) throws StandardException{
        forceDeserialization();
        return dvd.getTime(cal);
    }

    @Override
    public Timestamp getTimestamp(Calendar cal) throws StandardException{
        if(cal!=null && serializer!=null && serializer instanceof TimeValuedSerializer)
            ((TimeValuedSerializer)serializer).setCalendar(cal);
        forceDeserialization();
        return dvd.getTimestamp(cal);
    }

    @Override
    public DateTime getDateTime() throws StandardException{
        forceDeserialization();
        return dvd.getDateTime();
    }

    @Override
    public Object getObject() throws StandardException{
        forceDeserialization();
        return dvd.getObject();
    }

    @Override
    public InputStream getStream() throws StandardException{
        forceDeserialization();
        return dvd.getStream();
    }

    @Override
    public boolean hasStream(){
        return false;
    }

    @Override
    public DataValueDescriptor recycle(){
        if (dvd != null) {
            ((NullValueData)dvd).setIsNull(true);
        }
        // needs to match resetForSerialization method
        length=0;
        bytes=null;
        offset=0;
        deserialized=true;
        isNull = true;
        return this;
    }

    @Override
    public void setValueFromResultSet(ResultSet resultSet,int colNumber,boolean isNullable) throws StandardException, SQLException{
        if(dvd==null) createNewDescriptor();
        dvd.setValueFromResultSet(resultSet,colNumber,isNullable);
        resetForSerialization();
    }

    @Override
    public void setInto(PreparedStatement ps,int position) throws SQLException, StandardException{
        if(dvd==null) createNewDescriptor();
        dvd.setInto(ps,position);
        resetForSerialization();
    }

    @Override
    public void setInto(ResultSet rs,int position) throws SQLException, StandardException{
        if(dvd==null) createNewDescriptor();
        dvd.setInto(rs,position);
    }

    @Override
    public void setValue(int theValue) throws StandardException{
        if(dvd==null) createNewDescriptor();
        dvd.setValue(theValue);
        resetForSerialization();
    }

    @Override
    public void setValue(double theValue) throws StandardException{
        if(dvd==null) createNewDescriptor();
        dvd.setValue(theValue);
        resetForSerialization();
    }

    @Override
    public void setValue(float theValue) throws StandardException{
        if(dvd==null) createNewDescriptor();
        dvd.setValue(theValue);
        resetForSerialization();
    }

    @Override
    public void setValue(short theValue) throws StandardException{
        if(dvd==null) createNewDescriptor();
        dvd.setValue(theValue);
        resetForSerialization();
    }

    @Override
    public void setValue(long theValue) throws StandardException{
        if(dvd==null) createNewDescriptor();
        dvd.setValue(theValue);
        resetForSerialization();
    }

    @Override
    public void setValue(byte theValue) throws StandardException{
        if(dvd==null) createNewDescriptor();
        dvd.setValue(theValue);
        resetForSerialization();
    }

    @Override
    public void setValue(boolean theValue) throws StandardException{
        if(dvd==null) createNewDescriptor();
        dvd.setValue(theValue);
        resetForSerialization();
    }

    @Override
    public void setValue(Object theValue) throws StandardException{
        if(dvd==null) createNewDescriptor();
        dvd.setValue(theValue);
        resetForSerialization();
    }

    @Override
    public void setValue(RowId theValue) throws StandardException{
        if(dvd==null) createNewDescriptor();
        dvd.setValue(theValue);
        resetForSerialization();
    }

    @Override
    public void setValue(byte[] theValue) throws StandardException{
        if(dvd==null) createNewDescriptor();
        dvd.setValue(theValue);
        resetForSerialization();
    }

    @Override
    public void setBigDecimal(Number bigDecimal) throws StandardException{
        if(dvd==null) createNewDescriptor();
        dvd.setBigDecimal(bigDecimal);
        resetForSerialization();
    }

    @Override
    public void setValue(String theValue) throws StandardException{
        if(dvd==null) createNewDescriptor();
        dvd.setValue(theValue);
        resetForSerialization();
    }

    @Override
    public void setValue(Blob theValue) throws StandardException{
        if(dvd==null) createNewDescriptor();
        dvd.setValue(theValue);
        resetForSerialization();
    }

    @Override
    public void setValue(Clob theValue) throws StandardException{
        if(dvd==null) createNewDescriptor();
        dvd.setValue(theValue);
        resetForSerialization();
    }

    @Override
    public void setValue(Time theValue) throws StandardException{
        if(dvd==null) createNewDescriptor();
        dvd.setValue(theValue);
        resetForSerialization();
    }

    @Override
    public void setValue(Time theValue,Calendar cal) throws StandardException{
        if(dvd==null) createNewDescriptor();
        dvd.setValue(theValue,cal);
        resetForSerialization();
    }

    @Override
    public void setValue(Timestamp theValue) throws StandardException{
        if(dvd==null) createNewDescriptor();
        dvd.setValue(theValue);
        resetForSerialization();
    }

    @Override
    public void setValue(Timestamp theValue,Calendar cal) throws StandardException{
        if(dvd==null) createNewDescriptor();
        dvd.setValue(theValue,cal);
        resetForSerialization();
    }

    @Override
    public void setValue(DateTime theValue) throws StandardException{
        if(dvd==null) createNewDescriptor();
        dvd.setValue(theValue);
        resetForSerialization();
    }

    @Override
    public void setValue(Date theValue) throws StandardException{
        if(dvd==null) createNewDescriptor();
        dvd.setValue(theValue);
        resetForSerialization();
    }

    @Override
    public void setValue(Date theValue,Calendar cal) throws StandardException{
        if(dvd==null) createNewDescriptor();
        dvd.setValue(theValue,cal);
        resetForSerialization();
    }

    @Override
    public void setValue(DataValueDescriptor theValue) throws StandardException{
        if(dvd==null) createNewDescriptor();
        dvd.setValue(theValue);
        resetForSerialization();
    }

    @Override
    public void setToNull(){
        if(dvd!=null)
            dvd.setToNull();
        resetForSerialization();
    }

    @Override
    public void normalize(DataTypeDescriptor dtd,DataValueDescriptor source) throws StandardException{
        if(dvd==null) createNewDescriptor();
        dvd.normalize(dtd,source);
        resetForSerialization();
    }

    @Override
    public BooleanDataValue isNullOp(){
        forceDeserialization();
        return dvd.isNullOp();
    }

    @Override
    public BooleanDataValue isNotNull(){
        forceDeserialization();
        return dvd.isNotNull();
    }

    @Override
    public String getTypeName(){
        if(dvd==null) createNewDescriptor();
        return dvd.getTypeName();
    }

    @Override
    public void setObjectForCast(Object value,boolean instanceOfResultType,String resultTypeClassName) throws StandardException{
        if(dvd==null) createNewDescriptor();
        dvd.setObjectForCast(value,instanceOfResultType,resultTypeClassName);
        resetForSerialization();
    }

    @Override
    public void readExternalFromArray(ArrayInputStream ais) throws IOException, ClassNotFoundException{
		try {
			if(dvd==null) createNewDescriptor();
			dvd.readExternalFromArray(ais);
			resetForSerialization();
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
    }

    @Override
    public int typePrecedence(){
        if(dvd==null) createNewDescriptor();
        return dvd.typePrecedence();
    }

    protected DataValueDescriptor unwrap(DataValueDescriptor dvd){

        DataValueDescriptor unwrapped;

        if(dvd instanceof LazyDataValueDescriptor){
            LazyDataValueDescriptor ldvd=(LazyDataValueDescriptor)dvd;
            ldvd.forceDeserialization();
            unwrapped=ldvd.dvd;
        }else{
            unwrapped=dvd;
        }

        return unwrapped;
    }

    @Override
    public BooleanDataValue equals(DataValueDescriptor left,DataValueDescriptor right) throws StandardException{
        return SQLBoolean.truthValue(left,right,left.compare(right)==0);
    }

    @Override
    public BooleanDataValue notEquals(DataValueDescriptor left,DataValueDescriptor right) throws StandardException{
        return SQLBoolean.truthValue(left,right,left.compare(right)!=0);
    }

    @Override
    public BooleanDataValue lessThan(DataValueDescriptor left,DataValueDescriptor right) throws StandardException{
        return SQLBoolean.truthValue(left,right,left.compare(right)<0);
    }

    @Override
    public BooleanDataValue greaterThan(DataValueDescriptor left,DataValueDescriptor right) throws StandardException{
        return SQLBoolean.truthValue(left,right,left.compare(right)>0);
    }

    @Override
    public BooleanDataValue lessOrEquals(DataValueDescriptor left,DataValueDescriptor right) throws StandardException{
        return SQLBoolean.truthValue(left,right,left.compare(right)<=0);
    }

    @Override
    public BooleanDataValue greaterOrEquals(DataValueDescriptor left,DataValueDescriptor right) throws StandardException{
        return SQLBoolean.truthValue(left,right,left.compare(right)>=0);
    }

    @Override
    public DataValueDescriptor coalesce(DataValueDescriptor[] list,DataValueDescriptor returnValue) throws StandardException{
        forceDeserialization();
        return dvd.coalesce(list,returnValue);
    }

    @Override
    public BooleanDataValue in(DataValueDescriptor left,DataValueDescriptor[] inList,boolean orderedList) throws StandardException{
        forceDeserialization();
        return dvd.in(left,inList,orderedList);
    }

    @Override
    public int compare(DataValueDescriptor other) throws StandardException{

        int result;

        boolean thisIsNull=this.isNull();
        boolean otherIsNull=other.isNull();

        if(thisIsNull || otherIsNull){
            if(thisIsNull && otherIsNull){
                result=0;
            }else if(thisIsNull){
                result=1;
            }else{
                result=-1;
            }
        }else if(other.isLazy() && isSameType(other)){
            LazyDataValueDescriptor lOther=(LazyDataValueDescriptor)other;
            if(isSerialized()){
                if(lOther.isSerialized()){
                    byte[] d;
                    int off;
                    int len;
                    if(descendingOrder==lOther.descendingOrder){
                        d = bytes;
                        off = offset;
                        len = length;
                    }else{
                        d =reverseBytes();
                        off = 0;
                        len = d.length;
                    }
                    return ByteComparisons.comparator().compare(d,off,len,lOther.bytes,lOther.offset,lOther.length);
                }else{
                    forceDeserialization();
                }
            }else if(!lOther.isDeserialized()){
               lOther.forceDeserialization();
            }
            return dvd.compare(lOther.dvd);
        }else{
            forceDeserialization();
            result=dvd.compare(other);
        }

        return result;
    }

    private boolean isSameType(DataValueDescriptor dvd){
        return getTypeFormatId()==dvd.getTypeFormatId();
    }

    @Override
    public int compare(DataValueDescriptor other,boolean nullsOrderedLow) throws StandardException{

        int result;

        boolean isThisNull=this.isNull();
        boolean isOtherNull=other.isNull();

        if(isThisNull || isOtherNull){

            if(isThisNull && isOtherNull){

                result=0;

            }else if(isThisNull){

                result=nullsOrderedLow?-1:1;

            }else{

                result=nullsOrderedLow?1:-1;

            }

        }else{
            result=compare(other);
        }

        return result;
    }

    @Override
    public boolean compare(int op,DataValueDescriptor other,boolean orderedNulls,boolean unknownRV) throws StandardException{

        return compareResultWithOperator(op,compare(other));

    }

    @Override
    public boolean compare(int op,DataValueDescriptor other,boolean orderedNulls,boolean nullsOrderedLow,boolean unknownRV) throws StandardException{

        return compareResultWithOperator(op,compare(other,nullsOrderedLow));

    }

    private boolean compareResultWithOperator(int op,int compareResult){

        boolean result;

        switch(op){
            case ORDER_OP_LESSTHAN:
                result=(compareResult<0);   // this <  other
                break;
            case ORDER_OP_EQUALS:
                result=(compareResult==0);  // this == other
                break;
            case ORDER_OP_LESSOREQUALS:
                result=(compareResult<=0);  // this <= other
                break;
            case ORDER_OP_GREATERTHAN:
                result=(compareResult>0);   // this > other
                break;
            case ORDER_OP_GREATEROREQUALS:
                result=(compareResult>=0);  // this >= other
                break;
            default:
                result=false;
        }

        return result;
    }

    @Override
    public void setValue(InputStream theStream,int valueLength) throws StandardException{
        if(dvd==null) createNewDescriptor();
        dvd.setValue(theStream,valueLength);
        resetForSerialization();
    }

    @Override
    public void checkHostVariable(int declaredLength) throws StandardException{
        if(dvd==null) return;
        dvd.checkHostVariable(declaredLength);
    }

    @Override
    public int estimateMemoryUsage(){
        forceDeserialization();
        return dvd.estimateMemoryUsage();
    }

    @Override
    public boolean isLazy(){
        return true;
    }

	private final boolean evaluateNull()
	{
        if(dvd==null||((NullValueData)dvd).getIsNull())
            return bytes==null||length==0;
        else return false;
    }

    @Override
    public void restoreToNull(){
        // needs to match resetForSerialization method
        length=0;
        bytes=null;
        offset=0;
        deserialized=true;
        if (dvd != null)
            dvd.restoreToNull();
        isNull = true;
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException{
        boolean isN=isNull();
        out.writeBoolean(isN);
        if(!isN){
            if(!isSerialized())
                forceSerialization();
            byte[] bytes;
            try{
                bytes=getBytes();
            }catch(StandardException e){
                throw new IOException(e);
            }

            out.writeInt(bytes.length);
            out.write(bytes);
            out.writeUTF(tableVersion);
        }
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException{
        if(!in.readBoolean()){
            byte[] data=new byte[in.readInt()];
            in.readFully(data);
            this.bytes=data;
            this.offset=0;
            this.length=data.length;
            tableVersion=in.readUTF();
            this.serializer=VersionedSerializers.forVersion(tableVersion,true).getEagerSerializer(getTypeFormatId());
            this.isNull = this.evaluateNull();
        }
    }

    @Override
    public int getTypeFormatId(){
        if(dvd==null) createNewDescriptor();
        return dvd.getTypeFormatId();
    }

    @Override
    public int hashCode(){
        forceDeserialization();
        return dvd.hashCode();
    }

    @Override
    public boolean equals(Object o){
        if(o==this) return true;

        if(!(o instanceof DataValueDescriptor)){
           return false;
        }

        DataValueDescriptor oDvd = (DataValueDescriptor)o;

//        if(oDvd.getTypeFormatId()!=getTypeFormatId()) return false;

        if(isNull()){
            return oDvd.isNull();
        }

        if(oDvd.isLazy()){
            LazyDataValueDescriptor ldvd = (LazyDataValueDescriptor)oDvd;
            if(isSerialized()){
                if(ldvd.isSerialized()){
                    byte[] d;
                    int off;
                    int len;
                    if(descendingOrder==ldvd.descendingOrder){
                        d=bytes;
                        off = offset;
                        len=length;
                    }else{
                        d= reverseBytes();
                        off = 0;
                        len = d.length;
                    }
                    return Bytes.equals(d,off,len,ldvd.bytes,ldvd.offset,ldvd.length);
                }else{
                    forceDeserialization();
                    return dvd.equals(ldvd.dvd);
                }
            }else if(!ldvd.isDeserialized()){
                ldvd.forceDeserialization();
            }
            return dvd.equals(ldvd.dvd);
        }else{
            forceDeserialization();
            return dvd.equals(oDvd);
        }
    }

    public byte[] getBytes(boolean desc,String tableVersion) throws StandardException{
        forceSerialization(desc,tableVersion);
        return getBytes();
    }

    public void encodeInto(MultiFieldEncoder fieldEncoder,boolean desc,String destinationVersion){
        forceSerialization(desc,destinationVersion);
        fieldEncoder.setRawBytes(bytes,offset,length);
    }

    public void setSerializer(DescriptorSerializer serializer){
        this.serializer=serializer;
    }

    @Override
    public Format getFormat(){
        return dvd.getFormat();
    }

    @Override
    public String toString(){
        if(isNull()) return "NULL";
        try{
            return getString();
        }catch(StandardException se){
            throw new RuntimeException(se);
        }
    }

    private byte[] reverseBytes(){
        byte[] data=Arrays.copyOfRange(bytes,offset,offset+length);
        for(int i=0;i<data.length;i++){
            data[i] ^=0xff;
        }
        return data;
    }
    
    @Override
    public int compare(Object o1, Object o2) {
        try {
            DataValueDescriptor dvd1 = (DataValueDescriptor)o1;
            DataValueDescriptor dvd2 = (DataValueDescriptor)o2;
            return dvd1.compare(dvd2);
        } catch (StandardException se) {
            throw new RuntimeException(se);
        }
    }

    @Override
    public int compareTo(Object otherDVD) {
        LazyDataValueDescriptor other = (LazyDataValueDescriptor) otherDVD;
        try {

            // Use compare method from the dominant type.
            if (typePrecedence() < other.typePrecedence())
                return (-1 * other.compare(this));

            return compare(other);

        } catch (StandardException se) {

            if (SanityManager.DEBUG) {
                SanityManager.THROWASSERT("Encountered error while " +
                        "trying to compare two DataValueDescriptors: " +
                        se.getMessage());
            }

			/* In case of an error in insane mode, just treat the
			 * values as "equal".
			 */
            return 0;
        }
    }
    @Override
    public void updateThetaSketch(UpdateSketch updateSketch) {
        forceDeserialization();
        dvd.updateThetaSketch(updateSketch);
    }

    @Override
    public com.yahoo.sketches.frequencies.ItemsSketch getFrequenciesSketch() throws StandardException {
        return new com.yahoo.sketches.frequencies.ItemsSketch(1024);
    }

    @Override
    public int getRowWidth() {
        forceDeserialization();
        return dvd.getRowWidth();
    }

    @Override
    public UpdateSketch getThetaSketch() throws StandardException {
        return UpdateSketch.builder().build(4096);
    }

    @Override
    public com.yahoo.sketches.quantiles.ItemsSketch getQuantilesSketch() throws StandardException {
        forceDeserialization();
        return com.yahoo.sketches.quantiles.ItemsSketch.getInstance(256,dvd);
    }
}

