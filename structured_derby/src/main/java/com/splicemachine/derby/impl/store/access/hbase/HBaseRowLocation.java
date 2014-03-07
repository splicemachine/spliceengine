package com.splicemachine.derby.impl.store.access.hbase;

import com.splicemachine.utils.ByteSlice;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.services.cache.ClassSize;
import org.apache.derby.iapi.services.io.ArrayInputStream;
import org.apache.derby.iapi.services.io.StoredFormatIds;
import org.apache.derby.iapi.services.sanity.SanityManager;
import org.apache.derby.iapi.types.DataType;
import org.apache.derby.iapi.types.DataValueDescriptor;
import org.apache.derby.iapi.types.RowLocation;

import java.io.ObjectOutput;
import java.io.ObjectInput;
import java.io.IOException;

/**     
 * 
 * 
 * 
 */

public class HBaseRowLocation extends DataType implements RowLocation {

	private ByteSlice slice;
    private static final int BASE_MEMORY_USAGE = ClassSize.estimateBaseFromCatalog( HBaseRowLocation.class);
    private static final int RECORD_HANDLE_MEMORY_USAGE = ClassSize.estimateBaseFromCatalog( org.apache.derby.impl.store.raw.data.RecordId.class);

	public HBaseRowLocation()
	{
	}

	public HBaseRowLocation(byte[] rowKey) {
		this.slice = ByteSlice.wrap(rowKey);
	}
	
	public HBaseRowLocation(ByteSlice slice) {
		this.slice = slice;
	}
	
    public int estimateMemoryUsage() {
        return BASE_MEMORY_USAGE;
    } 

    @Override
    public final void setValue(byte[] theValue) {
    	this.slice = ByteSlice.wrap(theValue);
	}
    
    public final byte[]	getBytes() throws StandardException {
		return slice != null?slice.getByteCopy():null;
	}
    @Override
	public String getTypeName() {
		return "HBaseRowLocation";
	}

	public void setValueFromResultSet(java.sql.ResultSet resultSet, int colNumber,
		boolean isNullable) {
	}

	public DataValueDescriptor getNewNull() {
		return new HBaseRowLocation();
	}
	@Override
	public Object getObject() {
		return this.slice;
	}

	@Override
	public void setValue(Object theValue) throws StandardException {
		this.slice = (ByteSlice) theValue;
	}

	public DataValueDescriptor cloneValue(boolean forceMaterialization) {
		return new HBaseRowLocation(this);
	}

	public String getString() {
		return toString();
	}
	
	public int getLength() {
		return this.slice == null ? 0 : this.slice.length();//what is the length of the primary key?
	}

	/*
	** Methods of Orderable (from RowLocation)
	**
	** see description in
	** protocol/Database/Storage/Access/Interface/Orderable.java 
	**
	*/

	public boolean compare(int op,
						   DataValueDescriptor other,
						   boolean orderedNulls,
						   boolean unknownRV) throws StandardException
	{
		// HeapRowLocation should not be null, ignore orderedNulls
		int result = compare(other);

		switch(op)
		{
		case ORDER_OP_LESSTHAN:
			return (result < 0); // this < other
		case ORDER_OP_EQUALS:
			return (result == 0);  // this == other
		case ORDER_OP_LESSOREQUALS:
			return (result <= 0);  // this <= other
		default:

            if (SanityManager.DEBUG)
                SanityManager.THROWASSERT("Unexpected operation");
			return false;
		}
	}

	public int compare(DataValueDescriptor other) throws StandardException
	{
		// REVISIT: do we need this check?
        if (SanityManager.DEBUG)
            SanityManager.ASSERT(other instanceof HBaseRowLocation);

		HBaseRowLocation arg = (HBaseRowLocation) other;

		if (this.slice.hashCode() < arg.slice.hashCode())
			return -1;
		else if (this.slice.hashCode() > arg.slice.hashCode())
			return 1;
		else
			return 0;
	}

	/*
	** Methods of HeapRowLocation
	*/



	/* For cloning */
	public HBaseRowLocation(HBaseRowLocation other) {
		this.slice = other.slice;
	}

	/*
	 * Storable interface, implies Externalizable, TypedFormat
	 */

	/**
		Return my format identifier.

		@see org.apache.derby.iapi.services.io.TypedFormat#getTypeFormatId
	*/
	public int getTypeFormatId() {
		return StoredFormatIds.ACCESS_HEAP_ROW_LOCATION_V1_ID;
	}

    public boolean isNull() {
    	return slice == null;
    }

	public void writeExternal(ObjectOutput out) throws IOException {
		out.writeObject(slice);
	}
	/**
	  @exception java.lang.ClassNotFoundException A class needed to read the
	  stored form of this object could not be found.
	  @see java.io.Externalizable#readExternal
	  */
	public void readExternal(ObjectInput in)  throws IOException, ClassNotFoundException {
		slice = (ByteSlice) in.readObject();
	}
	public void readExternalFromArray(ArrayInputStream in) throws IOException, ClassNotFoundException {
		
	}

    public void restoreToNull() {
    }
    
	protected void setFrom(DataValueDescriptor theValue)  {
        if (SanityManager.DEBUG)
            SanityManager.ASSERT(theValue instanceof HBaseRowLocation,
                    "Should only be set from another HeapRowLocation");
        HBaseRowLocation that = (HBaseRowLocation) theValue;
        this.slice = that.slice;
	}
	/*
	**		Methods of Object
	*/

	/**
		Implement value equality.
		<BR>
		MT - Thread safe
	*/
	public boolean equals(Object ref)  {
		if ((ref instanceof HBaseRowLocation)) {
            HBaseRowLocation other = (HBaseRowLocation) ref;
            return((this.slice == other.slice) && (this.slice == other.slice));
        }
        else {
			return false;
        }

	}

	/**
		Return a hashcode based on value.
		<BR>
		MT - thread safe
	*/
	public int hashCode()  {
		return this.slice.hashCode();
	}

    /*
     * Standard toString() method.
     */
    public String toString() {
        return("(row key "+this.slice+")");
    }
}
