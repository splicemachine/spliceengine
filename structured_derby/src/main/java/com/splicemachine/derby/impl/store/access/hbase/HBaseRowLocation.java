package com.splicemachine.derby.impl.store.access.hbase;

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
 *     @author jessiezhang: currently we use an UUIDHexGenerator to generate RowKey (not use PrimaryKey from
 *     						the base table so I will comment out primary key impl). I am making this class as byte[] DataType
 **/

public class HBaseRowLocation extends DataType implements RowLocation {

	private byte[] rowKey;
    private static final int BASE_MEMORY_USAGE = ClassSize.estimateBaseFromCatalog( HBaseRowLocation.class);
    private static final int RECORD_HANDLE_MEMORY_USAGE = ClassSize.estimateBaseFromCatalog( org.apache.derby.impl.store.raw.data.RecordId.class);

	public HBaseRowLocation()
	{
	}
		
	public HBaseRowLocation(byte[] rowKey) {
		this.rowKey = rowKey;
	}
	
    public int estimateMemoryUsage()
    {
        return BASE_MEMORY_USAGE;
    } // end of estimateMemoryUsage

    public final void setValue(byte[] theValue)
	{
		this.rowKey = theValue;
	}
    
    public final byte[]	getBytes() throws StandardException
	{
		return this.rowKey;
	}
    
	public String getTypeName() {
		return "HBaseRowLocation";
	}

	public void setValueFromResultSet(java.sql.ResultSet resultSet, int colNumber,
		boolean isNullable) {
	}

	public DataValueDescriptor getNewNull() {
		return new HBaseRowLocation();
	}

	public Object getObject() {
		return this.rowKey;
	}

	public DataValueDescriptor cloneValue(boolean forceMaterialization) {
		return new HBaseRowLocation(this);
	}

	public String getString() {
		return toString();
	}
	
	public int getLength() {
		return this.rowKey == null ? 0 : this.rowKey.length;//what is the length of the primary key?
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

		if (this.rowKey.hashCode() < arg.rowKey.hashCode())
			return -1;
		else if (this.rowKey.hashCode() > arg.rowKey.hashCode())
			return 1;
		else
			return 0;
	}

	/*
	** Methods of HeapRowLocation
	*/



	/* For cloning */
	public HBaseRowLocation(HBaseRowLocation other) {
		this.rowKey = other.rowKey;
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
    	return rowKey == null;
    }

	public void writeExternal(ObjectOutput out) 
        throws IOException
    {
		//Need it? This is already byte array
		//int len = this.rowKey.length;
        //writeLength( out, len );
		//out.write(this.rowKey, 0, this.rowKey.length);
    }

	/*private void writeLength( ObjectOutput out, int len ) throws IOException
    {
		if (len <= 31)
		{
			out.write((byte) (0x80 | (len & 0xff)));
		}
		else if (len <= 0xFFFF)
		{
			out.write((byte) 0xA0);
			out.writeShort((short) len);
		}
		else
		{
			out.write((byte) 0xC0);
			out.writeInt(len);

		}
    }*/
	/**
	  @exception java.lang.ClassNotFoundException A class needed to read the
	  stored form of this object could not be found.
	  @see java.io.Externalizable#readExternal
	  */
	public void readExternal(ObjectInput in) 
        throws IOException, ClassNotFoundException
    {
        //FIXME: need it?
    }
	public void readExternalFromArray(ArrayInputStream in) 
        throws IOException, ClassNotFoundException
    {
        //FIXME: need it?
    }

    public void restoreToNull() {
	//	if (SanityManager.DEBUG) 
	//		SanityManager.THROWASSERT("HBaseRowLocation is never null");
    }
    
	protected void setFrom(DataValueDescriptor theValue)  {
        if (SanityManager.DEBUG)
            SanityManager.ASSERT(theValue instanceof HBaseRowLocation,
                    "Should only be set from another HeapRowLocation");
        HBaseRowLocation that = (HBaseRowLocation) theValue;
        //this.primaryKey = that.primaryKey;
        //this.primaryKeyType = that.primaryKeyType;
        this.rowKey = that.rowKey;
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
            return((this.rowKey == other.rowKey) && (this.rowKey == other.rowKey));
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
	public int hashCode() 
    {
		return this.rowKey.hashCode();
	}

    /*
     * Standard toString() method.
     */
    public String toString()
    {
        //return ("(" + this.primaryKey + " of type " + this.primaryKeyType + ")";
        return("(row key "+this.rowKey+")");
    }
}
