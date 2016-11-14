/*
 * Apache Derby is a subproject of the Apache DB project, and is licensed under
 * the Apache License, Version 2.0 (the "License"); you may not use these files
 * except in compliance with the License. You may obtain a copy of the License at:
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 * Splice Machine, Inc. has modified this file.
 *
 * All Splice Machine modifications are Copyright 2012 - 2016 Splice Machine, Inc.,
 * and are licensed to you under the License; you may not use this file except in
 * compliance with the License.
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 */

package com.splicemachine.db.impl.sql;

import com.splicemachine.db.catalog.types.RoutineAliasInfo;
import com.splicemachine.db.iapi.sql.ResultColumnDescriptor;
import com.splicemachine.db.iapi.types.DataTypeDescriptor;

import com.splicemachine.db.iapi.services.sanity.SanityManager;

import com.splicemachine.db.iapi.services.io.StoredFormatIds;
import com.splicemachine.db.iapi.services.io.Formatable;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;

import java.io.ObjectOutput;
import java.io.ObjectInput;
import java.io.IOException;
import java.sql.Types;

/**
 * This is a stripped down implementation of a column
 * descriptor that is intended for generic use.  It
 * can be seralized and attached to plans.
 */
public final class GenericColumnDescriptor
	implements ResultColumnDescriptor, Formatable
{

	/********************************************************
	**
	**	This class implements Formatable. That means that it
	**	can write itself to and from a formatted stream. If
	**	you add more fields to this class, make sure that you
	**	also write/read them with the writeExternal()/readExternal()
	**	methods.
	**
	**	If, inbetween releases, you add more fields to this class,
	**	then you should bump the version number emitted by the getTypeFormatId()
	**	method.
	**
	********************************************************/

	private String				name;
	private String				schemaName;
	private String				tableName;
	private int					columnPos;
	private DataTypeDescriptor	type;
	private boolean 			isAutoincrement;
	private boolean 			updatableByCursor;
    private boolean             hasGenerationClause;

	/**
	 * Niladic constructor for Formatable
	 */
	public GenericColumnDescriptor()
	{
	}

	public GenericColumnDescriptor(String name, DataTypeDescriptor	type) {
		this.name = name;
		this.type = type;
	}

	/** 
	 * This constructor is used to build a generic (and
	 * formatable) ColumnDescriptor.  The idea is that
	 * it can be passed a ColumnDescriptor from a query
	 * tree and convert it to something that can be used
	 * anywhere.
	 *
	 * @param rcd the ResultColumnDescriptor
	 */
	public GenericColumnDescriptor(ResultColumnDescriptor rcd)
	{
		name = rcd.getName();
		tableName = rcd.getSourceTableName();
 		schemaName = rcd.getSourceSchemaName();
		columnPos = rcd.getColumnPosition();
		type = rcd.getType();
		isAutoincrement = rcd.isAutoincrement();
		updatableByCursor = rcd.updatableByCursor();
        hasGenerationClause = rcd.hasGenerationClause();
	}

	/**
	 * Returns a DataTypeDescriptor for the column. This DataTypeDescriptor
	 * will not represent an actual value, it will only represent the type
	 * that all values in the column will have.
	 *
	 * @return	A DataTypeDescriptor describing the type of the column.
	 */
	public DataTypeDescriptor	getType()
	{
		return type;
	}

	/**
	 * Returns a Spark Type for the column. This StructField
	 * will not represent an actual value, it will only represent the Spark type
	 * that all values in the column will have.
	 *
	 * @return	A StructField describing the type of the column.
	 */
	public StructField getStructField() {
		DataTypeDescriptor type = getType();
		switch (type.getJDBCTypeId()) {
			case Types.BIGINT:
				return DataTypes.createStructField(getName(), DataTypes.LongType, true);
			case Types.INTEGER:
				return DataTypes.createStructField(getName(), DataTypes.IntegerType, true);
			case Types.SMALLINT:
				return DataTypes.createStructField(getName(), DataTypes.ShortType, true);
			case Types.TINYINT:
				return DataTypes.createStructField(getName(), DataTypes.ShortType, true);
			case Types.DECIMAL:
				return DataTypes.createStructField(getName(), DataTypes.FloatType, true);
			case Types.NUMERIC:
				return DataTypes.createStructField(getName(), DataTypes.DoubleType, true);
			case Types.DOUBLE:
				return DataTypes.createStructField(getName(), DataTypes.DoubleType, true);
			case Types.FLOAT:
				return DataTypes.createStructField(getName(), DataTypes.FloatType, true);
			case Types.CHAR:
				return DataTypes.createStructField(getName(), DataTypes.StringType, true);
			case Types.VARCHAR:
				return DataTypes.createStructField(getName(), DataTypes.StringType, true);
			case Types.DATE:
				return DataTypes.createStructField(getName(), DataTypes.DateType, true);
			case Types.TIMESTAMP:
				return DataTypes.createStructField(getName(), DataTypes.TimestampType, true);
			case Types.BOOLEAN:
				return DataTypes.createStructField(getName(), DataTypes.BooleanType, true);
			case Types.TIME:
				return DataTypes.createStructField(getName(), DataTypes.TimestampType, true); /* TODO: (MZ) Not sure if this is the right conversion */
			case Types.NULL:
				return DataTypes.createStructField(getName(), DataTypes.NullType, true);
			default:
				return DataTypes.createStructField(getName(), DataTypes.NullType, true);
		}
	}



	/**
	 * Returns the name of the Column.
	 *
	 * @return	A String containing the name of the column.
	 */
	public String	getName()
	{
		return name;
	}

	/**
	 * Get the name of the schema for the Column's base table, if any.
	 * Following example queries will all return SPLICE (assuming user is in schema SPLICE)
	 * select t.a from t
	 * select b.a from t as b
	 * select app.t.a from t
	 *
	 * @return	A String containing the name of the schema of the Column's table.
	 *		If the column is not in a schema (i.e. is a derived column), it returns NULL.
	 */
	public String	getSourceSchemaName()
	{
		return schemaName;
	}

	/**
	 * Get the name of the underlying(base) table this column comes from, if any.
	 * Following example queries will all return T
	 * select a from t
	 * select b.a from t as b
	 * select t.a from t
	 *
	 * @return	A String containing the name of the Column's base table.
	 *		If the column is not in a table (i.e. is a derived column), it returns NULL.
	 */
	public String	getSourceTableName()
	{
		return tableName;
	}

	/**
	 * Get the position of the Column.
	 * NOTE - position is 1-based.
	 *
	 * @return	An int containing the position of the Column
	 *		within the table.
	 */
	public int getColumnPosition()
	{
		return columnPos;
	}

    public boolean isAutoincrement()
	{
		return isAutoincrement;
	}

	public boolean updatableByCursor()
	{
		return updatableByCursor;
	}

    public boolean hasGenerationClause() { return hasGenerationClause; }

	//////////////////////////////////////////////
	//
	// FORMATABLE
	//
	//////////////////////////////////////////////
	/**
	 * Write this object out
	 *
	 * @param out write bytes here
	 *
 	 * @exception IOException thrown on error
	 */
	public void writeExternal(ObjectOutput out) throws IOException {
        out.writeUTF(name);
		if (tableName != null) {
            out.writeBoolean(true);
            out.writeUTF(tableName);
        } else {
            out.writeBoolean(false);
        }
        if (schemaName != null) {
            out.writeBoolean(true);
            out.writeUTF(schemaName);
        } else {
            out.writeBoolean(false);
        }
		out.writeInt(columnPos);
		out.writeObject(type);
		out.writeBoolean(isAutoincrement);
		out.writeBoolean(updatableByCursor);		
	}

	/**
	 * Read this object from a stream of stored objects.
	 *
	 * @param in read this.
	 *
	 * @exception IOException					thrown on error
	 * @exception ClassNotFoundException		thrown on error
	 */
	public void readExternal(ObjectInput in) 
		throws IOException, ClassNotFoundException {
        name = in.readUTF();
		if (in.readBoolean()) {
            tableName = in.readUTF();
        }
        if (in.readBoolean()) {
		    schemaName = in.readUTF();
        }
		columnPos = in.readInt();
		type = getStoredDataTypeDescriptor(in.readObject());
		isAutoincrement = in.readBoolean();
		updatableByCursor = in.readBoolean();
	}
	
	/**
	 * Get the formatID which corresponds to this class.
	 *
	 *	@return	the formatID of this class
	 */
	public	int getTypeFormatId()	{ return StoredFormatIds.GENERIC_COLUMN_DESCRIPTOR_V02_ID; }

	public String toString()
	{
		if (SanityManager.DEBUG)
		{
			return "GenericColumnDescriptor\n\tname: "+name+
				"\n\tTable: "+schemaName+"."+tableName+
				"\n\tcolumnPos: "+columnPos+
				"\n\tType: "+type+
				"\n\tisAutoincrement: " + isAutoincrement +
				"\n\thasGenerationClause: " + hasGenerationClause;
		}
		else
		{
			return "";
		}
	}

    /**
     * When retrieving a DataTypeDescriptor, it might just be a regular
     * DataTypeDescriptor or may be an OldRoutineType, as used for Routine
     * parameters and return values prior to DERBY-2775. If it is not a regular
     * DataTypeDescriptor, it must be an OldRoutineType, so convert it to a
     * DataTypeDescriptor DERBY-4913
     * 
     * @param o
     *            object as obtained by fh.get("type") in readExternal
     * @return DataTypeDescriptor
     */
    private DataTypeDescriptor getStoredDataTypeDescriptor(Object o) {

        if (o instanceof DataTypeDescriptor)
            return (DataTypeDescriptor) o;
        else
            // Must be an OldRoutineType, so we will convert it to a
            // DataTypeDescriptor for our purposes
            return DataTypeDescriptor
                    .getType(RoutineAliasInfo.getStoredType(o));
    }

}
