/*

   Derby - Class org.apache.derby.impl.sql.execute.WindowFunctionInfo

   Licensed to the Apache Software Foundation (ASF) under one or more
   contributor license agreements.  See the NOTICE file distributed with
   this work for additional information regarding copyright ownership.
   The ASF licenses this file to you under the Apache License, Version 2.0
   (the "License"); you may not use this file except in compliance with
   the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.

 */

package org.apache.derby.impl.sql.execute;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

import org.apache.derby.iapi.services.io.Formatable;
import org.apache.derby.iapi.services.io.FormatableArrayHolder;
import org.apache.derby.iapi.services.io.FormatableHashtable;
import org.apache.derby.iapi.services.io.StoredFormatIds;
import org.apache.derby.iapi.services.sanity.SanityManager;
import org.apache.derby.iapi.sql.ResultDescription;
import org.apache.derby.iapi.store.access.ColumnOrdering;

/**
 * This is a simple class used to store the run time information
 * needed to invoke a window function.  This class is serializable
 * because it is stored with the plan.  It is serializable rather
 * than externalizable because it isn't particularly complicated
 * and presumbably we don't need version control on plans.
 *
 */
public class WindowFunctionInfo implements Formatable
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
	**	method.  OR, since this is something that is used
	**	in stored prepared statements, it is ok to change it
	**	if you make sure that stored prepared statements are
	**	invalidated across releases.
	**
	********************************************************/

	/*
	** See the constructor for the meaning of these fields
	*/
	String	functionName;
	int[]	inputColumns;
	int		outputColumn;
	int		aggregatorColumn;
	String	functionClassName;
	ResultDescription	rd;
    FormatableArrayHolder partitionInfo;
    FormatableArrayHolder orderByInfo;
    FormatableArrayHolder keyInfo;
    FormatableHashtable frameInfo;

    /**
	 * Niladic constructor for Formattable
	 */
	public WindowFunctionInfo() {}

    /**
	 * Consructor
	 *
	 * @param functionName	the name of the window function.  Not
 	 *		actually used anywhere except diagnostics.  Should
	 *		be the names as found in the language (e.g. RANK).
	 * @param functionClassName	the name of the class
	 *		used to process this function.  Function class expected
	 *		to have a no-arg constructor and implement
	 *		WindowFunction.
	 * @param inputColNums	the input column numbers
	 * @param outputColNum	the output column number
	 * @param aggregatorColNum	the column number in which the
	 *		window function is stored.
	 * @param rd	the result description
	 *
	 */
	public WindowFunctionInfo(String functionName,
                              String functionClassName,
                              int[] inputColNums,
                              int outputColNum,
                              int aggregatorColNum,
                              ResultDescription rd,
                              FormatableArrayHolder partitionInfo,
                              FormatableArrayHolder orderByInfo,
                              FormatableArrayHolder keyInfo,
                              FormatableHashtable frameInfo) {
		this.functionName	= functionName;
		this.functionClassName = functionClassName;
		this.inputColumns	= inputColNums;
		this.outputColumn	= outputColNum;
		this.aggregatorColumn = aggregatorColNum;
		this.rd 			= rd;
        this.partitionInfo = partitionInfo;
        this.orderByInfo = orderByInfo;
        this.keyInfo = keyInfo;
        this.frameInfo = frameInfo;
	}

	/**
	 * Get the name of the function (e.g. MAX)
	 *
	 * @return the function name
	 */
	public String getFunctionName()
	{
		return functionName;
	}

	/**
	 * Get the name of the class that implements the user
	 * aggregator for this class.
	 *
	 * @return the window function class name
	 */
	public String getWindowFunctionClassName()
	{
		return functionClassName;
	}


	/**
	 * Get the column number for the aggregator
	 * column.
	 *
	 * @return the window function colid
	 */
	public int getWindowFunctionColNum()
	{
		return aggregatorColumn;
	}

	/**
	 * Get the column numbers for the input
	 * (addend) columns.
	 *
	 * @return the colids
	 */
	public int[] getInputColNums()
	{
		return inputColumns;
	}

	/**
	 * Get the column number for the output
	 * (result) column.
	 *
	 * @return the output colid
	 */
	public int getOutputColNum()
	{
		return outputColumn;
	}

	/**
	 * Get the result description for the input value
	 * to this aggregate.
	 *
	 * @return the rd
	 */
	public ResultDescription getResultDescription()
	{
		return rd;
	}

    public ColumnOrdering[] getPartitionInfo() {
        ColumnOrdering[] partition = null;
        if (partitionInfo != null) {
            partition = (ColumnOrdering[]) partitionInfo.getArray(ColumnOrdering.class);
        }
        return partition;
    }

    public void setPartitionInfo(FormatableArrayHolder partitionInfo) {
        this.partitionInfo = partitionInfo;
    }

    public ColumnOrdering[] getOrderByInfo() {
        ColumnOrdering[] orderings = null;
        if (orderByInfo != null) {
            orderings = (ColumnOrdering[]) orderByInfo.getArray(ColumnOrdering.class);
        }
        return orderings;
    }

    public void setOrderByInfo(FormatableArrayHolder orderByInfo) {
        this.orderByInfo = orderByInfo;
    }


    public ColumnOrdering[] getKeyInfo() {
        ColumnOrdering[] keys = null;
        if (keyInfo != null) {
            keys = (ColumnOrdering[]) keyInfo.getArray(ColumnOrdering.class);
        }
        return keys;
    }

    public void setKeyInfo(FormatableArrayHolder keyInfo) {
        this.keyInfo = keyInfo;
    }
    public FormatableHashtable getFrameInfo() {
        return frameInfo;
    }

    public void setFrameInfo(FormatableHashtable frameInfo) {
        this.frameInfo = frameInfo;
    }

    /**
	 * Get a string for the object
	 *
	 * @return string
	 */
	public String toString()
	{
		if (SanityManager.DEBUG)
		{
			return "WindowFunctionInfo = Name: "+ functionName +
				"\n\tClass: " + functionClassName +
				"\n\tInputCols: " + arrayToString(inputColumns) +
				"\n\tOutputCol: " + outputColumn +
				"\n\tPartCols: " + partitionInfo +
				"\n\tOrderByCols: " + orderByInfo +
				"\n\tKeyCols: " + keyInfo +
				"\n\tWindowFunctionColNum: " + aggregatorColumn +
				"\n\tResultDescription: " + rd;
		}
		else
		{
			return "";
		}
	}

    private static String arrayToString(int[] cols) {
        StringBuilder buf = new StringBuilder();
        if (cols != null && cols.length >0) {
            for (int colID : cols) {
                buf.append(colID).append(',');
            }
            // chop off last ','
            if (buf.length() > 0) buf.setLength(buf.length()-1);
        }
        return buf.toString();
    }


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
 	 * @exception java.io.IOException thrown on error
	 */
	public void writeExternal(ObjectOutput out) throws IOException
	{
		out.writeObject(functionName);

        int length = inputColumns.length;
        out.writeInt( length );
        for (int inputColumn : inputColumns) { out.writeInt(inputColumn); }

		out.writeInt(outputColumn);
		out.writeInt(aggregatorColumn);
		out.writeObject(functionClassName);
		out.writeObject(rd);
        out.writeObject(partitionInfo);
        out.writeObject(orderByInfo);
        out.writeObject(keyInfo);
        out.writeObject(frameInfo);
	}

	/**
	 * Read this object from a stream of stored objects.
	 *
	 * @param in read this.
	 *
	 * @exception java.io.IOException					thrown on error
	 * @exception ClassNotFoundException		thrown on error
	 */
	public void readExternal(ObjectInput in)
		throws IOException, ClassNotFoundException
	{
		functionName = (String)in.readObject();

        int length = in.readInt();
        inputColumns = new int[ length ];
        for ( int i = 0; i < length; i++ ) { inputColumns[ i ] = in.readInt(); }

        outputColumn = in.readInt();
		aggregatorColumn = in.readInt();
		functionClassName = (String)in.readObject();
		rd = (ResultDescription)in.readObject();
        partitionInfo = (FormatableArrayHolder) in.readObject();
        orderByInfo = (FormatableArrayHolder) in.readObject();
        keyInfo = (FormatableArrayHolder) in.readObject();
        frameInfo = (FormatableHashtable) in.readObject();
	}
	
	/**
	 * Get the formatID which corresponds to this class.
	 *
	 *	@return	the formatID of this class
	 */
	public int	getTypeFormatId()	{ return StoredFormatIds.WINDOW_FUNCTION_INFO_V01_ID; }
}
