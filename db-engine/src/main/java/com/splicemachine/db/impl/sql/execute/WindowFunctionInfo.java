/*
 * This file is part of Splice Machine.
 * Splice Machine is free software: you can redistribute it and/or modify it under the terms of the
 * GNU Affero General Public License as published by the Free Software Foundation, either
 * version 3, or (at your option) any later version.
 * Splice Machine is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU Affero General Public License for more details.
 * You should have received a copy of the GNU Affero General Public License along with Splice Machine.
 * If not, see <http://www.gnu.org/licenses/>.
 *
 * Some parts of this source code are based on Apache Derby, and the following notices apply to
 * Apache Derby:
 *
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
 * Splice Machine, Inc. has modified the Apache Derby code in this file.
 *
 * All such Splice Machine modifications are Copyright 2012 - 2017 Splice Machine, Inc.,
 * and are licensed to you under the GNU Affero General Public License.
 */

package com.splicemachine.db.impl.sql.execute;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

import com.splicemachine.db.iapi.services.io.Formatable;
import com.splicemachine.db.iapi.services.io.FormatableArrayHolder;
import com.splicemachine.db.iapi.services.io.FormatableHashtable;
import com.splicemachine.db.iapi.services.io.StoredFormatIds;
import com.splicemachine.db.iapi.sql.ResultDescription;
import com.splicemachine.db.iapi.store.access.ColumnOrdering;
import static com.splicemachine.db.iapi.sql.compile.AggregateDefinition.*;
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
	int[] operandColNums;
	int		outputColumn;
	int		aggregatorColumn;
	String	functionClassName;
	ResultDescription resultDescription;
    FormatableArrayHolder partitionInfo;
    FormatableArrayHolder orderByInfo;
    FormatableArrayHolder keyInfo;
    FormatableHashtable frameInfo;
    FormatableHashtable functionSpecificArgs;
	FunctionType type;

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
	 * @param operandColNumns the function operand column numbers
	 * @param outputColNum	the output column number
	 * @param aggregatorColNum the column number in which the window function class is stored.
	 * @param resultDescription	the result description
	 *
	 */
	public WindowFunctionInfo(String functionName,
							  FunctionType type,
                              String functionClassName,
                              int[] operandColNumns,
                              int outputColNum,
                              int aggregatorColNum,
                              ResultDescription resultDescription,
                              FormatableArrayHolder partitionInfo,
                              FormatableArrayHolder orderByInfo,
                              FormatableArrayHolder keyInfo,
                              FormatableHashtable frameInfo,
                              FormatableHashtable functionSpecificArgs) {
		this.functionName	= functionName;
		this.type = type;
		this.functionClassName = functionClassName;
		this.operandColNums = operandColNumns;
		this.outputColumn	= outputColNum;
		this.aggregatorColumn = aggregatorColNum;
		this.resultDescription = resultDescription;
        this.partitionInfo = partitionInfo;
        this.orderByInfo = orderByInfo;
        this.keyInfo = keyInfo;
        this.frameInfo = frameInfo;
        this.functionSpecificArgs = functionSpecificArgs;
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
		return operandColNums;
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
	 * @return the resultDescription
	 */
	public ResultDescription getResultDescription()
	{
		return resultDescription;
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

    public FormatableHashtable getFunctionSpecificArgs() {
        return functionSpecificArgs;
    }

    public void setFunctionSpecificArgs(FormatableHashtable functionSpecificArgs) {
        this.functionSpecificArgs = functionSpecificArgs;
    }

    public String toHTMLString() {
        return "Name: "+ functionName + "<br/>" +
            "PartCols: " + (partitionInfo != null ? getColumnNumString(partitionInfo) : "()" ) + "<br/>" +
            "OrderByCols: " + (orderByInfo != null ? getColumnNumString(orderByInfo) : "()") + "<br/>" +
            "KeyCols: " + (keyInfo != null ? getColumnNumString(keyInfo) : "()") + "<br/>" +
            "ResultCol: " + outputColumn + "<br/>" +
            "OperandCols: " + arrayToString(operandColNums) + "<br/>" +
            "FunctionColNum: " + aggregatorColumn + "<br/>";
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

    private String getColumnNumString(FormatableArrayHolder fah) {
        StringBuilder buf = new StringBuilder();
        for (Object icoObj : fah.getArray(IndexColumnOrder.class)) {
            IndexColumnOrder ico = (IndexColumnOrder)icoObj;
            buf.append(ico.getColumnId()).append(',');
        }
        // chop off last ','
        if (buf.length() > 0) buf.setLength(buf.length()-1);
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
        int length = operandColNums.length;
        out.writeInt( length );
        for (int inputColumn : operandColNums) { out.writeInt(inputColumn); }

		out.writeInt(outputColumn);
		out.writeInt(aggregatorColumn);
		out.writeObject(functionClassName);
		out.writeObject(type);
		out.writeObject(resultDescription);
        out.writeObject(partitionInfo);
        out.writeObject(orderByInfo);
        out.writeObject(keyInfo);
        out.writeObject(frameInfo);
        out.writeObject(functionSpecificArgs);
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
        operandColNums = new int[ length ];
        for ( int i = 0; i < length; i++ ) { operandColNums[ i ] = in.readInt(); }

        outputColumn = in.readInt();
		aggregatorColumn = in.readInt();
		functionClassName = (String)in.readObject();
		type = (FunctionType) in.readObject();
		resultDescription = (ResultDescription)in.readObject();
        partitionInfo = (FormatableArrayHolder) in.readObject();
        orderByInfo = (FormatableArrayHolder) in.readObject();
        keyInfo = (FormatableArrayHolder) in.readObject();
        frameInfo = (FormatableHashtable) in.readObject();
        functionSpecificArgs = (FormatableHashtable) in.readObject();
	}
	
	/**
	 * Get the formatID which corresponds to this class.
	 *
	 *	@return	the formatID of this class
	 */
	public int	getTypeFormatId()	{ return StoredFormatIds.WINDOW_FUNCTION_INFO_V01_ID; }

	/**
	 * This is the type of window functions
	 * example : SUM, AVG
	 * @return
	 */

	public FunctionType getType() {
		return type;
	}
}
