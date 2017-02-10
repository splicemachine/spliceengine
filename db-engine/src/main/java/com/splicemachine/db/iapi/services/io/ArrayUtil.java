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

package com.splicemachine.db.iapi.services.io;

import java.io.ObjectOutput;
import java.io.ObjectInput;
import java.io.IOException;

/**
  Utility class for constructing and reading and writing arrays from/to
  formatId streams.
 
  @version 0.1
 */
public abstract class ArrayUtil
{
	///////////////////////////////////////////////////////////////////
	//
	// Methods for Arrays of OBJECTS.  Cannot be used for an
	// array of primitives, see below for something for primitives
	//
	///////////////////////////////////////////////////////////////////
	/**
	  Write the length of an array of objects to an output stream.

	  The length

	  @param	out		ObjectOutput stream
	  @param	a		array of objects whose length should be written.

	  @exception java.io.IOException The write caused an IOException. 
	  */
	public static void writeArrayLength(ObjectOutput out, Object[] a)
		 throws IOException
	{
		out.writeInt(a.length);
	}

	/**
	  Write an array of objects to an output stream.

	  @param	out		Object output stream to write to.
	  @param	a		array of objects to write.

	  @exception java.io.IOException The write caused an IOException. 
	  */
	public static void writeArrayItems(ObjectOutput out, Object[] a)
		 throws IOException
	{
		if (a == null)
			return;

		for(int ix = 0; ix < a.length; ix++)
		{	out.writeObject(a[ix]); }
	}

	/**
	  Write an array of objects and length to an output stream.
	  Does equivalent of writeArrayLength() followed by writeArrayItems()

	  @param	out		Object output stream to write to.
	  @param	a		array of objects to write.

	  @exception java.io.IOException The write caused an IOException. 
	  */
	public static void writeArray(ObjectOutput out, Object[] a)
		 throws IOException
	{
		if (a == null) 
		{
			out.writeInt(0);
			return;
		}

		out.writeInt(a.length);
		for(int ix = 0; ix < a.length; ix++)
		{	out.writeObject(a[ix]); }
	}

	/**
	  Read an array of objects out of a stream.

	  @param	in	Input stream
	  @param	a	array to read into

	  @exception java.io.IOException The write caused an IOException. 
	  @exception java.lang.ClassNotFoundException The Class for an Object we are reading does not exist
	  */
	public static void readArrayItems(ObjectInput in, Object[] a)
		 throws IOException, ClassNotFoundException
	{
		for (int ix=0; ix<a.length; ix++)
		{
			a[ix]=in.readObject();
		}
	}

	/**
	  Read the length of an array of objects in an object stream.

	  @param	in	Input stream.

	  @return	length of the array of objects
	  
	  @exception java.io.IOException The write caused an IOException. 
	  */
	public static int readArrayLength(ObjectInput in)
		 throws IOException
	{
		return in.readInt();
	}

	/**
	  Reads an array of objects from the stream.

	  @param	in	Input stream

	  @exception java.io.IOException The write caused an IOException. 
	  @exception java.lang.ClassNotFoundException The Class for an Object we are reading does not exist
	  */
	public static Object[] readObjectArray(ObjectInput in)
		 throws IOException, ClassNotFoundException
	{
		int	size = in.readInt();
		if ( size == 0 ) { return null; }

		Object[]	result = new Object[ size ];

		readArrayItems( in, result );

		return result;
	}

	///////////////////////////////////////////////////////////////////
	//
	// Methods for Arrays of INTs
	//
	///////////////////////////////////////////////////////////////////

	/**
	  Write an array of integers to an ObjectOutput. This writes the array
	  in a format readIntArray understands.

	  @param out the ObjectOutput.
	  @param a the array.
	  @exception java.io.IOException The write caused an IOException. 
	  */
	public static void writeIntArray(ObjectOutput out, int[] a) throws IOException {
		if (a == null)
			out.writeBoolean(false);
		else {
			out.writeBoolean(true);
			out.writeInt(a.length);
			for (int i=0; i<a.length; i++)
				out.writeInt(a[i]);
		}
	}

	/**
	  Read an array of integers from an ObjectInput. This allocates the
	  array.

	  @param	in	the ObjectInput.
	  @return   the array of integers.

	  @exception java.io.IOException The write caused an IOException. 
	  */
	public static int[] readIntArray(ObjectInput in) throws IOException {
		if (!in.readBoolean())
			return null;
		int length = in.readInt();
		int[] a = new int[length];
		for (int i=0; i<length; i++)
			a[i] = in.readInt();
		return a;
	}

	public	static	void	writeInts( ObjectOutput out, int[][] val )
		throws IOException
	{
		if (val == null)
		{
			out.writeBoolean(false);
		}
		else
		{
			out.writeBoolean(true);

			int	count = val.length;
			out.writeInt( count );

			for (int i = 0; i < count; i++)
			{
				ArrayUtil.writeIntArray( out, val[i] );
			}
		}
	}

	public	static	int[][]	readInts( ObjectInput in )
		 throws IOException, ClassNotFoundException
	{
		int[][]	retVal = null;

		if ( in.readBoolean() )
		{
			int	count = in.readInt();

			retVal = new int[ count ][];

			for (int i = 0; i < count; i++)
			{
				retVal[ i ] = ArrayUtil.readIntArray( in );
			}
		}

		return retVal;
	}

    public static String toString(int[] value)
    {
        if (value == null || value.length == 0)
        {
            return "null";
        }
        else
        {
            StringBuffer ret_val = new StringBuffer();
            for (int i = 0; i < value.length; i++)
            {
                ret_val.append("[").append(value[i]).append("],");
            }
            return ret_val.toString();
        }
    }


	///////////////////////////////////////////////////////////////////
	//
	// Methods for Arrays of LONGs
	//
	///////////////////////////////////////////////////////////////////

	/**
	  Write an array of longs to an ObjectOutput. This writes the array
	  in a format readLongArray understands.

	  @param out the ObjectOutput.
	  @param a the array.
	  @exception java.io.IOException The write caused an IOException. 
	  */
	public static void writeLongArray(ObjectOutput out, long[] a) throws IOException {
		if (a == null)
			out.writeInt(0);
		else {
			out.writeInt(a.length);
			for (int i=0; i<a.length; i++)
				out.writeLong(a[i]);
		}
	}

	/**
	  Read an array of integers from an ObjectInput. This allocates the
	  array.

	  @param	in	the ObjectInput.
	  @return   the array of integers.

	  @exception java.io.IOException The write caused an IOException. 
	  */
	public static long[] readLongArray(ObjectInput in) throws IOException {
		int length = in.readInt();
		long[] a = new long[length];
		for (int i=0; i<length; i++)
			a[i] = in.readLong();
		return a;
	}

	/**
	  Read an array of strings from an ObjectInput. This allocates the
	  array.

	  @param	in	the ObjectInput.
	  @return   the array of integers.

	  @exception java.io.IOException The write caused an IOException. 
	  */
	public static String[] readStringArray(ObjectInput in) 
		throws IOException, ClassNotFoundException
	{
		Object[] objArray = readObjectArray(in);
		int size = 0;

		if (objArray == null)
			return null;

		String[] stringArray = new String[size = objArray.length];

		for (int i = 0; i < size; i++)
		{
			stringArray[i] = (String)objArray[i];
		} 

		return stringArray;
	}
	
	///////////////////////////////////////////////////////////////////
	//
	// Methods for Arrays of BOOLEANS
	//
	///////////////////////////////////////////////////////////////////

	/**
	  Write an array of booleans to an ObjectOutput. This writes the array
	  in a format readBooleanArray understands.

	  @param out the ObjectOutput.
	  @param a the array.
	  @exception java.io.IOException The write caused an IOException. 
	  */
	public static void writeBooleanArray(ObjectOutput out, boolean[] a) throws IOException {
		if (a == null)
			out.writeInt(0);
		else {
			out.writeInt(a.length);
			for (int i=0; i<a.length; i++)
				out.writeBoolean(a[i]);
		}
	}

	/**
	  Read an array of integers from an ObjectInput. This allocates the
	  array.

	  @param	in	the ObjectInput.
	  @return   the array of integers.

	  @exception java.io.IOException The write caused an IOException. 
	  */
	public static boolean[] readBooleanArray(ObjectInput in) throws IOException {
		int length = in.readInt();
		boolean[] a = new boolean[length];
		for (int i=0; i<length; i++)
			a[i] = in.readBoolean();
		return a;
	}

	/**
	 * Write a byte array to an ObjectOutput. This writes the array in a format readBytesArray understands.
	 *
	 * @param out the ObjectOutput
	 * @param a a byte array
	 * @throws IOException
	 */
	public static void writeByteArray(ObjectOutput out, byte[] a) throws IOException {
		if (a==null)
			out.writeInt(0);
		else {
			out.writeInt(a.length);
			out.write(a);
		}
	}

	/**
	 *  Read a byte array from an ObjectInput. This allocates the array.
	 *
	 * @param in the ObjectInput.
	 * @return a byte array
	 * @throws IOException
	 */
	public static byte[] readByteArray(ObjectInput in) throws IOException {
		int size = in.readInt();
		byte[] b = new byte[size];
		in.read(b);
		return b;
	}
}
