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
 * All such Splice Machine modifications are Copyright 2012 - 2020 Splice Machine, Inc.,
 * and are licensed to you under the GNU Affero General Public License.
 */

package com.splicemachine.db.iapi.util;

import java.util.Properties;
import java.util.Enumeration;
import java.io.InputStream;
import java.io.IOException;

public class PropertyUtil {
	

	//////////////////////////////////////////////////////////////////////////////
	//
	//	SORTS A PROPERTY LIST AND STRINGIFIES THE SORTED PROPERTIES
	//
	/////////////////////////////////////////////////////////////////////////////

	/**
	  *	Sorts a property list and turns the sorted list into a string.
	  *
	  *	@param	list	property list to sort
	  *
	  *	@return	a string version of the sorted list
	  */
	public	static	String	sortProperties( Properties list )
	{
		// stringify them with no indentation
		return sortProperties(list, null);
	}

	/**
	 * Sorts property list and print out each key=value pair prepended with 
	 * specific indentation.  If indent is null, do not prepend with
	 * indentation. 
	 *
	 * The output string shows up in two styles, style 1 looks like
	 * { key1=value1, key2=value2, key3=value3 }
	 *
	 * style 2 looks like
	 *		key1=value1
	 *		key2=value2
	 *		key3=value3
	 * where indent goes between the new line and the keys
	 *
	 * To get style 1, pass in a null indent
	 * To get sytle 2, pass in non-null indent (whatever you want to go before
	 * the key value)
	 */
	public	static	String	sortProperties( Properties list, String indent )
	{
		int				size = list == null ? 0 : list.size();
		int				count = 0;
		String[]		array = new String[size];
		String			key;
		String			value;
		StringBuffer	buffer;

		// Calculate the number of properties in the property list and
		// build an array of all the property names.
		// We need to go thru the enumeration because Properties has a
		// recursive list of defaults.
		if (list != null)
		{
			for (Enumeration propertyNames = list.propertyNames();
				 propertyNames.hasMoreElements(); )
			{
				if (count == size)
				{
					// need to expand the array
					size = size*2;
					String[] expandedArray = new String[size];
					System.arraycopy(array, 0, expandedArray, 0, count);
					array = expandedArray;
				}
				key = (String) propertyNames.nextElement();
				array[ count++ ] = key;
			}

			// now sort the array
			java.util.Arrays.sort(array, 0, count);
		}

		// now stringify the array
		buffer = new StringBuffer();
		if (indent == null)
			buffer.append( "{ " );

		for ( int ictr = 0; ictr < count; ictr++ )
		{
			if ( ictr > 0 && indent == null)
				buffer.append( ", " );

			key = array[ ictr ];

			if (indent != null)
				buffer.append( indent );

			buffer.append( key ); buffer.append( "=" );

			value = list.getProperty( key, "MISSING_VALUE" );
			buffer.append( value );

			if (indent != null)
				buffer.append( "\n" );

		}
		if (indent == null)
			buffer.append( " }" );

		return	buffer.toString();
	}

    /**
     * Copy a set of properties from one Property to another.
     * <p>
     *
     * @param src_prop  Source set of properties to copy from.
     * @param dest_prop Dest Properties to copy into.
     *
     **/
    public static void copyProperties(Properties src_prop, Properties dest_prop)
    {
        for (Enumeration propertyNames = src_prop.propertyNames();
             propertyNames.hasMoreElements(); )
        {
            Object key = propertyNames.nextElement();
            dest_prop.put(key, src_prop.get(key));
        }
    }

	/** 
	 * Read a set of properties from the received input stream, strip
	 * off any excess white space that exists in those property values,
	 * and then add those newly-read properties to the received
	 * Properties object; not explicitly removing the whitespace here can
	 * lead to problems.
	 *
	 * This method exists because of the manner in which the jvm reads
	 * properties from file--extra spaces are ignored after a _key_, but
	 * if they exist at the _end_ of a property decl line (i.e. as part
	 * of a _value_), they are preserved, as outlined in the Java API:
	 *
	 * "Any whitespace after the key is skipped; if the first non-
	 * whitespace character after the key is = or :, then it is ignored
 	 * and any whitespace characters after it are also skipped. All
	 * remaining characters on the line become part of the associated
	 * element string."
	 *
	 * @param	iStr An input stream from which the new properties are to be
	 *  loaded (should already be initialized).
	 * @param prop A set of properties to which the properties from
	 *  iStr will be added (should already be initialized).
	 * properties loaded from 'iStr' (with the extra whitespace (if any)
	 *  removed from all values), will be returned via the parameter.
	 *
	 **/
	public static void loadWithTrimmedValues(InputStream iStr,
		Properties prop) throws IOException {

		if ((iStr == null) || (prop == null)) {
		// shouldn't happen; just ignore this call and return.
			return;
		}

		// Else, load the properties from the received input stream.
		Properties p = new Properties();
		p.load(iStr);

		// Now, trim off any excess whitespace, if any, and then
		// add the properties from file to the received Properties
		// set.
		for (Enumeration propKeys = p.propertyNames();
		  propKeys.hasMoreElements();) {
		// get the value, trim off the whitespace, then store it
		// in the received properties object.
			String tmpKey = (String)propKeys.nextElement();
			String tmpValue = p.getProperty(tmpKey);
			tmpValue = tmpValue.trim();
			prop.put(tmpKey, tmpValue);
		}

    }
}

