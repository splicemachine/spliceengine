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

package com.splicemachine.db.iapi.error;

import com.splicemachine.db.iapi.services.stream.PrintWriterGetHeader;

import java.io.StringWriter;
import java.io.PrintWriter;

/**
 * Class used to form error messages.  Primary
 * reason for existence is to allow a way to call
 * printStackTrace() w/o automatically writting
 * to a stream.
 */
public class ErrorStringBuilder 
{
	private StringWriter	stringWriter;
	private PrintWriter		printWriter;
	private PrintWriterGetHeader	headerGetter;

	/**
	** Construct an error string builder
	*/
	public ErrorStringBuilder(PrintWriterGetHeader headerGetter)
	{
		this.headerGetter = headerGetter;
		this.stringWriter = new StringWriter();
		this.printWriter = new PrintWriter(stringWriter);
	}

	/**
	** Append an error string 
	**
	** @param s 	the string to append
	*/
	public void append(String s)
	{
		if (headerGetter != null)
			printWriter.print(headerGetter.getHeader());
		printWriter.print(s);
	}


	/**
	** Append an error string with a newline
	**
	** @param s 	the string to append
	*/
	public void appendln(String s)
	{
		if (headerGetter != null)
			printWriter.print(headerGetter.getHeader());
		printWriter.println(s);
	}

	/**
	** Print a stacktrace from the throwable in the error
	** buffer.
	**
	** @param t	the error
	*/
	public void stackTrace(Throwable t)
	{
		int level = 0;
		while(t != null)
		{
			if (level > 0)	
				printWriter.println("============= begin nested exception, level (" +
									level + ") ===========");

			t.printStackTrace(printWriter);

			if (t instanceof java.sql.SQLException) {
				Throwable next = ((java.sql.SQLException)t).getNextException();
				t = (next == null) ? t.getCause() : next;
			} else {
				t = t.getCause();
			}

			if (level > 0)	
				printWriter.println("============= end nested exception, level (" + 
									level + ") ===========");

			level++;

		}

	}

	/**
	** Reset the buffer -- truncate it down to nothing.
	**
	*/
	public void reset()
	{
		// Is this the most effecient way to do this?
		stringWriter.getBuffer().setLength(0);
	}

	/**
	** Get the buffer
	*/
	public StringBuffer get()
	{
		return stringWriter.getBuffer();
	}	
}	
