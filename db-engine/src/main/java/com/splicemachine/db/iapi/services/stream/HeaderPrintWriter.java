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

package com.splicemachine.db.iapi.services.stream;

import java.io.PrintWriter;

/**
 * A HeaderPrintWriter is like a PrintWriter with support for
 * including a header in the output. It is expected users
 * will use HeaderPrintWriters to prepend headings to trace
 * and log messages.
 * 
 */
public interface HeaderPrintWriter
{
	/**
	 * Puts out some setup info for
	 * the current write and the write(s) that will be put out next.
	 * It ends with a \n\r.
	 * <p>
	 * All other writes to the stream use the
	 * PrintStream interface.
	 */
	public void printlnWithHeader(String message);

	/**
	 * Return the header for the stream.
	 */
	public PrintWriterGetHeader getHeader();
	
	/**
	 * Gets a PrintWriter object for writing to this HeaderPrintWriter.
	 * Users may use the HeaderPrintWriter to access methods not included
	 * in this interface or to invoke methods or constructors which require
	 * a PrintWriter. 
	 *
	 * Interleaving calls to a printWriter and its associated HeaderPrintWriter
	 * is not supported.
	 * 
	 */
	public PrintWriter getPrintWriter();

	/**
	 * Gets the name of the wrapped writer or stream
	 */
	public String getName ();

	/*
	 * The routines that mimic java.io.PrintWriter...
	 */
	/**
	 * @see java.io.PrintWriter#print
	 */
	public void print(String message);

	/**
	 * @see java.io.PrintWriter#println
	 */
	public void println(String message);

	/**
	 * @see java.io.PrintWriter#println
	 */
	public void println(Object message);

	/**
	* @see java.io.PrintWriter#flush
	 */
	public void flush();
}

