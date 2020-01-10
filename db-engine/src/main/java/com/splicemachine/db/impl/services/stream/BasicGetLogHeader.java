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

package com.splicemachine.db.impl.services.stream;

import java.util.Date;
import com.splicemachine.db.iapi.services.stream.PrintWriterGetHeader;

/**
 * Get a header to prepend to a line of output. *
 * A HeaderPrintWriter requires an object which implements
 * this interface to construct line headers.
 *
 * @see com.splicemachine.db.iapi.services.stream.HeaderPrintWriter
 */

class BasicGetLogHeader implements PrintWriterGetHeader
{

	/* 
	 * STUB: This should take a header template. Check if
	 *		 the error message facility provides something.
	 *	
	 *		 This should be localizable. How?
	 */
	/**
	 * Constructor for a BasicGetLogHeader object.
	 * <p>
	 * @param doThreadId	true means include the calling thread's
	 *							id in the header.
	 * @param doTimeStamp	true means include the current time in 
	 *							the header.
	 * @param tag			A string to prefix the header. null
	 *						means don't prefix the header with
	 *						a string.
	 */
	BasicGetLogHeader(boolean doThreadId,
				boolean doTimeStamp,
				String tag){
	}
	
	public String getHeader()
	{
		return ""; // no need for headers
	}
}
	
