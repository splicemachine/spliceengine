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

package com.splicemachine.db.iapi.types;

import com.splicemachine.db.iapi.error.StandardException;

/**
 * The VariableSizeDataValue interface corresponds to 
 * Datatypes that have adjustable width. 
 *
 * The following methods are defined herein:
 *		setWidth()
 *
 */
public interface VariableSizeDataValue 
{

	int IGNORE_PRECISION = -1;

	/*
	 * Set the width and scale (if relevant).  Sort of a poor
	 * man's normalize.  Used when we need to normalize a datatype
	 * but we don't want to use a NormalizeResultSet (e.g.
	 * for an operator that can change the width/scale of a
	 * datatype, namely CastNode).
	 *
	 * @param desiredWidth width
	 * @param desiredScale scale, if relevant (ignored for strings)
	 * @param errorOnTrunc	throw an error on truncation of value
	 *
	 * @exception StandardException		Thrown on error
	 */
	void setWidth(int desiredWidth,
				  int desiredScale,
				  boolean errorOnTrunc)
							throws StandardException;
}
