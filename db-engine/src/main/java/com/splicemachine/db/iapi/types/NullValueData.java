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

package com.splicemachine.db.iapi.types;

/**
 *
 * NullValueData provides a standard way for data types and number types
 * to handle null value and provides an optimization path to check
 * null and give the compiler the best chance to inline isNull()
 *
 * This requires users of this class update isNull on any change that
 * would impact wether the class was considered to be Null (even beyond
 * the class handle being null)
 *
 */
public abstract class NullValueData
{


	protected boolean isNull = true;

	public boolean isNull()
	{
		return isNull;
	}

	public final boolean getIsNull()
	{
		return isNull;
	}

	public final void setIsNull(boolean value)
	{
		isNull = value;
	}
}
