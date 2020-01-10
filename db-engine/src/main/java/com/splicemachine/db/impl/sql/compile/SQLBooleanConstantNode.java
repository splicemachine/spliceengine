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

package com.splicemachine.db.impl.sql.compile;

import com.splicemachine.db.iapi.types.TypeId;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.services.compiler.MethodBuilder;
import com.splicemachine.db.iapi.types.SQLBoolean;
import com.splicemachine.db.iapi.services.sanity.SanityManager;
import com.splicemachine.db.iapi.util.ReuseFactory;
import com.splicemachine.db.iapi.util.StringUtil;

public class SQLBooleanConstantNode extends ConstantNode
{
	/**
	 * Initializer for a SQLBooleanConstantNode.
	 *
	 * @param newValue	A String containing the value of the constant: true, false, unknown
	 *
	 * @exception StandardException
	 */

	public void init(
					Object newValue)
		throws StandardException
	{
		String strVal = (String) newValue;
		Boolean val = null;

		if (SanityManager.DEBUG)
		{
			SanityManager.ASSERT((StringUtil.SQLEqualsIgnoreCase(strVal,"true")) ||
								(StringUtil.SQLEqualsIgnoreCase(strVal,"false")) ||
								(StringUtil.SQLEqualsIgnoreCase(strVal,"unknown")),
								"String \"" + strVal +
								"\" cannot be converted to a SQLBoolean");
		}

		if (StringUtil.SQLEqualsIgnoreCase(strVal,"true"))
			val = Boolean.TRUE;
		else if (StringUtil.SQLEqualsIgnoreCase(strVal,"false"))
			val = Boolean.FALSE;

		/*
		** RESOLVE: The length is fixed at 1, even for nulls.
		** Is that OK?
		*/

		/* Fill in the type information in the parent ValueNode */
		super.init(
			 TypeId.BOOLEAN_ID,
			 Boolean.TRUE,
			 ReuseFactory.getInteger(1));

		setValue(new SQLBoolean(val));
	}

	/**
	 * This generates the proper constant.  It is implemented
	 * by every specific constant node (e.g. IntConstantNode).
	 *
	 * @param acb	The ExpressionClassBuilder for the class being built
	 * @param mb	The method the expression will go into
	 *
	 *
	 * @exception StandardException		Thrown on error
	 */
	void generateConstant(ExpressionClassBuilder acb, MethodBuilder mb)
		throws StandardException
	{
		mb.push(value.getBoolean());
	}
	
	public int hashCode(){
		return value ==null? 0: value.hashCode();
	}
}
