/*

   Derby - Class org.apache.derby.impl.sql.compile.VarbitConstantNode

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

package com.splicemachine.db.impl.sql.compile;

import com.splicemachine.db.iapi.error.StandardException;

import com.splicemachine.db.iapi.util.ReuseFactory;

public final class VarbitConstantNode extends BitConstantNode
{
	/**
	 * Initializer for a VarbitConstantNode.
	 *
	 * @param arg1  The TypeId for the type of the node OR A Bit containing the value of the constant
	 *
	 * @exception StandardException
	 */

	public void init(
						Object arg1)
		throws StandardException
	{
		init(
					arg1,
					Boolean.TRUE,
					ReuseFactory.getInteger(0));

	}
}
