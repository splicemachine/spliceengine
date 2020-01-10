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

package com.splicemachine.db.iapi.sql.compile;

import com.splicemachine.db.iapi.services.context.ContextManager;

import com.splicemachine.db.iapi.error.StandardException;

/**
	This is an interface for NodeFactories.
	<p>
	There is expected to be only one of these configured per database.

 */

public abstract class NodeFactory
{
	/**
		Module name for the monitor's module locating system.
	 */
	public static final String MODULE = "com.splicemachine.db.iapi.sql.compile.NodeFactory";

	/**
	 * Tell whether to do join order optimization.
	 *
	 * @return	Boolean.TRUE means do join order optimization, Boolean.FALSE
	 *			means don't do it.
	 */
	public abstract Boolean	doJoinOrderOptimization();

	/**
	 * Get a node that takes no initializer arguments.
	 *
	 * @param nodeType		Identifier for the type of node.
	 * @param cm			A ContextManager
	 *
	 * @return	A new QueryTree node.
	 *
	 * @exception StandardException		Thrown on error.
	 */
	public abstract Node getNode(int nodeType,
							ContextManager cm) throws StandardException;

	/**
	 * Get a node that takes one initializer argument.
	 *
	 * @param nodeType		Identifier for the type of node.
	 * @param arg1	The initializer argument
	 * @param cm			A ContextManager
	 *
	 * @return	A new QueryTree node.
	 *
	 * @exception StandardException		Thrown on error.
	 */
	public final Node getNode(int nodeType, Object arg1, ContextManager cm)
													throws StandardException
	{
		Node retval = getNode(nodeType, cm);

		retval.init(arg1);

		return  retval;
	}

	/**
	 * Get a node that takes two initializer arguments.
	 *
	 * @param nodeType		Identifier for the type of node.
	 * @param arg1	An initializer argument
	 * @param arg2	An initializer argument
	 * @param cm			A ContextManager
	 *
	 * @return	A new QueryTree node.
	 *
	 * @exception StandardException		Thrown on error.
	 */
	public final Node getNode(int nodeType,
							Object arg1,
							Object arg2,
							ContextManager cm)
								throws StandardException
	{
		Node retval =  getNode(nodeType, cm);

		retval.init(arg1, arg2);

		return  retval;
	}

	/**
	 * Get a node that takes three initializer arguments.
	 *
	 * @param nodeType		Identifier for the type of node.
	 * @param arg1	An initializer argument
	 * @param arg2	An initializer argument
	 * @param arg3	An initializer argument
	 * @param cm			A ContextManager
	 *
	 * @return	A new QueryTree node.
	 *
	 * @exception StandardException		Thrown on error.
	 */
	public final Node getNode(int nodeType,
							Object arg1,
							Object arg2,
							Object arg3,
							ContextManager cm)
								throws StandardException
	{
		Node retval =  getNode(nodeType, cm);

		retval.init(arg1, arg2, arg3);

		return  retval;
	}

	/**
	 * Get a node that takes four initializer arguments.
	 *
	 * @param nodeType		Identifier for the type of node.
	 * @param arg1	An initializer argument
	 * @param arg2	An initializer argument
	 * @param arg3	An initializer argument
	 * @param arg4	An initializer argument
	 * @param cm			A ContextManager
	 *
	 * @return	A new QueryTree node.
	 *
	 * @exception StandardException		Thrown on error.
	 */
	public Node getNode(int nodeType,
							Object arg1,
							Object arg2,
							Object arg3,
							Object arg4,
							ContextManager cm)
								throws StandardException
	{
		Node retval =  getNode(nodeType, cm);

		retval.init(arg1, arg2, arg3, arg4);

		return  retval;
	}


	/**
	 * Get a node that takes five initializer arguments.
	 *
	 * @param nodeType		Identifier for the type of node.
	 * @param arg1	An initializer argument
	 * @param arg2	An initializer argument
	 * @param arg3	An initializer argument
	 * @param arg4	An initializer argument
	 * @param arg5	An initializer argument
	 * @param cm			A ContextManager
	 *
	 * @return	A new QueryTree node.
	 *
	 * @exception StandardException		Thrown on error.
	 */
	public Node getNode(int nodeType,
							Object arg1,
							Object arg2,
							Object arg3,
							Object arg4,
							Object arg5,
							ContextManager cm)
								throws StandardException
	{
		Node retval =  getNode(nodeType, cm);

		retval.init(arg1, arg2, arg3, arg4, arg5);

		return  retval;
	}

	/**
	 * Get a node that takes six initializer arguments.
	 *
	 * @param nodeType		Identifier for the type of node.
	 * @param arg1	An initializer argument
	 * @param arg2	An initializer argument
	 * @param arg3	An initializer argument
	 * @param arg4	An initializer argument
	 * @param arg5	An initializer argument
	 * @param arg6	An initializer argument
	 * @param cm			A ContextManager
	 *
	 * @return	A new QueryTree node.
	 *
	 * @exception StandardException		Thrown on error.
	 */
	public final Node getNode(int nodeType,
							Object arg1,
							Object arg2,
							Object arg3,
							Object arg4,
							Object arg5,
							Object arg6,
							ContextManager cm)
								throws StandardException
	{
		Node retval =  getNode(nodeType, cm);

		retval.init(arg1, arg2, arg3, arg4, arg5, arg6);

		return  retval;
	}

	/**
	 * Get a node that takes seven initializer arguments.
	 *
	 * @param nodeType		Identifier for the type of node.
	 * @param arg1	An initializer argument
	 * @param arg2	An initializer argument
	 * @param arg3	An initializer argument
	 * @param arg4	An initializer argument
	 * @param arg5	An initializer argument
	 * @param arg6	An initializer argument
	 * @param arg7	An initializer argument
	 * @param cm			A ContextManager
	 *
	 * @return	A new QueryTree node.
	 *
	 * @exception StandardException		Thrown on error.
	 */
	public final Node getNode(int nodeType,
							Object arg1,
							Object arg2,
							Object arg3,
							Object arg4,
							Object arg5,
							Object arg6,
							Object arg7,
							ContextManager cm)
								throws StandardException
	{
		Node retval =  getNode(nodeType, cm);

		retval.init(arg1, arg2, arg3, arg4, arg5, arg6, arg7);

		return  retval;
	}
	/**
	 * Get a node that takes eight initializer arguments.
	 *
	 * @param nodeType		Identifier for the type of node.
	 * @param arg1	An initializer argument
	 * @param arg2	An initializer argument
	 * @param arg3	An initializer argument
	 * @param arg4	An initializer argument
	 * @param arg5	An initializer argument
	 * @param arg6	An initializer argument
	 * @param arg7	An initializer argument
	 * @param arg8	An initializer argument
	 * @param cm			A ContextManager
	 *
	 * @return	A new QueryTree node.
	 *
	 * @exception StandardException		Thrown on error.
	 */
	public final Node getNode(int nodeType,
							Object arg1,
							Object arg2,
							Object arg3,
							Object arg4,
							Object arg5,
							Object arg6,
							Object arg7,
							Object arg8,
							ContextManager cm)
								throws StandardException
	{
		Node retval =  getNode(nodeType, cm);

		retval.init(arg1, arg2, arg3, arg4, arg5, arg6, arg7, arg8);

		return  retval;
	}
	/**
	 * Get a node that takes nine initializer arguments.
	 *
	 * @param nodeType		Identifier for the type of node.
	 * @param arg1	An initializer argument
	 * @param arg2	An initializer argument
	 * @param arg3	An initializer argument
	 * @param arg4	An initializer argument
	 * @param arg5	An initializer argument
	 * @param arg6	An initializer argument
	 * @param arg7	An initializer argument
	 * @param arg8	An initializer argument
	 * @param arg9	An initializer argument
	 * @param cm			A ContextManager
	 *
	 * @return	A new QueryTree node.
	 *
	 * @exception StandardException		Thrown on error.
	 */
	public final Node getNode(int nodeType,
							Object arg1,
							Object arg2,
							Object arg3,
							Object arg4,
							Object arg5,
							Object arg6,
							Object arg7,
							Object arg8,
							Object arg9,
							ContextManager cm)
								throws StandardException
	{
		Node retval =  getNode(nodeType, cm);

		retval.init(arg1, arg2, arg3, arg4, arg5, arg6, arg7, arg8, arg9);

		return  retval;
	}
	/**
	 * Get a node that takes ten initializer arguments.
	 *
	 * @param nodeType		Identifier for the type of node.
	 * @param arg1	An initializer argument
	 * @param arg2	An initializer argument
	 * @param arg3	An initializer argument
	 * @param arg4	An initializer argument
	 * @param arg5	An initializer argument
	 * @param arg6	An initializer argument
	 * @param arg7	An initializer argument
	 * @param arg8	An initializer argument
	 * @param arg9	An initializer argument
	 * @param arg10	An initializer argument
	 * @param cm			A ContextManager
	 *
	 * @return	A new QueryTree node.
	 *
	 * @exception StandardException		Thrown on error.
	 */
	public final Node getNode(int nodeType,
							Object arg1,
							Object arg2,
							Object arg3,
							Object arg4,
							Object arg5,
							Object arg6,
							Object arg7,
							Object arg8,
							Object arg9,
							Object arg10,
							ContextManager cm)
								throws StandardException
	{
		Node retval =  getNode(nodeType, cm);

		retval.init(arg1, arg2, arg3, arg4, arg5, arg6, arg7, arg8, arg9,
					arg10);

		return  retval;
	}
	/**
	 * Get a node that takes eleven initializer arguments.
	 *
	 * @param nodeType		Identifier for the type of node.
	 * @param arg1	An initializer argument
	 * @param arg2	An initializer argument
	 * @param arg3	An initializer argument
	 * @param arg4	An initializer argument
	 * @param arg5	An initializer argument
	 * @param arg6	An initializer argument
	 * @param arg7	An initializer argument
	 * @param arg8	An initializer argument
	 * @param arg9	An initializer argument
	 * @param arg10	An initializer argument
	 * @param arg11	An initializer argument
	 * @param cm			A ContextManager
	 *
	 * @return	A new QueryTree node.
	 *
	 * @exception StandardException		Thrown on error.
	 */
	public final Node getNode(int nodeType,
							Object arg1,
							Object arg2,
							Object arg3,
							Object arg4,
							Object arg5,
							Object arg6,
							Object arg7,
							Object arg8,
							Object arg9,
							Object arg10,
							Object arg11,
							ContextManager cm)
								throws StandardException
	{
		Node retval =  getNode(nodeType, cm);

		retval.init(arg1, arg2, arg3, arg4, arg5, arg6, arg7, arg8, arg9,
					arg10, arg11);

		return  retval;
	}
	/**
	 * Get a node that takes twelve initializer arguments.
	 *
	 * @param nodeType		Identifier for the type of node.
	 * @param arg1	An initializer argument
	 * @param arg2	An initializer argument
	 * @param arg3	An initializer argument
	 * @param arg4	An initializer argument
	 * @param arg5	An initializer argument
	 * @param arg6	An initializer argument
	 * @param arg7	An initializer argument
	 * @param arg8	An initializer argument
	 * @param arg9	An initializer argument
	 * @param arg10	An initializer argument
	 * @param arg11	An initializer argument
	 * @param arg12	An initializer argument
	 * @param cm			A ContextManager
	 *
	 * @return	A new QueryTree node.
	 *
	 * @exception StandardException		Thrown on error.
	 */
	public final Node getNode(int nodeType,
							Object arg1,
							Object arg2,
							Object arg3,
							Object arg4,
							Object arg5,
							Object arg6,
							Object arg7,
							Object arg8,
							Object arg9,
							Object arg10,
							Object arg11,
							Object arg12,
							ContextManager cm)
								throws StandardException
	{
		Node retval =  getNode(nodeType, cm);

		retval.init(arg1, arg2, arg3, arg4, arg5, arg6, arg7, arg8, arg9,
					arg10, arg11, arg12);

		return  retval;
	}
	/**
	 * Get a node that takes thirteen initializer arguments.
	 *
	 * @param nodeType		Identifier for the type of node.
	 * @param arg1	An initializer argument
	 * @param arg2	An initializer argument
	 * @param arg3	An initializer argument
	 * @param arg4	An initializer argument
	 * @param arg5	An initializer argument
	 * @param arg6	An initializer argument
	 * @param arg7	An initializer argument
	 * @param arg8	An initializer argument
	 * @param arg9	An initializer argument
	 * @param arg10	An initializer argument
	 * @param arg11	An initializer argument
	 * @param arg12	An initializer argument
	 * @param arg13	An initializer argument
	 * @param cm			A ContextManager
	 *
	 * @return	A new QueryTree node.
	 *
	 * @exception StandardException		Thrown on error.
	 */
	public final Node getNode(int nodeType,
							Object arg1,
							Object arg2,
							Object arg3,
							Object arg4,
							Object arg5,
							Object arg6,
							Object arg7,
							Object arg8,
							Object arg9,
							Object arg10,
							Object arg11,
							Object arg12,
							Object arg13,
							ContextManager cm)
								throws StandardException
	{
		Node retval =  getNode(nodeType, cm);

		retval.init(arg1, arg2, arg3, arg4, arg5, arg6, arg7, arg8, arg9,
					arg10, arg11, arg12, arg13);

		return  retval;
	}
	/**
	 * Get a node that takes fourteen initializer arguments.
	 *
	 * @param nodeType		Identifier for the type of node.
	 * @param arg1	An initializer argument
	 * @param arg2	An initializer argument
	 * @param arg3	An initializer argument
	 * @param arg4	An initializer argument
	 * @param arg5	An initializer argument
	 * @param arg6	An initializer argument
	 * @param arg7	An initializer argument
	 * @param arg8	An initializer argument
	 * @param arg9	An initializer argument
	 * @param arg10	An initializer argument
	 * @param arg11	An initializer argument
	 * @param arg12	An initializer argument
	 * @param arg13	An initializer argument
	 * @param arg14	An initializer argument
	 * @param cm			A ContextManager
	 *
	 * @return	A new QueryTree node.
	 *
	 * @exception StandardException		Thrown on error.
	 */
	public final Node getNode(int nodeType,
							Object arg1,
							Object arg2,
							Object arg3,
							Object arg4,
							Object arg5,
							Object arg6,
							Object arg7,
							Object arg8,
							Object arg9,
							Object arg10,
							Object arg11,
							Object arg12,
							Object arg13,
							Object arg14,
							ContextManager cm)
								throws StandardException
	{
		Node retval =  getNode(nodeType, cm);

		retval.init(arg1, arg2, arg3, arg4, arg5, arg6, arg7, arg8, arg9,
					arg10, arg11, arg12, arg13, arg14);

		return  retval;
	}

	public final Node getNode(int nodeType,
							  Object arg1,
							  Object arg2,
							  Object arg3,
							  Object arg4,
							  Object arg5,
							  Object arg6,
							  Object arg7,
							  Object arg8,
							  Object arg9,
							  Object arg10,
							  Object arg11,
							  Object arg12,
							  Object arg13,
							  Object arg14,
							  Object arg15,
                              Object arg16,
                              Object arg17,
							  Object arg18,
							  Object arg19,
							  ContextManager cm)
			throws StandardException
	{
		Node retval =  getNode(nodeType, cm);

		retval.init(arg1, arg2, arg3, arg4, arg5, arg6, arg7, arg8, arg9, arg10, arg11, arg12,
				arg13, arg14, arg15, arg16, arg17, arg18, arg19);

		return  retval;
	}
}
