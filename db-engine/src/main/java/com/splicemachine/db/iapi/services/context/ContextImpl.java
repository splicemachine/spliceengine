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

package com.splicemachine.db.iapi.services.context;

/**
 * Contexts are created and used to manage the execution
 * environment. They provide a convenient location for
 * storing globals organized by the module using the
 * globals. 
 * <p>
 * We provide this abstract class for other implementations
 * to use so that they can simply add fields and operations on
 * them. To be usable by the context manager, the subclasses
 * must define CleanupOnError and call super() in any constructor.
 * <p>
 * Contexts assist in cleanup
 * when errors are caught in the outer block.
 * <p>
 * Contexts implement the sanity interface to check and provide
 * information about their contents.
 */
public abstract class ContextImpl 
	implements Context
{
	private final String myIdName;
	private final ContextManager myContextManager;

	/*
	 * class interface
	 */
	protected ContextImpl(ContextManager cm, String id) {
		myIdName = id;
		myContextManager = cm;
		cm.pushContext(this);
	}

	/*
	 * Context interface
	 */
	/**
	 * @see com.splicemachine.db.iapi.services.context.Context#getContextManager
	 */
	final public ContextManager getContextManager()
	{
		return myContextManager;
	}

	/**
	 * @see com.splicemachine.db.iapi.services.context.Context#getIdName
	 */
	final public String getIdName()
	{
		return myIdName;
	}

	final public void pushMe() {
		myContextManager.pushContext(this);
	}

	/** @see Context#popMe */
	final public void popMe() {
		myContextManager.popContext(this);
	}

	/**
	 * @see Context#isLastHandler
	 */
	public boolean isLastHandler(int severity)
	{
		return false;
	}

	public StringBuffer appendErrorInfo() {
		return null;
	}
}
