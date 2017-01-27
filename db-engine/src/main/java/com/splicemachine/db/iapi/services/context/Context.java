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

package com.splicemachine.db.iapi.services.context;

import com.splicemachine.db.iapi.error.StandardException;

/**
 * Contexts are created and used to manage the execution
 * environment. They provide a convenient location for
 * storing globals organized by the module using the
 * globals. 
 * <p>
 * A basic context implementation is provided as an abstract
 * class; this implementation satisfies the interface and
 * should in general be used as the supertype of all context
 * types.  Otherwise, context classes must satisfy the
 * semantics of the interface through their own distinct
 * implementations.
 * <p>
 * Contexts assist in cleanup
 * when errors are caught in the outer block.
 * <p>
 * Use of context cleanup is preferred over using try/catch
 * blocks throughout the code.
 * <p>
 * Use of context pushing and popping is preferred over
 * using many instance or local variables, even when try/catch is present.
 * when the instance or local variables would be holding resources.
 <P>
 Usually Context's have a reference based equality, ie. they do not provide
 an implementation of equals(). Contexts may implement a value based equality
 but this usually means there is only one instance of the Context on the stack,
 This is because the popContext(Context) will remove the most recently pushed
 Context that matches via the equals method, not by a reference check.
 Implementing equals is useful for Contexts used in notifyAllThreads() that
 is not aimed at a single thread.
 */
public interface Context
{
	/**
	 * Returns the context manager that has stored this
	 * context in its stack.
	 */
	public ContextManager getContextManager();

	/**
	 * Returns the current id name associated
	 * with this context. Contexts are placed into
	 * stacks by id, in a context manager. Null
	 * if the context is not assigned to an id.
	 * Contexts known by context managers are always
	 * assigned to an id.
	 * <p>
	 * A default Id name should be defined in each
	 * specific context interface as a static final
	 * field with the name CONTEXT_ID. For example,
	 * see com.splicemachine.db.iapi.sql.compile.CompilerContext.CONTEXT_ID.
	 * @see com.splicemachine.db.iapi.sql.compile.CompilerContext
	 */
	public String getIdName();

	/**
	 * Contexts will be passed errors that are caught
	 * by the outer system when they are serious enough
	 * to require corrective action. They will be told
	 * what the error is, so that they can react appropriately.
	 * Most of the time, the contexts will react by either
	 * doing nothing or by removing themselves from the
	 * context manager. If there are no other references
	 * to the context, removing itself from the manager
	 * equates to freeing it.
     * <BR>
     * On an exception that is session severity or greater
     * the Context must push itself off the stack. This is
     * to ensure that after a session has been closed there
     * are no Contexts on the stack that potentially hold
     * references to objects, thus delaying their garbage
     * collection.
	 * <p>
	 * Contexts must release all their resources before
	 * removing themselves from their context manager.
	 * <p>
	 * The context manager 
	 * will "unwind" the contexts during cleanup in the
	 * reverse order they were placed on its global stack.
	 *
	 * <P>
	 * If error is an instance of StandardException then an implementation
	 * of this method may throw a new exception if and only if the new exception
	 * is an instance of StandardException that is more severe than the original error
	 * or the new exception is a not an instance of StandardException (e.g java.lang.NullPointerException).
	 *
	 * @exception StandardException thrown if cleanup goes awry
	 */
	public void cleanupOnError(Throwable error)
		throws StandardException;

	/**
		Push myself onto my context stack.
	*/
	public void pushMe();

	/**
		Pop myself of the context stack.
	*/
	public void popMe();

	/**
	 * Return whether or not this context is the "last" handler for a
	 * the specified severity level.  Previously, the context manager would march
	 * through all of the contexts in cleanupOnError() and call each of 
	 * their cleanupOnError() methods.  That did not work with server side
	 * JDBC, especially for a StatementException, because outer contexts
	 * could get cleaned up incorrectly.  This functionality is specific
	 * to the Language system.  Any non-language system contexts should
	 * return ExceptionSeverity.NOT_APPLICABLE_SEVERITY.
	 *
	 * NOTE: Both the LanguageConnectionContext and the JDBC Connection Context are
	 * interested in session level errors because they both have clean up to do.
	 * This method allows both of them to return false so that all such handlers
	 * under them can do their clean up.
	 */
	public boolean isLastHandler(int severity);
}
