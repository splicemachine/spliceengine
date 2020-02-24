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

package com.splicemachine.db.iapi.services.context;

import com.splicemachine.db.iapi.error.*;
import com.splicemachine.db.iapi.reference.ContextId;
import com.splicemachine.db.iapi.services.i18n.LocaleFinder;
import com.splicemachine.db.iapi.services.info.JVMInfo;
import com.splicemachine.db.iapi.services.monitor.Monitor;
import com.splicemachine.db.iapi.services.sanity.SanityManager;
import com.splicemachine.db.iapi.services.stream.HeaderPrintWriter;
import org.apache.log4j.Logger;

import java.sql.SQLException;
import java.util.*;

/**
 *
 * The ContextManager collects contexts as they are
 * created. It maintains stacks of contexts by
 * named ids, so that the top context of a given
 * type can be returned. It also maintains a global
 * stack so that contexts can be traversed in the
 * order they were created.
 * <p>
 * The first implementation of the context manager
 * assumes there is only one thread to worry about
 * and that the user(s) of the class only create one
 * instance of ContextManager.
 */

public class ContextManager
{
	private static final Logger LOG = Logger.getLogger(ContextManager.class);

	/**
	 * The CtxStack implement a stack on top of an ArrayList (to avoid
	 * the inherent overhead associated with java.util.Stack which is
	 * built on top of java.util.Vector, which is fully
	 * synchronized).
	 */
	private static final class CtxStack {
		/** Internal list with all the elements of the stack. */
		private final List<Context> stack_ = new ArrayList<>();

		// Keeping a reference to the top element on the stack
		// optimizes the frequent accesses to this element. The
		// tradeoff is that pushing and popping becomes more
		// expensive, but those operations are infrequent.
		private Context top_ = null;

		void push(Context context) {
			stack_.add(context);
			top_ = context;
		}
		void pop() {
            stack_.remove(stack_.size() - 1);
            top_ = stack_.isEmpty() ? null : stack_.get(stack_.size() - 1);
        }
		void remove(Context context) {
			if (context == top_) {
				pop();
				return;
			}
			int index=stack_.lastIndexOf(context);
			if(index>=0)
				stack_.remove(index);
		}
		Context top() {
			return top_;
		}
		boolean isEmpty() { return stack_.isEmpty(); }
	}

	/**
	 * Parent context manager used in parallel operations
	 * Each forked task creates its own ContextManager pointing to the parent
	 * assuming it remains unchanged until all forked tasks are joined.
	 * Only getContext() delegates to parent.
	 */
	private ContextManager parent;

	/**
	 * HashMap that holds the Context objects. The Contexts are stored
	 * with a String key.
	 * @see ContextManager#pushContext(Context)
	 */
	private final Map<String, CtxStack> ctxTable = new HashMap<>();

	/**
	 * List of all Contexts of all types.
	 */
	private final List<Context> holder = new ArrayList<>();

	public void setActiveThread(){
		this.activeThread = Thread.currentThread();
	}

	/**
	 * Add a Context object to the ContextManager. The object is added
	 * both to the holder list and to a stack for the specific type of
	 * Context.
	 * @param newContext the new Context object
	 */
	public void pushContext(Context newContext)
	{
		checkInterrupt();
		final String contextId = newContext.getIdName();
		CtxStack idStack = ctxTable.get(contextId);

		// if the stack is null, create a new one.
		if (idStack == null) {
			idStack = new CtxStack();
			ctxTable.put(contextId, idStack);
		}

		// add to top of id's stack
		idStack.push(newContext);

		// add to top of global stack too
		holder.add(newContext);
	}
	
	/**
	 * Obtain the last pushed Context object of the type indicated by
	 * the contextId argument.
	 * @param contextId a String identifying the type of Context
	 * @return The Context object with the corresponding contextId, or null if not found
	 */
	public Context getContext(String contextId) {
		checkInterrupt();
		
		final CtxStack idStack = ctxTable.get(contextId);
		if (SanityManager.DEBUG) {
			SanityManager.ASSERT(idStack == null || idStack.isEmpty() ||
					Objects.equals(idStack.top().getIdName(), contextId));
		}
		if (idStack != null) {
			return idStack.top();
		}
		if (parent != null) {
			return parent.getContext(contextId);
		}
		return null;
	}

	/**
	 * Remove the last pushed Context object, regardless of type. If
	 * there are no Context objects, no action is taken.
	 */
	public void popContext()
	{
		checkInterrupt();
		// no contexts to remove, so we're done.
		if (holder.isEmpty()) {
			return;
		}

		// remove the top context from the global stack
		Context theContext = holder.remove(holder.size()-1);

		// now find its id and remove it from there, too
		final String contextId = theContext.getIdName();
		final CtxStack idStack = ctxTable.get(contextId);

		if (SanityManager.DEBUG) {
			SanityManager.ASSERT( idStack != null &&
								  (! idStack.isEmpty()) &&
					Objects.equals(idStack.top().getIdName(), contextId));
		}
		idStack.pop();
	}

	/**
	 * Removes the specified Context object. If the specified Context object does not exist
	 * (already removed), no action is taken.
	 * @param theContext the Context object to remove.
	 */
	void popContext(Context theContext)
	{
		checkInterrupt();
		if (SanityManager.DEBUG)
			SanityManager.ASSERT(!holder.isEmpty());

		// first, remove it from the global stack.
        int index = holder.lastIndexOf(theContext);
        if (index >=0) {
            holder.remove(index);
        }

        final String contextId = theContext.getIdName();
		final CtxStack idStack = ctxTable.get(contextId);

		if (idStack != null) {
			// now remove it from its id's stack.
			idStack.remove(theContext);
		}
	}
    
    /**
     * Is the ContextManager empty containing no Contexts.
     */
    public final boolean isEmpty()
    {
        return holder.isEmpty();
    }
	
	/**
	 * Return an unmodifiable list reference to the ArrayList backing
	 * CtxStack object for this type of Contexts. This method allows
	 * fast traversal of all Contexts on that stack. The first element
	 * in the List corresponds to the bottom of the stack. The
	 * assumption is that the Stack will not be modified while it is
	 * being traversed.
	 * @param contextId the type of Context stack to return.
	 * @return an unmodifiable "view" of the ArrayList backing the stack
	 * @see com.splicemachine.db.iapi.sql.conn.StatementContext#resetSavePoint()
	 */
	public final List<Context> getContextStack(String contextId) {
		List<Context> contexts= new ArrayList<>();
		for (ContextManager cm = this; cm != null; cm = cm.parent) {
			cm.accumulate(contexts, contextId);
		}
		return Collections.unmodifiableList(contexts);
	}

	void accumulate(List<Context> contexts, String contextId) {
		CtxStack cs = ctxTable.get(contextId);
		if (cs != null) {
			contexts.addAll(cs.stack_);
		}
	}

    /**
     * clean up error and print it to db.log. Extended diagnosis including
     * thread dump to db.log and javaDump if available, will print if the
     * database is active and severity is greater than or equals to
     * SESSTION_SEVERITY or as configured by
     * db.stream.error.extendedDiagSeverityLevel property
     * 
     * @param error the error we want to clean up
     * @param diagActive
     *        true if extended diagnostics should be considered, 
     *        false not interested of extended diagnostic information
     * @return true if the context manager is shutdown, false otherwise.
     */
    public boolean cleanupOnError(Throwable error, boolean diagActive)
	{
		if (shutdown)
			return true;

		if (errorStringBuilder == null)
			errorStringBuilder = new ErrorStringBuilder(errorStream.getHeader());

		ThreadDeath seenThreadDeath = null;
		if (error instanceof ThreadDeath)
			seenThreadDeath = (ThreadDeath) error;

        if (error instanceof PassThroughException) {
            error = error.getCause();
        }

		boolean reportError = reportError(error);

		if (reportError) 
		{
			ContextImpl lcc = null;
			StringBuffer sb = null;
			if (! shutdown)
			{
				// report an id for the message if possible
				lcc = (ContextImpl) getContext(ContextId.LANG_CONNECTION);
				if (lcc != null) {
					sb = lcc.appendErrorInfo();
				}
			}

			String cleanup = "Cleanup action starting";

			if (sb != null) {
				sb.append(cleanup);
				cleanup = sb.toString();
			}

			errorStringBuilder.appendln(cleanup);
			
			if (!shutdown)		// Do this only during normal processing.
			{	
				ContextImpl sc = (ContextImpl) getContext(ContextId.LANG_STATEMENT);
				// Output the SQL statement that failed in the log file.
				if (sc != null)
				{					
					sb = sc.appendErrorInfo();
					if (sb != null)
						errorStringBuilder.appendln(sb.toString());
				}
			}
		}
		
		/*
		  REVISIT RESOLVE
		  Ensure that the traversal of the stack works in all 
		  cases where contexts can  pop themselves *and* 
		  contexts can pop other contexts off the stack.
		*/ 
		

forever: for (;;) {

            int errorSeverity = getErrorSeverity(error);
 			if (reportError) {
				errorStringBuilder.stackTrace(error);
				flushErrorString();
			}


			boolean	lastHandler = false;


			/*
			 *	Walk down the stack, calling cleanup on each context.
			 *  Be robust against multiple context popping (see GenericLanguageConnectionContext).
			 */
			Context[] contexts = holder.toArray(new Context[holder.size()]);
			for (int index = contexts.length - 1; index >= 0; --index) {
				try {
					if (lastHandler)
					{
						break;
					}

					Context ctx = contexts[index];
					lastHandler = ctx.isLastHandler(errorSeverity);

					ctx.cleanupOnError(error);
                    //When errorSeverity greater or equals Property.EXT_DIAG_SEVERITY_LEVEL,
                    //the threadDump information will be in db.log and
                    //the diagnosis information will be prepared.
                    //If Property.EXT_DIAG_SEVERITY_LEVEL is not set in JVM property or
                    //db property, we will only handle threadDump information and diagnosis
                    //information for errorSeverity = ExceptionSeverity.SESSION_SEVERITY.
                    if (reportError && diagActive
                            && (errorSeverity >= extDiagSeverityLevel)) {
                        threadDump = ExceptionUtil.dumpThreads();
                    } else {
                        threadDump = null;
                    }
				}
				catch (StandardException se) {

					if (error instanceof StandardException) {

						if (se.getSeverity() > ((StandardException) error).getSeverity()) {
							// Ok, error handling raised a more severe error,
							// restart with the more severe error
							error = se;
							reportError = reportError(se);
							if (reportError) {
								errorStream.printThrowable("New exception raised during cleanup", error);
							}
							continue forever;
						}
					}

					if (reportError(se)) {
						errorStringBuilder.appendln("Less severe exception raised during cleanup (ignored) " + se.getMessage());
						errorStringBuilder.stackTrace(se);
						flushErrorString();
					}

					/*
						For a less severe error, keep with the last error
					 */
                }
				catch (Throwable t) {
					reportError = reportError(t);


					if (error instanceof StandardException) {
						/*
							Ok, error handling raised a more severe error,
							restart with the more severe error
							A Throwable after a StandardException is always 
							more severe.
						 */
						error = t;
						if (reportError) {
							errorStream.printThrowable("New exception raised during cleanup", error);
						}
						continue forever;
					}


					if (reportError) {
						errorStringBuilder.appendln("Equally severe exception raised during cleanup (ignored) " + t.getMessage());
						errorStringBuilder.stackTrace(t);
						flushErrorString();
					}

					if (t instanceof ThreadDeath) {
						if (seenThreadDeath != null)
							throw seenThreadDeath;

						seenThreadDeath = (ThreadDeath) t;
					}
	
					/*
						For a less severe error, just continue with the last
						error
					 */
                }
			}

            if (threadDump != null) {
                errorStream.println(threadDump);
                JVMInfo.javaDump();
            }

			if (reportError) {
				errorStream.println("Cleanup action completed");
			}

			if (seenThreadDeath != null)
				throw seenThreadDeath;

			return false;
		}

	}


	synchronized boolean  setInterrupted(Context c) {

		boolean interruptMe = (c == null) || holder.contains(c);

		if (interruptMe) {
			this.shutdown = true;
		}
		return interruptMe;
	}

	/**
		Check to see if we have been interrupted. If we have then
		a ShutdownException will be thrown. This will be either the
		one passed to interrupt or a generic one if some outside
		source interrupted the thread.
	*/
	private void checkInterrupt() {
		if (shutdown) {
			// system must have changed underneath us
			throw new ShutdownException();
		}
	}

	/**
		Set the locale for this context.
	*/
	public void setLocaleFinder(LocaleFinder finder) {
		this.finder = finder;
	}

	private Locale messageLocale;

	public void setMessageLocale(String localeID) throws StandardException {
		this.messageLocale = Monitor.getLocaleFromString(localeID);
	}

	public Locale getMessageLocale()
	{
		if (messageLocale != null)
			return messageLocale;
		else if (finder != null) {
			try {
				return finder.getCurrentLocale();
			} catch (StandardException ignored) {
				
			}
		}
		return Locale.getDefault();
	}

	/**
	 * Flush the built up error string to wherever
	 * it is supposed to go, and reset the error string
	 */
	private void flushErrorString()
	{
		errorStream.println(errorStringBuilder.get().toString());
		errorStringBuilder.reset();
	}

	/*
	** Class methods
	*/

	private boolean reportError(Throwable t) {

		if (t instanceof StandardException) {

			StandardException se = (StandardException) t;

			switch (se.report()) {
			case StandardException.REPORT_DEFAULT:
				int level = se.getSeverity();
				return (level >= logSeverityLevel) ||
					(level == ExceptionSeverity.NO_APPLICABLE_SEVERITY);

			case StandardException.REPORT_NEVER:
				return false;

			case StandardException.REPORT_ALWAYS:
			default:
				return true;
			}
		}

		return !(t instanceof ShutdownException);

	}
    
    /**
     * return the severity of the exception. Currently, this method 
     * does not determine a severity that is not StandardException 
     * or SQLException.
     * @param error - Throwable error
     * 
     * @return int vendorcode/severity for the Throwable error
     *            - error/exception to extract vendorcode/severity. 
     *            For error that we can not get severity, 
     *            NO_APPLICABLE_SEVERITY will return.
     */
    public int getErrorSeverity(Throwable error) {
        
        if (error instanceof StandardException) {
            return ((StandardException) error).getErrorCode();
        }
        
        if (error instanceof SQLException) {
            return ((SQLException) error).getErrorCode();
        }
        return ExceptionSeverity.NO_APPLICABLE_SEVERITY;
    }

	/**
	 * Constructs a new instance. No CtxStacks are inserted into the
	 * hashMap as they will be allocated on demand.
	 * @param parent parent context manager
	 * @param stream error stream for reporting errors
	 */
	ContextManager(ContextManager parent, HeaderPrintWriter stream)
	{
		this.parent = parent;
		errorStream = stream;
		logSeverityLevel = 0;
		extDiagSeverityLevel = 40000;
/*		XXX - TODO John Leach: Need to make this configurable.  Quick hack to stop hitting file system.
      logSeverityLevel = PropertyUtil.getSystemInt(Property.LOG_SEVERITY_LEVEL,
			SanityManager.DEBUG ? 0 : ExceptionSeverity.SESSION_SEVERITY);
        extDiagSeverityLevel = PropertyUtil.getSystemInt(
                Property.EXT_DIAG_SEVERITY_LEVEL,
                ExceptionSeverity.SESSION_SEVERITY);
                */
	}

	private int		logSeverityLevel;
    // DERBY-4856 track extendedDiagSeverityLevel variable
    private int extDiagSeverityLevel;

	private HeaderPrintWriter errorStream;
	private ErrorStringBuilder errorStringBuilder;
    // DERBY-4856 add thread dump information.
    private String threadDump;

	private boolean shutdown;
	private LocaleFinder finder;

    /**
     * The thread that owns this ContextManager, set by
     * ContextService.setCurrentContextManager and reset
     * by resetCurrentContextManager. Only a single
     * thread can be active in a ContextManager at any time,
     * and the thread only "owns" the ContextManager while
     * it is executing code within Derby. In the JDBC case
     * setCurrentContextManager is called at the start of
     * a JBDC method and resetCurrentContextManager on completion.
     * Nesting within the same thread is supported, such as server-side
     * JDBC calls in a Java routine or procedure. In that case
     * the activeCount will represent the level of nesting, in
     * some situations.
     * <BR>

     * @see ContextService#setCurrentContextManager(ContextManager)
     * @see ContextService#resetCurrentContextManager(ContextManager)
     */
	volatile Thread	activeThread;
    
    /**
     * Count of the number of setCurrentContextManager calls
     * by a single thread, for nesting situations with a single
     * active ContextManager.
     */
	int		activeCount;
}
