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

package com.splicemachine.db.shared.common.sanity;


import java.util.Hashtable;

/**
 * The SanityService provides assertion checking and debug
 * control.
 * <p>
 * Assertions and debug checks
 * can only be used for testing conditions that might occur
 * in development code but not in production code.	
 * <b>They are compiled out of production code.</b>
 * <p>
 * Uses of assertions should not add AssertFailure catches or
 * throws clauses; AssertFailure is under RuntimeException
 * in the java exception hierarchy. Our outermost system block
 * will bring the system down when it detects an assertion
 * failure.
 * <p>
 * In addition to ASSERTs in code, classes can choose to implement
 * an isConsistent method that would be used by ASSERTs, UnitTests,
 * and any other code wanting to check the consistency of an object.
 * <p>
 * Assertions are meant to be used to verify the state of the system
 * and bring the system down if the state is not correct. Debug checks
 * are meant to display internal information about a running system.
 * <p>
 */
public class SanityManager {
    /**
     * The build tool may be configured to alter
     * this source file to reset the static final variables
     * so that assertion and debug checks can be compiled out
     * of the code.
     */

    public static final boolean ASSERT = Boolean.getBoolean("com.splicemachine.enableLegacyAsserts");
    public static final boolean DEBUG = Boolean.getBoolean("com.splicemachine.enableLegacyAsserts");

    public static final String DEBUGDEBUG = "DumpSanityDebug";

    /**
     * debugStream holds a pointer to the debug stream for writing out
     * debug messages.  It is cached at the first debug write request.
     */
    static private java.io.PrintWriter debugStream = new java.io.PrintWriter(System.err);
    /**
     * DebugFlags holds the values of all debug flags in
     * the configuration file.
     */
    static private Hashtable DebugFlags = new Hashtable();
    /**
     * AllDebugOn and AllDebugOff override individual flags
     */
    static private boolean AllDebugOn = false;
    static private boolean AllDebugOff = false;

    //
    // class interface
    //

    /**
     * ASSERT checks the condition, and if it is
     * false, throws AssertFailure.
     * A message about the assertion failing is
     * printed.
     * <p>
     */
    public static void ASSERT(boolean mustBeTrue) {
        if (DEBUG)
            if (! mustBeTrue) {
                if (DEBUG) {
                    IllegalStateException af = new IllegalStateException("ASSERT FAILED");
                    if (DEBUG_ON("AssertFailureTrace")) {
                        showTrace(af);
                    }
                    throw af;
                }
                else
                    throw new IllegalStateException("ASSERT FAILED");
            }
    }

    /**
     * ASSERT checks the condition, and if it is
     * false, throws AssertFailure. The message will
     * be printed and included in the assertion.
     * <p>
     */
    public static void ASSERT(boolean mustBeTrue, String msgIfFail) {
        if (DEBUG)
            if (! mustBeTrue) {
                if (DEBUG) {
                    IllegalStateException af = new IllegalStateException("ASSERT FAILED " + msgIfFail);
                    if (DEBUG_ON("AssertFailureTrace")) {
                        showTrace(af);
                    }
                    throw af;
                }
                else
                    throw new IllegalStateException("ASSERT FAILED " + msgIfFail);
            }
    }

    /**
     * THROWASSERT throws AssertFailure. This is used in cases where
     * the caller has already detected the assertion failure (such as
     * in the default case of a switch). This method should be used,
     * rather than throwing AssertFailure directly, to allow us to
     * centralize all sanity checking.  The message argument will
     * be printed and included in the assertion.
     * <p>
     * @param msgIfFail message to print with the assertion
     *
     */
    public static void THROWASSERT(String msgIfFail) {
        // XXX (nat) Hmm, should we check ASSERT here?  The caller is
        // not expecting this function to return, whether assertions
        // are compiled in or not.
        THROWASSERT(msgIfFail, null);
    }

    /**
     * THROWASSERT throws AssertFailure.
     * This flavor will print the stack associated with the exception.
     * The message argument will
     * be printed and included in the assertion.
     * <p>
     * @param msg message to print with the assertion
     * @param t exception to print with the assertion
     *
     */
    public static void THROWASSERT(String msg, Throwable t) {
        IllegalStateException af = new IllegalStateException("ASSERT FAILED " + msg, t);
        if (DEBUG) {
            if (DEBUG_ON("AssertFailureTrace")) {
                showTrace(af);
            }
        }
        if (t != null) {
            showTrace(t);
        }
        throw af;
    }

    /**
     * THROWASSERT throws AssertFailure.
     * This flavor will print the stack associated with the exception.
     * <p>
     * @param t exception to print with the assertion
     *
     */
    public static void THROWASSERT(Throwable t) {
        THROWASSERT(t.toString(), t);
    }

    /**
     * The DEBUG calls provide the ability to print information or
     * perform actions based on whether a debug flag is set or not.
     * debug flags are set in configurations and picked up by the
     * sanity manager when the monitor finds them (see CONFIG below).
     * <p>
     * The message is output to the trace stream, so it ends up in
     * db2j.LOG. It will include a header line of
     *   DEBUG <flagname> OUTPUT:
     * before the message.
     * <p>
     * If the debugStream stream cannot be found, the message is printed to
     * System.out.
     */
    public static void DEBUG(String flag, String message) {
        if (DEBUG) {
            if (DEBUG_ON(flag)) {
                DEBUG_PRINT(flag, message);
            }
        }
    }

    /**
     * This can be called directly if you want to control
     * what is done once the debug flag has been verified --
     * for example, if you are calling a routine that prints to
     * the trace stream directly rather than returning a string to
     * be printed, or if you want to perform more (or fewer!)
     *
     * <p>
     * Calls to this method should be surrounded with
     *     if (SanityManager.DEBUG) {
     *     }
     * so that they can be compiled out completely.
     *
     * @return true if the flag has been set to "true"; false
     * if the flag is not set, or is set to something other than "true".
     */
    public static boolean DEBUG_ON(String flag) {
        if (DEBUG) {
            if (AllDebugOn) return true;
            else if (AllDebugOff) return false;
            else{
                Boolean flagValue=(Boolean)DebugFlags.get(flag);
                if(!DEBUGDEBUG.equals(flag)){
                    if(DEBUG_ON(DEBUGDEBUG)){
                        DEBUG_PRINT(DEBUGDEBUG,"DEBUG_ON: Debug flag "+flag+" = "+flagValue);
                    }
                }
                //noinspection UnnecessaryUnboxing
                return flagValue!=null && flagValue.booleanValue();
            }
        }
        else return false;
    }

    /**
     * Set the named debug flag to true.
     *
     * <p>
     * Calls to this method should be surrounded with
     *     if (SanityManager.DEBUG) {
     *     }
     * so that they can be compiled out completely.
     *
     * @param flag	The name of the debug flag to set to true
     */
    public static void DEBUG_SET(String flag) {
        if (DEBUG) {
            if (! DEBUGDEBUG.equals(flag)) {
                if (DEBUG_ON(DEBUGDEBUG))
                    DEBUG_PRINT(DEBUGDEBUG, "DEBUG_SET: Debug flag " + flag);
            }

            DebugFlags.put(flag, Boolean.TRUE);
        }
    }

    /**
     * Set the named debug flag to false.
     *
     * <p>
     * Calls to this method should be surrounded with
     *     if (SanityManager.DEBUG) {
     *     }
     * so that they can be compiled out completely.
     *
     * @param flag	The name of the debug flag to set to false
     */
    public static void DEBUG_CLEAR(String flag) {
        if (DEBUG) {
            if (! DEBUGDEBUG.equals(flag)) {
                if (DEBUG_ON(DEBUGDEBUG))
                    DEBUG_PRINT(DEBUGDEBUG, "DEBUG_CLEAR: Debug flag " + flag);
            }

            DebugFlags.put(flag, Boolean.FALSE);
        }
    }

    /**
     * This can be used to have the SanityManager return TRUE
     * for any DEBUG_ON check. DEBUG_CLEAR of an individual
     * flag will appear to have no effect.
     */
    public static void DEBUG_ALL_ON() {
        if (DEBUG) {
            AllDebugOn = true;
            AllDebugOff = false;
        }
    }

    /**
     * This can be used to have the SanityManager return FALSE
     * for any DEBUG_ON check. DEBUG_SET of an individual
     * flag will appear to have no effect.
     */
    public static void DEBUG_ALL_OFF() {
        if (DEBUG) {
            AllDebugOff = true;
            AllDebugOn = false;
        }
    }

    //
    // class implementation
    //

    static public void SET_DEBUG_STREAM(java.io.PrintWriter pw) {
        debugStream = pw;
    }

    static public java.io.PrintWriter GET_DEBUG_STREAM() {
        return debugStream;
    }

    static private void showTrace(IllegalStateException af) {
        af.printStackTrace();
        java.io.PrintWriter assertStream = GET_DEBUG_STREAM();

        assertStream.println("Assertion trace:");
        af.printStackTrace(assertStream);
        assertStream.flush();
    }

    static public void showTrace(Throwable t) {
        java.io.PrintWriter assertStream = GET_DEBUG_STREAM();

        assertStream.println("Exception trace: ");
        t.printStackTrace(assertStream);
    }

    /**
     * The DEBUG_PRINT calls provides a convenient way to print debug
     * information to the db2j.LOG file,  The message includes a header
     *<p>
     *	DEBUG <flag> OUTPUT:
     * before the message
     *<p>
     * If the debugStream stream cannot be found, the message is printed to
     * System.out.
     *
     */
    static public void DEBUG_PRINT(String flag, String message) {
        java.io.PrintWriter debugStream = GET_DEBUG_STREAM();

        debugStream.println("DEBUG "+flag+" OUTPUT: " + message);
        debugStream.flush();
    }

    public static void NOTREACHED() {
        THROWASSERT("code should not be reached");
    }
}

