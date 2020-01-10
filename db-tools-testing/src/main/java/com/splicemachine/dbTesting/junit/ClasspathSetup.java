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
package com.splicemachine.dbTesting.junit;

import java.net.URL;
import java.net.URLClassLoader;
import java.security.AccessController;
import java.security.PrivilegedActionException;

import junit.extensions.TestSetup;
import junit.framework.Test;

/**
 * <p>
 * This decorator adds another resource to the classpath, removing
 * it at tearDown().
 * </p>
 */
public class ClasspathSetup extends TestSetup
{
    ///////////////////////////////////////////////////////////////////////////////////
    //
    // CONSTANTS
    //
    ///////////////////////////////////////////////////////////////////////////////////

    ///////////////////////////////////////////////////////////////////////////////////
    //
    // STATE
    //
    ///////////////////////////////////////////////////////////////////////////////////

    private URL             _resource;
    private ClassLoader _originalClassLoader;

    ///////////////////////////////////////////////////////////////////////////////////
    //
    // CONSTRUCTORS
    //
    ///////////////////////////////////////////////////////////////////////////////////

    /**
     * <p>
     * Add the indicated URL to the classpath.
     * </p>
     */
    public  ClasspathSetup( Test test, URL resource )  throws Exception
    {
        super( test );
        
        _resource = resource;
    }

    ///////////////////////////////////////////////////////////////////////////////////
    //
    // JUnit BEHAVIOR
    //
    ///////////////////////////////////////////////////////////////////////////////////

    protected void setUp() throws PrivilegedActionException
    {
        AccessController.doPrivileged
            (
             new java.security.PrivilegedExceptionAction()
             {
                 public Object run() throws PrivilegedActionException
                 { 
                     _originalClassLoader = Thread.currentThread().getContextClassLoader();

                     URLClassLoader newClassLoader = new URLClassLoader( new URL[] { _resource }, _originalClassLoader );

                     Thread.currentThread().setContextClassLoader( newClassLoader );
                     
                     return null;
                 }
             }
             );
    }
    
    protected void tearDown() throws PrivilegedActionException
    {
        AccessController.doPrivileged
            (
             new java.security.PrivilegedExceptionAction()
             {
                 public Object run() throws PrivilegedActionException
                 { 
                     Thread.currentThread().setContextClassLoader( _originalClassLoader );
                     
                     return null;
                 }
             }
             );
    }

}


