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

package com.splicemachine.db.iapi.services.loader;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.services.context.Context;

/**
	Generated classes must implement this interface.

*/
public interface GeneratedByteCode {

	/**
		Initialize the generated class from a context.
		Called by the class manager just after
		creating the instance of the new class.
	*/
	public void initFromContext(Context context)
		throws StandardException;

	/**
		Set the Generated Class. Call by the class manager just after
		calling initFromContext.
	*/
	public void setGC(GeneratedClass gc);

	/**
		Called by the class manager just after calling setGC().
	*/
	public void postConstructor() throws StandardException;

	/**
		Get the GeneratedClass object for this object.
	*/
	public GeneratedClass getGC();

	public GeneratedMethod getMethod(String methodName) throws StandardException;


	public Object e0() throws StandardException ; 
	public Object e1() throws StandardException ;
	public Object e2() throws StandardException ;
	public Object e3() throws StandardException ;
	public Object e4() throws StandardException ; 
	public Object e5() throws StandardException ;
	public Object e6() throws StandardException ;
	public Object e7() throws StandardException ;
	public Object e8() throws StandardException ; 
	public Object e9() throws StandardException ;

    public String e0ToString() throws StandardException ;
    public String e1ToString() throws StandardException ;
    public String e2ToString() throws StandardException ;
    public String e3ToString() throws StandardException ;
    public String e4ToString() throws StandardException ;
    public String e5ToString() throws StandardException ;
    public String e6ToString() throws StandardException ;
    public String e7ToString() throws StandardException ;
    public String e8ToString() throws StandardException ;
    public String e9ToString() throws StandardException ;
}
