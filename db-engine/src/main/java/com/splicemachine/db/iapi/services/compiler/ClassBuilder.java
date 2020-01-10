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

package com.splicemachine.db.iapi.services.compiler;

import com.splicemachine.db.iapi.services.loader.GeneratedClass;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.util.ByteArray;

/**
 * ClassBuilder is used to construct a java class's byte array
 * representation.
 *
 * Limitations:
 *   No checking for language use violations such as invalid modifiers
 *	or duplicate field names.
 *   All classes must have a superclass; java.lang.Object must be
 *      supplied if there is no superclass.
 *
 * <p>
 * When a class is first created, it has:
 * <ul>
 * <li> a superclass
 * <li> modifiers
 * <li> a name
 * <li> a package
 * <li> no superinterfaces, methods, fields, or constructors
 * <li> an empty static initializer
 * </ul>
 * <p>
 * MethodBuilder implementations are required to get code out of the
 * constructs within their bodies in some manner. 
 * Most typically, they may have a stream to which the statement and 
 * expression constructs write the code that they represent,
 * and they walk over the statements and expressions in the appropriate order.
 *
 */
public interface ClassBuilder {

	/**
	 * add a field to this class. Fields cannot
	 * be initialized here, they must be initialized
	 * in the static initializer code (static fields)
	 * or in the constructors.
	 * <p>
	 * Methods are added when they are created with the JavaFactory.
	 * @param type	The type of the field in java language.
	 * @param name	The name of the field.
	 * @param modifiers	The | of the modifier values such as
	 *					public, static, etc.
	 * @see ClassBuilder#newMethodBuilder
	 * @see #newConstructorBuilder
	 */
	LocalField addField(String type, String name, int modifiers);

	/**
		Fully create the bytecode and load the
		class using the ClassBuilder's ClassFactory.

		@exception StandardException Standard Derby policy
	*/
	GeneratedClass getGeneratedClass() throws StandardException;

	/**
	 * At the time the class is completed and bytecode
	 * generated, if there are no constructors then
	 * the default no-arg constructor will be defined.
	 */
	ByteArray getClassBytecode() throws StandardException;

	/**
	 * the class's unqualified name
	 */
	String getName();

	/**
	 * the class's qualified name
	 */
	String getFullName();

	/**
	 * a method. Once it is created, parameters, thrown
	 * exceptions, statements, and local variable declarations
	 * must be added to it. It is put into its defining class
	 * when it is created.
	 * <verbatim>
	   Java: #modifiers #returnType #methodName() {}
	  		// modifiers is the | of the JVM constants for
	  		// the modifiers such as static, public, etc.
       </verbatim>
	   <p>
	 * This is used to start a constructor as well; pass in
	 * null for the returnType when used in that manner.
	 *
	 * @param modifiers the | of the Modifier
	 *	constants representing the visibility and control of this
	 *	method.
	 * @param returnType the return type of the method as its
	 *	Java language type name.
	 * @param methodName the name of the method.
	 *
	 * @return the method builder.
	 * @see java.lang.reflect.Modifier
	 */
	MethodBuilder newMethodBuilder(int modifiers, String returnType,
		String methodName);
	
	/**
	 * a method with parameters. Once it is created, thrown
	 * exceptions, statements, and local variable declarations
	 * must be added to it. It is put into its defining class
	 * when it is created.
	 * <verbatim>
	   Java: #modifiers #returnType #methodName() {}
	  		// modifiers is the | of the JVM constants for
	  		// the modifiers such as static, public, etc.
       </verbatim>
	   <p>
	 * This is used to start a constructor as well; pass in
	 * null for the returnType when used in that manner.
	 *
	 * @param modifiers the | of the Modifier
	 *	constants representing the visibility and control of this
	 *	method.
	 * @param returnType the return type of the method as its
	 *	Java language type name.
	 * @param methodName the name of the method.
	 * @param parms	an array of String representing the
	 *				method's parameter types
	 *
	 * @return the method builder.
	 * @see java.lang.reflect.Modifier
	 */
	MethodBuilder newMethodBuilder(int modifiers, String returnType,
		String methodName, String[] parms);

	/**
	 * a constructor. Once it is created, parameters, thrown
	 * exceptions, statements, and local variable declarations
	 * must be added to it. It is put into its defining class
	 * when it is created.
	 * <verbatim>
	   Java: #modifiers #className() {}
	  		// modifiers is the | of the JVM constants for
	  		// the modifiers such as static, public, etc.
	  		// className is taken from definingClass.name()
       </verbatim>
	 * <p>
	 * This is used to start a constructor as well; pass in
	 * null for the returnType when used in that manner.
	 *
	 * @param modifiers the | of the Modifier
	 *	constants representing the visibility and control of this
	 *	method.
	 *
	 * @return the method builder for the constructor.
	 * @see java.lang.reflect.Modifier
	 */
	MethodBuilder newConstructorBuilder(int modifiers);

	/**
		Create a new private field and its getter and setter methods.

		@param getter getter for field
		@param setter setter for field
		@param methodModifier modifier for method
		@param staticField true if the field is static
		@param type type of the field, return type of the get method and
		parameter type of the set method.

	*/
	void newFieldWithAccessors(String getter, String setter, int methodModifier,
		boolean staticField, String type);

	boolean existsField(String javaType, String name);
}
