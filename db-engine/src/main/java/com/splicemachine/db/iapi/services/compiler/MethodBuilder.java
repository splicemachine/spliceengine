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

/**
	MethodBuilder is used to generate the code for a method.
	<P>

	The code for a method is built in a way that corresponds to the
	layout of the stack machine that is the Java Virtual Machine.
	Values are pushed on the stack, moved about on the stack
	and then popped off the stack by operations such as method
	calls. An understanding of hoe the JVM operates is useful
	before using this class.

	<P>
	All the method descriptions below are generating bytecode
	to achieved the desired behaviour when the generated class
	is loaded. None of this class's methods calls actually
	invoke methods or create objects described by the callers.
 */
public interface MethodBuilder {

	/**
	 * Declare the method throws an exception.
	   Must be called before any code is added
	   to the method.
	 */
	void addThrownException(String exceptionClass);

	/**
	 * return the name of the method.
	 */
	String getName();

	/**
		Indicate the method is complete. Once this
		call has been made the caller must discard
		the reference to this object.
	 */
	void complete();

	/**
		Push a parameter value.
		<PRE>
		Stack ...  =>
		      ...,param_value
		</PRE>
		@param id position of the parameter (zero based).
	*/
	void getParameter(int id);

	/**
		Push a byte constant onto the stack
		<PRE>
		Stack ...  =>
		      ...,byte_value
		</PRE>
	*/
	void push(byte value);

	/**
		Push a boolean constant onto the stack
		<PRE>
		Stack ...  =>
		      ...,boolean_value
		</PRE>
	*/
	void push(boolean value);

	/**
		Push a short constant onto the stack
		<PRE>
		Stack ...  =>
		      ...,short_value
		</PRE>
	*/
	void push(short value);

	/**
		Push a int constant onto the stack
		<PRE>
		Stack ...  =>
		      ...,int_value
		</PRE>
	*/
	void push(int value);

	/**
		Push a long constant onto the stack
		<PRE>
		Stack ...  =>
		      ...,long_value
		</PRE>
	*/
	void push(long value);

	/**
		Push a float constant onto the stack
		<PRE>
		Stack ...  =>
		      ...,float_value
		</PRE>
	*/
	void push(float value);

	/**
		Push a double constant onto the stack
		<PRE>
		Stack ...  =>
		      ...,double_value
		</PRE>
	*/
	void push(double value);

	/**
		Push a String constant onto the stack
		<PRE>
		Stack ...  =>
		      ...,String_value
		</PRE>
	*/
	void push(String value);

	/**
		Push a typed null onto the stack
		<PRE>
		Stack ...  =>
		      ...,null
		</PRE>
	*/
	void pushNull(String className);

	/**
		Push the contents of the local field onto the stack.
		This call pushes the this instance required to access the field itself.
		<PRE>
		Stack ...  =>
		      ...,field_value
		</PRE>

	*/
	void getField(LocalField field);

	/**
		Push the contents of the described field onto the stack.
		This call requires the instance (reference) to be pushed by the caller.

		<PRE>
		Stack ...,field_ref  =>
		      ...,field_value
		</PRE>
		
	*/
	void getField(String declaringClass, String fieldName, String fieldType);

	/**
		Push the contents of the described static field onto the stack.
		<PRE>
		Stack ...  =>
		      ...,field_value
		</PRE>
	*/
	void getStaticField(String declaringClass, String fieldName, String fieldType);

	/**
	Pop the top stack value and store it in the local field. 
	This call pushes the this instance required to access the field itself.
	This call does not leave any value on the stack.

	<PRE>
	Stack ...,value  =>
	      ...
	</PRE>
	*/
	void setField(LocalField field);

	/**
		Pop the top stack value and store it in the local field. 
		This call pushes the this instance required to access the field itself.
		Like the Java language 'field = value', this leaves the value on the stack.

		<PRE>
		Stack ...,value  =>
		      ...,value
		</PRE>
	*/
	void putField(LocalField field);

	/**
		Pop the top stack value and store it in the instance field of this class.
		This call pushes the this instance required to access the field itself.
		Like the Java language 'field = value', this leaves the value on the stack.

		<PRE>
		Stack ...,value  =>
		      ...,value
		</PRE>
	*/
	void putField(String fieldName, String fieldType);

	/**
		Pop the top stack value and store it in the field.
		This call requires the instance to be pushed by the caller.
		Like the Java language 'field = value', this leaves the value on the stack.

		<PRE>
		Stack ...,field_ref,value  =>
		      ...,value
		</PRE>
	*/
	void putField(String declaringClass, String fieldName, String fieldType);

	/**
		Initiate a sequence that calls a constructor, equivalent to the new operator in Java.
		After this call, the caller must push any arguments and then complete the
		construction with a call to pushNewComplete(). Only arguments to the constructor
		can be pushed onto the stack between the pushNewStart() and pushNewComplete() method
		calls.

		<PRE>
		Stack ... => [unchanged]
		      ...
		</PRE>

		@param className class name of object to be created.
	*/
	void pushNewStart(String className);


	/**
		Complete the sequence that was started with pushNewStart().
		Pop the arguments to the constructor and push the reference
		to the newly created object.

		<PRE>
		Stack ...,value* => [numArgs number of values will be popped]
		      ...,new_ref
		</PRE>

		@param numArgs number of arguments to the constructor (can be 0).
	*/
	void pushNewComplete(int numArgs);

	/**
		Create an instance of an array and push it onto the stack. 

		<PRE>
		Stack ...  =>
		      ...,array_ref
		</PRE>

		@param className - type of array.
		@param size - number of elements in the array
	*/
	void pushNewArray(String className, int size);


	/**
		Push this onto the stack.
		<PRE>
		Stack ...  =>
		      ...,this_ref
		</PRE>
	*/
	void pushThis();

	/**
		Upcast the top stack value. This is used for correct method resolution
		by upcasting method parameters. It does not put any casting code into the
		byte code stream. Can only be used for refrences.
		<PRE>
		Stack ...,ref =>
		      ...,ref
		</PRE>
	*/
	void upCast(String className);

	/**
		Cast the top stack value. Correctly down-casts a reference or casts
		a primitive type (e.g. int to short).
		<PRE>
		Stack ...,value =>
		      ...,cast_value
		</PRE>

		@param className type (primitive, interface or class) to cast to.
	*/
	void cast(String className);

	/**
		Pop the top stack value and push a boolean that is the result of
		an instanceof check on the popped reference.
		<PRE>
		Stack ...,ref =>
		      ...,boolean_value
		</PRE>.
	*/
	void isInstanceOf(String className);
	
	/**
	 * Pop the top value off the stack
		<PRE>
		Stack ..., value =>
		      ...
		</PRE>.
	*/
	void pop();
		
	/**
		End a statement.
		Pops the top-word of the stack, if any.
		Must only be called if zero or one item exists
		on the stack.
		<PRE>
		Stack value =>
		      :empty:
		or

		Stack :empty: =>
		      :empty:

		</PRE>.
	*/
	void endStatement();

	/**
		Return from a method, optionally with a value.
		Must only be called if zero or one item exists
		on the stack. If the stack contains a single
		value then that is popped and used as the returned value.
		<PRE>
		Stack value =>
		      :empty:
		or

		Stack :empty: =>
		      :empty:

		</PRE>.
	*/
	void methodReturn();

	/**
		Initiate a conditional sequence.
		The top value on the stack (a reference) is popped and compared to 'null'.
		If the value is null then the code following this call until the startElseCode()
		will be executed at runtime, otherwise the code following startElseCode() until
		the completeConditional() is called.
		<BR>
		E.g. 

		<PRE>
		mb.callMethod(...); // pushes an object onto the stack
		mb.conditionalIfNull();
		  mb.push(3);
		mb.startElseCode();
		  mb.push(5);
		mb.completeConditional();
		// at this point 3 or 5 will be on the stack
		</PRE>

		Each path through the ?: statement must leave the stack at the same depth
		as the other.
		<BR>
		If the if or else code pops values from the stack that were before the conditional
		value, then they must use the same number of values from the stack.

		<PRE>
		Stack ...,ref =>
		      ...
		</PRE>.

	*/

	void conditionalIfNull();
	
	/**
		Initiate a conditional sequence.
		The top value on the stack must be a boolean and will be popped. If it
		is true then the code following this call until the startElseCode()
		will be executed at runtime, otherwise the code following startElseCode() until
		the completeConditional() is called. See conditionalIfNull() for example
		and restrictions.

		<PRE>
		Stack ...,boolean_value =>
		      ...
		</PRE>.
	*/
	void conditionalIf();

	/**
		Complete the true code path of a conditional.
	*/
	void startElseCode();

	/**
		Complete a conditional which completes the false code path.
	*/
	void completeConditional();

	/**
		Call a method. The instance (receiver or reference) for non-static methods
		must be pushed by the caller. The instance (for non-static) and the arguments
		are popped of the stack, and the return value (if any) is pushed onto the stack.
		<BR>
		The type needs to be one of:
		<UL>
		<LI> VMOpcode.INVOKESTATIC - call a static method
		<LI> VMOpcode.INVOKEVIRTUAL - call method declared in the class or super-class.
		<LI> VMOpcode.INVOKEINTERFACE - call a method declared in an interface
		</UL>


		<PRE>
		static methods

		Stack ...,value* => [numArgs number of values will be popped]
		      ...,return_value [void methods will not push a value]

		non-static methods

		Stack ...,ref,value* => [numArgs number of values will be popped]
		      ...,return_value [void methods will not push a value]
		</PRE>

		<BR>
		The type of the arguments to the methods must exactly match the declared types
		of the parameters to the methods. If a argument is of the incorrect type the
		caller must up cast it or down cast it.

		@param type type of method invocation
		@param declaringClass Class or interface the method is declared in. If it is a non-static
			method call then if declaringClass is null, the declared type is taken to be the
			type of the reference that will be popped.

		@param methodName name of the method
		@param returnType class name or primitive type (including "void") of the return type of the method, can not be null.
		@param numArgs number of arguments to the method (can be 0).

	*/
	int callMethod(short type, String declaringClass, String methodName,
				   String returnType, int numArgs);

	/**	
		Return an object that efficiently (to the implementation) describes a zero-argument method and
		can be used with the single argument callMethod(). Descriptions for the parameters to this
		method are the same as the five argument callMethod(). This allows the caller to cache frequently
		used methods. The returned object is only valid for use by this MethodBuilder.
		<BR>
		This call does not affect the Stack.
	*/
	Object describeMethod(short opcode, String declaringClass, String methodName, String returnType);

	/**
		Call a method previously described by describeMethod().
		<PRE>
		static methods

		Stack ...,value* => [numArgs number of values will be popped]
		      ...,return_value [void methods will not push a value]

		non-static methods

		Stack ...,ref,value* => [numArgs number of values will be popped]
		      ...,return_value [void methods will not push a value]
		</PRE>

	*/
	int callMethod(Object methodDescriptor);

	/**
		Call super(). Caller must only add this to a constructor.
		<PRE>

		Stack ... =>
		      ... 
		</PRE>

	*/
	void callSuper();

	/**
		Pop an array refrence off the stack and push an element from that array.
		<PRE>
		Stack ...,array_ref =>
		      ...,value
		</PRE>

		@param element Offset into the array (zero based)
	*/
	void getArrayElement(int element);

	/**
		Pop an array reference off the stack, store a value in the array at the passed in offset.
		<PRE>
		Stack ...,array_ref, value =>
		      ...
		</PRE>

		@param element Offset into the array (zero based)
	*/
	void setArrayElement(int element);


	/**
		Swap the top two values on the stack.
		<PRE>
		Stack ...,valueA,valueB =>
		      ...,valueB,valueA
		</PRE>
	*/
	void swap();

	/**
		Duplicate the top value on the stack.
		<PRE>
		Stack ...,value =>
		      ...,value,value
		</PRE>
	*/
	void dup();

	/**
		Tell if statement number in this method builder hits limit.  This
		method builder keeps a counter of how many statements are added to it.
		Caller should call this function every time it tries to add a statement
		to this method builder (counter is increased by 1), then the function
		returns whether the accumulated statement number hits a limit.
		The reason of doing this is that Java compiler has a limit of 64K code
		size for each method.  We might hit this limit if an extremely long
		insert statement is issued, for example (see beetle 4293).  Counting
		statement number is an approximation without too much overhead.
	*/
	boolean statementNumHitLimit(int noStatementsAdded);

	void setSparkExplain(boolean sparkExplain);

	boolean isSparkExplain();
}

