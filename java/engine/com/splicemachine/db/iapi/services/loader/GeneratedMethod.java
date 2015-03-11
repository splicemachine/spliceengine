package com.splicemachine.db.iapi.services.loader;

import com.splicemachine.db.iapi.error.StandardException;

/**
	Handle for a method within a generated class.

	@see GeneratedClass
*/

public interface GeneratedMethod {


	/**
		Invoke a generated method that has no arguments.
		(Similar to java.lang.refect.Method.invoke)

		Returns the value returned by the method.

		@exception 	StandardException	Standard Derby error policy
	*/

	public Object invoke(Object ref)
		throws StandardException;
	
	public String getMethodName();
	
}
