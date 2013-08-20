package com.splicemachine.derby.impl;

import java.io.IOException;
import java.lang.reflect.Method;
import java.util.HashSet;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.reference.SQLState;
import org.apache.derby.iapi.sql.Activation;
import org.apache.derby.impl.sql.execute.BaseActivation;
import org.apache.log4j.Logger;

import com.google.common.collect.Sets;
import com.splicemachine.utils.SpliceLogUtils;

public class SpliceMethod<T> {
	private static Logger LOG = Logger.getLogger(SpliceMethod.class);
	protected String methodName;
	protected Activation activation;
	protected Method method;
	protected static enum DirectCalls {
		e0,e1,e2,e3,e4,e5,e6,e7,e8,e9;
	  protected static HashSet<String> strings; 
	  static {
		  strings = Sets.newHashSet("e0","e1","e2","e3","e4","e5","e6","e7","e8","e9");
	  }	
	  public static boolean contains(String s) {
		  return strings.contains(s);
	  } 
	}
	
	
	public SpliceMethod() {
		
	}
	public SpliceMethod(String methodName, Activation activation) {
		this.methodName = methodName;
		this.activation = activation;
	}
	
	@SuppressWarnings("unchecked")
	public T invoke() throws StandardException{
		if (DirectCalls.contains(methodName)) 
			return (T) directInvoke(DirectCalls.valueOf(methodName));
		else { 	
			try {
				if (method == null)
					method = activation.getClass().getMethod(methodName, null);
				return (T) method.invoke(activation, null);
			} catch (Exception e) {
				SpliceLogUtils.logAndThrow(LOG, "error during invoke",
						StandardException.newException(SQLState.DATA_UNEXPECTED_EXCEPTION,e));
				return null;
			} 
		}
	}
	
	public Object directInvoke(DirectCalls dc) throws StandardException {
		BaseActivation ba = ((BaseActivation) activation);
		switch (dc) {
		case e0:
			return ba.e0();
		case e1:
			return ba.e1();
		case e2:
			return ba.e2();
		case e3:
			return ba.e3();
		case e4:
			return ba.e4();
		case e5:
			return ba.e5();
		case e6:
			return ba.e6();
		case e7:
			return ba.e7();
		case e8:
			return ba.e8();
		case e9:
			return ba.e9();
		default:
			throw StandardException.unexpectedUserException(new IOException("DirectInvoke Wrong Field"));		
		}
	}
}
