package com.splicemachine.derby.impl;

import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.reference.SQLState;
import org.apache.derby.iapi.services.loader.GeneratedMethod;
import org.apache.derby.iapi.sql.Activation;
import org.apache.derby.impl.sql.execute.BaseActivation;
import org.apache.log4j.Logger;

import com.splicemachine.utils.SpliceLogUtils;

public class SpliceMethod<T> {
	private static Logger LOG = Logger.getLogger(SpliceMethod.class);
	protected String methodName;
	protected Activation activation;
	protected Method method;
	private static final HashMap<String,GeneratedMethod> directs;
	 static {
		directs = new HashMap<String,GeneratedMethod>(10);
		for (int i = 0; i < 10; i++) {
			directs.put("e"+i, new DirectCall(i));
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
		GeneratedMethod genMethod = directs.get(methodName);
		if (genMethod != null)
			return (T) genMethod.invoke(activation);
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
	
	static class DirectCall implements GeneratedMethod {
		private final int which;

		DirectCall(int which) {

			this.which = which;
		}

		public Object invoke(Object activation)
			throws StandardException {

			try {
				BaseActivation ba = ((BaseActivation) activation);
				switch (which) {
				case 0:
					return ba.e0();
				case 1:
					return ba.e1();
				case 2:
					return ba.e2();
				case 3:
					return ba.e3();
				case 4:
					return ba.e4();
				case 5:
					return ba.e5();
				case 6:
					return ba.e6();
				case 7:
					return ba.e7();
				case 8:
					return ba.e8();
				case 9:
					return ba.e9();
				}
				return null;
			} catch (StandardException se) {
				throw se;
			}		
			catch (Throwable t) {
				throw StandardException.unexpectedUserException(t);
			}
		}

//		@Override
		public String getMethodName() {
			return "e"+which;
		}
	}
	
}
