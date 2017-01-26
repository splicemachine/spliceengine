/*
 * Copyright (c) 2012 - 2017 Splice Machine, Inc.
 *
 * This file is part of Splice Machine.
 * Splice Machine is free software: you can redistribute it and/or modify it under the terms of the
 * GNU Affero General Public License as published by the Free Software Foundation, either
 * version 3, or (at your option) any later version.
 * Splice Machine is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU Affero General Public License for more details.
 * You should have received a copy of the GNU Affero General Public License along with Splice Machine.
 * If not, see <http://www.gnu.org/licenses/>.
 */

package com.splicemachine.derby.impl;

import java.lang.reflect.Method;
import java.util.HashMap;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.services.loader.GeneratedMethod;
import com.splicemachine.db.iapi.sql.Activation;
import com.splicemachine.db.impl.sql.execute.BaseActivation;
import org.apache.log4j.Logger;

import com.splicemachine.pipeline.Exceptions;

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
                    method = activation.getClass().getMethod(methodName);
                return (T) method.invoke(activation);
            } catch (Exception e) {
                throw Exceptions.parseException(e);
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
	
	public Activation getActivation() {
		return activation;
	}
	
}
