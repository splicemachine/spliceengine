/*
 * Copyright (c) 2012 - 2020 Splice Machine, Inc.
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

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.Activation;
import org.apache.log4j.Logger;

import com.splicemachine.pipeline.Exceptions;

public class SpliceMethod<T> {
    private static Logger LOG = Logger.getLogger(SpliceMethod.class);
    protected String methodName;
    protected Activation activation;
    protected Method method;

    public SpliceMethod() {

    }

    public SpliceMethod(String methodName, Activation activation) {
        this.methodName = methodName;
        this.activation = activation;
    }

    @SuppressWarnings("unchecked")
    public T invoke() throws StandardException {
        try {
        	if (method == null) {
        	    method =  activation.getClass().getMethod(methodName);
            }
            return (T) method.invoke(activation);
        } catch (Throwable t) {
            throw Exceptions.parseException(t);
        }
    }

	public Activation getActivation() {
		return activation;
	}
	
}
