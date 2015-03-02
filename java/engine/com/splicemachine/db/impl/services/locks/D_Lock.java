/*

   Derby - Class com.splicemachine.db.impl.services.locks.D_Lock

   Licensed to the Apache Software Foundation (ASF) under one or more
   contributor license agreements.  See the NOTICE file distributed with
   this work for additional information regarding copyright ownership.
   The ASF licenses this file to you under the Apache License, Version 2.0
   (the "License"); you may not use this file except in compliance with
   the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.

 */

package com.splicemachine.db.impl.services.locks;

import com.splicemachine.db.iapi.services.diag.Diagnosticable;
import com.splicemachine.db.iapi.services.diag.DiagnosticUtil;
import com.splicemachine.db.iapi.error.StandardException;

import java.util.Properties;

/**
**/

public class D_Lock implements Diagnosticable
{
    protected Lock lock;

    public D_Lock()
    {
    }

    /* Private/Protected methods of This class: */

	/*
	** Methods of Diagnosticable
	*/
    public void init(Object obj)
    {
        lock = (Lock) obj;
    }

    /**
     *
	 * @exception  StandardException  Standard exception policy.
     **/
    public String diag()
        throws StandardException
    {
		StringBuffer sb = new StringBuffer(128);

		sb.append("Lockable=");
		sb.append(DiagnosticUtil.toDiagString(lock.getLockable()));

		sb.append(" Qual=");
		sb.append(DiagnosticUtil.toDiagString(lock.getQualifier()));

		sb.append(" CSpc=");
		sb.append(lock.getCompatabilitySpace());

		sb.append(" count=" + lock.count + " ");

		return sb.toString();
    }

	public void diag_detail(Properties prop) {}
}

