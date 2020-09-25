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

package com.splicemachine.db.impl.sql.execute;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.services.context.Context;
import com.splicemachine.db.iapi.services.context.ContextManager;
import com.splicemachine.db.iapi.services.loader.GeneratedByteCode;
import com.splicemachine.db.iapi.services.loader.GeneratedClass;
import com.splicemachine.db.iapi.services.loader.GeneratedMethod;
import com.splicemachine.db.iapi.services.sanity.SanityManager;
import com.splicemachine.db.iapi.sql.conn.LanguageConnectionContext;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.iapi.types.DataValueDescriptor;
import com.splicemachine.db.iapi.types.DataValueFactory;
import com.splicemachine.db.iapi.types.NumberDataValue;

public abstract class BaseExecutableIndexExpression implements GeneratedByteCode {
    private GeneratedClass gc;

    private   LanguageConnectionContext lcc;
    protected ContextManager            cm;
    protected DataValueFactory          dvf;

    // class interface

    abstract public void runExpression(ExecRow baseRow, ExecRow indexRow) throws StandardException;

    // needed methods

    public DataValueFactory getDataValueFactory() {
        return dvf;
    }

    // this is needed by the SQL LENGTH() function and borrowed from BaseActivation
    public NumberDataValue getDB2Length(DataValueDescriptor value,
                                        int constantLength,
                                        NumberDataValue reUse)
            throws StandardException
    {
        if( reUse == null)
            reUse = getDataValueFactory().getNullInteger( null);
        if( value == null || value.isNull())
            reUse.setToNull();
        else {
            if( constantLength >= 0)
                reUse.setValue(constantLength);
            else
                reUse.setValue(value.getLength());
        }
        return reUse;
    }

    // GeneratedByteCode interface

    public final void initFromContext(Context context) throws StandardException {
        if (SanityManager.DEBUG) {
            SanityManager.ASSERT(context!=null, "NULL context passed to BaseExecutableIndexExpression.initFromContext");
        }
        this.cm = context.getContextManager();
        lcc = (LanguageConnectionContext) cm.getContext(LanguageConnectionContext.CONTEXT_ID);
        dvf = lcc.getDataValueFactory();
    }

    public void postConstructor(){}

    public final void setGC(GeneratedClass gc) {
        this.gc = gc;
    }

    public final GeneratedClass getGC() {
        if (SanityManager.DEBUG) {
            if (gc == null)
                SanityManager.THROWASSERT("move code requiring GC to postConstructor() method!!");
        }
        return gc;
    }

    public final GeneratedMethod getMethod(String methodName) throws StandardException {
        return getGC().getMethod(methodName);
    }

    public Object e0() throws StandardException { return null; }
    public Object e1() throws StandardException { return null; }
    public Object e2() throws StandardException { return null; }
    public Object e3() throws StandardException { return null; }
    public Object e4() throws StandardException { return null; }
    public Object e5() throws StandardException { return null; }
    public Object e6() throws StandardException { return null; }
    public Object e7() throws StandardException { return null; }
    public Object e8() throws StandardException { return null; }
    public Object e9() throws StandardException { return null; }

    public String e0ToString() throws StandardException { return null; }
    public String e1ToString() throws StandardException { return null; }
    public String e2ToString() throws StandardException { return null; }
    public String e3ToString() throws StandardException { return null; }
    public String e4ToString() throws StandardException { return null; }
    public String e5ToString() throws StandardException { return null; }
    public String e6ToString() throws StandardException { return null; }
    public String e7ToString() throws StandardException { return null; }
    public String e8ToString() throws StandardException { return null; }
    public String e9ToString() throws StandardException { return null; }
}
