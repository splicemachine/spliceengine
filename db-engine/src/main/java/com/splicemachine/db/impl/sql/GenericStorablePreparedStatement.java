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

package com.splicemachine.db.impl.sql;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.services.context.ContextService;
import com.splicemachine.db.iapi.services.io.ArrayUtil;
import com.splicemachine.db.iapi.services.io.Formatable;
import com.splicemachine.db.iapi.services.io.StoredFormatIds;
import com.splicemachine.db.iapi.services.loader.ClassFactory;
import com.splicemachine.db.iapi.services.loader.GeneratedClass;
import com.splicemachine.db.iapi.services.sanity.SanityManager;
import com.splicemachine.db.iapi.sql.ResultDescription;
import com.splicemachine.db.iapi.sql.Statement;
import com.splicemachine.db.iapi.sql.StorablePreparedStatement;
import com.splicemachine.db.iapi.sql.conn.LanguageConnectionContext;
import com.splicemachine.db.iapi.sql.execute.ConstantAction;
import com.splicemachine.db.iapi.sql.execute.ExecPreparedStatement;
import com.splicemachine.db.iapi.types.DataTypeDescriptor;
import com.splicemachine.db.iapi.util.ByteArray;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

/**
 * Prepared statement that can be made persistent.
 */
public class GenericStorablePreparedStatement extends GenericPreparedStatement
        implements Formatable, StorablePreparedStatement {

    // formatable
    private ByteArray byteCode;
    private String className;

    /**
     * Default constructor, for formatable only.
     */
    public GenericStorablePreparedStatement() {
        super();
    }

    public GenericStorablePreparedStatement(Statement stmt) {
        super(stmt);
    }

    /**
     * Get our byte code array.  Used by others to save off our byte code for us.
     *
     * @return the byte code saver
     */
    @Override
    public ByteArray getByteCodeSaver() {
        if (byteCode == null) {
            byteCode = new ByteArray();
        }
        return byteCode;
    }

    /**
     * Get and load the activation class.  Will always return a loaded/valid class or null if the class cannot be loaded.
     *
     * @return the generated class, or null if the class cannot be loaded
     */
    @Override
    public GeneratedClass getActivationClass() throws StandardException {
        if (activationClass == null) {
            loadGeneratedClass();
        }
        return activationClass;
    }

    @Override
    public void setActivationClass(GeneratedClass ac) {

        super.setActivationClass(ac);
        if (ac != null) {
            className = ac.getName();

            // see if this is an pre-compiled class
            if (byteCode != null && byteCode.getArray() == null)
                byteCode = null;
        }
    }

    /////////////////////////////////////////////////////////////
    //
    // STORABLEPREPAREDSTATEMENT INTERFACE
    //
    /////////////////////////////////////////////////////////////

    /**
     * Load up the class from the saved bytes.
     */
    @Override
    public void loadGeneratedClass() throws StandardException {
        LanguageConnectionContext lcc = (LanguageConnectionContext) ContextService.getContext
                (LanguageConnectionContext.CONTEXT_ID);
        ClassFactory classFactory = lcc.getLanguageConnectionFactory().getClassFactory();
        GeneratedClass gc = classFactory.loadGeneratedClass(className, byteCode);
       /* No special try catch logic to write out bad classes here.  We don't expect any problems, and in any
        * event, we don't have the class builder available here. */
        setActivationClass(gc);
    }

    @Override
    public ExecPreparedStatement getClone() throws StandardException {
        GenericStorablePreparedStatement clone = new GenericStorablePreparedStatement(statement);
        shallowClone(clone);
        clone.className = this.className;
        clone.byteCode = this.byteCode;
        return clone;
    }

    /////////////////////////////////////////////////////////////
    //
    // EXTERNALIZABLE INTERFACE
    //
    /////////////////////////////////////////////////////////////

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeObject(getCursorInfo());
        out.writeBoolean(needsSavepoint());
        out.writeBoolean(isAtomic);
        out.writeObject(executionConstants);
        out.writeObject(resultDesc);

        // savedObjects may be null
        if (savedObjects == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            ArrayUtil.writeArrayLength(out, savedObjects);
            ArrayUtil.writeArrayItems(out, savedObjects);
        }

        /*  Write out the class name and byte code if we have them.  They might be null if we don't want to write out
         * the plan, and would prefer it just write out null (e.g. we know the plan is invalid). */
        out.writeObject(className);
        out.writeBoolean(byteCode != null);
        if (byteCode != null)
            byteCode.writeExternal(out);

        out.writeBoolean(paramTypeDescriptors != null);
        if (paramTypeDescriptors != null) {
            ArrayUtil.writeArrayLength(out, paramTypeDescriptors);
            ArrayUtil.writeArrayItems(out, paramTypeDescriptors);
        }
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        setCursorInfo((CursorInfo) in.readObject());
        setNeedsSavepoint(in.readBoolean());
        isAtomic = (in.readBoolean());
        executionConstants = (ConstantAction) in.readObject();
        resultDesc = (ResultDescription) in.readObject();

        if (in.readBoolean()) {
            savedObjects = new Object[ArrayUtil.readArrayLength(in)];
            ArrayUtil.readArrayItems(in, savedObjects);
        }

        className = (String) in.readObject();
        if (in.readBoolean()) {
            byteCode = new ByteArray();
            byteCode.readExternal(in);
        } else
            byteCode = null;

        if (in.readBoolean()) {
            paramTypeDescriptors = new DataTypeDescriptor[ArrayUtil.readArrayLength(in)];
            ArrayUtil.readArrayItems(in, paramTypeDescriptors);
        }
    }

    /////////////////////////////////////////////////////////////
    //
    // FORMATABLE INTERFACE
    //
    /////////////////////////////////////////////////////////////

    /**
     * Get the formatID which corresponds to this class.
     */
    @Override
    public int getTypeFormatId() {
        return StoredFormatIds.STORABLE_PREPARED_STATEMENT_V01_ID;
    }

    /////////////////////////////////////////////////////////////
    //
    // MISC
    //
    /////////////////////////////////////////////////////////////

    @Override
    public boolean isStorable() {
        return true;
    }

    @Override
    public String toString() {
        if (SanityManager.DEBUG) {
            String acn;
            if (activationClass == null)
                acn = "null";
            else
                acn = activationClass.getName();

            return "GSPS " + System.identityHashCode(this) + " activationClassName=" + acn + " className=" + className;
        } else {
            return "";
        }
    }


}

