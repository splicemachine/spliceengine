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

package com.splicemachine.db.impl.sql.compile;

import com.splicemachine.db.iapi.error.StandardException;

import com.splicemachine.db.iapi.sql.compile.TypeCompiler;

import com.splicemachine.db.iapi.services.compiler.MethodBuilder;
import com.splicemachine.db.iapi.services.compiler.LocalField;
import com.splicemachine.db.iapi.services.io.StoredFormatIds;

import com.splicemachine.db.iapi.services.sanity.SanityManager;

import com.splicemachine.db.catalog.TypeDescriptor;

import com.splicemachine.db.iapi.util.ReuseFactory;
import com.splicemachine.db.iapi.services.classfile.VMOpcode;
import com.splicemachine.db.iapi.types.*;

import java.lang.reflect.Modifier;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;

/**
    User type constants.  These are created by built-in types
    that use user types as their implementation. This could also
    potentially be used by an optimizer that wanted to store plans
    for frequently-used parameter values.

    This is also used to represent nulls in user types, which occurs
    when NULL is inserted into or supplied as the update value for
    a usertype column.

 */
public class UserTypeConstantNode extends ConstantNode {
    /*
    ** This value field hides the value in the super-type.  It is here
    ** Because user-type constants work differently from built-in constants.
    ** User-type constant values are stored as Objects, while built-in
    ** constants are stored as StorableDataValues.
    **
    ** RESOLVE: This is a bit of a mess, and should be fixed.  All constants
    ** should be represented the same way.
    */
    Object    value;

    /**
     * Initializer for a typed null node
     * or a date, time, or timestamp value. Parameters may be:
     *
     * <ul>
     * <li>arg1    The TypeId for the type of the node</li>
     * <li>arg2    The factory to get the TypeId and DataTypeServices factories from.</li>
     * </ul>
     *
     * <p>
     * - OR -
     * </p>
     *
     * <ul>
     * <li>arg1 the date, time, or timestamp value</li>
     * </ul>
     *
     * @exception StandardException thrown on failure
     */
    public void init(Object arg1)
            throws StandardException {
        DataValueDescriptor dvd = null;
        
        if (arg1 instanceof TypeId)
        {
            super.init(
                    arg1,
                    Boolean.TRUE,
                    ReuseFactory.getInteger(
                                        TypeDescriptor.MAXIMUM_WIDTH_UNKNOWN));
        }
        else
        {
            Integer maxWidth = null;
            TypeId    typeId = null;

            if( arg1 instanceof DataValueDescriptor)
                dvd = (DataValueDescriptor) arg1;
            if (arg1 instanceof Date
                || (dvd != null && dvd.getTypeFormatId() == StoredFormatIds.SQL_DATE_ID))
            {
                maxWidth = ReuseFactory.getInteger(TypeId.DATE_MAXWIDTH);
                typeId = TypeId.getBuiltInTypeId(Types.DATE);
            }
            else if (arg1 instanceof Time
                     || (dvd != null && dvd.getTypeFormatId() == StoredFormatIds.SQL_TIME_ID))
            {
                maxWidth = ReuseFactory.getInteger(TypeId.TIME_MAXWIDTH);
                typeId = TypeId.getBuiltInTypeId(Types.TIME);
            }
            else if (arg1 instanceof Timestamp
                     || (dvd != null && dvd.getTypeFormatId() == StoredFormatIds.SQL_TIMESTAMP_ID))
            {
                maxWidth = ReuseFactory.getInteger(TypeId.TIMESTAMP_MAXWIDTH);
                typeId = TypeId.getBuiltInTypeId(Types.TIMESTAMP);
            }
            else
            {
                if (SanityManager.DEBUG)
                {
                    SanityManager.THROWASSERT(
                            "Unexpected class " + arg1.getClass().getName());
                }
            }

            assert maxWidth != null;

            super.init(
                typeId,
                (arg1 == null) ? Boolean.TRUE : Boolean.FALSE,
                maxWidth);

            if( dvd != null) {
                setValue(dvd);
                if (dvd.isNull()) {
                    value = null;
                } else {
                    value = dvd;
                }
            } else {
                if (arg1 instanceof Date) {
                    setValue(new SQLDate((Date) arg1));
                } else if (arg1 instanceof Time) {
                    setValue(new SQLTime((Time) arg1));
                } else if (arg1 instanceof Timestamp) {
                    setValue(new SQLTimestamp((Timestamp) arg1));
                }
                value = arg1;
            }


        }
    }

    /**
     * Return the object value of this user defined type.
     *
     * @return    the value of this constant. can't use getValue() for this.
     *            getValue() returns the DataValueDescriptor for the built-in
     *            types that are implemented as user types (date, time, timestamp)
     */
    public    Object    getObjectValue() { return value; }

    /**
     * Return whether or not this node represents a typed null constant.
     *
     */
    public boolean isNull()
    {
        return (value == null);
    }

    /**
     * Return an Object representing the bind time value of this
     * expression tree.  If the expression tree does not evaluate to
     * a constant at bind time then we return null.
     * This is useful for bind time resolution of VTIs.
     * RESOLVE: What do we do for primitives?
     *
     * @return    An Object representing the bind time value of this expression tree.
     *            (null if not a bind time constant.)
     *
     */
    public Object getConstantValueAsObject()
    {
        return value;
    }

    /**
     * For a UserTypeConstantNode, we have to store away the object somewhere
     * and have a way to get it back at runtime.
     * These objects are serializable.  This gives us at least two options:
     * 1) serialize it out into a byte array field, and serialize
     *      it back in when needed, from the field.
     * 2) have an array of objects in the prepared statement and a #,
     *      to find the object directly. Because it is serializable, it
     *      will store with the rest of the executable just fine.
     * Choice 2 gives better performance -- the ser/deser cost is paid
     * on database access for the statement, not for each execution of it.
     * However, it requires some infrastructure support from prepared
     * statements.  For now, we take choice 3, and make some assumptions
     * about available methods on the user type.  This choice has the
     * shortcoming that it will not work for arbitrary user types.
     * REVISIT and implement choice 2 when a general solution is needed.
     * <p>
     * A null is generated as a Null value cast to the type of
     * the constant node.
     *
     * @param acb    The ExpressionClassBuilder for the class being built
     * @param mb    The method the expression will go into
     *
     *
     * @exception StandardException        Thrown on error
     */
    public void generateExpression(ExpressionClassBuilder acb,
                                            MethodBuilder mb)
                                    throws StandardException {

        TypeCompiler        tc = getTypeCompiler();
        String fieldType = tc.interfaceName();

        /*
        ** NOTE: DO NOT CALL THE CONSTRUCTOR TO GENERATE ANYTHING.  IT HAS
        ** A DIFFERENT value FIELD.
        */

        /* Are we generating a SQL null value? */
        if (value == null)
        {
            acb.generateNull(mb, tc, getTypeServices());
        }
        // The code generated here is invoked when the generated class is constructed. However the prepared statement
        // is not set into the activation class when it is constructed, but later. So we cannot use the getSavedObject
        // method to retrieve the value.
//         else if( value instanceof DataValueDescriptor)
//         {
//             acb.pushThisAsActivation( mb);
//             mb.callMethod( VMOpcode.INVOKEINTERFACE,
//                            null,
//                            "getPreparedStatement",
//                            ClassName.ExecPreparedStatement,
//                            0);
//             mb.push( acb.addItem( value));
//             mb.callMethod( VMOpcode.INVOKEINTERFACE,
//                            null,
//                            "getSavedObject",
//                            "java.lang.Object",
//                            1);
//             mb.cast( fieldType);
//         }
        else
        {
            /*
                The generated java is the expression:
                    <java type name>.valueOf("<value.toString>")

                super.generateValue will wrap this expression in
                the appropriate column constructor.

                If the type doesn't have a valueOf method, then we will
                give an error.  We have to assume that valueOf will
                reconstruct the object from a String literal.  If this is
                a false assumption, some other object may be constructed,
                or a runtime error may result due to valueOf failing.
             */
            String typeName = getTypeId().getCorrespondingJavaTypeName();

            mb.push(value.toString());
            mb.callMethod(VMOpcode.INVOKESTATIC, typeName, "valueOf", typeName, 1);

            LocalField field = acb.newFieldDeclaration(Modifier.PRIVATE, fieldType);

            acb.generateDataValue(mb, tc, getTypeServices().getCollationType(), field);
        }
    }


    /**
     * Should never be called for UserTypeConstantNode because
     * we have our own generateExpression().
     *
     * @param acb    The ExpressionClassBuilder for the class being built
     * @param mb    The method the expression will go into
     *
     * @exception StandardException        Thrown on error
     */
    void generateConstant(ExpressionClassBuilder acb, MethodBuilder mb)
    throws StandardException
    {
        if (SanityManager.DEBUG)
        {
            SanityManager.THROWASSERT("geneateConstant() not expected to be called for UserTypeConstantNode because we have implemented our own generateExpression().");
        }
    }
}
