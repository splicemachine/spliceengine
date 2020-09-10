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

import com.splicemachine.db.iapi.services.loader.ClassInspector;

import com.splicemachine.db.iapi.services.compiler.MethodBuilder;
import com.splicemachine.db.iapi.services.compiler.LocalField;


import com.splicemachine.db.iapi.services.sanity.SanityManager;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.services.i18n.MessageService;

import com.splicemachine.db.iapi.sql.dictionary.TableDescriptor;

import com.splicemachine.db.iapi.reference.SQLState;

import com.splicemachine.db.iapi.util.JBitSet;

import com.splicemachine.db.catalog.TypeDescriptor;

import java.lang.reflect.Member;
import java.lang.reflect.Modifier;

import java.util.List;

/**
 * A NewInvocationNode represents a new object() invocation.
 *
 */
public class NewInvocationNode extends MethodCallNode
{
    // Whether or not to do a single instantiation
    private boolean singleInstantiation = false;

    private boolean isBuiltinVTI = false;

    /**
     * Initializer for a NewInvocationNode. Parameters are:
     *
     * <ul>
     * <li>javaClassName        The full package.class name of the class</li>
     * <li>parameterList        The parameter list for the constructor</li>
     * </ul>
     *
     * @exception StandardException        Thrown on error
     */
    public void init(
                    Object javaClassName,
                    Object params)
        throws StandardException
    {
        super.init("<init>");
        addParms((List) params);

        this.javaClassName = (String) javaClassName;
    }

    /* This version of the "init" method is used for mapping a table name
     * or table function name to a corresponding VTI class name.  The VTI
     * is then invoked as a regular NEW invocation node.
     *
     * There are two kinds of VTI mappings that we do: the first is for
     * "table names", the second is for "table function names".  Table
     * names can only be mapped to VTIs that do not accept any arguments;
     * any VTI that has at least one constructor which accepts one or more
     * arguments must be mapped from a table *function* name.  The way we
     * tell the difference is by looking at the received arguments: if
     * the vtiTableFuncName that we receive is null then we are mapping
     * a "table name" and tableDescriptor must be non-null; if the
     * vtiTableFuncName is non-null then we are mapping a "table
     * function name" and tableDescriptor must be null.
     *
     * Note that we could have just used a single "init()" method and
     * performed the mappings based on what type of Object "javaClassName"
     * was (String, TableDescriptor, or TableName), but making this VTI
     * mapping method separate from the "normal" init() method seems
     * cleaner...
     *
     * @param vtiTableFuncName A TableName object holding a qualified name
     *  that maps to a VTI which accepts arguments.  If vtiTableFuncName is
     *  null then tableDescriptor must NOT be null.
     * @param tableDescriptor A table descriptor that corresponds to a
     *  table name (as opposed to a table function name) that will be
     *  mapped to a no-argument VTI.  If tableDescriptor is null then
     *  vtiTableFuncName should not be null.
     * @param params Parameter list for the VTI constructor.
     * @param delimitedIdentifier Whether or not the target class name
     *  is a delimited identifier.
     */
    public void init(
                    Object vtiTableFuncName,
                    Object tableDescriptor,
                    Object params)
        throws StandardException
    {
        super.init("<init>");
        addParms((List) params);

        if (SanityManager.DEBUG)
        {
            // Exactly one of vtiTableFuncName or tableDescriptor should
            // be null.
            SanityManager.ASSERT(
                ((vtiTableFuncName == null) && (tableDescriptor != null)) ||
                ((vtiTableFuncName != null) && (tableDescriptor == null)),
                "Exactly one of vtiTableFuncName or tableDescriptor should " +
                "be null, but neither or both of them were null.");
        }

        TableName vtiName = (TableName)vtiTableFuncName;
        TableDescriptor td = (TableDescriptor)tableDescriptor;
        boolean isTableFunctionVTI = (vtiTableFuncName != null);
        if (isTableFunctionVTI)
        {
            // We have to create a generic TableDescriptor to
            // pass to the data dictionary.
            td = new TableDescriptor(getDataDictionary(),
                    vtiName.getTableName(),
                    getSchemaDescriptor(vtiName.getSchemaName()),
                    TableDescriptor.VTI_TYPE,
                    TableDescriptor.DEFAULT_LOCK_GRANULARITY,-1,
                    null,null,null,null,null,null, false,false, null);
        }

        /* Use the table descriptor to figure out what the corresponding
         * VTI class name is; we let the data dictionary do the mapping
         * for us.
         */
        this.javaClassName = getDataDictionary().getVTIClass(
            td, isTableFunctionVTI);

        this.isBuiltinVTI =
            ( getDataDictionary().getBuiltinVTIClass( td, isTableFunctionVTI) != null);

        /* If javaClassName is still null at this point then we
         * could not find the target class for the received table
         * (or table function) name.  So throw the appropriate
         * error.
         */
        if (this.javaClassName == null)
        {
            if (!isTableFunctionVTI)
            {
                /* Create a TableName object from the table descriptor
                 * that we received.  This gives us the name to use
                 * in the error message.
                 */
                vtiName = makeTableName(td.getSchemaName(),
                    td.getDescriptorName());
            }

            throw StandardException.newException(
                isTableFunctionVTI
                    ? SQLState.LANG_NO_SUCH_METHOD_ALIAS
                    : SQLState.LANG_TABLE_NOT_FOUND,
                vtiName.getFullTableName());
        }
    }

    /**
     * Report whether this node represents a builtin VTI.
     */
    public  boolean isBuiltinVTI()  { return isBuiltinVTI; }

    /**
     * Mark this node as only needing to
     * to a single instantiation.  (We can
     * reuse the object after newing it.)
     */
    void setSingleInstantiation()
    {
        singleInstantiation = true;
    }

    /**
     * Bind this expression.  This means binding the sub-expressions,
     * as well as figuring out what the return type is for this expression.
     *
     * @param fromList        The FROM list for the query this
     *                expression is in, for binding columns.
     * @param subqueryList        The subquery list being built as we find SubqueryNodes
     * @param aggregateVector    The aggregate vector being built as we find AggregateNodes
     *
     * @return    Nothing
     *
     * @exception StandardException        Thrown on error
     */

    public JavaValueNode bindExpression( FromList fromList, SubqueryList subqueryList, List<AggregateNode> aggregateVector)  throws StandardException {
        bindParameters(fromList, subqueryList, aggregateVector);

        verifyClassExist(javaClassName);
        /*
        ** Get the parameter type names out of the parameters and put them
        ** in an array.
        */
        String[]    parmTypeNames = getObjectSignature();
        boolean[]    isParam = getIsParam();
        ClassInspector classInspector = getClassFactory().getClassInspector();

        /*
        ** Find the matching constructor.
        */
        try
        {
            /* First try with built-in types and mappings */
            method = classInspector.findPublicConstructor(javaClassName,
                                            parmTypeNames, null, isParam);

            /* If no match, then retry to match any possible combinations of
             * object and primitive types.
             */
            if (method == null)
            {
                String[] primParmTypeNames = getPrimitiveSignature(false);
                method = classInspector.findPublicConstructor(javaClassName,
                                parmTypeNames, primParmTypeNames, isParam);
            }
        }
        catch (ClassNotFoundException e)
        {
            /*
            ** If one of the classes couldn't be found, just act like the
            ** method couldn't be found.  The error lists all the class names,
            ** which should give the user enough info to diagnose the problem.
            */
            method = null;
        }

        if (method == null)
        {
            /* Put the parameter type names into a single string */
            StringBuilder parmTypes = new StringBuilder();
            for (int i = 0; i < parmTypeNames.length; i++)
            {
                if (i != 0)
                    parmTypes.append(", ");
                parmTypes.append(!parmTypeNames[i].isEmpty() ?
                        parmTypeNames[i] :
                        MessageService.getTextMessage(
                                SQLState.LANG_UNTYPED));
            }

            throw StandardException.newException(SQLState.LANG_NO_CONSTRUCTOR_FOUND,
                                                    javaClassName,
                    parmTypes.toString());
        }

        methodParameterTypes = classInspector.getParameterTypes(method);

        for (int i = 0; i < methodParameterTypes.length; i++)
        {
            if (ClassInspector.primitiveType(methodParameterTypes[i]))
                methodParms[i].castToPrimitive(true);
        }

        /* Set type info for any null parameters */
        if ( someParametersAreNull() )
        {
            setNullParameterInfo(methodParameterTypes);
        }

        /* Constructor always returns an object of type javaClassName */
        if (SanityManager.DEBUG) {
            SanityManager.ASSERT(javaClassName.equals(classInspector.getType(method)),
                "Constructor is wrong type, expected " + javaClassName +
                " actual is " + classInspector.getType(method));
        }
         setJavaTypeName( javaClassName );
         if (routineInfo != null)
                {
                    TypeDescriptor returnType = routineInfo.getReturnType();
                    if (returnType != null)
                    {
                        setCollationType(returnType.getCollationType());
                    }
                }
         return this;
    }

    /**
     * Categorize this predicate.  Initially, this means
     * building a bit map of the referenced tables for each predicate.
     * If the source of this ColumnReference (at the next underlying level)
     * is not a ColumnReference or a VirtualColumnNode then this predicate
     * will not be pushed down.
     *
     * For example, in:
     *        select * from (select 1 from s) a (x) where x = 1
     * we will not push down x = 1.
     * NOTE: It would be easy to handle the case of a constant, but if the
     * inner SELECT returns an arbitrary expression, then we would have to copy
     * that tree into the pushed predicate, and that tree could contain
     * subqueries and method calls.
     * RESOLVE - revisit this issue once we have views.
     *
     * @param referencedTabs    JBitSet with bit map of referenced FromTables
     * @param simplePredsOnly    Whether or not to consider method
     *                            calls, field references and conditional nodes
     *                            when building bit map
     *
     * @return boolean        Whether or not source.expression is a ColumnReference
     *                        or a VirtualColumnNode.
     * @exception StandardException        Thrown on error
     */
    public boolean categorize(JBitSet referencedTabs, boolean simplePredsOnly)
        throws StandardException
    {
        /* We stop here when only considering simple predicates
         *  as we don't consider new opeators when looking
         * for null invariant predicates.
         */
        if (simplePredsOnly)
        {
            return false;
        }

        boolean pushable = true;

        pushable = pushable && super.categorize(referencedTabs, simplePredsOnly);

        return pushable;
    }

    /**
     * Is this class assignable to the specified class?
     * This is useful for the VTI interface where we want to see
     * if the class implements java.sql.ResultSet.
     *
     * @param toClassName    The java class name we want to assign to
     *
     * @return boolean        Whether or not this class is assignable to
     *                        the specified class
     *
     * @exception StandardException        Thrown on error
     */
    protected boolean assignableTo(String toClassName) throws StandardException
    {
        ClassInspector classInspector = getClassFactory().getClassInspector();
        return classInspector.assignableTo(javaClassName, toClassName);
    }


    /**
     * Is this class have a public method with the specified signiture
     * This is useful for the VTI interface where we want to see
     * if the class has the option static method for returning the
     * ResultSetMetaData.
     *
     * @param methodName    The method name we are looking for
     * @param staticMethod    Whether or not the method we are looking for is static
     *
     * @return Member        The Member representing the method (or null
     *                        if the method doesn't exist).
     *
     * @exception StandardException        Thrown on error
     */
    protected Member findPublicMethod(String methodName, boolean staticMethod)
        throws StandardException
    {
        Member publicMethod;

        /*
        ** Get the parameter type names out of the parameters and put them
        ** in an array.
        */
        String[]    parmTypeNames = getObjectSignature();
        boolean[]    isParam = getIsParam();

        ClassInspector classInspector = getClassFactory().getClassInspector();

        try
        {
            publicMethod = classInspector.findPublicMethod(javaClassName, methodName,
                                               parmTypeNames, null, isParam, staticMethod, false);

            /* If no match, then retry to match any possible combinations of
             * object and primitive types.
             */
            if (publicMethod == null)
            {
                String[] primParmTypeNames = getPrimitiveSignature(false);
                publicMethod = classInspector.findPublicMethod(javaClassName,
                                        methodName, parmTypeNames,
                                        primParmTypeNames, isParam, staticMethod, false);
            }
        }
        catch (ClassNotFoundException e)
        {
            /* We should always be able to find the class at this point
             * since the protocol is to check to see if it exists
             * before checking for a method off of it.  Anyway, just return
             * null if the class doesn't exist, since the method doesn't
             * exist in that case.
             */
            if (SanityManager.DEBUG)
            {
                SanityManager.THROWASSERT("Unexpected exception", e);
            }
            return null;
        }

        return    publicMethod;
    }

    /**
     * Do code generation for this method call
     *
     * @param acb    The ExpressionClassBuilder for the class we're generating
     * @param mb    The method the expression will go into
     *
     *
     * @exception StandardException        Thrown on error
     */

    public void generateExpression(ExpressionClassBuilder acb,
                                            MethodBuilder mb)
                                    throws StandardException
    {
        /* If this node is for an ungrouped aggregator,
         * then we generate a conditional
         * wrapper so that we only new the aggregator once.
         *        (fx == null) ? fx = new ... : fx
         */
        LocalField objectFieldLF = null;
        if (singleInstantiation)
        {
            /* Declare the field */
            objectFieldLF = acb.newFieldDeclaration(Modifier.PRIVATE, javaClassName);

            // now we fill in the body of the conditional

            mb.getField(objectFieldLF);
            mb.conditionalIfNull();
        }

        mb.pushNewStart(javaClassName);
        int nargs = generateParameters(acb, mb);
        mb.pushNewComplete(nargs);

        if (singleInstantiation) {

              mb.putField(objectFieldLF);
            mb.startElseCode();
              mb.getField(objectFieldLF);
            mb.completeConditional();
        }
    }
}
