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

import com.splicemachine.db.catalog.TypeDescriptor;
import com.splicemachine.db.catalog.types.RoutineAliasInfo;
import com.splicemachine.db.catalog.types.TypeDescriptorImpl;
import com.splicemachine.db.catalog.types.UserDefinedTypeIdImpl;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.reference.JDBC30Translation;
import com.splicemachine.db.iapi.reference.SQLState;
import com.splicemachine.db.iapi.services.compiler.MethodBuilder;
import com.splicemachine.db.iapi.services.loader.ClassInspector;
import com.splicemachine.db.iapi.services.sanity.SanityManager;
import com.splicemachine.db.iapi.sql.compile.C_NodeTypes;
import com.splicemachine.db.iapi.sql.compile.TypeCompiler;
import com.splicemachine.db.iapi.sql.compile.TypeCompilerFactory;
import com.splicemachine.db.iapi.sql.compile.Visitor;
import com.splicemachine.db.iapi.store.access.Qualifier;
import com.splicemachine.db.iapi.types.DataTypeDescriptor;
import com.splicemachine.db.iapi.types.JSQLType;
import com.splicemachine.db.iapi.types.TypeId;
import com.splicemachine.db.iapi.util.JBitSet;

import java.lang.reflect.Member;
import java.sql.ResultSet;
import java.util.*;

/**
 * A MethodCallNode represents a Java method call.  Method calls can be done
 * through DML (as expressions) or through the CALL statement.
 *
 */

abstract class MethodCallNode extends JavaValueNode
{
    /*
    ** Name of the method.
    */
     String    methodName;

    /** The name of the class containing the method. May not be known until bindExpression() has been called.
     * @see #bindExpression
     * @see #getJavaClassName()
     */
    String javaClassName;

    /**
        For a procedure or function call
    */
    RoutineAliasInfo routineInfo;


    /**
        True if this is an internal call, just used to set up a generated method call.
    */
    boolean internalCall;

    // bound signature of arguments, stated in universal types (JSQLType)
    protected JSQLType[]                signature;

    /*
    ** Parameters to the method, if any.  No elements if no parameters.
    */
    protected JavaValueNode[]    methodParms;

    /* The method call */
    protected Member method;

    protected String actualMethodReturnType;

    /**
        The parameter types for the resolved method.
    */
    String[] methodParameterTypes;

    /**
     * Initializer for a MethodCallNode
     *
     * @param    methodName    The name of the method to call
     */
    public void init(Object methodName)
    {
        this.methodName = (String) methodName;
    }

    public String getMethodName()
    {
        return  methodName;
    }

    /**
     * @return the name of the class that contains the method, null if not known. It may not be known
     *         until this node has been bound.
     */
    public String getJavaClassName()
    {
        return javaClassName;
    }

    /**
     * @return get the Java method or constructor determined during the bind() phase.
     */
    public Member getResolvedMethod()
    {
        return method;
    }

    /**
     * Get the details on the invoked routines.
     */
    public RoutineAliasInfo getRoutineInfo()
    {
        return routineInfo;
    }

    /**
     * Add the parameter list
     *
     * @param parameterList        A list of the parameters
     *
     * @exception StandardException        Thrown on error
     */
    public void addParms(List parameterList) throws StandardException
    {
        methodParms = new JavaValueNode[parameterList.size()];

        int    plSize = parameterList.size();
        for (int index = 0; index < plSize; index++)
        {
            QueryTreeNode    qt;

            qt = (QueryTreeNode) parameterList.get(index);

            /*
            ** Since we need the parameter to be in Java domain format, put a
            ** SQLToJavaValueNode on top of the parameter node if it is a
            ** SQLValueNode. But if the parameter is already in Java domain
            ** format, then we don't need to do anything.
            */
            if ( ! (qt instanceof JavaValueNode))
            {
                qt = (QueryTreeNode) getNodeFactory().getNode(
                        C_NodeTypes.SQL_TO_JAVA_VALUE_NODE,
                        qt,
                        getContextManager());
            }

            methodParms[index] = (JavaValueNode) qt;
        }
    }

    /**
      *    Get the resolved Classes of our parameters
      *
      *    @return    the Classes of our parameters
      */
    public    Class[]    getMethodParameterClasses()
    {
        ClassInspector ci = getClassFactory().getClassInspector();

        Class[]    parmTypeClasses = new Class[methodParms.length];
        for (int i = 0; i < methodParms.length; i++)
        {
            String className = methodParameterTypes[i];
            try
            {
                parmTypeClasses[i] = ci.getClass(className);
            }
            catch (ClassNotFoundException cnfe)
            {
                /* We should never get this exception since we verified
                 * that the classes existed at bind time.  Just return null.
                 */
                if (SanityManager.DEBUG)
                {
                    SanityManager.THROWASSERT("Unexpected exception", cnfe);
                }
                return null;
            }
        }

        return parmTypeClasses;
    }

    /**
     * Build a JBitSet of all of the tables that we are
     * correlated with.
     *
     * @param correlationMap    The JBitSet of the tables that we are correlated with.
     */
    void getCorrelationTables(JBitSet correlationMap)
        throws StandardException
    {
        CollectNodesVisitor getCRs = new CollectNodesVisitor(ColumnReference.class);
        accept(getCRs);
        Vector colRefs = getCRs.getList();
        for (Enumeration e = colRefs.elements(); e.hasMoreElements(); )
        {
            ColumnReference ref = (ColumnReference)e.nextElement();
            if (ref.getCorrelated())
            {
                correlationMap.set(ref.getTableNumber());
            }
        }
    }

    /**
     * Prints the sub-nodes of this object.  See QueryTreeNode.java for
     * how tree printing is supposed to work.
     *
     * @param depth        The depth of this node in the tree
     */

    public void printSubNodes(int depth)
    {
        if (SanityManager.DEBUG)
        {
            int    parm;

            super.printSubNodes(depth);
            if (methodParms != null)
            {
                for (parm = 0; parm < methodParms.length; parm++)
                {
                    if (methodParms[parm] != null)
                    {
                        printLabel(depth, "methodParms[" + parm + "] :");
                        methodParms[parm].treePrint(depth + 1);
                    }
                }
            }
        }
    }

    /**
     * Convert this object to a String.  See comments in QueryTreeNode.java
     * for how this should be done for tree printing.
     *
     * @return    This object as a String
     */

    public String toString()
    {
        if (SanityManager.DEBUG)
        {
            return "methodName: " +
                    (methodName != null ? methodName : "null") + "\n" +
                    super.toString();
        }
        else
        {
            return "";
        }
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
     * @exception StandardException        Thrown on error
     */

    final void bindParameters(FromList fromList,
                              SubqueryList subqueryList,
                              List<AggregateNode>aggregateVector)  throws StandardException {
        /* Bind the parameters */
        if (methodParms != null) {
            int        count = methodParms.length;

            // with a procedure call the signature
            // is preformed in StaticMethodCall from
            // the procedures signature.
            if (signature == null)
                signature = new JSQLType[ count ];

            for (int parm = 0; parm < count; parm++)
            {
                if (methodParms[parm] != null)
                {
                    methodParms[parm] =
                        methodParms[parm].bindExpression(
                            fromList, subqueryList, aggregateVector);

                    if (routineInfo == null)
                        signature[ parm ] = methodParms[ parm ].getJSQLType();
                    
                }
            }
        }
    }

    /**
     * Return whether or not all of the parameters to this node are
     * QUERY_INVARIANT or CONSTANT.  This is useful for VTIs - a VTI is a candidate
     * for materialization if all of its parameters are QUERY_INVARIANT or CONSTANT
     *
     * @return Whether or not all of the parameters to this node are QUERY_INVARIANT or CONSTANT
     * @exception StandardException    thrown on error
     */
     protected boolean areParametersQueryInvariant() throws StandardException
     {
        return (getVariantTypeOfParams() == Qualifier.QUERY_INVARIANT);
     }

    /**
     * Build parameters for error message and throw the exception when there
     * is no matching signature found.
     *
     * @param receiverTypeName    Type name for receiver
     * @param parmTypeNames        Type names for parameters as object types
     * @param primParmTypeNames    Type names for parameters as primitive types
     *
     * @exception StandardException        Thrown on error
     */
    void throwNoMethodFound(String receiverTypeName,
                                      String[] parmTypeNames,
                                      String[] primParmTypeNames)
        throws StandardException
    {
        /* Put the parameter type names into a single string */
        StringBuffer    parmTypes = new StringBuffer();
        for (int i = 0; i < parmTypeNames.length; i++)
        {
            if (i != 0)
                parmTypes.append(", ");
            /* RESOLVE - shouldn't be using hard coded strings for output */
            parmTypes.append( (!parmTypeNames[i].isEmpty() ?
                                parmTypeNames[i] :
                                "UNTYPED"));
            if ((primParmTypeNames != null) &&
                ! primParmTypeNames[i].equals(parmTypeNames[i]))  // has primitive
                parmTypes.append("(").append(primParmTypeNames[i]).append(")");
        }

        throw StandardException.newException(SQLState.LANG_NO_METHOD_FOUND,
                                                receiverTypeName,
                                                methodName,
                                                 parmTypes);
    }

    /**
     * Preprocess an expression tree.  We do a number of transformations
     * here (including subqueries, IN lists, LIKE and BETWEEN) plus
     * subquery flattening.
     * NOTE: This is done before the outer ResultSetNode is preprocessed.
     *
     * @param    numTables            Number of tables in the DML Statement
     * @param    outerFromList        FromList from outer query block
     * @param    outerSubqueryList    SubqueryList from outer query block
     * @param    outerPredicateList    PredicateList from outer query block
     *
     * @exception StandardException        Thrown on error
     */
    public void preprocess(int numTables,
                            FromList outerFromList,
                            SubqueryList outerSubqueryList,
                            PredicateList outerPredicateList)
                    throws StandardException
    {
        int    parm;

        /* Preprocess the parameters */
        if (methodParms != null)
        {
            for (parm = 0; parm < methodParms.length; parm++)
            {
                if (methodParms[parm] != null)
                {
                    methodParms[parm].preprocess(numTables,
                                                 outerFromList,
                                                 outerSubqueryList,
                                                 outerPredicateList);
                }
            }
        }
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
     * @exception StandardException            Thrown on error
     */
    public boolean categorize(JBitSet referencedTabs, boolean simplePredsOnly)
        throws StandardException
    {
        /* We stop here when only considering simple predicates
         *  as we don't consider method calls when looking
         * for null invariant predicates.
         */
        if (simplePredsOnly)
        {
            return false;
        }

        boolean pushable = true;
        int        param;

        if (methodParms != null)
        {
            for (param = 0; param < methodParms.length; param++)
            {
                if (methodParms[param] != null)
                {
                    pushable = methodParms[param].categorize(referencedTabs, simplePredsOnly) &&
                               pushable;
                }
            }
        }

        /* We need to push down method call.  Then the predicate can be used for start/stop
         * key for index scan.  The fact that method call's cost is not predictable and can
         * be expensive doesn't mean we shouldn't push it down. Beetle 4826.
         */
        return pushable;
    }

    /**
     * Remap all ColumnReferences in this tree to be clones of the
     * underlying expression.
     *
     * @return JavaValueNode            The remapped expression tree.
     *
     * @exception StandardException            Thrown on error
     */
    public JavaValueNode remapColumnReferencesToExpressions()
        throws StandardException
    {
        int    param;

        if (methodParms != null)
        {
            for (param = 0; param < methodParms.length; param++)
            {
                if (methodParms[param] != null)
                {
                    methodParms[param] =
                        methodParms[param].remapColumnReferencesToExpressions();
                }
            }
        }
        return this;
    }

    /**
     * Generate the parameters to the given method call
     *
     * @param acb    The ExpressionClassBuilder for the class we're generating
     * @param mb the method  the expression will go into
     *
     * @return    Count of arguments to the method.
     *
     * @exception StandardException        Thrown on error
     */

    public    int generateParameters(ExpressionClassBuilder acb,
                                            MethodBuilder mb)
            throws StandardException
    {
        // Determine whether the method is the Java wrapper method for Python procedure call
        boolean isPy = javaClassName.equals(StaticMethodCallNode.PYROUTINE_WRAPPER_CLASS_NAME) &&
                (method.getName().equals(StaticMethodCallNode.PYPROCEDURE_WRAPPER_METHOD_NAME) ||
                        method.getName().equals(StaticMethodCallNode.PYFUNCTION_WRAPPER_METHOD_NAME));

        int                param;

        String[] expectedTypes = methodParameterTypes;

        ClassInspector classInspector = getClassFactory().getClassInspector();

        /* Generate the code for each user parameter, generating the appropriate
         * cast when the passed type needs to get widened to the expected type.
         */
        for (param = 0; param < methodParms.length; param++)
        {
            if(isPy){
                mb.dup();
            }

            generateOneParameter( acb, mb, param );

            // type from the SQL-J expression
            String argumentType = getParameterTypeName( methodParms[param] );

            // type of the method
            String parameterType = expectedTypes[param];

            if (!parameterType.equals(argumentType))
            {
                // since we reached here through method resolution
                // casts are only required for primitive types.
                // In any other case the expression type must be assignable
                // to the parameter type.
                if (ClassInspector.primitiveType(parameterType)) {
                    mb.cast(parameterType);
                } else {

                    // for a prodcedure
                    if (routineInfo != null) {
                        if(isPy){
                            mb.setArrayElement(param); // Before Continue add the parameter into the Object[]
                        }
                        continue; // probably should be only for INOUT/OUT parameters.
                    }

                    if (SanityManager.DEBUG) {
                        SanityManager.ASSERT(classInspector.assignableTo(argumentType, parameterType),
                            "Argument type " + argumentType + " is not assignable to parameter " + parameterType);
                    }

                    /*
                    ** Set the parameter type in case the argument type is narrower
                    ** than the parameter type.
                    */
                    mb.upCast(parameterType);

                }
            }

            if(isPy){
                mb.setArrayElement(param); // put the element into the array
            }
        }

        return methodParms.length;
    }

    static    public    String    getParameterTypeName( JavaValueNode param )
        throws StandardException
    {
        String    argumentType;

        // RESOLVE - shouldn't this logic be inside JavaValueNode ??
        // I.e. once the value is primitive then its java type name is its
        // primitive type name.
        if (param.isPrimitiveType()) { argumentType = param.getPrimitiveTypeName(); }
        else { argumentType = param.getJavaTypeName(); }

        return    argumentType;
    }

    /**
     * Generate one parameter to the given method call. This method is overriden by
     * RepStaticMethodCallNode.
     *
     * @param acb                The ExpressionClassBuilder for the class we're generating
     * @param mb the method the expression will go into
     * @param parameterNumber    Identifies which parameter to generate. 0 based.
     *
     * @exception StandardException        Thrown on error
     */

    public    void generateOneParameter(ExpressionClassBuilder acb,
                                            MethodBuilder mb,
                                            int parameterNumber )
            throws StandardException
    {
        methodParms[parameterNumber].generateExpression(acb, mb);
    }

    /**
     * Set the appropriate type information for a null passed as a parameter.
     * This method is called after method resolution, when a signature was
     * successfully matched.
     *
     * @param parmTypeNames    String[] with the java type names for the parameters
     *        as declared by the method
     *
     * @exception StandardException        Thrown on error
     */
    public void    setNullParameterInfo(String[] parmTypeNames)
            throws StandardException
    {
        for (int i = 0; i < methodParms.length; i++)
        {
            /* null parameters are represented by a java type name of "" */
            if (methodParms[i].getJavaTypeName().isEmpty())
            {
                /* Set the type information in the null constant node */
                DataTypeDescriptor dts = DataTypeDescriptor.getSQLDataTypeDescriptor(parmTypeNames[i]);
                ((SQLToJavaValueNode)methodParms[i]).value.setType(dts);

                /* Set the correct java type name */
                methodParms[i].setJavaTypeName(parmTypeNames[i]);
                signature[i] = methodParms[i].getJSQLType();
            }
        }
    }

    protected void resolveMethodCall(String javaClassName,
                                     boolean staticMethod)
                throws StandardException
    {
        // only allow direct method calls through routines and internal SQL.
//        if (routineInfo == null && !internalCall)
//        {
//            // See if we are being executed in an internal context
//            if ((getCompilerContext().getReliability() & CompilerContext.INTERNAL_SQL_ILLEGAL) != 0) {
//                throw StandardException.newException(SQLState.LANG_SYNTAX_ERROR,  javaClassName + (staticMethod ? "::" : ".") + methodName);
//            }
//        }

        int            count = signature.length;

        ClassInspector classInspector = getClassFactory().getClassInspector();


        String[]        parmTypeNames;
        String[]        primParmTypeNames = null;
        boolean[]        isParam = getIsParam();

        boolean hasDynamicResultSets = (routineInfo != null) && (count != 0) && (count != methodParms.length);

        /*
        ** Find the matching method that is public.
        */

            int signatureOffset = methodName.indexOf('(');

            // support Java signatures by checking if the method name contains a '('
            if (signatureOffset != -1) {
                   parmTypeNames = parseValidateSignature(methodName, signatureOffset, hasDynamicResultSets);
               methodName = methodName.substring(0, signatureOffset);
               
               // If the signature is specified then Derby resolves to exactly
               // that method. Setting this flag to false disables the method
               // resolution from automatically optionally repeating the last
               // parameter as needed.
               hasDynamicResultSets = false;

            }
            else
            {
                parmTypeNames = getObjectSignature();
            }
        try
        {
                method = classInspector.findPublicMethod(javaClassName,
                                                    methodName,
                                                    parmTypeNames,
                                                    null,
                                                    isParam,
                                                    staticMethod,
                                                    hasDynamicResultSets);


                // DB2 LUW does not support Java object types for SMALLINT, INTEGER, BIGINT, REAL, DOUBLE
                // and these are the only types that can map to a primitive or an object type according
                // to SQL part 13. So we never have a second chance match.
                // Also if the DDL specified a signature, then no alternate resolution
                if (signatureOffset == -1 && routineInfo == null) {

                    /* If no match, then retry with combinations of object and
                     * primitive types.
                     */
                    if (method == null)
                    {
                        primParmTypeNames = getPrimitiveSignature(false);

                        method = classInspector.findPublicMethod(javaClassName,
                                                    methodName,
                                                    parmTypeNames,
                                                    primParmTypeNames,
                                                    isParam,
                                                    staticMethod,
                                                    hasDynamicResultSets);
                    }
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
        /* Throw exception if no matching signature found */
        if (method == null)
        {
            throwNoMethodFound(javaClassName, parmTypeNames, primParmTypeNames);
        }

        boolean isPyFunction = methodName.equals(StaticMethodCallNode.PYFUNCTION_WRAPPER_METHOD_NAME) &&
                javaClassName.equals(StaticMethodCallNode.PYROUTINE_WRAPPER_CLASS_NAME);
        String    typeName = classInspector.getType(method);
        actualMethodReturnType = typeName;
        if(isPyFunction){
            // For Python function, the actual return type is unknown until runtime.
            // Therefore skip the following process and directly set the returnType according
            // to what user declares upon the creation of the function.
            // If the type is incorrect, the user will get Exception thrown at the runtime.
            TypeDescriptorImpl returnType = (TypeDescriptorImpl) routineInfo.getReturnType();
            typeName = TypeId.getBuiltInTypeId(returnType.getJDBCTypeId()).getCorrespondingJavaTypeName();
        }
        else {
            actualMethodReturnType = typeName;

            if (routineInfo == null) {

                /* void methods are only okay for CALL Statements */
                if (typeName.equals("void")) {
                    if (!forCallStatement)
                        throw StandardException.newException(SQLState.LANG_VOID_METHOD_CALL);
                }
            } else {
                String promoteName = null;
                TypeDescriptorImpl returnType = (TypeDescriptorImpl) routineInfo.getReturnType();
                String requiredType;
                if (returnType == null) {
                    // must have a void method for a procedure call.
                    requiredType = "void";
                } else {
                    TypeId returnTypeId = TypeId.getBuiltInTypeId(returnType.getJDBCTypeId());

                    if (
                            returnType.isRowMultiSet() &&
                                    (routineInfo.getParameterStyle() == RoutineAliasInfo.PS_SPLICE_JDBC_RESULT_SET)
                            ) {
                        requiredType = FromVTI.DATASET_PROVIDER;
                    } else if (returnType.getTypeId().userType()) {
                        requiredType = ((UserDefinedTypeIdImpl) returnType.getTypeId()).getClassName();
                    } else {
                        requiredType = returnTypeId.getCorrespondingJavaTypeName();

                        if (!requiredType.equals(typeName)) {
                            switch (returnType.getJDBCTypeId()) {
                                case java.sql.Types.BOOLEAN:
                                case java.sql.Types.SMALLINT:
                                case java.sql.Types.INTEGER:
                                case java.sql.Types.BIGINT:
                                case java.sql.Types.REAL:
                                case java.sql.Types.DOUBLE:
                                    TypeCompiler tc = getTypeCompiler(returnTypeId);
                                    requiredType = tc.getCorrespondingPrimitiveTypeName();
                                    if (!routineInfo.calledOnNullInput() && routineInfo.getParameterCount() != 0) {
                                        promoteName = returnTypeId.getCorrespondingJavaTypeName();
                                    }
                                    break;
                                default:
                                    // Nothing
                            }
                        }
                    }
                }

                boolean foundCorrectType;
                if (ResultSet.class.getName().equals(requiredType)) {
                    // allow subtypes of ResultSet too
                    try {
                        Class actualType = classInspector.getClass(typeName);

                        foundCorrectType = ResultSet.class.isAssignableFrom(actualType);
                    } catch (ClassNotFoundException cnfe) {
                        foundCorrectType = false;
                    }
                } else {
                    foundCorrectType = requiredType.equals(typeName);
                }

                if (!foundCorrectType) {
                    throwNoMethodFound(requiredType + " " + javaClassName, parmTypeNames, primParmTypeNames);
                }

                // for a returns null on null input with a primitive
                // type we need to promote to an object so we can return null.
                if (promoteName != null)
                    typeName = promoteName;
                //propogate collation type from RoutineAliasInfo to
                // MethodCallNode DERBY-2972
                if (routineInfo.getReturnType() != null)
                    setCollationType(routineInfo.getReturnType().getCollationType());
            }
        }
         setJavaTypeName( typeName );

        if(javaClassName.equals(StaticMethodCallNode.PYROUTINE_WRAPPER_CLASS_NAME) &&
                (methodName.equals(StaticMethodCallNode.PYPROCEDURE_WRAPPER_METHOD_NAME) ||
                methodName.equals(StaticMethodCallNode.PYFUNCTION_WRAPPER_METHOD_NAME))){
            // Added for PYTHON stored procedure
            // Since the actual method gets called fo Python Stored Procedure takes Object.. args
            // as its arguments. The method ParameterTypes cannot be directly derived
            // via reflection. Hence it will be manually set.
            int parameterTypesLen = signature.length;
            int maxDynamicResultSets = this.routineInfo.getMaxDynamicResultSets();
            if(maxDynamicResultSets > 1){
                parameterTypesLen += (maxDynamicResultSets - 1);
            }

            methodParameterTypes = new String[parameterTypesLen];
            for(int i = 0; i < this.signature.length; ++i){
                // methodParameterTypes = convert JSQLTYPE to corresponding Java Type
                methodParameterTypes[i] = signature[i].getSQLType().getTypeId().getCorrespondingJavaWrapperTypeName();
            }

            // Add the remaining ResultSet to the parameterTypes array.
            for(int i = this.signature.length; i < parameterTypesLen; ++i){
                methodParameterTypes[i] = signature[this.signature.length-1].getSQLType().getTypeId().getCorrespondingJavaWrapperTypeName();
            }
        }else{
            methodParameterTypes = classInspector.getParameterTypes(method);
        }

        for (int i = 0; i < methodParameterTypes.length; i++)
        {
            String methodParameter = methodParameterTypes[i];

            if (routineInfo != null) {
                if (i < routineInfo.getParameterCount()) {
                    int parameterMode = routineInfo.getParameterModes()[i];

                    switch (parameterMode) {
                    case JDBC30Translation.PARAMETER_MODE_IN:
                        break;
                    case JDBC30Translation.PARAMETER_MODE_IN_OUT:
                        // we need to see if the type of the array is
                        // primitive, not the array itself.
                        methodParameter = methodParameter.substring(0, methodParameter.length() - 2);
                        break;

                    case JDBC30Translation.PARAMETER_MODE_OUT:
                        // value is not obtained *from* parameter.
                        continue;
                    }
                }
            }

            if (ClassInspector.primitiveType(methodParameter))
                methodParms[i].castToPrimitive(true);
        }

        /* Set type info for any null parameters */
        if ( someParametersAreNull() )
        {
            setNullParameterInfo(methodParameterTypes);
        }


    
        /* bug 4450 - if the callable statement is ? = call form, generate the metadata
        infor for the return parameter. We don't really need that info in order to
        execute the callable statement. But with jdbc3.0, this information should be
        made available for return parameter through ParameterMetaData class.
        Parser sets a flag in compilercontext if ? = call. If the flag is set,
        we generate the metadata info for the return parameter and reset the flag
        in the compilercontext for future call statements*/
        DataTypeDescriptor dts = DataTypeDescriptor.getSQLDataTypeDescriptor(typeName);
        if (getCompilerContext().getReturnParameterFlag()) {
            getCompilerContext().getParameterTypes()[0] = dts;
        }
  }

    /**
     * Parse the user supplied signature for a method and validate
     * it, need to match the number of parameters passed in and match
     * the valid types for the parameter.
     * @param offset Character offset of first paren
     * @param hasDynamicResultSets Can ResultSet[] parameters be specified.
     * @return The valid array of types for resolution.
     * @throws StandardException
     */
    private String[] parseValidateSignature(String externalName, int offset,
            boolean hasDynamicResultSets)
        throws StandardException
    {
        int siglen = externalName.length();

        // Ensure the opening paren is not the last
        // character and that the last character is a close paren
        if (((offset + 1) == siglen)
            || (externalName.charAt(siglen - 1) != ')'))
            throw StandardException.newException(SQLState.SQLJ_SIGNATURE_INVALID); // invalid

        StringTokenizer st = new StringTokenizer(externalName.substring(offset + 1, siglen - 1), ",", true);
        
        String[] signatureTypes = new String[signature.length];
        int count;
        boolean seenClass = false;
        for (count = 0; st.hasMoreTokens();)
        {
               String type = st.nextToken().trim();
 
               // check sequence is <class><comma>class> etc.
               if (",".equals(type))
               {
                   if (!seenClass)
                       throw StandardException.newException(SQLState.SQLJ_SIGNATURE_INVALID); // invalid
                   seenClass = false;
                   continue;
               }
               else
               {
                   if (type.isEmpty())
                       throw StandardException.newException(SQLState.SQLJ_SIGNATURE_INVALID); // invalid
                   seenClass = true;
                   count++;
               }

               if (count > signature.length)
            {
                if (hasDynamicResultSets)
                {
                    // Allow any number of dynamic result set holders
                    // but they must match the exact type.
                    String rsType = signature[signature.length - 1].getSQLType().
                        getTypeId().getCorrespondingJavaTypeName();

                    if (!type.equals(rsType))
                        throw StandardException.newException(SQLState.LANG_DATA_TYPE_GET_MISMATCH,
                                type, rsType);

                    if (signatureTypes.length == signature.length)
                    {
                        // expand once
                        String[] sigs = new String[st.countTokens()];
                        System.arraycopy(signatureTypes, 0, sigs, 0, signatureTypes.length);
                        signatureTypes = sigs;
                    }

                    signatureTypes[count - 1] = type;
                    continue;

                }
                throw StandardException.newException(SQLState.SQLJ_SIGNATURE_PARAMETER_COUNT,
                        Integer.toString(count),
                        Integer.toString(signature.length)); // too many types
            }


            TypeId    paramTypeId = signature[count - 1].getSQLType().getTypeId();

            // Does it match the object name
            if (type.equals(paramTypeId.getCorrespondingJavaTypeName()))
            {
                signatureTypes[count - 1] = type;
                continue;
            }

            // how about the primitive name
            if ((paramTypeId.isNumericTypeId() && !paramTypeId.isDecimalTypeId())
                    || paramTypeId.isBooleanTypeId())
            {
                TypeCompiler tc = getTypeCompiler(paramTypeId);
                if (type.equals(tc.getCorrespondingPrimitiveTypeName()))
                {
                       signatureTypes[count - 1] = type;
                    continue;
                }
            }
            throw StandardException.newException(SQLState.LANG_DATA_TYPE_GET_MISMATCH,
                        type, paramTypeId.getSQLTypeName()); // type conversion error
        }
        
        // Did signature end with trailing comma?
        if (count != 0 && !seenClass)
            throw StandardException.newException(SQLState.SQLJ_SIGNATURE_INVALID); // invalid
        
        if (count < signatureTypes.length)
        {
            if (hasDynamicResultSets)
            {
                // we can tolerate a count of one less than the
                // expected count, which means the procedure is declared
                // to have dynamic result sets, but the explict signature
                // doesn't have any ResultSet[] types.
                // So accept, and procedure will automatically have 0
                // dynamic results at runtime
                if (count == (signature.length - 1))
                {
                    String[] sigs = new String[count];
                    System.arraycopy(signatureTypes, 0, sigs, 0, count);
                    return sigs;
                }
            }
            throw StandardException.newException(SQLState.SQLJ_SIGNATURE_PARAMETER_COUNT,
                    Integer.toString(count),
                    Integer.toString(signature.length)); // too few types
        }

        return signatureTypes;
    }

    /**
      *    Return true if some parameters are null, false otherwise.
      */
    protected    boolean    someParametersAreNull()
    {
        for (JSQLType aSignature : signature) {
            if (aSignature == null) {
                return true;
            }
        }

        return false;
    }

    /**
      *    Build an array of names of the argument types. These types are biased toward
      *    Java objects. That is, if an argument is of SQLType, then we map it to the
      *    corresponding Java synonym class (e.g., SQLINT is mapped to 'java.lang.Integer').
      *
      *
      *    @return    array of type names
      *
      * @exception StandardException        Thrown on error
      */
    protected    String[]    getObjectSignature( )
        throws StandardException
    {
        int        count = signature.length;
        String    parmTypeNames[] = new String[ count ];

        TypeCompilerFactory tcf = (routineInfo == null ) ? null : getCompilerContext().getTypeCompilerFactory();

        for ( int i = 0; i < count; i++ ) { parmTypeNames[i] = getObjectTypeName( signature[ i ], tcf ); }

        return parmTypeNames;
    }

    /**
     * Build an array of booleans denoting whether or not a given method
     * parameter is a ?.
     *
     * @return array of booleans denoting wheter or not a given method
     * parameter is a ?.
     */
    protected boolean[] getIsParam()
    {
        if (methodParms == null)
        {
            return new boolean[0];
        }

        boolean[] isParam = new boolean[methodParms.length];

        for (int index = 0; index < methodParms.length; index++)
        {
            if (methodParms[index] instanceof SQLToJavaValueNode)
            {
                SQLToJavaValueNode stjvn = (SQLToJavaValueNode) methodParms[index];
                if (stjvn.value.requiresTypeFromContext())
                {
                    isParam[index] = true;
                }
            }
        }

        return isParam;
    }

    static  String    getObjectTypeName( JSQLType jsqlType, TypeCompilerFactory tcf )
        throws StandardException
    {
        if ( jsqlType != null )
        {
            switch( jsqlType.getCategory() )
            {
                case JSQLType.SQLTYPE:

                    TypeId    ctid = mapToTypeID( jsqlType );

                    if ( ctid == null ) { return null; }
                    else {
                        // DB2 LUW does not support Java object types for SMALLINT, INTEGER, BIGINT, REAL, DOUBLE
                        // and these are the only types that can map to a primitive or an object type according
                        // to SQL part 13. So always map to the primitive type. We can not use the getPrimitiveSignature()
                        // as it (incorrectly but historically always has) maps a DECIMAL to a double.

                        switch (ctid.getJDBCTypeId()) {
                        case java.sql.Types.BOOLEAN:
                        case java.sql.Types.SMALLINT:
                        case java.sql.Types.INTEGER:
                        case java.sql.Types.BIGINT:
                        case java.sql.Types.REAL:
                        case java.sql.Types.DOUBLE:
                            if (tcf != null) {
                                return tcf.getTypeCompiler( ctid ).getCorrespondingPrimitiveTypeName();
                            }
                            // fall through
                        default:
                            return ctid.getCorrespondingJavaTypeName();
                        }
                    }

                case JSQLType.JAVA_CLASS: return jsqlType.getJavaClassName();

                case JSQLType.JAVA_PRIMITIVE: return JSQLType.getPrimitiveName( jsqlType.getPrimitiveKind() );

                default:

                    if (SanityManager.DEBUG)
                    { SanityManager.THROWASSERT( "Unknown JSQLType: " + jsqlType ); }

            }
        }

        return "";
    }

    String[]    getPrimitiveSignature( boolean castToPrimitiveAsNecessary )
        throws StandardException
    {
        int                    count = signature.length;
        String[]             primParmTypeNames = new String[ count ];
        JSQLType            jsqlType;

        for (int i = 0; i < count; i++)
        {
            jsqlType = signature[ i ];

            if ( jsqlType == null ) { primParmTypeNames[i] = ""; }
            else
            {
                switch( jsqlType.getCategory() )
                {
                    case JSQLType.SQLTYPE:

                        TypeId    ctid = mapToTypeID( jsqlType );

                        assert ctid != null;
                        if ((ctid.isNumericTypeId() && !ctid.isDecimalTypeId()) || ctid.isBooleanTypeId())
                        {
                            TypeCompiler tc = getTypeCompiler(ctid);
                            primParmTypeNames[i] = tc.getCorrespondingPrimitiveTypeName();
                            if ( castToPrimitiveAsNecessary) { methodParms[i].castToPrimitive(true); }
                        }
                        else { primParmTypeNames[i] = ctid.getCorrespondingJavaTypeName(); }

                        break;

                    case JSQLType.JAVA_CLASS:

                        primParmTypeNames[i] = jsqlType.getJavaClassName();
                        break;

                    case JSQLType.JAVA_PRIMITIVE:

                        primParmTypeNames[i] = JSQLType.getPrimitiveName( jsqlType.getPrimitiveKind() );
                        if ( castToPrimitiveAsNecessary) { methodParms[i].castToPrimitive(true); }
                        break;

                    default:

                        if (SanityManager.DEBUG)
                            { SanityManager.THROWASSERT( "Unknown JSQLType: " + jsqlType ); }

                }    // end switch

            }        // end if

        }            // end for

        return primParmTypeNames;
    }

    /**
     * Return the variant type for the underlying expression.
     * The variant type can be:
     *        VARIANT                - variant within a scan
     *                              (non-static field access)
     *        SCAN_INVARIANT        - invariant within a scan
     *                              (column references from outer tables)
     *        QUERY_INVARIANT        - invariant within the life of a query
     *                              (constant expressions)
     *
     * @return    The variant type for the underlying expression.
     */
    protected int getOrderableVariantType() throws StandardException
    {
        // beetle 4880. We return the most variant type of the parameters. If no
        // params then query-invariant. This makes more sense, and we can evaluate
        // only once per query (good for performance) because method call could be
        // expensive.  And if we push down method qualifier to store, language
        // can pre-evaluate the method call.  This avoids letting store evaluate
        // the method while holding page latch, causing deadlock.

        return getVariantTypeOfParams();
    }

    private int getVariantTypeOfParams() throws StandardException
    {
        int variance = Qualifier.QUERY_INVARIANT;

        if (methodParms != null)
        {
            for (JavaValueNode methodParm : methodParms) {
                if (methodParm != null) {
                    int paramVariantType =
                            methodParm.getOrderableVariantType();
                    if (paramVariantType < variance)    //return the most variant type
                        variance = paramVariantType;
                } else {
                    variance = Qualifier.VARIANT;
                }
            }
        }

        return variance;
    }

    /**
     * Override method in ancestor.
     */
    public DataTypeDescriptor getDataType() throws StandardException
    {
        if ( routineInfo != null )
        {
            TypeDescriptor td = routineInfo.getReturnType();

            if ( td != null ) { return DataTypeDescriptor.getType( td ); }
        }

        return super.getDataType();
    }


    /////////////////////////////////////////////////////////////////////
    //
    //    ACCESSORS
    //
    /////////////////////////////////////////////////////////////////////
    /**
     * Get the method parameters.
     *
     * @return    The method parameters
     */
    public JavaValueNode[]    getMethodParms()
    {
        return methodParms;
    }

    /**
     * Accept the visitor for all visitable children of this node.
     *
     * @param v the visitor
     */
    @Override
    public void acceptChildren(Visitor v) throws StandardException {
        super.acceptChildren(v);

        for (int parm = 0;
            !v.stopTraversal() && parm < methodParms.length;
            parm++)
        {
            if (methodParms[parm] != null)
            {
                methodParms[parm] = (JavaValueNode)methodParms[parm].accept(v, this);
            }
        }
    }

    @Override
    public List<? extends QueryTreeNode> getChildren() {
        return Arrays.asList(methodParms);
    }

    @Override
    public QueryTreeNode getChild(int index) {
        return methodParms[index];
    }

    @Override
    public void setChild(int index, QueryTreeNode newValue) {
        methodParms[index] = (JavaValueNode) newValue;
    }

    /**
     * Add the one single parameter
     *
     * @param newParm        The new parameter to be appended to methodParms
     *
     * @exception StandardException        Thrown on error
     */
    public void addOneParm(QueryTreeNode newParm) throws StandardException
    {
        JavaValueNode[] newMethodParms = new JavaValueNode[methodParms.length + 1];

        QueryTreeNode qt = (QueryTreeNode) newParm;

        /*
         ** Since we need the parameter to be in Java domain format, put a
         ** SQLToJavaValueNode on top of the parameter node if it is a
         ** SQLValueNode. But if the parameter is already in Java domain
         ** format, then we don't need to do anything.
         */
        if ( ! (qt instanceof JavaValueNode))
        {
            qt = (QueryTreeNode) getNodeFactory().getNode(
                    C_NodeTypes.SQL_TO_JAVA_VALUE_NODE,
                    qt,
                    getContextManager());
        }
        for(int i = 0; i < methodParms.length; ++i){
            newMethodParms[i] = methodParms[i];
        }

        newMethodParms[methodParms.length] = (JavaValueNode) qt;
        methodParms = newMethodParms;
    }

    @Override
    public boolean isConstantOrParameterTreeNode() {
        for (JavaValueNode methodParm : methodParms) {
            if (!methodParm.isConstantOrParameterTreeNode())
                return false;
        }
        return true;
    }
}
