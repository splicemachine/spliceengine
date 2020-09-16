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

import com.splicemachine.db.catalog.types.BaseTypeIdImpl;
import com.splicemachine.db.catalog.types.TypeDescriptorImpl;
import com.splicemachine.db.iapi.services.compiler.MethodBuilder;
import com.splicemachine.db.iapi.services.context.ContextManager;

import com.splicemachine.db.iapi.services.io.StoredFormatIds;
import com.splicemachine.db.iapi.services.sanity.SanityManager;

import com.splicemachine.db.iapi.sql.compile.CompilerContext;
import com.splicemachine.db.iapi.sql.compile.C_NodeTypes;
import com.splicemachine.db.iapi.types.JSQLType;
import com.splicemachine.db.iapi.types.DataTypeDescriptor;
import com.splicemachine.db.iapi.types.StringDataValue;
import com.splicemachine.db.iapi.types.TypeId;

import com.splicemachine.db.iapi.sql.dictionary.AliasDescriptor;
import com.splicemachine.db.iapi.sql.dictionary.SchemaDescriptor;

import com.splicemachine.db.iapi.reference.ClassName;
import com.splicemachine.db.iapi.reference.SQLState;
import com.splicemachine.db.iapi.reference.JDBC30Translation;
import com.splicemachine.db.iapi.error.StandardException;

import com.splicemachine.db.iapi.services.compiler.LocalField;

import com.splicemachine.db.iapi.util.JBitSet;
import com.splicemachine.db.iapi.services.classfile.VMOpcode;

import com.splicemachine.db.iapi.sql.conn.Authorizer;

import com.splicemachine.db.catalog.AliasInfo;
import com.splicemachine.db.catalog.TypeDescriptor;
import com.splicemachine.db.catalog.types.RoutineAliasInfo;

import java.sql.Types;
import java.util.List;
import java.lang.reflect.Modifier;

/**
 * A StaticMethodCallNode represents a static method call from a Class
 * (as opposed to from an Object).

   For a procedure the call requires that the arguments be ? parameters.
   The parameter is *logically* passed into the method call a number of different ways.

   <P>
   For a application call like CALL MYPROC(?) the logically Java method call is
   (in psuedo Java/SQL code) (examples with CHAR(10) parameter)
   <BR>
   Fixed length IN parameters - com.acme.MyProcedureMethod(?)
   <BR>
   Variable length IN parameters - com.acme.MyProcedureMethod(CAST (? AS CHAR(10))
   <BR>
   Fixed length INOUT parameter -
        String[] holder = new String[] {?}; com.acme.MyProcedureMethod(holder); ? = holder[0]
   <BR>
   Variable length INOUT parameter -
        String[] holder = new String[] {CAST (? AS CHAR(10)}; com.acme.MyProcedureMethod(holder); ? = CAST (holder[0] AS CHAR(10))

   <BR>
   Fixed length OUT parameter -
        String[] holder = new String[1]; com.acme.MyProcedureMethod(holder); ? = holder[0]

   <BR>
   Variable length INOUT parameter -
        String[] holder = new String[1]; com.acme.MyProcedureMethod(holder); ? = CAST (holder[0] AS CHAR(10))


    <P>
    For static method calls there is no pre-definition of an IN or INOUT parameter, so a call to CallableStatement.registerOutParameter()
    makes the parameter an INOUT parameter, provided:
        - the parameter is passed directly to the method call (no casts or expressions).
        - the method's parameter type is a Java array type.

    Since this is a dynmaic decision we compile in code to take both paths, based upon a boolean isINOUT which is dervied from the
    ParameterValueSet. Code is logically (only single parameter String[] shown here). Note, no casts can exist here.

    boolean isINOUT = getParameterValueSet().getParameterMode(0) == PARAMETER_IN_OUT;
    if (isINOUT) {
        String[] holder = new String[] {?}; com.acme.MyProcedureMethod(holder); ? = holder[0]

    } else {
        com.acme.MyProcedureMethod(?)
    }

 *
 */
public class StaticMethodCallNode extends MethodCallNode {
    public static final String PYROUTINE_WRAPPER_CLASS_NAME = "com.splicemachine.derby.utils.PyRoutineWrapper";
    public static final String PYPROCEDURE_WRAPPER_METHOD_NAME = "pyProcedureWrapper";
    public static final String PYFUNCTION_WRAPPER_METHOD_NAME = "pyFunctionWrapper";

    private TableName procedureName;

    private LocalField[] outParamArrays;
    private int[]         applicationParameterNumbers;

    private boolean        isSystemCode;
    private boolean        alreadyBound;
    private boolean     systemFunction = false;

    /**
     * Generated boolean field to hold the indicator
     * for if any of the parameters to a
     * RETURNS NULL ON NULL INPUT function are NULL.
     * Only set if this node is calling such a function.
     * Set at generation time.
     */
    private LocalField    returnsNullOnNullState;

    /**
     * Authorization id of user owning schema in which routine is defined.
     */
    private String routineDefiner = null;

    AliasDescriptor    ad;

    private AggregateNode   resolvedAggregate;
    private boolean appearsInGroupBy = false;
    /**
     * Intializer for a NonStaticMethodCallNode
     *
     * @param methodName        The name of the method to call
     * @param javaClassName        The name of the java class that the static method belongs to.
     */
    public void init(Object methodName, Object javaClassName)
    {
        if (methodName instanceof String)
            init(methodName);
        else {
            procedureName = (TableName) methodName;
            init(procedureName.getTableName());
        }

        this.javaClassName = (String) javaClassName;
    }
    /**
     * Get the aggregate, if any, which this method call resolves to.
     */
    public  AggregateNode   getResolvedAggregate() { return resolvedAggregate; }

    /** Flag that this function invocation appears in a GROUP BY clause */
    public  void    setAppearsInGroupBy() { appearsInGroupBy = true; }

    public void setIsSystemFunction(boolean newVal) { systemFunction = newVal; }
    public boolean isSystemFunction() { return systemFunction; }

    /**
     * Bind this expression.  This means binding the sub-expressions,
     * as well as figuring out what the return type is for this expression.
     *
     * @param fromList        The FROM list for the query this
     *                expression is in, for binding columns.
     * @param subqueryList        The subquery list being built as we find SubqueryNodes
     * @param aggregateVector    The aggregate vector being built as we find AggregateNodes
     *
     * @return    this or an AggregateNode
     *
     * @exception StandardException        Thrown on error
     */

    public JavaValueNode bindExpression(FromList fromList,
                                        SubqueryList subqueryList,
                                        List<AggregateNode> aggregateVector) throws StandardException {
        // for a function we can get called recursively
        /*if (alreadyBound)
            return this;*/


        bindParameters(fromList, subqueryList, aggregateVector);


        /* If javaClassName is null then we assume that the current methodName
         * is an alias and we must go to sysmethods to
         * get the real method and java class names for this alias.
         */
        if (javaClassName == null)
        {
            CompilerContext cc = getCompilerContext();

            // look for a routine

            String schemaName = procedureName.getSchemaName();

            boolean noSchema = schemaName == null;

            SchemaDescriptor sd = getSchemaDescriptor(schemaName, schemaName != null);

            // The field methodName is used by resolveRoutine and
            // is set to the name of the routine (procedureName.getTableName()).
            resolveRoutine(fromList, subqueryList, aggregateVector, sd);

            // (Splice)    This logic, to implicitly check for routine using SYSFUN schema,
            // was moved here from a few lines down, so that it has a chance to find
            // aggregate functions in SYSFUN catalog too, such that it will get
            // a resolvedAggregate. There was no clean way to avoid this Derby fork,
            // because there's no clean way to tell the parser to use a Splice override
            // of StaticMethodCallNode.

            if (ad == null && noSchema && !forCallStatement)
            {
                // Resolve to a built-in SYSFUN function but only
                // if this is a function call and the call
                // was not qualified. E.g. COS(angle). The
                // SYSFUN functions are not in SYSALIASES but
                // an in-memory table, set up in DataDictionaryImpl.
                sd = getSchemaDescriptor("SYSFUN", true);

                resolveRoutine(fromList, subqueryList, aggregateVector, sd);
                setIsSystemFunction(true);
            }

            if ( (ad != null) && (ad.getAliasType() == AliasInfo.ALIAS_TYPE_AGGREGATE_AS_CHAR) )
            {
                resolvedAggregate = (AggregateNode) getNodeFactory().getNode
                                     (
                                     C_NodeTypes.AGGREGATE_NODE,
                                     ((SQLToJavaValueNode) methodParms[ 0 ]).getSQLValueNode(),
                                     new UserAggregateDefinition( ad ),
                                     Boolean.FALSE,
                                     ad.getJavaClassName(),
                                     getContextManager()
                                     );
                                // The parser may have noticed that this aggregate is invoked in a
                                // GROUP BY clause. That is not allowed.
                                if ( appearsInGroupBy )
                                {
                                    throw StandardException.newException(SQLState.LANG_AGGREGATE_IN_GROUPBY_LIST);
                                }

                return this;
            }

            /* Throw exception if no routine found */
            if (ad == null)
            {
                throw StandardException.newException(
                        SQLState.LANG_NO_SUCH_METHOD_ALIAS, procedureName);
            }

            if ( !routineInfo.isDeterministic() )
            {
                checkReliability( getMethodName(), CompilerContext.NON_DETERMINISTIC_ILLEGAL );
            }
            if ( permitsSQL( routineInfo ) )
            {
                checkReliability( getMethodName(), CompilerContext.SQL_IN_ROUTINES_ILLEGAL );
            }



                cc.createDependency(ad);


            methodName = ad.getAliasInfo().getMethodName();
            javaClassName = ad.getJavaClassName();

            // DERBY-2330 Do not allow a routine to resolve to
            // a Java method that is part of the Derby runtime code base.
            // This is a security measure to stop user-defined routines
            // bypassing security by making calls directly to Derby's
            // internal methods. E.g. open a table's conglomerate
            // directly and read the file, bypassing any authorization.
            // This is a simpler mechanism than analyzing all of
            // Derby's public static methods and ensuring they have
            // no Security holes.
            if (javaClassName.startsWith("com.splicemachine.db."))
            {
                if (!sd.isSystemSchema())
                    throw StandardException.newException(
                        SQLState.LANG_TYPE_DOESNT_EXIST2, (Throwable) null,
                        javaClassName);
            }
        }

        verifyClassExist(javaClassName);

        /* Resolve the method call */
        resolveMethodCall(javaClassName, true);


        if (isPrivilegeCollectionRequired())
            getCompilerContext().addRequiredRoutinePriv(ad);

        // If this is a function call with a variable length
        // return type, then we need to push a CAST node.
        if (routineInfo != null)
        {
            if (methodParms != null)
                optimizeDomainValueConversion();

            TypeDescriptor returnType = routineInfo.getReturnType();

            // create type dependency if return type is an ANSI UDT
            if ( returnType != null ) { createTypeDependency( DataTypeDescriptor.getType( returnType ) ); }

            if ( returnType != null && !returnType.isRowMultiSet() && !returnType.isUserDefinedType() )
            {
                if (alreadyBound) return this;
                alreadyBound = true;
                TypeId returnTypeId = TypeId.getBuiltInTypeId(returnType.getJDBCTypeId());

                if (returnTypeId.variableLength()) {
                    // Cast the return using a cast node, but have to go
                    // into the SQL domain, and back to the Java domain.

                    DataTypeDescriptor returnValueDtd = new DataTypeDescriptor(
                                returnTypeId,
                                returnType.getPrecision(),
                                returnType.getScale(),
                                returnType.isNullable(),
                                returnType.getMaximumWidth()
                            );


                    ValueNode returnValueToSQL = (ValueNode) getNodeFactory().getNode(
                                C_NodeTypes.JAVA_TO_SQL_VALUE_NODE,
                                this,
                                getContextManager());

                    ValueNode returnValueCastNode = (ValueNode) getNodeFactory().getNode(
                                    C_NodeTypes.CAST_NODE,
                                    returnValueToSQL,
                                    returnValueDtd,
                                    getContextManager());

                    // DERBY-2972  Match the collation of the RoutineAliasInfo
                    returnValueCastNode.setCollationInfo(
                            returnType.getCollationType(),
                            StringDataValue.COLLATION_DERIVATION_IMPLICIT);


                    JavaValueNode returnValueToJava = (JavaValueNode) getNodeFactory().getNode(
                                        C_NodeTypes.SQL_TO_JAVA_VALUE_NODE,
                                        returnValueCastNode,
                                        getContextManager());
                    returnValueToJava.setCollationType(returnType.getCollationType());
                    return returnValueToJava.bindExpression(fromList, subqueryList, aggregateVector);
                }

            }
        }

        return this;
    }

    /**
     * Returns true if the routine permits SQL.
     */
    private boolean permitsSQL( RoutineAliasInfo rai )
    {
        short       sqlAllowed = rai.getSQLAllowed();

        switch( sqlAllowed )
        {
        case RoutineAliasInfo.MODIFIES_SQL_DATA:
        case RoutineAliasInfo.READS_SQL_DATA:
        case RoutineAliasInfo.CONTAINS_SQL:
            return true;

        default:    return false;
        }
    }

    /**
     * If this SQL function has parameters which are SQLToJavaValueNode over
     * JavaToSQLValueNode and the java value node underneath is a SQL function
     * defined with CALLED ON NULL INPUT, then we can get rid of the wrapper
     * nodes over the java value node for such parameters. This is because
     * SQL functions defined with CALLED ON NULL INPUT need access to only
     * java domain values.
     * This can't be done for parameters which are wrappers over SQL function
     * defined with RETURN NULL ON NULL INPUT because such functions need
     * access to both sql domain value and java domain value. - Derby479
     * This optimization is not available if the outer function is
     * RETURN NULL ON NULL INPUT. That is because the SQLToJavaNode is
     * responsible for compiling the byte code which skips the method call if
     * the parameter is null--if we remove the SQLToJavaNode, then we don't
     * compile that check and we get bug DERBY-1030.
     */
    private void optimizeDomainValueConversion() throws StandardException {

        //
        // This optimization is not possible if we are compiling a call to
        // a NULL ON NULL INPUT method. See DERBY-1030 and the header
        // comment above.
        //
        if ( !routineInfo.calledOnNullInput() ) { return; }

        int count = methodParms == null ? 0 : methodParms.length;
        for (int parm = 0; parm < count; parm++)
        {
            //
            // We also skip the optimization if the argument must be cast to a primitive. In this case we need
            // a runtime check to make sure that the argument is not null. See DERBY-4459.
            //
            if ( methodParms[ parm ].mustCastToPrimitive() ) { continue; }

            if (methodParms[parm] instanceof SQLToJavaValueNode &&
                ((SQLToJavaValueNode)methodParms[parm]).getSQLValueNode() instanceof
                JavaToSQLValueNode)
            {
                //If we are here, then it means that the parameter is
                //SQLToJavaValueNode on top of JavaToSQLValueNode
                JavaValueNode paramIsJavaValueNode =
                    ((JavaToSQLValueNode)((SQLToJavaValueNode)methodParms[parm]).getSQLValueNode()).getJavaValueNode();
                if (paramIsJavaValueNode instanceof StaticMethodCallNode)
                {
                    //If we are here, then it means that the parameter has
                    //a MethodCallNode underneath it.
                    StaticMethodCallNode paramIsMethodCallNode = (StaticMethodCallNode)paramIsJavaValueNode;
                    //If the MethodCallNode parameter is defined as
                    //CALLED ON NULL INPUT, then we can remove the wrappers
                    //for the param and just set the parameter to the
                    //java value node.
                    if (paramIsMethodCallNode.routineInfo != null &&
                            paramIsMethodCallNode.routineInfo.calledOnNullInput())
                        methodParms[parm] =
                            ((JavaToSQLValueNode)((SQLToJavaValueNode)methodParms[parm]).getSQLValueNode()).getJavaValueNode();
                }
            }
        }
    }

    /**
     * Resolve a routine. Obtain a list of routines from the data dictionary
     * of the correct type (functions or procedures) and name.
     * Pick the best routine from the list. Currently only a single routine
     * with a given type and name is allowed, thus if changes are made to
     * support overloaded routines, careful code inspection and testing will
     * be required.
     * @param fromList
     * @param subqueryList
     * @param aggregateVector
     * @param sd
     * @throws StandardException
     */
    private void resolveRoutine(FromList fromList,
                                SubqueryList subqueryList,
                                List<AggregateNode> aggregateVector,
                                SchemaDescriptor sd) throws StandardException {
        if (sd.getUUID() != null) {

            List<AliasDescriptor> list = getDataDictionary().getRoutineList(
                    sd.getUUID().toString(), methodName,
                    forCallStatement ? AliasInfo.ALIAS_NAME_SPACE_PROCEDURE_AS_CHAR : AliasInfo.ALIAS_NAME_SPACE_FUNCTION_AS_CHAR
            );

            for (int i = list.size() - 1; i >= 0; i--) {

                AliasDescriptor proc = list.get(i);

                RoutineAliasInfo routineInfo = (RoutineAliasInfo) proc.getAliasInfo();
                int parameterCount = routineInfo.getParameterCount();
                if (parameterCount != methodParms.length)
                    continue;

                // pre-form the method signature. If it is a dynamic result set procedure
                // then we need to add in the ResultSet array

                TypeDescriptor[] parameterTypes = routineInfo.getParameterTypes();

                int sigParameterCount = parameterCount;
                if (routineInfo.getMaxDynamicResultSets() > 0)
                    sigParameterCount++; // When the method has ResultSet, increment the sigParameterCount by one.

                signature = new JSQLType[sigParameterCount];
                for (int p = 0; p < parameterCount; p++) {

                    // find the declared type.

                    TypeDescriptor td = parameterTypes[p];

                    TypeId typeId = TypeId.getTypeId(td);

                    TypeId parameterTypeId = typeId;


                    // if it's an OUT or INOUT parameter we need an array.
                    int parameterMode = routineInfo.getParameterModes()[p];

                    if (parameterMode != JDBC30Translation.PARAMETER_MODE_IN) {

                        String arrayType;
                        switch (typeId.getJDBCTypeId()) {
                            case java.sql.Types.BOOLEAN:
                            case java.sql.Types.SMALLINT:
                            case java.sql.Types.INTEGER:
                            case java.sql.Types.BIGINT:
                            case java.sql.Types.REAL:
                            case java.sql.Types.DOUBLE:
                                arrayType = getTypeCompiler(typeId).getCorrespondingPrimitiveTypeName() + "[]";
                                break;
                            default:
                                arrayType = typeId.getCorrespondingJavaTypeName() + "[]";
                                break;
                        }

                        typeId = TypeId.getUserDefinedTypeId(arrayType, false);
                    }

                    // this is the type descriptor of the require method parameter
                    DataTypeDescriptor methoddtd = new DataTypeDescriptor(
                            typeId,
                            td.getPrecision(),
                            td.getScale(),
                            td.isNullable(),
                            td.getMaximumWidth()
                    );

                    signature[p] = new JSQLType(methoddtd);

                    // check parameter is a ? node for INOUT and OUT parameters.

                    ValueNode sqlParamNode = null;

                    if (methodParms[p] instanceof SQLToJavaValueNode) {
                        SQLToJavaValueNode sql2j = (SQLToJavaValueNode) methodParms[p];
                        sqlParamNode = sql2j.getSQLValueNode();
                    }

                    boolean isParameterMarker = true;
                    if ((sqlParamNode == null) || !sqlParamNode.requiresTypeFromContext()) {
                        if (parameterMode != JDBC30Translation.PARAMETER_MODE_IN) {

                            throw StandardException.newException(SQLState.LANG_DB2_PARAMETER_NEEDS_MARKER,
                                    RoutineAliasInfo.parameterMode(parameterMode),
                                    routineInfo.getParameterNames()[p]);
                        }
                        isParameterMarker = false;
                    } else {
                        if (applicationParameterNumbers == null)
                            applicationParameterNumbers = new int[parameterCount];
                        if (sqlParamNode instanceof UnaryOperatorNode) {
                            ParameterNode pn = ((UnaryOperatorNode) sqlParamNode).getParameterOperand();
                            applicationParameterNumbers[p] = pn.getParameterNumber();
                        } else
                            applicationParameterNumbers[p] = ((ParameterNode) sqlParamNode).getParameterNumber();
                    }

                    // this is the SQL type of the procedure parameter.
                    DataTypeDescriptor paramdtd = new DataTypeDescriptor(
                            parameterTypeId,
                            td.getPrecision(),
                            td.getScale(),
                            td.isNullable(),
                            td.getMaximumWidth()
                    );

                    boolean needCast = false;
                    if (!isParameterMarker) {

                        // can only be an IN parameter.
                        // check that the value can be assigned to the
                        // type of the procedure parameter.
                        if (sqlParamNode instanceof UntypedNullConstantNode) {
                            sqlParamNode.setType(paramdtd);
                        } else {


                            DataTypeDescriptor dts;
                            TypeId argumentTypeId;

                            if (sqlParamNode != null) {
                                // a node from the SQL world
                                argumentTypeId = sqlParamNode.getTypeId();
                                dts = sqlParamNode.getTypeServices();
                            } else {
                                // a node from the Java world
                                dts = DataTypeDescriptor.getSQLDataTypeDescriptor(methodParms[p].getJavaTypeName());
                                if (dts == null) {
                                    throw StandardException.newException(SQLState.LANG_NO_CORRESPONDING_S_Q_L_TYPE,
                                            methodParms[p].getJavaTypeName());
                                }

                                argumentTypeId = dts.getTypeId();
                            }

                            if (!getTypeCompiler(parameterTypeId).storable(argumentTypeId, getClassFactory()))
                                throw StandardException.newException(SQLState.LANG_NOT_STORABLE,
                                        parameterTypeId.getSQLTypeName(),
                                        argumentTypeId.getSQLTypeName());

                            // if it's not an exact length match then some cast will be needed.
                            if (!paramdtd.isExactTypeAndLengthMatch(dts))
                                needCast = true;
                        }
                    } else {
                        // any variable length type will need a cast from the
                        // Java world (the ? parameter) to the SQL type. This
                        // ensures values like CHAR(10) are passed into the procedure
                        // correctly as 10 characters long.
                        if (parameterTypeId.variableLength()) {

                            if (parameterMode != JDBC30Translation.PARAMETER_MODE_OUT)
                                needCast = true;
                        }
                    }


                    if (needCast) {
                        // push a cast node to ensure the
                        // correct type is passed to the method
                        // this gets tacky because before we knew
                        // it was a procedure call we ensured all the
                        // parameter are JavaNodeTypes. Now we need to
                        // push them back to the SQL domain, cast them
                        // and then push them back to the Java domain.

                        if (sqlParamNode == null) {

                            sqlParamNode = (ValueNode) getNodeFactory().getNode(
                                    C_NodeTypes.JAVA_TO_SQL_VALUE_NODE,
                                    methodParms[p],
                                    getContextManager());
                        }

                        ValueNode castNode = makeCast
                                (
                                        sqlParamNode,
                                        paramdtd,
                                        getContextManager()
                                );


                        methodParms[p] = (JavaValueNode) getNodeFactory().getNode(
                                C_NodeTypes.SQL_TO_JAVA_VALUE_NODE,
                                castNode,
                                getContextManager());

                        methodParms[p] = methodParms[p].bindExpression(fromList, subqueryList, aggregateVector);
                    }

                    // only force the type for a ? so that the correct type shows up
                    // in parameter meta data
                    if (isParameterMarker)
                        sqlParamNode.setType(paramdtd);
                }

                if (sigParameterCount != parameterCount) {

                    TypeId typeId = TypeId.getUserDefinedTypeId("java.sql.ResultSet[]", false);

                    DataTypeDescriptor dtd = new DataTypeDescriptor(
                            typeId,
                            0,
                            0,
                            false,
                            -1
                    );

                    signature[parameterCount] = new JSQLType(dtd);

                }

                this.routineInfo = routineInfo;
                ad = proc;

                // If a procedure is in the system schema and defined as executing
                // SQL do we set we are in system code.
                if (sd.isSystemSchema() && (routineInfo.getReturnType() == null) && routineInfo.getSQLAllowed() != RoutineAliasInfo.NO_SQL)
                    isSystemCode = true;

                routineDefiner = sd.getAuthorizationId();

                break;
            }

            // If the resolved StaticMethodCallNode has the LANGUAGE PYTHON,
            // it will resolves in calling method pyProcedureWrapper
            // The parameters, and retruen type for these two Stored Procedures are the same
            if(this.routineInfo!=null && this.routineInfo.getLanguage() != null &&this.routineInfo.getLanguage().equals("PYTHON"))
            {

                // determine whether the routine is for a procedure or function
                boolean isFunction = !forCallStatement;
                this.methodName = isFunction?PYFUNCTION_WRAPPER_METHOD_NAME:PYPROCEDURE_WRAPPER_METHOD_NAME;
                // need to add compiled Python code bytes into signature and methodParms
                byte[] compiledPyCode = this.routineInfo.getCompiledPyCode();

                // Update the parameterCnt ,parameterModes, parameterNames, parameterTypes
                int origParmCnt = this.routineInfo.getParameterCount();
                int updatedParmCnt = origParmCnt + 1;

                int[] updatedParmModes = new int[updatedParmCnt];
                String[] updatedParmNames = new String[updatedParmCnt];
                TypeDescriptor[] updatedParmTypes = new TypeDescriptor[updatedParmCnt];
                if(origParmCnt > 0) {
                    System.arraycopy(this.routineInfo.getParameterModes(), 0, updatedParmModes,0,origParmCnt);
                    System.arraycopy(this.routineInfo.getParameterNames(), 0, updatedParmNames,0,origParmCnt);
                    System.arraycopy(this.routineInfo.getParameterTypes(), 0, updatedParmTypes, 0, origParmCnt);
                }
                updatedParmModes[origParmCnt] = 1; // States the input script is an IN parameter
                // Here Need to use a unique name
                updatedParmNames[origParmCnt] = "PYCODE";
                updatedParmTypes[origParmCnt] = new TypeDescriptorImpl(new BaseTypeIdImpl(StoredFormatIds.BLOB_TYPE_ID_IMPL),
                        true,
                        compiledPyCode.length);

                // update the routineInfo and the AliasDescriptor
                this.routineInfo = new RoutineAliasInfo(this.methodName,
                        "JAVA",
                        updatedParmCnt,
                        updatedParmNames,
                        updatedParmTypes,
                        updatedParmModes,
                        this.routineInfo.getMaxDynamicResultSets(),
                        this.routineInfo.getParameterStyle(),
                        this.routineInfo.getSQLAllowed(),
                        this.routineInfo.isDeterministic(),
                        this.routineInfo.hasDefinersRights(),
                        this.routineInfo.calledOnNullInput(),
                        this.routineInfo.getReturnType(),
                        null);

                this.ad = new AliasDescriptor(this.ad.getDataDictionary(),
                        ad.getUUID(),
                        ad.getName(),
                        ad.getSchemaUUID(),
                        PYROUTINE_WRAPPER_CLASS_NAME,
                        ad.getAliasType(),
                        ad.getNameSpace(),
                        ad.getSystemAlias(),
                        routineInfo,
                        ad.getSpecificName());

                // Update signature by adding in the script String's corresponding JSQLType
                JSQLType pyCodeType = new JSQLType(DataTypeDescriptor.getBuiltInDataTypeDescriptor(TypeId.BLOB_NAME));
                JSQLType[] updatedSigs = new JSQLType[signature.length+1];
                // Adding the Pycode parameter right before the first occurance of the ResultSet[]
                // If no ResultSet[] exists, appending Pycode at the end of all the existing parameters
                int j = 0;
                for(int i = 0; i < signature.length+1; i++){
                    if(i<signature.length && signature[i].getSQLType().getTypeId().getCorrespondingJavaTypeName().equals("java.sql.ResultSet[]")){
                        updatedSigs[i] = pyCodeType;
                    }
                    else{
                        if(j == signature.length){
                            updatedSigs[i] = pyCodeType;
                        }
                        else{
                            updatedSigs[i] = signature[j];
                            j++;
                        }
                    }
                }
                this.signature = updatedSigs;
                ContextManager cm = getContextManager();
                CompilerContext cc = getCompilerContext();
                QueryTreeNode pyCodeNode = null;
                String hexCompiledPyCodeStr = com.splicemachine.db.iapi.util.StringUtil.toHexString(compiledPyCode,0,compiledPyCode.length);
                try{

                    pyCodeNode = (QueryTreeNode) cc.getNodeFactory().getNode(C_NodeTypes.BLOB_CONSTANT_NODE,
                            hexCompiledPyCodeStr, compiledPyCode.length,cm);
                    pyCodeNode = (QueryTreeNode) cc.getNodeFactory().getNode(C_NodeTypes.SQL_TO_JAVA_VALUE_NODE, pyCodeNode,cm);
                } catch(Exception e){
                    // Fill in Exception Handling
                    throw StandardException.plainWrapException(e);
                }
                // Update methodParms by adding in the script String's corresponding JSQLType
                addOneParm((JavaValueNode)pyCodeNode);
            }

            if ( (ad == null) && (methodParms.length == 1) )
            {
                ad = AggregateNode.resolveAggregate( getDataDictionary(), sd, methodName );
            }
        }
    }
        /**
         * Wrap a parameter in a CAST node.
         */
        public static ValueNode makeCast (ValueNode parameterNode,
                                          DataTypeDescriptor targetType,
                                          ContextManager cm)
                throws StandardException
        {
            ValueNode castNode = new CastNode(parameterNode, targetType, cm);

            // Argument type has the same semantics as assignment:
            // Section 9.2 (Store assignment). There, General Rule
            // 2.b.v.2 says that the database should raise an exception
            // if truncation occurs when stuffing a string value into a
            // VARCHAR, so make sure CAST doesn't issue warning only.
            ((CastNode)castNode).setAssignmentSemantics();

            return castNode;
        }


    /**
     * Add code to set up the SQL session context for a stored
     * procedure or function which needs a nested SQL session
     * context (only needed for those which can contain SQL).
     *
     * The generated code calls setupNestedSessionContext.
     * @see com.splicemachine.db.iapi.sql.conn.LanguageConnectionContext#setupNestedSessionContext
     *
     * @param acb activation class builder
     * @param mb  method builder
     */
    private void generateSetupNestedSessionContext(
        ActivationClassBuilder acb,
        MethodBuilder mb,
        boolean hadDefinersRights,
        String definer) throws StandardException {

        // Generates the following Java code:
        // ((Activation)this).getLanguageConnectionContext().
        //       setupNestedSessionContext((Activation)this);

        acb.pushThisAsActivation(mb);
        mb.callMethod(VMOpcode.INVOKEINTERFACE, null,
                      "getLanguageConnectionContext",
                      ClassName.LanguageConnectionContext, 0);
        acb.pushThisAsActivation(mb);
        mb.push(hadDefinersRights);
        mb.push(definer);
        mb.callMethod(VMOpcode.INVOKEINTERFACE, null,
                      "setupNestedSessionContext",
                      "void", 3);
    }


    /**
        Push extra code to generate the casts within the
        arrays for the parameters passed as arrays.
    */
    public    void generateOneParameter(ExpressionClassBuilder acb,
                                            MethodBuilder mb,
                                            int parameterNumber )
            throws StandardException
    {
        int parameterMode;

        SQLToJavaValueNode sql2j = null;
        if (methodParms[parameterNumber] instanceof SQLToJavaValueNode)
            sql2j = (SQLToJavaValueNode) methodParms[parameterNumber];

        if (routineInfo != null) {
            parameterMode = routineInfo.getParameterModes()[parameterNumber];
        } else {
            // for a static method call the parameter always starts out as a in parameter, but
            // may be registered as an IN OUT parameter. For a static method argument to be
            // a dynmaically registered out parameter it must be a simple ? parameter

            parameterMode = JDBC30Translation.PARAMETER_MODE_IN;

            if (sql2j != null) {
                if (sql2j.getSQLValueNode().requiresTypeFromContext()) {
                      ParameterNode pn;
                      if (sql2j.getSQLValueNode() instanceof UnaryOperatorNode)
                          pn = ((UnaryOperatorNode)sql2j.getSQLValueNode()).getParameterOperand();
                      else
                          pn = (ParameterNode) (sql2j.getSQLValueNode());

                    // applicationParameterNumbers is only set up for a procedure.
                    int applicationParameterNumber = pn.getParameterNumber();

                    String parameterType = methodParameterTypes[parameterNumber];

                    if (parameterType.endsWith("[]")) {

                        // constructor  - setting up correct paramter type info
                        MethodBuilder constructor = acb.getConstructor();
                        acb.pushThisAsActivation(constructor);
                        constructor.callMethod(VMOpcode.INVOKEINTERFACE, null,
                                            "getParameterValueSet", ClassName.ParameterValueSet, 0);

                        constructor.push(applicationParameterNumber);
                        constructor.push(JDBC30Translation.PARAMETER_MODE_UNKNOWN);
                        constructor.callMethod(VMOpcode.INVOKEINTERFACE, null,
                                            "setParameterMode", "void", 2);
                        constructor.endStatement();
                    }
                }
            }
        }

        switch (parameterMode) {
        case JDBC30Translation.PARAMETER_MODE_IN:
        case JDBC30Translation.PARAMETER_MODE_IN_OUT:
        case JDBC30Translation.PARAMETER_MODE_UNKNOWN:
            if (sql2j != null)
                sql2j.returnsNullOnNullState = returnsNullOnNullState;
            super.generateOneParameter(acb, mb, parameterNumber);
            break;

        case JDBC30Translation.PARAMETER_MODE_OUT:
            // For an OUT parameter we require nothing to be pushed into the
            // method call from the parameter node.
            break;
        }

        switch (parameterMode) {
        case JDBC30Translation.PARAMETER_MODE_IN:
        case JDBC30Translation.PARAMETER_MODE_UNKNOWN:
            break;

        case JDBC30Translation.PARAMETER_MODE_IN_OUT:
        case JDBC30Translation.PARAMETER_MODE_OUT:
        {
            // Create the array used to pass into the method. We create a
            // new array for each call as there is a small chance the
            // application could retain a reference to it and corrupt
            // future calls with the same CallableStatement object.

            String methodParameterType = methodParameterTypes[parameterNumber];
            String arrayType = methodParameterType.substring(0, methodParameterType.length() - 2);
            LocalField lf = acb.newFieldDeclaration(Modifier.PRIVATE, methodParameterType);

            if (outParamArrays == null)
                outParamArrays = new LocalField[methodParms.length];

            outParamArrays[parameterNumber] = lf;
            mb.pushNewArray(arrayType, 1);
            mb.putField(lf);

            // set the IN part of the parameter into the INOUT parameter.
            if (parameterMode != JDBC30Translation.PARAMETER_MODE_OUT) {
                mb.swap();
                mb.setArrayElement(0);
                mb.getField(lf);
            }
            break;
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
     *
     * @exception StandardException        Thrown on error
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

        pushable = pushable && super.categorize(referencedTabs, simplePredsOnly);

        return pushable;
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
            return "javaClassName: " +
                (javaClassName != null ? javaClassName : "null") + "\n" +
                super.toString();
        }
        else
        {
            return "";
        }
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
        boolean isPy = false;
        if(routineInfo != null &&
                (getMethodName().equals(StaticMethodCallNode.PYPROCEDURE_WRAPPER_METHOD_NAME) ||
                getMethodName().equals(StaticMethodCallNode.PYFUNCTION_WRAPPER_METHOD_NAME))&&
                getJavaClassName().equals(StaticMethodCallNode.PYROUTINE_WRAPPER_CLASS_NAME)){
            isPy = true;
        }

        if(isPy){
            int arrayLen = methodParameterTypes.length;
            // construct Object[] which will be passed as an argument to PythonProcedure wrapper static method
            mb.pushNewArray("java.lang.Object",arrayLen);
        }

        if (routineInfo != null) {

            if (!routineInfo.calledOnNullInput() && routineInfo.getParameterCount() != 0)
                returnsNullOnNullState = acb.newFieldDeclaration(Modifier.PRIVATE, "boolean");

        }

        // reset the parameters are null indicator.
        if (returnsNullOnNullState != null) {
            mb.push(false);
            mb.setField(returnsNullOnNullState);

            // for the call to the generated method below.
            mb.pushThis();
        }

        int nargs;

        nargs = generateParameters(acb, mb);


        LocalField functionEntrySQLAllowed = null;

        if (routineInfo != null && acb instanceof ActivationClassBuilder) {

            short sqlAllowed = routineInfo.getSQLAllowed();

            // Before we set up our authorization level, add a check to see if this
            // method can be called. If the routine is NO SQL or CONTAINS SQL
            // then there is no need for a check. As follows:
            //
            // Current Level = NO_SQL - CALL will be rejected when getting CALL result set
            // Current Level = anything else - calls to procedures defined as NO_SQL and CONTAINS SQL both allowed.


            if (sqlAllowed != RoutineAliasInfo.NO_SQL)
            {

                int sqlOperation;

                if (sqlAllowed == RoutineAliasInfo.READS_SQL_DATA)
                    sqlOperation = Authorizer.SQL_SELECT_OP;
                else if (sqlAllowed == RoutineAliasInfo.MODIFIES_SQL_DATA)
                    sqlOperation = Authorizer.SQL_WRITE_OP;
                else
                    sqlOperation = Authorizer.SQL_ARBITARY_OP;

                generateAuthorizeCheck((ActivationClassBuilder) acb, mb, sqlOperation);
            }

            int statmentContextReferences = isSystemCode ? 2 : 1;

            boolean isFunction = routineInfo.getReturnType() != null;

            if (isFunction)
                statmentContextReferences++;


            if (statmentContextReferences != 0) {
                acb.pushThisAsActivation(mb);
                mb.callMethod(VMOpcode.INVOKEINTERFACE, null,
                                    "getLanguageConnectionContext", ClassName.LanguageConnectionContext, 0);
                mb.callMethod(VMOpcode.INVOKEINTERFACE, null,
                                    "getStatementContext", "com.splicemachine.db.iapi.sql.conn.StatementContext", 0);

                for (int scc = 1; scc < statmentContextReferences; scc++)
                    mb.dup();
            }

            /**
                Set the statement context to reflect we are running
                System procedures, so that we can execute non-standard SQL.
            */
            if (isSystemCode) {
                mb.callMethod(VMOpcode.INVOKEINTERFACE, null,
                                    "setSystemCode", "void", 0);
            }

            // If no SQL, there is no need to setup a nested session
            // context.
            if (sqlAllowed != RoutineAliasInfo.NO_SQL) {
                generateSetupNestedSessionContext(
                    (ActivationClassBuilder) acb,
                    mb,
                    routineInfo.hasDefinersRights(),
                    routineDefiner);
            }

            // for a function we need to fetch the current SQL control
            // so that we can reset it once the function is complete.
            //
            if (isFunction)
            {
                functionEntrySQLAllowed = acb.newFieldDeclaration(Modifier.PRIVATE, "short");
                mb.callMethod(VMOpcode.INVOKEINTERFACE, null,
                                    "getSQLAllowed", "short", 0);
                mb.setField(functionEntrySQLAllowed);

            }


            // Set up the statement context to reflect the
            // restricted SQL execution allowed by this routine.

            mb.push(sqlAllowed);
            mb.push(false);
            mb.callMethod(VMOpcode.INVOKEINTERFACE, null,
                                "setSQLAllowed", "void", 2);

        }

        // add in the ResultSet arrays.
        if (routineInfo != null) {

            int compiledResultSets = methodParameterTypes.length - methodParms.length;

            if (compiledResultSets != 0) {

                // Add a method that indicates the maxium number of dynamic result sets.
                int maxDynamicResults = routineInfo.getMaxDynamicResultSets();
                if (maxDynamicResults > 0) {
                    MethodBuilder gdr = acb.getClassBuilder().newMethodBuilder(Modifier.PUBLIC, "int", "getMaxDynamicResults");
                    gdr.push(maxDynamicResults);
                    gdr.methodReturn();
                    gdr.complete();
                }

                // add a method to return all the dynamic result sets (unordered)
                MethodBuilder gdr = acb.getClassBuilder().newMethodBuilder(Modifier.PUBLIC, "java.sql.ResultSet[][]", "getDynamicResults");

                MethodBuilder cons = acb.getConstructor();
                // if (procDef.getParameterStyle() == RoutineAliasInfo.PS_JAVA)
                {
                    // PARAMETER STYLE JAVA

                    LocalField procedureResultSetsHolder = acb.newFieldDeclaration(Modifier.PRIVATE, "java.sql.ResultSet[][]");

                    // getDynamicResults body
                    gdr.getField(procedureResultSetsHolder);

                    // create the holder of all the ResultSet arrays, new java.sql.ResultSet[][compiledResultSets]
                    cons.pushNewArray("java.sql.ResultSet[]", compiledResultSets);
                    cons.setField(procedureResultSetsHolder);


                    // arguments for the dynamic result sets
                    for (int i = 0; i < compiledResultSets; i++) {
                        if(isPy){
                            mb.dup(); // Duplicate the Object[] array
                        }

                        mb.pushNewArray("java.sql.ResultSet", 1);
                        mb.dup();

                        mb.getField(procedureResultSetsHolder);
                        mb.swap();

                        mb.setArrayElement(i);

                        if(isPy){
                            mb.setArrayElement(i+nargs); // Set the Object[] array's element
                        }
                    }
                }

                // complete the method that returns the ResultSet[][] to the
                gdr.methodReturn();
                gdr.complete();

                nargs += compiledResultSets;
            }

        }

        String javaReturnType = getJavaTypeName();

        MethodBuilder mbnc = null;
        MethodBuilder mbcm = mb;


        // If any of the parameters are null then
        // do not call the method, just return null.
        if (returnsNullOnNullState != null)
        {
            mbnc = acb.newGeneratedFun(javaReturnType, Modifier.PRIVATE, methodParameterTypes);

            // add the throws clause for the public static method we are going to call.
            Class[] throwsSet = ((java.lang.reflect.Method) method).getExceptionTypes();
            for (Class aThrowsSet : throwsSet) {
                mbnc.addThrownException(aThrowsSet.getName());
            }

            mbnc.getField(returnsNullOnNullState);
            mbnc.conditionalIf();

            // set up for a null!!
            // for objects is easy.
            mbnc.pushNull(javaReturnType);

            mbnc.startElseCode();

            if (!actualMethodReturnType.equals(javaReturnType))
                mbnc.pushNewStart(javaReturnType);

            // fetch all the arguments
            for (int pa = 0; pa < nargs; pa++)
            {
                mbnc.getParameter(pa);
            }

            mbcm = mbnc;
        }

        if(isPy){
            // The Python Java wrapper method only takes in one argument
            mbcm.callMethod(VMOpcode.INVOKESTATIC, method.getDeclaringClass().getName(), methodName,
                    actualMethodReturnType, 1);
            // for function, casting the result type
            if(!forCallStatement){
                DataTypeDescriptor returntd = DataTypeDescriptor.getType(routineInfo.getReturnType());
                //The actual return type for the wrapper function is Object
                // Hence need to cast the result to the desired return type upon return
                int returnJDBCTypeId =  returntd.getJDBCTypeId();
                switch (returnJDBCTypeId){
                    case(Types.CLOB):
                        mb.cast("java.lang.String");
                        break;
                    case(Types.REAL):
                    case(Types.DOUBLE):
                    case(Types.FLOAT):
                        // Although FLOAT can be mapped to java.lang.Double and java.lang.Float
                        // In the implementation, it is mapped to java.lang.Double, which does not
                        // cause loss of precision. Accordingly, here FLOAT is cast to java.lang.Double
                        mb.cast("java.lang.Double");
                        break;
                    case(Types.INTEGER):
                        mb.cast("java.lang.Integer");
                        break;
                    case(Types.BIGINT):
                        mb.cast("java.lang.Long");
                        break;
                    case(Types.DECIMAL):
                    case(Types.NUMERIC):
                        mb.cast("java.math.BigDecimal");
                        break;
                    default:
                        mb.cast(javaReturnType);
                        break;
                }
            }
        }
        else{
            mbcm.callMethod(VMOpcode.INVOKESTATIC, method.getDeclaringClass().getName(), methodName,
                    actualMethodReturnType, nargs);
        }


        if (returnsNullOnNullState != null)
        {
            // DERBY-3360. In the case of function returning
            // a SMALLINT if we specify RETURN NULL ON NULL INPUT
            // the javaReturnType will be java.lang.Integer. In
            // order to initialize the integer properly, we need
            // to upcast the short.  This is a special case for
            // SMALLINT functions only as other types are
            // compatible with their function return types.
            if (!actualMethodReturnType.equals(javaReturnType)) {
                if (actualMethodReturnType.equals("short") &&
                        javaReturnType.equals("java.lang.Integer"))
                    mbnc.upCast("int");

                mbnc.pushNewComplete(1);
            }
            mbnc.completeConditional();

            mbnc.methodReturn();
            mbnc.complete();

            // now call the wrapper method
            mb.callMethod(VMOpcode.INVOKEVIRTUAL, acb.getClassBuilder().getFullName(), mbnc.getName(),
                    javaReturnType, nargs);
            mbnc = null;
        }


        if (routineInfo != null && acb instanceof ActivationClassBuilder) {

            // reset the SQL allowed setting that we set upon
            // entry to the method.
            if (functionEntrySQLAllowed != null) {
                acb.pushThisAsActivation(mb);
                mb.callMethod(VMOpcode.INVOKEINTERFACE, null,
                                    "getLanguageConnectionContext", ClassName.LanguageConnectionContext, 0);
                mb.callMethod(VMOpcode.INVOKEINTERFACE, null,
                                    "getStatementContext", "com.splicemachine.db.iapi.sql.conn.StatementContext", 0);
                mb.getField(functionEntrySQLAllowed);
                mb.push(true); // override as we are ending the control set by this function all.
                mb.callMethod(VMOpcode.INVOKEINTERFACE, null,
                                    "setSQLAllowed", "void", 2);

            }

            if (outParamArrays != null) {

                MethodBuilder constructor = acb.getConstructor();

                // constructor  - setting up correct paramter type info
                acb.pushThisAsActivation(constructor);
                constructor.callMethod(VMOpcode.INVOKEINTERFACE, null,
                                    "getParameterValueSet", ClassName.ParameterValueSet, 0);

                // execute  - passing out parameters back.
                acb.pushThisAsActivation(mb);
                mb.callMethod(VMOpcode.INVOKEINTERFACE, null,
                                    "getParameterValueSet", ClassName.ParameterValueSet, 0);

                int[] parameterModes = routineInfo.getParameterModes();
                for (int i = 0; i < outParamArrays.length; i++) {

                    int parameterMode = parameterModes[i];
                    if (parameterMode != JDBC30Translation.PARAMETER_MODE_IN) {

                        // must be a parameter if it is INOUT or OUT.
                        ValueNode sqlParamNode = ((SQLToJavaValueNode) methodParms[i]).getSQLValueNode();


                        int applicationParameterNumber = applicationParameterNumbers[i];

                        // Set the correct parameter nodes in the ParameterValueSet at constructor time.
                        constructor.dup();
                        constructor.push(applicationParameterNumber);
                        constructor.push(parameterMode);
                        constructor.callMethod(VMOpcode.INVOKEINTERFACE, null,
                                        "setParameterMode", "void", 2);

                        // Pass the value of the outparameters back to the calling code
                        LocalField lf = outParamArrays[i];

                        mb.dup();
                        mb.push(applicationParameterNumber);
                        mb.callMethod(VMOpcode.INVOKEINTERFACE, null,
                                    "getParameter", ClassName.DataValueDescriptor, 1);

                        // see if we need to set the desired length/scale/precision of the type
                        DataTypeDescriptor paramdtd = sqlParamNode.getTypeServices();

                        boolean isNumericType = paramdtd.getTypeId().isNumericTypeId();
                        boolean isAnsiUDT = paramdtd.getTypeId().getBaseTypeId().isAnsiUDT();

                        // is the underlying type for the OUT/INOUT parameter primitive.
                        // Here it is using reflection to get the type of the parameter,
                        // Which cannot be applied properly to the Wrapper method where the parameter is an array of Object
                        boolean isPrimitive;
                        if(isPy){
                            isPrimitive = false;
                        }
                        else{
                            isPrimitive = ((java.lang.reflect.Method) method).getParameterTypes()[i].getComponentType().isPrimitive();
                        }

                        if (isNumericType) {
                            // need to up-cast as the setValue(Number) method only exists on NumberDataValue

                            if (!isPrimitive)
                                mb.cast(ClassName.NumberDataValue);
                        }
                        else if (paramdtd.getTypeId().isBooleanTypeId())
                        {
                            // need to cast as the setValue(Boolean) method only exists on BooleanDataValue
                            if (!isPrimitive)
                                mb.cast(ClassName.BooleanDataValue);
                        }

                        if (paramdtd.getTypeId().variableLength()) {
                            // need another DVD reference for the set width below.
                            mb.dup();
                        }


                        mb.getField(lf); // pvs, dvd, array
                        mb.getArrayElement(0); // pvs, dvd, value

                        // The value needs to be set thorugh the setValue(Number) method.
                        if (isNumericType && !isPrimitive)
                        {
                            mb.upCast("java.lang.Number");
                        }

                        // The value needs to be set thorugh the setValue(Object) method.
                        if (isAnsiUDT)
                        {
                            mb.upCast("java.lang.Object");
                        }

                        mb.callMethod(VMOpcode.INVOKEINTERFACE, null, "setValue", "void", 1);

                        if (paramdtd.getTypeId().variableLength()) {
                            mb.push(isNumericType ? paramdtd.getPrecision() : paramdtd.getMaximumWidth());
                            mb.push(paramdtd.getScale());
                            mb.push(isNumericType);
                            mb.callMethod(VMOpcode.INVOKEINTERFACE, ClassName.VariableSizeDataValue, "setWidth", "void", 3);
                            // mb.endStatement();
                        }
                    }
                }
                constructor.endStatement();
                mb.endStatement();
            }

        }
    }

    /**
     * Set default privilege of EXECUTE for this node.
     */
    int getPrivType()
    {
        return Authorizer.EXECUTE_PRIV;
    }
}
