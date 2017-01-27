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

package com.splicemachine.db.impl.sql.compile;

import com.splicemachine.db.catalog.AliasInfo;
import com.splicemachine.db.catalog.TypeDescriptor;
import com.splicemachine.db.catalog.types.AggregateAliasInfo;
import com.splicemachine.db.catalog.types.RoutineAliasInfo;
import com.splicemachine.db.catalog.types.SynonymAliasInfo;
import com.splicemachine.db.catalog.types.UDTAliasInfo;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.reference.JDBC40Translation;
import com.splicemachine.db.iapi.reference.Limits;
import com.splicemachine.db.iapi.reference.SQLState;
import com.splicemachine.db.iapi.services.context.ContextManager;
import com.splicemachine.db.iapi.services.sanity.SanityManager;
import com.splicemachine.db.iapi.sql.dictionary.AliasDescriptor;
import com.splicemachine.db.iapi.sql.dictionary.SchemaDescriptor;
import com.splicemachine.db.iapi.sql.execute.ConstantAction;
import com.splicemachine.db.iapi.types.DataTypeDescriptor;
import com.splicemachine.db.iapi.types.TypeId;

import java.util.List;
import java.util.Vector;

/**
 * A CreateAliasNode represents a CREATE ALIAS statement.
 */

public class CreateAliasNode extends DDLStatementNode{
    // indexes into routineElements
    public static final int PARAMETER_ARRAY=0;
    public static final int TABLE_NAME=PARAMETER_ARRAY+1;
    public static final int DYNAMIC_RESULT_SET_COUNT=TABLE_NAME+1;
    public static final int LANGUAGE=DYNAMIC_RESULT_SET_COUNT+1;
    public static final int EXTERNAL_NAME=LANGUAGE+1;
    public static final int PARAMETER_STYLE=EXTERNAL_NAME+1;
    public static final int SQL_CONTROL=PARAMETER_STYLE+1;
    public static final int DETERMINISTIC=SQL_CONTROL+1;
    public static final int NULL_ON_NULL_INPUT=DETERMINISTIC+1;
    public static final int RETURN_TYPE=NULL_ON_NULL_INPUT+1;
    public static final int ROUTINE_SECURITY_DEFINER=RETURN_TYPE+1;

    // Keep ROUTINE_ELEMENT_COUNT last (determines set cardinality).
    // Note: Remember to also update the map ROUTINE_CLAUSE_NAMES in
    // sqlgrammar.jj when elements are added.
    public static final int ROUTINE_ELEMENT_COUNT=
            ROUTINE_SECURITY_DEFINER+1;

    //
    // These are the names of 1-arg builtin functions which are represented in the
    // grammar as non-reserved keywords. These names may not be used as
    // the unqualified names of user-defined aggregates.
    //
    // If additional 1-arg builtin functions are added to the grammar, they should
    // be put in this table.
    //
    private static final String[] NON_RESERVED_FUNCTION_NAMES=
            {
                    "ABS",
                    "ABSVAL",
                    "DATE",
                    "DAY",
                    "LCASE",
                    "LENGTH",
                    "MONTH",
                    "SQRT",
                    "TIME",
                    "TIMESTAMP",
                    "UCASE",
            };
    //
    // These are aggregate names defined by the SQL Standard which do not
    // behave as reserved keywords in Derby.
    //
    private static final String[] NON_RESERVED_AGGREGATES=
            {
                    "COLLECT",
                    "COUNT",
                    "EVERY",
                    "FUSION",
                    "INTERSECTION",
                    "STDDEV_POP",
                    "STDDEV_SAMP",
                    "VAR_POP",
                    "VAR_SAMP",
            };

    // aggregate arguments
    public static final int AGG_FOR_TYPE=0;
    public static final int AGG_RETURN_TYPE=AGG_FOR_TYPE+1;
    public static final int AGG_ELEMENT_COUNT=AGG_RETURN_TYPE+1;

    private String javaClassName;
    private String methodName;
    private char aliasType;
    private boolean delimitedIdentifier;

    private AliasInfo aliasInfo;

    /**
     * Constructor
     *
     * @param aliasName         The name of the alias
     * @param targetObject      Target name string or, if
     *                          aliasType == ALIAS_TYPE_SYNONYM_AS_CHAR, a TableName
     * @param methodName        The method name
     * @param aliasSpecificInfo An array of objects, see code for
     *                          interpretation
     * @param cm                The context manager
     * @throws StandardException Thrown on error
     */
    public CreateAliasNode(){
    }

    public CreateAliasNode(TableName aliasName,
                           Object targetObject,
                           String methodName,
                           Object aliasSpecificInfo,
                           char aliasType,
                           ContextManager cm)
            throws StandardException{
        super(aliasName,cm);
        init(aliasName,targetObject,methodName,
                aliasSpecificInfo,new Character(aliasType),(Object)Boolean.FALSE);
    }


    /**
     * Initializer for a CreateAliasNode
     *
     * @param aliasName           The name of the alias
     * @param targetObject        Target name
     * @param methodName          The method name
     * @param aliasType           The alias type
     * @param delimitedIdentifier Whether or not to treat the class name
     *                            as a delimited identifier if trying to
     *                            resolve it as a class alias
     * @throws StandardException Thrown on error
     */
    public void init(
            Object aliasName,
            Object targetObject,
            Object methodName,
            Object aliasSpecificInfo,
            Object aliasType,
            Object delimitedIdentifier)
            throws StandardException{
        TableName qn=(TableName)aliasName;
        this.aliasType=((Character)aliasType).charValue();

        initAndCheck(qn);

        switch(this.aliasType){
            case AliasInfo.ALIAS_TYPE_AGGREGATE_AS_CHAR:
                this.javaClassName=(String)targetObject;

                Object[] aggElements=(Object[])aliasSpecificInfo;
                TypeDescriptor aggForType=bindUserCatalogType((TypeDescriptor)aggElements[AGG_FOR_TYPE]);
                TypeDescriptor aggReturnType=bindUserCatalogType((TypeDescriptor)aggElements[AGG_RETURN_TYPE]);
                // XML not allowed because SQLXML support has not been implemented
                if(
                        (aggForType.getJDBCTypeId()==JDBC40Translation.SQLXML) ||
                                (aggReturnType.getJDBCTypeId()==JDBC40Translation.SQLXML)
                        ){
                    throw StandardException.newException(SQLState.LANG_XML_NOT_ALLOWED_DJRS);
                }

                aliasInfo=new AggregateAliasInfo(aggForType,aggReturnType);
                implicitCreateSchema=true;
                break;

            case AliasInfo.ALIAS_TYPE_UDT_AS_CHAR:
                this.javaClassName=(String)targetObject;
                aliasInfo=new UDTAliasInfo();

                implicitCreateSchema=true;
                break;

            case AliasInfo.ALIAS_TYPE_PROCEDURE_AS_CHAR:
            case AliasInfo.ALIAS_TYPE_FUNCTION_AS_CHAR:{
                this.javaClassName=(String)targetObject;
                this.methodName=(String)methodName;
                this.delimitedIdentifier=
                        ((Boolean)delimitedIdentifier).booleanValue();

                //routineElements contains the description of the procedure.
                //
                // 0 - Object[] 3 element array for parameters
                // 1 - TableName - specific name
                // 2 - Integer - dynamic result set count
                // 3 - String language (always java) - ignore
                // 4 - String external name (also passed directly to create alias node - ignore
                // 5 - Integer parameter style
                // 6 - Short - SQL control
                // 7 - Boolean - whether the routine is DETERMINISTIC
                // 8 - Boolean - CALLED ON NULL INPUT (always TRUE for procedures)
                // 9 - TypeDescriptor - return type (always NULL for procedures)

                Object[] routineElements=(Object[])aliasSpecificInfo;
                Object[] parameters=(Object[])routineElements[PARAMETER_ARRAY];
                int paramCount=((Vector)parameters[0]).size();

                String[] names=null;
                TypeDescriptor[] types=null;
                int[] modes=null;

                if(paramCount>Limits.DB2_MAX_PARAMS_IN_STORED_PROCEDURE)
                    throw StandardException.newException(SQLState.LANG_TOO_MANY_PARAMETERS_FOR_STORED_PROC,
                            String.valueOf(Limits.DB2_MAX_PARAMS_IN_STORED_PROCEDURE),aliasName,String.valueOf(paramCount));

                if(paramCount!=0){

                    names=new String[paramCount];
                    ((Vector)parameters[0]).copyInto(names);

                    types=new TypeDescriptor[paramCount];
                    ((Vector)parameters[1]).copyInto(types);

                    modes=new int[paramCount];
                    for(int i=0;i<paramCount;i++){
                        int currentMode= (Integer) (((Vector) parameters[2]).get(i));
                        modes[i]=currentMode;

                        //
                        // We still don't support XML values as parameters.
                        // Presumably, the XML datatype would map to a JDBC java.sql.SQLXML type.
                        // We have no support for that type today.
                        //
                        if(!types[i].isUserDefinedType()){
                            if(TypeId.getBuiltInTypeId(types[i].getJDBCTypeId()).isXMLTypeId()){
                                throw StandardException.newException(SQLState.LANG_LONG_DATA_TYPE_NOT_ALLOWED,names[i]);
                            }
                        }
                    }

                    if(paramCount>1){
                        String[] dupNameCheck=new String[paramCount];
                        System.arraycopy(names,0,dupNameCheck,0,paramCount);
                        java.util.Arrays.sort(dupNameCheck);
                        for(int dnc=1;dnc<dupNameCheck.length;dnc++){
                            if(!dupNameCheck[dnc].equals("") && dupNameCheck[dnc].equals(dupNameCheck[dnc-1]))
                                throw StandardException.newException(SQLState.LANG_DB2_DUPLICATE_NAMES,dupNameCheck[dnc],getFullName());
                        }
                    }
                }

                Integer drso=(Integer)routineElements[DYNAMIC_RESULT_SET_COUNT];
                int drs=drso==null?0:drso.intValue();

                short sqlAllowed;
                Short sqlAllowedObject=(Short)routineElements[SQL_CONTROL];
                if(sqlAllowedObject!=null)
                    sqlAllowed=sqlAllowedObject.shortValue();
                else
                    sqlAllowed=(this.aliasType==AliasInfo.ALIAS_TYPE_PROCEDURE_AS_CHAR?
                            RoutineAliasInfo.MODIFIES_SQL_DATA:RoutineAliasInfo.READS_SQL_DATA);

                Boolean isDeterministicO=(Boolean)routineElements[DETERMINISTIC];
                boolean isDeterministic=(isDeterministicO==null)?false:isDeterministicO.booleanValue();

                Boolean definersRightsO=
                        (Boolean)routineElements[ROUTINE_SECURITY_DEFINER];
                boolean definersRights=
                        (definersRightsO==null)?false:
                                definersRightsO.booleanValue();

                Boolean calledOnNullInputO=(Boolean)routineElements[NULL_ON_NULL_INPUT];
                boolean calledOnNullInput;
                if(calledOnNullInputO==null)
                    calledOnNullInput=true;
                else
                    calledOnNullInput=calledOnNullInputO.booleanValue();

                // bind the return type if it is a user defined type. this fills
                // in the class name.
                TypeDescriptor returnType=(TypeDescriptor)routineElements[RETURN_TYPE];
                if(returnType!=null){
                    DataTypeDescriptor dtd=DataTypeDescriptor.getType(returnType);

                    dtd=bindUserType(dtd);
                    returnType=dtd.getCatalogType();
                }

                aliasInfo=new RoutineAliasInfo(
                        this.methodName,
                        paramCount,
                        names,
                        types,
                        modes,
                        drs,
                        // parameter style:
                        ((Short)routineElements[PARAMETER_STYLE]).shortValue(),
                        sqlAllowed,
                        isDeterministic,
                        definersRights,
                        calledOnNullInput,
                        returnType);

                implicitCreateSchema=true;
            }
            break;

            case AliasInfo.ALIAS_TYPE_SYNONYM_AS_CHAR:
                String targetSchema;
                implicitCreateSchema=true;
                TableName t=(TableName)targetObject;
                if(t.getSchemaName()!=null)
                    targetSchema=t.getSchemaName();
                else targetSchema=getSchemaDescriptor().getSchemaName();
                aliasInfo=new SynonymAliasInfo(targetSchema,t.getTableName());
                break;

            default:
                if(SanityManager.DEBUG){
                    SanityManager.THROWASSERT(
                            "Unexpected value for aliasType ("+aliasType+")");
                }
        }
    }

    public String statementToString(){
        switch(this.aliasType){
            case AliasInfo.ALIAS_TYPE_AGGREGATE_AS_CHAR:
                return "CREATE DERBY AGGREGATE";
            case AliasInfo.ALIAS_TYPE_UDT_AS_CHAR:
                return "CREATE TYPE";
            case AliasInfo.ALIAS_TYPE_PROCEDURE_AS_CHAR:
                return "CREATE PROCEDURE";
            case AliasInfo.ALIAS_TYPE_SYNONYM_AS_CHAR:
                return "CREATE SYNONYM";
            default:
                return "CREATE FUNCTION";
        }
    }

    /**
     * CreateAliasNode creates the RoutineAliasInfo for a user defined function
     * or procedure in it's init method, which is called by the parser. But at
     * that time, we do not have the SchemaDescriptor ready to determine the
     * collation type. Hence, at the bind time, when we do have the
     * SchemaDescriptor available, we should go back and fix the
     * RoutineAliasInfo to have correct collation for its character string
     * parameters and also fix its return type (for functions) so as to have
     * correct collation if it is returning character string type.
     * <p/>
     * This method here checks if the RoutineAliasInfo has any character string
     * types associated with it. If not, then the RoutineAliasInfo that got
     * created at parsing time is just fine. But if not, then we should take
     * care of the collation type of it's character string types.
     *
     * @return true if it has a parameter or return type of character string
     */
    private boolean anyStringTypeDescriptor(){
        RoutineAliasInfo rai=(RoutineAliasInfo)aliasInfo;
        TypeDescriptor aType=rai.getReturnType();
        TypeId compTypeId;
        /*
		** Try for a built in type matching the
		** type name.  
		*/
        if(aType!=null) //that means we are not dealing with a procedure
        {
            compTypeId=TypeId.getBuiltInTypeId(aType.getTypeName());
            if(compTypeId!=null && compTypeId.isStringTypeId())
                return true;
        }
        if(rai.getParameterCount()!=0){
            int paramCount=rai.getParameterCount();
            TypeDescriptor[] paramTypes=rai.getParameterTypes();
            for(int i=0;i<paramCount;i++){
                compTypeId=TypeId.getBuiltInTypeId(paramTypes[i].getTypeName());
                if(compTypeId!=null && compTypeId.isStringTypeId())
                    return true;
            }
        }
        return false;
    }

    // We inherit the generate() method from DDLStatementNode.

    /**
     * Bind this CreateAliasNode.  This means doing any static error
     * checking that can be done before actually creating the table.
     * For example, verifying that the column name list does not
     * contain any duplicate column names.
     *
     * @throws StandardException Thrown on error
     */

    public void bindStatement() throws StandardException{
        //Are we dealing with user defined function or procedure?
        if(aliasType==AliasInfo.ALIAS_TYPE_FUNCTION_AS_CHAR ||
                aliasType==AliasInfo.ALIAS_TYPE_PROCEDURE_AS_CHAR){

            // Set the collation for all string types in parameters
            // and return types including row multi-sets to be that of
            // the schema the routine is being defined in.
            ((RoutineAliasInfo)aliasInfo).setCollationTypeForAllStringTypes(
                    getSchemaDescriptor().getCollationType());

            bindParameterTypes((RoutineAliasInfo)aliasInfo);
        }

        // validity checking for UDTs
        if(aliasType==AliasInfo.ALIAS_TYPE_UDT_AS_CHAR){
            //
            // Make sure that the java class name is not the name of a builtin
            // type. This skirts problems caused by logic across the system
            // which assumes a tight association between the builtin SQL types
            // and the Java classes which implement them.
            //
            // For security reasons we do not allow the user to bind a UDT
            // to a Derby class.
            //
            TypeId[] allSystemTypeIds=TypeId.getAllBuiltinTypeIds();
            int systemTypeCount=allSystemTypeIds.length;

            boolean foundConflict=javaClassName.startsWith("com.splicemachine.db.");

            if(!foundConflict){
                for(int i=0;i<systemTypeCount;i++){
                    TypeId systemType=allSystemTypeIds[i];
                    String systemTypeName=systemType.getCorrespondingJavaTypeName();

                    if(systemTypeName.equals(javaClassName)){
                        foundConflict=true;
                        break;
                    }
                }
            }

            if(foundConflict){
                throw StandardException.newException
                        (SQLState.LANG_UDT_BUILTIN_CONFLICT,javaClassName);
            }

            return;
        }

        // validity checking for aggregates
        if(aliasType==AliasInfo.ALIAS_TYPE_AGGREGATE_AS_CHAR){
            bindAggregate();
        }

        // Aggregates, procedures and functions do not check class or method validity until
        // runtime execution. Synonyms do need some validity checks.
        if(aliasType!=AliasInfo.ALIAS_TYPE_SYNONYM_AS_CHAR)
            return;

        // Don't allow creating synonyms in SESSION schema. Causes confusion if
        // a temporary table is created later with same name.
        if(isSessionSchema(getSchemaDescriptor().getSchemaName()))
            throw StandardException.newException(SQLState.LANG_OPERATION_NOT_ALLOWED_ON_SESSION_SCHEMA_TABLES);

        String targetSchema=((SynonymAliasInfo)aliasInfo).getSynonymSchema();
        String targetTable=((SynonymAliasInfo)aliasInfo).getSynonymTable();
        if(this.getObjectName().equals(targetSchema,targetTable))
            throw StandardException.newException(SQLState.LANG_SYNONYM_CIRCULAR,
                    this.getFullName(),
                    targetSchema+"."+targetTable);

        SchemaDescriptor targetSD=getSchemaDescriptor(targetSchema,false);
        if((targetSD!=null) && isSessionSchema(targetSD))
            throw StandardException.newException(SQLState.LANG_OPERATION_NOT_ALLOWED_ON_SESSION_SCHEMA_TABLES);

    }

    /**
     * Extra logic for binding user-defined aggregate definitions
     */
    private void bindAggregate() throws StandardException{
        String unqualifiedName=getRelativeName();

        //
        // A user-defined aggregate cannot have the name of a builtin function which takes 1 argument.
        //
        SchemaDescriptor sysfun=getSchemaDescriptor("SYSFUN",true);
        List<AliasDescriptor> systemFunctions=getDataDictionary().getRoutineList(sysfun.getUUID().toString(),
                unqualifiedName, AliasInfo.ALIAS_NAME_SPACE_FUNCTION_AS_CHAR );

        for(AliasDescriptor systemFunction : systemFunctions){
            AliasDescriptor function=(AliasDescriptor)systemFunction;

            RoutineAliasInfo routineInfo=(RoutineAliasInfo)function.getAliasInfo();
            int parameterCount=routineInfo.getParameterCount();
            if(parameterCount==1){
                throw illegalAggregate();
            }
        }

        //
        // Additional builtin 1-arg functions which are represented in the grammar
        // as non-reserved keywords.
        //
        for(int i=0;i<NON_RESERVED_FUNCTION_NAMES.length;i++){
            if(NON_RESERVED_FUNCTION_NAMES[i].equals(unqualifiedName)){
                throw illegalAggregate();
            }
        }

        //
        // Additional SQL Standard aggregate names which are not represented in
        // the Derby grammar as reserved keywords.
        //
        for(int i=0;i<NON_RESERVED_AGGREGATES.length;i++){
            if(NON_RESERVED_AGGREGATES[i].equals(unqualifiedName)){
                throw illegalAggregate();
            }
        }

        // now bind the input and return types
        AggregateAliasInfo aai=(AggregateAliasInfo)aliasInfo;

        aai.setCollationTypeForAllStringTypes(getSchemaDescriptor().getCollationType());
    }

    /**
     * Construct an exception flagging an illegal aggregate name
     */
    private StandardException illegalAggregate(){
        return StandardException.newException(SQLState.LANG_ILLEGAL_UDA_NAME,getRelativeName());
    }

    /**
     * Bind the class names for UDTs
     */
    private void bindParameterTypes(RoutineAliasInfo aliasInfo) throws StandardException{
        TypeDescriptor[] parameterTypes=aliasInfo.getParameterTypes();

        if(parameterTypes==null){
            return;
        }

        int count=parameterTypes.length;
        for(int i=0;i<count;i++){
            TypeDescriptor td=parameterTypes[i];

            parameterTypes[i]=bindUserCatalogType(parameterTypes[i]);

        }
    }

    /**
     * Create the Constant information that will drive the guts of Execution.
     *
     * @throws StandardException Thrown on failure
     */
    public ConstantAction makeConstantAction() throws StandardException{
        String schemaName=getSchemaDescriptor().getSchemaName();

        return getGenericConstantActionFactory().getCreateAliasConstantAction(
                getRelativeName(),
                schemaName,
                javaClassName,
                aliasInfo,
                aliasType);
    }
}
