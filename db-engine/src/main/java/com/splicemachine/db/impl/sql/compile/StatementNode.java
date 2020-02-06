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
import com.splicemachine.db.iapi.reference.ClassName;
import com.splicemachine.db.iapi.reference.SQLState;
import com.splicemachine.db.iapi.services.classfile.VMOpcode;
import com.splicemachine.db.iapi.services.compiler.MethodBuilder;
import com.splicemachine.db.iapi.services.context.ContextManager;
import com.splicemachine.db.iapi.services.loader.GeneratedClass;
import com.splicemachine.db.iapi.services.sanity.SanityManager;
import com.splicemachine.db.iapi.sql.ResultDescription;
import com.splicemachine.db.iapi.sql.dictionary.TableDescriptor;
import com.splicemachine.db.iapi.util.ByteArray;

import java.lang.reflect.Modifier;
import java.util.HashMap;
import java.util.Map;
import java.util.Vector;

/**
 * A StatementNode represents a single statement in the language.  It is
 * the top node for any statement.
 * <p/>
 * StatementNode controls the class generation for query tree nodes.
 */
public abstract class StatementNode extends QueryTreeNode{
    /* Cached empty list object. */
    static final TableDescriptor[] EMPTY_TD_LIST=new TableDescriptor[0];
    /*
     * create the outer shell class builder for the class we will
     * be generating, generate the expression to stuff in it,
     * and turn it into a class.
     */
    static final int NEED_DDL_ACTIVATION=5;
    static final int NEED_CURSOR_ACTIVATION=4;
    static final int NEED_PARAM_ACTIVATION=2;
    static final int NEED_ROW_ACTIVATION=1;
    static final int NEED_NOTHING_ACTIVATION=0;

    StatementNode(){ }

    StatementNode(ContextManager cm){ super(cm); }

    /**
     * By default, assume StatementNodes are atomic.
     * The rare statements that aren't atomic (e.g.
     * CALL method()) override this.
     *
     * @return true if the statement is atomic
     * @throws StandardException Thrown on error
     */
    @Override
    public boolean isAtomic() throws StandardException{ return true; }

    /**
     * Returns whether or not this Statement requires a set/clear savepoint
     * around its execution.  The following statement "types" do not require them:
     * Cursor	- unnecessary and won't work in a read only environment
     * Xact	- savepoint will get blown away underneath us during commit/rollback
     * <p/>
     * ONLY CALLABLE AFTER GENERATION
     * <p/>
     * This implementation returns true, sub-classes can override the
     * method to not require a savepoint.
     *
     * @return boolean    Whether or not this Statement requires a set/clear savepoint
     */
    public boolean needsSavepoint(){ return true; }

    /**
     * Get the name of the SPS that is used to execute this statement. Only
     * relevant for an ExecSPSNode -- otherwise, returns null.
     *
     * @return the name of the underlying sps
     */
    public String getSPSName(){ return null; }

    /**
     * Returns the name of statement in EXECUTE STATEMENT command. Returns null
     * for all other commands.
     *
     * @return String null unless overridden for Execute Statement command
     */
    public String executeStatementName(){
        return null;
    }

    /**
     * Returns name of schema in EXECUTE STATEMENT command. Returns null for all
     * other commands.
     *
     * @return String schema for EXECUTE STATEMENT null for all others
     */
    public String executeSchemaName(){ return null; }

    /**
     * Only DML statements have result descriptions - for all others return
     * null. This method is overridden in DMLStatementNode.
     *
     * @return null
     */
    public ResultDescription makeResultDescription(){ return null; }

    /**
     * Convert this object to a String. See comments in QueryTreeNode.java for
     * how this should be done for tree printing.
     *
     * @return This object as a String
     */
    @Override
    public String toString(){
        if(SanityManager.DEBUG){
            return "statementType: "+statementToString()+"\n"+ super.toString();
        }else{
            return "";
        }
    }

    public abstract String statementToString();

    /**
     * Perform the binding operation statement.  Binding consists of
     * permissions checking, view resolution, datatype resolution, and
     * creation of a dependency list (for determining whether a tree or
     * plan is still up to date).
     * <p/>
     * This bindStatement() method does nothing.
     * Each StatementNode type that can appear
     * at the top of a tree can override this method with its
     * own bindStatement() method that does "something".
     *
     * @throws StandardException Thrown on error
     */
    public void bindStatement() throws StandardException{ }

    /**
     * Generates an optimized statement from a bound StatementNode.  Actually,
     * it annotates the tree in place rather than generating a new tree.
     * <p/>
     * For non-optimizable statements (for example, CREATE TABLE),
     * return the bound tree without doing anything.  For optimizable
     * statements, this method will be over-ridden in the statement's
     * root node (DMLStatementNode in all cases we know about so far).
     * <p/>
     * Throws an exception if the tree is not bound, or if the binding
     * is out of date.
     *
     * @throws StandardException Thrown on error
     */
    public void optimizeStatement() throws StandardException{  }

    /**
     * Do code generation for this statement.
     *
     * @param byteCode the generated byte code for this statement.
     *                 if non-null, then the byte code is saved
     *                 here.
     * @throws StandardException Thrown on error
     * @return A GeneratedClass for this statement
     */
    public GeneratedClass generate(ByteArray byteCode) throws StandardException{
        // start the new activation class.
        // it starts with the Execute method
        // and the appropriate superclass (based on
        // statement type, from inspecting the queryTree).

        int nodeChoice=activationKind();

		/* RESOLVE: Activation hierarchy was way too complicated
		 * and added no value.  Simple thing to do was to simply
		 * leave calling code alone and to handle here and to
		 * eliminate unnecessary classes.
		 */
        String superClass;
        switch(nodeChoice){
            case NEED_CURSOR_ACTIVATION:
                superClass=ClassName.CursorActivation;
                break;
            case NEED_DDL_ACTIVATION:
                return getClassFactory().loadGeneratedClass(
                        "com.splicemachine.db.impl.sql.execute.ConstantActionActivation",null);

            case NEED_NOTHING_ACTIVATION:
            case NEED_ROW_ACTIVATION:
            case NEED_PARAM_ACTIVATION:
                superClass=ClassName.BaseActivation;
                break;
            default:
                throw StandardException.newException(SQLState.LANG_UNAVAILABLE_ACTIVATION_NEED,
                        String.valueOf(nodeChoice));
        }

        ActivationClassBuilder generatingClass=new ActivationClassBuilder(
                superClass,
                getCompilerContext());

        // Set Spark
        generatingClass.setDataSetProcessorType(getCompilerContext().getDataSetProcessorType());

        /*
         * Generate the code to execute this statement.
         * Two methods are generated here: execute() and
         * fillResultSet().
         * <BR>
         * execute is called for every execution of the
         * Activation. Nodes may add code to this using
         * ActivationClassBuilder.getExecuteMethod().
         * This code will be executed every execution.
         * <BR>
         * fillResultSet is called by execute if the BaseActivation's
         * resultSet field is null and the returned ResultSet is
         * set into the the resultSet field.
         * <P>
         * The generated code is equivalent to:
         * <code>
         * public ResultSet execute() {
         *
         *    // these two added by ActivationClassBuilder
         *    throwIfClosed("execute");
         *    startExecution();
         *
         *    [per-execution code added by nodes]
         *
         *    if (resultSet == null)
         *        resultSet = fillResultSet();
         *
         *    return resultSet;
         * }
         * </code>
         */

        MethodBuilder executeMethod=generatingClass.getExecuteMethod();

        MethodBuilder mbWorker=generatingClass.getClassBuilder().newMethodBuilder(
                Modifier.PRIVATE,
                ClassName.ResultSet,
                "fillResultSet");
        mbWorker.addThrownException(ClassName.StandardException);

        // Generate the complete ResultSet tree for this statement.
        // This step may add statements into the execute method
        // for per-execution actions.
        generate(generatingClass,mbWorker);
        mbWorker.methodReturn();
        mbWorker.complete();

        executeMethod.pushThis();
        executeMethod.getField(ClassName.BaseActivation,"resultSet", ClassName.ResultSet);

        executeMethod.conditionalIfNull();

        // Generate the result set tree and store the
        // resulting top-level result set into the resultSet
        // field, as well as returning it from the execute method.

        executeMethod.pushThis();
        executeMethod.callMethod(VMOpcode.INVOKEVIRTUAL,null,"fillResultSet",ClassName.ResultSet,0);
        executeMethod.pushThis();
        executeMethod.swap();
        executeMethod.putField(ClassName.BaseActivation,"resultSet",ClassName.ResultSet);

        executeMethod.startElseCode(); // this is here as the compiler only supports ? :
        executeMethod.pushThis();
        executeMethod.getField(ClassName.BaseActivation,"resultSet",ClassName.ResultSet);
        executeMethod.completeConditional();

        // wrap up the activation class definition
        // generate on the tree gave us back the newExpr
        // for getting a result set on the tree.
        // we put it in a return statement and stuff
        // it in the execute method of the activation.
        // The generated statement is the expression:
        // the activation class builder takes care of constructing it
        // for us, given the resultSetExpr to use.
        //   return (this.resultSet = #resultSetExpr);
        generatingClass.finishExecuteMethod(this instanceof CursorNode);

        // wrap up the constructor by putting a return at the end of it
        generatingClass.finishConstructor();

        generatingClass.finishMaterializationMethod();
        try{
            // cook the completed class into a real class
            // and stuff it into activationClass
            return generatingClass.getGeneratedClass(byteCode);
        }catch(StandardException e){

            String msgId=e.getMessageId();

            if(SQLState.GENERATED_CLASS_LIMIT_EXCEEDED.equals(msgId)
                    || SQLState.GENERATED_CLASS_LINKAGE_ERROR.equals(msgId)){
                throw StandardException.newException(
                        SQLState.LANG_QUERY_TOO_COMPLEX,e);
            }

            throw e;
        }
    }

    /**
     * Returns a list of base tables for which the index statistics of the
     * associated indexes should be updated.
     * <p/>
     * This default implementation always returns an empty list.
     *
     * @return A list of table descriptors (potentially empty).
     * @throws StandardException if accessing the index descriptors of a base
     *                           table fails
     */
    public TableDescriptor[] updateIndexStatisticsFor()
            throws StandardException{
        // Do nothing, overridden by appropriate nodes.
        return EMPTY_TD_LIST;
    }

    abstract int activationKind();

    protected Vector withParameterList;

    public void setWithVector(Vector withParameterList) {
        this.withParameterList = withParameterList;
    }

    /**
     *
     * Bind and Optimize Real Time Views (OK, That is a made up name).
     */
    public void bindAndOptimizeRealTimeViews() throws StandardException {
        if (this.withParameterList != null) {
            Map<String,TableDescriptor> withMap = new HashMap<>(withParameterList.size());
            for (Object aWithParameterList : withParameterList) {
                CreateViewNode createViewNode = (CreateViewNode) aWithParameterList;
                createViewNode.bindStatement();
                createViewNode.optimizeStatement();
                withMap.put(createViewNode.getRelativeName(), createViewNode.createDynamicView());
                this.getLanguageConnectionContext().setWithStack(withMap);
            }
        }
    }

}
