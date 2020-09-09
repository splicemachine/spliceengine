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

import com.splicemachine.db.catalog.AliasInfo;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.reference.SQLState;
import com.splicemachine.db.iapi.services.compiler.MethodBuilder;
import com.splicemachine.db.iapi.services.loader.ClassFactory;
import com.splicemachine.db.iapi.services.loader.ClassInspector;
import com.splicemachine.db.iapi.services.sanity.SanityManager;
import com.splicemachine.db.iapi.sql.compile.AggregateDefinition;
import com.splicemachine.db.iapi.sql.compile.C_NodeTypes;
import com.splicemachine.db.iapi.sql.compile.CompilerContext;
import com.splicemachine.db.iapi.sql.dictionary.AliasDescriptor;
import com.splicemachine.db.iapi.sql.dictionary.DataDictionary;
import com.splicemachine.db.iapi.sql.dictionary.SchemaDescriptor;
import com.splicemachine.db.iapi.types.DataTypeDescriptor;
import com.splicemachine.db.iapi.types.DataValueDescriptor;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.util.List;

import static com.splicemachine.db.iapi.sql.compile.AggregateDefinition.FunctionType;
import static com.splicemachine.db.iapi.sql.compile.AggregateDefinition.fromString;

/**
 * An Aggregate Node is a node that reprsents a set function/aggregate.
 * It used for all system aggregates as well as user defined aggregates.
 */

@SuppressFBWarnings(value="HE_INHERITS_EQUALS_USE_HASHCODE", justification="DB-9277")
public class AggregateNode extends UnaryOperatorNode {
    protected boolean distinct;

    protected AggregateDefinition uad;
    protected TableName userAggregateName;
    protected StringBuffer aggregatorClassName;
    private String aggregateDefinitionClassName;
    private Class aggregateDefinitionClass;
    protected ClassInspector classInspector;
    protected String aggregateName;
    protected FunctionType type;

    /*
     ** We wind up pushing all aggregates into a different
     ** resultColumnList.  When we do this (in
     ** replaceAggregateWithColumnReference), we return a
     ** column reference and create a new result column.
     ** This is used to store that result column.
     */
    protected ResultColumn generatedRC;
    protected ColumnReference generatedRef;

    private boolean isWindowFunction;

    /**
     * Intializer.  Used for user defined and internally defined aggregates.
     * Called when binding a StaticMethodNode that we realize is an aggregate.
     *
     * @param operand       the value expression for the aggregate
     * @param uadClass      the class name for user aggregate definition for the aggregate
     *                      or the Class for the internal aggregate type.
     * @param distinct      boolean indicating whether this is distinct
     *                      or not.
     * @param aggregateName the name of the aggregate from the user's perspective,
     *                      e.g. MAX
     * @throws StandardException on error
     */
    public void init
    (
            Object operand,
            Object uadClass,
            Object distinct,
            Object aggregateName
    ) throws StandardException {
        super.init(operand);
        this.aggregateName = (String) aggregateName;

        if (uadClass instanceof UserAggregateDefinition) {
            setUserDefinedAggregate((UserAggregateDefinition) uadClass);
            this.distinct = (Boolean) distinct;
        } else if (uadClass instanceof TableName) {
            this.userAggregateName = (TableName) uadClass;
            this.distinct = (Boolean) distinct;
        } else {
            this.aggregateDefinitionClass = (Class) uadClass;
            this.type = fromString((String) aggregateName);


            // Distinct is meaningless for min and max
            if (!aggregateDefinitionClass.equals(MaxMinAggregateDefinition.class)) {
                this.distinct = (Boolean) distinct;
            }
            this.aggregateDefinitionClassName = aggregateDefinitionClass.getName();
        }
    }

    /**
     * initialize fields for user defined aggregate
     */
    protected void setUserDefinedAggregate(UserAggregateDefinition userAgg) {
        this.uad = userAgg;
        this.aggregateDefinitionClass = uad.getClass();

        this.aggregateDefinitionClassName = aggregateDefinitionClass.getName();
    }

    /**
     * Replace aggregates in the expression tree with a ColumnReference to
     * that aggregate, append the aggregate to the supplied RCL (assumed to
     * be from the child ResultSetNode) and return the ColumnReference.
     * This is useful for pushing aggregates in the Having clause down to
     * the user's select at parse time.  It is also used for moving around
     * Aggregates in the select list when creating the Group By node.  In
     * that case it is called <B> after </B> bind time, so we need to create
     * the column differently.
     *
     * @param rcl         The RCL to append to.
     * @param tableNumber The tableNumber for the new ColumnReference
     * @return ValueNode    The (potentially) modified tree.
     * @throws StandardException Thrown on error
     */
    public ValueNode replaceAggregatesWithColumnReferences(ResultColumnList rcl, int tableNumber)
            throws StandardException {

        /*
         ** This call is idempotent.  Do
         ** the right thing if we have already
         ** replaced ourselves.
         */
        if (generatedRef == null) {
            String generatedColName;
            CompilerContext cc = getCompilerContext();
            generatedColName = "SQLCol" + cc.getNextColumnNumber();
            generatedRC = (ResultColumn) getNodeFactory().getNode(
                    C_NodeTypes.RESULT_COLUMN,
                    generatedColName,
                    this,
                    getContextManager());
            generatedRC.markGenerated();

            /*
             ** Parse time.
             */
            generatedRef = (ColumnReference) getNodeFactory().getNode(
                    C_NodeTypes.COLUMN_REFERENCE,
                    generatedRC.getName(),
                    null,
                    getContextManager());

            // RESOLVE - unknown nesting level, but not correlated, so nesting levels must be 0
            generatedRef.setSource(generatedRC);
            generatedRef.setNestingLevel(0);
            generatedRef.setSourceLevel(0);

            if (tableNumber != -1) {
                generatedRef.setTableNumber(tableNumber);
            }

            rcl.addResultColumn(generatedRC);

            /*
             ** Mark the ColumnReference as being generated to replace
             ** an aggregate
             */
            generatedRef.markGeneratedToReplaceAggregate();
        } else {
            rcl.addResultColumn(generatedRC);
        }

        return generatedRef;
    }

    /**
     * Get the AggregateDefinition.
     *
     * @return The AggregateDefinition
     */
    AggregateDefinition getAggregateDefinition() {
        return uad;
    }

    /**
     * Get the generated ResultColumn where this
     * aggregate now resides after a call to
     * replaceAggregatesWithColumnReference().
     *
     * @return the result column
     */
    public ResultColumn getGeneratedRC() {
        if (SanityManager.DEBUG) {
            SanityManager.ASSERT(generatedRC != null,
                    "generatedRC is null.  replaceAggregateWithColumnReference() " +
                            "has not been called on this AggergateNode.  Make sure " +
                            "the node is under a ResultColumn as expected.");
        }

        return generatedRC;
    }

    /**
     * Get the generated ColumnReference to this
     * aggregate after the parent called
     * replaceAggregatesWithColumnReference().
     *
     * @return the column reference
     */
    public ColumnReference getGeneratedRef() {
        if (SanityManager.DEBUG) {
            SanityManager.ASSERT(generatedRef != null,
                    "generatedRef is null.  replaceAggregateWithColumnReference() " +
                            "has not been called on this AggergateNode.  Make sure " +
                            "the node is under a ResultColumn as expected.");
        }
        return generatedRef;
    }

    /**
     * Bind this operator.  Determine the type of the subexpression,
     * and pass that into the UserAggregate.
     *
     * @param fromList        The query's FROM list
     * @param subqueryList    The subquery list being built as we find SubqueryNodes
     * @param aggregateVector The aggregate list being built as we find AggregateNodes
     * @throws StandardException Thrown on error
     * @return The new top of the expression tree.
     */
    @Override
    public ValueNode bindExpression(FromList fromList,
                                    SubqueryList subqueryList,
                                    List<AggregateNode> aggregateVector) throws StandardException {
        DataDictionary dd = getDataDictionary();
        DataTypeDescriptor dts = null;
        ClassFactory cf;

        cf = getClassFactory();
        classInspector = cf.getClassInspector();
        if (userAggregateName != null) {
            userAggregateName.bind(dd);
            AliasDescriptor ad = resolveAggregate
                    (
                            dd,
                            getSchemaDescriptor(userAggregateName.getSchemaName(), true),
                            userAggregateName.getTableName()
                    );
            if (ad == null) {
                throw StandardException.newException
                        (
                                SQLState.LANG_OBJECT_NOT_FOUND,
                                AliasDescriptor.getAliasType(AliasInfo.ALIAS_TYPE_AGGREGATE_AS_CHAR),
                                userAggregateName.getTableName()
                        );
            }

            setUserDefinedAggregate(new UserAggregateDefinition(ad));
            aggregateName = ad.getJavaClassName();
        }

        instantiateAggDef();
        // if this is a user-defined aggregate
        if (isUserDefinedAggregate()) {
            AliasDescriptor ad = ((UserAggregateDefinition) uad).getAliasDescriptor();

            // set up dependency on the user-defined aggregate and compile a check for USAGE
            // priv if needed
            getCompilerContext().createDependency(ad);

            if (isPrivilegeCollectionRequired()) {
                getCompilerContext().addRequiredUsagePriv(ad);
            }
        }
        /* Add ourselves to the aggregateVector before we do anything else */
        aggregateVector.add(this);

        CompilerContext cc = getCompilerContext();

        // operand being null means a count(*)
        if (operand != null) {
            int previousReliability = orReliability(CompilerContext.AGGREGATE_RESTRICTION);
            bindOperand(fromList, subqueryList, aggregateVector);
            cc.setReliability(previousReliability);

            /*
             ** Make sure that we don't have an aggregate
             ** IMMEDIATELY below us.  Don't search below
             ** any ResultSetNodes.
             */
            HasNodeVisitor visitor = new HasNodeVisitor(this.getClass(), ResultSetNode.class);
            operand.accept(visitor);

            /*
             * We relax the constraint there for window functions for SPLICE-969
             * We need to support nested aggregate for queries: 12,20,47,53,57,63,89,98.
             * in TPC-DS
             */
            if (visitor.hasNode() && !this.isWindowFunction) {
                throw StandardException.newException
                        (
                                SQLState.LANG_USER_AGGREGATE_CONTAINS_AGGREGATE,
                                getSQLName()
                        );

            }

            // Also forbid any window function inside an aggregate unless in
            // subquery, cf. SQL 2003, section 10.9, SR 7 a).
            SelectNode.checkNoWindowFunctions(operand, aggregateName);
            SelectNode.checkNoGroupingFunctions(operand, aggregateName);

            /*
             ** Check the type of the operand.  Make sure that the user
             ** defined aggregate can handle the operand datatype.
             */
            dts = operand.getTypeServices();

            /* Convert count(nonNullableColumn) to count(*)	*/
            if (uad instanceof CountAggregateDefinition &&
                    !dts.isNullable()) {
                setOperator(aggregateName);
                setMethodName(aggregateName);
            }

            /*
             ** If we have a distinct, then the value expression
             ** MUST implement Orderable because we are going
             ** to process it using it as part of a sort.
             */
            if (distinct) {
                /*
                 ** For now, we check to see if orderable() returns
                 ** true for this type.  In the future we may need
                 ** to check to see if the type implements Orderable
                 **
                 */
                if (!operand.getTypeId().orderable(cf)) {
                    throw StandardException.newException(SQLState.LANG_COLUMN_NOT_ORDERABLE_DURING_EXECUTION,
                            dts.getTypeId().getSQLTypeName());
                }

            }

            /*
             ** Don't allow an untyped null
             */
            if (operand instanceof UntypedNullConstantNode) {
                throw StandardException.newException
                        (SQLState.LANG_USER_AGGREGATE_BAD_TYPE_NULL, getSQLName());

            }
        }

        /*
         ** Ask the aggregate definition whether it can handle
         ** the input datatype.
         */
        aggregatorClassName = new StringBuffer();
        DataTypeDescriptor resultType = uad.getAggregator(dts, aggregatorClassName);

        if (resultType == null) {
            throw StandardException.newException
                    (
                            SQLState.LANG_USER_AGGREGATE_BAD_TYPE,
                            getSQLName(),
                            operand.getTypeId().getSQLTypeName()
                    );

        }
        // For user-defined aggregates, the input operand may need to be
        // coerced to the expected input type of the aggregator.
        if (isUserDefinedAggregate()) {
            ValueNode castNode = ((UserAggregateDefinition) uad).castInputValue
                    (operand, getContextManager());
            if (castNode != null) {
                operand = castNode.bindExpression(fromList, subqueryList, aggregateVector);
            }
        } else if (isWindowFunction() &&
                uad instanceof SumAvgAggregateDefinition &&
                operand != null &&
                operand.getTypeId().getTypeFormatId() !=
                        resultType.getTypeId().getTypeFormatId()) {
            // For now, the receiver data type picked by
            // getAggregator() must match the data type of the
            // operand when processing window functions
            // to avoid ClassCastExceptions for NativeSparkDataSets.
            // For non-windowed aggregations, we process the aggregation using
            // the original data type of the operand for performance.  If it overflows,
            // a new aggregator that uses the final result type is allocated.
            // When there is no overflow, a final CAST is applied in
            // methoc SpliceGeneratorAggregator.finish.
            operand =
                    (ValueNode)
                            getNodeFactory().getNode(C_NodeTypes.CAST_NODE,
                                    operand,
                                    resultType,
                                    getContextManager());
            ((CastNode) operand).bindCastNodeOnly();
        }

        checkAggregatorClassName(aggregatorClassName.toString());

        setType(resultType);

        return this;
    }

    /**
     * Resolve a user-defined aggregate.
     */
    public static AliasDescriptor resolveAggregate
    (DataDictionary dd, SchemaDescriptor sd, String rawName)
            throws StandardException {
        List<AliasDescriptor> list = dd.getRoutineList
                (sd.getUUID().toString(), rawName, AliasInfo.ALIAS_NAME_SPACE_AGGREGATE_AS_CHAR);

        if (!list.isEmpty()) {
            return list.get(0);
        }

        return null;
    }

    /*
     ** Make sure the aggregator class is ok
     */
    protected void checkAggregatorClassName(String className) throws StandardException {
        verifyClassExist(className);

        if (!classInspector.assignableTo(className, "com.splicemachine.db.iapi.sql.execute.ExecAggregator")) {
            throw StandardException.newException(SQLState.LANG_BAD_AGGREGATOR_CLASS2,
                    className,
                    getSQLName(),
                    operand.getTypeId().getSQLTypeName());
        }
    }


    /*
     ** Instantiate the aggregate definition.
     */
    protected void instantiateAggDef() throws StandardException {
        if (uad == null) {
            Class theClass = aggregateDefinitionClass;

            // get the class
            if (theClass == null) {
                String aggClassName = aggregateDefinitionClassName;
                verifyClassExist(aggClassName);

                try {
                    theClass = classInspector.getClass(aggClassName);
                } catch (Throwable t) {
                    throw StandardException.unexpectedUserException(t);
                }
            }


            // get an instance
            Object instance = null;
            try {
                instance = theClass.newInstance();
            } catch (Throwable t) {
                throw StandardException.unexpectedUserException(t);
            }

            if (!(instance instanceof AggregateDefinition)) {
                throw StandardException.newException(SQLState.LANG_INVALID_USER_AGGREGATE_DEFINITION2, aggregateDefinitionClassName);
            }

            if (instance instanceof MaxMinAggregateDefinition) {
                MaxMinAggregateDefinition temp = (MaxMinAggregateDefinition) instance;
                if (aggregateName.equals("MAX"))
                    temp.setMaxOrMin(true);
                else
                    temp.setMaxOrMin(false);
                temp.setWindowFunction(isWindowFunction);
            }

            if (instance instanceof SumAvgAggregateDefinition) {
                SumAvgAggregateDefinition temp1 = (SumAvgAggregateDefinition) instance;
                if (aggregateName.equals("SUM"))
                    temp1.setSumOrAvg(true);
                else
                    temp1.setSumOrAvg(false);
                temp1.setWindowFunction(isWindowFunction);
            }

            if (instance instanceof CountAggregateDefinition) {
                CountAggregateDefinition temp2 = (CountAggregateDefinition) instance;
                temp2.setWindowFunction(isWindowFunction);
            }

            if (instance instanceof StringAggregateDefinition) {
                ((StringAggregateDefinition) instance).setWindowFunction(isWindowFunction);
            }

            this.uad = (AggregateDefinition) instance;
        }
        setOperator(aggregateName);
        setMethodName(aggregateDefinitionClassName);

    }

    /**
     * Indicate whether this aggregate is distinct or not.
     *
     * @return true/false
     */
    public boolean isDistinct() {
        return distinct;
    }

    /**
     * Get the class that implements that aggregator for this
     * node.
     *
     * @return the class name
     */
    public String getAggregatorClassName() {
        return aggregatorClassName.toString();
    }

    /**
     * Get the class that implements that aggregator for this
     * node.
     *
     * @return the class name
     */
    public String getAggregateName() {
        return aggregateName;
    }

    /**
     * Get the result column that has a new aggregator.
     * This aggregator will be fed into the sorter.
     *
     * @param dd the data dictionary
     * @return the result column.  WARNING: it still needs to be bound
     * @throws StandardException on error
     */
    public ResultColumn getNewAggregatorResultColumn(DataDictionary dd)
            throws StandardException {
        String className = aggregatorClassName.toString();

        DataTypeDescriptor compType =
                DataTypeDescriptor.getSQLDataTypeDescriptor(className);

        /*
         ** Create a null of the right type.  The proper aggregators
         ** are created dynamically by the SortObservers
         */
        ValueNode nullNode = getNullNode(compType);

        nullNode.bindExpression(
                null,    // from
                null,    // subquery
                null);    // aggregate

        /*
         ** Create a result column with this new node below
         ** it.
         */
        return (ResultColumn) getNodeFactory().getNode(
                C_NodeTypes.RESULT_COLUMN,
                aggregateName,
                nullNode,
                getContextManager());
    }


    /**
     * Get the aggregate expression in a new result
     * column.
     *
     * @param dd the data dictionary
     * @return the result column.  WARNING: it still needs to be bound
     * @throws StandardException on error
     */
    public ResultColumn getNewExpressionResultColumn(DataDictionary dd)
            throws StandardException {
        ValueNode node;
        /*
         ** Create a result column with the aggrergate operand
         ** it.  If there is no operand, then we have a COUNT(*),
         ** so we'll have to create a new null node and put
         ** that in place.
         */
        node = (operand == null) ?
                this.getNewNullResultExpression() :
                operand;


        return (ResultColumn) getNodeFactory().getNode(
                C_NodeTypes.RESULT_COLUMN,
                "##aggregate expression",
                node,
                getContextManager());
    }

    /**
     * Get the null aggregate result expression
     * column.
     *
     * @return the value node
     * @throws StandardException on error
     */
    public ValueNode getNewNullResultExpression()
            throws StandardException {
        /*
         ** Create a result column with the aggrergate operand
         ** it.
         */
        DataTypeDescriptor type = null;
        try {
            type = getTypeServices();
            return getNullNode(type);
        } catch (StandardException e) {
            if (e.getSqlState().compareTo(SQLState.LANG_NONULL_DATATYPE) == 0) {
                throw StandardException.newException(SQLState.LANG_INVALID_AGGREGATION_DATATYPE, type.getTypeId().getSQLTypeName());
            } else throw e;
        }
    }

    /**
     * Do code generation for this unary operator.  Should
     * never be called for an aggregate -- it should be converted
     * into something else by code generation time.
     *
     * @param acb The ExpressionClassBuilder for the class we're generating
     * @param mb  The method the code to place the code
     * @throws StandardException Thrown on error
     */
    public void generateExpression(ExpressionClassBuilder acb,
                                   MethodBuilder mb)
            throws StandardException {
        if (SanityManager.DEBUG) {
            SanityManager.THROWASSERT("generateExpression() should never " +
                    "be called on an AggregateNode - " + getSQLName() + ". " +
                    "replaceAggregatesWithColumnReferences should have " +
                    "been called prior to generateExpression");
        }
    }

    /**
     * Print a string ref of this node.
     *
     * @return a string representation of this node
     */
    public String toString() {
        if (SanityManager.DEBUG) {
            return "aggregateName: " + getSQLName() + "\n" +
                    "distinct: " + distinct + "\n" +
                    super.toString();
        } else {
            return "";
        }
    }

    public boolean isConstant() {
        return false;
    }

    public boolean constantExpression(PredicateList where) {
        return false;
    }

    /**
     * Get the SQL name of the aggregate
     */
    public String getSQLName() {
        if (isUserDefinedAggregate()) {
            return ((UserAggregateDefinition) uad).
                    getAliasDescriptor().getQualifiedName();
        } else {
            return aggregateName;
        }
    }

    /**
     * Return true if this is a user-defined aggregate
     */
    protected boolean isUserDefinedAggregate() {
        return uad instanceof UserAggregateDefinition;
    }


    public void setWindowFunction(boolean isWindowFunction) {
        this.isWindowFunction = isWindowFunction;
    }

    public boolean isWindowFunction() {
        return this.isWindowFunction;
    }

    public FunctionType getType() {
        return this.type;
    }
}
