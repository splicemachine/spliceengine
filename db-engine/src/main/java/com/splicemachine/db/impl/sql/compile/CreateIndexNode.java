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

import com.splicemachine.db.catalog.UUID;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.reference.Property;
import com.splicemachine.db.iapi.reference.SQLState;
import com.splicemachine.db.iapi.services.compiler.MethodBuilder;
import com.splicemachine.db.iapi.services.loader.GeneratedClass;
import com.splicemachine.db.iapi.services.property.PropertyUtil;
import com.splicemachine.db.iapi.services.sanity.SanityManager;
import com.splicemachine.db.iapi.sql.compile.C_NodeTypes;
import com.splicemachine.db.iapi.sql.dictionary.ColumnDescriptor;
import com.splicemachine.db.iapi.sql.dictionary.DataDictionary;
import com.splicemachine.db.iapi.sql.dictionary.SchemaDescriptor;
import com.splicemachine.db.iapi.sql.dictionary.TableDescriptor;
import com.splicemachine.db.iapi.sql.execute.ConstantAction;
import com.splicemachine.db.iapi.types.DataTypeDescriptor;
import com.splicemachine.db.iapi.util.ByteArray;
import com.splicemachine.db.impl.ast.CollectingVisitor;
import splice.com.google.common.base.Predicates;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * A CreateIndexNode is the root of a QueryTree that represents a CREATE INDEX
 * statement.
 *
 */

public class CreateIndexNode extends DDLStatementNode
{
    boolean                 unique;
    boolean                 uniqueWithDuplicateNulls;
    DataDictionary          dd = null;
    Properties              properties;
    String                  indexType;
    TableName               indexName;
    TableName               tableName;
    Vector<IndexExpression> expressionList;
    String[]                involvedColumnNames = null;
    boolean[]               isAscending;
    int[]                   boundColumnIDs;
    boolean                 excludeNulls;
    boolean                 excludeDefaults;
    boolean                 preSplit;
    boolean                 isLogicalKey;
    boolean                 sampling;
    double                  sampleFraction;
    String                  splitKeyPath;
    String                  hfilePath;
    String                  columnDelimiter;
    String                  characterDelimiter;
    String                  timestampFormat;
    String                  dateFormat;
    String                  timeFormat;
    boolean                 onExpression;
    String[]                exprTexts;
    ByteArray[]             exprBytecode;
    String[]                generatedClassNames;
    DataTypeDescriptor[]    indexColumnTypes;

    TableDescriptor         td;

    /**
     * Initializer for a CreateIndexNode
     *
     * @param unique            True means it's a unique index
     * @param indexType         The type of index
     * @param indexName         The name of the index
     * @param tableName         The name of the table the index will be on
     * @param expressionList    A list of index key expressions, in the order they
     *                            appear in the index.
     * @param properties        The optional properties list associated with the index.
     *
     * @exception StandardException        Thrown on error
     */
    public void init(
                    Object unique,
                    Object indexType,
                    Object indexName,
                    Object tableName,
                    Object expressionList,
                    Object excludeNulls,
                    Object excludeDefaults,
                    Object preSplit,
                    Object isLogicalKey,
                    Object sampling,
                    Object sampleFraction,
                    Object splitKeyPath,
                    Object columnDelimiter,
                    Object characterDelimite,
                    Object timestampFormat,
                    Object dateFormat,
                    Object timeFormat,
                    Object hfilePath,
                    Object properties)
        throws StandardException
    {
        initAndCheck(indexName);
        this.unique = (Boolean) unique;
        this.dd = getDataDictionary();
        this.indexType = (String) indexType;
        this.indexName = (TableName) indexName;
        this.tableName = (TableName) tableName;
        this.expressionList = (Vector<IndexExpression>) expressionList;
        this.properties = (Properties) properties;
        this.excludeNulls = (Boolean) excludeNulls;
        this.excludeDefaults = (Boolean) excludeDefaults;
        this.preSplit = (Boolean)preSplit;
        this.isLogicalKey = (Boolean)isLogicalKey;
        this.sampling = (Boolean)sampling;
        this.sampleFraction = sampleFraction!=null ? ((NumericConstantNode)sampleFraction).getValue().getDouble():0;
        this.splitKeyPath = splitKeyPath!=null ? ((CharConstantNode)splitKeyPath).getString() : null;
        this.columnDelimiter = columnDelimiter != null ? ((CharConstantNode)columnDelimiter).getString() : null;
        this.characterDelimiter = characterDelimite != null ? ((CharConstantNode)characterDelimite).getString() : null;
        this.timestampFormat = timestampFormat != null ? ((CharConstantNode)timestampFormat).getString() : null;
        this.dateFormat = dateFormat != null ? ((CharConstantNode)dateFormat).getString() : null;
        this.timeFormat = timeFormat != null ? ((CharConstantNode)timeFormat).getString() : null;
        this.hfilePath = hfilePath != null ? ((CharConstantNode)hfilePath).getString() : null;

        this.onExpression = isIndexOnExpression();
        int exprSize = this.onExpression ? this.expressionList.size() : 0;
        this.exprTexts = new String[exprSize];
        this.exprBytecode = new ByteArray[exprSize];
        this.generatedClassNames = new String[exprSize];
        this.indexColumnTypes = new DataTypeDescriptor[exprSize];
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
            return super.toString() +
                "unique: " + unique + "\n" +
                "indexType: " + indexType + "\n" +
                "indexName: " + indexName + "\n" +
                "tableName: " + tableName + "\n" +
                "properties: " + properties + "\n";
        }
        else
        {
            return "";
        }
    }

    public String statementToString()
    {
        return "CREATE INDEX";
    }


    public  boolean     getUniqueness()     { return unique; }
    public  String      getIndexType()      { return indexType; }
    public  TableName   getIndexName()      { return indexName; }
    public  UUID        getBoundTableID()   { return td.getUUID(); }
    public  Properties  getProperties()     { return properties; }
    public  TableName   getIndexTableName() { return tableName; }
    public  String[]    getColumnNames()    { return involvedColumnNames; }

    // get 1-based column ids
    public  int[]       getKeyColumnIDs()   { return boundColumnIDs; }
    public  boolean[]   getIsAscending()    { return isAscending; }

    // We inherit the generate() method from DDLStatementNode.

    /**
     * Bind this CreateIndexNode.  This means doing any static error
     * checking that can be done before actually creating the table.
     * For example, verifying that the column name list does not
     * contain any duplicate column names.
     *
     * @exception StandardException        Thrown on error
     */

    public void bindStatement() throws StandardException
    {
        int columnCount;
        td = getTableDescriptor(tableName);

        //If total number of indexes on the table so far is more than 32767, then we need to throw an exception
/*        if (td.getTotalNumberOfIndexes() > Limits.DB2_MAX_INDEXES_ON_TABLE)
        {
            throw StandardException.newException(SQLState.LANG_TOO_MANY_INDEXES_ON_TABLE,
                String.valueOf(td.getTotalNumberOfIndexes()),
                tableName,
                String.valueOf(Limits.DB2_MAX_INDEXES_ON_TABLE));
        }
*/
        /* Validate the column name list */
        verifyAndGetUniqueNames();

        columnCount = involvedColumnNames.length;
        boundColumnIDs = new int[ columnCount ];

        // Verify that the columns exist
        for (int i = 0; i < columnCount; i++)
        {
            ColumnDescriptor columnDescriptor;

            columnDescriptor = td.getColumnDescriptor(involvedColumnNames[i]);
            if (columnDescriptor == null)
            {
                throw StandardException.newException(SQLState.LANG_COLUMN_NOT_FOUND_IN_TABLE,
                        involvedColumnNames[i], tableName);
            }
            boundColumnIDs[ i ] = columnDescriptor.getPosition();
            
            // set this only once -- if just one column does is missing "not null" constraint in schema
            uniqueWithDuplicateNulls = (! uniqueWithDuplicateNulls && (unique && ! columnDescriptor.hasNonNullDefault()));

            // Don't allow a column to be created on a non-orderable type
            if ( ! columnDescriptor.getType().getTypeId().orderable(getClassFactory()))
            {
                // Note that this exception is not SQL state 429BX, same as DB2 but not compatible.
                throw StandardException.newException(SQLState.LANG_COLUMN_NOT_ORDERABLE_DURING_EXECUTION,
                        columnDescriptor.getType().getTypeId().getSQLTypeName());
            }
        }

        /* Check for number of key columns to be less than 16 to match DB2 */
/*        if (columnCount > 16)
            throw StandardException.newException(SQLState.LANG_TOO_MANY_INDEX_KEY_COLS);
*/
        /* See if the index already exists in this schema.
         * NOTE: We still need to check at execution time
         * since the index name is only unique to the schema,
         * not the table.
         */
//          if (dd.getConglomerateDescriptor(indexName.getTableName(), sd, false) != null)
//          {
//              throw StandardException.newException(SQLState.LANG_OBJECT_ALREADY_EXISTS_IN_OBJECT,
//                                                   "Index",
//                                                   indexName.getTableName(),
//                                                   "schema",
//                                                   sd.getSchemaName());
//          }

        if (onExpression) {
            bindIndexExpressions();
        }

        /* Statement is dependent on the TableDescriptor */
        getCompilerContext().createDependency(td);

    }

    private void bindIndexExpressions() throws StandardException {
        if (!onExpression)
            return;

        // fake a FromList to bind index expressions
        FromList fromList = (FromList) getNodeFactory().getNode(
                C_NodeTypes.FROM_LIST,
                getNodeFactory().doJoinOrderOptimization(),
                getContextManager());
        FromTable fromTable = (FromTable) getNodeFactory().getNode(
                C_NodeTypes.FROM_BASE_TABLE,
                tableName,
                null, null, null, false, null,
                getContextManager());
        fromList.addFromTable(fromTable);
        fromList.bindTables(
                dd,
                (FromList) getNodeFactory().getNode(
                        C_NodeTypes.FROM_LIST,
                        getNodeFactory().doJoinOrderOptimization(),
                        getContextManager()
                )
        );

        // The following checks are done based on DB2 documentation on CREATE INDEX statement:
        // https://www.ibm.com/support/knowledgecenter/en/SSEPGG_11.5.0/com.ibm.db2.luw.sql.ref.doc/doc/r0000919.html

        CollectingVisitor<QueryTreeNode> restrictionVisitor = new CollectingVisitor<>(
                Predicates.or(
                        Predicates.instanceOf(SubqueryNode.class),
                        Predicates.instanceOf(AggregateNode.class),
                        Predicates.instanceOf(StringAggregateNode.class),
                        Predicates.instanceOf(WrappedAggregateFunctionNode.class),
                        Predicates.instanceOf(WindowFunctionNode.class),
                        Predicates.instanceOf(CurrentDatetimeOperatorNode.class),  // current_time(), now(), etc.
                        Predicates.instanceOf(CurrentOfNode.class),                // current of cursor
                        Predicates.instanceOf(NextSequenceNode.class),
                        Predicates.instanceOf(SpecialFunctionNode.class),          // current session functions
                        Predicates.instanceOf(ParameterNode.class),
                        Predicates.instanceOf(LikeEscapeOperatorNode.class)        // like predicate
                ));

        HashSet<String> notAllowedFunctions = Stream.of(
                "rand", "random", "regexp_like", "instr", "locate", "stddev_pop", "stddev_samp"
        ).collect(Collectors.toCollection(HashSet::new));

        CollectingVisitor<QueryTreeNode> fnVisitor = new CollectingVisitor<>(
                Predicates.or(
                        Predicates.instanceOf(StaticMethodCallNode.class),
                        Predicates.instanceOf(TernaryOperatorNode.class)
                ));

        for (int i = 0; i < expressionList.size(); i++) {
            IndexExpression ie = expressionList.elementAt(i);
            ie.expression.bindExpression(fromList, new SubqueryList(), new ArrayList<AggregateNode>() {});

            // check invalid nodes
            ie.expression.accept(restrictionVisitor);
            if (!restrictionVisitor.getCollected().isEmpty()) {
                throw StandardException.newException(SQLState.LANG_INVALID_INDEX_EXPRESSION, ie.exprText);
            }
            restrictionVisitor.getCollected().clear();

            // check invalid functions (including UDFs)
            ie.expression.accept(fnVisitor);
            List<QueryTreeNode> fnList = fnVisitor.getCollected();
            for (QueryTreeNode fnNode : fnList) {
                if (fnNode instanceof StaticMethodCallNode) {
                    StaticMethodCallNode fn = (StaticMethodCallNode) fnNode;
                    if (!fn.isSystemFunction() || notAllowedFunctions.contains(fn.getMethodName().toLowerCase())) {
                        throw StandardException.newException(SQLState.LANG_INVALID_INDEX_EXPRESSION, ie.exprText);
                    }
                } else if (fnNode instanceof TernaryOperatorNode) {
                    TernaryOperatorNode fn = (TernaryOperatorNode) fnNode;
                    if (notAllowedFunctions.contains(fn.methodName.toLowerCase())) {
                        throw StandardException.newException(SQLState.LANG_INVALID_INDEX_EXPRESSION, ie.exprText);
                    }
                }
            }
            fnList.clear();

            // check return type
            DataTypeDescriptor dtd = ie.expression.getTypeServices();
            if (!dtd.getTypeId().orderable(getClassFactory())) {
                throw StandardException.newException(SQLState.LANG_INVALID_INDEX_EXPRESSION, ie.exprText);
            }
            indexColumnTypes[i] = dtd;
        }
    }

    /**
     * Return true if the node references SESSION schema tables (temporary or permanent)
     *
     * @return    true if references SESSION schema tables, else false
     *
     * @exception StandardException        Thrown on error
     */
    public boolean referencesSessionSchema()
        throws StandardException
    {
        //If create index is on a SESSION schema table, then return true.
        return isSessionSchema(td.getSchemaName());
    }

    /**
     * Create the Constant information that will drive the guts of Execution.
     *
     * @exception StandardException        Thrown on failure
     */
    public ConstantAction    makeConstantAction() throws StandardException
    {
        SchemaDescriptor sd = getSchemaDescriptor();
        int approxLength = 0;

        if (onExpression) {
            generateExecutableIndexExpression();
        }

        // bump the page size for the index,
        // if the approximate sizes of the columns in the key are
        // greater than the bump threshold.
        // Ideally, we would want to have atleast 2 or 3 keys fit in one page
        // With fix for beetle 5728, indexes on long types is not allowed
        // so we do not have to consider key columns of long types
        for (String columnName : involvedColumnNames) {
            ColumnDescriptor columnDescriptor = td.getColumnDescriptor(columnName);
            DataTypeDescriptor dts = columnDescriptor.getType();
            approxLength += dts.getTypeId().getApproximateLengthInBytes(dts);
        }


        if (approxLength > Property.IDX_PAGE_SIZE_BUMP_THRESHOLD)
        {

            if (((properties == null) ||
                 (properties.get(Property.PAGE_SIZE_PARAMETER) == null)) &&
                (PropertyUtil.getServiceProperty(
                     getLanguageConnectionContext().getTransactionCompile(),
                     Property.PAGE_SIZE_PARAMETER) == null))
            {
                // do not override the user's choice of page size, whether it
                // is set for the whole database or just set on this statement.

                if (properties == null)
                    properties = new Properties();

                properties.put(
                    Property.PAGE_SIZE_PARAMETER,
                    Property.PAGE_SIZE_DEFAULT_LONG);

            }
        }


        return getGenericConstantActionFactory().getCreateIndexConstantAction(
                false, // not for CREATE TABLE
                unique,
                uniqueWithDuplicateNulls, // UniqueWithDuplicateNulls Index is a unique
                indexType,                    //  index but with no "not null" constraint
                sd.getSchemaName(),            //  on column in schema
                indexName.getTableName(),
                tableName.getTableName(),
                td.getUUID(),
                involvedColumnNames,
                indexColumnTypes,
                isAscending,
                false,
                null,
                excludeNulls,
                excludeDefaults,
                preSplit,
                isLogicalKey,
                sampling,
                sampleFraction,
                splitKeyPath,
                hfilePath,
                columnDelimiter,
                characterDelimiter,
                timestampFormat,
                dateFormat,
                timeFormat,
                exprTexts,
                exprBytecode,
                generatedClassNames,
                properties);
    }

    /**
     * Check the uniqueness of the column names within the derived column list.
     *
     * @exception StandardException    Thrown if column list contains a
     *                                            duplicate name.
     */
    private void verifyAndGetUniqueNames()
                throws StandardException
    {
        int size = expressionList.size();
        isAscending = new boolean[size];

        if (onExpression) {
            CollectNodesVisitor cnv = new CollectNodesVisitor(ColumnReference.class);
            Vector<ColumnReference> columnReferenceList = new Vector<>();
            for (int i = 0; i < size; i++) {
                IndexExpression ie = expressionList.get(i);
                ie.expression.accept(cnv);
                Vector<ColumnReference> crList = cnv.getList();
                if (crList.isEmpty()) {
                    throw StandardException.newException(SQLState.LANG_INVALID_INDEX_EXPRESSION, ie.exprText);
                }
                columnReferenceList.addAll(crList);
                crList.clear();
                isAscending[i] = ie.isAscending;
                exprTexts[i] = ie.exprText;
            }

            HashSet<String> columnNameSet = new HashSet<>();
            for (ColumnReference cr : columnReferenceList) {
                columnNameSet.add(cr.getColumnName());
            }
            involvedColumnNames = new String[columnNameSet.size()];
            columnNameSet.toArray(involvedColumnNames);
        }
        else {
            Hashtable    ht = new Hashtable(size + 2, (float) .999);
            involvedColumnNames = new String[size];

            for (int index = 0; index < size; index++) {
                IndexExpression ie = (IndexExpression) expressionList.get(index);
                assert (ie.expression instanceof ColumnReference);
                involvedColumnNames[index] = ie.expression.getColumnName();
                isAscending[index] = ie.isAscending;

                Object object = ht.put(involvedColumnNames[index], involvedColumnNames[index]);

                if (object != null && ((String) object).equals(involvedColumnNames[index])) {
                    throw StandardException.newException(SQLState.LANG_DUPLICATE_COLUMN_NAME_CREATE_INDEX, involvedColumnNames[index]);
                }
            }
        }
    }

    /**
     * Check if this index is created on expressions
     * @return true if this index is created on expressions, false otherwise
     */
    private boolean isIndexOnExpression() {
        for (Object o : expressionList) {
            IndexExpression ie = (IndexExpression) o;
            if (!(ie.expression instanceof ColumnReference))
                return true;
        }
        return false;
    }

    private void generateExecutableIndexExpression() throws StandardException{
        if (!onExpression)
            return;

        assert exprBytecode.length == expressionList.size();
        assert generatedClassNames.length == expressionList.size();
        for (int i = 0; i < expressionList.size(); i++) {
            ExecutableIndexExpressionClassBuilder ieb = new ExecutableIndexExpressionClassBuilder(getCompilerContext());
            MethodBuilder mb = ieb.getExecuteMethod();
            expressionList.elementAt(i).expression.generateExpression(ieb, mb);
            ieb.finishRunExpressionMethod(i + 1);

            exprBytecode[i] = new ByteArray();
            GeneratedClass gc = ieb.getGeneratedClass(exprBytecode[i]);
            generatedClassNames[i] = gc.getName();
        }
    }
}
