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
 * All such Splice Machine modifications are Copyright 2012 - 2019 Splice Machine, Inc.,
 * and are licensed to you under the GNU Affero General Public License.
 */

package com.splicemachine.db.impl.sql.compile;

import com.carrotsearch.hppc.LongArrayList;
import com.carrotsearch.hppc.LongLongHashMap;
import com.carrotsearch.hppc.LongObjectHashMap;
import com.splicemachine.db.catalog.AliasInfo;
import com.splicemachine.db.catalog.TypeDescriptor;
import com.splicemachine.db.catalog.types.RowMultiSetImpl;
import com.splicemachine.db.catalog.types.SynonymAliasInfo;
import com.splicemachine.db.catalog.types.UserDefinedTypeIdImpl;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.reference.ClassName;
import com.splicemachine.db.iapi.reference.JDBC40Translation;
import com.splicemachine.db.iapi.reference.SQLState;
import com.splicemachine.db.iapi.services.classfile.VMOpcode;
import com.splicemachine.db.iapi.services.compiler.MethodBuilder;
import com.splicemachine.db.iapi.services.context.ContextManager;
import com.splicemachine.db.iapi.services.i18n.MessageService;
import com.splicemachine.db.iapi.services.loader.ClassFactory;
import com.splicemachine.db.iapi.services.loader.ClassInspector;
import com.splicemachine.db.iapi.services.sanity.SanityManager;
import com.splicemachine.db.iapi.sql.StatementType;
import com.splicemachine.db.iapi.sql.compile.*;
import com.splicemachine.db.iapi.sql.conn.LanguageConnectionContext;
import com.splicemachine.db.iapi.sql.depend.DependencyManager;
import com.splicemachine.db.iapi.sql.dictionary.AliasDescriptor;
import com.splicemachine.db.iapi.sql.dictionary.DataDictionary;
import com.splicemachine.db.iapi.sql.dictionary.SchemaDescriptor;
import com.splicemachine.db.iapi.sql.dictionary.TableDescriptor;
import com.splicemachine.db.iapi.sql.execute.ConstantAction;
import com.splicemachine.db.iapi.sql.execute.ExecutionFactory;
import com.splicemachine.db.iapi.types.DataTypeDescriptor;
import com.splicemachine.db.iapi.types.DataValueDescriptor;
import com.splicemachine.db.iapi.types.TypeId;
import com.splicemachine.db.impl.ast.CollectingVisitor;
import com.splicemachine.db.impl.ast.RSUtils;
import com.splicemachine.db.impl.sql.execute.GenericConstantActionFactory;
import com.splicemachine.db.impl.sql.execute.GenericExecutionFactory;
import org.apache.commons.lang3.SystemUtils;
import org.spark_project.guava.base.Predicates;
import org.spark_project.guava.base.Strings;

import java.sql.Types;
import java.util.*;

/**
 * QueryTreeNode is the root class for all query tree nodes. All
 * query tree nodes inherit from QueryTreeNode except for those that extend
 * QueryTreeNodeVector.
 */

public abstract class QueryTreeNode implements Node, Visitable{
    int depth;
    public static final int AUTOINCREMENT_START_INDEX=0;
    public static final int AUTOINCREMENT_INC_INDEX=1;
    public static final int AUTOINCREMENT_IS_AUTOINCREMENT_INDEX=2;
    //Parser uses this static field to make a note if the autoincrement column
    //is participating in create or alter table.
    public static final int AUTOINCREMENT_CREATE_MODIFY=3;
    /**
     * In Derby SQL Standard Authorization, views, triggers and constraints
     * execute with definer's privileges. Taking a specific eg of views
     * user1
     * create table t1 (c11 int);
     * create view v1 as select * from user1.t1;
     * grant select on v1 to user2;
     * user2
     * select * from user1.v1;
     * Running with definer's privileges mean that since user2 has select
     * privileges on view v1 owned by user1, then that is sufficient for user2
     * to do a select from view v1. View v1 underneath might access some
     * objects that user2 doesn't have privileges on, but that is not a problem
     * since views execute with definer's privileges. In order to implement this
     * behavior, when doing a select from view v1, we only want to check for
     * select privilege on view v1. While processing the underlying query for
     * view v1, we want to stop collecting the privilege requirements for the
     * query underneath. Following flag, isPrivilegeCollectionRequired is used
     * for this purpose. The flag will be true when we are the top level of view
     * and then it is turned off while we process the query underlying the view
     * v1.
     */
    boolean isPrivilegeCollectionRequired=true;
    private int beginOffset=-1;        // offset into SQL input of the substring
    // which this query node encodes.
    private int endOffset=-1;
    private int nodeType;
    private ContextManager cm;
    private LanguageConnectionContext lcc;
    private GenericConstantActionFactory constantActionFactory;

    /**
     * Format a node that has been converted to a String for printing
     * as part of a tree.  This method indents the String to the given
     * depth by inserting tabs at the beginning of the string, and also
     * after every newline.
     *
     * @param nodeString The node formatted as a String
     * @param depth      The depth to indent the given node
     * @return The node String reformatted with tab indentation
     */

    public static String formatNodeString(String nodeString,int depth){
        if(SanityManager.DEBUG){
            StringBuilder nodeStringBuffer=new StringBuilder(nodeString);
            int pos;
            char c;
            char[] indent=new char[depth];

			/*
            ** Form an array of tab characters for indentation.
			*/
            while(depth>0){
                indent[depth-1]='\t';
                depth--;
            }

			/* Indent the beginning of the string */
            nodeStringBuffer.insert(0,indent);

			/*
			** Look for newline characters, except for the last character.
			** We don't want to indent after the last newline.
			*/
            for(pos=0;pos<nodeStringBuffer.length()-1;pos++){
                c=nodeStringBuffer.charAt(pos);
                if(c=='\n'){
					/* Indent again after each newline */
                    nodeStringBuffer.insert(pos+1,indent);
                }
            }

            return nodeStringBuffer.toString();
        }else{
            return "";
        }
    }

    /**
     * Print a String for debugging
     *
     * @param outputString The String to print
     */

    public static void debugPrint(String outputString){
        if(SanityManager.DEBUG){
            SanityManager.GET_DEBUG_STREAM().print(outputString);
        }
    }

    public static TableName makeTableName
            (
                    NodeFactory nodeFactory,
                    ContextManager contextManager,
                    String schemaName,
                    String flatName
            )
            throws StandardException{
        return (TableName)nodeFactory.getNode
                (
                        C_NodeTypes.TABLE_NAME,
                        schemaName,
                        flatName,
                        contextManager
                );
    }

    /**
     * Bind the parameters of OFFSET n ROWS and FETCH FIRST n ROWS ONLY, if
     * any.
     *
     * @param offset     the OFFSET parameter, if any
     * @param fetchFirst the FETCH parameter, if any
     * @throws StandardException Thrown on error
     */
    public static void bindOffsetFetch(ValueNode offset,
                                       ValueNode fetchFirst)
            throws StandardException{

        if(offset instanceof ConstantNode){
            DataValueDescriptor dvd=((ConstantNode)offset).getValue();
            long val=dvd.getLong();

            if(val<0){
                throw StandardException.newException(
                        SQLState.LANG_INVALID_ROW_COUNT_OFFSET,
                        Long.toString(val));
            }
        }else if(offset instanceof ParameterNode){
            offset.
                    setType(new DataTypeDescriptor(
                            TypeId.getBuiltInTypeId(Types.BIGINT),
                            false /* ignored tho; ends up nullable,
                                     so we test for NULL at execute time */));
        }


        if(fetchFirst instanceof ConstantNode){
            DataValueDescriptor dvd=((ConstantNode)fetchFirst).getValue();
            long val=dvd.getLong();

            if(val<1){
                throw StandardException.newException(
                        SQLState.LANG_INVALID_ROW_COUNT_FIRST,
                        Long.toString(val));
            }
        }else if(fetchFirst instanceof ParameterNode){
            fetchFirst.
                    setType(new DataTypeDescriptor(
                            TypeId.getBuiltInTypeId(Types.BIGINT),
                            false /* ignored tho; ends up nullable,
                                     so we test for NULL at execute time*/));
        }
    }

    /**
     * Flush the debug stream out
     */
    protected static void debugFlush(){
        if(SanityManager.DEBUG){
            SanityManager.GET_DEBUG_STREAM().flush();
        }
    }


    QueryTreeNode(){
    }

    QueryTreeNode(ContextManager cm){
        this.cm=cm;

        if(SanityManager.DEBUG){
            SanityManager.ASSERT(cm!=null,
                    "cm not expected to be null");
        }
    }

    /**
     * Get the current ContextManager.
     *
     * @return The current ContextManager.
     */
    public final ContextManager getContextManager(){
        if(SanityManager.DEBUG){
            if(cm==null)
                SanityManager.THROWASSERT("Null context manager in QueryTreeNode of type :"+this.getClass());
        }
        return cm;
    }

    /**
     * Set the ContextManager for this node.
     *
     * @param cm The ContextManager.
     */
    public void setContextManager(ContextManager cm){
        this.cm=cm;

        if(SanityManager.DEBUG){
            SanityManager.ASSERT(cm!=null,
                    "cm not expected to be null");
        }
    }

    /**
     * Gets the NodeFactory for this database.
     *
     * @return the node factory for this database.
     */
    public final NodeFactory getNodeFactory(){
        return getLanguageConnectionContext().getLanguageConnectionFactory().
                getNodeFactory();
    }

    /**
     * Gets the constant action factory for this database.
     *
     * @return the constant action factory.
     */
    public final GenericConstantActionFactory getGenericConstantActionFactory(){
        if(constantActionFactory==null){
            GenericExecutionFactory execFactory=(GenericExecutionFactory)getExecutionFactory();
            constantActionFactory=execFactory.getConstantActionFactory();
        }

        return constantActionFactory;
    }

    public final ExecutionFactory getExecutionFactory(){
        return getLanguageConnectionContext().getLanguageConnectionFactory().getExecutionFactory();
    }

    /**
     * Gets the beginning offset of the SQL substring which this
     * query node represents.
     *
     * @return The beginning offset of the SQL substring. -1 means unknown.
     */
    public int getBeginOffset(){
        return beginOffset;
    }

    /**
     * Sets the beginning offset of the SQL substring which this
     * query node represents.
     *
     * @param    beginOffset    The beginning offset of the SQL substring.
     */
    public void setBeginOffset(int beginOffset){
        this.beginOffset=beginOffset;
    }

    /**
     * Gets the ending offset of the SQL substring which this
     * query node represents.
     *
     * @return The ending offset of the SQL substring. -1 means unknown.
     */
    public int getEndOffset(){
        return endOffset;
    }

    /**
     * Sets the ending offset of the SQL substring which this
     * query node represents.
     *
     * @param    endOffset    The ending offset of the SQL substring.
     */
    public void setEndOffset(int endOffset){
        this.endOffset=endOffset;
    }

    /**
     * Print this tree for debugging purposes.  This recurses through
     * all the sub-nodes and prints them indented by their depth in
     * the tree.
     */

    public void treePrint(){
        if(SanityManager.DEBUG){
            debugPrint(nodeHeader());
            String thisStr=formatNodeString(this.toString(),0);

            if(containsInfo(thisStr) && !SanityManager.DEBUG_ON("DumpBrief")){
                debugPrint(thisStr);
            }

            printSubNodes(0);
            debugFlush();
        }
    }

    /**
     * Print this tree for debugging purposes.  This recurses through
     * all the sub-nodes and prints them indented by their depth in
     * the tree, starting with the given indentation.
     *
     * @param depth The depth of this node in the tree, thus,
     *              the amount to indent it when printing it.
     */

    public void treePrint(int depth){
        if(SanityManager.DEBUG){
            Map printed= getLanguageConnectionContext().getPrintedObjectsMap();

            if(printed.containsKey(this)){
                debugPrint(formatNodeString(nodeHeader(),depth));
                debugPrint(formatNodeString("***truncated***\n",depth));
            }else{
                //noinspection unchecked
                printed.put(this,null);
                debugPrint(formatNodeString(nodeHeader(),depth));
                String thisStr=formatNodeString(this.toString(),depth);

                if(containsInfo(thisStr) &&
                        !SanityManager.DEBUG_ON("DumpBrief")){
                    debugPrint(thisStr);
                }

                if(thisStr.charAt(thisStr.length()-1)!='\n'){
                    debugPrint("\n");
                }

                printSubNodes(depth);
            }

        }
    }

    /**
     * Print the sub-nodes of this node.
     * <p/>
     * Each sub-class of QueryTreeNode is expected to provide its own
     * printSubNodes() method.  In each case, it calls super.printSubNodes(),
     * passing along its depth, to get the sub-nodes of the super-class.
     * Then it prints its own sub-nodes by calling treePrint() on each
     * of its members that is a type of QueryTreeNode.  In each case where
     * it calls treePrint(), it should pass "depth + 1" to indicate that
     * the sub-node should be indented one more level when printing.
     * Also, it should call printLabel() to print the name of each sub-node
     * before calling treePrint() on the sub-node, so that the reader of
     * the printed tree can tell what the sub-node is.
     * <p/>
     * This printSubNodes() exists in here merely to act as a backstop.
     * In other words, the calls to printSubNodes() move up the type
     * hierarchy, and in this node the calls stop.
     * <p/>
     * I would have liked to put the call to super.printSubNodes() in
     * this super-class, but Java resolves "super" statically, so it
     * wouldn't get to the right super-class.
     *
     * @param depth The depth to indent the sub-nodes
     */
    public void printSubNodes(int depth){
    }

    /**
     * Format this node as a string
     * <p/>
     * Each sub-class of QueryTreeNode should implement its own toString()
     * method.  In each case, toString() should format the class members
     * that are not sub-types of QueryTreeNode (printSubNodes() takes care
     * of following the references to sub-nodes, and toString() takes care
     * of all members that are not sub-nodes).  Newlines should be used
     * liberally - one good way to do this is to have a newline at the
     * end of each formatted member.  It's also a good idea to put the
     * name of each member in front of the formatted value.  For example,
     * the code might look like:
     * <p/>
     * "memberName: " + memberName + "\n" + ...
     * <p/>
     * Vector members containing subclasses of QueryTreeNode should subclass
     * QueryTreeNodeVector. Such subclasses form a special case: These classes
     * should not implement printSubNodes, since there is generic handling in
     * QueryTreeNodeVector.  They should only implement toString if they
     * contain additional members.
     *
     * @return This node formatted as a String
     */
    @Override
    public String toString(){
        return "";
    }

    /**
     * Print the given label at the given indentation depth.
     *
     * @param depth The depth of indentation to use when printing
     *              the label
     * @param label The String to print
     */

    public void printLabel(int depth,String label){
        if(SanityManager.DEBUG){
            debugPrint(formatNodeString(label,depth));
        }
    }

    /**
     * Return true if the node references SESSION schema tables (temporary or permanent)
     *
     * @throws StandardException Thrown on error
     * @return true if references SESSION schema tables, else false
     */
    public boolean referencesSessionSchema()
            throws StandardException{
        return false;
    }

    /**
     * Return true from this method means that we need to collect privilege
     * requirement for this node. For following cases, this method will
     * return true.
     * 1)execute view - collect privilege to access view but do not collect
     * privilege requirements for objects accessed by actual view uqery
     * 2)execute select - collect privilege requirements for objects accessed
     * by select statement
     * 3)create view -  collect privileges for select statement : the select
     * statement for create view falls under 2) category above.
     *
     * @return true if need to collect privilege requirement for this node
     */
    public boolean isPrivilegeCollectionRequired(){
        return (isPrivilegeCollectionRequired);
    }

    /**
     * Parameter info is stored in the compiler context.
     * Hide this from the callers.
     *
     * @throws StandardException on error
     * @return null
     */
    public DataTypeDescriptor[] getParameterTypes()
            throws StandardException{
        return getCompilerContext().getParameterTypes();
    }

    /**
     * This creates a class that will do the work that's constant
     * across all Executions of a PreparedStatement. It's up to
     * our subclasses to override this method if they need to compile
     * constant actions into PreparedStatements.
     *
     * @throws StandardException Thrown on failure
     */
    public ConstantAction makeConstantAction() throws StandardException{
        return null;
    }

    /**
     * Get the DataDictionary
     *
     * @return The DataDictionary
     */
    public final DataDictionary getDataDictionary(){
        return getLanguageConnectionContext().getDataDictionary();
    }

    public final DependencyManager getDependencyManager(){
        return getDataDictionary().getDependencyManager();
    }

    /**
     * Get the CompilerContext
     *
     * @return The CompilerContext
     */
    public final CompilerContext getCompilerContext(){
        return (CompilerContext)getContextManager().
                getContext(CompilerContext.CONTEXT_ID);
    }

    /**
     * Accept a visitor, and call {@code v.visit()} on child nodes as
     * necessary. Sub-classes should not override this method, but instead
     * override the {@link #acceptChildren(Visitor)} method.
     *
     * @param visitor the visitor
     */
    @Override
    public final Visitable accept(Visitor visitor, QueryTreeNode parent) throws StandardException{
        final boolean childrenFirst= visitor.visitChildrenFirst(this);
        final boolean skipChildren= visitor.skipChildren(this);

        if(childrenFirst && !skipChildren && !visitor.stopTraversal()){
            acceptChildren(visitor);
        }

        final Visitable ret = visitor.stopTraversal() ? this : visitor.visit(this, parent);

        if(!childrenFirst && !skipChildren && !visitor.stopTraversal()){
            acceptChildren(visitor);
        }

        return ret;
    }

    @Override
    public final Visitable accept(Visitor visitor) throws StandardException{
        return accept(visitor, null);
    }

    /**
     * Accept a visitor on all child nodes. All sub-classes that add fields
     * that should be visited, should override this method and call
     * {@code accept(v)} on all visitable fields, as well as
     * {@code super.acceptChildren(v)} to make sure all visitable fields
     * defined by the super-class are accepted too.
     *
     * @param v the visitor
     */
    public void acceptChildren(Visitor v) throws StandardException{
        // no children
    }

    public boolean foundString(String[] list,String search){
        if(list==null){
            return false;
        }

        for(String str : list){
            if(str.equals(search)){
                return true;
            }
        }
        return false;
    }

    public int getConstantNodeType(DataTypeDescriptor type) throws StandardException {
        int constantNodeType;
        switch(type.getTypeId().getJDBCTypeId()){
            case Types.VARCHAR:
                constantNodeType=C_NodeTypes.VARCHAR_CONSTANT_NODE;
                break;
            case Types.CHAR:
                constantNodeType=C_NodeTypes.CHAR_CONSTANT_NODE;
                break;
            case Types.TINYINT:
                constantNodeType=C_NodeTypes.TINYINT_CONSTANT_NODE;
                break;
            case Types.SMALLINT:
                constantNodeType=C_NodeTypes.SMALLINT_CONSTANT_NODE;
                break;
            case Types.INTEGER:
                constantNodeType=C_NodeTypes.INT_CONSTANT_NODE;
                break;
            case Types.BIGINT:
                constantNodeType=C_NodeTypes.LONGINT_CONSTANT_NODE;
                break;
            case Types.REAL:
                constantNodeType=C_NodeTypes.FLOAT_CONSTANT_NODE;
                break;
            case Types.DOUBLE:
                constantNodeType=C_NodeTypes.DOUBLE_CONSTANT_NODE;
                break;
            case Types.NUMERIC:
            case Types.DECIMAL:
                constantNodeType=C_NodeTypes.DECIMAL_CONSTANT_NODE;
                break;
            case Types.DATE:
            case Types.TIME:
            case Types.TIMESTAMP:
                constantNodeType=C_NodeTypes.USERTYPE_CONSTANT_NODE;
                break;
            case Types.BINARY:
                constantNodeType=C_NodeTypes.BIT_CONSTANT_NODE;
                break;
            case Types.VARBINARY:
                constantNodeType=C_NodeTypes.VARBIT_CONSTANT_NODE;
                break;
            case Types.LONGVARCHAR:
                constantNodeType=C_NodeTypes.LONGVARCHAR_CONSTANT_NODE;
                break;
            case Types.CLOB:
                constantNodeType=C_NodeTypes.CLOB_CONSTANT_NODE;
                break;
            case Types.LONGVARBINARY:
                constantNodeType=C_NodeTypes.LONGVARBIT_CONSTANT_NODE;
                break;
            case Types.BLOB:
                constantNodeType=C_NodeTypes.BLOB_CONSTANT_NODE;
                break;
            case JDBC40Translation.SQLXML:
                constantNodeType=C_NodeTypes.XML_CONSTANT_NODE;
                break;
            case Types.BOOLEAN:
                constantNodeType=C_NodeTypes.BOOLEAN_CONSTANT_NODE;
                break;
            case Types.ARRAY:
                constantNodeType=C_NodeTypes.ARRAY_CONSTANT_NODE;
                break;
            default:
                if(type.getTypeId().userType()){
                    constantNodeType=C_NodeTypes.USERTYPE_CONSTANT_NODE;
                }else{
                    throw StandardException.newException(SQLState.LANG_NONULL_DATATYPE, type.getTypeId().getSQLTypeName());
                }
        }
        return constantNodeType;
    }

    public ValueNode getConstantNode(DataTypeDescriptor type, DataValueDescriptor value) throws StandardException {
        ConstantNode constantNode;
        int constantNodeType = getConstantNodeType(type);
        if (constantNodeType == C_NodeTypes.USERTYPE_CONSTANT_NODE) {
            constantNode = (ConstantNode) getNodeFactory().getNode(constantNodeType, value, cm);
        } else {
            constantNode = (ConstantNode) getNodeFactory().getNode(constantNodeType, type.getTypeId(), cm);
            constantNode.setValue(value);
            constantNode.setType(type.getNullabilityType(false));
        }
        return constantNode;

    }
    /**
     * Get a ConstantNode to represent a typed null value.
     *
     * @param type Type of the null node.
     * @throws StandardException Thrown on error
     * @return A ConstantNode with the specified type, and a value of null
     */
    public ValueNode getNullNode(DataTypeDescriptor type) throws StandardException{
        int constantNodeType = getConstantNodeType(type);

        ValueNode constantNode=(ValueNode) getNodeFactory().getNode(constantNodeType, type.getTypeId(), cm);

        constantNode.setType(type.getNullabilityType(true));

        return constantNode;
    }

    /**
     * Translate a Default node into a default value, given a type descriptor.
     *
     * @param typeDescriptor A description of the required data type.
     * @throws StandardException Thrown on error
     */
    public DataValueDescriptor convertDefaultNode(DataTypeDescriptor typeDescriptor) throws StandardException{
		/*
		** Override in cases where node type
		** can be converted to default value.
		*/
        return null;
    }

    /**
     * Initialize a query tree node.
     *
     * @throws StandardException Thrown on error
     */
    @Override
    public void init(Object arg1) throws StandardException{
        if(SanityManager.DEBUG){
            SanityManager.THROWASSERT("Single-argument init() not implemented for "+getClass().getName());
        }
    }

    /**
     * Initialize a query tree node.
     *
     * @throws StandardException Thrown on error
     */
    @Override
    public void init(Object arg1, Object arg2) throws StandardException{
        if(SanityManager.DEBUG){
            SanityManager.THROWASSERT("Two-argument init() not implemented for "+getClass().getName());
        }
    }

    /**
     * Initialize a query tree node.
     *
     * @throws StandardException Thrown on error
     */
    @Override
    public void init(Object arg1, Object arg2, Object arg3) throws StandardException{
        if(SanityManager.DEBUG){
            SanityManager.THROWASSERT("Three-argument init() not implemented for "+getClass().getName());
        }
    }

    /**
     * Initialize a query tree node.
     *
     * @throws StandardException Thrown on error
     */
    @Override
    public void init(Object arg1, Object arg2, Object arg3, Object arg4) throws StandardException{
        if(SanityManager.DEBUG){
            SanityManager.THROWASSERT("Four-argument init() not implemented for "+getClass().getName());
        }
    }

    /**
     * Initialize a query tree node.
     *
     * @throws StandardException Thrown on error
     */
    @Override
    public void init(Object arg1, Object arg2, Object arg3, Object arg4, Object arg5) throws StandardException{
        if(SanityManager.DEBUG){
            SanityManager.THROWASSERT("Five-argument init() not implemented for "+getClass().getName());
        }
    }

    /**
     * Initialize a query tree node.
     *
     * @throws StandardException Thrown on error
     */
    @Override
    public void init(Object arg1, Object arg2, Object arg3, Object arg4, Object arg5, Object arg6) throws StandardException{
        if(SanityManager.DEBUG){
            SanityManager.THROWASSERT("Six-argument init() not implemented for "+getClass().getName());
        }
    }

    /**
     * Initialize a query tree node.
     *
     * @throws StandardException Thrown on error
     */
    @Override
    public void init(Object arg1,
                     Object arg2,
                     Object arg3,
                     Object arg4,
                     Object arg5,
                     Object arg6,
                     Object arg7) throws StandardException{
        if(SanityManager.DEBUG){
            SanityManager.THROWASSERT("Seven-argument init() not implemented for "+getClass().getName());
        }
    }

    /**
     * Initialize a query tree node.
     *
     * @throws StandardException Thrown on error
     */
    @Override
    public void init(Object arg1,
                     Object arg2,
                     Object arg3,
                     Object arg4,
                     Object arg5,
                     Object arg6,
                     Object arg7,
                     Object arg8) throws StandardException{
        if(SanityManager.DEBUG){
            SanityManager.THROWASSERT("Eight-argument init() not implemented for "+getClass().getName());
        }
    }

    /**
     * Initialize a query tree node.
     *
     * @throws StandardException Thrown on error
     */
    @Override
    public void init(Object arg1,
                     Object arg2,
                     Object arg3,
                     Object arg4,
                     Object arg5,
                     Object arg6,
                     Object arg7,
                     Object arg8,
                     Object arg9) throws StandardException{
        if(SanityManager.DEBUG){
            SanityManager.THROWASSERT("Nine-argument init() not implemented for "+getClass().getName());
        }
    }

    /**
     * Initialize a query tree node.
     *
     * @throws StandardException Thrown on error
     */
    @Override
    public void init(Object arg1,
                     Object arg2,
                     Object arg3,
                     Object arg4,
                     Object arg5,
                     Object arg6,
                     Object arg7,
                     Object arg8,
                     Object arg9,
                     Object arg10) throws StandardException{
        if(SanityManager.DEBUG){
            SanityManager.THROWASSERT("Ten-argument init() not implemented for "+getClass().getName());
        }
    }

    /**
     * Initialize a query tree node.
     *
     * @throws StandardException Thrown on error
     */
    @Override
    public void init(Object arg1,
                     Object arg2,
                     Object arg3,
                     Object arg4,
                     Object arg5,
                     Object arg6,
                     Object arg7,
                     Object arg8,
                     Object arg9,
                     Object arg10,
                     Object arg11) throws StandardException{
        if(SanityManager.DEBUG){
            SanityManager.THROWASSERT("Eleven-argument init() not implemented for "+getClass().getName());
        }
    }

    /**
     * Initialize a query tree node.
     *
     * @throws StandardException Thrown on error
     */
    @Override
    public void init(Object arg1,
                     Object arg2,
                     Object arg3,
                     Object arg4,
                     Object arg5,
                     Object arg6,
                     Object arg7,
                     Object arg8,
                     Object arg9,
                     Object arg10,
                     Object arg11,
                     Object arg12) throws StandardException{
        if(SanityManager.DEBUG){
            SanityManager.THROWASSERT("Twelve-argument init() not implemented for "+getClass().getName());
        }
    }

    /**
     * Initialize a query tree node.
     *
     * @throws StandardException Thrown on error
     */
    @Override
    public void init(Object arg1,
                     Object arg2,
                     Object arg3,
                     Object arg4,
                     Object arg5,
                     Object arg6,
                     Object arg7,
                     Object arg8,
                     Object arg9,
                     Object arg10,
                     Object arg11,
                     Object arg12,
                     Object arg13) throws StandardException{
        if(SanityManager.DEBUG){
            SanityManager.THROWASSERT("Thirteen-argument init() not implemented for "+getClass().getName());
        }
    }

    /**
     * Initialize a query tree node.
     *
     * @throws StandardException Thrown on error
     */
    @Override
    public void init(Object arg1,
                     Object arg2,
                     Object arg3,
                     Object arg4,
                     Object arg5,
                     Object arg6,
                     Object arg7,
                     Object arg8,
                     Object arg9,
                     Object arg10,
                     Object arg11,
                     Object arg12,
                     Object arg13,
                     Object arg14) throws StandardException{
        if(SanityManager.DEBUG){
            SanityManager.THROWASSERT("Fourteen-argument init() not implemented for "+getClass().getName());
        }
    }

    @Override
    public void init(Object arg1,
                     Object arg2,
                     Object arg3,
                     Object arg4,
                     Object arg5,
                     Object arg6,
                     Object arg7,
                     Object arg8,
                     Object arg9,
                     Object arg10,
                     Object arg11,
                     Object arg12,
                     Object arg13,
                     Object arg14,
                     Object arg15,
                     Object arg16,
                     Object arg17,
                     Object arg18,
                     Object arg19) throws StandardException{
        if(SanityManager.DEBUG){
            SanityManager.THROWASSERT("Seventeen-argument init() not implemented for "+getClass().getName());
        }
    }

    public TableName makeTableName ( String schemaName, String flatName ) throws StandardException{
        return makeTableName(getNodeFactory(),getContextManager(),schemaName,flatName);
    }

    public boolean isAtomic() throws StandardException{
        if(SanityManager.DEBUG){
            SanityManager.THROWASSERT("isAtomic should not be called for this  class: "+getClass().getName());
        }

        return false;
    }

    public Object getCursorInfo() throws StandardException{
        return null;
    }

    /**
     * Get the descriptor for the named schema. If the schemaName
     * parameter is NULL, it gets the descriptor for the current
     * compilation schema.
     * <p/>
     * QueryTreeNodes must obtain schemas using this method or the two argument
     * version of it. This is to ensure that the correct default compliation schema
     * is returned and to allow determination of if the statement being compiled
     * depends on the current schema.
     * <p/>
     * Schema descriptors include authorization ids and schema ids.
     * SQL92 allows a schema to specify a default character set - we will
     * not support this.  Will check default schema for a match
     * before scanning a system table.
     *
     * @param schemaName The name of the schema we're interested in.
     *                   If the name is NULL, get the descriptor for the
     *                   current compilation schema.
     * @throws StandardException Thrown on error
     * @return The descriptor for the schema.
     */
    public final SchemaDescriptor getSchemaDescriptor(String schemaName)
            throws StandardException{
        //return getSchemaDescriptor(schemaName, schemaName != null);
        return getSchemaDescriptor(schemaName,true);
    }

    /**
     * Resolve table/view reference to a synonym. May have to follow a synonym chain.
     *
     * @throws StandardException Thrown on error
     * @param    tabName to match for a synonym
     * @return Synonym TableName if a match is found, NULL otherwise.
     */
    public TableName resolveTableToSynonym(TableName tabName) throws StandardException{
        DataDictionary dd=getDataDictionary();
        String nextSynonymTable=tabName.getTableName();
        String nextSynonymSchema=tabName.getSchemaName();
        boolean found=false;
        CompilerContext cc=getCompilerContext();

        // Circular synonym references should have been detected at the DDL time, so
        // the following loop shouldn't loop forever.
        for(;;){
            SchemaDescriptor nextSD=getSchemaDescriptor(nextSynonymSchema,false);
            if(nextSD==null || nextSD.getUUID()==null)
                break;

            AliasDescriptor nextAD=dd.getAliasDescriptor(nextSD.getUUID().toString(),
                    nextSynonymTable,AliasInfo.ALIAS_NAME_SPACE_SYNONYM_AS_CHAR);
            if(nextAD==null)
                break;

			/* Query is dependent on the AliasDescriptor */
            cc.createDependency(nextAD);

            found=true;
            SynonymAliasInfo info=((SynonymAliasInfo)nextAD.getAliasInfo());
            nextSynonymTable=info.getSynonymTable();
            nextSynonymSchema=info.getSynonymSchema();
        }

        if(!found)
            return null;

        TableName tableName=new TableName();
        tableName.init(nextSynonymSchema,nextSynonymTable);
        return tableName;
    }

    /**
     * set the Information gathered from the parent table that is
     * required to peform a referential action on dependent table.
     */
    public void setRefActionInfo(long fkIndexConglomId,
                                 int[] fkColArray,
                                 String parentResultSetId,
                                 boolean dependentScan){
        if(SanityManager.DEBUG){
            SanityManager.THROWASSERT(
                    "setRefActionInfo() not expected to be called for "+
                            getClass().getName());
        }
    }

    /**
     * Bind time logic. Raises an error if this ValueNode, once compiled, returns
     * unstable results AND if we're in a context where unstable results are
     * forbidden.
     * <p/>
     * Called by children who may NOT appear in the WHERE subclauses of ADD TABLE clauses.
     *
     * @throws StandardException Thrown on error
     * @param    fragmentType    Type of fragment as a String, for inclusion in error messages.
     * @param    fragmentBitMask    Type of fragment as a bitmask of possible fragment types
     */
    public void checkReliability(String fragmentType,int fragmentBitMask)
            throws StandardException{
        // if we're in a context that forbids unreliable fragments, raise an error
        if((getCompilerContext().getReliability()&fragmentBitMask)!=0){
            throwReliabilityException(fragmentType,fragmentBitMask);
        }
    }

    /**
     * Bind time logic. Raises an error if this ValueNode, once compiled, returns
     * unstable results AND if we're in a context where unstable results are
     * forbidden.
     * <p/>
     * Called by children who may NOT appear in the WHERE subclauses of ADD TABLE clauses.
     *
     * @throws StandardException Thrown on error
     * @param    fragmentBitMask    Type of fragment as a bitmask of possible fragment types
     * @param    fragmentType    Type of fragment as a String, to be fetch for the error message.
     */
    public void checkReliability(int fragmentBitMask,String fragmentType)
            throws StandardException{
        // if we're in a context that forbids unreliable fragments, raise an error
        if((getCompilerContext().getReliability()&fragmentBitMask)!=0){
            String fragmentTypeTxt=MessageService.getTextMessage(fragmentType);
            throwReliabilityException(fragmentTypeTxt,fragmentBitMask);
        }
    }

    /**
     * Bind a UDT. This involves looking it up in the DataDictionary and filling
     * in its class name.
     *
     * @param originalDTD A datatype: might be an unbound UDT and might not be
     * @return The bound UDT if originalDTD was an unbound UDT; otherwise returns originalDTD.
     */
    public DataTypeDescriptor bindUserType(DataTypeDescriptor originalDTD) throws StandardException{
        // if the type is a table type, then we need to bind its user-typed columns
        if(originalDTD.getCatalogType().isRowMultiSet()){
            return bindRowMultiSet(originalDTD);
        }

        // nothing to do if this is not a user defined type
        if(!originalDTD.getTypeId().userType()){
            return originalDTD;
        }

        UserDefinedTypeIdImpl userTypeID=(UserDefinedTypeIdImpl)originalDTD.getTypeId().getBaseTypeId();

        // also nothing to do if the type has already been resolved
        if(userTypeID.isBound()){
            return originalDTD;
        }

        // ok, we have an unbound UDT. lookup this type in the data dictionary

        DataDictionary dd=getDataDictionary();
        SchemaDescriptor typeSchema=getSchemaDescriptor(userTypeID.getSchemaName());
        char udtNameSpace=AliasInfo.ALIAS_NAME_SPACE_UDT_AS_CHAR;
        String unqualifiedTypeName=userTypeID.getUnqualifiedName();
        AliasDescriptor ad=dd.getAliasDescriptor(typeSchema.getUUID().toString(),unqualifiedTypeName,udtNameSpace);

        if(ad==null){
            throw StandardException.newException(SQLState.LANG_OBJECT_NOT_FOUND,AliasDescriptor.getAliasType(udtNameSpace),unqualifiedTypeName);
        }

        createTypeDependency(ad);

        return new DataTypeDescriptor(
                TypeId.getUserDefinedTypeId(typeSchema.getSchemaName(), unqualifiedTypeName, ad.getJavaClassName()),
                originalDTD.isNullable()
        );
    }

    /**
     * Bind user defined types as necessary
     */
    public TypeDescriptor bindUserCatalogType(TypeDescriptor td) throws StandardException{
        // if this is a user defined type, resolve the Java class name
        if(!td.isUserDefinedType()){
            return td;
        }else{
            DataTypeDescriptor dtd=DataTypeDescriptor.getType(td);

            dtd=bindUserType(dtd);
            return dtd.getCatalogType();
        }
    }

    /**
     * Bind the UDTs in a table type.
     *
     * @param originalDTD A datatype: might be an unbound UDT and might not be
     * @return The bound table type if originalDTD was an unbound table type; otherwise returns originalDTD.
     */
    public DataTypeDescriptor bindRowMultiSet(DataTypeDescriptor originalDTD) throws StandardException{
        if(!originalDTD.getCatalogType().isRowMultiSet()){
            return originalDTD;
        }

        RowMultiSetImpl originalMultiSet=(RowMultiSetImpl)originalDTD.getTypeId().getBaseTypeId();
        TypeDescriptor[] columnTypes=originalMultiSet.getTypes();
        int columnCount=columnTypes.length;

        for(int i=0;i<columnCount;i++){
            columnTypes[i]=bindUserCatalogType(columnTypes[i]);
        }

        return originalDTD;
    }

    /**
     * Declare a dependency on a type and check that you have privilege to use
     * it. This is only used if the type is an ANSI UDT.
     *
     * @param dtd Type which may have a dependency declared on it.
     */
    public void createTypeDependency(DataTypeDescriptor dtd) throws StandardException{
        AliasDescriptor ad=getDataDictionary().getAliasDescriptorForUDT(null,dtd);

        if(ad!=null){
            createTypeDependency(ad);
        }
    }

    /**
     * OR in more reliability bits and return the old reliability value.
     */
    public int orReliability(int newBits){
        CompilerContext cc=getCompilerContext();

        int previousReliability=cc.getReliability();

        cc.setReliability(previousReliability|newBits);

        return previousReliability;
    }

    /**
     * Get the ClassFactory to use with this database.
     */
    protected final ClassFactory getClassFactory(){
        return getLanguageConnectionContext().getLanguageConnectionFactory().getClassFactory();
    }

    /**
     * Gets the LanguageConnectionContext for this connection.
     *
     * @return the lcc for this connection
     */
    public final LanguageConnectionContext getLanguageConnectionContext(){
        if(lcc==null){
            lcc=(LanguageConnectionContext)getContextManager().getContext(LanguageConnectionContext.CONTEXT_ID);
        }
        return lcc;
    }

    /**
     * Return header information for debug printing of this query
     * tree node.
     *
     * @return Header information for debug printing of this query
     * tree node.
     */

    protected String nodeHeader(){
        if(SanityManager.DEBUG){
            return "\n"+this.getClass().getName()+'@'+
                    Integer.toHexString(hashCode())+"\n";
        }else{
            return "";
        }
    }

    /**
     * Do the code generation for this node.  This is a place-holder
     * method - it should be over-ridden in the sub-classes.
     *
     * @param acb The ActivationClassBuilder for the class being built
     * @param mb  The method for the generated code to go into
     * @throws StandardException Thrown on error
     */

    protected void generate(
            ActivationClassBuilder acb,
            MethodBuilder mb)
            throws StandardException{
        throw StandardException.newException(SQLState.LANG_UNABLE_TO_GENERATE, this.nodeHeader());
    }

    protected int getNodeType(){
        return nodeType;
    }

    /**
     * Set the node type for this node.
     *
     * @param nodeType The node type.
     */
    public void setNodeType(int nodeType){
        this.nodeType=nodeType;
    }

    /**
     * For final nodes, return whether or not
     * the node represents the specified nodeType.
     *
     * @param nodeType The nodeType of interest.
     * @return Whether or not
     * the node represents the specified nodeType.
     */
    protected boolean isInstanceOf(int nodeType){
        return (this.nodeType==nodeType);
    }

    /**
     * Get the TypeCompiler associated with the given TypeId
     *
     * @param typeId The TypeId to get a TypeCompiler for
     * @return The corresponding TypeCompiler
     */
    protected final TypeCompiler getTypeCompiler(TypeId typeId){
        return getCompilerContext().getTypeCompilerFactory().getTypeCompiler(typeId);
    }

    /**
     * Get the int value of a Property
     *
     * @param value Property value as a String
     * @param key   Key value of property
     * @throws StandardException Thrown on failure
     * @return The int value of the property
     */
    protected int getIntProperty(String value,String key) throws StandardException{
        int intVal;
        try{
            intVal=Integer.parseInt(value);
        }catch(NumberFormatException nfe){
            throw StandardException.newException(SQLState.LANG_INVALID_NUMBER_FORMAT_FOR_OVERRIDE,
                    value,key);
        }
        return intVal;
    }

    /**
     * Return the type of statement, something from
     * StatementType.
     *
     * @return the type of statement
     */
    protected int getStatementType(){
        return StatementType.UNKNOWN;
    }

    /**
     * Get the descriptor for the named table within the given schema.
     * If the schema parameter is NULL, it looks for the table in the
     * current (default) schema. Table descriptors include object ids,
     * object types (table, view, etc.)
     * If the schema is SESSION, then before looking into the data dictionary
     * for persistent tables, it first looks into LCC for temporary tables.
     * If no temporary table tableName found for the SESSION schema, then it goes and
     * looks through the data dictionary for persistent table
     * We added getTableDescriptor here so that we can look for non data dictionary
     * tables(ie temp tables) here. Any calls to getTableDescriptor in data dictionary
     * should be only for persistent tables
     *
     * @param tableName The name of the table to get the descriptor for
     * @param schema    The descriptor for the schema the table lives in.
     *                  If null, use the current (default) schema.
     * @throws StandardException Thrown on failure
     * @return The descriptor for the table, null if table does not
     * exist.
     */
    protected final TableDescriptor getTableDescriptor(String tableName,
                                                       SchemaDescriptor schema) throws StandardException{
        TableDescriptor retval;

        //Following if means we are dealing with SESSION schema.
        if(isSessionSchema(schema)){
            //First we need to look in the list of temporary tables to see if this table is a temporary table.
            retval=getLanguageConnectionContext().getTableDescriptorForDeclaredGlobalTempTable(tableName);
            if(retval!=null)
                return retval; //this is a temporary table
        }

        //Following if means we are dealing with SESSION schema and we are dealing with in-memory schema (ie there is no physical SESSION schema)
        //If following if is true, it means SESSION.table is not a declared table & it can't be physical SESSION.table
        //because there is no physical SESSION schema
        if(schema.getUUID()==null)
            return null;

        //it is not a temporary table, so go through the data dictionary to find the physical persistent table
        TableDescriptor td=getDataDictionary().getTableDescriptor(tableName,schema,
                this.getLanguageConnectionContext().getTransactionCompile());
        if(td==null || td.isSynonymDescriptor())
            return null;

        return td;
    }

    /**
     * Checks if the passed schema descriptor is for SESSION schema
     *
     * @return true if the passed schema descriptor is for SESSION schema
     */
    final boolean isSessionSchema(SchemaDescriptor sd){
        return isSessionSchema(sd.getSchemaName());
    }

    /**
     * Checks if the passed schema name is for SESSION schema
     *
     * @return true if the passed schema name is for SESSION schema
     */
    final boolean isSessionSchema(String schemaName){
        return SchemaDescriptor.STD_DECLARED_GLOBAL_TEMPORARY_TABLES_SCHEMA_NAME.equals(schemaName);
    }

    /**
     * Triggers, constraints and views get executed with their definers'
     * privileges and they can exist in the system only if their definers
     * still have all the privileges to create them. Based on this, any
     * time a trigger/view/constraint is executing, we do not need to waste
     * time in checking if the definer still has the right set of privileges.
     * At compile time, we will make sure that we do not collect the privilege
     * requirement for objects accessed with definer privileges by calling the
     * following method.
     */
    final void disablePrivilegeCollection(){
        isPrivilegeCollectionRequired=false;
    }

    /**
     * * Parse the a SQL statement from the body
     * of another SQL statement. Pushes and pops a
     * separate CompilerContext to perform the compilation.
     */
    StatementNode parseStatement(String sql,boolean internalSQL) throws StandardException{
		/*
		** Get a new compiler context, so the parsing of the text
		** doesn't mess up anything in the current context
		*/
        LanguageConnectionContext lcc=getLanguageConnectionContext();
        CompilerContext newCC=lcc.pushCompilerContext();
        if(internalSQL)
            newCC.setReliability(CompilerContext.INTERNAL_SQL_LEGAL);

        try{
            Parser p=newCC.getParser();
            return (StatementNode)p.parseStatement(sql);
        }finally{
            lcc.popCompilerContext(newCC);
        }
    }

    /**
     * Parse an SQL fragment that represents a {@code <search condition>}.
     *
     * @param sql a fragment of an SQL statement
     * @param internalSQL {@code true} if the SQL fragment is allowed to
     *   contain internal syntax, {@code false} otherwise
     * @return a {@code ValueNode} representing the parse tree of the
     *   SQL fragment
     * @throws StandardException if an error happens while parsing
     */
    ValueNode parseSearchCondition(String sql, boolean internalSQL)
        throws StandardException
    {
        return (ValueNode)
                parseStatementOrSearchCondition(sql, internalSQL, false);
    }

    /**
     * Parse a full SQL statement or a fragment representing a {@code <search
     * condition>}. This is a worker method that contains common logic for
     * {@link #parseStatement} and {@link #parseSearchCondition}.
     *
     * @param sql the SQL statement or fragment to parse
     * @param internalSQL {@code true} if it is allowed to contain internal
     *   syntax, {@code false} otherwise
     * @param isStatement {@code true} if {@code sql} is a full SQL statement,
     *   {@code false} if it is a fragment
     * @return a parse tree
     * @throws StandardException if an error happens while parsing
     */
    private Visitable parseStatementOrSearchCondition(
            String sql, boolean internalSQL, boolean isStatement)
        throws StandardException
    {
		/*
		** Get a new compiler context, so the parsing of the text
		** doesn't mess up anything in the current context
		*/
		LanguageConnectionContext lcc = getLanguageConnectionContext();
		CompilerContext newCC = lcc.pushCompilerContext();
		if (internalSQL)
		    newCC.setReliability(CompilerContext.INTERNAL_SQL_LEGAL);

		try
		{
			Parser p = newCC.getParser();
            return isStatement
                    ? p.parseStatement(sql)
                    : p.parseSearchCondition(sql);
		}

		finally
		{
			lcc.popCompilerContext(newCC);
		}
    }

    /**
     * Get the descriptor for the named schema. If the schemaName
     * parameter is NULL, it gets the descriptor for the current
     * compilation schema.
     * <p/>
     * QueryTreeNodes must obtain schemas using this method or the single argument
     * version of it. This is to ensure that the correct default compliation schema
     * is returned and to allow determination of if the statement being compiled
     * depends on the current schema.
     *
     * @param schemaName The name of the schema we're interested in.
     *                   If the name is NULL, get the descriptor for the current compilation schema.
     * @param raiseError True to raise an error if the schema does not exist,
     *                   false to return null if the schema does not exist.
     * @return Valid SchemaDescriptor or null if raiseError is false and the
     * schema does not exist.
     * @throws StandardException Schema does not exist and raiseError is true.
     */
    final SchemaDescriptor getSchemaDescriptor(String schemaName,boolean raiseError)
            throws StandardException{
		/*
		** Check for a compilation context.  Sometimes
		** there is a special compilation context in
	 	** place to recompile something that may have
		** been compiled against a different schema than
		** the current schema (e.g views):
	 	**
	 	** 	CREATE SCHEMA x
	 	** 	CREATE TABLE t
		** 	CREATE VIEW vt as SEELCT * FROM t
		** 	SET SCHEMA app
		** 	SELECT * FROM X.vt
		**
		** In the above view vt must be compiled against
		** the X schema.
		*/

        SchemaDescriptor sd=null;
        boolean isCurrent=false;
        boolean isCompilation=false;
        if(schemaName==null){

            CompilerContext cc=getCompilerContext();
            sd=cc.getCompilationSchema();

            if(sd==null){
                // Set the compilation schema to be the default,
                // notes that this query has schema dependencies.
                sd=getLanguageConnectionContext().getDefaultSchema();

                isCurrent=true;

                cc.setCompilationSchema(sd);
            }else{
                isCompilation=true;
            }
            schemaName=sd.getSchemaName();
        }

        DataDictionary dataDictionary=getDataDictionary();
        SchemaDescriptor sdCatalog=dataDictionary.getSchemaDescriptor(schemaName,
                getLanguageConnectionContext().getTransactionCompile(),raiseError);

        if(isCurrent || isCompilation){
            //if we are dealing with a SESSION schema and it is not physically
            //created yet, then it's uuid is going to be null. DERBY-1706
            //Without the getUUID null check below, following will give NPE
            //set schema session; -- session schema has not been created yet
            //create table t1(c11 int);
            if(sdCatalog!=null && sdCatalog.getUUID()!=null){
                // different UUID for default (current) schema than in catalog,
                // so reset default schema.
                if(!sdCatalog.getUUID().equals(sd.getUUID())){
                    if(isCurrent)
                        getLanguageConnectionContext().setDefaultSchema(sdCatalog);
                    getCompilerContext().setCompilationSchema(sdCatalog);
                }
            }else{
                // this schema does not exist, so ensure its UUID is null.
                sd.setUUID(null);
                sdCatalog=sd;
            }
        }
        return sdCatalog;
    }

    /**
     * Verify that a java class exists, is accessible (public)
     * and not a class representing a primitive type.
     *
     * @param javaClassName The name of the java class to resolve.
     * @throws StandardException Thrown on error
     */
    void verifyClassExist(String javaClassName) throws StandardException{
        ClassInspector classInspector=getClassFactory().getClassInspector();

        Throwable reason=null;
        boolean foundMatch=false;
        try{
            foundMatch=classInspector.accessible(javaClassName);
        }catch(ClassNotFoundException cnfe){
            reason=cnfe;
        }

        if(!foundMatch)
            throw StandardException.newException(SQLState.LANG_TYPE_DOESNT_EXIST2,reason,javaClassName);

        if(ClassInspector.primitiveType(javaClassName))
            throw StandardException.newException(SQLState.LANG_TYPE_DOESNT_EXIST3,javaClassName);
    }

    /**
     * Add an authorization check into the passed in method.
     */
    void generateAuthorizeCheck(ActivationClassBuilder acb, MethodBuilder mb, int sqlOperation){
        // add code to authorize statement execution.
        acb.pushThisAsActivation(mb);
        mb.callMethod(VMOpcode.INVOKEINTERFACE,null,"getLanguageConnectionContext",
                ClassName.LanguageConnectionContext,0);
        mb.callMethod(VMOpcode.INVOKEINTERFACE,null,"getAuthorizer",
                ClassName.Authorizer,0);

        acb.pushThisAsActivation(mb);
        mb.push(sqlOperation);
        mb.callMethod(VMOpcode.INVOKEINTERFACE,null,"authorize",
                "void",2);
    }

    private static boolean containsInfo(String str){
        for(int i=0;i<str.length();i++){
            if(str.charAt(i)!='\t' && str.charAt(i)!='\n'){
                return true;
            }
        }
        return false;
    }

    /**
     * Declare a dependency on an ANSI UDT, identified by its AliasDescriptor,
     * and check that you have privilege to use it.
     */
    private void createTypeDependency(AliasDescriptor ad) throws StandardException{
        getCompilerContext().createDependency(ad);

        if(isPrivilegeCollectionRequired()){
            getCompilerContext().addRequiredUsagePriv(ad);
        }
    }

    /**
     * Common code for the 2 checkReliability functions.  Always throws StandardException.
     *
     * @param fragmentType    Type of fragment as a string, for inclusion in error messages.
     * @param fragmentBitMask Describes the kinds of expressions we ar suspicious of
     * @throws StandardException Throws an error, always.
     */
    private void throwReliabilityException(String fragmentType,int fragmentBitMask) throws StandardException{
        String sqlState;
		/* Error string somewhat dependent on operation due to different
		 * nodes being allowed for different operations.
		 */
        if(getCompilerContext().getReliability()==CompilerContext.DEFAULT_RESTRICTION){
            sqlState=SQLState.LANG_INVALID_DEFAULT_DEFINITION;
        }else if(getCompilerContext().getReliability()==CompilerContext.GENERATION_CLAUSE_RESTRICTION){
            switch(fragmentBitMask){
                case CompilerContext.SQL_IN_ROUTINES_ILLEGAL:
                    sqlState=SQLState.LANG_ROUTINE_CANT_PERMIT_SQL;
                    break;

                default:
                    sqlState=SQLState.LANG_NON_DETERMINISTIC_GENERATION_CLAUSE;
                    break;
            }
        }else{
            sqlState=SQLState.LANG_UNRELIABLE_QUERY_FRAGMENT;
        }
        throw StandardException.newException(sqlState,fragmentType);
    }

    protected void setDepth(int depth) {
        this.depth = depth;
    }

    protected String spaceToLevel(){
        if(depth==0) return "";
        else
            return Strings.repeat(spaces, depth)+"->"+spaces;
    }

    /**
     * By default, return nothing
     *
     * @return
     * @throws StandardException
     */
    protected String getExtraInformation() throws StandardException {
        return null;
    }

    private static final String spaces="  ";

    public void buildTree(Collection<QueryTreeNode> tree, int depth) throws StandardException {
        setDepth(depth);
        tree.add(this);
    }

    public String printExplainInformation(int size, int i, boolean useSpark, boolean fromPlanPrinter) throws StandardException {

        String s = printExplainInformation(size - i, fromPlanPrinter);
        if (i > 0)
            return s;
        else {
            String engine = useSpark? ",engine=Spark)" : ",engine=control)";
            s = s.substring(0, s.length()-1) + engine;
        }
        return s;
    }

    public String printExplainInformation(int order, boolean fromPlanPrinter) throws StandardException {
        return printExplainInformation(",", order);
    }

    public String printExplainInformation(String attrDelim, int order) throws StandardException {
        throw new RuntimeException("Not implemented in: " + this.getClass());
    }

    public String printRuntimeInformation() throws StandardException {
        throw new RuntimeException("Not implemented in: " + this.getClass());
    }

    public String printExplainInformationForActivation() throws StandardException {
        return WordUtils.wrap(printExplainInformation(", ", -1), 40, null, false);
    }

    public String toHTMLString() {
        return toString();
    }

    public List<ColumnReference> getHashableJoinColumnReference() {
        return null;
    }

    public void setHashableJoinColumnReference(ColumnReference cr) {}


    /**
     * For a given ResultColumnList, return a map from
     * [resultSetNumber, virtualColumnId] => ResultColumn
     * where there is one entry for each ResultColumn down the chain of reference to
     * its source column on a table. This allows translation from a column reference at
     * any node below into the ResultColumn projected from the passed ResultColumnList.
     */
    public LongObjectHashMap<ResultColumn> rsnChainMap()
            throws StandardException {
        LongObjectHashMap<ResultColumn> chain = new LongObjectHashMap<>();
        List<ResultColumn> cols = RSUtils.collectNodes(this, ResultColumn.class);
        for (ResultColumn rc : cols) {
            long top = rc.getCoordinates();
            chain.put(top, rc);
            LongArrayList list = rc.chain();
            for (int i = 0; i< list.size(); i++) {
                chain.put(list.buffer[i],rc);
            }
        }
        return chain;
    }

    public LongLongHashMap childParentMap()
            throws StandardException {
        // Lots of duplication, thus the HashSet
        Set<ResultColumn> cols = new HashSet<>(RSUtils.collectNodes(this, ResultColumn.class));
        LongLongHashMap chain = new LongLongHashMap(cols.size());
        for (ResultColumn rc : cols) {
            if (rc.getExpression() ==null)
                continue;
            long top = rc.getCoordinates();
            if (top == -1L)
                continue;
            long child = rc.getChildLink();
            if (child != -1L) {
                if (!chain.containsKey(child))
                    chain.put(child, top);
            }
        }
        return chain;
    }

    /*  Workaround for a bug in commons-lang3 3.5 (LANG-1292)
     *  See SPLICE-1934. Can be reverted once SPARK2, providing commons-lang3 at runtime, is on 3.6
     */
    protected static class WordUtils {
        /**
         * <p>Wraps a single line of text, identifying words by <code>' '</code>.</p>
         *
         * <p>Leading spaces on a new line are stripped.
         * Trailing spaces are not stripped.</p>
         *
         * <table border="1" summary="Wrap Results">
         *  <tr>
         *   <th>input</th>
         *   <th>wrapLenght</th>
         *   <th>newLineString</th>
         *   <th>wrapLongWords</th>
         *   <th>result</th>
         *  </tr>
         *  <tr>
         *   <td>null</td>
         *   <td>*</td>
         *   <td>*</td>
         *   <td>true/false</td>
         *   <td>null</td>
         *  </tr>
         *  <tr>
         *   <td>""</td>
         *   <td>*</td>
         *   <td>*</td>
         *   <td>true/false</td>
         *   <td>""</td>
         *  </tr>
         *  <tr>
         *   <td>"Here is one line of text that is going to be wrapped after 20 columns."</td>
         *   <td>20</td>
         *   <td>"\n"</td>
         *   <td>true/false</td>
         *   <td>"Here is one line of\ntext that is going\nto be wrapped after\n20 columns."</td>
         *  </tr>
         *  <tr>
         *   <td>"Here is one line of text that is going to be wrapped after 20 columns."</td>
         *   <td>20</td>
         *   <td>"&lt;br /&gt;"</td>
         *   <td>true/false</td>
         *   <td>"Here is one line of&lt;br /&gt;text that is going&lt;br /&gt;to be wrapped after&lt;br /&gt;20 columns."</td>
         *  </tr>
         *  <tr>
         *   <td>"Here is one line of text that is going to be wrapped after 20 columns."</td>
         *   <td>20</td>
         *   <td>null</td>
         *   <td>true/false</td>
         *   <td>"Here is one line of" + systemNewLine + "text that is going" + systemNewLine + "to be wrapped after" + systemNewLine + "20 columns."</td>
         *  </tr>
         *  <tr>
         *   <td>"Click here to jump to the commons website - http://commons.apache.org"</td>
         *   <td>20</td>
         *   <td>"\n"</td>
         *   <td>false</td>
         *   <td>"Click here to jump\nto the commons\nwebsite -\nhttp://commons.apache.org"</td>
         *  </tr>
         *  <tr>
         *   <td>"Click here to jump to the commons website - http://commons.apache.org"</td>
         *   <td>20</td>
         *   <td>"\n"</td>
         *   <td>true</td>
         *   <td>"Click here to jump\nto the commons\nwebsite -\nhttp://commons.apach\ne.org"</td>
         *  </tr>
         * </table>
         *
         * @param str  the String to be word wrapped, may be null
         * @param wrapLength  the column to wrap the words at, less than 1 is treated as 1
         * @param newLineStr  the string to insert for a new line,
         *  <code>null</code> uses the system property line separator
         * @param wrapLongWords  true if long words (such as URLs) should be wrapped
         * @return a line with newlines inserted, <code>null</code> if null input
         */
        public static String wrap(final String str, int wrapLength, String newLineStr, final boolean wrapLongWords) {
            if (str == null) {
                return null;
            }
            if (newLineStr == null) {
                newLineStr = SystemUtils.LINE_SEPARATOR;
            }
            if (wrapLength < 1) {
                wrapLength = 1;
            }
            final int inputLineLength = str.length();
            int offset = 0;
            final StringBuilder wrappedLine = new StringBuilder(inputLineLength + 32);

            while (inputLineLength - offset > wrapLength) {
                if (str.charAt(offset) == ' ') {
                    offset++;
                    continue;
                }
                int spaceToWrapAt = str.lastIndexOf(' ', wrapLength + offset);

                if (spaceToWrapAt >= offset) {
                    // normal case
                    wrappedLine.append(str.substring(offset, spaceToWrapAt));
                    wrappedLine.append(newLineStr);
                    offset = spaceToWrapAt + 1;

                } else {
                    // really long word or URL
                    if (wrapLongWords) {
                        // wrap really long word one line at a time
                        wrappedLine.append(str.substring(offset, wrapLength + offset));
                        wrappedLine.append(newLineStr);
                        offset += wrapLength;
                    } else {
                        // do not wrap really long word, just extend beyond limit
                        spaceToWrapAt = str.indexOf(' ', wrapLength + offset);
                        if (spaceToWrapAt >= 0) {
                            wrappedLine.append(str.substring(offset, spaceToWrapAt));
                            wrappedLine.append(newLineStr);
                            offset = spaceToWrapAt + 1;
                        } else {
                            wrappedLine.append(str.substring(offset));
                            offset = inputLineLength;
                        }
                    }
                }
            }

            // Whatever is left in line is short enough to just pass through
            wrappedLine.append(str.substring(offset));

            return wrappedLine.toString();
        }
    }

    @Override
    public List<QueryTreeNode> collectReferencedColumns() throws StandardException {
        return Collections.emptyList();
    }

    @Override
    public Visitable projectionListPruning(boolean considerAllRCs) throws StandardException {
        return this;
    }

    @Override
    public void markReferencedResultColumns(List<QueryTreeNode> list) throws StandardException {
        for (QueryTreeNode col : list) {
            if (col instanceof ColumnReference) {
                ResultColumn rc = ((ColumnReference)col).getSource();
                if (rc != null)
                    rc.setReferenced();
            } else if (col instanceof VirtualColumnNode) {
                VirtualColumnNode vc = (VirtualColumnNode) col;
                if (vc != null)
                    vc.getSourceResultColumn().setReferenced();
            } else { //OrderedColumn
                if (col instanceof OrderByColumn) {
                    ResultColumn rc = ((OrderByColumn) col).getResultColumn();
                    if (rc != null)
                        rc.setReferenced();

                    ValueNode groupByExpression = ((OrderByColumn) col).getColumnExpression();
                    CollectingVisitor<ColumnReference> collectingVisitor = new CollectingVisitor<ColumnReference>(Predicates.instanceOf(ColumnReference.class));
                    groupByExpression.accept(collectingVisitor);
                    for (ColumnReference cr: collectingVisitor.getCollected()) {
                        rc = cr.getSource();
                        if (rc != null)
                            rc.setReferenced();
                    }
                } else { // GroupbyColumn
                    ValueNode groupByExpression = ((GroupByColumn) col).getColumnExpression();
                    CollectingVisitor<ColumnReference> collectingVisitor = new CollectingVisitor<ColumnReference>(Predicates.instanceOf(ColumnReference.class));
                    groupByExpression.accept(collectingVisitor);
                    for (ColumnReference cr: collectingVisitor.getCollected()) {
                        ResultColumn rc = cr.getSource();
                        if (rc != null)
                            rc.setReferenced();
                    }
                }
            }
        }
    }

    public boolean isConstantOrParameterTreeNode() {
        return false;
    }

    /**
     * Get all child nodes of a specific type, and return them in the order
     * in which they appear in the SQL text.
     *
     * @param <N> the type of node to look for
     * @param type the type of node to look for
     * @return all nodes of the specified type
     * @throws StandardException if an error occurs
     */
    public <N extends QueryTreeNode>
        SortedSet<N> getOffsetOrderedNodes(Class<N> type)
                throws StandardException {
        OffsetOrderVisitor<N> visitor = new OffsetOrderVisitor<N>(
                type, getBeginOffset(), getEndOffset() + 1);
        accept(visitor);
        return visitor.getNodes();
    }

}
