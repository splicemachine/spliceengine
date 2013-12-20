package com.splicemachine.derby.impl.sql.execute.actions;

import org.apache.derby.catalog.IndexDescriptor;
import org.apache.derby.catalog.types.IndexDescriptorImpl;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.services.context.ContextManager;
import org.apache.derby.iapi.services.context.ContextService;
import org.apache.derby.iapi.services.loader.GeneratedClass;
import org.apache.derby.iapi.sql.Activation;
import org.apache.derby.iapi.sql.ResultSet;
import org.apache.derby.iapi.sql.compile.ASTVisitor;
import org.apache.derby.iapi.sql.compile.CompilerContext;
import org.apache.derby.iapi.sql.conn.LanguageConnectionContext;
import org.apache.derby.iapi.sql.depend.Dependent;
import org.apache.derby.iapi.sql.dictionary.*;
import org.apache.derby.iapi.sql.execute.ConstantAction;
import org.apache.derby.iapi.store.access.TransactionController;
import org.apache.derby.iapi.util.ByteArray;
import org.apache.derby.impl.sql.CursorInfo;
import org.apache.derby.impl.sql.GenericStorablePreparedStatement;
import org.apache.derby.impl.sql.compile.StatementNode;
import org.apache.derby.impl.sql.execute.ColumnInfo;
import org.apache.derby.impl.sql.execute.CreateConstraintConstantAction;
import org.apache.derby.impl.sql.execute.CreateTableConstantAction;
import org.apache.derby.impl.sql.execute.DDLConstantAction;
import org.apache.derby.shared.common.reference.SQLState;

import java.sql.SQLWarning;
import java.util.Properties;

/**
 * A Constant Action for creating a table in a Splice-efficient fashion
 *
 * @author Scott Fines
 * Created on: 3/5/13
 */
public class SpliceCreateTableOperation extends CreateTableConstantOperation {
    private final int tableType;
    private final String tableName;
    private final String schemaName;
		private final StatementNode insertNode;
    /**
     * Make the ConstantAction for a CREATE TABLE statement.
     *
     * @param schemaName           name for the schema that table lives in.
     * @param tableName            Name of table.
     * @param tableType            Type of table (e.g., BASE, global temporary table).
     * @param columnInfo           Information on all the columns in the table.
     *                             (REMIND tableDescriptor ignored)
     * @param constraintActions    CreateConstraintConstantAction[] for constraints
     * @param properties           Optional table properties
     * @param lockGranularity      The lock granularity.
     * @param onCommitDeleteRows   If true, on commit delete rows else on commit preserve rows of temporary table.
     * @param onRollbackDeleteRows If true, on rollback, delete rows from temp tables which were logically modified. true is the only supported value
     */
    public SpliceCreateTableOperation(String schemaName,
																			String tableName,
																			int tableType,
																			ColumnInfo[] columnInfo,
																			ConstantAction[] constraintActions,
																			Properties properties,
																			char lockGranularity,
																			boolean onCommitDeleteRows,
																			boolean onRollbackDeleteRows,
																			StatementNode insertNode) {
        super(schemaName, tableName, tableType, columnInfo, constraintActions, properties, lockGranularity, onCommitDeleteRows, onRollbackDeleteRows);
        this.tableType = tableType;
        this.tableName = tableName;
        this.schemaName = schemaName;
				this.insertNode = insertNode;
    }

    @Override
    public void executeConstantAction(Activation activation) throws StandardException {
        /*
         * what follows here is a pretty nasty hack to prevent creating duplicate Table entries.
         *
         * There are two underlying problems preventing a more elegant solution here:
         *
         * 1. Derby manages system indices inside of the DataDictionary, rather than
         * allowing transparent insertions. This prevents the Index management coprocessors
         * from managing indices themselves.
         *
         * 2. Derby generates a UUID for a table which it uses as it's key for inserting
         * into its indices (because it manages them itself). Then, the HBase row key for
         * the system indices is just a random Row Key, rather than a primary key-type structure.
         * This means that a UniqueConstraint running in the Index coprocessor will never fail, which
         * will allow duplicates in.
         *
         * The most effective implementation is probably to remove explicit index management
         * from the DataDictionary, which should mean that UniqueConstraints can be enforced like
         * any other table, but that's a huge amount of work for very little reward in this case.
         *
         * Thus, this little hack: Basically, before creating the table, we first scan the SYSTABLES table
         * to see if any table with that name already exists in the specified Schema. If it doesn't, we
         * allow the table to be created; if it does, then we bomb out with an error.
         *
         * The downside? This is a check-then-act operation, which probably isn't thread-safe (and certainly
         * isn't cluster-safe), so we have to be certain that DDL operations occur within a transaction. Even then,
         * we're probably not safe, and this breaks adding tables concurrently. However, I am pretty sure that
         * the DataDictionary in general is not cluster-safe, so we are probably not any more screwed now
         * than we were before. Still, if this causes concurrent table-creations to fail, blame me.
         *
         * -Scott Fines, March 26 2013 -
         */
        LanguageConnectionContext lcc = activation.getLanguageConnectionContext();
        DataDictionary dd = lcc.getDataDictionary();
        TransactionController tc = lcc.getTransactionExecute();

        SchemaDescriptor sd;
        if(tableType == TableDescriptor.GLOBAL_TEMPORARY_TABLE_TYPE)
            sd = dd.getSchemaDescriptor(schemaName,tc, true);
        else
            sd = DDLConstantAction.getSchemaDescriptorForCreate(dd, activation, schemaName);

        TableDescriptor tableDescriptor = dd.getTableDescriptor(tableName, sd,tc);
        if(tableDescriptor!=null)
            throw StandardException.newException(SQLState.LANG_OBJECT_ALREADY_EXISTS_IN_OBJECT,
                    "table",tableName,"schema",schemaName);

				//if the table doesn't exist, allow super class to create it
				super.executeConstantAction(activation);


				if(insertNode!=null){
					/*
					 * If insertNode!=null, then this was a create table as ... with data statement,
					 * so we have to execute the insertion here.
					 *
					 * Unfortunately, we MUST bind and optimize the statement here, because otherwise
					 * the insert node will explode with a "Table does not exist" exception (or lots
					 * of NullPointers if you try and fix it).
					 */
						GenericStorablePreparedStatement gsps = new GenericStorablePreparedStatement();
						CompilerContext cc = lcc.pushCompilerContext(insertNode.getSchemaDescriptor(schemaName));
						CompilerContext compilerContext = insertNode.getCompilerContext();
						Dependent oldDependent = compilerContext.getCurrentDependent();
						if(oldDependent==null)
								compilerContext.setCurrentDependent(gsps);

						/*
						 * The following section is a stripped-down version of what occurs inside
						 * for GenericStatement.prepMinion (minus the parsing). If we can somehow
						 * find a better way to factor that code, we should delegate to it here
						 * instead of duplicating the logic.
						 */
						insertNode.bindStatement();
						insertNode.optimizeStatement();

						ASTVisitor visitor = lcc.getASTVisitor();
						if(visitor!=null){
								visitor.begin(insertNode.statementToString(),ASTVisitor.AFTER_OPTIMIZE);
								insertNode.accept(visitor);
								visitor.end(ASTVisitor.AFTER_OPTIMIZE);
						}


						ByteArray array = gsps.getByteCodeSaver();
						GeneratedClass ac = insertNode.generate(array);


						gsps.setConstantAction(insertNode.makeConstantAction());
						gsps.setSavedObjects(cc.getSavedObjects());
						gsps.setRequiredPermissionsList(cc.getRequiredPermissionsList());
						gsps.incrementVersionCounter();
						gsps.setActivationClass(ac);
						gsps.setNeedsSavepoint(insertNode.needsSavepoint());
						gsps.setCursorInfo((CursorInfo)cc.getCursorInfo());
						gsps.setIsAtomic(insertNode.isAtomic());
						gsps.setExecuteStatementNameAndSchema(insertNode.executeStatementName(),insertNode.executeSchemaName());
						gsps.setSPSName(insertNode.getSPSName());
						gsps.completeCompile(insertNode);
						gsps.setCompileTimeWarnings(cc.getWarnings());

						Activation insertActivation = gsps.getActivation(lcc, true);
						try{
								ResultSet insertionRs = gsps.execute(insertActivation, -1);
								SQLWarning warnings = insertionRs.getWarnings();
								activation.addWarning(warnings);
								int insertedRows = insertionRs.modifiedRowCount();
								activation.addRowsSeen(insertedRows);
								insertionRs.close();

						}finally{
								insertActivation.close();
						}
				}
    }

    @Override
    protected ConglomerateDescriptor getTableConglomerateDescriptor(TableDescriptor td, long conglomId, SchemaDescriptor sd, DataDescriptorGenerator ddg) throws StandardException {
        /*
         * If there is a PrimaryKey Constraint, build an IndexRowGenerator that returns the Primary Keys,
         * otherwise, do whatever Derby does by default
         */
        if(constraintActions ==null)
            return super.getTableConglomerateDescriptor(td,conglomId,sd,ddg);

        for(CreateConstraintConstantOperation constantAction:constraintActions){
            if(constantAction.getConstraintType()== DataDictionary.PRIMARYKEY_CONSTRAINT){
                int [] pkColumns = constantAction.genColumnPositions(td,true);
                boolean[] ascending = new boolean[pkColumns.length];
                for(int i=0;i<ascending.length;i++){
                   ascending[i] = true;
                }

                IndexDescriptor descriptor = new IndexDescriptorImpl("PRIMARYKEY",true,false,pkColumns,ascending,pkColumns.length);
                IndexRowGenerator irg = new IndexRowGenerator(descriptor);
                return ddg.newConglomerateDescriptor(conglomId,null,false,irg,false,null,td.getUUID(),sd.getUUID());
            }
        }
        return super.getTableConglomerateDescriptor(td,conglomId,sd,ddg);
    }
}
