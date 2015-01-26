package com.splicemachine.derby.impl.sql.execute.actions;

import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import org.apache.derby.catalog.UUID;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.reference.SQLState;
import org.apache.derby.iapi.services.sanity.SanityManager;
import org.apache.derby.iapi.sql.PreparedStatement;
import org.apache.derby.iapi.sql.ResultSet;
import org.apache.derby.iapi.sql.conn.LanguageConnectionContext;
import org.apache.derby.iapi.sql.dictionary.*;
import org.apache.derby.iapi.sql.execute.ExecRow;
import org.apache.derby.iapi.sql.execute.ConstantAction;
import org.apache.derby.iapi.store.access.TransactionController;
import org.apache.derby.iapi.types.DataValueDescriptor;
import org.apache.derby.iapi.types.NumberDataValue;
import org.apache.log4j.Logger;

import com.splicemachine.utils.SpliceLogUtils;

import java.util.List;

/**
 *	This class  describes actions that are ALWAYS performed for a
 *	constraint creation at Execution time.
 *
 *	@version 0.1
 */
public abstract class ConstraintConstantOperation extends DDLSingleTableConstantOperation {
	private static final Logger LOG = Logger.getLogger(ConstraintConstantOperation.class);
	protected String constraintName;
	protected int constraintType;
	protected String tableName;
	protected String schemaName;
	protected UUID schemaId;
	protected ConstantAction indexAction;

	/**
	 *	Make one of these puppies.
	 *
	 *  @param constraintName	Constraint name.
	 *  @param constraintType	Constraint type.
	 *  @param tableName		Table name.
	 *  @param tableId			UUID of table.
	 *  @param schemaName		schema that table and constraint lives in.
	 *  @param indexAction		IndexConstantAction for constraint (if necessary)
	 *  RESOLVE - the next parameter should go away once we use UUIDs
	 *			  (Generated constraint names will be based off of uuids)
	 */
	ConstraintConstantOperation(String constraintName,int constraintType,
		               String tableName,UUID tableId,String schemaName, ConstantAction indexAction) {
		super(tableId);
		SpliceLogUtils.trace(LOG, "ConstraintConstantOperation instance %s on table %s.%s",constraintName, schemaName, tableName);
		this.constraintName = constraintName;
		this.constraintType = constraintType;
		this.tableName = tableName;
		this.indexAction = indexAction;
		this.schemaName = schemaName;
		if (SanityManager.DEBUG)
			SanityManager.ASSERT(schemaName != null, "Constraint schema name is null");
	}

	/**
	 * Get the constraint type.
	 *
	 * @return The constraint type
	 */
	public	int getConstraintType() {
		return constraintType;
	}

	/**
	  *	Get the constraint name
	  *
	  *	@return	the constraint name
	  */
    public	String	getConstraintName() { 
    	return constraintName; 
    }

	/**
	  *	Get the associated index constant action.
	  *
	  *	@return	the constant action for the backing index
	  */
    public	ConstantAction	getIndexAction() { 
    	return indexAction; 
    }

	/**
	 * Make sure that the foreign key constraint is valid
	 * with the existing data in the target table.  Open
	 * the table, if there aren't any rows, ok.  If there
	 * are rows, open a scan on the referenced key with
	 * table locking at level 2.  Pass in the scans to
	 * the BulkRIChecker.  If any rows fail, barf.
	 *
	 * @param    tc        transaction controller
	 * @param    dd        data dictionary
	 * @param    fkConstraint        foreign key constraint
	 * @param    parentKeyConstraint    referenced key
	 * @param    indexTemplateRow    index template row
	 *
	 * @param lcc
     * @exception StandardException on error
	 */
    static void validateFKConstraint(
            TransactionController tc,
            DataDictionary dd,
            ForeignKeyConstraintDescriptor fkConstraint,
            ReferencedKeyConstraintDescriptor parentKeyConstraint,
            ExecRow indexTemplateRow, LanguageConnectionContext lcc) throws StandardException {

        TableDescriptor childTd = fkConstraint.getTableDescriptor();
        TableDescriptor parentTd = parentKeyConstraint.getTableDescriptor();

        ColumnDescriptorNameFunction childNameFunction = new ColumnDescriptorNameFunction(childTd);
        ColumnDescriptorNameFunction parentNameFunction = new ColumnDescriptorNameFunction(parentTd);

        ColumnDescriptorList childColumns = fkConstraint.getColumnDescriptors();
        ColumnDescriptorList parentColumns = parentKeyConstraint.getColumnDescriptors();

        assert childColumns.size() == parentColumns.size();

        Joiner commaJoiner = Joiner.on(",");
        Joiner andJoiner = Joiner.on(" AND ");

        // Find rows in C with non-null FK columns that do not exist in referenced columns of P.
        //
        // Example:
        //
        //  create table P(a int, b int, c int, d int, e int, f int, primary key(b,c));
        //  create table C(x int, y int, z int);
        //  alter table C add constraint fk1 foreign key(y,z) references P(b,c);
        //
        //        select distinct C.y,C.z
        //         from C
        //    left join P on C.y = P.b and C.z = P.c
        //        where C.y is not null and C.z is not null
        //          and P.b is null and P.c is null;
        //
        List<String> joinClauseParts = Lists.newArrayList();
        List<String> whereClauseParts = Lists.newArrayList();
        for (int i = 0; i < parentColumns.size(); i++) {
            joinClauseParts.add(childNameFunction.apply(childColumns.get(i)) + " = " + parentNameFunction.apply(parentColumns.get(i)));
            whereClauseParts.add(childNameFunction.apply(childColumns.get(i)) + " is not null");
            whereClauseParts.add(parentNameFunction.apply(parentColumns.get(i)) + " is null");
        }

        StringBuilder query = new StringBuilder();
        query.append("SELECT DISTINCT " + commaJoiner.join(Iterables.transform(childColumns, childNameFunction)));
        query.append(" FROM " + childTd.getName());
        query.append(" LEFT JOIN " + parentTd.getName() + " ON ");
        query.append(andJoiner.join(joinClauseParts));
        query.append(" WHERE ");
        query.append(andJoiner.join(whereClauseParts));
        query.append(" FETCH FIRST ROW ONLY");

        ResultSet rs = null;
        try {
            PreparedStatement ps = lcc.prepareInternalStatement(query.toString());
            rs = ps.executeSubStatement(lcc, false, 0L);
            ExecRow row = rs.getNextRow();
            if (row != null) {
                throw StandardException.newException(SQLState.LANG_ADD_FK_CONSTRAINT_VIOLATION,
                        fkConstraint.getConstraintName(), childTd.getQualifiedName());
            }
        } finally {
            if (rs != null) {
                rs.close();
            }
        }
    }

	/**
	 * Evaluate a check constraint or not null column constraint.  
	 * Generate a query of the
	 * form SELECT COUNT(*) FROM t where NOT(<check constraint>)
	 * and run it by compiling and executing it.   Will
	 * work ok if the table is empty and query returns null.
	 *
	 * @param constraintName	constraint name
	 * @param constraintText	constraint text
	 * @param td				referenced table
	 * @param lcc				the language connection context
	 * @param isCheckConstraint	the constraint is a check constraint
     *
	 * @return true if null constraint passes, false otherwise
	 *
	 * @exception StandardException if check constraint fails
	 */
	 public static boolean validateConstraint (
		String							constraintName,
		String							constraintText,
		TableDescriptor					td,
		LanguageConnectionContext		lcc,
		boolean							isCheckConstraint ) throws StandardException {
			SpliceLogUtils.error(LOG, "validateConstraint %s using qualifier {%s}",constraintName, constraintText);
		StringBuilder checkStmt = new StringBuilder();
		/* should not use select sum(not(<check-predicate>) ? 1: 0) because
		 * that would generate much more complicated code and may exceed Java
		 * limits if we have a large number of check constraints, beetle 4347
		 */
		checkStmt.append("SELECT COUNT(*) FROM ");
		checkStmt.append(td.getQualifiedName());
		checkStmt.append(" WHERE NOT(");
		checkStmt.append(constraintText);
		checkStmt.append(")");
	
		ResultSet rs = null;
		try
		{
			PreparedStatement ps = lcc.prepareInternalStatement(checkStmt.toString());

            // This is a substatement; for now, we do not set any timeout
            // for it. We might change this behaviour later, by linking
            // timeout to its parent statement's timeout settings.
			rs = ps.executeSubStatement(lcc, false, 0L);
			ExecRow row = rs.getNextRow();
			if (SanityManager.DEBUG)
			{
				if (row == null)
				{
					SanityManager.THROWASSERT("did not get any rows back from query: "+checkStmt.toString());
				}
			}

			Number value = ((Number)(row.getRowArray()[0]).getObject());
			/*
			** Value may be null if there are no rows in the
			** table.
			*/
			if ((value != null) && (value.longValue() != 0))
			{	
				//check constraint violated
				if (isCheckConstraint)
					throw StandardException.newException(SQLState.LANG_ADD_CHECK_CONSTRAINT_FAILED,
						constraintName, td.getQualifiedName(), value.toString());
				/*
				 * for not null constraint violations exception will be thrown in caller
				 * check constraint will not get here since exception is thrown
				 * above
				 */
				return false;
			}
		}
		finally
		{
			if (rs != null)
			{
				rs.close();
			}
		}
		return true;
	}

    private static class ColumnDescriptorNameFunction implements Function<ColumnDescriptor, String> {

        private TableDescriptor tableDescriptor;

        private ColumnDescriptorNameFunction(TableDescriptor tableDescriptor) {
            this.tableDescriptor = tableDescriptor;
        }

        @Override
        public String apply(ColumnDescriptor columnDescriptor) {
            return tableDescriptor.getName() + "." + columnDescriptor.getColumnName();
        }
    }
}
