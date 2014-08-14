package com.splicemachine.derby.impl.sql.compile;

import java.util.Vector;

import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.services.sanity.SanityManager;
import org.apache.derby.iapi.sql.compile.Optimizable;
import org.apache.derby.iapi.sql.compile.RowOrdering;
import org.apache.log4j.Logger;
import com.splicemachine.utils.SpliceLogUtils;

public class SpliceRowOrderingImpl implements RowOrdering {
    private static final Logger LOG = Logger.getLogger(SpliceRowOrderingImpl.class);

	/* This vector contains ColumnOrderings */
	Vector ordering;

	/*
	** This ColumnOrdering represents the columns that can be considered
	** ordered no matter what.  For example, columns that are compared to
	** constants with = are always ordered.  Also, all columns in a one-row
	** result set are ordered. Another instance of always ordered is when
	** the column is involved in an equijoin with an optimizable which is 
	** always ordered on the column on which the equijoin is happening.
	*/
	ColumnOrdering	columnsAlwaysOrdered;

	/*
	** This vector contains table numbers for tables that are always ordered.
	** This happens for one-row tables.
	*/
	Vector alwaysOrderedOptimizables;

	ColumnOrdering	currentColumnOrdering;

	/* This vector contains unordered Optimizables */
	Vector unorderedOptimizables;

	public SpliceRowOrderingImpl() {
		ordering = new Vector();
		unorderedOptimizables = new Vector();
		columnsAlwaysOrdered = new ColumnOrdering(RowOrdering.DONTCARE);
		alwaysOrderedOptimizables = new Vector();
	}
	
	/** @see RowOrdering#isColumnAlwaysOrdered */
	@Override
	public boolean isColumnAlwaysOrdered(int tableNumber, int columnNumber) {
		boolean ordered = columnsAlwaysOrdered.contains(tableNumber, columnNumber);
		SpliceLogUtils.trace(LOG, "isColumnAlwaysOrdered tableNumber=%d, columnNumber=%d, returns=%s",tableNumber,columnNumber,ordered);
		return (columnsAlwaysOrdered.contains(tableNumber, columnNumber)); 
	}

	/**
	 * @see RowOrdering#orderedOnColumn
	 *
	 * @exception StandardException		Thrown on error
	 */
	@Override
	public boolean orderedOnColumn(int direction,
									int orderPosition,
									int tableNumber,
									int columnNumber) throws StandardException {
		SpliceLogUtils.trace(LOG, "orderedOnColumn direction=%d, orderPosition=%d, tableNumber=%d, columnNumber=%d",direction,orderPosition,tableNumber,columnNumber);
		/*
		** Return true if the table is always ordered.
		*/
		if (vectorContainsOptimizable(tableNumber, alwaysOrderedOptimizables)) {
			return true;
		}

		/*
		** Return true if the column is always ordered.
		*/
		if (columnsAlwaysOrdered.contains(tableNumber, columnNumber)) {
			return true;
		}

		/*
		** Return false if we're looking for an ordering position that isn't
		** in this ordering.
		*/
		if (orderPosition >= ordering.size())
			return false;

		ColumnOrdering co = (ColumnOrdering) ordering.get(orderPosition);

		/*
		** Is the column in question ordered with the given direction at
		** this position?
		*/
		return co.ordered(direction, tableNumber, columnNumber);
	}

	/**
	 * @see RowOrdering#orderedOnColumn
	 *
	 * @exception StandardException		Thrown on error
	 */
	@Override
	public boolean orderedOnColumn(int direction,
									int tableNumber,
									int columnNumber)
				throws StandardException {
		SpliceLogUtils.trace(LOG, "orderedOnColumn direction=%d, tableNumber=%d, columnNumber=%d",direction,tableNumber,columnNumber);

		/*
		** Return true if the table is always ordered.
		*/
		if (vectorContainsOptimizable(tableNumber, alwaysOrderedOptimizables)) {
			return true;
		}

		/*
		** Return true if the column is always ordered.
		*/
		if (columnsAlwaysOrdered.contains(tableNumber, columnNumber)) {
			return true;
		}

		boolean ordered = false;

		for (int i = 0; i < ordering.size(); i++) {
			ColumnOrdering co = (ColumnOrdering) ordering.get(i);

			/*
			** Is the column in question ordered with the given direction at
			** this position?
			*/
			boolean thisOrdered = co.ordered(direction,
											tableNumber,
											columnNumber);

			if (thisOrdered) {
				ordered = true;
				break;
			}
		}

		return ordered;
	}

	/**
	 * Return true if the given vector of Optimizables contains an Optimizable
	 * with the given table number.
	 */
	private boolean vectorContainsOptimizable(int tableNumber, Vector vec) {
		int i;
		for (i = vec.size() - 1; i >= 0; i--) {
			Optimizable optTable = (Optimizable) vec.get(i);
			if (optTable.hasTableNumber()) {
				if (optTable.getTableNumber() == tableNumber) {
					return true;
				}
			}
		}
		return false;
	}

	/** @see RowOrdering#addOrderedColumn */
	@Override
	public void addOrderedColumn(int direction,
								int tableNumber,
								int columnNumber) {
		SpliceLogUtils.trace(LOG, "addOrderedColumn direction=%d, tableNumber=%d, columnNumber=%d",direction,tableNumber,columnNumber);

		if (unorderedOptimizables.size() > 0)
			return;
		ColumnOrdering currentColumnOrdering;

		if (ordering.size() == 0) {
			currentColumnOrdering = new ColumnOrdering(direction);
			ordering.add(currentColumnOrdering);
		}
		else {
			currentColumnOrdering =
				(ColumnOrdering) ordering.get(ordering.size() - 1);
		}

		if (SanityManager.DEBUG) {
			if (currentColumnOrdering.direction() != direction) {
				SanityManager.THROWASSERT("direction == " + direction +
					", currentColumnOrdering.direction() == " +
					currentColumnOrdering.direction());
			}
		}
		currentColumnOrdering.addColumn(tableNumber, columnNumber);
	}

	/** @see RowOrdering#nextOrderPosition */
	@Override
	public void nextOrderPosition(int direction) {
		SpliceLogUtils.trace(LOG, "nextOrderPosition direction=%d",direction);
		if (unorderedOptimizables.size() > 0)
			return;
		currentColumnOrdering = new ColumnOrdering(direction);
		ordering.add(currentColumnOrdering);
	}

	public void optimizableAlwaysOrdered(Optimizable optimizable) {
		SpliceLogUtils.trace(LOG, "optimizableAlwaysOrdered optimizable=%s",optimizable);
		// A table can't be ordered if there is an outer unordered table
		if (unorderedOptimizablesOtherThan(optimizable)) {
			return;
		}

		/*
		** A table is not "always ordered" if any of the other ordered tables
		** in the join order are not also "always ordered".  In other words,
		** if any outer table is not a one-row table, this table is not
		** always ordered.
		**
		** The table that was passed in as a parameter may have already been
		** added as a table with ordered columns.  If it is the first table
		** in the list of ordered columns, then there should be no other
		** tables in this list, so we remove it from the list and add it
		** to the list of always-ordered tables.
		*/
		boolean hasTableNumber = optimizable.hasTableNumber();
		int tableNumber = (hasTableNumber ? optimizable.getTableNumber() : 0);
		if (
			(
				(ordering.size() == 0) ||
				(
					hasTableNumber &&
					((ColumnOrdering) ordering.get(0)).hasTable(
																	tableNumber)
				)
			)
			&&
			(
				hasTableNumber &&
				! columnsAlwaysOrdered.hasAnyOtherTable(tableNumber)
			)
		   )
		{
			if (optimizable.hasTableNumber())
				removeOptimizable(optimizable.getTableNumber());

			alwaysOrderedOptimizables.add(optimizable);
		}
	}

	/** @see RowOrdering#columnAlwaysOrdered */
	@Override
	public void columnAlwaysOrdered(Optimizable optimizable, int columnNumber) {
		SpliceLogUtils.trace(LOG, "columnAlwaysOrdered optimizable=%s, columNumber=%d",optimizable, columnNumber);
		columnsAlwaysOrdered.addColumn(optimizable.getTableNumber(),columnNumber);
	}

	/** @see RowOrdering#alwaysOrdered */
	@Override
	public boolean alwaysOrdered(int tableNumber) {
		SpliceLogUtils.trace(LOG, "alwaysOrdered tableNumber=%d",tableNumber);
		return vectorContainsOptimizable(
										tableNumber,
										alwaysOrderedOptimizables
										);
	}

	/** @see RowOrdering#removeOptimizable */
	@Override
	public void removeOptimizable(int tableNumber) {
		SpliceLogUtils.trace(LOG, "removeOptimizable tableNumber=%d",tableNumber);
		int i;

		/*
		** Walk the list backwards, so we can remove elements
		** by position.
		*/
		for (i = ordering.size() - 1; i >= 0; i--)
		{
			/*
			** First, remove the table from all the ColumnOrderings
			*/
			ColumnOrdering ord = (ColumnOrdering) ordering.get(i);
			ord.removeColumns(tableNumber);
			if (ord.empty())
				ordering.remove(i);
		}

		/* Remove from list of always-ordered columns */
		columnsAlwaysOrdered.removeColumns(tableNumber);

		/* Also remove from list of unordered optimizables */
		removeOptimizableFromVector(tableNumber, unorderedOptimizables);

		/* Also remove from list of always ordered optimizables */
		removeOptimizableFromVector(tableNumber, alwaysOrderedOptimizables);
	}

	/**
	 * Remove all optimizables with the given table number from the
	 * given vector of optimizables.
	 */
	private void removeOptimizableFromVector(int tableNumber, Vector vec) {
		int i;

		for (i = vec.size() - 1; i >= 0; i--) {
			Optimizable optTable =
							(Optimizable) vec.get(i);

			if (optTable.hasTableNumber()) {
				if (optTable.getTableNumber() == tableNumber) {
					vec.remove(i);
				}
			}
		}
	}

	/** @see RowOrdering#addUnorderedOptimizable */
	@Override
	public void addUnorderedOptimizable(Optimizable optimizable) {
		SpliceLogUtils.trace(LOG, "addUnorderedOptimizable optimizable=%s",optimizable);
		unorderedOptimizables.add(optimizable);
	}

	/** @see RowOrdering#copy */
	public void copy(RowOrdering copyTo) {
		if (SanityManager.DEBUG) {
			if ( ! (copyTo instanceof SpliceRowOrderingImpl) ) {
				SanityManager.THROWASSERT(
					"copyTo should be a RowOrderingImpl, is a " +
					copyTo.getClass().getName());
			}
		}

		SpliceRowOrderingImpl dest = (SpliceRowOrderingImpl) copyTo;

		/* Clear the ordering of what we're copying to */
		dest.ordering.clear();
		dest.currentColumnOrdering = null;

		dest.unorderedOptimizables.clear();
		for (int i = 0; i < unorderedOptimizables.size(); i++) {
			dest.unorderedOptimizables.add(
											unorderedOptimizables.get(i));
		}

		dest.alwaysOrderedOptimizables.clear();
		for (int i = 0; i < alwaysOrderedOptimizables.size(); i++) {
			dest.alwaysOrderedOptimizables.add(
										alwaysOrderedOptimizables.get(i));
		}

		for (int i = 0; i < ordering.size(); i++) {
			ColumnOrdering co = (ColumnOrdering) ordering.get(i);

			dest.ordering.add(co.cloneMe());

			if (co == currentColumnOrdering)
				dest.rememberCurrentColumnOrdering(i);
		}

		dest.columnsAlwaysOrdered = null;
		if (columnsAlwaysOrdered != null)
			dest.columnsAlwaysOrdered = columnsAlwaysOrdered.cloneMe();
	}

	private void rememberCurrentColumnOrdering(int posn) {
		currentColumnOrdering = (ColumnOrdering) ordering.get(posn);
	}

	public String toString() {
		String retval = null;

		if (SanityManager.DEBUG) {
			int i;
			retval = "RowOrder { ";
			if (unorderedOptimizables.size()>0) {
				retval += "unorderedOptimizables=[";
				for (i = 0; i < unorderedOptimizables.size(); i++)  {
					displayString(unorderedOptimizables,i);
					if (i<unorderedOptimizables.size()-1)
						retval += ",";
				}
				retval += "] ";
			}
			if (alwaysOrderedOptimizables.size()>0) {
				retval += "alwaysOrderedOptimizables=[";
				for (i = 0; i < alwaysOrderedOptimizables.size(); i++) {				
					displayString(alwaysOrderedOptimizables,i);
				if (i<alwaysOrderedOptimizables.size()-1)
					retval += ",";
				}
				retval += "] ";
			}
			retval += "ColumnOrdering=[";
			for (i = 0; i < ordering.size(); i++) {
				retval += " ColumnOrdering " + i + ": " + ordering.get(i);
				if (i<unorderedOptimizables.size()-1)
					retval += ",";
			}
			retval += "]}";
		}

		return retval;
	}

	/**
	 * Returns true if there are unordered optimizables in the join order
	 * other than the given one.
	 */
	private boolean unorderedOptimizablesOtherThan(Optimizable optimizable)
	{
		for (int i = 0; i < unorderedOptimizables.size(); i++)
		{
			Optimizable thisOpt =
				(Optimizable) unorderedOptimizables.get(i);

			if (thisOpt != optimizable)
				return true;
		}

		return false;
	}
	
	private static String displayString(Vector optimizables, int i) {
		Optimizable opt = (Optimizable) optimizables.get(i);
		if (opt.getBaseTableName() != null)
			return opt.getBaseTableName();
		return optimizables.get(i).toString();
	}
	
	Vector getVector() {
		return ordering;
	}
	
}
