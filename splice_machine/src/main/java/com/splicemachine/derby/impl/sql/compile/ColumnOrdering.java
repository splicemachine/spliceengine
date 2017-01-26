/*
 * Copyright (c) 2012 - 2017 Splice Machine, Inc.
 *
 * This file is part of Splice Machine.
 * Splice Machine is free software: you can redistribute it and/or modify it under the terms of the
 * GNU Affero General Public License as published by the Free Software Foundation, either
 * version 3, or (at your option) any later version.
 * Splice Machine is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU Affero General Public License for more details.
 * You should have received a copy of the GNU Affero General Public License along with Splice Machine.
 * If not, see <http://www.gnu.org/licenses/>.
 */

package com.splicemachine.derby.impl.sql.compile;

import java.util.Vector;
import com.splicemachine.db.iapi.services.sanity.SanityManager;
import com.splicemachine.db.iapi.sql.compile.RowOrdering;

public class ColumnOrdering {

	/* See RowOrdering for possible values */
	int	myDirection;

	/* A vector of column numbers (Integers) */
	Vector<Integer> columns = new Vector<>();

	/*
	** A vector of table numbers (Integers), corresponding to the column
	** vector by position.
	*/
	Vector<Integer> tables = new Vector<>();

	/**
	 * @param direction	See RowOrdering for possible values
	 */
	ColumnOrdering(int direction) {
		myDirection = direction;
	}

	/**
	 * Does this ColumnOrdering contain the given column in the given table
	 * in the right direction?
	 *
	 * @param direction		See RowOrdering for possible values
	 * @param tableNumber	The number of the table in question
	 * @param columnNumber	The column number in the table (one-based)
	 *
	 * @return	true if the column is found here in the right direction
	 */
	boolean ordered(int direction, int tableNumber, int columnNumber) {
		/*
		** Check the direction only if the direction isn't DONTCARE
		*/
		if (direction != RowOrdering.DONTCARE) {
			if (direction != myDirection)
				return false;
		}

		/* The direction matches - see if the column is in this ordering */
		return contains(tableNumber, columnNumber);
	}

	/**
	 * Does this ColumnOrdering contain the given column?
	 *
	 * @param tableNumber	The number of table in question
	 * @param columnNumber	The column number in the table (one-based)
	 *
	 * @return	true if the column is found here in the right direction
	 */
	boolean contains(int tableNumber, int columnNumber)
	{
		for (int i = 0; i < columns.size(); i++) {
			Integer col = (Integer) columns.get(i);
			Integer tab = (Integer) tables.get(i);

			if (tab.intValue() == tableNumber &&
				col.intValue() == columnNumber) {

				return true;
			}
		}

		return false;
	}

	/**
	 * Get the direction of this ColumnOrdering
	 */
	int direction()
	{
		return myDirection;
	}

	/**
	 * Add a column in a table to this ColumnOrdering
	 *
	 * @param tableNumber	The number of table in question
	 * @param columnNumber	The column number in the table (one-based)
	 */
	void addColumn(int tableNumber, int columnNumber)
	{
		tables.add(tableNumber);
		columns.add(columnNumber);
	}

	/**
	 * Remove all columns with the given table number
	 */
	void removeColumns(int tableNumber)
	{
		/*
		** Walk the list backwards, so we can remove elements
		** by position.
		*/
		for (int i = tables.size() - 1; i >= 0; i--)
		{
			Integer tab = (Integer) tables.get(i);
			if (tab==tableNumber)
			{
				tables.remove(i);
				columns.remove(i);
			}
		}
	}

	/**
	 * Tell whether this ColumnOrdering has no elements.
	 */
	boolean empty()
	{
		return (tables.size() == 0);
	}

	/** Return a clone of this ColumnOrdering */
	ColumnOrdering cloneMe() {
		ColumnOrdering retval = new ColumnOrdering(myDirection);

		for (int i = 0; i < columns.size(); i++) {
			/* Integers are immutable, so just copy the pointers */
			retval.columns.addElement(columns.get(i));
			retval.tables.addElement(tables.get(i));
		}

		return retval;
	}

	/** Is the given table number in this ColumnOrdering? */
	boolean hasTable(int tableNumber) {
		if (tables.size() == 0)
			return false;

		for (int i = 0; i < tables.size(); i++) {
			Integer tab = (Integer) tables.get(i);
			
			if (tab.intValue() == tableNumber)
				return true;
		}

		return false;
	}

	/** Is there any table other than the given one in this ColumnOrdering? */
	boolean hasAnyOtherTable(int tableNumber) {
		if (tables.size() == 0)
			return false;

		for (int i = 0; i < tables.size(); i++) {
			Integer tab = (Integer) tables.get(i);
			
			if (tab.intValue() != tableNumber)
				return true;
		}

		return false;
	}

	public String toString() {
		String retval = "";

		if (SanityManager.DEBUG) {
			StringBuilder sb = new StringBuilder().append(myDirection).append(":");
			for (int i = 0; i < columns.size(); i++) {
				sb = sb.append(tables.get(i)).append(":").append(columns.get(i));
			}
			retval = sb.toString();
		}

		return retval;
	}
	public int size() {
		return columns.size();
	}
	
	public int[] get(int i) {
		int[] tabCol = new int[2];
		tabCol[0] =tables.get(i); // autobox: fix
		tabCol[1] =columns.get(i); // autobox
		return tabCol;
	}

}

