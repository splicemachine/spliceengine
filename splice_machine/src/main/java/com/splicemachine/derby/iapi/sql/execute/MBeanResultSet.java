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

package com.splicemachine.derby.iapi.sql.execute;

import java.sql.SQLException;
import java.util.List;

/**
 * @author Jeff Cunningham
 *         Date: 12/10/13
 */
public class MBeanResultSet extends SpliceAbstractResultSet {
    private final List<String> columnNames;
    private List<List<String>> rows;
    private int currentIndex = -1;

    public MBeanResultSet(List<List<String>> rows, List<String> columnNames) {
        this.columnNames = columnNames;
        this.rows = rows;
    }

    @Override
    public boolean next() throws SQLException {
        if (hasNext()) {
            this.currentIndex++;
        } else {
            throw new SQLException();
        }
        return hasNext();
    }

    private boolean hasNext() {
        return currentIndex < this.rows.size();
    }

    @Override
    public Object getObject(String columnLabel) throws SQLException {
        if (! hasNext() || ! columnNames.contains(columnLabel)) {
            throw new SQLException();
        }
        return getColumnValue(columnLabel);
    }


    @Override
    public Object getObject(int columnIndex) throws SQLException {
        if (! hasNext() || columnIndex - 1 > this.rows.get(this.currentIndex).size()) {
            return null;
        }
        return this.rows.get(this.currentIndex).get(columnIndex - 1);
    }

    @Override
    public int findColumn(String label) throws SQLException {
        int index = -1;
        String inCase = label.toUpperCase();
        for (String colName : this.columnNames) {
            ++index;
            if (colName.toUpperCase().equals(inCase)) {
                return index;
            }
        }
        throw new SQLException();
    }

		/*
		 * Put into place to make compiling under Java 7 easier--not truly
		 * implemented
		 */
//		@Override
		public <T> T getObject(int columnIndex, Class<T> type) throws SQLException {
				return (T) getObject(columnIndex);
		}

//		@Override
		public <T> T getObject(String columnLabel, Class<T> type) throws SQLException {
				return (T) getObject(columnLabel);
		}

		private Object getColumnValue(String columnLabel) throws SQLException {
        int index = findColumn(columnLabel);
        return this.rows.get(this.currentIndex).get(index);
    }

}
