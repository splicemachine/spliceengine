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

package com.splicemachine.db.impl.tools.ij;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLWarning;

import java.util.List;
import java.util.ArrayList;

/**
 * This impl is intended to be used with multiple resultsets, where
 * the execution of the statement is already complete.
 */
public class ijMultipleResultSetResult extends ijResultImpl {

    List resultSets = null;

    int[] displayColumns = null;
    int[] columnWidths = null;

    /**
     * Create a ijResultImpl that represents multiple result sets, only
     * displaying a subset of the columns, using specified column widths.
     * 
     * @param resultSets The result sets to display
     * @param display Which column numbers to display, or null to display
     *                all columns.
     * @param widths  The widths of the columns specified in 'display', or
     *                null to display using default column sizes.
     */
    public ijMultipleResultSetResult(List resultSets, int[] display,
                                     int[] widths) throws SQLException {
        this.resultSets = new ArrayList();
        this.resultSets.addAll(resultSets);

        displayColumns = display;
        columnWidths   = widths;
    }


    public void addResultSet(ResultSet rs){
        resultSets.add(rs);
    }

    public boolean isMultipleResultSetResult(){
        return true;
    }

    public List getMultipleResultSets() {
        return resultSets;
    }

    public void closeStatement() throws SQLException {
        if (resultSets != null) {
            ResultSet rs = null;
            for (int i = 0; i<resultSets.size(); i++){
                rs = (ResultSet)resultSets.get(i);
                if(rs.getStatement() != null) rs.getStatement().close();
                else rs.close(); 
            }
        }
    }

    public int[] getColumnDisplayList() { return displayColumns; }
    public int[] getColumnWidthList() { return columnWidths; }

    /**
     * @return the warnings from all resultsets as one SQLWarning chain
     */
    public SQLWarning getSQLWarnings() throws SQLException { 
        SQLWarning warning = null;
        ResultSet rs = null;
        for (int i=0; i<resultSets.size(); i++){
            rs = (ResultSet)resultSets.get(i);
            if (rs.getWarnings() != null) {
                if (warning == null) warning = rs.getWarnings();
                else                 warning.setNextWarning(rs.getWarnings());
            }
        }
        return warning;
    }
    
    /**
     * Clears the warnings in all resultsets
     */
    public void clearSQLWarnings() throws SQLException {
        for (int i=0; i<resultSets.size(); i++){
            ((ResultSet)resultSets.get(i)).clearWarnings();
        }
    }
}
