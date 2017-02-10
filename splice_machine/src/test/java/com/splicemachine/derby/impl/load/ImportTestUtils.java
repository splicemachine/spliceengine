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

package com.splicemachine.derby.impl.load;

import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.iapi.types.DataValueDescriptor;
import org.junit.Assert;

import com.splicemachine.db.iapi.error.StandardException;
import java.util.Comparator;

/**
 * @author Scott Fines
 *         Created on: 9/30/13
 */
public class ImportTestUtils{

    // WARNING: if you need to add a method to this class, you might need to add it
    // to SpliceUnitTest in engine_it module instead, for proper dependencies.
    // For example, printMsgSQLState and createBadLogDirectory were moved there.

    private ImportTestUtils(){}

    public static Comparator<ExecRow> columnComparator(int columnPosition){
        return new ExecRowComparator(columnPosition);
    }

    public static void assertRowsEquals(ExecRow correctRow,ExecRow actualRow){
        DataValueDescriptor[] correctRowArray=correctRow.getRowArray();
        DataValueDescriptor[] actualRowArray=actualRow.getRowArray();
        for(int dvdPos=0;dvdPos<correctRow.nColumns();dvdPos++){
            Assert.assertEquals("Incorrect column at position "+dvdPos,
                    correctRowArray[dvdPos],
                    actualRowArray[dvdPos]);

        }
    }

    private static class ExecRowComparator implements Comparator<ExecRow> {
        private final int colNumber;

        private ExecRowComparator(int colNumber) {
            this.colNumber = colNumber;
        }

        @Override
        public int compare(ExecRow o1, ExecRow o2) {
            if(o1==null){
                if(o2==null) return 0;
                else return -1;
            }else if(o2==null)
                return 1;
            else{
                try{
                    return o1.getColumn(colNumber).compare(o2.getColumn(colNumber));
                } catch (StandardException e) {
                    throw new RuntimeException(e);
                }
            }
        }
    }

}
