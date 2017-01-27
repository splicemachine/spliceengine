
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

package com.splicemachine.derby.impl.sql.execute.operations;

import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;

import java.util.List;

public class OperationUtils {

	private OperationUtils(){}

	public static void generateLeftOperationStack(SpliceOperation op,List<SpliceOperation> opAccumulator){
		SpliceOperation leftOp = op.getLeftOperation();
		if(leftOp !=null){
			generateLeftOperationStack(leftOp,opAccumulator);
//			opAccumulator.add(leftOp);
		}
		opAccumulator.add(op);
	}

    public static void generateAllOperationStack(SpliceOperation op, List<SpliceOperation> opAccumulator){
        SpliceOperation leftOp = op.getLeftOperation();
        if(leftOp !=null){
            //recursively generateLeftOperationStack
            generateAllOperationStack(leftOp,opAccumulator);
//			opAccumulator.add(leftOp);
        }

        SpliceOperation rightOp = op.getRightOperation();
        if(rightOp !=null){
            //recursively generateLeftOperationStack
            generateAllOperationStack(rightOp,opAccumulator);
//			opAccumulator.add(rightOp);
        }
        opAccumulator.add(op);
    }

}
