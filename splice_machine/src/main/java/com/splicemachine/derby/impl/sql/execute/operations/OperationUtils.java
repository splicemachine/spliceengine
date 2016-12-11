
/*
 * Copyright 2012 - 2016 Splice Machine, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
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
