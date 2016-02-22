
package com.splicemachine.derby.impl.sql.execute.operations;

import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import java.util.LinkedList;
import java.util.List;

public class OperationUtils {

	private OperationUtils(){}

	public static void generateLeftOperationStack(SpliceOperation op,List<SpliceOperation> opAccumulator){
		SpliceOperation leftOp = op.getLeftOperation();
		if(leftOp !=null){
			generateLeftOperationStack(leftOp,opAccumulator);
			opAccumulator.add(leftOp);
		}
		opAccumulator.add(op);
	}

    public static void generateAllOperationStack(SpliceOperation op, List<SpliceOperation> opAccumulator){
        SpliceOperation leftOp = op.getLeftOperation();
        if(leftOp !=null){
            //recursively generateLeftOperationStack
            generateAllOperationStack(leftOp,opAccumulator);
			opAccumulator.add(leftOp);
        }

        SpliceOperation rightOp = op.getRightOperation();
        if(rightOp !=null){
            //recursively generateLeftOperationStack
            generateAllOperationStack(rightOp,opAccumulator);
			opAccumulator.add(rightOp);
        }
        opAccumulator.add(op);
    }

}
