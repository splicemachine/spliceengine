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

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.ArrayList;
import java.util.List;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.Activation;
import com.splicemachine.db.iapi.sql.conn.StatementContext;
import org.apache.log4j.Logger;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperationContext;
import com.splicemachine.utils.SpliceLogUtils;
import org.sparkproject.guava.base.Strings;

public abstract class NoRowsOperation extends SpliceBaseOperation {
	private static Logger LOG = Logger.getLogger(NoRowsOperation.class);
	final Activation activation;

	public NoRowsOperation(Activation activation)  throws StandardException {
		super(activation,-1,0d,0d);
		this.activation = activation;
        init();
    }
	
	@Override
	public void init(SpliceOperationContext context) throws StandardException, IOException {
		super.init(context);
	}
	
	@Override
	public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
		super.readExternal(in);
	}

	@Override
	public void writeExternal(ObjectOutput out) throws IOException {
		super.writeExternal(out);
	}
	
	@Override
	public SpliceOperation getLeftOperation() {
		return (SpliceOperation)null;
	}

	@Override
	public List<SpliceOperation> getSubOperations() {
		return new ArrayList<SpliceOperation>(0);
	}
	
	public final Activation getActivation() {
		SpliceLogUtils.trace(LOG, "getActivation");
		return activation;
	}

	protected void setup() throws StandardException {
		isOpen = true;
		StatementContext sc = activation.getLanguageConnectionContext().getStatementContext();
		if (sc == null) {
        	SpliceLogUtils.trace(LOG, "Cannot get StatementContext from Activation's lcc");
        	return;
        }
	}
	
	public boolean isClosed() {
		return !isOpen;
	}
	
	@Override
	public long getTimeSpent(int type)
	{
		return 0;
	}
	
	@Override
	public void close() throws StandardException {
		SpliceLogUtils.trace(LOG, "close in NoRows");
		if (!isOpen)
			return;
		try {
			super.close();
		} catch (Exception e) {
			SpliceLogUtils.error(LOG, e);
		}
	}

    @Override
    public String prettyPrint(int indentLevel) {
        String indent = "\n"+ Strings.repeat("\t", indentLevel);

        return new StringBuilder()
                .append(indent).append("resultSetNumber:").append(resultSetNumber)
                .toString();
    }

	@Override
	public void openCore() throws StandardException {
		super.openCore();

        /*
        We have to compute modifiedRowCount and badRecords here because if there's an Exception it has to
        propagate from here, otherwise Derby code down the line won't clean up things properly, see SPLICE-1470
         */
		computeModifiedRows();
	}
}
