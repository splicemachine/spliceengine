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
import org.spark_project.guava.base.Strings;

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
}
