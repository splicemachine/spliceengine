package com.splicemachine.derby.impl.sql.execute.operations;

import com.splicemachine.derby.hbase.SpliceDriver;
import com.splicemachine.derby.hbase.SpliceObserverInstructions;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperationContext;
import com.splicemachine.derby.impl.spark.SpliceSpark;
import com.splicemachine.derby.impl.store.access.SpliceAccessManager;
import com.splicemachine.derby.jdbc.SpliceTransactionResourceImpl;
import com.splicemachine.utils.SpliceLogUtils;
import org.apache.derby.iapi.sql.Activation;
import org.apache.derby.jdbc.EmbeddedDriver;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

/**
 * Created by dgomezferro on 4/4/14.
 */
public abstract class SparkOperation2<Op extends SpliceBaseOperation, From, From2, To>
		implements Function2<From, From2, To>, Externalizable {

	protected static Logger LOG = Logger.getLogger(TableScanOperation.class);

	SpliceObserverInstructions soi;
	SpliceTransactionResourceImpl impl;
	Activation activation;
	SpliceOperationContext context;
	Op op;
	private boolean prepared;

	public SparkOperation2() {
	}

	protected SparkOperation2(Op spliceOperation, SpliceObserverInstructions soi) {
		this.soi = soi;
		this.op = spliceOperation;
	}

	@Override
	public void writeExternal(ObjectOutput out) throws IOException {
		out.writeObject(soi);
		out.writeObject(op);
	}

	public void readExternalInContext(ObjectInput in) throws IOException, ClassNotFoundException
	{}

	@Override
	public final void readExternal(ObjectInput in)
			throws IOException, ClassNotFoundException {

		SpliceSpark.setupSpliceStaticComponents();

		soi = (SpliceObserverInstructions) in.readObject();
		op = (Op) in.readObject();
		boolean prepared = false;
		try {
			impl = new SpliceTransactionResourceImpl();
			impl.prepareContextManager();
			prepared = true;
			impl.marshallTransaction(soi.getTxn());
			activation = soi.getActivation(impl.getLcc());
			context = SpliceOperationContext.newContext(activation);
			op.init(context);

			readExternalInContext(in);
		} catch (Exception e) {
			SpliceLogUtils.logAndThrowRuntime(LOG, e);
		} finally {
			if (prepared) {
				impl.resetContextManager();
			}
		}
	}
}