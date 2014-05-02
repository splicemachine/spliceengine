package com.splicemachine.derby.impl.sql.execute.operations;

import com.carrotsearch.hppc.BitSet;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.iapi.sql.execute.SpliceRuntimeContext;
import com.splicemachine.derby.utils.SpliceUtils;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.services.io.FormatableBitSet;
import org.apache.derby.iapi.sql.Activation;
import org.apache.derby.iapi.sql.execute.ExecIndexRow;
import org.apache.derby.iapi.sql.execute.ExecRow;
import org.apache.derby.iapi.store.access.Qualifier;
import org.apache.derby.iapi.store.access.ScanController;
import org.apache.derby.iapi.types.DataValueDescriptor;
import org.apache.hadoop.hbase.client.Scan;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.ArrayList;
import java.util.List;

/**
 * @author Scott Fines
 *         Created on: 10/1/13
 */
public class MultiProbeDerbyScanInformation extends DerbyScanInformation{
    private DataValueDescriptor[] probeValues;
    private DataValueDescriptor probeValue;
    public MultiProbeDerbyScanInformation(String resultRowAllocatorMethodName,
                                          String startKeyGetterMethodName,
                                          String stopKeyGetterMethodName,
                                          String scanQualifiersField,
                                          long conglomId,
                                          int colRefItem,
                                          boolean sameStartStopPosition,
                                          int startSearchOperator,
                                          int stopSearchOperator,
                                          DataValueDescriptor[] probeValues) {
        super(resultRowAllocatorMethodName, startKeyGetterMethodName, stopKeyGetterMethodName,
                scanQualifiersField, conglomId, colRefItem, sameStartStopPosition, startSearchOperator, stopSearchOperator);
        this.probeValues = probeValues;
    }

    @Deprecated
    public MultiProbeDerbyScanInformation() { }

		@Override
		protected ExecIndexRow getStopPosition() throws StandardException {
				ExecIndexRow stopPosition = sameStartStopPosition?super.getStartPosition():super.getStopPosition();
				if (stopPosition != null) {
						stopPosition.getRowArray()[0] = probeValue;
				}
				return stopPosition;
		}

	@Override
	protected ExecIndexRow getStartPosition() throws StandardException {
		ExecIndexRow startPosition = super.getStartPosition();
        if(sameStartStopPosition)
            startSearchOperator = ScanController.NA;
		if(startPosition!=null)
            startPosition.getRowArray()[0] = probeValue; 
		return startPosition;
	}

	@Override
    public List<Scan> getScans(String txnId, ExecRow startKeyOverride, Activation activation, SpliceOperation top, SpliceRuntimeContext spliceRuntimeContext) throws StandardException {
        /*
         * We must build the proper scan here in pieces
         */
        BitSet colsToReturn = new BitSet();
        FormatableBitSet accessedCols = getAccessedColumns();
        if (accessedCols != null) {
            for (int i = accessedCols.anySetBit(); i >= 0; i = accessedCols.anySetBit(i)) {
                colsToReturn.set(i);
            }
        }
        List<Scan> scans = new ArrayList<Scan>(probeValues.length);
        for (int i = 0; i < probeValues.length; i++) {
            probeValue = probeValues[i];
            Scan scan = getScan(txnId);
            SpliceUtils.setInstructions(scan, activation, top, spliceRuntimeContext);
            scans.add(scan);
        }
        return scans;
    }

	@Override
    protected Qualifier[][] populateQualifiers() throws StandardException {
		Qualifier[][] qualifiers = super.populateQualifiers();
		if(qualifiers!=null){
			/*
			 * The first qualifier is the qualifier for the start and stop keys, so
			 * set it on that field.
			 */
			Qualifier[] ands  = qualifiers[0];
			if(ands!=null){
					Qualifier first = ands[0];
					if(first!=null){
							first.clearOrderableCache();
							first.getOrderable().setValue(probeValue);
					}
			}
		}
		return qualifiers;
	}

		@Override
		public void writeExternal(ObjectOutput out) throws IOException {
				super.writeExternal(out);
				out.writeInt(probeValues.length);
				for(DataValueDescriptor dvd:probeValues){
						out.writeObject(dvd);
				}
                out.writeObject(probeValue);
		}

		@Override
		public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
				super.readExternal(in);
				probeValues = new DataValueDescriptor[in.readInt()];
				for(int i=0;i<probeValues.length;i++){
						probeValues[i] = (DataValueDescriptor)in.readObject();
				}
                probeValue = (DataValueDescriptor)in.readObject();
		}
}
