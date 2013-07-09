package com.splicemachine.derby.impl.store.access.hbase;

import com.splicemachine.constants.SpliceConstants;
import com.splicemachine.derby.impl.store.access.SpliceAccessManager;
import com.splicemachine.derby.impl.store.access.base.OpenSpliceConglomerate;
import com.splicemachine.derby.impl.store.access.base.SpliceController;
import com.splicemachine.derby.utils.SpliceUtils;
import com.splicemachine.derby.utils.marshall.RowMarshaller;
import com.splicemachine.encoding.MultiFieldEncoder;
import com.splicemachine.storage.EntryEncoder;
import com.splicemachine.utils.SpliceLogUtils;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.services.io.FormatableBitSet;
import org.apache.derby.iapi.store.raw.Transaction;
import org.apache.derby.iapi.types.DataValueDescriptor;
import org.apache.derby.iapi.types.RowLocation;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.BitSet;


public class HBaseController  extends SpliceController {
	protected static Logger LOG = Logger.getLogger(HBaseController.class);

    private EntryEncoder entryEncoder;
	public HBaseController() {
		super();
	}

	public HBaseController(OpenSpliceConglomerate openSpliceConglomerate, Transaction trans) {
		super(openSpliceConglomerate, trans);
	}

	@Override
	public int insert(DataValueDescriptor[] row) throws StandardException {
        SpliceLogUtils.trace(LOG,"insert into conglom %d row %s with txnId %s",openSpliceConglomerate.getConglomerate().getContainerid(),row,transID);
        HTableInterface htable = SpliceAccessManager.getHTable(openSpliceConglomerate.getConglomerate().getContainerid());
		try {
            Put put = SpliceUtils.createPut(SpliceUtils.getUniqueKey(),transID);

            BitSet bitSet = getNonNullColumns(row);
            EntryEncoder eEncoder = EntryEncoder.create(row.length,bitSet);

            RowMarshaller.sparsePacked().fill(row, null, eEncoder.getEntryEncoder());

            put.add(SpliceConstants.DEFAULT_FAMILY_BYTES,RowMarshaller.PACKED_COLUMN_KEY,eEncoder.encode());
            htable.put(put);
			return 0;
		} catch (Exception e) {
			LOG.error(e.getMessage(),e);
		}finally{
            try {
                htable.close();
            } catch (IOException e) {
                SpliceLogUtils.warn(LOG,"Unable to close htable");
            }
        }
		return -1;
	}

	@Override
	public void insertAndFetchLocation(DataValueDescriptor[] row,
			RowLocation destRowLocation) throws StandardException {
        SpliceLogUtils.trace(LOG,"insertAndFetchLocation into conglom %d row %s",openSpliceConglomerate.getConglomerate().getContainerid(),row);
        HTableInterface htable = SpliceAccessManager.getHTable(openSpliceConglomerate.getConglomerate().getContainerid());
		try {
            Put put = SpliceUtils.createPut(SpliceUtils.getUniqueKey(),transID);
            BitSet setCols = getNonNullColumns(row);
            EntryEncoder eEncoder = EntryEncoder.create(row.length,setCols);

            MultiFieldEncoder encoder = eEncoder.getEntryEncoder();
            RowMarshaller.sparsePacked().fill(row, null, encoder);
            put.add(SpliceConstants.DEFAULT_FAMILY_BYTES,RowMarshaller.PACKED_COLUMN_KEY,eEncoder.encode());
			destRowLocation.setValue(put.getRow());
			htable.put(put);
		} catch (Exception e) {
			throw StandardException.newException("insert and fetch location error",e);
		} finally{
            try {
                htable.close();
            } catch (IOException e) {
                SpliceLogUtils.warn(LOG,"Unable to close HTable");
            }
        }
	}

    private BitSet getNonNullColumns(DataValueDescriptor[] row) {
        BitSet setCols = new BitSet(row.length);
        for(int i=0;i<row.length;i++){
            DataValueDescriptor dvd = row[i];
            if(dvd!=null && !dvd.isNull())
                setCols.set(i);
        }
        return setCols;
    }

    @Override
	public boolean replace(RowLocation loc, DataValueDescriptor[] row, FormatableBitSet validColumns) throws StandardException {
        SpliceLogUtils.trace(LOG,"replace rowLocation %s, destRow %s, validColumns %s",loc,row,validColumns);
        HTableInterface htable = SpliceAccessManager.getHTable(openSpliceConglomerate.getConglomerate().getContainerid());
		try {
            Put put = SpliceUtils.createPut(loc.getBytes(),transID);

            BitSet bitSet;
            if(validColumns!=null){
                bitSet = new BitSet(validColumns.size());
                for(int i=validColumns.anySetBit();i>=0;i=validColumns.anySetBit(i)){
                    if(row[i]!=null&&!row[i].isNull())
                        bitSet.set(i);
                }
            }else {
                bitSet = getNonNullColumns(row);
            }
            EntryEncoder eEncoder = EntryEncoder.create(row.length,bitSet);
            MultiFieldEncoder encoder = eEncoder.getEntryEncoder();
            encoder.reset();
            RowMarshaller.sparsePacked().fill(row, SpliceUtils.bitSetToMap(validColumns), encoder);
            put.add(SpliceConstants.DEFAULT_FAMILY_BYTES,RowMarshaller.PACKED_COLUMN_KEY,eEncoder.encode());
            htable.put(put);
			return true;
		} catch (Exception e) {
			throw StandardException.newException("Error during replace " + e);
		} finally{
            try {
                htable.close();
            } catch (IOException e) {
                SpliceLogUtils.warn(LOG,"Unable to close HTable");
            }
        }
	}

}
