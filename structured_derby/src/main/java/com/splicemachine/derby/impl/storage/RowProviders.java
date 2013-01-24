package com.splicemachine.derby.impl.storage;

import java.util.NoSuchElementException;

import com.splicemachine.derby.iapi.storage.RowProvider;
import org.apache.derby.iapi.sql.execute.ExecRow;
import org.apache.derby.iapi.types.RowLocation;
import org.apache.hadoop.hbase.client.Scan;

/**
 * Static utility methods for managing RowProviders.
 *
 * This class provides implementations for common, obvious RowProvider situations:
 * empty Providers, providers of a single row, etc. as well as utilities relating to
 * RowProviders.
 */
public class RowProviders {

	private static final RowProvider EMPTY_PROVIDER = new RowProvider(){
		@Override public boolean hasNext() { return false; }
		@Override public ExecRow next() { return null; }
		@Override public void remove() { throw new UnsupportedOperationException(); }
		@Override public void open() { }
		@Override public void close() { }
		@Override public RowLocation getCurrentRowLocation() { return null; }
		@Override public Scan toScan() { return null; }
		@Override public byte[] getTableName() { return null; }

		@Override public int getModifiedRowCount() { return 0; }
	};

	private RowProviders(){}
	
	public static RowProvider singletonProvider(ExecRow row){
		return new SingletonRowProvider(row);
	}

    public static RowProvider emptyProvider() {
        return EMPTY_PROVIDER;
    }

	private static class SingletonRowProvider implements RowProvider{
		private final ExecRow row;
		private boolean emitted = false;
		
		SingletonRowProvider(ExecRow row){
			this.row = row;
		}

        @Override public void open() { emitted = false; }
		@Override public boolean hasNext() { return !emitted; }

		@Override
		public ExecRow next() {
			if(!hasNext()) throw new NoSuchElementException();
			emitted=true;
			return row;
		}

		@Override public void close() {  } //do nothing
        @Override public RowLocation getCurrentRowLocation() { return null; }
        @Override public Scan toScan() { return null; }
        @Override public byte[] getTableName() { return null; }

		@Override
		public int getModifiedRowCount() {
			return 0;
		}

		@Override public void remove() { throw new UnsupportedOperationException("Remove not supported"); }
    }

}
