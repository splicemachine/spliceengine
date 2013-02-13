package com.splicemachine.derby.impl.storage;

import java.util.NoSuchElementException;

import com.splicemachine.derby.iapi.storage.RowProvider;
import com.splicemachine.utils.SpliceLogUtils;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.sql.execute.ExecRow;
import org.apache.derby.iapi.sql.execute.NoPutResultSet;
import org.apache.derby.iapi.types.RowLocation;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.log4j.Logger;

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

	public static RowProvider sourceProvider(NoPutResultSet source, Logger log){
		return new SourceRowProvider(source,log);
	}
    public static RowProvider emptyProvider() {
        return EMPTY_PROVIDER;
    }

	public static abstract class DelegatingRowProvider implements RowProvider{
		protected final RowProvider provider;

		protected DelegatingRowProvider(RowProvider provider) {
			this.provider = provider;
		}

		@Override public void open() { provider.open(); }
		@Override public void close() { provider.close(); }
		@Override public Scan toScan() { return provider.toScan(); }
		@Override public byte[] getTableName() { return provider.getTableName(); }
		@Override public int getModifiedRowCount() { return provider.getModifiedRowCount(); }
		@Override public boolean hasNext() { return provider.hasNext(); }
		@Override public void remove() { provider.remove(); }

		@Override
		public RowLocation getCurrentRowLocation() {
			return provider.getCurrentRowLocation();
		}

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

	public static class SourceRowProvider implements RowProvider{
		private final NoPutResultSet source;
		private final Logger log;
		//private boolean populated = false;
		private ExecRow nextEntry;

		private SourceRowProvider(NoPutResultSet source, Logger log) {
			this.source = source;
			this.log = log;
		}

		@Override
		public void open() {
			try {
				source.open();
			} catch (StandardException e) {
				SpliceLogUtils.logAndThrowRuntime(log,e);
			}
		}

		@Override
		public void close() {
			try {
				source.close();
			} catch (StandardException e) {
				SpliceLogUtils.logAndThrowRuntime(log,e);
			}
		}

		@Override public RowLocation getCurrentRowLocation() { return null; }
		@Override public Scan toScan() { return null; }
		@Override public byte[] getTableName() { return null; }
		@Override public void remove() { throw new UnsupportedOperationException(); }

		@Override
		public int getModifiedRowCount() {
			return source.modifiedRowCount();
		}

		@Override
		public boolean hasNext() {
			//if(populated==true) return true;
			try {
				nextEntry = source.getNextRowCore();
			} catch (StandardException e) {
				SpliceLogUtils.logAndThrowRuntime(log,e);
			}
			return nextEntry!=null;
		}

		@Override
		public ExecRow next() {
			//if(!populated) return null;
			//populated=false;
			return nextEntry;
		}

	}
}
