package com.splicemachine.derby.impl.load;

import java.io.IOException;
import java.io.Reader;

/**
 * @author Scott Fines
 *         Date: 1/24/14
 */
public class MeasuredReader extends Reader {
		private long charsRead = 0l;
		private final Reader delegate;

		public MeasuredReader(Reader delegate) {
				this.delegate = delegate;
		}

		public MeasuredReader(Object lock, Reader delegate) {
				super(lock);
				this.delegate = delegate;
		}

		@Override
		public int read(char[] cbuf, int off, int len) throws IOException {
				int chars = delegate.read(cbuf,off,len);
				charsRead+=chars;
				return chars;
		}

		@Override
		public void close() throws IOException {
			delegate.close();
		}

		public long getCharsRead(){
				return charsRead;
		}
}
