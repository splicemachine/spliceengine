package com.splicemachine.derby.logging;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.Writer;
import org.apache.log4j.Logger;


	public class DerbyOutputLoggerWriter extends PrintWriter {
		private static Logger LOG = Logger.getLogger(DerbyOutputLoggerWriter.class);
		public DerbyOutputLoggerWriter() {
			super( new InternalCategoryWriter(LOG), true );
		}

		static class InternalCategoryWriter extends Writer {
			protected Logger logger;
			private boolean closed;

			public InternalCategoryWriter(final Logger logger) {
				lock = logger;
				//synchronize on this category
				this.logger = logger;
			}

			public void write( char[] cbuf, int off, int len ) throws IOException {
				if ( closed ) {
					throw new IOException( "Called write on closed Writer" );
				}
				// Remove the end of line chars
				while ( len > 0 && ( cbuf[len - 1] == '\n' || cbuf[len - 1] == '\r' ) ) {
					len--;
				}
				if ( len > 0 ) {
					logger.trace(String.copyValueOf( cbuf, off, len ));
				}
			}


			public void flush() throws IOException {
				if ( closed ) {
					throw new IOException( "Called flush on closed Writer" );
				}
			}

			public void close() {
				closed = true;
			}
		}
}