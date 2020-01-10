/*
 * Copyright (c) 2012 - 2020 Splice Machine, Inc.
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
