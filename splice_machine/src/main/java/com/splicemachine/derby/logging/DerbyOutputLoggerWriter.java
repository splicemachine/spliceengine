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