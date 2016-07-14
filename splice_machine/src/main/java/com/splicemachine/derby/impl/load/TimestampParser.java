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

package com.splicemachine.derby.impl.load;

import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;

/**
 * @author Scott Fines
 * Date: 4/2/14
 */
public class TimestampParser {
		private final SimpleDateFormat timestampFormat;
		private final boolean appendZeros;

		public TimestampParser(String timestampFormat){
				this.timestampFormat = timestampFormat!=null? new SimpleDateFormat(timestampFormat):null;
				this.appendZeros = timestampFormat != null && (timestampFormat.endsWith("Z") || timestampFormat.endsWith("X"));
		}

		public Timestamp parse(String text) throws ParseException {
				if(timestampFormat==null){
						try{
								return Timestamp.valueOf(text);
						}catch(IllegalArgumentException iae){
								throw new ParseException(iae.getMessage(),0);
						}
				}
				else{
						if(appendZeros)
								text+="00";
						return new Timestamp(timestampFormat.parse(text).getTime());
				}
		}
}
