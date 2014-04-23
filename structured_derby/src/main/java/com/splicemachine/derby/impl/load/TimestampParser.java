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
