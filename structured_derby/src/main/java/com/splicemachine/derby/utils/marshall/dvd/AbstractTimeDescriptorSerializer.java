package com.splicemachine.derby.utils.marshall.dvd;

import org.apache.derby.iapi.types.DataValueDescriptor;

import java.util.Calendar;
import java.util.GregorianCalendar;

/**
 * @author Scott Fines
 * Date: 4/2/14
 */
public abstract class AbstractTimeDescriptorSerializer implements TimeValuedSerializer{
		private Calendar calendar;

		@Override
		public void setCalendar(Calendar calendar){
				this.calendar = calendar;
		}

		protected Calendar getCalendar(){
				if(calendar==null)
						calendar = new GregorianCalendar();
				return calendar;
		}

		@Override public boolean isScalarType() { return true; }
		@Override public boolean isFloatType() { return false; }
		@Override public boolean isDoubleType() { return false; }

		@Override public void close() { calendar = null; }

		protected static abstract class Factory implements DescriptorSerializer.Factory{
				@Override public boolean applies(DataValueDescriptor dvd) { return dvd!= null && applies(dvd.getTypeFormatId()); }

				@Override public boolean isScalar() { return true; }
				@Override public boolean isFloat() { return false; }
				@Override public boolean isDouble() { return false; }
		}
}
