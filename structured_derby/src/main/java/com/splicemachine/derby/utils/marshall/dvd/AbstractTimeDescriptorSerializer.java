package com.splicemachine.derby.utils.marshall.dvd;

import com.splicemachine.encoding.MultiFieldDecoder;
import com.splicemachine.encoding.MultiFieldEncoder;
import org.apache.derby.iapi.error.StandardException;
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

		protected static abstract class Factory implements DescriptorSerializer.Factory{
				@Override public boolean applies(DataValueDescriptor dvd) { return dvd!= null && applies(dvd.getTypeFormatId()); }
		}
}
