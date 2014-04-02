package com.splicemachine.derby.utils.marshall.dvd;

import org.apache.derby.iapi.types.DataValueDescriptor;

import java.util.Calendar;

/**
 * @author Scott Fines
 * Date: 4/3/14
 */
public class LazyTimeValuedSerializer extends LazyDescriptorSerializer implements TimeValuedSerializer {

		public LazyTimeValuedSerializer(TimeValuedSerializer delegate) {
				super(delegate);
		}

		public static <T extends TimeValuedSerializer> Factory newFactory(final Factory<T> delegateFactory){
				return new Factory() {
						@Override public DescriptorSerializer newInstance() { return new LazyTimeValuedSerializer(delegateFactory.newInstance()); }
						@Override public boolean applies(DataValueDescriptor dvd) { return delegateFactory.applies(dvd); }
						@Override public boolean applies(int typeFormatId) { return delegateFactory.applies(typeFormatId); }
				};
		}

		@Override
		public void setCalendar(Calendar calendar) {
				((TimeValuedSerializer)delegate).setCalendar(calendar);
		}
}
