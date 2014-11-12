package com.splicemachine.derby.utils.marshall.dvd;

import org.apache.derby.iapi.types.DataValueDescriptor;

import java.util.Calendar;

/**
 * @author Scott Fines
 * Date: 4/3/14
 */
public class LazyTimeValuedSerializer extends LazyDescriptorSerializer implements TimeValuedSerializer {

		public LazyTimeValuedSerializer(TimeValuedSerializer delegate,String tableVersion) {
				super(delegate, tableVersion);
		}

		public static <T extends TimeValuedSerializer> Factory newFactory(final Factory<T> delegateFactory, final String tableVersion){
				return new Factory() {
						@Override public DescriptorSerializer newInstance() { return new LazyTimeValuedSerializer(delegateFactory.newInstance(),tableVersion); }
						@Override public boolean applies(DataValueDescriptor dvd) { return delegateFactory.applies(dvd); }
						@Override public boolean applies(int typeFormatId) { return delegateFactory.applies(typeFormatId); }

						@Override public boolean isScalar() { return delegateFactory.isScalar(); }
						@Override public boolean isFloat() { return delegateFactory.isFloat(); }
						@Override public boolean isDouble() { return delegateFactory.isDouble(); }
				};
		}

		@Override
		public void setCalendar(Calendar calendar) {
				((TimeValuedSerializer)delegate).setCalendar(calendar);
		}
}
