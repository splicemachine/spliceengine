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

package com.splicemachine.derby.utils.marshall.dvd;

import com.splicemachine.db.iapi.types.DataValueDescriptor;

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
