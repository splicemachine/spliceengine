package com.splicemachine.derby.utils.marshall.dvd;

import java.util.Calendar;

/**
 * @author Scott Fines
 *         Date: 4/3/14
 */
public interface TimeValuedSerializer extends DescriptorSerializer{

		void setCalendar(Calendar calendar);

}
