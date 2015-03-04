package com.splicemachine.derby.utils.marshall.dvd;

import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.iapi.types.DataValueDescriptor;

/**
 * @author Scott Fines
 *         Date: 4/3/14
 */
public interface SerializerMap {

		DescriptorSerializer getSerializer(DataValueDescriptor dvd);

		DescriptorSerializer getSerializer(int typeFormatId);

		DescriptorSerializer[] getSerializers(ExecRow row);

		DescriptorSerializer[] getSerializers(DataValueDescriptor[] kdvds);

		DescriptorSerializer getEagerSerializer(int typeFormatId);

		DescriptorSerializer[] getSerializers(int[] typeFormatIds);
}
