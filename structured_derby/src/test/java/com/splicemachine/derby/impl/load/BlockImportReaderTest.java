package com.splicemachine.derby.impl.load;

import com.splicemachine.utils.ByteDataInput;
import com.splicemachine.utils.ByteDataOutput;
import org.apache.hadoop.fs.BlockLocation;
import org.junit.Assert;
import org.junit.Test;

/**
 * @author Scott Fines
 * Date: 12/18/13
 */
public class BlockImportReaderTest {

		@Test
		public void testBlockImportReaderSerializesAndDeserializesCorrectly() throws Exception {
				String[] names = {"node1", "node2", "node3"};
				String[] hosts = {"node1", "node2", "node3"};
				long offset = 123456l;
				long length = 123454l;
				BlockLocation location = new BlockLocation(names, hosts, offset, length);
				BlockImportReader reader = new BlockImportReader(location);

				ByteDataOutput bdo = new ByteDataOutput();
				reader.writeExternal(bdo);

				byte[] bytes = bdo.toByteArray();

				BlockImportReader decoded = new BlockImportReader();
				decoded.readExternal(new ByteDataInput(bytes));

				BlockLocation decodedLocation = decoded.getLocation();
				Assert.assertArrayEquals("Incorrect names!",names,decodedLocation.getNames());
				Assert.assertArrayEquals("Incorrect hosts!",names,decodedLocation.getHosts());
				Assert.assertEquals("Incorrect offset!",offset,decodedLocation.getOffset());
				Assert.assertEquals("Incorrect length!",length,decodedLocation.getLength());
		}
}
