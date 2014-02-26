package com.splicemachine.si;

import org.junit.Assert;
import org.junit.Test;

/**
 * Packed (but not HBase) version of AsyncRollForwardTests
 * @author Scott Fines
 * Date: 2/17/14
 */
public class PackedAsyncRollForwardTest extends AsyncRollForwardTest{

		@Override
		public void setUp() throws Exception {
				usePacked=true;
				super.setUp();
		}
}
