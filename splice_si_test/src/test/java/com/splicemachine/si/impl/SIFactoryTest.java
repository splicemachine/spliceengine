package com.splicemachine.si.impl;

import org.junit.Assert;
import org.junit.Test;
import com.splicemachine.si.api.SIFactory;

public class SIFactoryTest {
	@Test
	public void testSIFactory() {
		SIFactory factory = SIFactoryDriver.siFactory;
		Assert.assertNotNull("factor is null",factory);
		Assert.assertNotNull(factory.getDataLib());
		Assert.assertNotNull(factory.getTransactionLib());
	}
	
}
