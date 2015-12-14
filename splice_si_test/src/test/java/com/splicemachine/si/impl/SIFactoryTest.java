package com.splicemachine.si.impl;

import com.splicemachine.si.api.driver.SIFactory;
import com.splicemachine.si.impl.driver.SIDriver;
import org.junit.Assert;
import org.junit.Test;
import com.splicemachine.si.api.SIFactory;

public class SIFactoryTest {
	@Test
	public void testSIFactory() {
		SIFactory factory = SIDriver.siFactory;
		Assert.assertNotNull("factor is null",factory);
		Assert.assertNotNull(factory.getDataLib());
		Assert.assertNotNull(factory.getTransactionLib());
	}
	
}
