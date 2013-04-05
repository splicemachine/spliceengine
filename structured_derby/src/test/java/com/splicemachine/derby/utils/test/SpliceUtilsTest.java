package com.splicemachine.derby.utils.test;

import com.splicemachine.derby.utils.ConglomerateUtils;
import org.junit.Assert;
import org.junit.Test;
import com.splicemachine.derby.utils.SpliceUtils;
import java.io.IOException;

public class SpliceUtilsTest {
	@Test 
	public void generateQuorumTest () {
		Assert.assertEquals("localhost:2181", SpliceUtils.generateQuorum());
	}
	
	@Test
	public void generateConglomSequence () throws IOException {
		Assert.assertNotNull(ConglomerateUtils.getNextConglomerateId());
	}
	
}
