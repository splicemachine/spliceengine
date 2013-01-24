package com.splicemachine.derby.utils.test;

import junit.framework.Assert;

import org.junit.Test;

import com.splicemachine.derby.utils.SpliceUtils;

public class SpliceUtilsTest {
	@Test 
	public void generateQuorumTest () {
		Assert.assertEquals("localhost:2181", SpliceUtils.generateQuorum());
	}
	
	@Test
	public void generateConglomSequence () {
		Assert.assertNotNull(SpliceUtils.generateConglomSequence());
	}
	
}
