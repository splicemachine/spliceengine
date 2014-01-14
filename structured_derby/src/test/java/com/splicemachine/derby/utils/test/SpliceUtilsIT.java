package com.splicemachine.derby.utils.test;

import com.splicemachine.derby.utils.ConglomerateUtils;
import org.junit.Assert;
import org.junit.Test;
import java.io.IOException;

public class SpliceUtilsIT { 
	
	@Test
	public void generateConglomSequence () throws IOException {
		Assert.assertNotNull(ConglomerateUtils.getNextConglomerateId());
	}
	
}
