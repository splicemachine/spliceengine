package com.splicemachine.si;

/**
 * @author Scott Fines
 *         Date: 2/18/14
 */
public class HBasePackedCompactionTest extends HBaseCompactionTest {
		static{
			CLASS_NAME = HBasePackedCompactionTest.class.getSimpleName();
		}

		public HBasePackedCompactionTest() {
				super();
				usePacked =true;
		}


}
