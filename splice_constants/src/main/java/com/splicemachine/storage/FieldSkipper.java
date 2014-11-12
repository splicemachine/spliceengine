package com.splicemachine.storage;

import com.splicemachine.encoding.MultiFieldDecoder;

/**
 * @author Scott Fines
 * Date: 4/4/14
 */
public interface FieldSkipper {

		void skipField(MultiFieldDecoder decoder,int position);
}
