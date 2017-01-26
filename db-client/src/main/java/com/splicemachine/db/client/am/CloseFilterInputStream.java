/*
 * Apache Derby is a subproject of the Apache DB project, and is licensed under
 * the Apache License, Version 2.0 (the "License"); you may not use these files
 * except in compliance with the License. You may obtain a copy of the License at:
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 * Splice Machine, Inc. has modified this file.
 *
 * All Splice Machine modifications are Copyright 2012 - 2017 Splice Machine, Inc.,
 * and are licensed to you under the License; you may not use this file except in
 * compliance with the License.
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 */

package com.splicemachine.db.client.am;

import java.io.InputStream;
import java.io.FilterInputStream;

import java.io.IOException;
import com.splicemachine.db.shared.common.reference.MessageId;

class CloseFilterInputStream extends FilterInputStream {
	
	private static final String ALREADY_CLOSED_ERR_MESSAGE = 
            SqlException.getMessageUtil().getTextMessage(
                MessageId.OBJECT_CLOSED);
	
	private boolean closed;
	
	public CloseFilterInputStream(InputStream is){
		
		super(is);
		closed = false;
		
	}
	
	
	public int read() 
		throws IOException {

		if(closed){
			throw new IOException(ALREADY_CLOSED_ERR_MESSAGE);
		}
		
		return super.read();
		
	}
	

	public int read(byte[] b) 
		throws IOException {
		
		if(closed){
			throw new IOException(ALREADY_CLOSED_ERR_MESSAGE);
		}

		return super.read(b);

	}
	
	
	public int read(byte[] b,
			int off,
			int len) 
		throws IOException{
		
		if(closed){
			throw new IOException(ALREADY_CLOSED_ERR_MESSAGE);
		}

		return super.read(b, off, len);

	}

	
	public long skip(long n)
		throws IOException{

		if(closed){
			throw new IOException(ALREADY_CLOSED_ERR_MESSAGE);
		}
		
		return super.skip(n);
		
	}
	
	
	public int available()
		throws IOException{
		
		if(closed){
			throw new IOException(ALREADY_CLOSED_ERR_MESSAGE);
		}

		return super.available();
		
	}
	
	
	public void close()
		throws IOException{
		
		super.close();
		closed = true;
		
	}
	
	
}
