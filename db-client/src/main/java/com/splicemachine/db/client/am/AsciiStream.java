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

import java.io.StringReader;

public class AsciiStream extends java.io.InputStream {
    private java.io.Reader reader_;
    private String materializedString_;
    private int charsRead_ = 0;
	
	public AsciiStream(String materializedString){
		this(materializedString,new StringReader(materializedString));
	}
	
    public AsciiStream(String materializedString, java.io.Reader reader) {
        reader_ = reader;
        materializedString_ = materializedString;
    }

    public int read() throws java.io.IOException {
        int oneChar = reader_.read();
        ++charsRead_;
        if (oneChar != -1) // if not eos
        {
		if(oneChar <= 0x00ff)
			return oneChar;
		else
			return 0x003f;
		
        } else {
            return -1; // end of stream
        }
    }

    public int available() {
        return materializedString_.length() - charsRead_;
    }
}
