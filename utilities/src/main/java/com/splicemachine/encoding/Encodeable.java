/*
 * Copyright 2012 - 2016 Splice Machine, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */

package com.splicemachine.encoding;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Represents something that can be encoded into bytes, either directly on to
 * a stream or indirectly into a byte[] (depending on the caller's preference).
 *
 * @author Scott Fines
 *         Date: 2/24/15
 */
public interface Encodeable {

    void encode(DataOutput encoder) throws IOException;

    void decode(DataInput decoder) throws IOException;
}
