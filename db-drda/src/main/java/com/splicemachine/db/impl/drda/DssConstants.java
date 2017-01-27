/*
 * This file is part of Splice Machine.
 * Splice Machine is free software: you can redistribute it and/or modify it under the terms of the
 * GNU Affero General Public License as published by the Free Software Foundation, either
 * version 3, or (at your option) any later version.
 * Splice Machine is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU Affero General Public License for more details.
 * You should have received a copy of the GNU Affero General Public License along with Splice Machine.
 * If not, see <http://www.gnu.org/licenses/>.
 *
 * Some parts of this source code are based on Apache Derby, and the following notices apply to
 * Apache Derby:
 *
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
 * Splice Machine, Inc. has modified the Apache Derby code in this file.
 *
 * All such Splice Machine modifications are Copyright 2012 - 2017 Splice Machine, Inc.,
 * and are licensed to you under the GNU Affero General Public License.
 */


package com.splicemachine.db.impl.drda;

/**
	This class defines DSS constants that are shared in the classes implementing
	the DRDA protocol.
*/
class DssConstants
{

  protected static final int MAX_DSS_LENGTH = 32767;

  // Registered DSS identifier indicating DDM data (xD0 for DDM data).
  protected static final int DSS_ID = 0xD0;

  // DSS chaining bit.
  protected static final int DSS_NOCHAIN = 0x00;
  protected static final int DSSCHAIN = 0x40;

  // DSS chaining bit for continuation on error
  protected static final int DSSCHAIN_ERROR_CONTINUE = 0x20;

  // DSS chaining bit where next DSS has same correlation ID.
  protected static final int DSSCHAIN_SAME_ID = 0x50;

  // DSS formatter for an OBJDSS.
  protected static final int DSSFMT_OBJDSS = 0x03;

  // DSS formatter for an RPYDSS.
  protected static final int DSSFMT_RPYDSS = 0x02;

  // DSSformatter for an RQSDSS.
  protected static final int DSSFMT_RQSDSS = 0x01;

  // DSS request correlation id unknown value
  protected static final int CORRELATION_ID_UNKNOWN = -1;

  // DSS length continuation bit
  protected static final int CONTINUATION_BIT = 0x8000;

 // Registered SNA GDS identifier indicating DDM data (xD0 for DDM data).
  static final int GDS_ID = 0xD0;

  // GDS chaining bits.
  static final int GDSCHAIN = 0x40;

  // GDS chaining bits where next DSS has different correlation ID.
  static final int GDSCHAIN_SAME_ID = 0x50;

  // GDS formatter for an OBJDSS.
  static final int GDSFMT_OBJDSS = 0x03;

  // GDS formatter for an RPYDSS.
  static final int GDSFMT_RPYDSS = 0x02;

  // GDS formatter for an RQSDSS.
  static final int GDSFMT_RQSDSS = 0x01;

  // hide the default constructor
  private DssConstants () {}
}
