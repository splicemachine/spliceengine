/*

   Derby - Class org.apache.derby.iapi.sql.execute.HasIncrement

   Licensed to the Apache Software Foundation (ASF) under one or more
   contributor license agreements.  See the NOTICE file distributed with
   this work for additional information regarding copyright ownership.
   The ASF licenses this file to you under the Apache License, Version 2.0
   (the "License"); you may not use this file except in compliance with
   the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.

 */
package org.apache.derby.iapi.sql.execute;

import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.types.DataValueDescriptor;

public interface HasIncrement {

	/**
	 * increment will get the auto-incremented value of the columnPosition
   * specified for the target table. If increment is non-zero we will also update
   * the autoincrement value.
   *
   * @param columnPosition	position of the column in the table (1-based)
   * @param increment				the amount to increment by
   *
   * @throws StandardException if anything goes wrong
	 */
	DataValueDescriptor increment(int columnPosition, long increment)
		throws StandardException;
}
