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
 * All such Splice Machine modifications are Copyright 2012 - 2020 Splice Machine, Inc.,
 * and are licensed to you under the GNU Affero General Public License.
 */
package com.splicemachine.dbTesting.system.optimizer.query;

/**
 * 
 * Class Query3: Returns a list of queries that Selects from multiple views with
 * joins on columns having indexes
 * 
 */

public class Query3 extends GenericQuery {

	public Query3() {
		description = "Select from multiple views with joins on columns having indexes";
		generateQueries();
	}

	/**
	 */
	public void generateQueries() {
		queries
				.add("select v8.col5, v8.col2 , v8.col3  from v8 inner join v8_2 on v8.col4=v8_2.col4 where (v8.col1>100 and v8.col1<110) union all select v8.col5, v8.col6 , v8_2.col7  from v8 inner join v8_2 on v8.col7=v8_2.col7 where (v8.col1>100 and v8.col1<110)");
		queries
				.add("select v16.col5, v16.col2 , v16.col3  from v16 inner join v16_2 on v16.col4=v16_2.col4 where (v16.col1>100 and v16.col1<110) union all select v16.col5, v16.col6 , v16_2.col7  from v16 inner join v16_2 on v16.col7=v16_2.col7 where (v16.col1>100 and v16.col1<110)");
		queries
				.add("select v32.col5, v32.col2 , v32.col3  from v32 inner join v32_2 on v32.col4=v32_2.col4 where (v32.col1>100 and v32.col1<110) union all select v32.col5, v32.col6 , v32_2.col7  from v32 inner join v32_2 on v32.col7=v32_2.col7 where (v32.col1>100 and v32.col1<110)");
		queries
				.add("select v42.col5, v42.col2 , v42.col3  from v42 inner join v42_2 on v42.col4=v42_2.col4 where (v42.col1>100 and v42.col1<110) union all select v42.col5, v42.col6 , v42_2.col7  from v42 inner join v42_2 on v42.col7=v42_2.col7 where (v42.col1>100 and v42.col1<110)");

	}

}
