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

package com.splicemachine.db.impl.tools.ij;


import java.sql.SQLException;
import java.sql.Connection;

/*
	An interface for running xa tests.
	The real implementation is only loaded if the requisite javax classes are
	in the classpath. 
 */
interface xaAbstractHelper
{

	void XADataSourceStatement(ij parser, Token dbname, Token shut, String create) throws SQLException;
	void XAConnectStatement(ij parser, Token user, Token pass, String id) throws SQLException;
	void XADisconnectStatement(ij parser, String n) throws SQLException;
	Connection XAGetConnectionStatement(ij parser, String n) throws SQLException;
	void CommitStatement(ij parser, Token onePhase, Token twoPhase, int xid) throws SQLException;
	void EndStatement(ij parser, int flag, int xid) throws SQLException;
	void ForgetStatement(ij parser, int xid) throws SQLException;
	void PrepareStatement(ij parser, int xid) throws SQLException;
	ijResult RecoverStatement(ij parser, int flag) throws SQLException;
	void RollbackStatement(ij parser, int xid) throws SQLException;
	void StartStatement(ij parser, int flag, int xid) throws SQLException;
	Connection DataSourceStatement(ij parser, Token dbname, Token protocol,
								   Token userT, Token passT, String id) throws SQLException;
	void CPDataSourceStatement(ij parser, Token dbname, Token protocol) throws SQLException;
	void CPConnectStatement(ij parser, Token userT, Token passT, String n) throws SQLException;
	Connection CPGetConnectionStatement(ij parser, String n) throws SQLException;
	void CPDisconnectStatement(ij parser, String n) throws SQLException;
	void setFramework(String framework);

}
