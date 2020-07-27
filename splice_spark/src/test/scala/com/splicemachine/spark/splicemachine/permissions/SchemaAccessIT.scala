/*
 * Copyright (c) 2012 - 2020 Splice Machine, Inc.
 *
 * This file is part of Splice Machine.
 * Splice Machine is free software: you can redistribute it and/or modify it under the terms of the
 * GNU Affero General Public License as published by the Free Software Foundation, either
 * version 3, or (at your option) any later version.
 * Splice Machine is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU Affero General Public License for more details.
 * You should have received a copy of the GNU Affero General Public License along with Splice Machine.
 * If not, see <http://www.gnu.org/licenses/>.
 */
package com.splicemachine.spark.splicemachine.permissions

import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class SchemaAccessIT extends PermissionsIT {
  override def userid: String = s"${module}_SchemaAccessUser"
  override def password: String = s"${module}_SchemaAccessUserPwd"

  override val msgExceptionDF = s"java.sql.SQLSyntaxErrorException: User '${userid}' does not have SELECT permission on column 'A' of table '$schema'.'$table'."
  override val msgExceptionTruncate = s"java.sql.SQLSyntaxErrorException: User '${userid}' can not perform the operation in schema '$schema'."
  override val msgExceptionDelete = s"java.sql.SQLSyntaxErrorException: User '${userid}' does not have DELETE permission on table '$schema'.'$table'."
  override val msgExceptionInsert = s"java.sql.SQLSyntaxErrorException: User '${userid}' does not have INSERT permission on table '$schema'.'$table'."
  override val msgExceptionUpdate = s"java.sql.SQLSyntaxErrorException: User '${userid}' does not have UPDATE permission on column 'B' of table '$schema'.'$table'."
  override val msgExceptionUpdate2 = msgExceptionDF
  override val msgExceptionGetSchema = msgExceptionDF

  override def beforeAll(): Unit = {
    super.beforeAll
    execute(s"grant access on schema $schema to ${userid}")
  }
}
