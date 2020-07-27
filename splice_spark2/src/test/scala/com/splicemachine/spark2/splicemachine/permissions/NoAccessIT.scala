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
package com.splicemachine.spark2.splicemachine.permissions

import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class NoAccessIT extends PermissionsIT {
  override def userid: String = s"${module}_noaccess"
  override def password: String = s"${module}_noaccess"

  val schemaDoesntExist = s"java.sql.SQLSyntaxErrorException: Schema '$schema' does not exist"
  val noPrimaryKey = s"java.lang.UnsupportedOperationException: $internalTN has no Primary Key, Required for the Table to Perform "
  
  override val msgExceptionDF = schemaDoesntExist
  override val msgExceptionTruncate = schemaDoesntExist
  override val msgExceptionDelete = noPrimaryKey+"Deletes"
  override val msgExceptionInsert = schemaDoesntExist
  override val msgExceptionUpdate = noPrimaryKey+"Updates"
  override val msgExceptionUpdate2 = msgExceptionUpdate
  override val msgExceptionGetSchema = s"java.lang.Exception: Table/View '$internalTN' does not exist."
}
