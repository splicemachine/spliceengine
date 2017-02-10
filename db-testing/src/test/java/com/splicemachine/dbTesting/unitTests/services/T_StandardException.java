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

package com.splicemachine.dbTesting.unitTests.services;

import com.splicemachine.db.iapi.error.StandardException;

import java.text.MessageFormat;
/**
  A standard exception for testing.

  The messages for this exception are not localized or stored
  with the product.
 */
public class T_StandardException extends StandardException
{
	String msgText = "Message text not set";

	protected T_StandardException(String messageID, String msgText)
	{
		super(messageID);
		myConstructorCommon( messageID, msgText );
	}
	protected T_StandardException(String messageID, String msgText, Throwable t)
	{
		super(messageID, t, (Object[]) null);
		myConstructorCommon( messageID, msgText );
	}
	protected T_StandardException(String messageID, String msgText, Throwable t, Object[] args)
	{
		super(messageID, t, args);
		myConstructorCommon( messageID, msgText );
	}

	protected	void	myConstructorCommon( String messageID, String msgText )
	{
		this.msgText = msgText;
	}

	public static
	StandardException newT_StandardException(String messageID, Throwable t, String msgText)
	{
		return new T_StandardException(messageID,msgText,t);
	}

	public static
	StandardException newT_StandardException(String messageID, String msgText)
	{
		return new T_StandardException(messageID,msgText);
	}

	public String getMessage() {return MessageFormat.format(msgText, getArguments());}
	public String getErrorProperty() {throw new Error("method not supported");}
}
