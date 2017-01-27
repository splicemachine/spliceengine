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

/*

/**
	DRDAProtocolException is the root of all protocol exceptions that are 
	handled in a standard fashion by the DRDA AS.
	If a protocol error message needs to send more than 
	SVRCOD, an ERRCD and CODPNT arg it should be subclassed

*/

package com.splicemachine.db.impl.drda;
import java.util.Hashtable;
import com.splicemachine.db.iapi.services.sanity.SanityManager;

class DRDAProtocolException extends Exception
{
	
	/* Static values, used in constructor if there is no associated 
	   Error Code or the codepoint argument.
	*/
	
	protected static final int NO_ASSOC_ERRCD = 0;
	protected static final int NO_CODPNT_ARG  = 0;

	
	private DRDAConnThread agent;

	// request correlation id
	private int correlationID;
	
	// correlation token
	private byte[] crrtkn;

	//Codepoint arg
	private int codpntArg;

	private DRDAProtocolExceptionInfo exceptionInfo;

	// CodePoint of this error
	private int errorCodePoint;
	
	// Severity Code
	private int svrcod;
	
	// error code (e.g. SYNERRCD)
	private int errcd;
	
	// messageid for logging errors.
	private String  messageid;

	// database name
	private String rdbnam;
	
	// database diagnostic information
	private String srvdgn;

	// message arguments
	private Object [] messageArgs;
	
	// A verbose error message string, will be helpful
	// when getMessage() is called on this Exception object
	private String msg;
    
	private static Hashtable errorInfoTable;
	
	protected static String DRDA_Proto_CMDCHKRM=	"DRDA_Proto_CMDCHKRM";
	protected static String DRDA_Proto_CMDNSPRM=	"DRDA_Proto_CMDNSPRM";
	protected static String DRDA_Proto_DTAMCHRM=	"DRDA_Proto_DTAMCHRM";

	protected static String DRDA_Proto_OBJNSPRM =	"DRDA_Proto_OBJNSPRM";
	protected static String DRDA_Proto_PKGBNARM=	"DRDA_Proto_PKGBNARM";
	protected static String DRDA_Proto_PRCCNVRM=   "DRDA_Proto_PRCCNVRM";
	protected static String DRDA_Proto_PRMNSRM =   "DRDA_Proto_PRMNSPRM";

	protected static String DRDA_Proto_SYNTAXRM=   "DRDA_Proto_SYNTAXRM";
	protected static String DRDA_Proto_VALNSPRM=   "DRDA_Proto_VALNSPRM";
	protected static String DRDA_Proto_MGRLVLRM=   "DRDA_Proto_MGRLVLRM";
	protected static String DRDA_Proto_RDBNFNRM=   "DRDA_Proto_RDBNFNRM";

	protected static String DRDA_Disconnect=	   "DRDA_Disconnect";
	protected static String DRDA_AgentError=	   "DRDA_AgentError";
    protected static String DRDA_Timeout=          "DRDA_Timeout";
    protected static String DRDA_InvalidClient =   "DRDA_InvalidClient";

	static {
	/* Create the errorInfoTable
	   The Hashtable is keyed on messageid and holds 
	   DRDAProtocolExceptionInfo for each of our messages.
	*/
	
	errorInfoTable = new Hashtable();
	
	errorInfoTable.put(
			   DRDA_Proto_CMDCHKRM,
			   new  DRDAProtocolExceptionInfo(
							  CodePoint.CMDCHKRM,
							  CodePoint.SVRCOD_ERROR,
							  NO_ASSOC_ERRCD,
							  false));

	errorInfoTable.put(
			   DRDA_Proto_CMDNSPRM,
			   new  DRDAProtocolExceptionInfo(
							  CodePoint.CMDNSPRM,
							  CodePoint.SVRCOD_ERROR,
							  NO_ASSOC_ERRCD,
							  true));
	errorInfoTable.put(
			   DRDA_Proto_DTAMCHRM,
			   new  DRDAProtocolExceptionInfo(
							  CodePoint.DTAMCHRM,
							  CodePoint.SVRCOD_ERROR,
							  NO_ASSOC_ERRCD,
							  false));
	errorInfoTable.put(
			   DRDA_Proto_OBJNSPRM,
			   new  DRDAProtocolExceptionInfo(
							  CodePoint.OBJNSPRM,
							  CodePoint.SVRCOD_ERROR,
							  NO_ASSOC_ERRCD,
							  true));
		
	errorInfoTable.put(
					   DRDA_Proto_PKGBNARM,
					   new  DRDAProtocolExceptionInfo(
							   CodePoint.PKGBNARM,
							   CodePoint.SVRCOD_ERROR,
							   NO_ASSOC_ERRCD,
							   false));
			   
	errorInfoTable.put(DRDA_Proto_PRCCNVRM,
			   new DRDAProtocolExceptionInfo(
							 CodePoint.PRCCNVRM,
							 CodePoint.SVRCOD_ERROR,
							 CodePoint.PRCCNVCD,
							 false));

	errorInfoTable.put(DRDA_Proto_SYNTAXRM,
			   new DRDAProtocolExceptionInfo(
							 CodePoint.SYNTAXRM,
							 CodePoint.SVRCOD_ERROR,
							 CodePoint.SYNERRCD,
							 true));

	errorInfoTable.put(DRDA_Proto_VALNSPRM,
			   new DRDAProtocolExceptionInfo(
							 CodePoint.VALNSPRM,
							 CodePoint.SVRCOD_ERROR,
							 NO_ASSOC_ERRCD,
							 true));

	errorInfoTable.put(DRDA_Proto_MGRLVLRM,
			   new DRDAProtocolExceptionInfo(
							 CodePoint.MGRLVLRM,
							 CodePoint.SVRCOD_ERROR,
							 NO_ASSOC_ERRCD,
							 false));

	errorInfoTable.put(DRDA_Proto_RDBNFNRM,
			   new DRDAProtocolExceptionInfo(
							 CodePoint.RDBNFNRM,
							 CodePoint.SVRCOD_ERROR,
							 NO_ASSOC_ERRCD,
							 false));

			   
	errorInfoTable.put(DRDA_Disconnect,
			   new DRDAProtocolExceptionInfo(
							 0,
							 0,
							 NO_ASSOC_ERRCD,
							 false));

    // Permanent Agent Error (AGNPRMRM) Reply Message indicates that the command
    // requested could not be completed because of a permanent error
    // condition detected at the target system.
	errorInfoTable.put(DRDA_AgentError,
			   new DRDAProtocolExceptionInfo(
							 CodePoint.AGNPRMRM,
							 CodePoint.SVRCOD_PRMDMG,
							 NO_ASSOC_ERRCD,
							 false));


	errorInfoTable.put(DRDA_Timeout,
			   new DRDAProtocolExceptionInfo(
							 0,
							 0,
							 NO_ASSOC_ERRCD,
							 false));

	}
		
	
	/**  Create a new Protocol exception 
	 *
	 * @param agent		DRDAConnThread  that threw this exception
	 *
	 * @param cpArg		CODPNT value  to pass to send
	 *
	 *
	 * @param msgid		  The messageid for this message. (needs to be
	 * integrated into logging mechanism)
	 *
	 * @param args		   Argments for the message in an Object[]
	 *
	 */
	
	DRDAProtocolException(String msgid,
									DRDAConnThread agent, 
									int cpArg, 
									int errCdArg, Object []args)
						
	{
		
		boolean agentError = false;

		exceptionInfo = 
			(DRDAProtocolExceptionInfo) errorInfoTable.get(msgid);
				
		if (agent != null)
		{
			this.correlationID = agent.getCorrelationID();
			this.crrtkn = agent.getCrrtkn();
		}

		this.codpntArg= cpArg;
		this.errorCodePoint = exceptionInfo.errorCodePoint;
		this.errcd = errCdArg;
		this.messageid = msgid;

		if (msgid.equals(DRDA_AgentError))
		{
			this.svrcod = ((Integer)args[0]).intValue();
			this.rdbnam = (String)args[1];
            // retrieve the server diagnostic error message 
            String srvdgn = (String)args[2];
			msg = "Execution failed because of Permanent Agent Error: SVRCOD = " +
				java.lang.Integer.toHexString(this.svrcod) +
				"; RDBNAM = "+ rdbnam +"; diagnostic msg = "+ srvdgn;
			agentError = true;
		}
		else if (msgid.equals(DRDA_Proto_RDBNFNRM))
		{
			this.svrcod = exceptionInfo.svrcod;
			this.rdbnam = (String)args[0];
			msg = "Execution failed because of Distributed Protocol Error:  " 
				+ messageid +
				"; RDBNAM = "+ rdbnam;
		}
		else if (cpArg == CodePoint.PRDID) {
			this.svrcod = exceptionInfo.svrcod;
			msg = "Execution failed because of invalid client connection attempt, please use Slice Machine Driver.";
		}
		else
		{
			this.svrcod = exceptionInfo.svrcod;
			msg = "Execution failed because of a Distributed Protocol Error:  " 
				+ messageid +
				"; CODPNT arg  = " + java.lang.Integer.toHexString(cpArg)  +
				"; Error Code Value = " + java.lang.Integer.toHexString(errCdArg);
			if (msgid.equals(DRDA_Proto_SYNTAXRM)) {
				msg += ". Plaintext connection attempt from an SSL enabled client?";
			}
		}
		
		
		if (!agentError && args != null)
		{
			messageArgs = args;
			for (int i = 0; i < args.length; i++)
			{
				//args contain managers and manager levels display in hex
				if (msgid.equals(DRDA_Proto_MGRLVLRM))
					msg += "," + 
						java.lang.Integer.toHexString(((Integer)args[i]).intValue());
				else
					msg += "," + args[i];
				
			}
		}


		// for now dump all errors except disconnects to console		
		// and log
		if (!isDisconnectException())
		{
			DRDAConnThread.println2Log(agent.getDbName(),
								   agent.getSession().drdaID, 
								   msg);
			NetworkServerControlImpl s = agent.getServer();
			s.consoleExceptionPrintTrace(this);
		}
	}
	
	// Constructor with no additional args
	DRDAProtocolException(String msgid,
									DRDAConnThread agent, 
									int cpArg, 
									int errCdArg)
	{
		this(msgid,agent,  cpArg, errCdArg, (Object []) null);
	}


	protected static DRDAProtocolException newDisconnectException(DRDAConnThread
																  agent,Object[] args)
	{
		return new DRDAProtocolException(DRDA_Disconnect,
										 agent,
										 NO_CODPNT_ARG,
										 NO_ASSOC_ERRCD,
										 args);
		
	}
	
	protected static DRDAProtocolException newAgentError(DRDAConnThread agent,
		int svrcod, String rdbnam, String srvdgn)
	{
        if ( SanityManager.DEBUG )
            agent.trace("agentError in " + agent);
		Object[] oa = {new Integer(svrcod), rdbnam, srvdgn};
		return new DRDAProtocolException(DRDA_AgentError,
										agent,
										NO_CODPNT_ARG,
										NO_ASSOC_ERRCD,
										oa);
	}
	
	protected final boolean isDisconnectException()
	{
		return (errorCodePoint == 0);
	}
	
	/** write will write the Error information to the buffer.
	 * Most errors will write only the codepoint and svrcod 
	 * Where appropriate the codepoint specific error code and
	 * codePoint of origin will be written
	 *
	 * @param writer  The DDMWriter for the agent.
	 */
	
	protected void write(DDMWriter writer)
	{
		//Writing Protocol Error
		writer.createDssReply();
		writer.startDdm(errorCodePoint);
		writer.writeScalar2Bytes(CodePoint.SVRCOD,svrcod);
		if (exceptionInfo.sendsCodpntArg)
			writer.writeScalar2Bytes(CodePoint.CODPNT,codpntArg);
		if (exceptionInfo.errCdCodePoint !=  NO_ASSOC_ERRCD)
			writer.writeScalar1Byte(exceptionInfo.errCdCodePoint,
									errcd);
		if (rdbnam != null && agent != null)
		{
			try {
				agent.writeRDBNAM(rdbnam);
			} catch (DRDAProtocolException e) {} //ignore exceptions while processing
		}
		// for MGRLVLRM, need to write out the manager levels
		if (errorCodePoint == CodePoint.MGRLVLRM)
		{
			writer.startDdm(CodePoint.MGRLVLLS);
			for (int i = 0; i < messageArgs.length ; i += 2)
			{
				writer.writeNetworkShort(((Integer)messageArgs[i]).intValue());
				writer.writeNetworkShort(((Integer)messageArgs[i+1]).intValue());
			}
			writer.endDdm();
		}
		writer.endDdmAndDss();
	}
    
    /**
     * Override getMessage() 
     * @return the server diagnostic error message for this exception
     */
    public String getMessage()
    {
        return msg;
    }
}


/**
 * Class used to transport that fact that we had a timeout and should
 * *NOT* disconnect the connection.
 * See DRDAConnThread.run()
 */
class DRDASocketTimeoutException extends DRDAProtocolException {
    DRDASocketTimeoutException(DRDAConnThread t) {
        super(DRDA_Timeout,t,0,0);
    }
}
