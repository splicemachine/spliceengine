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
/**
 * <p>
 * Machinery shared across Derby DRDA clients and server.
 * </p>
 */

package com.splicemachine.db.iapi.reference;

public	interface	DRDAConstants
{
	/////////////////////////////////////////////////////////////
	//
	//	CONSTANTS
	//
	/////////////////////////////////////////////////////////////

	//
	// Derby Product Identifiers as defined by the Open Group.
	// See http://www.opengroup.org/dbiop/prodid.htm for the
	// list of legal DRDA Product Identifiers.
	//
	String	DERBY_DRDA_SERVER_ID = "CSS";
	String	DERBY_DRDA_CLIENT_ID = "SNC";

    // Maximum size of a DDM block
	int DATA_STREAM_STRUCTURE_MAX_LENGTH = 32767;
	
	///////////////////////
	//
	// DRDA Type constants.
	//
	///////////////////////

	int DRDA_TYPE_INTEGER = 0x02;
	int DRDA_TYPE_NINTEGER = 0x03;
	int DRDA_TYPE_SMALL = 0x04;
	int DRDA_TYPE_NSMALL = 0x05;
	int DRDA_TYPE_1BYTE_INT = 0x06;
	int DRDA_TYPE_N1BYTE_INT = 0x07;
	int DRDA_TYPE_FLOAT16 = 0x08;
	int DRDA_TYPE_NFLOAT16 = 0x09;
	int DRDA_TYPE_FLOAT8 = 0x0A;
	int DRDA_TYPE_NFLOAT8 = 0x0B;
	int DRDA_TYPE_FLOAT4 = 0x0C;
	int DRDA_TYPE_NFLOAT4 = 0x0D;
	int DRDA_TYPE_DECIMAL = 0x0E;
	int DRDA_TYPE_NDECIMAL = 0x0F;
	int DRDA_TYPE_ZDECIMAL = 0x10;
	int DRDA_TYPE_NZDECIMAL = 0x11;
	int DRDA_TYPE_NUMERIC_CHAR = 0x12;
	int DRDA_TYPE_NNUMERIC_CHAR = 0x13;
	int DRDA_TYPE_RSET_LOC = 0x14;
	int DRDA_TYPE_NRSET_LOC = 0x15;
	int DRDA_TYPE_INTEGER8 = 0x16;
	int DRDA_TYPE_NINTEGER8 = 0x17;
	int DRDA_TYPE_LOBLOC = 0x18;
	int DRDA_TYPE_NLOBLOC = 0x19;
	int DRDA_TYPE_CLOBLOC = 0x1A;
	int DRDA_TYPE_NCLOBLOC = 0x1B;
	int DRDA_TYPE_DBCSCLOBLOC = 0x1C;
	int DRDA_TYPE_NDBCSCLOBLOC = 0x1D;
	int DRDA_TYPE_ROWID = 0x1E;
	int DRDA_TYPE_NROWID = 0x1F;
	int DRDA_TYPE_DATE = 0x20;
	int DRDA_TYPE_NDATE = 0x21;
	int DRDA_TYPE_TIME = 0x22;
	int DRDA_TYPE_NTIME = 0x23;
	int DRDA_TYPE_TIMESTAMP = 0x24;
	int DRDA_TYPE_NTIMESTAMP = 0x25;
	int DRDA_TYPE_FIXBYTE = 0x26;
	int DRDA_TYPE_NFIXBYTE = 0x27;
	int DRDA_TYPE_VARBYTE = 0x28;
	int DRDA_TYPE_NVARBYTE = 0x29;
	int DRDA_TYPE_LONGVARBYTE = 0x2A;
	int DRDA_TYPE_NLONGVARBYTE = 0x2B;
	int DRDA_TYPE_NTERMBYTE = 0x2C;
	int DRDA_TYPE_NNTERMBYTE = 0x2D;
	int DRDA_TYPE_CSTR = 0x2E;
	int DRDA_TYPE_NCSTR = 0x2F;
	int DRDA_TYPE_CHAR = 0x30;
	int DRDA_TYPE_NCHAR = 0x31;
	int DRDA_TYPE_VARCHAR = 0x32;
	int DRDA_TYPE_NVARCHAR = 0x33;
	int DRDA_TYPE_LONG = 0x34;
	int DRDA_TYPE_NLONG = 0x35;
	int DRDA_TYPE_GRAPHIC = 0x36;
	int DRDA_TYPE_NGRAPHIC = 0x37;
	int DRDA_TYPE_VARGRAPH = 0x38;
	int DRDA_TYPE_NVARGRAPH = 0x39;
	int DRDA_TYPE_LONGRAPH = 0x3A;
	int DRDA_TYPE_NLONGRAPH = 0x3B;
	int DRDA_TYPE_MIX = 0x3C;
	int DRDA_TYPE_NMIX = 0x3D;
	int DRDA_TYPE_VARMIX = 0x3E;
	int DRDA_TYPE_NVARMIX = 0x3F;
	int DRDA_TYPE_LONGMIX = 0x40;
	int DRDA_TYPE_NLONGMIX = 0x41;
	int DRDA_TYPE_CSTRMIX = 0x42;
	int DRDA_TYPE_NCSTRMIX = 0x43;
	int DRDA_TYPE_PSCLBYTE = 0x44;
	int DRDA_TYPE_NPSCLBYTE = 0x45;
	int DRDA_TYPE_LSTR = 0x46;
	int DRDA_TYPE_NLSTR = 0x47;
	int DRDA_TYPE_LSTRMIX = 0x48;
	int DRDA_TYPE_NLSTRMIX = 0x49;
	int DRDA_TYPE_SDATALINK = 0x4C;
	int DRDA_TYPE_NSDATALINK = 0x4D;
	int DRDA_TYPE_MDATALINK = 0x4E;
	int DRDA_TYPE_NMDATALINK = 0x4F;

	// --- Override LIDs 0x50 - 0xAF

    // this type is shown in the DRDA spec, volume 1, in the
    // section on SQLUDTGRP
	int DRDA_TYPE_UDT = 0x50;
	int DRDA_TYPE_NUDT = 0x51;
	int DRDA_TYPE_ARRAY = 0x52;
	int DRDA_TYPE_NARRAY = 0x53;

	int DRDA_TYPE_LOBBYTES = 0xC8;
	int DRDA_TYPE_NLOBBYTES = 0xC9;
	int DRDA_TYPE_LOBCSBCS = 0xCA;
	int DRDA_TYPE_NLOBCSBCS = 0xCB;
	int DRDA_TYPE_LOBCDBCS = 0xCC;
	int DRDA_TYPE_NLOBCDBCS = 0xCD;
	int DRDA_TYPE_LOBCMIXED = 0xCE;
	int DRDA_TYPE_NLOBCMIXED = 0xCF;

	int DRDA_TYPE_BOOLEAN = 0xBE;
	int DRDA_TYPE_NBOOLEAN = 0xBF;

    // This is the maximum size which a udt can serialize to in order to
    // be transported across DRDA
	int MAX_DRDA_UDT_SIZE = DATA_STREAM_STRUCTURE_MAX_LENGTH;
    
	///////////////////////
	//
	// DB2 datatypes
	//
	///////////////////////

	int DB2_SQLTYPE_DATE = 384;        // DATE
	int DB2_SQLTYPE_NDATE = 385;
	int DB2_SQLTYPE_TIME = 388;        // TIME
	int DB2_SQLTYPE_NTIME = 389;
	int DB2_SQLTYPE_TIMESTAMP = 392;   // TIMESTAMP
	int DB2_SQLTYPE_NTIMESTAMP = 393;
	int DB2_SQLTYPE_DATALINK = 396;    // DATALINK
	int DB2_SQLTYPE_NDATALINK = 397;

	int DB2_SQLTYPE_BLOB = 404;        // BLOB
	int DB2_SQLTYPE_NBLOB = 405;
	int DB2_SQLTYPE_CLOB = 408;        // CLOB
	int DB2_SQLTYPE_NCLOB = 409;
	int DB2_SQLTYPE_DBCLOB = 412;      // DBCLOB
	int DB2_SQLTYPE_NDBCLOB = 413;

	int DB2_SQLTYPE_VARCHAR = 448;     // VARCHAR(i) - varying length string
	int DB2_SQLTYPE_NVARCHAR = 449;
	int DB2_SQLTYPE_CHAR = 452;        // CHAR(i) - fixed length
	int DB2_SQLTYPE_NCHAR = 453;
	int DB2_SQLTYPE_LONG = 456;        // LONG VARCHAR - varying length string
	int DB2_SQLTYPE_NLONG = 457;
	int DB2_SQLTYPE_CSTR = 460;        // SBCS - null terminated
	int DB2_SQLTYPE_NCSTR = 461;
	int DB2_SQLTYPE_VARGRAPH = 464;    // VARGRAPHIC(i) - varying length
                                                  // graphic string (2 byte length)
												  int DB2_SQLTYPE_NVARGRAPH = 465;
	int DB2_SQLTYPE_GRAPHIC = 468;     // GRAPHIC(i) - fixed length graphic string                                                             */
	int DB2_SQLTYPE_NGRAPHIC = 469;
	int DB2_SQLTYPE_LONGRAPH = 472;    // LONG VARGRAPHIC(i) - varying length graphic string                                              */
	int DB2_SQLTYPE_NLONGRAPH = 473;
	int DB2_SQLTYPE_LSTR = 476;        // varying length string for Pascal (1-byte length)                                                     */
	int DB2_SQLTYPE_NLSTR = 477;

	int DB2_SQLTYPE_FLOAT = 480;       // FLOAT - 4 or 8 byte floating point
	int DB2_SQLTYPE_NFLOAT = 481;
	int DB2_SQLTYPE_DECIMAL = 484;     // DECIMAL (m,n)
	int DB2_SQLTYPE_NDECIMAL = 485;
	int DB2_SQLTYPE_ZONED = 488;       // Zoned Decimal -> DECIMAL(m,n)
	int DB2_SQLTYPE_NZONED = 489;

	int DB2_SQLTYPE_BIGINT = 492;      // BIGINT - 8-byte signed integer
	int DB2_SQLTYPE_NBIGINT = 493;
	int DB2_SQLTYPE_INTEGER = 496;     // INTEGER
	int DB2_SQLTYPE_NINTEGER = 497;
	int DB2_SQLTYPE_SMALL = 500;       // SMALLINT - 2-byte signed integer                                                                    */
	int DB2_SQLTYPE_NSMALL = 501;

	int DB2_SQLTYPE_NUMERIC = 504;     // NUMERIC -> DECIMAL (m,n)
	int DB2_SQLTYPE_NNUMERIC = 505;

	int DB2_SQLTYPE_ROWID = 904;           // ROWID
	int DB2_SQLTYPE_NROWID = 905;
	int DB2_SQLTYPE_BLOB_LOCATOR = 960;    // BLOB locator
	int DB2_SQLTYPE_NBLOB_LOCATOR = 961;
	int DB2_SQLTYPE_CLOB_LOCATOR = 964;    // CLOB locator
	int DB2_SQLTYPE_NCLOB_LOCATOR = 965;
	int DB2_SQLTYPE_DBCLOB_LOCATOR = 968;  // DBCLOB locator
	int DB2_SQLTYPE_NDBCLOB_LOCATOR = 969;

    int DB2_SQLTYPE_BOOLEAN = 2436;     // BOOLEAN
    int DB2_SQLTYPE_NBOOLEAN = 2437;

    // there is no DB2 type for UDTs. we invent one
	int DB2_SQLTYPE_FAKE_UDT = 2000;
    int DB2_SQLTYPE_FAKE_NUDT = 2001;
	int DB2_SQLTYPE_FAKE_ARRAY = 2002;

    // DB2 and DRDA support timestamps with microseconds precision, but not
    // nanoseconds precision: yyyy-mm-dd-hh.mm.ss.ffffff
    // In contrast, JDBC supports full nanoseconds precision: yyyy-mm-dd-hh.mm.ss.fffffffff
    //
	int DRDA_OLD_TIMESTAMP_LENGTH = 26;
    int DRDA_TIMESTAMP_LENGTH = 29;
    int JDBC_TIMESTAMP_LENGTH = 29;

    // Values for the EXTDTA stream status byte.
    // The use of this status byte is a product specific extension. The same
    // goes for the values below, they are not described by DRDA (nor DDM).

    /** Constant indicating a valid stream transfer. */
	byte STREAM_OK = 0x7F;
    /**
     * Constant indicating that the client encountered an error when reading
     * the user stream.
     */
	byte STREAM_READ_ERROR = 0x01;
    /** Constant indicating that the user stream was too short. */
	byte STREAM_TOO_SHORT = 0x02;
    /** Constant indicating that the user stream was too long. */
	byte STREAM_TOO_LONG = 0x04;
}
