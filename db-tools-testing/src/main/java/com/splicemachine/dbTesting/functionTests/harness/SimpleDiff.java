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

//SimpleDiff.java
package com.splicemachine.dbTesting.functionTests.harness;

import java.io.IOException;
import java.io.BufferedInputStream;
import java.util.Vector;
import java.io.PrintWriter;
import java.io.BufferedReader;
import java.io.FileReader;

public class SimpleDiff
{

	PrintWriter pw;

	boolean debugOn = Boolean.getBoolean("simplediff.debug");
	int debugLevel = 1;

	boolean diffsFound = true;

	int lookAhead = 20;

	public void debug(int level, String msg)
	{
		if (debugLevel >= level)
		{
			debug(msg);
		}
	}
	public void debug(String msg)
	{
		if (debugOn)
		{
			System.out.println("DEBUG: " + msg);
		}
	}

	int lineCount(String file) throws IOException
	{
		BufferedReader input = new BufferedReader(new FileReader(file));
		int count = 0;
		String aLine = input.readLine();
		while (aLine != null)
		{
			count++;
			aLine = input.readLine();
		}
		input.close();
		return count;
	}


	public String[] readFile(BufferedReader input) throws IOException
	{

		Vector vec = new Vector();

		String aLine = "";
		//int count = 0;
		aLine = input.readLine();
		while (aLine != null)
		{
			vec.addElement(aLine);
			//count++;
			aLine = input.readLine();
		}
		input.close();

		String rV[] = new String[vec.size()];
		//debug(2, ""+count + " lines in " + filename);
		debug(2, ""+vec.size() + " lines in input");
		vec.copyInto(rV);

		return rV;
	}

	public void printFile(String file[])
	{
		for (int i = 0; i < file.length; i++)
		{
			pw.println(i + ": " + file[i]);
			System.out.println(i + ": " + file[i]);
		}
	}

	public static String usage = "java SimpleDiff <file1> <file2>";

	/**
        @param file1 Name of first file to be read
        @param file2 Name of file with which to compare
        @return String array of file contents
        @exception IOException thrown by underlying calls
            to the file system
	 */

	public String[] diffFiles(  DiffBuffer file1,
			DiffBuffer file2)
					throws IOException
					{

		int currentLine1 = 0;
		int currentLine2 = 0;
		Vector returnVec = new Vector();

		while ( file1.isValidOffset(currentLine1) &&
				file2.isValidOffset(currentLine2))
		{
			String f1 = file1.lineAt(currentLine1);
			String f2 = file2.lineAt(currentLine2);

			// special strings we are not able to get rid of yet but are not actual errors
			// - also empty lines if we have any
			while (f2.contains("SpliceService initialized") 
					|| f2.contains("log4j:WARN") 
					|| f2.contains("connection made to existing database instead") 
					|| f2.contains("SCDynamicStore")
					|| f2.contains("Splice Beta")
					|| f2.contains("RecoverableZooKeeper")
					|| f2.length() == 0) {
				debug(1,"Ignoring " + f2);
				currentLine2++;
				f2 = file2.lineAt(currentLine2);
			}

			// skip empty lines on source as well
			while (f1.length() == 0) {
				currentLine1++;
				f1 = file1.lineAt(currentLine1);
			}

			if (f1.equals(f2))
			{
				debug(1, currentLine1 + ", " + f1 + ": match");
				currentLine1++;
				currentLine2++;
				file1.setLowWater(currentLine1);
				file2.setLowWater(currentLine2);
			}
			else
			{
				debug(1, currentLine1 + ", " + f1 + ", " + f2 + ": no match");
				boolean foundMatch = false;
				int checkLine2 = currentLine2;
				int checkCount = 1;
				//                while ( (currentLine2 + checkCount) < (file2Length - 1) &&
				while ( file2.isValidOffset(currentLine2 + checkCount) &&
						checkCount < lookAhead)
				{
					debug(1, "currentLine1 " + currentLine1 + "  currentLine2 " + (currentLine2 +checkCount));
					debug(1, "about to reference file2[" + (currentLine2 + checkCount) + "]");
					f2 = file2.lineAt(currentLine2 + checkCount);
					debug(2, "did");
					if (f1.equals(f2))
					{
						foundMatch = true;
						if (checkCount > 1)
						{
							returnVec.addElement(currentLine1 + "a" + (currentLine2 + 1) + "," + (currentLine2 + checkCount));
						}
						else
						{
							returnVec.addElement(currentLine1 + "a" + (currentLine2 + 1));
						}

						for (int j = 0; j < checkCount; j++)
						{
							returnVec.addElement("> " +
									file2.lineAt(currentLine2 + j) );
						}
						currentLine2 = currentLine2 + checkCount;
						checkCount = 0;

						// This break statement was commented out, which
						// caused problems in the diff output.  I don't
						// know why it was commented out, and uncommenting
						// it fixed the problem.
						//
						//			-	Jeff Lichtman
						//				March 24, 1999
						break;
					}
					checkCount ++;
				}
				if (!foundMatch && file2.isValidOffset(currentLine2))
				{
					int checkLine1 = currentLine1;
					checkCount = 1;
					f2 = file2.lineAt(currentLine2);
					while ( file1.isValidOffset(currentLine1 + checkCount) &&
							checkCount < lookAhead)
					{
						debug(1, "currentLine2 " + currentLine2 + "  currentLine1 " + (currentLine1 + checkCount));
						f1 = file1.lineAt(currentLine1 + checkCount);
						if ( f2.equals(f1))
						{
							foundMatch = true;
							if (checkCount > 1)
							{
								returnVec.addElement((currentLine1 + 1) + "," + (currentLine1 + checkCount) + "d" + currentLine2);
							}
							else
							{
								returnVec.addElement((currentLine1 + 1) + "d" + currentLine2);
							}

							for (int j = 0; j < checkCount; j++)
							{
								returnVec.addElement("< " +
										file1.lineAt(currentLine1 + j) );

							}
							currentLine1 = currentLine1 + checkCount;
							checkCount = 0;
							debug(1, "continuing");
							break;
						}
						checkCount ++;
					}

				}
				if (!foundMatch)
				{
					debug(1, currentLine1 + ": NOMATCH");
					returnVec.addElement((currentLine1 + 1) +" del");// + (currentLine2 + 1));
					returnVec.addElement("< " + file1.lineAt(currentLine1));
					currentLine1++;
				}
				else
				{
					currentLine1++;
					currentLine2++;
					file1.setLowWater(currentLine1);
					file2.setLowWater(currentLine2);
				}
			}
		}

		if (file1.isValidOffset(currentLine1))
		{
			returnVec.addElement((currentLine2) + " del");
			for (int i = currentLine1; file1.isValidOffset(i); i++)
			{
				returnVec.addElement("< " + file1.lineAt(i));
			}
		}
		if (file2.isValidOffset(currentLine2))
		{
			returnVec.addElement((currentLine1) + " add");
			for (int i = currentLine2; file2.isValidOffset(i); i++)
			{
				returnVec.addElement("> " + file2.lineAt(i));
			}
		}

		file1.close();
		file2.close();

		if (returnVec.size() == 0)
		{
			return null;
		}


		String [] returnArray = new String[returnVec.size()];
		returnVec.copyInto(returnArray);
		return returnArray;


					}

	private void reportMemory()
	{
		reportMemory(null);
	}

	private void reportMemory(String header)
	{
		if (header != null)
		{
			System.out.println(header);
		}
		long free = Runtime.getRuntime().freeMemory();
		long total = Runtime.getRuntime().totalMemory();

		System.out.println("total:         " + total );
		System.out.println("free:          " + free );
		System.out.println("used:          " + (total - free));
		System.gc();
		System.out.println("used: <postgc> " + (Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory()));
		System.out.println("  ");
	}

	public boolean doWork(BufferedReader in1, BufferedReader in2, PrintWriter localPW) throws IOException
	{
		this.pw=(localPW==null?new PrintWriter(System.out):localPW);
		try
		{
			DiffBuffer db1 = new DiffBuffer(in1, "1");
			DiffBuffer db2 = new DiffBuffer(in2, "2");

			String diffs[] = diffFiles(db1, db2);

			if (diffs == null)
			{
				debug(1, "no diff");
				return false;
			}
			else
			{
				for (int i = 0; i < diffs.length; i++)
				{
					this.pw.println(diffs[i]);
					System.out.println(diffs[i]);
				}
			}

		}
		catch (IOException ioe)
		{
			System.err.println("IOException comparing <" + in1 +
					"> and <" + in2 + ">");
			System.err.println(ioe);

			this.pw.println("IOException comparing <" + in1 +
					"> and <" + in2 + ">");
			this.pw.println(ioe);
		}
		return true;
	}
	public static void main(String args[]) throws IOException
	{

		if (args.length < 2)
		{
			System.err.println("Invalid number of arguments");
			System.err.println("Usage: " + usage);
			System.exit(1);
		}

		SimpleDiff me = new SimpleDiff();

		try
		{
			BufferedReader br1 = new BufferedReader(new FileReader(args[0]));
			BufferedReader br2 = new BufferedReader(new FileReader(args[1]));
			me.doWork(br1, br2, null);
		}
		catch (IOException ioe)
		{
			System.out.println("IOExeption: " + ioe);
		}
	}

	public void pause()
	{
		BufferedInputStream bis = new BufferedInputStream(System.in);
		try
		{
			bis.read();
		}
		catch (IOException ioe)
		{
			pw.println("Error trying to pause...");
			System.out.println("Error trying to pause...");
		}
	}

	class DiffBuffer extends Vector
	{


		public boolean atEOF()
		{
			return atEnd;
		}

		public DiffBuffer(BufferedReader rb)
		{
			this(rb, "");
		}

		public DiffBuffer(BufferedReader rb, String name)
		{
			this (rb, name, 1024);
		}

		public DiffBuffer(BufferedReader rb, String name, int size)
		{
			super(size);
			readBuffer = rb;
			currentLowWater = 0;
			currentHighWater = -1;
			oldLow = 0;
			myName = name;
			atEnd = false;
		}

		public boolean isValidOffset(int lineNumber) throws IOException
		{
			if (atEnd)
			{
				return lineNumber <= actualEndOfFile;
			}

			if (lineAt(lineNumber) == null)
			{
				return false;
			}
			return true;
		}

		public String lineAt(int offset) throws IOException
		{
			/*
System.out.println("offset: " + offset);
System.out.println("currentHighWater: " + currentHighWater);
System.out.println("");
			 */
			 if (offset > currentHighWater)
			 {
				 for (int i = 0; i < offset - currentHighWater; i++)
				 {
					 String aLine = readBuffer.readLine();
					 addElement(aLine);
					 /*
System.out.println("aLine: " + aLine);
					  */
					 if (aLine == null)
					 {
						 if (!atEnd)
						 {
							 //first time we've tried to read past the EOF
							 actualEndOfFile = currentHighWater + i;
							 //System.out.println(myName + ": length " + actualEndOfFile);
							 atEnd = true;
						 }
					 }
				 }
				 currentHighWater = offset;
			 }
			 return (String) elementAt(offset);
		}


		public final String EMPTY = null;
		protected BufferedReader readBuffer;
		protected int currentLowWater;
		protected int currentHighWater;
		private int oldLow;
		protected String myName;
		protected boolean atEnd;
		protected int actualEndOfFile;
		/**
            Useful to keep memory requirements low
		 */
		public void setLowWater(int newLow)
		{

			for (int i = oldLow; i < newLow; i++)
			{
				setElementAt(EMPTY, i);
			}
			currentLowWater = newLow;
			oldLow = newLow -1;
		}


		public void iterate(boolean verbose)
		{
			int nulls = 0;
			int nonnulls = 0;


			for (int i = 0; i < this.size(); i++)
			{
				if (elementAt(i) == null)
				{
					if (verbose)
					{
						System.out.print("[" + i + "] ");
						System.out.println("null");
					}
					nulls++;

				}
				else
				{
					if (verbose)
					{
						System.out.print("[" + i + "] ");
						System.out.println("NotNULL");
					}
					nonnulls++;
				}
			}
			System.out.println("nulls: " + nulls + "  nonNull: " + nonnulls);
		}

		public void close() throws IOException
		{
			//    		System.out.println("Closing BufferedReader");
			readBuffer.close();
			readBuffer = null;
		}
	}

}
