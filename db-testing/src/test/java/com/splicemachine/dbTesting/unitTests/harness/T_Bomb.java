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

package com.splicemachine.dbTesting.unitTests.harness;

import com.splicemachine.db.iapi.services.property.PropertyUtil;
import com.splicemachine.db.iapi.services.context.ContextService;

import com.splicemachine.db.iapi.services.context.Context;

import java.util.Enumeration;
import java.util.Vector;

public class T_Bomb implements Runnable { 
	public static String BOMB_DELAY_PN="derby.testing.BombDelay";
	private static int DEFAULT_BOMB_DELAY=3600000; //1 hour

	private static T_Bomb me;
	
	private Thread t;
	private Vector v;
	private long delay;
	private boolean armed = false;

	private T_Bomb()
	{
		delay =
			PropertyUtil.getSystemInt(BOMB_DELAY_PN,0,
									  Integer.MAX_VALUE,
									  DEFAULT_BOMB_DELAY);
		v = new Vector();
		t = new Thread(this);
		t.setDaemon(true);
		t.start();
	}

	/**
	  Make an armed bomb set to go off in 1 hour.
	  */
	public synchronized static void makeBomb() {
		if (me==null) me = new T_Bomb();
		me.armBomb();
	}

	/**
	  Arm a bomb to go off. If the bomb does not exist
	  make it.
	  */
	public synchronized void armBomb() {
		if (me == null) me = new T_Bomb();
		me.armed = true;
	}

	/**
	  Cause a bomb to explode. If the bomb does not exist
	  make it.
	  */
	public synchronized static void explodeBomb() {
		if (me == null) me = new T_Bomb();
		me.armed = true;
		me.blowUp();
	}

	public synchronized static void registerBombable(T_Bombable b)
	{
		if (me == null) me = new T_Bomb();
		me.v.addElement(b);
	}

	public synchronized static void unRegisterBombable(T_Bombable b)
	{
		if (null == me || null == b )
            return;
        me.v.removeElement(b);
        if( me.v.isEmpty())
        {
            me.armed = false;
            me.t.interrupt();
            me = null;
        }
	}

	public void run() {

		try {
			Thread.sleep(delay);
		}

		catch (InterruptedException e) {
		}

		if (armed)
		{
			me.blowUp();
		}
	}

	private void blowUp()
	{
			performLastGasp();
			ContextService csf = ContextService.getFactory();
			if (csf != null)
			{
				System.out.println("ran out of time");
				csf.notifyAllActiveThreads
					((Context) null);
			}

			try {
				Thread.currentThread().sleep(30*1000); //Give threads 30 sec to shut down.
			}
			catch (InterruptedException ie) {}
			System.out.println("Exit due to time bomb");
			Runtime.getRuntime().exit(1234);
	}

	private void performLastGasp()
	{
		for (Enumeration e = v.elements() ; e.hasMoreElements() ;) {
			try{
             T_Bombable b = (T_Bombable)e.nextElement();
			 b.lastChance();
			}


			catch (Exception exc) {
				System.out.println("Last Gasp exception");
				exc.printStackTrace();
			}
		} //end for

	}
}
