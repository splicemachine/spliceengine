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

package com.splicemachine.db.impl.drda;

import java.io.*;
import java.net.*;
import java.security.*;

final class ClientThread extends Thread {

	NetworkServerControlImpl parent;
	ServerSocket serverSocket;
	private int timeSlice;
	private int connNum;
    
    ClientThread (NetworkServerControlImpl nsi, ServerSocket ss) {
        
        // Create a more meaningful name for this thread (but preserve its
        // thread id from the default name).
        NetworkServerControlImpl.setUniqueThreadName(this, "NetworkServerThread");
        
        parent=nsi;
        serverSocket=ss;
        timeSlice=nsi.getTimeSlice();
    }
	
    public void run() 
    {
        Socket clientSocket = null;
        
        for (;;) { // Nearly infinite loop. The loop is terminated if
                   // 1) We are shut down or 2) SSL won't work. In all
                   // other cases we just continue and try another
                   // accept on the socket.

            try { // Check for all other exceptions....

                try { // Check for underlying InterruptedException,
                      // SSLException and IOException

                    try { // Check for PrivilegedActionException
                        clientSocket =
                                    acceptClientWithRetry();
                        // Server may have been shut down.  If so, close this
                        // client socket and break out of the loop.
                        // DERBY-3869
                        if (parent.getShutdown()) {
                            if (clientSocket != null)
                                clientSocket.close();
                            return;
                        }
                            
                        clientSocket.setKeepAlive(parent.getKeepAlive());
                        
                        // Set time out: Stops DDMReader.fill() from
                        // waiting indefinitely when timeSlice is set.
                        if (timeSlice > 0)
                            clientSocket.setSoTimeout(timeSlice);
                        
                        //create a new Session for this socket
                        parent.addSession(clientSocket);
                        
                    } catch (PrivilegedActionException e) {
                        // Just throw the underlying exception
                        throw e.getException();
                    } // end inner try/catch block
                    
                } catch (InterruptedException ie) {
                    if (parent.getShutdown()) {
                        // This is a shutdown and we'll just exit the
                        // thread. NOTE: This is according to the logic
                        // before this rewrite. I am not convinced that it
                        // is allways the case, but will not alter the
                        // behaviour since it is not within the scope of
                        // this change (DERBY-2108).
                    	clientSocket.close();
            	        return;
                    }
                    parent.consoleExceptionPrintTrace(ie);
                    if (clientSocket != null)
                        clientSocket.close();

                } catch (javax.net.ssl.SSLException ssle) {
                    // SSLException is a subclass of
                    // IOException. Print stack trace and...
                    
                    parent.consoleExceptionPrintTrace(ssle);
                    
                    // ... we need to do a controlled shutdown of the
                    // server, since SSL for some reason will not
                    // work.
                    // DERBY-3537: circumvent any shutdown security checks
                    parent.directShutdownInternal();
                    
                    return; // Exit the thread
                    
                } catch (IOException ioe) {
                    if (clientSocket != null)
                        clientSocket.close();
                    // IOException causes this thread to stop.  No
                    // console error message if this was caused by a
                    // shutdown
                    synchronized (parent.getShutdownSync()) {
                        if (parent.getShutdown()) {
                            return; // Exit the thread
                        } 
                    }
                    parent.consoleExceptionPrintTrace(ioe);
                }
            } catch (Exception e) {
                // Catch and log all other exceptions
                
                parent.consoleExceptionPrintTrace(e);
                try {
                    if (clientSocket != null)
                        clientSocket.close();
                } catch (IOException closeioe)
                {
                    parent.consoleExceptionPrintTrace(closeioe);
                }
            } // end outer try/catch block
            
        } // end for(;;)
        
    }// end run()

    /**
     * Perform a server socket accept. Allow three attempts with a one second
     * wait between each
     * 
     * @return client socket or null if accept failed.
     * 
     */
    private Socket acceptClientWithRetry() {
        return (Socket) AccessController.doPrivileged(
                new PrivilegedAction() {
                    public Object run() {
                        for (int trycount = 1; trycount <= 3; trycount++) {
                            try {
                                // DERBY-5347 Need to exit if
                                // accept fails with IOException
                                // Cannot just aimlessly loop
                                // writing errors
                                return serverSocket.accept();
                            } catch (IOException acceptE) {
                                // If not a normal shutdown,
                                // log and shutdown the server
                                if (!parent.getShutdown()) {
                                    parent
                                            .consoleExceptionPrintTrace(acceptE);
                                    if (trycount == 3) {
                                        // give up after three tries
                                        parent.directShutdownInternal();
                                    } else {
                                        // otherwise wait 1 second and retry
                                        try {
                                            Thread.sleep(1000);
                                        } catch (InterruptedException ie) {
                                            parent
                                            .consoleExceptionPrintTrace(ie);
                                        }
                                    }
                                }
                            }
                        }
                        return null; // no socket to return after three tries
                    }
                }

                );
    }
}







