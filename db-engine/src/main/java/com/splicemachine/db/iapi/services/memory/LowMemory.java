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

package com.splicemachine.db.iapi.services.memory;

/**
 * Methods to aid classes recover from OutOfMemoryErrors by denying
 * or reducing service rather than a complete shutdown of the JVM.
 * It's intended that classes use to functionality to allow then to
 * deny service when memory is low to allow the JVM to recover,
 * rather than start new operations that are probably doomed to
 * failure due to the low memory.
 * <P>
 * Expected usage is one instance of this class per major logical
 * operation, e.g. creating a connection, preparing a statement,
 * adding an entry to a specific cache etc.
 * <BR>
 * The logical operation would call isLowMemory() before starting
 * the operation, and thrown a static exception if it returns true.
 * <BR>
 * If during the operation an OutOfMemoryException is thrown the
 * operation would call setLowMemory() and throw its static exception
 * representing low memory.
 * <P>
 * Future enhancments could be a callback mechanism for modules
 * where they register they can reduce memory usage on a low
 * memory situation. These callbacks would be triggered by
 * a call to setLowMemory. For example the page cache could
 * reduce its current size by 10% in a low memory situation.
 * 
 */
public class LowMemory {

    /**
     * Free memory seen when caller indicated an out of
     * memory situation. Becomes a low memory watermark
     * for five seconds that causes isLowMemory to return
     * true if free memory is lower than this value.
     * This allows the JVM a chance to recover memory
     * rather than start new operations that are probably
     * doomed to failure due to the low memory.
     * 
     */
    private long lowMemory;
    
    /**
     * Time in ms corresponding to System.currentTimeMillis() when
     * lowMemory was set.
     */
    private long whenLowMemorySet;
    
    /**
     * Set a low memory watermark where the owner of this object just hit an
     * OutOfMemoryError. The caller is assumed it has just freed up any
     * references it obtained during the operation, so that the freeMemory call
     * as best as it can reflects the memory before the action that caused the
     * OutOfMemoryError, not part way through the action.
     * 
     */
    public void setLowMemory() {
        
        // Can read lowMemory unsynchronized, worst
        // case is that we force extra garbage collection.
        if (lowMemory == 0L) {
            
            // The caller tried to dereference any objects it
            // created during its instantation. Try to garbage
            // collect these so that we can a best-guess effort
            // at the free memory before the overall operation we are
            // failing on occurred. Of course in active multi-threading
            // systems we run the risk that some other thread just freed
            // up some memory that throws off our calcuation. This is
            // avoided by clearing lowMemory some time later on an
            // isLowMemory() call.
            boolean interrupted = false;

            for (int i = 0; i < 5; i++) {
                System.gc();
                System.runFinalization();
                try {
                    Thread.sleep(50L);
                } catch (InterruptedException e) {
                    interrupted = true;
                }
            }
            if (interrupted) {
                // reinstate flag
                Thread.currentThread().interrupt();
            }
        }
        synchronized (this) {
            if (lowMemory == 0L) {
                lowMemory = Runtime.getRuntime().freeMemory();
                whenLowMemorySet = System.currentTimeMillis();
            }
        }
    }

    /**
     * Return true if a low memory water mark has been set and the current free
     * memory is lower than it. Otherwise return false.
     */
    public boolean isLowMemory() {
        synchronized (this) {
            long lm = lowMemory;
            if (lm == 0)
                return false;
            
            if (Runtime.getRuntime().freeMemory() > lm)
                return false;
            
            // Only allow an low memory watermark to be valid
            // for five seconds after it was set. This stops
            // an incorrect limit being set for ever. This could
            // occur if other threads were freeing memory when
            // we called Runtime.getRuntime().freeMemory()
           
            long now = System.currentTimeMillis();
            if ((now - this.whenLowMemorySet) > 5000L) {
                lowMemory = 0L;
                whenLowMemorySet = 0L;
                return false;
            }
            return true;
        }
    }
}
