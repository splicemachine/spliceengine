/*
 * Copyright 2012 - 2016 Splice Machine, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */

package com.splicemachine.storage;

/**
 * Considering key-values from data tables, based on the column family and qualifier they can be classified as one of
 * these types.
 */
public enum CellType{

    /* Column "0" */
    COMMIT_TIMESTAMP,

    /* Column "1" (if empty value) */
    TOMBSTONE,

    /* Column "1" (if "0" value) */
    ANTI_TOMBSTONE,

    /* Column "7" */
    USER_DATA,

    /* Column "9" */
    FOREIGN_KEY_COUNTER,

    /* Unrecognized column/column-value. */
    OTHER
}
