/*
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
 * Splice Machine, Inc. has modified this file.
 *
 * All Splice Machine modifications are Copyright 2012 - 2016 Splice Machine, Inc.,
 * and are licensed to you under the License; you may not use this file except in
 * compliance with the License.
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 */

package com.splicemachine.db.iapi.sql.compile;

/**
 * Compilation phases for tree handling
 */
public enum CompilationPhase {

    // derby used to represent with integer 0
    AFTER_PARSE,

    // derby used to represent with integer 1
    AFTER_BIND,

    // derby used to represent with integer 2
    AFTER_OPTIMIZE,

    // Derby didn't have an constant for this phase. However 'generate' was always called as the 4th step of
    // GenericStatement.fourPhasePrepare() -- splice currently uses this constant only to generate AST visualization
    // post generate (yes, tree can change significantly in the generate phase).
    AFTER_GENERATE
}
