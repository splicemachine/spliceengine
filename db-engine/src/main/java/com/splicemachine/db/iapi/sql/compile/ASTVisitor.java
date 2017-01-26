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

package	com.splicemachine.db.iapi.sql.compile;

import com.splicemachine.db.iapi.error.StandardException;

/**
 * <p>
 * A Visitor which handles nodes in Derby's abstract syntax trees. In addition
 * to this contract, it is expected that an ASTVisitor will have a 0-arg
 * constructor. You use an ASTVisitor like this:
 * </p>
 *
 * <blockquote><pre>
 * // initialize your visitor
 * MyASTVisitor myVisitor = new MyASTVisitor();
 * myVisitor.initializeVisitor();
 * languageConnectionContext.setASTVisitor( myVisitor );
 *
 * // then run your queries.
 * ...
 *
 * // when you're done inspecting query trees, release resources and
 * // remove your visitor
 * languageConnectionContext.setASTVisitor( null );
 * myVisitor.teardownVisitor();
 * </pre></blockquote>
 *
 */
public interface ASTVisitor extends Visitor {

    /**
     * Initialize the Visitor before processing any trees. User-written code
     * calls this method before poking the Visitor into the
     * LanguageConnectionContext. For example, an
     * implementation of this method might open a trace file.
     */
    public void initializeVisitor() throws StandardException;

    /**
     * Final call to the Visitor. User-written code calls this method when it is
     * done inspecting query trees. For instance, an implementation of this method
     * might release resources, closing files it has opened.
     */
    public void teardownVisitor() throws StandardException;

    /**
     * The compiler calls this method just before walking a query tree.
     *
     * @param statementText Text used to create the tree.
     * @param phase of compilation (AFTER_PARSE, AFTER_BIND, or AFTER_OPTIMIZE).
     */
    public void begin( String statementText, CompilationPhase phase) throws StandardException;
    
    /**
     * The compiler calls this method when it's done walking a tree.
     *
     * @param phase of compilation (AFTER_PARSE, AFTER_BIND, or AFTER_OPTIMIZE).
     */
    public void end(CompilationPhase phase) throws StandardException;
    
}	
