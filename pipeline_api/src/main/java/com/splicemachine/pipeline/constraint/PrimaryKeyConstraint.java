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

package com.splicemachine.pipeline.constraint;

import com.splicemachine.si.api.data.OperationStatusFactory;

/**
 * Indicates a Primary Key Constraint.
 *
 * @author Scott Fines
 *         Created on: 2/28/13
 */
public class PrimaryKeyConstraint extends UniqueConstraint {


    public PrimaryKeyConstraint(ConstraintContext cc,OperationStatusFactory osf) {
        super(cc,osf);
    }

    @Override
    public BatchConstraintChecker asChecker() {
        return new UniqueConstraintChecker(true, getConstraintContext(),this.opStatusFactory);
    }

    @Override
    public Type getType() {
        return Type.PRIMARY_KEY;
    }

    @Override
    public String toString() {
        return "PrimaryKey";
    }
}
