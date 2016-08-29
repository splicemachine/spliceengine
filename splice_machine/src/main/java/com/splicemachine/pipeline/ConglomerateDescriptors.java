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

package com.splicemachine.pipeline;

import org.spark_project.guava.base.Function;
import org.spark_project.guava.base.Predicate;
import com.splicemachine.db.iapi.sql.dictionary.ConglomerateDescriptor;
import com.splicemachine.db.iapi.sql.dictionary.IndexRowGenerator;

/**
 * Predicates and Functions for working with ConglomerateDescriptors.
 */
public class ConglomerateDescriptors {

    // - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    // Factory methods for predicates
    // - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    public static Predicate<ConglomerateDescriptor> isUniqueIndex() {
        return new UniqueIndexPredicate();
    }

    public static Predicate<ConglomerateDescriptor> isIndex() {
        return new IndexPredicate();
    }

    // - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    // Factory methods for functions
    // - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    public static Function<ConglomerateDescriptor, Long> numberFunction() {
        return new NumberFunction();
    }

    // - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    // Functions
    // - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    /**
     * Function that returns conglomerate number.
     */
    private static class NumberFunction implements Function<ConglomerateDescriptor, Long> {
        @Override
        public Long apply(ConglomerateDescriptor conglomerateDescriptor) {
            return conglomerateDescriptor.getConglomerateNumber();
        }
    }

    // - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    // Predicates
    // - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    private static class UniqueIndexPredicate implements Predicate<ConglomerateDescriptor> {
        @Override
        public boolean apply(ConglomerateDescriptor cd) {
            // intentionally calling getIndexDescriptor() at two levels here.
            IndexRowGenerator indexDescriptor = cd.getIndexDescriptor();
            return indexDescriptor.getIndexDescriptor() != null && indexDescriptor.getIndexDescriptor().isUnique();
        }
    }

    private static class IndexPredicate implements Predicate<ConglomerateDescriptor> {
        @Override
        public boolean apply(ConglomerateDescriptor cd) {
            return cd.isIndex();
        }
    }


}
