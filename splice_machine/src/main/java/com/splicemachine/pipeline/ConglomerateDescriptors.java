/*
 * Copyright (c) 2012 - 2017 Splice Machine, Inc.
 *
 * This file is part of Splice Machine.
 * Splice Machine is free software: you can redistribute it and/or modify it under the terms of the
 * GNU Affero General Public License as published by the Free Software Foundation, either
 * version 3, or (at your option) any later version.
 * Splice Machine is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU Affero General Public License for more details.
 * You should have received a copy of the GNU Affero General Public License along with Splice Machine.
 * If not, see <http://www.gnu.org/licenses/>.
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
