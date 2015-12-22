package com.splicemachine.pipeline;

import com.google.common.base.Function;
import com.google.common.base.Predicate;
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
