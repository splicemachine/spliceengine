package com.splicemachine.stats;

/**
 * Represents a data structure which can be <em>merged</em>.
 *
 * A set data structure (or set function) {@code F} is said to be <em>linear</em> if, for any
 * two multisets {@code X} and {@code Y}, {@code F(X union Y) = F(X) + F(Y)} for some
 * well-defined operator '+'. In this scenario, the '+' operator is defined as the <em>merge</em>
 * operator (although we will loosely refer to it as the <em>addition</em> operator as well, in
 * order to maximize confusion).
 *
 * This interface represents data structures which have a well-defined merge operation(and
 * therefore represent a linear operator). Note, however, that many mergeable interfaces
 * are only mergeable with other versions of themselves, as their internal data structure may
 * dictact how effectively it may be merged.
 *
 * @author Scott Fines
 *         Date: 10/27/14
 */
public interface Mergeable<T,M extends Mergeable<T,M>> {

    /**
     * Merge this entity with another instance of the same type.
     *
     * @param other the element to be merged
     */
    M merge(M other);
}
