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

package com.splicemachine.utils;

/**
 * Our own representation of a pair. Allows us to keep explicit HBase dependencies out our API.
 *
 * @author Scott Fines
 *         Date: 11/24/14
 */
public class Pair<T, V> {
    protected T first = null;
    protected V second = null;

    /**
     * Default constructor.
     */
    public Pair() { }

    /**
     * Constructor
     * @param a operand
     * @param b operand
     */
    public Pair(T a, V b) {
        this.first = a;
        this.second = b;
    }

    /**
     * Constructs a new pair, inferring the type via the passed arguments
     * @param <T1> type for first
     * @param <T2> type for second
     * @param a first element
     * @param b second element
     * @return a new pair containing the passed arguments
     */
    public static <T1,T2> Pair<T1,T2> newPair(T1 a, T2 b) {
        return new Pair<T1,T2>(a, b);
    }

    /**
     * Replace the first element of the pair.
     * @param a operand
     */
    public void setFirst(T a) {
        this.first = a;
    }

    /**
     * Replace the second element of the pair.
     * @param b operand
     */
    public void setSecond(V b) {
        this.second = b;
    }

    /**
     * Return the first element stored in the pair.
     * @return T
     */
    public T getFirst() {
        return first;
    }

    /**
     * Return the second element stored in the pair.
     * @return V
     */
    public V getSecond() {
        return second;
    }

    private static boolean equals(Object x, Object y) {
        return (x == null && y == null) || (x != null && x.equals(y));
    }

    @Override
    @SuppressWarnings("unchecked")
    public boolean equals(Object other) {
        return other instanceof Pair && equals(first, ((Pair)other).first) &&
                equals(second, ((Pair)other).second);
    }

    @Override
    public int hashCode() {
        if (first == null)
            return (second == null) ? 0 : second.hashCode() + 1;
        else if (second == null)
            return first.hashCode() + 2;
        else
            return first.hashCode() * 17 + second.hashCode();
    }

    @Override
    public String toString() {
        return "{" + getFirst() + "," + getSecond() + "}";
    }
}
