package com.splicemachine.stream;

import java.io.IOException;
import java.util.Iterator;

/**
 * Utility classes around Streams.
 *
 * @author Scott Fines
 * Date: 8/13/14
 */
public class Streams {

    public static <T> Stream<T> of(T...elements){
        return new ArrayStream<T>(elements);
    }

    public static <T> Stream<T> wrap(Iterator<T> iterator){
        return new IteratorStream<T>(iterator);
    }

    public static <T> Stream<T> wrap(Iterable<T> iterable){
        return new IteratorStream<T>(iterable.iterator());
    }

    public static <T> PeekableStream<T> peekingStream(Stream<T> stream){
        return new PeekingStream<T>(stream);
    }

    public static <T> CloseableStream<T> asCloseableStream(Stream<T> stream){
        return new NoOpCloseableStream<T>(stream);
    }

    private static class IteratorStream<T> extends BaseStream<T>{
        private final Iterator<T> iterator;

        private IteratorStream(Iterator<T> iterator) {
            this.iterator = iterator;
        }

        @Override
        public T next() throws StreamException {
            if(!iterator.hasNext()) return null;
            return iterator.next();
        }
    }

    private static class ArrayStream<T> extends BaseStream<T>{
        private final T[] stream;
        private int position = 0;

        private ArrayStream(T[] stream) {
            this.stream = stream;
        }

        @Override
        public T next() throws StreamException {
            if(position>=stream.length) return null;
            T n =  stream[position];
            position++;
            return n;
        }
    }

    private static class PeekingStream<T> extends BaseStream<T> implements PeekableStream<T> {
        private final Stream<T> stream;

        private T n;

        public PeekingStream(Stream<T> stream) {
            this.stream = stream;
        }

        @Override
        public T peek() throws StreamException {
            if(n!=null) return n;
            n = stream.next();
            return n;
        }

        @Override
        public T next() throws StreamException {
            T next = peek();
            n = null; //strip n away
            return next;
        }
    }

    private static class NoOpCloseableStream<T> extends BaseCloseableStream<T> {
        private final Stream<T> stream;

        public NoOpCloseableStream(Stream<T> stream) {
            this.stream = stream;
        }

        @Override
        public void close() throws IOException {
            //no-op
        }

        @Override
        public T next() throws StreamException {
            return stream.next();
        }
    }
}
