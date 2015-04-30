package com.splicemachine.derby.stream.iterator;

import com.splicemachine.derby.impl.sql.execute.operations.LocatedRow;
import com.splicemachine.derby.stream.iapi.IteratorWithContext;

import java.util.Iterator;

/**
 *
 *
 */
public abstract class BaseIteratorWithContext implements IteratorWithContext<LocatedRow> {
        private final Iterator<LocatedRow> source;
        private boolean populated;
        private LocatedRow next;
        private boolean prepared = false;
        private boolean closed = false;

        public BaseIteratorWithContext(Iterator<LocatedRow> source) {
            this.source = source;
        }

        @Override
        public Iterator<LocatedRow> iterator() {
            return this;
        }

        @Override
        public boolean hasNext() {
            if (closed)
                return false;
            if (populated)
                return true;
            try {
                if (!prepared) {
                    prepare();
                    prepared = true;
                }
                next = null;
                while(next == null && source.hasNext()) {
                    LocatedRow r = source.next();
                    next = call(r);
                }
            } catch (Exception e) {
                if (prepared) {
                    closed = true;
                    reset();
                }
                throw new RuntimeException(e);
            }
            populated = next != null;
            if (!populated) {
                closed = true;
                reset();
            }
            return populated;
        }

        @Override
        public LocatedRow next() {
            if (hasNext())  {
                populated = false;
                LocatedRow result = next;
                next = null;
                return result;
            }
            return null;
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }
  }
