package com.splicemachine.db.impl.sql.execute;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.ArrayList;
import java.util.List;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.reference.Limits;
import com.splicemachine.db.iapi.reference.SQLState;
import com.splicemachine.db.iapi.services.sanity.SanityManager;

/**
 * A trigger execution stack holds a stack of {@link TriggerExecutionContext}s.<br/>
 * This class is pulled out of LCC for serialization.
 */
public class TriggerExecutionStack implements Externalizable {
    private List<TriggerExecutionContext> triggerExecutionContexts = new ArrayList<>();;

    public List<TriggerExecutionContext> asList() {
        return this.triggerExecutionContexts;
    }

    /**
     * Push a new trigger execution context.  Multiple TriggerExecutionContexts may be active at any given time.
     *
     * @param tec the trigger execution context
     *
     * @exception StandardException on trigger recursion error
     */
    public void pushTriggerExecutionContext(TriggerExecutionContext tec) throws StandardException {
            /* Maximum 16 nesting levels allowed */
        if (triggerExecutionContexts.size() >= Limits.DB2_MAX_TRIGGER_RECURSION) {
            throw StandardException.newException(SQLState.LANG_TRIGGER_RECURSION_EXCEEDED);
        }
        triggerExecutionContexts.add(tec);
        }

    /**
     * Remove the tec.  Does an object identity (tec == tec) comparison.  Asserts that the tec is found.
     *
     * @param tec the tec to remove
     */
    public void popTriggerExecutionContext(TriggerExecutionContext tec) throws StandardException {
        if (triggerExecutionContexts.isEmpty()) {
            return;
        }
        boolean foundElement = triggerExecutionContexts.remove(tec);
        if (SanityManager.DEBUG) {
            if (!foundElement) {
                SanityManager.THROWASSERT("trigger execution context "+tec+" not found");
            }
        }
    }

    /**
     * Pop all TriggerExecutionContexts off the stack. This usually means an error occurred.
     */
    public void popAllTriggerExecutionContexts() {
        if (triggerExecutionContexts.isEmpty()) {
            return;
        }
        triggerExecutionContexts.clear();
    }

    /**
     * Get the topmost tec.
     */
    public TriggerExecutionContext getTriggerExecutionContext() {
        return triggerExecutionContexts.isEmpty() ? null :
            triggerExecutionContexts.get(triggerExecutionContexts.size() - 1);
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeObject(triggerExecutionContexts);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        triggerExecutionContexts = (List<TriggerExecutionContext>) in.readObject();
    }

    public boolean isEmpty() {
        return this.triggerExecutionContexts.isEmpty();
    }
}
