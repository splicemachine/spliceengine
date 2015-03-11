package com.splicemachine.pipeline.writehandler.foreignkey;

import com.splicemachine.constants.bytes.BytesUtil;
import com.splicemachine.pipeline.api.Code;
import com.splicemachine.pipeline.api.WriteContext;
import com.splicemachine.pipeline.constraint.ConstraintContext;
import com.splicemachine.pipeline.constraint.ConstraintViolation;
import com.splicemachine.pipeline.impl.WriteResult;
import com.splicemachine.db.iapi.sql.dictionary.ForeignKeyConstraintDescriptor;
import org.apache.hadoop.hbase.client.RetriesExhaustedWithDetailsException;

/**
 * We intercept writes on either the parent or child table and check for the existence of referenced or referring
 * rows on the child or parent table(s).  When those writes fail the remote *CheckWriteHandler returns a failure
 * to us in the form of an exception.  This class is used by the intercept write handlers (*InterceptWriteHandler) to
 * take the remote exception and translate it into a user-friendly foreign key violation error message.
 */
class ForeignKeyViolationProcessor {

    private FkConstraintContextProvider fkConstraintContextProvider;

    ForeignKeyViolationProcessor(FkConstraintContextProvider fkConstraintContextProvider) {
        this.fkConstraintContextProvider = fkConstraintContextProvider;
    }

    /**
     * This code looks fragile but it is validated by every single FK IT test method. Breakages in this method would
     * result in all FK ITs failing. Still, it would be nice if would could simplify this.  DB-2952 is for simplifying
     * how error details are passed between FK CheckWriteHandlers and FK InterceptWriteHandlers.
     */
    public void failWrite(Exception originalException, WriteContext ctx) {
        Throwable t = originalException;
        while ((t = t.getCause()) != null) {
            if (t instanceof RetriesExhaustedWithDetailsException) {
                RetriesExhaustedWithDetailsException retriesException = (RetriesExhaustedWithDetailsException) t;
                if (retriesException.getCauses() != null && !retriesException.getCauses().isEmpty()) {
                    Throwable cause = retriesException.getCause(0);
                    if (cause instanceof ConstraintViolation.ForeignKeyConstraintViolation) {
                        doFail(ctx, (ConstraintViolation.ForeignKeyConstraintViolation) cause);
                    }
                }
            }
        }
    }

    private void doFail(WriteContext ctx, ConstraintViolation.ForeignKeyConstraintViolation cause) {
        String hexEncodedFailedRowKey = cause.getConstraintContext().getMessages()[0];
        byte[] failedRowKey = BytesUtil.fromHex(hexEncodedFailedRowKey);
        ConstraintContext constraintContext = fkConstraintContextProvider.get(cause);
        ctx.result(failedRowKey, new WriteResult(Code.FOREIGN_KEY_VIOLATION, constraintContext));
    }


    /**
     * For the FK violation error message we need: table name, constraint name, and fk columns.  There is a
     * factory method in ConstraintContext for creating a ConstraintContext with just this information from a
     * ForeignKeyConstraintDescriptor.  A slight complication is that how we get a ForeignKeyConstraintDescriptor
     * depends on where the failure happened, etc. Thus the abstraction below.
     */
    static interface FkConstraintContextProvider {
        public ConstraintContext get(ConstraintViolation.ForeignKeyConstraintViolation cause);
    }

    static class ChildFkConstraintContextProvider implements FkConstraintContextProvider {
        private ForeignKeyConstraintDescriptor fkConstraintDescriptor;

        public ChildFkConstraintContextProvider(ForeignKeyConstraintDescriptor fkConstraintDescriptor) {
            this.fkConstraintDescriptor = fkConstraintDescriptor;
        }

        @Override
        public ConstraintContext get(ConstraintViolation.ForeignKeyConstraintViolation cause) {
            // I'm on the child and thus have a local reference to the FK constraint descriptor.
            //
            // Error message looks like: INSERT on table 'C' caused a violation of foreign key constraint 'FK_1' for key (5).
            //
            return ConstraintContext.foreignKey(fkConstraintDescriptor);
        }
    }

    static class ParentFkConstraintContextProvider implements FkConstraintContextProvider {

        private String parentTableName;

        ParentFkConstraintContextProvider(String parentTableName) {
            this.parentTableName = parentTableName;
        }

        @Override
        public ConstraintContext get(ConstraintViolation.ForeignKeyConstraintViolation cause) {
            // I'm on the parent table. The correct error message in this case should have the
            // FK constraint name and keys from the child (only it knows which FK constraint actually failed)
            // but the PARENT table name.
            //
            // Error message looks like: DELETE on table 'P' caused a violation of foreign key constraint 'FK_1' for key (5).
            //
            return cause.getConstraintContext()
                    .withoutMessage(0)                      // Remove the rowKey
                    .withMessage(1, parentTableName);       // Add correct table name
        }
    }

}
