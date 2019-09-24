package com.splicemachine.db.shared.common.reference;

/*
    List of all events' name used for audit logging.
 */
public enum AuditEventType {
        CREATE_USER("SYSCS_UTIL.SYSCS_CREATE_USER"),
        DROP_USER("SYSCS_UTIL.SYSCS_DROP_USER"),
        MODIFY_PASSWORD("SYSCS_UTIL.SYSCS_MODIFY_PASSWORD"),
        RESET_PASSWORD("SYSCS_UTIL.SYSCS_RESET_PASSWORD"),
        LOGIN("LOGIN");

        private String procedure;

        AuditEventType(String procedure) {
                this.procedure = procedure;
        }

        public String getProcedure() {
                return procedure;
        }

}
