ALTER TABLE ROUNDTRIP_FOREIGNKEY.C2
    DROP CONSTRAINT C2_FK;

ALTER TABLE ROUNDTRIP_FOREIGNKEY.C1
    DROP CONSTRAINT C1_FK;

-- -----------------------------------------------------------------------
-- TABLE ROUNDTRIP_FOREIGNKEY.P
-- -----------------------------------------------------------------------

DROP TABLE IF EXISTS ROUNDTRIP_FOREIGNKEY.P;

-- -----------------------------------------------------------------------
-- TABLE ROUNDTRIP_FOREIGNKEY.C2
-- -----------------------------------------------------------------------

DROP TABLE IF EXISTS ROUNDTRIP_FOREIGNKEY.C2;

-- -----------------------------------------------------------------------
-- TABLE ROUNDTRIP_FOREIGNKEY.C1
-- -----------------------------------------------------------------------

DROP TABLE IF EXISTS ROUNDTRIP_FOREIGNKEY.C1;

-- -----------------------------------------------------------------------
--  SCHEMA ROUNDTRIP_FOREIGNKEY
-- -----------------------------------------------------------------------

DROP SCHEMA ROUNDTRIP_FOREIGNKEY RESTRICT;

