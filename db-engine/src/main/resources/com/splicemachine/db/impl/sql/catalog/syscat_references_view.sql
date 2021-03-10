CREATE VIEW REFERENCES AS
SELECT X.CONSTNAME
     , X.TABSCHEMA
     , X.TABNAME
--     , CAST(NULL AS VARCHAR(128)) AS OWNER
--     , CAST(NULL AS CHAR(1)) AS OWNERTYPE
     , X.REFKEYNAME
     , X.REFTABSCHEMA
     , X.REFTABNAME
     , X.COLCOUNT
     , X.DELETERULE
     , X.UPDATERULE
     , CAST(X.FK_COLNAMES AS VARCHAR(640)) AS FK_COLNAMES
     , CAST(STRING_AGG(X.PK_COLNAME, ',') AS VARCHAR(640)) AS PK_COLNAMES
FROM (
    SELECT W.CONSTNAME
         , W.TABSCHEMA
         , W.TABNAME
         , W.REFKEYNAME
         , W.REFTABSCHEMA
         , W.REFTABNAME
         , W.COLCOUNT
         , W.DELETERULE
         , W.UPDATERULE
         , W.FK_COLNAMES
         , COLSP.COLUMNNAME AS PK_COLNAME
         , CONGLOMSP.DESCRIPTOR.getKeyColumnPosition(COLSP.COLUMNNUMBER) AS PKCOLS_ORD
    FROM --splice-properties joinOrder=fixed
    (
        SELECT V.CONSTNAME
             , V.TABSCHEMA
             , V.TABNAME
             , V.REFKEYNAME
             , V.REFTABSCHEMA
             , V.REFTABNAME
             , V.COLCOUNT
             , V.DELETERULE
             , V.UPDATERULE
             , V.PK_TABLEID
             , V.PK_CONSTRAINTID
             , STRING_AGG(V.FK_COLNAME, ',') AS FK_COLNAMES
        FROM (
            SELECT U.CONSTNAME
                 , U.TABSCHEMA
                 , U.TABNAME
                 , U.REFKEYNAME
                 , U.REFTABSCHEMA
                 , U.REFTABNAME
                 , CAST(CONGLOMSC.DESCRIPTOR.numberOfOrderedColumns() AS SMALLINT) AS COLCOUNT
                 , U.DELETERULE
                 , U.UPDATERULE
                 , COLSC.COLUMNNAME AS FK_COLNAME
                 , CONGLOMSC.DESCRIPTOR.getKeyColumnPosition(COLSC.COLUMNNUMBER) AS FKCOLS_ORD
                 , U.PK_TABLEID
                 , U.PK_CONSTRAINTID
            FROM --splice-properties joinOrder=fixed
            (
                  SELECT CC.CONSTRAINTNAME AS CONSTNAME
                       , VC.SCHEMANAME AS TABSCHEMA
                       , TC.TABLENAME AS TABNAME
                       , CP.CONSTRAINTNAME AS REFKEYNAME
                       , VP.SCHEMANAME AS REFTABSCHEMA
                       , TP.TABLENAME AS REFTABNAME
                       , (CASE FK.DELETERULE
                            WHEN 'R' THEN 'A'
                            WHEN 'S' THEN 'R'
                            WHEN 'C' THEN 'C'
                            WHEN 'U' THEN 'N'
                          END) AS DELETERULE
                       , (CASE FK.UPDATERULE
                            WHEN 'R' THEN 'A'
                            WHEN 'S' THEN 'R'
                          END) AS UPDATERULE
                       , CAST(NULL AS TIMESTAMP) AS CREATE_TIME
                       , FK.CONGLOMERATEID as FK_CONGLOMID
                       , CC.TABLEID as FK_TABLEID
                       , CP.TABLEID as PK_TABLEID
                       , FK.KEYCONSTRAINTID as PK_CONSTRAINTID
                  FROM --splice-properties joinOrder=fixed
                       SYS.SYSFOREIGNKEYS FK
                     , SYS.SYSCONSTRAINTS CC
                     , SYS.SYSCONSTRAINTS CP
                     , SYS.SYSTABLES TC
                     , SYS.SYSTABLES TP
                     , SYSVW.SYSSCHEMASVIEW VC
                     , SYSVW.SYSSCHEMASVIEW VP
                  WHERE FK.CONSTRAINTID = CC.CONSTRAINTID
                    AND CC.TABLEID = TC.TABLEID
                    AND CC.SCHEMAID = VC.SCHEMAID
                    AND FK.KEYCONSTRAINTID = CP.CONSTRAINTID
                    AND CP.TABLEID = TP.TABLEID
                    AND CP.SCHEMAID = VP.SCHEMAID
                ) U
              , SYS.SYSCONGLOMERATES CONGLOMSC
              , SYS.SYSCOLUMNS COLSC
            WHERE U.FK_CONGLOMID = CONGLOMSC.CONGLOMERATEID
              AND U.FK_TABLEID = COLSC.REFERENCEID
              AND CASE WHEN CONGLOMSC.DESCRIPTOR IS NULL THEN FALSE ELSE (CONGLOMSC.DESCRIPTOR.getKeyColumnPosition(COLSC.COLUMNNUMBER) > 0) END
            ORDER BY U.CONSTNAME, FKCOLS_ORD
            ) V
        GROUP BY V.CONSTNAME
               , V.TABSCHEMA
               , V.TABNAME
               , V.REFKEYNAME
               , V.REFTABSCHEMA
               , V.REFTABNAME
               , V.COLCOUNT
               , V.DELETERULE
               , V.UPDATERULE
               , V.PK_TABLEID
               , V.PK_CONSTRAINTID
        ) W
      , SYS.SYSPRIMARYKEYS PK
      , SYS.SYSCONGLOMERATES CONGLOMSP
      , SYS.SYSCOLUMNS COLSP
    WHERE W.PK_CONSTRAINTID = PK.CONSTRAINTID
      AND PK.CONGLOMERATEID = CONGLOMSP.CONGLOMERATEID
      AND W.PK_TABLEID = COLSP.REFERENCEID
      AND CASE WHEN CONGLOMSP.DESCRIPTOR IS NULL THEN FALSE ELSE (CONGLOMSP.DESCRIPTOR.getKeyColumnPosition(COLSP.COLUMNNUMBER) > 0) END
    ORDER BY W.CONSTNAME, PKCOLS_ORD
    ) X
GROUP BY X.CONSTNAME
       , X.TABSCHEMA
       , X.TABNAME
       , X.REFKEYNAME
       , X.REFTABSCHEMA
       , X.REFTABNAME
       , X.COLCOUNT
       , X.DELETERULE
       , X.UPDATERULE
       , X.FK_COLNAMES