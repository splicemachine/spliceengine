CREATE TABLE CONTENT (ID INTEGER NOT NULL, CREATOR VARCHAR(128)
NOT NULL, CREATION_DATE DATE NOT NULL, URL VARCHAR(256) NOT NULL, TITLE
VARCHAR(128) NOT NULL, DESCRIPTION VARCHAR(512) NOT NULL, HEIGHT INTEGER NOT
NULL, WIDTH INTEGER NOT NULL);

CREATE TABLE STYLE (ID INTEGER NOT NULL,DESCRIPTION VARCHAR(128) NOT
NULL);
CREATE TABLE CONTENT_STYLE  (CONTENT_ID INTEGER NOT NULL, STYLE_ID
INTEGER NOT NULL);

CREATE TABLE KEYGEN (KEYVAL INTEGER NOT NULL, KEYNAME VARCHAR(256) NOT
NULL);

CREATE TABLE RATING  (ID INTEGER NOT NULL,RATING DOUBLE PRECISION NOT
NULL,ENTRIES DOUBLE PRECISION NOT NULL);

INSERT INTO STYLE VALUES (1, 'BIRD');
INSERT INTO STYLE VALUES (2, 'CAR');
INSERT INTO STYLE VALUES (3, 'BUILDING');
INSERT INTO STYLE VALUES (4, 'PERSON');
INSERT INTO CONTENT values(1, 'djd', CURRENT_DATE,
'http://url.1', 'title1', 'desc1', 100, 100);
INSERT INTO CONTENT values(2, 'djd', CURRENT_DATE,
'http://url.2', 'title2', 'desc2', 100, 100);
INSERT INTO CONTENT values(3, 'djd', CURRENT_DATE,
'http://url.3', 'title3', 'desc3', 100, 100);
INSERT INTO CONTENT values(4, 'djd', CURRENT_DATE,
'http://url.4', 'title4', 'desc4', 100, 100);
INSERT INTO CONTENT values(5, 'djd', CURRENT_DATE,
'http://url.5', 'title5', 'desc5', 100, 100);
INSERT INTO CONTENT_STYLE VALUES(1,1);
INSERT INTO CONTENT_STYLE VALUES(1,2);
INSERT INTO CONTENT_STYLE VALUES(2,1);
INSERT INTO CONTENT_STYLE VALUES(2,4);
INSERT INTO CONTENT_STYLE VALUES(3,3);
INSERT INTO CONTENT_STYLE VALUES(3,4);
INSERT INTO CONTENT_STYLE VALUES(3,1);
INSERT INTO CONTENT_STYLE VALUES(4,4);
INSERT INTO CONTENT_STYLE VALUES(5,1);
INSERT INTO RATING VALUES(1, 4.5, 1);
INSERT INTO RATING VALUES(2, 4.0, 1);
INSERT INTO RATING VALUES(3, 3.9, 1);
INSERT INTO RATING VALUES(4, 4.1, 1);
INSERT INTO RATING VALUES(5, 4.0, 1);
