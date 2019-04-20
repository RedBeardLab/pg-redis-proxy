CREATE TABLE foo(a INT, b INT, c INT);
INSERT INTO foo VALUES(1,2,3);
PREPARE insertfoo (int, int, int) AS
    INSERT INTO foo VALUES($1, $2, $3);
EXECUTE insertfoo(5,6,7);
INSERT INTO foo VALUES(4,5,6);
EXECUTE insertfoo(8,9,0); 

SELECT * FROM foo;
