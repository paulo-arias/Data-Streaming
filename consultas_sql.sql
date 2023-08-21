CREATE STREAM finnhub_trades_stream (
    c ARRAY<STRING>,
    p DOUBLE,
    s STRING,
    t BIGINT,
    v DOUBLE
) WITH (
    KAFKA_TOPIC='finnhub-trades',
    VALUE_FORMAT='JSON'
);


CREATE TABLE contar_symbolos AS SELECT s, COUNT(s) FROM finnhub_trades_stream GROUP BY s EMIT CHANGES;
CREATE TABLE promedio_symbolos AS SELECT s,avg(p) FROM finnhub_trades_stream GROUP BY s EMIT CHANGES;
CREATE TABLE pro_max AS SELECT s,max(p) FROM finnhub_trades_stream GROUP BY s EMIT CHANGES;
CREATE TABLE pro_min AS SELECT s,min(p) FROM finnhub_trades_stream GROUP BY s EMIT CHANGES;
CREATE TABLE pro_count AS SELECT s,count(p) FROM finnhub_trades_stream GROUP BY s EMIT CHANGES;


=== CONSULTA DE DATOS 

select * from contar_symbolos;

select * from promedio_symbolos;

select * from pro_max;

select * from pro_min;

select * from pro_count;