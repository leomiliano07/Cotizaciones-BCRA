CREATE TABLE "2024_leonardo_ezequiel_miliano_schema"."pais_moneda" (
    pais_id INT,
    moneda_id INT,
    es_principal BOOLEAN NOT NULL,
    fecha_adopcion DATE,
    FOREIGN KEY (pais_id) REFERENCES "2024_leonardo_ezequiel_miliano_schema"."pais"(id),
    FOREIGN KEY (moneda_id) REFERENCES "2024_leonardo_ezequiel_miliano_schema"."moneda"(id),
    PRIMARY KEY (pais_id, moneda_id)
);