CREATE TABLE "2024_leonardo_ezequiel_miliano_schema"."banco_central" (
    id INT PRIMARY KEY,
    nombre VARCHAR(100) NOT NULL,
    pais_id INT,
    ano_fundacion INT,
    FOREIGN KEY (pais_id) REFERENCES "2024_leonardo_ezequiel_miliano_schema"."pais"(id)
);