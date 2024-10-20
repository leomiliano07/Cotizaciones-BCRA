CREATE TABLE "2024_leonardo_ezequiel_miliano_schema"."pais" (
    id INT PRIMARY KEY,
    nombre VARCHAR(100) NOT NULL,
    codigo_iso VARCHAR(2) NOT NULL,
    region_id INT,
    FOREIGN KEY (region_id) REFERENCES "2024_leonardo_ezequiel_miliano_schema"."region"(id)
);