DROP VIEW  IF EXISTS v_bom_explosion;
DROP VIEW  IF EXISTS v_bom_annual;
DROP TABLE IF EXISTS bom_raw;

CREATE TABLE bom_raw (
    year                                INTEGER,
    month                               INTEGER,
    produced_material                   INTEGER,
    produced_material_production_type   INTEGER,
    produced_material_release_type      VARCHAR(10),
    produced_material_quantity          VARCHAR(30),
    component_material                  INTEGER,
    component_material_production_type  INTEGER,
    component_material_release_type     VARCHAR(10),
    component_material_quantity         VARCHAR(30),
    plant_id                            VARCHAR(20)
);


CREATE VIEW v_bom_annual AS
WITH base AS (
    SELECT plant_id,
           year,
           month,
           produced_material,
           produced_material_production_type,
           produced_material_release_type,
           CAST(REPLACE(produced_material_quantity, ',', '') AS NUMERIC)  AS produced_qty,
           component_material,
           component_material_production_type,
           component_material_release_type,
           CAST(REPLACE(component_material_quantity, ',', '') AS NUMERIC) AS component_qty
    FROM bom_raw
),
produced_per_month AS (
    SELECT DISTINCT plant_id, year, month, produced_material, produced_qty
    FROM base
),
produced_annual AS (
    SELECT plant_id, year, produced_material,
           SUM(produced_qty) AS produced_qty
    FROM produced_per_month
    GROUP BY plant_id, year, produced_material
),
edges_annual AS (
    SELECT plant_id,
           year,
           produced_material,
           MAX(produced_material_production_type)  AS produced_production_type,
           MAX(produced_material_release_type)     AS produced_release_type,
           component_material,
           MAX(component_material_production_type) AS component_production_type,
           MAX(component_material_release_type)    AS component_release_type,
           SUM(component_qty)                      AS component_qty
    FROM base
    GROUP BY plant_id, year, produced_material, component_material
)
SELECT e.plant_id,
       e.year,
       e.produced_material,
       e.produced_production_type,
       e.produced_release_type,
       p.produced_qty,
       e.component_material,
       e.component_production_type,
       e.component_release_type,
       e.component_qty
FROM edges_annual e
JOIN produced_annual p
  ON  p.plant_id          = e.plant_id
  AND p.year              = e.year
  AND p.produced_material = e.produced_material;

CREATE VIEW v_bom_explosion AS
WITH RECURSIVE tree AS (
    SELECT a.plant_id,
           a.year,
           a.produced_material   AS fin_material_id,
           a.component_material  AS material_id,
           0                     AS lvl
    FROM v_bom_annual a
    WHERE a.produced_release_type   = 'FIN'
      AND a.component_release_type  = 'PROD'

    UNION ALL

    SELECT t.plant_id,
           t.year,
           t.fin_material_id,
           e.component_material,
           t.lvl + 1
    FROM tree t
    JOIN v_bom_annual e
      ON  e.plant_id          = t.plant_id
      AND e.year              = t.year
      AND e.produced_material = t.material_id
    WHERE e.component_release_type IN ('PROD', 'FIN')
),
fin_nodes AS (
    SELECT DISTINCT plant_id, year, produced_material,
           produced_production_type, produced_release_type, produced_qty
    FROM v_bom_annual
)
SELECT t.plant_id                    AS plant,
       t.fin_material_id             AS fin_material_id,
       f.produced_release_type       AS fin_material_release_type,
       f.produced_production_type    AS fin_material_production_type,
       f.produced_qty                AS fin_production_quantity,
       e.produced_material           AS prod_material_id,
       e.produced_release_type       AS prod_material_release_type,
       e.produced_production_type    AS prod_material_production_type,
       e.produced_qty                AS prod_material_production_quantity,
       e.component_material          AS component_id,
       e.component_release_type      AS component_material_release_type,
       e.component_production_type   AS component_material_production_type,
       e.component_qty               AS component_consumption_quantity,
       t.year                        AS year
FROM tree t
JOIN v_bom_annual e
  ON  e.plant_id          = t.plant_id
  AND e.year              = t.year
  AND e.produced_material = t.material_id
JOIN fin_nodes f
  ON  f.plant_id          = t.plant_id
  AND f.year              = t.year
  AND f.produced_material = t.fin_material_id
ORDER BY t.plant_id, t.fin_material_id, t.year, t.lvl, e.produced_material, e.component_material;
