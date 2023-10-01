querySet = """
WITH groupings AS
(
SELECT
  product_id,
  group_id,
  groups.name as setName,
  abbreviation,
  groups.updated_at AS releaseDate
  FROM product_groups LEFT JOIN groups ON (groups.id = product_groups.group_id)
), averaged_price as
(
  SELECT
  date_trunc('day',date) as date,
  AVG(market) as market,
  abbreviation,
  prices.product_id,
  groupings.setName,
  releaseDate
  FROM prices INNER JOIN groupings ON groupings.product_id = prices.product_id
  WHERE market IS NOT NULL
  GROUP BY prices.product_id,date_trunc('day',date),abbreviation,setName,releaseDate
)
 SELECT
 SUM(market),
 abbreviation,
 date,
 averaged_price.setName,
  averaged_price.releaseDate
 FROM averaged_price
 -- COMMENT: Add indexes you want included in the graph below by using the abbreviation and adding, editing, or deleting the "OR" statements
 WHERE abbreviation = 'BODE' OR abbreviation = 'LED8' OR abbreviation = 'OP17' OR abbreviation = 'SDCS' OR abbreviation = 'MP21' OR abbreviation = 'DAMA' OR abbreviation = 'KICO' OR abbreviation = 'EGS1'  OR abbreviation = 'EG01' OR abbreviation = 'OP16' OR abbreviation ='LIOV' OR abbreviation = 'ANGU'  OR abbreviation = 'GFTP' OR abbreviation = 'SDFC'  OR abbreviation = 'BLVO' OR abbreviation = 'MGED'  OR abbreviation = 'GEIM' OR abbreviation = 'BROL' OR abbreviation = 'GRCR' OR abbreviation = 'BACH'
 AND cast("date" as date) <> '2021-09-23'
 AND cast("date" as date) <> '2021-09-29'
 AND cast("date" as date) <> '2021-10-13'
 AND cast("date" as date) <> '2021-10-23'
 AND cast("date" as date) <> '2021-10-27'
 GROUP BY date, abbreviation,setName,releaseDate;
"""

querySetCards = """
WITH groupings AS
(
  SELECT
  product_id,
  group_id,
  groups.name AS setName,
  abbreviation
  FROM product_groups LEFT JOIN groups ON (groups.id = product_groups.group_id)
), price as
(
  SELECT
  date_trunc('day',date) as date,
  prices.market,
  abbreviation,
  prices.product_id,
  setName
  FROM prices INNER JOIN groupings ON groupings.product_id = prices.product_id
  WHERE market IS NOT NULL

), info as
(
  SELECT
   date,
   market,
   abbreviation,
   products.name,
   products.number,
   products.product_id,
   setName
   FROM products INNER JOIN price on price.product_id = products.id
)
 SELECT
 date,
 market,
 abbreviation,
 product_id,
 name,
 number,
 setName
 FROM info
 -- COMMENT: Add indexes you want included in the graph below by using the abbreviation and adding, editing, or deleting the "OR" statements
 WHERE abbreviation = 'BODE' OR abbreviation = 'LED8' OR abbreviation = 'OP17' OR abbreviation = 'SDCS' OR abbreviation = 'MP21' OR abbreviation = 'DAMA' OR abbreviation = 'KICO' OR abbreviation = 'EGS1'  OR abbreviation = 'EG01' OR abbreviation = 'OP16' OR abbreviation ='LIOV' OR abbreviation = 'ANGU'  OR abbreviation = 'GFTP' OR abbreviation = 'SDFC'  OR abbreviation = 'BLVO' OR abbreviation = 'MGED'  OR abbreviation = 'GEIM' OR abbreviation = 'BROL' OR abbreviation = 'GRCR' OR abbreviation = 'BACH'
 AND cast("date" as date) <> '2021-09-23'
 AND cast("date" as date) <> '2021-09-29'
 AND cast("date" as date) <> '2021-10-13'
 AND cast("date" as date) <> '2021-10-23'
 AND cast("date" as date) <> '2021-10-27'
"""