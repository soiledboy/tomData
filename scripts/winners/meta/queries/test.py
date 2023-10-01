
import psycopg2
import pandas.io.sql as psql
import pandas as pd
conn = psycopg2.connect(dbname='d4glguq0as3fe6', user='ucjrvhda1pfbv1', password='pa045091d3e285537e8a31eb863c11df1dfa42659a9a447ce231b18f0070892a1', host='ec2-34-195-123-119.compute-1.amazonaws.com', port='5432', sslmode='require')
cur = conn.cursor()
print('Connection Established 7 days')




query = """with time_period as
(
  SELECT
    product_id,
    MIN(date) as first_date,
    MAX(date) as last_date
  FROM
    prices
 WHERE
 Date
	between (CURRENT_DATE- interval %s day)
	AND     (CURRENT_DATE + 1)
  GROUP BY product_id


), first_date_price as
(
  SELECT
  prices.product_id as product_id,
  date_trunc('day',first_date) as first_date,
  AVG(market) as first_price,
  number,
  name
  FROM time_period INNER JOIN prices ON (date_trunc('day',prices.date) = date_trunc('day',first_date) AND time_period.product_id = prices.product_id)
  INNER JOIN products ON (prices.product_id = products.id)
  WHERE market IS NOT NULL
  GROUP BY prices.product_id,date_trunc('day',first_date),number, name
), last_date_price as
(
  SELECT
  prices.product_id as product_id,
  AVG(market) as last_price,
  date_trunc('day',last_date) as last_date
  FROM time_period INNER JOIN prices ON (date_trunc('day',prices.date) = date_trunc('day',last_date) AND time_period.product_id = prices.product_id)
  WHERE market IS NOT NULL
  GROUP BY prices.product_id,date_trunc('day',last_date)
), top_cards as
(
SELECT
  first_date,
  first_date_price.product_id,
  last_date,
  first_price,
  last_price,
  (last_price-first_price) as difference,
  number,
  name
  FROM last_date_price
  INNER JOIN first_date_price ON (first_date_price.product_id = last_date_price.product_id)
  ORDER BY difference DESC LIMIT 5000
), groupings as
(
SELECT
  product_id,
  group_id,
  groups.updated_at,
  groups.name as set_name
  FROM product_groups LEFT JOIN groups ON (groups.id = product_groups.group_id)
), top_cards2 as
(
SELECT
  first_date,
  last_date,
  first_price,
  last_price,
  difference,
  number,
  name,
  groupings.product_id,
  groupings.group_id,
  groupings.updated_at,
  set_name,
  ((last_price-first_price)/last_price) as p_change
  FROM top_cards LEFT JOIN groupings ON ( groupings.product_id = top_cards.product_id)
), top_cards3 as
(
SELECT
	products.product_id as tcg_product_id,
	products.id,
	top_cards2.first_date,
  	top_cards2.last_date,
  	top_cards2.first_price,
  	top_cards2.last_price,
  	top_cards2.difference,
	top_cards2.number,
	top_cards2.name,
  	top_cards2.product_id,
  	top_cards2.group_id,
  	top_cards2.updated_at,
  	top_cards2.set_name,
	((last_price-first_price)/last_price) as p_change
FROM products LEFT JOIN top_cards2 ON (products.id = top_cards2.product_id)
)
  SELECT
	    name,
	    number,
    	set_name,
      market,
	    date,
    	sub_type,
    	prices.product_id,
    	prices.low,
    	prices.mid,
    	prices.high,
    	tcg_product_id,
    	updated_at,
    	difference,
    	first_date,
    	last_date,
    	first_price,
    	last_price,
    	p_change
FROM top_cards3 INNER JOIN prices ON top_cards3.id = prices.product_id
AND
 date
	between (CURRENT_DATE- interval %s day)
	AND     (CURRENT_DATE +1)
AND updated_at > '01/01/2011'
	ORDER BY updated_at DESC"""

df = cur.execute(query, ('1','1',))
df['period'] = '1'
df07 = df
df.to_csv('test.csv')
cur.close()
conn.close()
print(df07)