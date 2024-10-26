-- Cette requette permet de calculer, pour chaque client et pour la période du 01/01/19 au 31/12/19, les ventes meubles et les ventes deco.

SELECT
  client_id,
  SUM(IF(p.product_type = 'MEUBLE', t.prod_price*t.prod_qty, 0)) as ventes_meuble,
  SUM(IF(p.product_type = 'DECO', t.prod_price*t.prod_qty, 0)) as ventes_deco
FROM
    `transaction` t
    LEFT JOIN product_nomenclature p
    ON t.prod_id = p.product_id
-- Si la colonne date est de type STRING et de format 'jj/mm/yy' (comme dans l'exemple de l'enoncé), if faut remplacer cette ligne par: WHERE PARSE_DATE('%d/%m/%y', t.date) BETWEEN '2019-01-01' AND '2019-12-31'
WHERE t.date BETWEEN '2019-01-01' AND '2019-12-31' 
GROUP BY client_id
;