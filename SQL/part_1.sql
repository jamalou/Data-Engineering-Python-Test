-- Cette requête permet de calculer le chiffre d'affaires par jour entre le 01/01/19 et le 31/12/19.
-- Le resultat est trié par date
SELECT
  FORMAT_DATE('%d/%m/%Y', `date`),
  SUM(prod_price*prod_qty) as ventes
FROM `transaction`
WHERE `date` BETWEEN '2019-01-01' AND '2019-12-31'
GROUP BY `date`
ORDER BY `date`
;

-- Si la colonne date est de type STRING et de format 'jj/mm/yy' (comme dans l'enoncé), il faut la convertir en DATE pour pouvoir faire des comparaisons et le tri.
-- Le resultat est trié par date croissante (du 01/01/19 au 31/12/19) en gardant le format 'jj/mm/yy'
SELECT
  FORMAT_DATE('%d/%m/%Y', `date`),
  SUM(prod_price*prod_qty) as ventes
FROM `transaction`
WHERE PARSE_DATE('%d/%m/%y', `date`) BETWEEN '2019-01-01' AND '2019-12-31'
GROUP BY `date`
ORDER BY PARSE_DATE('%d/%m/%y', `date`)
;