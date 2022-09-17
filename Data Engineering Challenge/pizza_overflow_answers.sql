--- Creating a new pizza recipe that includes the most used 5 ingredients in all the ordered pizzas in the past 6 months 
WITH ingredients AS (
SELECT 
	a.order_id,
	a.pizza_id, 
	c.value AS ingredients
FROM [dbo].[Orders] AS orders
INNER JOIN [dbo].[Pizza] pizza ON a.pizza_id=b.id
CROSS APPLY STRING_SPLIT(TRIM(b.ingredients), ',') as c
WHERE a.order_time >= '2021-03-11 19:45:29.000' --- last 6 months in the dataset
),
extras AS (
SELECT 
	a.order_id,
	a.pizza_id,
	CAST(
		CASE b.value 
			WHEN '' THEN NULL 
			WHEN 'null' THEN NULL
		ELSE b.value END 
	AS INT) AS ingredients
FROM [dbo].[Orders] as a
CROSS APPLY STRING_SPLIT(TRIM(a.extras),',') as b
WHERE a.order_time >= '2021-03-11 19:45:29.000' --- last 6 months in the dataset
),
exclusions AS (
SELECT 
	a.order_id,
	a.pizza_id,
	CAST(
		CASE b.value 
			WHEN '' THEN NULL 
			WHEN 'null' THEN NULL
		ELSE b.value END 
	AS INT) * -1 AS ingredients --- in order to deduct the excluded ingredients this value needs to be negative
FROM [dbo].[Orders] as a
CROSS APPLY STRING_SPLIT(TRIM(a.exclusions),',') as b
WHERE a.order_time >= '2021-03-11 19:45:29.000' --- last 6 months in the dataset
),
union_all AS (
SELECT * FROM ingredients
UNION ALL
SELECT * FROM extras
UNION ALL
SELECT * FROM exclusions
) 

SELECT 
	string_agg(ingredients_name, ',') 
		within group (ORDER BY ingredients_name) as new_pizza_recipe
FROM 
(
	SELECT 
		i.name as ingredientes_name,
		sum(CASE WHEN a.ingredients > 0 THEN 1 ELSE -1 END) as total
	FROM union_all as a
	LEFT JOIN [dbo].[Ingredients] i ON i.id = a.ingredients
	GROUP BY i.name
	ORDER BY total DESC
  LIMIT 5
) top_ingredients


-- Helping the cook with an alphabetically list ordered comma separated ingredient list for each ordered pizza and add a 2x in front of any ingredient that is requested as extra and is present in the standard recipe too.
WITH ingredients AS (
SELECT 
	ROW_NUMBER() over(ORDER BY order_id, customer_id, pizza_id, exclusions, extras, order_time) AS ordered_pizza,
	a.order_id,
	a.pizza_id, 
	c.value AS ingredients
FROM [dbo].[Orders] AS a
INNER JOIN [dbo].[Pizza] b ON a.pizza_id=b.id
CROSS APPLY STRING_SPLIT(TRIM(b.ingredients), ',') c
),
extras AS (
SELECT 
	ROW_NUMBER() over(ORDER BY order_id, customer_id, pizza_id, exclusions, extras, order_time) AS ordered_pizza,
	a.order_id,
	a.pizza_id,
	CAST(
		CASE b.value 
			WHEN '' THEN NULL 
			WHEN 'null' THEN NULL
		ELSE b.value END 
	AS INT) AS ingredients
FROM [dbo].[Orders] AS a
CROSS APPLY STRING_SPLIT(TRIM(a.extras),',') b
WHERE 
	b.value NOT IN ('','null') AND b.value IS NOT NULL
),
exclusions AS (
SELECT 
	ROW_NUMBER() over(ORDER BY order_id, customer_id, pizza_id, exclusions, extras, order_time) AS ordered_pizza,
	a.order_id,
	a.pizza_id,
	CAST(
		CASE b.value 
			WHEN '' THEN NULL 
			WHEN 'null' THEN NULL
		ELSE b.value END 
	AS INT) * -1 AS ingredients --- in order to deduct the excluded ingredients this value needs to be negative
FROM [dbo].[Orders] AS a
CROSS APPLY STRING_SPLIT(TRIM(a.exclusions),',') b
WHERE 
	b.value NOT IN ('','null') AND b.value IS NOT NULL
),
union_all AS (
SELECT * FROM ingredients
UNION ALL
SELECT * FROM extras
UNION ALL
SELECT * FROM exclusions
) ,

pizzas_ingredients_list AS (
	SELECT 
		a.ordered_pizza,
		a.order_id,
		a.pizza_id,
		i.name as ingredient_name,
		ABS(a.ingredients) AS ingredients, 
		sum(CASE WHEN a.ingredients > 0 THEN 1 ELSE -1 END) total
	FROM union_all a
	LEFT JOIN [dbo].[Ingredients] i ON i.id = a.ingredients
	GROUP BY 
		ABS(a.ingredients),
		a.ordered_pizza,
		a.order_id,
		a.pizza_id)

SELECT  
	a.order_id, 
	a.pizza_id, 
	STRING_AGG(concat(CASE WHEN a.total >= 2 THEN concat(a.total, 'x') ELSE NULL END , ingredient_name), ',') within group (ORDER BY ingredient_name) ingredients

FROM pizzas_ingredients_list a
WHERE a.total > 0
GROUP BY 
	 a.order_id,
	 a.pizza_id,
	 ordered_pizza
ORDER BY order_id
