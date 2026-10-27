SELECT c.name AS category, COUNT(fc.film_id) AS movies_count
FROM category c
JOIN film_category fc ON c.category_id = fc.category_id
GROUP BY c.name
ORDER BY movies_count DESC;

SELECT a.first_name, a.last_name, COUNT(r.rental_id) AS rental_count
FROM actor a
JOIN film_actor fa ON a.actor_id = fa.actor_id
JOIN inventory i ON fa.film_id = i.film_id
JOIN rental r ON i.inventory_id = r.inventory_id
GROUP BY a.actor_id, a.first_name, a.last_name
ORDER BY rental_count DESC
LIMIT 10;

SELECT c.name AS category, SUM(p.amount) AS total_spent
FROM category c
JOIN film_category fc ON c.category_id = fc.category_id
JOIN inventory i ON fc.film_id = i.film_id
JOIN rental r ON i.inventory_id = r.inventory_id
JOIN payment p ON r.rental_id = p.rental_id
GROUP BY c.name
ORDER BY total_spent DESC
LIMIT 1;

SELECT f.title
FROM film f
LEFT JOIN inventory i ON f.film_id = i.film_id
WHERE i.inventory_id IS NULL;

WITH ActorChildrenCount AS (
    SELECT a.actor_id, a.first_name, a.last_name, COUNT(fa.film_id) AS film_count
    FROM actor a
    JOIN film_actor fa ON a.actor_id = fa.actor_id
    JOIN film_category fc ON fa.film_id = fc.film_id
    JOIN category c ON fc.category_id = c.category_id
    WHERE c.name = 'Children'
    GROUP BY a.actor_id, a.first_name, a.last_name
),
RankedActors AS (
    SELECT first_name, last_name, film_count,
           RANK() OVER (ORDER BY film_count DESC) as rnk
    FROM ActorChildrenCount
)
SELECT first_name, last_name, film_count
FROM RankedActors
WHERE rnk <= 3;

SELECT 
    ci.city,
    COUNT(CASE WHEN c.active = 1 THEN 1 END) AS active_customers,
    COUNT(CASE WHEN c.active = 0 THEN 1 END) AS inactive_customers
FROM city ci
JOIN address a ON ci.city_id = a.city_id
JOIN customer c ON a.address_id = c.address_id
GROUP BY ci.city
ORDER BY inactive_customers DESC;

WITH CityRentalHours AS (
    SELECT 
        ci.city,
        c.name AS category,
        SUM(EXTRACT(EPOCH FROM (r.return_date - r.rental_date)) / 3600) AS total_hours
    FROM city ci
    JOIN address a ON ci.city_id = a.city_id
    JOIN customer cu ON a.address_id = cu.address_id
    JOIN rental r ON cu.customer_id = r.customer_id
    JOIN inventory i ON r.inventory_id = i.inventory_id
    JOIN film_category fc ON i.film_id = fc.film_id
    JOIN category c ON fc.category_id = c.category_id
    WHERE r.return_date IS NOT NULL 
    GROUP BY ci.city, c.name
),
RankedCategories AS (
    SELECT 
        city, 
        category, 
        total_hours,
        ROW_NUMBER() OVER(PARTITION BY city ORDER BY total_hours DESC) as rn
    FROM CityRentalHours
)
SELECT city, category, total_hours
FROM RankedCategories
WHERE rn = 1 
  AND (
    city LIKE 'a%' 
    OR 
    city LIKE '%-%'
  )
ORDER BY city;
