SELECT 'customers' as table_name, COUNT(*) as count
FROM customers
UNION ALL
SELECT 'sellers', COUNT(*)
FROM sellers
UNION ALL
SELECT 'products', COUNT(*)
FROM products
UNION ALL
SELECT 'stores', COUNT(*)
FROM stores
UNION ALL
SELECT 'suppliers', COUNT(*)
FROM suppliers
UNION ALL
SELECT 'fact_sales', COUNT(*)
FROM fact_sales;