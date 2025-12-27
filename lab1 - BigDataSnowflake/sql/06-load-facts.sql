INSERT INTO fact_sales (customer_id, seller_id, product_id, store_id, supplier_id, sale_date, quantity, total_price)
SELECT c.customer_id,
       s.seller_id,
       p.product_id,
       st.store_id,
       sp.supplier_id,
       m.sale_date,
       m.sale_quantity,
       m.sale_total_price
FROM main_data m
         JOIN customers c ON m.customer_email = c.email
         JOIN sellers s ON m.seller_email = s.email
         JOIN products p ON m.product_name = p.name AND m.product_category = p.category
         JOIN stores st ON m.store_name = st.name AND m.store_city = st.city
         JOIN suppliers sp ON m.supplier_name = sp.name AND m.supplier_email = sp.email
LIMIT 1000 OFFSET 10;

DROP TABLE main_data;