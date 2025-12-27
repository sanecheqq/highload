CREATE TABLE IF NOT EXISTS fact_sales
(
    sale_id     SERIAL PRIMARY KEY,
    customer_id INT REFERENCES customers (customer_id),
    seller_id   INT REFERENCES sellers (seller_id),
    product_id  INT REFERENCES products (product_id),
    store_id    INT REFERENCES stores (store_id),
    supplier_id INT REFERENCES suppliers (supplier_id),
    sale_date   DATE,
    quantity    INT,
    total_price NUMERIC(10, 2)
);