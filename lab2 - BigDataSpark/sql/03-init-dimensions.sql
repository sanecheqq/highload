CREATE TABLE IF NOT EXISTS customers
(
    customer_id SERIAL PRIMARY KEY,
    first_name  TEXT,
    last_name   TEXT,
    age         INT,
    email       TEXT UNIQUE,
    country     TEXT,
    postal_code TEXT,
    pet_type    TEXT,
    pet_name    TEXT,
    pet_breed   TEXT
);


CREATE TABLE IF NOT EXISTS sellers
(
    seller_id   SERIAL PRIMARY KEY,
    first_name  TEXT,
    last_name   TEXT,
    email       TEXT UNIQUE,
    country     TEXT,
    postal_code TEXT
);


CREATE TABLE IF NOT EXISTS products
(
    product_id   SERIAL PRIMARY KEY,
    name         TEXT,
    category     TEXT,
    price        NUMERIC(10, 2),
    weight       NUMERIC(10, 2),
    color        TEXT,
    size         TEXT,
    brand        TEXT,
    material     TEXT,
    description  TEXT,
    rating       NUMERIC(3, 1),
    reviews      INT,
    release_date DATE,
    expiry_date  DATE
);


CREATE TABLE IF NOT EXISTS stores
(
    store_id SERIAL PRIMARY KEY,
    name     TEXT,
    location TEXT,
    city     TEXT,
    state    TEXT,
    country  TEXT,
    phone    TEXT,
    email    TEXT
);


CREATE TABLE IF NOT EXISTS suppliers
(
    supplier_id SERIAL PRIMARY KEY,
    name        TEXT,
    contact     TEXT,
    email       TEXT,
    phone       TEXT,
    address     TEXT,
    city        TEXT,
    country     TEXT
);
