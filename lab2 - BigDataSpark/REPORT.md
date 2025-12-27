## Отчет по лабораторной работе №2 - ETL на Apache Spark
## Семин Александр Витальевич, гр. М8О-209СВ-24
Реализован ETL-пайплайн на Spark для трансформации данных из исходных CSV-файлов в модель "звезда" в PostgreSQL с последующей генерацией аналитических отчетов в ClickHouse.

## 1. Сборка
```bash
mvn clean package
```

## 2. Запуск инфраструктуры
```bash
docker-compose up -d
```

## 3. Запуск ETL-процессов
Заполнение модели "звезда" в PostgreSQL и генерация отчетов в ClickHouse

```bash
docker exec -it spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 --deploy-mode client --class ru.sanecheqq.PopulatePostgres /opt/java-apps/spark-postgres-clickhouse-1.0-SNAPSHOT.jar

docker exec -it spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 --deploy-mode client --class ru.sanecheqq.ReportsClickHouse /opt/java-apps/spark-postgres-clickhouse-1.0-SNAPSHOT.jar
```

## Отчеты

| Требуемая витрина | Отчет в ClickHouse |
|-------------------|-------------------|
| **1. Витрина продаж по продуктам** | |
| Топ-10 самых продаваемых продуктов | `top10_products` |
| Общая выручка по категориям продуктов | `revenue_by_category` |
| Средний рейтинг и количество отзывов | `product_ratings` |
| **2. Витрина продаж по клиентам** | |
| Топ-10 клиентов с наибольшей суммой покупок | `top10_customers` |
| Распределение клиентов по странам | `customers_by_country` |
| Средний чек для каждого клиента | `avg_check_by_customer` |
| **3. Витрина продаж по времени** | |
| Месячные и годовые тренды продаж | `monthly_trends`, `yearly_trends` |
| Сравнение выручки за разные периоды | `monthly_trends`, `yearly_trends` |
| Средний размер заказа по месяцам | `monthly_trends` |
| **4. Витрина продаж по магазинам** | |
| Топ-5 магазинов с наибольшей выручкой | `top5_stores` |
| Распределение продаж по городам и странам | `sales_by_city_country` |
| Средний чек для каждого магазина | `avg_check_by_store` |
| **5. Витрина продаж по поставщикам** | |
| Топ-5 поставщиков с наибольшей выручкой | `top5_suppliers` |
| Средняя цена товаров от каждого поставщика | `avg_price_by_supplier` |
| Распределение продаж по странам поставщиков | `sales_by_supplier_country` |
| **6. Витрина качества продукции** | |
| Продукты с наивысшим и наименьшим рейтингом | `highest_rated_products`, `lowest_rated_products` |
| Корреляция между рейтингом и объемом продаж | `rating_sales_correlation` |
| Продукты с наибольшим количеством отзывов | `top_reviewed_products` |