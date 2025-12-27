package ru.sanecheqq;

import org.apache.spark.sql.*;
import org.apache.spark.sql.expressions.Window;
import static org.apache.spark.sql.functions.*;

public class PopulatePostgres {
    public static void main(String[] args) {
        var spark = SparkSession
                .builder()
                .appName("PopulatePostgres")
                .getOrCreate();

        var pgUrl = "jdbc:postgresql://postgres:5432/postgres";
        var pgProps = new java.util.Properties();
        pgProps.setProperty("user", "postgres");
        pgProps.setProperty("password", "postgres");
        pgProps.setProperty("driver", "org.postgresql.Driver");

        var stg = spark.read()
                .format("jdbc")
                .option("url", pgUrl)
                .option("dbtable", "main_data")
                .option("user", "postgres")
                .option("password", "postgres")
                .option("driver", "org.postgresql.Driver")
                .load();

        loadDim(stg, "customer_email", "sale_date",
                new String[]{"customer_first_name", "customer_last_name", "customer_age",
                        "customer_country", "customer_postal_code", "customer_pet_type",
                        "customer_pet_name", "customer_pet_breed"},
                new String[][]{{"customer_email", "email"}, {"customer_first_name", "first_name"},
                        {"customer_last_name", "last_name"}, {"customer_age", "age"},
                        {"customer_country", "country"}, {"customer_postal_code", "postal_code"},
                        {"customer_pet_type", "pet_type"}, {"customer_pet_name", "pet_name"},
                        {"customer_pet_breed", "pet_breed"}},
                "customers", pgUrl, pgProps);

        loadDim(stg, "seller_email", "sale_date",
                new String[]{"seller_first_name", "seller_last_name", "seller_country", "seller_postal_code"},
                new String[][]{{"seller_email", "email"}, {"seller_first_name", "first_name"},
                        {"seller_last_name", "last_name"}, {"seller_country", "country"},
                        {"seller_postal_code", "postal_code"}},
                "sellers", pgUrl, pgProps);

        loadDim(stg, "sale_product_id", "sale_date",
                new String[]{"product_name", "product_category", "product_price", "product_weight",
                        "product_color", "product_size", "product_brand", "product_material",
                        "product_description", "product_rating", "product_reviews",
                        "product_release_date", "product_expiry_date"},
                new String[][]{{"sale_product_id", "product_id"}, {"product_name", "name"},
                        {"product_category", "category"}, {"product_price", "price"},
                        {"product_weight", "weight"}, {"product_color", "color"},
                        {"product_size", "size"}, {"product_brand", "brand"},
                        {"product_material", "material"}, {"product_description", "description"},
                        {"product_rating", "rating"}, {"product_reviews", "reviews"},
                        {"product_release_date", "release_date"}, {"product_expiry_date", "expiry_date"}},
                "products", pgUrl, pgProps);

        loadDim(stg, "store_name", "sale_date",
                new String[]{"store_location", "store_city", "store_state", "store_country", "store_phone", "store_email"},
                new String[][]{{"store_name", "name"}, {"store_location", "location"},
                        {"store_city", "city"}, {"store_state", "state"},
                        {"store_country", "country"}, {"store_phone", "phone"},
                        {"store_email", "email"}},
                "stores", pgUrl, pgProps);

        loadDim(stg, "supplier_name", "sale_date",
                new String[]{"supplier_contact", "supplier_email", "supplier_phone",
                        "supplier_address", "supplier_city", "supplier_country"},
                new String[][]{{"supplier_name", "name"}, {"supplier_contact", "contact"},
                        {"supplier_email", "email"}, {"supplier_phone", "phone"},
                        {"supplier_address", "address"}, {"supplier_city", "city"},
                        {"supplier_country", "country"}},
                "suppliers", pgUrl, pgProps);

        var customers = spark.read().jdbc(pgUrl, "customers", pgProps);
        var sellers = spark.read().jdbc(pgUrl, "sellers", pgProps);
        var products = spark.read().jdbc(pgUrl, "products", pgProps);
        var stores = spark.read().jdbc(pgUrl, "stores", pgProps);
        var suppliers = spark.read().jdbc(pgUrl, "suppliers", pgProps);

        var factSales = stg
                .join(customers, stg.col("customer_email").equalTo(customers.col("email")))
                .join(sellers, stg.col("seller_email").equalTo(sellers.col("email")))
                .join(products, stg.col("sale_product_id").equalTo(products.col("product_id")))
                .join(stores, stg.col("store_name").equalTo(stores.col("name")))
                .join(suppliers, stg.col("supplier_name").equalTo(suppliers.col("name")))
                .select(
                        customers.col("customer_id").alias("customer_id"),
                        sellers.col("seller_id").alias("seller_id"),
                        products.col("product_id").alias("product_id"),
                        stores.col("store_id").alias("store_id"),
                        suppliers.col("supplier_id").alias("supplier_id"),
                        stg.col("sale_date"),
                        stg.col("sale_quantity").alias("quantity"),
                        stg.col("sale_total_price").alias("total_price")
                );

        factSales.write()
                .mode(SaveMode.Append)
                .jdbc(pgUrl, "fact_sales", pgProps);

        System.out.println("Data loading completed successfully!");
        spark.stop();
    }

    private static void loadDim(Dataset<Row> df, String partitionCol, String orderCol,
                                String[] selects, String[][] renames, String targetTable,
                                String pgUrl, java.util.Properties pgProps) {
        var window = Window.partitionBy(partitionCol).orderBy(col(orderCol));

        var selectCols = new Column[selects.length + 2];
        selectCols[0] = col(partitionCol);
        selectCols[1] = col(orderCol);
        for (int i = 0; i < selects.length; i++) {
            selectCols[i + 2] = col(selects[i]);
        }

        var dfDim = df.select(selectCols)
                .withColumn("rn", row_number().over(window))
                .filter(col("rn").equalTo(1))
                .drop("rn", orderCol);

        for (var rename : renames) {
            if (rename[0].equals(partitionCol)) {
                dfDim = dfDim.withColumnRenamed(partitionCol, rename[1]);
                break;
            }
        }

        for (var rename : renames) {
            if (!rename[0].equals(partitionCol)) {
                dfDim = dfDim.withColumnRenamed(rename[0], rename[1]);
            }
        }

        dfDim.write()
                .mode(SaveMode.Append)
                .jdbc(pgUrl, targetTable, pgProps);
    }
}