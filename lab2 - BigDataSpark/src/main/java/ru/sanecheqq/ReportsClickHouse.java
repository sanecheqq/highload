package ru.sanecheqq;

import org.apache.spark.sql.*;
import org.apache.spark.sql.catalyst.expressions.GenericRow;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import static org.apache.spark.sql.functions.*;

public class ReportsClickHouse {
    public static void main(String[] args) {
        var spark = SparkSession.builder()
                .appName("ReportsClickHouse")
                .getOrCreate();

        var pgUrl = "jdbc:postgresql://postgres:5432/postgres";
        var pgProps = new java.util.Properties();
        pgProps.setProperty("user", "postgres");
        pgProps.setProperty("password", "postgres");
        pgProps.setProperty("driver", "org.postgresql.Driver");

        var chUrl = "jdbc:clickhouse://clickhouse:8123/default";

        var fact = spark.read().jdbc(pgUrl, "fact_sales", pgProps);
        var dimP = spark.read().jdbc(pgUrl, "products", pgProps);
        var dimC = spark.read().jdbc(pgUrl, "customers", pgProps);
        var dimSt = spark.read().jdbc(pgUrl, "stores", pgProps);
        var dimSup = spark.read().jdbc(pgUrl, "suppliers", pgProps);

        var top10Products = fact.groupBy("product_id")
                .agg(sum("quantity").alias("total_quantity"),
                        sum("total_price").alias("total_revenue"))
                .join(dimP, "product_id")
                .select("product_id", "name", "category", "total_quantity", "total_revenue")
                .orderBy(col("total_quantity").desc())
                .limit(10);

        writeToClickHouse(top10Products, chUrl, "top10_products");

        var revenueByCategory = fact.join(dimP, "product_id")
                .groupBy("category")
                .agg(sum("total_price").alias("total_revenue"))
                .orderBy(col("total_revenue").desc());

        writeToClickHouse(revenueByCategory, chUrl, "revenue_by_category");

        var productRatings = dimP.select("product_id", "name", "category", "rating", "reviews");
        writeToClickHouse(productRatings, chUrl, "product_ratings");

        var top10Customers = fact.groupBy("customer_id")
                .agg(sum("total_price").alias("total_spent"))
                .join(dimC, "customer_id")
                .select("customer_id", "first_name", "last_name", "country", "total_spent")
                .orderBy(col("total_spent").desc())
                .limit(10);

        writeToClickHouse(top10Customers, chUrl, "top10_customers");

        var customersByCountry = dimC.groupBy("country")
                .agg(countDistinct("customer_id").alias("num_customers"))
                .orderBy(col("num_customers").desc());

        writeToClickHouse(customersByCountry, chUrl, "customers_by_country");

        var avgCheckByCustomer = fact.groupBy("customer_id")
                .agg(sum("total_price").divide(count("quantity")).alias("avg_check"))
                .join(dimC, "customer_id")
                .select("customer_id", "avg_check");

        writeToClickHouse(avgCheckByCustomer, chUrl, "avg_check_by_customer");

        var monthlyTrends = fact.withColumn("year", year(col("sale_date")))
                .withColumn("month", month(col("sale_date")))
                .groupBy("year", "month")
                .agg(sum("total_price").alias("revenue"),
                        sum("quantity").alias("quantity"))
                .withColumn("avg_order_size", col("revenue").divide(col("quantity")))
                .orderBy("year", "month");

        writeToClickHouse(monthlyTrends, chUrl, "monthly_trends");

        var yearlyTrends = fact.withColumn("year", year(col("sale_date")))
                .groupBy("year")
                .agg(sum("total_price").alias("revenue"),
                        sum("quantity").alias("quantity"))
                .orderBy("year");

        writeToClickHouse(yearlyTrends, chUrl, "yearly_trends");

        var top5Stores = fact.groupBy("store_id")
                .agg(sum("total_price").alias("revenue"))
                .join(dimSt, "store_id")
                .select("name", "city", "country", "revenue")
                .orderBy(col("revenue").desc())
                .limit(5);

        writeToClickHouse(top5Stores, chUrl, "top5_stores");

        var salesByCityCountry = fact.join(dimSt, "store_id")
                .groupBy("city", "country")
                .agg(sum("total_price").alias("revenue"),
                        sum("quantity").alias("quantity"))
                .orderBy(col("revenue").desc());

        writeToClickHouse(salesByCityCountry, chUrl, "sales_by_city_country");

        var avgCheckByStore = fact.groupBy("store_id")
                .agg(sum("total_price").divide(count("quantity")).alias("avg_check"))
                .join(dimSt, "store_id")
                .select("name", "avg_check");

        writeToClickHouse(avgCheckByStore, chUrl, "avg_check_by_store");

        var top5Suppliers = fact.groupBy("supplier_id")
                .agg(sum("total_price").alias("revenue"))
                .join(dimSup, "supplier_id")
                .select("name", "city", "country", "revenue")
                .orderBy(col("revenue").desc())
                .limit(5);

        writeToClickHouse(top5Suppliers, chUrl, "top5_suppliers");

        var avgPriceBySupplier = fact.groupBy("supplier_id")
                .agg(avg(col("total_price").divide(col("quantity"))).alias("avg_price"))
                .join(dimSup, "supplier_id")
                .select("name", "avg_price");

        writeToClickHouse(avgPriceBySupplier, chUrl, "avg_price_by_supplier");

        var salesBySupplierCountry = fact.join(dimSup, "supplier_id")
                .groupBy("country")
                .agg(sum("total_price").alias("revenue"),
                        sum("quantity").alias("quantity"))
                .orderBy(col("revenue").desc());

        writeToClickHouse(salesBySupplierCountry, chUrl, "sales_by_supplier_country");

        var highestRated = dimP.orderBy(col("rating").desc())
                .limit(10)
                .select("product_id", "name", "rating");

        writeToClickHouse(highestRated, chUrl, "highest_rated_products");

        var lowestRated = dimP.orderBy(col("rating").asc())
                .limit(10)
                .select("product_id", "name", "rating");

        writeToClickHouse(lowestRated, chUrl, "lowest_rated_products");

        var ratingSales = fact.join(dimP, "product_id")
                .groupBy("product_id", "name")
                .agg(avg("rating").alias("avg_rating"),
                        sum("quantity").alias("total_quantity"));

        var corrValue = ratingSales.stat().corr("avg_rating", "total_quantity");
        var corrDf = spark.createDataFrame(
                java.util.Arrays.asList(new GenericRow(new Object[]{corrValue})),
                new StructType(new StructField[]{
                        new StructField("rating_sales_correlation", DataTypes.DoubleType, false, Metadata.empty())
                })
        );

        writeToClickHouse(corrDf, chUrl, "rating_sales_correlation");

        var topReviewed = dimP.orderBy(col("reviews").desc())
                .limit(10)
                .select("product_id", "name", "reviews");

        writeToClickHouse(topReviewed, chUrl, "top_reviewed_products");

        spark.stop();
    }

    private static void writeToClickHouse(Dataset<Row> df, String url, String table) {
        df.write()
                .format("jdbc")
                .mode(SaveMode.Overwrite)
                .option("url", url)
                .option("dbtable", table)
                .option("driver", "com.clickhouse.jdbc.ClickHouseDriver")
                .option("createTableOptions", "ENGINE = Log")
                .save();
    }
}