# Databricks notebook source
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("apple data analysis").getOrCreate()

from pyspark.sql.window import Window
from pyspark.sql.functions import lead, col, broadcast, collect_set, size, array_contains

class Transformer:
    def __init__(self):
        pass
    def transform(self, inputDF):
        pass

class AirpodsAfterIphoneTransformer(Transformer):

    def transform(self, inputDFs):
        """
        Customer who purchased Airpods after buying iphone
        """
        transactionInputDF = inputDFs.get("transactionInputDF")
        print("transactionInputDF in transform")
        transactionInputDF.show()

        windowSpec = Window.partitionBy("customer_id").orderBy("transaction_date")

        transformedDF = transactionInputDF.withColumn(
            "next_purchased_product", lead("product_name").over(windowSpec)
        )

        print("Airpods after buying iphones")
        transformedDF.orderBy("customer_id", "transaction_date","product_name").show()

        filteredDF = transformedDF.filter(
            ((col("product_name") == "iPhone") & (col("next_purchased_product") == "AirPods"))
            )

        # filteredDF.orderBy("customer_id", "transaction_date","product_name").show()

        customerInputDF = inputDFs.get("customerInputDF")
        # customerInputDF.show()

        joinDF = customerInputDF.join(
            broadcast(filteredDF), 
            "customer_id"
            )
        print("Joined DF with filtered DF")
        joinDF.select(
            "customer_id",
            "customer_name",
            "location"
        ).show()

        return joinDF.select(
            "customer_id",
            "customer_name",
            "location"
        )

class OnlyAirpodsAndIphoneTransformer(Transformer):

    def transform(self, inputDFs):
        """
        Customers who have bought only Airpods and Iphone
        """
        transactionInputDF = inputDFs.get("transactionInputDF")
        print("transactionInputDF in transform")
        transactionInputDF.show()
        groupedDF = transactionInputDF.groupBy("customer_id").agg(
            collect_set("product_name").alias("products")
        )

        groupedDF.show()

        filteredDF = groupedDF.filter(
            ((array_contains(col("products"), "iPhone")) & 
              (array_contains(col("products"), "AirPods"))&
              (size(col("products")) == 2)
              )
            )
        filteredDF.show()

        customerInputDF = inputDFs.get("customerInputDF")
        joinDF = customerInputDF.join(
            broadcast(filteredDF), 
            "customer_id"
            )
        print("Joined DF with filtered DF")
        joinDF.select(
            "customer_id",
            "customer_name",
            "location"
        ).show()

        return joinDF.select(
            "customer_id",
            "customer_name",
            "location"
        )
