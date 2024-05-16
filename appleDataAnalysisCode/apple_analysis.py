# Databricks notebook source
# MAGIC %run "./transform"

# COMMAND ----------

# MAGIC %run "./extractor"

# COMMAND ----------

# MAGIC %run "./loader"

# COMMAND ----------

class FirstWorkFlow:
    """
    ETL Pipeline to generate the data for all customers who have bought airpods just after buying iphones
    """
    def __init__(self):
        pass

    def runner(self):

        # Step 1: Extract all data from different sources
        inputDFs = AirpodsAfterIphoneExtractor().extract()
        
        # Step 2: Implement the transformation logic 
        # Customers who have bought Airpods after buying iphone
        firstTransformedDF = AirpodsAfterIphoneTransformer().transform(
            inputDFs 
        )

        # Step 3: Load all required data to differnt sink
        AirpodsAfterIphoneLoader(firstTransformedDF).sink()



# COMMAND ----------

class SecondWorkFlow:
    """
    ETL Pipeline to generate the data for all customers who have bought only airpods and iphones
    """
    def __init__(self):
        pass

    def runner(self):

        # Step 1: Extract all data from different sources
        inputDFs = AirpodsAfterIphoneExtractor().extract()
        
        # Step 2: Implement the transformation logic 
        # Customers who have bought Airpods after buying iphone
        onlyAirpodsAndIphoneDF = OnlyAirpodsAndIphoneTransformer().transform(
            inputDFs 
        )

        # Step 3: Load all required data to differnt sink
        OnlyAirpodsAndIphoneLoader(onlyAirpodsAndIphoneDF).sink()



# COMMAND ----------

class WorkFlowRunner:
    def __init__(self, name):
        self.name = name
    
    def runner(self):
        if self.name == "firstWorkFlow":
            return FirstWorkFlow().runner()
        elif self.name == "secondWorkFlow":
            return SecondWorkFlow().runner()
        else:
            raise ValueError(f"Not implemented for {self.name}")

workflowname = "secondWorkFlow"  

workFlowRunner = WorkFlowRunner(workflowname).runner()