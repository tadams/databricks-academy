# Databricks notebook source
json_data = {
  "inputs": "2yu73 thermostat 2yu66",
  "base_entities": [
    "hvac and refrigeration",
    "Freezer"
  ],
  "qualifiers": {
    "attribute": {
      "uom": [
                {
                    "value": "120",
                    "uomKey": "voltage"
                },
                {
                    "value": "24",
                    "uomKey": "voltage"
                }
            ],
      "other": [
        "thermostat"
      ]
    },
    "brand": [ "The Brand1", "Another Brand" ]
  },
  "part_no_entities": [
      "2y",
      "6502"
  ],
  "sku_entities": [
    "2yu73",
    "2yu66"
  ],
  "wp_entities": [],
  "upc_entities": [],
  "requestId": [],
  "modelVersion": "1.0",
  "noise": [],
  "cpPredRedirect": 1,
  "spell_corrected": 0,
  "corrected_keyword": {},
  "productCount": 33668,
  "ambiguous_entity_flag": 0,
  "broad_search_flag": 0,
  "tab_view": [],
  "close_match": {
    "node": [
      "1~HVAC and Refrigeration"
    ],
    "lowest_level": [
      "5~HVAC and Refrigeration~Heaters~Electric Heaters~Electric Wall & Ceiling Heaters~Suspended Electric Wall & Ceiling Unit Heaters",
      "5~HVAC and Refrigeration~Heaters~Electric Heaters~Electric Wall & Ceiling Heaters~Electric Wall & Ceiling Heater Thermostats & Accessories"
    ]
  }
}

json_map = {
    "city": "Dallas","State":"TX",
    "Order": {
        "amount": 100, "customer": "Sam",
        "lines": [ { "line_id": 1}, { "line_id": 2}]
    },
    "junk": { "more_junk": { "stuff": 0 }}
}

print(json_data)

# COMMAND ----------

df=spark.createDataFrame([(1, str(json_data))],["id","json_string"])
df.show(truncate=False)

# COMMAND ----------

from pyspark.sql.functions import get_json_object, explode, array_join, from_json, col, concat_ws, concat, arrays_zip, zip_with
from pyspark.sql.types import ArrayType, StringType, StructType, StructField, IntegerType, MapType

json_column = df.json_string

# json_schema = StructType([
#     StructField("inputs", StringType(), True),
#     StructField("base_entities", ArrayType(StringType()), True),
#     StructField("qualifiers", StructType([
#         StructField("attribute", StructType([
#             StructField("uom", ArrayType(StringType()), True),
#             StructField("other", ArrayType(StringType()), True)
#         ]), True),
#         StructField("brand", ArrayType(StringType()), True)
#     ]), True),
#     StructField("part_no_entities", ArrayType(StringType()), True),
#     StructField("sku_entities", ArrayType(StringType()), True),
# ])

json_schema = StructType([
    StructField("inputs", StringType(), True),
    StructField("base_entities", ArrayType(StringType()), True),
    StructField("qualifiers", StructType([
        StructField("attribute", StructType([
            StructField("uom", ArrayType(StructType([
                StructField("value", StringType(), True),
                StructField("uomKey", StringType(), True)
            ])), True),
            StructField("other", ArrayType(StringType()), True)
        ]), True),
        StructField("brand", ArrayType(StringType()), True)
    ]), True),
    StructField("part_no_entities", ArrayType(StringType()), True),
    StructField("sku_entities", ArrayType(StringType()), True)
])

df2 = (
    df.withColumn("json", from_json(col("json_string"), json_schema))
      .withColumn("base_entities", concat_ws(", ", col("json.base_entities")))
      .withColumn("brand", concat_ws(", ", col("json.qualifiers.brand")))
      .withColumn("uom_temp",
                  zip_with("json.qualifiers.attribute.uom.value", "json.qualifiers.attribute.uom.uomKey",
                           lambda v, k: concat_ws(":", v, k))
        )
      .withColumn("uom", concat_ws("|", "uom_temp"))
      .withColumn("part_no_entities", concat_ws(", ", col("json.part_no_entities")))
      .withColumn("sku_entities", concat_ws(", ", col("json.sku_entities")))
      .drop("uom_temp")
)
df2.printSchema()

df2.drop("json_string", "json").show(truncate=False)

# COMMAND ----------


