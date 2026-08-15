import delta
import utils

class Ingestor:
  
    def __init__(self, spark, catalog, schemaname, tablename, data_format):
        self.spark = spark
        self.catalog = catalog
        self.schemaname = schemaname
        self.tablename = tablename
        self.format = data_format
        self.set_schema()

    def set_schema(self):
        self.data_schema = utils.import_schema(self.tablename)

    def load(self, path):
        df = (self.spark
                  .read
                  .format(self.format)
                  .schema(self.data_schema)
                  .load(path))
        return df

    def save(self, df):
        (df.coalesce(1)
           .write
           .format("delta")
           .mode("overwrite")
           .saveAsTable(f"{self.catalog}.{self.schemaname}.{self.tablename}"))
        return True
        
    def execute(self, path):
        df = self.load(path)
        return self.save(df)
        

class IngestorCDC(Ingestor):
  
    def __init__(self, spark, catalog, schemaname, tablename, data_format, id_field, timestamp_field):
        super().__init__(spark, catalog, schemaname, tablename, data_format)
        self.id_field = id_field
        self.timestamp_field = timestamp_field
        self.set_deltatable()

    def set_deltatable(self):
        tablename = f"{self.catalog}.{self.schemaname}.{self.tablename}"
        self.deltatable = delta.DeltaTable.forName(self.spark, tablename)

    def upsert(self, df):

        df.createOrReplaceGlobalTempView(f"view_{self.tablename}")
        
        query = f'''
        SELECT *
        FROM global_temp.view_{self.tablename}
        QUALIFY ROW_NUMBER() OVER(PARTITION BY {self.id_field} ORDER BY {self.timestamp_field} DESC) = 1
        '''

        df_cdc = self.spark.sql(query)

        (self.deltatable.alias("b")
                        .merge(df_cdc.alias("d"), f"b.{self.id_field} = d.{self.id_field}")
                        .whenMatchedDelete(condition= "d._operation = 'DELETE'")
                        .whenMatchedUpdateAll(condition= "d._operation = 'UPDATE'")
                        .whenNotMatchedInsertAll(condition="d._operation = 'INSERT' OR d._operation = 'UPDATE'")
                        .execute())
        
    def load(self, path):
        df = (self.spark
                  .readStream
                  .format("cloudFiles")
                  .option("cloudFiles.format",self.format)
                  .schema(self.data_schema)
                  .load(path))
        return df
    
    def save(self, df):
        catalog = self.catalog
        schemaname = self.schemaname
        tablename = self.tablename
        id_field = self.id_field
        timestamp_field = self.timestamp_field

        def _upsert(df, batchID):
            spark = df.sparkSession
            df.createOrReplaceGlobalTempView(f"view_{tablename}")
            query = f'''
            SELECT *
            FROM global_temp.view_{tablename}
            QUALIFY ROW_NUMBER() OVER(PARTITION BY {id_field} ORDER BY {timestamp_field} DESC) = 1
            '''
            df_cdc = spark.sql(query)
            (delta.DeltaTable.forName(spark, f"{catalog}.{schemaname}.{tablename}")
                             .alias("b")
                             .merge(df_cdc.alias("d"), f"b.{id_field} = d.{id_field}")
                             .whenMatchedDelete(condition="d._operation = 'DELETE'")
                             .whenMatchedUpdateAll(condition="d._operation = 'UPDATE'")
                             .whenNotMatchedInsertAll(condition="d._operation = 'INSERT' OR d._operation = 'UPDATE'")
                             .execute())

        stream = (df.writeStream
                    .option("checkpointLocation", f"/Volumes/raw/{schemaname}/cdc/{tablename}/_checkpoints/")
                    .foreachBatch(_upsert)
                    .trigger(availableNow=True))
        return stream.start()

