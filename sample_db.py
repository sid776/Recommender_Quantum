import os
import logging
import datetime
from enum import Enum
from databricks.connect import DatabricksSession
from util.data_format import DataFormatter
from core.storage import AzStorage
import json
from pyspark.sql.connect.dataframe import DataFrame
class DataBricksConnection(object):
  SERVER = os.getenv('DATABRICKS_HOST')
  HTTP_PATH = os.getenv('DATABRICKS_SQL_WAREHOUSE_HTTP')
  ACCESS_TOKEN = None
  AUTH_TYPE = "databricks-oauth"
  
  def __init__(self):
    self.connection = None
    self.layer_map = dict(
                bronze = '%s.%s'%(os.getenv("DATABRICKS_CATALOG"), os.getenv('DATABRICKS_BRONZE_LAYER')),
                silver = '%s.%s'%(os.getenv("DATABRICKS_CATALOG"), os.getenv('DATABRICKS_SILVER_LAYER')),
                gold = '%s.%s'%(os.getenv("DATABRICKS_CATALOG"), os.getenv('DATABRICKS_GOLD_LAYER')),
                #gold = '%s.%s'%(os.getenv("DATABRICKS_NIWA_CATALOG"), os.getenv('DATABRICKS_NIWA_CATALOG_GOLD'))
    )
    
  def __enter__(self):
    if self.use_databricks_cluster():
      logging.info('Using DB cluster..')
      try:
        self.connection = DatabricksSession.builder.getOrCreate()
      except Exception as e:
        logging.info("Databricks Session is stopped, creating a new databricks session")
        logging.error(str(e))
        self.connection = DatabricksSession.builder.remote(user_agent="ussparc" + str(datetime.datetime.now())).getOrCreate()
    else:
      from databricks import sql
      logging.info('Using PyODBC')
      self.ACCESS_TOKEN =self._get_or_refresh_token()
      self.connection = sql.connect(server_hostname=os.getenv("DATABRICKS_HOST"), 
                                    http_path=os.getenv("DATABRICKS_SQL_WAREHOUSE_HTTP"), 
                                    auth_type=None if self.ACCESS_TOKEN else self.AUTH_TYPE,
                                    access_token=self.ACCESS_TOKEN)
    return self
    
  def __exit__(self, exc_type, exc_value, traceback):
    if not self.use_databricks_cluster():
      self.connection.close()
    
  def use_databricks_cluster(self):
    return os.environ.get("DATABRICKS_RUNTIME_VERSION") or os.environ.get("USE_DATABRICKS_CLUSTER")
  

  def _get_new_token(self):
    URL = f"https://login.microsoftonline.com/{os.getenv('ARM_TENANT_ID')}/oauth2/v2.0/token"
    data = {
        'grant_type' : 'client_credentials',
        'client_id' : os.getenv('ARM_CLIENT_ID'),
        'client_secret': os.getenv('ARM_CLIENT_SECRET'),
        'scope' : "2ff814a6-3304-4ab8-85cb-cd0e6f879c1d/.default"
    }
    import requests
    response = requests.post(URL, data=data, verify=False)
    token = response.json().get("access_token")
    return token


  def _get_or_refresh_token(self):

    try:
      container = f"{os.getenv('POLARIS_VALUATION_STORAGE')}/tokens/"
      blob_name = 'databricks_token.json'
      with AzStorage('USSPARC_BRONZE_VOLUME') as storage:
          # Upload the file to Azure Blob Storage
          json_data = storage.read(container,blob_name)
      
      token_data = json.loads(json_data)
      expires_at = datetime.datetime.fromisoformat(token_data['expires_at'])

      if datetime.datetime.now(datetime.timezone.utc) < expires_at - datetime.timedelta(minutes=5):
        return token_data["token"]
    except:
      pass
      
    new_token = self._get_new_token()
    new_expiry = datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(minutes=55)
    token_file_content = json.dumps({"token": new_token,"expires_at": new_expiry.isoformat()})
    lease_time = 15 # 15 secs
    with AzStorage('USSPARC_BRONZE_VOLUME') as storage:
          # Upload the file to Azure Blob Storage
          json_data = storage.write(container,blob_name,token_file_content)
    
    return new_token

  def _get_table_name(self,tbl, layer=None):
    layer = layer or 'bronze'
    table_name = self.layer_map.get(layer, layer) + '.' + tbl
    return table_name
  
  def execute(self, sql, df=False):
    if self.use_databricks_cluster():
      try:
        ret = self.connection.sql(sql)
      except Exception as e:
        logging.info("Databricks Session is stopped, creating a new databricks session")
        logging.error(str(e))
        self.connection = DatabricksSession.builder.remote(user_agent="ussparc" + str(datetime.datetime.now())).getOrCreate()
        ret = self.connection.sql(sql)
      if df:
        return ret.toPandas()
      return ret
      
    cursor = self.connection.cursor()
    cursor.fast_executemany = True
    ret = cursor.execute(sql)
    if df:
      import pandas
      data = ret.fetchall()
      return pandas.DataFrame([x.asDict() for x in data])
    return ret
  
  def dataframe_insert_query(self, df, tbl, layer=None):

    tbl = self._get_table_name(tbl, layer)
    sql_str = lambda x: DataFormatter.databricks_standard_formatter(x)
    qry = 'INSERT INTO %s %s'%(tbl, str(tuple(df.columns)))
    qry = qry.replace("'",'')
    qry += ' VALUES \n%s'%',\n'.join(['(%s)'%','.join(map(sql_str, (row))) for row in df.to_records(index=False)])
    return qry
  
  def dataframe_insert_spark(self, df, tbl, layer=None, mode=None):
    mode = mode or 'append'
    if self.connection.is_stopped:
      logging.info("Databricks Session is stopped, creating a new databricks session")
      self.connection = DatabricksSession.builder.remote().getOrCreate()
    tbl = self._get_table_name(tbl, layer)
    if isinstance(df, DataFrame):
        spark_df = df
    else:
        spark_df = self.connection.createDataFrame(df)
    db_table_schema = self.connection.table(tbl).schema
    spark_df_adjusted = spark_df.select(*[spark_df[field.name].cast(field.dataType) for field in db_table_schema.fields  if field.name in df.columns])
    
    try:
      spark_df_adjusted.write.mode(mode).saveAsTable(tbl)
    except Exception as e:
      raise ValueError(f'Failed to write to Spark table {tbl} in function dataframe_insert_spark : {str(e)}')

    
class MktDataDBConnection(DataBricksConnection):
  SERVER = "adb-2876230092097889.9.azuredatabricks.net"
  HTTP_PATH = "/sql/1.0/warehouses/fc1f9674da2289a2"    
  ACCESS_TOKEN = os.getenv("DATABRICKS_NIWA_TOKEN")
  
class DBConnection(DataBricksConnection):
  SERVER = "adb-7323138722438154.14.azuredatabricks.net"
  HTTP_PATH = "/sql/1.0/warehouses/f82ed7174cd4b585"     
  ACCESS_TOKEN = os.getenv("DATABRICKS_TOKEN")

class ObjectDBMapping(Enum):
  @classmethod
  def get_mapping(cls):
    return dict((k,v.value) for k,v in cls.__members__.items())

  @classmethod
  def get_inverse_mapping(cls):
    return dict((v.value,k) for k,v in cls.__members__.items())
