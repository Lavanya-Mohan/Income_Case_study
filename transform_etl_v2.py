                               ####   IMPORT LIBRARIES   ###
from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import avg, round, col,sum,count,split,weekofyear,struct,regexp_extract
from pyspark.sql.functions import *
from pyspark.sql import DataFrame
from functools import reduce
from pyspark.sql.functions import from_unixtime,to_date, unix_timestamp

category_mapping = {
"1": "Smartphone",
"2": "Charging Cable",
"3": "Headphones",
"4": "Monitor",
"5": "Batteries",
"6": "Laptop",
"7": "TV",
"8": "Dryer",
"9": "Washing Machine",
"10": "others"
}



replacements = {
    "int": 0,
    "double": 0.0,
    "float": 0.0,
    "string": ""
}

def create_spark_session(app_name: str) -> SparkSession:
    """ Create a spark session.
    """
    ss = SparkSession.builder.appName(app_name).config("spark.jars", "mysql-connector-j-8.3.0.jar").getOrCreate()#.master('local').appName(app_name).getOrCreate()
    return ss


def read_in_data(sc: SparkSession, file: str):
    """ Return a spark DataFrame of the excel file <file>.
    """
    return sc.read.csv(file, header='true', sep=',', inferSchema=True)


def unionAll(*dfs):
    """ Return a spark DataFrame combining all the dfs.
    """
    return reduce(DataFrame.unionAll, dfs) 

def missing_values_imputation(df):
    """ Return a spark DataFrame with missing values computes.
    """
    for col_name, data_type in df.dtypes:
            replacement_value = replacements.get(data_type.lower(), None)
            if replacement_value is not None:
                df = df.withColumn(col_name, when(col(col_name).isNull(), replacement_value).otherwise(col(col_name)))
    return df



def extract_date_columns_liquor(df,date_col):
    """ Return a spark DataFrame with derived columns from date in liquor.
    """
    df=df.withColumn('year', split(liquor_sales_df[date_col], '/').getItem(2)) \
   .withColumn('month', split(liquor_sales_df[date_col], '/').getItem(0)) 
    df=df.withColumn(date_col, to_date(date_col, "MM/dd/yyyy"))
    df=df.withColumn("week", weekofyear(date_col))
    return df


def get_lat_long(df,point_col):
    """ Return a spark DataFrame with derived columns lat and long from point.
    """
    df=df.withColumn("geometry_cleaned", regexp_replace(point_col, "POINT \\(", ""))\
    .withColumn("geometry_cleaned", regexp_replace("geometry_cleaned", "\\)", ""))\
    .withColumn("lat", split(col("geometry_cleaned"), " ")[1].cast("float")) \
    .withColumn("lon", split(col("geometry_cleaned"), " ")[0].cast("float"))\
    .drop("geometry_cleaned")
    return df  

def extract_date_columns_electronics(df,date_col):
    """ Return a spark DataFrame with derived columns from date in electronics.
    """
    df=df.withColumn('year', split(df[date_col], '-').getItem(0)) \
   .withColumn('month', split(df[date_col], '-').getItem(1)) 
    df = df.withColumn("orderDateOnly", to_date(date_col))
    df=df.withColumn("week", weekofyear(date_col))
    return df

def split_address(df,address_col):
    """ Return a spark DataFrame with derived column like city,state from address.
    """
    df = df.withColumn("city", split(df[address_col],",").getItem(1))\
    .withColumn("state_code", split(df[address_col],",").getItem(2))
    df=df.withColumn("state", split(df["state_code"], " ").getItem(1))
    df = df.drop("state_code")
    return df                                        

def write_to_rds_mysql_db(df,table):
    """ Loads data into RDS.
    """               
    SERVER_ADDR_port = "income-mysql-db.cjuyq4k026ji.us-east-2.rds.amazonaws.com:3306"
    db_name = "income" 
    table_name = "electronicsDataMart"  
    df.write \
        .format("jdbc") \
        .option("driver","com.mysql.cj.jdbc.Driver") \
        .mode("overwrite") \
        .option("url", f"jdbc:mysql://{SERVER_ADDR_port}/{db_name}") \
        .option("dbtable", table) \
        .option("user", "admin") \
        .option("password", "Logindatabase24") \
        .save()
    return "Loaded into the database"   
                                        
                                        
                                        
if __name__ == '__main__':

    # create a spark session
    spark = create_spark_session("datapipeline")

    #### EXTRACT ####
    ####  READ BOOKS DATA ####
    
    # read in the books_data.csv as a spark DataFrame
    books_df = read_in_data(spark, 'books_data.csv')

    # read in the Books_ratingas a spark DataFrame
    books_ratings_df = read_in_data(spark, 'Books_rating.csv')
    
    ####  READ SALES DATA ####
    # read in the Sales_2019.csv's a spark DataFrame
    sales_jan_df= read_in_data(spark, 'Sales_January_2019.csv')
    sales_feb_df= read_in_data(spark, 'Sales_February_2019.csv')
    sales_mar_df= read_in_data(spark, 'Sales_March_2019.csv')
    sales_april_df= read_in_data(spark, 'Sales_April_2019.csv')
    sales_may_df= read_in_data(spark, 'Sales_May_2019.csv')
    sales_june_df= read_in_data(spark, 'Sales_June_2019.csv')
    sales_july_df= read_in_data(spark, 'Sales_July_2019.csv')
    sales_aug_df= read_in_data(spark, 'Sales_August_2019.csv')
    sales_sep_df= read_in_data(spark, 'Sales_September_2019.csv')
    sales_oct_df= read_in_data(spark, 'Sales_October_2019.csv')
    sales_nov_df= read_in_data(spark, 'Sales_November_2019.csv')
    sales_dec_df= read_in_data(spark, 'Sales_December_2019.csv')
    
    
    ####  LIQUOR SALES DATA ####
    # read in the liquor.csv's a spark DataFrame
    liquor_sales_df= read_in_data(spark, 'Liquor_Sales.csv')
    
    #### TRANSFORM LIQUOR ####
    liquor_date_df=extract_date_columns_liquor(liquor_sales_df,"Date")
    liquor_date_df= get_lat_long(liquor_date_df,"Store Location")    
    liquor_data_mart_df=missing_values_imputation(liquor_date_df)

    #### TRANSFORM BOOKS  ####
    books_ratings_df=books_ratings_df.withColumn('Price',col('Price').cast("integer"))
    books_ratings_df = books_ratings_df.withColumn("review/time", from_unixtime("review/time"))
    books_df_joined=books_df.join(books_ratings_df,books_df.Title ==  books_ratings_df.Title,"inner").drop(books_ratings_df.Title)
    books_data_mart=missing_values_imputation(books_df_joined)

    #### NLP IMPUTATIONS ####
    replacement_value = ""

    col_onlystr_nourl_nonum = ['Title', 'description','authors','publisher','categories','User_id',]
    for col_name in col_onlystr_nourl_nonum:
        # Filter out full number values and URL links -- only string no numbers no urls
        books_data_mart = books_data_mart.withColumn(col_name, when(col(col_name).isNull() | (col(col_name) == "") | (regexp_extract(col(col_name), r'^\d+$', 0) != "") | (regexp_extract(col(col_name), r'^((http|https|ftp):\/\/[^\s\/$.?#].[^\s]*)$', 0) != ""), replacement_value).otherwise(col(col_name)))

    col_url = ['image', 'previewLink', 'infoLink',]
    for col_name in col_url:
        # Filter out non-URL values -- only url link
        books_data_mart = books_data_mart.withColumn(col_name, when(col(col_name).isNull() | (col(col_name) == "") | (regexp_extract(col(col_name), r'^((http|https|ftp):\/\/[^\s\/$.?#].[^\s]*)$', 0) == ""), replacement_value).otherwise(col(col_name)))

    col_onlyfloat_num = ['publishedDate','ratingsCount','review/score','review/time', 'Price']
    for col_name in col_onlyfloat_num:
        # Filter out non-numeric string values and URL links -- only float , number 
        books_data_mart = books_data_mart.withColumn(col_name, when(col(col_name).isNull() | (col(col_name) == "") | (regexp_extract(col(col_name), r'^\d+(\.\d+)?$', 0) == "") | (regexp_extract(col(col_name), r'^((http|https|ftp):\/\/[^\s\/$.?#].[^\s]*)$', 0) != ""), replacement_value).otherwise(col(col_name)))

    col_onlyfloat_num_slash = ['review/helpfulness',]
    for col_name in col_onlyfloat_num_slash:
        # Filter out non-numeric string values and URL links
        books_data_mart = books_data_mart.withColumn(col_name, when(col(col_name).isNull() | (col(col_name) == "") | (regexp_extract(col(col_name), r'^\d+(\.\d+)?(/\d+(\.\d+)?)?$', 0) == "") | (regexp_extract(col(col_name), r'^((http|https|ftp):\/\/[^\s\/$.?#].[^\s]*)$', 0) != ""), replacement_value).otherwise(col(col_name)))


    #### TRANSFORM ELECTRONICS  ####
    df_sales=unionAll(*[sales_jan_df, sales_feb_df,sales_mar_df,sales_april_df,sales_may_df,sales_june_df
           ,sales_july_df,sales_aug_df,sales_sep_df,sales_oct_df,sales_nov_df,sales_dec_df])
    
    ### SPLIT ADDRESS ###
    df_sales=df_sales.withColumn("Order Date", to_date(unix_timestamp("Order Date", "MM/dd/yy HH:mm").cast("timestamp")))
    df_sales_split=split_address(df_sales,"Purchase Address")
    
    ### date operations ###
    df_date=extract_date_columns_electronics(df_sales_split,"Order Date")
    df_date=df_date.withColumn("category",
        when(col("product").contains("Batteries"), "Batteries")
       .when(col("product").contains("Charging Cable"), "Charging Cable")
       .when(col("product").contains("Phone"), "Smartphone")
       .when(col("product").contains("Headphones"), "Headphones")
       .when(col("product").contains("Laptop"), "Laptop")
       .when(col("product").contains("TV"), "TV")
       .when(col("product").contains("Dryer"), "Dryer")
       .when(col("product").contains("Washing Machine"), "Washing Machine")
       .otherwise("Others"))
    electronics_data_mart=missing_values_imputation(df_date)
  

           ####  LOADING INTO DATABASE  ####                                

#### Electronics data mart ####                                        
                                        
write_to_rds_mysql_db(electronics_data_mart,"electronicsDataMart")  
                                        
#### Liquor data mart  ####                                   
write_to_rds_mysql_db(liquor_data_mart_df,"liquorDataMart")  
                                                                                
#### books data mart  ####                                             
write_to_rds_mysql_db(books_data_mart,"booksDataMart") 












