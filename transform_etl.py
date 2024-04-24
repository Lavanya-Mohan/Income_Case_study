from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import avg, round, col,sum,count,split,weekofyear,struct,regexp_extract
from pyspark.sql.functions import *
from pyspark.sql import DataFrame
from functools import reduce
from pyspark.sql.functions import from_unixtime,to_date, unix_timestamp

def create_spark_session(app_name: str) -> SparkSession:
    """ Create a spark session.
    """
    # ss = SparkSession.builder.master('local').appName(app_name).getOrCreate()
    ss = SparkSession.builder.master('local').appName(app_name).config("spark.jars", "s3://income-sales-data/airflow/scripts/mysql-connector-j-8.3.0.jar").getOrCreate()

    return ss


def read_in_data(sc: SparkSession, file: str):
    """ Return a spark DataFrame of the excel file <file>.
    """
    return sc.read.csv(file, header='true', sep=',', inferSchema=True)


# def sum_by_feature(df: DataFrame, feature: str) -> DataFrame:
#     """ Find the average salary of <feature> by groups.
#     """
   
#     return df.groupby(feature) \
#         .agg(sum('Price').alias('Total_Price')) \
#         .sort('Total_Price', ascending=False)

# def count_by_feature(df: DataFrame, feature: str) -> DataFrame:
#     """ Find the average salary of <feature> by groups.
#     """
#     df.printSchema()
#     return df.groupby(feature) \
#         .agg(count('Price').alias('Total_Quantity')) \
#         .sort('Total_Quantity', ascending=False)

def missing_values_imputation(df):
 # Loop through columns and apply replacement rules
    for col_name, data_type in df.dtypes:
            replacement_value = replacements.get(data_type.lower(), None)
            if replacement_value is not None:
                df = df.withColumn(col_name, when(col(col_name).isNull(), replacement_value).otherwise(col(col_name)))
    return df


def output_result(df: DataFrame, output_location: str, output_folder: str) -> None:
    """ Save the DataFrame <df> as a csv file in the location specified by
    <output_location>.
    """
    # condense all data points in one single file
    df.coalesce(1).write.csv(path=output_location + output_folder,
                             mode='append', header=True)


if __name__ == '__main__':

    # create a spark session
    spark = create_spark_session("datapipeline")

    #### EXTRACT ####

    # read in the train_features.csv as a spark DataFrame
    books_df = read_in_data(spark, 's3://income-sales-data/books_data.csv')


    books_ratings_df = read_in_data(spark, 'Books_rating.csv')
    
    
    ####  READ SALES DATA ####
    
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
    
    print("LIQUOR Transformations")
    ####  LIQUOR SALES DATA ####
    
    liquor_sales_df= read_in_data(spark, 'Liquor_Sales.csv')
    liquor_date_df=liquor_sales_df.withColumn('year', split(liquor_sales_df['Date'], '/').getItem(2)) \
   .withColumn('month', split(liquor_sales_df['Date'], '/').getItem(0)) 
    liquor_date_df=liquor_date_df.withColumn("Date", to_date("Date", "MM/dd/yyyy"))
    liquor_date_df=liquor_date_df.withColumn("week", weekofyear("Date"))
    
    liquor_date_df=liquor_date_df.withColumn("geometry_cleaned", regexp_replace("Store Location", "POINT \\(", ""))\
.withColumn("geometry_cleaned", regexp_replace("geometry_cleaned", "\\)", ""))\
.withColumn("lat", split(col("geometry_cleaned"), " ")[1].cast("float")) \
.withColumn("lon", split(col("geometry_cleaned"), " ")[0].cast("float"))\
.drop("geometry_cleaned")
    liquor_date_df=missing_values_imputation(liquor_date_df)

    #### TRANSFORM ####

    print("Book Transformations")
    books_ratings_df=books_ratings_df.withColumn('Price',col('Price').cast("integer"))
    books_ratings_df = books_ratings_df.withColumn("review/time", from_unixtime("review/time"))
    #print(df.printSchema())
  

    
    books_df_joined=books_df.join(books_ratings_df,books_df.Title ==  books_ratings_df.Title,"inner").drop("Title")
    
    books_df_joined=missing_values_imputation(books_df_joined)
    
    print("Electronics Transformations")
    ####### Electronics transformation #######
  
    
    df_sales=unionAll(*[sales_jan_df, sales_feb_df,sales_mar_df,sales_april_df,sales_may_df,sales_june_df
           ,sales_july_df,sales_aug_df,sales_sep_df,sales_oct_df,sales_nov_df,sales_dec_df])
    
    ### SPLIT ADDRESS ###
    df_sales=df_sales.withColumn("Order Date", to_date(unix_timestamp("Order Date", "MM/dd/yy HH:mm").cast("timestamp")))
    split_df = df_sales.withColumn("city", split(df_sales["Purchase Address"],",").getItem(1))\
.withColumn("state_code", split(df_sales["Purchase Address"],",").getItem(2))
    df_sales_split=split_df.withColumn("state", split(split_df["state_code"], " ").getItem(1))
    df_sales_split = df_sales_split.drop("state_code")
    
    ### date operations ###
    df_date=df_sales_split.withColumn('year', split(df_sales_split['Order Date'], '-').getItem(0)) \
       .withColumn('month', split(df_sales_split['Order Date'], '-').getItem(1)) 
    df_date = df_date.withColumn("orderDateOnly", to_date("Order Date"))
    df_date=df_date.withColumn("week", weekofyear("Order Date"))
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
    df_date_electronics=missing_values_imputation(df_date)
    print("Transformations completed")
    # books_df.write \
    # .format("jdbc") \
    # .option("driver","com.mysql.jdbc.Driver") \
    # .option("url", "jdbc:mysql://income-sales-db.cjuyq4k026ji.us-east-2.rds.amazonaws.com:1433/income") \
    # .option("dbtable", "books_data") \
    # .option("user", "admin") \
    # .option("password", "Logindatabase24") \
    # .save()

    ##Electronics data mart
    SERVER_ADDR_port = "income-mysql-db.cjuyq4k026ji.us-east-2.rds.amazonaws.com:3306"
    db_name = "income"
    print("Electronics data mart")
    df_date_electronics.write \
    .format("jdbc") \
    .option("driver","com.mysql.cj.jdbc.Driver") \
    .mode("overwrite") \
    .option("url", f"jdbc:mysql://{SERVER_ADDR_port}/{db_name}") \
    .option("dbtable", "electronicsDataMart") \
    .option("user", "admin") \
    .option("password", "Logindatabase24") \
    .save()

    print("books data mart")
    ##Books data mart
    SERVER_ADDR_port = "income-mysql-db.cjuyq4k026ji.us-east-2.rds.amazonaws.com:3306"
    db_name = "income"

    books_df_joined.write \
    .format("jdbc") \
    .option("driver","com.mysql.cj.jdbc.Driver") \
    .mode("overwrite") \
    .option("url", f"jdbc:mysql://{SERVER_ADDR_port}/{db_name}") \
    .option("dbtable", "booksDataMart") \
    .option("user", "admin") \
    .option("password", "Logindatabase24") \
    .save()


   
    

    spark.stop()