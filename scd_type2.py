from pyspark.sql import SparkSession
from pyspark.sql.functions import lit,date_add,date_sub
from pyspark.sql.functions import col,expr,when,unix_timestamp,from_unixtime,to_timestamp,sha2


def Type2SCD(hist_Df,curr_Df):
    REG_ID=hist_Df.REG_ID
    Name="Name"
    dfJoin=curr_Df.join(hist_Df,curr_Df.Name_Curr==hist_Df.Name,"left_outer")
    dfJoin.show()
    #Insert directly
    dfInsert=dfJoin.filter(REG_ID.isNull() | ((hist_Df.effective_dt==curr_Df.effective_dt_curr)\
    & (hist_Df.Name==curr_Df.Name_Curr) & (hist_Df.Location==curr_Df.Location_curr))).withColumn("ETL_FLAG",lit("I"))

    #Update
    dfUpdate=dfJoin.filter(REG_ID.isNotNull()) \
                    .filter((hist_Df.effective_dt!=curr_Df.effective_dt_curr)\
                    | (hist_Df.Name!=curr_Df.Name_Curr) |(hist_Df.Location!=curr_Df.Location_curr))\
                    .withColumn("ETL_FLAG", lit("U"))

    dfUpdate.show() # | Location!=Location | effective_dt!=effective_dt")
    dfInsert.show()

    return dfUpdate.unionAll(dfInsert)

if __name__ == "__main__":
    sc=SparkSession.builder.appName("Type 2 SCD").master("local[*]").getOrCreate()

    df_hist=sc.read.option("header",True).csv("C:/python-spark-tutorial/SCD/History.csv")
    #df_hist=df_hist.filter("expiry_dt=='3500-12-31'")
    df_hist.show(10)
    df_current=sc.read.option("header",True).csv("C:/python-spark-tutorial/SCD/File1_20191130.csv")
    new_names = ['REG_ID_Curr', 'Name_Curr', 'Location_curr','effective_dt_curr']
    df_current=df_current.toDF(*new_names)
    df_current.show()
    dfSCD=Type2SCD(df_hist,df_current)
    dfSCD.show()

    dfMergedUpdate=dfSCD.filter("ETL_FLAG=='U'")\
    .withColumn("expiry_dt_curr",date_sub(dfSCD.effective_dt_curr.cast("Date"),1))
    dfMergedUpdate.printSchema()
    dfMergedUpdate.show()

    dfMergedUpdate_1=dfMergedUpdate.select("REG_ID_Curr","Name_Curr","Location_curr","effective_dt_curr","expiry_dt")
    dfMergedUpdate_2=dfMergedUpdate.select("REG_ID","Name","Location","effective_dt","expiry_dt_curr")
    dfMergedUpdate_3=dfSCD.filter("ETL_FLAG=='I'").select("REG_ID_Curr","Name_Curr","Location_curr","effective_dt_curr",lit('3500-12-31'))
    dfFinal=dfMergedUpdate_1.unionAll(dfMergedUpdate_2).unionAll(dfMergedUpdate_3).show()
