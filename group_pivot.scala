import scala.collection.immutable.List
import org.apache.spark.sql.DataFrame

val col_names = List("John","Jane","Tim","Bill","Smith","Dan","Jim","Angie","Bob")
val select_cols = Seq("id","details.name", "details.salary")
val agg_expr_cols = List("John_name","John_salary","Jane_name","Jane_salary","Tim_name","Tim_salary","Bill_name","Bill_salary","Smith_name","Smith_salary","Dan_name","Dan_salary","Jim_name","Jim_salary","Angie_name","Angie_salary").map(c => max(c).as(c))

def expan_cols ( df : DataFrame , col_name : String ) : DataFrame = {
      df.withColumn(col_name+"_name", when($"name" === col_name, $"name").otherwise(null)).withColumn(col_name+"_salary", when($"name" === col_name, $"salary").otherwise(0))
      }

def gen_expan_cols (df : DataFrame, column_names : List[String]) : DataFrame = {
    if (column_names.size == 0)
    return df
    else 
        gen_expan_cols(expan_cols(df, column_names.head), column_names.tail)
}

val df = spark.read.json("nested.json")
df.show(false)
val d = df.withColumn("details", explode($"details")).select(select_cols.map(col(_)): _*)
d.show(false)
val bdf = gen_expan_cols(d, col_names).drop("name").drop("salary")
bdf.show(false)
bdf.groupBy($"id").agg(agg_expr_cols.head, agg_expr_cols.tail : _*).show

