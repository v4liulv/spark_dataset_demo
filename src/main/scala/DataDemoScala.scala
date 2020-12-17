import java.net.URLDecoder

import org.apache.hadoop.conf.Configuration
import org.apache.spark.sql.SparkSession


/**
 * Created by Administrator on 2016/11/22 0022.
 *
 * Spark	SQL能够自动推断JSON数据集的模式，加载它为一个Schema RDD
 * json File	：从一个包含JSON文件的目录中加载。文件中的每一行是一个JSON对象
 */
object DataDemoScala {

  def main(args: Array[String]): Unit = {


    val sparkBuilder = SparkSession.builder.master("local[2]").appName("DataFrameJFormJsonFileTest")
    import scala.collection.JavaConversions._
    for (cMap <- new Configuration) {
      sparkBuilder.config(cMap.getKey, cMap.getValue)
    }
    //如果hive的话放入配置文件后用这个
    //val spark = sparkBuilder.enableHiveSupport.getOrCreate;
    val spark = sparkBuilder.getOrCreate

    def getClassPath: String = "file://" + {
      var classPath = Thread.currentThread.getContextClassLoader.getResource("").getPath
      //classPath = classPath.substring(0, classPath.length - 8)
      classPath = URLDecoder.decode(classPath, "UTF-8")
      return classPath
    }

    val peopleJsonPath = getClassPath + "people.json"

    println("peopleJsonPath: " + peopleJsonPath)
    val people = spark.read.json(peopleJsonPath)
    people.printSchema()
    //	root
    //		|--	age:	integer	(nullable	=	true)
    //		|--	name:	string	(nullable	=	true)
    people.show()

    //方式一： SparkSql
    people.createOrReplaceTempView("people")
    import spark.sql
    import spark.implicits._
    println("================================")
    val gsql = "SELECT id, name, sum(age*0.2) as sum0d2, sum(age) as sum, sum(age*0.2)/sum(age) as sumb FROM people group by id, name"
    sql(gsql).show()
    println("================================")

    //方式一： 聚合函数
    //方式二
    import org.apache.spark.sql.functions._
    people.groupBy("id", "name").agg(sum("age") * 0.2 / sum("age").as("sum_num"))
      .show()

    spark.stop()
  }

}
