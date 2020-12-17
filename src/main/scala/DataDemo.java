import org.apache.spark.SparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.io.UnsupportedEncodingException;

import static org.apache.spark.sql.functions.*;

/**
 * @author liulv
 * @since 1.0.0
 * <p>
 * 说明：
 */
public class DataDemo {

    public static void main(String[] args) {
              SparkSession spark = SparkSession.builder().master("local").appName("cs").getOrCreate();

        Dataset<Row> json = spark.read().json(PUtil.CLASS_PATH + "people.json");
        //相当于 select id, name, sum("age") * 0.2 / sum("age")
        Dataset<Row> agg = json.groupBy("id", "name").agg(sum("age")
                .multiply(0.2).divide(sum("age").as("sum_num")));
        agg.show();

        //相当于 select id, name, sum("age") * id / sum("age")
        Dataset<Row> agg2 = json.groupBy("id", "name").agg(sum("age")
                .multiply(json.col("id")).divide(sum("age").as("sum_num")));
        agg2.show();

        spark.stop();

    }
}
