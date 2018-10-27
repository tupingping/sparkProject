package cn.tpp.sparkProject.test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

import com.alibaba.fastjson.JSONObject;

import cn.tpp.sparkProject.constant.Constants;
import cn.tpp.sparkProject.util.ParamUtils;
import scala.Tuple2;

public class sparkTest {

	public static void main(String[] args) {
		// TODO 自动生成的方法存根
		SparkConf conf = new SparkConf().setAppName("sparkTest")
				.setMaster("local");
		
		JavaSparkContext javaSparkContext = new JavaSparkContext(conf);
			
	}
	
	private static JavaRDD<Row> getActionRDDByDateRange(SQLContext sqlContext, JSONObject param){
			
		String dateStart = ParamUtils.getParam(param, Constants.PARAM_START_DATE);
		String dateEnd = ParamUtils.getParam(param, Constants.PARAM_END_DATE);
		
		String sql = "select * from user_visit_action "+
					"where "+
					"date>='" + dateStart + "' "+
					"and date<='" + dateEnd +"'";
		
		DataFrame dFrame = sqlContext.sql(sql);
				 
		return dFrame.javaRDD();
	}
	
	
	private static void mockData(JavaSparkContext jsc, SQLContext sqlContext){
		 List<Row> rows = new ArrayList<Row>();
		 String data=null;
		 String userid=null;
		 String[] param=null;
		 
		 for(int i = 0; i < 100; i++){
			 
			 Row row = RowFactory.create(data,userid,param);
			 rows.add(row);
		 }
		 
		 JavaRDD<Row> javaRDD = jsc.parallelize(rows);
		 
		 StructType schema = DataTypes.createStructType(Arrays.asList(
					DataTypes.createStructField("date", DataTypes.StringType, true),
					DataTypes.createStructField("user_id", DataTypes.LongType, true),
					DataTypes.createStructField("session_id", DataTypes.StringType, true),
					DataTypes.createStructField("page_id", DataTypes.LongType, true),
					DataTypes.createStructField("action_time", DataTypes.StringType, true),
					DataTypes.createStructField("search_keyword", DataTypes.StringType, true),
					DataTypes.createStructField("click_category_id", DataTypes.LongType, true),
					DataTypes.createStructField("click_product_id", DataTypes.LongType, true),
					DataTypes.createStructField("order_category_ids", DataTypes.StringType, true),
					DataTypes.createStructField("order_product_ids", DataTypes.StringType, true),
					DataTypes.createStructField("pay_category_ids", DataTypes.StringType, true),
					DataTypes.createStructField("pay_product_ids", DataTypes.StringType, true),
					DataTypes.createStructField("city_id", DataTypes.LongType, true)));
		 
		 DataFrame dFrame = sqlContext.createDataFrame(javaRDD, schema);
		 
		 dFrame.registerTempTable("user_visit_action");
	}

	
	private static JavaPairRDD<String, String> aggregateByUserid(JavaRDD<Row> actionRDD, SQLContext sc){
		
		JavaPairRDD<String,Row> session2ActionPairRDD = actionRDD.mapToPair(new PairFunction<Row, String, Row>() {

			@Override
			public Tuple2<String, Row> call(Row row) throws Exception {
				return new Tuple2<String, Row>(row.getString(2), row);
			}			
		});
		
		JavaPairRDD<String, Iterable<Row>>  session2ActionsPairRDD = session2ActionPairRDD.groupByKey();	
		
		JavaPairRDD<Long, String> session2PartAggrInfoRDD = session2ActionsPairRDD.mapToPair(new 
				PairFunction<Tuple2<String,Iterable<Row>>, Long, String>() {

					private static final long serialVersionUID = 1L;

					@Override
					public Tuple2<Long, String> call(Tuple2<String, Iterable<Row>> tuple) throws Exception {
						
						String sessionid = tuple._1;
						Iterator<Row> iterable = tuple._2.iterator();
						String aggregateInfo = "";
						String seachkey ="";
						Long userid = 0L;
						while (iterable.hasNext()) {
							Row row = iterable.next();
							
							
							
						}
												
						return new Tuple2<Long, String>(userid, aggregateInfo);
					}
		});
		
		String sql = "select * from user_info";
		DataFrame dFrame=sc.sql(sql);
		
		JavaRDD<Row> userInfoRDD = dFrame.javaRDD();
		
		JavaPairRDD<Long, Row> userInfPairRDD = userInfoRDD.mapToPair(new PairFunction<Row, Long, Row>() {
			private static final long serialVersionUID = 1L;

			@Override
			public Tuple2<Long, Row> call(Row t) throws Exception {
				return new Tuple2<Long, Row>(t.getLong(0), t);
			}
			
		});
		
		JavaPairRDD<Long, Tuple2<String,Row>>  infoPairRDD = session2PartAggrInfoRDD.join(userInfPairRDD);
		
		JavaPairRDD<String, String> fullAggregateInfoPairRDD =  infoPairRDD.mapToPair(new 
				PairFunction<Tuple2<Long,Tuple2<String,Row>>, String, String>() {
					private static final long serialVersionUID = 1L;

					@Override
					public Tuple2<String, String> call(Tuple2<Long, Tuple2<String, Row>> t) throws Exception {
						
						String sessionid="";
						String fullAgrrInfo="";
											
						return new Tuple2<String, String>(sessionid, fullAgrrInfo);
					}		
		});
		
		return fullAggregateInfoPairRDD;
	}
}
