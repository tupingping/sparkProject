package cn.tpp.sparkProject.spark;

import java.util.Iterator;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.hive.HiveContext;

import com.alibaba.fastjson.JSONObject;

import cn.tpp.sparkProject.conf.ConfigurationManager;
import cn.tpp.sparkProject.constant.Constants;
import cn.tpp.sparkProject.dao.ITaskDao;
import cn.tpp.sparkProject.dao.impl.DAOFactory;
import cn.tpp.sparkProject.domain.Task;
import cn.tpp.sparkProject.test.MockData;
import cn.tpp.sparkProject.util.ParamUtils;
import cn.tpp.sparkProject.util.StringUtils;
import cn.tpp.sparkProject.util.ValidUtils;
import scala.Tuple2;

public class UserVisitSessionAnalylizeSpark {
	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setAppName(Constants.SPARK_APP_NAME_SESSION).setMaster("local");
		JavaSparkContext jsc = new JavaSparkContext(conf);		
		SQLContext sqlContext = getSQLContext(jsc.sc());
		args = new String[]{"1"};
		
		//生成模拟用户数据
		MockData(jsc, sqlContext);
		ITaskDao taskDao = DAOFactory.getTaskDAO();
		
		//根据参数获取相应任务条件
		long taskId = ParamUtils.getTaskIdFromAgrs(args);
		Task task = taskDao.findById(taskId);
		
		//查询用户数据，生成RDD
		JSONObject params = JSONObject.parseObject(task.getTaskParam()); 
		JavaRDD<Row> rdd = getActionRddByDateRange(sqlContext,params);
		
		System.out.println(rdd.count());
		
		JavaPairRDD<String, String> infoRdd = aggregateByUserid(rdd,sqlContext);
		
		System.out.println(infoRdd.count());
		for(Tuple2<String, String> tuple2 : infoRdd.take(10)){ 
			System.out.println(tuple2._2);
		}
		
		JavaPairRDD<String, String> filterRdd = filterSession(infoRdd,params);
		
		System.out.println(filterRdd.count());
		for(Tuple2<String, String> tuple2 : filterRdd.take(10)){
			System.out.println(tuple2._1+":"+tuple2._2);
		}
		
		jsc.close();
 	}
	
	private static SQLContext getSQLContext(SparkContext sc){
		boolean local = ConfigurationManager.getBoolean(Constants.SPARK_LOCAL);
		if(local){
			return new SQLContext(sc);
		}else{
			return new HiveContext(sc);
		}			
	}
	
	private static void MockData(JavaSparkContext jsc, SQLContext sc){
		boolean local = ConfigurationManager.getBoolean(Constants.SPARK_LOCAL);
		if(local)
			MockData.mock(jsc, sc);
	}
	
	private static JavaRDD<Row> getActionRddByDateRange(SQLContext sc, JSONObject param){
		String startDate = ParamUtils.getParam(param, Constants.PARAM_START_DATE);
		String endDate = ParamUtils.getParam(param, Constants.PARAM_END_DATE);
		
		String sql="select * "
				+ "from user_visit_action "
				+ "where date>='" + startDate + "' "
				+ "and date<='" + endDate + "'";
		DataFrame dFrame = sc.sql(sql);
		return dFrame.javaRDD();		
	}
	
	private static JavaPairRDD<String, String> aggregateByUserid(JavaRDD<Row> actionRDD, SQLContext sqlContext){
		
		JavaPairRDD<String, Row> session2ActionPairRDD = actionRDD.mapToPair(new PairFunction<Row, String, Row>() {

			private static final long serialVersionUID = 1L;
		
			@Override
			public Tuple2<String, Row> call(Row row) throws Exception {
				return new Tuple2<String, Row>(row.getString(2), row);
			}
		});
				
		JavaPairRDD<String, Iterable<Row>> session2ActionsPairRDD = session2ActionPairRDD.groupByKey();
		
		JavaPairRDD<Long, String> session2PartAggrInfoRDD = session2ActionsPairRDD.mapToPair(new PairFunction<Tuple2<String,Iterable<Row>>, Long, String>() {
			private static final long serialVersionUID = 1L;

			@Override
			public Tuple2<Long, String> call(Tuple2<String, Iterable<Row>> rows) throws Exception {				
				String sessionid = rows._1;
				Iterator<Row> iterator = rows._2.iterator();
				StringBuffer seachkeysBuffer = new StringBuffer("");
				StringBuffer categoryIdsBuffer = new StringBuffer("");
				Long userid = null;
				
				while(iterator.hasNext()){
					Row row = iterator.next();
					String searchKey = row.getString(5);
					Long clickCategoryId = row.getLong(6);
					
					if(userid == null)
						userid = row.getLong(1);
					
					if(StringUtils.isNotEmpty(searchKey)){
						if(!seachkeysBuffer.toString().contains(searchKey)){
							seachkeysBuffer.append(searchKey+",");
						}
					}
					
					if(clickCategoryId != null){
						if(!categoryIdsBuffer.toString().contains(String.valueOf(clickCategoryId))){
							categoryIdsBuffer.append(clickCategoryId+",");
						}
					}			
				}
				
				String seachKeys = StringUtils.trimComma(seachkeysBuffer.toString());
				String clickCategoryIds = StringUtils.trimComma(categoryIdsBuffer.toString());
				
				String partAggrInfo = Constants.FIELD_SESSION_ID+"="+sessionid+"|"
						+Constants.FIELD_SEARCH_KEYWORDS+"="+seachKeys+"|"						
						+Constants.FIELD_CLICK_CATEGORY_IDS+"="+clickCategoryIds;
				
				return new Tuple2<Long, String>(userid, partAggrInfo);
			}		
		});
		
		String sql = "select * from user_info";
		JavaRDD<Row> userInfoRdd = sqlContext.sql(sql).javaRDD();
		
		JavaPairRDD<Long, Row> userid2InfoRdd = userInfoRdd.mapToPair(new PairFunction<Row, Long, Row>() {

			private static final long serialVersionUID = 1L;

			@Override
			public Tuple2<Long, Row> call(Row row) throws Exception {			
				return new Tuple2<Long, Row>(row.getLong(0), row);
			}
		});
		
		JavaPairRDD<Long, Tuple2<String, Row>> infoRDD = session2PartAggrInfoRDD.join(userid2InfoRdd);
		
		JavaPairRDD<String, String> fullInfoRDD = infoRDD.mapToPair(new PairFunction<Tuple2<Long,Tuple2<String,Row>>, String, String>() {
			private static final long serialVersionUID = 1L;

			@Override
			public Tuple2<String, String> call(Tuple2<Long, Tuple2<String, Row>> tuple) throws Exception {
				// TODO 自动生成的方法存根
				String  partAggrInfo = tuple._2._1;
				Row userInfo = tuple._2._2;
				String sessionid = StringUtils.getFieldFromConcatString(partAggrInfo, "\\|", Constants.FIELD_SESSION_ID);
				int age = userInfo.getInt(3);
				String professional = userInfo.getString(4);
				String city = userInfo.getString(5);
				String sex = userInfo.getString(6);
				
				String fullAgrrInfo = partAggrInfo +"|"
						+ Constants.FIELD_AGE + "="+ age+"|"
						+Constants.FIELD_PROFESSIONAL+"="+professional+"|"
						+Constants.FIELD_CITY+"="+city+"|"
						+Constants.FIELD_SEX+"="+sex;
				
				return new Tuple2<String, String>(sessionid, fullAgrrInfo);
			}
		});
		
		return fullInfoRDD;
	}

	private static JavaPairRDD<String, String> filterSession(JavaPairRDD<String, String> session2AggrRDD,
			final JSONObject param){
		
		String startAge = ParamUtils.getParam(param, Constants.PARAM_START_AGE);
		String endAge = ParamUtils.getParam(param, Constants.PARAM_END_AGE);
		String professionals = ParamUtils.getParam(param, Constants.PARAM_PROFESSIONALS);
		String city = ParamUtils.getParam(param, Constants.PARAM_CITIES);
		String sex = ParamUtils.getParam(param, Constants.PARAM_SEX);
		String keyword = ParamUtils.getParam(param, Constants.PARAM_KEYWORDS);
		String categoryids = ParamUtils.getParam(param, Constants.PARAM_CATEGORY_IDS);
		
		String _paramter = (startAge != null ? (Constants.PARAM_START_AGE+"="+startAge+"|"):"")
				+ (endAge != null ? (Constants.PARAM_END_AGE+"="+endAge+"|"):"")
				+ (professionals != null ? (Constants.PARAM_PROFESSIONALS+"="+professionals+"|"):"")
				+ (city != null ? (Constants.PARAM_CITIES+"="+city+"|"):"")
				+ (sex != null ? (Constants.PARAM_SEX+"="+sex+"|"):"")
				+ (keyword != null ? (Constants.PARAM_KEYWORDS+"="+keyword+"|"):"")
				+ (categoryids != null ? (Constants.PARAM_CATEGORY_IDS+"="+categoryids):"");
		
		if(_paramter.endsWith("\\|")){
			_paramter = _paramter.substring(0, _paramter.length()-1);
		}
		
		final String paramter = _paramter;
		
		JavaPairRDD<String, String> filterRDD = session2AggrRDD.filter(new Function<Tuple2<String,String>, Boolean>() {
			
			private static final long serialVersionUID = 1L;

			@Override
			public Boolean call(Tuple2<String, String> tuple) throws Exception {
				// TODO 自动生成的方法存根
				
				String aggrInfo = tuple._2;
				
				if(!ValidUtils.between(aggrInfo, Constants.FIELD_AGE, paramter, Constants.PARAM_START_AGE, Constants.PARAM_END_AGE)){
					return false;
				}
				
				if(!ValidUtils.in(aggrInfo, Constants.FIELD_PROFESSIONAL, paramter, Constants.PARAM_PROFESSIONALS)){
					return false;
				}
				
				if(!ValidUtils.in(aggrInfo, Constants.FIELD_CITY, paramter, Constants.PARAM_CITIES)){
					return false;
				}
				
				if(!ValidUtils.equal(aggrInfo, Constants.FIELD_SEX, paramter, Constants.PARAM_SEX)){
					return false;
				}
				
				if(!ValidUtils.in(aggrInfo, Constants.FIELD_SEARCH_KEYWORDS, paramter, Constants.PARAM_KEYWORDS)){
					return false;
				}
				
				if(!ValidUtils.in(aggrInfo, Constants.FIELD_CLICK_CATEGORY_IDS, paramter, Constants.PARAM_CATEGORY_IDS)){
					return false;
				}
				
				return true;
			}
		});
		
		return filterRDD;
	}
}
