package cn.tpp.sparkProject.spark;

import java.util.Date;
import java.util.Iterator;

import org.apache.spark.Accumulator;
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
import org.datanucleus.store.rdbms.mapping.StatementMappingIndex;

import com.alibaba.fastjson.JSONObject;

import cn.tpp.sparkProject.conf.ConfigurationManager;
import cn.tpp.sparkProject.constant.Constants;
import cn.tpp.sparkProject.dao.ISessionAggrStatDAO;
import cn.tpp.sparkProject.dao.ITaskDao;
import cn.tpp.sparkProject.dao.impl.DAOFactory;
import cn.tpp.sparkProject.domain.SessionAggrStat;
import cn.tpp.sparkProject.domain.Task;
import cn.tpp.sparkProject.test.MockData;
import cn.tpp.sparkProject.util.DateUtils;
import cn.tpp.sparkProject.util.NumberUtils;
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
		
		Accumulator<String> sessionAggrStatAccumulator = jsc.accumulator( "", new SessionAggrStatAccumulator());
		
		JavaPairRDD<String, String> filterRdd = filterSession(infoRdd,params,sessionAggrStatAccumulator);
		
		System.out.println(filterRdd.count());
		
		for(Tuple2<String, String> tuple2 : filterRdd.take(10)){
			System.out.println(tuple2._1+":"+tuple2._2);
		}
		
		filterRdd.count();
		
		calculateAndPersistAggrStat(sessionAggrStatAccumulator.value(), task.getTaskId());
		
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
				Date startTime = null;
				Date endTime = null;
				int stepLegth = 0;
				
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
					
					Date actionTime = DateUtils.parseTime(row.getString(4));
					
					if(startTime == null)
						startTime = actionTime;
					
					if(endTime == null)
						endTime = actionTime;
					
					if(actionTime.before(startTime))
						startTime = actionTime;
					
					if(actionTime.after(endTime))
						endTime = actionTime;
					
					stepLegth++;
				}
				
				String seachKeys = StringUtils.trimComma(seachkeysBuffer.toString());
				String clickCategoryIds = StringUtils.trimComma(categoryIdsBuffer.toString());
				long visitLength = (endTime.getTime()-startTime.getTime())/1000;
				
				String partAggrInfo = Constants.FIELD_SESSION_ID+"="+sessionid+"|"
						+Constants.FIELD_SEARCH_KEYWORDS+"="+seachKeys+"|"						
						+Constants.FIELD_CLICK_CATEGORY_IDS+"="+clickCategoryIds+"|"
						+Constants.FIELD_VISIT_LENGTH+"="+visitLength+"|"
						+Constants.FIELD_STEP_LENGTH+"="+stepLegth;
				
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
			final JSONObject param,final Accumulator<String> sessionAggrStatAccumulator){
		
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
				
				// 主要走到这一步，那么就是需要计数的session
				sessionAggrStatAccumulator.add(Constants.SESSION_COUNT);  
				
				// 计算出session的访问时长和访问步长的范围，并进行相应的累加
				long visitLength = Long.valueOf(StringUtils.getFieldFromConcatString(
						aggrInfo, "\\|", Constants.FIELD_VISIT_LENGTH)); 
				long stepLength = Long.valueOf(StringUtils.getFieldFromConcatString(
						aggrInfo, "\\|", Constants.FIELD_STEP_LENGTH));  
				calculateVisitLength(visitLength); 
				calculateStepLength(stepLength); 
				
				return true;
			}
			
			/**
			 * 计算访问时长范围
			 * @param visitLength
			 */
			private void calculateVisitLength(long visitLength) {
				if(visitLength >=1 && visitLength <= 3) {
					sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_1s_3s);  
				} else if(visitLength >=4 && visitLength <= 6) {
					sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_4s_6s);  
				} else if(visitLength >=7 && visitLength <= 9) {
					sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_7s_9s);  
				} else if(visitLength >=10 && visitLength <= 30) {
					sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_10s_30s);  
				} else if(visitLength > 30 && visitLength <= 60) {
					sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_30s_60s);  
				} else if(visitLength > 60 && visitLength <= 180) {
					sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_1m_3m);  
				} else if(visitLength > 180 && visitLength <= 600) {
					sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_3m_10m);  
				} else if(visitLength > 600 && visitLength <= 1800) {  
					sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_10m_30m);  
				} else if(visitLength > 1800) {
					sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_30m);  
				} 
			}
			
			/**
			 * 计算访问步长范围
			 * @param stepLength
			 */
			private void calculateStepLength(long stepLength) {
				if(stepLength >= 1 && stepLength <= 3) {
					sessionAggrStatAccumulator.add(Constants.STEP_PERIOD_1_3);  
				} else if(stepLength >= 4 && stepLength <= 6) {
					sessionAggrStatAccumulator.add(Constants.STEP_PERIOD_4_6);  
				} else if(stepLength >= 7 && stepLength <= 9) {
					sessionAggrStatAccumulator.add(Constants.STEP_PERIOD_7_9);  
				} else if(stepLength >= 10 && stepLength <= 30) {
					sessionAggrStatAccumulator.add(Constants.STEP_PERIOD_10_30);  
				} else if(stepLength > 30 && stepLength <= 60) {
					sessionAggrStatAccumulator.add(Constants.STEP_PERIOD_30_60);  
				} else if(stepLength > 60) {
					sessionAggrStatAccumulator.add(Constants.STEP_PERIOD_60);    
				}
			}
		});
		
		return filterRDD;
	}
	
	/**
	 * 计算各session范围占比，并写入MySQL
	 * @param value
	 */
	private static void calculateAndPersistAggrStat(String value, long taskid) {
		// 从Accumulator统计串中获取值
		long session_count = Long.valueOf(StringUtils.getFieldFromConcatString(
				value, "\\|", Constants.SESSION_COUNT));  
		
		long visit_length_1s_3s = Long.valueOf(StringUtils.getFieldFromConcatString(
				value, "\\|", Constants.TIME_PERIOD_1s_3s));  
		long visit_length_4s_6s = Long.valueOf(StringUtils.getFieldFromConcatString(
				value, "\\|", Constants.TIME_PERIOD_4s_6s));
		long visit_length_7s_9s = Long.valueOf(StringUtils.getFieldFromConcatString(
				value, "\\|", Constants.TIME_PERIOD_7s_9s));
		long visit_length_10s_30s = Long.valueOf(StringUtils.getFieldFromConcatString(
				value, "\\|", Constants.TIME_PERIOD_10s_30s));
		long visit_length_30s_60s = Long.valueOf(StringUtils.getFieldFromConcatString(
				value, "\\|", Constants.TIME_PERIOD_30s_60s));
		long visit_length_1m_3m = Long.valueOf(StringUtils.getFieldFromConcatString(
				value, "\\|", Constants.TIME_PERIOD_1m_3m));
		long visit_length_3m_10m = Long.valueOf(StringUtils.getFieldFromConcatString(
				value, "\\|", Constants.TIME_PERIOD_3m_10m));
		long visit_length_10m_30m = Long.valueOf(StringUtils.getFieldFromConcatString(
				value, "\\|", Constants.TIME_PERIOD_10m_30m));
		long visit_length_30m = Long.valueOf(StringUtils.getFieldFromConcatString(
				value, "\\|", Constants.TIME_PERIOD_30m));
		
		  
		long step_length_1_3 = Long.valueOf(StringUtils.getFieldFromConcatString(
				value, "\\|", Constants.STEP_PERIOD_1_3));
		long step_length_4_6 = Long.valueOf(StringUtils.getFieldFromConcatString(
				value, "\\|", Constants.STEP_PERIOD_4_6));
		long step_length_7_9 = Long.valueOf(StringUtils.getFieldFromConcatString(
				value, "\\|", Constants.STEP_PERIOD_7_9));
		long step_length_10_30 = Long.valueOf(StringUtils.getFieldFromConcatString(
				value, "\\|", Constants.STEP_PERIOD_10_30));
		long step_length_30_60 = Long.valueOf(StringUtils.getFieldFromConcatString(
				value, "\\|", Constants.STEP_PERIOD_30_60));
		long step_length_60 = Long.valueOf(StringUtils.getFieldFromConcatString(
				value, "\\|", Constants.STEP_PERIOD_60));
		
		// 计算各个访问时长和访问步长的范围
		double visit_length_1s_3s_ratio = NumberUtils.formatDouble(
				(double)visit_length_1s_3s / (double)session_count, 2);  
		double visit_length_4s_6s_ratio = NumberUtils.formatDouble(
				(double)visit_length_4s_6s / (double)session_count, 2);  
		double visit_length_7s_9s_ratio = NumberUtils.formatDouble(
				(double)visit_length_7s_9s / (double)session_count, 2);  
		double visit_length_10s_30s_ratio = NumberUtils.formatDouble(
				(double)visit_length_10s_30s / (double)session_count, 2);  
		double visit_length_30s_60s_ratio = NumberUtils.formatDouble(
				(double)visit_length_30s_60s / (double)session_count, 2);  
		double visit_length_1m_3m_ratio = NumberUtils.formatDouble(
				(double)visit_length_1m_3m / (double)session_count, 2);
		double visit_length_3m_10m_ratio = NumberUtils.formatDouble(
				(double)visit_length_3m_10m / (double)session_count, 2);  
		double visit_length_10m_30m_ratio = NumberUtils.formatDouble(
				(double)visit_length_10m_30m / (double)session_count, 2);
		double visit_length_30m_ratio = NumberUtils.formatDouble(
				(double)visit_length_30m / (double)session_count, 2);  
		
		double step_length_1_3_ratio = NumberUtils.formatDouble(
				(double)step_length_1_3 / (double)session_count, 2);  
		double step_length_4_6_ratio = NumberUtils.formatDouble(
				(double)step_length_4_6 / (double)session_count, 2);  
		double step_length_7_9_ratio = NumberUtils.formatDouble(
				(double)step_length_7_9 / (double)session_count, 2);  
		double step_length_10_30_ratio = NumberUtils.formatDouble(
				(double)step_length_10_30 / (double)session_count, 2);  
		double step_length_30_60_ratio = NumberUtils.formatDouble(
				(double)step_length_30_60 / (double)session_count, 2);  
		double step_length_60_ratio = NumberUtils.formatDouble(
				(double)step_length_60 / (double)session_count, 2);  
		
		// 将统计结果封装为Domain对象
		SessionAggrStat sessionAggrStat = new SessionAggrStat();
		sessionAggrStat.setTaskid(taskid);
		sessionAggrStat.setSession_count(session_count);  
		sessionAggrStat.setVisit_length_1s_3s_ratio(visit_length_1s_3s_ratio);  
		sessionAggrStat.setVisit_length_4s_6s_ratio(visit_length_4s_6s_ratio);  
		sessionAggrStat.setVisit_length_7s_9s_ratio(visit_length_7s_9s_ratio);  
		sessionAggrStat.setVisit_length_10s_30s_ratio(visit_length_10s_30s_ratio);  
		sessionAggrStat.setVisit_length_30s_60s_ratio(visit_length_30s_60s_ratio);  
		sessionAggrStat.setVisit_length_1m_3m_ratio(visit_length_1m_3m_ratio); 
		sessionAggrStat.setVisit_length_3m_10m_ratio(visit_length_3m_10m_ratio);  
		sessionAggrStat.setVisit_length_10m_30m_ratio(visit_length_10m_30m_ratio); 
		sessionAggrStat.setVisit_length_30m_ratio(visit_length_30m_ratio);  
		sessionAggrStat.setStep_length_1_3_ratio(step_length_1_3_ratio);  
		sessionAggrStat.setStep_length_4_6_ratio(step_length_4_6_ratio);  
		sessionAggrStat.setStep_length_7_9_ratio(step_length_7_9_ratio);  
		sessionAggrStat.setStep_length_10_30_ratio(step_length_10_30_ratio);  
		sessionAggrStat.setStep_length_30_60_ratio(step_length_30_60_ratio);  
		sessionAggrStat.setStep_length_60_ratio(step_length_60_ratio);  
		
		// 调用对应的DAO插入统计结果
		ISessionAggrStatDAO sessionAggrStatDAO = DAOFactory.getSessionAggrStatDAO();
		sessionAggrStatDAO.insert(sessionAggrStat);  
	}

	private static void getTop10Category(JavaPairRDD<String, String> filteredSession2AggrInfoRDD, JavaPairRDD<String, Row> session2actionRDD){
		
		JavaPairRDD<String, Row> session2detailRDD = filteredSession2AggrInfoRDD
				.join(session2actionRDD)
				.mapToPair(new PairFunction<Tuple2<String,Tuple2<String,Row>>, String, Row>() {

					private static final long serialVersionUID = 1L;

					@Override
					public Tuple2<String, Row> call(Tuple2<String, Tuple2<String, Row>> t) throws Exception {
						// TODO 自动生成的方法存根
						
						return new Tuple2<String, Row>(t._1, t._2._2);
					}
				});
		
		
		
	}
}
