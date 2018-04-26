package cn.tpp.sparkProject.constant;


public interface Constants {
	/**
	 * 项目配置相关的常量
	 */
	String JDBC_DRIVER="jdbc.driver.name"; 
	String JDBC_DATASOURCE_SIZE="jdbc.source.size";
	String JDBC_URL="jdbc.url";
	String JDBC_USER="jdbc.user";
	String JDBC_PASSWORD="jdbc.password";
	String SPARK_LOCAL="spark.local";
	
	/**
	 * spark 作业相关的常量
	 */
	String SPARK_APP_NAME_SESSION="UserVisitSessionAnalylizeSpark";
	String FIELD_SESSION_ID="sessionid";
	String FIELD_SEARCH_KEYWORDS="searchKeyWords";
	String FIELD_CLICK_CATEGORY_IDS="clickCategoryIds";
	String FIELD_AGE="age";
	String FIELD_PROFESSIONAL="professional";
	String FIELD_CITY="city";
	String FIELD_SEX="sex";
	
	/**
	 * 任务相关的常量
	 */
	String PARAM_START_DATE="startDate";
	String PARAM_END_DATE="endDate";
	String PARAM_START_AGE="startAge";
	String PARAM_END_AGE="endAge";
	String PARAM_PROFESSIONALS="professionals";
	String PARAM_CITIES="cities";
	String PARAM_SEX="sex";
	String PARAM_KEYWORDS="keyWords";
	String PARAM_CATEGORY_IDS="categoryIds";
	
}
