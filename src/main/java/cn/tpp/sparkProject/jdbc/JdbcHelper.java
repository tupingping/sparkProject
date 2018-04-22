package cn.tpp.sparkProject.jdbc;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Iterator;
import java.util.LinkedList;

import antlr.collections.List;
import cn.tpp.sparkProject.conf.ConfigurationManager;
import cn.tpp.sparkProject.constant.Constants;

public class JdbcHelper {
	static{
		try {
			
			Class.forName(ConfigurationManager.getProperty(Constants.JDBC_DRIVER));
			
		} catch (Exception e) {
			// TODO: handle exception
		}
	}
	
	private static JdbcHelper instance = null;
	private LinkedList<Connection> datasource = new LinkedList<Connection>();
	
	private JdbcHelper(){
		
		Integer dataSourceSize = ConfigurationManager.getInteger(Constants.JDBC_DATASOURCE_SIZE);
		String url = ConfigurationManager.getProperty(Constants.JDBC_URL);
		String user = ConfigurationManager.getProperty(Constants.JDBC_USER);
		String password = ConfigurationManager.getProperty(Constants.JDBC_PASSWORD);
		
		try {
			
			for (int i = 0; i < dataSourceSize; i++) {
				Connection connection = DriverManager.getConnection(url,user,password);
				datasource.push(connection);
			}
			
		} catch (Exception e) {
			// TODO: handle exception
		}		
	}
	
	/**
	 * 获取数据库连接
	 * @return
	 */
	public Connection getConnection() {
		while(datasource.size() == 0){			
			try {
				Thread.sleep(10);
			} catch (InterruptedException e) {
				// TODO 自动生成的 catch 块
				e.printStackTrace();
			}
		}
		
		return datasource.poll();
	}
	
	/**
	 * 获取单例对象
	 * @return
	 */
	public synchronized static JdbcHelper getInstance(){
		if(instance == null){
			synchronized (JdbcHelper.class) {
				if(instance == null){
					instance = new JdbcHelper();
				}
			}
		}
				
		return instance;
	}
	
	/**
	 * 执行增删改操作
	 * @param sql
	 * @param params
	 * @return 
	 */
	public int executeUpdate(String sql, Object[] params){
		int rtn=0;
		Connection connection = getConnection();
		try {
			
			PreparedStatement preparedStatement = connection.prepareStatement(sql);
			for (int i = 0; i < params.length; i++) {
				preparedStatement.setObject(i+1, params[i]);
			}
			rtn = preparedStatement.executeUpdate();
		} catch (SQLException e) {
			// TODO 自动生成的 catch 块
			e.printStackTrace();
		}finally {
			if(connection != null){
				datasource.push(connection);
			}
		}
			
		return rtn;
	}
	
	/**
	 * 执行查询操作
	 * @param sql
	 * @param params
	 * @param callback
	 * @return
	 */
	public int executeQuery(String sql, Object[] params, QueryCallback callback){
		int res = 0;
		Connection connection = null;
		PreparedStatement preparedStatement = null;
		ResultSet rSet = null;
		
		try {		
			connection = getConnection();
			preparedStatement = connection.prepareStatement(sql);
			for (int i = 0; i < params.length; i++) {
				preparedStatement.setObject(i+1, params[i]);
			}
			
			rSet = preparedStatement.executeQuery();
			callback.process(rSet);
			
		} catch (Exception e) {
			// TODO: handle exception
			e.printStackTrace();
		}finally {
			if(connection != null){
				datasource.push(connection);
			}
		}
		
		return res;
	}
	
	public static interface QueryCallback{
		public void process(ResultSet rs) throws Exception;
	}
	
	/**
	 * 执行批量查询
	 * @param sql
	 * @param paramsList
	 * @return
	 */
	public int[] executeBatchQuery(String sql, LinkedList<Object[]> paramsList){
		int[] rst = null;
		Connection connection = null;
		PreparedStatement preparedStatement = null;
		
		try {
			connection = getConnection();
			connection.setAutoCommit(false);
			preparedStatement = connection.prepareStatement(sql);
			
			for (Iterator iterator = paramsList.iterator(); iterator.hasNext();) {
				Object[] objects = (Object[]) iterator.next();
				for (int i = 0; i < objects.length; i++) {
					preparedStatement.setObject(i+1, objects[i]);
				}
				
				preparedStatement.addBatch();			
			}
			
			rst = preparedStatement.executeBatch();
			
			connection.commit();
			
		} catch (Exception e) {
			e.printStackTrace();
		}finally {
			if(connection != null){
				datasource.push(connection);
			}
		}
				
		return rst;
	}
	
}
