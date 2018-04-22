package cn.tpp.sparkProject.test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;



public class JdbcCURD {
	
	public static void main(String[] args) {
		//insert();
		//update();
		//select();
		update();
	}
	
	private static Boolean insert() {
		Connection connection = null;
		Statement statment = null;
		
		try {
			
			Class.forName("com.mysql.jdbc.Driver");		
			connection = DriverManager.getConnection("jdbc:mysql://localhost:3306/spark_project","root","root");
			statment  = connection.createStatement();
			String sql = "insert into test_user(name,age) values('tpp',28)";
			statment.executeUpdate(sql);
			
		} catch (Exception e) {
			e.printStackTrace();
			return false;
		}finally {		
			try {
				
				if(statment != null)
					statment.close();
				
				if(connection != null)
					connection.close();
				
			} catch (SQLException e) {
				e.printStackTrace();
			}			
		}		
		
		return true;
	}
	
	//PreparedStatement 防止sql注入及提高执行性能
	private static Boolean update() {
		Connection connection = null;
		PreparedStatement statment = null;
		
		try {
			
			Class.forName("com.mysql.jdbc.Driver");		
			connection = DriverManager.getConnection("jdbc:mysql://localhost:3306/spark_project","root","root");
			String sql = "update test_user set age=? where name=?";		
			statment = connection.prepareStatement(sql);
			statment.setString(1, "30");
			statment.setString(2, "tpp");
			statment.execute();
			
		} catch (Exception e) {
			e.printStackTrace();
			return false;
		}finally {		
			try {
				
				if(statment != null)
					statment.close();
				
				if(connection != null)
					connection.close();
				
			} catch (SQLException e) {
				e.printStackTrace();
			}			
		}
		
		
		return true;
	}
	
	private static Boolean select() {
		Connection connection = null;
		Statement statment = null;
		ResultSet rs = null;
		
		try {
			
			Class.forName("com.mysql.jdbc.Driver");		
			connection = DriverManager.getConnection("jdbc:mysql://localhost:3306/spark_project","root","root");
			statment  = connection.createStatement();
			String sql = "select * from test_user";
			rs = statment.executeQuery(sql);
			while(rs.next()){
				int id = rs.getInt(1);
				String name = rs.getString(2);
				int age = rs.getInt(3);
				System.out.println("id="+id+", name="+name+", age="+age);
			}
						
		} catch (Exception e) {
			e.printStackTrace();
			return false;
		}finally {		
			try {
				
				if(statment != null)
					statment.close();
				
				if(connection != null)
					connection.close();
				
			} catch (SQLException e) {
				e.printStackTrace();
			}			
		}
		
		
		return true;
	}
}
