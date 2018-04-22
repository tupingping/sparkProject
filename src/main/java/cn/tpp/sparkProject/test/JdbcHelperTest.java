package cn.tpp.sparkProject.test;

import java.sql.ResultSet;
import cn.tpp.sparkProject.jdbc.JdbcHelper;

public class JdbcHelperTest {
	public static void main(String[] args) {
		JdbcHelper jdbcHelper = JdbcHelper.getInstance();
		jdbcHelper.executeUpdate("insert into test_user(name,age) values(?,?)", new Object[]{"ccm", 25});
		/*jdbcHelper.executeQuery("select * from test_user where name=?", new Object[]{"tpp"}, new JdbcHelper.QueryCallback() {
			@Override
			public void process(ResultSet rs) throws Exception {
				while(rs.next()){
					String id=rs.getString(1);
					String name=rs.getString(2);
					int age=rs.getInt(3);
					System.out.println("id="+id+",name="+name+",age="+age);
				}				
			}
		});*/	
	}
}
