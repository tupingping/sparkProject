package cn.tpp.sparkProject.dao.impl;

import java.sql.ResultSet;

import cn.tpp.sparkProject.dao.ITaskDao;
import cn.tpp.sparkProject.domain.Task;
import cn.tpp.sparkProject.jdbc.JdbcHelper;

public class TaskDAOImpl implements ITaskDao{

	@Override
	public Task findById(long taskId) {
		
		final Task task = new Task();
		String sql = "select * from task where task_id=?";
		Object[] params = new Object[]{taskId};
		
		JdbcHelper jdbcHelper = JdbcHelper.getInstance();
		jdbcHelper.executeQuery(sql, params, new JdbcHelper.QueryCallback() {
			
			@Override
			public void process(ResultSet rs) throws Exception {
				// TODO 自动生成的方法存根
				if(rs.next()){
					long taskId = rs.getLong(1);				
					String taskName = rs.getString(2);
					String createTime = rs.getString(3);
					String startTime = rs.getString(4);
					String finishTime = rs.getString(5);
					String taskType = rs.getString(6);
					String taskStatue = rs.getString(7);
					String taskParam = rs.getString(8);
					
					task.setTaskId(taskId);
					task.setTaskName(taskName);
					task.setCreateTime(createTime);
					task.setStartTime(startTime);
					task.setFinishTime(finishTime);
					task.setTaskType(taskType);
					task.setTaskStatue(taskStatue);
					task.setTaskParam(taskParam);					
				}				
			}
		});
		

		
		return task;
	}
	
}
