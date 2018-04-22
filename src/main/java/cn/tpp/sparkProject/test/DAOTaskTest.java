package cn.tpp.sparkProject.test;

import cn.tpp.sparkProject.dao.ITaskDao;
import cn.tpp.sparkProject.dao.impl.DAOFactory;
import cn.tpp.sparkProject.domain.Task;

public class DAOTaskTest {
	public static void main(String[] args) {
		ITaskDao taskDao = DAOFactory.getTaskDAO();
		Task task = taskDao.findById(2);
		System.out.println(task.getTaskName());
	}
}
