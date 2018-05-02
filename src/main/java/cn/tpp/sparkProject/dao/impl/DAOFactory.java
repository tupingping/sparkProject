package cn.tpp.sparkProject.dao.impl;

import cn.tpp.sparkProject.dao.ISessionAggrStatDAO;
import cn.tpp.sparkProject.dao.ITaskDao;

public class DAOFactory {
	public static ITaskDao getTaskDAO(){
		return new TaskDAOImpl();
	}
	
	public static ISessionAggrStatDAO getSessionAggrStatDAO(){
		return new SessionAggrStatDAOImpl();
	}
}
