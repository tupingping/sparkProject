package cn.tpp.sparkProject.dao.impl;

import cn.tpp.sparkProject.dao.ISessionAggrStatDAO;
import cn.tpp.sparkProject.domain.SessionAggrStat;
import cn.tpp.sparkProject.jdbc.JdbcHelper;

public class SessionAggrStatDAOImpl implements ISessionAggrStatDAO{

	@Override
	public void insert(SessionAggrStat sessionAggrStat) {
		// TODO 自动生成的方法存根
		String sql = "insert into session_aggr_stat "
				+ "values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"; 
		
		Object[] params = new Object[]{sessionAggrStat.getTaskid(),
				sessionAggrStat.getSession_count(),
				sessionAggrStat.getVisit_length_1s_3s_ratio(),
				sessionAggrStat.getVisit_length_4s_6s_ratio(),
				sessionAggrStat.getVisit_length_7s_9s_ratio(),
				sessionAggrStat.getVisit_length_10s_30s_ratio(),
				sessionAggrStat.getVisit_length_30s_60s_ratio(),
				sessionAggrStat.getVisit_length_1m_3m_ratio(),
				sessionAggrStat.getVisit_length_3m_10m_ratio(),
				sessionAggrStat.getVisit_length_10m_30m_ratio(),
				sessionAggrStat.getVisit_length_30m_ratio(),
				sessionAggrStat.getStep_length_1_3_ratio(),
				sessionAggrStat.getStep_length_4_6_ratio(),
				sessionAggrStat.getStep_length_7_9_ratio(),
				sessionAggrStat.getStep_length_10_30_ratio(),
				sessionAggrStat.getStep_length_30_60_ratio(),
				sessionAggrStat.getStep_length_60_ratio()};
		
		JdbcHelper jdbcHelper = JdbcHelper.getInstance();
		jdbcHelper.executeUpdate(sql, params);
	}

}
