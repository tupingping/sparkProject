package cn.tpp.sparkProject.dao;

import cn.tpp.sparkProject.domain.Task;

public interface ITaskDao {
	Task findById(Long id);
}
