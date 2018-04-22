package cn.tpp.sparkProject.test;

import cn.tpp.sparkProject.conf.ConfigurationManager;

/**
 * 配置管理组件测试类
 * @author tpp
 *
 */
public class CofigurationManagerTest {
	public static void main(String[] args) {
		System.out.println(ConfigurationManager.getProperty("tpp"));
		System.out.println(ConfigurationManager.getProperty("ccm"));
		System.out.println(ConfigurationManager.getProperty("ttt"));
		System.out.println(ConfigurationManager.getProperty("ttti"));
	}
}
