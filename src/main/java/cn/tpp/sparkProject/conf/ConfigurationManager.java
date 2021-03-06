package cn.tpp.sparkProject.conf;

import java.io.InputStream;
import java.util.Properties;

import scala.xml.dtd.PublicID;

public class ConfigurationManager {
	private static Properties prop = new Properties();
	
	static{	
		try {
			
			InputStream inputStream = ConfigurationManager.class.getClassLoader().getResourceAsStream("my.Properties");
			prop.load(inputStream);
			
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	public static String getProperty(String key){
		return prop.getProperty(key);
	}
	
	public static Integer getInteger(String key){
		String value = getProperty(key);
		try {
			return Integer.valueOf(value);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return 0;
	}
	
	public static Boolean getBoolean(String key){
		String value = getProperty(key);
		try {
			return Boolean.valueOf(value);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return false;
	}
}
