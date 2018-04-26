package cn.tpp.sparkProject.util;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;

public class ParamUtils {
	public static Long getTaskIdFromAgrs(String[] agrs){
		
		try {
			if(agrs != null && agrs.length > 0){
				return Long.valueOf(agrs[0]);
			}
			
		} catch (Exception e) {
			// TODO: handle exception	
			e.printStackTrace();
		}
		
		return null;
	}
	
	public static String getParam(JSONObject jsonObject, String field) {
		JSONArray jsonArray = jsonObject.getJSONArray(field);
		if(jsonArray != null && jsonArray.size() > 0) {
			return jsonArray.getString(0);
		}
		return null;
	}
}
