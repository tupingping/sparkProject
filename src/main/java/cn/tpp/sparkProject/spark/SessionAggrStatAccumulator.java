package cn.tpp.sparkProject.spark;

import org.apache.spark.AccumulatorParam;

import cn.tpp.sparkProject.constant.Constants;
import cn.tpp.sparkProject.util.StringUtils;

public class SessionAggrStatAccumulator implements AccumulatorParam<String>{

	private static final long serialVersionUID = 6215189869403431982L;

	@Override
	public String zero(String arg0) {
		// TODO 自动生成的方法存根
		return Constants.SESSION_COUNT + "=0|"
		+ Constants.TIME_PERIOD_1s_3s + "=0|"
		+ Constants.TIME_PERIOD_4s_6s + "=0|"
		+ Constants.TIME_PERIOD_7s_9s + "=0|"
		+ Constants.TIME_PERIOD_10s_30s + "=0|"
		+ Constants.TIME_PERIOD_30s_60s + "=0|"
		+ Constants.TIME_PERIOD_1m_3m + "=0|"
		+ Constants.TIME_PERIOD_3m_10m + "=0|"
		+ Constants.TIME_PERIOD_10m_30m + "=0|"
		+ Constants.TIME_PERIOD_30m + "=0|"
		+ Constants.STEP_PERIOD_1_3 + "=0|"
		+ Constants.STEP_PERIOD_4_6 + "=0|"
		+ Constants.STEP_PERIOD_7_9 + "=0|"
		+ Constants.STEP_PERIOD_10_30 + "=0|"
		+ Constants.STEP_PERIOD_30_60 + "=0|"
		+ Constants.STEP_PERIOD_60 + "=0";
	}

	@Override
	public String addInPlace(String arg0, String arg1) {
		return add(arg0,arg1);
	}
	
	@Override
	public String addAccumulator(String arg0, String arg1) {
		return add(arg0,arg1);
	}

	private String add(String arg0, String arg1){
		// 校验：v1为空的话，直接返回v2
		if(StringUtils.isEmpty(arg0)) {
			return arg1;
		}
		
		// 使用StringUtils工具类，从v1中，提取v2对应的值，并累加1
		String oldValue = StringUtils.getFieldFromConcatString(arg0, "\\|", arg1);
		if(oldValue != null) {
			// 将范围区间原有的值，累加1
			int newValue = Integer.valueOf(oldValue) + 1;
			// 使用StringUtils工具类，将v1中，v2对应的值，设置成新的累加后的值
			return StringUtils.setFieldInConcatString(arg0, "\\|", arg1, String.valueOf(newValue));  
		}
		
		return arg0;
	}
}
