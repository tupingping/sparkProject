package cn.tpp.sparkProject.spark;

import org.apache.spark.AccumulatorParam;

import cn.tpp.sparkProject.constant.Constants;

public class SessionAggrStatAccumulator implements AccumulatorParam<String>{

	private static final long serialVersionUID = 6215189869403431982L;

	@Override
	public String addInPlace(String arg0, String arg1) {
		// TODO 自动生成的方法存根
		return null;
	}

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
	public String addAccumulator(String arg0, String arg1) {
		// TODO 自动生成的方法存根
		return null;
	}

	
}
