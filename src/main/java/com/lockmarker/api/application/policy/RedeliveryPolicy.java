package com.lockmarker.api.application.policy;

/**
 * This class implements the policy for message redelivery 
 * when the message cannot be delivered at the first time
 */
public class RedeliveryPolicy {
	private int INIT_RETRY_NUM = 3;
	private int MAX_RETRY_NUM = 30;
	private int INIT_RETRY_INTERVAL = 3000;

	public RedeliveryPolicy() {
	}
	
	public void setInitialRetryNum(int n) {
		INIT_RETRY_NUM = n;
	}
	
	public int getInitialRetryNum() {
		return INIT_RETRY_NUM;
	}
	
	public void setMaxRetryNum(int n) {
		MAX_RETRY_NUM = n;
	}
	
	public int getMaxRetryNum() {
		return MAX_RETRY_NUM;
	}
	
	public void setInitialRetryInterval(int n) {
		INIT_RETRY_INTERVAL = n;
	}
	
	public int getInitialRetryInterval() {
		return INIT_RETRY_INTERVAL;
	}
}
