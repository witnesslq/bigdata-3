package com.myhadoop.baseapi.rpc;

public interface LoginServiceInterface {

	static final long versionID = 1L;

	String login(String username, String password);
	

}
