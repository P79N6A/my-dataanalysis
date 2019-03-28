package com.nicia.bocai.dataanalysis.common;

import com.aliyun.odps.Odps;
import com.aliyun.odps.account.Account;
import com.aliyun.odps.account.AliyunAccount;
import com.aliyun.odps.tunnel.TableTunnel;

public class Constants {

	public static final String access_id = "LTAI9eRCDdYtpYZp";
	public static final String access_key = "XVMaI1H4OvUTQavp5fhypV28exzxrJ";

	/* 内网 */
//	 public static final String ossEndpoint =
//	 "http://oss-cn-hangzhou-internal.aliyuncs.com";
//	 public static final String odpsUrl =
//	 "http://odps-ext.aliyun-inc.com/api";
//	 public static final String tunnelUrl =
//	 "http://dt-ext.odps.aliyun-inc.com";
//	 public static final String datahubUrl =
//	 "http://dh-ext.odps.aliyun-inc.com";

	// /* 外网 */
	public static final String ossEndpoint = "http://oss-cn-hangzhou.aliyuncs.com";
	public static final String odpsUrl = "http://service.odps.aliyun.com/api";
	public static final String tunnelUrl = "http://dt.odps.aliyun.com";
	public static final String datahubUrl = "http://dh.odps.aliyun.com";

	 public static final String hadoop_project = "paopao_maxcompute";
//	public static final String hadoop_project = "bocaiapp_hadoop_test";
	public static final String oss_bucket = "paopao-new";

	// public static final String hadoop_project = "bocaiapp_hadoop_test";

	public static Odps odps;
	public static TableTunnel tunnel;

	static {
		Account account = new AliyunAccount(access_id, access_key);
		odps = new Odps(account);
		odps.setEndpoint(odpsUrl);
		odps.setDefaultProject(hadoop_project);
		tunnel = new TableTunnel(Constants.odps);
		tunnel.setEndpoint(Constants.tunnelUrl);
	}

}
