package com.nicia.bocai.dataanalysis.table;

import com.aliyun.odps.Column;
import com.aliyun.odps.OdpsException;
import com.aliyun.odps.OdpsType;
import com.aliyun.odps.TableSchema;
import com.nicia.bocai.dataanalysis.common.CommonUtil;
import com.nicia.bocai.dataanalysis.common.Constants;

public class LoginLogTable {

	private static final String TABLE_NAME = "new_login_log";

	public static void createTable(boolean delete) throws OdpsException {
		if (delete) {
			try {
				Constants.odps.tables().delete(TABLE_NAME, true);
			} catch (Exception e) {
			}
		}

		TableSchema schema = new TableSchema();
		schema.addColumn(new Column("log_date", OdpsType.STRING, ""));
		schema.addColumn(new Column("user_id", OdpsType.BIGINT, "用户ID"));
		schema.addColumn(new Column("platform", OdpsType.BIGINT, "平台"));
		schema.addColumn(new Column("user_type", OdpsType.BIGINT, "用户类型"));
		schema.addColumn(new Column("source", OdpsType.STRING, "用户来源"));

		schema.addColumn(new Column("times", OdpsType.BIGINT, "登陆次数"));
		schema.addColumn(new Column("login_time", OdpsType.BIGINT, "本日第一次登陆时间"));
		schema.addColumn(new Column("logout_time", OdpsType.BIGINT, "本日最后一次登出时间"));
		schema.addColumn(new Column("online_time", OdpsType.BIGINT, "在线时长"));
		schema.addColumn(new Column("live_time", OdpsType.BIGINT, "直播/观看时长"));

		schema.addPartitionColumn(new Column("partion_field", OdpsType.STRING, "分区字段"));

		Constants.odps.tables().create(TABLE_NAME, schema, true);
	}

	public static void downloadData(String dateStr) {
		String fileName = "output/" + dateStr + "/loginlog/login_log_";
		CommonUtil.downloadData(TABLE_NAME, fileName, dateStr);
	}
}
