package com.nicia.bocai.dataanalysis.table;

import com.aliyun.odps.Column;
import com.aliyun.odps.OdpsException;
import com.aliyun.odps.OdpsType;
import com.aliyun.odps.TableSchema;
import com.nicia.bocai.dataanalysis.common.CommonUtil;
import com.nicia.bocai.dataanalysis.common.Constants;

public class RegisterLogTable {

	private static final String TABLE_NAME = "register_log";

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
		schema.addColumn(new Column("user_type", OdpsType.BIGINT, "用户类型"));
		schema.addColumn(new Column("platform", OdpsType.BIGINT, "平台"));
		schema.addColumn(new Column("source", OdpsType.STRING, "用户来源"));

		schema.addColumn(new Column("register_type", OdpsType.BIGINT, "1qq 2weixin 3weibo 4mobile"));
		schema.addColumn(new Column("register_time", OdpsType.BIGINT, "注册时间"));
		schema.addColumn(new Column("register_version", OdpsType.BIGINT, "注册版本"));

		schema.addPartitionColumn(new Column("partion_field", OdpsType.STRING, "分区字段"));

		Constants.odps.tables().create(TABLE_NAME, schema, true);
	}

	public static void downloadData(String dateStr) {
		String fileName = "output/" + dateStr + "/registerlog/register_log_";
		CommonUtil.downloadData(TABLE_NAME, fileName, dateStr);
	}
}
