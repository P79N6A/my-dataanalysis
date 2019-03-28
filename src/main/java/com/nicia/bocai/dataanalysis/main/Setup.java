package com.nicia.bocai.dataanalysis.main;

import com.aliyun.odps.OdpsException;
import com.nicia.bocai.dataanalysis.common.CommonUtil;
import com.nicia.bocai.dataanalysis.table.LoginLogTable;
import com.nicia.bocai.dataanalysis.table.RegisterLogTable;

public class Setup {
	public static void main(String[] args) throws OdpsException {
		String dateStr = CommonUtil.getDateStr();
		boolean delete = false;

		System.out.println("Setup Function Begin!");
//		LoginLogTable.createTable(delete);
//		System.out.println("login_log table created");
		RegisterLogTable.createTable(delete);
		System.out.println("register_log table created");

		System.out.println("Setup Function End!");
	}
}
