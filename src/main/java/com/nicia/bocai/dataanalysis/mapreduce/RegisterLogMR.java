package com.nicia.bocai.dataanalysis.mapreduce;

import com.aliyun.odps.data.Record;
import com.aliyun.odps.mapred.MapperBase;
import com.aliyun.odps.mapred.ReducerBase;
import com.nicia.bocai.dataanalysis.common.CommonUtil;

import java.io.IOException;
import java.util.Iterator;

public class RegisterLogMR {


    private static final int MEANINGLESS = -10;

    public static class TokenizerMapper extends MapperBase {


        private Record __valueOut;

        @Override
        public void setup(TaskContext context) throws IOException {
            __valueOut = context.createOutputRecord();
        }

        @Override
        public void map(long recordNum, Record __record, TaskContext context) throws IOException {

            String command = (String) CommonUtil.getValueIfNull(__record.get("command"), null);
            if (!(((Object) command).equals("用户注册"))) {
                return;
            }
            Object user_id = CommonUtil.getValueIfNull(__record.get("user_id"), null);
            String source = (String) CommonUtil.getValueIfNull(__record.get("source"), null);
            long source_type = (long) CommonUtil.getValueIfNull(__record.get("source_type"), 0L);
            Object register_time = CommonUtil.getValueIfNull(__record.get("action_time"), null);
            long user_type = (long) CommonUtil.getValueIfNull(__record.get("user_type"), 0L);
            long platform = (long) CommonUtil.getValueIfNull(__record.get("platform"), 0L);
            long game_id = (long) CommonUtil.getValueIfNull(__record.get("game_id"), 0L);
            String custom_field1 = __record.getString("custom_field1");
            String custom_field2 = __record.getString("custom_field2");
            String custom_field3 = __record.getString("custom_field3");
            String brand_model = custom_field1;
            String sys_version = custom_field2;
            String wx_version = custom_field3;
            int accountType = MEANINGLESS;
            if (custom_field1.contains("@@")){
                String[] strs = custom_field1.split("@@");
                brand_model = strs[0];
                sys_version = strs[1];
                wx_version = strs[2];
                accountType = custom_field2 != null || !"".equals(custom_field2.trim())? Integer.parseInt(custom_field2) : MEANINGLESS;
            }
            String sUserId = null;
            String sRegisterTime = null;
            long lUserId = 0;
            long lRegisterTime = 0;
            if (user_id instanceof String){
                sUserId = (String) user_id;
                lUserId = Long.parseLong(sUserId.replaceAll("\\s",""));
            }
            else{
                if (user_id == null){ user_id = 0L; }
                lUserId = (Long)user_id;
            }

            if (register_time instanceof String){
                sRegisterTime = (String) user_id;
                lRegisterTime = Long.parseLong(sRegisterTime.replaceAll("\\s", ""));
            }
            else {
                if (register_time == null){
                    register_time = 0L;
                }
                lRegisterTime = (long) register_time;
            }

            __valueOut.setString("log_date", CommonUtil.getDateStr("yyyy-MM-dd"));
//            __valueOut.setString("log_date", "2018-11-13");
            __valueOut.set("user_id", lUserId);
            __valueOut.set("source", source);
            __valueOut.set("source_type", source_type);
            __valueOut.set("register_time", lRegisterTime);
            __valueOut.set("user_type", user_type);
            __valueOut.set("platform", platform);
            __valueOut.set("game_id", game_id);
            __valueOut.set("brand_model", brand_model);
            __valueOut.set("sys_version", sys_version);
            __valueOut.set("wx_version", wx_version);
            __valueOut.set("account_type", accountType);
            context.write(__valueOut);
        }
    }
}

