package com.nicia.bocai.dataanalysis.mapreduce;

import com.aliyun.odps.data.Record;
import com.aliyun.odps.data.TableInfo;
import com.aliyun.odps.mapred.JobClient;
import com.aliyun.odps.mapred.MapperBase;
import com.aliyun.odps.mapred.ReducerBase;
import com.aliyun.odps.mapred.conf.JobConf;
import com.aliyun.odps.mapred.utils.InputUtils;
import com.aliyun.odps.mapred.utils.OutputUtils;
import com.aliyun.odps.mapred.utils.SchemaUtils;
import com.nicia.bocai.dataanalysis.common.CommonUtil;

import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;

public class UserLogMR {


    static HashSet<String> commands = new HashSet();

    static HashSet<String> activeCommands = new HashSet<>();

    static {
        commands.add("注册奖励");
        commands.add("签到奖励");
        commands.add("游戏结算");
        commands.add("游戏结算抽奖");
        commands.add("游戏失败");
        commands.add("匹配门票");
        commands.add("广场聊天扣豆");
        commands.add("广告奖励");
        activeCommands.add("首页闯关红包任务");
        activeCommands.add("泡泡寻宝令");
        activeCommands.add("神秘使者");
        activeCommands.add("分享任务奖励");

        activeCommands.add("兑换扣豆");
        activeCommands.add("兑换奖励红包");
        activeCommands.add("幸运大转盘泡豆消耗");
        activeCommands.add("幸运大转盘泡豆奖励");
        activeCommands.add("幸运大转盘红包奖励");
        activeCommands.add("泡豆太少发放奖励");
        activeCommands.add("收藏大厅小程序发放奖励");
        activeCommands.add("大厅关注礼包奖励");

    }



    public static class TokenizerMapper extends MapperBase {


        private Record __keyOut;
        private Record __valueOut;

        @Override
        public void setup(TaskContext context) throws IOException {
            __keyOut = context.createMapOutputKeyRecord();
            __valueOut = context.createMapOutputValueRecord();
        }

        @Override
        public void map(long recordNum, Record __record, TaskContext context) throws IOException {


            String command = (String) CommonUtil.getValueIfNull(__record.get("command"), null);
            if (!(commands.contains(command) || activeCommands.contains(command))) { return; }

            long game_id = (long) CommonUtil.getValueIfNull(__record.get("game_id"), 0L);
            long user_id = (long) CommonUtil.getValueIfNull(__record.get("user_id"), 0L);
            long platform = (long) CommonUtil.getValueIfNull(__record.get("platform"), 0L);
            String source = (String) CommonUtil.getValueIfNull(__record.get("source"), null);
            long source_type = (long) CommonUtil.getValueIfNull(__record.get("source_type"), 0L);
            long gold_change = (long) CommonUtil.getValueIfNull(__record.get("gold_change"), 0L);
            String custom_field1 = (String) CommonUtil.getValueIfNull(__record.get("custom_field1"), null);
            String custom_field3 = (String) CommonUtil.getValueIfNull(__record.get("custom_field3"), null);
            long first_login = 0L;
            long sign_in = 0L;
            long active = 0L;
            long win_robot = 0L;
            long fail_robot = 0L;
            long tickets = 0L;
            double rmb_change = __record.getDouble("rmb_change_double");
            double share_rmb_get = 0;
            long chat = 0;
            long adver = 0;
            long adver_times = 0;

            long hall_cost1 = 0;
            long hall_cost2 = 0;
            long hall_collect = 0;
            long public_follow = 0;
            double activity_rmb = 0;
            long active_gold = 0;

            custom_field1 = custom_field1 == null ? "" : custom_field1;

            if (((Object) command).equals("注册奖励")) {
                first_login = gold_change;
            }
            if (((Object) command).equals("签到奖励")) {
                sign_in = gold_change;
            }
            if(activeCommands.contains(command)){
                if("分享任务奖励".equals(command)){ active = gold_change; }
                if("兑换扣豆".equals(command)){hall_cost1 = gold_change;}
                if("幸运大转盘泡豆消耗".equals(command)){hall_cost2 = gold_change;}
                if("收藏大厅小程序发放奖励".equals(command)){hall_collect = gold_change;}
                if("大厅关注礼包奖励".equals(command)){public_follow = gold_change;}
                if("幸运大转盘红包奖励".equals(command)){activity_rmb = rmb_change;}
                if("幸运大转盘泡豆奖励".equals(command)){active_gold = gold_change;}
                if("泡豆太少发放奖励".equals(command)){sign_in = gold_change;}
                if("兑换奖励红包".equals(command)){share_rmb_get = rmb_change;}
            }

            if ("游戏结算".equals(command) || "游戏结算抽奖".equals(command)) {
//                if ("2".equals(custom_field1)
//                        && custom_field3 != null
//                        && !custom_field3.isEmpty()
//                        && Integer.parseInt(custom_field3.trim()) >= 1000
//                        || "2".equals(custom_field3)) {
                    win_robot = Math.max(0, gold_change);
                    fail_robot = Math.min(0, gold_change);
//                } else { return; }
            }

            if ("匹配门票".equals(command)){
                if ("2".equals(custom_field1)) {
                    tickets = gold_change;
                }
                else {
                    return ;
                }
            }

            if ("广场聊天扣豆".equals(command)){
                chat = gold_change;
            }

            if ("广告奖励".equals(command)){
                adver = gold_change;
                adver_times ++;
            }

            __keyOut.set("game_id", game_id);
            __keyOut.set("user_id", user_id);
            __keyOut.set("platform", platform);
            __keyOut.set("source", source);
            __keyOut.set("source_type", source_type);


            __valueOut.set("first_login", first_login);
            __valueOut.set("sign_in", sign_in);
            __valueOut.set("active", active);
            __valueOut.set("win_robot", win_robot);
            __valueOut.set("fail_robot", fail_robot);
            __valueOut.set("tickets", tickets);
            __valueOut.set("share_rmb_get", share_rmb_get);
            __valueOut.set("chat", chat);
            __valueOut.set("adver", adver);
            __valueOut.set("adver_times", adver_times);

            __valueOut.set("hall_cost1", hall_cost1);
            __valueOut.set("hall_cost2", hall_cost2);
            __valueOut.set("hall_collect", hall_collect);
            __valueOut.set("public_follow", public_follow);
            __valueOut.set("activity_rmb", activity_rmb);
            __valueOut.set("active_gold", active_gold);
            context.write(__keyOut, __valueOut);
        }

    }
        /**
         * A combiner class that combines map output by sum them.
         **/
    public static class SumCombiner extends ReducerBase {

        private Record __valueOut;
        private Record __keyOut;

        @Override
        public void setup(TaskContext context) throws IOException {
            __valueOut = context.createMapOutputValueRecord();
            __keyOut = context.createMapOutputKeyRecord();
        }

        @Override
        public void reduce(Record __record, Iterator<Record> __values, TaskContext context) throws IOException {


            long first_login = 0L;
            long sign_in = 0L;
            long active = 0L;
            long win_robot = 0L;
            long fail_robot = 0L;
            long tickets = 0L;
            long chat = 0L;
            long adver = 0L;
            long adver_times = 0L;
            double share_rmb_get = 0;


            long hall_cost1 = 0;
            long hall_cost2 = 0;
            long hall_collect = 0;
            long public_follow = 0;
            double activity_rmb = 0;
            long active_gold = 0;

            while (__values.hasNext()) {
                Record __v = __values.next();
                first_login += (long) __v.getBigint("first_login");
                sign_in += (long) __v.getBigint("sign_in");
                active += (long) __v.getBigint("active");
                win_robot += (long) __v.getBigint("win_robot");
                fail_robot += (long) __v.getBigint("fail_robot");
                tickets += (long) __v.getBigint("tickets");
                share_rmb_get += __v.getDouble("share_rmb_get");
                chat += (long) __v.getBigint("chat");
                adver += __v.getBigint("adver");
                adver_times += __v.getBigint("adver_times");

                hall_cost1 += __v.getBigint("hall_cost1");
                hall_cost2 += __v.getBigint("hall_cost2");
                hall_collect += __v.getBigint("hall_collect");
                public_follow += __v.getBigint("public_follow");
                activity_rmb += __v.getDouble("activity_rmb");
                active_gold += __v.getBigint("active_gold");

            }


            __valueOut.set("first_login", first_login);
            __valueOut.set("sign_in", sign_in);
            __valueOut.set("active", active);
            __valueOut.set("win_robot", win_robot);
            __valueOut.set("fail_robot", fail_robot);
            __valueOut.set("tickets", tickets);
            __valueOut.set("chat", chat);
            __valueOut.set("share_rmb_get", share_rmb_get);
            __valueOut.set("adver", adver);
            __valueOut.set("adver_times", adver_times);


            __valueOut.set("hall_cost1", hall_cost1);
            __valueOut.set("hall_cost2", hall_cost2);
            __valueOut.set("hall_collect", hall_collect);
            __valueOut.set("public_follow", public_follow);
            __valueOut.set("activity_rmb", activity_rmb);
            __valueOut.set("active_gold", active_gold);


            context.write(__record, __valueOut);
        }
    }

        /**
         * A reducer class that just emits the sum of the input values.
         **/
    public static class SumReducer extends ReducerBase {

        private Record __valueOut = null;

        @Override
        public void setup(TaskContext context) throws IOException {
            __valueOut = context.createOutputRecord();
        }

        @Override
        public void reduce(Record __record, Iterator<Record> __values, TaskContext context) throws IOException {

            long game_id = (long) CommonUtil.getValueIfNull(__record.get("game_id"), 0L);
            long user_id = (long) CommonUtil.getValueIfNull(__record.get("user_id"), 0L);
            long platform = (long) CommonUtil.getValueIfNull(__record.get("platform"), 0L);
            String source = (String) CommonUtil.getValueIfNull(__record.get("source"), null);
            long source_type = (long) CommonUtil.getValueIfNull(__record.get("source_type"), 0L);
            long first_login = 0L;
            long sign_in = 0L;
            long active = 0L;
            long win_robot = 0L;
            long fail_robot = 0L;
            long tickets = 0L;
            long chat = 0L;
            long adver = 0L;
            long adver_times = 0L;
            String log_date = null;
            log_date = CommonUtil.getDateStr("yyyy-MM-dd");
            double share_rmb_get = 0;

            long hall_cost1 = 0;
            long hall_cost2 = 0;
            long hall_collect = 0;
            long public_follow = 0;
            double activity_rmb = 0;
            long active_gold = 0;

            while (__values.hasNext()) {
                Record __v = __values.next();
                first_login += (long) __v.getBigint("first_login");
                sign_in += (long) __v.getBigint("sign_in");
                active += (long) __v.getBigint("active");
                win_robot += (long) __v.getBigint("win_robot");
                fail_robot += (long) __v.getBigint("fail_robot");
                tickets += (long) __v.getBigint("tickets");
                share_rmb_get += __v.getDouble("share_rmb_get");
                chat += (long) __v.getBigint("chat");
                adver += __v.getBigint("adver");
                adver_times += __v.getBigint("adver_times");

                hall_cost1 += __v.getBigint("hall_cost1");
                hall_cost2 += __v.getBigint("hall_cost2");
                hall_collect += __v.getBigint("hall_collect");
                public_follow += __v.getBigint("public_follow");
                activity_rmb += __v.getDouble("activity_rmb");
                active_gold += __v.getBigint("active_gold");

            }
            __valueOut.set("game_id", game_id);
            __valueOut.set("user_id", user_id);
            __valueOut.set("platform", platform);
            __valueOut.set("source", source);
            __valueOut.set("source_type", source_type);
            __valueOut.set("first_login", first_login);
            __valueOut.set("sign_in", sign_in);
            __valueOut.set("active", active);
            __valueOut.set("win_robot", win_robot);
            __valueOut.set("fail_robot", fail_robot);
            __valueOut.set("log_date", log_date);
            __valueOut.set("tickets", tickets);
            __valueOut.set("chat", chat);
            __valueOut.set("share_rmb_get", share_rmb_get);
            __valueOut.set("adver", adver);
            __valueOut.set("adver_times", adver_times);


            __valueOut.set("hall_cost1", hall_cost1);
            __valueOut.set("hall_cost2", hall_cost2);
            __valueOut.set("hall_collect", hall_collect);
            __valueOut.set("public_follow", public_follow);
            __valueOut.set("activity_rmb", activity_rmb);
            __valueOut.set("active_gold", active_gold);
            context.write(__valueOut);
        }
    }
//    public static void main(String[] args) throws Exception {
//        if (args.length != 2) {
//            System.err.println("Usage: UserLog <in_table> <out_table>");
//            System.exit(2);
//        }
//        JobConf job = new JobConf();
//        job.setMapperClass(TokenizerMapper.class);
//        job.setCombinerClass(SumCombiner.class);
//        job.setReducerClass(SumReducer.class);
//        // 设置mapper中间结果的key和value的schema, mapper的中间结果输出也是record的形式
//        job.setMapOutputKeySchema(SchemaUtils.fromString("game_id:bigint,user_id:bigint,platform:bigint,source:string,source_type:bigint"));
//        job.setMapOutputValueSchema(SchemaUtils.fromString("first_login:bigint,sign_in:bigint,active:bigint,win_robot:bigint,fail_robot:bigint,tickets:bigint,share_rmb_get:double,chat:bigint,adver:bigint,adver_times:bigint,hall_cost1:bigint,hall_cost2:bigint,hall_collect:bigint,public_follow:bigint,activity_rmb:double,active_gold:bigint"));
//        // 设置输入和输出的表信息
//        InputUtils.addTable(TableInfo.builder().tableName(args[0]).build(), job);
//        OutputUtils.addTable(TableInfo.builder().tableName(args[1]).build(), job);
//        JobClient.runJob(job);
//    }
}

