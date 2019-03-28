package shutiao.dataanalysis.mr;

import com.aliyun.odps.OdpsException;
import com.aliyun.odps.data.Record;
import com.aliyun.odps.mapred.MapperBase;
import com.aliyun.odps.mapred.ReducerBase;
import com.aliyun.odps.mapred.utils.OutputUtils;
import com.nicia.bocai.dataanalysis.common.CommonUtil;

import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

public class RegisterLog {
    private static final Set<String> COMMANDS = new HashSet<>();
    private static String logDate;
    static {
        COMMANDS.add("用户注册");
    }

    public static class TokenizerMapper extends MapperBase {
        Record value;
        Record key;

        @Override
        public void setup(TaskContext context) throws IOException {
            key = context.createMapOutputKeyRecord();
            value = context.createMapOutputValueRecord();
//            value.set(new Object[]{1L});
        }

        @Override
        public void map(long recordNum, Record record, TaskContext context)
                throws IOException {
            String command = record.getString("command");
            if (!COMMANDS.contains(command)) { return; }

            Long userId = record.getBigint("user_id");
            String device = record.getString("device");
            String source = record.getString("source");
            Long platform = record.getBigint("platform");
            Long accountType = record.getBigint("account_type");
            Long gender = record.getBigint("gender");
            Long userType = record.getBigint("user_type");
            key.set("log_date", logDate);
            key.set("user_id", userId);
            key.set("device", device);
            key.set("source", source);
            key.set("platform", platform);
            key.set("account_type", accountType);
            key.set("gender", gender);
            key.set("user_type", userType);
            value.set("none", 1);
            context.write(value);
        }
    }

    public static class SumReducer extends ReducerBase {
        private Record result;

        @Override
        public void setup(TaskContext context) throws IOException {
            result = context.createOutputRecord();
        }

        @Override
        public void reduce(Record key, Iterator<Record> values, TaskContext context)
                throws IOException {
            if (key == null){
                throw new IOException("key == null");
            }
            Long userId = key.getBigint("user_id");
            String device = key.getString("device");
            String source = key.getString("source");
            Long platform = key.getBigint("platform");
            Long accountType = key.getBigint("account_type");
            Long gender = key.getBigint("gender");
            Long userType = key.getBigint("user_type");

            if (logDate == null) {
                logDate = BaseLog.resolveLogDate(OutputUtils.getTables(context.getJobConf())[0].getPartSpec().toString());
            }
            result.set("user_id", userId);
            result.set("log_date", logDate);
            result.set("device", device);
            result.set("source", source);
            result.set("platform", platform);
            result.set("accountType", accountType);
            result.set("gender", gender);
            result.set("userType", userType);
            context.write(result);
        }
    }

    public static void main(String[] args) throws OdpsException {
        if (args.length < 2){
            System.exit(1);
        }
        String[] tmp = new String[BaseLog.ARGS_LENGTH];
        System.arraycopy(args, 0, tmp, 0, args.length);
        int index = args.length;
        tmp[index ++] = "log_date:string,user_id:bigint,device:string,source:string,platform:bigint"
                    + ",accountType:bigint,gender:bigint,userType:bigint";
        tmp[index ++] = "none:bigint";
        args = tmp;
        logDate = BaseLog.resolveLogDate(args[1]);
        BaseLog log = new BaseLog(args, new RegisterLog());
        log.run();
    }
}
