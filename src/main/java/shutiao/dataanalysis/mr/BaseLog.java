package shutiao.dataanalysis.mr;

import com.aliyun.odps.OdpsException;
import com.aliyun.odps.data.TableInfo;
import com.aliyun.odps.mapred.JobClient;
import com.aliyun.odps.mapred.MapperBase;
import com.aliyun.odps.mapred.conf.JobConf;
import com.aliyun.odps.mapred.utils.InputUtils;
import com.aliyun.odps.mapred.utils.OutputUtils;
import com.aliyun.odps.mapred.utils.SchemaUtils;

import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class BaseLog {

    public static final int ARGS_LENGTH = 4;
    private static final Pattern DATE_PATTERN = Pattern.compile("(\\d{4})(\\d{2})(\\d{2})");
    private Object target;
    private JobConf job;

    public BaseLog(Object target) {
        this.target = target;
        job = new JobConf();
    }

    /**
     * @param args 请见{@link #resolveCommonArgs resolveCommonArgs}
     */
    public BaseLog(String[] args, Object target) {
        this.target = target;
        job = new JobConf();
        resolveCommonArgs(args);
    }

    /**
     * @param args 格式： args = {"输入表", "输出表", "输出key类型以及值", "输出value类型以及值"}
     *             示例：wc_in1,wc_in2 mr_multiinout_out1,mr_multiinout_out2|a=1/b=1|out1,mr_multiinout_out2|a=2/b=2|out2  world:string  count:bigint
     */
    public void resolveCommonArgs(String[] args){
        String[] inputs = null;
        String[] outputs = null;
        String key = null;
        String value = null;
        if (args.length == ARGS_LENGTH) {
            /*即包含输入以及输出的表*/
            inputs = args[0].split(",");
            outputs = args[1].split(",");
            key = args[2];
            value = args[3];
        } else {
            System.err.println("args.length = " + args.length);
            System.exit(1);
        }
        Class[] clazzs = target.getClass().getDeclaredClasses();
        for (Class clz : clazzs){
            switch (clz.getSimpleName()){
                case "TokenizerMapper": job.setMapperClass(clz);break;
                case "SumCombiner": job.setCombinerClass(clz); break;
                case "SumReducer": job.setReducerClass(clz);break;
                default: break;
            }
        }

        job.setMapOutputKeySchema(SchemaUtils.fromString(key));
        job.setMapOutputValueSchema(SchemaUtils.fromString(value));

        for (String in : inputs) {
            String[] ss = in.split("\\|");
            if (ss.length == 1) {
                InputUtils.addTable(TableInfo.builder().tableName(ss[0]).build(), job);
            } else if (ss.length == 2) {
                LinkedHashMap<String, String> map = convertPartSpecToMap(ss[1]);
                InputUtils.addTable(TableInfo.builder().tableName(ss[0]).partSpec(map).build(), job);
            } else {
                System.err.println("Style of input: " + in + " is not right");
                System.exit(1);
            }
        }

        //解析用户的输出表字符串
        for (String out : outputs) {
            String[] ss = out.split("\\|");
            if (ss.length == 1) {
                OutputUtils.addTable(TableInfo.builder().tableName(ss[0]).build(), job);
            } else if (ss.length == 2) {
                LinkedHashMap<String, String> map = convertPartSpecToMap(ss[1]);
                OutputUtils.addTable(TableInfo.builder().tableName(ss[0]).partSpec(map).build(), job);
            } else if (ss.length == 3) {
                if (ss[1].isEmpty()) {
                    LinkedHashMap<String, String> map = convertPartSpecToMap(ss[2]);
                    OutputUtils.addTable(TableInfo.builder().tableName(ss[0]).partSpec(map).build(), job);
                } else {
                    LinkedHashMap<String, String> map = convertPartSpecToMap(ss[1]);
                    OutputUtils.addTable(TableInfo.builder().tableName(ss[0]).partSpec(map)
                            .label(ss[2]).build(), job);
                }
            } else {
                System.err.println("Style of output: " + out + " is not right");
                System.exit(1);
            }
        }
    }

    public void run() throws OdpsException {
        JobClient.runJob(job);
    }
    //将分区字符串如"ds=1/pt=2"转为map的形式
    public static LinkedHashMap<String, String> convertPartSpecToMap(
            String partSpec) {
        LinkedHashMap<String, String> map = new LinkedHashMap<String, String>();
        if (partSpec != null && !partSpec.trim().isEmpty()) {
            String[] parts = partSpec.split("/");
            for (String part : parts) {
                String[] ss = part.split("=");
                if (ss.length != 2) {
                    throw new RuntimeException("ODPS-0730001: error part spec format: "
                            + partSpec);
                }
                map.put(ss[0], ss[1]);
            }
        }
        return map;
    }


    public static String resolveLogDate(String ord) {
        return resolveLogDate(ord, DATE_PATTERN);
    }

    public static String resolveLogDate(String ord, Pattern pattern) {
        Matcher m = pattern.matcher(ord);
        StringBuilder sb = new StringBuilder(10);
        if (m.find()) {
            sb.append(m.group(1));
            sb.append('-');
            sb.append(m.group(2));
            sb.append('-');
            sb.append(m.group(3));
        }
        return sb.toString();
    }

    public JobConf getJob() {
        return job;
    }
}
