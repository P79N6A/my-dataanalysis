import java.text.SimpleDateFormat;
import java.util.Date;

public class Test {
    public static void main(String[] args) {
//        String s = "a\ta\ta\n";
//        System.out.print(s);
//        System.out.print(s.replaceAll("[\\s]",""));
//        System.out.println("hahaha");

        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        String date = format.format(new Date());
        System.out.println(date);
    }
}
