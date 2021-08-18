import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;

/**
 * @author jack Deng
 * @version 1.0
 * @date 2021/8/13 18:08
 */
public class testDemo {
    public char[] empty;
    public static void main(String[] args) {

        //        for (int i=0;i<256;i++)
        //            for (int j =0;j<256;j++) {
        //                String temp_string = String.valueOf(i) + String.valueOf(j);
        //                System.out.println(temp_string+" "+temp_string.hashCode());
        //          }
        char[] temp = {'a', 'b'};
        String str = new String(temp);
        System.out.println(str);
        char [] result = str.toCharArray();
        System.out.println(result);

        HashMap<String, Integer> hashmap = new HashMap<>();
        hashmap.put("dengjirui",666);
        Integer dengjirui = hashmap.get("dengjirui");
        System.out.println(dengjirui);
        System.out.println(hashmap);
        System.out.println(hashmap.containsValue(666));

    }
}
