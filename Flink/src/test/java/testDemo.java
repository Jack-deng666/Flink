import java.util.*;

/**
 * @author jack Deng
 * @version 1.0
 * @date 2021/8/13 18:08
 */
public class testDemo {
    public char[] empty;
    public static void main(String[] args) {

        ArrayList<String> strings = new ArrayList<>();
        strings.add("wq");
        String s = strings.get(1);
        Iterator<String> iterator = strings.iterator();

        LinkedList<Integer> integers = new LinkedList<>();
        integers.add(1);
        Integer integer = integers.get(1);

        HashSet<String> hashset = new HashSet<>();
        hashset.add("we");
        hashset.add("we");
        hashset.add("wa");

    }
}
