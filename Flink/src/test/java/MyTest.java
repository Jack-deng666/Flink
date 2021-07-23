public class MyTest {

    public static void main(String[] args) {
        System.out.println("denfgjirui");
        AAP aap = new AAP();
        String name = aap.name;
    }



}
class AAP{
    final String name = "邓纪锐";
    final int age;
////    {
//        age = 1;
//    }
    public AAP(){
        age = 0;
    }

    public AAP(int age) {
        this.age = age;
    }
}