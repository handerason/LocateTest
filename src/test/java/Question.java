import org.junit.Test;
public class Question {
    public static void main(String args[]) {
        String ideal = getValue("IDEAL");
        System.out.println(ideal);
    }
    static String getValue(String word)
    {
        word="IDEAL";
        if (word.length() == 1)
            return "";
        else
            return getValue(word.substring(0, word.length() - 1))
                    + word.charAt(word.length() - 1);

    }

}