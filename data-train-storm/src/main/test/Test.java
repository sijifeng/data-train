import java.io.FileNotFoundException;
import java.io.FileReader;

/**
 * Created by jiyc on 2017/6/13.
 */
public class Test {
	public static void main(String[] args) {
		try {
			FileReader fileReader = new FileReader("\\resources\\word.txt");
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
	}
}
