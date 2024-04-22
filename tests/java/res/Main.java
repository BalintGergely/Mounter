import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

public class Main {
	public static final List<String> FRUIT_LIST;
	static {
		try(Scanner sc = new Scanner(Main.class.getResourceAsStream("FruitList.txt"))){
			ArrayList<String> f = new ArrayList<>();
			while(sc.hasNextLine()){
				f.add(sc.nextLine());
			}
			FRUIT_LIST = List.copyOf(f);
		}
	}
	public static void printAllFruits(){
		for(String f : FRUIT_LIST){
			System.out.println(f);
		}
	}
	public static void main(String[] atgs){
		printAllFruits();
	}	
}
