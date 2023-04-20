package res;

import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

public class Fruits {
	public static final List<String> FRUIT_LIST;
	static {
		try(Scanner sc = new Scanner(Fruits.class.getResourceAsStream("FruitList.txt"))){
			ArrayList<String> f = new ArrayList<>();
			String s;
			while((s = sc.nextLine()) != null){
				f.add(s);
			}
			FRUIT_LIST = List.copyOf(f);
		}
	}
	public static void printAllFruits(){
		for(String f : FRUIT_LIST){
			System.out.println(f);
		}
	}
}
