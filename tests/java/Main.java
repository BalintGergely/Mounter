import java.util.Scanner;

import nat.GCDNative;
import res.Fruits;

public class Main{
	public static void main(String[] atgs){
		try(Scanner sc = new Scanner(System.in)){
			int a = sc.nextInt();
			int b = sc.nextInt();
			int c = GCDNative.gcd(a, b);
			System.out.println(c);

			Fruits.printAllFruits();
		}
	}
}