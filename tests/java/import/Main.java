import java.util.Scanner;

import pak.Library;

public class Main {
	public static void main(String[] atgs){
		try(Scanner sc = new Scanner(System.in)){
			int a = sc.nextInt();
			int b = sc.nextInt();
			int c = Library.gcd(a,b);
			System.out.println(c);
		}
	}
}
