package pak;

public class Library {
	public static int gcd(int a,int b){
		int c;
		while(a != 0){
			c = b % a;
			b = a;
			a = c;
		}
		return b;
	}
}
