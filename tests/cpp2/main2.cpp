
#include <iostream>

#include "gcd.h"

int main(){
	int a,b,c;

	std::cin >> a >> b;

	c = gcd(a,b);
	
	std::cout << c << std::endl;
}