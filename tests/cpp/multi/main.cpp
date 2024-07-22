
#include <iostream>
#include <fstream>

#include "gcd.h"

int main(){
	const char* hello = "\0 \10 \112 \u{123}";
	
	int a,b,c;

	std::cin >> a >> b;

	c = gcd(a,b);
	
	std::cout << c << std::endl;
}