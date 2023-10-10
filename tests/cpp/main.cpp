
#include <iostream>
#include <fstream>

#include "gcd.h"

void printFile(){
	std::ifstream file("copyme.txt");
	
	while(true){
		char c;
		file.get(c);
		if(file.eof()){
			break;
		}
		std::cout.put(c);
	}
}

int main(){
	printFile();

	int a,b,c;

	std::cin >> a >> b;

	c = gcd(a,b);
	
	std::cout << c << std::endl;
}