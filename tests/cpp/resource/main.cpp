
#include <iostream>
#include <fstream>

int main(){
	std::ifstream file("bin/resource.txt");
	
	while(true){
		char c;
		file.get(c);
		if(file.eof()){
			break;
		}
		std::cout.put(c);
	}
}