package multithreading;

import java.io.File;
import java.util.List;
import java.util.ArrayList;
import java.util.Scanner;

public class fileReader {
	//function to read and return the contents of the input in a list
	public List<String> readFile(String file_name) {
		File file = new File(file_name);
		List<String> records = new ArrayList<>();
		try {
			Scanner sc = new Scanner(file);
			while(sc.hasNext()) {
				String line = sc.nextLine();
				records.add(line);
				}
			sc.close();
		}
		catch(Exception e) {
			System.out.println(e);
		}
		return records;
	}
	
	public static void main(String[] args) {
		fileReader fr = new fileReader();
		fr.readFile(args[0]);
	}
}
