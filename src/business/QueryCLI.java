package business;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Scanner;
import indexing.CreateIndex;

// TODO: Check if index exists. If not, create one.

public class QueryCLI {
	public static void main(String[] args) {
		
		LookupManager lm = null;
		Scanner kb = new Scanner(System.in);
		
		System.out.println("+-----------------------------------------------------+");
		System.out.println("| COMP 6521 Implementation Project - Dense Indexing   |");
		System.out.println("| Authors: Ankur Pandey, Julian Enoch, Richard Kallos |");
		System.out.println("+-----------------------------------------------------+");

		// Create index
		System.out.println("Creating Index:");
		try {
			CreateIndex.main(args);
		} catch (IOException e) {
			e.printStackTrace();
			System.out.println("Unable to create index. Exiting.");
			System.exit(1);
		}
		
		System.out.println("Index created. You may now enter queries");
		System.out.println("Enter q to quit.");
		
		// Instantiate LookupManager
		try {
			lm = new LookupManager();
		} catch (FileNotFoundException e1) {
			e1.printStackTrace();
			System.out.println("Unable to find indexes. Exiting.");
			System.exit(1);
		}
		
		// Query loop
		String input = "";
		short age = 0;
		while(!input.equalsIgnoreCase("q")) {
			System.out.print("age (18-99)> ");
			input = kb.nextLine();
			try {
				age = Short.parseShort(input);
			} catch(NumberFormatException e) {
				System.out.println("Invalid input. Please enter a number from 18-99");
				continue;
			}
			// Perform index lookup for age input
			try {
				lm.lookupHits(age);
			} catch (IOException e) {
				e.printStackTrace();
				System.out.println("Unable to perform lookup. Exiting.");
				System.exit(1);
			}
			// TODO: Print hits.txt to console
		}
		
		System.out.println("Received quit signal. Exiting.");
		kb.close();
	}
}
