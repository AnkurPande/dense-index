package lookup;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Scanner;

import model.Performance;
import indexing.CreateIndex;

// TODO: Check if index exists. If not, create one.

public class QueryCLI {
	public static void main(String[] args) {
		
		LookupManager lm = null;
		CreateIndex ci = null;
		Performance lookup_performance = new Performance();
		Scanner kb = new Scanner(System.in);
		
		System.out.println("+-----------------------------------------------------+");
		System.out.println("| COMP 6521 Implementation Project - Dense Indexing   |");
		System.out.println("| Authors: Ankur Pandey, Julian Enoch, Richard Kallos |");
		System.out.println("+-----------------------------------------------------+");

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
				//Start analyzing for indexing.
				lookup_performance.startTimer();
				//Calculate start memory.
				lookup_performance.calculateStartMemory();
				//Lookup
				lm.lookupHits(age);
				//Stop analyzing for indexing.
				lookup_performance.stopTimer();		
				//Calculate memory used.
				lookup_performance.calculateEndMemory();
				
			} catch (IOException e) {
				e.printStackTrace();
				System.out.println("Unable to perform lookup. Exiting.");
				System.exit(1);
			}
			// TODO: Print hits.txt to console.
			
			System.out.println("\nPerformnace data for lookup operation of age value "+ age);
			System.out.println("Time Taken : " + lookup_performance.getTimeElapsed() + "ms");
			System.out.println("Memory Taken (in bytes): " + lookup_performance.getMemUsed()+ " bytes");
			System.out.println("Memory Taken (in MB): " + (double) lookup_performance.getMemUsed() /(1024*1024) + " MB");
		}
		System.out.println("Received quit signal. Exiting.");
		kb.close();
	}
}
