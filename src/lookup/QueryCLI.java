package lookup;

import java.io.IOException;
import java.util.Scanner;

import model.Performance;

// TODO: Check if index exists. If not, create one.

public class QueryCLI {
	public static void main(String[] args) {

		LookupManager lm = null;
		Performance lookup_performance = new Performance();
		Scanner kb = new Scanner(System.in);

		System.out.println("+-----------------------------------------------------+");
		System.out.println("| COMP 6521 Implementation Project - Dense Indexing   |");
		System.out.println("| Authors: Ankur Pandey, Julian Enoch, Richard Kallos |");
		System.out.println("+-----------------------------------------------------+");

		// Instantiate LookupManager
		lm = new LookupManager();

		// Query loop
		String input = "";
		short age = 0;
		while (true) {
			System.out.println();
			System.out.print("age (18-99)> ");
			input = kb.nextLine();
			if (input.equalsIgnoreCase("q")) {
				break;
			}
			try {
				age = Short.parseShort(input);
			} catch (NumberFormatException e) {
				System.out.println("Invalid input. Please enter a number from 18-99");
				continue;
			}

			lookup_performance.startTimer();

			// Perform index lookup for age input
			try {
				lm.lookupHits(age);

			} catch (IOException e) {
				e.printStackTrace();
				System.out.println("Unable to perform lookup. Exiting.");
				System.exit(1);

			} finally {
				// Stop analyzing for indexing.
				lookup_performance.stopTimer();
				System.out.println("\nPerformance data for lookup operation of age value " + age);
				System.out.println("Time Taken :          " + (double) lookup_performance.getTimeElapsed() / 1000 + " s");
	
			}
		}

		System.out.println("Received quit signal. Exiting.");
		kb.close();
	}
}
