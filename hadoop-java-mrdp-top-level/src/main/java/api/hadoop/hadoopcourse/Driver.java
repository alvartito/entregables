package api.hadoop.hadoopcourse;

import api.hadoop.aux.ProgramDriver;
import api.hadoop.hadoopcourse.hadoop.basic.sortjob.SortJobHadoop;
import api.hadoop.hadoopcourse.hadoop.basic.wordcount.SortByOcurrences;
import api.hadoop.hadoopcourse.hadoop.basic.wordcount.WordCountHadoop;
import api.hadoop.hadoopcourse.hadoop.writables.HottestDayPerYear;

public class Driver extends ProgramDriver {

	public Driver() throws Throwable {
		super();
		addClass("sort-hadoop", SortJobHadoop.class, "Just write the input as output (text files)");
		addClass("wordcount-hadoop", WordCountHadoop.class, "Word count in Hadoop");
		addClass("sort-by-ocurrences", SortByOcurrences.class, "Sorts the wordcount output by ocurrences");
		addClass("hottest-day-per-year", HottestDayPerYear.class, "Calculates the average temperature in the hottest day in each year for each location");
		addClass("hottest-day-per-year-writables", HottestDayPerYear.class, "Calculates the average temperature in the hottest day in each year for each location. Using writables");
	}

	public static void main(String[] args) throws Throwable {
		Driver driver = new Driver();
		driver.driver(args);
		System.exit(0);
	}
}
