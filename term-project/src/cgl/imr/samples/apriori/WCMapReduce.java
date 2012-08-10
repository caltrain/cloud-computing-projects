package cgl.imr.samples.wordcount;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;
import java.util.StringTokenizer;
import java.util.Vector;
import org.safehaus.uuid.UUIDGenerator;
import cgl.imr.base.impl.JobConf;
import cgl.imr.client.TwisterDriver;
import cgl.imr.base.TwisterMonitor;
import cgl.imr.types.StringValue;

/**
 * Map task for the Apriori
 * 
 * @author Magesh Vadivelu(magevadi@indiana.edu) 
 *         Shivaraman Janakiraman(shivjana@indiana.edu)
 */
public class WCMapReduce
{
	public static void main(String[] args) throws Exception
	{
		if (args.length != 5)
		{
			System.out.println("Usage:[partition File][config file][output file][num maps][num reducers]");
			System.exit(-1);
		}

		String partitionFile = args[0];
		String configFile = args[1];
		String outputFile = args[2];
		int numMaps = Integer.parseInt(args[3]);
		int numReducers = Integer.parseInt(args[4]);

		WCMapReduce wc = new WCMapReduce();

		wc.aprioriProcess(partitionFile, configFile, outputFile, numMaps, numReducers);
	}

	public void aprioriProcess(String partitionFile, String configFile, String outputFile, int numMaps, int numReducers) throws Exception
	{
		Vector<String> candidates = new Vector<String>(); // the current
															// candidates
		int itemsetNumber = 0; // the current itemset being looked at
		WCMapReduce wc = new WCMapReduce();
		double beginTime = System.currentTimeMillis();
		Map<String, Integer> result;
		int totalNumItems = 0;
		int totalNumTransactions = 0;
		double minSup = 0.0;

		FileInputStream file_in = null;

		file_in = new FileInputStream(configFile);

		BufferedReader data_in = new BufferedReader(new InputStreamReader(file_in));
		// number of items

		totalNumItems = Integer.valueOf(data_in.readLine()).intValue();
		totalNumTransactions = Integer.valueOf(data_in.readLine()).intValue();
		//minSup = Integer.valueOf(data_in.readLine()).intValue();
		minSup = new Double(data_in.readLine());//.intValue();
		data_in.close();

		// JobConfigurations
		JobConf jobConf = new JobConf("word-count-map-reduce" + uuidGen.generateTimeBasedUUID());
		jobConf.setMapperClass(WCMapTask.class);
		jobConf.setReducerClass(WCReduceTask.class);
		jobConf.setCombinerClass(WCCombiner.class);
		jobConf.setNumMapTasks(numMaps);
		jobConf.setNumReduceTasks(numReducers);
		// jobConf.setFaultTolerance();
		TwisterDriver driver;

		driver = new TwisterDriver(jobConf);
		driver.configureMaps(partitionFile);
		System.out.println("----------------------------------\n");
		
		// while not complete
		do
		{
			// increase the itemset that is being looked at
			itemsetNumber++;
			String candidateString = "";
			// generate the candidates
			
			candidates = wc.generateCandidates(itemsetNumber, candidates, totalNumItems);

			for (String oneString : candidates)
			{
				candidateString = candidateString + "," + oneString;
			}
			candidateString = candidateString.replaceFirst(",", "");
		
			StringValue candidatesBroadcastString = new StringValue(candidateString);

			TwisterMonitor monitor = driver.runMapReduceBCast(candidatesBroadcastString);
			// TwisterMonitor monitor = driver.runMapReduce();
			monitor.monitorTillCompletion();
			Map<String, Integer> wordCounts = new HashMap<String, Integer>();
			wordCounts= ((WCCombiner) driver.getCurrentCombiner()).getResults();
			//System.out.println("Word counts after collecting the result: " + wordCounts);
			
			
			candidates = calculateFrequentItemsets(wordCounts, totalNumTransactions, minSup, outputFile);

			if (candidates.size() != 0)
			{
				System.out.println("Frequent " + itemsetNumber + "-itemsets");
				System.out.println(candidates + "\n");
				BufferedWriter writer = null;

				writer = new BufferedWriter(new FileWriter(outputFile, true));
				
				writer.write("Frequent " + itemsetNumber + "-itemsets\n");
				writer.write(candidates.toString() + "\n");
				writer.flush();
				writer.close();

			}
			System.out.println("----------------------------------\n");
		}
		while (candidates.size() > 1);

		driver.close();
		double endTime = System.currentTimeMillis();
		// word count print out was here

		System.out.println("------------------------------------------------------");
		System.out.println("Apriori took " + (endTime - beginTime) / 1000 + " seconds.");
		System.out.println("------------------------------------------------------");
		System.exit(0);
	}

	private UUIDGenerator uuidGen = UUIDGenerator.getInstance();

	/************************************************************************
	 * Method Name : generateCandidates Purpose : Generate all possible
	 * candidates for the n-th itemsets : these candidates are stored in the
	 * candidates class vector Parameters : n - integer value representing the
	 * current itemsets to be created Return : None
	 * 
	 * @param candidates
	 * @param totalNumTransactions
	 * @param totalNumItems
	 *************************************************************************/
	private Vector<String> generateCandidates(int n, Vector<String> candidates, int totalNumItems)
	{
		Vector<String> tempCandidates = new Vector<String>(); // temporary
																// candidate
																// string vector
		String str1, str2; // strings that will be used for comparisons
		StringTokenizer st1, st2; // string tokenizers for the two itemsets
									// being compared

		// if its the first set, candidates are just the numbers
		if (n == 1)
		{
			for (int i = 1; i <= totalNumItems; i++)
			{
				tempCandidates.add(Integer.toString(i));
			}
		}
		else if (n == 2) // second itemset is just all combinations of itemset 1
		{
			// add each itemset from the previous frequent itemsets together
			for (int i = 0; i < candidates.size(); i++)
			{
				st1 = new StringTokenizer(candidates.get(i));
				str1 = st1.nextToken();
				for (int j = i + 1; j < candidates.size(); j++)
				{
					st2 = new StringTokenizer(candidates.elementAt(j));
					str2 = st2.nextToken();
					tempCandidates.add(str1 + " " + str2);
				}
			}
		}
		else
		{
			// for each itemset
			for (int i = 0; i < candidates.size(); i++)
			{
				// compare to the next itemset
				for (int j = i + 1; j < candidates.size(); j++)
				{
					// create the strigns
					str1 = new String();
					str2 = new String();
					// create the tokenizers
					st1 = new StringTokenizer(candidates.get(i));
					st2 = new StringTokenizer(candidates.get(j));

					// make a string of the first n-2 tokens of the strings
					for (int s = 0; s < n - 2; s++)
					{
						str1 = str1 + " " + st1.nextToken();
						str2 = str2 + " " + st2.nextToken();
					}

					// if they have the same n-2 tokens, add them together
					if (str2.compareToIgnoreCase(str1) == 0)
						tempCandidates.add((str1 + " " + st1.nextToken() + " " + st2.nextToken()).trim());
				}
			}
		}
		// clear the old candidates
		candidates.clear();
		// set the new ones
		candidates = new Vector<String>(tempCandidates);
		tempCandidates.clear();
		return candidates;
	}

	/************************************************************************
	 * Method Name : calculateFrequentItemsets Purpose : Determine which
	 * candidates are frequent in the n-th itemsets : from all possible
	 * candidates Parameters : n - iteger representing the current itemsets
	 * being evaluated Return : None
	 * 
	 * @param numTransactions
	 * @param minSup
	 * @param outputFile
	 * @param result
	 * @return
	 *************************************************************************/
	private Vector<String> calculateFrequentItemsets(Map<String, Integer> candidatesMap, int numTransactions, double minSup, String outputFile)
	{
		System.out.println("Calculating frequent itemsets...");
		//System.out.println("Received candidates map: " + candidatesMap);
		//System.out.println("Number of transactions: " + numTransactions);
		Map<String, Integer> frequentCandidateMap = new HashMap<String, Integer>();
		FileWriter fileWriter;
		BufferedWriter file_out;
		Vector<String> candidates = new Vector<String>();
		try
		{
			fileWriter = new FileWriter(outputFile, true);
			file_out = new BufferedWriter(fileWriter);
			file_out.write("\n");
			// This method lives here.
			for (String oneCandidate : candidatesMap.keySet())
			{
				// if the count% is larger than the minSup%, add to the
				// candidate to the frequent candidates
				if ((candidatesMap.get(oneCandidate) / (double) numTransactions) >= minSup)
				{
					frequentCandidateMap.put(oneCandidate, candidatesMap.get(oneCandidate));
					// put the frequent itemset into the output file
					file_out.write("\n" + oneCandidate + ", " + candidatesMap.get(oneCandidate) / (double) numTransactions);
				}
			}
			file_out.write("\n");
			file_out.close();

			candidatesMap.clear();
			candidates.clear();
			// new candidates are the old frequent candidates
			candidates = new Vector<String>(frequentCandidateMap.keySet());
			candidatesMap = frequentCandidateMap;
			//System.out.println("Frequent candidates map before returning: " + frequentCandidateMap);
			//System.out.println("Candidates before returning: " + candidates);
			frequentCandidateMap.clear();
			// List<KeyValuePair> candidates = frequentCandidateMap;
		}
		catch (Exception e)
		{
			//This is one big try catch block
			e.printStackTrace();
		}
		return candidates;
	}
}
