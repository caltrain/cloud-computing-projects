package cgl.imr.samples.wordcount;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.StringTokenizer;
import java.util.concurrent.ConcurrentHashMap;

import cgl.imr.base.Key;
import cgl.imr.base.MapOutputCollector;
import cgl.imr.base.MapTask;
import cgl.imr.base.TwisterException;
import cgl.imr.base.Value;
import cgl.imr.base.impl.JobConf;
import cgl.imr.base.impl.MapperConf;
import cgl.imr.data.file.FileData;
import cgl.imr.types.IntValue;
import cgl.imr.types.StringKey;

/**
 * Map task for the Apriori
 * 
 * @author Magesh Vadivelu(magevadi@indiana.edu) 
 *         Shivaraman Janakiraman(shivjana@indiana.edu)
 */

public class WCMapTask implements MapTask
{

	private FileData fileData;
	private Map<String, Integer> candidatesMap;

	public WCMapTask()
	{
		candidatesMap = new HashMap<String, Integer>();
	}

	public void close() throws TwisterException
	{
	}

	public void configure(JobConf jobConf, MapperConf mapConf) throws TwisterException
	{
		fileData = (FileData) mapConf.getDataPartition();
	}

	@Override
	public void map(MapOutputCollector collector, Key key, Value val) throws TwisterException
	{
		String itemSep = " ";
		int numItems;
		int numTransactions = 0;
		boolean trans[];

		String[] oneVal;

		// start of paste
		// Frequent candidate for current itemset
		ConcurrentHashMap<String, Integer> frequentCandidateMap = new ConcurrentHashMap<String, Integer>();

		String[] strArrayFreqCandidates = val.toString().split(",");
		for (String oneString : strArrayFreqCandidates)
		{
			candidatesMap.put(oneString, 0);
		}

		StringTokenizer st, stFile; // tokenizer for candidate and transaction
		boolean match; // whether the transaction has all the items in an
						// itemset

		// load the transaction file
		BufferedReader data_in = null;
		try
		{
			data_in = new BufferedReader(new FileReader(fileData.getFileName()), 65536);
			String inputLine = null;

			numItems = Integer.parseInt(data_in.readLine());

			numTransactions = Integer.parseInt(data_in.readLine());
			
			trans = new boolean[numItems]; // array to hold a
											// transaction
			oneVal = new String[numItems];

			for (int i = 0; i < oneVal.length; i++)
			{
				oneVal[i] = "1";
			}
			// so that can be checked

			// for each transaction
			for (int i = 0; i < numTransactions; i++)
			{
				stFile = new StringTokenizer(data_in.readLine(), itemSep); // read
																			// a
																			// line
																			// from
																			// the
																			// file
																			// to
																			// the
																			// tokenizer
				// put the contents of that line into the transaction array
				for (int j = 0; j < numItems; j++)
				{
					trans[j] = (stFile.nextToken().compareToIgnoreCase(oneVal[j]) == 0); // if
																							// it
																							// is
																							// not
																							// a
																							// 0,
																							// assign
																							// the
																							// value
																							// to
																							// true
				}
				
				// check each candidate
				Iterator candidateMapIter = candidatesMap.keySet().iterator();
				while (candidateMapIter.hasNext())
				{
					String oneCandidate = candidateMapIter.next().toString();
					match = false; // reset match to false
					// tokenize the candidate so that we know what items need to
					// be present for a match
					st = new StringTokenizer(oneCandidate);
					String oneToken = "";

					// check each item in the itemset to see if it is present in
					// the transaction
					while (st.hasMoreTokens())
					{
						oneToken = st.nextToken();
						match = (trans[Integer.valueOf(oneToken) - 1]);
						//System.out.println("Match text: " + match);
						if (!match) // if it is not present in the transaction
						{ // stop checking
							break;
						}
					}
					if (match)
					{
						if (candidatesMap.get(oneCandidate) == null)
						{
							candidatesMap.put(oneCandidate, 0);
						}
						else
						{
							candidatesMap.put(oneCandidate, candidatesMap.get(oneCandidate) + 1);
						}
						//System.out.println(candidatesMap.toString());
					}
				}

			}

			data_in.close();
			// end of paste
			// Now add the collected words and their respective counts to the
			// output collector.
			Iterator<String> ite = candidatesMap.keySet().iterator();
			String strKey = null;
			while (ite.hasNext())
			{
				strKey = ite.next();
				collector.collect(new StringKey(strKey), new IntValue(candidatesMap.get(strKey).intValue()));
			}
			candidatesMap.clear();
		}
		catch (Exception e)
		{
			// This is one big try catch block
			e.printStackTrace();
		}
	}
}
