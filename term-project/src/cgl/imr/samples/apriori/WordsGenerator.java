/*
 * Software License, Version 1.0
 *
 *  Copyright 2003 The Trustees of Indiana University.  All rights reserved.
 *
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 * 1) All redistributions of source code must retain the above copyright notice,
 *  the list of authors in the original source code, this list of conditions and
 *  the disclaimer listed in this license;
 * 2) All redistributions in binary form must reproduce the above copyright
 *  notice, this list of conditions and the disclaimer listed in this license in
 *  the documentation and/or other materials provided with the distribution;
 * 3) Any documentation included with all redistributions must include the
 *  following acknowledgement:
 *
 * "This product includes software developed by the Community Grids Lab. For
 *  further information contact the Community Grids Lab at
 *  http://communitygrids.iu.edu/."
 *
 *  Alternatively, this acknowledgement may appear in the software itself, and
 *  wherever such third-party acknowledgments normally appear.
 *
 * 4) The name Indiana University or Community Grids Lab or Twister,
 *  shall not be used to endorse or promote products derived from this software
 *  without prior written permission from Indiana University.  For written
 *  permission, please contact the Advanced Research and Technology Institute
 *  ("ARTI") at 351 West 10th Street, Indianapolis, Indiana 46202.
 * 5) Products derived from this software may not be called Twister,
 *  nor may Indiana University or Community Grids Lab or Twister appear
 *  in their name, without prior written permission of ARTI.
 *
 *
 *  Indiana University provides no reassurances that the source code provided
 *  does not infringe the patent or any other intellectual property rights of
 *  any other entity.  Indiana University disclaims any liability to any
 *  recipient for claims brought by any other entity based on infringement of
 *  intellectual property rights or otherwise.
 *
 * LICENSEE UNDERSTANDS THAT SOFTWARE IS PROVIDED "AS IS" FOR WHICH NO
 * WARRANTIES AS TO CAPABILITIES OR ACCURACY ARE MADE. INDIANA UNIVERSITY GIVES
 * NO WARRANTIES AND MAKES NO REPRESENTATION THAT SOFTWARE IS FREE OF
 * INFRINGEMENT OF THIRD PARTY PATENT, COPYRIGHT, OR OTHER PROPRIETARY RIGHTS.
 * INDIANA UNIVERSITY MAKES NO WARRANTIES THAT SOFTWARE IS FREE FROM "BUGS",
 * "VIRUSES", "TROJAN HORSES", "TRAP DOORS", "WORMS", OR OTHER HARMFUL CODE.
 * LICENSEE ASSUMES THE ENTIRE RISK AS TO THE PERFORMANCE OF SOFTWARE AND/OR
 * ASSOCIATED MATERIALS, AND TO THE PERFORMANCE AND VALIDITY OF INFORMATION
 * GENERATED USING SOFTWARE.
 */

package cgl.imr.samples.wordcount;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;

/**
 * Generates a set of data files to match a given number of gigabytes.
 * 
 * @author Jaliya Ekanayake (jaliyae@gmail.com)
 * 
 */
public class WordsGenerator {

	public static String commonLine = "Counting word occurrences within a large document collection is a typical example used to illustrate the MapReduce technique The data set is split into smaller segments and the map function is executed on each of these data segments Map function outputs aa";
	public static long GIG = 1024 * 1024 * 1024;
	public static int LEN = 256;
	public static String rareLine = "The framework groups the word count pairs depending on the words Reduce tasks sum the counts associated with a particular word and ouput partial counts for a given word A combine function is used to sum all the reduce outputs together to produce final counts";

	public static void main(String[] args) throws IOException {

		if (args.length != 4) {
			System.out
					.println("Usage: [file prefix][directory][totalSize in GB][number of files]");
			System.exit(0);
		}
		String filePrefix = args[0];
		String directory = args[1];
		int totalSizeGB = Integer.valueOf(args[2]); // Size in GB for all the
													// files.
		int numFiles = Integer.valueOf(args[3]); // number of partitions.

		long totalNumLines = (totalSizeGB * GIG) / LEN;
		if ((totalSizeGB % LEN) > 0) {
			totalNumLines++;
		}
		System.out.println("Generating Words. Total Lines = " + totalNumLines);
		long perFile = totalNumLines / numFiles;
		long rem = totalNumLines % numFiles;

		String filePart = directory + "/" + filePrefix + "_";

		for (long i = 0; i < numFiles; i++) {
			File file = new File(filePart + i + ".txt");
			BufferedOutputStream bout = new BufferedOutputStream(
					new FileOutputStream(file));
			if (rem > 0 && i < rem) {
				perFile++;
			}
			perFile--;
			for (long j = 0; j < perFile; j++) {
				bout.write((commonLine + "\n").getBytes());
			}
			bout.write((rareLine + "\n").getBytes());
			bout.flush();
			bout.close();
		}
	}
}
