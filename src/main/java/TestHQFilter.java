import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import java.io.*;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.Vector;

public class TestHQFilter {
    public static long encode(int z, int x, int y) {
        assert x >= 0;
        assert y >= y;
        assert x < (1 << z);
        assert y < (1 << z);
        return (1L << (2 * z)) | (((long) x) << z) | (y);
    }

    public static long findParent(int z, int x, int y, int level) {
        while (z > level) {
            x /= 2;
            y /= 2;
            z--;
        }
        return encode(z, x, y);
    }

    public static void main(String[] args) throws IOException, ParseException {
        String HQFilterPath = "BloomFilters/0_5_1M_HQ";
        String csvFile = HQFilterPath + "/non_empty_tileids.tsv/part-00000";
        BufferedReader br = new BufferedReader(new FileReader(csvFile));
        String lineCSV = "";
        HashSet<Long> nonEmptyTileIDs = new HashSet<>();
        while ((lineCSV = br.readLine()) != null) {
            nonEmptyTileIDs.add(Long.valueOf(lineCSV));
        }

        int start = 2;
        int stop = 2;
        int increment = 2;

        int levelLimit = 12;

        String userType = "Dense";
        double skewness_parameter = 0.5;
        String inputPath = "Inputs/Skewness/0_5_Dense"; //To make a combination of requests add additional paths with comas
        String testTitle = "Skewness Test 0_5 1M Dense";

        //Load Bloom Filter
        JSONParser jsonParser = new JSONParser();
        FileReader reader = new FileReader(HQFilterPath + "/bloomfilter_properties.json");
        //Read JSON file
        Object obj = jsonParser.parse(reader);
        JSONObject HQFilterConfiguration = (JSONObject) obj;
        int bloomLevel = Integer.valueOf(HQFilterConfiguration.get("levels").toString());
        int sizeOfTheBloomFilter = Integer.valueOf(HQFilterConfiguration.get("size").toString());
        int numberOfHashFunctions = Integer.valueOf(HQFilterConfiguration.get("k").toString());

        BloomFilter bloomFilter = new BloomFilter(sizeOfTheBloomFilter, numberOfHashFunctions);
        bloomFilter.read(HQFilterPath + "/bloom_filter");
        System.out.println("Bloom Filter loaded with false positive probability of " + bloomFilter.getFPP() + " and estimated n is " + bloomFilter.estimateSize());


        String[] inputPaths = inputPath.split(",");
        File[] folders = new File[inputPaths.length];
        folders[0] = new File(inputPaths[0]);
        int minFolder = folders[0].listFiles(new Main.ExtFileNameFilter(".json")).length;
        for (int i = 1; i < inputPaths.length; i++) {
            //if(!inputPaths[i].endsWith(".json")) continue;
            folders[i] = new File(inputPaths[i]);
            if (folders[i].listFiles().length < minFolder) minFolder = folders[i].listFiles().length;
        }

        JSONArray users = new JSONArray();
        for (int i = 0; i < minFolder; i++) {
            try {
                for (int j = 0; j < inputPaths.length; j++) {
                    users.add((JSONObject) new JSONParser().parse(new FileReader(folders[j].listFiles(new Main.ExtFileNameFilter(".json"))[i])));
                }

            } catch (IOException e) {
                e.printStackTrace();
            } catch (ParseException e) {
                e.printStackTrace();
            } catch (ClassCastException e) {
                e.printStackTrace();
            }

        }

        System.out.println("Test files are loaded.");

        PrintStream finalTestResult;
        File finalFile = new File("Outputs_Skewness_Level12/" + testTitle + "/" + testTitle + "-" + start + "_" + stop + ".tsv");
        finalFile.getParentFile().mkdirs();
        finalTestResult = new PrintStream(finalFile);


        for (int i = start; i <= stop; i += increment) {
            System.out.println("Loading test for " + i + " users");

            JSONObject testObject = new JSONObject();
            int requestCount = 0;

            for (int j = 0; j < i; j++) {

                JSONObject userObject = (JSONObject) users.get(j % users.size());

                for (Object o : userObject.keySet()) {
                    String k = o.toString();

                    JSONArray t = new JSONArray();

                    if (testObject.containsKey(k)) {
                        //System.out.println("Contains:" + kI);
                        t = (JSONArray) testObject.get(k);
                    }
                    t.add(userObject.get(k));

                    testObject.put(k, t);
                }
            }

            System.out.println(testObject.keySet().size() + " request times are loaded");
            System.out.println("Test is started");

            Vector<Long> requests = new Vector<Long>();
            Vector<Long> bloomed = new Vector<Long>();
            int counter = 0;

            Set globalJOKeys = testObject.keySet();
            Iterator globalJOIterator = globalJOKeys.iterator();

            while (globalJOIterator.hasNext()) {
                String k = globalJOIterator.next().toString();
                JSONArray req = (JSONArray) testObject.get(k);

                final Iterator jaIterator = req.iterator();

                while (jaIterator.hasNext()) {

                    final JSONObject tile = (JSONObject) jaIterator.next();

                    int z = Integer.parseInt(tile.get("z").toString());
                    int x = Integer.valueOf(tile.get("x").toString());
                    int y = Integer.valueOf(tile.get("y").toString());

                    if(z > levelLimit) break;

                    long tileID = encode(z, x, y);

                    if(!nonEmptyTileIDs.contains(tileID)) counter++;

                    if (z >= bloomLevel) {
                        if (!bloomFilter.mightContain(findParent(z, x, y, bloomLevel))) {
                            bloomed.add(tileID);
                        } else requests.add(tileID);
                    } else {
                        if (!bloomFilter.mightContain(tileID)) {
                            bloomed.add(tileID);
                        } else requests.add(tileID);
                    }
                }

            }

            if (i == start) {
                finalTestResult.println("Users\t# Requests Skipped by HQ Filter\t# Requests Sent\tTotal #Requests\t% Skipped Requests\tSkewness Parameter\tUser Type\t# Empty tiles\tHQ Filter Size\tLevel\tp");
            }
            int total = bloomed.size() + requests.size();
            double percentage = ((double)bloomed.size() / (double)total) * 100.0;
            String line = (i) + "\t"+ bloomed.size() + "\t" + requests.size() + "\t" + total + "\t" + percentage + "\t" + skewness_parameter + "\t" + userType + "\t" + counter + "\t" + bloomFilter.size() + "\t" + bloomLevel + "\t" + bloomFilter.getFPP();
            System.out.println("Users\t# Requests Skipped by HQ Filter\t# Requests Sent\tTotal #Requests\t% Skipped Requests\tSkewness Parameter\tUser Type\t# Empty Tiles\tHQ Filter Size\tLevel\tp");
            System.out.println(line);
            finalTestResult.println(line);


        }
    }
}
