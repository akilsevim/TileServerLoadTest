import java.io.*;
import java.util.*;

import com.mashape.unirest.http.Unirest;
import com.mashape.unirest.http.exceptions.UnirestException;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.*;

public class Main {
    private static int bloomLevel;

    // FileNameFilter implementation
    public static class ExtFileNameFilter implements FilenameFilter {

        private String extension;

        public ExtFileNameFilter(String extension) {
            this.extension = extension.toLowerCase();
        }

        @Override
        public boolean accept(File dir, String name) {
            return name.toLowerCase().endsWith(extension);
        }

    }

    public static void main(String[] args) throws IOException, InterruptedException {

        //Bloom Filter Configuration
        boolean useBloom = true;
        boolean useHQ = true;
        String bloomFilterPath = "BloomFilters/test_0_2_hq_filter";

        //Static tile redirection configuration
        boolean useStatic = false;
        String staticTileIDsPath = "StaticTileIds/staticTileIDs.json";

        //Test configuration
        int start = 2;
        int stop = 10;
        int increment = 2;
        String inputPath = "Inputs/skewness_dense"; //To make a combination of requests add additional paths with comas
        String testTitle = "Skewness Test 0_2 Dense Only";
        int limit = 500; //Time limit for the response times
        String server = "http://localhost:8890/dynamic/visualize.cgi/test_0_2_plot_csv/";
        String staticServer = "https://s3-us-west-1.amazonaws.com/visualizationserver/ebd_plot/";
        String tilePattern = "tile-{z}-{x}-{y}.png";

        BloomFilter bloomFilter = null;
        if (useBloom) {
            //Load Bloom Filter
            JSONParser jsonParser = new JSONParser();
            try (FileReader reader = new FileReader(bloomFilterPath + "/bloomfilter_properties.json")) {
                //Read JSON file
                Object obj = jsonParser.parse(reader);
                JSONObject HQFilterConfiguration = (JSONObject) obj;
                bloomLevel = Integer.valueOf(HQFilterConfiguration.get("levels").toString());
                int sizeOfTheBloomFilter = Integer.valueOf(HQFilterConfiguration.get("size").toString());
                int numberOfHashFunctions = Integer.valueOf(HQFilterConfiguration.get("k").toString());

                bloomFilter = new BloomFilter(sizeOfTheBloomFilter, numberOfHashFunctions);
                bloomFilter.read(bloomFilterPath+ "/bloom_filter");
                System.out.println("Bloom Filter loaded with false positive probability of " + bloomFilter.getFPP() + " and estimated n is " + bloomFilter.estimateSize());


            } catch (IOException | ParseException e) {
                e.printStackTrace();
            }
        }

        //Load Static Tile IDs
        JSONArray staticTiles = new JSONArray();
        if (useStatic) {
            try {
                staticTiles = (JSONArray) new JSONParser().parse(new FileReader(new File(staticTileIDsPath)));
                System.out.println(staticTiles.size() + " Static Tile IDs are loaded");
            } catch (ParseException e) {
                e.printStackTrace();
            }
        }

        try {
            System.out.println("Testing server...");
            Unirest.setTimeouts(0, 0);
            Unirest.setConcurrency(400, 400);
            int status = Unirest.get(server + "index.html").asString().getStatus();
            System.out.println("Test result: " + status);
        } catch (UnirestException e) {
            e.printStackTrace();
            return;
        }

        String[] inputPaths = inputPath.split(",");
        File[] folders = new File[inputPaths.length];
        folders[0] = new File(inputPaths[0]);
        int minFolder = folders[0].listFiles(new ExtFileNameFilter(".json")).length;
        for (int i = 1; i < inputPaths.length; i++) {
            //if(!inputPaths[i].endsWith(".json")) continue;
            folders[i] = new File(inputPaths[i]);
            if (folders[i].listFiles().length < minFolder) minFolder = folders[i].listFiles().length;
        }

        JSONArray users = new JSONArray();
        for (int i = 0; i < minFolder; i++) {
            try {
                for (int j = 0; j < inputPaths.length; j++) {
                    users.add((JSONObject) new JSONParser().parse(new FileReader(folders[j].listFiles(new ExtFileNameFilter(".json"))[i])));
                }

            } catch (IOException e) {
                e.printStackTrace();
            } catch (ParseException e) {
                e.printStackTrace();
            } catch (ClassCastException e) {
                e.printStackTrace();
            }

        }
        /*

        File folder = new File(inputPath);
        JSONArray users = new JSONArray();
        for (final File fileEntry : folder.listFiles()) {
            try {
                //System.out.println("User:" + users);
                JSONObject user = (JSONObject) new JSONParser().parse(new FileReader(fileEntry));
                users.add(user);

            } catch (IOException e) {
                e.printStackTrace();
            } catch (ParseException e) {
                e.printStackTrace();
            } catch (ClassCastException e) {
                e.printStackTrace();
                System.out.println(fileEntry.getName());
            }

        }*/

        System.out.println("Test files are loaded.");

        PrintStream finalTestResult;
        File finalFile = new File("Outputs/" + testTitle + "/" + testTitle + "-" + (useBloom ? "bloom_on" : "bloom_off") + "-" + start + "_" + stop + ".tsv");
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
            Vector<Long> staticCatched = new Vector<Long>();

            Vector<Long> times = new Vector<Long>();

            Set globalJOKeys = testObject.keySet();
            Iterator globalJOIterator = globalJOKeys.iterator();

            ArrayList<TimedRequest> tasks = new ArrayList<TimedRequest>();
            Timer timer = new Timer();
            int counter = 0;
            while (globalJOIterator.hasNext()) {
                String k = globalJOIterator.next().toString();
                if (useBloom) {
                    tasks.add(counter, new TimedRequest(counter, server, tilePattern, (JSONArray) testObject.get(k), 1, requests,
                            useBloom, bloomed, bloomFilter, bloomLevel, useHQ,
                            staticCatched, staticTiles, staticServer,
                            times));
                } else {
                    tasks.add(counter, new TimedRequest(counter, server, tilePattern, (JSONArray) testObject.get(k), 1, requests, useBloom, staticCatched, staticTiles, staticServer, times));
                }

                timer.schedule(tasks.get(counter), Long.valueOf(k));
                counter++;
            }

            boolean test = true;
            while (test) {
                test = false;
                for (TimedRequest t : tasks) {
                    //System.out.println(t.toString() +":"+ t.isDone());
                    if (!t.isDone()) {
                        Thread.sleep(10);
                        test = true;
                        break;
                    }
                }
            }

            System.out.println("Finished");

            Thread.sleep(100);

            long overLimit = 0;
            long underLimit = 0;
            long totalResponseTimes = 0;
            float averageResponseTime = 0;

            long totalBloomedTimes = 0;
            float averageBloomedTime = 0;

            long totalStaticTimes = 0;
            float averageStaticTime = 0;

            PrintStream timesForEachRequest;
            File timesfile = new File("Outputs/" + testTitle + "/" + testTitle + "-" + (useBloom ? "bloom_on" : "bloom_off") + "-" + i + ".tsv");
            timesForEachRequest = new PrintStream(timesfile);

            for (long t : requests) {
                //System.out.println(t);
                timesForEachRequest.println(t);
                totalResponseTimes += t;
                if (t >= limit) overLimit++;
                else underLimit++;
            }

            for (long t : bloomed) {
                //System.out.println(t);
                //timesForEachRequest.println(t);
                totalBloomedTimes += t;
            }

            for (long t : staticCatched) {
                //System.out.println(t);
                //timesForEachRequest.println(t);
                totalStaticTimes += t;
                if (useStatic) {
                    if (t >= limit) overLimit++;
                    else underLimit++;
                }
            }


            timesForEachRequest.close();

            averageResponseTime = (float) totalResponseTimes / (overLimit + underLimit);
            averageBloomedTime = (float) totalBloomedTimes / bloomed.size();
            averageStaticTime = (float) totalStaticTimes / staticCatched.size();

            if (i == start) {
                finalTestResult.println("Users\tBloom Filter\tStatic Tiles\tRequests\tDelayed\tD.Percentage\tAverage R. Time\tAverage B. Time\tAverage S. Time");
            }
            float p = ((float) overLimit / (float) ((bloomed.size()) + (overLimit + underLimit))) * 100;
            //String line = (users * multiplier) +"\t" + (useBloom ? "On" : "Off") + "\t" +  (overLimit + underLimit) + "\t" + failedCounter + "\t" + overLimit;
            String line = (i) + "\t" + bloomed.size() + "\t" + staticCatched.size() + "\t" + (overLimit + underLimit) + "\t" + overLimit + "\t" + p + "\t" + averageResponseTime + "\t" + averageBloomedTime + "\t" + averageStaticTime;
            System.out.println("Users\tBloom Filter\tStatic Tiles\tRequests\tDelayed\tD.Percentage\tAverage R. Time\tAverage B. Time\tAverage S. Time");
            System.out.println(line);
            finalTestResult.println(line);

            Thread.sleep(150);

        }

        finalTestResult.close();
        Unirest.shutdown();

        return;

    }

}