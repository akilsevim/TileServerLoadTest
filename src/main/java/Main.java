import java.io.*;
import java.util.*;

import com.mashape.unirest.http.Unirest;
import com.mashape.unirest.http.exceptions.UnirestException;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.*;

public class Main {
    public static void main(String[] args) throws IOException, InterruptedException {

        //Bloom Filter Configuration
        boolean useBloom = true;
        int bloomLevel = 19;
        int numberOfHashFunctions = 17;
        int sizeOfTheBloomFilter = 567619585;
        String bloomFilterPath = "BloomFilters/ebd_bloomfilter_55MB_19/bloom_filter";

        //Static tile redirection configuration
        boolean useStatic = true;
        String staticTileIDsPath = "StaticTileIds/staticTileIDs.json";

        //Test configuration
        int start = 2;
        int stop = 50;
        int increment = 2;
        String inputPath = "Inputs/ebd_input_dense,Inputs/ebd_input_sparse"; //To make a combination of requests add additional paths with comas
        String testTitle = "Ebird_N_California_Combined_Static_On_Bloom_On_S3_2-50";
        int limit = 500; //Time limit for the response times
        String server = "http://ec2-54-215-192-47.us-west-1.compute.amazonaws.com/dynamic/visualize.cgi/ebd_plot/";
        String staticServer = "https://s3-us-west-1.amazonaws.com/visualizationserver/ebd_plot/";
        String tilePattern = "tile-{z}-{x}-{y}.png";

        //Load Bloom Filter
        BloomFilter bloomFilter = new BloomFilter(sizeOfTheBloomFilter,numberOfHashFunctions);
        if(useBloom) {
            bloomFilter.read(bloomFilterPath);
            System.out.println("Bloom Filter loaded with false positive probability of " + bloomFilter.getFPP() + " and estimated n is " + bloomFilter.estimateSize());
        }

        //Load Static Tile IDs
        JSONArray staticTiles = new JSONArray();
        if(useStatic) {
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
            int status = Unirest.get(server+"index.html").asString().getStatus();
            System.out.println("Test result: "+ status);
        } catch (UnirestException e) {
            e.printStackTrace();
            return;
        }

        String[] inputPaths = inputPath.split(",");
        File[] folders = new File[inputPaths.length];
        folders[0] = new File(inputPaths[0]);
        int minFolder = folders[0].listFiles().length;
        for(int i = 1; i < inputPaths.length; i++) {
            folders[i] = new File(inputPaths[i]);
            if(folders[0].listFiles().length < minFolder) minFolder = folders[0].listFiles().length;
        }

        JSONArray users = new JSONArray();
        for (int i = 0; i < minFolder; i++) {
            try {
                for(int j = 0; j < inputPaths.length; j++) {
                    users.add((JSONObject) new JSONParser().parse(new FileReader(folders[j].listFiles()[i])));
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
        File finalFile = new File("Outputs/"+testTitle+"/"+testTitle + "-" + (useBloom ? "bloom_on" : "bloom_off") + "-"+start+"_"+stop+".tsv");
        finalFile.getParentFile().mkdirs();
        finalTestResult = new PrintStream(finalFile);


        for(int i = start; i <= stop; i += increment) {
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

            System.out.println(testObject.keySet().size()+" request times are loaded");
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
                tasks.add(counter, new TimedRequest(counter, server, tilePattern, (JSONArray) testObject.get(k), 1, requests,
                        useBloom, bloomed, bloomFilter, bloomLevel,
                        staticCatched, staticTiles, staticServer,
                        times));

                timer.schedule(tasks.get(counter), Long.valueOf(k));
                counter++;
            }

            boolean test = true;
            while (test) {
                test = false;
                for (TimedRequest t : tasks) {
                    //System.out.println(t.toString() +":"+ t.isDone());
                    if (!t.isDone()) {
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
            File timesfile = new File("Outputs/"+testTitle+"/"+testTitle+"-" + (useBloom ? "bloom_on" : "bloom_off") + "-" + i + ".tsv");
            timesForEachRequest = new PrintStream(timesfile);

            for (long t : requests) {
                //System.out.println(t);
                timesForEachRequest.println(t);
                totalResponseTimes+=t;
                if (t >= limit) overLimit++;
                else underLimit++;
            }

            for (long t : bloomed) {
                //System.out.println(t);
                //timesForEachRequest.println(t);
                totalBloomedTimes+=t;
            }

            for (long t : staticCatched) {
                //System.out.println(t);
                //timesForEachRequest.println(t);
                totalStaticTimes+=t;
                if(useStatic) {
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
            String line = (i) + "\t" + bloomed.size() + "\t"+ staticCatched.size() + "\t" + (overLimit + underLimit) + "\t" + overLimit + "\t" + p + "\t" + averageResponseTime+ "\t" + averageBloomedTime+ "\t" + averageStaticTime;
            System.out.println("Users\tBloom Filter\tStatic Tiles\tRequests\tDelayed\tD.Percentage\tAverage R. Time\tAverage B. Time\tAverage S. Time");
            System.out.println(line);
            finalTestResult.println(line);

            Thread.sleep(150);

        }

        finalTestResult.close();
        Unirest.shutdown();

        /*

        int userLimit = 21;
        int magnifier = 1;

        for(int userCount = 1; userCount < userLimit; userCount++) {

            int users = 0;
            JSONObject globalJO = new JSONObject();

            for (final File fileEntry : folder.listFiles()) {
                if (users == userCount) break;
                try {
                    users++;
                    //System.out.println("User:" + users);
                    JSONObject jo = (JSONObject) new JSONParser().parse(new FileReader(fileEntry));

                    for (Object o : jo.keySet()) {
                        String k = o.toString();

                        int kI = Integer.parseInt(o.toString());
                        kI = kI - (kI % magnifier);

                        JSONArray t = new JSONArray();

                        if (globalJO.containsKey(Integer.toString(kI))) {
                            //System.out.println("Contains:" + kI);
                            t = (JSONArray) globalJO.get(Integer.toString(kI));
                        }
                        t.add(jo.get(k));

                        globalJO.put(Integer.toString(kI), t);
                    }


                } catch (IOException e) {
                    e.printStackTrace();
                } catch (ParseException e) {
                    e.printStackTrace();
                } catch (ClassCastException e) {
                    e.printStackTrace();
                    System.out.println(fileEntry.getName());
                }

            }

            PrintStream outb;
            outb = new PrintStream(new File("ebd-user-california-" + (useBloom ? "bloom_on" : "bloom_off") + "-" + (System.nanoTime()) + ".tsv"));




            for (int testCounter = 1; testCounter < 2; testCounter++) {


                Vector<Long> requests = new Vector<Long>();
                Vector<Long> bloomed = new Vector<Long>();
                Vector<Long> failed = new Vector<Long>();

                Set globalJOKeys = globalJO.keySet();
                Iterator globalJOIterator = globalJOKeys.iterator();

                ArrayList<TimedRequest> tasks = new ArrayList<TimedRequest>();
                Timer timer = new Timer();
                int counter = 0;
                while (globalJOIterator.hasNext()) {
                    String k = globalJOIterator.next().toString();
                    if (useBloom)
                        tasks.add(counter, new TimedRequest(counter, server, (JSONArray) globalJO.get(k), testCounter, requests, failed, bloomed, bloomFilter, bloomLevel));
                    else
                        tasks.add(counter, new TimedRequest(counter, server, (JSONArray) globalJO.get(k), testCounter, requests, failed));
                    timer.schedule(tasks.get(counter), Long.valueOf(k));
                    counter++;
                }

                boolean test = true;
                while (test) {
                    test = false;
                    for (TimedRequest t : tasks) {
                        //System.out.println(t.toString() +":"+ t.isDone());
                        if (!t.isDone()) {
                            test = true;
                            break;
                        }
                    }
                }

                Thread.sleep(100);

                int limit = 500;

                long overLimit = 0;
                long underLimit = 0;
                long failedCounter = failed.size();

                PrintStream outc;
                outc = new PrintStream(new File("ebd-tile-california-" + (useBloom ? "bloom_on" : "bloom_off") + "-" + (userCount * testCounter) + "-" + (System.nanoTime()) + ".tsv"));

                Iterator it = requests.iterator();
                while (it.hasNext()) {
                    long t = (Long) it.next();
                    //System.out.println(t);
                    outc.println(t);
                    if (t >= limit) overLimit++;
                    else underLimit++;
                }

                outc.close();

            /*
            Iterator it_b = bloomed.iterator();
            while (it_b.hasNext()) {
                long t = (Long) it_b.next();
                System.out.println(t);
                if (t >= limit) overLimit++;
                else underLimit++;
            }


                if (testCounter == 1) {
                    System.out.println("Users\tBloom Filter\tRequests\tFailed Requests\tDelayed\tD.Percentage");
                    outb.println("Users\tBloom Filter\tRequests\tFailed Requests\tDelayed\tD.Percentage");
                }
                float p = ((float) overLimit / (float) ((bloomed.size() * testCounter) + (overLimit + underLimit + failedCounter))) * 100;
                //String line = (users * multiplier) +"\t" + (useBloom ? "On" : "Off") + "\t" +  (overLimit + underLimit) + "\t" + failedCounter + "\t" + overLimit;
                String line = (users * testCounter) + "\t" + bloomed.size() * testCounter + "\t" + (overLimit + underLimit) + "\t" + failedCounter + "\t" + overLimit + "\t" + p;
                System.out.println(line);
                outb.println(line);

                Thread.sleep(150);

            }


            outb.close();

        }

        Unirest.shutdown();*/

    }

}