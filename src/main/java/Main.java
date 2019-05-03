import java.io.*;
import java.util.*;

import com.mashape.unirest.http.Unirest;
import com.mashape.unirest.http.exceptions.UnirestException;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.*;

public class Main {

    public static void main(String[] args) throws IOException, InterruptedException {

        BloomFilter bloomFilter = new BloomFilter(63631427, 0.01, 7);
        bloomFilter.read("Cemetery_Opt/bloom_filter.json");


        boolean useBloom = false;
        int bloomLevel = 9;

        //String server = "http://ec2-54-92-194-172.compute-1.amazonaws.com/dynamic/visualize.cgi/ebd_plot/";
        //String server = "http://ec2-13-229-201-19.ap-southeast-1.compute.amazonaws.com/dynamic/visualize.cgi/ebd_plot/";
        String server = "http://localhost:8890/dynamic/visualize.cgi/output/Plots/Cemetery/";

        String tilePattern = "tile-{z}-{x}-{y}.png";

        //String testTitle = "EBird_N_California";
        String testTitle = "Cemetery_Local";
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

        server += tilePattern;

        File folder = new File("input");
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

        }

        System.out.println("Test files are loaded.");


        int start = 5;
        int stop = 100;
        int increment = 5;

        int userNumber = users.size();

        PrintStream finalTestResult;
        finalTestResult = new PrintStream(new File(testTitle + "-" + (useBloom ? "bloom_on" : "bloom_off") + ".tsv"));

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

            Set globalJOKeys = testObject.keySet();
            Iterator globalJOIterator = globalJOKeys.iterator();

            ArrayList<TimedRequest> tasks = new ArrayList<TimedRequest>();
            Timer timer = new Timer();
            int counter = 0;
            while (globalJOIterator.hasNext()) {
                String k = globalJOIterator.next().toString();
                if (useBloom)
                    tasks.add(counter, new TimedRequest(counter, server, (JSONArray) testObject.get(k), 1, requests, bloomed, bloomFilter, bloomLevel));
                else
                    tasks.add(counter, new TimedRequest(counter, server, (JSONArray) testObject.get(k), 1, requests));
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

            int limit = 500;

            long overLimit = 0;
            long underLimit = 0;

            PrintStream timesForEachRequest;
            timesForEachRequest = new PrintStream(new File(testTitle+"-" + (useBloom ? "bloom_on" : "bloom_off") + "-" + i + ".tsv"));

            Iterator it = requests.iterator();
            while (it.hasNext()) {
                long t = (Long) it.next();
                //System.out.println(t);
                timesForEachRequest.println(t);
                if (t >= limit) overLimit++;
                else underLimit++;
            }

            timesForEachRequest.close();

            if (i == start) {
                finalTestResult.println("Users\tBloom Filter\tRequests\tDelayed\tD.Percentage");
            }
            float p = ((float) overLimit / (float) ((bloomed.size()) + (overLimit + underLimit))) * 100;
            //String line = (users * multiplier) +"\t" + (useBloom ? "On" : "Off") + "\t" +  (overLimit + underLimit) + "\t" + failedCounter + "\t" + overLimit;
            String line = (i) + "\t" + bloomed.size() + "\t" + (overLimit + underLimit) + "\t" + overLimit + "\t" + p;
            System.out.println("Users\tBloom Filter\tRequests\tDelayed\tD.Percentage");
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