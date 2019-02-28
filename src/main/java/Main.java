import java.io.*;
import java.util.*;

import com.google.common.hash.BloomFilter;
import com.google.common.hash.Funnels;
import com.mashape.unirest.http.Unirest;
import com.mashape.unirest.http.exceptions.UnirestException;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.*;

public class Main {

    public static void main(String[] args) throws IOException, InterruptedException {

        FileInputStream fis = new FileInputStream(new File("ebd_bloom_filter_10"));
        BloomFilter<Long> bloomFilter = BloomFilter.readFrom(fis, Funnels.longFunnel());

        boolean useBloom = false;
        int bloomLevel = 10;

        String server = "http://ec2-13-52-80-164.us-west-1.compute.amazonaws.com/dynamic/visualize.cgi/ebd_plot/tile-{z}-{x}-{y}.png";

        try {
            Unirest.setTimeouts(0, 0);
            Unirest.setConcurrency(400, 40);
            Unirest.get(server).asString().getStatus();
        } catch (UnirestException e) {
            e.printStackTrace();
        }

        File folder = new File("input");



        int magnifier = 1;
        int users = 0;
        int userLimit = 25;

        JSONObject globalJO = new JSONObject();

        for (final File fileEntry : folder.listFiles()) {
            if (users >= userLimit) break;
            try {
                users++;
                Object obj = new JSONParser().parse(new FileReader(fileEntry));
                JSONObject jo = (JSONObject) obj;

                Set joKeys = jo.keySet();

                Iterator joIterator = joKeys.iterator();

                while (joIterator.hasNext()) {
                    String k = joIterator.next().toString();

                    Integer kI = Integer.valueOf(k);
                    kI = kI - (kI % magnifier);

                    JSONArray t = new JSONArray();

                    if (globalJO.containsKey(kI.toString())) {
                        //System.out.println("Contains:" + kI);
                        t = (JSONArray) globalJO.get(kI.toString());
                    }
                    t.add(jo.get(k));

                    globalJO.put(kI.toString(), t);
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
        outb = new PrintStream(new File("ebd-result-" + (useBloom ? "bloom_on" : "bloom_off") + "-" + (new Date().getTime()) + ".tsv"));


        for(int testCounter = 1; testCounter < 6; testCounter++) {

            int multiplier = testCounter;
            Hashtable<String, Long> requests = new Hashtable<String, Long>();
            Hashtable<String, Long> failed = new Hashtable<String, Long>();

            Set globalJOKeys = globalJO.keySet();
            Iterator globalJOIterator = globalJOKeys.iterator();


            ArrayList<TimedRequest> tasks = new ArrayList<TimedRequest>();
            int counter = 0;
            Timer timer = new Timer();
            while (globalJOIterator.hasNext()) {
                String k = globalJOIterator.next().toString();
                if (useBloom)
                    tasks.add(counter, new TimedRequest(counter, server, (JSONArray) globalJO.get(k), multiplier, requests, failed, bloomFilter, bloomLevel));
                else
                    tasks.add(counter, new TimedRequest(counter, server, (JSONArray) globalJO.get(k), multiplier, requests, failed));
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

            Set k = requests.keySet();
            Iterator it = k.iterator();

            int limit = 500;

            long overLimit = 0;
            long underLimit = 0;
            long failedCounter = 0;

            while (it.hasNext()) {
                long t = requests.get(it.next());
                //System.out.println(it.next() + ","+t);
                if (t >= limit) overLimit++;
                else underLimit++;
            }

            for (String aLong : failed.keySet()) {
                failedCounter++;
                if (failed.get(aLong) >= limit) overLimit++;
                else underLimit++;
            }

            if(testCounter == 1) {
                System.out.println("Users\tBloom Filter\tRequests\tFailed Requests\tDelayed");
                outb.println("Users\tBloom Filter\tRequests\tFailed Requests\tDelayed");
            }

            String line = (users * multiplier) +"\t" + (useBloom ? "On" : "Off") + "\t" +  (overLimit + underLimit) + "\t" + failedCounter + "\t" + overLimit;
            System.out.println(line);
            outb.println(line);

            Thread.sleep(150);

        }

        Unirest.shutdown();
        outb.close();

    }

}