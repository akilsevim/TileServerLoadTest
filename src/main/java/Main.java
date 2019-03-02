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

        boolean useBloom = true;
        int bloomLevel = 9;

        //String server = "http://ec2-54-92-194-172.compute-1.amazonaws.com/dynamic/visualize.cgi/ebd_plot/";
        //String server = "http://ec2-13-229-201-19.ap-southeast-1.compute.amazonaws.com/dynamic/visualize.cgi/ebd_plot/";
        String server = "http://ec2-52-53-150-190.us-west-1.compute.amazonaws.com/dynamic/visualize.cgi/ebd_plot/";

        String tilePattern = "tile-{z}-{x}-{y}.png";

        try {
            Unirest.setTimeouts(0, 0);
            Unirest.setConcurrency(400, 40);
            Unirest.get(server+"index.html").asString().getStatus();
        } catch (UnirestException e) {
            e.printStackTrace();
        }

        server += tilePattern;

        File folder = new File("input");
        int userLimit = 16;
        int magnifier = 1;

        for(int userCount = 1; userCount < userLimit; userCount++) {

            int users = 0;
            JSONObject globalJO = new JSONObject();

            for (final File fileEntry : folder.listFiles()) {
                if (users == userCount) break;
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
            outb = new PrintStream(new File("ebd-result-" + (useBloom ? "bloom_on" : "bloom_off") + "-" + (System.nanoTime()) + ".tsv"));


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

                Iterator it = requests.iterator();
                while (it.hasNext()) {
                    long t = (Long) it.next();
                    //System.out.println(t);
                    if (t >= limit) overLimit++;
                    else underLimit++;
                }

            /*
            Iterator it_b = bloomed.iterator();
            while (it_b.hasNext()) {
                long t = (Long) it_b.next();
                System.out.println(t);
                if (t >= limit) overLimit++;
                else underLimit++;
            }
            */

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

        Unirest.shutdown();

    }

}