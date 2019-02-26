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

        FileInputStream fis = new FileInputStream(new File("bloom-filter-9-14"));
        BloomFilter<Long> bloomFilter = BloomFilter.readFrom(fis, Funnels.longFunnel());

        boolean useBloom = false;
        int bloomLevel = 14;

        String server = "http://localhost:10000/dynamic/visualize.cgi/plots/CEMETERY_plot/tile-{z}-{x}-{y}.png";

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
        int userLimit = 23;

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


                //threads[i] = new RequestThread(jo, server);
                //threads[i].run();


            } catch (IOException e) {
                e.printStackTrace();
            } catch (ParseException e) {
                e.printStackTrace();
            } catch (ClassCastException e) {
                e.printStackTrace();
                System.out.println(fileEntry.getName());
            }

        }

        for(int testCount = 1; testCount < 6; testCount++) {

            int multiplier = testCount;
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

            PrintStream outb;

            outb = new PrintStream(new File("result-" + (useBloom ? "bloom_on" : "bloom_off") + "-" + (users * multiplier) + "-" + (new Date().getTime()) + ".txt"));
            outb.println("Total Request: " + (overLimit + underLimit) +
                    "\nFailed: " + failedCounter +
                    "\nOver Limit: " + overLimit + "(" + ((float) overLimit / (float) (overLimit + underLimit) * 100) + "%)" +
                    "\n#Users: " + users * multiplier +
                    "\nBloom Filter: " + (useBloom ? "On" : "Off"));

            outb.close();


            System.out.println("Total Request: " + (overLimit + underLimit) +
                    "\nFailed: " + failedCounter +
                    "\nOver Limit: " + overLimit + "(" + ((float) overLimit / (float) (overLimit + underLimit) * 100) + "%)" +
                    "\n#Users: " + users * multiplier +
                    "\nBloom Filter: " + (useBloom ? "On" : "Off"));

            Thread.sleep(200);

        }

        Unirest.shutdown();
    }

}