import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;

import com.mashape.unirest.http.Unirest;
import com.mashape.unirest.http.exceptions.UnirestException;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.*;

public class Main {

    public static void main(String[] args) throws IOException {

        Hashtable<Long, Long> requests = new Hashtable<Long, Long>();
        Hashtable<Long, Long> failed = new Hashtable<Long, Long>();


        String server = "http://localhost:10000/dynamic/visualize.cgi/plots/CEMETERY_plot/tile-{z}-{x}-{y}.png";

        try {
            Unirest.setTimeouts(0, 0);
            Unirest.get(server).asString().getStatus();
        } catch (UnirestException e) {
            e.printStackTrace();
        }

        File folder = new File("input");

        int multiplier = 10;
        int users = 0;
        int magnifier = 5;

        JSONObject globalJO = new JSONObject();

        int i = 0;
        for (final File fileEntry : folder.listFiles()) {

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

                    if(globalJO.containsKey(kI.toString())) {
                        //System.out.println("Contains:" + kI);
                        t = (JSONArray) globalJO.get(kI.toString());
                    }
                    t.add(jo.get(k));

                    globalJO.put(kI.toString(), t);
                }


                //threads[i] = new RequestThread(jo, server);
                //threads[i].run();
                i++;


            } catch (IOException e) {
                e.printStackTrace();
            } catch (ParseException e) {
                e.printStackTrace();
            } catch (ClassCastException e) {
                e.printStackTrace();
                System.out.println(fileEntry.getName());
            }

        }

        Set globalJOKeys = globalJO.keySet();
        Iterator globalJOIterator = globalJOKeys.iterator();



        ArrayList<TimedRequest> tasks = new ArrayList<TimedRequest>();
        int counter = 0;
        Timer timer = new Timer();
        while (globalJOIterator.hasNext()) {
            String k = globalJOIterator.next().toString();
            tasks.add(counter,new TimedRequest(counter, server, (JSONArray) globalJO.get(k), multiplier, requests, failed));
            timer.schedule(tasks.get(counter), Long.valueOf(k));
            counter++;
        }


        boolean test = true;
        while(test) {
            test = false;
            for (TimedRequest t : tasks) {
                //System.out.println(t.toString() +":"+ t.isDone());
                if(!t.isDone()) {
                    test = true;
                    break;
                }
            }
        }

        Set k = requests.keySet();
        Iterator it = k.iterator();

        int limit = 500;

        long overLimit = 0;
        long underLimit = 0;
        long failedCounter = 0;

        while(it.hasNext()) {
            if(requests.get(it.next()) >= limit) overLimit++;
            underLimit++;
        }
        Iterator it1 = failed.keySet().iterator();
        while(it1.hasNext()) {
            failedCounter++;
            if(failed.get(it1.next()) >= limit) overLimit++;
            underLimit++;
        }

        System.out.println("Total Request: " + (overLimit + underLimit) +
                "\nFailed: " + failedCounter +
                "\nOver Limit: " + overLimit + "(" + ((float)overLimit / (float)(overLimit + underLimit) * 100) + "%)" +
                "\n#Users: " + users * multiplier);

        Unirest.shutdown();
    }

}
