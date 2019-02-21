import com.mashape.unirest.http.HttpResponse;
import com.mashape.unirest.http.Unirest;
import com.mashape.unirest.http.async.Callback;
import com.mashape.unirest.http.exceptions.UnirestException;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

import java.io.InputStream;
import java.util.*;

public class TimedRequest extends TimerTask {
    private String url;
    private JSONArray ja;
    private int multiplier;
    private boolean isDone;
    private int id;
    private Hashtable<Long, Long> requests;
    private Hashtable<Long, Long> failed;

    TimedRequest(int id, String url, JSONArray ja, int multiplier, Hashtable<Long,Long> requests,Hashtable<Long,Long> failed) {
        this.url = url;
        this.ja = ja;
        this.multiplier = multiplier;
        this.isDone = false;
        this.id = id;
        this.requests = requests;
        this.failed = failed;
    }

    public boolean isDone() {
        return isDone;
    }

    public static long encode(int z, int x, int y) {
        assert x >= 0;
        assert y >= y;
        assert x < (1 << z);
        assert y < (1 << z);
        return (1L << (2*z)) | (((long)x) << z) | (y);
    }

    public void run() {
        final Iterator jaIterator = ja.iterator();

        while (jaIterator.hasNext()) {

            final JSONObject tile = (JSONObject) jaIterator.next();

            String finalUrl = this.url.replace("{z}", tile.get("z").toString());
            finalUrl = finalUrl.replace("{x}", tile.get("x").toString());
            finalUrl = finalUrl.replace("{y}", tile.get("y").toString());

            final long tileID = encode(Integer.valueOf(tile.get("z").toString()), Integer.valueOf(tile.get("x").toString()), Integer.valueOf(tile.get("y").toString()));

            final String finalUrl1 = finalUrl;
            final long start = new Date().getTime();
            for(int i = 0; i < multiplier; i++) {
                Unirest.post(finalUrl).asBinaryAsync(new Callback<InputStream>() {
                    public void completed(HttpResponse<InputStream> httpResponse) {
                        long ft = new Date().getTime() - start;
                        //System.out.println(finalUrl1 + ": Done (" + (ft) + ")");
                        synchronized (requests){
                            requests.put(tileID, ft);
                        }
                        if(!jaIterator.hasNext()) isDone = true;
                    }

                    public void failed(UnirestException e) {

                        long ft = new Date().getTime() - start;
                        System.out.println("Failed:" + e.getMessage());
                        synchronized (failed){
                            failed.put(tileID, ft);
                        }
                        if(!jaIterator.hasNext()) isDone = true;

                    }

                    public void cancelled() {

                    }
                });
            }

        }

    }
}
