import com.google.common.hash.BloomFilter;
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
    private Hashtable<String, Long> requests;
    private Hashtable<String, Long> failed;
    private BloomFilter bloom;
    private boolean bloomEnabled = false;
    private int bloomLevel;

    TimedRequest(int id, String url, JSONArray ja, int multiplier, Hashtable<String,Long> requests,Hashtable<String,Long> failed) {
        this.url = url;
        this.ja = ja;
        this.multiplier = multiplier;
        this.isDone = false;
        this.id = id;
        this.requests = requests;
        this.failed = failed;
    }
    TimedRequest(int id, String url, JSONArray ja, int multiplier, Hashtable<String,Long> requests, Hashtable<String,Long> failed, BloomFilter bloom, int bloomLevel) {
        this.url = url;
        this.id = id;
        this.ja = ja;
        this.multiplier = multiplier;
        this.isDone = false;
        this.requests = requests;
        this.failed = failed;
        this.bloom = bloom;
        this.bloomEnabled = true;
        this.bloomLevel = bloomLevel;
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

    public long findParent(int z, int x, int y, int level) {
        while(z > level) {
            x /= 2;
            y /= 2;
            z--;
        }
        return encode(z,x,y);
    }

    public void run() {
        final Iterator jaIterator = ja.iterator();

        while (jaIterator.hasNext()) {

            final JSONObject tile = (JSONObject) jaIterator.next();

            Integer z = Integer.valueOf(tile.get("z").toString());
            Integer x = Integer.valueOf(tile.get("x").toString());
            Integer y = Integer.valueOf(tile.get("y").toString());

            String finalUrl = this.url.replace("{z}", z.toString());
            finalUrl = finalUrl.replace("{x}", x.toString());
            finalUrl = finalUrl.replace("{y}", y.toString());


            final long tileID = encode(z, x, y);

            if(bloomEnabled) {
                long tID = tileID;

                if(z > bloomLevel) {
                    tID = findParent(z,x,y,bloomLevel);
                }

                if(!bloom.mightContain(tID)) {
                    if(!jaIterator.hasNext()) isDone = true;
                    continue;
                }

            }

            final String finalUrl1 = finalUrl;
            final long start = new Date().getTime();
            for(int i = 0; i < multiplier; i++) {
                final int finalI = i;
                Unirest.post(finalUrl).asBinaryAsync(new Callback<InputStream>() {
                    public void completed(HttpResponse<InputStream> httpResponse) {
                        long ft = new Date().getTime() - start;
                        //System.out.println(finalUrl1 + ": Done (" + (ft) + ")");

                        requests.put(id + "-"+ finalI +"-"+tileID, ft);

                        if(!jaIterator.hasNext()) isDone = true;
                    }

                    public void failed(UnirestException e) {

                        long ft = new Date().getTime() - start;
                        //System.out.println("Failed:" + e.getMessage());

                        failed.put(id + "-"+ finalI +"-"+tileID, ft);
                        requests.put(id + "-"+ finalI +"-"+tileID, ft);

                        if(!jaIterator.hasNext()) isDone = true;

                    }

                    public void cancelled() {

                    }
                });
            }

        }

    }
}
