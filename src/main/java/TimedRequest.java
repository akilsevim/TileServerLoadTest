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
    private Vector<Long> requests;
    private Vector<Long> failed;
    private Vector<Long> bloomed;
    private BloomFilter bloom;
    private boolean bloomEnabled = false;
    private int bloomLevel;

    TimedRequest(int id, String url, JSONArray ja, int multiplier, Vector<Long> requests,Vector<Long> failed) {
        this.url = url;
        this.ja = ja;
        this.multiplier = multiplier;
        this.isDone = false;
        this.id = id;
        this.requests = requests;
        this.failed = failed;
    }
    TimedRequest(int id, String url, JSONArray ja, int multiplier, Vector<Long> requests, Vector<Long> failed, Vector<Long> bloomed, BloomFilter bloom, int bloomLevel) {
        this.url = url;
        this.id = id;
        this.ja = ja;
        this.multiplier = multiplier;
        this.isDone = false;
        this.requests = requests;
        this.failed = failed;
        this.bloomed = bloomed;
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

            final String fu = finalUrl;


            final long tileID = encode(z, x, y);

            if(bloomEnabled) {
                long start = System.nanoTime();
                long tID = tileID;

                if(z > bloomLevel) {
                    tID = findParent(z,x,y,bloomLevel);
                }

                if(!bloom.mightContain(tID)) {
                    long ft = (System.nanoTime() - start) / 1000000;
                    //System.out.println(fu + "\t" + tID + "\t" + tileID);
                    bloomed.add(ft);
                    if(!jaIterator.hasNext()) isDone = true;
                    continue;
                }

            }


            for(int i = 0; i < multiplier; i++) {
                final long start = System.nanoTime();
                final int finalI = i;
                Unirest.post(finalUrl).asBinaryAsync(new Callback<InputStream>() {
                    public void completed(HttpResponse<InputStream> httpResponse) {
                        long ft = (System.nanoTime() - start) / 1000000;


                        requests.add(ft);

                        if(!jaIterator.hasNext()) isDone = true;
                    }

                    public void failed(UnirestException e) {

                        long ft = (System.nanoTime() - start) / 1000000;
                        //System.out.println("Failed:" + e.getMessage());

                        failed.add(ft);
                        //requests.put(id + "-"+ finalI +"-"+tileID, ft);

                        if(!jaIterator.hasNext()) isDone = true;

                    }

                    public void cancelled() {

                    }
                });
            }

        }

    }
}
