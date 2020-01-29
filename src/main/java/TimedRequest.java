import com.mashape.unirest.http.HttpResponse;
import com.mashape.unirest.http.Unirest;
import com.mashape.unirest.http.async.Callback;
import com.mashape.unirest.http.exceptions.UnirestException;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

import java.io.InputStream;
import java.util.*;

public class TimedRequest extends TimerTask {
    public String url;
    private String tilePattern;
    private JSONArray ja;
    private int multiplier;
    private boolean isDone;
    private int id;
    private Vector<Long> requests;
    private Vector<Long> bloomed;
    private Vector<Long> statics;
    private Vector<Long> times;
    private BloomFilter bloom;
    private boolean bloomEnabled = false;
    private boolean useHQ = false;
    private int bloomLevel;

    private boolean staticEnabled = false;
    private JSONArray staticTileIDs;
    private String staticServer;

    TimedRequest(int id, String url, JSONArray ja, int multiplier, Vector<Long> requests) {
        this.url = url;
        this.ja = ja;
        this.multiplier = multiplier;
        this.isDone = false;
        this.id = id;
        this.requests = requests;
    }
    TimedRequest(int id, String url, String tilePattern, JSONArray ja, int multiplier, Vector<Long> requests, boolean useBloom, Vector<Long> bloomed, BloomFilter bloom, int bloomLevel, boolean useHQ, Vector<Long> staticCatched, JSONArray staticTiles, String staticServer, Vector<Long> times) {
        this.url = url;
        this.id = id;
        this.ja = ja;
        this.multiplier = multiplier;
        this.isDone = false;
        this.requests = requests;
        this.tilePattern = tilePattern;

        this.bloomEnabled = useBloom;
        this.useHQ = useHQ;
        this.bloomed = bloomed;
        this.bloom = bloom;
        this.bloomLevel = bloomLevel;

        if(staticTiles.size() != 0) this.staticEnabled = true;
        this.staticTileIDs = staticTiles;
        this.statics = staticCatched;
        this.staticServer = staticServer;

        this.times = times;
    }

    TimedRequest(int id, String url, String tilePattern, JSONArray ja, int multiplier, Vector<Long> requests, boolean useBloom, Vector<Long> staticCatched, JSONArray staticTiles, String staticServer, Vector<Long> times) {
        this.url = url;
        this.id = id;
        this.ja = ja;
        this.multiplier = multiplier;
        this.isDone = false;
        this.requests = requests;
        this.tilePattern = tilePattern;

        this.bloomEnabled = useBloom;

        if(staticTiles.size() != 0) this.staticEnabled = true;
        this.staticTileIDs = staticTiles;
        this.statics = staticCatched;
        this.staticServer = staticServer;

        this.times = times;
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

            int z = Integer.parseInt(tile.get("z").toString());
            int x = Integer.valueOf(tile.get("x").toString());
            int y = Integer.valueOf(tile.get("y").toString());

            final long tileID = encode(z, x, y);

            if(bloomEnabled) {
                long start = System.nanoTime();
                long tID = tileID;

                if(z > bloomLevel && useHQ) {
                    tID = findParent(z,x,y,bloomLevel);
                } else if (z > bloomLevel) {
                    continue;
                }

                if(!bloom.mightContain(tID)) {
                    long ft = (System.nanoTime() - start) / 1000000;
                    //System.out.println(tileID + ":" +(System.nanoTime() - start));
                    bloomed.add(ft);
                    if(!jaIterator.hasNext()) isDone = true;
                    continue;
                }
            }

            String finalTileFile = this.tilePattern.replace("{z}", String.valueOf(z));
            finalTileFile = finalTileFile.replace("{x}", String.valueOf(x));
            finalTileFile = finalTileFile.replace("{y}", String.valueOf(y));

            String finalUrl = this.url + finalTileFile;

            boolean staticFound = false;

            if(staticEnabled) {
                //long start = System.nanoTime();
                if(staticTileIDs.contains(tileID)) {
                    //long ft = (System.nanoTime() - start) / 1000000;
                    //System.out.println(tileID + ":" + ft);
                    //statics.add(ft);
                    //if(!jaIterator.hasNext()) isDone = true;
                    //continue;
                    staticFound = true;
                    finalUrl = this.staticServer + finalTileFile;
                }
            }

            final boolean sf = staticFound;
            final String fu = finalUrl;


            for(int i = 0; i < multiplier; i++) {
                final long start = System.nanoTime();
                try {
                    Unirest.post(finalUrl).asBinaryAsync(new Callback<InputStream>() {
                        public void completed(HttpResponse<InputStream> httpResponse) {
                            //System.out.println("Completed:" + fu);
                            long ft = (System.nanoTime() - start) / 1000000;
                            if (sf) statics.add(ft);
                            else requests.add(ft);

                            if (!jaIterator.hasNext()) isDone = true;
                        }

                        public void failed(UnirestException e) {
                            //System.out.println("Failed:" + fu);
                            long ft = (System.nanoTime() - start) / 1000000;
                            //System.out.println("Failed:" + e.getMessage());

                            if (sf) statics.add(ft);
                            else requests.add(ft);

                            //requests.put(id + "-"+ finalI +"-"+tileID, ft);

                            if (!jaIterator.hasNext()) isDone = true;

                        }

                        public void cancelled() {

                        }
                    });
                } catch (Exception e) {
                    System.out.println(fu + ":" +e.getMessage());
                }
            }

        }

    }
}
