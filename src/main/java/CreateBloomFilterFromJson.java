import com.google.common.hash.BloomFilter;
import com.google.common.hash.Funnels;
import org.json.simple.JSONArray;
import org.json.simple.parser.*;

import java.io.*;

public class CreateBloomFilterFromJson {
    public static void main(String[] args) {

        BloomFilter<Long> bloomFilter = BloomFilter.create(
                Funnels.longFunnel(),
                50000,
                0.01);

        Object ids_o = null;

        try {
            ids_o = new JSONParser().parse(new FileReader("ebd-empty-tiles.json"));
        } catch (ParseException e) {
            e.printStackTrace();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }

        JSONArray ids = (JSONArray) ids_o;


        for (Object idx:ids
             ) {
            bloomFilter.put((Long) idx);
        }


        try {
            PrintStream outb;
            outb = new PrintStream(new File("ebd_bloom_filter_10"));
            bloomFilter.writeTo(outb);
            outb.close();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }


    }
}
