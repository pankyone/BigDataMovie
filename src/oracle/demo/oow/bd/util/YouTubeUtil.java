package oracle.demo.oow.bd.util;

import java.util.Hashtable;

public class YouTubeUtil {
    private static Hashtable<Integer, String> ht = null;

    public static void setup() {
        if (ht == null) {
            ht = new Hashtable<Integer, String>();
            ht.put(10138, "vM81xWzJAmg"); //Iron Man 2
            ht.put(568, "nEl0NsYn1fU"); //Apollo 13
            ht.put(13448, "bcE8QaKiTGk"); //Angels & Demons
            ht.put(10193, "roADdYWAv4A"); //Toy Story 3
            ht.put(10136, "K44VfaWppLI"); //Big
            ht.put(857, "zwhP5b4tD6g"); //Saving Private Ryan
            ht.put(10191, "Uh0Nb_DPNWk"); //How to Train Your Dragon
            ht.put(10315, "n2igjYFojUo"); //Fantastic Mr. Fox
            ht.put(180, "QH-6UImAP7c"); //Minority Report
            ht.put(330, "NAsME4Wtt6w"); //The Lost World: Jurassic Park
            ht.put(8358, "2TWYDogv4WQ"); //Cast Away
            ht.put(5255, "DwLCXIEfPrU"); //The Polar Express
            ht.put(594, "IqgzXQ3b0nU"); //The Terminal 
        } //if
    } //setup


    public static String getKey(int movieId) {
        //run setup first
        setup();
        
        String youtubeKey = ht.get(movieId);
        if (youtubeKey == null) {
            //default to IronMan trailer
            youtubeKey = ht.get(10138);
        }
        return youtubeKey;
    } //getYouTubeKey

}
