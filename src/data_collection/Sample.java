package data_collection;

import java.text.ParseException;
import java.text.SimpleDateFormat;

public class Sample {
    public static void main(String[] args) throws ParseException {
        SimpleDateFormat fromDate = new SimpleDateFormat("E MMM dd, yyyy");
        SimpleDateFormat toDate = new SimpleDateFormat("dd/MM/yyyy");
        String date = "Sun Apr 05, 2015 01:45 AM";
        System.out.println(toDate.format(fromDate.parse(date)));
        CommunityBreastCancerMiner abc = new CommunityBreastCancerMiner();
        abc.readThread("abc", "https://community.breastcancer.org/forum/83/topics/858196");
        /*String s = "by TurtleTAM on Mon Aug 10, 2009 05:30 AM";
        System.out.println(s.substring(s.indexOf(" on ") + 7));*/
    }
}
