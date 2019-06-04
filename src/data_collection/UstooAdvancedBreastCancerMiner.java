/*
 *   This is part of the source code of Patient Reported Information Multidimensional Exploration (PRIME)
 *
 *   This program is free software: you can redistribute it and/or modify
 *   it under the terms of the GNU General Public License as published by
 *   the Free Software Foundation, either version 3 of the License, or
 *   (at your option) any later version.
 *
 *   This program is distributed in the hope that it will be useful,
 *   but WITHOUT ANY WARRANTY; without even the implied warranty of
 *   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *   GNU General Public License for more details.
 *
 *   You should have received a copy of the GNU General Public License
 *   along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package data_collection;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.csv.CSVRecord;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;
import us.monoid.json.JSONArray;
import us.monoid.json.JSONException;
import us.monoid.json.JSONObject;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Semaphore;

public class UstooAdvancedBreastCancerMiner {
    private final String index = "inspireadvancedhealthforum";
    private final String type = "inspireadvancedbreastcancerforums";
    private final String baseURL = "https://www.inspire.com";
    private static SimpleDateFormat fromDate = new SimpleDateFormat("yyyy-MM-dd");
    private static SimpleDateFormat toDate = new SimpleDateFormat("dd/MM/yyyy");
    private final ESBulkFeeder esBulkFeeder = new ESBulkFeeder(index, type, 100);
    private static final Semaphore s = new Semaphore(1);
    private final String threadFileName = "inspireadvancedbreastcancerforums_forum_threads.csv";
    private final String authorFileName = "inspireadvancedbreastcancerforums_authors.txt";


    public static void main(String... args) throws Exception {
        UstooAdvancedBreastCancerMiner cancerforumsMiner = new UstooAdvancedBreastCancerMiner();
        cancerforumsMiner.collectThreadURLS();
        cancerforumsMiner.readThreads();
        cancerforumsMiner.collectAuthorProfiles();
    }

    private void collectAuthorProfiles() throws Exception {
        BufferedReader authorFileReader = new BufferedReader(new FileReader(authorFileName));

        String folderName = "BreastCancerProfiles/" + type;
        File temp = new File(folderName);
        if(!temp.exists())
        {
            temp.mkdirs();
        }

        String line;
        while ((line = authorFileReader.readLine()) != null) {
            line = line.trim();
            if (!line.isEmpty()) {
                String authorID = line;

                JSONArray posts = DataCollectionUtils.getAuthoredPosts(authorID,esBulkFeeder.getClient(),index,type);

                if(posts.length() > 0) {
                    String AuthorURL = posts.getJSONObject(0).getString("AuthorURL");

                    JSONObject profile = new JSONObject();
                    profile.put("Posts", posts);
                    profile.put("AuthorID", authorID);
                    profile.put("AuthorURL", AuthorURL);

                    BufferedWriter writer = new BufferedWriter(new FileWriter(folderName + "/" + util.Utils.removePunctuations(authorID) + ".json"));
                    writer.write(profile.toString(2));
                    writer.close();
                    System.out.println("Author: " + authorID + " processed with " + posts.length() + " posts");
                }
            }
        }
    }


    private void readThreads() {
        try {
            //new Resty().json("http://localhost:9200/" + index + "/" + type + "/_mapping", Resty.put(Resty.content(DataCollectionUtils.getIndexMapping()))).object();
            final Set<String> authors = new HashSet<>();
            BufferedWriter authorFileWriter = new BufferedWriter(new FileWriter(authorFileName));
            CSVParser csvParser = new CSVParser(new FileReader(threadFileName), CSVFormat.RFC4180.withHeader().withDelimiter(','));

            for (final CSVRecord record : csvParser.getRecords()) {

                s.acquire();
                new Thread() {
                    public void run() {
                        try {
                            ForumThread forumThread = readThread(record.get(0), record.get(1));
                            feedToES(forumThread);

                            synchronized (this) {
                                for (Message m : forumThread.messages) {
                                    authors.add(m.getAuthorID());
                                }
                            }

                        } catch (Exception e) {
                            e.printStackTrace();
                        } finally {
                            s.release();
                        }
                    }
                }.start();

            }

            synchronized (this) {
                esBulkFeeder.flushBulk();

                for (String author : authors) {
                    authorFileWriter.write(author + "\n");
                }

                authorFileWriter.close();
            }
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    private ForumThread readThread(String title, String threadURL) {
        ForumThread forumThread = new ForumThread(title, threadURL);
        forumThread.messages = new ArrayList<>();
        int pageNo = 1;



        try {
            String pageURL = null;
            if(pageNo == 1) {
               pageURL = threadURL;
            }
            Document doc = Jsoup.connect(pageURL).userAgent("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_9_2) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/33.0.1750.152 Safari/537.36").timeout(10000).ignoreContentType(true).get();

            int nPages = getNumberOfPages(doc);
            int i = 0;

            while (true) {
                Elements messageElements = doc.select("article");


                for (Element messageElement : messageElements) {
                    Element userElement = messageElement.select("div[class^=username]").first();

                    if(userElement != null) {
                        String authorID = userElement.select("a").text();
                        String authorURL = baseURL + userElement.select("a").attr("href");
                        String date = messageElement.select("time").attr("datetime");

                        String text = messageElement.select("div[class=post-content]").text();
                        Message message = new Message(text, date, i++, authorURL, authorID);

                        forumThread.messages.add(message);
                    }
                    break;
                }

                messageElements = doc.select("div[class^=replies]>article");


                for (Element messageElement : messageElements) {
                    Element userElement = messageElement.select("div[class^=username]").first();

                    if(userElement != null) {
                        String authorID = userElement.text();
                        String authorURL = baseURL + userElement.select("a").attr("href");
                        String date = messageElement.select("time").attr("datetime");

                        String text = messageElement.select("div[class=post-content]").text();
                        Message message = new Message(text, date, i++, authorURL, authorID);

                        forumThread.messages.add(message);
                    }
                }


                System.out.println("Processed: "+ pageURL);
                pageNo++;
                if(pageNo > nPages) break;

                pageURL = threadURL.substring(0, threadURL.lastIndexOf(",")) + "," + pageNo + ".htm";
                doc = Jsoup.connect(pageURL).userAgent("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_9_2) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/33.0.1750.152 Safari/537.36").timeout(10000).ignoreContentType(true).get();

            }
        } catch (Exception e) {
            e.printStackTrace();
        }


        forumThread.nMessages = forumThread.messages.size();
        return forumThread;
    }

    private int getNumberOfPages(Document doc) {
        Elements elements = doc.select("span[id=MainContent_ContentPlaceHolder1_messageList1_dtpTop]>a");
        int nPages = 1;
        if(elements.size()>0) {
            nPages=elements.size();
        }
        return nPages;
    }

    private String getDate(String postDate) {
        if(postDate.contains("Today") || postDate.contains("minutes") || postDate.contains("hours"))
        {
            return "05-08-2016, 10:00 aM";
        }else if(postDate.contains("Yesterday"))
        {
            return "05-07-2016, 10:00 aM";
        }else
        {
           return postDate;
        }
    }

    private void collectThreadURLS() {
        Map<String,String> threadMap = new HashMap<>();
        try {
         //   new Resty().json("http://localhost:9200/" + index + "/" + type + "/_mapping", Resty.put(Resty.content(DataCollectionUtils.getIndexMapping()))).object();
            CSVPrinter printer = new CSVPrinter(new FileWriter(threadFileName), CSVFormat.RFC4180.withHeader(new String[]{"Title", "Link"}).withDelimiter(','));

            for (int i = 0; i <= 10; i++) {
                String pageURL = baseURL + "/groups/advanced-breast-cancer/?page="+i;

                Document doc = Jsoup.connect(pageURL).userAgent("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_9_2) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/33.0.1750.152 Safari/537.36").timeout(10000).ignoreContentType(true).get();

                Elements threads = doc.select("article[class^=post]");
                for (Element thread : threads) {
                    Element element = thread.select("a").get(0);
                    String link = baseURL + element.attr("href");
                    String title =  element.text();
                    System.out.println("URL " + link + " title " + title);
                    threadMap.put(title, link);
                }

                System.out.println("Processed Page: " + pageURL + " with threads: " + threads.size());
            }

            for(Map.Entry<String,String> entry: threadMap.entrySet()){
                printer.printRecord(new String[]{entry.getKey(),entry.getValue()});
            }

            printer.close();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void feedToES(ForumThread forumThread) throws JSONException, ParseException {
        synchronized (this) {
            String baseID = forumThread.url.replaceFirst("http://", "").trim();
            for (Message message : forumThread.messages) {
                JSONObject messageJSON = new JSONObject();
                messageJSON.put("Title", forumThread.title);
                messageJSON.put("Content", message.getText());
                messageJSON.put("MessageIndex", message.getIndex());
                messageJSON.put("ThreadURL", forumThread.url);
                messageJSON.put("PostDate", toDate.format(fromDate.parse(message.getDate())));
                messageJSON.put("AuthorID", message.getAuthorID());
                messageJSON.put("AuthorURL", baseURL + message.getAuthorURL());
                messageJSON.put("Signature", message.getData("signature"));

                esBulkFeeder.feedToES(baseID + "_" + message.getIndex(), messageJSON);
            }
        }
    }
}
