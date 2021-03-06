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
import org.apache.commons.lang.StringUtils;
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

public class HealthBoardsforumsBreastCancerMiner {
    private final String index = "healthboardsbreastcancerforum";
    private final String type = "healthboardsbreastcancerforums";
    private final String baseURL = "https://www.healthboards.com";
    private static SimpleDateFormat fromDate = new SimpleDateFormat("MM-dd-yyyy");
    private static SimpleDateFormat toDate = new SimpleDateFormat("dd/MM/yyyy");
    private final ESBulkFeeder esBulkFeeder = new ESBulkFeeder(index, type, 100);
    private static final Semaphore s = new Semaphore(1);
    private final String threadFileName = "healthboardsbreastcancerforums_forum_threads.csv";
    private final String authorFileName = "healthboardsbreastcancerforums_authors.txt";


    public static void main(String... args) throws Exception {
        HealthBoardsforumsBreastCancerMiner cancerforumsMiner = new HealthBoardsforumsBreastCancerMiner();
        cancerforumsMiner.collectThreadURLS();
        cancerforumsMiner.readThreads();
        cancerforumsMiner.collectAuthorProfiles();
    }

    private void collectAuthorProfiles() throws Exception {
        BufferedReader authorFileReader = new BufferedReader(new FileReader(authorFileName));

        String folderName = "BreastCancerProfiles/" + type;
        File temp = new File(folderName);
        if (!temp.exists()) {
            temp.mkdirs();
        }

        String line;
        while ((line = authorFileReader.readLine()) != null) {
            line = line.trim();
            if (!line.isEmpty()) {
                String authorID = line;

                JSONArray posts = DataCollectionUtils.getAuthoredPosts(authorID, esBulkFeeder.getClient(), index, type);

                if (posts.length() > 0) {
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

    public ForumThread readThread(String title, String threadURL) {
        ForumThread forumThread = new ForumThread(title, threadURL);
        System.out.println(title);
        forumThread.messages = new ArrayList<>();
        int pageNo = 1;


        try {
            String pageURL = threadURL;
            if (pageNo != 1) {
                pageURL.replace(".html", "-" + pageNo + ".html");
            }
            Document doc = Jsoup.connect(pageURL).userAgent("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_9_2) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/33.0.1750.152 Safari/537.36").timeout(10000).ignoreContentType(true).get();

            int nPages = getNumberOfPages(doc);
            System.out.println("No of pages " + nPages);
            int i = 0;

            while (true) {

                Elements messageElements = doc.select("table[id^=post]");

                for (Element messageElement : messageElements) {
                    Element userElement = messageElement.select("div[id^=postmenu]").first();

                    if (userElement != null) {
                        String authorID = userElement.text();
                        String authorURL = baseURL + "/" + userElement.text();
                        String date = getDate(messageElement.select("td[class^=thead]").first().text());

                        String text = messageElement.select("div[id^=post_message]").first().text();
                        Message message = new Message(text, date, i++, authorURL, authorID);

                        forumThread.messages.add(message);
                    }
                }

                System.out.println("Processed: " + pageURL);
                pageNo++;

                if (pageNo > nPages) break;

                pageURL= pageURL.replace(".html", "-" + pageNo + ".html");

                doc = Jsoup.connect(pageURL).userAgent("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_9_2) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/33.0.1750.152 Safari/537.36").timeout(10000).ignoreContentType(true).get();

            }
        } catch (Exception e) {
            e.printStackTrace();
        }


        forumThread.nMessages = forumThread.messages.size();
        return forumThread;
    }

    private int getNumberOfPages(Document doc) {
        Elements element = doc.select("td[class^=vbmenu_control");
        String count = null;
        if (element.size() > 0) {
            if(element.first().text().contains("of")) {
                count = element.first().text().split("of")[1].trim();
            }
        }
        int nPages;
        nPages = 1;
        if (!StringUtils.isEmpty(count)) {
            nPages = Integer.parseInt(count);
        }
        return nPages;
    }

    private String getDate(String postDate) {
        if (postDate.contains("Today") || postDate.contains("minutes") || postDate.contains("hours")) {
            return "05-08-2016, 10:00 aM";
        } else if (postDate.contains("Yesterday")) {
            return "05-07-2016, 10:00 aM";
        } else {
            return postDate;
        }
    }

    private void collectThreadURLS() {
        Map<String, String> threadMap = new HashMap<>();
        try {
            //   new Resty().json("http://localhost:9200/" + index + "/" + type + "/_mapping", Resty.put(Resty.content(DataCollectionUtils.getIndexMapping()))).object();
            CSVPrinter printer = new CSVPrinter(new FileWriter(threadFileName), CSVFormat.RFC4180.withHeader(new String[]{"Title", "Link"}).withDelimiter(','));

            for (int i = 0; i <= 59; i++) {
                String pageURL;
                if (i == 0) {
                    pageURL = baseURL + "/boards/cancer-breast/";
                } else {
                    pageURL = baseURL + "/boards/cancer-breast/index" + i + ".html";
                }
                Document doc = Jsoup.connect(pageURL).userAgent("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_9_2) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/33.0.1750.152 Safari/537.36").timeout(10000).ignoreContentType(true).get();

                Elements threads = doc.select("tbody[id^=threadbits]>tr");
                for (Element thread : threads) {
                    String link = thread.select("a[href]").attr("href");
                    String title = thread.select("a[href]").text();
                    threadMap.put(title, link);
                }

                System.out.println("Processed Page: " + pageURL + " with" +
                        " threads: " + threads.size());
            }

            for (Map.Entry<String, String> entry : threadMap.entrySet()) {
                printer.printRecord(new String[]{entry.getKey(), entry.getValue()});
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
