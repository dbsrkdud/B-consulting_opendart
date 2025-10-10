package api;

import com.google.gson.*;
import org.json.JSONArray;
import org.json.JSONObject;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import java.io.*;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLEncoder;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

public class G2B {

    public static void main(String[] args) throws IOException {

        // readJson();

        // 기업 고유번호 조회
        // JSONArray corpCodeJsonArr = retrieveCorpCode();

        // 기업개황 데이터 json 파일로 저장
        // writeJson();

        JsonParser jsonParser = new JsonParser();
        FileReader reader = new FileReader("companyInfo.json");
        JsonElement element = jsonParser.parse(reader);

        System.out.println();


    }

    // 나라장터 입찰공고 조회 - 공공데이터포털
    public static void getDataSetOpnStdBidPblancInfo () throws IOException {
        StringBuilder urlBuilder = new StringBuilder("http://apis.data.go.kr/1230000/ao/PubDataOpnStdService/getDataSetOpnStdBidPblancInfo"); /*URL*/
        urlBuilder.append("?" + URLEncoder.encode("serviceKey","UTF-8") + "=odXIjL8t56rG0Fz1a69qgTTpLKxSPvIJG%2FlPU3bFsLOjdSAKcwHy8Wx0OCox8vLCtZEl6B9Jw%2BlWyoMylWEwsg%3D%3D"); /*Service Key*/
        urlBuilder.append("&" + URLEncoder.encode("bidNtceBgnDt","UTF-8") + "=" + URLEncoder.encode("202508050000", "UTF-8"));
        urlBuilder.append("&" + URLEncoder.encode("bidNtceEndDt","UTF-8") + "=" + URLEncoder.encode("202509042359", "UTF-8"));
        urlBuilder.append("&" + URLEncoder.encode("numOfRows","UTF-8") + "=" + URLEncoder.encode("999", "UTF-8"));
        urlBuilder.append("&" + URLEncoder.encode("pageNo","UTF-8") + "=" + URLEncoder.encode("35", "UTF-8"));
        urlBuilder.append("&" + URLEncoder.encode("type","UTF-8") + "=" + URLEncoder.encode("json", "UTF-8"));

        URL url = new URL(urlBuilder.toString());
        HttpURLConnection conn = (HttpURLConnection) url.openConnection();
        conn.setRequestMethod("GET");
        conn.setRequestProperty("Content-type", "application/json");
        System.out.println("Response code: " + conn.getResponseCode());
        BufferedReader rd;
        if(conn.getResponseCode() >= 200 && conn.getResponseCode() <= 300) {
            rd = new BufferedReader(new InputStreamReader(conn.getInputStream()));
        } else {
            rd = new BufferedReader(new InputStreamReader(conn.getErrorStream()));
        }

        StringBuilder sb = new StringBuilder();
        String line;
        while ((line = rd.readLine()) != null) {
            sb.append(line);
        }
        rd.close();
        conn.disconnect();
//        System.out.println(sb.toString());
//        System.out.println("=======================================================================\n");

        /*
        JsonObject root = JsonParser.parseString(sb.toString()).getAsJsonObject();
        JsonArray items = root.getAsJsonObject("response").getAsJsonObject("body").getAsJsonArray("items");

        int index = 0;
        for (JsonElement element : items) {

            JsonObject item = element.getAsJsonObject();
            String bsnsDivNm = item.get("bsnsDivNm").getAsString();

            if (bsnsDivNm.equals("용역") || bsnsDivNm.equals("용역")) {
                System.out.println(bsnsDivNm);
            }
            index++;
        }
         */

        JsonObject root = JsonParser.parseString(sb.toString()).getAsJsonObject();
        JsonArray items = root.getAsJsonObject("response").getAsJsonObject("body").getAsJsonArray("items");

        List<JsonObject> list = new ArrayList<>();
        items.forEach(e -> list.add(e.getAsJsonObject()));

        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm");

        list.sort((o1, o2) -> {
            String date1 = o1.has("bidNtceDate") ? o1.get("bidNtceDate").getAsString() : "";
            String time1 = o1.has("bidNtceBgn") ? o1.get("bidNtceBgn").getAsString() : "00:00";

            String date2 = o2.has("bidNtceDate") ? o2.get("bidNtceDate").getAsString() : "";
            String time2 = o2.has("bidNtceBgn") ? o2.get("bidNtceBgn").getAsString() : "00:00";

            if (date1.isEmpty() && date2.isEmpty()) return 0;
            if (date1.isEmpty()) return 1;
            if (date2.isEmpty()) return -1;

            LocalDateTime dt1 = LocalDateTime.parse(date1 + " " + time1, formatter);
            LocalDateTime dt2 = LocalDateTime.parse(date2 + " " + time2, formatter);

            return dt2.compareTo(dt1);
        });
    }

    // a1ab4687628095bbae0fd90f4c34c9c897fda441

    // 고유번호 - 오픈다트
    public static JSONArray retrieveCorpCode() throws IOException {
        String crtfc_key = "a1ab4687628095bbae0fd90f4c34c9c897fda441";
        String zipFilePath = "corpCode.zip";
        String xmlFilePath = "corpCode.xml";
        String jsonFilePath = "corpCode.json";

        JSONArray jsonArr = new JSONArray();

        try {
            // 1. API URL 생성
            String encodedKey = URLEncoder.encode(crtfc_key, "UTF-8");
            String apiUrl = "https://opendart.fss.or.kr/api/corpCode.xml?crtfc_key=" + encodedKey;

            URL url = new URL(apiUrl);
            HttpURLConnection conn = (HttpURLConnection) url.openConnection();
            conn.setRequestMethod("GET");

            int responseCode = conn.getResponseCode();
            if (responseCode == HttpURLConnection.HTTP_OK) {
                // 2. ZIP 파일 다운로드
                try (BufferedInputStream bis = new BufferedInputStream(conn.getInputStream());
                     FileOutputStream fos = new FileOutputStream(zipFilePath)) {

                    byte[] buffer = new byte[4096];
                    int bytesRead;
                    while((bytesRead = bis.read(buffer)) != -1){
                        fos.write(buffer, 0, bytesRead);
                    }

                    System.out.println("전체 기업코드 XML 파일 다운로드 완료 : " + zipFilePath);
                }
            } else {
                System.out.println("HTTP 요청 실패, 코드 : " + responseCode);
            }

            conn.disconnect();
            
            // 3. 압축 해제 (현재 디렉토리)
            try (ZipInputStream zis = new ZipInputStream(new FileInputStream(zipFilePath))) {
                ZipEntry entry;
                while ((entry = zis.getNextEntry()) != null) {
                    String outPath = entry.getName(); // ZIP 안 파일 이름 그대로 사용

                    try (FileOutputStream fos = new FileOutputStream(outPath)) {
                        byte[] buffer = new byte[4096];
                        int bytesRead;
                        while ((bytesRead = zis.read(buffer)) != -1) {
                            fos.write(buffer, 0, bytesRead);
                        }
                    }
                    zis.closeEntry();
                    System.out.println("압축 해제 완료 : " + outPath);
                }
            }

            // 4. zip 파일 삭제
            File zipFile = new File(zipFilePath);
            if (zipFile.exists() && zipFile.delete()) {
                System.out.println("ZIP 파일 삭제 완료 : " + zipFilePath);
            } else {
                System.out.println("ZIP 파일 삭제 실패 : " + zipFilePath);
            }

            // 5. xml 파일 파싱, json 파일 생성
            File xmlFile = new File(xmlFilePath);
            DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
            DocumentBuilder db = dbf.newDocumentBuilder();
            Document doc = db.parse(xmlFile);
            doc.getDocumentElement().normalize();

            NodeList list = doc.getElementsByTagName("list");
            // JSONArray jsonArr = new JSONArray();

            for (int i = 0; i < list.getLength(); i++) {
                Element ele = (Element) list.item(i);

                JSONObject obj = new JSONObject();
                obj.put("corp_code", ele.getElementsByTagName("corp_code").item(0).getTextContent().trim());
                obj.put("corp_name", ele.getElementsByTagName("corp_name").item(0).getTextContent().trim());
                obj.put("corp_eng_name", ele.getElementsByTagName("corp_eng_name").item(0).getTextContent().trim());
                obj.put("stock_code", ele.getElementsByTagName("stock_code").item(0).getTextContent().trim());
                obj.put("modify_date", ele.getElementsByTagName("modify_date").item(0).getTextContent().trim());

                jsonArr.put(obj);
            }

            try (FileWriter fw = new FileWriter(jsonFilePath)) {
                fw.write(jsonArr.toString(2));
            }

            System.out.println("JSON 변환 완료 : " + jsonFilePath);

            // 6. xml 파일 삭제
            if (xmlFile.exists() && xmlFile.delete()) {
                System.out.println("XML 파일 삭제 완료 : " + xmlFilePath);
            } else {
                System.out.println("XML 파일 삭제 실패 : " + xmlFilePath);
            }

        } catch(Exception e) {
            e.printStackTrace();
        }

        return jsonArr;
    }

    // 기업개황 - 오픈다트
    public static JSONObject retrieveCompany(String corpCode) throws IOException {
        String crtfc_key = "a1ab4687628095bbae0fd90f4c34c9c897fda441";

        // 1. API URL 생성
        String encodedKey = URLEncoder.encode(crtfc_key, "UTF-8");
        String apiUrl = "https://opendart.fss.or.kr/api/company.json?crtfc_key=" + encodedKey + "&corp_code=" + corpCode;

        URL url = new URL(apiUrl);
        HttpURLConnection conn = (HttpURLConnection) url.openConnection();
        conn.setRequestMethod("GET");

        int responseCode = conn.getResponseCode();
        if (responseCode == HttpURLConnection.HTTP_OK) {
            // 응답(InputStream) 읽기
            BufferedReader rd = new BufferedReader(new InputStreamReader(conn.getInputStream(), "UTF-8"));
            StringBuilder sb = new StringBuilder();
            String line;

            while ((line = rd.readLine()) != null) {
                sb.append(line);
            }
            rd.close();
            conn.disconnect();
            // System.out.println(sb.toString());

            // json 형태로 리턴
            return new JSONObject(sb.toString());

        } else {
            // System.out.println("HTTP 요청 실패, 코드 : " + responseCode);
            throw new IOException("HTTP 요청 실패, 코드 : " + responseCode);
        }
    }

    // 기업개황 데이터 json 파일로 저장
    public static void writeJson() throws IOException {

        JsonParser jsonParser = new JsonParser();

        FileReader reader = new FileReader("corpCode.json");
        JsonElement element = jsonParser.parse(reader);
        JsonArray corpCodeJsonArr = element.getAsJsonArray();

        // 기업 정보를 저장하기 위한 JSONArray
        JSONArray companyJsonArr = new JSONArray();

        int total =  corpCodeJsonArr.size();
        System.out.println("총 기업 수  : " + total);

        int batchSize = 113910;
        // API 차단 방지용
        int delayMillis = 600;

        for (int i = 95000; i < batchSize; i++){
            String corp_code = corpCodeJsonArr.get(i).getAsJsonObject().get("corp_code").getAsString();
            String corp_eng_name = corpCodeJsonArr.get(i).getAsJsonObject().get("corp_eng_name").getAsString();
            String corp_name = corpCodeJsonArr.get(i).getAsJsonObject().get("corp_name").getAsString();
            String stock_code = corpCodeJsonArr.get(i).getAsJsonObject().get("stock_code").getAsString();
            String modify_date = corpCodeJsonArr.get(i).getAsJsonObject().get("modify_date").getAsString();

            try {
                JSONObject companyInfo = (JSONObject) retrieveCompany(corp_code);

                companyJsonArr.put(companyInfo);

                System.out.println(i + " / " + batchSize + " 건 처리 완료");

                TimeUnit.MILLISECONDS.sleep(delayMillis);

            } catch (IOException | InterruptedException e) {
                throw new RuntimeException(e);
            }

        };

        FileWriter writer = new FileWriter("company_10.json");
        writer.write(companyJsonArr.toString());
        writer.flush();
        writer.close();
    }

    // 기업개황 json 파일 묶어서 하나의 json으로 저장
    public static void readJson() throws IOException {

        JsonArray companyJsonArr = new JsonArray();

        for (int i = 1; i <= 10; i++) {
            JsonParser jsonParser = new JsonParser();
            Reader reader = new FileReader("company_" + i + ".json");

            Object obj = jsonParser.parse(reader);
            JsonArray companyInfo = (JsonArray) obj;

            for (int j = 0; j < companyInfo.size(); j++) {
                companyJsonArr.add(companyInfo.get(j));
            }
        }

        System.out.println();

        FileWriter writer = new FileWriter("companyInfo.json");
        writer.write(companyJsonArr.toString());
        writer.flush();
        writer.close();


    }

}


