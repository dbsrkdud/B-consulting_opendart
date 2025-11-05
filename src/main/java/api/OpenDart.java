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
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

public class OpenDart {

    public static void main(String[] args) throws IOException {

        // 1. 기업 고유번호 조회 -> 수집 결과 파일 : corpCode.json
        // retrieveCorpCode();

        // 2. 기업개황 데이터 수집 -> 파라미터 : 기업고유번호 json 파일
        // setCompanyJsonArr("corpCode.json");

        // 3. 새로운 기업고유번호에 대한 기업개황 데이터 수집 -> 수집 결과 파일 : newCorpCode.json
        // setnewCorpCode();

        // 4. 새로 추가 될 기업고유번호에 대한 기업개황 데이터 수집
        setCompanyJsonArr("newCorpCode.json");

        // 두 개의 json 파일 합쳐서 하나의 json 파일로 저장
        joinJsonArray("companyInfo.json", "newCompanyInfo.json");

        JsonParser jsonParser = new JsonParser();

        FileReader reader = new FileReader("newCorpCode.json");
        JsonElement element = jsonParser.parse(reader);
        JsonArray jsonArr = element.getAsJsonArray();

        System.out.println();

        // json -> csv 변환
        convertJsonToCsv("resultCompanyInfo");

    }

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
    public static void setCompanyJsonArr(String fileName) throws IOException {

        JsonParser jsonParser = new JsonParser();

        FileReader reader = new FileReader(fileName);
        JsonElement element = jsonParser.parse(reader);
        JsonArray corpCodeJsonArr = element.getAsJsonArray();

        // 기업 정보를 저장하기 위한 JSONArray
        JSONArray companyJsonArr = new JSONArray();

        int total =  corpCodeJsonArr.size();
        System.out.println("총 기업 수  : " + total);

        // 오픈 다트 하루 수집량 10,000건으로 수집한 기업고유번호 데이터 갯수에 따른 조정 필요 (corpCodeJsonArr)
        int batchSize = corpCodeJsonArr.size();
        // API 차단 방지용
        int delayMillis = 600;

        for (int i = 0; i < batchSize; i++){
            String corp_code = corpCodeJsonArr.get(i).getAsJsonObject().get("corp_code").getAsString();
//            String corp_eng_name = corpCodeJsonArr.get(i).getAsJsonObject().get("corp_eng_name").getAsString();
//            String corp_name = corpCodeJsonArr.get(i).getAsJsonObject().get("corp_name").getAsString();
//            String stock_code = corpCodeJsonArr.get(i).getAsJsonObject().get("stock_code").getAsString();
//            String modify_date = corpCodeJsonArr.get(i).getAsJsonObject().get("modify_date").getAsString();

            try {
                // 기업개황 API
                JSONObject companyInfo = (JSONObject) retrieveCompany(corp_code);

                companyJsonArr.put(companyInfo);

                System.out.println(i + 1 + " / " + batchSize + " 건 처리 완료");

                TimeUnit.MILLISECONDS.sleep(delayMillis);

            } catch (IOException | InterruptedException e) {
                throw new RuntimeException(e);
            }

        };

        // 기업개황 데이터 수집 결과 저장할 json 파일명
        String resultFileName = "newCompanyInfo.json";

        FileWriter writer = new FileWriter(resultFileName);
        writer.write(companyJsonArr.toString());
        writer.flush();
        writer.close();
    }

    // 두개의 json 파일 묶어서 하나의 json 파일로 저장
    public static void joinJsonArray(String fileNameA, String fileNameB) throws IOException {

        JsonArray resultJson = new JsonArray();

        JsonParser jsonParser = new JsonParser();
        FileReader readerA = new FileReader(fileNameA);

        Object objA = jsonParser.parse(readerA);
        JsonArray jsonArrayA = (JsonArray) objA;

        for (int i = 0; i < jsonArrayA.size(); i++) {
            resultJson.add(jsonArrayA.get(i));
        }

        FileReader readerB = new FileReader(fileNameB);

        Object objB = jsonParser.parse(readerB);
        JsonArray jsonArrayB = (JsonArray) objB;

        for (int i = 0; i < jsonArrayB.size(); i++) {
            resultJson.add(jsonArrayB.get(i));
        }

        String resultFileName = "resultCompanyInfo.json";

        FileWriter writer = new FileWriter(resultFileName);
        writer.write(resultJson.toString());
        writer.flush();
        writer.close();

    }

    // 새로운 기업고유번호에 대한 기업개황 데이터 수집
    public static void setnewCorpCode() throws IOException {

        // 오픈 다트 기업고유번호 수집 데이터 json 파일
        JsonParser corpParser = new JsonParser();
        FileReader corpReader = new FileReader("corpCode.json");
        JsonElement corpElement = corpParser.parse(corpReader);
        JsonArray corpJsonArr = corpElement.getAsJsonArray();

        // 기존에 수집했던 기업개황 데이터 json 파일
        JsonParser companyInfoParser = new JsonParser();
        FileReader companyInfoReader = new FileReader("companyInfo.json");
        JsonElement companyInfoElement = companyInfoParser.parse(companyInfoReader);
        JsonArray companyInfoJsonArr = companyInfoElement.getAsJsonArray();

        // HashSet 사용하는 이유 : 중복 허용 X, 탐색 속도 ↑, 집합 연산 지원
        Set<String> corpSet = new HashSet<>();
        Set<String> companyInfoSet = new HashSet<>();

        for (JsonElement el : corpJsonArr) {
            JsonObject corpObj = el.getAsJsonObject();
            if (corpObj.has("corp_code")) {
                corpSet.add(corpObj.get("corp_code").getAsString());
            }
        }

        for (JsonElement el : companyInfoJsonArr) {
            JsonObject companyInfoObj = el.getAsJsonObject();
            if (companyInfoObj.has("corp_code")) {
                companyInfoSet.add(companyInfoObj.get("corp_code").getAsString());
            }
        }

        // corpSet 에는 있고 companyInfoSet 에는 없는 corpCode 만 저장 : 차집합 연산 (새로운 기업고유번호만 저장)
        Set<String> newCorpSet = new HashSet<>(corpSet);
        newCorpSet.removeAll(companyInfoSet);

        // String date = "20250915";

//        for (JsonElement el : corpJsonArr) {
//            JsonObject corpObj = el.getAsJsonObject();
//            String modifyDate = corpObj.get("modify_date").getAsString();
//
//            if (modifyDate.compareTo(date) >= 0) {
//                newCorpSet.add(corpObj.get("corp_code").getAsString());
//            }
//        }

        // HashSet -> JsonArray 변환
        JsonArray newCorpJsonArr = new JsonArray();
        for (String corpCode : newCorpSet) {
            JsonObject corpObj = new JsonObject();
            corpObj.addProperty("corp_code", corpCode);
            newCorpJsonArr.add(corpObj);
        }

        FileWriter writer = new FileWriter("newCorpCode.json");
        writer.write(newCorpJsonArr.toString());
        writer.flush();
        writer.close();

    }

    public static void convertJsonToCsv(String fileName) throws IOException {

        JsonParser jsonParser = new JsonParser();
        FileReader reader = new FileReader(fileName + ".json");
        JsonElement element = jsonParser.parse(reader);
        JsonArray jsonArr = element.getAsJsonArray();
        JsonObject corpObj = jsonArr.get(0).getAsJsonObject();

        BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(fileName + ".csv"), StandardCharsets.UTF_8));

        ArrayList<String> keyArr =  new ArrayList<>();
        Set<String> Keys = corpObj.keySet();
        int idx = 0;

        for (String key : Keys) {
            keyArr.add(key);
            writer.write(key);
            if (++idx < Keys.size()) {
                writer.write(",");
            }
        }

        writer.write("\n");

        for (JsonElement jsonElement : jsonArr) {
            idx = 0;
            for (int i = 0; i < keyArr.size(); i++) {
                writer.write(jsonElement.getAsJsonObject().get(keyArr.get(i)).toString());
                if (++idx < Keys.size()) {
                    writer.write(",");
                }
            }
            writer.write("\n");
        }

        writer.close();

    }

}


