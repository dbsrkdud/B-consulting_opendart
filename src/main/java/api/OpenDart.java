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
import java.sql.*;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.Date;
import java.util.concurrent.TimeUnit;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

public class OpenDart {

    public static void main(String[] args) throws IOException, ClassNotFoundException {

        // 1. 기업 고유번호 조회 -> 수집 결과 파일 : corpCode.json
        retrieveCorpCode();

        // 2. 기업개황 데이터 수집 -> 파라미터 : 기업고유번호 json 파일
        // setCompanyJsonArr("corpCode.json");

        // 3. 새로운 기업고유번호에 대한 기업개황 데이터 수집 -> 수집 결과 파일 : newCorpCode.json
        setnewCorpCode();

        // 4. 새로 추가 될 기업고유번호에 대한 기업개황 데이터 수집
        setCompanyJsonArr("newCorpCode.json");

        // 두 개의 json 파일 합쳐서 하나의 json 파일로 저장 (corp_code가 중복일 시 새로운 기업개황 정보로 저장)
        joinJsonArray("resultCompanyInfo.json", "newCompanyInfo.json");

        // json -> csv 변환 : 기업고유번호
        convertJsonToCsv("corpCode");

        // json -> csv 변환 : 기업개황
        convertJsonToCsv("resultCompanyInfo");

        // 기업고유번호 MERGE
        insertCorpCode();

        // 기업개황 MERGE
        insertCompanyInfo();

//        JsonParser jsonParser = new JsonParser();
//
//        Reader reader = new InputStreamReader(new FileInputStream("C:\\Users\\admin\\Desktop\\B-consulting_opendart\\newCorpCode.json"), "UTF-8");
//        JsonElement element = jsonParser.parse(reader);
//        JsonArray jsonArr = element.getAsJsonArray();
//
//        System.out.println("arr 크기 : " + jsonArr.size());

    }

    // 고유번호 - 오픈다트
    public static JSONArray retrieveCorpCode() throws IOException {
        String crtfc_key = "a1ab4687628095bbae0fd90f4c34c9c897fda441";
        String zipFilePath = "corpCode.zip";
        String xmlFilePath = "corpCode.xml";
        String jsonFilePath = "C:\\Users\\admin\\Desktop\\B-consulting_opendart\\corpCode.json";

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

                // 마지막에 큰따옴표 있을 시 삭제 -> csv 변환 시 컬럼 경계 깨지는 현상 발생
                String corp_code = ele.getElementsByTagName("corp_code").item(0).getTextContent().trim();
                if (corp_code.endsWith("\"")) corp_code = corp_code.substring(0, corp_code.length() - 1);
                String corp_name = ele.getElementsByTagName("corp_name").item(0).getTextContent().trim();
                if (corp_name.endsWith("\"")) corp_name = corp_name.substring(0, corp_name.length() - 1);
                String corp_eng_name = ele.getElementsByTagName("corp_eng_name").item(0).getTextContent().trim();
                if (corp_eng_name.endsWith("\"")) corp_eng_name = corp_eng_name.substring(0, corp_eng_name.length() - 1);
                String stock_code = ele.getElementsByTagName("stock_code").item(0).getTextContent().trim();
                if (stock_code.endsWith("\"")) stock_code = stock_code.substring(0, stock_code.length() - 1);
                String modify_date = ele.getElementsByTagName("modify_date").item(0).getTextContent().trim();
                if (modify_date.endsWith("\"")) modify_date = modify_date.substring(0, modify_date.length() - 1);

                obj.put("corp_code", corp_code);
                obj.put("corp_name", corp_name);
                obj.put("corp_eng_name", corp_eng_name);
                obj.put("stock_code", stock_code);
                obj.put("modify_date", modify_date);

                jsonArr.put(obj);
            }

            try (FileWriter fw = new FileWriter(jsonFilePath)) {
                // indetFactor : 들여쓰기 공백 수
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

        Reader reader = new InputStreamReader(new FileInputStream("C:\\Users\\admin\\Desktop\\B-consulting_opendart\\" + fileName), "UTF-8");
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

        FileWriter writer = new FileWriter("C:\\Users\\admin\\Desktop\\B-consulting_opendart\\" + resultFileName);
        writer.write(companyJsonArr.toString());
        writer.flush();
        writer.close();
    }

    // 두개의 json 파일 묶어서 하나의 json 파일로 저장
    public static void joinJsonArray(String fileNameA, String fileNameB) throws IOException {

        JsonArray resultJson = new JsonArray();

        JsonParser jsonParser = new JsonParser();
        Reader readerA = new InputStreamReader(new FileInputStream("C:\\Users\\admin\\Desktop\\B-consulting_opendart\\" + fileNameA), "UTF-8");

        Object objA = jsonParser.parse(readerA);
        JsonArray jsonArrayA = (JsonArray) objA;

        Reader readerB = new InputStreamReader(new FileInputStream("C:\\Users\\admin\\Desktop\\B-consulting_opendart\\" + fileNameB), "UTF-8");

        Object objB = jsonParser.parse(readerB);
        JsonArray jsonArrayB = (JsonArray) objB;

        HashMap<String, JsonObject> map = new HashMap<>();

        for (int i = 0; i < jsonArrayA.size(); i++) {
            JsonObject obj = jsonArrayA.get(i).getAsJsonObject();
            String corp_code = obj.get("corp_code").getAsString();
            map.put(corp_code, obj);
        }

        for (int i = 0; i < jsonArrayB.size(); i++) {
            JsonObject obj = jsonArrayB.get(i).getAsJsonObject();
            String corp_code = obj.get("corp_code").getAsString();
            map.put(corp_code, obj);
        }

        for (JsonObject obj : map.values()) {
            resultJson.add(obj);
        }

        String resultFileName = "C:\\Users\\admin\\Desktop\\B-consulting_opendart\\resultCompanyInfo.json";

        FileWriter writer = new FileWriter(resultFileName);
        writer.write(resultJson.toString());
        writer.flush();
        writer.close();

    }

    // 새로운 기업고유번호에 대한 기업개황 데이터 수집
    public static void setnewCorpCode() throws IOException {

        // 오픈 다트 기업고유번호 수집 데이터 json 파일
        JsonParser corpParser = new JsonParser();
        Reader corpReader = new InputStreamReader(new FileInputStream("C:\\Users\\admin\\Desktop\\B-consulting_opendart\\corpCode.json"), "UTF-8");

        JsonElement corpElement = corpParser.parse(corpReader);
        JsonArray corpJsonArr = corpElement.getAsJsonArray();

        // 기존에 수집했던 기업개황 데이터 json 파일
        JsonParser companyInfoParser = new JsonParser();
        Reader companyInfoReader = new InputStreamReader(new FileInputStream("C:\\Users\\admin\\Desktop\\B-consulting_opendart\\resultCompanyInfo.json"), "UTF-8");

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

        newCorpSet.forEach(corp -> System.out.println("새로운 기업고유번호 : " + corp));

        Calendar cal = Calendar.getInstance();
        String format = "yyyyMMdd";
        SimpleDateFormat sdf = new SimpleDateFormat(format);
        cal.add(cal.DATE, -1);
        String date = sdf.format(cal.getTime());

        for (JsonElement el : corpJsonArr) {
            JsonObject corpObj = el.getAsJsonObject();
            String modifyDate = corpObj.get("modify_date").getAsString();

            if (modifyDate.compareTo(date) >= 0) {
                newCorpSet.add(corpObj.get("corp_code").getAsString());
            }
        }

        JsonArray newCorpJsonArr = new JsonArray();

        for (int i = 0; i < corpJsonArr.size(); i++) {
            JsonObject obj = corpJsonArr.get(i).getAsJsonObject();
            String corp_code = obj.get("corp_code").getAsString();

            if (newCorpSet.contains(corp_code)) {
                newCorpJsonArr.add(obj);
            }
        }

        FileWriter writer = new FileWriter("C:\\Users\\admin\\Desktop\\B-consulting_opendart\\newCorpCode.json");
        writer.write(newCorpJsonArr.toString());
        writer.flush();
        writer.close();

    }

    public static void convertJsonToCsv(String fileName) throws IOException {

        JsonParser jsonParser = new JsonParser();
        Reader reader = new InputStreamReader(new FileInputStream("C:\\Users\\admin\\Desktop\\B-consulting_opendart\\" + fileName + ".json"), "UTF-8");
        JsonElement element = jsonParser.parse(reader);
        JsonArray jsonArr = element.getAsJsonArray();
        JsonObject corpObj = jsonArr.get(0).getAsJsonObject();

        BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream("C:\\Users\\admin\\Desktop\\B-consulting_opendart\\" + fileName + ".csv"), StandardCharsets.UTF_8));

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

    // 기업고유번호 DB MERGE
    public static void insertCorpCode() throws ClassNotFoundException, IOException {

        Class.forName("org.postgresql.Driver");

        String url = "jdbc:postgresql://localhost:5432/postgres";
        String user = "postgres";
        String password = "postgres";


        try (Connection connection = DriverManager.getConnection(url, user, password);) {
            Statement statement = connection.createStatement();
            /* SELECT
            ResultSet rs = statement.executeQuery("SELECT * FROM COMPANY WHERE 1=1 AND CORP_CODE = '00434003'");

            while (rs.next()) {
                String stockName =  rs.getString("stock_name");
                System.out.println(stockName);
            }
            rs.close();
            statement.close();
            connection.close();
            */

            // 새로운 기업고유번호 INSERT
            JsonParser jsonParser = new JsonParser();

            Reader reader = new InputStreamReader(new FileInputStream("C:\\Users\\admin\\Desktop\\B-consulting_opendart\\newCorpCode.json"), "UTF-8");
            JsonElement element = jsonParser.parse(reader);
            JsonArray jsonArr = element.getAsJsonArray();

            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            String now = sdf.format(new Date());

            String sql = "MERGE INTO CORP_CODE AS A " +
                         "USING (VALUES (?, ?, ?, ?, ?)) AS B (CORP_CODE, CORP_ENG_NAME, CORP_NAME, STOCK_CODE, MODIFY_DATE)" +
                         "ON A.CORP_CODE = B.CORP_CODE " +
                         "WHEN MATCHED THEN " +
                             "UPDATE SET CORP_CODE = B.CORP_CODE, " +
                                         "CORP_ENG_NAME = B.CORP_ENG_NAME, " +
                                         "CORP_NAME = B.CORP_NAME, " +
                                         "STOCK_CODE = B.STOCK_CODE, " +
                                         "MODIFY_DATE = B.MODIFY_DATE, " +
                                         "LAST_CHNG_DTL_DTTM = '" + now + "' " +
                         "WHEN NOT MATCHED THEN " +
                             "INSERT (CORP_CODE, CORP_ENG_NAME, CORP_NAME, STOCK_CODE, MODIFY_DATE, DEL_YN, FRST_RGSR_DTL_DTTM, LAST_CHNG_DTL_DTTM) " +
                             "VALUES (B.CORP_CODE, B.CORP_ENG_NAME, B.CORP_NAME, B.STOCK_CODE, B.MODIFY_DATE, 'N', '" + now +  "', '" + now + "')";

            PreparedStatement ps = connection.prepareStatement(sql);

            for (JsonElement e : jsonArr) {
                String corpCode = e.getAsJsonObject().get("corp_code").getAsString();
                String corpEngName = e.getAsJsonObject().get("corp_eng_name").getAsString();
                String corpName = e.getAsJsonObject().get("corp_name").getAsString();
                String stockCode = e.getAsJsonObject().get("stock_code").getAsString();
                String modifyDate = e.getAsJsonObject().get("modify_date").getAsString();

                ps.setString(1, corpCode);
                ps.setString(2, corpEngName);
                ps.setString(3, corpName);
                ps.setString(4, stockCode);
                ps.setString(5, modifyDate);

                int result = ps.executeUpdate();

                if (result > 0) {
                    System.out.println("[SUCCESS] CORP_CODE : " + corpCode + ", CORP_NAME : " + corpName + " -> MERGE 처리");
                } else {
                    System.out.println("[NO CHANGE] CORP_CODE : " + corpCode + ", CORP_NAME : " + corpName + " -> 변경 없음");
                }
            }


        } catch (SQLException e) {
            e.printStackTrace();
        }
    }


    // 기업개황정보 DB MERGE
    public static void insertCompanyInfo() throws ClassNotFoundException, IOException {

        Class.forName("org.postgresql.Driver");

        String url = "jdbc:postgresql://localhost:5432/postgres";
        String user = "postgres";
        String password = "postgres";


        try (Connection connection = DriverManager.getConnection(url, user, password);) {
            Statement statement = connection.createStatement();
            /* SELECT
            ResultSet rs = statement.executeQuery("SELECT * FROM COMPANY WHERE 1=1 AND CORP_CODE = '00434003'");

            while (rs.next()) {
                String stockName =  rs.getString("stock_name");
                System.out.println(stockName);
            }
            rs.close();
            statement.close();
            connection.close();
            */

            // 새로운 기업개황정보 INSERT
            JsonParser jsonParser = new JsonParser();

            Reader reader = new InputStreamReader(new FileInputStream("C:\\Users\\admin\\Desktop\\B-consulting_opendart\\newCompanyInfo.json"), "UTF-8");
            JsonElement element = jsonParser.parse(reader);
            JsonArray jsonArr = element.getAsJsonArray();

            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            String now = sdf.format(new Date());

            String sql = "MERGE INTO COMPANY AS A " +
                         "USING (VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)) AS B " +
                                        "(PHN_NO, ACC_MT, CEO_NM, STOCK_NAME, CORP_CODE, INDUTY_CODE, JURIR_NO, MESSAGE, CORP_NAME, EST_DT, HM_URL, CORP_CLS, CORP_NAME_ENG, IR_URL, ADRES, STOCK_CODE, BIZR_NO, FAX_NO, STATUS)" +
                         "ON A.CORP_CODE = B.CORP_CODE " +
                         "WHEN MATCHED THEN " +
                             "UPDATE SET PHN_NO = B.PHN_NO, " +
                                        "ACC_MT = B.ACC_MT, " +
                                        "CEO_NM = B.CEO_NM, " +
                                        "STOCK_NAME = B.STOCK_NAME, " +
                                        "CORP_CODE = B.CORP_CODE, " +
                                        "INDUTY_CODE = B.INDUTY_CODE, " +
                                        "JURIR_NO = B.JURIR_NO, " +
                                        "MESSAGE = B.MESSAGE, " +
                                        "CORP_NAME = B.CORP_NAME, " +
                                        "EST_DT = B.EST_DT, " +
                                        "HM_URL = B.HM_URL, " +
                                        "CORP_CLS = B.CORP_CLS, " +
                                        "CORP_NAME_ENG = B.CORP_NAME_ENG, " +
                                        "IR_URL = B.IR_URL, " +
                                        "ADRES = B.ADRES, " +
                                        "STOCK_CODE = B.STOCK_CODE, " +
                                        "BIZR_NO = B.BIZR_NO, " +
                                        "FAX_NO = B.FAX_NO, " +
                                        "STATUS = B.STATUS, " +
                                        "LAST_CHNG_DTL_DTTM = '" + now + "' " +
                         "WHEN NOT MATCHED THEN " +
                             "INSERT (PHN_NO, ACC_MT, CEO_NM, STOCK_NAME, CORP_CODE, INDUTY_CODE, JURIR_NO, MESSAGE, CORP_NAME, EST_DT, HM_URL, CORP_CLS, CORP_NAME_ENG, IR_URL, ADRES, STOCK_CODE, BIZR_NO, FAX_NO, STATUS, DEL_YN, FRST_RGSR_DTL_DTTM, LAST_CHNG_DTL_DTTM) " +
                             "VALUES (B.PHN_NO, B.ACC_MT, B.CEO_NM, B.STOCK_NAME, B.CORP_CODE, B.INDUTY_CODE, B.JURIR_NO, B.MESSAGE, B.CORP_NAME, B.EST_DT, B.HM_URL, B.CORP_CLS, B.CORP_NAME_ENG, B.IR_URL, B.ADRES, B.STOCK_CODE, B.BIZR_NO, B.FAX_NO, B.STATUS, 'N', '" + now + "', '" + now + "')";

            PreparedStatement ps = connection.prepareStatement(sql);

            for (JsonElement e : jsonArr) {
                String phnNo = e.getAsJsonObject().get("phn_no").getAsString();
                String accMt = e.getAsJsonObject().get("acc_mt").getAsString();
                String ceoNm = e.getAsJsonObject().get("ceo_nm").getAsString();
                String stockName = e.getAsJsonObject().get("stock_name").getAsString();
                String corpCode = e.getAsJsonObject().get("corp_code").getAsString();
                String indutyCode = e.getAsJsonObject().get("induty_code").getAsString();
                String jurirNo = e.getAsJsonObject().get("jurir_no").getAsString();
                String message = e.getAsJsonObject().get("message").getAsString();
                String corpName = e.getAsJsonObject().get("corp_name").getAsString();
                String estDT = e.getAsJsonObject().get("est_dt").getAsString();
                String hmUrl = e.getAsJsonObject().get("hm_url").getAsString();
                String corpCLS = e.getAsJsonObject().get("corp_cls").getAsString();
                String corpNameEng = e.getAsJsonObject().get("corp_name_eng").getAsString();
                String irUrl = e.getAsJsonObject().get("ir_url").getAsString();
                String adres = e.getAsJsonObject().get("adres").getAsString();
                String stockCode = e.getAsJsonObject().get("stock_code").getAsString();
                String bizrNo = e.getAsJsonObject().get("bizr_no").getAsString();
                String faxNo = e.getAsJsonObject().get("fax_no").getAsString();
                String status = e.getAsJsonObject().get("status").getAsString();

                ps.setString(1, phnNo);
                ps.setString(2, accMt);
                ps.setString(3, ceoNm);
                ps.setString(4, stockName);
                ps.setString(5, corpCode);
                ps.setString(6, indutyCode);
                ps.setString(7, jurirNo);
                ps.setString(8, message);
                ps.setString(9, corpName);
                ps.setString(10, estDT);
                ps.setString(11, hmUrl);
                ps.setString(12, corpCLS);
                ps.setString(13, corpNameEng);
                ps.setString(14, irUrl);
                ps.setString(15, adres);
                ps.setString(16, stockCode);
                ps.setString(17, bizrNo);
                ps.setString(18, faxNo);
                ps.setString(19, status);

                int result = ps.executeUpdate();

                if (result > 0) {
                    System.out.println("[SUCCESS] CORP_CODE : " + corpCode + ", CORP_NAME : " + corpName + " -> MERGE 처리");
                } else {
                    System.out.println("[NO CHANGE] CORP_CODE : " + corpCode + ", CORP_NAME : " + corpName + " -> 변경 없음");
                }
            }


        } catch (SQLException e) {
            e.printStackTrace();
        }
    }


}


