package org.example.dags.realestate;

import com.google.api.services.bigquery.model.TableRow;
import org.apache.beam.sdk.coders.DefaultCoder;
import org.apache.beam.sdk.extensions.avro.coders.AvroCoder;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.example.Utils.*;

@DefaultCoder(AvroCoder.class)
public class RealEstatesXactRec {
    // 種類
    public String dealType;

    // 価格情報区分
    public String priceType;

    // 地域
    public String landPurpose;

    // 市区町村コード
    public String districtCode;

    // 都道府県名
    public String prefectureName;

    // 市区町村名
    public String districtName;

    // 地区名
    public String cityName;

    // 最寄駅：名称
    public String closestStationName;

    // 最寄駅：距離（分）
    public int durationToClosestStationInMin;

    // 取引価格（総額）
    public int closedPrice;

    // 坪単価
    public int unitPriceOfFloorspace;

    // 面積（㎡）
    public int areaInSquareMeter;

    // 取引価格（㎡単価）
    public int unitPriceOfSquareMeter;

    // 土地の形状
    public String shapeOfLand;

    // 間口
    public float facadeInMeters;

    // 延床面積（％）
    public int areaTotal;

    // 建築年
    public int yearBuilt;
    
    // 建物の構造
    public String architectureType;

    // 用途
    public String purpose;

    // 今後の利用目的
    public String futurePurpose;

    // 前面道路：方位
    public String frontRoadDirection;

    // 前面道路：種類
    public String frontRoadType;

    // 前面道路：幅員（ｍ）
    public float frontRoadWidthInMeters;

    // 都市計画
    public String cityPlan;

    // 建ぺい率（％）
    public float buildingToLandRatio;

    // 容積率（％）
    public float floorToLandRatio;

    // 取引時期
    public String agreementPointOfTime;

    //取引の事情等
    public String agreementNote;

    // 四半期の日付
    public String quarterAsDate;

    static Logger LOG = LoggerFactory.getLogger(RealEstatesXactRec.class);

    /***
     *
     * @param csvLine
     * @return
     */
    public static RealEstatesXactRec of(String csvLine) throws IOException {

        List<String> fields = CSVParser.parse(csvLine, CSVFormat.RFC4180)
                .getRecords().get(0).toList();

        RealEstatesXactRec r = new RealEstatesXactRec();
        r.dealType = validateStr(trimQuotes(fields.get(0)));
        r.priceType = validateStr(trimQuotes(fields.get(1)));
        r.landPurpose = validateStr(trimQuotes(fields.get(2)));
        r.districtCode = validateStr(trimQuotes(fields.get(3)));
        r.prefectureName = validateStr(trimQuotes(fields.get(4)));
        r.districtName = validateStr(trimQuotes(fields.get(5)));
        r.cityName = validateStr(trimQuotes(fields.get(6)));
        r.closestStationName = validateStr(trimQuotes(fields.get(7)));
        r.durationToClosestStationInMin = asInt(fields.get(8));
        r.closedPrice = asInt(fields.get(9));
        r.unitPriceOfFloorspace = asInt(fields.get(10));
        r.areaInSquareMeter = asInt(fields.get(11));
        r.unitPriceOfSquareMeter = asInt(fields.get(12));
        r.shapeOfLand = validateStr(trimQuotes(fields.get(13)));
        r.facadeInMeters = asFloat(fields.get(14));
        r.areaTotal = asInt(fields.get(15));
        r.yearBuilt = asInt(fields.get(16));
        r.architectureType = validateStr(trimQuotes(fields.get(17)));
        r.purpose = validateStr(trimQuotes(fields.get(18)));
        r.futurePurpose = validateStr(trimQuotes(fields.get(19)));
        r.frontRoadDirection = validateStr(trimQuotes(fields.get(20)));
        r.frontRoadType = validateStr(trimQuotes(fields.get(21)));
        r.frontRoadWidthInMeters = asFloat(fields.get(22));
        r.cityPlan = validateStr(trimQuotes(fields.get(23)));
        r.buildingToLandRatio = asFloat(fields.get(24));
        r.floorToLandRatio = asFloat(fields.get(25));
        r.agreementPointOfTime = validateStr(trimQuotes(fields.get(26)));
        r.agreementNote = validateStr(trimQuotes(fields.get(27)));

        try {
            r.quarterAsDate = parseQuarterDateFormat(r.agreementPointOfTime);
        } catch (IllegalArgumentException e) {
            LOG.warn(e.getMessage());
            r.quarterAsDate = "";
        }

        return r;
    }

    public TableRow toTableRow() {
        return new TableRow()
                .set("dealType", dealType)
                .set("priceType", priceType)
                .set("landPurpose", landPurpose)
                .set("districtCode", districtCode)
                .set("prefectureName", prefectureName)
                .set("districtName", districtName)
                .set("cityName", cityName)
                .set("closestStationName", closestStationName)
                .set("durationToClosestStationInMin", durationToClosestStationInMin)
                .set("closedPrice", closedPrice)
                .set("unitPriceOfFloorspace", unitPriceOfFloorspace)
                .set("areaInSquareMeter", areaInSquareMeter)
                .set("unitPriceOfSquareMeter", unitPriceOfSquareMeter)
                .set("shapeOfLand", shapeOfLand)
                .set("facadeInMeters", facadeInMeters)
                .set("areaTotal", areaTotal)
                .set("yearBuilt", yearBuilt)
                .set("architectureType", architectureType)
                .set("purpose", purpose)
                .set("futurePurpose", futurePurpose)
                .set("frontRoadDirection", frontRoadDirection)
                .set("frontRoadType", frontRoadType)
                .set("frontRoadWidthInMeters", frontRoadWidthInMeters)
                .set("cityPlan", cityPlan)
                .set("buildingToLandRatio", buildingToLandRatio)
                .set("floorToLandRatio", floorToLandRatio)
                .set("agreementPointOfTime", agreementPointOfTime)
                .set("agreementNote", agreementNote)
                .set("quarterAsDate", quarterAsDate);
    }

    private static String trimQuotes(String str) {
        return str.replaceAll("\"", "");
    }

    private static String parseQuarterDateFormat(String s) {
        String pattern = "^(\\d+)年(.*?)$";
        Pattern r = Pattern.compile(pattern);
        Matcher m = r.matcher(s);
        String formattedDate;

        if (!m.find()) {
            throw new IllegalArgumentException("Unknown quarterly date format: " + s);
        }

        formattedDate = switch (m.group(2)) {
            case "第1四半期" -> m.group(1) + "-01-01";
            case "第2四半期" -> m.group(1) + "-04-01";
            case "第3四半期" -> m.group(1) + "-07-01";
            case "第4四半期" -> m.group(1) + "-10-01";
            default -> throw new IllegalArgumentException("Unexpected value: " + m.group(2));
        };
        return formattedDate;
    }
}
