package com.pabu5h.evs2.oqghelper;

import com.pabu5h.evs2.dto.ItemIdTypeEnum;
import com.pabu5h.evs2.dto.ItemIdTypeEnum;
import com.pabu5h.evs2.dto.ItemTypeEnum;
import com.pabu5h.evs2.dto.ItemTypeEnum;
import com.pabu5h.evs2.dto.MeterBypassDto;
import com.pabu5h.evs2.dto.MeterInfoDto;
import com.xt.utils.DateTimeUtil;
import com.xt.utils.MathUtil;
import com.xt.utils.SqlUtil;

import org.springframework.beans.factory.annotation.Autowired;
//import org.springframework.http.HttpStatus;
//import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.*;
import java.util.logging.Logger;
import java.util.stream.Collectors;

import static java.time.LocalDateTime.now;

@Service
public class QueryHelper {
    @Autowired
    private OqgHelper oqgHelper;
    private final Logger logger = Logger.getLogger(QueryHelper.class.getName());

    public Map<String, Object> getTableField(String tableName, String fieldName, String idConstraintKey, String idConstraintValue){
        String sql = "select " + fieldName + " from " + tableName + " where " + idConstraintKey + " = '" + idConstraintValue + "'";
        List<Map<String, Object>> resp;
        try {
            resp = oqgHelper.OqgR2(sql, true);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        if(resp.isEmpty()){
            return Map.of("error", "No record found for " + idConstraintKey + " = " + idConstraintValue);
        }
        return resp.getFirst();
    }

    public Map<String, Object> checkExists(String table, String key, String value, boolean useR2){
        String sql = "select * from " + table + " where " + key + " = '" + value + "'";
        List<Map<String, Object>> resp;
        try {
            if(useR2){
                resp = oqgHelper.OqgR2(sql, true);
            }else {
                resp = oqgHelper.OqgR(sql);
            }
        } catch (Exception e) {
//            throw new RuntimeException(e);
            logger.info("Error checking exists for table: " + table + ", key: " + key + ", value: " + value);
            return Map.of("error", "Error checking exists for table: " + table + ", key: " + key + ", value: " + value);
        }
        if(resp.isEmpty()){
            return Map.of("exists", false);
        }
        return Map.of("exists", true);
    }

    @Deprecated
    public List<String> getNewerMeterSns(){
        // meterSn must be at least 10 characters long
        // and must start with 2018, 2019, 2020, 2021 and above
        String sql = "select meter_sn from meter " +
                "where length(meter_sn) > 10 " +
                "and (" +
                " meter_sn like '2018%' " +
                " or meter_sn like '2019%' " +
                " or meter_sn like '2020%' " +
                " or meter_sn like '2021%'" +
                " or meter_sn like '2022%'" +
                " or meter_sn like '2023%'" +
                " or meter_sn like '2024%'" +
                " or meter_sn like '2025%'" +
                " or meter_sn like '2026%'" +
                " or meter_sn like '2027%'" +
                " or meter_sn like '2028%'" +
                " or meter_sn like '2029%'" +
                " or meter_sn like '2030%')";

        List<Map<String, Object>> meterSns = new ArrayList<>();
        try {
            meterSns = oqgHelper.OqgR2(sql, true);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return meterSns.stream().map(meterSn -> meterSn.get("meter_sn").toString()).toList();
    }

    public List<String> getActiveMeterSns(String tableName){
        String timekey = "kwh_timestamp";
        if(tableName ==null || tableName.isEmpty()){
            tableName = "meter_tariff";
            timekey = "tariff_timestamp";
        }

        if(tableName.equals("meter_tariff")){
            timekey = "tariff_timestamp";
        }

        List<Map<String, Object>> meterSns = new ArrayList<>();

        String sgNow = DateTimeUtil.getZonedDateTimeStr(now(), ZoneId.of("Asia/Singapore"));
        String sql = "select distinct meter_sn from " + tableName +
                " where " + timekey + " > timestamp '" + sgNow + "' - interval '24 hours' ";

        try {
            meterSns = oqgHelper.OqgR2(sql, true);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return meterSns.stream().map(meterSn -> meterSn.get("meter_sn").toString()).toList();
    }

    public Map<String, Object> getMeterInfo(String meterSnStr){
        String sql = "select * from meter where meter_sn = '" + meterSnStr + "'";
        List<Map<String, Object>> meterInfo = new ArrayList<>();
        try {
            meterInfo = oqgHelper.OqgR2(sql, true);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        if(meterInfo.isEmpty()){
            return Map.of("info", "meter not found");
        }
        return meterInfo.get(0);
    }
    public Map<String, Object> getMeterInfoDtoFromSn(String meterSnStr){
        String sql = "select * from meter where meter_sn = '" + meterSnStr + "'";
        List<Map<String, Object>> meterInfo = new ArrayList<>();
        try {
            meterInfo = oqgHelper.OqgR2(sql, true);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        if(meterInfo.isEmpty()){
            return Map.of("info", "meter not found");
        }
        Map<String, Object> meterInfoMap = meterInfo.getFirst();
        MeterInfoDto meterInfoDto = MeterInfoDto.fromFieldMap(meterInfoMap);

        return Map.of("meter_info", meterInfoDto);
    }
    public Map<String, Object> getMeterInfoDtoFromSn2(String meterSnStr, String bypassPolicyTableName){
        String sql = "select * from meter where meter_sn = '" + meterSnStr + "'";
        List<Map<String, Object>> meterInfo = new ArrayList<>();
        try {
            meterInfo = oqgHelper.OqgR2(sql, true);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        if(meterInfo.isEmpty()){
            return Map.of("info", "meter not found");
        }
        Map<String, Object> meterInfoMap = meterInfo.getFirst();
        MeterInfoDto meterInfoDto = MeterInfoDto.fromFieldMap(meterInfoMap);

        if(bypassPolicyTableName != null && !bypassPolicyTableName.isEmpty()){
            String bypassSql = "select * from " +bypassPolicyTableName+ " where meter_sn = '" + meterSnStr + "'";
            List<Map<String, Object>> bypassInfo = new ArrayList<>();
            try {
                bypassInfo = oqgHelper.OqgR2(bypassSql, false);
            } catch (Exception e) {
//                throw new RuntimeException(e);
                logger.info("Error getting bypass policy for meter: " + meterSnStr);
                return Map.of("error", "Error getting bypass policy for meter: " + meterSnStr);
            }
            if(!bypassInfo.isEmpty()){
                Map<String, Object> meterBypassInfoMap = bypassInfo.getFirst();
                MeterBypassDto meterBypassDto = MeterBypassDto.fromFieldMap(meterBypassInfoMap);
                meterInfoDto.setBypassPolicy(meterBypassDto);
            }
        }

        return Map.of("meter_info", meterInfoDto);
    }

    public Map<String, Object> getMeterInfoDtoFromDisplayname(String meterDisplayname){
        String sql = "select * from meter where meter_displayname = '" + meterDisplayname + "'";
        List<Map<String, Object>> meterInfo = new ArrayList<>();
        try {
            meterInfo = oqgHelper.OqgR2(sql, true);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        if(meterInfo.isEmpty()){
            return Map.of("info", "meter not found");
        }
        Map<String, Object> meterInfoMap = meterInfo.getFirst();
        MeterInfoDto meterInfoDto = MeterInfoDto.fromFieldMap(meterInfoMap);

        return Map.of("meter_info", meterInfoDto);
    }
    public Map<String, Object> getMeterProperty(String meterSnStr, String property){
        String sql = "select " + property + " from meter where meter_sn = '" + meterSnStr + "'";
        List<Map<String, Object>> meterInfo = new ArrayList<>();
        try {
            meterInfo = oqgHelper.OqgR2(sql, true);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        if(meterInfo.isEmpty()){
            return Map.of("info", "meter not found");
        }
        return meterInfo.getFirst();
    }
    public Map<String, Object> getAllConcIds(){
        String sql = "select id from concentrator";

        List<Map<String, Object>> resp = new ArrayList<>();
        try {
            resp = oqgHelper.OqgR2(sql, true);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        if(resp.isEmpty()){
            return Map.of("info", "concentrator not found");
        }
        return Map.of("concentrator_id_list", resp.stream().map(meter -> meter.get("id")).toList());
    }
    public Map<String, Object> getAllConcs(){
        String sql = "select id, address from concentrator where version = 2";

        List<Map<String, Object>> resp;
        try {
            resp = oqgHelper.OqgR2(sql, true);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        if(resp.isEmpty()){
            return Map.of("info", "concentrator not found");
        }
        return Map.of("concentrator_list", resp);
    }
    public Map<String, Object> getCons(String projectScope, String siteScope){
        String sql = "select DISTINCT concentrator_id from meter";
        Map<String, Object> likeTargets = new HashMap<>();
        if(projectScope != null && !projectScope.isEmpty()){
            likeTargets.put("scope_str", projectScope);
        }
        if(siteScope != null && !siteScope.isEmpty()){
            likeTargets.put("site_tag", siteScope);
        }
        Map<String, String> sqlResult = SqlUtil.makeSelectSql2(
                Map.of("from", "meter",
                        "select",
                        "DISTINCT concentrator_id",
                        "like_targets", likeTargets
                ));

        sql = sqlResult.get("sql");
        sql = sql + " ORDER BY concentrator_id ASC";

        List<Map<String, Object>> meterInfo = new ArrayList<>();
        try {
            meterInfo = oqgHelper.OqgR2(sql, true);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        if(meterInfo.isEmpty()){
            return Map.of("info", "concentrator not found");
        }
        return Map.of("concentrator_list", meterInfo.stream().map(meter -> meter.get("concentrator_id")).toList());
    }

    public Map<String, Object> getMmsBuildings(String projectScope, String siteScope){
        String sql = "select DISTINCT mms_building from meter";
//        if(projectScope != null && !projectScope.isEmpty()){
//            sql = "select DISTINCT mms_building from meter where scope_str LIKE '%" + projectScope + "%'" +
//            " ORDER BY mms_building ASC";
//        }
        Map<String, Object> likeTargets = new HashMap<>();
        if(projectScope != null && !projectScope.isEmpty()){
            likeTargets.put("scope_str", projectScope);
        }
        if(siteScope != null && !siteScope.isEmpty()){
            likeTargets.put("site_tag", siteScope);
        }
        Map<String, String> sqlResult = SqlUtil.makeSelectSql2(
                Map.of("from", "meter",
                        "select",
                        "DISTINCT mms_building",
                        "like_targets", likeTargets
                ));
        sql = sqlResult.get("sql");
        sql = sql + " ORDER BY mms_building ASC";

        List<Map<String, Object>> meterInfo = new ArrayList<>();
        try {
            meterInfo = oqgHelper.OqgR2(sql, true);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        if(meterInfo.isEmpty()){
            return Map.of("info", "building not found");
        }
        return Map.of("building_list", meterInfo.stream().map(meter -> meter.get("mms_building")).toList());
    }

    public Map<String, Object> getScopeBuildings(String projectScope, String siteScope){
        String sql = "select DISTINCT mms_building from meter";

        String meterTableName = "meter";
        String colNameBuilding = "mms_building";
        if(projectScope != null && !projectScope.isEmpty()){
            if(projectScope.toLowerCase().contains("cw_nus")){
                meterTableName = "meter_iwow";
                colNameBuilding = "loc_building";
            }
        }
        String finalColNameBuilding = colNameBuilding;

        Map<String, Object> likeTargets = new HashMap<>();
        if(projectScope != null && !projectScope.isEmpty()){
            likeTargets.put("scope_str", projectScope);
        }
        if(siteScope != null && !siteScope.isEmpty()){
            likeTargets.put("site_tag", siteScope);
        }
        Map<String, String> sqlResult = SqlUtil.makeSelectSql2(
                Map.of("from", meterTableName,
                       "select",
                       "DISTINCT " + colNameBuilding,
                       "like_targets", likeTargets
                ));
        sql = sqlResult.get("sql");
        sql = sql + " ORDER BY "+colNameBuilding+" ASC";

        List<Map<String, Object>> meterInfo = new ArrayList<>();
        try {
            meterInfo = oqgHelper.OqgR2(sql, true);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        if(meterInfo.isEmpty()){
            return Map.of("info", "building not found");
        }
        return Map.of("building_list", meterInfo.stream().map(meter -> meter.get(finalColNameBuilding)).toList());
    }
    public Map<String, Object> getScopeLevels(String building, String block,
                                              String projectScope, String siteScope){
        String buildingNameSqlSafe = building.replace("'", "''");
        String sql = "select DISTINCT mms_level from meter where mms_building = '" + buildingNameSqlSafe + "' and mms_block = '" + block + "'"
                + " ORDER BY mms_level ASC";

        String meterTableName = "meter";
        String colNameBuilding = "mms_building";
        String colNameBlock = "mms_block";
        String colNameLevel = "mms_level";
        Map<String, Object> targets = new HashMap<>();
        targets.put("mms_building", buildingNameSqlSafe);
        targets.put("mms_block", block);

        if(projectScope != null && !projectScope.isEmpty()){
            if(projectScope.toLowerCase().contains("cw_nus")){
                meterTableName = "meter_iwow";
                colNameBuilding = "loc_building";
//                colNameBlock = "loc_block";
                colNameLevel = "loc_level";
                targets.clear();
                targets.put("loc_building", buildingNameSqlSafe);
            }
        }

        String finalColNameLevel = colNameLevel;

        Map<String, Object> likeTargets = new HashMap<>();
        if(projectScope != null && !projectScope.isEmpty()){
            likeTargets.put("scope_str", projectScope);
        }
        if(siteScope != null && !siteScope.isEmpty()){
            likeTargets.put("site_tag", siteScope);
        }
        Map<String, String> sqlResult = SqlUtil.makeSelectSql2(
                Map.of("from", meterTableName,
                        "select",
                        "DISTINCT " + colNameLevel,
                        "targets", targets,
                        "like_targets", likeTargets
                ));
        sql = sqlResult.get("sql");
        sql = sql + " ORDER BY "+colNameLevel+" ASC";

        List<Map<String, Object>> meterInfo = new ArrayList<>();
        try {
            meterInfo = oqgHelper.OqgR2(sql, true);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        if(meterInfo.isEmpty()){
            return Map.of("info", "level not found");
        }
        return Map.of("level_list", meterInfo.stream().map(meter -> meter.get(finalColNameLevel)).toList());
    }

    public Map<String, Object> getMmsBuildingBlocks (String building, String projectScope, String siteScope){
        String buildingNameSqlSafe = building.replace("'", "''");
        String sql = "select DISTINCT mms_block from meter where " +
                " mms_building = '" + buildingNameSqlSafe + "'" +
                " ORDER BY mms_block ASC";

        Map<String, Object> likeTargets = new HashMap<>();
        if(projectScope != null && !projectScope.isEmpty()){
            likeTargets.put("scope_str", projectScope);
        }
        if(siteScope != null && !siteScope.isEmpty()){
            likeTargets.put("site_tag", siteScope);
        }
        Map<String, String> sqlResult = SqlUtil.makeSelectSql2(
                Map.of("from", "meter",
                        "select",
                        "DISTINCT mms_block",
                        "targets", Map.of("mms_building", buildingNameSqlSafe),
                        "like_targets", likeTargets
                ));
        sql = sqlResult.get("sql");
        sql = sql + " ORDER BY mms_block ASC";

        List<Map<String, Object>> meterInfo = new ArrayList<>();
        try {
            meterInfo = oqgHelper.OqgR2(sql, true);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        if(meterInfo.isEmpty()){
            return Map.of("info", "block not found");
        }
        return Map.of("block_list", meterInfo.stream().map(meter -> meter.get("mms_block")).toList());
    }

    public Map<String, Object> getMmsLevels (String building, String block, String projectScope, String siteScope){
        String buildingNameSqlSafe = building.replace("'", "''");
        String sql = "select DISTINCT mms_level from meter where mms_building = '" + buildingNameSqlSafe + "' and mms_block = '" + block + "'"
                + " ORDER BY mms_level ASC";
        Map<String, Object> likeTargets = new HashMap<>();
        if(projectScope != null && !projectScope.isEmpty()){
            likeTargets.put("scope_str", projectScope);
        }
        if(siteScope != null && !siteScope.isEmpty()){
            likeTargets.put("site_tag", siteScope);
        }
        Map<String, String> sqlResult = SqlUtil.makeSelectSql2(
                Map.of("from", "meter",
                        "select",
                        "DISTINCT mms_level",
                        "targets", Map.of("mms_building", buildingNameSqlSafe, "mms_block", block),
                        "like_targets", likeTargets
                ));
        sql = sqlResult.get("sql");
        sql = sql + " ORDER BY mms_level ASC";

        List<Map<String, Object>> meterInfo = new ArrayList<>();
        try {
            meterInfo = oqgHelper.OqgR2(sql, true);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        if(meterInfo.isEmpty()){
            return Map.of("info", "level not found");
        }
        return Map.of("level_list", meterInfo.stream().map(meter -> meter.get("mms_level")).toList());
    }
    public Map<String, Object> getMmsUnits (String building, String block, String level, String projectScope, String siteScope){
        String buildingNameSqlSafe = building.replace("'", "''");
        String sql = "select DISTINCT mms_unit from meter where mms_building = '" + buildingNameSqlSafe + "' and mms_block = '" + block + "'" +
                " and mms_level = '" + level + "'"
                + " ORDER BY mms_unit ASC";
//        if(projectScope != null && !projectScope.isEmpty()){
//            // syntax error
////            sql = sql + " and scope_str LIKE '%" + projectScope + "%'";
//            sql = "select DISTINCT mms_unit from meter where mms_building = '" + building + "' and mms_block = '" + block + "' and mms_level = '" + level + "' and scope_str LIKE '%" + projectScope + "%'"
//                    + " ORDER BY mms_unit ASC";
//        }
        Map<String, Object> likeTargets = new HashMap<>();
        if(projectScope != null && !projectScope.isEmpty()){
            likeTargets.put("scope_str", projectScope);
        }
        if(siteScope != null && !siteScope.isEmpty()){
            likeTargets.put("site_tag", siteScope);
        }
        Map<String, String> sqlResult = SqlUtil.makeSelectSql2(
                Map.of("from", "meter",
                        "select",
                        "DISTINCT mms_unit",
                        "targets", Map.of("mms_building", buildingNameSqlSafe, "mms_block", block, "mms_level", level),
                        "like_targets", likeTargets
                ));
        sql = sqlResult.get("sql");
        sql = sql + " ORDER BY mms_unit ASC";

        List<Map<String, Object>> meterInfo = new ArrayList<>();
        try {
            meterInfo = oqgHelper.OqgR2(sql, true);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        if(meterInfo.isEmpty()){
            return Map.of("info", "unit not found");
        }
        return Map.of("unit_list", meterInfo.stream().map(meter -> meter.get("mms_unit")).toList());
    }

    public List<Map<String, Object>> getAllMeterInfo(){
        String sql = "select meter_sn, meter_displayname, reading_interval, commissioned_timestamp" +
                " unit, street, block, building" +
                " from meter" +
                " inner join premise p on meter.premise_id = p.id";
        List<Map<String, Object>> meterInfo = new ArrayList<>();
        try {
            meterInfo = oqgHelper.OqgR2(sql, true);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return meterInfo;
    }
    public List<Map<String, Object>> getAllMmsMeterInfo(){
        String sql = "select meter_sn, meter_displayname, reading_interval, concentrator_id, commissioned_timestamp, " +
                " mms_address, mms_unit, mms_level, mms_block, mms_building, mms_online_timestamp, " +
                " esim_id, data_subscription_id, scope_str, " +
                " daily_usage_timestamp " +
                " from meter " +
                " where esim_id is not null and esim_id != '' ";
        List<Map<String, Object>> meterInfo = new ArrayList<>();
        try {
            meterInfo = oqgHelper.OqgR2(sql, true);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return meterInfo;
    }

    public void insertMeterDataBal(String meterSn, String dataBal, String initBal){
        String sgNow = DateTimeUtil.getSgNowStr();
        String tableName = "meter_comm_data";

        String sql = "insert into " + tableName + " (meter_sn, data_bal, data_bal_ini, data_bal_timestamp) " +
                "values ('" + meterSn + "', " + dataBal + ", " + initBal + ", timestamp '" + sgNow + "')";
        try {
            oqgHelper.OqgIU(sql);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
    public Map<String, Object> getMeterDataBal(String meterSnStr){
        String tableName = "meter_comm_data";
        String sql = "select * from " + tableName + " where meter_sn = '" + meterSnStr + "' order by data_bal_timestamp desc limit 1";
        List<Map<String, Object>> meterInfo = new ArrayList<>();
        try {
            meterInfo = oqgHelper.OqgR2(sql, true);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        if(meterInfo.isEmpty()){
            return Map.of("info", "meter not found");
        }
        return meterInfo.getFirst();
    }

    public long getActiveMeterCount(String tableName){
        String timekey = "kwh_timestamp";
        if(tableName ==null || tableName.isEmpty()){
            tableName = "meter_tariff";
            timekey = "tariff_timestamp";
        }

        if(tableName.equals("meter_tariff")){
            timekey = "tariff_timestamp";
        }

        List<Map<String, Object>> count = new ArrayList<>();

        String sgNow = DateTimeUtil.getZonedDateTimeStr(now(), ZoneId.of("Asia/Singapore"));
        String sql = "select count(distinct meter_sn) from " + tableName +
                " where " + timekey + " > timestamp '" + sgNow + "' - interval '24 hours' ";

        try {
            count = oqgHelper.OqgR2(sql, true);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        if(count.isEmpty()){
            return 0;
        }
        return Long.parseLong(count.getFirst().get("count").toString());
    }

    public Map<String, Object> getActiveMeterCountHistory(String tableName, int days){
        String timekey = "kwh_timestamp";
        if(tableName ==null || tableName.isEmpty()){
            tableName = "meter_tariff";
            timekey = "tariff_timestamp";
        }

        if(tableName.equals("meter_tariff")){
            timekey = "tariff_timestamp";
        }

        List<Map<String, Object>> count = new ArrayList<>();

        // Create a StringBuilder to build the SQL query
        StringBuilder queryBuilder = new StringBuilder();

        // Iterate over the past 7 days
        for (int i = 0; i < days; i++) {
            // Subtract i days from the current date
            LocalDateTime sgNow =
                    DateTimeUtil.getZonedLocalDateTimeFromSystemLocalDateTime(now().minusDays(i), ZoneId.of("Asia/Singapore"));
            String sgNowStr = DateTimeUtil.getLocalDateTimeStr(sgNow);

            // Append the SQL statement for each day to the query builder
            queryBuilder.append("SELECT '")
                    .append(sgNowStr)
                    .append("' AS timestamp, COUNT(DISTINCT meter_sn) FROM ")
                    .append(tableName)
                    .append(" WHERE ")
                    .append(timekey)
                    .append(" > TIMESTAMP '")
                    .append(sgNowStr)
                    .append("' - INTERVAL '24 hours'")
                    .append(" AND ")
                    .append(timekey)
                    .append(" <= TIMESTAMP '")
                    .append(sgNowStr)
                    .append("'");

            // Add a UNION ALL between each statement except the last one
            if (i < days - 1) {
                queryBuilder.append(" UNION ALL ");
            }
        }
        // Build the outer query to sort the result by date in descending order
//        queryBuilder.insert(0, "SELECT * FROM (")
//                .append(") AS subquery ORDER BY date DESC");

        // Store the final SQL query string
        String sql = queryBuilder.toString();

        try {
            count = oqgHelper.OqgR2(sql, true);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        if(count.isEmpty()){
            return Collections.singletonMap("info", "no data");
        }
        //sort by date in descending order
        count.sort(Comparator.comparing(m -> m.get("timestamp").toString()));
        Collections.reverse(count);

        return Collections.singletonMap("active_meter_count_history", count);
    }

    public double getActiveKwhConsumption(String meterSnStr){
        List<Map<String, Object>> kwhConsumption = new ArrayList<>();

        String sgNow = DateTimeUtil.getZonedDateTimeStr(now(), ZoneId.of("Asia/Singapore"));
        String sql = "select sum(kwh_diff) as kwh_total from meter_tariff " +
                " where meter_sn = '" + meterSnStr + "' " +
                " and kwh_diff is not null " +
                " and kwh_diff > 0 " +
                " and tariff_timestamp > timestamp '" + sgNow + "' - interval '24 hours' ";

        try {
            kwhConsumption = oqgHelper.OqgR2(sql, true);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        if(kwhConsumption.isEmpty()){
            return 0;
        }
        //sum() will always return a value, even if there is no data, the list will still have 1 element
        if(kwhConsumption.getFirst().get("total_kwh") == null){
            return 0;
        }
        return Double.parseDouble(kwhConsumption.get(0).get("kwh_total").toString());
    }

    public Map<String, Object> getScopeConstraint(Map<String, String>scope, String meterIdColName){
        if(scope == null || scope.isEmpty()){
            return Map.of("scope_constraint", "");
        }

        String meterIdInStr = "";
        if(scope != null && !scope.isEmpty()){
            String scopeConstraint = "";
            //priority: site > project
            if(scope.containsKey("site_tag")){
                scopeConstraint = " site_tag = '" + scope.get("site_tag") + "' ";
            }else if(scope.containsKey("scope_str")){
                scopeConstraint = " scope_str ilike '%" + scope.get("scope_str") + "%' ";
            }
            if(!scopeConstraint.isEmpty()){
                //get meterSn within the scope
                String sql = "select " +meterIdColName+ " from meter where " + scopeConstraint;
                List<Map<String, Object>> meterIds = new ArrayList<>();
                try {
                    meterIds = oqgHelper.OqgR2(sql, true);
                } catch (Exception e) {
                    logger.info("Error getting "+meterIdColName+" within scope: " + scopeConstraint);
                    return Map.of("error", "Error getting "+meterIdColName+" within scope: " + scopeConstraint);
                }

                if(!meterIds.isEmpty()){
                    //build a 'in' string for sql
                    meterIdInStr = meterIds.stream().map(meter -> "'" + meter.get(meterIdColName) + "'").collect(Collectors.joining(","));
                    meterIdInStr = " and "+meterIdColName+" in (" + meterIdInStr + ")";
                }
            }
        }
        return Map.of("scope_constraint", meterIdInStr);
    }

    public Map<String, Object> getScopeItemConstraint(Map<String, String>scope,
                                                      String itemTableName, String itemIdColName){
        if(scope == null || scope.isEmpty()){
            return Map.of("scope_constraint", "");
        }

        String itemIdInStr = "";
        String scopeConstraint = "";
        //priority: site > project
        if(scope.containsKey("site_tag")){
            scopeConstraint = " site_tag = '" + scope.get("site_tag") + "' ";
        }else if(scope.containsKey("scope_str")){
            scopeConstraint = " scope_str ilike '%" + scope.get("scope_str") + "%' ";
        }
        Map<String, Object> result = new HashMap<>();
        if(!scopeConstraint.isEmpty()){
            //get itemId within the scope
            String sql = "select " +itemIdColName+ " from "+itemTableName+" where " + scopeConstraint;
            List<Map<String, Object>> itemIds = new ArrayList<>();
            try {
                itemIds = oqgHelper.OqgR2(sql, true);
            } catch (Exception e) {
                logger.info("Error getting "+itemIdColName+" within scope: " + scopeConstraint);
                return Map.of("error", "Error getting "+itemIdColName+" within scope: " + scopeConstraint);
            }

            if(!itemIds.isEmpty()){
                //build a 'in' string for sql
                itemIdInStr = itemIds.stream().map(meter -> "'" + meter.get(itemIdColName) + "'").collect(Collectors.joining(","));
                itemIdInStr = " and "+itemIdColName+" in (" + itemIdInStr + ")";
            }
            result.put("item_ids", itemIds);
            result.put("scope_constraint", itemIdInStr);
        }
        return result;
    }

    public Map<String, Object> getScopeItemConstraint3(Map<String, String>scope,
                                                       String itemTableName,
                                                       String itemIdColName,
                                                       List<String> selColNames){
        if(scope == null || scope.isEmpty()){
            return Map.of("scope_constraint", "");
        }

        String itemIdInStr = "";
        String scopeConstraint = "";
        //priority: site > project
        if(scope.containsKey("site_tag")){
            scopeConstraint = " site_tag = '" + scope.get("site_tag") + "' ";
        }else if(scope.containsKey("scope_str")){
            scopeConstraint = " scope_str ilike '%" + scope.get("scope_str") + "%' ";
        }
        Map<String, Object> result = new HashMap<>();
        if(!scopeConstraint.isEmpty()){
            //get itemId within the scope
            String sql = "select " +String.join(",", selColNames)+ " from "+itemTableName+" where " + scopeConstraint;
//            String sql = "select " +itemIdColName+ ", site_tag, scope_str from "+itemTableName+" where " + scopeConstraint;
            List<Map<String, Object>> items = new ArrayList<>();
            try {
                items = oqgHelper.OqgR2(sql, true);
            } catch (Exception e) {
                logger.info("Error getting "+ itemIdColName +" within scope: " + scopeConstraint);
                return Map.of("error", "Error getting "+ itemIdColName +" within scope: " + scopeConstraint);
            }

            if(!items.isEmpty()){
                //build a 'in' string for sql
                itemIdInStr = items.stream().map(meter -> "'" + meter.get(itemIdColName) + "'").collect(Collectors.joining(","));
                itemIdInStr =
//                        " and "+
                        itemIdColName+" in (" + itemIdInStr + ")";
            }
            result.put("items", items);
            result.put("scope_constraint", itemIdInStr);
        }
        return result;
    }

    public Map<String, Object> getScopeItemConstraint2(Map<String, String>scope,
                                                      String itemTableName, String itemIdColName){
        if(scope == null || scope.isEmpty()){
            return Map.of("scope_constraint", "");
        }

        String itemIdInStr = "";
        String scopeConstraint = "";
        //priority: site > project
        if(scope.containsKey("site_tag")){
            scopeConstraint = " site_tag = '" + scope.get("site_tag") + "' ";
        }else if(scope.containsKey("scope_str")){
            scopeConstraint = " scope_str ilike '%" + scope.get("scope_str") + "%' ";
        }
        Map<String, Object> result = new HashMap<>();
        if(!scopeConstraint.isEmpty()){
            //get itemId within the scope
            String sql = "select " +itemIdColName+ ", site_tag, scope_str from "+itemTableName+" where " + scopeConstraint;
            List<Map<String, Object>> items = new ArrayList<>();
            try {
                items = oqgHelper.OqgR2(sql, true);
            } catch (Exception e) {
                logger.info("Error getting "+itemIdColName+" within scope: " + scopeConstraint);
                return Map.of("error", "Error getting "+itemIdColName+" within scope: " + scopeConstraint);
            }

            if(!items.isEmpty()){
                //build a 'in' string for sql
                itemIdInStr = items.stream().map(meter -> "'" + meter.get(itemIdColName) + "'").collect(Collectors.joining(","));
                itemIdInStr =
//                        " and "+
                                itemIdColName+" in (" + itemIdInStr + ")";
            }
            result.put("items", items);
            result.put("scope_constraint", itemIdInStr);
        }
        return result;
    }

    @Deprecated
    public Map<String, Object> getInConstraint(String selectSql, String meterIdColName){

        String meterIdInStr = "";

        List<Map<String, Object>> meterIds;
        try {
            meterIds = oqgHelper.OqgR2(selectSql, true);
        } catch (Exception e) {
            logger.info("Error getting meterIds from sql: " + selectSql);
            return Map.of("error", "Error getting meterIds from sql: " + selectSql);
        }

        if(!meterIds.isEmpty()){
            //build a 'in' string for sql
            meterIdInStr = meterIds.stream().map(meter -> "'" + meter.get(meterIdColName) + "'").collect(Collectors.joining(","));
            meterIdInStr = " and "+meterIdColName+" in (" + meterIdInStr + ")";
        }

        return Map.of("in_constraint", meterIdInStr);
    }

    @Deprecated
    public Map<String, Object> getInConstraint2(String selectSql, String srcIdColName, String targetIdColName){

        String idInStr = "";

        String inConstraint = "";

        List<Map<String, Object>> ids = new ArrayList<>();
        try {
            ids = oqgHelper.OqgR2(selectSql, true);
        } catch (Exception e) {
            logger.info("Error getting ids from sql: " + selectSql);
            return Map.of("error", "Error getting ids from sql: " + selectSql);
        }

        if(!ids.isEmpty()){
            //build a 'in' string for sql
            idInStr = ids.stream().map(meter -> "'" + meter.get(srcIdColName) + "'").collect(Collectors.joining(","));
            idInStr = " and "+targetIdColName+" in (" + idInStr + ")";
        }

        return Map.of("in_constraint", idInStr);
    }

    public Map<String, Object> getInConstraint3(String selectSql, String srcIdColName, String targetIdColName){

        String idInStr = "";

//        String inConstraint = "";

        List<Map<String, Object>> ids = new ArrayList<>();
        try {
            ids = oqgHelper.OqgR2(selectSql, true);
        } catch (Exception e) {
            logger.info("Error getting ids from sql: " + selectSql);
            return Map.of("error", "Error getting ids from sql: " + selectSql);
        }

        if(!ids.isEmpty()){
            //build a 'in' string for sql
            idInStr = ids.stream().map(meter -> "'" + meter.get(srcIdColName) + "'").collect(Collectors.joining(","));

            String idColName = srcIdColName;
            if(targetIdColName != null && !targetIdColName.isEmpty()){
                idColName = targetIdColName;
            }
            idInStr = idColName+ " in (" + idInStr + ")";
        }
        return Map.of("in_constraint", idInStr);
    }

    public Map<String, Object> getFullRecordTimeRange(String tableName, String timeKey, String itemKey, String itemValue){
        String sql = "select min("+timeKey+") as min_time, max("+timeKey+") as max_time from " + tableName
                + " where " + itemKey + " = '" + itemValue + "'";

        List<Map<String, Object>> timeRange = new ArrayList<>();
        try {
            timeRange = oqgHelper.OqgR2(sql, true);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        if(timeRange.isEmpty()){
            return Collections.singletonMap("info", "no data");
        }
        return Collections.singletonMap("time_range", timeRange.get(0));
    }

    //get total kwh consumption for a list of meters in a scope for the past 24 hours
    public Map<String, Object> getAllActiveKwhConsumption(String localNow, Map<String, String> scope){
        List<Map<String, Object>> kwhConsumption = new ArrayList<>();

        Map<String, Object> scopeConstraint = getScopeConstraint(scope, "meter_sn");
        if (scopeConstraint.containsKey("error")){
            return scopeConstraint;
        }
        String meterSnInStr = scopeConstraint.get("scope_constraint").toString();

        String sql = "select sum(kwh_diff) as kwh_total from meter_tariff " +
                " where kwh_diff is not null " +
                " and tariff_timestamp > timestamp '" + localNow + "' - interval '24 hours' "
                + meterSnInStr;

        try {
            kwhConsumption = oqgHelper.OqgR2(sql, true);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        if(kwhConsumption.isEmpty()){
            return Collections.singletonMap("info", "no data");
        }
        //sum() will always return a value, even if there is no data, the list will still have 1 element
        if(kwhConsumption.getFirst().get("kwh_total") == null){
            return Collections.singletonMap("info", "no data");
        }
        double kwhTotal = Double.parseDouble(kwhConsumption.getFirst().get("kwh_total").toString());
        return Collections.singletonMap("active_kwh_consumption", kwhTotal);
    }

    public double getAllRecentCommData(){
        String tableName = "meter_comm_data";
        List<Map<String, Object>> dataConsumption = new ArrayList<>();

        String sgNow = DateTimeUtil.getZonedDateTimeStr(now(), ZoneId.of("Asia/Singapore"));
        String sql = "select sum(data_bal_diff) as data_total from " + tableName +
                " where data_bal_diff is not null " +
                " and data_bal_timestamp > timestamp '" + sgNow + "' - interval '24 hours' ";

        try {
            dataConsumption = oqgHelper.OqgR2(sql, true);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        if(dataConsumption.isEmpty()){
            return 0;
        }
        //sum() will always return a value, even if there is no data, the list will still have 1 element
        if(dataConsumption.getFirst().get("data_total") == null){
            return 0;
        }
        return Double.parseDouble(dataConsumption.getFirst().get("data_total").toString());
    }

    public Map<String, Object> getAllActiveKwhConsumptionHistory(int days, LocalDateTime localNow, Map<String, String> scope){

        String tableName = "meter_tariff";
        String timeKey = "tariff_timestamp";
        String labelAs = "total_kwh";

        Map<String, Object> scopeConstraint = getScopeConstraint(scope, "meter_sn");
        if (scopeConstraint.containsKey("error")){
            return scopeConstraint;
        }
        String meterSnInStr = scopeConstraint.get("scope_constraint").toString();

        StringBuilder queryBuilder = new StringBuilder();

        // Iterate over the past 7 days
        for (int i = 0; i < days; i++) {
            // Subtract i days from the current date
            LocalDateTime localNowOffset = localNow.minusDays(i);
            String localNowOffsetStr = DateTimeUtil.getLocalDateTimeStr(localNowOffset);

            // Append the SQL statement for each day to the query builder
            queryBuilder.append("SELECT '")
                    .append(localNowOffsetStr).append("' AS timestamp, sum(kwh_diff) as ").append(labelAs).append(" FROM ")
                    .append(tableName)
                    .append(" WHERE ")
                    .append(timeKey)
                    .append(" > TIMESTAMP '")
                    .append(localNowOffsetStr)
                    .append("' - INTERVAL '24 hours'")
                    .append(" AND ")
                    .append(timeKey)
                    .append(" <= TIMESTAMP '")
                    .append(localNowOffsetStr)
                    .append("'")
                    .append(meterSnInStr);

            // Add a UNION ALL between each statement except the last one
            if (i < days - 1) {
                queryBuilder.append(" UNION ALL ");
            }
        }

        // Store the final SQL query string
        String sql = queryBuilder.toString();
        List<Map<String, Object>> kwhConsumption = new ArrayList<>();
        try {
            kwhConsumption = oqgHelper.OqgR2(sql, true);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        if(kwhConsumption.isEmpty()){
            return Collections.singletonMap("info", "no data");
        }
        //sort by date in descending order
        kwhConsumption.sort(Comparator.comparing(m -> m.get("timestamp").toString()));
        Collections.reverse(kwhConsumption);

        return Collections.singletonMap("active_kwh_consumption_history", kwhConsumption);
    }
    public Map<String, Object> getAllTopupAmount(String localNow, Map<String, String> scope){
        List<Map<String, Object>>topupTotal = new ArrayList<>();
        String labelAs = "total_topup";

        Map<String, Object> scopeConstraint = getScopeConstraint(scope, "meter_displayname");
        if (scopeConstraint.containsKey("error")){
            return scopeConstraint;
        }
        String meterDisplaynameInStr = scopeConstraint.get("scope_constraint").toString();

//        String sgNow = DateTimeUtil.getZonedDateTimeStr(now(), ZoneId.of("Asia/Singapore"));
//        String sql = "select sum(credit_amt) as credit_total from meter_tariff " +
//                " where credit_amt is not null " +
//                " and tariff_timestamp > timestamp '" + sgNow + "' - interval '24 hours' ";

        String sql = "select sum(topup_amt) as "+labelAs+" from transaction_log " +
                " where topup_amt is not null " +
                " and transaction_status = 3 " +
                " and payment_mode != 4" +
                " and transaction_log_timestamp > timestamp '" + localNow + "' - interval '24 hours' " +
                " and meter_displayname not like 'pi%' "
                + meterDisplaynameInStr;
        try {
            topupTotal = oqgHelper.OqgR2(sql, true);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        if(topupTotal.isEmpty()){
            return Collections.singletonMap("info", "no data");
        }
        //sum() will always return a value, even if there is no data, the list will still have 1 element
        if(topupTotal.get(0).get(labelAs) == null){
            return Collections.singletonMap("info", "no data");
        }
        double totalTopup = Double.parseDouble(topupTotal.get(0).get(labelAs).toString());
        return Collections.singletonMap(labelAs, totalTopup);
    }

    public Map<String, Object> getTotalTopupHistory(int days, LocalDateTime localNow, Map<String, String> scope){
        String tableName = "transaction_log";
        String timeKey = "transaction_log_timestamp";
        String labelAs = "total_topup";

        Map<String, Object> scopeConstraint = getScopeConstraint(scope, "meter_displayname");
        if (scopeConstraint.containsKey("error")){
            return scopeConstraint;
        }
        String meterDisplaynameInStr = scopeConstraint.get("scope_constraint").toString();

        StringBuilder queryBuilder = new StringBuilder();
        // Iterate over the past n days
        for (int i = 0; i < days; i++) {
            // Subtract i days from the current date
            LocalDateTime localNowOffset = localNow.minusDays(i);
            String localNowOffsetStr = DateTimeUtil.getLocalDateTimeStr(localNowOffset);

            // Append the SQL statement for each day to the query builder
            queryBuilder.append("SELECT '")
                    .append(localNowOffsetStr).append("' AS timestamp, sum(topup_amt) as ").append(labelAs).append(" FROM ")
                    .append(tableName)
                    .append(" WHERE ")
                    .append(" topup_amt is not null ")
                    .append(" and transaction_status = 3 ")
                    .append(" and payment_mode != 4 ")
                    .append(" and ")
                    .append(timeKey)
                    .append(" > TIMESTAMP '")
                    .append(localNowOffsetStr)
                    .append("' - INTERVAL '24 hours'")
                    .append(" AND ")
                    .append(timeKey)
                    .append(" <= TIMESTAMP '")
                    .append(localNowOffsetStr)
                    .append("'")
                    .append(" and meter_displayname not like 'pi%' ")
                    .append(meterDisplaynameInStr);

            // Add a UNION ALL between each statement except the last one
            if (i < days - 1) {
                queryBuilder.append(" UNION ALL ");
            }
        }

        // Store the final SQL query string
        String sql = queryBuilder.toString();
        List<Map<String, Object>> totalTopup = new ArrayList<>();
        try {
            totalTopup = oqgHelper.OqgR2(sql, true);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        if(totalTopup.isEmpty()){
            return Collections.singletonMap("info", "no data");
        }
        for(Map<String, Object> topup : totalTopup){
            topup.putIfAbsent(labelAs, 0);
        }
        //sort by date in descending order
        totalTopup.sort(Comparator.comparing(m -> m.get("timestamp").toString()));
        Collections.reverse(totalTopup);

        return Collections.singletonMap("total_topup_history", totalTopup);
    }

    public Map<String, Object> getRecentKwhConsumptions(int days){

        String tableName = "meter_tariff";
        String timeKey = "tariff_timestamp";
        String sgNow = DateTimeUtil.getZonedDateTimeStr(now(), ZoneId.of("Asia/Singapore"));
        int hours = days * 24;
        // Create a StringBuilder to build the SQL query
        String sql = "select sum(kwh_diff) as kwh_total, meter_sn from " + tableName +
                " where kwh_diff is not null " +
                " and tariff_timestamp > timestamp '" + sgNow + "' - interval '" + hours+ " hours' " +
                " group by meter_sn";

        List<Map<String, Object>> totalConsumptions = new ArrayList<>();
        try {
            totalConsumptions = oqgHelper.OqgR2(sql, true);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        if(totalConsumptions.isEmpty()){
            return Collections.singletonMap("info", "no data");
        }
//        //sort by date in descending order
//        totalTopup.sort(Comparator.comparing(m -> m.get("timestamp").toString()));
//        Collections.reverse(totalTopup);

        return Collections.singletonMap("recent_kwh_consumptions", totalConsumptions);
    }
    public List<String> getAllMeterSns(String tableName){
        if(tableName ==null || tableName.isEmpty()){
            tableName = "meter_tariff";
        }

        List<Map<String, Object>> meterSns = new ArrayList<>();

        String sgNow = DateTimeUtil.getZonedDateTimeStr(now(), ZoneId.of("Asia/Singapore"));
        String sql = "select distinct meter_sn from " + tableName;

        try {
            meterSns = oqgHelper.OqgR2(sql, true);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
//        return meterSns.stream().map(meterSn -> meterSn.get("meter_sn").toString()).toList();
        List<String> meterSnList = new ArrayList<>();
        for(Map<String, Object> meterSn : meterSns){
            if(meterSn.get("meter_sn") != null){
                meterSnList.add(meterSn.get("meter_sn").toString());
            }
        }
        return meterSnList;
    }
    public String getTimeKey(String tableName){
        return switch (tableName) {
            case "meter_reading" -> "kwh_timestamp";
            case "meter_tariff" -> "tariff_timestamp";
            default -> "kwh_timestamp";
        };
    }
    public List<String> getActiveMeterSns2(String tableName, String sinceDateTimeStr){
        String timeKey = getTimeKey(tableName);

        if(tableName ==null || tableName.isEmpty()){
            tableName = "meter_tariff";
            timeKey = "tariff_timestamp";
        }

        List<Map<String, Object>> meterSns;
        String sql = "select distinct meter_sn from " + tableName;

        if(sinceDateTimeStr != null && !sinceDateTimeStr.isEmpty()){
            sql = sql + " where "+timeKey+" > timestamp '" + sinceDateTimeStr + "'";
        }

        try {
            meterSns = oqgHelper.OqgR2(sql, true);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
//        return meterSns.stream().map(meterSn -> meterSn.get("meter_sn").toString()).toList();
        List<String> meterSnList = new ArrayList<>();
        for(Map<String, Object> meterSn : meterSns){
            if(meterSn.get("meter_sn") != null){
                meterSnList.add(meterSn.get("meter_sn").toString());
            }
        }
        return meterSnList;
    }
    public String getMeterSnFromMeterDisplayname(String meterDisplayname) {
        String sql = "select meter_sn from meter where meter_displayname = '" + meterDisplayname + "'";
        List<Map<String, Object>> meterSn = new ArrayList<>();
        try {
            meterSn = oqgHelper.OqgR2(sql, true);
        } catch (Exception e) {
            logger.info("Error getting meter_sn for meterDisplayname: " + meterDisplayname);
            throw new RuntimeException(e);
        }
        if (meterSn.isEmpty()) {
            logger.info("meter_sn is empty for meterDisplayname: " + meterDisplayname);
            return "";
        }
        return (meterSn.getFirst().get("meter_sn") == null ? "" : meterSn.getFirst().get("meter_sn").toString());
    }
    public String getMeterDisplaynameFromSn(String meterSn) {
        String sql = "select meter_displayname from meter where meter_sn = '" + meterSn + "'";
        List<Map<String, Object>> meterSnList = new ArrayList<>();
        try {
            meterSnList = oqgHelper.OqgR2(sql, true);
        } catch (Exception e) {
            logger.info("Error getting meter_displayname for meter_sn: " + meterSn);
            throw new RuntimeException(e);
        }
        if (meterSnList.isEmpty()) {
            logger.info("meter_displayname is empty for meter_sn: " + meterSn);
            return "";
        }
        return (meterSnList.getFirst().get("meter_displayname") == null ? "" : meterSnList.getFirst().get("meter_displayname").toString());
    }
    public Map<String, Object> getLatestMeterCredit(String meterSnStr, String tableName){
        if(tableName == null || tableName.isBlank()){
            tableName = "meter_tariff";
        }
        String sql = "select ref_bal, tariff_timestamp from " + tableName + " where meter_sn = '" + meterSnStr + "'" +
                " and ref_bal is not null order by tariff_timestamp desc limit 1";
        List<Map<String, Object>> meterCredit = new ArrayList<>();
        try {
            meterCredit = oqgHelper.OqgR2(sql, true);
        } catch (Exception e) {
            logger.info("Error getting credit for meterSn: " + meterSnStr);
            throw new RuntimeException(e);
        }
        if(meterCredit.isEmpty()){
            logger.info("ref_bal is empty for meterSn: " + meterSnStr);
            return Collections.singletonMap("info", "ref_bal is empty for meterSn: " + meterSnStr);
        }
        return meterCredit.getFirst();
    }

    public List<Long> getConcentratorIDs(){
        String sql = "select id from concentrator";
        List<Map<String, Object>> concentratorIds = new ArrayList<>();
        try {
            concentratorIds = oqgHelper.OqgR2(sql, true);
        } catch (Exception e) {
            logger.info("Error getting concentrator IDs");
            throw new RuntimeException(e);
        }
        if(concentratorIds.isEmpty()){
            logger.info("no concentrator IDs found");
            return new ArrayList<>();
        }
        return concentratorIds.stream().map(concentratorId -> MathUtil.ObjToLong(concentratorId.get("id"))).toList();
    }

    public long getConcentratorIdFromMeterSn(String meterSnStr){
        String sql = "select concentrator_id from meter where meter_sn = '" + meterSnStr + "'";
        List<Map<String, Object>> concentrator = new ArrayList<>();
        try {
            concentrator = oqgHelper.OqgR2(sql, true);
        } catch (Exception e) {
            logger.info("Error getting concentrator_id for meterSn: " + meterSnStr);
            throw new RuntimeException(e);
        }
        if(concentrator.isEmpty()){
            logger.info("concentrator is empty for meterSn: " + meterSnStr);
            return -1;
        }
        return MathUtil.ObjToLong(concentrator.getFirst().get("concentrator_id")==null?-1:concentrator.getFirst().get("concentrator_id"));
    }
    public Map<Long, Map<String, Object>> getConcentratorTariff(){
        //get a list of concentrator_id from concentrator table
        String sqlConcentrator = "select id from concentrator";
        List<Map<String, Object>> concentrators = new ArrayList<>();
        try {
            concentrators = oqgHelper.OqgR2(sqlConcentrator, true);
        } catch (Exception e) {
            logger.info("Error getting concentrator list");
            throw new RuntimeException(e);
        }

        Map<Long, Map<String, Object>> concentratorTariff = new HashMap<>();

        for(Map<String, Object> concentrator : concentrators) {
            long concentratorId = MathUtil.ObjToLong(concentrator.get("id"));

            //get tariff for this concentrator_id from concentrator_tariff table
            String sqlTariff = "select offer_id, tariff_price from concentrator_tariff where concentrator_id = " + concentratorId;
            List<Map<String, Object>> tariff = new ArrayList<>();
            try {
                tariff = oqgHelper.OqgR2(sqlTariff, true);
            } catch (Exception e) {
                logger.info("Error getting tariff for concentratorId: " + concentratorId);
                throw new RuntimeException(e);
            }
            if(tariff.isEmpty()){
                logger.info("tariff is empty for concentratorId: " + concentratorId);
                continue;
            }
            concentratorTariff.put(concentratorId,
                    Map.of("tariff_price", MathUtil.ObjToDouble(tariff.getFirst().get("tariff_price")),
                            "offer_id", MathUtil.ObjToLong(tariff.getFirst().get("offer_id"))));
        }
        return concentratorTariff;
    }
    public Map<String, Object> getMeterTariffFromSn(String meterSnStr){
        String sql = "select tariff_price from concentrator_tariff " +
                " inner join meter on meter.concentrator_id = concentrator_tariff.concentrator_id " +
                " where meter_sn = '" + meterSnStr + "'";
        List<Map<String, Object>> meterTariff = new ArrayList<>();
        try {
            meterTariff = oqgHelper.OqgR2(sql, true);
        } catch (Exception e) {
            logger.info("Error getting tariff for meterSn: " + meterSnStr);
            throw new RuntimeException(e);
        }
        if(meterTariff.isEmpty()){
            logger.info("tariff is empty for meterSn: " + meterSnStr);
            return Collections.singletonMap("info", "tariff is empty for meterSn: " + meterSnStr);
        }
        return meterTariff.getFirst();
    }
    public boolean meterSnExistsInMeterTable(String meterSnStr){
        String sql = "select id from meter where meter_sn = '" + meterSnStr + "'";
        List<Map<String, Object>> meter = new ArrayList<>();
        try {
            meter = oqgHelper.OqgR2(sql, true);
        } catch (Exception e) {
            logger.info("Error getting meter for meterSn: " + meterSnStr);
            throw new RuntimeException(e);
        }
        return !meter.isEmpty();
    }

    public Map<String, Object> getEpochRefBalTimeStamp(String meterSnStr, String tableName){
        if(tableName == null || tableName.isBlank()){
            tableName = "meter_tariff";
        }
        String sql = "select tariff_timestamp from " + tableName + " where meter_sn = '" + meterSnStr + "'" +
                " and ref_bal_tag = 'ref_bal_epoch' order by id desc limit 1";
        List<Map<String, Object>> meterCredit = new ArrayList<>();
        try {
            meterCredit = oqgHelper.OqgR2(sql, true);
        } catch (Exception e) {
            logger.info("Error getting credit for meterSn: " + meterSnStr);
            throw new RuntimeException(e);
        }
        if(meterCredit.isEmpty()){
            logger.info("epoch_ref_bal is empty for meterSn: " + meterSnStr);
            return Collections.singletonMap("info", "epoch_ref_bal is empty for meterSn: " + meterSnStr);
        }
        return meterCredit.getFirst();
    }
    public Map<String, Object> getRecentMeterKiv(){
        List<Map<String, Object>>meterKiv = new ArrayList<>();

        String sgNow = DateTimeUtil.getZonedDateTimeStr(now(), ZoneId.of("Asia/Singapore"));
        String sql = "select * from meter_kiv " +
                " where kiv_tag != 'missing_ref_bal_epoch' " +
                " and kiv_tag != 'reading_interval' " +
                " and kiv_start_timestamp > timestamp '" + sgNow + "' - interval '72 hours' "
                + " order by kiv_start_timestamp desc";
        try {
            meterKiv = oqgHelper.OqgR2(sql, true);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return Map.of("meter_kiv", meterKiv);
    }

    public void postMeterKiv2(String meterSnStr,
                              String kivTag,
                              String postDateTimeStr,
                              long numOfEvents,
                              String kivRef,
                              double kivVal,
                              String postedBy,
                              String sessionId){
        String meterKivTable = "meter_kiv";

        if(postDateTimeStr == null || postDateTimeStr.isEmpty()) {
            postDateTimeStr = DateTimeUtil.getZonedDateTimeStr(now(), ZoneId.of("Asia/Singapore"));;
        }
        if(sessionId == null || sessionId.isEmpty()) {
            sessionId = UUID.randomUUID().toString();
        }

        if(!meterSnExistsInMeterTable(meterSnStr)){
            logger.info("meter_sn: " + meterSnStr + " does not exist in meter table");
            return;
        }

        //check if the record for that session_id exists
        String sqlCheck = "select id, number_of_events from " + meterKivTable +
                " where meter_sn = '" + meterSnStr +
                "' and session_id = '" + sessionId + "'";
        List<Map<String, Object>> meterKiv = new ArrayList<>();
        try {
            meterKiv = oqgHelper.OqgR2(sqlCheck, true);
        } catch (Exception e) {
            logger.info("Error getting meterKiv for meterSn: " + meterSnStr);
            throw new RuntimeException(e);
        }
        if(meterKiv.isEmpty()) {
            Map<String, Object> kivSqlMap = Map.of("table", meterKivTable,
                    "content", Map.of(
                            "meter_sn", meterSnStr,
                            "kiv_tag", kivTag,
                            "kiv_start_timestamp", postDateTimeStr,
                            "number_of_events", numOfEvents,
                            "kiv_status","posted",
                            "kiv_ref", kivRef,
                            "kiv_val", kivVal,
                            "posted_by", postedBy,
                            "session_id", sessionId
                    ));
            Map<String, String> sqlInsert = SqlUtil.makeInsertSql(kivSqlMap);
            if(sqlInsert.get("sql")==null){
                logger.info("Error getting insert sql for meterKiv for meterSn: " + meterSnStr);
                throw new RuntimeException("Error getting insert sql for meterKiv for meterSn: " + meterSnStr);
            }
            try {
                oqgHelper.OqgIU(sqlInsert.get("sql"));
            } catch (Exception e) {
                logger.info("Error inserting meterKiv for meterSn: " + meterSnStr);
                throw new RuntimeException(e);
            }
        } else {
            //update the record
            long id = MathUtil.ObjToLong(meterKiv.getFirst().get("id"));
            long numOfEventsOld = MathUtil.ObjToLong(meterKiv.get(0).get("number_of_events")==null?0:meterKiv.get(0).get("number_of_events"));
            long numOfEventsNew = numOfEventsOld + numOfEvents;
            String sqlUpdate = "update " + meterKivTable + " set number_of_events = " + numOfEventsNew +
                    " where id = " + id;
            try {
                oqgHelper.OqgIU(sqlUpdate);
            } catch (Exception e) {
                logger.info("Error updating meterKiv for meterSn: " + meterSnStr);
                throw new RuntimeException(e);
            }
        }
    }

    public void postMeterKiv3(String meterSnStr,
                              String kivTag,
                              String postDateTimeStr,
                              long numOfEvents,
                              LinkedHashMap<String, Double> kivRefValMap,
                              String postedBy,
                              String sessionId){
        String meterKivTable = "meter_kiv";

        if(postDateTimeStr == null || postDateTimeStr.isEmpty()) {
            postDateTimeStr = DateTimeUtil.getZonedDateTimeStr(now(), ZoneId.of("Asia/Singapore"));;
        }
        if(sessionId == null || sessionId.isEmpty()) {
            sessionId = UUID.randomUUID().toString();
        }

        if(!meterSnExistsInMeterTable(meterSnStr)){
            logger.info("meter_sn: " + meterSnStr + " does not exist in meter table");
            return;
        }

        //check if the record for that session_id exists
        String sqlCheck = "select id, number_of_events from " + meterKivTable +
                " where meter_sn = '" + meterSnStr +
                "' and session_id = '" + sessionId + "'";
        List<Map<String, Object>> meterKiv = new ArrayList<>();
        try {
            meterKiv = oqgHelper.OqgR2(sqlCheck, true);
        } catch (Exception e) {
            logger.info("Error getting meterKiv for meterSn: " + meterSnStr);
            throw new RuntimeException(e);
        }
        if(meterKiv.isEmpty()) {
            Map<String, Object> kivSqlMap = Map.of("table", meterKivTable,
                    "content", Map.ofEntries(
                            Map.entry("meter_sn", meterSnStr),
                            Map.entry("kiv_tag", kivTag),
                            Map.entry("kiv_start_timestamp", postDateTimeStr),
                            Map.entry("number_of_events", numOfEvents),
                            Map.entry("kiv_status","posted"),
                            Map.entry("kiv_ref", kivRefValMap.keySet().toArray()[0]),
                            Map.entry("kiv_val", kivRefValMap.values().toArray()[0]),
                            Map.entry("kiv_ref2", kivRefValMap.keySet().toArray()[1]),
                            Map.entry("kiv_val2", kivRefValMap.values().toArray()[1]),
                            Map.entry("kiv_ref3", kivRefValMap.keySet().toArray()[2]),
                            Map.entry("kiv_val3", kivRefValMap.values().toArray()[2]),
                            Map.entry("posted_by", postedBy),
                            Map.entry("session_id", sessionId)
                    ));
            Map<String, String> sqlInsert = SqlUtil.makeInsertSql(kivSqlMap);
            if(sqlInsert.get("sql")==null){
                logger.info("Error getting insert sql for meterKiv for meterSn: " + meterSnStr);
                throw new RuntimeException("Error getting insert sql for meterKiv for meterSn: " + meterSnStr);
            }
            try {
                oqgHelper.OqgIU(sqlInsert.get("sql"));
            } catch (Exception e) {
                logger.info("Error inserting meterKiv for meterSn: " + meterSnStr);
                throw new RuntimeException(e);
            }
        } else {
            //update the record
            long id = MathUtil.ObjToLong(meterKiv.get(0).get("id"));
            long numOfEventsOld = MathUtil.ObjToLong(meterKiv.get(0).get("number_of_events")==null?0:meterKiv.get(0).get("number_of_events"));
            long numOfEventsNew = numOfEventsOld + numOfEvents;
            String sqlUpdate = "update " + meterKivTable + " set number_of_events = " + numOfEventsNew +
                    " where id = " + id;
            try {
                oqgHelper.OqgIU(sqlUpdate);
            } catch (Exception e) {
                logger.info("Error updating meterKiv for meterSn: " + meterSnStr);
                throw new RuntimeException(e);
            }
        }
    }

    public void postMeterKiv4(String meterSnStr,
                              String kivTag,
                              String postDateTimeStr,
                              long numOfEvents,
                              LinkedHashMap<String, Object> kivRefValMap,
                              String postedBy,
                              String sessionId){
        String meterKivTable = "meter_kiv";

        if(postDateTimeStr == null || postDateTimeStr.isEmpty()) {
            postDateTimeStr = DateTimeUtil.getZonedDateTimeStr(now(), ZoneId.of("Asia/Singapore"));;
        }
        if(sessionId == null || sessionId.isEmpty()) {
            sessionId = UUID.randomUUID().toString();
        }

        if(!meterSnExistsInMeterTable(meterSnStr)){
            logger.info("meter_sn: " + meterSnStr + " does not exist in meter table");
            return;
        }

        //check if the record for that session_id exists
        String sqlCheck = "select id, number_of_events from " + meterKivTable +
                " where meter_sn = '" + meterSnStr +
                "' and session_id = '" + sessionId + "'";
        List<Map<String, Object>> meterKiv = new ArrayList<>();
        try {
            meterKiv = oqgHelper.OqgR2(sqlCheck, true);
        } catch (Exception e) {
            logger.info("Error getting meterKiv for meterSn: " + meterSnStr);
            throw new RuntimeException(e);
        }
        if(meterKiv.isEmpty()) {
            Map<String, Object> kivSqlMap = Map.of("table", meterKivTable,
                    "content", Map.ofEntries(
                            Map.entry("meter_sn", meterSnStr),
                            Map.entry("kiv_tag", kivTag),
                            Map.entry("kiv_start_timestamp", postDateTimeStr),
                            Map.entry("number_of_events", numOfEvents),
                            Map.entry("kiv_status","posted"),
                            Map.entry("kiv_ref", kivRefValMap.keySet().toArray()[0]),
                            Map.entry("kiv_val", kivRefValMap.values().toArray()[0]),
                            Map.entry("kiv_ref2", kivRefValMap.keySet().toArray()[1]),
                            Map.entry("kiv_val2", kivRefValMap.values().toArray()[1]),
                            Map.entry("kiv_ref3", kivRefValMap.keySet().toArray()[2]),
                            Map.entry("kiv_val3", kivRefValMap.values().toArray()[2]),
                            Map.entry("kiv_ref4", kivRefValMap.keySet().toArray()[3]),
                            Map.entry("kiv_val4", kivRefValMap.values().toArray()[3]),
                            Map.entry("posted_by", postedBy),
                            Map.entry("session_id", sessionId)
                    ));
            Map<String, String> sqlInsert = SqlUtil.makeInsertSql(kivSqlMap);
            if(sqlInsert.get("sql")==null){
                logger.info("Error getting insert sql for meterKiv for meterSn: " + meterSnStr);
                throw new RuntimeException("Error getting insert sql for meterKiv for meterSn: " + meterSnStr);
            }
            try {
                oqgHelper.OqgIU(sqlInsert.get("sql"));
            } catch (Exception e) {
                logger.info("Error inserting meterKiv for meterSn: " + meterSnStr);
                throw new RuntimeException(e);
            }
        } else {
            //update the record
            long id = MathUtil.ObjToLong(meterKiv.getFirst().get("id"));
            long numOfEventsOld = MathUtil.ObjToLong(meterKiv.getFirst().get("number_of_events")==null?0:meterKiv.get(0).get("number_of_events"));
            long numOfEventsNew = numOfEventsOld + numOfEvents;
            String sqlUpdate = "update " + meterKivTable + " set number_of_events = " + numOfEventsNew +
                    " where id = " + id;
            try {
                oqgHelper.OqgIU(sqlUpdate);
            } catch (Exception e) {
                logger.info("Error updating meterKiv for meterSn: " + meterSnStr);
                throw new RuntimeException(e);
            }
        }
    }

    public long findMeterReadingInterval(String meterSnStr){
        String sql = "select reading_interval from meter_tariff where meter_sn = '" + meterSnStr + "'" +
                " and kwh_diff is not null " +
                " and reading_interval is not null " +
                " order by tariff_timestamp desc " +
                " limit 8";
        List<Map<String, Object>> intervals = new ArrayList<>();
        try {
            intervals = oqgHelper.OqgR2(sql, true);
        } catch (Exception e) {
            logger.info("Error getting meter tariff for meterSn: " + meterSnStr);
            throw new RuntimeException(e);
        }
//        if(intervals.isEmpty()){
//            return 0;
//        }
        if(intervals.size()<8){
            return 0;
        }
        List<Double> intervalList = new ArrayList<>();
        intervalList = intervals.stream().map(m -> MathUtil.ObjToDouble(m.get("reading_interval"))/60).collect(Collectors.toList());
        return MathUtil.findDominantLong(intervalList);
    }
    public void updateMeterReadingInterval(String meterSnStr, long interval){
        String sql = "update meter set reading_interval = " + interval +
                " where meter_sn = '" + meterSnStr + "'" ;
        try {
            oqgHelper.OqgIU(sql);
        } catch (Exception e) {
            logger.info("Error updating meter reading interval for meterSn: " + meterSnStr);
            throw new RuntimeException(e);
        }
    }
    public void updateMeterMeterMmsFullAddress(String meterSnStr, String mmsAddress){
        //add escape character for single quote
        mmsAddress = mmsAddress.replace("'", "''");
        String sql = "update meter set mms_address = '" + mmsAddress +
                "' where meter_sn = '" + meterSnStr + "'" ;
        try {
            oqgHelper.OqgIU(sql);
        } catch (Exception e) {
            logger.info("Error updating meter mms address for meterSn: " + meterSnStr);
            throw new RuntimeException(e);
        }
    }
    public void updateMeterMmsInfo(String meterSnStr, Map<String, String> mmsInfo){
        String mmsAddress = mmsInfo.get("mms_address");
        String mmsUnit = mmsInfo.get("mms_unit");
        String mmsLevel = mmsInfo.get("mms_level");
        String mmsBlk = mmsInfo.get("mms_block");
        String mmsBuilding = mmsInfo.get("mms_building");
        String eSimId = mmsInfo.get("esim_id");
        String mmsOnlineTimestamp = mmsInfo.get("mms_online_timestamp");
//        if(mmsAddress==null && mmsBlk==null && mmsBuilding==null){
//            return;
//        }
        //add escape character for single quote
        mmsAddress = mmsAddress.replace("'", "''");
        mmsUnit = mmsUnit == null? "": mmsUnit.replace("'", "''");
        mmsBlk = mmsBlk == null? "": mmsBlk.replace("'", "''");
        mmsLevel = mmsLevel == null? "": mmsLevel.replace("'", "''");
        mmsBuilding = mmsBuilding == null? "": mmsBuilding.replace("'", "''");
        eSimId = eSimId == null? "": eSimId;
        String sql = "update meter set " +
                " mms_address = '" + mmsAddress +
                "', mms_unit = '" + mmsUnit +
                "', mms_level = '" + mmsLevel +
                "', mms_block = '" + mmsBlk +
                "', mms_building = '" + mmsBuilding +
                "', esim_id = '" + eSimId + "' ";
        if(mmsOnlineTimestamp!=null){
            sql += ", mms_online_timestamp = '" + mmsOnlineTimestamp + "' ";
        }
        sql += " where meter_sn = '" + meterSnStr + "'" ;

        try {
            oqgHelper.OqgIU(sql);
        } catch (Exception e) {
            logger.info("Error updating meter mms info for meterSn: " + meterSnStr);
            throw new RuntimeException(e);
        }
    }
    public void updateMmsPremise(Map<String, String> mmsAddressInfo){
        String unit = mmsAddressInfo.get("mms_unit");
        String level = mmsAddressInfo.get("mms_level");
        String block = mmsAddressInfo.get("mms_block");
        String building = mmsAddressInfo.get("mms_building");
        String street = mmsAddressInfo.get("mms_street");
        String postalCode = mmsAddressInfo.get("mms_postal_code");
        String scopeStr = mmsAddressInfo.get("scope_str");

        //add escape character for single quote
        unit = unit == null? "": unit.replace("'", "''");
        block = block == null? "": block.replace("'", "''");
        level = level == null? "": level.replace("'", "''");
        building = building == null? "": building.replace("'", "''");
        street = street == null? "": street.replace("'", "''");

        String sel = "select id from premise where " +
                " street = '" + street + "' and " +
                " building = '" + building + "' and " +
                " block = '" + block + "' and " +
                " level = '" + level + "' and " +
                " unit = '" + unit + "' and " +
                " postal_code = '" + postalCode + "'";
        List<Map<String, Object>> premises = new ArrayList<>();
        try {
            premises = oqgHelper.OqgR2(sel, true);
        } catch (Exception e) {
            logger.info("Error getting premise for address info: " + building + " " + block + " " + level + " " + postalCode);
            throw new RuntimeException(e);
        }

        String buildingId =
                block.trim().toLowerCase() + "-" +
                        building.trim().replace(" ", "-").replace("'", "-").toLowerCase() + "-" +
                        postalCode.trim().toLowerCase();
        if(premises.isEmpty()) {
            String ins = "insert into premise " +
                    "(id, street, building, block, level, unit, postal_code, premise_type_id, building_identifier, scope_str) " +
                    "values (" + "(select max(id)+1 as available_id from premise)" + "," +
                    "'" + street + "'," +
                    "'" + building + "'," +
                    "'" + block + "'," +
                    "'" + level + "'," +
                    "'" + unit + "'," +
                    "'" + postalCode + "'," +
                    " 23, " +
                    "'" + buildingId + "'," +
                    "'" + scopeStr + "'" +
                    ")";
            try {
                oqgHelper.OqgIU(ins);
            } catch (Exception e) {
                logger.info("Error inserting premise for address info: " + building + " " + block + " " + level + " " + postalCode);
                throw new RuntimeException(e);
            }
        }else if(premises.size()==1) {
            String upd = "update premise set " +
                    " street = '" + street + "'," +
                    " building = '" + building + "'," +
                    " block = '" + block + "'," +
                    " level = '" + level + "'," +
                    " unit = '" + unit + "'," +
                    " postal_code = '" + postalCode + "'," +
                    " building_identifier = '" + buildingId + "'," +
                    " scope_str = '" + scopeStr + "'" +
                    " where id = " + premises.getFirst().get("id");
            try {
                oqgHelper.OqgIU(upd);
            } catch (Exception e) {
                logger.info("Error updating premise for address info: " + building + " " + block + " " + level + " " + postalCode);
                throw new RuntimeException(e);
            }
        }
    }

    public Map<String, Object> getScopeBuildings(String scope){
        String sql = "SELECT DISTINCT building_identifier, building, block, postal_code FROM premise WHERE scope_str Like '%" + scope + "%'";
        List<Map<String, Object>> buildings = new ArrayList<>();
        try {
            buildings = oqgHelper.OqgR2(sql, true);
        }catch (Exception e){
            logger.info("oqgHelper error: "+e.getMessage());
            return Map.of("error", e.getMessage());
        }
        return Map.of("buildings", buildings);
    }
    public Map<String, Object> getMeterSnInBuilding(String building, String block){

        String mmsBlk = block == null? "": block.replace("'", "''");
        String mmsBuilding = building == null? "": building.replace("'", "''");

        String sql = "SELECT meter_sn, meter_displayname FROM meter WHERE " +
                " mms_building = '" + mmsBuilding + "' AND mms_block = '" + mmsBlk + "'";
        List<Map<String, Object>> meters = new ArrayList<>();
        try {
            meters = oqgHelper.OqgR2(sql, true);
        }catch (Exception e){
            logger.info("oqgHelper error: "+e.getMessage());
        }
        return Map.of("meters", meters);
    }
    public void updateMeterTataInfo(Map<String, String> tataInfo) {
        String iccId = tataInfo.get("icc_id");
        String subId = tataInfo.get("sub_id");
        if(iccId==null || subId==null){
            logger.info("null tata info for esim_id: " + iccId);
            return;
        }
        //sql to update iccId where esim_id contains iccId
        String sql = "update meter set " +
                " data_subscription_id = '" + subId +
                "' where esim_id like '%" + iccId + "%'" ;
        try {
            oqgHelper.OqgIU(sql);
        } catch (Exception e) {
            logger.info("Error updating meter tata info for esim_id: " + iccId);
            throw new RuntimeException(e);
        }
    }
    public Map<String, Object> getMeterEffectiveBypassPolicy(String meterSnStr, String bypassPolicyTableName){
        String tableName = bypassPolicyTableName;
        if(bypassPolicyTableName == null || bypassPolicyTableName.isBlank()){
            tableName = "bypass_policy";
        }
        String sql = "select * from "+tableName+" where meter_sn = '" + meterSnStr + "'";
        List<Map<String, Object>> bypassPolicy = new ArrayList<>();
        try {
            bypassPolicy = oqgHelper.OqgR2(sql, true);
        } catch (Exception e) {
            logger.info("Error getting bypass policy for meterSn: " + meterSnStr);
            throw new RuntimeException(e);
        }
        if(bypassPolicy.isEmpty()){
            logger.info("bypass policy is empty for meterSn: " + meterSnStr);
            return Collections.singletonMap("info", "bypass policy is empty for meterSn: " + meterSnStr);
        }
        //        return bypassPolicy.get(0);
        Map<String, Object> effectiveBypassPolicy = bypassPolicy.getFirst();
        boolean bypassAlways = effectiveBypassPolicy.get("bypass_always") != null && (boolean) effectiveBypassPolicy.get("bypass_always");
        if(bypassAlways){
            return Map.of("bypass_always", true);
        }
        LocalDateTime localNow = DateTimeUtil.getSgNow();
        //consider effective bypass policy only if end timestamp is not too long ago
        LocalDateTime refTime = localNow.minusDays(21);
        //if bypass end is before now, remove bypass start and end
        String bypass1EndTimestampStr = (String) effectiveBypassPolicy.get("bypass1_end_timestamp");
        if(bypass1EndTimestampStr!=null && !bypass1EndTimestampStr.isEmpty()){
            LocalDateTime bypass1EndTimestamp = DateTimeUtil.getLocalDateTime(bypass1EndTimestampStr);
            if(bypass1EndTimestamp.isBefore(refTime)){
                effectiveBypassPolicy.remove("bypass1_start_timestamp");
                effectiveBypassPolicy.remove("bypass1_end_timestamp");
            }
        }
        String bypass2EndTimestampStr = (String) effectiveBypassPolicy.get("bypass2_end_timestamp");
        if(bypass2EndTimestampStr!=null && !bypass2EndTimestampStr.isEmpty()){
            LocalDateTime bypass2EndTimestamp = DateTimeUtil.getLocalDateTime(bypass2EndTimestampStr);
            if(bypass2EndTimestamp.isBefore(refTime)){
                effectiveBypassPolicy.remove("bypass2_start_timestamp");
                effectiveBypassPolicy.remove("bypass2_end_timestamp");
            }
        }
        String bypass3EndTimestampStr = (String) effectiveBypassPolicy.get("bypass3_end_timestamp");
        if(bypass3EndTimestampStr!=null && !bypass3EndTimestampStr.isEmpty()){
            LocalDateTime bypass3EndTimestamp = DateTimeUtil.getLocalDateTime(bypass3EndTimestampStr);
            if(bypass3EndTimestamp.isBefore(refTime)){
                effectiveBypassPolicy.remove("bypass3_start_timestamp");
                effectiveBypassPolicy.remove("bypass3_end_timestamp");
            }
        }
        MeterBypassDto meterBypassDto = MeterBypassDto.fromFieldMap(effectiveBypassPolicy);
        return Map.of("effective_bypass_policy", meterBypassDto);
    }
    public Map<String, Object> getBypassTariff(String meterSnStr, String fromTimestamp, String toTimestamp){
        String sql = "select tariff_timestamp, debit_ref from meter_tariff where meter_sn = '" + meterSnStr + "'" +
                " and debit_ref like '%bypass%' " +
                " and tariff_timestamp >= '" + fromTimestamp + "'" +
                " and tariff_timestamp <= '" + toTimestamp + "'" +
                " order by tariff_timestamp desc ";
        List<Map<String, Object>> bypasses = new ArrayList<>();
        try {
            bypasses = oqgHelper.OqgR2(sql, true);
        } catch (Exception e) {
            logger.info("Error getting bypass for meterSn: " + meterSnStr);
            return Collections.singletonMap("error", "Error getting bypass for meterSn: " + meterSnStr);
        }
        if(bypasses.isEmpty()){
            logger.info("bypass is empty for meterSn: " + meterSnStr);
            return Collections.singletonMap("info", "bypass is empty for meterSn: " + meterSnStr);
        }
        return Map.of("bypasses", bypasses);
    }
    public Map<String, Object> getDailyBypassCount(String meterSnStr, List<Map<String, String>> dailyTimeSlot){
        if(dailyTimeSlot.isEmpty()){
            return Collections.singletonMap("error", "dailyTimeSlot is empty");
        }
        StringBuilder sql = new StringBuilder();
        for (Map<String, String> timeSlot : dailyTimeSlot) {
            String fromTimestamp = timeSlot.get("from_timestamp");
            String toTimestamp = timeSlot.get("to_timestamp");
            String slotSql = "select count(*) as bypass_count from meter_tariff where meter_sn = '" + meterSnStr + "'" +
                    " and debit_ref like '%bypass%' " +
                    " and tariff_timestamp >= '" + fromTimestamp + "'" +
                    " and tariff_timestamp <= '" + toTimestamp + "'";
            sql.append(slotSql).append(" union all ");
        }
        sql.delete(sql.length()-11, sql.length());
        List<Map<String, Object>> resp = new ArrayList<>();
        try {
            resp = oqgHelper.OqgR2(sql.toString(), true);
        } catch (Exception e) {
            logger.info("Error getting bypass for meterSn: " + meterSnStr);
            return Collections.singletonMap("error", "Error getting bypass for meterSn: " + meterSnStr);
        }
        if(resp.isEmpty()){
            logger.info("bypass is empty for meterSn: " + meterSnStr);
            return Collections.singletonMap("info", "bypass is empty for meterSn: " + meterSnStr);
        }
        List<Map<String, String>> bypasses = new ArrayList<>();
        int i = 0;
        for(Map<String, String> timeslot : dailyTimeSlot){
            String fromTimestamp = timeslot.get("from_timestamp");
            String toTimestamp = timeslot.get("to_timestamp");
            String count = resp.get(i++).get("bypass_count").toString();

            bypasses.add(Map.of("from_timestamp", fromTimestamp,
                                "to_timestamp", toTimestamp,
                                "bypass_count", count));
        }
        return Map.of("bypasses", bypasses);
    }
    public Map<String, Object> getDailyBypassCount2(String meterSnStr, String meterDisplaynameStr, List<Map<String, String>> dailyTimeSlot){
        if(dailyTimeSlot.isEmpty()){
            return Collections.singletonMap("error", "dailyTimeSlot is empty");
        }
        List<Map<String, String>> bypasses = new ArrayList<>();
        for (Map<String, String> timeSlot : dailyTimeSlot) {
            String fromTimestamp = timeSlot.get("from_timestamp");
            String toTimestamp = timeSlot.get("to_timestamp");
            //get bypass from meter_reading_daily
            String sql1 = "select bypass_count from meter_reading_daily where " +
                    " bypass_count is not null " +
                    " and meter_displayname = '" + meterDisplaynameStr + "'" +
                    " and kwh_timestamp = '" + toTimestamp + "'";
            List<Map<String, Object>> resp1 = new ArrayList<>();
            try {
                resp1 = oqgHelper.OqgR2(sql1, true);
            } catch (Exception e) {
                logger.info("Error getting bypass for meterSn: " + meterSnStr + "in meter_reading_daily");
                continue;
            }
            if(resp1.isEmpty()){
                logger.info("bypass is empty for meterSn: " + meterSnStr + "in meter_reading_daily");
            }else {
                String count1 = (String) resp1.get(0).get("bypass_count");
                bypasses.add(Map.of("from_timestamp", fromTimestamp,
                                    "to_timestamp", toTimestamp,
                                    "bypass_count", count1));
                continue;
            }
            //get bypass from meter_tariff
            String slotSql = "select count(*) as bypass_count from meter_tariff where meter_sn = '" + meterSnStr + "'" +
                    " and debit_ref like '%bypass%' " +
                    " and tariff_timestamp >= '" + fromTimestamp + "'" +
                    " and tariff_timestamp <= '" + toTimestamp + "'";
            List<Map<String, Object>> resp2 = new ArrayList<>();
            try {
                resp2 = oqgHelper.OqgR2(slotSql, true);
            } catch (Exception e) {
                logger.info("Error getting bypass for meterSn: " + meterSnStr + "in meter_tariff");
                continue;
            }
            if(resp2.isEmpty()){
                logger.info("bypass is empty for meterSn: " + meterSnStr + "in meter_tariff");
                continue;
            }
            String count2 = (String)resp2.getFirst().get("bypass_count");
            bypasses.add(Map.of("from_timestamp", fromTimestamp,
                                "to_timestamp", toTimestamp,
                                "bypass_count", count2));
            //update meter_reading_daily
            String sqlUpdate = "update meter_reading_daily set bypass_count = " + count2 +
                    " where meter_displayname = '" + meterDisplaynameStr + "'" +
                    " and kwh_timestamp = '" + toTimestamp + "'";
            try {
                oqgHelper.OqgIU(sqlUpdate);
            } catch (Exception e) {
                logger.info("Error updating bypass for meterSn: " + meterSnStr + "in meter_reading_daily");
                continue;
            }
        }
        return Map.of("bypasses", bypasses);
    }

    public void postOpLog2(String postDateTimeStr,
                          long userId,
                          String target,
                          String operation,
                          String targetSpec,
                          String opRef,
                          double opVal,
                          String remark,
                          String refId,
                          String sessionId){
        String opsLogTable = "evs2_op_log";

        if(postDateTimeStr == null || postDateTimeStr.isEmpty()) {
//            postDateTimeStr = DateTimeUtil.getZonedDateTimeStr(now(), ZoneId.of("Asia/Singapore"));;
            throw new RuntimeException("postDateTimeStr is null or empty");
        }

        if(sessionId == null || sessionId.isEmpty()) {
            sessionId = UUID.randomUUID().toString();
        }

        Map<String, Object> oplogSqlMap = Map.of("table", opsLogTable,
                "content", Map.of(
                        "op_timestamp", postDateTimeStr,
                        "user_id", userId,
                        "evs2_acl_target", target,
                        "target_spec", targetSpec,
                        "evs2_acl_operation", operation,
                        "op_ref", opRef,
                        "op_val", opVal,
                        "remark", remark,
                        "ref_id", refId,
                        "session_id", sessionId
                ));
        Map<String, String> sqlInsert = SqlUtil.makeInsertSql(oplogSqlMap);
        if(sqlInsert.get("sql")==null){
            logger.info("Error getting insert sql for opsLogTable");
            throw new RuntimeException("Error getting insert sql for opsLogTable");
        }
        try {
            oqgHelper.OqgIU(sqlInsert.get("sql"));
        } catch (Exception e) {
            logger.info("Error inserting opsLogTable");
            throw new RuntimeException(e);
        }
    }

    public Map<String, Object> getMeter3pAllFields(String meterSnStr){
        String sql = "select * from meter_3p where meter_sn = '" + meterSnStr + "'";
        List<Map<String, Object>> meter3pAllFields = new ArrayList<>();
        try {
            meter3pAllFields = oqgHelper.OqgR2(sql, true);
        } catch (Exception e) {
            logger.info("Error getting meter 3p info for meterSn: " + meterSnStr);
            throw new RuntimeException(e);
        }
        if(meter3pAllFields.isEmpty()){
            logger.info("meter 3p info is empty for meterSn: " + meterSnStr);
            return Collections.singletonMap("info", "meter 3p info is empty for meterSn: " + meterSnStr);
        }
        return meter3pAllFields.get(0);
    }

    public Map<String, Object> getAllActiveKwh3p(String localNow,
                                                 Map<String, String> scope,
                                                 String itemTableName, String itemIdColName, String timeKey){
        Map<String, Object> scopeConstraint = getScopeItemConstraint(scope, itemTableName, itemIdColName);
        if (scopeConstraint.containsKey("error")){
            return scopeConstraint;
        }
        String itemIdInStr = scopeConstraint.get("scope_constraint").toString();
        List<Map<String, Object>> itemIds = (List<Map<String, Object>>) scopeConstraint.get("item_ids");

        double totalKwh3p = 0;
        for(Map<String, Object> item : itemIds){
            String itemId = (String) item.get(itemIdColName);

            //last 24 hours
            String sql = "select "+itemIdColName+" from "+itemTableName+" where "+itemIdColName+" = '" + itemId + "'" +
                    " and "+timeKey+" >= '" + localNow + "' - interval '24 hours' ";
            List<Map<String, Object>> kwh3p = new ArrayList<>();
            try {
                kwh3p = oqgHelper.OqgR2(sql, true);
            } catch (Exception e) {
                logger.info("Error getting kwh3p for itemId: " + itemId);
                throw new RuntimeException(e);
            }
            //get the diff of the first and last record
            if(kwh3p.size()<2){
                continue;
            }
            String firstKwhStr = (String) kwh3p.get(0).get(itemIdColName);
            String lastKwhStr = (String) kwh3p.get(kwh3p.size()-1).get(itemIdColName);
            double firstKwh = MathUtil.ObjToDouble(firstKwhStr);
            double lastKwh = MathUtil.ObjToDouble(lastKwhStr);
            double diffKwh = lastKwh - firstKwh;
            totalKwh3p += diffKwh;
        }

        return Map.of("total_kwh_3p", totalKwh3p);
    }

    public Map<String, Object> getOpsLogItem(String key, String val){
        String tableName = "evs2_op_log";
        String userTableName = "evs2_user";
        String sql = "select * from "+tableName+" where "+key+" = '" + val + "'";
        List<Map<String, Object>> historyOps = new ArrayList<>();
        try {
            historyOps = oqgHelper.OqgR2(sql, true);
        } catch (Exception e) {
            logger.info("Error getting history ops for tableName: " + tableName);
            throw new RuntimeException(e);
        }
        if(historyOps.isEmpty()){
            logger.info("history ops is empty for tableName: " + tableName);
            return Collections.singletonMap("info", "history ops is empty for tableName: " + tableName);
        }
        //get username from user table
        String userIdStr = (String) historyOps.getFirst().get("user_id");
        String sql2 = "select username from "+userTableName+" where id = " + userIdStr;
        List<Map<String, Object>> users = new ArrayList<>();
        try {
            users = oqgHelper.OqgR2(sql2, true);
        } catch (Exception e) {
            logger.info("Error getting user for userId: " + userIdStr);
            throw new RuntimeException(e);
        }
        if(users.isEmpty()){
            logger.info("user is empty for userId: " + userIdStr);
            return Collections.singletonMap("info", "user is empty for userId: " + userIdStr);
        }
        String username = (String) users.getFirst().get("username");
        historyOps.getFirst().put("username", username);
        return historyOps.getFirst();
    }

    public Map<String, Object> getItemInfo(String itemId, ItemIdTypeEnum itemIdType, ItemTypeEnum itemType){
        Map<String, String> itemTableInfo = getItemTableInfo(itemType, itemIdType);
        if(itemTableInfo == null){
            return Collections.singletonMap("error", "itemType not supported");
        }
        String tableName = itemTableInfo.get("item_table_name");
        String itemIdColName = itemTableInfo.get("item_id_col_name");
        String propSelectStr = itemTableInfo.get("prop_select");

        String sql = "select "+propSelectStr+" from "+tableName+" where "+itemIdColName+" = '" + itemId + "'";

        List<Map<String, Object>> itemInfo = new ArrayList<>();
        try {
            itemInfo = oqgHelper.OqgR2(sql, true);
        } catch (Exception e) {
            logger.info("Error getting item info for itemId: " + itemId);
//            throw new RuntimeException(e);
            return Collections.singletonMap("error", "Error getting item info for itemId: " + itemId);
        }
        if(itemInfo.isEmpty()){
            logger.info("item info not found for itemId: " + itemId);
            return Collections.singletonMap("info", "item info not found for itemId: " + itemId);
        }
        return Collections.singletonMap("item_info", itemInfo.getFirst());
    }
    public Map<String, Object> getItemLastReading(String itemId, ItemIdTypeEnum itemIdType, ItemTypeEnum itemType){
        Map<String, String> itemReadingTableInfo = getItemTableInfo(itemType, itemIdType);
        if(itemReadingTableInfo == null){
            return Collections.singletonMap("error", "itemType not supported");
        }
        String tableName = itemReadingTableInfo.get("item_reading_table_name");
        String timeKey = itemReadingTableInfo.get("time_key");
        String valKey = itemReadingTableInfo.get("val_key");
        String itemIdColName = itemReadingTableInfo.get("item_id_col_name");

        String sql = "select "+valKey+",  "+timeKey+" from "+tableName+" where "+itemIdColName+" = '" + itemId + "'" +
                " order by "+timeKey+" desc limit 1";
        List<Map<String, Object>> lastReading = new ArrayList<>();
        try {
            lastReading = oqgHelper.OqgR2(sql, true);
        } catch (Exception e) {
            logger.info("Error getting last reading for itemId: " + itemId);
//            throw new RuntimeException(e);
            return Collections.singletonMap("error", "Error getting last reading for itemId: " + itemId);
        }
        if(lastReading.isEmpty()){
            logger.info("last reading is empty for itemId: " + itemId);
            return Collections.singletonMap("info", "last reading is empty for itemId: " + itemId);
        }
        return Collections.singletonMap("last_reading", lastReading.getFirst());
    }

    private Map<String, String> getItemTableInfo(ItemTypeEnum itemType, ItemIdTypeEnum itemIdType){
        String itemTableName = "";
        String itemReadingTableName = "";
        String itemIdColName = "";
        String valKey = "";
        String timeKey = "";
        String propSelect = "id,";

        switch (itemType) {
            case METER:
                itemTableName = "meter";
                itemReadingTableName = "meter_reading";
                timeKey = "kwh_timestamp";
                valKey = "kwh_total";
                itemIdColName = "meter_sn";
                if(itemIdType == ItemIdTypeEnum.NAME){
                    itemIdColName = "meter_displayname";
                }
                propSelect += "meter_sn, meter_displayname, site_tag, scope_str, mms_address, mms_unit, mms_level, mms_block, mms_building, esim_id, mms_online_timestamp";
                break;
            case METER_3P:
                itemTableName = "meter_3p";
                itemReadingTableName = "meter_reading_3p";
                timeKey = "dt";
                valKey = "a_import";
                itemIdColName = "meter_sn";
                if(itemIdType == ItemIdTypeEnum.NAME){
                    itemIdColName = "meter_id";
                }
                propSelect += "meter_sn, meter_id, site_tag, scope_str";
                break;
            case SENSOR:
                itemTableName = "sensor";
                itemReadingTableName = "sensor_reading_multi";
                timeKey = "dt";
                valKey = "temperature, humidity";
                itemIdColName = "item_id";
                if(itemIdType == ItemIdTypeEnum.NAME){
                    itemIdColName = "item_name";
                }
                propSelect += "item_id, item_name, site_tag, scope_str";
                break;
            case METER_IWOW:
                itemTableName = "meter_iwow";
                itemReadingTableName = "meter_reading_iwow";
                timeKey = "dt";
                valKey = "val";
                itemIdColName = "item_name";
                if(itemIdType == ItemIdTypeEnum.NAME){
                    itemIdColName = "item_name";
                }
                propSelect += "item_sn, item_name, meter_type, site_tag, scope_str, alt_name, location_tag, loc_building, loc_level, lc_status, commissioned_timestamp";
                break;
            case METER_GROUP:
                itemTableName = "meter_group";
                itemReadingTableName = "";
                timeKey = "";
                valKey = "";
                itemIdColName = "name";
                if(itemIdType == ItemIdTypeEnum.NAME){
                    itemIdColName = "name";
                }
                propSelect += "name, label, meter_type, scope_str, meter_info_str, created_timestamp, updated_timestamp";
                break;
            case TENANT:
                itemTableName = "tenant";
                itemReadingTableName = "";
                timeKey = "";
                valKey = "";
                itemIdColName = "tenant_name";
                if(itemIdType == ItemIdTypeEnum.NAME){
                    itemIdColName = "tenant_name";
                }
                propSelect += "tenant_name, tenant_label, type, scope_str, sap_wbs, location_tag, created_timestamp, updated_timestamp, " +
                        "tariff_package_id_e, tariff_package_id_w, tariff_package_id_b, tariff_package_id_n";
                break;
            case TARIFF_PACKAGE:
                itemTableName = "tariff_package";
                itemReadingTableName = "";
                timeKey = "";
                valKey = "";
                itemIdColName = "name";
                if(itemIdType == ItemIdTypeEnum.NAME){
                    itemIdColName = "name";
                }
                propSelect += "name, label, scope_str, meter_type, package_type, updated_timestamp";
                break;
            case TARIFF_PACKAGE_RATE:
                itemTableName = "tariff_package_rate";
                itemReadingTableName = "";
                timeKey = "created_timestamp";
                valKey = "";
                itemIdColName = "id";
                propSelect += "tariff_package_id, from_timestamp, to_timestamp, rate, gst, created_timestamp, updated_timestamp";
                break;
            case JOB_TYPE:
                itemTableName = "job_type";
                itemReadingTableName = "";
                timeKey = "";
                valKey = "";
                itemIdColName = "id";
                if(itemIdType == ItemIdTypeEnum.NAME){
                    itemIdColName = "name";
                }
                propSelect += "name, label, scope_str, site_tag, cat, updated_timestamp";
                break;
            case JOB_TYPE_SUB:
                itemTableName = "job_sub";
                itemReadingTableName = "";
                timeKey = "updated_timestamp";
                valKey = "";
                itemIdColName = "id";
                propSelect += "job_type_id, sub_fullname, sub_email, sub_salutation, user_id, is_active, rank";
                break;
            case BILLING_REC:
                itemTableName = "billing_rec_cw";
                itemReadingTableName = "";
                timeKey = "";
                valKey = "";
                itemIdColName = "id";
                if(itemIdType == ItemIdTypeEnum.NAME){
                    itemIdColName = "name";
                }
                propSelect += "name, scope_str, site_tag, tenant_id, tariff_package_id, recon_user_id, "
                + "from_timestamp, to_timestamp, is_monthly, "
                + "tariff_package_rate_id_e, tariff_package_rate_id_w, tariff_package_rate_id_b, tariff_package_rate_id_n, "
                + "manual_usage_e, manual_usage_w, manual_usage_b, manual_usage_n, "
                + "line_item_label_1, line_item_amount_1, "
                + "lc_status, created_timestamp, updated_timestamp";
                break;
            default:
                return null;
        }
        if(itemIdType == ItemIdTypeEnum.ID){
            itemIdColName = "id";
        }
        return Map.of("item_table_name", itemTableName,
                      "item_reading_table_name", itemReadingTableName,
                      "prop_select", propSelect,
                      "time_key", timeKey,
                      "val_key", valKey,
                      "item_id_col_name", itemIdColName);
    }

    public Map<String, Object> getProjectScopeFromSiteScope(String siteScopeStr){
        String sql = "select project_scope from scope_setting where site_scope = '" + siteScopeStr + "'";

        List<Map<String, Object>> resp;

        try {
            resp = oqgHelper.OqgR2(sql, true);
        } catch (Exception e) {
            logger.info("Error getting project scope for site scope: " + siteScopeStr);
            throw new RuntimeException(e);
        }
        if(resp.isEmpty()){
            logger.info("project scope is empty for site scope: " + siteScopeStr);
            return Collections.singletonMap("info", "project scope is empty for site scope: " + siteScopeStr);
        }
        return resp.getFirst();
    }

    public Map<String, Object> setUserScope(String username, String projectScope){
        String sql = "update evs2_user set scope_str = '" + projectScope + "' where username = '" + username + "'";

        try {
            oqgHelper.OqgIU(sql);
        } catch (Exception e) {
            logger.info("Error setting project scope for username: " + username);
//            throw new RuntimeException(e);
            return Collections.singletonMap("error", "Error setting project scope for username: " + username);
        }
        return Collections.singletonMap("success", "project scope is set for username: " + username);
    }
    public Map<String, Object> getVersion(String serviceName, String projectScope){
        String sql = "select version from service_version where service_name = '" + serviceName + "' and scope_str = '" + projectScope + "'";

        List<Map<String, Object>> resp;
        try {
            resp = oqgHelper.OqgR2(sql, true);
        } catch (Exception e) {
            logger.info("Error getting version for service name: " + serviceName + " and project scope: " + projectScope);
            return Collections.singletonMap("error", "Error getting version for service name: " + serviceName + " and project scope: " + projectScope);
        }
        return resp.getFirst();
    }
    public Map<String, Object> getReportSub(int reportId){
        String sql = "select user_id from report_sub where report_id = " + reportId + " and active = true";

        List<Map<String, Object>> resp;
        try {
            resp = oqgHelper.OqgR2(sql, true);
        } catch (Exception e) {
            logger.info("Error getting report sub");
            return Collections.singletonMap("error", "Error getting report sub");
        }

        List<Map<String, Object>> reportSubs = new ArrayList<>();
        for(Map<String, Object> sub : resp){
            String userIdStr = (String) sub.get("user_id");
            String sql2 = "select username, fullname, email from evs2_user " +
                    "where id = " + userIdStr;
            List<Map<String, Object>> users;
            try {
                users = oqgHelper.OqgR2(sql2, true);
            } catch (Exception e) {
                logger.info("Error getting user for userId: " + userIdStr);
                throw new RuntimeException(e);
            }
            if(users.isEmpty()){
                logger.info("user is empty for userId: " + userIdStr);
                continue;
            }
            String username = (String) users.getFirst().get("username");
            String fullname = (String) users.getFirst().get("fullname");
            String email = (String) users.getFirst().get("email");
            reportSubs.add(Map.of(
                "username", username,
                "fullname", fullname,
                "email", email));
        }
        return Map.of("report_subs", reportSubs);
    }

    public Map<String, Object> getItemSnFromItemName(String itemName, ItemTypeEnum itemTypeEnum){
        String itemTableName = "meter";
        String itemNameColName = "meter_displayname";
        String itemSnColName = "meter_sn";
        switch (itemTypeEnum) {
            case METER:
                itemTableName = "meter";
                itemNameColName = "meter_displayname";
                itemSnColName = "meter_sn";
                break;
            case METER_3P:
                itemTableName = "meter_3p";
                itemNameColName = "meter_id";
                itemSnColName = "meter_sn";
                break;
            case SENSOR:
                itemTableName = "sensor";
                itemNameColName = "item_name";
                itemSnColName = "item_id";
                break;
            case METER_IWOW:
                itemTableName = "meter_iwow";
                itemNameColName = "item_name";
                itemSnColName = "item_sn";
                break;
            case METER_GROUP:
                itemTableName = "meter_group";
                itemNameColName = "name";
                itemSnColName = "name";
                break;
            case TENANT:
                itemTableName = "tenant";
                itemNameColName = "tenant_name";
                itemSnColName = "tenant_name";
                break;
            default:
                return Collections.singletonMap("error", "itemType not supported");
        }

        String sql = "select "+itemSnColName+" from "+itemTableName+" where "+itemNameColName+" = '" + itemName + "'";
        List<Map<String, Object>> resp;
        try {
            resp = oqgHelper.OqgR2(sql, true);
        } catch (Exception e) {
            logger.info("Error getting item sn for itemName: " + itemName);
            return Collections.singletonMap("error", "Error getting item sn for itemName: " + itemName);
        }
        if(resp.isEmpty()){
            logger.info("item sn is empty for itemName: " + itemName);
            return Collections.singletonMap("info", "item sn is empty for itemName: " + itemName);
        }
        return resp.getFirst();
    }

    public Map<String, Object> getJobSubs(Long jobTypeId, Integer maxRank){
//        logger.info("getJobSub() called");
        String sql = "select * from job_sub where job_type_id = " + jobTypeId
                + " AND (is_active != false OR is_active IS NULL)";
        if(maxRank != null){
            sql += " AND (rank <= " + maxRank + " OR rank IS NULL)";
        }
        sql = sql + " ORDER BY updated_timestamp DESC";
        List<Map<String, Object>> resp;
        try {
            resp = oqgHelper.OqgR2(sql,true);
        } catch (Exception e) {
            logger.severe("Failed to query job_sub table: " + e.getMessage());
            return Map.of("error", e.getMessage());
        }
        if(resp.isEmpty()){
            logger.info("Job sub not found for job type: " + jobTypeId);
            return Map.of("info", "Job sub not found for job type: " + jobTypeId);
        }
        return Map.of("subs", resp);
    }

    public Map<String, Object> getGroupMeters(Map<String, Object> request, String targetGroupTargetTableName){
        String scopeStr = request.get("scope_str") == null ? "" : (String) request.get("scope_str");
        String itemIdTypeStr = request.get("item_id_type") == null ? "" : (String) request.get("item_id_type");
        String meterGroupName = request.get("item_name") == null ? "" : (String) request.get("item_name");
        String meterGroupLabel = request.get("label") == null ? "" : (String) request.get("label");
        String meterType = request.get("meter_type") == null ? "" : (String) request.get("meter_type");
        String meterGroupIndexStr = (String) request.get("item_index");
        Long meterGroupIndex = MathUtil.ObjToLong(meterGroupIndexStr);

        String meterTypeStr = (String) request.get("item_type");
        ItemTypeEnum meterTypeEnum = null;
        if(meterTypeStr != null) {
            meterTypeEnum = ItemTypeEnum.valueOf(meterTypeStr.toUpperCase());
        }
        if(meterTypeEnum == null){
            return Map.of("error", "invalid meter type");
        }

        String sql = "select meter_id, percentage from "+targetGroupTargetTableName+" where meter_group_id="+meterGroupIndex;
        List<Map<String, Object>> resp;
        try {
            resp = oqgHelper.OqgR2(sql, false);
        } catch (Exception e) {
            logger.severe(e.getMessage());
            return Map.of("error", e.getMessage());
        }
        List<Map<String, Object>> groupMeterList = new ArrayList<>();
        try{
            for(Map<String, Object> respItem: resp){
                String meterId = (String) respItem.get("meter_id");
                Double percentage = MathUtil.ObjToDouble(respItem.get("percentage"));

                //get full meter info
                Map<String, Object> meterInfoResult = getItemInfo(meterId, ItemIdTypeEnum.ID, meterTypeEnum);

                Map<String, Object> meterInfo = (Map<String, Object>) meterInfoResult.get("item_info");
                meterInfo.put("meter_id", meterId);
                meterInfo.put("percentage", percentage);

//                groupMeterList.add(Map.of("meter_id", meterId,"percentage", percentage));
                groupMeterList.add(meterInfo);
            }
            if(request.get("item_name") == null){
                Map<String, Object> groupInfoResult = getItemInfo(meterGroupIndex.toString(), ItemIdTypeEnum.ID, ItemTypeEnum.METER_GROUP);
                Map<String, Object> itemInfo = (Map<String, Object>) groupInfoResult.get("item_info");
                if(itemInfo==null){
                    return Map.of("error", "meter group not found");
                }
                meterGroupName = (String) itemInfo.get("name");
                meterGroupLabel = (String) itemInfo.get("label");
                meterType = (String) itemInfo.get("meter_type");
            }
            return Map.of(
                    "group_name", meterGroupName,
                    "group_label", meterGroupLabel,
                    "meter_type", meterType,
                    "group_meter_list", groupMeterList);
        }catch (Exception e){
            logger.severe(e.getMessage());
            return Map.of("error", e.getMessage());
        }
    }
    public Map<String, Object> getTenantMeterGroups(Map<String, Object> request){
        String itemType = (String) request.get("meter_type");
        String targetGroupTargetTableName = (String) request.get("target_group_target_table_name");
        String tenantTargetGroupTableName = (String) request.get("tenant_target_group_table_name");

        String scopeStr = request.get("scope_str") == null ? "" : (String) request.get("scope_str");
        String itemIdTypeStr = request.get("item_id_type") == null ? "" : (String) request.get("item_id_type");
        String tenantName = (String) request.get("item_name");
        String tenantIndexStr = (String) request.get("item_index");
        long tenantIndex = MathUtil.ObjToLong(tenantIndexStr);

        boolean isGetFullInfo = request.get("get_full_info") != null && Boolean.parseBoolean((String) request.get("get_full_info"));

        String sql = "select meter_group_id from "+tenantTargetGroupTableName+" where tenant_id="+tenantIndex;
        List<Map<String, Object>> resp;
        try {
            resp = oqgHelper.OqgR2(sql, false);
        } catch (Exception e) {
            logger.severe(e.getMessage());
            return Map.of("error", e.getMessage());
        }
        List<Map<String, Object>> tenantMeterGroupList = new ArrayList<>();

        Map<String, Object> result = new HashMap<>();
        List<Map<String, Object>> tenantMeterGroupListFullInfo = new ArrayList<>();
        if(isGetFullInfo){
            result.put("group_full_info", tenantMeterGroupListFullInfo);
        }
        try{
            for(Map<String, Object> respItem: resp){
                String meterGroupId = (String) respItem.get("meter_group_id");

                //get group info
                String sql2 = "SELECT name, label from meter_group where id="+meterGroupId;
                List<Map<String, Object>> resp2;
                try {
                    resp2 = oqgHelper.OqgR2(sql2, false);
                } catch (Exception e) {
                    logger.severe(e.getMessage());
                    return Map.of("error", e.getMessage());
                }
                if(resp2.size() > 1){
                    return Map.of("error", "more than one meter group found for meter group "+meterGroupId);
                }
                if(resp2.isEmpty()){
                    logger.info("no meter group found for meter group "+meterGroupId);
                    continue;
                }
                Map<String, Object> group = resp2.getFirst();
                tenantMeterGroupList.add(Map.of(
                        "meter_group_id", meterGroupId,
                        "name", group.get("name"),
                        "label", group.get("label")));
//                tenantMeterGroupList.add(Map.of("meter_group_id", meterGroupId));

                if(isGetFullInfo){
                    Map<String, Object> meterGroupInfo = getGroupMeters(
                        Map.of(
                        "scope_str", scopeStr,
                        "item_id_type", itemIdTypeStr,
                        "item_index", meterGroupId,
                        "item_type", itemType/*request.get("item_type")*/),
                        targetGroupTargetTableName
                    );
                    tenantMeterGroupListFullInfo.add(meterGroupInfo);
                }
            }
            result.put("tenant_meter_group_list", tenantMeterGroupList);
            return result;
        }catch (Exception e){
            logger.severe(e.getMessage());
            return Map.of("error", e.getMessage());
        }
    }
    public Map<String, Object> getTenantTariffPackages(Long tenantIndex) {
        String sql = "select * from tenant where id=" + tenantIndex;
        List<Map<String, Object>> resp;
        try {
            resp = oqgHelper.OqgR2(sql, false);
        } catch (Exception e) {
            logger.severe(e.getMessage());
            return Map.of("error", e.getMessage());
        }
        if(resp.size() > 1){
            return Map.of("error", "more than one tenant found for tenant "+tenantIndex);
        }
        if(resp.isEmpty()){
            logger.info("no tenant found for tenant "+tenantIndex);
            return Map.of("info", "no tenant found for tenant "+tenantIndex);
        }
        Map<String, Object> tenant = resp.getFirst();
        String tariffPackageIdE = (String) tenant.get("tariff_package_id_e");
        String tariffPackageIdW = (String) tenant.get("tariff_package_id_w");
        String tariffPackageIdB = (String) tenant.get("tariff_package_id_b");
        String tariffPackageIdN = (String) tenant.get("tariff_package_id_n");

        Map<String, Object> result = new HashMap<>();
        result.put("tariff_package_id_e", tariffPackageIdE);
        result.put("tariff_package_id_w", tariffPackageIdW);
        result.put("tariff_package_id_b", tariffPackageIdB);
        result.put("tariff_package_id_n", tariffPackageIdN);

        return result;
    }
    public Map<String, Object> getTariffPackageRates(Long tariffPackageId, int limit) {
        String sql = "select * from tariff_package_rate where tariff_package_id=" + tariffPackageId + " order by from_timestamp desc limit " + limit;
        List<Map<String, Object>> resp;
        try {
            resp = oqgHelper.OqgR2(sql, false);
        } catch (Exception e) {
            logger.severe(e.getMessage());
            return Map.of("error", e.getMessage());
        }
        if(resp.isEmpty()){
            logger.info("no tariff package found for tariff package "+tariffPackageId);
            return Map.of("info", "no tariff package found for tariff package "+tariffPackageId);
        }
        return Map.of("tariff_package_rates", resp);
    }

    public Map<String, Object> findTariff(String meterTypeTag, Map<String, Object> tenantTariffIds, String firstReadingTimeStr, String lastReadingTimeStr) {
        logger.info("Finding tariff");

        //find the id that ends with the meterTypeTag
        String tariffId = null;
        for (Map.Entry<String, Object> entry : tenantTariffIds.entrySet()) {
            if (entry.getKey().endsWith("_"+meterTypeTag.toLowerCase())) {
                tariffId = entry.getKey();
                break;
            }
        }
        if(tariffId == null){
            logger.severe("Tariff not found for meterTypeTag: " + meterTypeTag);
            return Collections.singletonMap("error", "Tariff not found for meterTypeTag: " + meterTypeTag);
        }

        Long tariffPackageIndex = MathUtil.ObjToLong(tenantTariffIds.get(tariffId));
        Map<String, Object> tariffRateResult = getTariffPackageRates(tariffPackageIndex, 12);
        List<Map<String, Object>> tariffRates = (List<Map<String, Object>>) tariffRateResult.get("tariff_package_rates");
        if(tariffRates == null || tariffRates.isEmpty()){
            logger.severe("No tariff rates found for tariffPackageIndex: " + tariffPackageIndex);
            return Collections.singletonMap("error", "No tariff rates found for tariffPackageIndex: " + tariffPackageIndex);
        }

        for(Map<String, Object> tariffRate : tariffRates){
            //find the tariff that is valid for the firstReadingTime
            String fromTimestampStr = (String) tariffRate.get("from_timestamp");
            LocalDateTime fromTimestamp = DateTimeUtil.getLocalDateTime(fromTimestampStr);
            String toTimestampStr = (String) tariffRate.get("to_timestamp");
            LocalDateTime toTimestamp = DateTimeUtil.getLocalDateTime(toTimestampStr);

            LocalDateTime firstReadingTime = DateTimeUtil.getLocalDateTime(firstReadingTimeStr);
            LocalDateTime lastReadingTime = DateTimeUtil.getLocalDateTime(lastReadingTimeStr);
            LocalDateTime midTime = firstReadingTime.plusSeconds(Duration.between(firstReadingTime, lastReadingTime).getSeconds()/2);

            if(fromTimestamp.isBefore(midTime) && toTimestamp.isAfter(midTime)){
//                tariffRate.put("tariff_package_rate_id_col_name", "tariff_package_rate_id_"+meterTypeTag.toLowerCase());
                return Collections.singletonMap("result", tariffRate);
            }
        }
        logger.severe("No valid tariff found for meterTypeTag: " + meterTypeTag);
        return Collections.singletonMap("error", "No valid tariff found for meterTypeTag: " + meterTypeTag);
    }

    public Map<String, Object> logFeedback(Map<String, Object> feedbackInfo){
        String feedbackTableName = "feedback";
        String submit_timestamp = (String) feedbackInfo.get("submit_timestamp");
        String type = (String) feedbackInfo.get("type");
        String scopeStr = (String) feedbackInfo.get("scope_str");
        String serviceName = (String) feedbackInfo.get("service_name");
        String itemName = (String) feedbackInfo.get("item_name");
        String address = (String) feedbackInfo.get("address");
        String email = (String) feedbackInfo.get("email");
        String feedbackEmail = (String) feedbackInfo.get("feedback_email");
        String message = (String) feedbackInfo.get("message");

        Map<String, Object> feedbackSqlMap = Map.of("table", feedbackTableName,
                "content", Map.of(
                        "submit_timestamp", submit_timestamp,
                        "type", type,
                        "scope_str", scopeStr,
                        "service_name", serviceName,
                        "item_name", itemName,
                        "address", address,
                        "email", email,
                        "feedback_email", feedbackEmail,
                        "message", message
                ));
        Map<String, String> sqlInsert = SqlUtil.makeInsertSql(feedbackSqlMap);
        if(sqlInsert.get("sql")==null){
            logger.info("Error getting insert sql for feedback");
            return Collections.singletonMap("error", "Error getting insert sql for feedback");
        }
        try {
            oqgHelper.OqgIU(sqlInsert.get("sql"));
        } catch (Exception e) {
            logger.info("Error inserting feedback");
            return Collections.singletonMap("error", "Error inserting feedback");
        }
        return Collections.singletonMap("success", "feedback is inserted");
    }
}