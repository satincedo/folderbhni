-- MySQL dump 10.13  Distrib 8.0.40, for Linux (x86_64)
--
-- Host: qa-bhni-serverless-mysql.cluster-ro-clrwadraipam.us-west-2.rds.amazonaws.com    Database: analytics_spog
-- ------------------------------------------------------
-- Server version	8.0.32

/*!40101 SET @OLD_CHARACTER_SET_CLIENT=@@CHARACTER_SET_CLIENT */;
/*!40101 SET @OLD_CHARACTER_SET_RESULTS=@@CHARACTER_SET_RESULTS */;
/*!40101 SET @OLD_COLLATION_CONNECTION=@@COLLATION_CONNECTION */;
/*!50503 SET NAMES utf8mb4 */;
/*!40103 SET @OLD_TIME_ZONE=@@TIME_ZONE */;
/*!40103 SET TIME_ZONE='+00:00' */;
/*!40014 SET @OLD_UNIQUE_CHECKS=@@UNIQUE_CHECKS, UNIQUE_CHECKS=0 */;
/*!40014 SET @OLD_FOREIGN_KEY_CHECKS=@@FOREIGN_KEY_CHECKS, FOREIGN_KEY_CHECKS=0 */;
/*!40101 SET @OLD_SQL_MODE=@@SQL_MODE, SQL_MODE='NO_AUTO_VALUE_ON_ZERO' */;
/*!40111 SET @OLD_SQL_NOTES=@@SQL_NOTES, SQL_NOTES=0 */;
SET @MYSQLDUMP_TEMP_LOG_BIN = @@SESSION.SQL_LOG_BIN;
SET @@SESSION.SQL_LOG_BIN= 0;

--
-- GTID state at the beginning of the backup 
--

SET @@GLOBAL.GTID_PURGED=/*!80000 '+'*/ '';

--
-- Dumping routines for database 'analytics_spog'
--
/*!50003 DROP PROCEDURE IF EXISTS `GetActivePassiveSubnetFilterData` */;
/*!50003 SET @saved_cs_client      = @@character_set_client */ ;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;
/*!50003 SET character_set_client  = latin1 */ ;
/*!50003 SET character_set_results = latin1 */ ;
/*!50003 SET collation_connection  = latin1_swedish_ci */ ;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;
/*!50003 SET sql_mode              = '' */ ;
DELIMITER ;;
CREATE DEFINER=`analytics_dev`@`%` PROCEDURE `GetActivePassiveSubnetFilterData`(
    IN Device VARCHAR(255), 
    IN fromTime INT, 
    IN toTime INT
)
BEGIN
    SET @sqlActiveTelemetry = CONCAT('
        SELECT 
          DISTINCT ASSET_ID,PORT_NUMBER FROM analytics.ACTIVE_TELEMETRY PARTITION (', Device, ') AS AT WHERE MEASURE_DATE BETWEEN ', fromTime, ' AND ', toTime, ';');

        SET @sqlTelemetryHeader = CONCAT('
                SELECT DISTINCT ADDRESS, ETH_ADDRESS FROM (
                SELECT SRC_ADDRESS AS ADDRESS, SRC_ETH_ADDRESS AS ETH_ADDRESS FROM analytics.TELEMETRY_HEADER PARTITION (', Device, ')  WHERE MEASURE_START_DATE BETWEEN ', fromTime, ' AND ', toTime, '
                UNION
                SELECT DST_ADDRESS AS ADDRESS, DST_ETH_ADDRESS AS ETH_ADDRESS FROM analytics.TELEMETRY_HEADER PARTITION (', Device, ')  WHERE MEASURE_START_DATE BETWEEN ', fromTime, ' AND ', toTime, ') as ALL_ETH_ADDRESS;');

    PREPARE stmt FROM @sqlActiveTelemetry;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;

    PREPARE stmt FROM @sqlTelemetryHeader;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;

END ;;
DELIMITER ;
/*!50003 SET sql_mode              = @saved_sql_mode */ ;
/*!50003 SET character_set_client  = @saved_cs_client */ ;
/*!50003 SET character_set_results = @saved_cs_results */ ;
/*!50003 SET collation_connection  = @saved_col_connection */ ;
/*!50003 DROP PROCEDURE IF EXISTS `GetAddressCounts` */;
/*!50003 SET @saved_cs_client      = @@character_set_client */ ;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;
/*!50003 SET character_set_client  = latin1 */ ;
/*!50003 SET character_set_results = latin1 */ ;
/*!50003 SET collation_connection  = latin1_swedish_ci */ ;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;
/*!50003 SET sql_mode              = '' */ ;
DELIMITER ;;
CREATE DEFINER=`analytics_dev`@`%` PROCEDURE `GetAddressCounts`(
    IN Device VARCHAR(255),
    IN fromTime INT,
    IN toTime INT,
    IN IP VARCHAR(255),
    IN assetList TEXT
)
BEGIN
    SET @sqlQuery = CONCAT('
        SELECT
            MEASURE_START_DATE DIV 30 * 30 AS time,
            COUNT(DISTINCT all_ip) AS "All IP",
            COUNT(DISTINCT local_ip) AS "Local IP",
            COUNT(DISTINCT all_ip) - COUNT(DISTINCT local_ip) AS "Remote IP"
        FROM (
            SELECT 
                MEASURE_START_DATE,
                address AS all_ip,
                CASE 
                    WHEN
                    ((INET_ATON(address) > INET_ATON(''192.168.0.0'') AND INET_ATON(address) < INET_ATON(''192.168.255.255'')) 
                    OR (INET_ATON(address) > INET_ATON(''172.16.0.0'') AND INET_ATON(address) < INET_ATON(''172.31.255.255'')) 
                    OR (INET_ATON(address) > INET_ATON(''10.0.0.0'') AND INET_ATON(address) < INET_ATON(''10.255.255.255'')) 
                    OR (INET_ATON(address) > INET_ATON(''127.0.0.0'') AND INET_ATON(address) < INET_ATON(''127.255.255.255'')))
                    THEN address
                    ELSE NULL
                END AS local_ip
            FROM (
                SELECT MEASURE_START_DATE, DST_ADDRESS as address FROM analytics.TELEMETRY_HEADER PARTITION (', Device, ') 
                WHERE MEASURE_START_DATE BETWEEN ', fromTime, ' AND ', toTime, 
                IF(IP IS NOT NULL AND IP != '''', CONCAT(' AND SRC_ADDRESS = ''', IP, ''''), ''),
                IF(assetList IS NOT NULL AND assetList != '''', CONCAT(' AND (SRC_ETH_ADDRESS IN (', assetList, ') OR DST_ETH_ADDRESS IN (', assetList, '))'),''),
                ' 
                UNION ALL 
                SELECT MEASURE_START_DATE, SRC_ADDRESS as address FROM analytics.TELEMETRY_HEADER PARTITION (', Device, ') 
                WHERE MEASURE_START_DATE BETWEEN ', fromTime, ' AND ', toTime, 
                IF(IP IS NOT NULL AND IP != '''', CONCAT(' AND DST_ADDRESS = ''', IP, ''''), ''),
                IF(assetList IS NOT NULL AND assetList != '''', CONCAT(' AND (SRC_ETH_ADDRESS IN (', assetList, ') OR DST_ETH_ADDRESS IN (', assetList, '))'),''),
            ') AS CombinedAddresses
        ) AS Data 
        GROUP BY time
        ORDER BY time;');

    PREPARE stmt FROM @sqlQuery;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;
END ;;
DELIMITER ;
/*!50003 SET sql_mode              = @saved_sql_mode */ ;
/*!50003 SET character_set_client  = @saved_cs_client */ ;
/*!50003 SET character_set_results = @saved_cs_results */ ;
/*!50003 SET collation_connection  = @saved_col_connection */ ;
/*!50003 DROP PROCEDURE IF EXISTS `GetAlertDetails` */;
/*!50003 SET @saved_cs_client      = @@character_set_client */ ;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;
/*!50003 SET character_set_client  = latin1 */ ;
/*!50003 SET character_set_results = latin1 */ ;
/*!50003 SET collation_connection  = latin1_swedish_ci */ ;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;
/*!50003 SET sql_mode              = '' */ ;
DELIMITER ;;
CREATE DEFINER=`analytics_dev`@`%` PROCEDURE `GetAlertDetails`(
    IN ID VARCHAR(255)
)
BEGIN
    SELECT
    ANOMALY_START_DATE AS 'Start',
    ANOMALY_END_DATE AS 'End',
    SRC_ADDRESS AS SRC_IP,
    SRC_PORT,
    DST_ADDRESS AS DST_IP,
    DST_PORT,
    SEVERITY AS Severity,
    ERROR_TAG AS Error,
    ANOMALY_DESCRIPTION AS Description
FROM
    analytics.ANOMALY_MEASURE_HEADER AS H
LEFT JOIN analytics.ANOMALY AS L ON
    H.ANOMALY_ID = L.ANOMALY_ID
WHERE
    ANOMALY_GUID = ID;
END ;;
DELIMITER ;
/*!50003 SET sql_mode              = @saved_sql_mode */ ;
/*!50003 SET character_set_client  = @saved_cs_client */ ;
/*!50003 SET character_set_results = @saved_cs_results */ ;
/*!50003 SET collation_connection  = @saved_col_connection */ ;
/*!50003 DROP PROCEDURE IF EXISTS `GetAnomaliesByAsset` */;
/*!50003 SET @saved_cs_client      = @@character_set_client */ ;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;
/*!50003 SET character_set_client  = latin1 */ ;
/*!50003 SET character_set_results = latin1 */ ;
/*!50003 SET collation_connection  = latin1_swedish_ci */ ;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;
/*!50003 SET sql_mode              = '' */ ;
DELIMITER ;;
CREATE DEFINER=`analytics_dev`@`%` PROCEDURE `GetAnomaliesByAsset`(
    IN Device VARCHAR(255), 
    IN fromTime INT, 
    IN toTime INT,
    IN assetList TEXT
)
BEGIN
    SET @sql = CONCAT('
        SELECT SRC_ADDRESS AS IP,
          MAX(ANOMALY_START_DATE) AS Last_Seen,
          SUM(CASE WHEN A.ERROR_TAG = 1 THEN 1 ELSE 0 END) AS Errors,
          COUNT(CASE WHEN A.ERROR_TAG = 0 THEN 1 END) AS Anomalies
        FROM analytics.ANOMALY_MEASURE_HEADER PARTITION (', Device, ') AS H
          INNER JOIN analytics.ANOMALY AS A ON H.ANOMALY_ID=A.ANOMALY_ID
        WHERE H.ANOMALY_START_DATE BETWEEN ', fromTime, ' AND ', toTime, IF(assetList IS NOT NULL AND assetList != '''', CONCAT(' AND (SRC_ETH_ADDRESS IN (', assetList, ') OR DST_ETH_ADDRESS IN (', assetList, '))'),''), '
            AND H.ANOMALY_ID != ''ML''
        GROUP BY IP;
    ');

    PREPARE stmt FROM @sql;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;
END ;;
DELIMITER ;
/*!50003 SET sql_mode              = @saved_sql_mode */ ;
/*!50003 SET character_set_client  = @saved_cs_client */ ;
/*!50003 SET character_set_results = @saved_cs_results */ ;
/*!50003 SET collation_connection  = @saved_col_connection */ ;
/*!50003 DROP PROCEDURE IF EXISTS `GetAnomaliesCount` */;
/*!50003 SET @saved_cs_client      = @@character_set_client */ ;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;
/*!50003 SET character_set_client  = latin1 */ ;
/*!50003 SET character_set_results = latin1 */ ;
/*!50003 SET collation_connection  = latin1_swedish_ci */ ;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;
/*!50003 SET sql_mode              = '' */ ;
DELIMITER ;;
CREATE DEFINER=`analytics_dev`@`%` PROCEDURE `GetAnomaliesCount`(
    IN Device VARCHAR(255), 
    IN fromTime INT, 
    IN toTime INT,
    IN assetList TEXT
)
BEGIN
    SET @sql = CONCAT('
        SELECT 
          SUM(CASE WHEN A.ERROR_TAG = 1 THEN 1 ELSE 0 END) AS Errors,
          COUNT(CASE WHEN A.ERROR_TAG = 0 THEN 1 END) AS Anomalies
        FROM analytics.ANOMALY_MEASURE_HEADER PARTITION (', Device, ') AS H
          INNER JOIN analytics.ANOMALY AS A ON H.ANOMALY_ID=A.ANOMALY_ID
        WHERE H.ANOMALY_START_DATE BETWEEN ', fromTime, ' AND ', toTime, IF(assetList IS NOT NULL AND assetList != '''', CONCAT(' AND (SRC_ETH_ADDRESS IN (', assetList, ') OR DST_ETH_ADDRESS IN (', assetList, '))'),''), '
            AND H.ANOMALY_ID != ''ML''
    ');

    PREPARE stmt FROM @sql;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;
END ;;
DELIMITER ;
/*!50003 SET sql_mode              = @saved_sql_mode */ ;
/*!50003 SET character_set_client  = @saved_cs_client */ ;
/*!50003 SET character_set_results = @saved_cs_results */ ;
/*!50003 SET collation_connection  = @saved_col_connection */ ;
/*!50003 DROP PROCEDURE IF EXISTS `GetAnomaliesDetailedList` */;
/*!50003 SET @saved_cs_client      = @@character_set_client */ ;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;
/*!50003 SET character_set_client  = latin1 */ ;
/*!50003 SET character_set_results = latin1 */ ;
/*!50003 SET collation_connection  = latin1_swedish_ci */ ;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;
/*!50003 SET sql_mode              = '' */ ;
DELIMITER ;;
CREATE DEFINER=`analytics_dev`@`%` PROCEDURE `GetAnomaliesDetailedList`(
    IN Device VARCHAR(255), 
    IN fromTime INT, 
    IN toTime INT,
    IN IP VARCHAR(255),
    IN assetList TEXT,
    IN pageSize INT
)
BEGIN
    SET @sql = CONCAT('
        SELECT ERROR_TAG as Type, 
        ANOMALY_TAG as Report,
        ANOMALY_START_DATE AS Start_Time,
        ANOMALY_END_DATE as End_Time,
        SEVERITY,
        ANOMALY_TYPE as Category,
        SRC_ADDRESS,
        SRC_PORT,
        DST_ADDRESS,
        DST_PORT,
        ANOMALY_GUID AS alertid
        FROM analytics.ANOMALY_MEASURE_HEADER PARTITION (', Device, ') AS H
        INNER JOIN analytics.ANOMALY AS L ON H.ANOMALY_ID = L.ANOMALY_ID
        WHERE ANOMALY_START_DATE BETWEEN ', fromTime, ' AND ', toTime, 
        IF(IP IS NOT NULL AND IP != '''', CONCAT(' AND SRC_ADDRESS = ''', IP, ''''), ''),
        IF(assetList IS NOT NULL AND assetList != '''', CONCAT(' AND (SRC_ETH_ADDRESS IN (', assetList, ') OR DST_ETH_ADDRESS IN (', assetList, '))'),''),
        ' AND H.ANOMALY_ID != ''ML''
        ORDER BY Start_Time DESC
        LIMIT ',pageSize,'
    ');

    PREPARE stmt FROM @sql;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;
END ;;
DELIMITER ;
/*!50003 SET sql_mode              = @saved_sql_mode */ ;
/*!50003 SET character_set_client  = @saved_cs_client */ ;
/*!50003 SET character_set_results = @saved_cs_results */ ;
/*!50003 SET collation_connection  = @saved_col_connection */ ;
/*!50003 DROP PROCEDURE IF EXISTS `GetAnomaliesDetected` */;
/*!50003 SET @saved_cs_client      = @@character_set_client */ ;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;
/*!50003 SET character_set_client  = latin1 */ ;
/*!50003 SET character_set_results = latin1 */ ;
/*!50003 SET collation_connection  = latin1_swedish_ci */ ;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;
/*!50003 SET sql_mode              = '' */ ;
DELIMITER ;;
CREATE DEFINER=`analytics_dev`@`%` PROCEDURE `GetAnomaliesDetected`(
    IN Device VARCHAR(255),
    IN fromTime INT, 
    IN toTime INT,
    IN assetList TEXT
)
BEGIN
    SET @sql = CONCAT('
        SELECT 
          COUNT(CASE WHEN A.ERROR_TAG = 0 THEN 1 END) AS Anomalies
        FROM analytics.ANOMALY_MEASURE_HEADER PARTITION (', Device, ') AS H
        INNER JOIN analytics.ANOMALY AS A ON H.ANOMALY_ID=A.ANOMALY_ID
        WHERE H.ANOMALY_START_DATE BETWEEN ', fromTime, ' AND ', toTime, 
        IF(assetList IS NOT NULL AND assetList != '''', 
            CONCAT(' AND (SRC_ETH_ADDRESS IN (', assetList, ') OR DST_ETH_ADDRESS IN (', assetList, '))'), 
            ''
        ), 
        ' AND H.ANOMALY_ID != ''ML'' 
    ');
    
    PREPARE stmt FROM @sql;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;
END ;;
DELIMITER ;
/*!50003 SET sql_mode              = @saved_sql_mode */ ;
/*!50003 SET character_set_client  = @saved_cs_client */ ;
/*!50003 SET character_set_results = @saved_cs_results */ ;
/*!50003 SET collation_connection  = @saved_col_connection */ ;
/*!50003 DROP PROCEDURE IF EXISTS `GetAnomaliesList` */;
/*!50003 SET @saved_cs_client      = @@character_set_client */ ;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;
/*!50003 SET character_set_client  = latin1 */ ;
/*!50003 SET character_set_results = latin1 */ ;
/*!50003 SET collation_connection  = latin1_swedish_ci */ ;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;
/*!50003 SET sql_mode              = '' */ ;
DELIMITER ;;
CREATE DEFINER=`analytics_dev`@`%` PROCEDURE `GetAnomaliesList`(
    IN Device VARCHAR(255), 
    IN fromTime INT, 
    IN toTime INT,
    IN IP VARCHAR(255)
)
BEGIN
    SET @sql = CONCAT('
        SELECT CONCAT(SRC_ADDRESS, " -> ", DST_ADDRESS) as name_IP,
               ANOMALY_START_DATE AS Time,
               CONCAT(L.ANOMALY_TYPE, ": ", Anomaly_Tag) AS Description,
               Error_Tag AS Error,
               ANOMALY_GUID AS alertid
          FROM analytics.ANOMALY_MEASURE_HEADER PARTITION (', Device, ') AS H
          INNER JOIN analytics.ANOMALY AS L ON H.ANOMALY_ID = L.ANOMALY_ID
          WHERE ANOMALY_START_DATE BETWEEN ', fromTime, ' AND ', toTime, 
          IF(IP IS NOT NULL AND IP != '''', CONCAT(' AND SRC_ADDRESS = ''', IP, ''''), ''),
          ' AND H.ANOMALY_ID != ''ML''
          ORDER BY Time DESC;
    ');

    PREPARE stmt FROM @sql;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;
END ;;
DELIMITER ;
/*!50003 SET sql_mode              = @saved_sql_mode */ ;
/*!50003 SET character_set_client  = @saved_cs_client */ ;
/*!50003 SET character_set_results = @saved_cs_results */ ;
/*!50003 SET collation_connection  = @saved_col_connection */ ;
/*!50003 DROP PROCEDURE IF EXISTS `GetAnomaliesPerAsset` */;
/*!50003 SET @saved_cs_client      = @@character_set_client */ ;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;
/*!50003 SET character_set_client  = latin1 */ ;
/*!50003 SET character_set_results = latin1 */ ;
/*!50003 SET collation_connection  = latin1_swedish_ci */ ;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;
/*!50003 SET sql_mode              = '' */ ;
DELIMITER ;;
CREATE DEFINER=`analytics_dev`@`%` PROCEDURE `GetAnomaliesPerAsset`(
    IN Device VARCHAR(255), 
    IN fromTime INT, 
    IN toTime INT
)
BEGIN
    SET @sql = CONCAT('
      SELECT Asset, TotalAnomalies AS Anomalies
      FROM (
          SELECT COALESCE(SRC_ADDRESS, SRC_ETH_ADDRESS) AS Asset,
          COUNT(*) AS TotalAnomalies
          FROM analytics.ANOMALY_MEASURE_HEADER PARTITION(', Device, ') AS H
          WHERE ANOMALY_START_DATE BETWEEN ', fromTime, ' AND ', toTime, '
            AND H.ANOMALY_ID != ''ML''
          GROUP BY COALESCE(SRC_ADDRESS, SRC_ETH_ADDRESS)
      ) AS CombinedData
      GROUP BY Asset
      ORDER BY Anomalies DESC
      LIMIT 5;
    ');

    PREPARE stmt FROM @sql;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;
END ;;
DELIMITER ;
/*!50003 SET sql_mode              = @saved_sql_mode */ ;
/*!50003 SET character_set_client  = @saved_cs_client */ ;
/*!50003 SET character_set_results = @saved_cs_results */ ;
/*!50003 SET collation_connection  = @saved_col_connection */ ;
/*!50003 DROP PROCEDURE IF EXISTS `GetAnomaliesSummary` */;
/*!50003 SET @saved_cs_client      = @@character_set_client */ ;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;
/*!50003 SET character_set_client  = latin1 */ ;
/*!50003 SET character_set_results = latin1 */ ;
/*!50003 SET collation_connection  = latin1_swedish_ci */ ;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;
/*!50003 SET sql_mode              = '' */ ;
DELIMITER ;;
CREATE DEFINER=`analytics_dev`@`%` PROCEDURE `GetAnomaliesSummary`(
    IN Device VARCHAR(255), 
    IN fromTime INT, 
    IN toTime INT
)
BEGIN
    SET @sql = CONCAT('
      SELECT ANOMALY_TAG AS Type,
      COUNT(*) AS Anomalies
      FROM (
        SELECT ANOMALY_TAG
        FROM analytics.ANOMALY_MEASURE_HEADER PARTITION(', Device, ') AS H
        LEFT JOIN analytics.ANOMALY AS A ON A.ANOMALY_ID = H.ANOMALY_ID
        WHERE ANOMALY_START_DATE BETWEEN ', fromTime, ' AND ', toTime, '
            AND H.ANOMALY_ID != ''ML''
     ) AS CombinedData
      GROUP BY ANOMALY_TAG
      HAVING COUNT(*) > 0
      ORDER BY Anomalies DESC
      LIMIT 5;
    ');

    PREPARE stmt FROM @sql;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;
END ;;
DELIMITER ;
/*!50003 SET sql_mode              = @saved_sql_mode */ ;
/*!50003 SET character_set_client  = @saved_cs_client */ ;
/*!50003 SET character_set_results = @saved_cs_results */ ;
/*!50003 SET collation_connection  = @saved_col_connection */ ;
/*!50003 DROP PROCEDURE IF EXISTS `GetAnomaliesTrend` */;
/*!50003 SET @saved_cs_client      = @@character_set_client */ ;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;
/*!50003 SET character_set_client  = latin1 */ ;
/*!50003 SET character_set_results = latin1 */ ;
/*!50003 SET collation_connection  = latin1_swedish_ci */ ;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;
/*!50003 SET sql_mode              = '' */ ;
DELIMITER ;;
CREATE DEFINER=`analytics_dev`@`%` PROCEDURE `GetAnomaliesTrend`(
    IN Device VARCHAR(255),
    IN fromTime INT,
    IN toTime INT,
    IN assetList TEXT
)
BEGIN
    SET @sqlQuery = CONCAT('
        SELECT 
            H.ANOMALY_START_DATE DIV 30 * 30 AS time,
            COALESCE(SUM(CASE WHEN A.ERROR_TAG = 1 THEN 1 ELSE 0 END),0) AS Errors,
            COALESCE(SUM(CASE WHEN A.ERROR_TAG = 0 THEN 1 END),0) AS Anomalies
        FROM analytics.ANOMALY_MEASURE_HEADER PARTITION (', Device, ') AS H
        INNER JOIN analytics.ANOMALY AS A ON H.ANOMALY_ID = A.ANOMALY_ID
        WHERE H.ANOMALY_START_DATE BETWEEN ', fromTime, ' AND ', toTime, IF(assetList IS NOT NULL AND assetList != '''', CONCAT(' AND (SRC_ETH_ADDRESS IN (', assetList, ') OR DST_ETH_ADDRESS IN (', assetList, '))'),''),' 
        GROUP BY time
        ORDER BY time;');
    
    SET @count_query = CONCAT('SELECT COUNT(*) AS `Anomalies Detected` FROM analytics.ANOMALY_MEASURE_HEADER PARTITION (', Device, ') ',
                      'WHERE ANOMALY_START_DATE BETWEEN ', fromTime, ' AND ', toTime, IF(assetList IS NOT NULL AND assetList != '''', CONCAT(' AND (SRC_ETH_ADDRESS IN (', assetList, ') OR DST_ETH_ADDRESS IN (', assetList, '))'),''), 
                      ' AND ANOMALY_ID != "ML"');

    PREPARE stmt FROM @sqlQuery;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;

    PREPARE count_stmt FROM @count_query;
    EXECUTE count_stmt;
    DEALLOCATE PREPARE count_stmt;

END ;;
DELIMITER ;
/*!50003 SET sql_mode              = @saved_sql_mode */ ;
/*!50003 SET character_set_client  = @saved_cs_client */ ;
/*!50003 SET character_set_results = @saved_cs_results */ ;
/*!50003 SET collation_connection  = @saved_col_connection */ ;
/*!50003 DROP PROCEDURE IF EXISTS `GetAssetDetails` */;
/*!50003 SET @saved_cs_client      = @@character_set_client */ ;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;
/*!50003 SET character_set_client  = latin1 */ ;
/*!50003 SET character_set_results = latin1 */ ;
/*!50003 SET collation_connection  = latin1_swedish_ci */ ;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;
/*!50003 SET sql_mode              = '' */ ;
DELIMITER ;;
CREATE DEFINER=`analytics_dev`@`%` PROCEDURE `GetAssetDetails`(
    IN Device VARCHAR(255),
    IN fromTime INT,
    IN toTime INT,
    IN asset VARCHAR(255),
    IN assetType VARCHAR(20)
)
BEGIN
    SET @sqlQuery = CONCAT('
        SELECT 
            GROUP_CONCAT(DISTINCT Device SEPARATOR '', '') AS Device,
            IP,
            MAX(MEASURE_START_DATE) AS Last_Seen,
            CASE WHEN MAX(Manufacturer) = '''' THEN ''Unknown'' ELSE MAX(Manufacturer) END AS Manufacturer,
            COUNT(*) AS ''Network Flow'',
            CAST(SUM(Packets_Sent) AS SIGNED) AS ''Packets Sent'',
            CAST(SUM(Packets_Received) AS SIGNED) AS ''Packets Received'',
            MAX(Anomalies) AS Anomalies
        FROM (
            SELECT 
                MAX(H.SRC_ETH_ADDRESS) AS Device,
                MAX(H.SRC_ADDRESS) AS IP,
                MAX(MEASURE_START_DATE) AS MEASURE_START_DATE,
                MAX(CASE WHEN T.MEASURE_TYPE = ''eth_src_oui_resolved'' THEN T.MEASURE_STRING ELSE NULL END) AS Manufacturer,
                MAX(CASE 
                    WHEN T.MEASURE_TYPE = ''count_of_frames_sent'' THEN T.MEASURE_VALUE
                    WHEN T.MEASURE_TYPE = ''num_of_frames_withinwindow_per_src_dst'' THEN T.MEASURE_VALUE
                ELSE 0 
                END) AS Packets_Sent,
                0 AS Packets_Received,
                COALESCE(MAX(Anomalies), 0) AS Anomalies
            FROM analytics.TELEMETRY_HEADER PARTITION (', Device, ') AS H
            INNER JOIN analytics.TELEMETRY PARTITION (', Device, ') AS T 
                ON H.TELEMETRY_GUID = T.TELEMETRY_GUID 
                AND ', IF(assetType = 'IP', 'H.SRC_ADDRESS', 'H.SRC_ETH_ADDRESS'), ' = ''', asset, '''  AND MEASURE_TYPE IN (''eth_src_oui_resolved'',''count_of_frames_sent'',''num_of_frames_withinwindow_per_src_dst'')
LEFT JOIN
              (SELECT count(*) as Anomalies, SRC_ETH_ADDRESS 
              FROM analytics.ANOMALY_MEASURE_HEADER PARTITION (', Device, ')
              WHERE ANOMALY_ID <> ''ML'' 
  AND ', IF(assetType = 'IP', 'SRC_ADDRESS', 'SRC_ETH_ADDRESS'), ' = ''', asset, '''
              AND ANOMALY_START_DATE BETWEEN ', fromTime, ' AND ', toTime, '
              GROUP BY SRC_ETH_ADDRESS ) AS A
            ON H.SRC_ETH_ADDRESS = A.SRC_ETH_ADDRESS
            WHERE MEASURE_START_DATE BETWEEN ', fromTime, ' AND ', toTime, '
            GROUP BY H.TELEMETRY_GUID

            UNION ALL

            SELECT 
                MAX(DST_ETH_ADDRESS) AS Device,
                MAX(DST_ADDRESS) AS IP,
                MAX(MEASURE_START_DATE) AS MEASURE_START_DATE,
                MAX(CASE WHEN T.MEASURE_TYPE = ''eth_dst_oui_resolved'' THEN T.MEASURE_STRING ELSE NULL END) AS Manufacturer,
                0 AS Packets_Sent,
                MAX(CASE 
                    WHEN T.MEASURE_TYPE = ''count_of_frames_received_by_dstport'' THEN T.MEASURE_VALUE
                    WHEN T.MEASURE_TYPE = ''num_of_frames_withinwindow_per_src_dst'' THEN T.MEASURE_VALUE
                ELSE 0 
                END) AS Packets_Received,
                0 AS Anomalies
            FROM analytics.TELEMETRY_HEADER PARTITION (', Device, ') AS H
            INNER JOIN analytics.TELEMETRY PARTITION (', Device, ') AS T 
                ON H.TELEMETRY_GUID = T.TELEMETRY_GUID 
                AND ', IF(assetType = 'IP', 'H.DST_ADDRESS', 'H.DST_ETH_ADDRESS'), ' = ''', asset, '''  AND MEASURE_TYPE IN (''eth_dst_oui_resolved'',''count_of_frames_received_by_dstport'',''num_of_frames_withinwindow_per_src_dst'')
            WHERE MEASURE_START_DATE BETWEEN ', fromTime, ' AND ', toTime, '
            GROUP BY H.TELEMETRY_GUID
        ) AS CombinedData
        GROUP BY IP;');

    PREPARE stmt FROM @sqlQuery;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;
END ;;
DELIMITER ;
/*!50003 SET sql_mode              = @saved_sql_mode */ ;
/*!50003 SET character_set_client  = @saved_cs_client */ ;
/*!50003 SET character_set_results = @saved_cs_results */ ;
/*!50003 SET collation_connection  = @saved_col_connection */ ;
/*!50003 DROP PROCEDURE IF EXISTS `GetAssetList` */;
/*!50003 SET @saved_cs_client      = @@character_set_client */ ;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;
/*!50003 SET character_set_client  = latin1 */ ;
/*!50003 SET character_set_results = latin1 */ ;
/*!50003 SET collation_connection  = latin1_swedish_ci */ ;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;
/*!50003 SET sql_mode              = '' */ ;
DELIMITER ;;
CREATE DEFINER=`analytics_dev`@`%` PROCEDURE `GetAssetList`(
    IN Device VARCHAR(255),
    IN fromTime INT,
    IN toTime INT,
    IN pageSize INT,
    IN pageNumber INT
)
BEGIN
    DECLARE offsetParam INT;
    SET offsetParam = pageNumber * pageSize;
 
    SET @mainQuery = CONCAT('
            SELECT 
        ''Passive'' as Discovery,
        GROUP_CONCAT(distinct ASSET_ETH_ADDRESS SEPARATOR '', '') as Device,
                ASSET_IP AS IP,
                MAX(ASSET_LAST_SEEN) AS Last_Seen,
                MAX(CASE WHEN OUI = '''' OR OUI IS NULL THEN ''Unknown'' ELSE OUI END) AS Manufacturer
            FROM analytics.LATEST_ASSET
            WHERE ASSET_LAST_SEEN BETWEEN ', fromTime, ' AND ', toTime, ' AND DEVICE_ID = REPLACE(''' , Device, ''', ''_'', '':'')
      GROUP BY  CASE WHEN ASSET_IP IS NULL THEN ASSET_ETH_ADDRESS ELSE 0 END ,Discovery, ASSET_IP
        
        UNION ALL
        
       SELECT
            ''Active'' as Discovery,
            ASSET_ETH_ADDRESS AS Device,
            ASSET_ID AS IP,
            ETL_TIME AS Last_Seen,
            SYS_DESCRIPTION AS Manufacturer
          FROM analytics.SUBNET_ASSET_DETAILS AS H
          INNER JOIN analytics.SUBNET_DETAILS as T ON H.SUBNET_ID = T.SUBNET_ID
          WHERE ETL_TIME BETWEEN ', fromTime, ' AND ', toTime, ' AND DEVICE_ID = REPLACE(''' , Device, ''', ''_'', '':'')
          GROUP BY H.SUBNET_ID, T.DEVICE_ID, ASSET_ID, ETL_TIME, SYS_DESCRIPTION
          order by Last_Seen DESC 
          LIMIT ', pageSize, ' OFFSET ', offsetParam, ';');
 
    SET @countQuery = CONCAT(' SELECT COUNT(COUNTS) FROM (
                SELECT COUNT(*) as COUNTS
                FROM analytics.LATEST_ASSET 
                WHERE ASSET_LAST_SEEN BETWEEN ', fromTime, ' AND ', toTime, ' AND DEVICE_ID = REPLACE(''' , Device, ''', ''_'', '':'')
        GROUP BY ASSET_IP, CASE WHEN ASSET_IP IS NULL THEN ASSET_ETH_ADDRESS ELSE 0 END
                UNION ALL

                SELECT COUNT(*) as COUNTS
                FROM analytics.SUBNET_ASSET_DETAILS H
                INNER JOIN analytics.SUBNET_DETAILS T ON H.SUBNET_ID = T.SUBNET_ID
                WHERE ETL_TIME BETWEEN ', fromTime, ' AND ', toTime, ' AND DEVICE_ID = REPLACE(''' , Device, ''', ''_'', '':'')
                GROUP BY ASSET_ID
) as abc ;');
 
    PREPARE mainStmt FROM @mainQuery;
    EXECUTE mainStmt;
    DEALLOCATE PREPARE mainStmt;
    PREPARE countStmt FROM @countQuery;
    EXECUTE countStmt;
    DEALLOCATE PREPARE countStmt;
END ;;
DELIMITER ;
/*!50003 SET sql_mode              = @saved_sql_mode */ ;
/*!50003 SET character_set_client  = @saved_cs_client */ ;
/*!50003 SET character_set_results = @saved_cs_results */ ;
/*!50003 SET collation_connection  = @saved_col_connection */ ;
/*!50003 DROP PROCEDURE IF EXISTS `GetAssetListExport` */;
/*!50003 SET @saved_cs_client      = @@character_set_client */ ;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;
/*!50003 SET character_set_client  = latin1 */ ;
/*!50003 SET character_set_results = latin1 */ ;
/*!50003 SET collation_connection  = latin1_swedish_ci */ ;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;
/*!50003 SET sql_mode              = '' */ ;
DELIMITER ;;
CREATE DEFINER=`analytics_dev`@`%` PROCEDURE `GetAssetListExport`(
    IN device VARCHAR(255), 
    IN fromTime INT, 
    IN toTime INT
)
BEGIN
    SET @sql = CONCAT('
        SELECT 
            ''Passive'' AS Discovery,
            GROUP_CONCAT(DISTINCT COALESCE(Device, '''') ORDER BY Device SEPARATOR '', '') AS Device,
            IP,
            DATE_FORMAT(FROM_UNIXTIME(MAX(MEASURE_START_DATE)), ''%m/%d/%Y %H:%i:%s'') AS Last_Seen,
            COALESCE(MAX(Manufacturer), ''Unknown'') AS Manufacturer,
            COUNT(*) AS ''Network Flow'',
            SUM(Packets_Sent) AS ''Packets Sent'',
            SUM(Packets_Received) AS ''Packets Received'',
            SUM(Anomalies) AS Anomalies
        FROM (
            SELECT 
                H.SRC_ETH_ADDRESS AS Device,
                H.SRC_ADDRESS AS IP,
                MEASURE_START_DATE,
                CASE 
                    WHEN T.MEASURE_TYPE = ''eth_src_oui_resolved'' THEN T.MEASURE_STRING 
                END AS Manufacturer,
                CASE 
                    WHEN T.MEASURE_TYPE = ''count_of_frames_sent'' THEN T.MEASURE_VALUE
                    WHEN T.MEASURE_TYPE = ''num_of_frames_withinwindow_per_src_dst'' THEN T.MEASURE_VALUE
                    ELSE 0 
                END AS Packets_Sent,
                0 AS Packets_Received,
                COALESCE(A.Anomalies, 0) AS Anomalies
            FROM analytics.TELEMETRY PARTITION (', device, ') AS T
            INNER JOIN analytics.TELEMETRY_HEADER PARTITION (', device, ') AS H ON H.TELEMETRY_GUID = T.TELEMETRY_GUID  AND MEASURE_TYPE IN (''eth_src_oui_resolved'',''count_of_frames_sent'',''num_of_frames_withinwindow_per_src_dst'')
            LEFT JOIN (
                SELECT COUNT(*) AS Anomalies, SRC_ADDRESS
                FROM analytics.ANOMALY_MEASURE_HEADER PARTITION (', device, ') 
                WHERE ANOMALY_ID <> ''ML''
                  AND ANOMALY_START_DATE BETWEEN ', fromTime, ' AND ', toTime, ' 
                GROUP BY SRC_ADDRESS
            ) AS A ON H.SRC_ADDRESS = A.SRC_ADDRESS
            WHERE MEASURE_START_DATE BETWEEN ', fromTime, ' AND ', toTime, '
            UNION ALL
            SELECT
                H.DST_ETH_ADDRESS AS Device,
                H.DST_ADDRESS AS IP,
                MEASURE_START_DATE,
                CASE 
                    WHEN T.MEASURE_TYPE = ''eth_dst_oui_resolved'' THEN T.MEASURE_STRING 
                END AS Manufacturer,
                0 AS Packets_Sent,
                CASE 
                    WHEN T.MEASURE_TYPE = ''count_of_frames_received_by_dstport'' THEN T.MEASURE_VALUE
                    WHEN T.MEASURE_TYPE = ''num_of_frames_withinwindow_per_src_dst'' THEN T.MEASURE_VALUE
                    ELSE 0 
                END AS Packets_Received,
                0 AS Anomalies
            FROM analytics.TELEMETRY PARTITION (', device, ') AS T
            INNER JOIN analytics.TELEMETRY_HEADER PARTITION (', device, ') AS H ON H.TELEMETRY_GUID = T.TELEMETRY_GUID  AND MEASURE_TYPE IN (''eth_dst_oui_resolved'',''count_of_frames_received_by_dstport'',''num_of_frames_withinwindow_per_src_dst'')
            WHERE MEASURE_START_DATE BETWEEN ', fromTime, ' AND ', toTime, '
        ) AS CombinedData
        GROUP BY Discovery, IP,
            CASE WHEN IP IS NULL THEN Device ELSE 0 END
    ');

    PREPARE stmt FROM @sql;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;
END ;;
DELIMITER ;
/*!50003 SET sql_mode              = @saved_sql_mode */ ;
/*!50003 SET character_set_client  = @saved_cs_client */ ;
/*!50003 SET character_set_results = @saved_cs_results */ ;
/*!50003 SET collation_connection  = @saved_col_connection */ ;
/*!50003 DROP PROCEDURE IF EXISTS `GetAssetsDiscovered` */;
/*!50003 SET @saved_cs_client      = @@character_set_client */ ;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;
/*!50003 SET character_set_client  = latin1 */ ;
/*!50003 SET character_set_results = latin1 */ ;
/*!50003 SET collation_connection  = latin1_swedish_ci */ ;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;
/*!50003 SET sql_mode              = '' */ ;
DELIMITER ;;
CREATE DEFINER=`analytics_dev`@`%` PROCEDURE `GetAssetsDiscovered`(
    IN Device VARCHAR(255), 
    IN fromTime INT, 
    IN toTime INT,
    IN assetList TEXT
)
BEGIN
    
    SET @sql = CONCAT('
        SELECT COUNT(*) AS `Assets Discovered`
        FROM (
            SELECT COUNT(*) AS entriesInGroup
            FROM (
                SELECT SRC_ETH_ADDRESS AS Device, SRC_ADDRESS AS IP, MEASURE_START_DATE
                FROM analytics.TELEMETRY_HEADER PARTITION (', Device, ') AS H
                WHERE MEASURE_START_DATE BETWEEN ', fromTime, ' AND ', toTime, IF(assetList IS NOT NULL AND assetList != '''', CONCAT(' AND (SRC_ETH_ADDRESS IN (', assetList, ') OR DST_ETH_ADDRESS IN (', assetList, '))'),''), '
                UNION ALL
                SELECT DST_ETH_ADDRESS AS Device, DST_ADDRESS AS IP, MEASURE_START_DATE
                FROM analytics.TELEMETRY_HEADER PARTITION (', Device, ') AS H
                WHERE MEASURE_START_DATE BETWEEN ', fromTime, ' AND ', toTime, IF(assetList IS NOT NULL AND assetList != '''', CONCAT(' AND (SRC_ETH_ADDRESS IN (', assetList, ') OR DST_ETH_ADDRESS IN (', assetList, '))'),''), '
            ) AS CombinedData
            GROUP BY IP, CASE WHEN IP IS NULL THEN Device ELSE 0 END
        ) AS GroupedData;');

    PREPARE stmt FROM @sql;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;
END ;;
DELIMITER ;
/*!50003 SET sql_mode              = @saved_sql_mode */ ;
/*!50003 SET character_set_client  = @saved_cs_client */ ;
/*!50003 SET character_set_results = @saved_cs_results */ ;
/*!50003 SET collation_connection  = @saved_col_connection */ ;
/*!50003 DROP PROCEDURE IF EXISTS `GetAssetSelection` */;
/*!50003 SET @saved_cs_client      = @@character_set_client */ ;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;
/*!50003 SET character_set_client  = latin1 */ ;
/*!50003 SET character_set_results = latin1 */ ;
/*!50003 SET collation_connection  = latin1_swedish_ci */ ;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;
/*!50003 SET sql_mode              = '' */ ;
DELIMITER ;;
CREATE DEFINER=`analytics_dev`@`%` PROCEDURE `GetAssetSelection`(
    IN Device VARCHAR(255),
    IN fromTime INT,
    IN toTime INT
)
BEGIN
    SET @sqlQuery = CONCAT('
        SELECT IP, MAC
        FROM (
            SELECT SRC_ADDRESS AS IP,
                   CASE WHEN SRC_ADDRESS IS NULL THEN SRC_ETH_ADDRESS END AS MAC
            FROM analytics.TELEMETRY_HEADER PARTITION (', Device, ') AS H
            WHERE MEASURE_START_DATE BETWEEN ', fromTime, ' AND ', toTime, '
            
            UNION ALL
            
            SELECT DST_ADDRESS AS IP,
                   CASE WHEN DST_ADDRESS IS NULL THEN DST_ETH_ADDRESS END AS MAC
            FROM analytics.TELEMETRY_HEADER PARTITION (', Device, ') AS H
            WHERE MEASURE_START_DATE BETWEEN ', fromTime, ' AND ', toTime, '
        ) AS CombinedData
        GROUP BY MAC, IP,
                 CASE WHEN IP IS NULL THEN MAC ELSE 0 END
        ORDER BY MAC, IP;');

    PREPARE stmt FROM @sqlQuery;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;
END ;;
DELIMITER ;
/*!50003 SET sql_mode              = @saved_sql_mode */ ;
/*!50003 SET character_set_client  = @saved_cs_client */ ;
/*!50003 SET character_set_results = @saved_cs_results */ ;
/*!50003 SET collation_connection  = @saved_col_connection */ ;
/*!50003 DROP PROCEDURE IF EXISTS `GetAssetTreeList` */;
/*!50003 SET @saved_cs_client      = @@character_set_client */ ;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;
/*!50003 SET character_set_client  = latin1 */ ;
/*!50003 SET character_set_results = latin1 */ ;
/*!50003 SET collation_connection  = latin1_swedish_ci */ ;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;
/*!50003 SET sql_mode              = '' */ ;
DELIMITER ;;
CREATE DEFINER=`analytics_dev`@`%` PROCEDURE `GetAssetTreeList`(
    IN Device VARCHAR(255),
    IN fromTime INT,
    IN toTime INT
)
BEGIN
 
    SET @mainQuery = CONCAT('
        SELECT ASSET_ID AS IP,
        ASSET_ETH_ADDRESS AS Device,
        HOSTNAME as HOST_NAME
        FROM analytics.SUBNET_ASSET_DETAILS AS H
        JOIN analytics.SUBNET_DETAILS as T ON H.SUBNET_ID = T.SUBNET_ID AND DEVICE_ID = REPLACE(''' , Device, ''', ''_'', '':'')
        WHERE ETL_TIME BETWEEN ', fromTime, ' AND ', toTime, ' 
        GROUP BY Device
        
        UNION ALL
        
        SELECT DISTINCT IP, Device, NULL as HOST_NAME FROM (
        SELECT SRC_ADDRESS AS IP, SRC_ETH_ADDRESS AS Device FROM analytics.TELEMETRY_HEADER PARTITION (', Device, ')  WHERE MEASURE_START_DATE BETWEEN ', fromTime, ' AND ', toTime, '
        UNION
        SELECT DST_ADDRESS AS IP, DST_ETH_ADDRESS AS Device FROM analytics.TELEMETRY_HEADER PARTITION (', Device, ')  WHERE MEASURE_START_DATE BETWEEN ', fromTime, ' AND ', toTime, ') as ALL_ETH_ADDRESS;
        
       ');
 
 
    PREPARE mainStmt FROM @mainQuery;
    EXECUTE mainStmt;
    DEALLOCATE PREPARE mainStmt;

END ;;
DELIMITER ;
/*!50003 SET sql_mode              = @saved_sql_mode */ ;
/*!50003 SET character_set_client  = @saved_cs_client */ ;
/*!50003 SET character_set_results = @saved_cs_results */ ;
/*!50003 SET collation_connection  = @saved_col_connection */ ;
/*!50003 DROP PROCEDURE IF EXISTS `GetAssetTreeList2` */;
/*!50003 SET @saved_cs_client      = @@character_set_client */ ;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;
/*!50003 SET character_set_client  = latin1 */ ;
/*!50003 SET character_set_results = latin1 */ ;
/*!50003 SET collation_connection  = latin1_swedish_ci */ ;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;
/*!50003 SET sql_mode              = '' */ ;
DELIMITER ;;
CREATE DEFINER=`analytics_dev`@`%` PROCEDURE `GetAssetTreeList2`(
    IN Device VARCHAR(255),
    IN fromTime INT,
    IN toTime INT
)
BEGIN
 
    SET @mainQuery = CONCAT('
        SELECT ASSET_ID AS IP,
        ASSET_ETH_ADDRESS AS MAC_ID,
        HOSTNAME as HOST_NAME
        FROM analytics.SUBNET_ASSET_DETAILS AS H
        JOIN analytics.SUBNET_DETAILS as T ON H.SUBNET_ID = T.SUBNET_ID AND DEVICE_ID = REPLACE(''' , Device, ''', ''_'', '':'')
        WHERE ETL_TIME BETWEEN ', fromTime, ' AND ', toTime, ' 
        GROUP BY MAC_ID
        
        UNION ALL
        
        SELECT DISTINCT IP, MAC_ID, NULL as HOST_NAME FROM (
        SELECT SRC_ADDRESS AS IP, SRC_ETH_ADDRESS AS MAC_ID FROM analytics.TELEMETRY_HEADER PARTITION (', Device, ')  WHERE MEASURE_START_DATE BETWEEN ', fromTime, ' AND ', toTime, '
        UNION
        SELECT DST_ADDRESS AS IP, DST_ETH_ADDRESS AS MAC_ID FROM analytics.TELEMETRY_HEADER PARTITION (', Device, ')  WHERE MEASURE_START_DATE BETWEEN ', fromTime, ' AND ', toTime, ') as ALL_ETH_ADDRESS;
        
       ');
 
 
    PREPARE mainStmt FROM @mainQuery;
    EXECUTE mainStmt;
    DEALLOCATE PREPARE mainStmt;

END ;;
DELIMITER ;
/*!50003 SET sql_mode              = @saved_sql_mode */ ;
/*!50003 SET character_set_client  = @saved_cs_client */ ;
/*!50003 SET character_set_results = @saved_cs_results */ ;
/*!50003 SET collation_connection  = @saved_col_connection */ ;
/*!50003 DROP PROCEDURE IF EXISTS `GetAvgPacketSize` */;
/*!50003 SET @saved_cs_client      = @@character_set_client */ ;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;
/*!50003 SET character_set_client  = latin1 */ ;
/*!50003 SET character_set_results = latin1 */ ;
/*!50003 SET collation_connection  = latin1_swedish_ci */ ;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;
/*!50003 SET sql_mode              = '' */ ;
DELIMITER ;;
CREATE DEFINER=`analytics_dev`@`%` PROCEDURE `GetAvgPacketSize`(
    IN Device VARCHAR(255),
    IN fromTime INT,
    IN toTime INT,
    IN IP VARCHAR(255),
    IN assetList TEXT
)
BEGIN
    SET @sqlQuery = CONCAT('
        SELECT
            MEASURE_START_DATE DIV 30 * 30 AS time,
            CAST((IFNULL(MAX(frames1), 0) + IFNULL(MAX(frames2), 0) + IFNULL(MAX(frames3), 0)) AS DECIMAL(10, 2)) AS "Average Packet Size",
            THRESHOLD_HIGH AS Threshold
        FROM (
            SELECT 
                MEASURE_START_DATE,
                AVG(
                    CASE 
                        WHEN PROTOCOL IN (''TCP'', ''MODBUS'') AND MEASURE_TYPE = ''avg_frame_length'' THEN MEASURE_VALUE
                        ELSE NULL
                    END
                ) AS frames1,
                AVG(
                    CASE 
                        WHEN PROTOCOL = ''UDP'' AND MEASURE_TYPE = ''avg_frame_udp_len_within_timewindow_per_src_dst'' THEN MEASURE_VALUE
                        ELSE NULL
                    END
                ) AS frames2,
                AVG(
                    CASE 
                        WHEN PROTOCOL = ''ICMP'' AND MEASURE_TYPE = ''avg_frame_len_within_timewindow_per_src_dst'' THEN MEASURE_VALUE
                        ELSE NULL
                    END
                ) AS frames3
            FROM analytics.TELEMETRY_HEADER PARTITION (', Device, ') AS H
            INNER JOIN analytics.TELEMETRY PARTITION (', Device, ') AS T 
                ON H.TELEMETRY_GUID = T.TELEMETRY_GUID 
                AND T.MEASURE_TYPE IN (''avg_frame_length'', ''avg_frame_udp_len_within_timewindow_per_src_dst'',''avg_frame_len_within_timewindow_per_src_dst'') 
            WHERE MEASURE_START_DATE BETWEEN ', fromTime, ' AND ', toTime,
            IF(IP IS NOT NULL AND IP != '''', CONCAT(' AND SRC_ADDRESS = ''', IP, ''''), ''),
            IF(assetList IS NOT NULL AND assetList != '''', CONCAT(' AND (SRC_ETH_ADDRESS IN (', assetList, ') OR DST_ETH_ADDRESS IN (', assetList, '))'),''),
            ' GROUP BY MEASURE_START_DATE
        ) AS CombinedData
        LEFT JOIN analytics.THRESHOLD AS T ON T.THRESHOLD_ID = 3
        GROUP BY time
        ORDER BY time
        LIMIT 50;');

    PREPARE stmt FROM @sqlQuery;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;
END ;;
DELIMITER ;
/*!50003 SET sql_mode              = @saved_sql_mode */ ;
/*!50003 SET character_set_client  = @saved_cs_client */ ;
/*!50003 SET character_set_results = @saved_cs_results */ ;
/*!50003 SET collation_connection  = @saved_col_connection */ ;
/*!50003 DROP PROCEDURE IF EXISTS `GetBroadcastTraffic` */;
/*!50003 SET @saved_cs_client      = @@character_set_client */ ;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;
/*!50003 SET character_set_client  = latin1 */ ;
/*!50003 SET character_set_results = latin1 */ ;
/*!50003 SET collation_connection  = latin1_swedish_ci */ ;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;
/*!50003 SET sql_mode              = '' */ ;
DELIMITER ;;
CREATE DEFINER=`analytics_dev`@`%` PROCEDURE `GetBroadcastTraffic`(
    IN Device VARCHAR(255),
    IN fromTime INT,
    IN toTime INT,
    IN assetList TEXT,
    IN pageSize INT,
    IN page INT
)
BEGIN
    DECLARE offsetParam INT;
    SET offsetParam = page * pageSize;

    SET @mainQuery = CONCAT('
        SELECT
            TH.TELEMETRY_GUID AS ID,
            TH.SRC_ADDRESS AS Source_IP,
            TH.DST_ADDRESS AS Destination_IP,
            COALESCE((CASE 
                WHEN TH.PROTOCOL = ''TCP'' THEN COALESCE(P2.TCP_SERVICE_NAME, P1.TCP_SERVICE_NAME, ''TCP'')
                WHEN TH.PROTOCOL = ''UDP'' THEN COALESCE(P2.UDP_SERVICE_NAME, P1.UDP_SERVICE_NAME, ''UDP'')
                WHEN TH.PROTOCOL = ''MODBUS'' THEN ''MODBUS''
                WHEN TH.PROTOCOL = ''ICMP'' THEN ''ICMP''
                WHEN TH.PROTOCOL = ''ARP'' THEN ''ARP''
                WHEN TH.PROTOCOL = ''IP_NIP'' AND analytics.TELEMETRY.MEASURE_TYPE = ''ip_proto'' AND analytics.TELEMETRY.MEASURE_VALUE = 255 THEN ''Non IP'' 
                ELSE NULL END), ''Other'') AS Service,
            CAST(SUBSTRING_INDEX(TH.SRC_PORT, ''.'', 1) AS UNSIGNED) AS Source_Port,
            CAST(SUBSTRING_INDEX(TH.DST_PORT, ''.'', 1) AS UNSIGNED) AS Destination_Port,
            TH.SRC_ETH_ADDRESS AS Source_MAC,
            TH.DST_ETH_ADDRESS AS Destination_MAC,
            TH.PROTOCOL AS PROTOCOL,
            CONCAT(CEIL((TH.MEASURE_END_DATE - TH.MEASURE_START_DATE)), '' s'') AS Duration,
            TH.MEASURE_START_DATE AS Time
        FROM
            analytics.TELEMETRY_HEADER PARTITION (', Device, ') AS TH
        LEFT JOIN analytics.TELEMETRY PARTITION (', Device, ') ON
            TH.TELEMETRY_GUID = analytics.TELEMETRY.TELEMETRY_GUID AND
            analytics.TELEMETRY.MEASURE_TYPE = ''ip_proto'' AND
            analytics.TELEMETRY.MEASURE_VALUE = 255 
        LEFT JOIN analytics.PORT AS P1 ON
            P1.PORT_NUMBER = TH.SRC_PORT
        LEFT JOIN analytics.PORT AS P2 ON
            P2.PORT_NUMBER = TH.DST_PORT
        WHERE
            TH.MEASURE_START_DATE BETWEEN ', fromTime, ' AND ', toTime, IF(assetList IS NOT NULL AND assetList != '''', CONCAT(' AND (SRC_ETH_ADDRESS IN (', assetList, ') OR DST_ETH_ADDRESS IN (', assetList, '))'),''), '  AND
            ((TH.PROTOCOL IN (''UDP'', ''IP_NIP'') AND (
                TH.DST_ETH_ADDRESS LIKE ''ff:ff:ff:ff:ff:ff'' OR
                TH.DST_ADDRESS = ''255.255.255.255'' OR 
                (SUBSTRING(LPAD(CONV(SUBSTRING_INDEX(TH.DST_ADDRESS, ''.'', 1), 10, 2), 8, ''0''), 1, 1) = ''0'' AND SUBSTRING_INDEX(TH.DST_ETH_ADDRESS, '':'', -3) = ''255.255.255'') OR
                (SUBSTRING(LPAD(CONV(SUBSTRING_INDEX(TH.DST_ADDRESS, ''.'', 1), 10, 2), 8, ''0''), 1, 2) = ''10'' AND SUBSTRING_INDEX(TH.DST_ETH_ADDRESS, '':'', -2) = ''255.255'') OR 
                (SUBSTRING(LPAD(CONV(SUBSTRING_INDEX(TH.DST_ADDRESS, ''.'', 1), 10, 2), 8, ''0''), 1, 3) = ''110'' AND SUBSTRING_INDEX(TH.DST_ETH_ADDRESS, '':'', -1) = ''255''))
            ) OR (TH.PROTOCOL = ''ARP'' AND TH.DST_ETH_ADDRESS = ''ff:ff:ff:ff:ff:ff''))  
        LIMIT ', pageSize, ' OFFSET ', offsetParam, ';');

    SET @countQuery = CONCAT('
        SELECT COUNT(*) AS count
        FROM
            analytics.TELEMETRY_HEADER PARTITION (', Device, ') AS TH
        WHERE
            TH.MEASURE_START_DATE BETWEEN ', fromTime, ' AND ', toTime, IF(assetList IS NOT NULL AND assetList != '''', CONCAT(' AND (SRC_ETH_ADDRESS IN (', assetList, ') OR DST_ETH_ADDRESS IN (', assetList, '))'),''), ' AND
            ((TH.PROTOCOL IN (''UDP'', ''IP_NIP'') AND (
                TH.DST_ETH_ADDRESS LIKE ''ff:ff:ff:ff:ff:ff'' OR
                TH.DST_ADDRESS = ''255.255.255.255'' OR 
                (SUBSTRING(LPAD(CONV(SUBSTRING_INDEX(TH.DST_ADDRESS, ''.'', 1), 10, 2), 8, ''0''), 1, 1) = ''0'' AND SUBSTRING_INDEX(TH.DST_ETH_ADDRESS, '':'', -3) = ''255.255.255'') OR
                (SUBSTRING(LPAD(CONV(SUBSTRING_INDEX(TH.DST_ADDRESS, ''.'', 1), 10, 2), 8, ''0''), 1, 2) = ''10'' AND SUBSTRING_INDEX(TH.DST_ETH_ADDRESS, '':'', -2) = ''255.255'') OR 
                (SUBSTRING(LPAD(CONV(SUBSTRING_INDEX(TH.DST_ADDRESS, ''.'', 1), 10, 2), 8, ''0''), 1, 3) = ''110'' AND SUBSTRING_INDEX(TH.DST_ETH_ADDRESS, '':'', -1) = ''255''))
            ) OR (TH.PROTOCOL = ''ARP'' AND TH.DST_ETH_ADDRESS = ''ff:ff:ff:ff:ff:ff''));');

    PREPARE mainStmt FROM @mainQuery;
    EXECUTE mainStmt;
    DEALLOCATE PREPARE mainStmt;
    
    PREPARE countStmt FROM @countQuery;
    EXECUTE countStmt;
    DEALLOCATE PREPARE countStmt;
END ;;
DELIMITER ;
/*!50003 SET sql_mode              = @saved_sql_mode */ ;
/*!50003 SET character_set_client  = @saved_cs_client */ ;
/*!50003 SET character_set_results = @saved_cs_results */ ;
/*!50003 SET collation_connection  = @saved_col_connection */ ;
/*!50003 DROP PROCEDURE IF EXISTS `GetConnectedAssets` */;
/*!50003 SET @saved_cs_client      = @@character_set_client */ ;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;
/*!50003 SET character_set_client  = latin1 */ ;
/*!50003 SET character_set_results = latin1 */ ;
/*!50003 SET collation_connection  = latin1_swedish_ci */ ;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;
/*!50003 SET sql_mode              = '' */ ;
DELIMITER ;;
CREATE DEFINER=`analytics_dev`@`%` PROCEDURE `GetConnectedAssets`(
    IN Device VARCHAR(255),
    IN fromTime INT,
    IN toTime INT,
    IN asset VARCHAR(255),
    IN assetType VARCHAR(20)
)
BEGIN
    SET @sqlQuery = CONCAT('
        SELECT GROUP_CONCAT(DISTINCT Device SEPARATOR '', '') as Device,
        IP,
        MAX(MEASURE_START_DATE) AS Last_Seen
        FROM (
            SELECT DST_ETH_ADDRESS AS Device,
            DST_ADDRESS AS IP,
            MEASURE_START_DATE AS MEASURE_START_DATE
            FROM analytics.TELEMETRY_HEADER PARTITION (', Device, ') AS H
            WHERE MEASURE_START_DATE BETWEEN ', fromTime, ' AND ', toTime, '
            AND ', IF(assetType = 'IP', 'SRC_ADDRESS', 'SRC_ETH_ADDRESS'), ' = ''', asset, '''
            
            UNION ALL
            
            SELECT SRC_ETH_ADDRESS AS Device,
            SRC_ADDRESS AS IP,
            MEASURE_START_DATE AS MEASURE_START_DATE
            FROM analytics.TELEMETRY_HEADER PARTITION (', Device, ') AS H
            WHERE MEASURE_START_DATE BETWEEN ', fromTime, ' AND ', toTime, '
            AND ', IF(assetType = 'IP', 'DST_ADDRESS', 'DST_ETH_ADDRESS'), ' = ''', asset, '''
        ) AS CombinedData
        ', IF(assetType = 'MAC', 'WHERE IP IS NULL', ''), '
        GROUP BY ', IF(assetType = 'MAC', 'Device, IP', 'IP, CASE WHEN IP IS NULL THEN Device ELSE 0 END'), ';
    ');

    PREPARE stmt FROM @sqlQuery;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;
END ;;
DELIMITER ;
/*!50003 SET sql_mode              = @saved_sql_mode */ ;
/*!50003 SET character_set_client  = @saved_cs_client */ ;
/*!50003 SET character_set_results = @saved_cs_results */ ;
/*!50003 SET collation_connection  = @saved_col_connection */ ;
/*!50003 DROP PROCEDURE IF EXISTS `GetHttpDNS` */;
/*!50003 SET @saved_cs_client      = @@character_set_client */ ;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;
/*!50003 SET character_set_client  = latin1 */ ;
/*!50003 SET character_set_results = latin1 */ ;
/*!50003 SET collation_connection  = latin1_swedish_ci */ ;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;
/*!50003 SET sql_mode              = '' */ ;
DELIMITER ;;
CREATE DEFINER=`analytics_dev`@`%` PROCEDURE `GetHttpDNS`(
    IN Device VARCHAR(255),
    IN fromTime INT,
    IN toTime INT,
    IN IP VARCHAR(255),
    IN assetList TEXT
)
BEGIN
    SET @sqlQuery = CONCAT('
        SELECT 
            MEASURE_START_DATE DIV 30 * 30 AS time,
            SUM(
                CASE 
                    WHEN PROTOCOL IN (''TCP'', ''MODBUS'') AND MEASURE_TYPE = ''Count_of_HTTP_Request'' 
                    THEN MEASURE_VALUE
                    ELSE 0
                END
            ) AS "HTTP Requests",
            SUM(
                CASE 
                    WHEN PROTOCOL IN (''TCP'', ''MODBUS'') AND MEASURE_TYPE = ''Count_of_HTTP_Response'' 
                    THEN MEASURE_VALUE
                    ELSE 0
                END
            ) AS "HTTP Responses",
            SUM(
                CASE 
                    WHEN PROTOCOL = ''UDP'' AND MEASURE_TYPE = ''query_count'' 
                    THEN MEASURE_VALUE
                    ELSE 0
                END
            ) AS "DNS Queries",
            SUM(
                CASE 
                    WHEN PROTOCOL = ''UDP'' AND MEASURE_TYPE = ''count_of_response'' 
                    THEN MEASURE_VALUE
                    ELSE 0
                END
            ) AS "DNS Requests"
        FROM analytics.TELEMETRY_HEADER PARTITION (', Device, ') AS H
        INNER JOIN analytics.TELEMETRY PARTITION (', Device, ') AS T 
            ON H.TELEMETRY_GUID = T.TELEMETRY_GUID 
            AND MEASURE_TYPE IN (''Count_of_HTTP_Request'', ''Count_of_HTTP_Response'', ''query_count'', ''count_of_response'')
        WHERE MEASURE_START_DATE BETWEEN ', fromTime, ' AND ', toTime,
        IF(IP IS NOT NULL AND IP != '''', CONCAT(' AND SRC_ADDRESS = ''', IP, ''''), ''),
        IF(assetList IS NOT NULL AND assetList != '''', CONCAT(' AND (SRC_ETH_ADDRESS IN (', assetList, ') OR DST_ETH_ADDRESS IN (', assetList, '))'),''),
        ' GROUP BY time
        ORDER BY time;');

    PREPARE stmt FROM @sqlQuery;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;
END ;;
DELIMITER ;
/*!50003 SET sql_mode              = @saved_sql_mode */ ;
/*!50003 SET character_set_client  = @saved_cs_client */ ;
/*!50003 SET character_set_results = @saved_cs_results */ ;
/*!50003 SET collation_connection  = @saved_col_connection */ ;
/*!50003 DROP PROCEDURE IF EXISTS `GetITOTProtocols` */;
/*!50003 SET @saved_cs_client      = @@character_set_client */ ;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;
/*!50003 SET character_set_client  = latin1 */ ;
/*!50003 SET character_set_results = latin1 */ ;
/*!50003 SET collation_connection  = latin1_swedish_ci */ ;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;
/*!50003 SET sql_mode              = '' */ ;
DELIMITER ;;
CREATE DEFINER=`analytics_dev`@`%` PROCEDURE `GetITOTProtocols`(
    IN Device VARCHAR(255), 
    IN fromTime INT, 
    IN toTime INT
)
BEGIN
    SET @sql1 = CONCAT('
        SELECT
            SUM(CASE WHEN PROTOCOL = ''TCP'' AND COALESCE(P2.TCP_PROTOCOL_TAG, P1.TCP_PROTOCOL_TAG) = ''IT'' THEN 1
                     WHEN PROTOCOL = ''UDP'' AND COALESCE(P2.UDP_PROTOCOL_TAG, P1.UDP_PROTOCOL_TAG) = ''IT'' THEN 1
                     ELSE 0 END) AS "IT",
            SUM(CASE WHEN PROTOCOL = ''TCP'' AND COALESCE(P2.TCP_PROTOCOL_TAG, P1.TCP_PROTOCOL_TAG) = ''OT'' THEN 1
                     WHEN PROTOCOL = ''UDP'' AND COALESCE(P2.UDP_PROTOCOL_TAG, P1.UDP_PROTOCOL_TAG) = ''OT'' THEN 1
                     WHEN PROTOCOL = ''MODBUS'' THEN 1
                     ELSE 0 END) AS "OT",
            COUNT(*) AS Total
        FROM analytics.TELEMETRY_HEADER PARTITION (', Device, ') AS TH
        LEFT JOIN analytics.PORT AS P1 ON P1.PORT_NUMBER = TH.SRC_PORT
        LEFT JOIN analytics.PORT AS P2 ON P2.PORT_NUMBER = TH.DST_PORT
        WHERE MEASURE_START_DATE BETWEEN ', fromTime, ' AND ', toTime, ';');

    PREPARE stmt1 FROM @sql1;
    EXECUTE stmt1;
    DEALLOCATE PREPARE stmt1;

    SET @sql2 = CONCAT('
        SELECT *, COUNT(*) as count FROM (
            SELECT
            (CASE WHEN PROTOCOL = ''TCP'' THEN COALESCE(COALESCE(P1.TCP_SERVICE_NAME, P1.TCP_IND_STD_PORT_NAME), COALESCE(P2.TCP_SERVICE_NAME, P2.TCP_IND_STD_PORT_NAME), ''TCP'')
                WHEN PROTOCOL = ''UDP'' THEN COALESCE(COALESCE(P1.UDP_SERVICE_NAME, P1.UDP_IND_STD_PORT_NAME), COALESCE(P2.UDP_SERVICE_NAME, P2.UDP_IND_STD_PORT_NAME), ''UDP'')
                WHEN PROTOCOL = ''MODBUS'' THEN ''MODBUS''
                WHEN PROTOCOL = ''ICMP'' THEN ''ICMP''
                WHEN PROTOCOL = ''ARP'' THEN ''ARP''
                WHEN PROTOCOL = ''IP_NIP'' THEN ''Other'' END) AS Service,
            (CASE WHEN PROTOCOL = ''TCP'' AND (P1.TCP_PROTOCOL_TAG = ''OT'' AND P2.TCP_PROTOCOL_TAG = ''IT'') THEN ''OT''
                WHEN PROTOCOL = ''TCP'' AND (P1.TCP_PROTOCOL_TAG = ''IT'' AND P2.TCP_PROTOCOL_TAG = ''OT'') THEN ''IT''
                WHEN PROTOCOL = ''TCP'' THEN COALESCE(P1.TCP_PROTOCOL_TAG, P2.TCP_PROTOCOL_TAG)
                WHEN PROTOCOL = ''UDP'' AND (P1.UDP_PROTOCOL_TAG = ''OT'' AND P2.UDP_PROTOCOL_TAG = ''IT'') THEN ''OT''
                WHEN PROTOCOL = ''UDP'' AND (P1.UDP_PROTOCOL_TAG = ''IT'' AND P2.UDP_PROTOCOL_TAG = ''OT'') THEN ''IT''
                WHEN PROTOCOL = ''UDP'' THEN COALESCE(P1.UDP_PROTOCOL_TAG, P2.UDP_PROTOCOL_TAG)
                WHEN PROTOCOL = ''MODBUS'' THEN ''OT''
                ELSE ''Other'' END) AS Tag
            FROM analytics.TELEMETRY_HEADER PARTITION (', Device, ') AS TH
            LEFT JOIN analytics.PORT AS P1 ON P1.PORT_NUMBER = TH.SRC_PORT
            LEFT JOIN analytics.PORT AS P2 ON P2.PORT_NUMBER = TH.DST_PORT
            WHERE MEASURE_START_DATE BETWEEN ', fromTime, ' AND ', toTime, '
            UNION ALL
            SELECT
            (CASE WHEN PROTOCOL = ''IP_NIP'' AND MEASURE_TYPE = ''ip_proto'' AND MEASURE_VALUE = 255 THEN ''Non IP'' END) AS Service,
            ''Other'' AS Tag
            FROM analytics.TELEMETRY_HEADER PARTITION (', Device, ') AS TH
            INNER JOIN analytics.TELEMETRY PARTITION (', Device, ') ON TH.TELEMETRY_GUID = analytics.TELEMETRY.TELEMETRY_GUID  AND TH.PROTOCOL = ''IP_NIP'' AND analytics.TELEMETRY.MEASURE_TYPE = ''ip_proto'' AND MEASURE_DATE BETWEEN  ', fromTime, ' AND ', toTime, '
            LEFT JOIN analytics.PORT AS P1 ON P1.PORT_NUMBER = TH.SRC_PORT
            LEFT JOIN analytics.PORT AS P2 ON P2.PORT_NUMBER = TH.DST_PORT
            WHERE MEASURE_START_DATE BETWEEN ', fromTime, ' AND ', toTime, ' AND PROTOCOL = ''IP_NIP''
        ) AS Data
        WHERE Service IS NOT NULL AND Tag IS NOT NULL
        GROUP BY Service, Tag
        ORDER BY count DESC;');

    PREPARE stmt2 FROM @sql2;
    EXECUTE stmt2;
    DEALLOCATE PREPARE stmt2;
END ;;
DELIMITER ;
/*!50003 SET sql_mode              = @saved_sql_mode */ ;
/*!50003 SET character_set_client  = @saved_cs_client */ ;
/*!50003 SET character_set_results = @saved_cs_results */ ;
/*!50003 SET collation_connection  = @saved_col_connection */ ;
/*!50003 DROP PROCEDURE IF EXISTS `GetMulticastTraffic` */;
/*!50003 SET @saved_cs_client      = @@character_set_client */ ;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;
/*!50003 SET character_set_client  = latin1 */ ;
/*!50003 SET character_set_results = latin1 */ ;
/*!50003 SET collation_connection  = latin1_swedish_ci */ ;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;
/*!50003 SET sql_mode              = '' */ ;
DELIMITER ;;
CREATE DEFINER=`analytics_dev`@`%` PROCEDURE `GetMulticastTraffic`(
    IN Device VARCHAR(255),
    IN fromTime INT,
    IN toTime INT,
    IN assetList TEXT,
    IN pageSize INT,
    IN page INT
)
BEGIN
    DECLARE offsetParam INT;
    SET offsetParam = page * pageSize;

    SET @mainQuery = CONCAT('
        SELECT
            TH.TELEMETRY_GUID AS ID,
            TH.SRC_ADDRESS AS Source_IP,
            TH.DST_ADDRESS AS Destination_IP,
            COALESCE((CASE 
                WHEN TH.PROTOCOL = ''TCP'' THEN COALESCE(P2.TCP_SERVICE_NAME, P1.TCP_SERVICE_NAME, ''TCP'')
                WHEN TH.PROTOCOL = ''UDP'' THEN COALESCE(P2.UDP_SERVICE_NAME, P1.UDP_SERVICE_NAME, ''UDP'')
                WHEN TH.PROTOCOL = ''MODBUS'' THEN ''MODBUS''
                WHEN TH.PROTOCOL = ''ICMP'' THEN ''ICMP''
                WHEN TH.PROTOCOL = ''ARP'' THEN ''ARP''
                WHEN TH.PROTOCOL = ''IP_NIP'' AND analytics.TELEMETRY.MEASURE_TYPE = ''ip_proto'' AND analytics.TELEMETRY.MEASURE_VALUE = 255 THEN ''Non IP''
                ELSE NULL END), ''Other'') AS Service,
            CAST(SUBSTRING_INDEX(TH.SRC_PORT, ''.'', 1) AS UNSIGNED) AS Source_Port,
            CAST(SUBSTRING_INDEX(TH.DST_PORT, ''.'', 1) AS UNSIGNED) AS Destination_Port,
            TH.SRC_ETH_ADDRESS AS Source_MAC,
            TH.DST_ETH_ADDRESS AS Destination_MAC,
            TH.PROTOCOL AS PROTOCOL,
            CONCAT(CEIL((TH.MEASURE_END_DATE - TH.MEASURE_START_DATE)), '' s'') AS Duration,
            TH.MEASURE_START_DATE AS Time
        FROM
            analytics.TELEMETRY_HEADER PARTITION (', Device, ') AS TH
        LEFT JOIN analytics.TELEMETRY PARTITION (', Device, ') ON
            TH.TELEMETRY_GUID = analytics.TELEMETRY.TELEMETRY_GUID AND
            analytics.TELEMETRY.MEASURE_TYPE = ''ip_proto'' AND
            analytics.TELEMETRY.MEASURE_VALUE = 255 
        LEFT JOIN analytics.PORT AS P1 ON
            P1.PORT_NUMBER = TH.SRC_PORT
        LEFT JOIN analytics.PORT AS P2 ON
            P2.PORT_NUMBER = TH.DST_PORT
        WHERE
            TH.MEASURE_START_DATE BETWEEN ', fromTime, ' AND ', toTime, IF(assetList IS NOT NULL AND assetList != '''', CONCAT(' AND (SRC_ETH_ADDRESS IN (', assetList, ') OR DST_ETH_ADDRESS IN (', assetList, '))'),''), '
            AND (TH.PROTOCOL IN (''UDP'', ''IP_NIP'') AND LEFT(REPLACE(TH.DST_ETH_ADDRESS, '':'', ''''), 6) = ''01005E'' AND
            CONV(SUBSTRING_INDEX(TH.DST_ADDRESS, ''.'', 1), 10, 2) LIKE ''1110%'')
        LIMIT ', pageSize, ' OFFSET ', offsetParam, ';');

    SET @countQuery = CONCAT('
        SELECT COUNT(*) AS count
        FROM
            analytics.TELEMETRY_HEADER PARTITION (', Device, ') AS TH
        WHERE
            TH.MEASURE_START_DATE BETWEEN ', fromTime, ' AND ', toTime,IF(assetList IS NOT NULL AND assetList != '''', CONCAT(' AND (SRC_ETH_ADDRESS IN (', assetList, ') OR DST_ETH_ADDRESS IN (', assetList, '))'),''), ' AND
            (TH.PROTOCOL IN (''UDP'', ''IP_NIP'') AND
             LEFT(REPLACE(TH.DST_ETH_ADDRESS, '':'', ''''), 6) = ''01005E'' AND
             CONV(SUBSTRING_INDEX(TH.DST_ADDRESS, ''.'', 1), 10, 2) LIKE ''1110%'');');

    PREPARE mainStmt FROM @mainQuery;
    EXECUTE mainStmt;
    DEALLOCATE PREPARE mainStmt;
    
    PREPARE countStmt FROM @countQuery;
    EXECUTE countStmt;
    DEALLOCATE PREPARE countStmt;
END ;;
DELIMITER ;
/*!50003 SET sql_mode              = @saved_sql_mode */ ;
/*!50003 SET character_set_client  = @saved_cs_client */ ;
/*!50003 SET character_set_results = @saved_cs_results */ ;
/*!50003 SET collation_connection  = @saved_col_connection */ ;
/*!50003 DROP PROCEDURE IF EXISTS `GetNetworkConversation` */;
/*!50003 SET @saved_cs_client      = @@character_set_client */ ;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;
/*!50003 SET character_set_client  = latin1 */ ;
/*!50003 SET character_set_results = latin1 */ ;
/*!50003 SET collation_connection  = latin1_swedish_ci */ ;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;
/*!50003 SET sql_mode              = '' */ ;
DELIMITER ;;
CREATE DEFINER=`analytics_dev`@`%` PROCEDURE `GetNetworkConversation`(
    IN Device VARCHAR(255),
    IN fromTime INT,
    IN toTime INT,
    IN IP VARCHAR(255),
    IN assetList TEXT
)
BEGIN
    SET @sqlQuery = CONCAT('
        SELECT 
            MEASURE_START_DATE DIV 30 * 30 AS time,
            SUM(
                CASE 
                    WHEN PROTOCOL = ''TCP'' THEN 1
                    ELSE 0
                END
            ) AS "TCP Flows",
            SUM(
                CASE 
                    WHEN PROTOCOL = ''UDP'' THEN 1
                    ELSE 0
                END
            ) AS "UDP Flows"
        FROM analytics.TELEMETRY_HEADER PARTITION (', Device, ') AS H
        WHERE MEASURE_START_DATE BETWEEN ', fromTime, ' AND ', toTime,
        IF(IP IS NOT NULL AND IP != '''', CONCAT(' AND SRC_ADDRESS = ''', IP, ''''), ''),
        IF(assetList IS NOT NULL AND assetList != '''', CONCAT(' AND (SRC_ETH_ADDRESS IN (', assetList, ') OR DST_ETH_ADDRESS IN (', assetList, '))'),''),
        ' GROUP BY time
        ORDER BY time;');

    PREPARE stmt FROM @sqlQuery;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;
END ;;
DELIMITER ;
/*!50003 SET sql_mode              = @saved_sql_mode */ ;
/*!50003 SET character_set_client  = @saved_cs_client */ ;
/*!50003 SET character_set_results = @saved_cs_results */ ;
/*!50003 SET collation_connection  = @saved_col_connection */ ;
/*!50003 DROP PROCEDURE IF EXISTS `GetNetworkDelay` */;
/*!50003 SET @saved_cs_client      = @@character_set_client */ ;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;
/*!50003 SET character_set_client  = utf8mb4 */ ;
/*!50003 SET character_set_results = utf8mb4 */ ;
/*!50003 SET collation_connection  = utf8mb4_0900_ai_ci */ ;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;
/*!50003 SET sql_mode              = '' */ ;
DELIMITER ;;
CREATE DEFINER=`analytics_dev`@`%` PROCEDURE `GetNetworkDelay`(
    IN Device VARCHAR(255),
    IN fromTime INT,
    IN toTime INT,
    IN IP VARCHAR(255)
)
BEGIN
    SET @sqlQuery = CONCAT('
        SELECT 
            MEASURE_DATE DIV 30 * 30 AS time,
            COALESCE(AVG(MEASURE_VALUE), 0 ) AS "Network Delay"
        FROM analytics.TELEMETRY PARTITION (', Device, ') AS T
INNER JOIN analytics.TELEMETRY_HEADER PARTITION (', Device, ') AS H ON H.TELEMETRY_GUID = T.TELEMETRY_GUID  AND MEASURE_TYPE = ''network_delay'' 
        WHERE MEASURE_DATE BETWEEN ', fromTime, ' AND ', toTime,
        IF(IP IS NOT NULL AND IP != '''', CONCAT(' AND SRC_ADDRESS = ''', IP, ''''), ''),
        ' GROUP BY time
        -- ORDER BY time
        ;');

    PREPARE stmt FROM @sqlQuery;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;
END ;;
DELIMITER ;
/*!50003 SET sql_mode              = @saved_sql_mode */ ;
/*!50003 SET character_set_client  = @saved_cs_client */ ;
/*!50003 SET character_set_results = @saved_cs_results */ ;
/*!50003 SET collation_connection  = @saved_col_connection */ ;
/*!50003 DROP PROCEDURE IF EXISTS `GetNetworkErrors` */;
/*!50003 SET @saved_cs_client      = @@character_set_client */ ;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;
/*!50003 SET character_set_client  = latin1 */ ;
/*!50003 SET character_set_results = latin1 */ ;
/*!50003 SET collation_connection  = latin1_swedish_ci */ ;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;
/*!50003 SET sql_mode              = '' */ ;
DELIMITER ;;
CREATE DEFINER=`analytics_dev`@`%` PROCEDURE `GetNetworkErrors`(
    IN Device VARCHAR(255), 
    IN fromTime INT, 
    IN toTime INT,
    IN assetList TEXT
)
BEGIN
    SET @sql = CONCAT('
        SELECT COUNT(*) as "Network Errors"
        FROM analytics.ANOMALY_MEASURE_HEADER PARTITION (', Device, ') AS H
        INNER JOIN analytics.ANOMALY AS A ON H.ANOMALY_ID=A.ANOMALY_ID AND A.ERROR_TAG = 1
        WHERE H.ANOMALY_START_DATE BETWEEN ', fromTime, ' AND ', toTime, IF(assetList IS NOT NULL AND assetList != '''', CONCAT(' AND (SRC_ETH_ADDRESS IN (', assetList, ') OR DST_ETH_ADDRESS IN (', assetList, '))'),''), '
        AND H.ANOMALY_ID != "ML"');

    PREPARE stmt FROM @sql;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;
END ;;
DELIMITER ;
/*!50003 SET sql_mode              = @saved_sql_mode */ ;
/*!50003 SET character_set_client  = @saved_cs_client */ ;
/*!50003 SET character_set_results = @saved_cs_results */ ;
/*!50003 SET collation_connection  = @saved_col_connection */ ;
/*!50003 DROP PROCEDURE IF EXISTS `GetOneMetricBreakdown` */;
/*!50003 SET @saved_cs_client      = @@character_set_client */ ;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;
/*!50003 SET character_set_client  = latin1 */ ;
/*!50003 SET character_set_results = latin1 */ ;
/*!50003 SET collation_connection  = latin1_swedish_ci */ ;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;
/*!50003 SET sql_mode              = '' */ ;
DELIMITER ;;
CREATE DEFINER=`analytics_dev`@`%` PROCEDURE `GetOneMetricBreakdown`(
    IN fromTime INT,
    IN toTime INT,
    IN partitionName VARCHAR(20)
)
BEGIN
SET
    @sqlQuery = CONCAT(
        '
SELECT 
    (AVG(AVAILABILITY_FACTOR * CAPACITY_FACTOR * QUALITY_FACTOR) ) * 100 AS overall,
    SUBSTRING_INDEX(ASSET_ID, ".", 3) AS subnet
  FROM 
(
SELECT 
  P2.asset_id AS ASSET_ID, 
  P2.port_number AS PORT_NUMBER, 
  P2.measure_date AS MEASURE_DATE, 
                    COALESCE(
          (
          (
          case
                    when (P2.OPERATION_STATUS = ''up'') then (P2.MEASURE_DATE - P2.MEASURE_DATE_PREV)
                    else 0
          end
          ) / case
                    when (P2.ADMIN_STATUS = ''up'') then (P2.MEASURE_DATE - P2.MEASURE_DATE_PREV)
                    else NULL
          end
          ),
          NULL
          ) AS AVAILABILITY_FACTOR,
            CASE WHEN P2.ADMIN_STATUS <> ''up'' then NULL ELSE
          (
          1 - abs(
          COALESCE(
          (
          ((P2.RECEIVED_BPS - CASE WHEN P2.RECEIVED_BPS_PREV > P2.RECEIVED_BPS THEN 0 ELSE P2.RECEIVED_BPS_PREV END ) * 8) / (
          P2.NEGOTIATED_BPS * (P2.MEASURE_DATE - P2.MEASURE_DATE_PREV)
          )
          ),
          0
          )
          )
          ) END AS CAPACITY_FACTOR,
          CASE WHEN P2.ADMIN_STATUS <> ''up'' then NULL ELSE
          (
          COALESCE(
          (
          (
          (
          (
          P2.TOTAL_PACKETS_RECEIVED - CASE WHEN P2.TOTAL_PACKETS_RECEIVED_PREV > P2.TOTAL_PACKETS_RECEIVED THEN 0 ELSE P2.TOTAL_PACKETS_RECEIVED_PREV END
          ) - (P2.CRC_ERROR_COUNT - CASE WHEN P2.CRC_ERROR_COUNT_PREV > P2.CRC_ERROR_COUNT THEN 0 ELSE P2.CRC_ERROR_COUNT_PREV END)
          ) - (
          P2.COLLISION_ERROR_COUNT - CASE WHEN P2.COLLISION_ERROR_COUNT_PREV > P2.COLLISION_ERROR_COUNT THEN 0 ELSE P2.COLLISION_ERROR_COUNT_PREV END
          )
          ) / (
          P2.TOTAL_PACKETS_RECEIVED - CASE WHEN P2.TOTAL_PACKETS_RECEIVED_PREV > P2.TOTAL_PACKETS_RECEIVED THEN 0 ELSE P2.TOTAL_PACKETS_RECEIVED_PREV END
          )
          ),
          1
          )) END AS QUALITY_FACTOR
FROM 
  (
    SELECT 
      P1.asset_id AS ASSET_ID, 
      P1.port_number AS PORT_NUMBER, 
      P1.operation_status AS OPERATION_STATUS, 
      P1.ADMIN_STATUS AS ADMIN_STATUS,
                    coalesce(
                    lag (P1.OPERATION_STATUS) OVER (
                    PARTITION BY P1.ASSET_ID,
                              P1.PORT_NUMBER
                    ORDER BY  P1.MEASURE_DATE
                    ),
                    P1.OPERATION_STATUS
                    ) AS OPERATION_STATUS_PREV,
                    P1.MEASURE_DATE AS MEASURE_DATE,
                    coalesce(
                    lag (P1.MEASURE_DATE) OVER (
                    PARTITION BY P1.ASSET_ID,
                              P1.PORT_NUMBER
                    ORDER BY  P1.MEASURE_DATE
                    ),
                    P1.MEASURE_DATE
                    ) AS MEASURE_DATE_PREV,
                    coalesce(
                    lead (P1.MEASURE_DATE) OVER (
                    PARTITION BY P1.ASSET_ID,
                              P1.PORT_NUMBER
                    ORDER BY  P1.MEASURE_DATE
                    ),
                    P1.MEASURE_DATE
                    ) AS MEASURE_DATE_FUTURE,
                    P1.NEGOTIATED_BPS AS NEGOTIATED_BPS,
                    coalesce(
                    lag (P1.NEGOTIATED_BPS) OVER (
                    PARTITION BY P1.ASSET_ID,
                              P1.PORT_NUMBER
                    ORDER BY  P1.MEASURE_DATE
                    ),
                    P1.NEGOTIATED_BPS
                    ) AS NEGOTIATED_BPS_PREV,
                    P1.RECEIVED_BPS AS RECEIVED_BPS,
                    coalesce(
                    lag (P1.RECEIVED_BPS) OVER (
                    PARTITION BY P1.ASSET_ID,
                              P1.PORT_NUMBER
                    ORDER BY  P1.MEASURE_DATE
                    ),
                    P1.RECEIVED_BPS
                    ) AS RECEIVED_BPS_PREV,
                    P1.CRC_ERROR_COUNT AS CRC_ERROR_COUNT,
                    coalesce(
                    lag (P1.CRC_ERROR_COUNT) OVER (
                    PARTITION BY P1.ASSET_ID,
                              P1.PORT_NUMBER
                    ORDER BY  P1.MEASURE_DATE
                    ),
                    P1.CRC_ERROR_COUNT
                    ) AS CRC_ERROR_COUNT_PREV,
                    P1.COLLISION_ERROR_COUNT AS COLLISION_ERROR_COUNT,
                    coalesce(
                    lag (P1.COLLISION_ERROR_COUNT) OVER (
                    PARTITION BY P1.ASSET_ID,
                              P1.PORT_NUMBER
                    ORDER BY  P1.MEASURE_DATE
                    ),
                    P1.COLLISION_ERROR_COUNT
                    ) AS COLLISION_ERROR_COUNT_PREV,
                    P1.TOTAL_PACKETS_RECEIVED AS TOTAL_PACKETS_RECEIVED,
                    coalesce(
                    lag (P1.TOTAL_PACKETS_RECEIVED) OVER (
                    PARTITION BY P1.ASSET_ID,
                              P1.PORT_NUMBER
                    ORDER BY  P1.MEASURE_DATE
                    ),
                    P1.TOTAL_PACKETS_RECEIVED
                    ) AS TOTAL_PACKETS_RECEIVED_PREV,
    ROW_NUMBER() OVER (PARTITION BY ASSET_ID , PORT_NUMBER ORDER BY MEASURE_DATE DESC) AS row_num
    FROM 
      (
        SELECT 
          asset_id AS ASSET_ID, 
          port_number AS PORT_NUMBER, 
          measure_date AS MEASURE_DATE, 
                              max(
                              (
                              case
                                        when (
                                        analytics.ACTIVE_TELEMETRY.MEASURE_TYPE = ''operation_status''
                                        ) then analytics.ACTIVE_TELEMETRY.MEASURE_STRING
                                        else ''''
                              end
                              )
                              ) AS OPERATION_STATUS,
                              max(
                              (
                              case
                                        when (
                                        analytics.ACTIVE_TELEMETRY.MEASURE_TYPE = ''negotiated_bps''
                                        ) then analytics.ACTIVE_TELEMETRY.MEASURE_VALUE
                                        else 0
                              end
                              )
                              ) AS NEGOTIATED_BPS,
                              max(
                              (
                              case
                                        when (
                                        analytics.ACTIVE_TELEMETRY.MEASURE_TYPE = ''received_bps''
                                        ) then analytics.ACTIVE_TELEMETRY.MEASURE_VALUE
                                        else 0
                              end
                              )
                              ) AS RECEIVED_BPS,
                              max(
                              (
                              case
                                        when (
                                        analytics.ACTIVE_TELEMETRY.MEASURE_TYPE = ''crc_error_count''
                                        ) then analytics.ACTIVE_TELEMETRY.MEASURE_VALUE
                                        else 0
                              end
                              )
                              ) AS CRC_ERROR_COUNT,
                              max(
                              (
                              case
                                        when (
                                        analytics.ACTIVE_TELEMETRY.MEASURE_TYPE = ''collision_error_count''
                                        ) then analytics.ACTIVE_TELEMETRY.MEASURE_VALUE
                                        else 0
                              end
                              )
                              ) AS COLLISION_ERROR_COUNT,
                              max(
                              (
                              case
                                        when (
                                        analytics.ACTIVE_TELEMETRY.MEASURE_TYPE = ''total_packets_recieved''
                                        ) then analytics.ACTIVE_TELEMETRY.MEASURE_VALUE
                                        else 0
                              end
                              )
                              ) AS TOTAL_PACKETS_RECEIVED,
                              max(
                              (
                              case
                                        when (
                                        analytics.ACTIVE_TELEMETRY.MEASURE_TYPE = ''admin_status''
                                        ) then analytics.ACTIVE_TELEMETRY.MEASURE_STRING
                                        else ''''
                              end
                              )
                              ) AS ADMIN_STATUS
        FROM 
          analytics.ACTIVE_TELEMETRY PARTITION (',
        partitionName,
        ')
    WHERE MEASURE_DATE BETWEEN ',
        fromTime,
        ' AND ',
        toTime,
        '   
        GROUP BY 
          asset_id, 
          port_number, 
          measure_date
      ) P1
      WHERE ADMIN_STATUS = ''up''
  ) P2
  WHERE row_num=1
  ) as P3 '
    );

PREPARE stmt
FROM
    @sqlQuery;

EXECUTE stmt;

DEALLOCATE PREPARE stmt;

END ;;
DELIMITER ;
/*!50003 SET sql_mode              = @saved_sql_mode */ ;
/*!50003 SET character_set_client  = @saved_cs_client */ ;
/*!50003 SET character_set_results = @saved_cs_results */ ;
/*!50003 SET collation_connection  = @saved_col_connection */ ;
/*!50003 DROP PROCEDURE IF EXISTS `GetOneMetricBreakdownForPort` */;
/*!50003 SET @saved_cs_client      = @@character_set_client */ ;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;
/*!50003 SET character_set_client  = latin1 */ ;
/*!50003 SET character_set_results = latin1 */ ;
/*!50003 SET collation_connection  = latin1_swedish_ci */ ;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;
/*!50003 SET sql_mode              = '' */ ;
DELIMITER ;;
CREATE DEFINER=`analytics_dev`@`%` PROCEDURE `GetOneMetricBreakdownForPort`(
    IN fromTime INT,
    IN toTime INT,
    IN portNumber VARCHAR(255),
    IN assetId VARCHAR(255),
    IN partitionName VARCHAR(20)
)
BEGIN
SET
    @sqlQuery = CONCAT(
        '
SELECT overall,
  switch,
   port FROM (
select
          (
          (
          case
                    when (P2.OPERATION_STATUS = ''up'') then (P2.MEASURE_DATE - P2.MEASURE_DATE_PREV)
                    else 0
          end
          ) / case
                    when (P2.ADMIN_STATUS = ''up'') then (P2.MEASURE_DATE - P2.MEASURE_DATE_PREV)
                    else NULL
          end
          )  *
          CASE WHEN P2.ADMIN_STATUS <> ''up'' then NULL ELSE
          (
          1 - abs(
          COALESCE(
          (
          ((P2.RECEIVED_BPS - CASE WHEN P2.RECEIVED_BPS_PREV > P2.RECEIVED_BPS THEN 0 ELSE P2.RECEIVED_BPS_PREV END ) * 8) / (
          P2.NEGOTIATED_BPS * (P2.MEASURE_DATE - P2.MEASURE_DATE_PREV)
          )
          ),
          0
          )
          )
          ) END  *
          CASE WHEN P2.ADMIN_STATUS <> ''up'' then NULL ELSE
          (
          COALESCE(
          (
          (
          (
          (
          P2.TOTAL_PACKETS_RECEIVED - CASE WHEN P2.TOTAL_PACKETS_RECEIVED_PREV > P2.TOTAL_PACKETS_RECEIVED THEN 0 ELSE P2.TOTAL_PACKETS_RECEIVED_PREV END
          ) - (P2.CRC_ERROR_COUNT - CASE WHEN P2.CRC_ERROR_COUNT_PREV > P2.CRC_ERROR_COUNT THEN 0 ELSE P2.CRC_ERROR_COUNT_PREV END)
          ) - (
          P2.COLLISION_ERROR_COUNT - CASE WHEN P2.COLLISION_ERROR_COUNT_PREV > P2.COLLISION_ERROR_COUNT THEN 0 ELSE P2.COLLISION_ERROR_COUNT_PREV END
          )
          ) / (
          P2.TOTAL_PACKETS_RECEIVED - CASE WHEN P2.TOTAL_PACKETS_RECEIVED_PREV > P2.TOTAL_PACKETS_RECEIVED THEN 0 ELSE P2.TOTAL_PACKETS_RECEIVED_PREV END
          )
          ),
          1
          )) END * 100 overall,
  P2.ASSET_ID AS switch,
  P2.PORT_NUMBER AS port,
  ROW_NUMBER() OVER( ORDER BY MEASURE_DATE DESC ) as row_num
from
  (
    select
      P1.ASSET_ID AS ASSET_ID,
      P1.PORT_NUMBER AS PORT_NUMBER,
      P1.OPERATION_STATUS AS OPERATION_STATUS,
      P1.ADMIN_STATUS AS ADMIN_STATUS,
                    coalesce(
                    lag (P1.OPERATION_STATUS) OVER (
                    PARTITION BY P1.ASSET_ID,
                              P1.PORT_NUMBER
                    ORDER BY  P1.MEASURE_DATE
                    ),
                    P1.OPERATION_STATUS
                    ) AS OPERATION_STATUS_PREV,
                    P1.MEASURE_DATE AS MEASURE_DATE,
                    coalesce(
                    lag (P1.MEASURE_DATE) OVER (
                    PARTITION BY P1.ASSET_ID,
                              P1.PORT_NUMBER
                    ORDER BY  P1.MEASURE_DATE
                    ),
                    P1.MEASURE_DATE
                    ) AS MEASURE_DATE_PREV,
                    coalesce(
                    lead (P1.MEASURE_DATE) OVER (
                    PARTITION BY P1.ASSET_ID,
                              P1.PORT_NUMBER
                    ORDER BY  P1.MEASURE_DATE
                    ),
                    P1.MEASURE_DATE
                    ) AS MEASURE_DATE_FUTURE,
                    P1.NEGOTIATED_BPS AS NEGOTIATED_BPS,
                    coalesce(
                    lag (P1.NEGOTIATED_BPS) OVER (
                    PARTITION BY P1.ASSET_ID,
                              P1.PORT_NUMBER
                    ORDER BY  P1.MEASURE_DATE
                    ),
                    P1.NEGOTIATED_BPS
                    ) AS NEGOTIATED_BPS_PREV,
                    P1.RECEIVED_BPS AS RECEIVED_BPS,
                    coalesce(
                    lag (P1.RECEIVED_BPS) OVER (
                    PARTITION BY P1.ASSET_ID,
                              P1.PORT_NUMBER
                    ORDER BY  P1.MEASURE_DATE
                    ),
                    P1.RECEIVED_BPS
                    ) AS RECEIVED_BPS_PREV,
                    P1.CRC_ERROR_COUNT AS CRC_ERROR_COUNT,
                    coalesce(
                    lag (P1.CRC_ERROR_COUNT) OVER (
                    PARTITION BY P1.ASSET_ID,
                              P1.PORT_NUMBER
                    ORDER BY  P1.MEASURE_DATE
                    ),
                    P1.CRC_ERROR_COUNT
                    ) AS CRC_ERROR_COUNT_PREV,
                    P1.COLLISION_ERROR_COUNT AS COLLISION_ERROR_COUNT,
                    coalesce(
                    lag (P1.COLLISION_ERROR_COUNT) OVER (
                    PARTITION BY P1.ASSET_ID,
                              P1.PORT_NUMBER
                    ORDER BY  P1.MEASURE_DATE
                    ),
                    P1.COLLISION_ERROR_COUNT
                    ) AS COLLISION_ERROR_COUNT_PREV,
                    P1.TOTAL_PACKETS_RECEIVED AS TOTAL_PACKETS_RECEIVED,
                    coalesce(
                    lag (P1.TOTAL_PACKETS_RECEIVED) OVER (
                    PARTITION BY P1.ASSET_ID,
                              P1.PORT_NUMBER
                    ORDER BY  P1.MEASURE_DATE
                    ),
                    P1.TOTAL_PACKETS_RECEIVED
                    ) AS TOTAL_PACKETS_RECEIVED_PREV
    from
      (
        select
          analytics.ACTIVE_TELEMETRY.ASSET_ID AS ASSET_ID,
          analytics.ACTIVE_TELEMETRY.PORT_NUMBER AS PORT_NUMBER,
          analytics.ACTIVE_TELEMETRY.MEASURE_DATE AS MEASURE_DATE,
                              max(
                              (
                              case
                                        when (
                                        analytics.ACTIVE_TELEMETRY.MEASURE_TYPE = ''operation_status''
                                        ) then analytics.ACTIVE_TELEMETRY.MEASURE_STRING
                                        else ''''
                              end
                              )
                              ) AS OPERATION_STATUS,
                              max(
                              (
                              case
                                        when (
                                        analytics.ACTIVE_TELEMETRY.MEASURE_TYPE = ''negotiated_bps''
                                        ) then analytics.ACTIVE_TELEMETRY.MEASURE_VALUE
                                        else 0
                              end
                              )
                              ) AS NEGOTIATED_BPS,
                              max(
                              (
                              case
                                        when (
                                        analytics.ACTIVE_TELEMETRY.MEASURE_TYPE = ''received_bps''
                                        ) then analytics.ACTIVE_TELEMETRY.MEASURE_VALUE
                                        else 0
                              end
                              )
                              ) AS RECEIVED_BPS,
                              max(
                              (
                              case
                                        when (
                                        analytics.ACTIVE_TELEMETRY.MEASURE_TYPE = ''crc_error_count''
                                        ) then analytics.ACTIVE_TELEMETRY.MEASURE_VALUE
                                        else 0
                              end
                              )
                              ) AS CRC_ERROR_COUNT,
                              max(
                              (
                              case
                                        when (
                                        analytics.ACTIVE_TELEMETRY.MEASURE_TYPE = ''collision_error_count''
                                        ) then analytics.ACTIVE_TELEMETRY.MEASURE_VALUE
                                        else 0
                              end
                              )
                              ) AS COLLISION_ERROR_COUNT,
                              max(
                              (
                              case
                                        when (
                                        analytics.ACTIVE_TELEMETRY.MEASURE_TYPE = ''total_packets_recieved''
                                        ) then analytics.ACTIVE_TELEMETRY.MEASURE_VALUE
                                        else 0
                              end
                              )
                              ) AS TOTAL_PACKETS_RECEIVED,
                              max(
                              (
                              case
                                        when (
                                        analytics.ACTIVE_TELEMETRY.MEASURE_TYPE = ''admin_status''
                                        ) then analytics.ACTIVE_TELEMETRY.MEASURE_STRING
                                        else ''''
                              end
                              )
                              ) AS ADMIN_STATUS
        from
          analytics.ACTIVE_TELEMETRY PARTITION (',
        partitionName,
        ' )
where 
MEASURE_DATE BETWEEN ',
        fromTime,
        ' AND ',
        toTime,
        '
      AND PORT_NUMBER = ',
        portNumber,
        '
      AND ASSET_ID = ''',
        assetId,
        '''
        group by
          analytics.ACTIVE_TELEMETRY.ASSET_ID,
          analytics.ACTIVE_TELEMETRY.PORT_NUMBER,
          analytics.ACTIVE_TELEMETRY.MEASURE_DATE
      ) P1 
      WHERE ADMIN_STATUS = ''up''
  ) P2
  ) P3
  WHERE row_num = 1
  '
    );

PREPARE stmt FROM @sqlQuery;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;
END ;;
DELIMITER ;
/*!50003 SET sql_mode              = @saved_sql_mode */ ;
/*!50003 SET character_set_client  = @saved_cs_client */ ;
/*!50003 SET character_set_results = @saved_cs_results */ ;
/*!50003 SET collation_connection  = @saved_col_connection */ ;
/*!50003 DROP PROCEDURE IF EXISTS `GetOneMetricBreakdownForSubnet` */;
/*!50003 SET @saved_cs_client      = @@character_set_client */ ;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;
/*!50003 SET character_set_client  = latin1 */ ;
/*!50003 SET character_set_results = latin1 */ ;
/*!50003 SET collation_connection  = latin1_swedish_ci */ ;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;
/*!50003 SET sql_mode              = '' */ ;
DELIMITER ;;
CREATE DEFINER=`analytics_dev`@`%` PROCEDURE `GetOneMetricBreakdownForSubnet`(
    IN fromTime INT,
    IN toTime INT,
    IN node VARCHAR(255),
    IN partitionName VARCHAR(20)
)
BEGIN
SET
    @sqlQuery = CONCAT(
        '
SELECT 
    (AVG(AVAILABILITY_FACTOR * CAPACITY_FACTOR * QUALITY_FACTOR)) * 100 AS overall,
    ASSET_ID AS switch
FROM
(
select    P2.ASSET_ID AS ASSET_ID,
          P2.PORT_NUMBER AS PORT_NUMBER,
          P2.MEASURE_DATE AS MEASURE_DATE,
                    COALESCE(
          (
          (
          case
                    when (P2.OPERATION_STATUS = ''up'') then (P2.MEASURE_DATE - P2.MEASURE_DATE_PREV)
                    else 0
          end
          ) / case
                    when (P2.ADMIN_STATUS = ''up'') then (P2.MEASURE_DATE - P2.MEASURE_DATE_PREV)
                    else NULL
          end
          ),
          NULL
          ) AS AVAILABILITY_FACTOR,
            CASE WHEN P2.ADMIN_STATUS <> ''up'' then NULL ELSE
          (
          1 - abs(
          COALESCE(
          (
          ((P2.RECEIVED_BPS - CASE WHEN P2.RECEIVED_BPS_PREV > P2.RECEIVED_BPS THEN 0 ELSE P2.RECEIVED_BPS_PREV END ) * 8) / (
          P2.NEGOTIATED_BPS * (P2.MEASURE_DATE - P2.MEASURE_DATE_PREV)
          )
          ),
          0
          )
          )
          ) END AS CAPACITY_FACTOR,
          CASE WHEN P2.ADMIN_STATUS <> ''up'' then NULL ELSE
          (
          COALESCE(
          (
          (
          (
          (
          P2.TOTAL_PACKETS_RECEIVED - CASE WHEN P2.TOTAL_PACKETS_RECEIVED_PREV > P2.TOTAL_PACKETS_RECEIVED THEN 0 ELSE P2.TOTAL_PACKETS_RECEIVED_PREV END
          ) - (P2.CRC_ERROR_COUNT - CASE WHEN P2.CRC_ERROR_COUNT_PREV > P2.CRC_ERROR_COUNT THEN 0 ELSE P2.CRC_ERROR_COUNT_PREV END)
          ) - (
          P2.COLLISION_ERROR_COUNT - CASE WHEN P2.COLLISION_ERROR_COUNT_PREV > P2.COLLISION_ERROR_COUNT THEN 0 ELSE P2.COLLISION_ERROR_COUNT_PREV END
          )
          ) / (
          P2.TOTAL_PACKETS_RECEIVED - CASE WHEN P2.TOTAL_PACKETS_RECEIVED_PREV > P2.TOTAL_PACKETS_RECEIVED THEN 0 ELSE P2.TOTAL_PACKETS_RECEIVED_PREV END
          )
          ),
          1
          )) END AS QUALITY_FACTOR
from
  (
    select
      P1.ASSET_ID AS ASSET_ID,
      P1.PORT_NUMBER AS PORT_NUMBER,
      P1.OPERATION_STATUS AS OPERATION_STATUS,
      P1.ADMIN_STATUS AS ADMIN_STATUS,
                    coalesce(
                    lag (P1.OPERATION_STATUS) OVER (
                    PARTITION BY P1.ASSET_ID,
                              P1.PORT_NUMBER
                    ORDER BY  P1.MEASURE_DATE
                    ),
                    P1.OPERATION_STATUS
                    ) AS OPERATION_STATUS_PREV,
                    P1.MEASURE_DATE AS MEASURE_DATE,
                    coalesce(
                    lag (P1.MEASURE_DATE) OVER (
                    PARTITION BY P1.ASSET_ID,
                              P1.PORT_NUMBER
                    ORDER BY  P1.MEASURE_DATE
                    ),
                    P1.MEASURE_DATE
                    ) AS MEASURE_DATE_PREV,
                    coalesce(
                    lead (P1.MEASURE_DATE) OVER (
                    PARTITION BY P1.ASSET_ID,
                              P1.PORT_NUMBER
                    ORDER BY  P1.MEASURE_DATE
                    ),
                    P1.MEASURE_DATE
                    ) AS MEASURE_DATE_FUTURE,
                    P1.NEGOTIATED_BPS AS NEGOTIATED_BPS,
                    coalesce(
                    lag (P1.NEGOTIATED_BPS) OVER (
                    PARTITION BY P1.ASSET_ID,
                              P1.PORT_NUMBER
                    ORDER BY  P1.MEASURE_DATE
                    ),
                    P1.NEGOTIATED_BPS
                    ) AS NEGOTIATED_BPS_PREV,
                    P1.RECEIVED_BPS AS RECEIVED_BPS,
                    coalesce(
                    lag (P1.RECEIVED_BPS) OVER (
                    PARTITION BY P1.ASSET_ID,
                              P1.PORT_NUMBER
                    ORDER BY  P1.MEASURE_DATE
                    ),
                    P1.RECEIVED_BPS
                    ) AS RECEIVED_BPS_PREV,
                    P1.CRC_ERROR_COUNT AS CRC_ERROR_COUNT,
                    coalesce(
                    lag (P1.CRC_ERROR_COUNT) OVER (
                    PARTITION BY P1.ASSET_ID,
                              P1.PORT_NUMBER
                    ORDER BY  P1.MEASURE_DATE
                    ),
                    P1.CRC_ERROR_COUNT
                    ) AS CRC_ERROR_COUNT_PREV,
                    P1.COLLISION_ERROR_COUNT AS COLLISION_ERROR_COUNT,
                    coalesce(
                    lag (P1.COLLISION_ERROR_COUNT) OVER (
                    PARTITION BY P1.ASSET_ID,
                              P1.PORT_NUMBER
                    ORDER BY  P1.MEASURE_DATE
                    ),
                    P1.COLLISION_ERROR_COUNT
                    ) AS COLLISION_ERROR_COUNT_PREV,
                    P1.TOTAL_PACKETS_RECEIVED AS TOTAL_PACKETS_RECEIVED,
                    coalesce(
                    lag (P1.TOTAL_PACKETS_RECEIVED) OVER (
                    PARTITION BY P1.ASSET_ID,
                              P1.PORT_NUMBER
                    ORDER BY  P1.MEASURE_DATE
                    ),
                    P1.TOTAL_PACKETS_RECEIVED
                    ) AS TOTAL_PACKETS_RECEIVED_PREV,
    ROW_NUMBER() OVER (PARTITION BY ASSET_ID , PORT_NUMBER ORDER BY MEASURE_DATE DESC ) as row_num
    from
      (
        select
          analytics.ACTIVE_TELEMETRY.ASSET_ID AS ASSET_ID,
          analytics.ACTIVE_TELEMETRY.PORT_NUMBER AS PORT_NUMBER,
          analytics.ACTIVE_TELEMETRY.MEASURE_DATE AS MEASURE_DATE,
                              max(
                              (
                              case
                                        when (
                                        analytics.ACTIVE_TELEMETRY.MEASURE_TYPE = ''operation_status''
                                        ) then analytics.ACTIVE_TELEMETRY.MEASURE_STRING
                                        else ''''
                              end
                              )
                              ) AS OPERATION_STATUS,
                              max(
                              (
                              case
                                        when (
                                        analytics.ACTIVE_TELEMETRY.MEASURE_TYPE = ''negotiated_bps''
                                        ) then analytics.ACTIVE_TELEMETRY.MEASURE_VALUE
                                        else 0
                              end
                              )
                              ) AS NEGOTIATED_BPS,
                              max(
                              (
                              case
                                        when (
                                        analytics.ACTIVE_TELEMETRY.MEASURE_TYPE = ''received_bps''
                                        ) then analytics.ACTIVE_TELEMETRY.MEASURE_VALUE
                                        else 0
                              end
                              )
                              ) AS RECEIVED_BPS,
                              max(
                              (
                              case
                                        when (
                                        analytics.ACTIVE_TELEMETRY.MEASURE_TYPE = ''crc_error_count''
                                        ) then analytics.ACTIVE_TELEMETRY.MEASURE_VALUE
                                        else 0
                              end
                              )
                              ) AS CRC_ERROR_COUNT,
                              max(
                              (
                              case
                                        when (
                                        analytics.ACTIVE_TELEMETRY.MEASURE_TYPE = ''collision_error_count''
                                        ) then analytics.ACTIVE_TELEMETRY.MEASURE_VALUE
                                        else 0
                              end
                              )
                              ) AS COLLISION_ERROR_COUNT,
                              max(
                              (
                              case
                                        when (
                                        analytics.ACTIVE_TELEMETRY.MEASURE_TYPE = ''total_packets_recieved''
                                        ) then analytics.ACTIVE_TELEMETRY.MEASURE_VALUE
                                        else 0
                              end
                              )
                              ) AS TOTAL_PACKETS_RECEIVED,
                                max(
                              (
                              case
                                        when (
                                        analytics.ACTIVE_TELEMETRY.MEASURE_TYPE = ''admin_status''
                                        ) then analytics.ACTIVE_TELEMETRY.MEASURE_STRING
                                        else ''''
                              end
                              )
                              ) AS ADMIN_STATUS
        from
          analytics.ACTIVE_TELEMETRY  PARTITION ( ',
        partitionName,
        ' )
    WHERE
      MEASURE_DATE BETWEEN ',
        fromTime,
        ' AND ',
        toTime,
        ' AND ASSET_ID like ''',
        node,
        ''' 
        group by
          analytics.ACTIVE_TELEMETRY.ASSET_ID,
          analytics.ACTIVE_TELEMETRY.PORT_NUMBER,
          analytics.ACTIVE_TELEMETRY.MEASURE_DATE
    ) P1 
      WHERE ADMIN_STATUS = ''up''
  ) P2
  WHERE row_num = 1) as P3
  GROUP BY ASSET_ID'
    );

PREPARE stmt
FROM
    @sqlQuery;

EXECUTE stmt;

DEALLOCATE PREPARE stmt;

END ;;
DELIMITER ;
/*!50003 SET sql_mode              = @saved_sql_mode */ ;
/*!50003 SET character_set_client  = @saved_cs_client */ ;
/*!50003 SET character_set_results = @saved_cs_results */ ;
/*!50003 SET collation_connection  = @saved_col_connection */ ;
/*!50003 DROP PROCEDURE IF EXISTS `GetOneMetricBreakdownForSwitch` */;
/*!50003 SET @saved_cs_client      = @@character_set_client */ ;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;
/*!50003 SET character_set_client  = latin1 */ ;
/*!50003 SET character_set_results = latin1 */ ;
/*!50003 SET collation_connection  = latin1_swedish_ci */ ;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;
/*!50003 SET sql_mode              = '' */ ;
DELIMITER ;;
CREATE DEFINER=`analytics_dev`@`%` PROCEDURE `GetOneMetricBreakdownForSwitch`(
    IN fromTime INT,
    IN toTime INT,
    IN node VARCHAR(255),
    IN partitionName VARCHAR(20)
)
BEGIN
SET
    @sqlQuery = CONCAT(
        '
SELECT 
    QUALITY_FACTOR * CAPACITY_FACTOR * AVAILABILITY_FACTOR * 100 AS overall,
    ASSET_ID AS switch,
    PORT_NUMBER AS port
FROM
(
select
  P2.ASSET_ID AS ASSET_ID,
  P2.PORT_NUMBER AS PORT_NUMBER,
  P2.MEASURE_DATE AS MEASURE_DATE,
                    COALESCE(
          (
          (
          case
                    when (P2.OPERATION_STATUS = ''up'') then (P2.MEASURE_DATE - P2.MEASURE_DATE_PREV)
                    else 0
          end
          ) / case
                    when (P2.ADMIN_STATUS = ''up'') then (P2.MEASURE_DATE - P2.MEASURE_DATE_PREV)
                    else NULL
          end
          ),
          NULL
          ) AS AVAILABILITY_FACTOR,
            CASE WHEN P2.ADMIN_STATUS <> ''up'' then NULL ELSE
          (
          1 - abs(
          COALESCE(
          (
          ((P2.RECEIVED_BPS - CASE WHEN P2.RECEIVED_BPS_PREV > P2.RECEIVED_BPS THEN 0 ELSE P2.RECEIVED_BPS_PREV END ) * 8) / (
          P2.NEGOTIATED_BPS * (P2.MEASURE_DATE - P2.MEASURE_DATE_PREV)
          )
          ),
          0
          )
          )
          ) END AS CAPACITY_FACTOR,
          CASE WHEN P2.ADMIN_STATUS <> ''up'' then NULL ELSE
          (
          COALESCE(
          (
          (
          (
          (
          P2.TOTAL_PACKETS_RECEIVED - CASE WHEN P2.TOTAL_PACKETS_RECEIVED_PREV > P2.TOTAL_PACKETS_RECEIVED THEN 0 ELSE P2.TOTAL_PACKETS_RECEIVED_PREV END
          ) - (P2.CRC_ERROR_COUNT - CASE WHEN P2.CRC_ERROR_COUNT_PREV > P2.CRC_ERROR_COUNT THEN 0 ELSE P2.CRC_ERROR_COUNT_PREV END)
          ) - (
          P2.COLLISION_ERROR_COUNT - CASE WHEN P2.COLLISION_ERROR_COUNT_PREV > P2.COLLISION_ERROR_COUNT THEN 0 ELSE P2.COLLISION_ERROR_COUNT_PREV END
          )
          ) / (
          P2.TOTAL_PACKETS_RECEIVED - CASE WHEN P2.TOTAL_PACKETS_RECEIVED_PREV > P2.TOTAL_PACKETS_RECEIVED THEN 0 ELSE P2.TOTAL_PACKETS_RECEIVED_PREV END
          )
          ),
          1
          )) END AS QUALITY_FACTOR
from
  (
    select
      P1.ASSET_ID AS ASSET_ID,
      P1.PORT_NUMBER AS PORT_NUMBER,
      P1.OPERATION_STATUS AS OPERATION_STATUS,
      P1.ADMIN_STATUS AS ADMIN_STATUS,
                    coalesce(
                    lag (P1.OPERATION_STATUS) OVER (
                    PARTITION BY P1.ASSET_ID,
                              P1.PORT_NUMBER
                    ORDER BY  P1.MEASURE_DATE
                    ),
                    P1.OPERATION_STATUS
                    ) AS OPERATION_STATUS_PREV,
                    P1.MEASURE_DATE AS MEASURE_DATE,
                    coalesce(
                    lag (P1.MEASURE_DATE) OVER (
                    PARTITION BY P1.ASSET_ID,
                              P1.PORT_NUMBER
                    ORDER BY  P1.MEASURE_DATE
                    ),
                    P1.MEASURE_DATE
                    ) AS MEASURE_DATE_PREV,
                    coalesce(
                    lead (P1.MEASURE_DATE) OVER (
                    PARTITION BY P1.ASSET_ID,
                              P1.PORT_NUMBER
                    ORDER BY  P1.MEASURE_DATE
                    ),
                    P1.MEASURE_DATE
                    ) AS MEASURE_DATE_FUTURE,
                    P1.NEGOTIATED_BPS AS NEGOTIATED_BPS,
                    coalesce(
                    lag (P1.NEGOTIATED_BPS) OVER (
                    PARTITION BY P1.ASSET_ID,
                              P1.PORT_NUMBER
                    ORDER BY  P1.MEASURE_DATE
                    ),
                    P1.NEGOTIATED_BPS
                    ) AS NEGOTIATED_BPS_PREV,
                    P1.RECEIVED_BPS AS RECEIVED_BPS,
                    coalesce(
                    lag (P1.RECEIVED_BPS) OVER (
                    PARTITION BY P1.ASSET_ID,
                              P1.PORT_NUMBER
                    ORDER BY  P1.MEASURE_DATE
                    ),
                    P1.RECEIVED_BPS
                    ) AS RECEIVED_BPS_PREV,
                    P1.CRC_ERROR_COUNT AS CRC_ERROR_COUNT,
                    coalesce(
                    lag (P1.CRC_ERROR_COUNT) OVER (
                    PARTITION BY P1.ASSET_ID,
                              P1.PORT_NUMBER
                    ORDER BY  P1.MEASURE_DATE
                    ),
                    P1.CRC_ERROR_COUNT
                    ) AS CRC_ERROR_COUNT_PREV,
                    P1.COLLISION_ERROR_COUNT AS COLLISION_ERROR_COUNT,
                    coalesce(
                    lag (P1.COLLISION_ERROR_COUNT) OVER (
                    PARTITION BY P1.ASSET_ID,
                              P1.PORT_NUMBER
                    ORDER BY  P1.MEASURE_DATE
                    ),
                    P1.COLLISION_ERROR_COUNT
                    ) AS COLLISION_ERROR_COUNT_PREV,
                    P1.TOTAL_PACKETS_RECEIVED AS TOTAL_PACKETS_RECEIVED,
                    coalesce(
                    lag (P1.TOTAL_PACKETS_RECEIVED) OVER (
                    PARTITION BY P1.ASSET_ID,
                              P1.PORT_NUMBER
                    ORDER BY  P1.MEASURE_DATE
                    ),
                    P1.TOTAL_PACKETS_RECEIVED
                    ) AS TOTAL_PACKETS_RECEIVED_PREV,
    ROW_NUMBER() OVER (PARTITION BY ASSET_ID , PORT_NUMBER ORDER BY MEASURE_DATE DESC ) as row_num
    from
      (
        select
          analytics.ACTIVE_TELEMETRY.ASSET_ID AS ASSET_ID,
          analytics.ACTIVE_TELEMETRY.PORT_NUMBER AS PORT_NUMBER,
          analytics.ACTIVE_TELEMETRY.MEASURE_DATE AS MEASURE_DATE,
                              max(
                              (
                              case
                                        when (
                                        analytics.ACTIVE_TELEMETRY.MEASURE_TYPE = ''operation_status''
                                        ) then analytics.ACTIVE_TELEMETRY.MEASURE_STRING
                                        else ''''
                              end
                              )
                              ) AS OPERATION_STATUS,
                              max(
                              (
                              case
                                        when (
                                        analytics.ACTIVE_TELEMETRY.MEASURE_TYPE = ''negotiated_bps''
                                        ) then analytics.ACTIVE_TELEMETRY.MEASURE_VALUE
                                        else 0
                              end
                              )
                              ) AS NEGOTIATED_BPS,
                              max(
                              (
                              case
                                        when (
                                        analytics.ACTIVE_TELEMETRY.MEASURE_TYPE = ''received_bps''
                                        ) then analytics.ACTIVE_TELEMETRY.MEASURE_VALUE
                                        else 0
                              end
                              )
                              ) AS RECEIVED_BPS,
                              max(
                              (
                              case
                                        when (
                                        analytics.ACTIVE_TELEMETRY.MEASURE_TYPE = ''crc_error_count''
                                        ) then analytics.ACTIVE_TELEMETRY.MEASURE_VALUE
                                        else 0
                              end
                              )
                              ) AS CRC_ERROR_COUNT,
                              max(
                              (
                              case
                                        when (
                                        analytics.ACTIVE_TELEMETRY.MEASURE_TYPE = ''collision_error_count''
                                        ) then analytics.ACTIVE_TELEMETRY.MEASURE_VALUE
                                        else 0
                              end
                              )
                              ) AS COLLISION_ERROR_COUNT,
                              max(
                              (
                              case
                                        when (
                                        analytics.ACTIVE_TELEMETRY.MEASURE_TYPE = ''total_packets_recieved''
                                        ) then analytics.ACTIVE_TELEMETRY.MEASURE_VALUE
                                        else 0
                              end
                              )
                              ) AS TOTAL_PACKETS_RECEIVED,
                              max(
                              (
                              case
                                        when (
                                        analytics.ACTIVE_TELEMETRY.MEASURE_TYPE = ''admin_status''
                                        ) then analytics.ACTIVE_TELEMETRY.MEASURE_STRING
                                        else ''''
                              end
                              )
                              ) AS ADMIN_STATUS
        from
          analytics.ACTIVE_TELEMETRY  PARTITION ( ',
        partitionName,
        ' )
    WHERE
      MEASURE_DATE BETWEEN ',
        fromTime,
        ' AND ',
        toTime,
        ' AND ASSET_ID = ''',
        node,
        ''' 
        group by
          analytics.ACTIVE_TELEMETRY.ASSET_ID,
          analytics.ACTIVE_TELEMETRY.PORT_NUMBER,
          analytics.ACTIVE_TELEMETRY.MEASURE_DATE
      ) P1
      WHERE ADMIN_STATUS = ''up''
  ) P2
  WHERE row_num = 1) as P3'
    );

PREPARE stmt
FROM
    @sqlQuery;

EXECUTE stmt;

DEALLOCATE PREPARE stmt;

END ;;
DELIMITER ;
/*!50003 SET sql_mode              = @saved_sql_mode */ ;
/*!50003 SET character_set_client  = @saved_cs_client */ ;
/*!50003 SET character_set_results = @saved_cs_results */ ;
/*!50003 SET collation_connection  = @saved_col_connection */ ;
/*!50003 DROP PROCEDURE IF EXISTS `GetOneMetricChange` */;
/*!50003 SET @saved_cs_client      = @@character_set_client */ ;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;
/*!50003 SET character_set_client  = latin1 */ ;
/*!50003 SET character_set_results = latin1 */ ;
/*!50003 SET collation_connection  = latin1_swedish_ci */ ;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;
/*!50003 SET sql_mode              = '' */ ;
DELIMITER ;;
CREATE DEFINER=`analytics_dev`@`%` PROCEDURE `GetOneMetricChange`(
    IN fromTime INT,
    IN toTime INT,
    IN col VARCHAR(255),
    IN pageNum INT,
    IN pageSize INT,
    IN partitionName VARCHAR(20)
)
BEGIN
SET
    @sqlQuery = CONCAT(
        'SELECT time,
      Value,
      ONEChange
      FROM (
        SELECT (MEASURE_DATE) DIV 30 * 30 as time,
        AVG(COALESCE(',
        col,
        ', 0)) as Value,
        AVG(COALESCE(',
        col,
        ', 0)) - LAG(AVG(COALESCE(',
        col,
        ', 0))) OVER (ORDER BY (MEASURE_DATE) DIV 30 * 30) AS ONEChange
        FROM 
    (
    
select
  P2.ASSET_ID AS ASSET_ID,
  P2.PORT_NUMBER AS PORT_NUMBER,
  P2.MEASURE_DATE AS MEASURE_DATE,
          COALESCE(
          (
          (
          case
                    when (P2.OPERATION_STATUS = ''up'') then (P2.MEASURE_DATE - P2.MEASURE_DATE_PREV)
                    else 0
          end
          ) / case
                    when (P2.ADMIN_STATUS = ''up'') then (P2.MEASURE_DATE - P2.MEASURE_DATE_PREV)
                    else NULL
          end
          ),
          NULL
          ) AS AVAILABILITY_FACTOR,
            CASE WHEN P2.ADMIN_STATUS <> ''up'' then NULL ELSE
          (
          1 - abs(
          COALESCE(
          (
          ((P2.RECEIVED_BPS - CASE WHEN P2.RECEIVED_BPS_PREV > P2.RECEIVED_BPS THEN 0 ELSE P2.RECEIVED_BPS_PREV END ) * 8) / (
          P2.NEGOTIATED_BPS * (P2.MEASURE_DATE - P2.MEASURE_DATE_PREV)
          )
          ),
          0
          )
          )
          ) END AS CAPACITY_FACTOR,
          CASE WHEN P2.ADMIN_STATUS <> ''up'' then NULL ELSE
          (
          COALESCE(
          (
          (
          (
          (
          P2.TOTAL_PACKETS_RECEIVED - CASE WHEN P2.TOTAL_PACKETS_RECEIVED_PREV > P2.TOTAL_PACKETS_RECEIVED THEN 0 ELSE P2.TOTAL_PACKETS_RECEIVED_PREV END
          ) - (P2.CRC_ERROR_COUNT - CASE WHEN P2.CRC_ERROR_COUNT_PREV > P2.CRC_ERROR_COUNT THEN 0 ELSE P2.CRC_ERROR_COUNT_PREV END)
          ) - (
          P2.COLLISION_ERROR_COUNT - CASE WHEN P2.COLLISION_ERROR_COUNT_PREV > P2.COLLISION_ERROR_COUNT THEN 0 ELSE P2.COLLISION_ERROR_COUNT_PREV END
          )
          ) / (
          P2.TOTAL_PACKETS_RECEIVED - CASE WHEN P2.TOTAL_PACKETS_RECEIVED_PREV > P2.TOTAL_PACKETS_RECEIVED THEN 0 ELSE P2.TOTAL_PACKETS_RECEIVED_PREV END
          )
          ),
          1
          )) END AS QUALITY_FACTOR
from
  (
    select
      P1.ASSET_ID AS ASSET_ID,
      P1.PORT_NUMBER AS PORT_NUMBER,
      P1.OPERATION_STATUS AS OPERATION_STATUS,
      P1.ADMIN_STATUS AS ADMIN_STATUS,
                    coalesce(
                    lag (P1.OPERATION_STATUS) OVER (
                    PARTITION BY P1.ASSET_ID,
                              P1.PORT_NUMBER
                    ORDER BY  P1.MEASURE_DATE
                    ),
                    P1.OPERATION_STATUS
                    ) AS OPERATION_STATUS_PREV,
                    P1.MEASURE_DATE AS MEASURE_DATE,
                    coalesce(
                    lag (P1.MEASURE_DATE) OVER (
                    PARTITION BY P1.ASSET_ID,
                              P1.PORT_NUMBER
                    ORDER BY  P1.MEASURE_DATE
                    ),
                    P1.MEASURE_DATE
                    ) AS MEASURE_DATE_PREV,
                    coalesce(
                    lead (P1.MEASURE_DATE) OVER (
                    PARTITION BY P1.ASSET_ID,
                              P1.PORT_NUMBER
                    ORDER BY  P1.MEASURE_DATE
                    ),
                    P1.MEASURE_DATE
                    ) AS MEASURE_DATE_FUTURE,
                    P1.NEGOTIATED_BPS AS NEGOTIATED_BPS,
                    coalesce(
                    lag (P1.NEGOTIATED_BPS) OVER (
                    PARTITION BY P1.ASSET_ID,
                              P1.PORT_NUMBER
                    ORDER BY  P1.MEASURE_DATE
                    ),
                    P1.NEGOTIATED_BPS
                    ) AS NEGOTIATED_BPS_PREV,
                    P1.RECEIVED_BPS AS RECEIVED_BPS,
                    coalesce(
                    lag (P1.RECEIVED_BPS) OVER (
                    PARTITION BY P1.ASSET_ID,
                              P1.PORT_NUMBER
                    ORDER BY  P1.MEASURE_DATE
                    ),
                    P1.RECEIVED_BPS
                    ) AS RECEIVED_BPS_PREV,
                    P1.CRC_ERROR_COUNT AS CRC_ERROR_COUNT,
                    coalesce(
                    lag (P1.CRC_ERROR_COUNT) OVER (
                    PARTITION BY P1.ASSET_ID,
                              P1.PORT_NUMBER
                    ORDER BY  P1.MEASURE_DATE
                    ),
                    P1.CRC_ERROR_COUNT
                    ) AS CRC_ERROR_COUNT_PREV,
                    P1.COLLISION_ERROR_COUNT AS COLLISION_ERROR_COUNT,
                    coalesce(
                    lag (P1.COLLISION_ERROR_COUNT) OVER (
                    PARTITION BY P1.ASSET_ID,
                              P1.PORT_NUMBER
                    ORDER BY  P1.MEASURE_DATE
                    ),
                    P1.COLLISION_ERROR_COUNT
                    ) AS COLLISION_ERROR_COUNT_PREV,
                    P1.TOTAL_PACKETS_RECEIVED AS TOTAL_PACKETS_RECEIVED,
                    coalesce(
                    lag (P1.TOTAL_PACKETS_RECEIVED) OVER (
                    PARTITION BY P1.ASSET_ID,
                              P1.PORT_NUMBER
                    ORDER BY  P1.MEASURE_DATE
                    ),
                    P1.TOTAL_PACKETS_RECEIVED
                    ) AS TOTAL_PACKETS_RECEIVED_PREV
  from
      (
        select
          analytics.ACTIVE_TELEMETRY.ASSET_ID AS ASSET_ID,
          analytics.ACTIVE_TELEMETRY.PORT_NUMBER AS PORT_NUMBER,
          analytics.ACTIVE_TELEMETRY.MEASURE_DATE AS MEASURE_DATE,
                              max(
                              (
                              case
                                        when (
                                        analytics.ACTIVE_TELEMETRY.MEASURE_TYPE = ''operation_status''
                                        ) then analytics.ACTIVE_TELEMETRY.MEASURE_STRING
                                        else ''''
                              end
                              )
                              ) AS OPERATION_STATUS,
                              max(
                              (
                              case
                                        when (
                                        analytics.ACTIVE_TELEMETRY.MEASURE_TYPE = ''negotiated_bps''
                                        ) then analytics.ACTIVE_TELEMETRY.MEASURE_VALUE
                                        else 0
                              end
                              )
                              ) AS NEGOTIATED_BPS,
                              max(
                              (
                              case
                                        when (
                                        analytics.ACTIVE_TELEMETRY.MEASURE_TYPE = ''received_bps''
                                        ) then analytics.ACTIVE_TELEMETRY.MEASURE_VALUE
                                        else 0
                              end
                              )
                              ) AS RECEIVED_BPS,
                              max(
                              (
                              case
                                        when (
                                        analytics.ACTIVE_TELEMETRY.MEASURE_TYPE = ''crc_error_count''
                                        ) then analytics.ACTIVE_TELEMETRY.MEASURE_VALUE
                                        else 0
                              end
                              )
                              ) AS CRC_ERROR_COUNT,
                              max(
                              (
                              case
                                        when (
                                        analytics.ACTIVE_TELEMETRY.MEASURE_TYPE = ''collision_error_count''
                                        ) then analytics.ACTIVE_TELEMETRY.MEASURE_VALUE
                                        else 0
                              end
                              )
                              ) AS COLLISION_ERROR_COUNT,
                              max(
                              (
                              case
                                        when (
                                        analytics.ACTIVE_TELEMETRY.MEASURE_TYPE = ''total_packets_recieved''
                                        ) then analytics.ACTIVE_TELEMETRY.MEASURE_VALUE
                                        else 0
                              end
                              )
                              ) AS TOTAL_PACKETS_RECEIVED,
                              max(
                              (
                              case
                                        when (
                                        analytics.ACTIVE_TELEMETRY.MEASURE_TYPE = ''admin_status''
                                        ) then analytics.ACTIVE_TELEMETRY.MEASURE_STRING
                                        else ''''
                              end
                              )
                              ) AS ADMIN_STATUS
        from
          analytics.ACTIVE_TELEMETRY  PARTITION ( ',
        partitionName,
        ' )
    WHERE
        MEASURE_DATE BETWEEN ',
        fromTime,
        ' AND ',
        toTime,
        '
        group by
          analytics.ACTIVE_TELEMETRY.ASSET_ID,
          analytics.ACTIVE_TELEMETRY.PORT_NUMBER,
          analytics.ACTIVE_TELEMETRY.MEASURE_DATE
      ) P1
      WHERE ADMIN_STATUS = ''up''
  ) P2
    ) P3
        GROUP BY time
        ORDER BY time
      ) AS Grouped
      WHERE ONEChange >= 0.0001 OR ONEChange <= -0.0001
      LIMIT ',
        pageSize,
        ' OFFSET ',
        pageNum,
        ''
    );

PREPARE stmt
FROM
    @sqlQuery;

EXECUTE stmt;

DEALLOCATE PREPARE stmt;

END ;;
DELIMITER ;
/*!50003 SET sql_mode              = @saved_sql_mode */ ;
/*!50003 SET character_set_client  = @saved_cs_client */ ;
/*!50003 SET character_set_results = @saved_cs_results */ ;
/*!50003 SET collation_connection  = @saved_col_connection */ ;
/*!50003 DROP PROCEDURE IF EXISTS `GetOneMetricChangeByPort` */;
/*!50003 SET @saved_cs_client      = @@character_set_client */ ;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;
/*!50003 SET character_set_client  = latin1 */ ;
/*!50003 SET character_set_results = latin1 */ ;
/*!50003 SET collation_connection  = latin1_swedish_ci */ ;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;
/*!50003 SET sql_mode              = '' */ ;
DELIMITER ;;
CREATE DEFINER=`analytics_dev`@`%` PROCEDURE `GetOneMetricChangeByPort`(
    IN fromTime INT,
    IN toTime INT,
    IN col VARCHAR(255),
    IN pageNum INT,
    IN pageSize INT,
    IN node VARCHAR(255),
    IN portNumber VARCHAR(255),
    IN partitionName VARCHAR(20)
)
BEGIN
SET
    @sqlQuery = CONCAT(
        '
SELECT
  time,
  Value,
  ONEChange
FROM
  (
    SELECT
      (MEASURE_DATE DIV 30) * 30 AS time,
      MAX(COALESCE(',
        col,
        ', 0)) AS Value,
      MAX(COALESCE(',
        col,
        ', 0)) - LAG (MAX(COALESCE(',
        col,
        ', 0))) OVER (
        ORDER BY
          (MEASURE_DATE DIV 30) * 30
      ) AS ONEChange FROM (
        SELECT
          P2.ASSET_ID AS ASSET_ID,
          P2.PORT_NUMBER AS PORT_NUMBER,
          P2.MEASURE_DATE AS MEASURE_DATE,
                    COALESCE(
          (
          (
          case
                    when (P2.OPERATION_STATUS = ''up'') then (P2.MEASURE_DATE - P2.MEASURE_DATE_PREV)
                    else 0
          end
          ) / case
                    when (P2.ADMIN_STATUS = ''up'') then (P2.MEASURE_DATE - P2.MEASURE_DATE_PREV)
                    else NULL
          end
          ),
          NULL
          ) AS AVAILABILITY_FACTOR,
            CASE WHEN P2.ADMIN_STATUS <> ''up'' then NULL ELSE
          (
          1 - abs(
          COALESCE(
          (
          ((P2.RECEIVED_BPS - CASE WHEN P2.RECEIVED_BPS_PREV > P2.RECEIVED_BPS THEN 0 ELSE P2.RECEIVED_BPS_PREV END ) * 8) / (
          P2.NEGOTIATED_BPS * (P2.MEASURE_DATE - P2.MEASURE_DATE_PREV)
          )
          ),
          0
          )
          )
          ) END AS CAPACITY_FACTOR,
          CASE WHEN P2.ADMIN_STATUS <> ''up'' then NULL ELSE
          (
          COALESCE(
          (
          (
          (
          (
          P2.TOTAL_PACKETS_RECEIVED - CASE WHEN P2.TOTAL_PACKETS_RECEIVED_PREV > P2.TOTAL_PACKETS_RECEIVED THEN 0 ELSE P2.TOTAL_PACKETS_RECEIVED_PREV END
          ) - (P2.CRC_ERROR_COUNT - CASE WHEN P2.CRC_ERROR_COUNT_PREV > P2.CRC_ERROR_COUNT THEN 0 ELSE P2.CRC_ERROR_COUNT_PREV END)
          ) - (
          P2.COLLISION_ERROR_COUNT - CASE WHEN P2.COLLISION_ERROR_COUNT_PREV > P2.COLLISION_ERROR_COUNT THEN 0 ELSE P2.COLLISION_ERROR_COUNT_PREV END
          )
          ) / (
          P2.TOTAL_PACKETS_RECEIVED - CASE WHEN P2.TOTAL_PACKETS_RECEIVED_PREV > P2.TOTAL_PACKETS_RECEIVED THEN 0 ELSE P2.TOTAL_PACKETS_RECEIVED_PREV END
          )
          ),
          1
          )) END AS QUALITY_FACTOR
        from
          (
            select
              P1.ASSET_ID AS ASSET_ID,
              P1.PORT_NUMBER AS PORT_NUMBER,
              P1.OPERATION_STATUS AS OPERATION_STATUS,
              P1.ADMIN_STATUS AS ADMIN_STATUS,
              coalesce(
                lag (P1.OPERATION_STATUS) OVER (
                  PARTITION BY
                    P1.ASSET_ID,
                    P1.PORT_NUMBER
                  ORDER BY
                    P1.MEASURE_DATE
                ),
                P1.OPERATION_STATUS
              ) AS OPERATION_STATUS_PREV,
              P1.MEASURE_DATE AS MEASURE_DATE,
              coalesce(
                lag (P1.MEASURE_DATE) OVER (
                  PARTITION BY
                    P1.ASSET_ID,
                    P1.PORT_NUMBER
                  ORDER BY
                    P1.MEASURE_DATE
                ),
                P1.MEASURE_DATE
              ) AS MEASURE_DATE_PREV,
              coalesce(
                lead (P1.MEASURE_DATE) OVER (
                  PARTITION BY
                    P1.ASSET_ID,
                    P1.PORT_NUMBER
                  ORDER BY
                    P1.MEASURE_DATE
                ),
                P1.MEASURE_DATE
              ) AS MEASURE_DATE_FUTURE,
              P1.NEGOTIATED_BPS AS NEGOTIATED_BPS,
              coalesce(
                lag (P1.NEGOTIATED_BPS) OVER (
                  PARTITION BY
                    P1.ASSET_ID,
                    P1.PORT_NUMBER
                  ORDER BY
                    P1.MEASURE_DATE
                ),
                P1.NEGOTIATED_BPS
              ) AS NEGOTIATED_BPS_PREV,
              P1.RECEIVED_BPS AS RECEIVED_BPS,
              coalesce(
                lag (P1.RECEIVED_BPS) OVER (
                  PARTITION BY
                    P1.ASSET_ID,
                    P1.PORT_NUMBER
                  ORDER BY
                    P1.MEASURE_DATE
                ),
                P1.RECEIVED_BPS
              ) AS RECEIVED_BPS_PREV,
              P1.CRC_ERROR_COUNT AS CRC_ERROR_COUNT,
              coalesce(
                lag (P1.CRC_ERROR_COUNT) OVER (
                  PARTITION BY
                    P1.ASSET_ID,
                    P1.PORT_NUMBER
                  ORDER BY
                    P1.MEASURE_DATE
                ),
                P1.CRC_ERROR_COUNT
              ) AS CRC_ERROR_COUNT_PREV,
              P1.COLLISION_ERROR_COUNT AS COLLISION_ERROR_COUNT,
              coalesce(
                lag (P1.COLLISION_ERROR_COUNT) OVER (
                  PARTITION BY
                    P1.ASSET_ID,
                    P1.PORT_NUMBER
                  ORDER BY
                    P1.MEASURE_DATE
                ),
                P1.COLLISION_ERROR_COUNT
              ) AS COLLISION_ERROR_COUNT_PREV,
              P1.TOTAL_PACKETS_RECEIVED AS TOTAL_PACKETS_RECEIVED,
              coalesce(
                lag (P1.TOTAL_PACKETS_RECEIVED) OVER (
                  PARTITION BY
                    P1.ASSET_ID,
                    P1.PORT_NUMBER
                  ORDER BY
                    P1.MEASURE_DATE
                ),
                P1.TOTAL_PACKETS_RECEIVED
              ) AS TOTAL_PACKETS_RECEIVED_PREV
            from
              (
                select
                  analytics.ACTIVE_TELEMETRY.ASSET_ID AS ASSET_ID,
                  analytics.ACTIVE_TELEMETRY.PORT_NUMBER AS PORT_NUMBER,
                  analytics.ACTIVE_TELEMETRY.MEASURE_DATE AS MEASURE_DATE,
                  max(
                    (
                      case
                        when (
                          analytics.ACTIVE_TELEMETRY.MEASURE_TYPE = ''operation_status''
                        ) then analytics.ACTIVE_TELEMETRY.MEASURE_STRING
                        else ''''
                      end
                    )
                  ) AS OPERATION_STATUS,
                  max(
                    (
                      case
                        when (
                          analytics.ACTIVE_TELEMETRY.MEASURE_TYPE = ''negotiated_bps''
                        ) then analytics.ACTIVE_TELEMETRY.MEASURE_VALUE
                        else 0
                      end
                    )
                  ) AS NEGOTIATED_BPS,
                  max(
                    (
                      case
                        when (
                          analytics.ACTIVE_TELEMETRY.MEASURE_TYPE = ''received_bps''
                        ) then analytics.ACTIVE_TELEMETRY.MEASURE_VALUE
                        else 0
                      end
                    )
                  ) AS RECEIVED_BPS,
                  max(
                    (
                      case
                        when (
                          analytics.ACTIVE_TELEMETRY.MEASURE_TYPE = ''crc_error_count''
                        ) then analytics.ACTIVE_TELEMETRY.MEASURE_VALUE
                        else 0
                      end
                    )
                  ) AS CRC_ERROR_COUNT,
                  max(
                    (
                      case
                        when (
                          analytics.ACTIVE_TELEMETRY.MEASURE_TYPE = ''collision_error_count''
                        ) then analytics.ACTIVE_TELEMETRY.MEASURE_VALUE
                        else 0
                      end
                    )
                  ) AS COLLISION_ERROR_COUNT,
                  max(
                    (
                      case
                        when (
                          analytics.ACTIVE_TELEMETRY.MEASURE_TYPE = ''total_packets_recieved''
                        ) then analytics.ACTIVE_TELEMETRY.MEASURE_VALUE
                        else 0
                      end
                    )
                  ) AS TOTAL_PACKETS_RECEIVED,
                              max(
                              (
                              case
                                        when (
                                        analytics.ACTIVE_TELEMETRY.MEASURE_TYPE = ''admin_status''
                                        ) then analytics.ACTIVE_TELEMETRY.MEASURE_STRING
                                        else ''''
                              end
                              )
                              ) AS ADMIN_STATUS
                FROM
                  analytics.ACTIVE_TELEMETRY PARTITION (',
        partitionName,
        ')
                WHERE
                  PORT_NUMBER = ',
        portNumber,
        '
                  AND MEASURE_DATE BETWEEN ',
        fromTime,
        ' AND ',
        toTime,
        '
                  AND ASSET_ID like ''',
        node,
        '''
                group by
                  analytics.ACTIVE_TELEMETRY.ASSET_ID,
                  analytics.ACTIVE_TELEMETRY.PORT_NUMBER,
                  analytics.ACTIVE_TELEMETRY.MEASURE_DATE
              ) P1
              WHERE ADMIN_STATUS = ''up''
          ) P2
      ) P3
    GROUP BY
      time
    ORDER BY
      time
  ) Grouped
WHERE
  ONEChange >= 0.0001
  OR ONEChange <= -0.0001
    LIMIT ',
        pageSize,
        ' OFFSET ',
        pageNum,
        ''
    );

PREPARE stmt
FROM
    @sqlQuery;

EXECUTE stmt;

DEALLOCATE PREPARE stmt;

END ;;
DELIMITER ;
/*!50003 SET sql_mode              = @saved_sql_mode */ ;
/*!50003 SET character_set_client  = @saved_cs_client */ ;
/*!50003 SET character_set_results = @saved_cs_results */ ;
/*!50003 SET collation_connection  = @saved_col_connection */ ;
/*!50003 DROP PROCEDURE IF EXISTS `GetOneMetricChangeBySwitch` */;
/*!50003 SET @saved_cs_client      = @@character_set_client */ ;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;
/*!50003 SET character_set_client  = latin1 */ ;
/*!50003 SET character_set_results = latin1 */ ;
/*!50003 SET collation_connection  = latin1_swedish_ci */ ;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;
/*!50003 SET sql_mode              = '' */ ;
DELIMITER ;;
CREATE DEFINER=`analytics_dev`@`%` PROCEDURE `GetOneMetricChangeBySwitch`(
    IN fromTime INT,
    IN toTime INT,
    IN col VARCHAR(255),
    IN offsetCount INT,
    IN limitCount INT,
    IN node VARCHAR(255),
    IN partitionName VARCHAR(20)
)
BEGIN
SET
    @sqlQuery = CONCAT(
        '
SELECT
  time,
  Value,
  ONEChange
FROM
  (
    SELECT
      (MEASURE_DATE DIV 30) * 30 AS time,
      AVG(COALESCE(',
        col,
        ', 0)) AS Value,
      AVG(COALESCE(',
        col,
        ', 0)) - LAG (AVG(COALESCE(',
        col,
        ', 0))) OVER (
        ORDER BY
          (MEASURE_DATE DIV 30) * 30
      ) AS ONEChange FROM (
        SELECT
          P2.ASSET_ID AS ASSET_ID,
          P2.PORT_NUMBER AS PORT_NUMBER,
          P2.MEASURE_DATE AS MEASURE_DATE,
          COALESCE(
          (
          (
          case
                    when (P2.OPERATION_STATUS = ''up'') then (P2.MEASURE_DATE - P2.MEASURE_DATE_PREV)
                    else 0
          end
          ) / case
                    when (P2.ADMIN_STATUS = ''up'') then (P2.MEASURE_DATE - P2.MEASURE_DATE_PREV)
                    else NULL
          end
          ),
          NULL
          ) AS AVAILABILITY_FACTOR,
            CASE WHEN P2.ADMIN_STATUS <> ''up'' then NULL ELSE
          (
          1 - abs(
          COALESCE(
          (
          ((P2.RECEIVED_BPS - CASE WHEN P2.RECEIVED_BPS_PREV > P2.RECEIVED_BPS THEN 0 ELSE P2.RECEIVED_BPS_PREV END ) * 8) / (
          P2.NEGOTIATED_BPS * (P2.MEASURE_DATE - P2.MEASURE_DATE_PREV)
          )
          ),
          0
          )
          )
          ) END AS CAPACITY_FACTOR,
          CASE WHEN P2.ADMIN_STATUS <> ''up'' then NULL ELSE
          (
          COALESCE(
          (
          (
          (
          (
          P2.TOTAL_PACKETS_RECEIVED - CASE WHEN P2.TOTAL_PACKETS_RECEIVED_PREV > P2.TOTAL_PACKETS_RECEIVED THEN 0 ELSE P2.TOTAL_PACKETS_RECEIVED_PREV END
          ) - (P2.CRC_ERROR_COUNT - CASE WHEN P2.CRC_ERROR_COUNT_PREV > P2.CRC_ERROR_COUNT THEN 0 ELSE P2.CRC_ERROR_COUNT_PREV END)
          ) - (
          P2.COLLISION_ERROR_COUNT - CASE WHEN P2.COLLISION_ERROR_COUNT_PREV > P2.COLLISION_ERROR_COUNT THEN 0 ELSE P2.COLLISION_ERROR_COUNT_PREV END
          )
          ) / (
          P2.TOTAL_PACKETS_RECEIVED - CASE WHEN P2.TOTAL_PACKETS_RECEIVED_PREV > P2.TOTAL_PACKETS_RECEIVED THEN 0 ELSE P2.TOTAL_PACKETS_RECEIVED_PREV END
          )
          ),
          1
          )) END AS QUALITY_FACTOR
        from
          (
            select
              P1.ASSET_ID AS ASSET_ID,
              P1.PORT_NUMBER AS PORT_NUMBER,
              P1.OPERATION_STATUS AS OPERATION_STATUS,
              P1.ADMIN_STATUS AS ADMIN_STATUS,
              coalesce(
                lag (P1.OPERATION_STATUS) OVER (
                  PARTITION BY
                    P1.ASSET_ID,
                    P1.PORT_NUMBER
                  ORDER BY
                    P1.MEASURE_DATE
                ),
                P1.OPERATION_STATUS
              ) AS OPERATION_STATUS_PREV,
              P1.MEASURE_DATE AS MEASURE_DATE,
              coalesce(
                lag (P1.MEASURE_DATE) OVER (
                  PARTITION BY
                    P1.ASSET_ID,
                    P1.PORT_NUMBER
                  ORDER BY
                    P1.MEASURE_DATE
                ),
                P1.MEASURE_DATE
              ) AS MEASURE_DATE_PREV,
              coalesce(
                lead (P1.MEASURE_DATE) OVER (
                  PARTITION BY
                    P1.ASSET_ID,
                    P1.PORT_NUMBER
                  ORDER BY
                    P1.MEASURE_DATE
                ),
                P1.MEASURE_DATE
              ) AS MEASURE_DATE_FUTURE,
              P1.NEGOTIATED_BPS AS NEGOTIATED_BPS,
              coalesce(
                lag (P1.NEGOTIATED_BPS) OVER (
                  PARTITION BY
                    P1.ASSET_ID,
                    P1.PORT_NUMBER
                  ORDER BY
                    P1.MEASURE_DATE
                ),
                P1.NEGOTIATED_BPS
              ) AS NEGOTIATED_BPS_PREV,
              P1.RECEIVED_BPS AS RECEIVED_BPS,
              coalesce(
                lag (P1.RECEIVED_BPS) OVER (
                  PARTITION BY
                    P1.ASSET_ID,
                    P1.PORT_NUMBER
                  ORDER BY
                    P1.MEASURE_DATE
                ),
                P1.RECEIVED_BPS
              ) AS RECEIVED_BPS_PREV,
              P1.CRC_ERROR_COUNT AS CRC_ERROR_COUNT,
              coalesce(
                lag (P1.CRC_ERROR_COUNT) OVER (
                  PARTITION BY
                    P1.ASSET_ID,
                    P1.PORT_NUMBER
                  ORDER BY
                    P1.MEASURE_DATE
                ),
                P1.CRC_ERROR_COUNT
              ) AS CRC_ERROR_COUNT_PREV,
              P1.COLLISION_ERROR_COUNT AS COLLISION_ERROR_COUNT,
              coalesce(
                lag (P1.COLLISION_ERROR_COUNT) OVER (
                  PARTITION BY
                    P1.ASSET_ID,
                    P1.PORT_NUMBER
                  ORDER BY
                    P1.MEASURE_DATE
                ),
                P1.COLLISION_ERROR_COUNT
              ) AS COLLISION_ERROR_COUNT_PREV,
              P1.TOTAL_PACKETS_RECEIVED AS TOTAL_PACKETS_RECEIVED,
              coalesce(
                lag (P1.TOTAL_PACKETS_RECEIVED) OVER (
                  PARTITION BY
                    P1.ASSET_ID,
                    P1.PORT_NUMBER
                  ORDER BY
                    P1.MEASURE_DATE
                ),
                P1.TOTAL_PACKETS_RECEIVED
              ) AS TOTAL_PACKETS_RECEIVED_PREV
            from
              (
                select
                  analytics.ACTIVE_TELEMETRY.ASSET_ID AS ASSET_ID,
                  analytics.ACTIVE_TELEMETRY.PORT_NUMBER AS PORT_NUMBER,
                  analytics.ACTIVE_TELEMETRY.MEASURE_DATE AS MEASURE_DATE,
                  max(
                    (
                      case
                        when (
                          analytics.ACTIVE_TELEMETRY.MEASURE_TYPE = ''operation_status''
                        ) then analytics.ACTIVE_TELEMETRY.MEASURE_STRING
                        else ''''
                      end
                    )
                  ) AS OPERATION_STATUS,
                  max(
                    (
                      case
                        when (
                          analytics.ACTIVE_TELEMETRY.MEASURE_TYPE = ''negotiated_bps''
                        ) then analytics.ACTIVE_TELEMETRY.MEASURE_VALUE
                        else 0
                      end
                    )
                  ) AS NEGOTIATED_BPS,
                  max(
                    (
                      case
                        when (
                          analytics.ACTIVE_TELEMETRY.MEASURE_TYPE = ''received_bps''
                        ) then analytics.ACTIVE_TELEMETRY.MEASURE_VALUE
                        else 0
                      end
                    )
                  ) AS RECEIVED_BPS,
                  max(
                    (
                      case
                        when (
                          analytics.ACTIVE_TELEMETRY.MEASURE_TYPE = ''crc_error_count''
                        ) then analytics.ACTIVE_TELEMETRY.MEASURE_VALUE
                        else 0
                      end
                    )
                  ) AS CRC_ERROR_COUNT,
                  max(
                    (
                      case
                        when (
                          analytics.ACTIVE_TELEMETRY.MEASURE_TYPE = ''collision_error_count''
                        ) then analytics.ACTIVE_TELEMETRY.MEASURE_VALUE
                        else 0
                      end
                    )
                  ) AS COLLISION_ERROR_COUNT,
                  max(
                    (
                      case
                        when (
                          analytics.ACTIVE_TELEMETRY.MEASURE_TYPE = ''total_packets_recieved''
                        ) then analytics.ACTIVE_TELEMETRY.MEASURE_VALUE
                        else 0
                      end
                    )
                  ) AS TOTAL_PACKETS_RECEIVED,
                              max(
                              (
                              case
                                        when (
                                        analytics.ACTIVE_TELEMETRY.MEASURE_TYPE = ''admin_status''
                                        ) then analytics.ACTIVE_TELEMETRY.MEASURE_STRING
                                        else ''''
                              end
                              )
                              ) AS ADMIN_STATUS
                FROM
                  analytics.ACTIVE_TELEMETRY PARTITION (',
        partitionName,
        ')
                WHERE
                  MEASURE_DATE BETWEEN ',
        fromTime,
        ' AND ',
        toTime,
        ' AND ASSET_ID like ''',
        node,
        '''
                group by
                  analytics.ACTIVE_TELEMETRY.ASSET_ID,
                  analytics.ACTIVE_TELEMETRY.PORT_NUMBER,
                  analytics.ACTIVE_TELEMETRY.MEASURE_DATE
              ) P1
              WHERE ADMIN_STATUS = ''up''
          ) P2
      ) P3
    GROUP BY
      time
    ORDER BY
      time
  ) Grouped
WHERE
  ONEChange >= 0.0001
  OR ONEChange <= -0.0001
    LIMIT ',
        limitCount,
        ' OFFSET ',
        offsetCount,
        ''
    );

PREPARE stmt
FROM
    @sqlQuery;

EXECUTE stmt;

DEALLOCATE PREPARE stmt;

END ;;
DELIMITER ;
/*!50003 SET sql_mode              = @saved_sql_mode */ ;
/*!50003 SET character_set_client  = @saved_cs_client */ ;
/*!50003 SET character_set_results = @saved_cs_results */ ;
/*!50003 SET collation_connection  = @saved_col_connection */ ;
/*!50003 DROP PROCEDURE IF EXISTS `GetOneMetricChangeCount` */;
/*!50003 SET @saved_cs_client      = @@character_set_client */ ;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;
/*!50003 SET character_set_client  = latin1 */ ;
/*!50003 SET character_set_results = latin1 */ ;
/*!50003 SET collation_connection  = latin1_swedish_ci */ ;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;
/*!50003 SET sql_mode              = '' */ ;
DELIMITER ;;
CREATE DEFINER=`analytics_dev`@`%` PROCEDURE `GetOneMetricChangeCount`(
    IN fromTime INT,
    IN toTime INT,
    IN col VARCHAR(255),
    IN partitionName VARCHAR(20)
)
BEGIN
SET
    @sqlQuery = CONCAT(
        'select COUNT(*) AS count
      FROM (
        SELECT (MEASURE_DATE) DIV 30 * 30 as time,
        MAX(COALESCE(',
        col,
        ', 0)) as Value,
        MAX(COALESCE(',
        col,
        ', 0)) - LAG(MAX(COALESCE(',
        col,
        ', 0))) OVER (ORDER BY (MEASURE_DATE) DIV 30 * 30) AS ONEChange
        FROM 
        (
        select
  P2.ASSET_ID AS ASSET_ID,
  P2.PORT_NUMBER AS PORT_NUMBER,
  P2.MEASURE_DATE AS MEASURE_DATE,
          COALESCE(
          (
          (
          case
                    when (P2.OPERATION_STATUS = ''up'') then (P2.MEASURE_DATE - P2.MEASURE_DATE_PREV)
                    else 0
          end
          ) / case
                    when (P2.ADMIN_STATUS = ''up'') then (P2.MEASURE_DATE - P2.MEASURE_DATE_PREV)
                    else NULL
          end
          ),
          NULL
          ) AS AVAILABILITY_FACTOR,
            CASE WHEN P2.ADMIN_STATUS <> ''up'' then NULL ELSE
          (
          1 - abs(
          COALESCE(
          (
          ((P2.RECEIVED_BPS - CASE WHEN P2.RECEIVED_BPS_PREV > P2.RECEIVED_BPS THEN 0 ELSE P2.RECEIVED_BPS_PREV END ) * 8) / (
          P2.NEGOTIATED_BPS * (P2.MEASURE_DATE - P2.MEASURE_DATE_PREV)
          )
          ),
          0
          )
          )
          ) END AS CAPACITY_FACTOR,
          CASE WHEN P2.ADMIN_STATUS <> ''up'' then NULL ELSE
          (
          COALESCE(
          (
          (
          (
          (
          P2.TOTAL_PACKETS_RECEIVED - CASE WHEN P2.TOTAL_PACKETS_RECEIVED_PREV > P2.TOTAL_PACKETS_RECEIVED THEN 0 ELSE P2.TOTAL_PACKETS_RECEIVED_PREV END
          ) - (P2.CRC_ERROR_COUNT - CASE WHEN P2.CRC_ERROR_COUNT_PREV > P2.CRC_ERROR_COUNT THEN 0 ELSE P2.CRC_ERROR_COUNT_PREV END)
          ) - (
          P2.COLLISION_ERROR_COUNT - CASE WHEN P2.COLLISION_ERROR_COUNT_PREV > P2.COLLISION_ERROR_COUNT THEN 0 ELSE P2.COLLISION_ERROR_COUNT_PREV END
          )
          ) / (
          P2.TOTAL_PACKETS_RECEIVED - CASE WHEN P2.TOTAL_PACKETS_RECEIVED_PREV > P2.TOTAL_PACKETS_RECEIVED THEN 0 ELSE P2.TOTAL_PACKETS_RECEIVED_PREV END
          )
          ),
          1
          )) END AS QUALITY_FACTOR
from
  (
    select
      P1.ASSET_ID AS ASSET_ID,
      P1.PORT_NUMBER AS PORT_NUMBER,
      P1.OPERATION_STATUS AS OPERATION_STATUS,
      P1.ADMIN_STATUS AS ADMIN_STATUS,
                    coalesce(
                    lag (P1.OPERATION_STATUS) OVER (
                    PARTITION BY P1.ASSET_ID,
                              P1.PORT_NUMBER
                    ORDER BY  P1.MEASURE_DATE
                    ),
                    P1.OPERATION_STATUS
                    ) AS OPERATION_STATUS_PREV,
                    P1.MEASURE_DATE AS MEASURE_DATE,
                    coalesce(
                    lag (P1.MEASURE_DATE) OVER (
                    PARTITION BY P1.ASSET_ID,
                              P1.PORT_NUMBER
                    ORDER BY  P1.MEASURE_DATE
                    ),
                    P1.MEASURE_DATE
                    ) AS MEASURE_DATE_PREV,
                    coalesce(
                    lead (P1.MEASURE_DATE) OVER (
                    PARTITION BY P1.ASSET_ID,
                              P1.PORT_NUMBER
                    ORDER BY  P1.MEASURE_DATE
                    ),
                    P1.MEASURE_DATE
                    ) AS MEASURE_DATE_FUTURE,
                    P1.NEGOTIATED_BPS AS NEGOTIATED_BPS,
                    coalesce(
                    lag (P1.NEGOTIATED_BPS) OVER (
                    PARTITION BY P1.ASSET_ID,
                              P1.PORT_NUMBER
                    ORDER BY  P1.MEASURE_DATE
                    ),
                    P1.NEGOTIATED_BPS
                    ) AS NEGOTIATED_BPS_PREV,
                    P1.RECEIVED_BPS AS RECEIVED_BPS,
                    coalesce(
                    lag (P1.RECEIVED_BPS) OVER (
                    PARTITION BY P1.ASSET_ID,
                              P1.PORT_NUMBER
                    ORDER BY  P1.MEASURE_DATE
                    ),
                    P1.RECEIVED_BPS
                    ) AS RECEIVED_BPS_PREV,
                    P1.CRC_ERROR_COUNT AS CRC_ERROR_COUNT,
                    coalesce(
                    lag (P1.CRC_ERROR_COUNT) OVER (
                    PARTITION BY P1.ASSET_ID,
                              P1.PORT_NUMBER
                    ORDER BY  P1.MEASURE_DATE
                    ),
                    P1.CRC_ERROR_COUNT
                    ) AS CRC_ERROR_COUNT_PREV,
                    P1.COLLISION_ERROR_COUNT AS COLLISION_ERROR_COUNT,
                    coalesce(
                    lag (P1.COLLISION_ERROR_COUNT) OVER (
                    PARTITION BY P1.ASSET_ID,
                              P1.PORT_NUMBER
                    ORDER BY  P1.MEASURE_DATE
                    ),
                    P1.COLLISION_ERROR_COUNT
                    ) AS COLLISION_ERROR_COUNT_PREV,
                    P1.TOTAL_PACKETS_RECEIVED AS TOTAL_PACKETS_RECEIVED,
                    coalesce(
                    lag (P1.TOTAL_PACKETS_RECEIVED) OVER (
                    PARTITION BY P1.ASSET_ID,
                              P1.PORT_NUMBER
                    ORDER BY  P1.MEASURE_DATE
                    ),
                    P1.TOTAL_PACKETS_RECEIVED
                    ) AS TOTAL_PACKETS_RECEIVED_PREV
    from
      (
        select
          analytics.ACTIVE_TELEMETRY.ASSET_ID AS ASSET_ID,
          analytics.ACTIVE_TELEMETRY.PORT_NUMBER AS PORT_NUMBER,
          analytics.ACTIVE_TELEMETRY.MEASURE_DATE AS MEASURE_DATE,
                              max(
                              (
                              case
                                        when (
                                        analytics.ACTIVE_TELEMETRY.MEASURE_TYPE = ''operation_status''
                                        ) then analytics.ACTIVE_TELEMETRY.MEASURE_STRING
                                        else ''''
                              end
                              )
                              ) AS OPERATION_STATUS,
                              max(
                              (
                              case
                                        when (
                                        analytics.ACTIVE_TELEMETRY.MEASURE_TYPE = ''negotiated_bps''
                                        ) then analytics.ACTIVE_TELEMETRY.MEASURE_VALUE
                                        else 0
                              end
                              )
                              ) AS NEGOTIATED_BPS,
                              max(
                              (
                              case
                                        when (
                                        analytics.ACTIVE_TELEMETRY.MEASURE_TYPE = ''received_bps''
                                        ) then analytics.ACTIVE_TELEMETRY.MEASURE_VALUE
                                        else 0
                              end
                              )
                              ) AS RECEIVED_BPS,
                              max(
                              (
                              case
                                        when (
                                        analytics.ACTIVE_TELEMETRY.MEASURE_TYPE = ''crc_error_count''
                                        ) then analytics.ACTIVE_TELEMETRY.MEASURE_VALUE
                                        else 0
                              end
                              )
                              ) AS CRC_ERROR_COUNT,
                              max(
                              (
                              case
                                        when (
                                        analytics.ACTIVE_TELEMETRY.MEASURE_TYPE = ''collision_error_count''
                                        ) then analytics.ACTIVE_TELEMETRY.MEASURE_VALUE
                                        else 0
                              end
                              )
                              ) AS COLLISION_ERROR_COUNT,
                              max(
                              (
                              case
                                        when (
                                        analytics.ACTIVE_TELEMETRY.MEASURE_TYPE = ''total_packets_recieved''
                                        ) then analytics.ACTIVE_TELEMETRY.MEASURE_VALUE
                                        else 0
                              end
                              )
                              ) AS TOTAL_PACKETS_RECEIVED,
                              max(
                              (
                              case
                                        when (
                                        analytics.ACTIVE_TELEMETRY.MEASURE_TYPE = ''admin_status''
                                        ) then analytics.ACTIVE_TELEMETRY.MEASURE_STRING
                                        else ''''
                              end
                              )
                              ) AS ADMIN_STATUS
        from
          analytics.ACTIVE_TELEMETRY PARTITION ( ',
        partitionName,
        ')
    WHERE 
        MEASURE_DATE BETWEEN ',
        fromTime,
        ' AND ',
        toTime,
        '
        group by
          analytics.ACTIVE_TELEMETRY.ASSET_ID,
          analytics.ACTIVE_TELEMETRY.PORT_NUMBER,
          analytics.ACTIVE_TELEMETRY.MEASURE_DATE
      ) P1
      WHERE ADMIN_STATUS = ''up''
  ) P2
        ) P3
        GROUP BY time
        ORDER BY time
      ) AS Grouped
      WHERE ONEChange >= 0.0001 OR ONEChange <= -0.0001'
    );

PREPARE stmt
FROM
    @sqlQuery;

EXECUTE stmt;

DEALLOCATE PREPARE stmt;

END ;;
DELIMITER ;
/*!50003 SET sql_mode              = @saved_sql_mode */ ;
/*!50003 SET character_set_client  = @saved_cs_client */ ;
/*!50003 SET character_set_results = @saved_cs_results */ ;
/*!50003 SET collation_connection  = @saved_col_connection */ ;
/*!50003 DROP PROCEDURE IF EXISTS `GetOneMetricChangeCountByPort` */;
/*!50003 SET @saved_cs_client      = @@character_set_client */ ;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;
/*!50003 SET character_set_client  = latin1 */ ;
/*!50003 SET character_set_results = latin1 */ ;
/*!50003 SET collation_connection  = latin1_swedish_ci */ ;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;
/*!50003 SET sql_mode              = '' */ ;
DELIMITER ;;
CREATE DEFINER=`analytics_dev`@`%` PROCEDURE `GetOneMetricChangeCountByPort`(
    IN fromTime INT,
    IN toTime INT,
    IN col VARCHAR(255),
    IN node VARCHAR(255),
    IN portNumber VARCHAR(255),
    IN partitionName VARCHAR(20)
)
BEGIN
SET
    @sqlQuery = CONCAT(
        '
SELECT 
  COUNT(*) AS count
FROM
  (
    SELECT
      (MEASURE_DATE DIV 30) * 30 AS time,
      MAX(COALESCE(',
        col,
        ', 0)) AS Value,
      MAX(COALESCE(',
        col,
        ', 0)) - LAG (MAX(COALESCE(',
        col,
        ', 0))) OVER (
        ORDER BY
          (MEASURE_DATE DIV 30) * 30
      ) AS ONEChange FROM (
        SELECT
          P2.ASSET_ID AS ASSET_ID,
          P2.PORT_NUMBER AS PORT_NUMBER,
          P2.MEASURE_DATE AS MEASURE_DATE,
                    COALESCE(
          (
          (
          case
                    when (P2.OPERATION_STATUS = ''up'') then (P2.MEASURE_DATE - P2.MEASURE_DATE_PREV)
                    else 0
          end
          ) / case
                    when (P2.ADMIN_STATUS = ''up'') then (P2.MEASURE_DATE - P2.MEASURE_DATE_PREV)
                    else NULL
          end
          ),
          NULL
          ) AS AVAILABILITY_FACTOR,
            CASE WHEN P2.ADMIN_STATUS <> ''up'' then NULL ELSE
          (
          1 - abs(
          COALESCE(
          (
          ((P2.RECEIVED_BPS - CASE WHEN P2.RECEIVED_BPS_PREV > P2.RECEIVED_BPS THEN 0 ELSE P2.RECEIVED_BPS_PREV END ) * 8) / (
          P2.NEGOTIATED_BPS * (P2.MEASURE_DATE - P2.MEASURE_DATE_PREV)
          )
          ),
          0
          )
          )
          ) END AS CAPACITY_FACTOR,
          CASE WHEN P2.ADMIN_STATUS <> ''up'' then NULL ELSE
          (
          COALESCE(
          (
          (
          (
          (
          P2.TOTAL_PACKETS_RECEIVED - CASE WHEN P2.TOTAL_PACKETS_RECEIVED_PREV > P2.TOTAL_PACKETS_RECEIVED THEN 0 ELSE P2.TOTAL_PACKETS_RECEIVED_PREV END
          ) - (P2.CRC_ERROR_COUNT - CASE WHEN P2.CRC_ERROR_COUNT_PREV > P2.CRC_ERROR_COUNT THEN 0 ELSE P2.CRC_ERROR_COUNT_PREV END)
          ) - (
          P2.COLLISION_ERROR_COUNT - CASE WHEN P2.COLLISION_ERROR_COUNT_PREV > P2.COLLISION_ERROR_COUNT THEN 0 ELSE P2.COLLISION_ERROR_COUNT_PREV END
          )
          ) / (
          P2.TOTAL_PACKETS_RECEIVED - CASE WHEN P2.TOTAL_PACKETS_RECEIVED_PREV > P2.TOTAL_PACKETS_RECEIVED THEN 0 ELSE P2.TOTAL_PACKETS_RECEIVED_PREV END
          )
          ),
          1
          )) END AS QUALITY_FACTOR
        from
          (
            select
              P1.ASSET_ID AS ASSET_ID,
              P1.PORT_NUMBER AS PORT_NUMBER,
              P1.OPERATION_STATUS AS OPERATION_STATUS,
              P1.ADMIN_STATUS AS ADMIN_STATUS,
              coalesce(
                lag (P1.OPERATION_STATUS) OVER (
                  PARTITION BY
                    P1.ASSET_ID,
                    P1.PORT_NUMBER
                  ORDER BY
                    P1.MEASURE_DATE
                ),
                P1.OPERATION_STATUS
              ) AS OPERATION_STATUS_PREV,
              P1.MEASURE_DATE AS MEASURE_DATE,
              coalesce(
                lag (P1.MEASURE_DATE) OVER (
                  PARTITION BY
                    P1.ASSET_ID,
                    P1.PORT_NUMBER
                  ORDER BY
                    P1.MEASURE_DATE
                ),
                P1.MEASURE_DATE
              ) AS MEASURE_DATE_PREV,
              coalesce(
                lead (P1.MEASURE_DATE) OVER (
                  PARTITION BY
                    P1.ASSET_ID,
                    P1.PORT_NUMBER
                  ORDER BY
                    P1.MEASURE_DATE
                ),
                P1.MEASURE_DATE
              ) AS MEASURE_DATE_FUTURE,
              P1.NEGOTIATED_BPS AS NEGOTIATED_BPS,
              coalesce(
                lag (P1.NEGOTIATED_BPS) OVER (
                  PARTITION BY
                    P1.ASSET_ID,
                    P1.PORT_NUMBER
                  ORDER BY
                    P1.MEASURE_DATE
                ),
                P1.NEGOTIATED_BPS
              ) AS NEGOTIATED_BPS_PREV,
              P1.RECEIVED_BPS AS RECEIVED_BPS,
              coalesce(
                lag (P1.RECEIVED_BPS) OVER (
                  PARTITION BY
                    P1.ASSET_ID,
                    P1.PORT_NUMBER
                  ORDER BY
                    P1.MEASURE_DATE
                ),
                P1.RECEIVED_BPS
              ) AS RECEIVED_BPS_PREV,
              P1.CRC_ERROR_COUNT AS CRC_ERROR_COUNT,
              coalesce(
                lag (P1.CRC_ERROR_COUNT) OVER (
                  PARTITION BY
                    P1.ASSET_ID,
                    P1.PORT_NUMBER
                  ORDER BY
                    P1.MEASURE_DATE
                ),
                P1.CRC_ERROR_COUNT
              ) AS CRC_ERROR_COUNT_PREV,
              P1.COLLISION_ERROR_COUNT AS COLLISION_ERROR_COUNT,
              coalesce(
                lag (P1.COLLISION_ERROR_COUNT) OVER (
                  PARTITION BY
                    P1.ASSET_ID,
                    P1.PORT_NUMBER
                  ORDER BY
                    P1.MEASURE_DATE
                ),
                P1.COLLISION_ERROR_COUNT
              ) AS COLLISION_ERROR_COUNT_PREV,
              P1.TOTAL_PACKETS_RECEIVED AS TOTAL_PACKETS_RECEIVED,
              coalesce(
                lag (P1.TOTAL_PACKETS_RECEIVED) OVER (
                  PARTITION BY
                    P1.ASSET_ID,
                    P1.PORT_NUMBER
                  ORDER BY
                    P1.MEASURE_DATE
                ),
                P1.TOTAL_PACKETS_RECEIVED
              ) AS TOTAL_PACKETS_RECEIVED_PREV
            from
              (
                select
                  analytics.ACTIVE_TELEMETRY.ASSET_ID AS ASSET_ID,
                  analytics.ACTIVE_TELEMETRY.PORT_NUMBER AS PORT_NUMBER,
                  analytics.ACTIVE_TELEMETRY.MEASURE_DATE AS MEASURE_DATE,
                  max(
                    (
                      case
                        when (
                          analytics.ACTIVE_TELEMETRY.MEASURE_TYPE = ''operation_status''
                        ) then analytics.ACTIVE_TELEMETRY.MEASURE_STRING
                        else ''''
                      end
                    )
                  ) AS OPERATION_STATUS,
                  max(
                    (
                      case
                        when (
                          analytics.ACTIVE_TELEMETRY.MEASURE_TYPE = ''negotiated_bps''
                        ) then analytics.ACTIVE_TELEMETRY.MEASURE_VALUE
                        else 0
                      end
                    )
                  ) AS NEGOTIATED_BPS,
                  max(
                    (
                      case
                        when (
                          analytics.ACTIVE_TELEMETRY.MEASURE_TYPE = ''received_bps''
                        ) then analytics.ACTIVE_TELEMETRY.MEASURE_VALUE
                        else 0
                      end
                    )
                  ) AS RECEIVED_BPS,
                  max(
                    (
                      case
                        when (
                          analytics.ACTIVE_TELEMETRY.MEASURE_TYPE = ''crc_error_count''
                        ) then analytics.ACTIVE_TELEMETRY.MEASURE_VALUE
                        else 0
                      end
                    )
                  ) AS CRC_ERROR_COUNT,
                  max(
                    (
                      case
                        when (
                          analytics.ACTIVE_TELEMETRY.MEASURE_TYPE = ''collision_error_count''
                        ) then analytics.ACTIVE_TELEMETRY.MEASURE_VALUE
                        else 0
                      end
                    )
                  ) AS COLLISION_ERROR_COUNT,
                  max(
                    (
                      case
                        when (
                          analytics.ACTIVE_TELEMETRY.MEASURE_TYPE = ''total_packets_recieved''
                        ) then analytics.ACTIVE_TELEMETRY.MEASURE_VALUE
                        else 0
                      end
                    )
                  ) AS TOTAL_PACKETS_RECEIVED,
                              max(
                              (
                              case
                                        when (
                                        analytics.ACTIVE_TELEMETRY.MEASURE_TYPE = ''admin_status''
                                        ) then analytics.ACTIVE_TELEMETRY.MEASURE_STRING
                                        else ''''
                              end
                              )
                              ) AS ADMIN_STATUS
                FROM
                  analytics.ACTIVE_TELEMETRY PARTITION (',
        partitionName,
        ')
                WHERE
                  PORT_NUMBER = ',
        portNumber,
        ' AND
          MEASURE_DATE BETWEEN ',
        fromTime,
        ' AND ',
        toTime,
        ' AND ASSET_ID like ''',
        node,
        '''
                group by
                  analytics.ACTIVE_TELEMETRY.ASSET_ID,
                  analytics.ACTIVE_TELEMETRY.PORT_NUMBER,
                  analytics.ACTIVE_TELEMETRY.MEASURE_DATE
              ) P1
              WHERE ADMIN_STATUS = ''up''
          ) P2
      ) P3
    GROUP BY
      time
    ORDER BY
      time
  ) Grouped
WHERE
  ONEChange >= 0.0001
  OR ONEChange <= -0.0001'
    );

PREPARE stmt
FROM
    @sqlQuery;

EXECUTE stmt;

DEALLOCATE PREPARE stmt;

END ;;
DELIMITER ;
/*!50003 SET sql_mode              = @saved_sql_mode */ ;
/*!50003 SET character_set_client  = @saved_cs_client */ ;
/*!50003 SET character_set_results = @saved_cs_results */ ;
/*!50003 SET collation_connection  = @saved_col_connection */ ;
/*!50003 DROP PROCEDURE IF EXISTS `GetOneMetricChangeCountBySwitch` */;
/*!50003 SET @saved_cs_client      = @@character_set_client */ ;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;
/*!50003 SET character_set_client  = latin1 */ ;
/*!50003 SET character_set_results = latin1 */ ;
/*!50003 SET collation_connection  = latin1_swedish_ci */ ;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;
/*!50003 SET sql_mode              = '' */ ;
DELIMITER ;;
CREATE DEFINER=`analytics_dev`@`%` PROCEDURE `GetOneMetricChangeCountBySwitch`(
    IN fromTime INT,
    IN toTime INT,
    IN col VARCHAR(255),
    IN node VARCHAR(255),
    IN partitionName VARCHAR(20)
)
BEGIN
SET
    @sqlQuery = CONCAT(
        '
SELECT 
  COUNT(*) AS count
FROM
  (
    SELECT
      (MEASURE_DATE DIV 30) * 30 AS time,
      MAX(COALESCE(',
        col,
        ', 0)) AS Value,
      MAX(COALESCE(',
        col,
        ', 0)) - LAG (MAX(COALESCE(',
        col,
        ', 0))) OVER (
        ORDER BY
          (MEASURE_DATE DIV 30) * 30
      ) AS ONEChange FROM (
        SELECT
          P2.ASSET_ID AS ASSET_ID,
          P2.PORT_NUMBER AS PORT_NUMBER,
          P2.MEASURE_DATE AS MEASURE_DATE,
          COALESCE(
          (
          (
          case
                    when (P2.OPERATION_STATUS = ''up'') then (P2.MEASURE_DATE - P2.MEASURE_DATE_PREV)
                    else 0
          end
          ) / case
                    when (P2.ADMIN_STATUS = ''up'') then (P2.MEASURE_DATE - P2.MEASURE_DATE_PREV)
                    else NULL
          end
          ),
          NULL
          ) AS AVAILABILITY_FACTOR,
            CASE WHEN P2.ADMIN_STATUS <> ''up'' then NULL ELSE
          (
          1 - abs(
          COALESCE(
          (
          ((P2.RECEIVED_BPS - CASE WHEN P2.RECEIVED_BPS_PREV > P2.RECEIVED_BPS THEN 0 ELSE P2.RECEIVED_BPS_PREV END ) * 8) / (
          P2.NEGOTIATED_BPS * (P2.MEASURE_DATE - P2.MEASURE_DATE_PREV)
          )
          ),
          0
          )
          )
          ) END AS CAPACITY_FACTOR,
          CASE WHEN P2.ADMIN_STATUS <> ''up'' then NULL ELSE
          (
          COALESCE(
          (
          (
          (
          (
          P2.TOTAL_PACKETS_RECEIVED - CASE WHEN P2.TOTAL_PACKETS_RECEIVED_PREV > P2.TOTAL_PACKETS_RECEIVED THEN 0 ELSE P2.TOTAL_PACKETS_RECEIVED_PREV END
          ) - (P2.CRC_ERROR_COUNT - CASE WHEN P2.CRC_ERROR_COUNT_PREV > P2.CRC_ERROR_COUNT THEN 0 ELSE P2.CRC_ERROR_COUNT_PREV END)
          ) - (
          P2.COLLISION_ERROR_COUNT - CASE WHEN P2.COLLISION_ERROR_COUNT_PREV > P2.COLLISION_ERROR_COUNT THEN 0 ELSE P2.COLLISION_ERROR_COUNT_PREV END
          )
          ) / (
          P2.TOTAL_PACKETS_RECEIVED - CASE WHEN P2.TOTAL_PACKETS_RECEIVED_PREV > P2.TOTAL_PACKETS_RECEIVED THEN 0 ELSE P2.TOTAL_PACKETS_RECEIVED_PREV END
          )
          ),
          1
          )) END AS QUALITY_FACTOR
        from
          (
            select
              P1.ASSET_ID AS ASSET_ID,
              P1.PORT_NUMBER AS PORT_NUMBER,
              P1.OPERATION_STATUS AS OPERATION_STATUS,
              P1.ADMIN_STATUS AS ADMIN_STATUS,
              coalesce(
                lag (P1.OPERATION_STATUS) OVER (
                  PARTITION BY
                    P1.ASSET_ID,
                    P1.PORT_NUMBER
                  ORDER BY
                    P1.MEASURE_DATE
                ),
                P1.OPERATION_STATUS
              ) AS OPERATION_STATUS_PREV,
              P1.MEASURE_DATE AS MEASURE_DATE,
              coalesce(
                lag (P1.MEASURE_DATE) OVER (
                  PARTITION BY
                    P1.ASSET_ID,
                    P1.PORT_NUMBER
                  ORDER BY
                    P1.MEASURE_DATE
                ),
                P1.MEASURE_DATE
              ) AS MEASURE_DATE_PREV,
              coalesce(
                lead (P1.MEASURE_DATE) OVER (
                  PARTITION BY
                    P1.ASSET_ID,
                    P1.PORT_NUMBER
                  ORDER BY
                    P1.MEASURE_DATE
                ),
                P1.MEASURE_DATE
              ) AS MEASURE_DATE_FUTURE,
              P1.NEGOTIATED_BPS AS NEGOTIATED_BPS,
              coalesce(
                lag (P1.NEGOTIATED_BPS) OVER (
                  PARTITION BY
                    P1.ASSET_ID,
                    P1.PORT_NUMBER
                  ORDER BY
                    P1.MEASURE_DATE
                ),
                P1.NEGOTIATED_BPS
              ) AS NEGOTIATED_BPS_PREV,
              P1.RECEIVED_BPS AS RECEIVED_BPS,
              coalesce(
                lag (P1.RECEIVED_BPS) OVER (
                  PARTITION BY
                    P1.ASSET_ID,
                    P1.PORT_NUMBER
                  ORDER BY
                    P1.MEASURE_DATE
                ),
                P1.RECEIVED_BPS
              ) AS RECEIVED_BPS_PREV,
              P1.CRC_ERROR_COUNT AS CRC_ERROR_COUNT,
              coalesce(
                lag (P1.CRC_ERROR_COUNT) OVER (
                  PARTITION BY
                    P1.ASSET_ID,
                    P1.PORT_NUMBER
                  ORDER BY
                    P1.MEASURE_DATE
                ),
                P1.CRC_ERROR_COUNT
              ) AS CRC_ERROR_COUNT_PREV,
              P1.COLLISION_ERROR_COUNT AS COLLISION_ERROR_COUNT,
              coalesce(
                lag (P1.COLLISION_ERROR_COUNT) OVER (
                  PARTITION BY
                    P1.ASSET_ID,
                    P1.PORT_NUMBER
                  ORDER BY
                    P1.MEASURE_DATE
                ),
                P1.COLLISION_ERROR_COUNT
              ) AS COLLISION_ERROR_COUNT_PREV,
              P1.TOTAL_PACKETS_RECEIVED AS TOTAL_PACKETS_RECEIVED,
              coalesce(
                lag (P1.TOTAL_PACKETS_RECEIVED) OVER (
                  PARTITION BY
                    P1.ASSET_ID,
                    P1.PORT_NUMBER
                  ORDER BY
                    P1.MEASURE_DATE
                ),
                P1.TOTAL_PACKETS_RECEIVED
              ) AS TOTAL_PACKETS_RECEIVED_PREV
            from
              (
                select
                  analytics.ACTIVE_TELEMETRY.ASSET_ID AS ASSET_ID,
                  analytics.ACTIVE_TELEMETRY.PORT_NUMBER AS PORT_NUMBER,
                  analytics.ACTIVE_TELEMETRY.MEASURE_DATE AS MEASURE_DATE,
                  max(
                    (
                      case
                        when (
                          analytics.ACTIVE_TELEMETRY.MEASURE_TYPE = ''operation_status''
                        ) then analytics.ACTIVE_TELEMETRY.MEASURE_STRING
                        else ''''
                      end
                    )
                  ) AS OPERATION_STATUS,
                  max(
                    (
                      case
                        when (
                          analytics.ACTIVE_TELEMETRY.MEASURE_TYPE = ''negotiated_bps''
                        ) then analytics.ACTIVE_TELEMETRY.MEASURE_VALUE
                        else 0
                      end
                    )
                  ) AS NEGOTIATED_BPS,
                  max(
                    (
                      case
                        when (
                          analytics.ACTIVE_TELEMETRY.MEASURE_TYPE = ''received_bps''
                        ) then analytics.ACTIVE_TELEMETRY.MEASURE_VALUE
                        else 0
                      end
                    )
                  ) AS RECEIVED_BPS,
                  max(
                    (
                      case
                        when (
                          analytics.ACTIVE_TELEMETRY.MEASURE_TYPE = ''crc_error_count''
                        ) then analytics.ACTIVE_TELEMETRY.MEASURE_VALUE
                        else 0
                      end
                    )
                  ) AS CRC_ERROR_COUNT,
                  max(
                    (
                      case
                        when (
                          analytics.ACTIVE_TELEMETRY.MEASURE_TYPE = ''collision_error_count''
                        ) then analytics.ACTIVE_TELEMETRY.MEASURE_VALUE
                        else 0
                      end
                    )
                  ) AS COLLISION_ERROR_COUNT,
                  max(
                    (
                      case
                        when (
                          analytics.ACTIVE_TELEMETRY.MEASURE_TYPE = ''total_packets_recieved''
                        ) then analytics.ACTIVE_TELEMETRY.MEASURE_VALUE
                        else 0
                      end
                    )
                  ) AS TOTAL_PACKETS_RECEIVED,
                              max(
                              (
                              case
                                        when (
                                        analytics.ACTIVE_TELEMETRY.MEASURE_TYPE = ''admin_status''
                                        ) then analytics.ACTIVE_TELEMETRY.MEASURE_STRING
                                        else ''''
                              end
                              )
                              ) AS ADMIN_STATUS
                FROM
                  analytics.ACTIVE_TELEMETRY PARTITION (',
        partitionName,
        ')
                WHERE
                  MEASURE_DATE BETWEEN ',
        fromTime,
        ' AND ',
        toTime,
        ' AND ASSET_ID like ''',
        node,
        '''
                group by
                  analytics.ACTIVE_TELEMETRY.ASSET_ID,
                  analytics.ACTIVE_TELEMETRY.PORT_NUMBER,
                  analytics.ACTIVE_TELEMETRY.MEASURE_DATE
              ) P1
              WHERE ADMIN_STATUS = ''up''
          ) P2
      ) P3
    GROUP BY
      time
    ORDER BY
      time
  ) Grouped
WHERE
  ONEChange >= 0.0001
  OR ONEChange <= -0.0001'
    );

PREPARE stmt
FROM
    @sqlQuery;

EXECUTE stmt;

DEALLOCATE PREPARE stmt;

END ;;
DELIMITER ;
/*!50003 SET sql_mode              = @saved_sql_mode */ ;
/*!50003 SET character_set_client  = @saved_cs_client */ ;
/*!50003 SET character_set_results = @saved_cs_results */ ;
/*!50003 SET collation_connection  = @saved_col_connection */ ;
/*!50003 DROP PROCEDURE IF EXISTS `GetOneMetricGraph` */;
/*!50003 SET @saved_cs_client      = @@character_set_client */ ;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;
/*!50003 SET character_set_client  = latin1 */ ;
/*!50003 SET character_set_results = latin1 */ ;
/*!50003 SET collation_connection  = latin1_swedish_ci */ ;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;
/*!50003 SET sql_mode              = '' */ ;
DELIMITER ;;
CREATE DEFINER=`analytics_dev`@`%` PROCEDURE `GetOneMetricGraph`(
    IN fromTime INT,
    IN toTime INT,
    IN subnetList TEXT,
    IN assetList TEXT,
    IN portList TEXT,
    IN partitionName VARCHAR(20)
)
BEGIN
SET
    @sqlQuery = CONCAT(
        '
                    SELECT 
    MEASURE_START_DATE DIV 30 * 30 AS time,
    (AVG(Availability * Capacity * Quality) - STDDEV_POP(Availability * Capacity * Quality) ) * 100 AS Overall,
    AVG(Availability) * 100 AS Availability,
    AVG(Capacity) * 100 AS Capacity,
    AVG(Quality) * 100 AS Quality
FROM(
SELECT 
      MEASURE_DATE  AS MEASURE_START_DATE ,
            AVG(AVAILABILITY_FACTOR)  AS Availability,
            AVG(CAPACITY_FACTOR) AS Capacity,
            AVG(QUALITY_FACTOR) AS Quality,
            ASSET_ID
    FROM
        (
        select
  P2.ASSET_ID AS ASSET_ID,
  P2.PORT_NUMBER AS PORT_NUMBER,
  P2.MEASURE_DATE AS MEASURE_DATE,
          COALESCE(
          (
          (
          case
                    when (P2.OPERATION_STATUS = ''up'') then (P2.MEASURE_DATE - P2.MEASURE_DATE_PREV)
                    else 0
          end
          ) / case
                    when (P2.ADMIN_STATUS = ''up'') then (P2.MEASURE_DATE - P2.MEASURE_DATE_PREV)
                    else NULL
          end
          ),
          NULL
          ) AS AVAILABILITY_FACTOR,
            CASE WHEN P2.ADMIN_STATUS <> ''up'' then NULL ELSE
          (
          1 - abs(
          COALESCE(
          (
          ((P2.RECEIVED_BPS - CASE WHEN P2.RECEIVED_BPS_PREV > P2.RECEIVED_BPS THEN 0 ELSE P2.RECEIVED_BPS_PREV END ) * 8) / (
          P2.NEGOTIATED_BPS * (P2.MEASURE_DATE - P2.MEASURE_DATE_PREV)
          )
          ),
          0
          )
          )
          ) END AS CAPACITY_FACTOR,
          CASE WHEN P2.ADMIN_STATUS <> ''up'' then NULL ELSE
          (
          COALESCE(
          (
          (
          (
          (
          P2.TOTAL_PACKETS_RECEIVED - CASE WHEN P2.TOTAL_PACKETS_RECEIVED_PREV > P2.TOTAL_PACKETS_RECEIVED THEN 0 ELSE P2.TOTAL_PACKETS_RECEIVED_PREV END
          ) - (P2.CRC_ERROR_COUNT - CASE WHEN P2.CRC_ERROR_COUNT_PREV > P2.CRC_ERROR_COUNT THEN 0 ELSE P2.CRC_ERROR_COUNT_PREV END)
          ) - (
          P2.COLLISION_ERROR_COUNT - CASE WHEN P2.COLLISION_ERROR_COUNT_PREV > P2.COLLISION_ERROR_COUNT THEN 0 ELSE P2.COLLISION_ERROR_COUNT_PREV END
          )
          ) / (
          P2.TOTAL_PACKETS_RECEIVED - CASE WHEN P2.TOTAL_PACKETS_RECEIVED_PREV > P2.TOTAL_PACKETS_RECEIVED THEN 0 ELSE P2.TOTAL_PACKETS_RECEIVED_PREV END
          )
          ),
          1
          )) END AS QUALITY_FACTOR
from
  (
    select
      P1.ASSET_ID AS ASSET_ID,
      P1.PORT_NUMBER AS PORT_NUMBER,
      P1.OPERATION_STATUS AS OPERATION_STATUS,
      P1.ADMIN_STATUS AS ADMIN_STATUS,
                    coalesce(
                    lag (P1.OPERATION_STATUS) OVER (
                    PARTITION BY P1.ASSET_ID,
                              P1.PORT_NUMBER
                    ORDER BY  P1.MEASURE_DATE
                    ),
                    P1.OPERATION_STATUS
                    ) AS OPERATION_STATUS_PREV,
                    P1.MEASURE_DATE AS MEASURE_DATE,
                    coalesce(
                    lag (P1.MEASURE_DATE) OVER (
                    PARTITION BY P1.ASSET_ID,
                              P1.PORT_NUMBER
                    ORDER BY  P1.MEASURE_DATE
                    ),
                    P1.MEASURE_DATE
                    ) AS MEASURE_DATE_PREV,
                    coalesce(
                    lead (P1.MEASURE_DATE) OVER (
                    PARTITION BY P1.ASSET_ID,
                              P1.PORT_NUMBER
                    ORDER BY  P1.MEASURE_DATE
                    ),
                    P1.MEASURE_DATE
                    ) AS MEASURE_DATE_FUTURE,
                    P1.NEGOTIATED_BPS AS NEGOTIATED_BPS,
                    coalesce(
                    lag (P1.NEGOTIATED_BPS) OVER (
                    PARTITION BY P1.ASSET_ID,
                              P1.PORT_NUMBER
                    ORDER BY  P1.MEASURE_DATE
                    ),
                    P1.NEGOTIATED_BPS
                    ) AS NEGOTIATED_BPS_PREV,
                    P1.RECEIVED_BPS AS RECEIVED_BPS,
                    coalesce(
                    lag (P1.RECEIVED_BPS) OVER (
                    PARTITION BY P1.ASSET_ID,
                              P1.PORT_NUMBER
                    ORDER BY  P1.MEASURE_DATE
                    ),
                    P1.RECEIVED_BPS
                    ) AS RECEIVED_BPS_PREV,
                    P1.CRC_ERROR_COUNT AS CRC_ERROR_COUNT,
                    coalesce(
                    lag (P1.CRC_ERROR_COUNT) OVER (
                    PARTITION BY P1.ASSET_ID,
                              P1.PORT_NUMBER
                    ORDER BY  P1.MEASURE_DATE
                    ),
                    P1.CRC_ERROR_COUNT
                    ) AS CRC_ERROR_COUNT_PREV,
                    P1.COLLISION_ERROR_COUNT AS COLLISION_ERROR_COUNT,
                    coalesce(
                    lag (P1.COLLISION_ERROR_COUNT) OVER (
                    PARTITION BY P1.ASSET_ID,
                              P1.PORT_NUMBER
                    ORDER BY  P1.MEASURE_DATE
                    ),
                    P1.COLLISION_ERROR_COUNT
                    ) AS COLLISION_ERROR_COUNT_PREV,
                    P1.TOTAL_PACKETS_RECEIVED AS TOTAL_PACKETS_RECEIVED,
                    coalesce(
                    lag (P1.TOTAL_PACKETS_RECEIVED) OVER (
                    PARTITION BY P1.ASSET_ID,
                              P1.PORT_NUMBER
                    ORDER BY  P1.MEASURE_DATE
                    ),
                    P1.TOTAL_PACKETS_RECEIVED
                    ) AS TOTAL_PACKETS_RECEIVED_PREV
    from
      (
        select
          analytics.ACTIVE_TELEMETRY.ASSET_ID AS ASSET_ID,
          analytics.ACTIVE_TELEMETRY.PORT_NUMBER AS PORT_NUMBER,
          analytics.ACTIVE_TELEMETRY.MEASURE_DATE AS MEASURE_DATE,
                              max(
                              (
                              case
                                        when (
                                        analytics.ACTIVE_TELEMETRY.MEASURE_TYPE = ''operation_status''
                                        ) then analytics.ACTIVE_TELEMETRY.MEASURE_STRING
                                        else ''''
                              end
                              )
                              ) AS OPERATION_STATUS,
                              max(
                              (
                              case
                                        when (
                                        analytics.ACTIVE_TELEMETRY.MEASURE_TYPE = ''negotiated_bps''
                                        ) then analytics.ACTIVE_TELEMETRY.MEASURE_VALUE
                                        else 0
                              end
                              )
                              ) AS NEGOTIATED_BPS,
                              max(
                              (
                              case
                                        when (
                                        analytics.ACTIVE_TELEMETRY.MEASURE_TYPE = ''received_bps''
                                        ) then analytics.ACTIVE_TELEMETRY.MEASURE_VALUE
                                        else 0
                              end
                              )
                              ) AS RECEIVED_BPS,
                              max(
                              (
                              case
                                        when (
                                        analytics.ACTIVE_TELEMETRY.MEASURE_TYPE = ''crc_error_count''
                                        ) then analytics.ACTIVE_TELEMETRY.MEASURE_VALUE
                                        else 0
                              end
                              )
                              ) AS CRC_ERROR_COUNT,
                              max(
                              (
                              case
                                        when (
                                        analytics.ACTIVE_TELEMETRY.MEASURE_TYPE = ''collision_error_count''
                                        ) then analytics.ACTIVE_TELEMETRY.MEASURE_VALUE
                                        else 0
                              end
                              )
                              ) AS COLLISION_ERROR_COUNT,
                              max(
                              (
                              case
                                        when (
                                        analytics.ACTIVE_TELEMETRY.MEASURE_TYPE = ''total_packets_recieved''
                                        ) then analytics.ACTIVE_TELEMETRY.MEASURE_VALUE
                                        else 0
                              end
                              )
                              ) AS TOTAL_PACKETS_RECEIVED,
                              max(
                              (
                              case
                                        when (
                                        analytics.ACTIVE_TELEMETRY.MEASURE_TYPE = ''admin_status''
                                        ) then analytics.ACTIVE_TELEMETRY.MEASURE_STRING
                                        else ''''
                              end
                              )
                              ) AS ADMIN_STATUS
        from
          analytics.ACTIVE_TELEMETRY PARTITION ( ',
        partitionName,
        ' )
    WHERE
      MEASURE_DATE BETWEEN ',
        fromTime,
        ' AND ',
        toTime,
        ' AND (',
                            IF(portList IS NOT NULL AND portList != '''', 
                                CONCAT('CONCAT(ASSET_ID, '':'', PORT_NUMBER) IN (', portList, ') OR '), 
                                ''
                            ),
                            IF(assetList IS NOT NULL AND assetList != '''', 
                                CONCAT('ASSET_ID IN (', assetList, ') OR '), 
                                ''
                            ),
                            IF(subnetList IS NOT NULL AND subnetList != '''', 
                                CONCAT('SUBSTRING_INDEX(ASSET_ID,''.'',3) IN (', subnetList, ')'), 
                                '1=0'
                            ),

                            IF(subnetList IS NULL AND assetList IS NULL AND portList IS NULL, '1=1', ''),
                        ')
        group by
          analytics.ACTIVE_TELEMETRY.ASSET_ID,
          analytics.ACTIVE_TELEMETRY.PORT_NUMBER,
          analytics.ACTIVE_TELEMETRY.MEASURE_DATE
      ) P1
      WHERE ADMIN_STATUS = ''up''
  ) P2
        ) P3
    WHERE
        AVAILABILITY_FACTOR IS NOT NULL AND CAPACITY_FACTOR IS NOT NULL AND QUALITY_FACTOR IS NOT NULL
    GROUP BY MEASURE_DATE,ASSET_ID
    ) P4
    GROUP BY time
    ORDER BY time'
    );

PREPARE stmt
FROM
    @sqlQuery;

EXECUTE stmt;

DEALLOCATE PREPARE stmt;

END ;;
DELIMITER ;
/*!50003 SET sql_mode              = @saved_sql_mode */ ;
/*!50003 SET character_set_client  = @saved_cs_client */ ;
/*!50003 SET character_set_results = @saved_cs_results */ ;
/*!50003 SET collation_connection  = @saved_col_connection */ ;
/*!50003 DROP PROCEDURE IF EXISTS `GetOneMetricGraphForPort` */;
/*!50003 SET @saved_cs_client      = @@character_set_client */ ;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;
/*!50003 SET character_set_client  = latin1 */ ;
/*!50003 SET character_set_results = latin1 */ ;
/*!50003 SET collation_connection  = latin1_swedish_ci */ ;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;
/*!50003 SET sql_mode              = '' */ ;
DELIMITER ;;
CREATE DEFINER=`analytics_dev`@`%` PROCEDURE `GetOneMetricGraphForPort`(
    IN fromTime INT,
    IN toTime INT,
    IN portNumber VARCHAR(255),
    IN assetId VARCHAR(255),
    IN partitionName VARCHAR(20)
)
BEGIN
SET
    @sqlQuery = CONCAT(
        '
SELECT 
    MEASURE_DATE DIV 30 * 30 AS time,
    MAX(AVAILABILITY_FACTOR * CAPACITY_FACTOR * QUALITY_FACTOR * 100)  AS Overall,
    MAX( AVAILABILITY_FACTOR * 100)  AS Availability,
    MAX(CAPACITY_FACTOR * 100)  AS Capacity,
    MAX(QUALITY_FACTOR * 100)  AS Quality
FROM
        (
        select
  P2.ASSET_ID AS ASSET_ID,
  P2.PORT_NUMBER AS PORT_NUMBER,
  P2.MEASURE_DATE AS MEASURE_DATE,
          COALESCE(
          (
          (
          case
                    when (P2.OPERATION_STATUS = ''up'') then (P2.MEASURE_DATE - P2.MEASURE_DATE_PREV)
                    else 0
          end
          ) / case
                    when (P2.ADMIN_STATUS = ''up'') then (P2.MEASURE_DATE - P2.MEASURE_DATE_PREV)
                    else NULL
          end
          ),
          NULL
          ) AS AVAILABILITY_FACTOR,
            CASE WHEN P2.ADMIN_STATUS <> ''up'' then NULL ELSE
          (
          1 - abs(
          COALESCE(
          (
          ((P2.RECEIVED_BPS - CASE WHEN P2.RECEIVED_BPS_PREV > P2.RECEIVED_BPS THEN 0 ELSE P2.RECEIVED_BPS_PREV END ) * 8) / (
          P2.NEGOTIATED_BPS * (P2.MEASURE_DATE - P2.MEASURE_DATE_PREV)
          )
          ),
          0
          )
          )
          ) END AS CAPACITY_FACTOR,
          CASE WHEN P2.ADMIN_STATUS <> ''up'' then NULL ELSE
          (
          COALESCE(
          (
          (
          (
          (
          P2.TOTAL_PACKETS_RECEIVED - CASE WHEN P2.TOTAL_PACKETS_RECEIVED_PREV > P2.TOTAL_PACKETS_RECEIVED THEN 0 ELSE P2.TOTAL_PACKETS_RECEIVED_PREV END
          ) - (P2.CRC_ERROR_COUNT - CASE WHEN P2.CRC_ERROR_COUNT_PREV > P2.CRC_ERROR_COUNT THEN 0 ELSE P2.CRC_ERROR_COUNT_PREV END)
          ) - (
          P2.COLLISION_ERROR_COUNT - CASE WHEN P2.COLLISION_ERROR_COUNT_PREV > P2.COLLISION_ERROR_COUNT THEN 0 ELSE P2.COLLISION_ERROR_COUNT_PREV END
          )
          ) / (
          P2.TOTAL_PACKETS_RECEIVED - CASE WHEN P2.TOTAL_PACKETS_RECEIVED_PREV > P2.TOTAL_PACKETS_RECEIVED THEN 0 ELSE P2.TOTAL_PACKETS_RECEIVED_PREV END
          )
          ),
          1
          )) END AS QUALITY_FACTOR
from
  (
    select
      P1.ASSET_ID AS ASSET_ID,
      P1.PORT_NUMBER AS PORT_NUMBER,
      P1.OPERATION_STATUS AS OPERATION_STATUS,
      P1.ADMIN_STATUS AS ADMIN_STATUS,
                    coalesce(
                    lag (P1.OPERATION_STATUS) OVER (
                    PARTITION BY P1.ASSET_ID,
                              P1.PORT_NUMBER
                    ORDER BY  P1.MEASURE_DATE
                    ),
                    P1.OPERATION_STATUS
                    ) AS OPERATION_STATUS_PREV,
                    P1.MEASURE_DATE AS MEASURE_DATE,
                    coalesce(
                    lag (P1.MEASURE_DATE) OVER (
                    PARTITION BY P1.ASSET_ID,
                              P1.PORT_NUMBER
                    ORDER BY  P1.MEASURE_DATE
                    ),
                    P1.MEASURE_DATE
                    ) AS MEASURE_DATE_PREV,
                    coalesce(
                    lead (P1.MEASURE_DATE) OVER (
                    PARTITION BY P1.ASSET_ID,
                              P1.PORT_NUMBER
                    ORDER BY  P1.MEASURE_DATE
                    ),
                    P1.MEASURE_DATE
                    ) AS MEASURE_DATE_FUTURE,
                    P1.NEGOTIATED_BPS AS NEGOTIATED_BPS,
                    coalesce(
                    lag (P1.NEGOTIATED_BPS) OVER (
                    PARTITION BY P1.ASSET_ID,
                              P1.PORT_NUMBER
                    ORDER BY  P1.MEASURE_DATE
                    ),
                    P1.NEGOTIATED_BPS
                    ) AS NEGOTIATED_BPS_PREV,
                    P1.RECEIVED_BPS AS RECEIVED_BPS,
                    coalesce(
                    lag (P1.RECEIVED_BPS) OVER (
                    PARTITION BY P1.ASSET_ID,
                              P1.PORT_NUMBER
                    ORDER BY  P1.MEASURE_DATE
                    ),
                    P1.RECEIVED_BPS
                    ) AS RECEIVED_BPS_PREV,
                    P1.CRC_ERROR_COUNT AS CRC_ERROR_COUNT,
                    coalesce(
                    lag (P1.CRC_ERROR_COUNT) OVER (
                    PARTITION BY P1.ASSET_ID,
                              P1.PORT_NUMBER
                    ORDER BY  P1.MEASURE_DATE
                    ),
                    P1.CRC_ERROR_COUNT
                    ) AS CRC_ERROR_COUNT_PREV,
                    P1.COLLISION_ERROR_COUNT AS COLLISION_ERROR_COUNT,
                    coalesce(
                    lag (P1.COLLISION_ERROR_COUNT) OVER (
                    PARTITION BY P1.ASSET_ID,
                              P1.PORT_NUMBER
                    ORDER BY  P1.MEASURE_DATE
                    ),
                    P1.COLLISION_ERROR_COUNT
                    ) AS COLLISION_ERROR_COUNT_PREV,
                    P1.TOTAL_PACKETS_RECEIVED AS TOTAL_PACKETS_RECEIVED,
                    coalesce(
                    lag (P1.TOTAL_PACKETS_RECEIVED) OVER (
                    PARTITION BY P1.ASSET_ID,
                              P1.PORT_NUMBER
                    ORDER BY  P1.MEASURE_DATE
                    ),
                    P1.TOTAL_PACKETS_RECEIVED
                    ) AS TOTAL_PACKETS_RECEIVED_PREV
    from
      (
        select
          analytics.ACTIVE_TELEMETRY.ASSET_ID AS ASSET_ID,
          analytics.ACTIVE_TELEMETRY.PORT_NUMBER AS PORT_NUMBER,
          analytics.ACTIVE_TELEMETRY.MEASURE_DATE AS MEASURE_DATE,
                              max(
                              (
                              case
                                        when (
                                        analytics.ACTIVE_TELEMETRY.MEASURE_TYPE = ''operation_status''
                                        ) then analytics.ACTIVE_TELEMETRY.MEASURE_STRING
                                        else ''''
                              end
                              )
                              ) AS OPERATION_STATUS,
                              max(
                              (
                              case
                                        when (
                                        analytics.ACTIVE_TELEMETRY.MEASURE_TYPE = ''negotiated_bps''
                                        ) then analytics.ACTIVE_TELEMETRY.MEASURE_VALUE
                                        else 0
                              end
                              )
                              ) AS NEGOTIATED_BPS,
                              max(
                              (
                              case
                                        when (
                                        analytics.ACTIVE_TELEMETRY.MEASURE_TYPE = ''received_bps''
                                        ) then analytics.ACTIVE_TELEMETRY.MEASURE_VALUE
                                        else 0
                              end
                              )
                              ) AS RECEIVED_BPS,
                              max(
                              (
                              case
                                        when (
                                        analytics.ACTIVE_TELEMETRY.MEASURE_TYPE = ''crc_error_count''
                                        ) then analytics.ACTIVE_TELEMETRY.MEASURE_VALUE
                                        else 0
                              end
                              )
                              ) AS CRC_ERROR_COUNT,
                              max(
                              (
                              case
                                        when (
                                        analytics.ACTIVE_TELEMETRY.MEASURE_TYPE = ''collision_error_count''
                                        ) then analytics.ACTIVE_TELEMETRY.MEASURE_VALUE
                                        else 0
                              end
                              )
                              ) AS COLLISION_ERROR_COUNT,
                              max(
                              (
                              case
                                        when (
                                        analytics.ACTIVE_TELEMETRY.MEASURE_TYPE = ''total_packets_recieved''
                                        ) then analytics.ACTIVE_TELEMETRY.MEASURE_VALUE
                                        else 0
                              end
                              )
                              ) AS TOTAL_PACKETS_RECEIVED,
                              max(
                              (
                              case
                                        when (
                                        analytics.ACTIVE_TELEMETRY.MEASURE_TYPE = ''admin_status''
                                        ) then analytics.ACTIVE_TELEMETRY.MEASURE_STRING
                                        else ''''
                              end
                              )
                              ) AS ADMIN_STATUS

        from
          analytics.ACTIVE_TELEMETRY  PARTITION ( ',
        partitionName,
        ' )
    WHERE
      MEASURE_DATE BETWEEN ',
        fromTime,
        ' AND ',
        toTime,
        ' AND ASSET_ID = ''',
        assetId,
        '''  AND
      PORT_NUMBER = ',
        portNumber,
        ' 
        group by
          analytics.ACTIVE_TELEMETRY.ASSET_ID,
          analytics.ACTIVE_TELEMETRY.PORT_NUMBER,
          analytics.ACTIVE_TELEMETRY.MEASURE_DATE
      ) P1
      WHERE ADMIN_STATUS = ''up''
  ) P2
        ) P3
    WHERE
             AVAILABILITY_FACTOR IS NOT NULL
            AND CAPACITY_FACTOR IS NOT NULL
            AND QUALITY_FACTOR IS NOT NULL
    GROUP BY 1
ORDER BY 1'
    );

PREPARE stmt
FROM
    @sqlQuery;

EXECUTE stmt;

DEALLOCATE PREPARE stmt;

END ;;
DELIMITER ;
/*!50003 SET sql_mode              = @saved_sql_mode */ ;
/*!50003 SET character_set_client  = @saved_cs_client */ ;
/*!50003 SET character_set_results = @saved_cs_results */ ;
/*!50003 SET collation_connection  = @saved_col_connection */ ;
/*!50003 DROP PROCEDURE IF EXISTS `GetOneMetricGraphForRoot` */;
/*!50003 SET @saved_cs_client      = @@character_set_client */ ;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;
/*!50003 SET character_set_client  = latin1 */ ;
/*!50003 SET character_set_results = latin1 */ ;
/*!50003 SET collation_connection  = latin1_swedish_ci */ ;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;
/*!50003 SET sql_mode              = '' */ ;
DELIMITER ;;
CREATE DEFINER=`analytics_dev`@`%` PROCEDURE `GetOneMetricGraphForRoot`(
    IN fromTime INT,
    IN toTime INT,
    IN partitionName VARCHAR(20)
)
BEGIN
SET
    @sqlQuery = CONCAT(
        'SELECT 
    MEASURE_DATE DIV 30 * 30 AS time,
    (AVG(AVAILABILITY_FACTOR * CAPACITY_FACTOR * QUALITY_FACTOR)  - STDDEV_POP(AVAILABILITY_FACTOR * CAPACITY_FACTOR * QUALITY_FACTOR)) * 100 AS Overall,
    AVG(AVAILABILITY_FACTOR) * 100 AS Availability,
    AVG(CAPACITY_FACTOR) * 100 AS Capacity,
    AVG(QUALITY_FACTOR) * 100 AS Quality
FROM
(
select
  P2.ASSET_ID AS ASSET_ID,
  P2.MEASURE_DATE ,
          AVG(COALESCE(
          (
          (
          case
                    when (P2.OPERATION_STATUS = ''up'') then (P2.MEASURE_DATE - P2.MEASURE_DATE_PREV)
                    else 0
          end
          ) / case
                    when (P2.ADMIN_STATUS = ''up'') then (P2.MEASURE_DATE - P2.MEASURE_DATE_PREV)
                    else NULL
          end
          ),
          NULL
          )) AS AVAILABILITY_FACTOR,
      AVG(CASE WHEN P2.ADMIN_STATUS <> ''up'' then NULL ELSE
          (
          1 - abs(
          COALESCE(
          (
          ((P2.RECEIVED_BPS - CASE WHEN P2.RECEIVED_BPS_PREV > P2.RECEIVED_BPS THEN 0 ELSE P2.RECEIVED_BPS_PREV END ) * 8) / (
          P2.NEGOTIATED_BPS * (P2.MEASURE_DATE - P2.MEASURE_DATE_PREV)
          )
          ),
          0
          )
          )
          ) END) AS CAPACITY_FACTOR,
          AVG(CASE WHEN P2.ADMIN_STATUS <> ''up'' then NULL ELSE
          (
          COALESCE(
          (
          (
          (
          (
          P2.TOTAL_PACKETS_RECEIVED - CASE WHEN P2.TOTAL_PACKETS_RECEIVED_PREV > P2.TOTAL_PACKETS_RECEIVED THEN 0 ELSE P2.TOTAL_PACKETS_RECEIVED_PREV END
          ) - (P2.CRC_ERROR_COUNT - CASE WHEN P2.CRC_ERROR_COUNT_PREV > P2.CRC_ERROR_COUNT THEN 0 ELSE P2.CRC_ERROR_COUNT_PREV END)
          ) - (
          P2.COLLISION_ERROR_COUNT - CASE WHEN P2.COLLISION_ERROR_COUNT_PREV > P2.COLLISION_ERROR_COUNT THEN 0 ELSE P2.COLLISION_ERROR_COUNT_PREV END
          )
          ) / (
          P2.TOTAL_PACKETS_RECEIVED - CASE WHEN P2.TOTAL_PACKETS_RECEIVED_PREV > P2.TOTAL_PACKETS_RECEIVED THEN 0 ELSE P2.TOTAL_PACKETS_RECEIVED_PREV END
          )
          ),
          1
          )) END) AS QUALITY_FACTOR
from
  (
    select
      P1.ASSET_ID AS ASSET_ID,
      P1.PORT_NUMBER AS PORT_NUMBER,
      P1.OPERATION_STATUS AS OPERATION_STATUS,
      P1.ADMIN_STATUS AS ADMIN_STATUS,
                    coalesce(
                    lag (P1.OPERATION_STATUS) OVER (
                    PARTITION BY P1.ASSET_ID,
                              P1.PORT_NUMBER
                    ORDER BY  P1.MEASURE_DATE
                    ),
                    P1.OPERATION_STATUS
                    ) AS OPERATION_STATUS_PREV,
                    P1.MEASURE_DATE AS MEASURE_DATE,
                    coalesce(
                    lag (P1.MEASURE_DATE) OVER (
                    PARTITION BY P1.ASSET_ID,
                              P1.PORT_NUMBER
                    ORDER BY  P1.MEASURE_DATE
                    ),
                    P1.MEASURE_DATE
                    ) AS MEASURE_DATE_PREV,
                    coalesce(
                    lead (P1.MEASURE_DATE) OVER (
                    PARTITION BY P1.ASSET_ID,
                              P1.PORT_NUMBER
                    ORDER BY  P1.MEASURE_DATE
                    ),
                    P1.MEASURE_DATE
                    ) AS MEASURE_DATE_FUTURE,
                    P1.NEGOTIATED_BPS AS NEGOTIATED_BPS,
                    coalesce(
                    lag (P1.NEGOTIATED_BPS) OVER (
                    PARTITION BY P1.ASSET_ID,
                              P1.PORT_NUMBER
                    ORDER BY  P1.MEASURE_DATE
                    ),
                    P1.NEGOTIATED_BPS
                    ) AS NEGOTIATED_BPS_PREV,
                    P1.RECEIVED_BPS AS RECEIVED_BPS,
                    coalesce(
                    lag (P1.RECEIVED_BPS) OVER (
                    PARTITION BY P1.ASSET_ID,
                              P1.PORT_NUMBER
                    ORDER BY  P1.MEASURE_DATE
                    ),
                    P1.RECEIVED_BPS
                    ) AS RECEIVED_BPS_PREV,
                    P1.CRC_ERROR_COUNT AS CRC_ERROR_COUNT,
                    coalesce(
                    lag (P1.CRC_ERROR_COUNT) OVER (
                    PARTITION BY P1.ASSET_ID,
                              P1.PORT_NUMBER
                    ORDER BY  P1.MEASURE_DATE
                    ),
                    P1.CRC_ERROR_COUNT
                    ) AS CRC_ERROR_COUNT_PREV,
                    P1.COLLISION_ERROR_COUNT AS COLLISION_ERROR_COUNT,
                    coalesce(
                    lag (P1.COLLISION_ERROR_COUNT) OVER (
                    PARTITION BY P1.ASSET_ID,
                              P1.PORT_NUMBER
                    ORDER BY  P1.MEASURE_DATE
                    ),
                    P1.COLLISION_ERROR_COUNT
                    ) AS COLLISION_ERROR_COUNT_PREV,
                    P1.TOTAL_PACKETS_RECEIVED AS TOTAL_PACKETS_RECEIVED,
                    coalesce(
                    lag (P1.TOTAL_PACKETS_RECEIVED) OVER (
                    PARTITION BY P1.ASSET_ID,
                              P1.PORT_NUMBER
                    ORDER BY  P1.MEASURE_DATE
                    ),
                    P1.TOTAL_PACKETS_RECEIVED
                    ) AS TOTAL_PACKETS_RECEIVED_PREV
    from
      (
        select
          analytics.ACTIVE_TELEMETRY.ASSET_ID AS ASSET_ID,
          analytics.ACTIVE_TELEMETRY.PORT_NUMBER AS PORT_NUMBER,
          analytics.ACTIVE_TELEMETRY.MEASURE_DATE AS MEASURE_DATE,
                              max(
                              (
                              case
                                        when (
                                        analytics.ACTIVE_TELEMETRY.MEASURE_TYPE = ''operation_status''
                                        ) then analytics.ACTIVE_TELEMETRY.MEASURE_STRING
                                        else ''''
                              end
                              )
                              ) AS OPERATION_STATUS,
                              max(
                              (
                              case
                                        when (
                                        analytics.ACTIVE_TELEMETRY.MEASURE_TYPE = ''negotiated_bps''
                                        ) then analytics.ACTIVE_TELEMETRY.MEASURE_VALUE
                                        else 0
                              end
                              )
                              ) AS NEGOTIATED_BPS,
                              max(
                              (
                              case
                                        when (
                                        analytics.ACTIVE_TELEMETRY.MEASURE_TYPE = ''received_bps''
                                        ) then analytics.ACTIVE_TELEMETRY.MEASURE_VALUE
                                        else 0
                              end
                              )
                              ) AS RECEIVED_BPS,
                              max(
                              (
                              case
                                        when (
                                        analytics.ACTIVE_TELEMETRY.MEASURE_TYPE = ''crc_error_count''
                                        ) then analytics.ACTIVE_TELEMETRY.MEASURE_VALUE
                                        else 0
                              end
                              )
                              ) AS CRC_ERROR_COUNT,
                              max(
                              (
                              case
                                        when (
                                        analytics.ACTIVE_TELEMETRY.MEASURE_TYPE = ''collision_error_count''
                                        ) then analytics.ACTIVE_TELEMETRY.MEASURE_VALUE
                                        else 0
                              end
                              )
                              ) AS COLLISION_ERROR_COUNT,
                              max(
                              (
                              case
                                        when (
                                        analytics.ACTIVE_TELEMETRY.MEASURE_TYPE = ''total_packets_recieved''
                                        ) then analytics.ACTIVE_TELEMETRY.MEASURE_VALUE
                                        else 0
                              end
                              )
                              ) AS TOTAL_PACKETS_RECEIVED,
                              max(
                              (
                              case
                                        when (
                                        analytics.ACTIVE_TELEMETRY.MEASURE_TYPE = ''admin_status''
                                        ) then analytics.ACTIVE_TELEMETRY.MEASURE_STRING
                                        else ''''
                              end
                              )
                              ) AS ADMIN_STATUS
        from
          analytics.ACTIVE_TELEMETRY  PARTITION ( ',
        partitionName,
        ' )
    WHERE
      MEASURE_DATE BETWEEN ',
        fromTime,
        ' AND ',
        toTime,
        '
        group by
          analytics.ACTIVE_TELEMETRY.ASSET_ID,
          analytics.ACTIVE_TELEMETRY.PORT_NUMBER,
          analytics.ACTIVE_TELEMETRY.MEASURE_DATE
      ) P1
      WHERE ADMIN_STATUS = ''up''
  ) P2
GROUP BY MEASURE_DATE , ASSET_ID
)
P3
  WHERE AVAILABILITY_FACTOR IS NOT NULL
            AND CAPACITY_FACTOR IS NOT NULL
            AND QUALITY_FACTOR IS NOT NULL
GROUP BY 1
ORDER BY time'
    );

PREPARE stmt
FROM
    @sqlQuery;

EXECUTE stmt;

DEALLOCATE PREPARE stmt;

END ;;
DELIMITER ;
/*!50003 SET sql_mode              = @saved_sql_mode */ ;
/*!50003 SET character_set_client  = @saved_cs_client */ ;
/*!50003 SET character_set_results = @saved_cs_results */ ;
/*!50003 SET collation_connection  = @saved_col_connection */ ;
/*!50003 DROP PROCEDURE IF EXISTS `GetOneMetricGraphForRootTuned` */;
/*!50003 SET @saved_cs_client      = @@character_set_client */ ;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;
/*!50003 SET character_set_client  = utf8mb4 */ ;
/*!50003 SET character_set_results = utf8mb4 */ ;
/*!50003 SET collation_connection  = utf8mb4_0900_ai_ci */ ;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;
/*!50003 SET sql_mode              = '' */ ;
DELIMITER ;;
CREATE DEFINER=`analytics_dev`@`%` PROCEDURE `GetOneMetricGraphForRootTuned`(
    IN fromTime INT,
    IN toTime INT,
    IN partitionName VARCHAR(20)
)
BEGIN
SET
    @sqlQuery = CONCAT(
        'SELECT 
    MEASURE_DATE DIV 30 * 30 AS time,
    (AVG(AVAILABILITY_FACTOR * CAPACITY_FACTOR * QUALITY_FACTOR)  - STDDEV_POP(AVAILABILITY_FACTOR * CAPACITY_FACTOR * QUALITY_FACTOR)) * 100 AS Overall,
    AVG(AVAILABILITY_FACTOR) * 100 AS Availability,
    AVG(CAPACITY_FACTOR) * 100 AS Capacity,
    AVG(QUALITY_FACTOR) * 100 AS Quality
FROM
(
select
  P2.ASSET_ID AS ASSET_ID,
  P2.MEASURE_DATE ,
          AVG(COALESCE(
          (
          (
          case
                    when (P2.OPERATION_STATUS = ''up'') then (P2.MEASURE_DATE - P2.MEASURE_DATE_PREV)
                    else 0
          end
          ) / case
                    when (P2.ADMIN_STATUS = ''up'') then (P2.MEASURE_DATE - P2.MEASURE_DATE_PREV)
                    else NULL
          end
          ),
          NULL
          )) AS AVAILABILITY_FACTOR,
      AVG(CASE WHEN P2.ADMIN_STATUS <> ''up'' then NULL ELSE
          (
          1 - abs(
          COALESCE(
          (
          ((P2.RECEIVED_BPS - CASE WHEN P2.RECEIVED_BPS_PREV > P2.RECEIVED_BPS THEN 0 ELSE P2.RECEIVED_BPS_PREV END ) * 8) / (
          P2.NEGOTIATED_BPS * (P2.MEASURE_DATE - P2.MEASURE_DATE_PREV)
          )
          ),
          0
          )
          )
          ) END) AS CAPACITY_FACTOR,
          AVG(CASE WHEN P2.ADMIN_STATUS <> ''up'' then NULL ELSE
          (
          COALESCE(
          (
          (
          (
          (
          P2.TOTAL_PACKETS_RECEIVED - CASE WHEN P2.TOTAL_PACKETS_RECEIVED_PREV > P2.TOTAL_PACKETS_RECEIVED THEN 0 ELSE P2.TOTAL_PACKETS_RECEIVED_PREV END
          ) - (P2.CRC_ERROR_COUNT - CASE WHEN P2.CRC_ERROR_COUNT_PREV > P2.CRC_ERROR_COUNT THEN 0 ELSE P2.CRC_ERROR_COUNT_PREV END)
          ) - (
          P2.COLLISION_ERROR_COUNT - CASE WHEN P2.COLLISION_ERROR_COUNT_PREV > P2.COLLISION_ERROR_COUNT THEN 0 ELSE P2.COLLISION_ERROR_COUNT_PREV END
          )
          ) / (
          P2.TOTAL_PACKETS_RECEIVED - CASE WHEN P2.TOTAL_PACKETS_RECEIVED_PREV > P2.TOTAL_PACKETS_RECEIVED THEN 0 ELSE P2.TOTAL_PACKETS_RECEIVED_PREV END
          )
          ),
          1
          )) END) AS QUALITY_FACTOR
from
  (
    select
      P1.ASSET_ID AS ASSET_ID,
      P1.PORT_NUMBER AS PORT_NUMBER,
      P1.OPERATION_STATUS AS OPERATION_STATUS,
      P1.ADMIN_STATUS AS ADMIN_STATUS,
                    coalesce(
                    lag (P1.OPERATION_STATUS) OVER (
                    PARTITION BY P1.ASSET_ID,
                              P1.PORT_NUMBER
                    ORDER BY  P1.MEASURE_DATE
                    ),
                    P1.OPERATION_STATUS
                    ) AS OPERATION_STATUS_PREV,
                    P1.MEASURE_DATE AS MEASURE_DATE,
                    coalesce(
                    lag (P1.MEASURE_DATE) OVER (
                    PARTITION BY P1.ASSET_ID,
                              P1.PORT_NUMBER
                    ORDER BY  P1.MEASURE_DATE
                    ),
                    P1.MEASURE_DATE
                    ) AS MEASURE_DATE_PREV,
                    coalesce(
                    lead (P1.MEASURE_DATE) OVER (
                    PARTITION BY P1.ASSET_ID,
                              P1.PORT_NUMBER
                    ORDER BY  P1.MEASURE_DATE
                    ),
                    P1.MEASURE_DATE
                    ) AS MEASURE_DATE_FUTURE,
                    P1.NEGOTIATED_BPS AS NEGOTIATED_BPS,
                    coalesce(
                    lag (P1.NEGOTIATED_BPS) OVER (
                    PARTITION BY P1.ASSET_ID,
                              P1.PORT_NUMBER
                    ORDER BY  P1.MEASURE_DATE
                    ),
                    P1.NEGOTIATED_BPS
                    ) AS NEGOTIATED_BPS_PREV,
                    P1.RECEIVED_BPS AS RECEIVED_BPS,
                    coalesce(
                    lag (P1.RECEIVED_BPS) OVER (
                    PARTITION BY P1.ASSET_ID,
                              P1.PORT_NUMBER
                    ORDER BY  P1.MEASURE_DATE
                    ),
                    P1.RECEIVED_BPS
                    ) AS RECEIVED_BPS_PREV,
                    P1.CRC_ERROR_COUNT AS CRC_ERROR_COUNT,
                    coalesce(
                    lag (P1.CRC_ERROR_COUNT) OVER (
                    PARTITION BY P1.ASSET_ID,
                              P1.PORT_NUMBER
                    ORDER BY  P1.MEASURE_DATE
                    ),
                    P1.CRC_ERROR_COUNT
                    ) AS CRC_ERROR_COUNT_PREV,
                    P1.COLLISION_ERROR_COUNT AS COLLISION_ERROR_COUNT,
                    coalesce(
                    lag (P1.COLLISION_ERROR_COUNT) OVER (
                    PARTITION BY P1.ASSET_ID,
                              P1.PORT_NUMBER
                    ORDER BY  P1.MEASURE_DATE
                    ),
                    P1.COLLISION_ERROR_COUNT
                    ) AS COLLISION_ERROR_COUNT_PREV,
                    P1.TOTAL_PACKETS_RECEIVED AS TOTAL_PACKETS_RECEIVED,
                    coalesce(
                    lag (P1.TOTAL_PACKETS_RECEIVED) OVER (
                    PARTITION BY P1.ASSET_ID,
                              P1.PORT_NUMBER
                    ORDER BY  P1.MEASURE_DATE
                    ),
                    P1.TOTAL_PACKETS_RECEIVED
                    ) AS TOTAL_PACKETS_RECEIVED_PREV
    from
      (
        select
          analytics.ACTIVE_TELEMETRY.ASSET_ID AS ASSET_ID,
          analytics.ACTIVE_TELEMETRY.PORT_NUMBER AS PORT_NUMBER,
          analytics.ACTIVE_TELEMETRY.MEASURE_DATE AS MEASURE_DATE,
                              max(
                              (
                              case
                                        when (
                                        analytics.ACTIVE_TELEMETRY.MEASURE_TYPE = ''operation_status''
                                        ) then analytics.ACTIVE_TELEMETRY.MEASURE_STRING
                                        else ''''
                              end
                              )
                              ) AS OPERATION_STATUS,
                              max(
                              (
                              case
                                        when (
                                        analytics.ACTIVE_TELEMETRY.MEASURE_TYPE = ''negotiated_bps''
                                        ) then analytics.ACTIVE_TELEMETRY.MEASURE_VALUE
                                        else 0
                              end
                              )
                              ) AS NEGOTIATED_BPS,
                              max(
                              (
                              case
                                        when (
                                        analytics.ACTIVE_TELEMETRY.MEASURE_TYPE = ''received_bps''
                                        ) then analytics.ACTIVE_TELEMETRY.MEASURE_VALUE
                                        else 0
                              end
                              )
                              ) AS RECEIVED_BPS,
                              max(
                              (
                              case
                                        when (
                                        analytics.ACTIVE_TELEMETRY.MEASURE_TYPE = ''crc_error_count''
                                        ) then analytics.ACTIVE_TELEMETRY.MEASURE_VALUE
                                        else 0
                              end
                              )
                              ) AS CRC_ERROR_COUNT,
                              max(
                              (
                              case
                                        when (
                                        analytics.ACTIVE_TELEMETRY.MEASURE_TYPE = ''collision_error_count''
                                        ) then analytics.ACTIVE_TELEMETRY.MEASURE_VALUE
                                        else 0
                              end
                              )
                              ) AS COLLISION_ERROR_COUNT,
                              max(
                              (
                              case
                                        when (
                                        analytics.ACTIVE_TELEMETRY.MEASURE_TYPE = ''total_packets_recieved''
                                        ) then analytics.ACTIVE_TELEMETRY.MEASURE_VALUE
                                        else 0
                              end
                              )
                              ) AS TOTAL_PACKETS_RECEIVED,
                              max(
                              (
                              case
                                        when (
                                        analytics.ACTIVE_TELEMETRY.MEASURE_TYPE = ''admin_status''
                                        ) then analytics.ACTIVE_TELEMETRY.MEASURE_STRING
                                        else ''''
                              end
                              )
                              ) AS ADMIN_STATUS
        from
          analytics.ACTIVE_TELEMETRY  PARTITION ( ',
        partitionName,
        ' )
    WHERE
      MEASURE_DATE BETWEEN ',
        fromTime,
        ' AND ',
        toTime,
        '
        group by
          analytics.ACTIVE_TELEMETRY.ASSET_ID,
          analytics.ACTIVE_TELEMETRY.PORT_NUMBER,
          analytics.ACTIVE_TELEMETRY.MEASURE_DATE
      ) P1
      WHERE ADMIN_STATUS = ''up''
  ) P2
GROUP BY MEASURE_DATE , ASSET_ID
)
P3
  WHERE AVAILABILITY_FACTOR IS NOT NULL
            AND CAPACITY_FACTOR IS NOT NULL
            AND QUALITY_FACTOR IS NOT NULL
GROUP BY 1
ORDER BY time'
    );

PREPARE stmt
FROM
    @sqlQuery;

EXECUTE stmt;

DEALLOCATE PREPARE stmt;

END ;;
DELIMITER ;
/*!50003 SET sql_mode              = @saved_sql_mode */ ;
/*!50003 SET character_set_client  = @saved_cs_client */ ;
/*!50003 SET character_set_results = @saved_cs_results */ ;
/*!50003 SET collation_connection  = @saved_col_connection */ ;
/*!50003 DROP PROCEDURE IF EXISTS `GetOneMetricTree` */;
/*!50003 SET @saved_cs_client      = @@character_set_client */ ;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;
/*!50003 SET character_set_client  = latin1 */ ;
/*!50003 SET character_set_results = latin1 */ ;
/*!50003 SET collation_connection  = latin1_swedish_ci */ ;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;
/*!50003 SET sql_mode              = '' */ ;
DELIMITER ;;
CREATE DEFINER=`analytics_dev`@`%` PROCEDURE `GetOneMetricTree`(
    IN fromTime INT,
    IN toTime INT,
    IN partitionName VARCHAR(20)
)
BEGIN
SET
    @sqlQuery = CONCAT(
        ' SELECT subnet,switch,port FROM (
SELECT  SUBSTRING_INDEX(ASSET_ID, ".", 3) AS subnet, ASSET_ID as switch ,PORT_NUMBER as port,
                              max(
                              (
                              case
                                        when (
                                        analytics.ACTIVE_TELEMETRY.MEASURE_TYPE = ''admin_status''
                                        ) then analytics.ACTIVE_TELEMETRY.MEASURE_STRING
                                        else ''''
                              end
                              )
                              ) AS ADMIN_STATUS
FROM analytics.ACTIVE_TELEMETRY  PARTITION ( ',
        partitionName,
        ' )
WHERE MEASURE_DATE BETWEEN ',
        fromTime,
        ' AND ',
        toTime,
        '
GROUP BY ASSET_ID,PORT_NUMBER) P1 
WHERE ADMIN_STATUS = ''up''
'
    );

PREPARE stmt
FROM
    @sqlQuery;

EXECUTE stmt;

DEALLOCATE PREPARE stmt;

END ;;
DELIMITER ;
/*!50003 SET sql_mode              = @saved_sql_mode */ ;
/*!50003 SET character_set_client  = @saved_cs_client */ ;
/*!50003 SET character_set_results = @saved_cs_results */ ;
/*!50003 SET collation_connection  = @saved_col_connection */ ;
/*!50003 DROP PROCEDURE IF EXISTS `GetOneMetricValues` */;
/*!50003 SET @saved_cs_client      = @@character_set_client */ ;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;
/*!50003 SET character_set_client  = latin1 */ ;
/*!50003 SET character_set_results = latin1 */ ;
/*!50003 SET collation_connection  = latin1_swedish_ci */ ;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;
/*!50003 SET sql_mode              = '' */ ;
DELIMITER ;;
CREATE DEFINER=`analytics_dev`@`%` PROCEDURE `GetOneMetricValues`(
    IN fromTime INT,
    IN toTime INT,
    IN subnetList TEXT,
    IN assetList TEXT,
    IN portList TEXT,
    IN partitionName VARCHAR(20)
)
BEGIN
SET
    @sqlQuery = CONCAT(
        '
SELECT 
    (AVG(AVAILABILITY_FACTOR * CAPACITY_FACTOR * QUALITY_FACTOR) - STDDEV_POP(AVAILABILITY_FACTOR * CAPACITY_FACTOR * QUALITY_FACTOR) ) * 100 AS Overall,
	QUALITY_FACTOR * 100 AS quality,
    CAPACITY_FACTOR * 100 AS capacity,
    AVAILABILITY_FACTOR * 100 AS available
	from ( select
    AVG(QUALITY_FACTOR) as QUALITY_FACTOR,
    AVG(CAPACITY_FACTOR)  AS CAPACITY_FACTOR,
    AVG(AVAILABILITY_FACTOR)  AS AVAILABILITY_FACTOR    
FROM
-- GIVES AVAILABILITY,CAPACITY,QUALITY
(
select COALESCE(
          ((case when (P2.OPERATION_STATUS = ''up'') then (P2.MEASURE_DATE - P2.MEASURE_DATE_PREV) else 0 end
          ) / case when (P2.ADMIN_STATUS = ''up'') then (P2.MEASURE_DATE - P2.MEASURE_DATE_PREV) else NULL end
          ),NULL) AS AVAILABILITY_FACTOR,

          CASE WHEN P2.ADMIN_STATUS <> ''up'' then NULL ELSE
          (1 - abs(COALESCE((((P2.RECEIVED_BPS - CASE WHEN P2.RECEIVED_BPS_PREV > P2.RECEIVED_BPS THEN 0 ELSE P2.RECEIVED_BPS_PREV END ) * 8) / 
          (P2.NEGOTIATED_BPS * (P2.MEASURE_DATE - P2.MEASURE_DATE_PREV))),0))) END AS CAPACITY_FACTOR,

          CASE WHEN P2.ADMIN_STATUS <> ''up'' then NULL ELSE
          (COALESCE(
          ((((P2.TOTAL_PACKETS_RECEIVED - CASE WHEN P2.TOTAL_PACKETS_RECEIVED_PREV > P2.TOTAL_PACKETS_RECEIVED THEN 0 ELSE P2.TOTAL_PACKETS_RECEIVED_PREV END) - 
          (P2.CRC_ERROR_COUNT - CASE WHEN P2.CRC_ERROR_COUNT_PREV > P2.CRC_ERROR_COUNT THEN 0 ELSE P2.CRC_ERROR_COUNT_PREV END)) - 
          (P2.COLLISION_ERROR_COUNT - CASE WHEN P2.COLLISION_ERROR_COUNT_PREV > P2.COLLISION_ERROR_COUNT THEN 0 ELSE P2.COLLISION_ERROR_COUNT_PREV END)) / 
          (P2.TOTAL_PACKETS_RECEIVED - CASE WHEN P2.TOTAL_PACKETS_RECEIVED_PREV > P2.TOTAL_PACKETS_RECEIVED THEN 0 ELSE P2.TOTAL_PACKETS_RECEIVED_PREV END)),1)) END AS QUALITY_FACTOR
from
  (
    select
      P1.ASSET_ID AS ASSET_ID,
      P1.PORT_NUMBER AS PORT_NUMBER,
      P1.OPERATION_STATUS AS OPERATION_STATUS,
      P1.ADMIN_STATUS AS ADMIN_STATUS,
                    coalesce(lag (P1.OPERATION_STATUS) OVER (PARTITION BY P1.ASSET_ID,P1.PORT_NUMBER ORDER BY  P1.MEASURE_DATE),P1.OPERATION_STATUS) AS OPERATION_STATUS_PREV,
                    P1.MEASURE_DATE AS MEASURE_DATE,
                    coalesce(lag (P1.MEASURE_DATE) OVER (PARTITION BY P1.ASSET_ID,P1.PORT_NUMBER ORDER BY  P1.MEASURE_DATE),P1.MEASURE_DATE) AS MEASURE_DATE_PREV,
                    coalesce(lead (P1.MEASURE_DATE) OVER (PARTITION BY P1.ASSET_ID,P1.PORT_NUMBER ORDER BY  P1.MEASURE_DATE),P1.MEASURE_DATE) AS MEASURE_DATE_FUTURE,
                    P1.NEGOTIATED_BPS AS NEGOTIATED_BPS,
                    coalesce(lag (P1.NEGOTIATED_BPS) OVER (PARTITION BY P1.ASSET_ID,P1.PORT_NUMBER ORDER BY  P1.MEASURE_DATE),P1.NEGOTIATED_BPS) AS NEGOTIATED_BPS_PREV,
                    P1.RECEIVED_BPS AS RECEIVED_BPS,
                    coalesce(lag (P1.RECEIVED_BPS) OVER (PARTITION BY P1.ASSET_ID,P1.PORT_NUMBER ORDER BY  P1.MEASURE_DATE),P1.RECEIVED_BPS) AS RECEIVED_BPS_PREV,
                    P1.CRC_ERROR_COUNT AS CRC_ERROR_COUNT,
                    coalesce(lag (P1.CRC_ERROR_COUNT) OVER (PARTITION BY P1.ASSET_ID,P1.PORT_NUMBER ORDER BY  P1.MEASURE_DATE),P1.CRC_ERROR_COUNT) AS CRC_ERROR_COUNT_PREV,
                    P1.COLLISION_ERROR_COUNT AS COLLISION_ERROR_COUNT,
                    coalesce(lag (P1.COLLISION_ERROR_COUNT) OVER (PARTITION BY P1.ASSET_ID,P1.PORT_NUMBER ORDER BY  P1.MEASURE_DATE),P1.COLLISION_ERROR_COUNT) AS COLLISION_ERROR_COUNT_PREV,
                    P1.TOTAL_PACKETS_RECEIVED AS TOTAL_PACKETS_RECEIVED,
                    coalesce(lag (P1.TOTAL_PACKETS_RECEIVED) OVER (PARTITION BY P1.ASSET_ID, P1.PORT_NUMBER ORDER BY  P1.MEASURE_DATE), P1.TOTAL_PACKETS_RECEIVED) AS TOTAL_PACKETS_RECEIVED_PREV,
      ROW_NUMBER() OVER (PARTITION BY ASSET_ID , PORT_NUMBER ORDER BY MEASURE_DATE DESC ) as row_num
    from
      (
        select
          analytics.ACTIVE_TELEMETRY.ASSET_ID AS ASSET_ID,
          analytics.ACTIVE_TELEMETRY.PORT_NUMBER AS PORT_NUMBER,
          analytics.ACTIVE_TELEMETRY.MEASURE_DATE AS MEASURE_DATE,
                              max(
                              (
                              case
                                        when (
                                        analytics.ACTIVE_TELEMETRY.MEASURE_TYPE = ''operation_status''
                                        ) then analytics.ACTIVE_TELEMETRY.MEASURE_STRING
                                        else ''''
                              end
                              )
                              ) AS OPERATION_STATUS,
                              max(
                              (
                              case
                                        when (
                                        analytics.ACTIVE_TELEMETRY.MEASURE_TYPE = ''negotiated_bps''
                                        ) then analytics.ACTIVE_TELEMETRY.MEASURE_VALUE
                                        else 0
                              end
                              )
                              ) AS NEGOTIATED_BPS,
                              max(
                              (
                              case
                                        when (
                                        analytics.ACTIVE_TELEMETRY.MEASURE_TYPE = ''received_bps''
                                        ) then analytics.ACTIVE_TELEMETRY.MEASURE_VALUE
                                        else 0
                              end
                              )
                              ) AS RECEIVED_BPS,
                              max(
                              (
                              case
                                        when (
                                        analytics.ACTIVE_TELEMETRY.MEASURE_TYPE = ''crc_error_count''
                                        ) then analytics.ACTIVE_TELEMETRY.MEASURE_VALUE
                                        else 0
                              end
                              )
                              ) AS CRC_ERROR_COUNT,
                              max(
                              (
                              case
                                        when (
                                        analytics.ACTIVE_TELEMETRY.MEASURE_TYPE = ''collision_error_count''
                                        ) then analytics.ACTIVE_TELEMETRY.MEASURE_VALUE
                                        else 0
                              end
                              )
                              ) AS COLLISION_ERROR_COUNT,
                              max(
                              (
                              case
                                        when (
                                        analytics.ACTIVE_TELEMETRY.MEASURE_TYPE = ''total_packets_recieved''
                                        ) then analytics.ACTIVE_TELEMETRY.MEASURE_VALUE
                                        else 0
                              end
                              )
                              ) AS TOTAL_PACKETS_RECEIVED,
                              max(
                              (
                              case
                                        when (
                                        analytics.ACTIVE_TELEMETRY.MEASURE_TYPE = ''admin_status''
                                        ) then analytics.ACTIVE_TELEMETRY.MEASURE_STRING
                                        else ''''
                              end
                              )
                              ) AS ADMIN_STATUS
        from
          analytics.ACTIVE_TELEMETRY  PARTITION ( ',
        partitionName,
        ' )
    WHERE
      MEASURE_DATE BETWEEN ',
        fromTime,
        ' AND ',
        toTime,
       ' AND (',
                            IF(portList IS NOT NULL AND portList != '''', 
                                CONCAT('CONCAT(ASSET_ID, '':'', PORT_NUMBER) IN (', portList, ') OR '), 
                                ''
                            ),
                            IF(assetList IS NOT NULL AND assetList != '''', 
                                CONCAT('ASSET_ID IN (', assetList, ') OR '), 
                                ''
                            ),
                            IF(subnetList IS NOT NULL AND subnetList != '''', 
                                CONCAT('SUBSTRING_INDEX(ASSET_ID,''.'',3) IN (', subnetList, ')'), 
                                '1=0'
                            ),

                            IF(subnetList IS NULL AND assetList IS NULL AND portList IS NULL, '1=1', ''),
                        ')
        group by
          analytics.ACTIVE_TELEMETRY.ASSET_ID,
          analytics.ACTIVE_TELEMETRY.PORT_NUMBER,
          analytics.ACTIVE_TELEMETRY.MEASURE_DATE
      ) P1
      WHERE ADMIN_STATUS = ''up''
  ) P2
  WHERE row_num = 1
) P3 ) P4'
    );

PREPARE stmt
FROM
    @sqlQuery;

EXECUTE stmt;

DEALLOCATE PREPARE stmt;

END ;;
DELIMITER ;
/*!50003 SET sql_mode              = @saved_sql_mode */ ;
/*!50003 SET character_set_client  = @saved_cs_client */ ;
/*!50003 SET character_set_results = @saved_cs_results */ ;
/*!50003 SET collation_connection  = @saved_col_connection */ ;
/*!50003 DROP PROCEDURE IF EXISTS `GetOneMetricValues##` */;
/*!50003 SET @saved_cs_client      = @@character_set_client */ ;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;
/*!50003 SET character_set_client  = utf8mb4 */ ;
/*!50003 SET character_set_results = utf8mb4 */ ;
/*!50003 SET collation_connection  = utf8mb4_0900_ai_ci */ ;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;
/*!50003 SET sql_mode              = '' */ ;
DELIMITER ;;
CREATE DEFINER=`analytics_dev`@`%` PROCEDURE `GetOneMetricValues##`(
    IN fromTime INT,
    IN toTime INT,
    IN subnetList TEXT,
    IN assetList TEXT,
    IN portList TEXT,
    IN partitionName VARCHAR(20)
)
BEGIN
SET
    @sqlQuery = CONCAT(
        '
SELECT 
    (AVG(AVAILABILITY_FACTOR * CAPACITY_FACTOR * QUALITY_FACTOR) - STDDEV_POP(AVAILABILITY_FACTOR * CAPACITY_FACTOR * QUALITY_FACTOR) ) * 100 AS Overall,
    AVG(QUALITY_FACTOR) * 100 AS quality,
    AVG(CAPACITY_FACTOR) * 100 AS capacity,
    AVG(AVAILABILITY_FACTOR) * 100 AS available
FROM
-- GIVES AVAILABILITY,CAPACITY,QUALITY
(
select COALESCE(
          ((case when (P2.OPERATION_STATUS = ''up'') then (P2.MEASURE_DATE - P2.MEASURE_DATE_PREV) else 0 end
          ) / case when (P2.ADMIN_STATUS = ''up'') then (P2.MEASURE_DATE - P2.MEASURE_DATE_PREV) else NULL end
          ),NULL) AS AVAILABILITY_FACTOR,

          CASE WHEN P2.ADMIN_STATUS <> ''up'' then NULL ELSE
          (1 - abs(COALESCE((((P2.RECEIVED_BPS - CASE WHEN P2.RECEIVED_BPS_PREV > P2.RECEIVED_BPS THEN 0 ELSE P2.RECEIVED_BPS_PREV END ) * 8) / 
          (P2.NEGOTIATED_BPS * (P2.MEASURE_DATE - P2.MEASURE_DATE_PREV))),0))) END AS CAPACITY_FACTOR,

          CASE WHEN P2.ADMIN_STATUS <> ''up'' then NULL ELSE
          (COALESCE(
          ((((P2.TOTAL_PACKETS_RECEIVED - CASE WHEN P2.TOTAL_PACKETS_RECEIVED_PREV > P2.TOTAL_PACKETS_RECEIVED THEN 0 ELSE P2.TOTAL_PACKETS_RECEIVED_PREV END) - 
          (P2.CRC_ERROR_COUNT - CASE WHEN P2.CRC_ERROR_COUNT_PREV > P2.CRC_ERROR_COUNT THEN 0 ELSE P2.CRC_ERROR_COUNT_PREV END)) - 
          (P2.COLLISION_ERROR_COUNT - CASE WHEN P2.COLLISION_ERROR_COUNT_PREV > P2.COLLISION_ERROR_COUNT THEN 0 ELSE P2.COLLISION_ERROR_COUNT_PREV END)) / 
          (P2.TOTAL_PACKETS_RECEIVED - CASE WHEN P2.TOTAL_PACKETS_RECEIVED_PREV > P2.TOTAL_PACKETS_RECEIVED THEN 0 ELSE P2.TOTAL_PACKETS_RECEIVED_PREV END)),1)) END AS QUALITY_FACTOR
from
  (
    select
      P1.ASSET_ID AS ASSET_ID,
      P1.PORT_NUMBER AS PORT_NUMBER,
      P1.OPERATION_STATUS AS OPERATION_STATUS,
      P1.ADMIN_STATUS AS ADMIN_STATUS,
                    coalesce(lag (P1.OPERATION_STATUS) OVER (PARTITION BY P1.ASSET_ID,P1.PORT_NUMBER ORDER BY  P1.MEASURE_DATE),P1.OPERATION_STATUS) AS OPERATION_STATUS_PREV,
                    P1.MEASURE_DATE AS MEASURE_DATE,
                    coalesce(lag (P1.MEASURE_DATE) OVER (PARTITION BY P1.ASSET_ID,P1.PORT_NUMBER ORDER BY  P1.MEASURE_DATE),P1.MEASURE_DATE) AS MEASURE_DATE_PREV,
                    coalesce(lead (P1.MEASURE_DATE) OVER (PARTITION BY P1.ASSET_ID,P1.PORT_NUMBER ORDER BY  P1.MEASURE_DATE),P1.MEASURE_DATE) AS MEASURE_DATE_FUTURE,
                    P1.NEGOTIATED_BPS AS NEGOTIATED_BPS,
                    coalesce(lag (P1.NEGOTIATED_BPS) OVER (PARTITION BY P1.ASSET_ID,P1.PORT_NUMBER ORDER BY  P1.MEASURE_DATE),P1.NEGOTIATED_BPS) AS NEGOTIATED_BPS_PREV,
                    P1.RECEIVED_BPS AS RECEIVED_BPS,
                    coalesce(lag (P1.RECEIVED_BPS) OVER (PARTITION BY P1.ASSET_ID,P1.PORT_NUMBER ORDER BY  P1.MEASURE_DATE),P1.RECEIVED_BPS) AS RECEIVED_BPS_PREV,
                    P1.CRC_ERROR_COUNT AS CRC_ERROR_COUNT,
                    coalesce(lag (P1.CRC_ERROR_COUNT) OVER (PARTITION BY P1.ASSET_ID,P1.PORT_NUMBER ORDER BY  P1.MEASURE_DATE),P1.CRC_ERROR_COUNT) AS CRC_ERROR_COUNT_PREV,
                    P1.COLLISION_ERROR_COUNT AS COLLISION_ERROR_COUNT,
                    coalesce(lag (P1.COLLISION_ERROR_COUNT) OVER (PARTITION BY P1.ASSET_ID,P1.PORT_NUMBER ORDER BY  P1.MEASURE_DATE),P1.COLLISION_ERROR_COUNT) AS COLLISION_ERROR_COUNT_PREV,
                    P1.TOTAL_PACKETS_RECEIVED AS TOTAL_PACKETS_RECEIVED,
                    coalesce(lag (P1.TOTAL_PACKETS_RECEIVED) OVER (PARTITION BY P1.ASSET_ID, P1.PORT_NUMBER ORDER BY  P1.MEASURE_DATE), P1.TOTAL_PACKETS_RECEIVED) AS TOTAL_PACKETS_RECEIVED_PREV,
      ROW_NUMBER() OVER (PARTITION BY ASSET_ID , PORT_NUMBER ORDER BY MEASURE_DATE DESC ) as row_num
    from
      (
        select
          analytics.ACTIVE_TELEMETRY.ASSET_ID AS ASSET_ID,
          analytics.ACTIVE_TELEMETRY.PORT_NUMBER AS PORT_NUMBER,
          analytics.ACTIVE_TELEMETRY.MEASURE_DATE AS MEASURE_DATE,
                              max(
                              (
                              case
                                        when (
                                        analytics.ACTIVE_TELEMETRY.MEASURE_TYPE = ''operation_status''
                                        ) then analytics.ACTIVE_TELEMETRY.MEASURE_STRING
                                        else ''''
                              end
                              )
                              ) AS OPERATION_STATUS,
                              max(
                              (
                              case
                                        when (
                                        analytics.ACTIVE_TELEMETRY.MEASURE_TYPE = ''negotiated_bps''
                                        ) then analytics.ACTIVE_TELEMETRY.MEASURE_VALUE
                                        else 0
                              end
                              )
                              ) AS NEGOTIATED_BPS,
                              max(
                              (
                              case
                                        when (
                                        analytics.ACTIVE_TELEMETRY.MEASURE_TYPE = ''received_bps''
                                        ) then analytics.ACTIVE_TELEMETRY.MEASURE_VALUE
                                        else 0
                              end
                              )
                              ) AS RECEIVED_BPS,
                              max(
                              (
                              case
                                        when (
                                        analytics.ACTIVE_TELEMETRY.MEASURE_TYPE = ''crc_error_count''
                                        ) then analytics.ACTIVE_TELEMETRY.MEASURE_VALUE
                                        else 0
                              end
                              )
                              ) AS CRC_ERROR_COUNT,
                              max(
                              (
                              case
                                        when (
                                        analytics.ACTIVE_TELEMETRY.MEASURE_TYPE = ''collision_error_count''
                                        ) then analytics.ACTIVE_TELEMETRY.MEASURE_VALUE
                                        else 0
                              end
                              )
                              ) AS COLLISION_ERROR_COUNT,
                              max(
                              (
                              case
                                        when (
                                        analytics.ACTIVE_TELEMETRY.MEASURE_TYPE = ''total_packets_recieved''
                                        ) then analytics.ACTIVE_TELEMETRY.MEASURE_VALUE
                                        else 0
                              end
                              )
                              ) AS TOTAL_PACKETS_RECEIVED,
                              max(
                              (
                              case
                                        when (
                                        analytics.ACTIVE_TELEMETRY.MEASURE_TYPE = ''admin_status''
                                        ) then analytics.ACTIVE_TELEMETRY.MEASURE_STRING
                                        else ''''
                              end
                              )
                              ) AS ADMIN_STATUS
        from
          analytics.ACTIVE_TELEMETRY  PARTITION ( ',
        partitionName,
        ' )
    WHERE
      MEASURE_DATE BETWEEN ',
        fromTime,
        ' AND ',
        toTime,
       ' AND (',
                            IF(portList IS NOT NULL AND portList != '''', 
                                CONCAT('CONCAT(ASSET_ID, '':'', PORT_NUMBER) IN (', portList, ') OR '), 
                                ''
                            ),
                            IF(assetList IS NOT NULL AND assetList != '''', 
                                CONCAT('ASSET_ID IN (', assetList, ') OR '), 
                                ''
                            ),
                            IF(subnetList IS NOT NULL AND subnetList != '''', 
                                CONCAT('SUBSTRING_INDEX(ASSET_ID,''.'',3) IN (', subnetList, ')'), 
                                '1=0'
                            ),

                            IF(subnetList IS NULL AND assetList IS NULL AND portList IS NULL, '1=1', ''),
                        ')
        group by
          analytics.ACTIVE_TELEMETRY.ASSET_ID,
          analytics.ACTIVE_TELEMETRY.PORT_NUMBER,
          analytics.ACTIVE_TELEMETRY.MEASURE_DATE
      ) P1
      WHERE ADMIN_STATUS = ''up''
  ) P2
  WHERE row_num = 1
) P3'
    );

PREPARE stmt
FROM
    @sqlQuery;

EXECUTE stmt;

DEALLOCATE PREPARE stmt;

END ;;
DELIMITER ;
/*!50003 SET sql_mode              = @saved_sql_mode */ ;
/*!50003 SET character_set_client  = @saved_cs_client */ ;
/*!50003 SET character_set_results = @saved_cs_results */ ;
/*!50003 SET collation_connection  = @saved_col_connection */ ;
/*!50003 DROP PROCEDURE IF EXISTS `GetOneMetricValuesForPort` */;
/*!50003 SET @saved_cs_client      = @@character_set_client */ ;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;
/*!50003 SET character_set_client  = latin1 */ ;
/*!50003 SET character_set_results = latin1 */ ;
/*!50003 SET collation_connection  = latin1_swedish_ci */ ;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;
/*!50003 SET sql_mode              = '' */ ;
DELIMITER ;;
CREATE DEFINER=`analytics_dev`@`%` PROCEDURE `GetOneMetricValuesForPort`(
    IN fromTime INT,
    IN toTime INT,
    IN portNumber VARCHAR(255),
    IN assetId VARCHAR(255),
    IN partitionName VARCHAR(20)
)
BEGIN
SET
    @sqlQuery = CONCAT(
        '
SELECT 
   (QUALITY_FACTOR) * (CAPACITY_FACTOR) * (AVAILABILITY_FACTOR) * 100 AS overall,
    (QUALITY_FACTOR) * 100 AS quality,
    (CAPACITY_FACTOR) * 100 AS capacity,
    (AVAILABILITY_FACTOR) * 100 AS available
FROM
(

select
  P2.ASSET_ID AS ASSET_ID,
  P2.PORT_NUMBER AS PORT_NUMBER,
  P2.MEASURE_DATE AS MEASURE_DATE,
          COALESCE(
          (
          (
          case
                    when (P2.OPERATION_STATUS = ''up'') then (P2.MEASURE_DATE - P2.MEASURE_DATE_PREV)
                    else 0
          end
          ) / case
                    when (P2.ADMIN_STATUS = ''up'') then (P2.MEASURE_DATE - P2.MEASURE_DATE_PREV)
                    else NULL
          end
          ),
          NULL
          ) AS AVAILABILITY_FACTOR,
            CASE WHEN P2.ADMIN_STATUS <> ''up'' then NULL ELSE
          (
          1 - abs(
          COALESCE(
          (
          ((P2.RECEIVED_BPS - CASE WHEN P2.RECEIVED_BPS_PREV > P2.RECEIVED_BPS THEN 0 ELSE P2.RECEIVED_BPS_PREV END ) * 8) / (
          P2.NEGOTIATED_BPS * (P2.MEASURE_DATE - P2.MEASURE_DATE_PREV)
          )
          ),
          0
          )
          )
          ) END AS CAPACITY_FACTOR,
          CASE WHEN P2.ADMIN_STATUS <> ''up'' then NULL ELSE
          (
          COALESCE(
          (
          (
          (
          (
          P2.TOTAL_PACKETS_RECEIVED - CASE WHEN P2.TOTAL_PACKETS_RECEIVED_PREV > P2.TOTAL_PACKETS_RECEIVED THEN 0 ELSE P2.TOTAL_PACKETS_RECEIVED_PREV END
          ) - (P2.CRC_ERROR_COUNT - CASE WHEN P2.CRC_ERROR_COUNT_PREV > P2.CRC_ERROR_COUNT THEN 0 ELSE P2.CRC_ERROR_COUNT_PREV END)
          ) - (
          P2.COLLISION_ERROR_COUNT - CASE WHEN P2.COLLISION_ERROR_COUNT_PREV > P2.COLLISION_ERROR_COUNT THEN 0 ELSE P2.COLLISION_ERROR_COUNT_PREV END
          )
          ) / (
          P2.TOTAL_PACKETS_RECEIVED - CASE WHEN P2.TOTAL_PACKETS_RECEIVED_PREV > P2.TOTAL_PACKETS_RECEIVED THEN 0 ELSE P2.TOTAL_PACKETS_RECEIVED_PREV END
          )
          ),
          1
          )) END AS QUALITY_FACTOR,
          ROW_NUMBER() OVER(ORDER BY MEASURE_DATE DESC ) as row_num
from
  (
    select
      P1.ASSET_ID AS ASSET_ID,
      P1.PORT_NUMBER AS PORT_NUMBER,
      P1.OPERATION_STATUS AS OPERATION_STATUS,
      P1.ADMIN_STATUS AS ADMIN_STATUS,
                    coalesce(
                    lag (P1.OPERATION_STATUS) OVER (
                    PARTITION BY P1.ASSET_ID,
                              P1.PORT_NUMBER
                    ORDER BY  P1.MEASURE_DATE
                    ),
                    P1.OPERATION_STATUS
                    ) AS OPERATION_STATUS_PREV,
                    P1.MEASURE_DATE AS MEASURE_DATE,
                    coalesce(
                    lag (P1.MEASURE_DATE) OVER (
                    PARTITION BY P1.ASSET_ID,
                              P1.PORT_NUMBER
                    ORDER BY  P1.MEASURE_DATE
                    ),
                    P1.MEASURE_DATE
                    ) AS MEASURE_DATE_PREV,
                    coalesce(
                    lead (P1.MEASURE_DATE) OVER (
                    PARTITION BY P1.ASSET_ID,
                              P1.PORT_NUMBER
                    ORDER BY  P1.MEASURE_DATE
                    ),
                    P1.MEASURE_DATE
                    ) AS MEASURE_DATE_FUTURE,
                    P1.NEGOTIATED_BPS AS NEGOTIATED_BPS,
                    coalesce(
                    lag (P1.NEGOTIATED_BPS) OVER (
                    PARTITION BY P1.ASSET_ID,
                              P1.PORT_NUMBER
                    ORDER BY  P1.MEASURE_DATE
                    ),
                    P1.NEGOTIATED_BPS
                    ) AS NEGOTIATED_BPS_PREV,
                    P1.RECEIVED_BPS AS RECEIVED_BPS,
                    coalesce(
                    lag (P1.RECEIVED_BPS) OVER (
                    PARTITION BY P1.ASSET_ID,
                              P1.PORT_NUMBER
                    ORDER BY  P1.MEASURE_DATE
                    ),
                    P1.RECEIVED_BPS
                    ) AS RECEIVED_BPS_PREV,
                    P1.CRC_ERROR_COUNT AS CRC_ERROR_COUNT,
                    coalesce(
                    lag (P1.CRC_ERROR_COUNT) OVER (
                    PARTITION BY P1.ASSET_ID,
                              P1.PORT_NUMBER
                    ORDER BY  P1.MEASURE_DATE
                    ),
                    P1.CRC_ERROR_COUNT
                    ) AS CRC_ERROR_COUNT_PREV,
                    P1.COLLISION_ERROR_COUNT AS COLLISION_ERROR_COUNT,
                    coalesce(
                    lag (P1.COLLISION_ERROR_COUNT) OVER (
                    PARTITION BY P1.ASSET_ID,
                              P1.PORT_NUMBER
                    ORDER BY  P1.MEASURE_DATE
                    ),
                    P1.COLLISION_ERROR_COUNT
                    ) AS COLLISION_ERROR_COUNT_PREV,
                    P1.TOTAL_PACKETS_RECEIVED AS TOTAL_PACKETS_RECEIVED,
                    coalesce(
                    lag (P1.TOTAL_PACKETS_RECEIVED) OVER (
                    PARTITION BY P1.ASSET_ID,
                              P1.PORT_NUMBER
                    ORDER BY  P1.MEASURE_DATE
                    ),
                    P1.TOTAL_PACKETS_RECEIVED
                    ) AS TOTAL_PACKETS_RECEIVED_PREV
    from
      (
        select
          analytics.ACTIVE_TELEMETRY.ASSET_ID AS ASSET_ID,
          analytics.ACTIVE_TELEMETRY.PORT_NUMBER AS PORT_NUMBER,
          analytics.ACTIVE_TELEMETRY.MEASURE_DATE AS MEASURE_DATE,
                              max(
                              (
                              case
                                        when (
                                        analytics.ACTIVE_TELEMETRY.MEASURE_TYPE = ''operation_status''
                                        ) then analytics.ACTIVE_TELEMETRY.MEASURE_STRING
                                        else ''''
                              end
                              )
                              ) AS OPERATION_STATUS,
                              max(
                              (
                              case
                                        when (
                                        analytics.ACTIVE_TELEMETRY.MEASURE_TYPE = ''negotiated_bps''
                                        ) then analytics.ACTIVE_TELEMETRY.MEASURE_VALUE
                                        else 0
                              end
                              )
                              ) AS NEGOTIATED_BPS,
                              max(
                              (
                              case
                                        when (
                                        analytics.ACTIVE_TELEMETRY.MEASURE_TYPE = ''received_bps''
                                        ) then analytics.ACTIVE_TELEMETRY.MEASURE_VALUE
                                        else 0
                              end
                              )
                              ) AS RECEIVED_BPS,
                              max(
                              (
                              case
                                        when (
                                        analytics.ACTIVE_TELEMETRY.MEASURE_TYPE = ''crc_error_count''
                                        ) then analytics.ACTIVE_TELEMETRY.MEASURE_VALUE
                                        else 0
                              end
                              )
                              ) AS CRC_ERROR_COUNT,
                              max(
                              (
                              case
                                        when (
                                        analytics.ACTIVE_TELEMETRY.MEASURE_TYPE = ''collision_error_count''
                                        ) then analytics.ACTIVE_TELEMETRY.MEASURE_VALUE
                                        else 0
                              end
                              )
                              ) AS COLLISION_ERROR_COUNT,
                              max(
                              (
                              case
                                        when (
                                        analytics.ACTIVE_TELEMETRY.MEASURE_TYPE = ''total_packets_recieved''
                                        ) then analytics.ACTIVE_TELEMETRY.MEASURE_VALUE
                                        else 0
                              end
                              )
                              ) AS TOTAL_PACKETS_RECEIVED,
                              max(
                              (
                              case
                                        when (
                                        analytics.ACTIVE_TELEMETRY.MEASURE_TYPE = ''admin_status''
                                        ) then analytics.ACTIVE_TELEMETRY.MEASURE_STRING
                                        else ''''
                              end
                              )
                              ) AS ADMIN_STATUS
        from
          analytics.ACTIVE_TELEMETRY  PARTITION ( ',
        partitionName,
        ' )
    WHERE
      MEASURE_DATE BETWEEN ',
        fromTime,
        ' AND ',
        toTime,
        ' AND ASSET_ID = ''',
        assetId,
        ''' AND
      PORT_NUMBER = ',
        portNumber,
        ' 
        group by
          analytics.ACTIVE_TELEMETRY.ASSET_ID,
          analytics.ACTIVE_TELEMETRY.PORT_NUMBER,
          analytics.ACTIVE_TELEMETRY.MEASURE_DATE
      ) P1
      WHERE ADMIN_STATUS = ''up''
  ) P2
) P3
WHERE row_num =1

'
    );

PREPARE stmt
FROM
    @sqlQuery;

EXECUTE stmt;

DEALLOCATE PREPARE stmt;

END ;;
DELIMITER ;
/*!50003 SET sql_mode              = @saved_sql_mode */ ;
/*!50003 SET character_set_client  = @saved_cs_client */ ;
/*!50003 SET character_set_results = @saved_cs_results */ ;
/*!50003 SET collation_connection  = @saved_col_connection */ ;
/*!50003 DROP PROCEDURE IF EXISTS `GetOneMetricValuesForRoot` */;
/*!50003 SET @saved_cs_client      = @@character_set_client */ ;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;
/*!50003 SET character_set_client  = latin1 */ ;
/*!50003 SET character_set_results = latin1 */ ;
/*!50003 SET collation_connection  = latin1_swedish_ci */ ;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;
/*!50003 SET sql_mode              = '' */ ;
DELIMITER ;;
CREATE DEFINER=`analytics_dev`@`%` PROCEDURE `GetOneMetricValuesForRoot`(
    IN fromTime INT,
    IN toTime INT,
    IN partitionName VARCHAR(20)
)
BEGIN
SET
    @sqlQuery = CONCAT(
        'SELECT 
    (AVG(AVAILABILITY_FACTOR * CAPACITY_FACTOR * QUALITY_FACTOR) ) * 100 AS overall,
    AVG(QUALITY_FACTOR) * 100 AS quality,
    AVG(CAPACITY_FACTOR) * 100 AS capacity,
    AVG(AVAILABILITY_FACTOR) * 100 AS available
FROM
    (
    select
  P2.ASSET_ID AS ASSET_ID,
  P2.PORT_NUMBER AS PORT_NUMBER,
  P2.MEASURE_DATE AS MEASURE_DATE,
          COALESCE(
          (
          (
          case
                    when (P2.OPERATION_STATUS = ''up'') then (P2.MEASURE_DATE - P2.MEASURE_DATE_PREV)
                    else 0
          end
          ) / case
                    when (P2.ADMIN_STATUS = ''up'') then (P2.MEASURE_DATE - P2.MEASURE_DATE_PREV)
                    else NULL
          end
          ),
          NULL
          ) AS AVAILABILITY_FACTOR,
            CASE WHEN P2.ADMIN_STATUS <> ''up'' then NULL ELSE
          (
          1 - abs(
          COALESCE(
          (
          ((P2.RECEIVED_BPS - CASE WHEN P2.RECEIVED_BPS_PREV > P2.RECEIVED_BPS THEN 0 ELSE P2.RECEIVED_BPS_PREV END ) * 8) / (
          P2.NEGOTIATED_BPS * (P2.MEASURE_DATE - P2.MEASURE_DATE_PREV)
          )
          ),
          0
          )
          )
          ) END AS CAPACITY_FACTOR,
          CASE WHEN P2.ADMIN_STATUS <> ''up'' then NULL ELSE
          (
          COALESCE(
          (
          (
          (
          (
          P2.TOTAL_PACKETS_RECEIVED - CASE WHEN P2.TOTAL_PACKETS_RECEIVED_PREV > P2.TOTAL_PACKETS_RECEIVED THEN 0 ELSE P2.TOTAL_PACKETS_RECEIVED_PREV END
          ) - (P2.CRC_ERROR_COUNT - CASE WHEN P2.CRC_ERROR_COUNT_PREV > P2.CRC_ERROR_COUNT THEN 0 ELSE P2.CRC_ERROR_COUNT_PREV END)
          ) - (
          P2.COLLISION_ERROR_COUNT - CASE WHEN P2.COLLISION_ERROR_COUNT_PREV > P2.COLLISION_ERROR_COUNT THEN 0 ELSE P2.COLLISION_ERROR_COUNT_PREV END
          )
          ) / (
          P2.TOTAL_PACKETS_RECEIVED - CASE WHEN P2.TOTAL_PACKETS_RECEIVED_PREV > P2.TOTAL_PACKETS_RECEIVED THEN 0 ELSE P2.TOTAL_PACKETS_RECEIVED_PREV END
          )
          ),
          1
          )) END AS QUALITY_FACTOR
from
  (
    select
      P1.ASSET_ID AS ASSET_ID,
      P1.PORT_NUMBER AS PORT_NUMBER,
      P1.OPERATION_STATUS AS OPERATION_STATUS,
      P1.ADMIN_STATUS AS ADMIN_STATUS,
                    coalesce(
                    lag (P1.OPERATION_STATUS) OVER (
                    PARTITION BY P1.ASSET_ID,
                              P1.PORT_NUMBER
                    ORDER BY  P1.MEASURE_DATE
                    ),
                    P1.OPERATION_STATUS
                    ) AS OPERATION_STATUS_PREV,
                    P1.MEASURE_DATE AS MEASURE_DATE,
                    coalesce(
                    lag (P1.MEASURE_DATE) OVER (
                    PARTITION BY P1.ASSET_ID,
                              P1.PORT_NUMBER
                    ORDER BY  P1.MEASURE_DATE
                    ),
                    P1.MEASURE_DATE
                    ) AS MEASURE_DATE_PREV,
                    coalesce(
                    lead (P1.MEASURE_DATE) OVER (
                    PARTITION BY P1.ASSET_ID,
                              P1.PORT_NUMBER
                    ORDER BY  P1.MEASURE_DATE
                    ),
                    P1.MEASURE_DATE
                    ) AS MEASURE_DATE_FUTURE,
                    P1.NEGOTIATED_BPS AS NEGOTIATED_BPS,
                    coalesce(
                    lag (P1.NEGOTIATED_BPS) OVER (
                    PARTITION BY P1.ASSET_ID,
                              P1.PORT_NUMBER
                    ORDER BY  P1.MEASURE_DATE
                    ),
                    P1.NEGOTIATED_BPS
                    ) AS NEGOTIATED_BPS_PREV,
                    P1.RECEIVED_BPS AS RECEIVED_BPS,
                    coalesce(
                    lag (P1.RECEIVED_BPS) OVER (
                    PARTITION BY P1.ASSET_ID,
                              P1.PORT_NUMBER
                    ORDER BY  P1.MEASURE_DATE
                    ),
                    P1.RECEIVED_BPS
                    ) AS RECEIVED_BPS_PREV,
                    P1.CRC_ERROR_COUNT AS CRC_ERROR_COUNT,
                    coalesce(
                    lag (P1.CRC_ERROR_COUNT) OVER (
                    PARTITION BY P1.ASSET_ID,
                              P1.PORT_NUMBER
                    ORDER BY  P1.MEASURE_DATE
                    ),
                    P1.CRC_ERROR_COUNT
                    ) AS CRC_ERROR_COUNT_PREV,
                    P1.COLLISION_ERROR_COUNT AS COLLISION_ERROR_COUNT,
                    coalesce(
                    lag (P1.COLLISION_ERROR_COUNT) OVER (
                    PARTITION BY P1.ASSET_ID,
                              P1.PORT_NUMBER
                    ORDER BY  P1.MEASURE_DATE
                    ),
                    P1.COLLISION_ERROR_COUNT
                    ) AS COLLISION_ERROR_COUNT_PREV,
                    P1.TOTAL_PACKETS_RECEIVED AS TOTAL_PACKETS_RECEIVED,
                    coalesce(
                    lag (P1.TOTAL_PACKETS_RECEIVED) OVER (
                    PARTITION BY P1.ASSET_ID,
                              P1.PORT_NUMBER
                    ORDER BY  P1.MEASURE_DATE
                    ),
                    P1.TOTAL_PACKETS_RECEIVED
                    ) AS TOTAL_PACKETS_RECEIVED_PREV,
      ROW_NUMBER() OVER (PARTITION BY ASSET_ID,PORT_NUMBER ORDER BY MEASURE_DATE DESC) as row_num
    from
      (
        select
          analytics.ACTIVE_TELEMETRY.ASSET_ID AS ASSET_ID,
          analytics.ACTIVE_TELEMETRY.PORT_NUMBER AS PORT_NUMBER,
          analytics.ACTIVE_TELEMETRY.MEASURE_DATE AS MEASURE_DATE,
                              max(
                              (
                              case
                                        when (
                                        analytics.ACTIVE_TELEMETRY.MEASURE_TYPE = ''operation_status''
                                        ) then analytics.ACTIVE_TELEMETRY.MEASURE_STRING
                                        else ''''
                              end
                              )
                              ) AS OPERATION_STATUS,
                              max(
                              (
                              case
                                        when (
                                        analytics.ACTIVE_TELEMETRY.MEASURE_TYPE = ''negotiated_bps''
                                        ) then analytics.ACTIVE_TELEMETRY.MEASURE_VALUE
                                        else 0
                              end
                              )
                              ) AS NEGOTIATED_BPS,
                              max(
                              (
                              case
                                        when (
                                        analytics.ACTIVE_TELEMETRY.MEASURE_TYPE = ''received_bps''
                                        ) then analytics.ACTIVE_TELEMETRY.MEASURE_VALUE
                                        else 0
                              end
                              )
                              ) AS RECEIVED_BPS,
                              max(
                              (
                              case
                                        when (
                                        analytics.ACTIVE_TELEMETRY.MEASURE_TYPE = ''crc_error_count''
                                        ) then analytics.ACTIVE_TELEMETRY.MEASURE_VALUE
                                        else 0
                              end
                              )
                              ) AS CRC_ERROR_COUNT,
                              max(
                              (
                              case
                                        when (
                                        analytics.ACTIVE_TELEMETRY.MEASURE_TYPE = ''collision_error_count''
                                        ) then analytics.ACTIVE_TELEMETRY.MEASURE_VALUE
                                        else 0
                              end
                              )
                              ) AS COLLISION_ERROR_COUNT,
                              max(
                              (
                              case
                                        when (
                                        analytics.ACTIVE_TELEMETRY.MEASURE_TYPE = ''total_packets_recieved''
                                        ) then analytics.ACTIVE_TELEMETRY.MEASURE_VALUE
                                        else 0
                              end
                              )
                              ) AS TOTAL_PACKETS_RECEIVED,
                              max(
                              (
                              case
                                        when (
                                        analytics.ACTIVE_TELEMETRY.MEASURE_TYPE = ''admin_status''
                                        ) then analytics.ACTIVE_TELEMETRY.MEASURE_STRING
                                        else ''''
                              end
                              )
                              ) AS ADMIN_STATUS
        from
          analytics.ACTIVE_TELEMETRY  PARTITION ( ',
        partitionName,
        ' )
    WHERE
      MEASURE_DATE BETWEEN ',
        fromTime,
        ' AND ',
        toTime,
        ' 
        group by
          analytics.ACTIVE_TELEMETRY.ASSET_ID,
          analytics.ACTIVE_TELEMETRY.PORT_NUMBER,
          analytics.ACTIVE_TELEMETRY.MEASURE_DATE
      ) P1
      WHERE ADMIN_STATUS = ''up''
  ) P2
  WHERE row_num = 1 ) P3'
    );

PREPARE stmt
FROM
    @sqlQuery;

EXECUTE stmt;

DEALLOCATE PREPARE stmt;

END ;;
DELIMITER ;
/*!50003 SET sql_mode              = @saved_sql_mode */ ;
/*!50003 SET character_set_client  = @saved_cs_client */ ;
/*!50003 SET character_set_results = @saved_cs_results */ ;
/*!50003 SET collation_connection  = @saved_col_connection */ ;
/*!50003 DROP PROCEDURE IF EXISTS `GetOpDeviceList` */;
/*!50003 SET @saved_cs_client      = @@character_set_client */ ;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;
/*!50003 SET character_set_client  = latin1 */ ;
/*!50003 SET character_set_results = latin1 */ ;
/*!50003 SET collation_connection  = latin1_swedish_ci */ ;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;
/*!50003 SET sql_mode              = '' */ ;
DELIMITER ;;
CREATE DEFINER=`analytics_dev`@`%` PROCEDURE `GetOpDeviceList`()
BEGIN
  SELECT DISTINCT deviceId AS DEVICE_ID FROM analytics.deviceList;
END ;;
DELIMITER ;
/*!50003 SET sql_mode              = @saved_sql_mode */ ;
/*!50003 SET character_set_client  = @saved_cs_client */ ;
/*!50003 SET character_set_results = @saved_cs_results */ ;
/*!50003 SET collation_connection  = @saved_col_connection */ ;
/*!50003 DROP PROCEDURE IF EXISTS `GetPacketCounts` */;
/*!50003 SET @saved_cs_client      = @@character_set_client */ ;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;
/*!50003 SET character_set_client  = latin1 */ ;
/*!50003 SET character_set_results = latin1 */ ;
/*!50003 SET collation_connection  = latin1_swedish_ci */ ;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;
/*!50003 SET sql_mode              = '' */ ;
DELIMITER ;;
CREATE DEFINER=`analytics_dev`@`%` PROCEDURE `GetPacketCounts`(
    IN Device VARCHAR(255),
    IN fromTime INT,
    IN toTime INT,
    IN IP VARCHAR(255),
    IN assetList TEXT
)
BEGIN
    SET @sqlQuery = CONCAT('
        SELECT 
            MEASURE_START_DATE DIV 30 * 30 AS time,
            SUM(
                CASE 
                    WHEN MEASURE_TYPE = ''Count_of_Small_Packets'' THEN MEASURE_VALUE
                    ELSE 0
                END
            ) AS "Small Packets",
            SUM(
                CASE 
                    WHEN MEASURE_TYPE = ''Count_of_Large_Packets'' THEN MEASURE_VALUE
                    ELSE 0
                END
            ) AS "Big Packets"
        FROM analytics.TELEMETRY_HEADER PARTITION (', Device, ') AS H
        INNER JOIN analytics.TELEMETRY PARTITION (', Device, ') AS T
            ON H.TELEMETRY_GUID = T.TELEMETRY_GUID 
            AND (MEASURE_TYPE = ''Count_of_Small_Packets'' OR MEASURE_TYPE = ''Count_of_Large_Packets'')
        WHERE MEASURE_START_DATE BETWEEN ', fromTime, ' AND ', toTime,
        IF(IP IS NOT NULL AND IP != '''', CONCAT(' AND SRC_ADDRESS = ''', IP, ''''), ''),
        IF(assetList IS NOT NULL AND assetList != '''', CONCAT(' AND (SRC_ETH_ADDRESS IN (', assetList, ') OR DST_ETH_ADDRESS IN (', assetList, '))'),''),
        ' GROUP BY time
        ORDER BY time;');

    PREPARE stmt FROM @sqlQuery;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;
END ;;
DELIMITER ;
/*!50003 SET sql_mode              = @saved_sql_mode */ ;
/*!50003 SET character_set_client  = @saved_cs_client */ ;
/*!50003 SET character_set_results = @saved_cs_results */ ;
/*!50003 SET collation_connection  = @saved_col_connection */ ;
/*!50003 DROP PROCEDURE IF EXISTS `GetPacketPerSec` */;
/*!50003 SET @saved_cs_client      = @@character_set_client */ ;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;
/*!50003 SET character_set_client  = latin1 */ ;
/*!50003 SET character_set_results = latin1 */ ;
/*!50003 SET collation_connection  = latin1_swedish_ci */ ;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;
/*!50003 SET sql_mode              = '' */ ;
DELIMITER ;;
CREATE DEFINER=`analytics_dev`@`%` PROCEDURE `GetPacketPerSec`(
    IN Device VARCHAR(255),
    IN fromTime INT,
    IN toTime INT,
    IN IP VARCHAR(255),
    IN assetList TEXT
)
BEGIN
    SET @sqlQuery = CONCAT('
        SELECT
            MEASURE_START_DATE DIV 30 * 30 AS time,
            CAST((IFNULL(MAX(frames1), 0) + IFNULL(MAX(frames2), 0) + IFNULL(MAX(frames3), 0)) AS DECIMAL(10, 2)) AS "Packet/s",
            THRESHOLD_HIGH AS Threshold
        FROM (
            SELECT 
                MEASURE_START_DATE,
                AVG(
                    CASE 
                        WHEN PROTOCOL IN (''TCP'', ''MODBUS'') AND MEASURE_TYPE = ''count_of_frames_sent'' THEN MEASURE_VALUE
                        ELSE NULL
                    END
                ) AS frames1,
                AVG(
                    CASE 
                        WHEN PROTOCOL = ''UDP'' AND MEASURE_TYPE = ''num_of_frames_withinwindow_per_src_dst'' THEN MEASURE_VALUE
                        ELSE NULL
                    END
                ) AS frames2,
                AVG(
                    CASE 
                        WHEN PROTOCOL = ''ICMP'' AND MEASURE_TYPE = ''num_of_frames_withinwindow_per_src_dst'' THEN MEASURE_VALUE
                        ELSE NULL
                    END
                ) AS frames3
            FROM analytics.TELEMETRY_HEADER PARTITION (', Device, ') AS H
            INNER JOIN analytics.TELEMETRY PARTITION (', Device, ') AS T 
                ON H.TELEMETRY_GUID = T.TELEMETRY_GUID 
                AND T.MEASURE_TYPE IN (''count_of_frames_sent'', ''num_of_frames_withinwindow_per_src_dst'')
            WHERE MEASURE_START_DATE BETWEEN ', fromTime, ' AND ', toTime,
            IF(IP IS NOT NULL AND IP != '''', CONCAT(' AND SRC_ADDRESS = ''', IP, ''''), ''),
            IF(assetList IS NOT NULL AND assetList != '''', CONCAT(' AND (SRC_ETH_ADDRESS IN (', assetList, ') OR DST_ETH_ADDRESS IN (', assetList, '))'),''),
            ' GROUP BY MEASURE_START_DATE
        ) AS CombinedData
        LEFT JOIN analytics.THRESHOLD AS T ON T.THRESHOLD_ID = 2
        GROUP BY time
        ORDER BY time;');

    PREPARE stmt FROM @sqlQuery;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;
END ;;
DELIMITER ;
/*!50003 SET sql_mode              = @saved_sql_mode */ ;
/*!50003 SET character_set_client  = @saved_cs_client */ ;
/*!50003 SET character_set_results = @saved_cs_results */ ;
/*!50003 SET collation_connection  = @saved_col_connection */ ;
/*!50003 DROP PROCEDURE IF EXISTS `GetProtocolDistribution` */;
/*!50003 SET @saved_cs_client      = @@character_set_client */ ;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;
/*!50003 SET character_set_client  = latin1 */ ;
/*!50003 SET character_set_results = latin1 */ ;
/*!50003 SET collation_connection  = latin1_swedish_ci */ ;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;
/*!50003 SET sql_mode              = '' */ ;
DELIMITER ;;
CREATE DEFINER=`analytics_dev`@`%` PROCEDURE `GetProtocolDistribution`(
    IN Device VARCHAR(255), 
    IN fromTime INT, 
    IN toTime INT,
    IN assetList TEXT
)
BEGIN
    SET @sql = CONCAT('
        SELECT
            SUM(CASE WHEN PROTOCOL = ''TCP'' OR PROTOCOL = ''MODBUS'' THEN 1 ELSE 0 END) AS "TCP Protocol",
            SUM(CASE WHEN PROTOCOL = ''UDP'' THEN 1 ELSE 0 END) AS "UDP Protocol",
            SUM(CASE WHEN PROTOCOL = ''ICMP'' THEN 1 ELSE 0 END) AS "ICMP Protocol",
            SUM(CASE WHEN PROTOCOL = ''IP_NIP'' AND MEASURE_TYPE = ''ip_proto'' AND MEASURE_VALUE = 255 THEN 1 ELSE 0 END) + SUM(CASE WHEN PROTOCOL = ''ARP'' THEN 1 ELSE 0 END) AS "Non IP Protocol",
            SUM(CASE WHEN PROTOCOL = ''IP_NIP'' OR PROTOCOL = ''ARP'' THEN 1 ELSE 0 END) - SUM(CASE WHEN PROTOCOL = ''IP_NIP'' AND MEASURE_TYPE = ''ip_proto'' AND MEASURE_VALUE = 255 THEN 1 ELSE 0 END) - SUM(CASE WHEN PROTOCOL = ''ARP'' THEN 1 ELSE 0 END) AS "Other IP Protocol"
        FROM 
            analytics.TELEMETRY_HEADER PARTITION (', Device, ') AS TH
        LEFT JOIN analytics.TELEMETRY PARTITION (', Device, ') ON 
            TH.TELEMETRY_GUID = analytics.TELEMETRY.TELEMETRY_GUID 
            AND analytics.TELEMETRY.MEASURE_TYPE = ''ip_proto''
        WHERE MEASURE_START_DATE BETWEEN ', fromTime, ' AND ', toTime,
        IF(assetList IS NOT NULL AND assetList != '''', CONCAT(' AND (SRC_ETH_ADDRESS IN (', assetList, ') OR DST_ETH_ADDRESS IN (', assetList, '))'),''),
        '  ');

    PREPARE stmt FROM @sql;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;
END ;;
DELIMITER ;
/*!50003 SET sql_mode              = @saved_sql_mode */ ;
/*!50003 SET character_set_client  = @saved_cs_client */ ;
/*!50003 SET character_set_results = @saved_cs_results */ ;
/*!50003 SET collation_connection  = @saved_col_connection */ ;
/*!50003 DROP PROCEDURE IF EXISTS `GetProtocolGraphDistribution` */;
/*!50003 SET @saved_cs_client      = @@character_set_client */ ;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;
/*!50003 SET character_set_client  = latin1 */ ;
/*!50003 SET character_set_results = latin1 */ ;
/*!50003 SET collation_connection  = latin1_swedish_ci */ ;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;
/*!50003 SET sql_mode              = '' */ ;
DELIMITER ;;
CREATE DEFINER=`analytics_dev`@`%` PROCEDURE `GetProtocolGraphDistribution`(
    IN Device VARCHAR(255), 
    IN fromTime INT, 
    IN toTime INT,
    IN assetList TEXT
)
BEGIN
    SET @sql = CONCAT('
        SELECT MEASURE_START_DATE DIV 30 * 30 AS time,
            SUM(CASE WHEN PROTOCOL = ''TCP'' OR PROTOCOL = ''MODBUS'' THEN 1 ELSE 0 END) AS "TCP Protocol",
            SUM(CASE WHEN PROTOCOL = ''UDP'' THEN 1 ELSE 0 END) AS "UDP Protocol",
            SUM(CASE WHEN PROTOCOL = ''ICMP'' THEN 1 ELSE 0 END) AS "ICMP Protocol",
            SUM(CASE WHEN PROTOCOL = ''IP_NIP'' AND MEASURE_TYPE = ''ip_proto'' AND MEASURE_VALUE = 255 THEN 1 ELSE 0 END) + SUM(CASE WHEN PROTOCOL = ''ARP'' THEN 1 ELSE 0 END) AS "Non IP Protocol",
            SUM(CASE WHEN PROTOCOL = ''IP_NIP'' OR PROTOCOL = ''ARP'' THEN 1 ELSE 0 END) - SUM(CASE WHEN PROTOCOL = ''IP_NIP'' AND MEASURE_TYPE = ''ip_proto'' AND MEASURE_VALUE = 255 THEN 1 ELSE 0 END) - SUM(CASE WHEN PROTOCOL = ''ARP'' THEN 1 ELSE 0 END) AS "Other IP Protocol"
        FROM 
            analytics.TELEMETRY_HEADER PARTITION (', Device, ') AS TH
        LEFT JOIN analytics.TELEMETRY PARTITION (', Device, ') ON 
            TH.TELEMETRY_GUID = analytics.TELEMETRY.TELEMETRY_GUID 
            AND analytics.TELEMETRY.MEASURE_TYPE = ''ip_proto''
        WHERE MEASURE_START_DATE BETWEEN ', fromTime, ' AND ', toTime,
        IF(assetList IS NOT NULL AND assetList != '''', CONCAT(' AND (SRC_ETH_ADDRESS IN (', assetList, ') OR DST_ETH_ADDRESS IN (', assetList, '))'),''),
        'GROUP BY time
        ORDER BY time; ');

    PREPARE stmt FROM @sql;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;
END ;;
DELIMITER ;
/*!50003 SET sql_mode              = @saved_sql_mode */ ;
/*!50003 SET character_set_client  = @saved_cs_client */ ;
/*!50003 SET character_set_results = @saved_cs_results */ ;
/*!50003 SET collation_connection  = @saved_col_connection */ ;
/*!50003 DROP PROCEDURE IF EXISTS `GetTCPFlags` */;
/*!50003 SET @saved_cs_client      = @@character_set_client */ ;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;
/*!50003 SET character_set_client  = latin1 */ ;
/*!50003 SET character_set_results = latin1 */ ;
/*!50003 SET collation_connection  = latin1_swedish_ci */ ;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;
/*!50003 SET sql_mode              = '' */ ;
DELIMITER ;;
CREATE DEFINER=`analytics_dev`@`%` PROCEDURE `GetTCPFlags`(
    IN Device VARCHAR(255),
    IN fromTime INT,
    IN toTime INT,
    IN IP VARCHAR(255),
    IN assetList TEXT
)
BEGIN
    SET @sqlQuery = CONCAT('
        SELECT 
            MEASURE_START_DATE DIV 30 * 30 AS time,
            SUM(
                CASE 
                    WHEN (PROTOCOL = ''TCP'' OR PROTOCOL = ''MODBUS'') AND MEASURE_TYPE = ''count_when_syn_flag_set'' 
                    THEN MEASURE_VALUE
                    ELSE 0
                END
            ) AS "TCP SYN",
            SUM(
                CASE 
                    WHEN (PROTOCOL = ''TCP'' OR PROTOCOL = ''MODBUS'') AND MEASURE_TYPE = ''Count_of_SYN_ACK_Flag_Set'' 
                    THEN MEASURE_VALUE
                    ELSE 0
                END
            ) AS "TCP SYN ACK",
            SUM(
                CASE 
                    WHEN (PROTOCOL = ''TCP'' OR PROTOCOL = ''MODBUS'') AND MEASURE_TYPE = ''count_when_reset_flag_set'' 
                    THEN MEASURE_VALUE
                    ELSE 0
                END
            ) AS "TCP RST"
        FROM analytics.TELEMETRY_HEADER PARTITION (', Device, ') AS H
        INNER JOIN analytics.TELEMETRY PARTITION (', Device, ') AS T 
            ON H.TELEMETRY_GUID = T.TELEMETRY_GUID 
            AND MEASURE_TYPE IN (''count_when_syn_flag_set'', ''Count_of_SYN_ACK_Flag_Set'', ''count_when_reset_flag_set'')
        WHERE MEASURE_START_DATE BETWEEN ', fromTime, ' AND ', toTime,
        IF(IP IS NOT NULL AND IP != '''', CONCAT(' AND SRC_ADDRESS = ''', IP, ''' AND (PROTOCOL = ''TCP'' OR PROTOCOL = ''MODBUS'')'), ''),
        IF(assetList IS NOT NULL AND assetList != '''', CONCAT(' AND (SRC_ETH_ADDRESS IN (', assetList, ') OR DST_ETH_ADDRESS IN (', assetList, '))'),''),
        ' GROUP BY time
        ORDER BY time;');

    PREPARE stmt FROM @sqlQuery;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;
END ;;
DELIMITER ;
/*!50003 SET sql_mode              = @saved_sql_mode */ ;
/*!50003 SET character_set_client  = @saved_cs_client */ ;
/*!50003 SET character_set_results = @saved_cs_results */ ;
/*!50003 SET collation_connection  = @saved_col_connection */ ;
/*!50003 DROP PROCEDURE IF EXISTS `GetTCPNetworkConnection` */;
/*!50003 SET @saved_cs_client      = @@character_set_client */ ;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;
/*!50003 SET character_set_client  = latin1 */ ;
/*!50003 SET character_set_results = latin1 */ ;
/*!50003 SET collation_connection  = latin1_swedish_ci */ ;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;
/*!50003 SET sql_mode              = '' */ ;
DELIMITER ;;
CREATE DEFINER=`analytics_dev`@`%` PROCEDURE `GetTCPNetworkConnection`(
    IN Device VARCHAR(255), 
    IN fromTime INT, 
    IN toTime INT
)
BEGIN
    SET @sql = CONCAT('
        SELECT MAX(flowindicator) - MIN(flowindicator) + 1 as tcpCount
        FROM (
            SELECT 
                MIN(b.MEASURE_VALUE) as flowindicator, 
                a.SRC_ADDRESS, 
                a.SRC_PORT, 
                a.DST_ADDRESS, 
                a.DST_PORT 
            FROM analytics.TELEMETRY_HEADER PARTITION (', Device, ') a
            INNER JOIN analytics.TELEMETRY PARTITION (', Device, ') b 
                ON a.TELEMETRY_GUID = b.TELEMETRY_GUID 
                AND b.MEASURE_TYPE = ''flowindicator''
            WHERE b.MEASURE_DATE BETWEEN ', fromTime, ' AND ', toTime, '
                AND a.PROTOCOL IN (''TCP'', ''MODBUS'') 
                AND a.MEASURE_START_DATE BETWEEN ', fromTime, ' AND ', toTime, '
            GROUP BY a.SRC_ADDRESS, a.SRC_PORT, a.DST_ADDRESS, a.DST_PORT, a.PROTOCOL
        ) as subquery');

    PREPARE stmt FROM @sql;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;
END ;;
DELIMITER ;
/*!50003 SET sql_mode              = @saved_sql_mode */ ;
/*!50003 SET character_set_client  = @saved_cs_client */ ;
/*!50003 SET character_set_results = @saved_cs_results */ ;
/*!50003 SET collation_connection  = @saved_col_connection */ ;
/*!50003 DROP PROCEDURE IF EXISTS `GetThroughput` */;
/*!50003 SET @saved_cs_client      = @@character_set_client */ ;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;
/*!50003 SET character_set_client  = latin1 */ ;
/*!50003 SET character_set_results = latin1 */ ;
/*!50003 SET collation_connection  = latin1_swedish_ci */ ;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;
/*!50003 SET sql_mode              = '' */ ;
DELIMITER ;;
CREATE DEFINER=`analytics_dev`@`%` PROCEDURE `GetThroughput`(
    IN Device VARCHAR(255),
    IN fromTime INT,
    IN toTime INT,
    IN IP VARCHAR(255),
    IN assetList TEXT
)
BEGIN
    SET @sqlQuery = CONCAT('
        SELECT 
            MEASURE_START_DATE DIV 30 * 30 AS time,
            SUM(
                CASE 
                    WHEN (PROTOCOL = ''IP_NIP'' OR PROTOCOL = ''ARP'' OR PROTOCOL = ''UDP'') AND 
                        T.MEASURE_TYPE = ''eth_dst'' AND (SUBSTRING(LPAD(CONV(SUBSTRING(T.MEASURE_STRING, 1, 2), 16, 2), 8, ''0''), 8, 8) = 0)
                        THEN 1
                    WHEN (PROTOCOL != ''IP_NIP'' AND PROTOCOL != ''ARP'' AND PROTOCOL != ''UDP'') THEN 1
                    ELSE 0
                END
            ) AS Unicast,
            SUM(
                CASE 
                    WHEN (PROTOCOL = ''IP_NIP'' OR PROTOCOL = ''UDP'') AND 
                        T.MEASURE_TYPE = ''eth_dst'' AND 
                        (((SUBSTRING(LPAD(CONV(SUBSTRING(T.MEASURE_STRING, 1, 2), 16, 2), 8, ''0''), 8, 8) = 1) OR
                        (SUBSTRING(LPAD(CONV(SUBSTRING_INDEX(DST_ETH_ADDRESS, ''.'', 1),10,2), 8, ''0''), 1, 4) = ''1110'')) AND (T.MEASURE_STRING NOT LIKE ''ff:ff:ff:ff:ff:ff''))
                        THEN 1
                    WHEN (PROTOCOL = ''ARP'') AND 
                        T.MEASURE_TYPE = ''eth_dst'' AND 
                        ((SUBSTRING(LPAD(CONV(SUBSTRING(T.MEASURE_STRING, 1, 2), 16, 2), 8, ''0''), 8, 8) = 1) AND (T.MEASURE_STRING NOT LIKE ''ff:ff:ff:ff:ff:ff''))
                        THEN 1
                    ELSE 0
                END
            ) AS Multicast,
            SUM(
                CASE 
                    WHEN (PROTOCOL = ''IP_NIP'' OR PROTOCOL = ''UDP'') AND 
                        T.MEASURE_TYPE = ''eth_dst'' AND 
                        ((T.MEASURE_STRING LIKE ''ff:ff:ff:ff:ff:ff'') OR (DST_ETH_ADDRESS = ''255.255.255.255'') OR 
                        (SUBSTRING(LPAD(CONV(SUBSTRING_INDEX(DST_ETH_ADDRESS, ''.'', 1),10,2), 8, ''0''), 1, 1) = ''0'' AND SUBSTRING_INDEX(DST_ETH_ADDRESS, ''.'', -3) = ''255.255.255'') OR
                        (SUBSTRING(LPAD(CONV(SUBSTRING_INDEX(DST_ETH_ADDRESS, ''.'', 1),10,2), 8, ''0''), 1, 2) = ''10'' AND SUBSTRING_INDEX(DST_ETH_ADDRESS, ''.'', -2) = ''255.255'') OR 
                        (SUBSTRING(LPAD(CONV(SUBSTRING_INDEX(DST_ETH_ADDRESS, ''.'', 1),10,2), 8, ''0''), 1, 3) = ''110'' AND SUBSTRING_INDEX(DST_ETH_ADDRESS, ''.'', -1) = ''255''))
                        THEN 1
                    WHEN (PROTOCOL = ''ARP'') AND 
                        T.MEASURE_TYPE = ''eth_dst'' AND 
                        (T.MEASURE_STRING LIKE ''ff:ff:ff:ff:ff:ff'')
                        THEN 1
                    ELSE 0
                END
            ) AS Broadcast,
            SUM(
                CASE 
                    WHEN (PROTOCOL = ''TCP'' OR PROTOCOL = ''MODBUS'') AND 
                        (T.MEASURE_TYPE = ''In_Out_Local_Traffic'' AND T.MEASURE_VALUE = 1) AND
                        (V.MEASURE_TYPE = ''count_of_frames_sent'')
                        THEN V.MEASURE_VALUE
                    WHEN (PROTOCOL = ''UDP'' OR PROTOCOL = ''ICMP'') AND 
                        (T.MEASURE_TYPE = ''In_Out_Local_Traffic'' AND T.MEASURE_VALUE = 1) AND
                        (V.MEASURE_TYPE = ''num_of_frames_withinwindow_per_src_dst'')
                        THEN V.MEASURE_VALUE
                    ELSE 0
                END
            ) AS Local,
            SUM(
                CASE 
                    WHEN (PROTOCOL = ''TCP'' OR PROTOCOL = ''MODBUS'') AND 
                        (T.MEASURE_TYPE = ''In_Out_Local_Traffic'' AND T.MEASURE_VALUE = 2) AND
                        (V.MEASURE_TYPE = ''count_of_frames_sent'')
                        THEN V.MEASURE_VALUE
                    WHEN (PROTOCOL = ''UDP'' OR PROTOCOL = ''ICMP'') AND 
                        (T.MEASURE_TYPE = ''In_Out_Local_Traffic'' AND T.MEASURE_VALUE = 2) AND
                        (V.MEASURE_TYPE = ''num_of_frames_withinwindow_per_src_dst'')
                        THEN V.MEASURE_VALUE
                    ELSE 0
                END
            ) AS Inbound,
            SUM(
                CASE 
                    WHEN (PROTOCOL = ''TCP'' OR PROTOCOL = ''MODBUS'') AND 
                        (T.MEASURE_TYPE = ''In_Out_Local_Traffic'' AND T.MEASURE_VALUE = 3) AND
                        (V.MEASURE_TYPE = ''count_of_frames_sent'')
                        THEN V.MEASURE_VALUE
                    WHEN (PROTOCOL = ''UDP'' OR PROTOCOL = ''ICMP'') AND 
                        (T.MEASURE_TYPE = ''In_Out_Local_Traffic'' AND T.MEASURE_VALUE = 3) AND
                        (V.MEASURE_TYPE = ''num_of_frames_withinwindow_per_src_dst'')
                        THEN V.MEASURE_VALUE
                    ELSE 0
                END
            ) AS Outbound
        FROM analytics.TELEMETRY_HEADER PARTITION (', Device, ') AS H
        INNER JOIN analytics.TELEMETRY PARTITION (', Device, ') AS T ON H.TELEMETRY_GUID = T.TELEMETRY_GUID AND (T.MEASURE_TYPE = ''eth_dst'' OR T.MEASURE_TYPE = ''In_Out_Local_Traffic'')
        INNER JOIN analytics.TELEMETRY PARTITION (', Device, ') AS V ON H.TELEMETRY_GUID = V.TELEMETRY_GUID AND (V.MEASURE_TYPE = ''count_of_frames_sent'' OR V.MEASURE_TYPE = ''num_of_frames_withinwindow_per_src_dst'')
        WHERE MEASURE_START_DATE BETWEEN ', fromTime, ' AND ', toTime,
        IF(IP IS NOT NULL AND IP != '''', CONCAT(' AND SRC_ADDRESS = ''', IP, ''''), ''),
        IF(assetList IS NOT NULL AND assetList != '''', CONCAT(' AND (SRC_ETH_ADDRESS IN (', assetList, ') OR DST_ETH_ADDRESS IN (', assetList, '))'),''),
        ' GROUP BY time
        ORDER BY time;');

    PREPARE stmt FROM @sqlQuery;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;
END ;;
DELIMITER ;
/*!50003 SET sql_mode              = @saved_sql_mode */ ;
/*!50003 SET character_set_client  = @saved_cs_client */ ;
/*!50003 SET character_set_results = @saved_cs_results */ ;
/*!50003 SET collation_connection  = @saved_col_connection */ ;
/*!50003 DROP PROCEDURE IF EXISTS `GetTopologyGraph` */;
/*!50003 SET @saved_cs_client      = @@character_set_client */ ;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;
/*!50003 SET character_set_client  = latin1 */ ;
/*!50003 SET character_set_results = latin1 */ ;
/*!50003 SET collation_connection  = latin1_swedish_ci */ ;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;
/*!50003 SET sql_mode              = '' */ ;
DELIMITER ;;
CREATE DEFINER=`analytics_dev`@`%` PROCEDURE `GetTopologyGraph`(
    IN Device VARCHAR(255),
    IN fromTime INT,
    IN toTime INT,
    IN asset VARCHAR(255),
    IN assetList TEXT
)
BEGIN
    SET @sqlQuery = CONCAT('
    SELECT COALESCE(SRC_ADDRESS, SRC_ETH_ADDRESS) AS Source,
           COALESCE(DST_ADDRESS, DST_ETH_ADDRESS) AS Destination,
           COUNT(*) as flow_count
    FROM analytics.TELEMETRY_HEADER PARTITION (', Device, ') AS H
    WHERE MEASURE_START_DATE BETWEEN ', fromTime, ' AND ', toTime,
    IF(asset IS NOT NULL AND asset != '', CONCAT(' AND (COALESCE(SRC_ADDRESS, SRC_ETH_ADDRESS) = ''', asset, ''' OR COALESCE(DST_ADDRESS, DST_ETH_ADDRESS) = ''', asset, ''')'),''),
    IF(assetList IS NOT NULL AND assetList != '''', CONCAT(' AND (SRC_ETH_ADDRESS IN (', assetList, ') OR DST_ETH_ADDRESS IN (', assetList, '))'),''),
    '
    GROUP BY Source, Destination
    ORDER BY MEASURE_START_DATE
    LIMIT 50;');

    PREPARE stmt FROM @sqlQuery;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;
END ;;
DELIMITER ;
/*!50003 SET sql_mode              = @saved_sql_mode */ ;
/*!50003 SET character_set_client  = @saved_cs_client */ ;
/*!50003 SET character_set_results = @saved_cs_results */ ;
/*!50003 SET collation_connection  = @saved_col_connection */ ;
/*!50003 DROP PROCEDURE IF EXISTS `GetTopologyGraphPipeSeparated` */;
/*!50003 SET @saved_cs_client      = @@character_set_client */ ;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;
/*!50003 SET character_set_client  = latin1 */ ;
/*!50003 SET character_set_results = latin1 */ ;
/*!50003 SET collation_connection  = latin1_swedish_ci */ ;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;
/*!50003 SET sql_mode              = '' */ ;
DELIMITER ;;
CREATE DEFINER=`analytics_dev`@`%` PROCEDURE `GetTopologyGraphPipeSeparated`(
    IN Device VARCHAR(255),
    IN fromTime INT,
    IN toTime INT,
    IN asset VARCHAR(255),
    IN assetList TEXT
)
BEGIN
    SET @sqlQuery = CONCAT('
    SELECT CASE 
               WHEN SRC_ADDRESS IS NOT NULL THEN CONCAT(SRC_ADDRESS, ''|'', SRC_ETH_ADDRESS)
               ELSE SRC_ETH_ADDRESS
           END AS Source,
           CASE 
               WHEN DST_ADDRESS IS NOT NULL THEN CONCAT(DST_ADDRESS, ''|'', DST_ETH_ADDRESS)
               ELSE DST_ETH_ADDRESS
           END AS Destination,
           COUNT(*) as flow_count
    FROM analytics.TELEMETRY_HEADER PARTITION (', Device, ') AS H
    WHERE MEASURE_START_DATE BETWEEN ', fromTime, ' AND ', toTime,
    IF(asset IS NOT NULL AND asset != '', CONCAT(' AND (COALESCE(SRC_ADDRESS, SRC_ETH_ADDRESS) = ''', asset, ''' OR COALESCE(DST_ADDRESS, DST_ETH_ADDRESS) = ''', asset, ''')'),''),
    IF(assetList IS NOT NULL AND assetList != '''', CONCAT(' AND (SRC_ETH_ADDRESS IN (', assetList, ') OR DST_ETH_ADDRESS IN (', assetList, '))'),''), 
    ' GROUP BY Source, Destination
    ORDER BY MEASURE_START_DATE
    LIMIT 50;');

    PREPARE stmt FROM @sqlQuery;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;
END ;;
DELIMITER ;
/*!50003 SET sql_mode              = @saved_sql_mode */ ;
/*!50003 SET character_set_client  = @saved_cs_client */ ;
/*!50003 SET character_set_results = @saved_cs_results */ ;
/*!50003 SET collation_connection  = @saved_col_connection */ ;
/*!50003 DROP PROCEDURE IF EXISTS `GetTrafficCountBroadcast` */;
/*!50003 SET @saved_cs_client      = @@character_set_client */ ;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;
/*!50003 SET character_set_client  = latin1 */ ;
/*!50003 SET character_set_results = latin1 */ ;
/*!50003 SET collation_connection  = latin1_swedish_ci */ ;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;
/*!50003 SET sql_mode              = '' */ ;
DELIMITER ;;
CREATE DEFINER=`analytics_dev`@`%` PROCEDURE `GetTrafficCountBroadcast`(
    IN Device VARCHAR(255), 
    IN fromTime INT, 
    IN toTime INT,
        IN timeInterval INT,
    IN assetList TEXT
)
BEGIN
    SET @sql_arp = CONCAT('select count(*) as count 
            FROM analytics.TELEMETRY_HEADER PARTITION (', Device, ')
                        as TH
                        where PROTOCOL = ''ARP''  AND DST_ETH_ADDRESS = ''ff:ff:ff:ff:ff:ff''
                        and     MEASURE_START_DATE BETWEEN ', fromTime, ' AND ', toTime , IF(assetList IS NOT NULL AND assetList != '''', CONCAT(' AND (SRC_ETH_ADDRESS IN (', assetList, ') OR DST_ETH_ADDRESS IN (', assetList, '))'),''),''  
                         );


    SET @sql_ipnip = CONCAT('select count(*) as count 
            FROM analytics.TELEMETRY_HEADER PARTITION (', Device, ') as TH
    LEFT JOIN 
      analytics.TELEMETRY PARTITION (', Device, ') as ip_proto
      ON TH.TELEMETRY_GUID = ip_proto.TELEMETRY_GUID 
      AND ip_proto.MEASURE_TYPE = ''ip_proto''
    LEFT JOIN 
      analytics.TELEMETRY PARTITION (', Device, ') as eth_type
      ON TH.TELEMETRY_GUID = eth_type.TELEMETRY_GUID 
      AND eth_type.MEASURE_TYPE = ''eth_type''
    LEFT JOIN 
      analytics.ETHER_TYPE et
      ON eth_type.MEASURE_VALUE = et.ETHER_TYPE
    WHERE 
      TH.PROTOCOL = ''IP_NIP'' 
      AND (
        (TH.DST_ETH_ADDRESS LIKE ''ff:ff:ff:ff:ff:ff'') 
        OR TH.DST_ADDRESS = ''255.255.255.255'' 
        OR (SUBSTRING(LPAD(CONV(SUBSTRING_INDEX(TH.DST_ADDRESS, ''.'', 1),10,2), 8, ''0''), 1, 1) = ''0'' AND SUBSTRING_INDEX(TH.DST_ADDRESS, ''.'', -3) = ''255.255.255'') 
        OR (SUBSTRING(LPAD(CONV(SUBSTRING_INDEX(TH.DST_ADDRESS, ''.'', 1),10,2), 8, ''0''), 1, 2) = ''10'' AND SUBSTRING_INDEX(TH.DST_ADDRESS, ''.'', -2) = ''255.255'' ) 
        OR (SUBSTRING(LPAD(CONV(SUBSTRING_INDEX(TH.DST_ADDRESS, ''.'', 1),10,2), 8, ''0''), 1, 3) = ''110'' AND SUBSTRING_INDEX(TH.DST_ADDRESS, ''.'', -1) = ''255'')
      )
      AND TH.MEASURE_START_DATE BETWEEN ', fromTime, ' AND ', toTime, IF(assetList IS NOT NULL AND assetList != '''', CONCAT(' AND (TH.SRC_ETH_ADDRESS IN (', assetList, ') OR TH.DST_ETH_ADDRESS IN (', assetList, '))'),''),'
      AND (et.ETHER_NAME <> ''Unknown'' OR et.ETHER_NAME IS NULL)
');

                         
    SET @sql_udp = CONCAT('select count(*) as count from (
 select *, CONCAT(CEIL((max(MEASURE_END_DATE) over ( partition by PROTOCOL,ip_src_pair,port_src_pair,ip_dst_pair,port_dst_pair,Flow_Indicator) - Time )), '' s'') as Duration from (   
      SELECT SRC_ADDRESS as Source_IP, 
                                DST_ADDRESS as Destination_IP, 
                Record_Indicator,
                SUM(Record_Indicator) over ( partition by PROTOCOL,ip_src_pair,port_src_pair,ip_dst_pair,port_dst_pair order by MEASURE_START_DATE) as Flow_Indicator,
                COALESCE( (CASE 
                                WHEN PROTOCOL = ''UDP'' THEN COALESCE(P2_UDP_SERVICE_NAME, P1_UDP_SERVICE_NAME, ''UDP'')
                ELSE NULL 
            END), ''Other'') AS Service,
                                CAST(SUBSTRING_INDEX(SRC_PORT, ''.'', 1) AS UNSIGNED) AS Source_Port,
            CAST(SUBSTRING_INDEX(DST_PORT, ''.'', 1) AS UNSIGNED) AS Destination_Port,
                        SRC_ETH_ADDRESS AS Source_MAC,
            DST_ETH_ADDRESS AS Destination_MAC,
            PROTOCOL AS PROTOCOL,
            MEASURE_START_DATE AS Time,
            MEASURE_END_DATE,
                        Previous_Time,
                        LD,ip_src_pair,port_src_pair,ip_dst_pair,port_dst_pair
            FROM 
            (select SRC_ADDRESS, SRC_PORT, DST_ADDRESS, DST_PORT, DST_ETH_ADDRESS,SRC_ETH_ADDRESS, PROTOCOL,P1_UDP_SERVICE_NAME, P2_UDP_SERVICE_NAME, MEASURE_START_DATE,MEASURE_END_DATE,Previous_Time,LD,ip_src_pair,port_src_pair,ip_dst_pair,port_dst_pair,
                   case
                when (MEASURE_START_DATE - Previous_Time > ', timeInterval, ') then 1                
                when (LD is null and Previous_Time is null)  then 1 
                when (Previous_Time is null)  then 1
                when (LD - MEASURE_START_DATE > ', timeInterval, ') then 0
                when (LD is null) then 0
                else NULL end as Record_Indicator
            from (select DEVICE_ID,SRC_ADDRESS,SRC_PORT,DST_ADDRESS,DST_PORT,PROTOCOL,MEASURE_START_DATE,MEASURE_END_DATE, DST_ETH_ADDRESS, SRC_ETH_ADDRESS, LEAD(MEASURE_START_DATE) OVER
            (PARTITION BY PROTOCOL,ip_src_pair,port_src_pair,ip_dst_pair,port_dst_pair order by MEASURE_START_DATE) AS LD,
                        LAG(MEASURE_START_DATE) OVER
            (PARTITION BY PROTOCOL,ip_src_pair,port_src_pair,ip_dst_pair,port_dst_pair order by MEASURE_START_DATE ) as Previous_Time  
                        ,ip_src_pair,port_src_pair,ip_dst_pair,port_dst_pair,P1_UDP_SERVICE_NAME ,P2_UDP_SERVICE_NAME
                        FROM (SELECT DEVICE_ID, SRC_ADDRESS,SRC_PORT,DST_ADDRESS,DST_PORT,PROTOCOL,MEASURE_START_DATE, MEASURE_END_DATE, DST_ETH_ADDRESS, SRC_ETH_ADDRESS,
            CASE WHEN (SRC_ADDRESS < DST_ADDRESS) THEN SRC_ADDRESS ELSE DST_ADDRESS END as ip_src_pair ,
            CASE WHEN (SRC_ADDRESS < DST_ADDRESS) THEN DST_ADDRESS ELSE SRC_ADDRESS END as ip_dst_pair, 
            CASE WHEN (SRC_ADDRESS < DST_ADDRESS) THEN SRC_PORT ELSE DST_PORT END as port_src_pair ,
            CASE WHEN (SRC_ADDRESS < DST_ADDRESS) THEN DST_PORT ELSE SRC_PORT END as port_dst_pair,
                        P1.UDP_SERVICE_NAME as P1_UDP_SERVICE_NAME ,P2.UDP_SERVICE_NAME as P2_UDP_SERVICE_NAME
            FROM analytics.TELEMETRY_HEADER PARTITION (', Device, ')
                        as TH
                        LEFT JOIN analytics.PORT AS P1 ON
            P1.PORT_NUMBER = TH.SRC_PORT
            LEFT JOIN analytics.PORT AS P2 ON
            P2.PORT_NUMBER = TH.DST_PORT
                        where PROTOCOL = ''UDP'' and ((DST_ETH_ADDRESS LIKE ''ff:ff:ff:ff:ff:ff'') OR
            DST_ADDRESS = ''255.255.255.255'' OR 
              (SUBSTRING(LPAD(CONV(SUBSTRING_INDEX(DST_ADDRESS, ''.'', 1),10,2), 8, ''0''), 1, 1) = ''0'' AND SUBSTRING_INDEX(DST_ADDRESS, ''.'', -3) = ''255.255.255'') OR
              (SUBSTRING(LPAD(CONV(SUBSTRING_INDEX(DST_ADDRESS, ''.'', 1),10,2), 8, ''0''), 1, 2) = ''10'' AND SUBSTRING_INDEX(DST_ADDRESS, ''.'', -2) = ''255.255'' ) OR 
              (SUBSTRING(LPAD(CONV(SUBSTRING_INDEX(DST_ADDRESS, ''.'', 1),10,2), 8, ''0''), 1, 3) = ''110'' AND SUBSTRING_INDEX(DST_ADDRESS, ''.'', -1) = ''255'')
            )', IF(assetList IS NOT NULL AND assetList != '''', CONCAT(' AND (SRC_ETH_ADDRESS IN (', assetList, ') OR DST_ETH_ADDRESS IN (', assetList, '))'),' '),'
                         AND MEASURE_START_DATE BETWEEN ', fromTime, ' AND ', toTime, ')as b) a
                        ) as data 
            ) final ) multicast  where Record_Indicator = 1'  
                         );


    PREPARE stmt FROM @sql_udp;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;                     

    PREPARE stmt FROM @sql_ipnip;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;

    PREPARE stmt FROM @sql_arp;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;


END ;;
DELIMITER ;
/*!50003 SET sql_mode              = @saved_sql_mode */ ;
/*!50003 SET character_set_client  = @saved_cs_client */ ;
/*!50003 SET character_set_results = @saved_cs_results */ ;
/*!50003 SET collation_connection  = @saved_col_connection */ ;
/*!50003 DROP PROCEDURE IF EXISTS `GetTrafficCountMulticast` */;
/*!50003 SET @saved_cs_client      = @@character_set_client */ ;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;
/*!50003 SET character_set_client  = latin1 */ ;
/*!50003 SET character_set_results = latin1 */ ;
/*!50003 SET collation_connection  = latin1_swedish_ci */ ;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;
/*!50003 SET sql_mode              = '' */ ;
DELIMITER ;;
CREATE DEFINER=`analytics_dev`@`%` PROCEDURE `GetTrafficCountMulticast`(
    IN Device VARCHAR(255), 
    IN fromTime INT, 
    IN toTime INT,
        IN timeInterval INT,
    IN assetList TEXT
)
BEGIN
    SET @sql_ipnip = CONCAT('select count(*) as count 
            FROM 
        analytics.TELEMETRY_HEADER PARTITION (', Device, ') AS TH
    LEFT JOIN 
        analytics.TELEMETRY PARTITION (', Device, ') AS ip_proto
        ON TH.TELEMETRY_GUID = ip_proto.TELEMETRY_GUID 
        AND ip_proto.MEASURE_TYPE = ''ip_proto''
    LEFT JOIN 
        analytics.TELEMETRY PARTITION (', Device, ') AS eth_type
        ON TH.TELEMETRY_GUID = eth_type.TELEMETRY_GUID 
        AND eth_type.MEASURE_TYPE = ''eth_type''
    LEFT JOIN 
        analytics.ETHER_TYPE et
        ON eth_type.MEASURE_VALUE = et.ETHER_TYPE
    WHERE 
        TH.PROTOCOL = ''IP_NIP''  
        AND LEFT(REPLACE(TH.DST_ETH_ADDRESS, '':'', ''''), 6) = ''01005E'' 
        AND CONV(SUBSTRING_INDEX(TH.DST_ADDRESS, ''.'', 1), 10, 2) LIKE ''1110%''
        AND TH.MEASURE_START_DATE BETWEEN ', fromTime, ' AND ', toTime,IF(assetList IS NOT NULL AND assetList != '''',CONCAT(' AND (TH.SRC_ETH_ADDRESS IN (', assetList, ') OR TH.DST_ETH_ADDRESS IN (', assetList, '))'),''),'
        AND (et.ETHER_NAME <> ''Unknown'' OR et.ETHER_NAME IS NULL)       
');


    SET @sql_udp = CONCAT('select count(*) as count from (
 select *, CONCAT(CEIL((max(MEASURE_END_DATE) over ( partition by PROTOCOL,ip_src_pair,port_src_pair,ip_dst_pair,port_dst_pair,Flow_Indicator) - Time )), '' s'') as Duration from (   
      SELECT SRC_ADDRESS as Source_IP, 
                                DST_ADDRESS as Destination_IP, 
                Record_Indicator,
                SUM(Record_Indicator) over ( partition by PROTOCOL,ip_src_pair,port_src_pair,ip_dst_pair,port_dst_pair order by MEASURE_START_DATE) as Flow_Indicator,
                COALESCE( (CASE 
                                WHEN PROTOCOL = ''UDP'' THEN COALESCE(P2_UDP_SERVICE_NAME, P1_UDP_SERVICE_NAME, ''UDP'')
                ELSE NULL 
            END), ''Other'') AS Service,
                                CAST(SUBSTRING_INDEX(SRC_PORT, ''.'', 1) AS UNSIGNED) AS Source_Port,
            CAST(SUBSTRING_INDEX(DST_PORT, ''.'', 1) AS UNSIGNED) AS Destination_Port,
                        SRC_ETH_ADDRESS AS Source_MAC,
            DST_ETH_ADDRESS AS Destination_MAC,
            PROTOCOL AS PROTOCOL,
            MEASURE_START_DATE AS Time,
            MEASURE_END_DATE,
                        Previous_Time,
                        LD,ip_src_pair,port_src_pair,ip_dst_pair,port_dst_pair
            FROM 
            (select SRC_ADDRESS, SRC_PORT, DST_ADDRESS, DST_PORT, DST_ETH_ADDRESS,SRC_ETH_ADDRESS, PROTOCOL,P1_UDP_SERVICE_NAME, P2_UDP_SERVICE_NAME, MEASURE_START_DATE,MEASURE_END_DATE,Previous_Time,LD,ip_src_pair,port_src_pair,ip_dst_pair,port_dst_pair,
                   case
                when (MEASURE_START_DATE - Previous_Time > ', timeInterval, ') then 1                
                when (LD is null and Previous_Time is null)  then 1 
                when (Previous_Time is null)  then 1
                when (LD - MEASURE_START_DATE > ', timeInterval, ') then 0
                when (LD is null) then 0
                else NULL end as Record_Indicator
            from (select DEVICE_ID,SRC_ADDRESS,SRC_PORT,DST_ADDRESS,DST_PORT,PROTOCOL,MEASURE_START_DATE,MEASURE_END_DATE, DST_ETH_ADDRESS, SRC_ETH_ADDRESS, LEAD(MEASURE_START_DATE) OVER
            (PARTITION BY PROTOCOL,ip_src_pair,port_src_pair,ip_dst_pair,port_dst_pair order by MEASURE_START_DATE) AS LD,
                        LAG(MEASURE_START_DATE) OVER
            (PARTITION BY PROTOCOL,ip_src_pair,port_src_pair,ip_dst_pair,port_dst_pair order by MEASURE_START_DATE ) as Previous_Time  
                        ,ip_src_pair,port_src_pair,ip_dst_pair,port_dst_pair,P1_UDP_SERVICE_NAME ,P2_UDP_SERVICE_NAME
                        FROM (SELECT DEVICE_ID, SRC_ADDRESS,SRC_PORT,DST_ADDRESS,DST_PORT,PROTOCOL,MEASURE_START_DATE, MEASURE_END_DATE, DST_ETH_ADDRESS, SRC_ETH_ADDRESS,
            CASE WHEN (SRC_ADDRESS < DST_ADDRESS) THEN SRC_ADDRESS ELSE DST_ADDRESS END as ip_src_pair ,
            CASE WHEN (SRC_ADDRESS < DST_ADDRESS) THEN DST_ADDRESS ELSE SRC_ADDRESS END as ip_dst_pair, 
            CASE WHEN (SRC_ADDRESS < DST_ADDRESS) THEN SRC_PORT ELSE DST_PORT END as port_src_pair ,
            CASE WHEN (SRC_ADDRESS < DST_ADDRESS) THEN DST_PORT ELSE SRC_PORT END as port_dst_pair,
                        P1.UDP_SERVICE_NAME as P1_UDP_SERVICE_NAME ,P2.UDP_SERVICE_NAME as P2_UDP_SERVICE_NAME
            FROM analytics.TELEMETRY_HEADER PARTITION (', Device, ')
                        as TH
                        LEFT JOIN analytics.PORT AS P1 ON
            P1.PORT_NUMBER = TH.SRC_PORT
            LEFT JOIN analytics.PORT AS P2 ON
            P2.PORT_NUMBER = TH.DST_PORT
                        where PROTOCOL = ''UDP'' and LEFT(REPLACE(DST_ETH_ADDRESS, '':'', ''''), 6) = ''01005E'' AND CONV(SUBSTRING_INDEX(DST_ADDRESS, ''.'', 1), 10, 2) LIKE ''1110%''
                        ',IF(assetList IS NOT NULL AND assetList != '''', CONCAT(' AND (SRC_ETH_ADDRESS IN (', assetList, ') OR DST_ETH_ADDRESS IN (', assetList, '))'),''),' AND MEASURE_START_DATE BETWEEN ', fromTime, ' AND ', toTime, ') as b) a
                        ) as data 
            ) final ) multicast  where Record_Indicator = 1'  
                         );



    PREPARE stmt FROM @sql_udp;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;

    PREPARE stmt FROM @sql_ipnip;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;


END ;;
DELIMITER ;
/*!50003 SET sql_mode              = @saved_sql_mode */ ;
/*!50003 SET character_set_client  = @saved_cs_client */ ;
/*!50003 SET character_set_results = @saved_cs_results */ ;
/*!50003 SET collation_connection  = @saved_col_connection */ ;
/*!50003 DROP PROCEDURE IF EXISTS `GetTrafficCountUnicast` */;
/*!50003 SET @saved_cs_client      = @@character_set_client */ ;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;
/*!50003 SET character_set_client  = latin1 */ ;
/*!50003 SET character_set_results = latin1 */ ;
/*!50003 SET collation_connection  = latin1_swedish_ci */ ;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;
/*!50003 SET sql_mode              = '' */ ;
DELIMITER ;;
CREATE DEFINER=`analytics_dev`@`%` PROCEDURE `GetTrafficCountUnicast`(
    IN Device VARCHAR(255), 
    IN fromTime INT, 
    IN toTime INT,
    IN timeInterval INT,
    IN assetList TEXT
)
BEGIN
    SET @sql_arp = CONCAT('select count(*) as count 
            FROM analytics.TELEMETRY_HEADER PARTITION (', Device, ')
                        as TH
                        where PROTOCOL = ''ARP''  AND DST_ETH_ADDRESS != ''ff:ff:ff:ff:ff:ff''
                        and     MEASURE_START_DATE BETWEEN ', fromTime, ' AND ', toTime, IF(assetList IS NOT NULL AND assetList != '''', CONCAT(' AND (SRC_ETH_ADDRESS IN (', assetList, ') OR DST_ETH_ADDRESS IN (', assetList, '))'),''),''  
                         );

                         
    SET @sql_ipnip = CONCAT('select count(*) as count 
            FROM analytics.TELEMETRY_HEADER PARTITION (', Device, ') AS TH
    LEFT JOIN 
        analytics.TELEMETRY PARTITION (', Device, ') AS ip_proto
        ON TH.TELEMETRY_GUID = ip_proto.TELEMETRY_GUID 
        AND ip_proto.MEASURE_TYPE = ''ip_proto''
    LEFT JOIN 
        analytics.TELEMETRY PARTITION (', Device, ') AS eth_type
        ON TH.TELEMETRY_GUID = eth_type.TELEMETRY_GUID 
        AND eth_type.MEASURE_TYPE = ''eth_type''
    LEFT JOIN 
        analytics.ETHER_TYPE et
        ON eth_type.MEASURE_VALUE = et.ETHER_TYPE
    WHERE 
        TH.PROTOCOL = ''IP_NIP''  
        AND CONV(SUBSTRING(TH.DST_ETH_ADDRESS, 1, 2), 16, 10) & 1 = 0
        AND NOT (CONV(SUBSTRING_INDEX(TH.DST_ADDRESS, ''.'', 1), 10, 2) LIKE ''1110%'') 
        AND TH.DST_ADDRESS != ''255.255.255.255''
        AND (et.ETHER_NAME <> ''Unknown'' OR et.ETHER_NAME IS NULL)
        AND TH.MEASURE_START_DATE BETWEEN ', fromTime, ' AND ', toTime, IF(assetList IS NOT NULL AND assetList != '''',CONCAT(' AND (TH.SRC_ETH_ADDRESS IN (', assetList, ') OR TH.DST_ETH_ADDRESS IN (', assetList, '))'),''), '
        
');


    SET @sql_tcpmodbus = CONCAT('select count(*) as count from
                                        (
                    SELECT P2.TCP_SERVICE_NAME as P2_TCP_SERVICE_NAME,
                                            P1.TCP_SERVICE_NAME as P1_TCP_SERVICE_NAME,
                        b.MEASURE_VALUE as flow_indicator, 
                        a.SRC_ADDRESS, a.SRC_PORT, a.DST_ADDRESS, a.DST_PORT ,SRC_ETH_ADDRESS,DST_ETH_ADDRESS,
                                                MEASURE_START_DATE,
                                                MEASURE_END_DATE,
                                                PROTOCOL,
                                                ROW_NUMBER() OVER (PARTITION BY SRC_ADDRESS, SRC_PORT, DST_ADDRESS, DST_PORT,PROTOCOL,b.MEASURE_VALUE ORDER BY MEASURE_START_DATE ASC) as row_num
                    FROM analytics.TELEMETRY_HEADER PARTITION (', Device, ') a 
                    INNER JOIN analytics.TELEMETRY PARTITION (', Device, ') b 
                                        ON a.TELEMETRY_GUID = b.TELEMETRY_GUID AND b.MEASURE_TYPE = ''flowindicator'' ',IF(assetList IS NOT NULL AND assetList != '''', CONCAT(' AND (SRC_ETH_ADDRESS IN (', assetList, ') OR DST_ETH_ADDRESS IN (', assetList, '))'),''),' AND PROTOCOL IN (''TCP'', ''MODBUS'') 
                                        AND MEASURE_START_DATE BETWEEN ', fromTime, ' AND ', toTime, '
                                        LEFT JOIN analytics.PORT AS P1 ON
                    P1.PORT_NUMBER = a.SRC_PORT
                    LEFT JOIN analytics.PORT AS P2 ON
                    P2.PORT_NUMBER = a.DST_PORT
                ) AS d
                where row_num = 1');

    SET @sql_udp = CONCAT('select count(*) as count from (
 select *, CONCAT(CEIL((max(MEASURE_END_DATE) over ( partition by PROTOCOL,ip_src_pair,port_src_pair,ip_dst_pair,port_dst_pair,Flow_Indicator) - Time )), '' s'') as Duration from (   
      SELECT SRC_ADDRESS as Source_IP, 
                                DST_ADDRESS as Destination_IP, 
                Record_Indicator,
                SUM(Record_Indicator) over ( partition by PROTOCOL,ip_src_pair,port_src_pair,ip_dst_pair,port_dst_pair order by MEASURE_START_DATE) as Flow_Indicator,
                COALESCE( (CASE 
                                WHEN PROTOCOL = ''UDP'' THEN COALESCE(P2_UDP_SERVICE_NAME, P1_UDP_SERVICE_NAME, ''UDP'')
                ELSE NULL 
            END), ''Other'') AS Service,
                                CAST(SUBSTRING_INDEX(SRC_PORT, ''.'', 1) AS UNSIGNED) AS Source_Port,
            CAST(SUBSTRING_INDEX(DST_PORT, ''.'', 1) AS UNSIGNED) AS Destination_Port,
                        SRC_ETH_ADDRESS AS Source_MAC,
            DST_ETH_ADDRESS AS Destination_MAC,
            PROTOCOL AS PROTOCOL,
            MEASURE_START_DATE AS Time,
            MEASURE_END_DATE,
                        Previous_Time,
                        LD,ip_src_pair,port_src_pair,ip_dst_pair,port_dst_pair
            FROM 
            (select SRC_ADDRESS, SRC_PORT, DST_ADDRESS, DST_PORT, DST_ETH_ADDRESS,SRC_ETH_ADDRESS, PROTOCOL,P1_UDP_SERVICE_NAME, P2_UDP_SERVICE_NAME, MEASURE_START_DATE,MEASURE_END_DATE,Previous_Time,LD,ip_src_pair,port_src_pair,ip_dst_pair,port_dst_pair,
                   case
                when (MEASURE_START_DATE - Previous_Time > ', timeInterval, ') then 1                
                when (LD is null and Previous_Time is null)  then 1 
                when (Previous_Time is null)  then 1
                when (LD - MEASURE_START_DATE > ', timeInterval, ') then 0
                when (LD is null) then 0
                else NULL end as Record_Indicator
            from (select DEVICE_ID,SRC_ADDRESS,SRC_PORT,DST_ADDRESS,DST_PORT,PROTOCOL,MEASURE_START_DATE,MEASURE_END_DATE, DST_ETH_ADDRESS, SRC_ETH_ADDRESS, LEAD(MEASURE_START_DATE) OVER
            (PARTITION BY PROTOCOL,ip_src_pair,port_src_pair,ip_dst_pair,port_dst_pair order by MEASURE_START_DATE) AS LD,
                        LAG(MEASURE_START_DATE) OVER
            (PARTITION BY PROTOCOL,ip_src_pair,port_src_pair,ip_dst_pair,port_dst_pair order by MEASURE_START_DATE ) as Previous_Time  
                        ,ip_src_pair,port_src_pair,ip_dst_pair,port_dst_pair,P1_UDP_SERVICE_NAME ,P2_UDP_SERVICE_NAME
                        FROM (SELECT DEVICE_ID, SRC_ADDRESS,SRC_PORT,DST_ADDRESS,DST_PORT,PROTOCOL,MEASURE_START_DATE, MEASURE_END_DATE, DST_ETH_ADDRESS, SRC_ETH_ADDRESS,
            CASE WHEN (SRC_ADDRESS < DST_ADDRESS) THEN SRC_ADDRESS ELSE DST_ADDRESS END as ip_src_pair ,
            CASE WHEN (SRC_ADDRESS < DST_ADDRESS) THEN DST_ADDRESS ELSE SRC_ADDRESS END as ip_dst_pair, 
            CASE WHEN (SRC_ADDRESS < DST_ADDRESS) THEN SRC_PORT ELSE DST_PORT END as port_src_pair ,
            CASE WHEN (SRC_ADDRESS < DST_ADDRESS) THEN DST_PORT ELSE SRC_PORT END as port_dst_pair,
                        P1.UDP_SERVICE_NAME as P1_UDP_SERVICE_NAME ,P2.UDP_SERVICE_NAME as P2_UDP_SERVICE_NAME
            FROM analytics.TELEMETRY_HEADER PARTITION (', Device, ')
                        as TH
                        LEFT JOIN analytics.PORT AS P1 ON
            P1.PORT_NUMBER = TH.SRC_PORT
            LEFT JOIN analytics.PORT AS P2 ON
            P2.PORT_NUMBER = TH.DST_PORT
                        where PROTOCOL = ''UDP'' and CONV(SUBSTRING(DST_ETH_ADDRESS, 1, 2), 16, 10) & 1 = 0 AND NOT (CONV(SUBSTRING_INDEX(DST_ADDRESS, ''.'', 1), 10, 2) LIKE ''1110%'') AND DST_ADDRESS != ''255.255.255.255'' ',IF(assetList IS NOT NULL AND assetList != '''', CONCAT(' AND (SRC_ETH_ADDRESS IN (', assetList, ') OR DST_ETH_ADDRESS IN (', assetList, '))'),''),'
                        )as b) a
             WHERE a.MEASURE_START_DATE BETWEEN ', fromTime, ' AND ', toTime, '
                        ) as data 
            ) final ) unicast  where Record_Indicator = 1'  
                         );


    PREPARE stmt FROM @sql_udp;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;


    PREPARE stmt FROM @sql_tcpmodbus;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;


    PREPARE stmt FROM @sql_ipnip;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;
                         

    PREPARE stmt FROM @sql_arp;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;


END ;;
DELIMITER ;
/*!50003 SET sql_mode              = @saved_sql_mode */ ;
/*!50003 SET character_set_client  = @saved_cs_client */ ;
/*!50003 SET character_set_results = @saved_cs_results */ ;
/*!50003 SET collation_connection  = @saved_col_connection */ ;
/*!50003 DROP PROCEDURE IF EXISTS `GetTrafficDetailBroadcast` */;
/*!50003 SET @saved_cs_client      = @@character_set_client */ ;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;
/*!50003 SET character_set_client  = latin1 */ ;
/*!50003 SET character_set_results = latin1 */ ;
/*!50003 SET collation_connection  = latin1_swedish_ci */ ;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;
/*!50003 SET sql_mode              = '' */ ;
DELIMITER ;;
CREATE DEFINER=`analytics_dev`@`%` PROCEDURE `GetTrafficDetailBroadcast`(
    IN Device VARCHAR(255), 
    IN fromTime INT, 
    IN toTime INT,
    IN timeInterval INT,
    IN assetList TEXT,
    IN pageNum INT,
    IN pageSize INT
)
BEGIN

    SET @detailed_statement_arp = CONCAT('SELECT TH.TELEMETRY_GUID AS ID,SRC_ADDRESS as Source_IP, 
                        DST_ADDRESS as Destination_IP,
                        COALESCE( (CASE WHEN TH.PROTOCOL = ''ARP'' THEN ''ARP''  ELSE NULL END), ''Other'') AS Service,
                        CAST(SUBSTRING_INDEX(SRC_PORT, ''.'', 1) AS UNSIGNED) AS Source_Port,
                        CAST(SUBSTRING_INDEX(DST_PORT, ''.'', 1) AS UNSIGNED) AS Destination_Port,
                        SRC_ETH_ADDRESS AS Source_MAC,
            DST_ETH_ADDRESS AS Destination_MAC,
                        PROTOCOL,
                        CONCAT(CEIL(MEASURE_END_DATE - MEASURE_START_DATE), '' s'') as Duration,
                        MEASURE_START_DATE AS Time
            FROM analytics.TELEMETRY_HEADER PARTITION (', Device, ')
                        as TH
                        where PROTOCOL = ''ARP''  AND DST_ETH_ADDRESS = ''ff:ff:ff:ff:ff:ff'' 
                        and     MEASURE_START_DATE BETWEEN ', fromTime, ' AND ', toTime, IF(assetList IS NOT NULL AND assetList != '''', CONCAT(' AND (SRC_ETH_ADDRESS IN (', assetList, ') OR DST_ETH_ADDRESS IN (', assetList, '))'),''),' LIMIT ',pageNum, ' OFFSET ',pageSize,''   
                         );


    SET @detailed_statement_ipnip = CONCAT('
    SELECT TH.TELEMETRY_GUID AS ID,
      TH.SRC_ADDRESS as Source_IP,
      TH.DST_ADDRESS as Destination_IP,
      COALESCE(
        (CASE 
          WHEN TH.PROTOCOL = ''IP_NIP'' 
               AND ip_proto.MEASURE_VALUE = 255 
               THEN COALESCE(et.ETHER_NAME, ''Non IP'')  
          ELSE NULL
        END),
        ''Other''
      ) AS Service,
      CAST(SUBSTRING_INDEX(TH.SRC_PORT, ''.'', 1) AS UNSIGNED) AS Source_Port,
      CAST(SUBSTRING_INDEX(TH.DST_PORT, ''.'', 1) AS UNSIGNED) AS Destination_Port,
      TH.SRC_ETH_ADDRESS AS Source_MAC,
      TH.DST_ETH_ADDRESS AS Destination_MAC,
      TH.PROTOCOL,
      CONCAT(CEIL(TH.MEASURE_END_DATE - TH.MEASURE_START_DATE), '' s'') as Duration,
      TH.MEASURE_START_DATE AS Time
    FROM 
      analytics.TELEMETRY_HEADER PARTITION (', Device, ') as TH
    LEFT JOIN 
      analytics.TELEMETRY PARTITION (', Device, ') as ip_proto
      ON TH.TELEMETRY_GUID = ip_proto.TELEMETRY_GUID 
      AND ip_proto.MEASURE_TYPE = ''ip_proto''
    LEFT JOIN 
      analytics.TELEMETRY PARTITION (', Device, ') as eth_type
      ON TH.TELEMETRY_GUID = eth_type.TELEMETRY_GUID 
      AND eth_type.MEASURE_TYPE = ''eth_type''
    LEFT JOIN 
      analytics.ETHER_TYPE et
      ON eth_type.MEASURE_VALUE = et.ETHER_TYPE
    WHERE 
      TH.PROTOCOL = ''IP_NIP'' 
      AND (
        (TH.DST_ETH_ADDRESS LIKE ''ff:ff:ff:ff:ff:ff'') 
        OR TH.DST_ADDRESS = ''255.255.255.255'' 
        OR (SUBSTRING(LPAD(CONV(SUBSTRING_INDEX(TH.DST_ADDRESS, ''.'', 1),10,2), 8, ''0''), 1, 1) = ''0'' AND SUBSTRING_INDEX(TH.DST_ADDRESS, ''.'', -3) = ''255.255.255'') 
        OR (SUBSTRING(LPAD(CONV(SUBSTRING_INDEX(TH.DST_ADDRESS, ''.'', 1),10,2), 8, ''0''), 1, 2) = ''10'' AND SUBSTRING_INDEX(TH.DST_ADDRESS, ''.'', -2) = ''255.255'' ) 
        OR (SUBSTRING(LPAD(CONV(SUBSTRING_INDEX(TH.DST_ADDRESS, ''.'', 1),10,2), 8, ''0''), 1, 3) = ''110'' AND SUBSTRING_INDEX(TH.DST_ADDRESS, ''.'', -1) = ''255'')
      )
      AND TH.MEASURE_START_DATE BETWEEN ', fromTime, ' AND ', toTime, IF(assetList IS NOT NULL AND assetList != '''', CONCAT(' AND (TH.SRC_ETH_ADDRESS IN (', assetList, ') OR TH.DST_ETH_ADDRESS IN (', assetList, '))'),''),'
      AND (et.ETHER_NAME <> ''Unknown'' OR et.ETHER_NAME IS NULL)
      LIMIT ',pageNum, ' OFFSET ',pageSize,'
');
                         
    

    SET @detailed_statement_udp = CONCAT('select ID,Source_IP,Destination_IP,Service,Source_Port,Destination_Port,Source_MAC,Destination_MAC,PROTOCOL,Duration,Time from (
 select *, CONCAT(CEIL((max(MEASURE_END_DATE) over ( partition by PROTOCOL,ip_src_pair,port_src_pair,ip_dst_pair,port_dst_pair,Flow_Indicator) - Time )), '' s'') as Duration from (   
      SELECT ID,SRC_ADDRESS as Source_IP, 
                                DST_ADDRESS as Destination_IP, 
                Record_Indicator,
                SUM(Record_Indicator) over ( partition by PROTOCOL,ip_src_pair,port_src_pair,ip_dst_pair,port_dst_pair order by MEASURE_START_DATE) as Flow_Indicator,
                COALESCE( (CASE 
                                WHEN PROTOCOL = ''UDP'' THEN COALESCE(P2_UDP_SERVICE_NAME, P1_UDP_SERVICE_NAME, ''UDP'')
                ELSE NULL 
            END), ''Other'') AS Service,
                                CAST(SUBSTRING_INDEX(SRC_PORT, ''.'', 1) AS UNSIGNED) AS Source_Port,
            CAST(SUBSTRING_INDEX(DST_PORT, ''.'', 1) AS UNSIGNED) AS Destination_Port,
                        SRC_ETH_ADDRESS AS Source_MAC,
            DST_ETH_ADDRESS AS Destination_MAC,
            PROTOCOL AS PROTOCOL,
            MEASURE_START_DATE AS Time,
            MEASURE_END_DATE,
                        Previous_Time,
                        LD,ip_src_pair,port_src_pair,ip_dst_pair,port_dst_pair
            FROM 
            (select ID,SRC_ADDRESS, SRC_PORT, DST_ADDRESS, DST_PORT, DST_ETH_ADDRESS,SRC_ETH_ADDRESS, PROTOCOL,P1_UDP_SERVICE_NAME, P2_UDP_SERVICE_NAME, MEASURE_START_DATE,MEASURE_END_DATE,Previous_Time,LD,ip_src_pair,port_src_pair,ip_dst_pair,port_dst_pair,
                   case
                when (MEASURE_START_DATE - Previous_Time > ', timeInterval, ') then 1                
                when (LD is null and Previous_Time is null)  then 1 
                when (Previous_Time is null)  then 1
                when (LD - MEASURE_START_DATE > ', timeInterval, ') then 0
                when (LD is null) then 0
                else NULL end as Record_Indicator
            from (select ID,DEVICE_ID,SRC_ADDRESS,SRC_PORT,DST_ADDRESS,DST_PORT,PROTOCOL,MEASURE_START_DATE,MEASURE_END_DATE, DST_ETH_ADDRESS, SRC_ETH_ADDRESS, LEAD(MEASURE_START_DATE) OVER
            (PARTITION BY PROTOCOL,ip_src_pair,port_src_pair,ip_dst_pair,port_dst_pair order by MEASURE_START_DATE) AS LD,
                        LAG(MEASURE_START_DATE) OVER
            (PARTITION BY PROTOCOL,ip_src_pair,port_src_pair,ip_dst_pair,port_dst_pair order by MEASURE_START_DATE ) as Previous_Time  
                        ,ip_src_pair,port_src_pair,ip_dst_pair,port_dst_pair,P1_UDP_SERVICE_NAME ,P2_UDP_SERVICE_NAME
                        FROM (SELECT TH.TELEMETRY_GUID AS ID,DEVICE_ID, SRC_ADDRESS,SRC_PORT,DST_ADDRESS,DST_PORT,PROTOCOL,MEASURE_START_DATE, MEASURE_END_DATE, DST_ETH_ADDRESS, SRC_ETH_ADDRESS,
            CASE WHEN (SRC_ADDRESS < DST_ADDRESS) THEN SRC_ADDRESS ELSE DST_ADDRESS END as ip_src_pair ,
            CASE WHEN (SRC_ADDRESS < DST_ADDRESS) THEN DST_ADDRESS ELSE SRC_ADDRESS END as ip_dst_pair, 
            CASE WHEN (SRC_ADDRESS < DST_ADDRESS) THEN SRC_PORT ELSE DST_PORT END as port_src_pair ,
            CASE WHEN (SRC_ADDRESS < DST_ADDRESS) THEN DST_PORT ELSE SRC_PORT END as port_dst_pair,
                        P1.UDP_SERVICE_NAME as P1_UDP_SERVICE_NAME ,P2.UDP_SERVICE_NAME as P2_UDP_SERVICE_NAME
            FROM analytics.TELEMETRY_HEADER PARTITION (', Device, ')
                        as TH
                        LEFT JOIN analytics.PORT AS P1 ON
            P1.PORT_NUMBER = TH.SRC_PORT
            LEFT JOIN analytics.PORT AS P2 ON
            P2.PORT_NUMBER = TH.DST_PORT
                        where PROTOCOL = ''UDP'' and ((DST_ETH_ADDRESS LIKE ''ff:ff:ff:ff:ff:ff'') OR
            DST_ADDRESS = ''255.255.255.255'' OR 
              (SUBSTRING(LPAD(CONV(SUBSTRING_INDEX(DST_ADDRESS, ''.'', 1),10,2), 8, ''0''), 1, 1) = ''0'' AND SUBSTRING_INDEX(DST_ADDRESS, ''.'', -3) = ''255.255.255'') OR
              (SUBSTRING(LPAD(CONV(SUBSTRING_INDEX(DST_ADDRESS, ''.'', 1),10,2), 8, ''0''), 1, 2) = ''10'' AND SUBSTRING_INDEX(DST_ADDRESS, ''.'', -2) = ''255.255'' ) OR 
              (SUBSTRING(LPAD(CONV(SUBSTRING_INDEX(DST_ADDRESS, ''.'', 1),10,2), 8, ''0''), 1, 3) = ''110'' AND SUBSTRING_INDEX(DST_ADDRESS, ''.'', -1) = ''255'')
            )', IF(assetList IS NOT NULL AND assetList != '''', CONCAT(' AND (SRC_ETH_ADDRESS IN (', assetList, ') OR DST_ETH_ADDRESS IN (', assetList, '))'),' '),'
                         AND MEASURE_START_DATE BETWEEN ', fromTime, ' AND ', toTime, '
                        )as b) a
                        ) as data 
            ) final ) broadcast  where Record_Indicator = 1 LIMIT ',pageNum, ' OFFSET ',pageSize,''  
                         );



        PREPARE stmt FROM @detailed_statement_udp;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;                     


        PREPARE stmt FROM @detailed_statement_ipnip;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;



        PREPARE stmt FROM @detailed_statement_arp;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;

END ;;
DELIMITER ;
/*!50003 SET sql_mode              = @saved_sql_mode */ ;
/*!50003 SET character_set_client  = @saved_cs_client */ ;
/*!50003 SET character_set_results = @saved_cs_results */ ;
/*!50003 SET collation_connection  = @saved_col_connection */ ;
/*!50003 DROP PROCEDURE IF EXISTS `GetTrafficDetailBroadcast##` */;
/*!50003 SET @saved_cs_client      = @@character_set_client */ ;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;
/*!50003 SET character_set_client  = utf8mb4 */ ;
/*!50003 SET character_set_results = utf8mb4 */ ;
/*!50003 SET collation_connection  = utf8mb4_0900_ai_ci */ ;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;
/*!50003 SET sql_mode              = '' */ ;
DELIMITER ;;
CREATE DEFINER=`analytics_dev`@`%` PROCEDURE `GetTrafficDetailBroadcast##`(
    IN Device VARCHAR(255), 
    IN fromTime INT, 
    IN toTime INT,
    IN timeInterval INT,
    IN assetList TEXT,
    IN pageNum INT
)
BEGIN

    SET @detailed_statement_arp = CONCAT('SELECT TH.TELEMETRY_GUID AS ID,SRC_ADDRESS as Source_IP, 
                        DST_ADDRESS as Destination_IP,
                        COALESCE( (CASE WHEN TH.PROTOCOL = ''ARP'' THEN ''ARP''  ELSE NULL END), ''Other'') AS Service,
                        CAST(SUBSTRING_INDEX(SRC_PORT, ''.'', 1) AS UNSIGNED) AS Source_Port,
                        CAST(SUBSTRING_INDEX(DST_PORT, ''.'', 1) AS UNSIGNED) AS Destination_Port,
                        SRC_ETH_ADDRESS AS Source_MAC,
            DST_ETH_ADDRESS AS Destination_MAC,
                        PROTOCOL,
                        CONCAT(CEIL(MEASURE_END_DATE - MEASURE_START_DATE), '' s'') as Duration,
                        MEASURE_START_DATE AS Time
            FROM analytics.TELEMETRY_HEADER PARTITION (', Device, ')
                        as TH
                        where PROTOCOL = ''ARP''  AND DST_ETH_ADDRESS = ''ff:ff:ff:ff:ff:ff'' 
                        and     MEASURE_START_DATE BETWEEN ', fromTime, ' AND ', toTime, IF(assetList IS NOT NULL AND assetList != '''', CONCAT(' AND (SRC_ETH_ADDRESS IN (', assetList, ') OR DST_ETH_ADDRESS IN (', assetList, '))'),''),' LIMIT ',pageNum, 
                        -- ' OFFSET ',pageSize,
                        ''   
                         );


    SET @detailed_statement_ipnip = CONCAT('
    SELECT TH.TELEMETRY_GUID AS ID,
      TH.SRC_ADDRESS as Source_IP,
      TH.DST_ADDRESS as Destination_IP,
      COALESCE(
        (CASE 
          WHEN TH.PROTOCOL = ''IP_NIP'' 
               AND ip_proto.MEASURE_VALUE = 255 
               THEN COALESCE(et.ETHER_NAME, ''Non IP'')  
          ELSE NULL
        END),
        ''Other''
      ) AS Service,
      CAST(SUBSTRING_INDEX(TH.SRC_PORT, ''.'', 1) AS UNSIGNED) AS Source_Port,
      CAST(SUBSTRING_INDEX(TH.DST_PORT, ''.'', 1) AS UNSIGNED) AS Destination_Port,
      TH.SRC_ETH_ADDRESS AS Source_MAC,
      TH.DST_ETH_ADDRESS AS Destination_MAC,
      TH.PROTOCOL,
      CONCAT(CEIL(TH.MEASURE_END_DATE - TH.MEASURE_START_DATE), '' s'') as Duration,
      TH.MEASURE_START_DATE AS Time
    FROM 
      analytics.TELEMETRY_HEADER PARTITION (', Device, ') as TH
    LEFT JOIN 
      analytics.TELEMETRY PARTITION (', Device, ') as ip_proto
      ON TH.TELEMETRY_GUID = ip_proto.TELEMETRY_GUID 
      AND ip_proto.MEASURE_TYPE = ''ip_proto''
    LEFT JOIN 
      analytics.TELEMETRY PARTITION (', Device, ') as eth_type
      ON TH.TELEMETRY_GUID = eth_type.TELEMETRY_GUID 
      AND eth_type.MEASURE_TYPE = ''eth_type''
    LEFT JOIN 
      analytics.ETHER_TYPE et
      ON eth_type.MEASURE_VALUE = et.ETHER_TYPE
    WHERE 
      TH.PROTOCOL = ''IP_NIP'' 
      AND (
        (TH.DST_ETH_ADDRESS LIKE ''ff:ff:ff:ff:ff:ff'') 
        OR TH.DST_ADDRESS = ''255.255.255.255'' 
        OR (SUBSTRING(LPAD(CONV(SUBSTRING_INDEX(TH.DST_ADDRESS, ''.'', 1),10,2), 8, ''0''), 1, 1) = ''0'' AND SUBSTRING_INDEX(TH.DST_ADDRESS, ''.'', -3) = ''255.255.255'') 
        OR (SUBSTRING(LPAD(CONV(SUBSTRING_INDEX(TH.DST_ADDRESS, ''.'', 1),10,2), 8, ''0''), 1, 2) = ''10'' AND SUBSTRING_INDEX(TH.DST_ADDRESS, ''.'', -2) = ''255.255'' ) 
        OR (SUBSTRING(LPAD(CONV(SUBSTRING_INDEX(TH.DST_ADDRESS, ''.'', 1),10,2), 8, ''0''), 1, 3) = ''110'' AND SUBSTRING_INDEX(TH.DST_ADDRESS, ''.'', -1) = ''255'')
      )
      AND TH.MEASURE_START_DATE BETWEEN ', fromTime, ' AND ', toTime, IF(assetList IS NOT NULL AND assetList != '''', CONCAT(' AND (TH.SRC_ETH_ADDRESS IN (', assetList, ') OR TH.DST_ETH_ADDRESS IN (', assetList, '))'),''),'
      AND (et.ETHER_NAME <> ''Unknown'' OR et.ETHER_NAME IS NULL)
      LIMIT ',pageNum, 
      -- ' OFFSET ',pageSize,
      '');
                         
    

    SET @detailed_statement_udp = CONCAT('select ID,Source_IP,Destination_IP,Service,Source_Port,Destination_Port,Source_MAC,Destination_MAC,PROTOCOL,Duration,Time from (
 select *, CONCAT(CEIL((max(MEASURE_END_DATE) over ( partition by PROTOCOL,ip_src_pair,port_src_pair,ip_dst_pair,port_dst_pair,Flow_Indicator) - Time )), '' s'') as Duration from (   
      SELECT ID,SRC_ADDRESS as Source_IP, 
                                DST_ADDRESS as Destination_IP, 
                Record_Indicator,
                SUM(Record_Indicator) over ( partition by PROTOCOL,ip_src_pair,port_src_pair,ip_dst_pair,port_dst_pair order by MEASURE_START_DATE) as Flow_Indicator,
                COALESCE( (CASE 
                                WHEN PROTOCOL = ''UDP'' THEN COALESCE(P2_UDP_SERVICE_NAME, P1_UDP_SERVICE_NAME, ''UDP'')
                ELSE NULL 
            END), ''Other'') AS Service,
                                CAST(SUBSTRING_INDEX(SRC_PORT, ''.'', 1) AS UNSIGNED) AS Source_Port,
            CAST(SUBSTRING_INDEX(DST_PORT, ''.'', 1) AS UNSIGNED) AS Destination_Port,
                        SRC_ETH_ADDRESS AS Source_MAC,
            DST_ETH_ADDRESS AS Destination_MAC,
            PROTOCOL AS PROTOCOL,
            MEASURE_START_DATE AS Time,
            MEASURE_END_DATE,
                        Previous_Time,
                        LD,ip_src_pair,port_src_pair,ip_dst_pair,port_dst_pair
            FROM 
            (select ID,SRC_ADDRESS, SRC_PORT, DST_ADDRESS, DST_PORT, DST_ETH_ADDRESS,SRC_ETH_ADDRESS, PROTOCOL,P1_UDP_SERVICE_NAME, P2_UDP_SERVICE_NAME, MEASURE_START_DATE,MEASURE_END_DATE,Previous_Time,LD,ip_src_pair,port_src_pair,ip_dst_pair,port_dst_pair,
                   case
                when (MEASURE_START_DATE - Previous_Time > ', timeInterval, ') then 1                
                when (LD is null and Previous_Time is null)  then 1 
                when (Previous_Time is null)  then 1
                when (LD - MEASURE_START_DATE > ', timeInterval, ') then 0
                when (LD is null) then 0
                else NULL end as Record_Indicator
            from (select ID,DEVICE_ID,SRC_ADDRESS,SRC_PORT,DST_ADDRESS,DST_PORT,PROTOCOL,MEASURE_START_DATE,MEASURE_END_DATE, DST_ETH_ADDRESS, SRC_ETH_ADDRESS, LEAD(MEASURE_START_DATE) OVER
            (PARTITION BY PROTOCOL,ip_src_pair,port_src_pair,ip_dst_pair,port_dst_pair order by MEASURE_START_DATE) AS LD,
                        LAG(MEASURE_START_DATE) OVER
            (PARTITION BY PROTOCOL,ip_src_pair,port_src_pair,ip_dst_pair,port_dst_pair order by MEASURE_START_DATE ) as Previous_Time  
                        ,ip_src_pair,port_src_pair,ip_dst_pair,port_dst_pair,P1_UDP_SERVICE_NAME ,P2_UDP_SERVICE_NAME
                        FROM (SELECT TH.TELEMETRY_GUID AS ID,DEVICE_ID, SRC_ADDRESS,SRC_PORT,DST_ADDRESS,DST_PORT,PROTOCOL,MEASURE_START_DATE, MEASURE_END_DATE, DST_ETH_ADDRESS, SRC_ETH_ADDRESS,
            CASE WHEN (SRC_ADDRESS < DST_ADDRESS) THEN SRC_ADDRESS ELSE DST_ADDRESS END as ip_src_pair ,
            CASE WHEN (SRC_ADDRESS < DST_ADDRESS) THEN DST_ADDRESS ELSE SRC_ADDRESS END as ip_dst_pair, 
            CASE WHEN (SRC_ADDRESS < DST_ADDRESS) THEN SRC_PORT ELSE DST_PORT END as port_src_pair ,
            CASE WHEN (SRC_ADDRESS < DST_ADDRESS) THEN DST_PORT ELSE SRC_PORT END as port_dst_pair,
                        P1.UDP_SERVICE_NAME as P1_UDP_SERVICE_NAME ,P2.UDP_SERVICE_NAME as P2_UDP_SERVICE_NAME
            FROM analytics.TELEMETRY_HEADER PARTITION (', Device, ')
                        as TH
                        LEFT JOIN analytics.PORT AS P1 ON
            P1.PORT_NUMBER = TH.SRC_PORT
            LEFT JOIN analytics.PORT AS P2 ON
            P2.PORT_NUMBER = TH.DST_PORT
                        where PROTOCOL = ''UDP'' and ((DST_ETH_ADDRESS LIKE ''ff:ff:ff:ff:ff:ff'') OR
            DST_ADDRESS = ''255.255.255.255'' OR 
              (SUBSTRING(LPAD(CONV(SUBSTRING_INDEX(DST_ADDRESS, ''.'', 1),10,2), 8, ''0''), 1, 1) = ''0'' AND SUBSTRING_INDEX(DST_ADDRESS, ''.'', -3) = ''255.255.255'') OR
              (SUBSTRING(LPAD(CONV(SUBSTRING_INDEX(DST_ADDRESS, ''.'', 1),10,2), 8, ''0''), 1, 2) = ''10'' AND SUBSTRING_INDEX(DST_ADDRESS, ''.'', -2) = ''255.255'' ) OR 
              (SUBSTRING(LPAD(CONV(SUBSTRING_INDEX(DST_ADDRESS, ''.'', 1),10,2), 8, ''0''), 1, 3) = ''110'' AND SUBSTRING_INDEX(DST_ADDRESS, ''.'', -1) = ''255'')
            )', IF(assetList IS NOT NULL AND assetList != '''', CONCAT(' AND (SRC_ETH_ADDRESS IN (', assetList, ') OR DST_ETH_ADDRESS IN (', assetList, '))'),' '),'
                         AND MEASURE_START_DATE BETWEEN ', fromTime, ' AND ', toTime, '
                        )as b) a
                        ) as data 
            ) final ) broadcast  where Record_Indicator = 1 LIMIT ',pageNum, 
            -- ' OFFSET ',pageSize,
            ''  
                         );



        PREPARE stmt FROM @detailed_statement_udp;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;                     


        PREPARE stmt FROM @detailed_statement_ipnip;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;



        PREPARE stmt FROM @detailed_statement_arp;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;

END ;;
DELIMITER ;
/*!50003 SET sql_mode              = @saved_sql_mode */ ;
/*!50003 SET character_set_client  = @saved_cs_client */ ;
/*!50003 SET character_set_results = @saved_cs_results */ ;
/*!50003 SET collation_connection  = @saved_col_connection */ ;
/*!50003 DROP PROCEDURE IF EXISTS `GetTrafficDetailMulticast` */;
/*!50003 SET @saved_cs_client      = @@character_set_client */ ;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;
/*!50003 SET character_set_client  = latin1 */ ;
/*!50003 SET character_set_results = latin1 */ ;
/*!50003 SET collation_connection  = latin1_swedish_ci */ ;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;
/*!50003 SET sql_mode              = '' */ ;
DELIMITER ;;
CREATE DEFINER=`analytics_dev`@`%` PROCEDURE `GetTrafficDetailMulticast`(
    IN Device VARCHAR(255), 
    IN fromTime INT, 
    IN toTime INT,
        IN timeInterval INT,
    IN assetList TEXT,
    IN pageNum INT,
    IN pageSize INT
)
BEGIN

    SET @detailed_statement_ipnip = CONCAT('
    SELECT TH.TELEMETRY_GUID AS ID,
        TH.SRC_ADDRESS as Source_IP,
        TH.DST_ADDRESS as Destination_IP,
        COALESCE(
            (CASE 
                WHEN TH.PROTOCOL = ''IP_NIP'' 
                     AND ip_proto.MEASURE_VALUE = 255 
                     THEN COALESCE(et.ETHER_NAME, ''Non IP'')  
                ELSE NULL
            END), 
            ''Other''
        ) AS Service,
        CAST(SUBSTRING_INDEX(TH.SRC_PORT, ''.'', 1) AS UNSIGNED) AS Source_Port,
        CAST(SUBSTRING_INDEX(TH.DST_PORT, ''.'', 1) AS UNSIGNED) AS Destination_Port,
        TH.SRC_ETH_ADDRESS AS Source_MAC,
        TH.DST_ETH_ADDRESS AS Destination_MAC,
        TH.PROTOCOL,
        CONCAT(CEIL(TH.MEASURE_END_DATE - TH.MEASURE_START_DATE), '' s'') as Duration,
        TH.MEASURE_START_DATE AS Time
    FROM 
        analytics.TELEMETRY_HEADER PARTITION (', Device, ') AS TH
    LEFT JOIN 
        analytics.TELEMETRY PARTITION (', Device, ') AS ip_proto
        ON TH.TELEMETRY_GUID = ip_proto.TELEMETRY_GUID 
        AND ip_proto.MEASURE_TYPE = ''ip_proto''
    LEFT JOIN 
        analytics.TELEMETRY PARTITION (', Device, ') AS eth_type
        ON TH.TELEMETRY_GUID = eth_type.TELEMETRY_GUID 
        AND eth_type.MEASURE_TYPE = ''eth_type''
    LEFT JOIN 
        analytics.ETHER_TYPE et
        ON eth_type.MEASURE_VALUE = et.ETHER_TYPE
    WHERE 
        TH.PROTOCOL = ''IP_NIP''  
        AND LEFT(REPLACE(TH.DST_ETH_ADDRESS, '':'', ''''), 6) = ''01005E'' 
        AND CONV(SUBSTRING_INDEX(TH.DST_ADDRESS, ''.'', 1), 10, 2) LIKE ''1110%''
        AND TH.MEASURE_START_DATE BETWEEN ', fromTime, ' AND ', toTime,IF(assetList IS NOT NULL AND assetList != '''',CONCAT(' AND (TH.SRC_ETH_ADDRESS IN (', assetList, ') OR TH.DST_ETH_ADDRESS IN (', assetList, '))'),''),'
        AND (et.ETHER_NAME <> ''Unknown'' OR et.ETHER_NAME IS NULL)
        LIMIT ',pageNum,' OFFSET ',pageSize,'       
');


    SET @detailed_statement_udp = CONCAT('select ID,Source_IP,Destination_IP,Service,Source_Port,Destination_Port,Source_MAC,Destination_MAC,PROTOCOL,Duration,Time from (
 select *, CONCAT(CEIL((max(MEASURE_END_DATE) over ( partition by PROTOCOL,ip_src_pair,port_src_pair,ip_dst_pair,port_dst_pair,Flow_Indicator) - Time )), '' s'') as Duration from (   
      SELECT ID,SRC_ADDRESS as Source_IP, 
                                DST_ADDRESS as Destination_IP, 
                Record_Indicator,
                SUM(Record_Indicator) over ( partition by PROTOCOL,ip_src_pair,port_src_pair,ip_dst_pair,port_dst_pair order by MEASURE_START_DATE) as Flow_Indicator,
                COALESCE( (CASE 
                                WHEN PROTOCOL = ''UDP'' THEN COALESCE(P2_UDP_SERVICE_NAME, P1_UDP_SERVICE_NAME, ''UDP'')
                ELSE NULL 
            END), ''Other'') AS Service,
                                CAST(SUBSTRING_INDEX(SRC_PORT, ''.'', 1) AS UNSIGNED) AS Source_Port,
            CAST(SUBSTRING_INDEX(DST_PORT, ''.'', 1) AS UNSIGNED) AS Destination_Port,
                        SRC_ETH_ADDRESS AS Source_MAC,
            DST_ETH_ADDRESS AS Destination_MAC,
            PROTOCOL AS PROTOCOL,
            MEASURE_START_DATE AS Time,
            MEASURE_END_DATE,
                        Previous_Time,
                        LD,ip_src_pair,port_src_pair,ip_dst_pair,port_dst_pair
            FROM 
            (select ID,SRC_ADDRESS, SRC_PORT, DST_ADDRESS, DST_PORT, DST_ETH_ADDRESS,SRC_ETH_ADDRESS, PROTOCOL,P1_UDP_SERVICE_NAME, P2_UDP_SERVICE_NAME, MEASURE_START_DATE,MEASURE_END_DATE,Previous_Time,LD,ip_src_pair,port_src_pair,ip_dst_pair,port_dst_pair,
                   case
                when (MEASURE_START_DATE - Previous_Time > ', timeInterval, ') then 1                
                when (LD is null and Previous_Time is null)  then 1 
                when (Previous_Time is null)  then 1
                when (LD - MEASURE_START_DATE > ', timeInterval, ') then 0
                when (LD is null) then 0
                else NULL end as Record_Indicator
            from (select ID,DEVICE_ID,SRC_ADDRESS,SRC_PORT,DST_ADDRESS,DST_PORT,PROTOCOL,MEASURE_START_DATE,MEASURE_END_DATE, DST_ETH_ADDRESS, SRC_ETH_ADDRESS, LEAD(MEASURE_START_DATE) OVER
            (PARTITION BY PROTOCOL,ip_src_pair,port_src_pair,ip_dst_pair,port_dst_pair order by MEASURE_START_DATE) AS LD,
                        LAG(MEASURE_START_DATE) OVER
            (PARTITION BY PROTOCOL,ip_src_pair,port_src_pair,ip_dst_pair,port_dst_pair order by MEASURE_START_DATE ) as Previous_Time  
                        ,ip_src_pair,port_src_pair,ip_dst_pair,port_dst_pair,P1_UDP_SERVICE_NAME ,P2_UDP_SERVICE_NAME
                        FROM (SELECT TH.TELEMETRY_GUID AS ID, DEVICE_ID, SRC_ADDRESS,SRC_PORT,DST_ADDRESS,DST_PORT,PROTOCOL,MEASURE_START_DATE, MEASURE_END_DATE, DST_ETH_ADDRESS, SRC_ETH_ADDRESS,
            CASE WHEN (SRC_ADDRESS < DST_ADDRESS) THEN SRC_ADDRESS ELSE DST_ADDRESS END as ip_src_pair ,
            CASE WHEN (SRC_ADDRESS < DST_ADDRESS) THEN DST_ADDRESS ELSE SRC_ADDRESS END as ip_dst_pair, 
            CASE WHEN (SRC_ADDRESS < DST_ADDRESS) THEN SRC_PORT ELSE DST_PORT END as port_src_pair ,
            CASE WHEN (SRC_ADDRESS < DST_ADDRESS) THEN DST_PORT ELSE SRC_PORT END as port_dst_pair,
                        P1.UDP_SERVICE_NAME as P1_UDP_SERVICE_NAME ,P2.UDP_SERVICE_NAME as P2_UDP_SERVICE_NAME
            FROM analytics.TELEMETRY_HEADER PARTITION (', Device, ')
                        as TH
                        LEFT JOIN analytics.PORT AS P1 ON
            P1.PORT_NUMBER = TH.SRC_PORT
            LEFT JOIN analytics.PORT AS P2 ON
            P2.PORT_NUMBER = TH.DST_PORT
                        where PROTOCOL = ''UDP'' and LEFT(REPLACE(DST_ETH_ADDRESS, '':'', ''''), 6) = ''01005E'' AND CONV(SUBSTRING_INDEX(DST_ADDRESS, ''.'', 1), 10, 2) LIKE ''1110%''
                        ',IF(assetList IS NOT NULL AND assetList != '''', CONCAT(' AND (SRC_ETH_ADDRESS IN (', assetList, ') OR DST_ETH_ADDRESS IN (', assetList, '))'),''),' AND MEASURE_START_DATE BETWEEN ', fromTime, ' AND ', toTime, ') as b) a
                        ) as data 
            ) final ) multicast  where Record_Indicator = 1 LIMIT ',pageNum, ' OFFSET ',pageSize,'' 
                         );



        PREPARE stmt FROM @detailed_statement_udp;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;



        PREPARE stmt FROM @detailed_statement_ipnip;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;

END ;;
DELIMITER ;
/*!50003 SET sql_mode              = @saved_sql_mode */ ;
/*!50003 SET character_set_client  = @saved_cs_client */ ;
/*!50003 SET character_set_results = @saved_cs_results */ ;
/*!50003 SET collation_connection  = @saved_col_connection */ ;
/*!50003 DROP PROCEDURE IF EXISTS `GetTrafficDetailMulticast##` */;
/*!50003 SET @saved_cs_client      = @@character_set_client */ ;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;
/*!50003 SET character_set_client  = utf8mb4 */ ;
/*!50003 SET character_set_results = utf8mb4 */ ;
/*!50003 SET collation_connection  = utf8mb4_0900_ai_ci */ ;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;
/*!50003 SET sql_mode              = '' */ ;
DELIMITER ;;
CREATE DEFINER=`analytics_dev`@`%` PROCEDURE `GetTrafficDetailMulticast##`(
    IN Device VARCHAR(255), 
    IN fromTime INT, 
    IN toTime INT,
        IN timeInterval INT,
    IN assetList TEXT,
    IN pageNum INT
)
BEGIN

    SET @detailed_statement_ipnip = CONCAT('
    SELECT TH.TELEMETRY_GUID AS ID,
        TH.SRC_ADDRESS as Source_IP,
        TH.DST_ADDRESS as Destination_IP,
        COALESCE(
            (CASE 
                WHEN TH.PROTOCOL = ''IP_NIP'' 
                     AND ip_proto.MEASURE_VALUE = 255 
                     THEN COALESCE(et.ETHER_NAME, ''Non IP'')  
                ELSE NULL
            END), 
            ''Other''
        ) AS Service,
        CAST(SUBSTRING_INDEX(TH.SRC_PORT, ''.'', 1) AS UNSIGNED) AS Source_Port,
        CAST(SUBSTRING_INDEX(TH.DST_PORT, ''.'', 1) AS UNSIGNED) AS Destination_Port,
        TH.SRC_ETH_ADDRESS AS Source_MAC,
        TH.DST_ETH_ADDRESS AS Destination_MAC,
        TH.PROTOCOL,
        CONCAT(CEIL(TH.MEASURE_END_DATE - TH.MEASURE_START_DATE), '' s'') as Duration,
        TH.MEASURE_START_DATE AS Time
    FROM 
        analytics.TELEMETRY_HEADER PARTITION (', Device, ') AS TH
    LEFT JOIN 
        analytics.TELEMETRY PARTITION (', Device, ') AS ip_proto
        ON TH.TELEMETRY_GUID = ip_proto.TELEMETRY_GUID 
        AND ip_proto.MEASURE_TYPE = ''ip_proto''
    LEFT JOIN 
        analytics.TELEMETRY PARTITION (', Device, ') AS eth_type
        ON TH.TELEMETRY_GUID = eth_type.TELEMETRY_GUID 
        AND eth_type.MEASURE_TYPE = ''eth_type''
    LEFT JOIN 
        analytics.ETHER_TYPE et
        ON eth_type.MEASURE_VALUE = et.ETHER_TYPE
    WHERE 
        TH.PROTOCOL = ''IP_NIP''  
        AND LEFT(REPLACE(TH.DST_ETH_ADDRESS, '':'', ''''), 6) = ''01005E'' 
        AND CONV(SUBSTRING_INDEX(TH.DST_ADDRESS, ''.'', 1), 10, 2) LIKE ''1110%''
        AND TH.MEASURE_START_DATE BETWEEN ', fromTime, ' AND ', toTime,IF(assetList IS NOT NULL AND assetList != '''',CONCAT(' AND (TH.SRC_ETH_ADDRESS IN (', assetList, ') OR TH.DST_ETH_ADDRESS IN (', assetList, '))'),''),'
        AND (et.ETHER_NAME <> ''Unknown'' OR et.ETHER_NAME IS NULL)
        LIMIT ',pageNum,
        -- ' OFFSET ',pageSize,
        '');


    SET @detailed_statement_udp = CONCAT('select ID,Source_IP,Destination_IP,Service,Source_Port,Destination_Port,Source_MAC,Destination_MAC,PROTOCOL,Duration,Time from (
 select *, CONCAT(CEIL((max(MEASURE_END_DATE) over ( partition by PROTOCOL,ip_src_pair,port_src_pair,ip_dst_pair,port_dst_pair,Flow_Indicator) - Time )), '' s'') as Duration from (   
      SELECT ID,SRC_ADDRESS as Source_IP, 
                                DST_ADDRESS as Destination_IP, 
                Record_Indicator,
                SUM(Record_Indicator) over ( partition by PROTOCOL,ip_src_pair,port_src_pair,ip_dst_pair,port_dst_pair order by MEASURE_START_DATE) as Flow_Indicator,
                COALESCE( (CASE 
                                WHEN PROTOCOL = ''UDP'' THEN COALESCE(P2_UDP_SERVICE_NAME, P1_UDP_SERVICE_NAME, ''UDP'')
                ELSE NULL 
            END), ''Other'') AS Service,
                                CAST(SUBSTRING_INDEX(SRC_PORT, ''.'', 1) AS UNSIGNED) AS Source_Port,
            CAST(SUBSTRING_INDEX(DST_PORT, ''.'', 1) AS UNSIGNED) AS Destination_Port,
                        SRC_ETH_ADDRESS AS Source_MAC,
            DST_ETH_ADDRESS AS Destination_MAC,
            PROTOCOL AS PROTOCOL,
            MEASURE_START_DATE AS Time,
            MEASURE_END_DATE,
                        Previous_Time,
                        LD,ip_src_pair,port_src_pair,ip_dst_pair,port_dst_pair
            FROM 
            (select ID,SRC_ADDRESS, SRC_PORT, DST_ADDRESS, DST_PORT, DST_ETH_ADDRESS,SRC_ETH_ADDRESS, PROTOCOL,P1_UDP_SERVICE_NAME, P2_UDP_SERVICE_NAME, MEASURE_START_DATE,MEASURE_END_DATE,Previous_Time,LD,ip_src_pair,port_src_pair,ip_dst_pair,port_dst_pair,
                   case
                when (MEASURE_START_DATE - Previous_Time > ', timeInterval, ') then 1                
                when (LD is null and Previous_Time is null)  then 1 
                when (Previous_Time is null)  then 1
                when (LD - MEASURE_START_DATE > ', timeInterval, ') then 0
                when (LD is null) then 0
                else NULL end as Record_Indicator
            from (select ID,DEVICE_ID,SRC_ADDRESS,SRC_PORT,DST_ADDRESS,DST_PORT,PROTOCOL,MEASURE_START_DATE,MEASURE_END_DATE, DST_ETH_ADDRESS, SRC_ETH_ADDRESS, LEAD(MEASURE_START_DATE) OVER
            (PARTITION BY PROTOCOL,ip_src_pair,port_src_pair,ip_dst_pair,port_dst_pair order by MEASURE_START_DATE) AS LD,
                        LAG(MEASURE_START_DATE) OVER
            (PARTITION BY PROTOCOL,ip_src_pair,port_src_pair,ip_dst_pair,port_dst_pair order by MEASURE_START_DATE ) as Previous_Time  
                        ,ip_src_pair,port_src_pair,ip_dst_pair,port_dst_pair,P1_UDP_SERVICE_NAME ,P2_UDP_SERVICE_NAME
                        FROM (SELECT TH.TELEMETRY_GUID AS ID, DEVICE_ID, SRC_ADDRESS,SRC_PORT,DST_ADDRESS,DST_PORT,PROTOCOL,MEASURE_START_DATE, MEASURE_END_DATE, DST_ETH_ADDRESS, SRC_ETH_ADDRESS,
            CASE WHEN (SRC_ADDRESS < DST_ADDRESS) THEN SRC_ADDRESS ELSE DST_ADDRESS END as ip_src_pair ,
            CASE WHEN (SRC_ADDRESS < DST_ADDRESS) THEN DST_ADDRESS ELSE SRC_ADDRESS END as ip_dst_pair, 
            CASE WHEN (SRC_ADDRESS < DST_ADDRESS) THEN SRC_PORT ELSE DST_PORT END as port_src_pair ,
            CASE WHEN (SRC_ADDRESS < DST_ADDRESS) THEN DST_PORT ELSE SRC_PORT END as port_dst_pair,
                        P1.UDP_SERVICE_NAME as P1_UDP_SERVICE_NAME ,P2.UDP_SERVICE_NAME as P2_UDP_SERVICE_NAME
            FROM analytics.TELEMETRY_HEADER PARTITION (', Device, ')
                        as TH
                        LEFT JOIN analytics.PORT AS P1 ON
            P1.PORT_NUMBER = TH.SRC_PORT
            LEFT JOIN analytics.PORT AS P2 ON
            P2.PORT_NUMBER = TH.DST_PORT
                        where PROTOCOL = ''UDP'' and LEFT(REPLACE(DST_ETH_ADDRESS, '':'', ''''), 6) = ''01005E'' AND CONV(SUBSTRING_INDEX(DST_ADDRESS, ''.'', 1), 10, 2) LIKE ''1110%''
                        ',IF(assetList IS NOT NULL AND assetList != '''', CONCAT(' AND (SRC_ETH_ADDRESS IN (', assetList, ') OR DST_ETH_ADDRESS IN (', assetList, '))'),''),' AND MEASURE_START_DATE BETWEEN ', fromTime, ' AND ', toTime, ') as b) a
                        ) as data 
            ) final ) multicast  where Record_Indicator = 1 LIMIT ',pageNum, 
            -- ' OFFSET ',pageSize,
            '' 
                         );



        PREPARE stmt FROM @detailed_statement_udp;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;



        PREPARE stmt FROM @detailed_statement_ipnip;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;

END ;;
DELIMITER ;
/*!50003 SET sql_mode              = @saved_sql_mode */ ;
/*!50003 SET character_set_client  = @saved_cs_client */ ;
/*!50003 SET character_set_results = @saved_cs_results */ ;
/*!50003 SET collation_connection  = @saved_col_connection */ ;
/*!50003 DROP PROCEDURE IF EXISTS `GetTrafficDetails` */;
/*!50003 SET @saved_cs_client      = @@character_set_client */ ;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;
/*!50003 SET character_set_client  = latin1 */ ;
/*!50003 SET character_set_results = latin1 */ ;
/*!50003 SET collation_connection  = latin1_swedish_ci */ ;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;
/*!50003 SET sql_mode              = '' */ ;
DELIMITER ;;
CREATE DEFINER=`analytics_dev`@`%` PROCEDURE `GetTrafficDetails`(
    IN Device VARCHAR(255),
    IN ID VARCHAR(255)
)
BEGIN
    SET @sql = CONCAT('
        SELECT 
            MAX(TH.SRC_ETH_ADDRESS) AS Source_MAC,
            MAX(TH.DST_ETH_ADDRESS) AS Destination_MAC,
            MAX(TH.SRC_ADDRESS) AS Source_IP,
            MAX(TH.DST_ADDRESS) AS Destination_IP,
            COALESCE(MAX(CASE 
                WHEN TH.PROTOCOL = ''TCP'' THEN COALESCE(P2.TCP_SERVICE_NAME, P1.TCP_SERVICE_NAME, ''TCP'')
                WHEN TH.PROTOCOL = ''UDP'' THEN COALESCE(P2.UDP_SERVICE_NAME, P1.UDP_SERVICE_NAME, ''UDP'')
                WHEN TH.PROTOCOL = ''MODBUS'' THEN ''MODBUS''
                WHEN TH.PROTOCOL = ''ICMP'' THEN ''ICMP''
                WHEN TH.PROTOCOL = ''ARP'' THEN ''ARP''
                WHEN TH.PROTOCOL = ''IP_NIP'' AND T.MEASURE_TYPE = ''ip_proto'' AND T.MEASURE_VALUE = 255 THEN ''Non IP'' 
                ELSE NULL 
            END), ''Other'') AS ''Network Service'',
            CAST(SUBSTRING_INDEX(TH.SRC_PORT, ''.'', 1) AS UNSIGNED) AS Source_Port,
            CAST(SUBSTRING_INDEX(TH.DST_PORT, ''.'', 1) AS UNSIGNED) AS Destination_Port,
            MAX(TH.MEASURE_START_DATE) AS Time,
            MAX(TH.PROTOCOL) AS Protocol,
            MAX(CASE 
                WHEN T.MEASURE_TYPE = ''count_of_frames_sent'' THEN T.MEASURE_VALUE
                WHEN T.MEASURE_TYPE = ''num_of_frames_withinwindow_per_src_dst'' THEN T.MEASURE_VALUE
                ELSE 0 
            END) AS Packets_Sent,
            MAX(TH.MEASURE_END_DATE - TH.MEASURE_START_DATE) AS Duration,
            MAX(CASE 
                WHEN T.MEASURE_TYPE = ''count_of_frames_sent'' THEN T.MEASURE_VALUE
                WHEN T.MEASURE_TYPE = ''num_of_frames_withinwindow_per_src_dst'' THEN T.MEASURE_VALUE
                ELSE 0 
            END) / MAX(TH.MEASURE_END_DATE - TH.MEASURE_START_DATE) AS ''Packets/s''
        FROM
            analytics.TELEMETRY_HEADER PARTITION (', Device, ') AS TH
        INNER JOIN analytics.TELEMETRY PARTITION (', Device, ') AS T ON TH.TELEMETRY_GUID = T.TELEMETRY_GUID
        LEFT JOIN analytics.PORT AS P1 ON P1.PORT_NUMBER = TH.SRC_PORT
        LEFT JOIN analytics.PORT AS P2 ON P2.PORT_NUMBER = TH.DST_PORT
        WHERE TH.TELEMETRY_GUID = ''', ID, '''
        GROUP BY TH.TELEMETRY_GUID;');

    PREPARE stmt FROM @sql;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;
END ;;
DELIMITER ;
/*!50003 SET sql_mode              = @saved_sql_mode */ ;
/*!50003 SET character_set_client  = @saved_cs_client */ ;
/*!50003 SET character_set_results = @saved_cs_results */ ;
/*!50003 SET collation_connection  = @saved_col_connection */ ;
/*!50003 DROP PROCEDURE IF EXISTS `GetTrafficDetailUnicast` */;
/*!50003 SET @saved_cs_client      = @@character_set_client */ ;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;
/*!50003 SET character_set_client  = latin1 */ ;
/*!50003 SET character_set_results = latin1 */ ;
/*!50003 SET collation_connection  = latin1_swedish_ci */ ;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;
/*!50003 SET sql_mode              = '' */ ;
DELIMITER ;;
CREATE DEFINER=`analytics_dev`@`%` PROCEDURE `GetTrafficDetailUnicast`(
    IN Device VARCHAR(255), 
    IN fromTime INT, 
    IN toTime INT,
    IN timeInterval INT,
    IN assetList TEXT,
    IN pageNum INT,
    IN pageSize INT
)
BEGIN
    

    SET @detailed_statement_arp = CONCAT('SELECT TH.TELEMETRY_GUID AS ID,
    SRC_ADDRESS as Source_IP, 
                        DST_ADDRESS as Destination_IP,
                        COALESCE( (CASE WHEN TH.PROTOCOL = ''ARP'' THEN ''ARP''  ELSE NULL END), ''Other'') AS Service,
                        CAST(SUBSTRING_INDEX(SRC_PORT, ''.'', 1) AS UNSIGNED) AS Source_Port,
                        CAST(SUBSTRING_INDEX(DST_PORT, ''.'', 1) AS UNSIGNED) AS Destination_Port,
                        SRC_ETH_ADDRESS AS Source_MAC,
            DST_ETH_ADDRESS AS Destination_MAC,
                        PROTOCOL,
                        CONCAT(CEIL(MEASURE_END_DATE - MEASURE_START_DATE), '' s'') as Duration,
                        MEASURE_START_DATE AS Time
            FROM analytics.TELEMETRY_HEADER PARTITION (', Device, ')
                        as TH
                        where PROTOCOL = ''ARP''  AND DST_ETH_ADDRESS != ''ff:ff:ff:ff:ff:ff'' 
                        and     MEASURE_START_DATE BETWEEN ', fromTime, ' AND ', toTime, IF(assetList IS NOT NULL AND assetList != '''', CONCAT(' AND (SRC_ETH_ADDRESS IN (', assetList, ') OR DST_ETH_ADDRESS IN (', assetList, '))'),''), ' LIMIT ',pageNum, ' OFFSET ',pageSize,''  
                         );
                         

    SET @detailed_statement_ipnip = CONCAT('
    SELECT TH.TELEMETRY_GUID AS ID,
        TH.SRC_ADDRESS as Source_IP,
        TH.DST_ADDRESS as Destination_IP,
        COALESCE(
            (CASE 
                WHEN TH.PROTOCOL = ''IP_NIP'' 
                     AND ip_proto.MEASURE_VALUE = 255 
                     THEN COALESCE(et.ETHER_NAME, ''Non IP'')   
                ELSE NULL
            END), 
            ''Other''
        ) AS Service,
        CAST(SUBSTRING_INDEX(TH.SRC_PORT, ''.'', 1) AS UNSIGNED) AS Source_Port,
        CAST(SUBSTRING_INDEX(TH.DST_PORT, ''.'', 1) AS UNSIGNED) AS Destination_Port,
        TH.SRC_ETH_ADDRESS AS Source_MAC,
        TH.DST_ETH_ADDRESS AS Destination_MAC,
        TH.PROTOCOL,
        CONCAT(CEIL(TH.MEASURE_END_DATE - TH.MEASURE_START_DATE), '' s'') as Duration,
        TH.MEASURE_START_DATE AS Time
    FROM 
        analytics.TELEMETRY_HEADER PARTITION (', Device, ') AS TH
    LEFT JOIN 
        analytics.TELEMETRY PARTITION (', Device, ') AS ip_proto
        ON TH.TELEMETRY_GUID = ip_proto.TELEMETRY_GUID 
        AND ip_proto.MEASURE_TYPE = ''ip_proto''
    LEFT JOIN 
        analytics.TELEMETRY PARTITION (', Device, ') AS eth_type
        ON TH.TELEMETRY_GUID = eth_type.TELEMETRY_GUID 
        AND eth_type.MEASURE_TYPE = ''eth_type''
    LEFT JOIN 
        analytics.ETHER_TYPE et
        ON eth_type.MEASURE_VALUE = et.ETHER_TYPE
    WHERE 
        TH.PROTOCOL = ''IP_NIP''  
        AND CONV(SUBSTRING(TH.DST_ETH_ADDRESS, 1, 2), 16, 10) & 1 = 0
        AND NOT (CONV(SUBSTRING_INDEX(TH.DST_ADDRESS, ''.'', 1), 10, 2) LIKE ''1110%'') 
        AND TH.DST_ADDRESS != ''255.255.255.255''
        AND (et.ETHER_NAME <> ''Unknown'' OR et.ETHER_NAME IS NULL)
        AND TH.MEASURE_START_DATE BETWEEN ', fromTime, ' AND ', toTime, IF(assetList IS NOT NULL AND assetList != '''',CONCAT(' AND (TH.SRC_ETH_ADDRESS IN (', assetList, ') OR TH.DST_ETH_ADDRESS IN (', assetList, '))'),''), '
        LIMIT ',pageNum, ' OFFSET ',pageSize,'
');

    
    SET @detailed_statement_tcpmodbus = CONCAT('SELECT  d.TELEMETRY_GUID AS ID,
    SRC_ADDRESS as Source_IP, 
                                DST_ADDRESS as Destination_IP, 
                                COALESCE( (CASE 
                WHEN PROTOCOL = ''TCP'' THEN COALESCE(P2_TCP_SERVICE_NAME, P1_TCP_SERVICE_NAME, ''TCP'')
                WHEN PROTOCOL = ''MODBUS'' THEN ''MODBUS''
                ELSE NULL 
            END), ''Other'') AS Service,
                                CAST(SUBSTRING_INDEX(SRC_PORT, ''.'', 1) AS UNSIGNED) AS Source_Port,
            CAST(SUBSTRING_INDEX(DST_PORT, ''.'', 1) AS UNSIGNED) AS Destination_Port,
                        SRC_ETH_ADDRESS AS Source_MAC,
            DST_ETH_ADDRESS AS Destination_MAC,
            PROTOCOL AS PROTOCOL,
            CONCAT(CEIL((MEASURE_END_DATE - MEASURE_START_DATE)), '' s'') AS Duration,
            MEASURE_START_DATE AS Time
                FROM (
                    SELECT a.TELEMETRY_GUID,
                    P2.TCP_SERVICE_NAME as P2_TCP_SERVICE_NAME,
                                            P1.TCP_SERVICE_NAME as P1_TCP_SERVICE_NAME,
                        b.MEASURE_VALUE as flow_indicator, 
                        a.SRC_ADDRESS, a.SRC_PORT, a.DST_ADDRESS, a.DST_PORT ,SRC_ETH_ADDRESS,DST_ETH_ADDRESS,
                                                MEASURE_START_DATE,
                                                MEASURE_END_DATE,
                                                PROTOCOL,
                                                ROW_NUMBER() OVER (PARTITION BY SRC_ADDRESS, SRC_PORT, DST_ADDRESS, DST_PORT,PROTOCOL,b.MEASURE_VALUE ORDER BY MEASURE_START_DATE ASC) as row_num
                    FROM analytics.TELEMETRY_HEADER PARTITION (', Device, ') a 
                    INNER JOIN analytics.TELEMETRY PARTITION (', Device, ') b 
                                        ON a.TELEMETRY_GUID = b.TELEMETRY_GUID AND b.MEASURE_TYPE = ''flowindicator'' ',IF(assetList IS NOT NULL AND assetList != '''', CONCAT(' AND (SRC_ETH_ADDRESS IN (', assetList, ') OR DST_ETH_ADDRESS IN (', assetList, '))'),''),' and PROTOCOL IN (''TCP'', ''MODBUS'') 
                                        AND MEASURE_START_DATE BETWEEN ', fromTime, ' AND ', toTime, '
                                        LEFT JOIN analytics.PORT AS P1 ON
                    P1.PORT_NUMBER = a.SRC_PORT
                    LEFT JOIN analytics.PORT AS P2 ON
                    P2.PORT_NUMBER = a.DST_PORT
                ) AS d
                where row_num = 1 LIMIT ',pageNum, ' OFFSET ',pageSize,''  );

    
    SET @detailed_statement_udp = CONCAT('select ID,Source_IP,Destination_IP,Service,Source_Port,Destination_Port,Source_MAC,Destination_MAC,PROTOCOL,Duration,Time from (
 select *, CONCAT(CEIL((max(MEASURE_END_DATE) over ( partition by PROTOCOL,ip_src_pair,port_src_pair,ip_dst_pair,port_dst_pair,Flow_Indicator) - Time )), '' s'') as Duration from (   
      SELECT ID,SRC_ADDRESS as Source_IP, 
                                DST_ADDRESS as Destination_IP, 
                Record_Indicator,
                SUM(Record_Indicator) over ( partition by PROTOCOL,ip_src_pair,port_src_pair,ip_dst_pair,port_dst_pair order by MEASURE_START_DATE) as Flow_Indicator,
                COALESCE( (CASE 
                                WHEN PROTOCOL = ''UDP'' THEN COALESCE(P2_UDP_SERVICE_NAME, P1_UDP_SERVICE_NAME, ''UDP'')
                ELSE NULL 
            END), ''Other'') AS Service,
                                CAST(SUBSTRING_INDEX(SRC_PORT, ''.'', 1) AS UNSIGNED) AS Source_Port,
            CAST(SUBSTRING_INDEX(DST_PORT, ''.'', 1) AS UNSIGNED) AS Destination_Port,
                        SRC_ETH_ADDRESS AS Source_MAC,
            DST_ETH_ADDRESS AS Destination_MAC,
            PROTOCOL AS PROTOCOL,
            MEASURE_START_DATE AS Time,
            MEASURE_END_DATE,
                        Previous_Time,
                        LD,ip_src_pair,port_src_pair,ip_dst_pair,port_dst_pair
            FROM 
            (select ID,SRC_ADDRESS, SRC_PORT, DST_ADDRESS, DST_PORT, DST_ETH_ADDRESS,SRC_ETH_ADDRESS, PROTOCOL,P1_UDP_SERVICE_NAME, P2_UDP_SERVICE_NAME, MEASURE_START_DATE,MEASURE_END_DATE,Previous_Time,LD,ip_src_pair,port_src_pair,ip_dst_pair,port_dst_pair,
                   case
                when (MEASURE_START_DATE - Previous_Time > ', timeInterval, ') then 1                
                when (LD is null and Previous_Time is null)  then 1 
                when (Previous_Time is null)  then 1
                when (LD - MEASURE_START_DATE > ', timeInterval, ') then 0
                when (LD is null) then 0
                else NULL end as Record_Indicator
            from (select ID,DEVICE_ID,SRC_ADDRESS,SRC_PORT,DST_ADDRESS,DST_PORT,PROTOCOL,MEASURE_START_DATE,MEASURE_END_DATE, DST_ETH_ADDRESS, SRC_ETH_ADDRESS, LEAD(MEASURE_START_DATE) OVER
            (PARTITION BY PROTOCOL,ip_src_pair,port_src_pair,ip_dst_pair,port_dst_pair order by MEASURE_START_DATE) AS LD,
                        LAG(MEASURE_START_DATE) OVER
            (PARTITION BY PROTOCOL,ip_src_pair,port_src_pair,ip_dst_pair,port_dst_pair order by MEASURE_START_DATE ) as Previous_Time  
                        ,ip_src_pair,port_src_pair,ip_dst_pair,port_dst_pair,P1_UDP_SERVICE_NAME ,P2_UDP_SERVICE_NAME
                        FROM (SELECT TH.TELEMETRY_GUID AS ID, DEVICE_ID, SRC_ADDRESS,SRC_PORT,DST_ADDRESS,DST_PORT,PROTOCOL,MEASURE_START_DATE, MEASURE_END_DATE, DST_ETH_ADDRESS, SRC_ETH_ADDRESS,
            CASE WHEN (SRC_ADDRESS < DST_ADDRESS) THEN SRC_ADDRESS ELSE DST_ADDRESS END as ip_src_pair ,
            CASE WHEN (SRC_ADDRESS < DST_ADDRESS) THEN DST_ADDRESS ELSE SRC_ADDRESS END as ip_dst_pair, 
            CASE WHEN (SRC_ADDRESS < DST_ADDRESS) THEN SRC_PORT ELSE DST_PORT END as port_src_pair ,
            CASE WHEN (SRC_ADDRESS < DST_ADDRESS) THEN DST_PORT ELSE SRC_PORT END as port_dst_pair,
                        P1.UDP_SERVICE_NAME as P1_UDP_SERVICE_NAME ,P2.UDP_SERVICE_NAME as P2_UDP_SERVICE_NAME
            FROM analytics.TELEMETRY_HEADER PARTITION (', Device, ')
                        as TH
                        LEFT JOIN analytics.PORT AS P1 ON
            P1.PORT_NUMBER = TH.SRC_PORT
            LEFT JOIN analytics.PORT AS P2 ON
            P2.PORT_NUMBER = TH.DST_PORT
                        where PROTOCOL = ''UDP'' and CONV(SUBSTRING(DST_ETH_ADDRESS, 1, 2), 16, 10) & 1 = 0 AND NOT (CONV(SUBSTRING_INDEX(DST_ADDRESS, ''.'', 1), 10, 2) LIKE ''1110%'') AND DST_ADDRESS != ''255.255.255.255'' ',IF(assetList IS NOT NULL AND assetList != '''', CONCAT(' AND (SRC_ETH_ADDRESS IN (', assetList, ') OR DST_ETH_ADDRESS IN (', assetList, '))'),''),'
                        )as b) a
             WHERE a.MEASURE_START_DATE BETWEEN ', fromTime, ' AND ', toTime, '
                        ) as data 
            ) final ) unicast  where Record_Indicator = 1 LIMIT ',pageNum, ' OFFSET ',pageSize,'' 
                         );


        PREPARE stmt FROM @detailed_statement_udp;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;



        PREPARE stmt FROM @detailed_statement_tcpmodbus;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;




        PREPARE stmt FROM @detailed_statement_ipnip;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;                     



        PREPARE stmt FROM @detailed_statement_arp;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;

END ;;
DELIMITER ;
/*!50003 SET sql_mode              = @saved_sql_mode */ ;
/*!50003 SET character_set_client  = @saved_cs_client */ ;
/*!50003 SET character_set_results = @saved_cs_results */ ;
/*!50003 SET collation_connection  = @saved_col_connection */ ;
/*!50003 DROP PROCEDURE IF EXISTS `GetTrafficDetailUnicast##` */;
/*!50003 SET @saved_cs_client      = @@character_set_client */ ;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;
/*!50003 SET character_set_client  = utf8mb4 */ ;
/*!50003 SET character_set_results = utf8mb4 */ ;
/*!50003 SET collation_connection  = utf8mb4_0900_ai_ci */ ;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;
/*!50003 SET sql_mode              = '' */ ;
DELIMITER ;;
CREATE DEFINER=`analytics_dev`@`%` PROCEDURE `GetTrafficDetailUnicast##`(
    IN Device VARCHAR(255), 
    IN fromTime INT, 
    IN toTime INT,
    IN timeInterval INT,
    IN assetList TEXT,
    IN pageNum INT
)
BEGIN
    

    SET @detailed_statement_arp = CONCAT('SELECT TH.TELEMETRY_GUID AS ID,
    SRC_ADDRESS as Source_IP, 
                        DST_ADDRESS as Destination_IP,
                        COALESCE( (CASE WHEN TH.PROTOCOL = ''ARP'' THEN ''ARP''  ELSE NULL END), ''Other'') AS Service,
                        CAST(SUBSTRING_INDEX(SRC_PORT, ''.'', 1) AS UNSIGNED) AS Source_Port,
                        CAST(SUBSTRING_INDEX(DST_PORT, ''.'', 1) AS UNSIGNED) AS Destination_Port,
                        SRC_ETH_ADDRESS AS Source_MAC,
            DST_ETH_ADDRESS AS Destination_MAC,
                        PROTOCOL,
                        CONCAT(CEIL(MEASURE_END_DATE - MEASURE_START_DATE), '' s'') as Duration,
                        MEASURE_START_DATE AS Time
            FROM analytics.TELEMETRY_HEADER PARTITION (', Device, ')
                        as TH
                        where PROTOCOL = ''ARP''  AND DST_ETH_ADDRESS != ''ff:ff:ff:ff:ff:ff'' 
                        and     MEASURE_START_DATE BETWEEN ', fromTime, ' AND ', toTime, IF(assetList IS NOT NULL AND assetList != '''', CONCAT(' AND (SRC_ETH_ADDRESS IN (', assetList, ') OR DST_ETH_ADDRESS IN (', assetList, '))'),''), ' LIMIT ',pageNum, 
                        -- ' OFFSET ',pageSize,
                        ''  
                         );
                         

    SET @detailed_statement_ipnip = CONCAT('
    SELECT TH.TELEMETRY_GUID AS ID,
        TH.SRC_ADDRESS as Source_IP,
        TH.DST_ADDRESS as Destination_IP,
        COALESCE(
            (CASE 
                WHEN TH.PROTOCOL = ''IP_NIP'' 
                     AND ip_proto.MEASURE_VALUE = 255 
                     THEN COALESCE(et.ETHER_NAME, ''Non IP'')   
                ELSE NULL
            END), 
            ''Other''
        ) AS Service,
        CAST(SUBSTRING_INDEX(TH.SRC_PORT, ''.'', 1) AS UNSIGNED) AS Source_Port,
        CAST(SUBSTRING_INDEX(TH.DST_PORT, ''.'', 1) AS UNSIGNED) AS Destination_Port,
        TH.SRC_ETH_ADDRESS AS Source_MAC,
        TH.DST_ETH_ADDRESS AS Destination_MAC,
        TH.PROTOCOL,
        CONCAT(CEIL(TH.MEASURE_END_DATE - TH.MEASURE_START_DATE), '' s'') as Duration,
        TH.MEASURE_START_DATE AS Time
    FROM 
        analytics.TELEMETRY_HEADER PARTITION (', Device, ') AS TH
    LEFT JOIN 
        analytics.TELEMETRY PARTITION (', Device, ') AS ip_proto
        ON TH.TELEMETRY_GUID = ip_proto.TELEMETRY_GUID 
        AND ip_proto.MEASURE_TYPE = ''ip_proto''
    LEFT JOIN 
        analytics.TELEMETRY PARTITION (', Device, ') AS eth_type
        ON TH.TELEMETRY_GUID = eth_type.TELEMETRY_GUID 
        AND eth_type.MEASURE_TYPE = ''eth_type''
    LEFT JOIN 
        analytics.ETHER_TYPE et
        ON eth_type.MEASURE_VALUE = et.ETHER_TYPE
    WHERE 
        TH.PROTOCOL = ''IP_NIP''  
        AND CONV(SUBSTRING(TH.DST_ETH_ADDRESS, 1, 2), 16, 10) & 1 = 0
        AND NOT (CONV(SUBSTRING_INDEX(TH.DST_ADDRESS, ''.'', 1), 10, 2) LIKE ''1110%'') 
        AND TH.DST_ADDRESS != ''255.255.255.255''
        AND (et.ETHER_NAME <> ''Unknown'' OR et.ETHER_NAME IS NULL)
        AND TH.MEASURE_START_DATE BETWEEN ', fromTime, ' AND ', toTime, IF(assetList IS NOT NULL AND assetList != '''',CONCAT(' AND (TH.SRC_ETH_ADDRESS IN (', assetList, ') OR TH.DST_ETH_ADDRESS IN (', assetList, '))'),''), '
        LIMIT ',pageNum, 
        -- ' OFFSET ',pageSize,
        '');

    
    SET @detailed_statement_tcpmodbus = CONCAT('SELECT  d.TELEMETRY_GUID AS ID,
    SRC_ADDRESS as Source_IP, 
                                DST_ADDRESS as Destination_IP, 
                                COALESCE( (CASE 
                WHEN PROTOCOL = ''TCP'' THEN COALESCE(P2_TCP_SERVICE_NAME, P1_TCP_SERVICE_NAME, ''TCP'')
                WHEN PROTOCOL = ''MODBUS'' THEN ''MODBUS''
                ELSE NULL 
            END), ''Other'') AS Service,
                                CAST(SUBSTRING_INDEX(SRC_PORT, ''.'', 1) AS UNSIGNED) AS Source_Port,
            CAST(SUBSTRING_INDEX(DST_PORT, ''.'', 1) AS UNSIGNED) AS Destination_Port,
                        SRC_ETH_ADDRESS AS Source_MAC,
            DST_ETH_ADDRESS AS Destination_MAC,
            PROTOCOL AS PROTOCOL,
            CONCAT(CEIL((MEASURE_END_DATE - MEASURE_START_DATE)), '' s'') AS Duration,
            MEASURE_START_DATE AS Time
                FROM (
                    SELECT a.TELEMETRY_GUID,
                    P2.TCP_SERVICE_NAME as P2_TCP_SERVICE_NAME,
                                            P1.TCP_SERVICE_NAME as P1_TCP_SERVICE_NAME,
                        b.MEASURE_VALUE as flow_indicator, 
                        a.SRC_ADDRESS, a.SRC_PORT, a.DST_ADDRESS, a.DST_PORT ,SRC_ETH_ADDRESS,DST_ETH_ADDRESS,
                                                MEASURE_START_DATE,
                                                MEASURE_END_DATE,
                                                PROTOCOL,
                                                ROW_NUMBER() OVER (PARTITION BY SRC_ADDRESS, SRC_PORT, DST_ADDRESS, DST_PORT,PROTOCOL,b.MEASURE_VALUE ORDER BY MEASURE_START_DATE ASC) as row_num
                    FROM analytics.TELEMETRY_HEADER PARTITION (', Device, ') a 
                    INNER JOIN analytics.TELEMETRY PARTITION (', Device, ') b 
                                        ON a.TELEMETRY_GUID = b.TELEMETRY_GUID AND b.MEASURE_TYPE = ''flowindicator'' ',IF(assetList IS NOT NULL AND assetList != '''', CONCAT(' AND (SRC_ETH_ADDRESS IN (', assetList, ') OR DST_ETH_ADDRESS IN (', assetList, '))'),''),' and PROTOCOL IN (''TCP'', ''MODBUS'') 
                                        AND MEASURE_START_DATE BETWEEN ', fromTime, ' AND ', toTime, '
                                        LEFT JOIN analytics.PORT AS P1 ON
                    P1.PORT_NUMBER = a.SRC_PORT
                    LEFT JOIN analytics.PORT AS P2 ON
                    P2.PORT_NUMBER = a.DST_PORT
                ) AS d
                where row_num = 1 LIMIT ',pageNum, 
                -- ' OFFSET ',pageSize,
                ''  );

    
    SET @detailed_statement_udp = CONCAT('select ID,Source_IP,Destination_IP,Service,Source_Port,Destination_Port,Source_MAC,Destination_MAC,PROTOCOL,Duration,Time from (
 select *, CONCAT(CEIL((max(MEASURE_END_DATE) over ( partition by PROTOCOL,ip_src_pair,port_src_pair,ip_dst_pair,port_dst_pair,Flow_Indicator) - Time )), '' s'') as Duration from (   
      SELECT ID,SRC_ADDRESS as Source_IP, 
                                DST_ADDRESS as Destination_IP, 
                Record_Indicator,
                SUM(Record_Indicator) over ( partition by PROTOCOL,ip_src_pair,port_src_pair,ip_dst_pair,port_dst_pair order by MEASURE_START_DATE) as Flow_Indicator,
                COALESCE( (CASE 
                                WHEN PROTOCOL = ''UDP'' THEN COALESCE(P2_UDP_SERVICE_NAME, P1_UDP_SERVICE_NAME, ''UDP'')
                ELSE NULL 
            END), ''Other'') AS Service,
                                CAST(SUBSTRING_INDEX(SRC_PORT, ''.'', 1) AS UNSIGNED) AS Source_Port,
            CAST(SUBSTRING_INDEX(DST_PORT, ''.'', 1) AS UNSIGNED) AS Destination_Port,
                        SRC_ETH_ADDRESS AS Source_MAC,
            DST_ETH_ADDRESS AS Destination_MAC,
            PROTOCOL AS PROTOCOL,
            MEASURE_START_DATE AS Time,
            MEASURE_END_DATE,
                        Previous_Time,
                        LD,ip_src_pair,port_src_pair,ip_dst_pair,port_dst_pair
            FROM 
            (select ID,SRC_ADDRESS, SRC_PORT, DST_ADDRESS, DST_PORT, DST_ETH_ADDRESS,SRC_ETH_ADDRESS, PROTOCOL,P1_UDP_SERVICE_NAME, P2_UDP_SERVICE_NAME, MEASURE_START_DATE,MEASURE_END_DATE,Previous_Time,LD,ip_src_pair,port_src_pair,ip_dst_pair,port_dst_pair,
                   case
                when (MEASURE_START_DATE - Previous_Time > ', timeInterval, ') then 1                
                when (LD is null and Previous_Time is null)  then 1 
                when (Previous_Time is null)  then 1
                when (LD - MEASURE_START_DATE > ', timeInterval, ') then 0
                when (LD is null) then 0
                else NULL end as Record_Indicator
            from (select ID,DEVICE_ID,SRC_ADDRESS,SRC_PORT,DST_ADDRESS,DST_PORT,PROTOCOL,MEASURE_START_DATE,MEASURE_END_DATE, DST_ETH_ADDRESS, SRC_ETH_ADDRESS, LEAD(MEASURE_START_DATE) OVER
            (PARTITION BY PROTOCOL,ip_src_pair,port_src_pair,ip_dst_pair,port_dst_pair order by MEASURE_START_DATE) AS LD,
                        LAG(MEASURE_START_DATE) OVER
            (PARTITION BY PROTOCOL,ip_src_pair,port_src_pair,ip_dst_pair,port_dst_pair order by MEASURE_START_DATE ) as Previous_Time  
                        ,ip_src_pair,port_src_pair,ip_dst_pair,port_dst_pair,P1_UDP_SERVICE_NAME ,P2_UDP_SERVICE_NAME
                        FROM (SELECT TH.TELEMETRY_GUID AS ID, DEVICE_ID, SRC_ADDRESS,SRC_PORT,DST_ADDRESS,DST_PORT,PROTOCOL,MEASURE_START_DATE, MEASURE_END_DATE, DST_ETH_ADDRESS, SRC_ETH_ADDRESS,
            CASE WHEN (SRC_ADDRESS < DST_ADDRESS) THEN SRC_ADDRESS ELSE DST_ADDRESS END as ip_src_pair ,
            CASE WHEN (SRC_ADDRESS < DST_ADDRESS) THEN DST_ADDRESS ELSE SRC_ADDRESS END as ip_dst_pair, 
            CASE WHEN (SRC_ADDRESS < DST_ADDRESS) THEN SRC_PORT ELSE DST_PORT END as port_src_pair ,
            CASE WHEN (SRC_ADDRESS < DST_ADDRESS) THEN DST_PORT ELSE SRC_PORT END as port_dst_pair,
                        P1.UDP_SERVICE_NAME as P1_UDP_SERVICE_NAME ,P2.UDP_SERVICE_NAME as P2_UDP_SERVICE_NAME
            FROM analytics.TELEMETRY_HEADER PARTITION (', Device, ')
                        as TH
                        LEFT JOIN analytics.PORT AS P1 ON
            P1.PORT_NUMBER = TH.SRC_PORT
            LEFT JOIN analytics.PORT AS P2 ON
            P2.PORT_NUMBER = TH.DST_PORT
                        where PROTOCOL = ''UDP'' and CONV(SUBSTRING(DST_ETH_ADDRESS, 1, 2), 16, 10) & 1 = 0 AND NOT (CONV(SUBSTRING_INDEX(DST_ADDRESS, ''.'', 1), 10, 2) LIKE ''1110%'') AND DST_ADDRESS != ''255.255.255.255'' ',IF(assetList IS NOT NULL AND assetList != '''', CONCAT(' AND (SRC_ETH_ADDRESS IN (', assetList, ') OR DST_ETH_ADDRESS IN (', assetList, '))'),''),'
                        )as b) a
             WHERE a.MEASURE_START_DATE BETWEEN ', fromTime, ' AND ', toTime, '
                        ) as data 
            ) final ) unicast  where Record_Indicator = 1 LIMIT ',pageNum, 
            -- ' OFFSET ',pageSize,
            '' 
                         );


        PREPARE stmt FROM @detailed_statement_udp;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;



        PREPARE stmt FROM @detailed_statement_tcpmodbus;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;




        PREPARE stmt FROM @detailed_statement_ipnip;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;                     



        PREPARE stmt FROM @detailed_statement_arp;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;

END ;;
DELIMITER ;
/*!50003 SET sql_mode              = @saved_sql_mode */ ;
/*!50003 SET character_set_client  = @saved_cs_client */ ;
/*!50003 SET character_set_results = @saved_cs_results */ ;
/*!50003 SET collation_connection  = @saved_col_connection */ ;
/*!50003 DROP PROCEDURE IF EXISTS `GetTrafficDiscributionUnicast` */;
/*!50003 SET @saved_cs_client      = @@character_set_client */ ;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;
/*!50003 SET character_set_client  = latin1 */ ;
/*!50003 SET character_set_results = latin1 */ ;
/*!50003 SET collation_connection  = latin1_swedish_ci */ ;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;
/*!50003 SET sql_mode              = '' */ ;
DELIMITER ;;
CREATE DEFINER=`analytics_dev`@`%` PROCEDURE `GetTrafficDiscributionUnicast`(
    IN Device VARCHAR(255), 
    IN fromTime INT, 
    IN toTime INT, 
    IN timeInterval INT, 
    IN protocolParam VARCHAR(255)
)
BEGIN
    SET @sql = CONCAT('
        SELECT SUM(noOFSessions) AS unicastTotal FROM (
            SELECT SUM(count) AS noOFSessions FROM (
                SELECT MAX(flowindicator_MAX - flowindicator_MIN) + 1 AS count, SRC_ADDRESS, SRC_PORT, DST_ADDRESS, DST_PORT 
                FROM (
                    SELECT 
                        MIN(MEASURE_VALUE) OVER (PARTITION BY SRC_ADDRESS, SRC_PORT, DST_ADDRESS, DST_PORT, PROTOCOL) AS flowindicator_MIN, 
                        MAX(MEASURE_VALUE) OVER (PARTITION BY SRC_ADDRESS, SRC_PORT, DST_ADDRESS, DST_PORT, PROTOCOL) AS flowindicator_MAX,
                        a.SRC_ADDRESS, a.SRC_PORT, a.DST_ADDRESS, a.DST_PORT 
                    FROM analytics.TELEMETRY_HEADER PARTITION (', Device, ') a 
                    INNER JOIN analytics.TELEMETRY PARTITION (', Device, ') b ON a.TELEMETRY_GUID = b.TELEMETRY_GUID AND b.MEASURE_TYPE = ''flowindicator''
                    WHERE PROTOCOL IN (''TCP'', ''MODBUS'') AND MEASURE_START_DATE BETWEEN ', fromTime, ' AND ', toTime, '
                ) AS d
                GROUP BY SRC_ADDRESS, SRC_PORT, DST_ADDRESS, DST_PORT
            ) AS TCP
            UNION ALL
            SELECT COUNT(*) AS noOFSessions FROM (
                SELECT DEVICE_ID, SRC_ADDRESS, SRC_PORT, DST_ADDRESS, DST_PORT, PROTOCOL, MEASURE_START_DATE, LEAD(MEASURE_START_DATE) 
                OVER (PARTITION BY PROTOCOL, ip_src_pair, port_src_pair, ip_dst_pair, port_dst_pair ORDER BY MEASURE_START_DATE DESC) AS LD, ip_src_pair, port_src_pair, ip_dst_pair, port_dst_pair 
                FROM (
                    SELECT DEVICE_ID, SRC_ADDRESS, SRC_PORT, DST_ADDRESS, DST_PORT, PROTOCOL, MEASURE_START_DATE,
                        CASE WHEN (SRC_ADDRESS < DST_ADDRESS) THEN SRC_ADDRESS ELSE DST_ADDRESS END AS ip_src_pair,
                        CASE WHEN (SRC_ADDRESS < DST_ADDRESS) THEN DST_ADDRESS ELSE SRC_ADDRESS END AS ip_dst_pair, 
                        CASE WHEN (SRC_ADDRESS < DST_ADDRESS) THEN SRC_PORT ELSE DST_PORT END AS port_src_pair,
                        CASE WHEN (SRC_ADDRESS < DST_ADDRESS) THEN DST_PORT ELSE SRC_PORT END AS port_dst_pair  
                    FROM analytics.TELEMETRY_HEADER PARTITION (', Device, ')
                    WHERE MEASURE_START_DATE BETWEEN ', fromTime, ' AND ', toTime, '
                ) AS b
            ) AS a
            WHERE a.MEASURE_START_DATE - a.LD > ', timeInterval, ' AND PROTOCOL = ''', protocolParam, '''
        ) AS unicast');

    PREPARE stmt FROM @sql;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;
END ;;
DELIMITER ;
/*!50003 SET sql_mode              = @saved_sql_mode */ ;
/*!50003 SET character_set_client  = @saved_cs_client */ ;
/*!50003 SET character_set_results = @saved_cs_results */ ;
/*!50003 SET collation_connection  = @saved_col_connection */ ;
/*!50003 DROP PROCEDURE IF EXISTS `GetTrafficDistribution` */;
/*!50003 SET @saved_cs_client      = @@character_set_client */ ;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;
/*!50003 SET character_set_client  = latin1 */ ;
/*!50003 SET character_set_results = latin1 */ ;
/*!50003 SET collation_connection  = latin1_swedish_ci */ ;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;
/*!50003 SET sql_mode              = '' */ ;
DELIMITER ;;
CREATE DEFINER=`analytics_dev`@`%` PROCEDURE `GetTrafficDistribution`(
    IN Device VARCHAR(255), 
    IN fromTime INT, 
    IN toTime INT, 
    IN timeInterval INT,
    IN assetList TEXT
)
BEGIN
    SET @sql = CONCAT('
        SELECT 
          SUM(CASE
            WHEN (PROTOCOL IN (''UDP'', ''IP_NIP'') AND CONV(SUBSTRING(DST_ETH_ADDRESS, 1, 2), 16, 10) & 1 = 0 AND NOT (CONV(SUBSTRING_INDEX(DST_ADDRESS, ''.'', 1), 10, 2) LIKE ''1110%'') AND DST_ADDRESS != ''255.255.255.255'') 
              THEN 1
            WHEN PROTOCOL = ''ARP'' AND DST_ETH_ADDRESS != ''ff:ff:ff:ff:ff:ff'' THEN 1
            ELSE 0
            END) AS Unicast,
          SUM(CASE
            WHEN PROTOCOL IN (''UDP'', ''IP_NIP'') AND LEFT(REPLACE(DST_ETH_ADDRESS, '':'', ''''), 6) = ''01005E'' AND CONV(SUBSTRING_INDEX(DST_ADDRESS, ''.'', 1), 10, 2) LIKE ''1110%'' 
              THEN 1
            ELSE 0
            END) AS Multicast,
          SUM(CASE
            WHEN PROTOCOL IN (''UDP'', ''IP_NIP'') AND ((DST_ETH_ADDRESS LIKE ''ff:ff:ff:ff:ff:ff'') OR
            DST_ADDRESS = ''255.255.255.255'' OR 
              (SUBSTRING(LPAD(CONV(SUBSTRING_INDEX(DST_ADDRESS, ''.'', 1),10,2), 8, ''0''), 1, 1) = ''0'' AND SUBSTRING_INDEX(DST_ADDRESS, ''.'', -3) = ''255.255.255'') OR
              (SUBSTRING(LPAD(CONV(SUBSTRING_INDEX(DST_ADDRESS, ''.'', 1),10,2), 8, ''0''), 1, 2) = ''10'' AND SUBSTRING_INDEX(DST_ADDRESS, ''.'', -2) = ''255.255'' ) OR 
              (SUBSTRING(LPAD(CONV(SUBSTRING_INDEX(DST_ADDRESS, ''.'', 1),10,2), 8, ''0''), 1, 3) = ''110'' AND SUBSTRING_INDEX(DST_ADDRESS, ''.'', -1) = ''255'')
            ) THEN 1
            WHEN PROTOCOL = ''ARP'' AND DST_ETH_ADDRESS = ''ff:ff:ff:ff:ff:ff'' THEN 1
            ELSE 0
            END) AS Broadcast
            FROM 
            (select SRC_ADDRESS, SRC_PORT, DST_ADDRESS, DST_PORT, DST_ETH_ADDRESS,PROTOCOL 
            from (select DEVICE_ID,SRC_ADDRESS,SRC_PORT,DST_ADDRESS,DST_PORT,PROTOCOL,MEASURE_START_DATE,DST_ETH_ADDRESS, LEAD(MEASURE_START_DATE) OVER
            (PARTITION BY PROTOCOL,ip_src_pair,port_src_pair,ip_dst_pair,port_dst_pair order by MEASURE_START_DATE DESC) AS LD,ip_src_pair,port_src_pair,ip_dst_pair,port_dst_pair FROM (SELECT DEVICE_ID, SRC_ADDRESS,SRC_PORT,DST_ADDRESS,DST_PORT,PROTOCOL,MEASURE_START_DATE,DST_ETH_ADDRESS,
            CASE WHEN (SRC_ADDRESS < DST_ADDRESS) THEN SRC_ADDRESS ELSE DST_ADDRESS END as ip_src_pair ,
            CASE WHEN (SRC_ADDRESS < DST_ADDRESS) THEN DST_ADDRESS ELSE SRC_ADDRESS END as ip_dst_pair, 
            CASE WHEN (SRC_ADDRESS < DST_ADDRESS) THEN SRC_PORT ELSE DST_PORT END as port_src_pair ,
            CASE WHEN (SRC_ADDRESS < DST_ADDRESS) THEN DST_PORT ELSE SRC_PORT END as port_dst_pair  
            FROM analytics.TELEMETRY_HEADER PARTITION (', Device, ')) as b) a
            WHERE a.MEASURE_START_DATE - a.LD > ', timeInterval, ' and PROTOCOL = ''UDP''
            AND a.MEASURE_START_DATE BETWEEN ', fromTime, ' AND ', toTime, IF(assetList IS NOT NULL AND assetList != '''', CONCAT(' AND (DST_ETH_ADDRESS IN (', assetList, '))'),''), ') as data');

    PREPARE stmt FROM @sql;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;
END ;;
DELIMITER ;
/*!50003 SET sql_mode              = @saved_sql_mode */ ;
/*!50003 SET character_set_client  = @saved_cs_client */ ;
/*!50003 SET character_set_results = @saved_cs_results */ ;
/*!50003 SET collation_connection  = @saved_col_connection */ ;
/*!50003 DROP PROCEDURE IF EXISTS `GetTrafficeDistribution` */;
/*!50003 SET @saved_cs_client      = @@character_set_client */ ;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;
/*!50003 SET character_set_client  = latin1 */ ;
/*!50003 SET character_set_results = latin1 */ ;
/*!50003 SET collation_connection  = latin1_swedish_ci */ ;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;
/*!50003 SET sql_mode              = '' */ ;
DELIMITER ;;
CREATE DEFINER=`analytics_dev`@`%` PROCEDURE `GetTrafficeDistribution`(
    IN Device VARCHAR(255), 
    IN fromTime INT, 
    IN toTime INT, 
    IN timeInterval INT,
    IN assetList TEXT
)
BEGIN
    SET @sql = CONCAT('
        SELECT 
          SUM(CASE
            WHEN (PROTOCOL IN (''UDP'', ''IP_NIP'') AND CONV(SUBSTRING(DST_ETH_ADDRESS, 1, 2), 16, 10) & 1 = 0 AND NOT (CONV(SUBSTRING_INDEX(DST_ADDRESS, ''.'', 1), 10, 2) LIKE ''1110%'') AND DST_ADDRESS != ''255.255.255.255'') 
              THEN 1
            WHEN PROTOCOL = ''ARP'' AND DST_ETH_ADDRESS != ''ff:ff:ff:ff:ff:ff'' THEN 1
            ELSE 0
            END) AS Unicast,
          SUM(CASE
            WHEN PROTOCOL IN (''UDP'', ''IP_NIP'') AND LEFT(REPLACE(DST_ETH_ADDRESS, '':'', ''''), 6) = ''01005E'' AND CONV(SUBSTRING_INDEX(DST_ADDRESS, ''.'', 1), 10, 2) LIKE ''1110%'' 
              THEN 1
            ELSE 0
            END) AS Multicast,
          SUM(CASE
            WHEN PROTOCOL IN (''UDP'', ''IP_NIP'') AND ((DST_ETH_ADDRESS LIKE ''ff:ff:ff:ff:ff:ff'') OR
            DST_ADDRESS = ''255.255.255.255'' OR 
              (SUBSTRING(LPAD(CONV(SUBSTRING_INDEX(DST_ADDRESS, ''.'', 1),10,2), 8, ''0''), 1, 1) = ''0'' AND SUBSTRING_INDEX(DST_ADDRESS, ''.'', -3) = ''255.255.255'') OR
              (SUBSTRING(LPAD(CONV(SUBSTRING_INDEX(DST_ADDRESS, ''.'', 1),10,2), 8, ''0''), 1, 2) = ''10'' AND SUBSTRING_INDEX(DST_ADDRESS, ''.'', -2) = ''255.255'' ) OR 
              (SUBSTRING(LPAD(CONV(SUBSTRING_INDEX(DST_ADDRESS, ''.'', 1),10,2), 8, ''0''), 1, 3) = ''110'' AND SUBSTRING_INDEX(DST_ADDRESS, ''.'', -1) = ''255'')
            ) THEN 1
            WHEN PROTOCOL = ''ARP'' AND DST_ETH_ADDRESS = ''ff:ff:ff:ff:ff:ff'' THEN 1
            ELSE 0
            END) AS Broadcast
            FROM 
            (select SRC_ADDRESS, SRC_PORT, DST_ADDRESS, DST_PORT, DST_ETH_ADDRESS,PROTOCOL 
            from (select DEVICE_ID,SRC_ADDRESS,SRC_PORT,DST_ADDRESS,DST_PORT,PROTOCOL,MEASURE_START_DATE,DST_ETH_ADDRESS, LEAD(MEASURE_START_DATE) OVER
            (PARTITION BY PROTOCOL,ip_src_pair,port_src_pair,ip_dst_pair,port_dst_pair order by MEASURE_START_DATE DESC) AS LD,ip_src_pair,port_src_pair,ip_dst_pair,port_dst_pair FROM (SELECT DEVICE_ID, SRC_ADDRESS,SRC_PORT,DST_ADDRESS,DST_PORT,PROTOCOL,MEASURE_START_DATE,DST_ETH_ADDRESS,
            CASE WHEN (SRC_ADDRESS < DST_ADDRESS) THEN SRC_ADDRESS ELSE DST_ADDRESS END as ip_src_pair ,
            CASE WHEN (SRC_ADDRESS < DST_ADDRESS) THEN DST_ADDRESS ELSE SRC_ADDRESS END as ip_dst_pair, 
            CASE WHEN (SRC_ADDRESS < DST_ADDRESS) THEN SRC_PORT ELSE DST_PORT END as port_src_pair ,
            CASE WHEN (SRC_ADDRESS < DST_ADDRESS) THEN DST_PORT ELSE SRC_PORT END as port_dst_pair  
            FROM analytics.TELEMETRY_HEADER PARTITION (', Device, ')) as b) a
            WHERE a.MEASURE_START_DATE - a.LD > ', timeInterval, ' and PROTOCOL = ''UDP''
            AND a.MEASURE_START_DATE BETWEEN ', fromTime, ' AND ', toTime, IF(assetList IS NOT NULL AND assetList != '''', CONCAT(' AND (DST_ETH_ADDRESS IN (', assetList, '))'),''), ') as data');

    PREPARE stmt FROM @sql;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;
END ;;
DELIMITER ;
/*!50003 SET sql_mode              = @saved_sql_mode */ ;
/*!50003 SET character_set_client  = @saved_cs_client */ ;
/*!50003 SET character_set_results = @saved_cs_results */ ;
/*!50003 SET collation_connection  = @saved_col_connection */ ;
/*!50003 DROP PROCEDURE IF EXISTS `GetTransportDiagnosis` */;
/*!50003 SET @saved_cs_client      = @@character_set_client */ ;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;
/*!50003 SET character_set_client  = latin1 */ ;
/*!50003 SET character_set_results = latin1 */ ;
/*!50003 SET collation_connection  = latin1_swedish_ci */ ;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;
/*!50003 SET sql_mode              = '' */ ;
DELIMITER ;;
CREATE DEFINER=`analytics_dev`@`%` PROCEDURE `GetTransportDiagnosis`(
    IN Device VARCHAR(255),
    IN fromTime INT,
    IN toTime INT,
    IN IP VARCHAR(255)  
)
BEGIN
    SET @sqlQuery = CONCAT('
        SELECT 
            MEASURE_START_DATE DIV 30 * 30 AS time,
            SUM(
                CASE 
                    WHEN MEASURE_TYPE = ''network_layer'' THEN MEASURE_VALUE
                    ELSE 0
                END
            ) AS "Network Diagnostics",
            SUM(
                CASE 
                    WHEN MEASURE_TYPE = ''transport_layer'' THEN MEASURE_VALUE
                    ELSE 0
                END
            ) AS "Transport Diagnostics"
        FROM analytics.TELEMETRY_HEADER PARTITION (', Device, ') AS H
        INNER JOIN analytics.TELEMETRY PARTITION (', Device, ') AS T 
            ON H.TELEMETRY_GUID = T.TELEMETRY_GUID 
            AND (MEASURE_TYPE = ''network_layer'' OR MEASURE_TYPE = ''transport_layer'')
        WHERE MEASURE_START_DATE BETWEEN ', fromTime, ' AND ', toTime,
        IF(IP IS NOT NULL AND IP != '''', CONCAT(' AND SRC_ADDRESS = ''', IP, ''''), ''),
        ' GROUP BY time
        ORDER BY time;');

    PREPARE stmt FROM @sqlQuery;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;
END ;;
DELIMITER ;
/*!50003 SET sql_mode              = @saved_sql_mode */ ;
/*!50003 SET character_set_client  = @saved_cs_client */ ;
/*!50003 SET character_set_results = @saved_cs_results */ ;
/*!50003 SET collation_connection  = @saved_col_connection */ ;
/*!50003 DROP PROCEDURE IF EXISTS `GetUDPAndICMPNetworkConnections` */;
/*!50003 SET @saved_cs_client      = @@character_set_client */ ;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;
/*!50003 SET character_set_client  = latin1 */ ;
/*!50003 SET character_set_results = latin1 */ ;
/*!50003 SET collation_connection  = latin1_swedish_ci */ ;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;
/*!50003 SET sql_mode              = '' */ ;
DELIMITER ;;
CREATE DEFINER=`analytics_dev`@`%` PROCEDURE `GetUDPAndICMPNetworkConnections`(
    IN Device VARCHAR(255), 
    IN fromTime INT, 
    IN toTime INT, 
    IN timeInterval INT
)
BEGIN
    SET @sql = CONCAT('
        SELECT COUNT(*) AS udpIcmpCount 
        FROM (
            SELECT DEVICE_ID, SRC_ADDRESS, SRC_PORT, DST_ADDRESS, DST_PORT,
                   PROTOCOL, MEASURE_START_DATE, LEAD(MEASURE_START_DATE) 
                   OVER (PARTITION BY PROTOCOL, ip_src_pair, port_src_pair, ip_dst_pair, port_dst_pair ORDER BY MEASURE_START_DATE DESC) AS LD, 
                   ip_src_pair, port_src_pair, ip_dst_pair, port_dst_pair 
            FROM (
                SELECT DEVICE_ID, SRC_ADDRESS, SRC_PORT, DST_ADDRESS, DST_PORT, PROTOCOL, MEASURE_START_DATE, 
                       CASE WHEN (SRC_ADDRESS < DST_ADDRESS) THEN SRC_ADDRESS ELSE DST_ADDRESS END as ip_src_pair,
                       CASE WHEN (SRC_ADDRESS < DST_ADDRESS) THEN DST_ADDRESS ELSE SRC_ADDRESS END as ip_dst_pair,
                       CASE WHEN (SRC_ADDRESS < DST_ADDRESS) THEN SRC_PORT ELSE DST_PORT END as port_src_pair,
                       CASE WHEN (SRC_ADDRESS < DST_ADDRESS) THEN DST_PORT ELSE SRC_PORT END as port_dst_pair  
                FROM analytics.TELEMETRY_HEADER PARTITION (', Device, ')
                WHERE MEASURE_START_DATE BETWEEN ', fromTime, ' AND ', toTime, '
            ) as b
        ) a
        WHERE a.MEASURE_START_DATE - a.LD > ', timeInterval, ' AND a.PROTOCOL IN (''UDP'', ''ICMP'');');

    PREPARE stmt FROM @sql;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;
END ;;
DELIMITER ;
/*!50003 SET sql_mode              = @saved_sql_mode */ ;
/*!50003 SET character_set_client  = @saved_cs_client */ ;
/*!50003 SET character_set_results = @saved_cs_results */ ;
/*!50003 SET collation_connection  = @saved_col_connection */ ;
/*!50003 DROP PROCEDURE IF EXISTS `GetUnicastTraffic` */;
/*!50003 SET @saved_cs_client      = @@character_set_client */ ;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;
/*!50003 SET character_set_client  = latin1 */ ;
/*!50003 SET character_set_results = latin1 */ ;
/*!50003 SET collation_connection  = latin1_swedish_ci */ ;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;
/*!50003 SET sql_mode              = '' */ ;
DELIMITER ;;
CREATE DEFINER=`analytics_dev`@`%` PROCEDURE `GetUnicastTraffic`(
    IN Device VARCHAR(255),
    IN fromTime INT,
    IN toTime INT,
    IN assetList TEXT,
    IN pageSize INT,
    IN page INT
)
BEGIN
    DECLARE offsetParam INT;
    SET offsetParam = page * pageSize;
    
    SET @sql_query = CONCAT('
        SELECT
            TH.TELEMETRY_GUID AS ID,
            TH.SRC_ADDRESS AS Source_IP,
            TH.DST_ADDRESS AS Destination_IP,
            COALESCE( (CASE 
                WHEN TH.PROTOCOL = ''TCP'' THEN COALESCE(P2.TCP_SERVICE_NAME, P1.TCP_SERVICE_NAME, ''TCP'')
                WHEN TH.PROTOCOL = ''UDP'' THEN COALESCE(P2.UDP_SERVICE_NAME, P1.UDP_SERVICE_NAME, ''UDP'')
                WHEN TH.PROTOCOL = ''MODBUS'' THEN ''MODBUS''
                WHEN TH.PROTOCOL = ''ICMP'' THEN ''ICMP''
                WHEN TH.PROTOCOL = ''ARP'' THEN ''ARP''
                WHEN TH.PROTOCOL = ''IP_NIP'' AND analytics.TELEMETRY.MEASURE_TYPE = ''ip_proto'' AND analytics.TELEMETRY.MEASURE_VALUE = 255 THEN ''Non IP''
                ELSE NULL 
            END), ''Other'') AS Service,
            CAST(SUBSTRING_INDEX(TH.SRC_PORT, ''.'', 1) AS UNSIGNED) AS Source_Port,
            CAST(SUBSTRING_INDEX(TH.DST_PORT, ''.'', 1) AS UNSIGNED) AS Destination_Port,
            TH.SRC_ETH_ADDRESS AS Source_MAC,
            TH.DST_ETH_ADDRESS AS Destination_MAC,
            TH.PROTOCOL AS PROTOCOL,
            CONCAT(CEIL((TH.MEASURE_END_DATE - TH.MEASURE_START_DATE)), '' s'') AS Duration,
            TH.MEASURE_START_DATE AS Time
        FROM
            analytics.TELEMETRY_HEADER PARTITION (', Device, ') AS TH
        LEFT JOIN analytics.TELEMETRY PARTITION (', Device, ') ON
            TH.TELEMETRY_GUID = analytics.TELEMETRY.TELEMETRY_GUID AND
            analytics.TELEMETRY.MEASURE_TYPE = ''ip_proto'' AND
            analytics.TELEMETRY.MEASURE_VALUE = 255
        LEFT JOIN analytics.PORT AS P1 ON
            P1.PORT_NUMBER = TH.SRC_PORT
        LEFT JOIN analytics.PORT AS P2 ON
            P2.PORT_NUMBER = TH.DST_PORT
        WHERE
            TH.MEASURE_START_DATE BETWEEN ', fromTime, ' AND ', toTime, IF(assetList IS NOT NULL AND assetList != '''', CONCAT(' AND (SRC_ETH_ADDRESS IN (', assetList, ') OR DST_ETH_ADDRESS IN (', assetList, '))'),''), ' AND
            ((TH.PROTOCOL IN (''UDP'', ''IP_NIP'') AND
              CONV(SUBSTRING(TH.DST_ETH_ADDRESS, 1, 2), 16, 10) & 1 = 0 AND
              NOT (CONV(SUBSTRING_INDEX(TH.DST_ADDRESS, ''.'', 1), 10, 2) LIKE ''1110%'') AND
              TH.DST_ADDRESS != ''255.255.255.255'') OR
             (TH.PROTOCOL = ''ARP'' AND
              TH.DST_ETH_ADDRESS != ''ff:ff:ff:ff:ff:ff'') OR
             TH.PROTOCOL IN (''TCP'', ''MODBUS'', ''ICMP''))
        LIMIT ', pageSize, ' OFFSET ', offsetParam, ';');

    SET @count_query = CONCAT('
        SELECT count(*) AS count
        FROM
            analytics.TELEMETRY_HEADER PARTITION (', Device, ') AS TH
        WHERE
            TH.MEASURE_START_DATE BETWEEN ', fromTime, ' AND ', toTime, IF(assetList IS NOT NULL AND assetList != '''', CONCAT(' AND (SRC_ETH_ADDRESS IN (', assetList, ') OR DST_ETH_ADDRESS IN (', assetList, '))'),''), ' AND
            ((TH.PROTOCOL IN (''UDP'', ''IP_NIP'') AND
              CONV(SUBSTRING(TH.DST_ETH_ADDRESS, 1, 2), 16, 10) & 1 = 0 AND
              NOT (CONV(SUBSTRING_INDEX(TH.DST_ADDRESS, ''.'', 1), 10, 2) LIKE ''1110%'') AND
              TH.DST_ADDRESS != ''255.255.255.255'') OR
             (TH.PROTOCOL = ''ARP'' AND
              TH.DST_ETH_ADDRESS != ''ff:ff:ff:ff:ff:ff'') OR
             TH.PROTOCOL IN (''TCP'', ''MODBUS'', ''ICMP''));');

    PREPARE main_stmt FROM @sql_query;
    EXECUTE main_stmt;
    DEALLOCATE PREPARE main_stmt;
    
    PREPARE count_stmt FROM @count_query;
    EXECUTE count_stmt;
    DEALLOCATE PREPARE count_stmt;
END ;;
DELIMITER ;
/*!50003 SET sql_mode              = @saved_sql_mode */ ;
/*!50003 SET character_set_client  = @saved_cs_client */ ;
/*!50003 SET character_set_results = @saved_cs_results */ ;
/*!50003 SET collation_connection  = @saved_col_connection */ ;
SET @@SESSION.SQL_LOG_BIN = @MYSQLDUMP_TEMP_LOG_BIN;
/*!40103 SET TIME_ZONE=@OLD_TIME_ZONE */;

/*!40101 SET SQL_MODE=@OLD_SQL_MODE */;
/*!40014 SET FOREIGN_KEY_CHECKS=@OLD_FOREIGN_KEY_CHECKS */;
/*!40014 SET UNIQUE_CHECKS=@OLD_UNIQUE_CHECKS */;
/*!40101 SET CHARACTER_SET_CLIENT=@OLD_CHARACTER_SET_CLIENT */;
/*!40101 SET CHARACTER_SET_RESULTS=@OLD_CHARACTER_SET_RESULTS */;
/*!40101 SET COLLATION_CONNECTION=@OLD_COLLATION_CONNECTION */;
/*!40111 SET SQL_NOTES=@OLD_SQL_NOTES */;

-- Dump completed on 2025-02-06 18:35:53
