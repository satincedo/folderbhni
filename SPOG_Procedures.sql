CREATE DATABASE IF NOT EXISTS `analytics_spog`;

USE `analytics_spog`;

CREATE TABLE IF NOT EXISTS `ROLES` (
  `ROLE_ID` int NOT NULL AUTO_INCREMENT,
  `ROLE_NAME` varchar(100) NOT NULL,
  PRIMARY KEY (`ROLE_ID`),
  UNIQUE KEY `ROLE_NAME` (`ROLE_NAME`)
) ENGINE=InnoDB AUTO_INCREMENT=6 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

INSERT IGNORE INTO ROLES VALUES(1,'Admin');
INSERT IGNORE INTO ROLES VALUES(3,'Belden Employee');
INSERT IGNORE INTO ROLES VALUES(2,'BHNI Team');
INSERT IGNORE INTO ROLES VALUES(5,'Customer');
INSERT IGNORE INTO ROLES VALUES(4,'Partner');

CREATE TABLE IF NOT EXISTS `USER_ROLES` (
  `USER_ID` varchar(100) NOT NULL,
  `ROLE_ID` int NOT NULL,
  `TENANT_ID` char(36) NOT NULL,
  PRIMARY KEY (`USER_ID`,`TENANT_ID`,`ROLE_ID`),
  KEY `ROLE_ID` (`ROLE_ID`),
  CONSTRAINT `USER_ROLES_ibfk_1` FOREIGN KEY (`ROLE_ID`) REFERENCES `ROLES` (`ROLE_ID`) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

CREATE TABLE IF NOT EXISTS `SESSIONS` (
  `SESSION_ID` char(36) NOT NULL DEFAULT (uuid()),
  `USER_ID` varchar(255) NOT NULL,
  `TENANT_ID` varchar(255) NOT NULL,
  `CREATED_AT` timestamp NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (`SESSION_ID`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

CREATE TABLE IF NOT EXISTS `DATA_MAINTENANCE` (
  `action` varchar(255) NOT NULL,
  `time` varchar(255) NOT NULL,
  `status` varchar(255) NOT NULL,
  `message` varchar(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci DEFAULT NULL,
  `maintenance_id` int NOT NULL AUTO_INCREMENT,
  PRIMARY KEY (`maintenance_id`)
) ENGINE=InnoDB AUTO_INCREMENT=6 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;


DROP PROCEDURE IF EXISTS `analytics_spog`.`GetAssetTreeList`;
DROP PROCEDURE IF EXISTS `analytics_spog`.`GetAssetsDiscovered`;
DROP PROCEDURE IF EXISTS `analytics_spog`.`GetTrafficeDistribution`;
DROP PROCEDURE IF EXISTS `analytics_spog`.`GetNetworkErrors`;
DROP PROCEDURE IF EXISTS `analytics_spog`.`GetAnomaliesDetected`;
DROP PROCEDURE IF EXISTS `analytics_spog`.`GetAnomaliesByAsset`;
DROP PROCEDURE IF EXISTS `analytics_spog`.`GetTopologyGraph`;
DROP PROCEDURE IF EXISTS `analytics_spog`.`GetProtocolGraphDistribution`;
DROP PROCEDURE IF EXISTS `analytics_spog`.`GetOneMetricValues`;
DROP PROCEDURE IF EXISTS `analytics_spog`.`GetOneMetricGraph`;
DROP PROCEDURE IF EXISTS `analytics_spog`.`GetTrafficDetails`;
DROP PROCEDURE IF EXISTS `analytics_spog`.`GetPacketPerSec`;
DROP PROCEDURE IF EXISTS `analytics_spog`.`GetAvgPacketSize`;
DROP PROCEDURE IF EXISTS `analytics_spog`.`GetPacketCounts`;
DROP PROCEDURE IF EXISTS `analytics_spog`.`GetAddressCounts`;
DROP PROCEDURE IF EXISTS `analytics_spog`.`GetNetworkConversation`;
DROP PROCEDURE IF EXISTS `analytics_spog`.`GetThroughput`;
DROP PROCEDURE IF EXISTS `analytics_spog`.`GetTCPFlags`;
DROP PROCEDURE IF EXISTS `analytics_spog`.`GetHttpDNS`;
DROP PROCEDURE IF EXISTS `analytics_spog`.`GetAnomaliesDetailedList`;
DROP PROCEDURE IF EXISTS `analytics_spog`.`GetAlertDetails`;
DROP PROCEDURE IF EXISTS `analytics_spog`.`GetAnomaliesCount`;
DROP PROCEDURE IF EXISTS `analytics_spog`.`GetAnomaliesTrend`;
DROP PROCEDURE IF EXISTS `analytics_spog`.`GetActivePassiveSubnetFilterData`;
DROP PROCEDURE IF EXISTS `analytics_spog`.`GetTrafficDetailUnicast`;
DROP PROCEDURE IF EXISTS `analytics_spog`.`GetTrafficCountUnicast`;
DROP PROCEDURE IF EXISTS `analytics_spog`.`GetTrafficCountMulticast`;
DROP PROCEDURE IF EXISTS `analytics_spog`.`GetTrafficDetailMulticast`;
DROP PROCEDURE IF EXISTS `analytics_spog`.`GetTrafficCountBroadcast`;
DROP PROCEDURE IF EXISTS `analytics_spog`.`GetTrafficDetailBroadcast`;



DELIMITER $$

CREATE PROCEDURE `analytics_spog`.`GetTrafficDetailUnicast`(
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
                        and     MEASURE_START_DATE BETWEEN ', fromTime, ' AND ', toTime, IF(assetList IS NOT NULL AND assetList != '''', CONCAT(' AND (SRC_ETH_ADDRESS IN (', assetList, ') OR DST_ETH_ADDRESS IN (', assetList, '))'),''), ' ORDER BY MEASURE_START_DATE DESC LIMIT ',pageNum, ' OFFSET ',pageSize,''  
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
        ORDER BY MEASURE_START_DATE DESC  LIMIT ',pageNum, ' OFFSET ',pageSize,'
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
                where row_num = 1 ORDER BY MEASURE_START_DATE DESC  LIMIT ',pageNum, ' OFFSET ',pageSize,''  );

    
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
            ) final ) unicast  where Record_Indicator = 1 ORDER BY Time DESC LIMIT ',pageNum, ' OFFSET ',pageSize,'' 
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

END $$

CREATE PROCEDURE `analytics_spog`.`GetTrafficDetailMulticast`(
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

END $$

CREATE PROCEDURE `analytics_spog`.`GetTrafficCountMulticast`(
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
                ORDER BY MEASURE_START_DATE DESC LIMIT ',pageNum,' OFFSET ',pageSize,'       
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
            ) final ) multicast  where Record_Indicator = 1 ORDER BY Time DESC LIMIT ',pageNum, ' OFFSET ',pageSize,'' 
                         );



        PREPARE stmt FROM @detailed_statement_udp;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;



        PREPARE stmt FROM @detailed_statement_ipnip;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;

END $$

CREATE PROCEDURE `analytics_spog`.`GetTrafficDetailBroadcast`(
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
                        and     MEASURE_START_DATE BETWEEN ', fromTime, ' AND ', toTime, IF(assetList IS NOT NULL AND assetList != '''', CONCAT(' AND (SRC_ETH_ADDRESS IN (', assetList, ') OR DST_ETH_ADDRESS IN (', assetList, '))'),''),' ORDER BY MEASURE_START_DATE DESC LIMIT ',pageNum, ' OFFSET ',pageSize,''   
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
      ORDER BY MEASURE_START_DATE DESC LIMIT ',pageNum, ' OFFSET ',pageSize,'
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
            ) final ) broadcast  where Record_Indicator = 1 ORDER BY Time DESC LIMIT ',pageNum, ' OFFSET ',pageSize,''  
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

END $$


CREATE PROCEDURE `analytics_spog`.`GetTrafficCountBroadcast`(
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


END $$




CREATE PROCEDURE `analytics_spog`.`GetTrafficCountUnicast`(
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


END $$




CREATE PROCEDURE `analytics_spog`.`GetActivePassiveSubnetFilterData`(
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

END $$

CREATE PROCEDURE `analytics_spog`.`GetAddressCounts`(
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
                SELECT MEASURE_START_DATE, SRC_ADDRESS as address FROM analytics.TELEMETRY_HEADER PARTITION (', Device, ') 
                WHERE MEASURE_START_DATE BETWEEN ', fromTime, ' AND ', toTime, 
                IF(IP IS NOT NULL AND IP != '''', CONCAT(' AND SRC_ADDRESS = ''', IP, ''''), ''),
                IF(assetList IS NOT NULL AND assetList != '''', CONCAT(' AND (SRC_ETH_ADDRESS IN (', assetList, ') OR DST_ETH_ADDRESS IN (', assetList, '))'),''),
                ' 
                UNION ALL 
                SELECT MEASURE_START_DATE, DST_ADDRESS as address FROM analytics.TELEMETRY_HEADER PARTITION (', Device, ') 
                WHERE MEASURE_START_DATE BETWEEN ', fromTime, ' AND ', toTime, 
                IF(IP IS NOT NULL AND IP != '''', CONCAT(' AND DST_ADDRESS = ''', IP, ''''), ''),
                IF(assetList IS NOT NULL AND assetList != '''', CONCAT(' AND (SRC_ETH_ADDRESS IN (', assetList, ') OR DST_ETH_ADDRESS IN (', assetList, '))'),''),
            ') AS CombinedAddresses
        ) AS Data 
        GROUP BY time
                ORDER BY time
                ;');

    PREPARE stmt FROM @sqlQuery;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;
END $$

CREATE PROCEDURE `analytics_spog`.`GetAlertDetails`(
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
END $$

CREATE PROCEDURE `analytics_spog`.`GetAnomaliesCount`(
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
END $$

CREATE PROCEDURE `analytics_spog`.`GetAnomaliesByAsset`(
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
END $$

CREATE PROCEDURE `analytics_spog`.`GetAnomaliesDetected`(
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
END $$



CREATE PROCEDURE `analytics_spog`.`GetAnomaliesTrend`(
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
        ORDER BY time
		;');
    
    SET @count_query = CONCAT('SELECT COUNT(*) AS `Anomalies Detected` FROM analytics.ANOMALY_MEASURE_HEADER PARTITION (', Device, ') ',
                      'WHERE ANOMALY_START_DATE BETWEEN ', fromTime, ' AND ', toTime, IF(assetList IS NOT NULL AND assetList != '''', CONCAT(' AND (SRC_ETH_ADDRESS IN (', assetList, ') OR DST_ETH_ADDRESS IN (', assetList, '))'),''), 
                      ' AND ANOMALY_ID != "ML"');

    PREPARE stmt FROM @sqlQuery;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;

    PREPARE count_stmt FROM @count_query;
    EXECUTE count_stmt;
    DEALLOCATE PREPARE count_stmt;

END $$

CREATE PROCEDURE `analytics_spog`.`GetAssetsDiscovered`(
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
END $$

CREATE PROCEDURE `analytics_spog`.`GetAvgPacketSize`(
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
END $$

CREATE PROCEDURE `analytics_spog`.`GetAnomaliesDetailedList`(
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
        SRC_ETH_ADDRESS,
        SRC_PORT,
        DST_ADDRESS,
        DST_ETH_ADDRESS,
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
END $$

CREATE PROCEDURE `analytics_spog`.`GetHttpDNS`(
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
          ORDER BY time
	   ;');

    PREPARE stmt FROM @sqlQuery;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;
END $$



CREATE PROCEDURE `analytics_spog`.`GetNetworkConversation`(
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
          ORDER BY time
	   ;');

    PREPARE stmt FROM @sqlQuery;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;
END $$

CREATE PROCEDURE `analytics_spog`.`GetNetworkErrors`(
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
END $$

CREATE PROCEDURE `analytics_spog`.`GetPacketCounts`(
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
          ORDER BY time
		;');

    PREPARE stmt FROM @sqlQuery;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;
END $$

CREATE PROCEDURE `analytics_spog`.`GetPacketPerSec`(
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
        ORDER BY time
	   ;');

    PREPARE stmt FROM @sqlQuery;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;
END $$



CREATE PROCEDURE `analytics_spog`.`GetTCPFlags`(
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
          ORDER BY time
	   ;');

    PREPARE stmt FROM @sqlQuery;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;
END $$

CREATE PROCEDURE `analytics_spog`.`GetProtocolGraphDistribution`(
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
        ' GROUP BY time
          ORDER BY time
	   ; ');

    PREPARE stmt FROM @sql;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;
END $$

CREATE PROCEDURE `analytics_spog`.`GetThroughput`(
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
                    WHEN 
                                            PROTOCOL = ''ARP''  AND DST_ETH_ADDRESS != ''ff:ff:ff:ff:ff:ff''  
                                                THEN 1
                                        WHEN
                                                PROTOCOL = ''IP_NIP''  
                                                AND CONV(SUBSTRING(H.DST_ETH_ADDRESS, 1, 2), 16, 10) & 1 = 0
                                                AND NOT (CONV(SUBSTRING_INDEX(H.DST_ADDRESS, ''.'', 1), 10, 2) LIKE ''1110%'') 
                                                AND H.DST_ADDRESS != ''255.255.255.255''
                                                THEN 1
                                        WHEN
                                                PROTOCOL IN (''TCP'',''MODBUS'')
                                                THEN 1
                                        WHEN
                                                PROTOCOL = ''UDP''
                                                -- Not Multicast
                                                AND NOT ( LEFT(REPLACE(DST_ETH_ADDRESS, '':'', ''''), 6) = ''01005E'' AND CONV(SUBSTRING_INDEX(DST_ADDRESS, ''.'', 1), 10, 2) LIKE ''1110%'')
                                                -- Not Broadcast
                                                AND NOT ((DST_ETH_ADDRESS LIKE ''ff:ff:ff:ff:ff:ff'') OR
                                                DST_ADDRESS = ''255.255.255.255'' OR 
                                                                  (SUBSTRING(LPAD(CONV(SUBSTRING_INDEX(DST_ADDRESS, ''.'', 1),10,2), 8, ''0''), 1, 1) = ''0'' AND SUBSTRING_INDEX(DST_ADDRESS, ''.'', -3) = ''255.255.255'') OR
                                                                  (SUBSTRING(LPAD(CONV(SUBSTRING_INDEX(DST_ADDRESS, ''.'', 1),10,2), 8, ''0''), 1, 2) = ''10'' AND SUBSTRING_INDEX(DST_ADDRESS, ''.'', -2) = ''255.255'' ) OR 
                                                                  (SUBSTRING(LPAD(CONV(SUBSTRING_INDEX(DST_ADDRESS, ''.'', 1),10,2), 8, ''0''), 1, 3) = ''110'' AND SUBSTRING_INDEX(DST_ADDRESS, ''.'', -1) = ''255'')
                                                                )

                                                THEN 1
                                                ELSE
                                                        0
                                END
            ) AS Unicast,
            SUM(
                CASE 
                    WHEN (PROTOCOL = ''IP_NIP'' OR PROTOCOL = ''UDP'') 
                                        AND 
                                                LEFT(REPLACE(DST_ETH_ADDRESS, '':'', ''''), 6) = ''01005E'' AND CONV(SUBSTRING_INDEX(DST_ADDRESS, ''.'', 1), 10, 2) LIKE ''1110%''
                                                AND CONV(SUBSTRING(H.DST_ETH_ADDRESS, 1, 2), 16, 10) & 1 = 1

                                        THEN 1
                                        ELSE
                                                0
                END
            ) AS Multicast,
            SUM(
                CASE 
                                        WHEN
                                                PROTOCOL IN (''UDP'' , ''IP_NIP'') and ((DST_ETH_ADDRESS = ''ff:ff:ff:ff:ff:ff'') OR
                                                DST_ADDRESS = ''255.255.255.255'' OR 
                                          (SUBSTRING(LPAD(CONV(SUBSTRING_INDEX(DST_ADDRESS, ''.'', 1),10,2), 8, ''0''), 1, 1) = ''0'' AND SUBSTRING_INDEX(DST_ADDRESS, ''.'', -3) = ''255.255.255'') OR
                                          (SUBSTRING(LPAD(CONV(SUBSTRING_INDEX(DST_ADDRESS, ''.'', 1),10,2), 8, ''0''), 1, 2) = ''10'' AND SUBSTRING_INDEX(DST_ADDRESS, ''.'', -2) = ''255.255'' ) OR 
                                          (SUBSTRING(LPAD(CONV(SUBSTRING_INDEX(DST_ADDRESS, ''.'', 1),10,2), 8, ''0''), 1, 3) = ''110'' AND SUBSTRING_INDEX(DST_ADDRESS, ''.'', -1) = ''255''))
                                                THEN 1
                                        WHEN 
                                                PROTOCOL = ''ARP''  AND DST_ETH_ADDRESS = ''ff:ff:ff:ff:ff:ff'' 
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
        LEFT JOIN analytics.TELEMETRY PARTITION (', Device, ') AS T ON H.TELEMETRY_GUID = T.TELEMETRY_GUID AND (T.MEASURE_TYPE = ''In_Out_Local_Traffic'')
        LEFT JOIN analytics.TELEMETRY PARTITION (', Device, ') AS V ON H.TELEMETRY_GUID = V.TELEMETRY_GUID AND (V.MEASURE_TYPE = ''count_of_frames_sent'' OR V.MEASURE_TYPE = ''num_of_frames_withinwindow_per_src_dst'')
        WHERE MEASURE_START_DATE BETWEEN ', fromTime, ' AND ', toTime,
        IF(IP IS NOT NULL AND IP != '''', CONCAT(' AND SRC_ADDRESS = ''', IP, ''''), ''),
        IF(assetList IS NOT NULL AND assetList != '''', CONCAT(' AND (SRC_ETH_ADDRESS IN (', assetList, ') OR DST_ETH_ADDRESS IN (', assetList, '))'),''),
        '
                GROUP BY time
        ORDER BY time
           ;');

    PREPARE stmt FROM @sqlQuery;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;
END $$


CREATE PROCEDURE `analytics_spog`.`GetTopologyGraph`(
    IN Device VARCHAR(255),
    IN fromTime INT,
    IN toTime INT,
  --  IN asset VARCHAR(255),
    IN assetList TEXT
)
BEGIN
    SET @sqlQuery = CONCAT('
    SELECT COALESCE( SRC_ETH_ADDRESS) AS Source,
           COALESCE( DST_ETH_ADDRESS) AS Destination,
           COUNT(*) as flow_count
    FROM analytics.TELEMETRY_HEADER PARTITION (', Device, ') AS H
    WHERE MEASURE_START_DATE BETWEEN ', fromTime, ' AND ', toTime,
  --  IF(asset IS NOT NULL AND asset != '', CONCAT(' AND (COALESCE(SRC_ADDRESS, SRC_ETH_ADDRESS) = ''', asset, ''' OR COALESCE(DST_ADDRESS, DST_ETH_ADDRESS) = ''', asset, ''')'),''),
    IF(assetList IS NOT NULL AND assetList != '''', CONCAT(' AND (SRC_ETH_ADDRESS IN (', assetList, ') OR DST_ETH_ADDRESS IN (', assetList, '))'),''),
    '
    GROUP BY Source, Destination
    ORDER BY MEASURE_START_DATE
    LIMIT 50
    ;');

    PREPARE stmt FROM @sqlQuery;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;
END $$

CREATE PROCEDURE `analytics_spog`.`GetTrafficDetails`(
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
END $$

CREATE PROCEDURE `analytics_spog`.`GetTrafficeDistribution`(
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
END $$







CREATE PROCEDURE `analytics_spog`.`GetOneMetricValues`(
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
select ADMIN_STATUS,
COALESCE(
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
      P1.ASSET_MAC AS ASSET_MAC,
      P1.PORT_NUMBER AS PORT_NUMBER,
      P1.OPERATION_STATUS AS OPERATION_STATUS,
      P1.ADMIN_STATUS AS ADMIN_STATUS,
                    coalesce(lag (P1.OPERATION_STATUS) OVER (PARTITION BY P1.ASSET_MAC,P1.PORT_NUMBER ORDER BY  P1.MEASURE_DATE),P1.OPERATION_STATUS) AS OPERATION_STATUS_PREV,
                    P1.MEASURE_DATE AS MEASURE_DATE,
                    coalesce(lag (P1.MEASURE_DATE) OVER (PARTITION BY P1.ASSET_MAC,P1.PORT_NUMBER ORDER BY  P1.MEASURE_DATE),P1.MEASURE_DATE) AS MEASURE_DATE_PREV,
                    coalesce(lead (P1.MEASURE_DATE) OVER (PARTITION BY P1.ASSET_MAC,P1.PORT_NUMBER ORDER BY  P1.MEASURE_DATE),P1.MEASURE_DATE) AS MEASURE_DATE_FUTURE,
                    P1.NEGOTIATED_BPS AS NEGOTIATED_BPS,
                    coalesce(lag (P1.NEGOTIATED_BPS) OVER (PARTITION BY P1.ASSET_MAC,P1.PORT_NUMBER ORDER BY  P1.MEASURE_DATE),P1.NEGOTIATED_BPS) AS NEGOTIATED_BPS_PREV,
                    P1.RECEIVED_BPS AS RECEIVED_BPS,
                    coalesce(lag (P1.RECEIVED_BPS) OVER (PARTITION BY P1.ASSET_MAC,P1.PORT_NUMBER ORDER BY  P1.MEASURE_DATE),P1.RECEIVED_BPS) AS RECEIVED_BPS_PREV,
                    P1.CRC_ERROR_COUNT AS CRC_ERROR_COUNT,
                    coalesce(lag (P1.CRC_ERROR_COUNT) OVER (PARTITION BY P1.ASSET_MAC,P1.PORT_NUMBER ORDER BY  P1.MEASURE_DATE),P1.CRC_ERROR_COUNT) AS CRC_ERROR_COUNT_PREV,
                    P1.COLLISION_ERROR_COUNT AS COLLISION_ERROR_COUNT,
                    coalesce(lag (P1.COLLISION_ERROR_COUNT) OVER (PARTITION BY P1.ASSET_MAC,P1.PORT_NUMBER ORDER BY  P1.MEASURE_DATE),P1.COLLISION_ERROR_COUNT) AS COLLISION_ERROR_COUNT_PREV,
                    P1.TOTAL_PACKETS_RECEIVED AS TOTAL_PACKETS_RECEIVED,
                    coalesce(lag (P1.TOTAL_PACKETS_RECEIVED) OVER (PARTITION BY P1.ASSET_MAC, P1.PORT_NUMBER ORDER BY  P1.MEASURE_DATE), P1.TOTAL_PACKETS_RECEIVED) AS TOTAL_PACKETS_RECEIVED_PREV,
      ROW_NUMBER() OVER (PARTITION BY ASSET_MAC , PORT_NUMBER ORDER BY MEASURE_DATE DESC ) as row_num
    from
      (
        select
          analytics.ACTIVE_TELEMETRY.ASSET_MAC AS ASSET_MAC,
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
                                CONCAT('CONCAT(ASSET_MAC, '':'', PORT_NUMBER) IN (', portList, ') OR '), 
                                ''
                            ),
                            IF(assetList IS NOT NULL AND assetList != '''', 
                                CONCAT('ASSET_MAC IN (', assetList, ') OR '), 
                                ''
                            ),
                            IF(subnetList IS NOT NULL AND subnetList != '''', 
                                CONCAT('SUBSTRING_INDEX(ASSET_ID,''.'',3) IN (', subnetList, ')'), 
                                '1=0'
                            ),

                            IF(subnetList IS NULL AND assetList IS NULL AND portList IS NULL, '1=1', ''),
                        ')
        group by
          analytics.ACTIVE_TELEMETRY.ASSET_MAC,
          analytics.ACTIVE_TELEMETRY.PORT_NUMBER,
          analytics.ACTIVE_TELEMETRY.MEASURE_DATE
      ) P1
     -- WHERE ADMIN_STATUS = ''up''
  ) P2
  WHERE row_num = 1
) P3
WHERE ADMIN_STATUS = ''up'' 
'
    );

PREPARE stmt
FROM
    @sqlQuery;

EXECUTE stmt;

DEALLOCATE PREPARE stmt;

-- CALL `analytics_spog`.`GetOneMetricValues`(0,999999999999,NULL,NULL,NULL, '00_90_0b_8e_93_73');
END $$

CREATE PROCEDURE `analytics_spog`.`GetOneMetricGraph`(
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
    MEASURE_START_DATE DIV 60 * 60 AS time,
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
            ASSET_MAC
    FROM
        (
        select
  P2.ASSET_MAC AS ASSET_MAC,
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
      P1.ASSET_MAC AS ASSET_MAC,
      P1.PORT_NUMBER AS PORT_NUMBER,
      P1.OPERATION_STATUS AS OPERATION_STATUS,
      P1.ADMIN_STATUS AS ADMIN_STATUS,
                    coalesce(
                    lag (P1.OPERATION_STATUS) OVER (
                    PARTITION BY P1.ASSET_MAC,
                              P1.PORT_NUMBER
                    ORDER BY  P1.MEASURE_DATE
                    ),
                    P1.OPERATION_STATUS
                    ) AS OPERATION_STATUS_PREV,
                    P1.MEASURE_DATE AS MEASURE_DATE,
                    coalesce(
                    lag (P1.MEASURE_DATE) OVER (
                    PARTITION BY P1.ASSET_MAC,
                              P1.PORT_NUMBER
                    ORDER BY  P1.MEASURE_DATE
                    ),
                    P1.MEASURE_DATE
                    ) AS MEASURE_DATE_PREV,
                    coalesce(
                    lead (P1.MEASURE_DATE) OVER (
                    PARTITION BY P1.ASSET_MAC,
                              P1.PORT_NUMBER
                    ORDER BY  P1.MEASURE_DATE
                    ),
                    P1.MEASURE_DATE
                    ) AS MEASURE_DATE_FUTURE,
                    P1.NEGOTIATED_BPS AS NEGOTIATED_BPS,
                    coalesce(
                    lag (P1.NEGOTIATED_BPS) OVER (
                    PARTITION BY P1.ASSET_MAC,
                              P1.PORT_NUMBER
                    ORDER BY  P1.MEASURE_DATE
                    ),
                    P1.NEGOTIATED_BPS
                    ) AS NEGOTIATED_BPS_PREV,
                    P1.RECEIVED_BPS AS RECEIVED_BPS,
                    coalesce(
                    lag (P1.RECEIVED_BPS) OVER (
                    PARTITION BY P1.ASSET_MAC,
                              P1.PORT_NUMBER
                    ORDER BY  P1.MEASURE_DATE
                    ),
                    P1.RECEIVED_BPS
                    ) AS RECEIVED_BPS_PREV,
                    P1.CRC_ERROR_COUNT AS CRC_ERROR_COUNT,
                    coalesce(
                    lag (P1.CRC_ERROR_COUNT) OVER (
                    PARTITION BY P1.ASSET_MAC,
                              P1.PORT_NUMBER
                    ORDER BY  P1.MEASURE_DATE
                    ),
                    P1.CRC_ERROR_COUNT
                    ) AS CRC_ERROR_COUNT_PREV,
                    P1.COLLISION_ERROR_COUNT AS COLLISION_ERROR_COUNT,
                    coalesce(
                    lag (P1.COLLISION_ERROR_COUNT) OVER (
                    PARTITION BY P1.ASSET_MAC,
                              P1.PORT_NUMBER
                    ORDER BY  P1.MEASURE_DATE
                    ),
                    P1.COLLISION_ERROR_COUNT
                    ) AS COLLISION_ERROR_COUNT_PREV,
                    P1.TOTAL_PACKETS_RECEIVED AS TOTAL_PACKETS_RECEIVED,
                    coalesce(
                    lag (P1.TOTAL_PACKETS_RECEIVED) OVER (
                    PARTITION BY P1.ASSET_MAC,
                              P1.PORT_NUMBER
                    ORDER BY  P1.MEASURE_DATE
                    ),
                    P1.TOTAL_PACKETS_RECEIVED
                    ) AS TOTAL_PACKETS_RECEIVED_PREV
    from
      (
        select
          analytics.ACTIVE_TELEMETRY.ASSET_MAC AS ASSET_MAC,
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
                                CONCAT('CONCAT(ASSET_MAC, '':'', PORT_NUMBER) IN (', portList, ') OR '), 
                                ''
                            ),
                            IF(assetList IS NOT NULL AND assetList != '''', 
                                CONCAT('ASSET_MAC IN (', assetList, ') OR '), 
                                ''
                            ),
                            IF(subnetList IS NOT NULL AND subnetList != '''', 
                                CONCAT('SUBSTRING_INDEX(ASSET_ID,''.'',3) IN (', subnetList, ')'), 
                                '1=0'
                            ),

                            IF(subnetList IS NULL AND assetList IS NULL AND portList IS NULL, '1=1', ''),
                        ')
        group by
          analytics.ACTIVE_TELEMETRY.ASSET_MAC,
          analytics.ACTIVE_TELEMETRY.PORT_NUMBER,
          analytics.ACTIVE_TELEMETRY.MEASURE_DATE
      ) P1
      WHERE ADMIN_STATUS = ''up''
  ) P2
        ) P3
    WHERE
        AVAILABILITY_FACTOR IS NOT NULL AND CAPACITY_FACTOR IS NOT NULL AND QUALITY_FACTOR IS NOT NULL
    GROUP BY MEASURE_DATE,ASSET_MAC
    ) P4
    GROUP BY time
    ORDER BY time
   ');

PREPARE stmt
FROM
    @sqlQuery;

EXECUTE stmt;

DEALLOCATE PREPARE stmt;

-- CALL `analytics_spog`.`GetOneMetricGraph`(0,999999999999,NULL,NULL,NULL, '00_90_0b_8e_93_73');

END $$ 

CREATE PROCEDURE `analytics_spog`.`GetAssetTreeList`(
    IN Device VARCHAR(255),
    IN fromTime INT,
    IN toTime INT
)
BEGIN
    SET @mainQuery = CONCAT('
    SELECT MAX(HOST_NAME) AS HOST_NAME, 
    -- CASE WHEN SUM(CASE WHEN HOST_NAME IS NOT NULL THEN 1 ELSE 0 END) = 1 THEN COALESCE(HOST_NAME, Device) ELSE NULL END AS HOST_NAME, 
    Device,
    CASE 
        WHEN (COALESCE(HOST_NAME, '''') = '''' AND COALESCE(Device, '''') = '''') 
        THEN GROUP_CONCAT(IP)  
        ELSE CONCAT(COALESCE(HOST_NAME, Device), 
                    CASE 
                        WHEN HOST_NAME IS NOT NULL AND HOST_NAME != '''' 
                        THEN CONCAT(''('', Device, '')'') 
                        ELSE '''' 
                    END) 
    END AS ASSET,
    GROUP_CONCAT(CASE WHEN IP != '''' THEN CONCAT("''", IP, "''") ELSE NULL END) AS IP
    FROM (
        SELECT ASSET_ID AS IP,
               ASSET_ETH_ADDRESS AS Device,
               HOSTNAME as HOST_NAME
        FROM analytics.SUBNET_ASSET_DETAILS AS H
        JOIN analytics.SUBNET_DETAILS as T
            ON H.SUBNET_ID = T.SUBNET_ID
            AND DEVICE_ID = REPLACE(''', Device, ''', ''_'', '':'')
        WHERE ETL_TIME BETWEEN ', fromTime, ' AND ', toTime, '
        UNION
        SELECT IP, Device, HOST_NAME
        FROM (
            SELECT IP, Device, HOST_NAME,
                   ROW_NUMBER() OVER (PARTITION BY Device ORDER BY CASE WHEN IP IS NULL THEN 1 ELSE 0 END, IP) as rn
            FROM (
                SELECT SRC_ADDRESS AS IP,
                       SRC_ETH_ADDRESS AS Device,
                       NULL as HOST_NAME
                FROM analytics.TELEMETRY_HEADER PARTITION (', Device, ')  
                WHERE MEASURE_START_DATE BETWEEN ', fromTime, ' AND ', toTime, '
                    AND NOT (PROTOCOL IN ( ''UDP'',''IP_NIP'') AND SRC_ADDRESS = ''0.0.0.0'')
                    AND NOT (SRC_ETH_ADDRESS = ''ff:ff:ff:ff:ff:ff'')
                UNION
                SELECT DST_ADDRESS AS IP,
                       DST_ETH_ADDRESS AS Device,
                       NULL as HOST_NAME
                FROM analytics.TELEMETRY_HEADER PARTITION (', Device, ')  
                WHERE MEASURE_START_DATE BETWEEN ', fromTime, ' AND ', toTime, '
                AND NOT ((DST_ETH_ADDRESS = ''ff:ff:ff:ff:ff:ff'')
                    -- AND NOT (PROTOCOL IN (''UDP'',''ARP'') AND  (DST_ETH_ADDRESS LIKE ''ff:ff:ff:ff:ff:ff'') 
                    OR
                    DST_ADDRESS = ''255.255.255.255'' OR
                    (SUBSTRING(LPAD(CONV(SUBSTRING_INDEX(DST_ADDRESS, ''.'', 1),10,2), 8, ''0''), 1, 1) = ''0'' AND SUBSTRING_INDEX(DST_ADDRESS, ''.'', -3) = ''255.255.255'') OR
                    (SUBSTRING(LPAD(CONV(SUBSTRING_INDEX(DST_ADDRESS, ''.'', 1),10,2), 8, ''0''), 1, 2) = ''10'' AND SUBSTRING_INDEX(DST_ADDRESS, ''.'', -2) = ''255.255'' ) OR
                    (SUBSTRING(LPAD(CONV(SUBSTRING_INDEX(DST_ADDRESS, ''.'', 1),10,2), 8, ''0''), 1, 3) = ''110'' AND SUBSTRING_INDEX(DST_ADDRESS, ''.'', -1) = ''255''))
                    AND NOT (PROTOCOL = ''IP_NIP''  
                    AND LEFT(REPLACE(DST_ETH_ADDRESS, '':'', ''''), 6) = ''01005E''
                    AND CONV(SUBSTRING_INDEX(DST_ADDRESS, ''.'', 1), 10, 2) LIKE ''1110%'')
                    AND NOT (
                    PROTOCOL = ''UDP'' and LEFT(REPLACE(DST_ETH_ADDRESS, '':'', ''''), 6) = ''01005E'' AND CONV(SUBSTRING_INDEX(DST_ADDRESS, ''.'', 1), 10, 2) LIKE ''1110%''
                    )
                    AND NOT (PROTOCOL IN ( ''IP_NIP'') AND DST_ADDRESS = ''0.0.0.0'')
            ) as src
        ) ranked
        WHERE rn = 1
    ) all_assets
    WHERE NOT ((HOST_NAME IS NULL OR HOST_NAME='''') AND (Device IS NULL OR Device='''') )
    GROUP BY COALESCE(HOST_NAME,Device),Device
    Limit 1000;
    ');
    
    PREPARE mainStmt FROM @mainQuery;
    EXECUTE mainStmt;
    DEALLOCATE PREPARE mainStmt;
END $$