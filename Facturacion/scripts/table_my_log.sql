/*
 Navicat Oracle Data Transfer

 Source Server         : CoopeGuanacaste
 Source Server Version : 102040
 Source Host           : 172.16.1.4
 Source Schema         : INFOGRAFICO

 Target Server Version : 102040
 File Encoding         : utf-8

 Date: 06/02/2015 15:20:35 PM
*/

-- ----------------------------
--  Table structure for my_log
-- ----------------------------
DROP TABLE "INFOGRAFICO"."my_log";
CREATE TABLE "my_log" (   "fecha_log" DATE, "reference_log" VARCHAR2(100BYTE), "count_log" NUMBER);

