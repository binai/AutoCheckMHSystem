查表名
select * from sqlite_master
清空表
DELETE FROM TableName;


煤耗数据巡检表
mh.sqlite

常规数据点表

临时表
每小时取一次数据，每天取24次数据。并于最后一次汇总至日表，并清除本表。
mh_temp
CREATE TABLE "mh_temp" ("name" VARCHAR, "time" time, "value" REAL NOT NULL, "site" INTEGER NOT NULL, "year" INTEGER NOT NULL "month" INTEGER NOT NULL, "day" INTEGER NOT NULL, "WeekNumber" INTEGER NOT NULL, "hour" INTEGER NOT NULL)

日表
每月最后一天汇总当月数据至月表，记录数据至月表数据表中，并清本表。
mh_day
CREATE TABLE "mh_day" ("name" VARCHAR, "date" DATE, "value" REAL NOT NULL, "site" INTEGER NOT NULL, "year" INTEGER NOT NULL "month" INTEGER NOT NULL, "day" INTEGER NOT NULL, "WeekNumber" INTEGER NOT NULL)

月表
每年最后一天汇总当年数据至年表，
mh_month
CREATE TABLE "mh_month" ("name" VARCHAR, "date" DATE, "value" REAL NOT NULL, "site" INTEGER NOT NULL, "year" INTEGER NOT NULL, "month" INTEGER NOT NULL)

月表数据
mh_month_data
CREATE TABLE "mh_month_list" ("name" VARCHAR, "value" TEXT NOT NULL, "site" INTEGER NOT NULL, "year" INTEGER NOT NULL, "month" INTEGER NOT NULL)

年表
mh_point_year
CREATE TABLE "mh_month" ("name" VARCHAR, "date" DATE, "value" REAL NOT NULL, "site" INTEGER NOT NULL, "year" INTEGER NOT NULL)


90%负荷数据
每天从web中取一次数据，得到负荷数据
mh_fh_temp
CREATE TABLE "mh_fh_temp" ("datetime" DATE, "value" REAL NOT NULL, "site" INTEGER NOT NULL,"perce" INTEGER NOT NULL, "year" INTEGER NOT NULL, "month" INTEGER NOT NULL, "day" INTEGER NOT NULL)


mh_fh_month
