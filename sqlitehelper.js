"use strict";

var fs = require("fs");
var sqlite3 = require("sqlite3");
var Promise = require("bluebird");
const Json2csvParser = require('json2csv').Parser;
const querystring = require('querystring');
var db;
var UUID = require('uuid');
const batchid = UUID.v1();
let _CLOCK_TIME = [];
let _EMPLOYEES = [];

var getConn = function (clocks, employees) {
  return new Promise(function (resolve, reject) {
    fs.readFile("conn.txt", "utf-8", function (err, data) {
      if (err) {
        reject(err);
      } else {
        console.log("db: ".concat(data));
        resolve(data.trim());
        _CLOCK_TIME = clocks;
        _EMPLOYEES = employees;
      }
    });
  });
}

var openDb = function (dbConn) {
  return new Promise(function (resolve, reject) {
    db = new sqlite3.Database(dbConn, function (err) {
      if (err) {
        reject(err);
      } else {
        console.log("open database");
        resolve();
      }
    });
  });
}

let getDayOfWeek = function (theDate) {
  let dayOfWeek = new Date(theDate).getDay();
  switch (dayOfWeek) {
    case 0:
      dayOfWeek = 'Sunday';
      break;
    case 1:
      dayOfWeek = 'Monday';
      break;
    case 2:
      dayOfWeek = 'Tuesday';
      break;
    case 3:
      dayOfWeek = 'Wednesday';
      break;
    case 4:
      dayOfWeek = 'Thursday';
      break;
    case 5:
      dayOfWeek = 'Friday';
      break;
    case 6:
      dayOfWeek = 'Saturday';
      break;
    default:
      break;
  }
  return dayOfWeek;
}

var createSchema = function () {
  return new Promise(function (resolve, reject) {
    db.serialize(function () {
      var dropEpmloyeeTable = "DROP TABLE IF EXISTS kq_employee"
      var createEpmloyeeTable = "CREATE TABLE IF NOT EXISTS kq_employee ('batchid' NVARCHAR(50), 'department' NVARCHAR(20), 'employee_id' NVARCHAR(15), 'employee_name' NVARCHAR(50), 'clock_date' DATE, 'day_of_week' NVARCHAR(20))";
      var createClockTable = "CREATE TABLE IF NOT EXISTS kq_clock_time ('batchid' NVARCHAR(50), 'department'  NVARCHAR(20), 'employee_id' NVARCHAR(15), 'employee_name' NVARCHAR(50), 'clock_time' DATETIME)";
      var createReportTable = "CREATE TABLE IF NOT EXISTS kq_clock_report ('batchid' NVARCHAR(50), 'department' NVARCHAR(20), 'employee_id' NVARCHAR(15), 'employee_name' NVARCHAR(50), 'clock_date' DATE, 'day_of_week' NVARCHAR(20), 'clock_in_t' DATETIME, 'clock_out_t' DATETIME, 'work_hour' INT, 'status' NVARCHAR(20),'stipulate_in_t' DATETIME,'stipulate_out_t' DATETIME,'create_t' DATETIME)";

      db.exec(dropEpmloyeeTable, function (err, data) {
        if (err) {
          reject(err);
        } else {
          console.log("drop table kq_employee");
        }
      });

      db.exec(createEpmloyeeTable, function (err) {
        if (err) {
          reject(err);
        } else {
          console.log("create table kq_employee");
          _EMPLOYEES.forEach(function (employee) {
            let rex = new RegExp('<data>(.*?)</data><data>(.*?)</data><data>(.*?)</data><data>(.*?)</data>', 'g'); // NOTE: 'g' is important
            let m = rex.exec(employee);
            if (m) {
              db.run("insert into kq_employee(batchid,department,employee_id,employee_name,clock_date,day_of_week) VALUES ( $batchid,  $department,  $employee_id,  $employee_name, $clock_date, $day_of_week)", {
                $batchid: batchid,
                $department: m[1],
                $employee_id: m[2],
                $employee_name: m[3],
                $clock_date: m[4],
                $day_of_week: getDayOfWeek(m[4])
              }, function (err, data) {
                if (err) {
                  reject(err);
                } else {
                  //console.log("成功插入1笔kq_employee记录");
                }
              });
            }
          });
          console.log("init kq_employee data");
        }
      });

      db.exec(createClockTable, function (err) {
        if (err) {
          reject(err);
        } else {
          console.log("create table kq_clock_time");
          _CLOCK_TIME.forEach(function (clock) {
            db.run("insert into kq_clock_time(batchid,department,employee_id,employee_name,clock_time) VALUES ($batchid, $department, $employee_id, $employee_name, $clock_time)", {
              $batchid: batchid,
              $department: clock.department,
              $employee_id: clock.employee_id,
              $employee_name: clock.name,
              $clock_time: clock.clock_time
            }, function (err, data) {
              if (err) {
                reject(err);
              } else {
                //console.log("成功插入1笔kq_clock_time记录");
              }
            });
          });
          console.log("init kq_clock_time data");
        }
      });

      db.exec(createReportTable, function (err, data) {
        if (err) {
          reject(err);
        } else {
          console.log("create table kq_clock_report");
          db.run(`insert into kq_clock_report (batchid,department,employee_id,employee_name,clock_date,day_of_week,stipulate_in_t,stipulate_out_t,create_t)	
                  select batchid,department,employee_id,employee_name,clock_date,day_of_week,strftime('%Y-%m-%d 08:50:59',clock_date,'localtime'),strftime('%Y-%m-%d 16:50:00',clock_date,'localtime'),datetime('now', 'localtime') from kq_employee ke WHERE batchid=$batchid`, {
            $batchid: batchid
          }, function (err, data) {
            if (err) {
              reject(err);
            } else {
              //console.log("成功插入1笔kq_clock_report记录");
            }
          });
          console.log("init kq_clock_report data");
          resolve(batchid);
        }
      });
    });
  });
}

var calculateClockData = function (batchid) {
  return new Promise(function (resolve, reject) {
    db.serialize(function () {
      //clock_date分组，clock_time排序，每组取第一个作为clock_time_start
      var insertStartData = `
        UPDATE kq_clock_report 
        SET clock_in_t = (
          SELECT clock_time FROM(
            SELECT batchid,department,employee_id,employee_name,clock_date,clock_time,datetime('now', 'localtime') FROM(
              SELECT SUBSTR(clock_time,0,INSTR(clock_time,' ')) AS clock_date,batchid,department,employee_id,employee_name,clock_time
                ,ROW_NUMBER() OVER(PARTITION BY SUBSTR(clock_time,0,INSTR(clock_time,' ')),batchid,department,employee_id,employee_name ORDER BY clock_time) AS RN
              FROM [kq_clock_time]
              WHERE batchid='${batchid}'
            ) T
            WHERE RN=1 	
          )T3
          WHERE T3.batchid=kq_clock_report.batchid AND T3.department=kq_clock_report.department 
            AND T3.employee_id=kq_clock_report.employee_id AND T3.employee_name=kq_clock_report.employee_name 
            AND T3.clock_date=kq_clock_report.clock_date
        )
        WHERE batchid='${batchid}'  
      `;
      //clock_date分组，clock_time排序，每组取最后一个作为clock_time_end
      var updateEndData = `
        UPDATE kq_clock_report 
        SET clock_out_t = (
          SELECT clock_time FROM(
            SELECT batchid,department,employee_id,employee_name,clock_date,clock_time,RN,RN_MAX
            FROM(
              SELECT 
                batchid,department,employee_id,employee_name,clock_date,clock_time,RN
                ,ROW_NUMBER() OVER(PARTITION BY clock_date,batchid,department,employee_id,employee_name ORDER BY RN DESC) AS RN_MAX
              FROM(
                SELECT SUBSTR(clock_time,0,INSTR(clock_time,' ')) AS clock_date,batchid,department,employee_id,employee_name,clock_time
                ,ROW_NUMBER() OVER(PARTITION BY SUBSTR(clock_time,0,INSTR(clock_time,' ')),batchid,department,employee_id,employee_name ORDER BY clock_time) AS RN
                FROM [kq_clock_time]
                WHERE batchid='${batchid}'
              ) T1
            ) T2
            WHERE T2.RN_MAX=1		
          )T3
          WHERE T3.batchid=kq_clock_report.batchid AND T3.department=kq_clock_report.department 
            AND T3.employee_id=kq_clock_report.employee_id AND T3.employee_name=kq_clock_report.employee_name 
            AND T3.clock_date=kq_clock_report.clock_date
        )
        WHERE batchid='${batchid}'         
      `;
      //更新work_hour
      var updateWorkHour = `
        UPDATE kq_clock_report
        SET work_hour=CASE WHEN clock_in_t IS NOT NULL AND clock_out_t IS NOT NULL
                    THEN (julianday(clock_out_t) - julianday(clock_in_t))*24 
                  ELSE 0
              END
        WHERE BatchID='${batchid}' 
      `;
      //更新Status
      var updateStatus = `
        UPDATE kq_clock_report
        SET status=CASE WHEN clock_date > create_t THEN '未發生'
                WHEN day_of_week in ('Saturday','Sunday') AND work_hour > 0 THEN '週末加班'
                WHEN day_of_week in ('Saturday','Sunday') AND work_hour = 0 THEN '週末'
                WHEN strftime('%m-%d',clock_date,'localtime')='01-01' THEN '元旦'
                WHEN strftime('%m-%d',clock_date,'localtime')='05-01' THEN '勞動節'
                WHEN strftime('%m-%d',clock_date,'localtime') in('10-01','10-02','10-03') THEN '國慶節'
                WHEN clock_in_t IS NULL AND clock_out_t IS NULL THEN '請假'
                WHEN clock_in_t IS NOT NULL AND clock_in_t > stipulate_in_t AND (clock_in_t = clock_out_t) THEN '遲到 只刷一次'
                WHEN clock_in_t IS NULL AND clock_out_t IS NOT NULL AND clock_out_t < stipulate_out_t THEN '早退 只刷一次'
                WHEN clock_in_t IS NULL OR clock_out_t IS NULL OR (clock_in_t = clock_out_t) THEN '只刷一次'
                WHEN clock_in_t IS NOT NULL AND clock_out_t IS NOT NULL
                  THEN (
                    CASE 
                      WHEN (clock_in_t > stipulate_in_t) AND (clock_out_t > stipulate_in_t AND clock_out_t < stipulate_out_t AND clock_in_t <> clock_out_t) AND (work_hour < 9) THEN '遲到 早退 工時不足' 
                      WHEN (clock_in_t > stipulate_in_t) AND (clock_out_t > stipulate_in_t AND clock_out_t < stipulate_out_t AND clock_in_t <> clock_out_t) THEN '遲到 早退'
                      WHEN (clock_in_t > stipulate_in_t) AND (work_hour < 9) THEN '遲到 工時不足'
                      WHEN (clock_out_t > stipulate_in_t AND clock_out_t < stipulate_out_t AND clock_in_t <> clock_out_t) AND (work_hour < 9) THEN '早退 工時不足'
                      WHEN clock_in_t > stipulate_in_t THEN '遲到' 
                        WHEN clock_out_t > stipulate_in_t AND clock_out_t < stipulate_out_t AND clock_in_t <> clock_out_t THEN '早退'
                        WHEN work_hour < 9 THEN '工時不足'
                        ELSE '正常'
                    END
                  )
                ELSE '正常'
              END
        WHERE BatchID='${batchid}'      
      `;

      db.run(insertStartData, function (err) {
        if (err) {
          reject(err);
        } else {
          console.log("update in data");
        }
      });

      db.run(updateEndData, function (err) {
        if (err) {
          reject(err);
        } else {
          console.log("update out data");
        }
      });

      db.run(updateWorkHour, function (err) {
        if (err) {
          reject(err);
        } else {
          console.log("update work hour");
        }
      });

      db.run(updateStatus, function (err) {
        if (err) {
          reject(err);
        } else {
          console.log("update status");
          resolve(batchid);
        }
      });
    });
  });
}

var deleteCurrentEmployeeData = function () {
  return new Promise(function (resolve, reject) {
    db.serialize(function () {
      var deleteEpmloyeeTable = "DELETE FROM kq_employee";
      db.exec(deleteEpmloyeeTable, function (err) {
        if (err) {
          reject(err);
        } else {
          console.log("delete current employee from kq_employee");
          resolve();
        }
      });
    });
  });
}

var getInqEmployeeList = function () {
  return new Promise(function (resolve, reject) {
    fs.readFile("employee.txt", "utf-8", function (err, data) {
      if (err) {
        reject(err);
      } else {
        console.log("employee list got");
        resolve(data.trim());
      }
    });
  });
}

var insertEmployee2DB = function (_CLOCK_TIME) {
  return new Promise(function (resolve, reject) {
    let employees = JSON.parse(data);
    employees.forEach(function (employee) {
      db.run("insert into kq_clock_time(batchid,department,employee_id,employee_name,clock_time) VALUES ($batchid, $department, $employee_id, $employee_name, $clock_time)", {
        $batchid: batchid,
        $department: clock.department,
        $employee_id: clock.employee_id,
        $employee_name: clock.name,
        $clock_time: clock.clock_time
      }, function (err, data) {
        if (err) {
          reject(err);
        } else {
          resolve(data);
        }
      });
    });
  });
}

var insertClockTime2DB = function (clock) {
  return new Promise(function (resolve, reject) {
    console.log(clock);
    db.run("insert into kq_clock_time(batchid,department,employee_id,name,clock_time) VALUES ($batchid, $department, $employee_id, $name, $clock_time)", {
      $batchid: batchid,
      $department: clock.department,
      $employee_id: clock.employee_id,
      $name: clock.name,
      $clock_time: clock.clock_time
    }, function (err, data) {
      if (err) {
        reject(err);
      } else {
        resolve(data);
      }
    });
  });
}

var getClockReportData = function (batchid) {
  return new Promise(function (resolve, reject) {
    db.all(`SELECT * FROM kq_clock_report where batchid='${batchid}' ORDER BY department,employee_id,clock_date`, function (err, data) {
      if (err) {
        reject(err);
      } else {
        console.log("getClockReportData");
        resolve(data);
      }
    });
  });
}

var export2CSV = function (data) {
  return new Promise(function (resolve, reject) {
    const fields = ['batchid', 'department', 'employee_id', 'employee_name', 'clock_date', 'day_of_week', 'clock_in_t', 'clock_out_t', 'work_hour', 'status', 'stipulate_in_t', 'stipulate_out_t', 'create_t'];
    const json2csvParser = new Json2csvParser({
      fields
    });
    let clockData = data;
    const csv = json2csvParser.parse(clockData);
    //console.log(csv);
    fs.writeFile("./tmp/Report.csv", `\ufeff${csv}`, 'utf-8', function (err, data) {
      if (err) {
        reject(err);
      } else {
        console.log("tmp/Report.csv was saved");
        resolve(clockData);
      }
    });
  });
}

var showClockReportData = function (employee) {
  console.log(employee);
}

module.exports = {
  getConn,
  openDb,
  createSchema,
  calculateClockData,
  deleteCurrentEmployeeData,
  getInqEmployeeList,
  insertEmployee2DB,
  insertClockTime2DB,
  getClockReportData,
  export2CSV,
  showClockReportData
}