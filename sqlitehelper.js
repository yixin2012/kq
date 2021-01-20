"use strict";

var fs = require("fs");
var sqlite3 = require("sqlite3");
var Promise = require("bluebird");

const querystring = require('querystring');
var db;

var UUID = require('uuid');
const batchid = UUID.v1();

let _CLOCK_TIME = [];

var getConn = function (clocks) {
  return new Promise(function (resolve, reject) {
    fs.readFile("conn.txt", "utf-8", function (err, data) {
      if (err) {
        reject(err);
      } else {
        console.log("db: ".concat(data));
        resolve(data.trim());
        _CLOCK_TIME = clocks;
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

var createSchema = function () {
  return new Promise(function (resolve, reject) {
    db.serialize(function () {
      var createEpmloyeeTable = "CREATE TABLE IF NOT EXISTS kq_employee ('department'  NVARCHAR(20),'employee_id' NVARCHAR(15), 'employee_name'  NVARCHAR(50), 'inq_start_t' NVARCHAR(20), 'inq_end_t' NVARCHAR(20), 'create_t' NVARCHAR(20))";
      var createClockTable = "CREATE TABLE IF NOT EXISTS kq_clock_time ('batchid' NVARCHAR(50), 'department'  NVARCHAR(20), 'employee_id' NVARCHAR(15), 'employee_name' NVARCHAR(50), 'clock_time' DATETIME)";
      var createReportTable = "CREATE TABLE IF NOT EXISTS kq_clock_report ('batchid' NVARCHAR(50), 'department' NVARCHAR(20), 'employee_id' NVARCHAR(15), 'employee_name' NVARCHAR(50), 'clock_date' NVARCHAR(20), 'clock_time_start' NVARCHAR(20), 'clock_time_end' NVARCHAR(20), 'work_hour' INT, 'status' NVARCHAR(20),'create_t' NVARCHAR(20))";
      db.exec(createEpmloyeeTable, function (err) {
        if (err) {
          reject(err);
        } else {
          console.log("create table kq_employee");
        }
      });

      db.exec(createClockTable, function (err) {
        if (err) {
          reject(err);
        } else {
          console.log("create table kq_clock_time");
        }
      });

      db.exec(createReportTable, function (err, data) {
        if (err) {
          reject(err);
        } else {
          console.log("create table kq_clock_report");
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
                //console.log("成功插入1笔记录");
              }
            });
          });
          console.log("init data");
          resolve(batchid);
        }
      });
    });
  });
}

var calculateClockData = function (batchid) {
  return new Promise(function (resolve, reject) {
    db.serialize(function () {
      //删掉相同批号的数据
      var deleteData = `DELETE FROM kq_clock_report WHERE batchid='${batchid}'`;
      //lock_date分组，clock_time排序，每组取第一个作为clock_time_start
      var insertStartData = `
        INSERT INTO kq_clock_report(batchid,department,employee_id,employee_name,clock_date,clock_time_start,Create_t)
        SELECT batchid,department,employee_id,employee_name,clock_date,clock_time,DATETIME() FROM(
          SELECT SUBSTR(clock_time,0,INSTR(clock_time,' ')) AS clock_date,batchid,department,employee_id,employee_name,clock_time
            ,ROW_NUMBER() OVER(PARTITION BY SUBSTR(clock_time,0,INSTR(clock_time,' ')),batchid,department,employee_id,employee_name ORDER BY clock_time) AS RN
          FROM [kq_clock_time]
          WHERE batchid='${batchid}'
        ) T
        WHERE RN=1      
      `;
      //clock_date分组，clock_time排序，每组取最后一个作为clock_time_end
      var updateEndData = `
        UPDATE kq_clock_report 
        SET clock_time_end = (
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
      var updateWorkHour=`
        UPDATE kq_clock_report
        SET work_hour=CASE WHEN clock_time_start IS NOT NULL AND clock_time_end IS NOT NULL
                    THEN --datediff(hour, clock_time_start,clock_time_end)
                    (julianday( clock_time_end )-   julianday(strftime('%Y-%m-%d %H:%M',clock_time_start)))*24 
                  ELSE 0
              END
        WHERE BatchID='${batchid}' 
      `;
      //更新Status
      var updateStatus=`
        UPDATE kq_clock_report
        SET Status=CASE WHEN clock_time_start IS NULL AND clock_time_end IS NULL THEN '請假'
                WHEN clock_time_start IS NULL OR clock_time_end IS NULL OR (clock_time_start = clock_time_end) THEN '只刷一次'
                WHEN clock_time_start IS NOT NULL AND clock_time_end IS NOT NULL
                  THEN (
                    CASE WHEN CAST(clock_time_start AS DATETIME) > CAST((clock_date+' 08:50:59') AS DATETIME) THEN '遲到' 
                      WHEN CAST(clock_time_end AS DATETIME) > CAST((clock_date+' 08:50:59') AS DATETIME) 
                        AND CAST(clock_time_end AS DATETIME) < CAST((clock_date+' 16:50:59') AS DATETIME) 
                        AND clock_time_start <> clock_time_end THEN '早退'
                      WHEN work_hour < 9 THEN '工時不足'
                      ELSE '正常'
                    END
                  )
                ELSE '正常'
              END
        WHERE BatchID='${batchid}'        
      `;

      db.run(deleteData, function (err) {
        if (err) {
          reject(err);
        } else {
          console.log("delete report data");
        }
      });

      db.run(insertStartData, function (err) {
        if (err) {
          reject(err);
        } else {
          console.log("insert start data");
        }
      });

      db.run(updateEndData, function (err) {
        if (err) {
          reject(err);
        } else {
          console.log("update end data");
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

var getClockReport = function (batchid) {
  return new Promise(function (resolve, reject) {
    db.all(`SELECT * FROM kq_clock_report where batchid='${batchid}' ORDER BY department,employee_id,CAST(clock_time_start AS DATETIME)`, function (err, data) {
      if (err) {
        reject(err);
      } else {
        console.log("getClockReport");
        resolve(data);
      }
    });
  });
}

var showClockReport = function (employee) {
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
  getClockReport,
  showClockReport
}