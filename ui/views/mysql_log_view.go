package views

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"

	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
	_ "github.com/go-sql-driver/mysql"

	"fyne.io/fyne/v2"
	"fyne.io/fyne/v2/container"
	"fyne.io/fyne/v2/widget"
)

func MySQLLogView(configFile string, tableConfigFile string) fyne.CanvasObject {
	// UI ส่วนแสดง Log
	logText := widget.NewMultiLineEntry()
	logText.SetText("กำลังเชื่อมต่อ MySQL...\n")
	logText.Wrapping = fyne.TextWrapWord
	logText.Disable()

	// แสดง Log บน UI
	appendLog := func(text string) {
		logText.SetText(logText.Text + text + "\n")
	}

	// ใช้ Goroutine เพื่ออ่าน Binlog
	go func() {
		// ตั้งค่าการเชื่อมต่อ MySQL
		mysqlDSN := `root:5HAg3rWT#m@tcp(209.15.97.58:3306)/`
		db, err := sql.Open("mysql", mysqlDSN)
		if err != nil {
			appendLog(fmt.Sprintf("❌ ไม่สามารถเชื่อมต่อ MySQL: %v", err))
			return
		}
		defer db.Close()

		// ดึง Binlog ล่าสุด
		var binlogFile string
		var binlogPos uint32
		var binlogIgnored1, binlogIgnored2, binlogIgnored3 string
		err = db.QueryRow("SHOW MASTER STATUS").Scan(&binlogFile, &binlogPos, &binlogIgnored1, &binlogIgnored2, &binlogIgnored3)
		if err != nil {
			appendLog(fmt.Sprintf("❌ ไม่สามารถดึง Binlog ล่าสุด: %v", err))
			return
		}

		appendLog(fmt.Sprintf("✅ Binlog ล่าสุด: %s, Position: %d", binlogFile, binlogPos))

		// ตั้งค่าการอ่าน Binlog
		cfg := replication.BinlogSyncerConfig{
			ServerID: 100,
			Flavor:   "mysql",
			Host:     "209.15.97.58",
			Port:     3306,
			User:     "root",
			Password: "5HAg3rWT#m",
		}

		// สร้าง Binlog Syncer
		syncer := replication.NewBinlogSyncer(cfg)

		// เริ่มอ่าน Binlog จากไฟล์ล่าสุด
		streamer, err := syncer.StartSync(mysql.Position{Name: binlogFile, Pos: binlogPos})
		if err != nil {
			appendLog(fmt.Sprintf("❌ เริ่มต้น Sync Binlog ไม่สำเร็จ: %v", err))
			return
		}

		appendLog("🔄 กำลังอ่าน MySQL Binlog ...")

		// ✅ Map สำหรับเก็บโครงสร้างของตาราง
		tableMap := make(map[uint64]*replication.TableMapEvent)

		for {
			ev, err := streamer.GetEvent(context.Background())
			if err != nil {
				appendLog(fmt.Sprintf("❌ เกิดข้อผิดพลาดในการอ่าน Binlog: %v", err))
				return
			}

			switch e := ev.Event.(type) {
			case *replication.TableMapEvent:
				// บันทึก TableMapEvent
				tableMap[e.TableID] = e

			case *replication.RowsEvent:
				// ตรวจสอบว่า TableMap มีข้อมูลหรือไม่
				table, ok := tableMap[e.TableID]
				if !ok {
					appendLog("⚠️ ไม่พบข้อมูล TableMapEvent")
					continue
				}

				// ดึงชื่อ Database และ Table
				dbName := string(table.Schema)
				tableName := string(table.Table)
				fullTableName := fmt.Sprintf("`%s`.`%s`", dbName, tableName)

				// สร้าง SQL Statement ตามประเภทของ Event
				switch ev.Header.EventType {
				case replication.WRITE_ROWS_EVENTv1, replication.WRITE_ROWS_EVENTv2:
					for _, row := range e.Rows {
						sql, primaryKeyJSON := buildInsertSQL(db, dbName, tableName, row)
						appendLog(fmt.Sprintf("📌 Table: %s", fullTableName))
						appendLog(sql)
						appendLog(fmt.Sprintf("🔑 Primary Key: %s", primaryKeyJSON))
					}
				case replication.UPDATE_ROWS_EVENTv1, replication.UPDATE_ROWS_EVENTv2:
					for i := 0; i < len(e.Rows); i += 2 {
						oldRow, newRow := e.Rows[i], e.Rows[i+1]
						sql, primaryKeyJSON := buildUpdateSQL(db, dbName, tableName, oldRow, newRow)
						appendLog(fmt.Sprintf("📌 Table: %s", fullTableName))
						appendLog(sql)
						appendLog(fmt.Sprintf("🔑 Primary Key: %s", primaryKeyJSON))
					}
				case replication.DELETE_ROWS_EVENTv1, replication.DELETE_ROWS_EVENTv2:
					for _, row := range e.Rows {
						sql, primaryKeyJSON := buildDeleteSQL(db, dbName, tableName, row)
						appendLog(fmt.Sprintf("📌 Table: %s", fullTableName))
						appendLog(sql)
						appendLog(fmt.Sprintf("🔑 Primary Key: %s", primaryKeyJSON))
					}
				default:
					appendLog(fmt.Sprintf("⚠️ ไม่รู้จัก Event: %v", ev.Header.EventType))
				}
			}
		}
	}()

	// Return UI
	return container.NewVBox(
		widget.NewLabel("MySQL Binlog Viewer"),
		logText,
	)
}

// ✅ ดึง Primary Key พร้อมค่าจากตาราง
func getPrimaryKey(db *sql.DB, dbName, tableName string) ([]string, error) {
	query := fmt.Sprintf("SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = '%s' AND TABLE_NAME = '%s' AND COLUMN_KEY = 'PRI' ORDER BY ORDINAL_POSITION", dbName, tableName)
	rows, err := db.Query(query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var primaryKeys []string
	for rows.Next() {
		var columnName string
		if err := rows.Scan(&columnName); err != nil {
			return nil, err
		}
		primaryKeys = append(primaryKeys, columnName)
	}

	return primaryKeys, nil
}

// ✅ ฟังก์ชันสร้าง JSON ของ Primary Key
func buildPrimaryKeyJSON(primaryKeys []string, row []interface{}) string {
	primaryKeyMap := make(map[string]interface{})
	for i, key := range primaryKeys {
		if i < len(row) {
			primaryKeyMap[key] = row[i]
		}
	}
	jsonData, _ := json.Marshal(primaryKeyMap)
	return string(jsonData)
}

// ✅ ฟังก์ชันสร้างคำสั่ง INSERT
func buildInsertSQL(db *sql.DB, dbName, tableName string, row []interface{}) (string, string) {
	return fmt.Sprintf("🟢 INSERT INTO `%s`.`%s` VALUES (%v);", dbName, tableName, row), "{}"
}

// ✅ ฟังก์ชันสร้างคำสั่ง UPDATE โดยใช้ Primary Key
func buildUpdateSQL(db *sql.DB, dbName, tableName string, oldRow, newRow []interface{}) (string, string) {
	primaryKeys, _ := getPrimaryKey(db, dbName, tableName)
	primaryKeyJSON := buildPrimaryKeyJSON(primaryKeys, oldRow)

	return fmt.Sprintf("🟠 UPDATE `%s`.`%s` SET ... WHERE %s;", dbName, tableName, primaryKeyJSON), primaryKeyJSON
}

// ✅ ฟังก์ชันสร้างคำสั่ง DELETE โดยใช้ Primary Key
func buildDeleteSQL(db *sql.DB, dbName, tableName string, row []interface{}) (string, string) {
	primaryKeys, _ := getPrimaryKey(db, dbName, tableName)
	primaryKeyJSON := buildPrimaryKeyJSON(primaryKeys, row)

	return fmt.Sprintf("🔴 DELETE FROM `%s`.`%s` WHERE %s;", dbName, tableName, primaryKeyJSON), primaryKeyJSON
}
