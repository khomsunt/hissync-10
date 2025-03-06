package views

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"image/color"
	"io/ioutil"
	"log"
	"os"
	"time"

	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
	_ "github.com/go-sql-driver/mysql"

	"fyne.io/fyne/v2"
	"fyne.io/fyne/v2/canvas"
	"fyne.io/fyne/v2/container"
	"fyne.io/fyne/v2/dialog"
	"fyne.io/fyne/v2/widget"

	config "hissync-10/functions"
)

// โครงสร้างสำหรับ db_table_config.json
type DBTableConfigEntry struct {
	Database string `json:"database"`
	Table    string `json:"table"`
}

type DBTableConfig []DBTableConfigEntry

// โครงสร้างสำหรับ state.json
type State struct {
	LastBinlogPosition string `json:"last_binlog_position"`
	LastLogDatetime    string `json:"last_log_datetime"`
	LastLogFile        string `json:"last_log_file"`
}

func MySQLLogView(configFile string, dbTableConfigFile string, w fyne.Window) fyne.CanvasObject {
	// โหลด config.json (สำหรับการเชื่อมต่อ MySQL)
	cfg, err := config.LoadConfig(configFile)
	if err != nil {
		log.Fatalf("❌ ไม่สามารถโหลด config.json: %v", err)
	}

	// โหลด db_table_config.json
	var dbTblCfg DBTableConfig
	dbTblConfigData, err := ioutil.ReadFile("db_table_config.json")
	if err != nil {
		if os.IsNotExist(err) {
			// แสดง popup เตือนถ้าไม่พบไฟล์ (ใช้ w ที่รับมาจากพารามิเตอร์)
			dialog.ShowInformation("Error", "ไม่พบไฟล์ db_table_config.json กรุณาสร้างไฟล์ก่อนใช้งาน", w)
			return widget.NewLabel("ไม่สามารถเริ่มโปรแกรมได้ เนื่องจากไม่พบไฟล์ db_table_config.json")
		}
		log.Fatalf("❌ ไม่สามารถโหลด db_table_config.json: %v", err)
	}
	if err := json.Unmarshal(dbTblConfigData, &dbTblCfg); err != nil {
		log.Fatalf("❌ ไม่สามารถแปลง db_table_config.json: %v", err)
	}

	// สร้าง map เพื่อตรวจสอบ table ที่ต้องการอย่างรวดเร็ว (key: "database.table")
	allowedDBTables := make(map[string]bool)
	for _, entry := range dbTblCfg {
		fullName := fmt.Sprintf("%s.%s", entry.Database, entry.Table)
		allowedDBTables[fullName] = true
	}

	// ตาราง Log
	data := [][]string{
		{"Binlog Pos.", "Timestamp", "Table", "Query Type", "Primary Key", "SQL"},
	}

	table := widget.NewTable(
		func() (int, int) { return len(data), 6 },
		func() fyne.CanvasObject {
			label := widget.NewLabelWithStyle("", fyne.TextAlignLeading, fyne.TextStyle{})
			label.Wrapping = fyne.TextWrapWord
			return container.NewStack(
				canvas.NewRectangle(color.Transparent),
				label,
			)
		},
		func(id widget.TableCellID, obj fyne.CanvasObject) {
			var label *widget.Label
			var bg *canvas.Rectangle
			switch o := obj.(type) {
			case *fyne.Container:
				cont := o
				label = cont.Objects[1].(*widget.Label)
				bg = cont.Objects[0].(*canvas.Rectangle)
			case *widget.Label:
				label = o
			default:
				panic("unexpected object type in table cell")
			}

			label.SetText(data[id.Row][id.Col])
			if id.Row == 0 {
				label.TextStyle = fyne.TextStyle{Bold: true}
				if bg != nil {
					bg.FillColor = color.Gray{0xE0}
					bg.Refresh()
				}
			} else {
				label.TextStyle = fyne.TextStyle{}
				if bg != nil {
					bg.FillColor = color.Transparent
					bg.Refresh()
				}
			}
			label.Refresh()
		},
	)

	table.SetColumnWidth(0, 100)
	table.SetColumnWidth(1, 180)
	table.SetColumnWidth(2, 250)
	table.SetColumnWidth(3, 100)
	table.SetColumnWidth(4, 300)
	table.SetColumnWidth(5, 500)

	calculateRowHeight := func(row int) float32 {
		maxHeight := float32(24)
		for col := 0; col < 6; col++ {
			lines := len(data[row][col]) / 50
			height := float32((lines + 1) * 24)
			if height > maxHeight {
				maxHeight = height
			}
		}
		return maxHeight
	}

	updateTable := func(binlogPosStr, timestamp, tableName, queryType, primaryKey, sql string) {
		data = append(data, []string{binlogPosStr, timestamp, tableName, queryType, primaryKey, sql})
		table.Length = func() (int, int) {
			return len(data), 6
		}
		table.Refresh()
		for i := range data {
			table.SetRowHeight(i, calculateRowHeight(i))
		}
	}

	// อ่านและบันทึก state.json
	saveState := func(pos string, datetime string, file string) {
		state := State{
			LastBinlogPosition: pos,
			LastLogDatetime:    datetime,
			LastLogFile:        file,
		}
		jsonData, _ := json.MarshalIndent(state, "", "  ")
		ioutil.WriteFile("state.json", jsonData, 0644)
	}

	loadState := func() (string, string, error) {
		file, err := ioutil.ReadFile(cfg.StateFile)
		if err != nil {
			if os.IsNotExist(err) {
				return "0", "", nil
			}
			return "0", "", err
		}
		var state State
		if err := json.Unmarshal(file, &state); err != nil {
			return "0", "", err
		}
		if state.LastLogFile == "" {
			return "0", "", nil
		}
		return state.LastBinlogPosition, state.LastLogFile, nil
	}

	go func() {
		mysqlDSN := fmt.Sprintf("%s:%s@tcp(%s:%s)/%s",
			cfg.Username, cfg.Password, cfg.Host, cfg.Port, cfg.DBName)

		db, err := sql.Open("mysql", mysqlDSN)
		if err != nil {
			updateTable("0", time.Now().Format("2006-01-02 15:04:05"), "", "", "", fmt.Sprintf("❌ ไม่สามารถเชื่อมต่อ MySQL: %v", err))
			return
		}
		defer db.Close()

		syncerCfg := replication.BinlogSyncerConfig{
			ServerID: 100,
			Flavor:   "mysql",
			Host:     cfg.Host,
			Port:     3306,
			User:     cfg.Username,
			Password: cfg.Password,
		}

		for {
			lastPosStr, lastFile, err := loadState()
			if err != nil {
				// updateTable("0", time.Now().Format("2006-01-02 15:04:05"), "", "", "", fmt.Sprintf("❌ ไม่สามารถโหลด state.json: %v", err))
				// return
			}

			var binlogPos uint32
			var binlogFile string
			binlogPosStr := lastPosStr

			if lastFile == "" || lastPosStr == "0" {
				var binlogIgnored1, binlogIgnored2, binlogIgnored3 string
				err = db.QueryRow("SHOW MASTER STATUS").Scan(&binlogFile, &binlogPos, &binlogIgnored1, &binlogIgnored2, &binlogIgnored3)
				if err != nil {
					updateTable("0", time.Now().Format("2006-01-02 15:04:05"), "", "", "", fmt.Sprintf("❌ ไม่สามารถดึง Binlog ล่าสุด: %v", err))
					return
				}
				binlogPosStr = fmt.Sprintf("%d", binlogPos)
				lastFile = binlogFile
			} else {
				binlogFile = lastFile
				binlogPos = uint32(atoi(lastPosStr))
			}

			syncer := replication.NewBinlogSyncer(syncerCfg)
			streamer, err := syncer.StartSync(mysql.Position{Name: binlogFile, Pos: binlogPos})
			if err != nil {
				updateTable("0", time.Now().Format("2006-01-02 15:04:05"), "", "", "", fmt.Sprintf("❌ เริ่มต้น Sync Binlog ไม่สำเร็จ: %v", err))
				return
			}

			tableMap := make(map[uint64]*replication.TableMapEvent)
			timeout := time.After(10 * time.Second)

		Loop:
			for {
				select {
				case <-timeout:
					saveState(binlogPosStr, time.Now().Format("2006-01-02 15:04:05.000 -07"), binlogFile)
					syncer.Close()
					break Loop
				default:
					ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
					ev, err := streamer.GetEvent(ctx)
					cancel()
					if err != nil && err != context.DeadlineExceeded {
						updateTable("0", time.Now().Format("2006-01-02 15:04:05"), "", "", "", fmt.Sprintf("❌ เกิดข้อผิดพลาดในการอ่าน Binlog: %v", err))
						return
					}
					if ev == nil {
						continue
					}

					switch e := ev.Event.(type) {
					case *replication.TableMapEvent:
						tableMap[e.TableID] = e

					case *replication.RowsEvent:
						table, ok := tableMap[e.TableID]
						if !ok {
							continue
						}

						dbName := string(table.Schema)
						tableName := string(table.Table)
						fullTableName := fmt.Sprintf("%s.%s", dbName, tableName)

						// ตรวจสอบว่า database และ table อยู่ใน db_table_config.json หรือไม่
						if !allowedDBTables[fullTableName] {
							continue
						}

						timestamp := time.Unix(int64(ev.Header.Timestamp), 0).Format("2006-01-02 15:04:05")
						binlogPosStr = fmt.Sprintf("%d", ev.Header.LogPos)

						switch ev.Header.EventType {
						case replication.WRITE_ROWS_EVENTv1, replication.WRITE_ROWS_EVENTv2:
							for _, row := range e.Rows {
								sql, primaryKeyJSON := buildInsertSQL(db, dbName, tableName, row)
								updateTable(binlogPosStr, timestamp, fullTableName, "INSERT", primaryKeyJSON, sql)
							}
						case replication.UPDATE_ROWS_EVENTv1, replication.UPDATE_ROWS_EVENTv2:
							for i := 0; i < len(e.Rows); i += 2 {
								oldRow, newRow := e.Rows[i], e.Rows[i+1]
								sql, primaryKeyJSON := buildUpdateSQL(db, dbName, tableName, oldRow, newRow)
								updateTable(binlogPosStr, timestamp, fullTableName, "UPDATE", primaryKeyJSON, sql)
							}
						case replication.DELETE_ROWS_EVENTv1, replication.DELETE_ROWS_EVENTv2:
							for _, row := range e.Rows {
								sql, primaryKeyJSON := buildDeleteSQL(db, dbName, tableName, row)
								updateTable(binlogPosStr, timestamp, fullTableName, "DELETE", primaryKeyJSON, sql)
							}
						}
					}
				}
			}
		}
	}()

	return container.NewMax(
		container.NewVScroll(table),
	)
}

// ฟังก์ชันแปลง string เป็น int
func atoi(s string) int {
	var result int
	fmt.Sscanf(s, "%d", &result)
	return result
}

// ฟังก์ชันสร้าง JSON ของ Primary Key
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

// ฟังก์ชันสร้างคำสั่ง INSERT
func buildInsertSQL(db *sql.DB, dbName, tableName string, row []interface{}) (string, string) {
	primaryKeys, _ := getPrimaryKey(db, dbName, tableName)
	primaryKeyJSON := buildPrimaryKeyJSON(primaryKeys, row)
	return fmt.Sprintf("🟢 INSERT INTO `%s`.`%s` VALUES (%v);", dbName, tableName, row), primaryKeyJSON
}

// ฟังก์ชันสร้างคำสั่ง UPDATE โดยใช้ Primary Key
func buildUpdateSQL(db *sql.DB, dbName, tableName string, oldRow, newRow []interface{}) (string, string) {
	primaryKeys, _ := getPrimaryKey(db, dbName, tableName)
	primaryKeyJSON := buildPrimaryKeyJSON(primaryKeys, oldRow)
	return fmt.Sprintf("🟠 UPDATE `%s`.`%s` SET ... WHERE %s;", dbName, tableName, primaryKeyJSON), primaryKeyJSON
}

// ฟังก์ชันสร้างคำสั่ง DELETE โดยใช้ Primary Key
func buildDeleteSQL(db *sql.DB, dbName, tableName string, row []interface{}) (string, string) {
	primaryKeys, _ := getPrimaryKey(db, dbName, tableName)
	primaryKeyJSON := buildPrimaryKeyJSON(primaryKeys, row)
	return fmt.Sprintf("🔴 DELETE FROM `%s`.`%s` WHERE %s;", dbName, tableName, primaryKeyJSON), primaryKeyJSON
}

// ดึง Primary Key พร้อมค่าจากตาราง
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