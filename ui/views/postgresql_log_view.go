package views

import (
	"bufio"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"fyne.io/fyne/v2"
	"fyne.io/fyne/v2/container"
	"fyne.io/fyne/v2/widget"
)

type LogState struct {
    LastLogDateTime string     `json:"last_log_datetime"`
    LastLogFile     string     `json:"last_log_file"`
}

type Config struct {
    DBType       string   `json:"dbtype"`
    Host         string   `json:"host"`
    Port         string   `json:"port"`
    Username     string   `json:"username"`
    Password     string   `json:"password"`
    DBName       string   `json:"dbname"`
    LogFilePath  string   `json:"log_file_path"`
    StateFile    string   `json:"state_file"`
    FilterTables []string `json:"filter_tables"`
}

type TableConfig struct {
    TableName  string   `json:"table_name"`
    Keys       []string `json:"keys"`
    PrimaryKey []string `json:"primary_key"`
}

var autoRefreshEnabled bool = true
var autoRefreshButton *widget.Button
var autoRefreshTicker *time.Ticker
var autoRefreshDone chan bool

func PostgreSQLLogView(configFile string, tableConfigFile string) fyne.CanvasObject { // เพิ่มพารามิเตอร์ tableConfigFile
    logData := [][]string{} 
        
    logTable := widget.NewTable(
        func() (int, int) { return len(logData), 4 }, // ปรับเป็น 4 คอลัมน์ (เพิ่มคอลัมน์สำหรับข้อมูลที่สกัด)
        func() fyne.CanvasObject { return widget.NewLabel("\n") },
        func(id widget.TableCellID, cell fyne.CanvasObject) {
            label := cell.(*widget.Label)
            if id.Col < len(logData[id.Row]) {
                label.SetText(logData[id.Row][id.Col])
            } else {
                label.SetText("")
            }
            label.Wrapping = fyne.TextWrapWord
        },
    )

    logTable.SetColumnWidth(0, 200) // วันที่และเวลา
    logTable.SetColumnWidth(1, 500) // ข้อความ Log
    logTable.SetColumnWidth(2, 100) // ประเภทคิวรี่
    logTable.SetColumnWidth(3, 200) // ข้อมูลที่สกัด (keys หรือ primary_key)


    // สร้าง header ด้วย *widget.Label และกำหนดขนาดผ่าน Resize
    dateTimeLabel := widget.NewLabelWithStyle("วันที่และเวลา", fyne.TextAlignLeading, fyne.TextStyle{Bold: true})
    dateTimeLabel.Resize(fyne.NewSize(200, 30)) // ความกว้าง 200, สูง 30

    logMessageLabel := widget.NewLabelWithStyle("ข้อความ Log", fyne.TextAlignLeading, fyne.TextStyle{Bold: true})
    logMessageLabel.Resize(fyne.NewSize(500, 30)) // ความกว้าง 500, สูง 30

    queryTypeLabel := widget.NewLabelWithStyle("ประเภทคิวรี่", fyne.TextAlignLeading, fyne.TextStyle{Bold: true})
    queryTypeLabel.Resize(fyne.NewSize(100, 30)) // ความกว้าง 100, สูง 30

    extractedDataLabel := widget.NewLabelWithStyle("ข้อมูลที่สกัด", fyne.TextAlignLeading, fyne.TextStyle{Bold: true})
    extractedDataLabel.Resize(fyne.NewSize(200, 30)) // ความกว้าง 200, สูง 30

    header := container.NewHBox(dateTimeLabel, logMessageLabel, queryTypeLabel, extractedDataLabel)


    tableContainer := container.NewBorder(header, nil, nil, nil, logTable)
    scrollContainer := container.NewScroll(tableContainer)
    scrollContainer.SetMinSize(fyne.NewSize(1000, 600))

    config, err := loadConfig(configFile)
    if err != nil {
        logData = [][]string{{"Error", fmt.Sprintf("ไม่สามารถโหลด config.json ได้: %v", err), "", ""}}
        logTable.Refresh()
        return scrollContainer
    }

    tableConfigs, err := loadTableConfig(tableConfigFile) // โหลด table_config.json
    if err != nil {
        logData = [][]string{{"Error", fmt.Sprintf("ไม่สามารถโหลด table_config.json ได้: %v", err), "", ""}}
        logTable.Refresh()
        return scrollContainer
    }

    var lastClearTime time.Time = time.Now()
    const refreshInterval = 10 * time.Minute

    loadLogs := func() {
        startTime := time.Now().Format("2006-01-02 15:04:05")
        logState, _ := loadLogState(config.StateFile)
        logFilePath, err := getLatestPostgresLogFile(config.LogFilePath)
        if err != nil {
            logData = append(logData, []string{"Error", fmt.Sprintf("ไม่สามารถค้นหา Log File ล่าสุดได้: %v", err), "", ""})
            logTable.Refresh()
            return
        }

        parsedLogs, lastDateTime, isNewDataFound, err := readPostgresLogFile(logFilePath, logState.LastLogDateTime, config.FilterTables, tableConfigs)
        if err != nil {
            logData = append(logData, []string{"Error", fmt.Sprintf("ไม่สามารถโหลด Log File ได้: %v", err), "", ""})
        } else if isNewDataFound {
            logData = append(logData, [][]string{{startTime, "เริ่มต้นอ่าน Log", "", ""}}...)
            logData = append(logData, parsedLogs...)
            saveLogState(config.StateFile, lastDateTime, filepath.Base(logFilePath)) // ลบ logData
        } else {
            logData = append(logData, []string{startTime, "ไม่มี Log ใหม่ ใช้ข้อมูลจาก state.json เดิม", "", ""})
        }

        logTable.Refresh()
        scrollContainer.ScrollToBottom() 
    }

    // เริ่มต้นการรีเฟรชอัตโนมัติทุก 10 วินาทีสำหรับการอัพเดทปกติ
    autoRefreshEnabled = true
    autoRefreshTicker = time.NewTicker(10 * time.Second)
    autoRefreshDone = make(chan bool)

    // เพิ่ม ticker สำหรับตรวจสอบการรีเฟรชทุก 10 นาที
    refreshCheckTicker := time.NewTicker(1 * time.Minute)
    refreshDone := make(chan bool)

    go func() {
        for {
            select {
            case <-autoRefreshDone:
                refreshCheckTicker.Stop()
                refreshDone <- true
                return
            case <-autoRefreshTicker.C:
                if autoRefreshEnabled {
                    loadLogs()
                }
            case <-refreshCheckTicker.C:
                if autoRefreshEnabled && time.Since(lastClearTime) >= refreshInterval {
                    loadLogs()
                }
            }
        }
    }()

    loadButton := widget.NewButton("โหลด Log File ล่าสุด", func() {
        loadLogs()
    })

    clearButton := widget.NewButton("เคลียร์ข้อมูล", func() {
        logData = [][]string{}
        logTable.Refresh()
        lastClearTime = time.Now() // รีเซ็ตเวลาการเคลียร์ล่าสุด
    })

    autoRefreshButton = widget.NewButton("ปิดการรีเฟรชอัตโนมัติ", func() {
        autoRefreshEnabled = !autoRefreshEnabled
        if autoRefreshEnabled {
            autoRefreshButton.SetText("ปิดการรีเฟรชอัตโนมัติ")
            autoRefreshTicker = time.NewTicker(10 * time.Second)
            autoRefreshDone = make(chan bool)
            refreshCheckTicker = time.NewTicker(1 * time.Minute)
            refreshDone = make(chan bool)

            go func() {
                for {
                    select {
                    case <-autoRefreshDone:
                        refreshCheckTicker.Stop()
                        refreshDone <- true
                        return
                    case <-autoRefreshTicker.C:
                        loadLogs()
                    case <-refreshCheckTicker.C:
                        if time.Since(lastClearTime) >= refreshInterval {
                            loadLogs()
                        }
                    }
                }
            }()
        } else {
            autoRefreshButton.SetText("เปิดการรีเฟรชอัตโนมัติ")
            if autoRefreshTicker != nil {
                autoRefreshTicker.Stop()
            }
            if refreshCheckTicker != nil {
                refreshCheckTicker.Stop()
            }
            if autoRefreshDone != nil {
                autoRefreshDone <- true
            }
            if refreshDone != nil {
                refreshDone <- true
            }
        }
    })

    controlContainer := container.NewHBox(loadButton, clearButton, autoRefreshButton)

    return container.NewBorder(controlContainer, nil, nil, nil, scrollContainer)
}


// loadConfig โหลดการตั้งค่าจากไฟล์ config.json
func loadConfig(filepath string) (Config, error) {
    var config Config
    file, err := os.Open(filepath)
    if err != nil {
        return config, err
    }
    defer file.Close()

    decoder := json.NewDecoder(file)
    err = decoder.Decode(&config)
    return config, err
}

func loadTableConfig(filepath string) ([]TableConfig, error) {
    var tableConfigs []TableConfig
    file, err := os.Open(filepath)
    if err != nil {
        return nil, err
    }
    defer file.Close()

    decoder := json.NewDecoder(file)
    err = decoder.Decode(&tableConfigs)
    return tableConfigs, err
}

// readPostgresLogFile อ่าน Log File โดยกรองเฉพาะคำสั่ง INSERT, UPDATE, DELETE ใน Table ที่สนใจ
func readPostgresLogFile(filePath, lastDateTime string, filterTables []string, tableConfigs []TableConfig) ([][]string, string, bool, error) {
    file, err := os.Open(filePath)
    if err != nil {
        return nil, "", false, err
    }
    defer file.Close()

    var logs [][]string
    var lastReadTime string
    isNewDataFound := false

    scanner := bufio.NewScanner(file)
    dateTimeFormat := "2006-01-02 15:04:05.000 -07"

    for scanner.Scan() {
        line := strings.TrimSpace(scanner.Text())
        if len(line) > 28 {
            logTime := strings.TrimSpace(line[:27])
            logMessage := strings.TrimSpace(line[28:])

            if parsedTime, err := time.Parse(dateTimeFormat, logTime); err == nil {
                if lastDateTime == "" || parsedTime.After(parseDateTime(lastDateTime)) {
                    for _, tableName := range filterTables {
                        queryType := ""
                        extractedData := ""

                        // ตรวจสอบประเภทคิวรี่
                        if strings.Contains(logMessage, fmt.Sprintf(`INSERT INTO "public"."%s"`, tableName)) ||
                            strings.Contains(logMessage, fmt.Sprintf(`INSERT INTO "%s"`, tableName)) {
                            queryType = "INSERT"
                        } else if strings.Contains(logMessage, fmt.Sprintf(`UPDATE "public"."%s"`, tableName)) ||
                            strings.Contains(logMessage, fmt.Sprintf(`UPDATE "%s"`, tableName)) {
                            queryType = "UPDATE"
                        } else if strings.Contains(logMessage, fmt.Sprintf(`DELETE FROM "public"."%s"`, tableName)) ||
                            strings.Contains(logMessage, fmt.Sprintf(`DELETE FROM "%s"`, tableName)) {
                            queryType = "DELETE"
                        }

                        if queryType != "" {
                            // หา table config ที่ตรงกับ tableName
                            for _, tc := range tableConfigs {
                                if tc.TableName == tableName {
                                    if queryType == "INSERT" {
                                        // สกัดข้อมูลจาก VALUES ใน INSERT
                                        valuesIdx := strings.Index(logMessage, "VALUES")
                                        if valuesIdx != -1 {
                                            valuesPart := logMessage[valuesIdx+6:] // ข้าม "VALUES"
                                            valuesPart = strings.Trim(valuesPart, "() \n") // ลบวงเล็บและช่องว่าง
                                            valuePairs := strings.Split(valuesPart, ",")

                                            // สร้าง mapping ของ column และ value จาก INSERT
                                            columns := []string{}
                                            columnIdx := strings.Index(logMessage, "(")
                                            if columnIdx != -1 {
                                                columnPart := logMessage[columnIdx+1 : strings.Index(logMessage, ")")]
                                                columns = strings.Split(strings.TrimSpace(columnPart), ",")
                                            }

                                            // สกัดข้อมูลจาก keys
                                            for i, col := range columns {
                                                col = strings.TrimSpace(strings.Trim(col, `"`))
                                                for _, key := range tc.Keys {
                                                    if col == key && i < len(valuePairs) {
                                                        value := strings.TrimSpace(valuePairs[i])
                                                        value = strings.Trim(value, `'`) // ลบเครื่องหมายคำพูด
                                                        extractedData += fmt.Sprintf("%s: %s, ", key, value)
                                                    }
                                                }
                                            }
                                        }                                    
                                    } else { // UPDATE หรือ DELETE
                                        // สกัดข้อมูลจาก primary_key
                                        for _, pk := range tc.PrimaryKey {
                                            if strings.Contains(logMessage, fmt.Sprintf(`"%s" =`, pk)) {
                                                startIdx := strings.Index(logMessage, fmt.Sprintf(`"%s" =`, pk))
                                                if startIdx != -1 {
                                                    valuePart := logMessage[startIdx+len(fmt.Sprintf(`"%s" =`, pk)):]
                                                    value := strings.Split(valuePart, " ")[1]
                                                    extractedData += fmt.Sprintf("%s: %s, ", pk, strings.TrimSpace(value))
                                                }
                                            }
                                        }
                                    }
                                    break
                                }
                            }
                            extractedData = strings.TrimSuffix(extractedData, ", ")
                            logs = append(logs, []string{logTime, logMessage, queryType, extractedData})
                            lastReadTime = logTime
                            isNewDataFound = true
                            break
                        }
                    }
                }
            } else {
                log.Printf("ไม่สามารถแปลงเวลา: %s, ข้อความ: %s\n", logTime, line)
            }
        }
    }

    if err := scanner.Err(); err != nil {
        return nil, "", false, err
    }

    return logs, lastReadTime, isNewDataFound, nil
}



// loadLogState โหลดสถานะการอ่าน Log จากไฟล์ state.json
func loadLogState(stateFile string) (LogState, error) {
    var logState LogState
    file, err := os.Open(stateFile)
    if err != nil {
        return logState, err
    }
    defer file.Close()

    decoder := json.NewDecoder(file)
    err = decoder.Decode(&logState)
    return logState, err
}

// saveLogState บันทึกวันที่เวลาสุดท้ายที่อ่านได้ใน state.json พร้อม Cached Logs
func saveLogState(stateFile, lastDateTime, lastLogFile string) error { // ลบพารามิเตอร์ cachedLogs
    logState := LogState{
        LastLogDateTime: lastDateTime,
        LastLogFile:     lastLogFile,
    }

    file, err := os.Create(stateFile)
    if err != nil {
        return err
    }
    defer file.Close()

    encoder := json.NewEncoder(file)
    return encoder.Encode(logState)
}

// parseDateTime แปลง String เป็น Time
func parseDateTime(dateTimeStr string) time.Time {
    dateTimeFormat := "2006-01-02 15:04:05.000 -07"
    t, _ := time.Parse(dateTimeFormat, dateTimeStr)
    return t
}

// getLatestPostgresLogFile ค้นหาไฟล์ Log ล่าสุดใน Directory
func getLatestPostgresLogFile(logDirectory string) (string, error) {
    var logFiles []string

    err := filepath.Walk(logDirectory, func(path string, info os.FileInfo, err error) error {
        if err != nil {
            return err
        }
        if !info.IsDir() && filepath.Ext(path) == ".log" {
            logFiles = append(logFiles, path)
        }
        return nil
    })

    if err != nil {
        return "", err
    }

    if len(logFiles) == 0 {
        return "", fmt.Errorf("ไม่พบไฟล์ Log ในโฟลเดอร์ %s", logDirectory)
    }

    sort.Slice(logFiles, func(i, j int) bool {
        iInfo, _ := os.Stat(logFiles[i])
        jInfo, _ := os.Stat(logFiles[j])
        return iInfo.ModTime().After(jInfo.ModTime())
    })

    return logFiles[0], nil
}
