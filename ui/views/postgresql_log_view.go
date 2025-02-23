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
    CachedLogs      [][]string `json:"cached_logs"`
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

var autoRefreshEnabled bool = true
var autoRefreshButton *widget.Button
var autoRefreshTicker *time.Ticker
var autoRefreshDone chan bool

func PostgreSQLLogView(configFile string) fyne.CanvasObject {
    logData := [][]string{} 

    logTable := widget.NewTable(
        func() (int, int) { return len(logData), 2 },
        func() fyne.CanvasObject { return widget.NewLabel("\n") },
        func(id widget.TableCellID, cell fyne.CanvasObject) {
            cell.(*widget.Label).SetText(logData[id.Row][id.Col])
        },
    )

    logTable.SetColumnWidth(0, 200)
    logTable.SetColumnWidth(1, 800)

    header := container.NewHBox(
        widget.NewLabelWithStyle("วันที่และเวลา", fyne.TextAlignLeading, fyne.TextStyle{Bold: true}),
        widget.NewLabelWithStyle("ข้อความ Log", fyne.TextAlignLeading, fyne.TextStyle{Bold: true}),
    )

    tableContainer := container.NewBorder(header, nil, nil, nil, logTable)
    scrollContainer := container.NewScroll(tableContainer)
    scrollContainer.SetMinSize(fyne.NewSize(1000, 600))

    config, err := loadConfig(configFile)
    if err != nil {
        logData = [][]string{{"Error", fmt.Sprintf("ไม่สามารถโหลด config.json ได้: %v", err)}}
        logTable.Refresh()
        return scrollContainer
    }

    loadLogs := func() {
        startTime := time.Now().Format("2006-01-02 15:04:05")
        logState, _ := loadLogState(config.StateFile)
        logFilePath, err := getLatestPostgresLogFile(config.LogFilePath)
        if err != nil {
            logData = append(logData, []string{"Error", fmt.Sprintf("ไม่สามารถค้นหา Log File ล่าสุดได้: %v", err)})
            logTable.Refresh()
            return
        }

        parsedLogs, lastDateTime, isNewDataFound, err := readPostgresLogFile(logFilePath, logState.LastLogDateTime, config.FilterTables)
        if err != nil {
            logData = append(logData, []string{"Error", fmt.Sprintf("ไม่สามารถโหลด Log File ได้: %v", err)})
        } else if isNewDataFound {
            logData = append(logData, [][]string{{startTime, "เริ่มต้นอ่าน Log"}}...)
            logData = append(logData, parsedLogs...)
            saveLogState(config.StateFile, lastDateTime, filepath.Base(logFilePath), logData)
        } else {
            logData = append(logData, []string{startTime, "ไม่มี Log ใหม่ ใช้ข้อมูลจาก state.json เดิม"})
        }

        logTable.Refresh()
        scrollContainer.ScrollToBottom() 
    }

    loadButton := widget.NewButton("โหลด Log File ล่าสุด", func() {
        loadLogs()
    })

    clearButton := widget.NewButton("เคลียร์ข้อมูล", func() {
        logData = [][]string{}
        logTable.Refresh()
    })

    autoRefreshButton = widget.NewButton("เปิดการรีเฟรชอัตโนมัติ", func() {
        autoRefreshEnabled = !autoRefreshEnabled
        if autoRefreshEnabled {
            autoRefreshButton.SetText("ปิดการรีเฟรชอัตโนมัติ")
            autoRefreshTicker = time.NewTicker(10 * time.Second)
            autoRefreshDone = make(chan bool)

            go func() {
                for {
                    select {
                    case <-autoRefreshDone:
                        return
                    case <-autoRefreshTicker.C:
                        loadLogs()
                    }
                }
            }()
        } else {
            autoRefreshButton.SetText("เปิดการรีเฟรชอัตโนมัติ")
            if autoRefreshTicker != nil {
                autoRefreshTicker.Stop()
            }
            if autoRefreshDone != nil {
                autoRefreshDone <- true
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

// readPostgresLogFile อ่าน Log File โดยกรองเฉพาะคำสั่ง INSERT, UPDATE, DELETE ใน Table ที่สนใจ
func readPostgresLogFile(filePath, lastDateTime string, filterTables []string) ([][]string, string, bool, error) {
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
                    
                    // กรองเฉพาะคำสั่ง INSERT, UPDATE, DELETE สำหรับ Table ที่สนใจ
                    for _, tableName := range filterTables {
                        if strings.Contains(logMessage, fmt.Sprintf(`INSERT INTO "public"."%s"`, tableName)) ||
                            strings.Contains(logMessage, fmt.Sprintf(`UPDATE "public"."%s"`, tableName)) ||
                            strings.Contains(logMessage, fmt.Sprintf(`DELETE FROM "public"."%s"`, tableName)) ||
                            strings.Contains(logMessage, fmt.Sprintf(`INSERT INTO "%s"`, tableName)) ||
                            strings.Contains(logMessage, fmt.Sprintf(`UPDATE "%s"`, tableName)) ||
                            strings.Contains(logMessage, fmt.Sprintf(`DELETE FROM "%s"`, tableName)) {
                                
                            logs = append(logs, []string{logTime, logMessage})
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
func saveLogState(stateFile, lastDateTime, lastLogFile string, cachedLogs [][]string) error {
    logState := LogState{
        LastLogDateTime: lastDateTime,
        LastLogFile:     lastLogFile,
        CachedLogs:      cachedLogs,
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
