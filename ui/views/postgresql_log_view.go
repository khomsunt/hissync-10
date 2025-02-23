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

// PostgreSQLLogView แสดง Log File ของ PostgreSQL ใน UI ของ HISSYNC v10.0
func PostgreSQLLogView(logDirectory string, stateFile string) fyne.CanvasObject {
    logData := [][]string{} // เก็บข้อมูลวันที่และข้อความ Log แต่ละแถว

    logTable := widget.NewTable(
        func() (int, int) { return len(logData), 2 },
        func() fyne.CanvasObject { return widget.NewLabel("") },
        func(id widget.TableCellID, cell fyne.CanvasObject) {
            cell.(*widget.Label).SetText(logData[id.Row][id.Col])
        },
    )

    logTable.SetColumnWidth(0, 200)
    logTable.SetColumnWidth(1, 800)

    loadButton := widget.NewButton("โหลด Log File ล่าสุด", func() {
        logState, _ := loadLogState(stateFile)
        logFilePath, err := getLatestPostgresLogFile(logDirectory)
        if err != nil {
            logData = [][]string{{"Error", fmt.Sprintf("ไม่สามารถค้นหา Log File ล่าสุดได้: %v", err)}}
            logTable.Refresh()
            return
        }

        log.Printf("กำลังโหลด Log File: %s", logFilePath)

        parsedLogs, lastDateTime, isNewDataFound, err := readPostgresLogFile(logFilePath, logState.LastLogDateTime)
        if err != nil {
            logData = [][]string{{"Error", fmt.Sprintf("ไม่สามารถโหลด Log File ได้: %v", err)}}
        } else if isNewDataFound {
            logData = parsedLogs
            saveLogState(stateFile, lastDateTime, filepath.Base(logFilePath), logData)
            log.Printf("บันทึกค่า State ใหม่: %s, %s\n", lastDateTime, filepath.Base(logFilePath))
        } else {
            logData = logState.CachedLogs
            log.Printf("ไม่มี Log ใหม่ ใช้ข้อมูลจาก state.json เดิม")
        }

        if len(logData) == 0 {
            logData = [][]string{{"Info", "ไม่มีข้อมูล Log ให้แสดง"}}
        }

        logTable.Refresh()
    })

    header := container.NewHBox(
        widget.NewLabelWithStyle("วันที่และเวลา", fyne.TextAlignLeading, fyne.TextStyle{Bold: true}),
        widget.NewLabelWithStyle("ข้อความ Log", fyne.TextAlignLeading, fyne.TextStyle{Bold: true}),
    )

    tableContainer := container.NewBorder(header, nil, nil, nil, logTable)

    scrollContainer := container.NewScroll(tableContainer)
    scrollContainer.SetMinSize(fyne.NewSize(1000, 600))

    return container.NewBorder(loadButton, nil, nil, nil, scrollContainer)
}

// readPostgresLogFile อ่าน Log File พร้อมคืนค่าข้อมูล Log และสถานะว่ามี Log ใหม่หรือไม่
func readPostgresLogFile(filePath, lastDateTime string) ([][]string, string, bool, error) {
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
                    logs = append(logs, []string{logTime, logMessage})
                    lastReadTime = logTime
                    isNewDataFound = true
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
