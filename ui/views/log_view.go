package views

import (
	"bufio"
	"fmt"
	"os"
	"path/filepath"
	"sort"

	"fyne.io/fyne/v2"
	"fyne.io/fyne/v2/container"
	"fyne.io/fyne/v2/widget"
)

// LogView แสดง Log File ของ PostgreSQL ใน UI ของ HISSYNC v10.0
func LogView(logDirectory string) fyne.CanvasObject {
    logContent := widget.NewRichText() // เปลี่ยนเป็น RichText เพื่อให้เป็น Read-Only
	logContent.ParseMarkdown(fmt.Sprintf("logdir=%s",logDirectory))

    loadButton := widget.NewButton("โหลด Log File ล่าสุด", func() {
		
        logFilePath, err := getLatestLogFile(logDirectory)
        if err != nil {
            logContent.ParseMarkdown(fmt.Sprintf("ไม่สามารถค้นหา Log File ล่าสุดได้: %v logFilePath %s", err, logFilePath))
            return
        }

        logText, err := readLogFile(logFilePath)
        if err != nil {
            logContent.ParseMarkdown(fmt.Sprintf("ไม่สามารถโหลด Log File ได้: %v %s", err, logFilePath))
        } else {
            logContent.ParseMarkdown(fmt.Sprintf("**ไฟล์ล่าสุด:** %s\n\n```\n%s\n```", logFilePath, logText))
        }
    })

    return container.NewBorder(loadButton, nil, nil, nil, logContent)
}

// getLatestLogFile ค้นหาไฟล์ Log ล่าสุดใน Directory
func getLatestLogFile(logDirectory string) (string, error) {
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

// readLogFile อ่านไฟล์ Log และคืนค่าเป็น String
func readLogFile(filePath string) (string, error) {
    file, err := os.Open(filePath)
    if err != nil {
        return "", err
    }
    defer file.Close()

    var content string
    scanner := bufio.NewScanner(file)
    for scanner.Scan() {
        content += scanner.Text() + "\n"
    }

    if err := scanner.Err(); err != nil {
        return "", err
    }
    return content, nil
}
