package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"os"

	"hissync-10/ui"
	"hissync-10/ui/forms"
	"hissync-10/ui/views"

	"fyne.io/fyne/v2"
	"fyne.io/fyne/v2/app"
	"fyne.io/fyne/v2/container"
	"fyne.io/fyne/v2/theme"
	"fyne.io/fyne/v2/widget"

	_ "github.com/denisenkom/go-mssqldb" // Microsoft SQL Server driver
	_ "github.com/go-sql-driver/mysql"   // MySQL driver
	_ "github.com/lib/pq"                // PostgreSQL driver
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// Config โครงสร้างสำหรับจัดเก็บการตั้งค่าฐานข้อมูล
type Config struct {
    DBType   string `json:"dbtype"`
    Host     string `json:"host"`
    Port     string `json:"port"`
    Username string `json:"username"`
    Password string `json:"password"`
    DBName   string `json:"dbname"`
    LogFilePath string `json:"log_directory"`
    StateFile    string `json:"state_file"`
}

// สร้างตัวแปรสำหรับ Status Bar และ Content
var statusLabel *widget.RichText
var contentContainer *fyne.Container
var appConfig Config

func main() {
    myApp := app.New()
    myWindow := myApp.NewWindow("HISSYNC v10.0")

    statusLabel = widget.NewRichTextFromMarkdown("**สถานะ:** กำลังตรวจสอบการเชื่อมต่อฐานข้อมูล...")

    statusBarContainer := container.NewVBox(
        widget.NewSeparator(), 
        container.NewPadded(
            container.NewMax(
                statusLabel,
            ),
        ),
    )

    config, err := loadConfig("config.json")
    appConfig = config

    if err != nil || !testConnection(config) {
        log.Println("Failed to connect to the database, showing connection form.")
        updateStatusBar("สถานะ: ไม่เชื่อมต่อฐานข้อมูล", false)
        forms.ShowConnectionForm(myWindow)
    } else {
        log.Println("Connected to the database successfully!")
        updateStatusBar(fmt.Sprintf("สถานะ: เชื่อมต่อฐานข้อมูล %s สำเร็จ", config.DBType), true)
    }

    contentContainer = container.NewMax(widget.NewLabel("ยินดีต้อนรับสู่แอพพลิเคชัน"))

    myWindow.SetMainMenu(ui.CreateTopbarMenu(myApp, myWindow, contentContainer))

    toolbar := ui.CreateToolbar()

    sidebarMenu := container.NewVBox(
        widget.NewButton("ข้อมูลค้างส่ง", func() {
            contentContainer.Objects = []fyne.CanvasObject{
                views.PendingDataView(),
            }
            contentContainer.Refresh()
        }),
        widget.NewButton("รายงาน", func() {
            contentContainer.Objects = []fyne.CanvasObject{
                views.ReportView(),
            }
            contentContainer.Refresh()
        }),
        widget.NewButton("การตั้งค่า", func() {
            contentContainer.Objects = []fyne.CanvasObject{
                views.SettingsView(),
            }
            contentContainer.Refresh()
        }),
        widget.NewButton("Log File"+appConfig.LogFilePath, func() {
            contentContainer.Objects = []fyne.CanvasObject{
                views.PostgreSQLLogView(appConfig.LogFilePath, appConfig.StateFile),
            }
            contentContainer.Refresh()
        }),
    )

    sidebarContainer := container.NewVBox(
        sidebarMenu,
        widget.NewLabel("เมนู Sidebar"),
    )
    sidebarContainer.Resize(fyne.NewSize(240, 800))

    splitContent := container.NewHSplit(sidebarContainer, contentContainer)
    splitContent.SetOffset(0.2)

    mainContent := container.NewBorder(toolbar, statusBarContainer, nil, nil, splitContent)

    myWindow.SetContent(mainContent)
    myWindow.Resize(fyne.NewSize(1200, 800))
    myWindow.CenterOnScreen()
    myWindow.ShowAndRun()
}


// updateStatusBar อัปเดตข้อความและสีของ Status Bar
func updateStatusBar(message string, isSuccess bool) {
    if isSuccess {
        statusLabel.Segments = []widget.RichTextSegment{
            &widget.TextSegment{
                Text: message,
                Style: widget.RichTextStyle{
                    ColorName: theme.ColorNameSuccess,
                    TextStyle: fyne.TextStyle{Bold: true},
                },
            },
        }
    } else {
        statusLabel.Segments = []widget.RichTextSegment{
            &widget.TextSegment{
                Text: message,
                Style: widget.RichTextStyle{
                    ColorName: theme.ColorNameError,
                    TextStyle: fyne.TextStyle{Bold: true},
                },
            },
        }
    }
    statusLabel.Refresh()
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

// testConnection ทดสอบการเชื่อมต่อกับฐานข้อมูล
func testConnection(config Config) bool {
    switch config.DBType {
    case "PostgreSQL":
        dsn := fmt.Sprintf("host=%s port=%s user=%s password=%s dbname=%s sslmode=disable",
            config.Host, config.Port, config.Username, config.Password, config.DBName)
        db, err := sql.Open("postgres", dsn)
        if err != nil {
            log.Println("PostgreSQL connection error:", err)
            return false
        }
        return db.Ping() == nil

    case "MySQL":
        dsn := fmt.Sprintf("%s:%s@tcp(%s:%s)/%s",
            config.Username, config.Password, config.Host, config.Port, config.DBName)
        db, err := sql.Open("mysql", dsn)
        if err != nil {
            log.Println("MySQL connection error:", err)
            return false
        }
        return db.Ping() == nil

    case "Microsoft SQL Server":
        dsn := fmt.Sprintf("sqlserver://%s:%s@%s:%s?database=%s",
            config.Username, config.Password, config.Host, config.Port, config.DBName)
        db, err := sql.Open("sqlserver", dsn)
        if err != nil {
            log.Println("SQL Server connection error:", err)
            return false
        }
        return db.Ping() == nil

    case "MongoDB":
        dsn := fmt.Sprintf("mongodb://%s:%s@%s:%s/%s",
            config.Username, config.Password, config.Host, config.Port, config.DBName)
        client, err := mongo.Connect(context.TODO(), options.Client().ApplyURI(dsn))
        if err != nil {
            log.Println("MongoDB connection error:", err)
            return false
        }
        return client.Ping(context.TODO(), nil) == nil

    default:
        log.Println("Unsupported database type:", config.DBType)
        return false
    }
}
