package forms

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/exec"

	"fyne.io/fyne/v2"
	"fyne.io/fyne/v2/container"
	"fyne.io/fyne/v2/dialog"
	"fyne.io/fyne/v2/widget"

	_ "github.com/denisenkom/go-mssqldb" // Microsoft SQL Server driver
	_ "github.com/go-sql-driver/mysql"   // MySQL driver
	_ "github.com/lib/pq"                // PostgreSQL driver
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type Config struct {
    DBType   string `json:"dbtype"`
    Host     string `json:"host"`
    Port     string `json:"port"`
    Username string `json:"username"`
    Password string `json:"password"`
    DBName   string `json:"dbname"`
}

// ShowConnectionForm แสดง Popup Form สำหรับกำหนดค่าการเชื่อมต่อกับฐานข้อมูล
func ShowConnectionForm(myWindow fyne.Window) {
    dbTypes := []string{"PostgreSQL", "MySQL", "Microsoft SQL Server", "MongoDB"}
    dbTypeSelect := widget.NewSelect(dbTypes, func(value string) {})

    hostEntry := widget.NewEntry()
    portEntry := widget.NewEntry()
    userEntry := widget.NewEntry()
    passwordEntry := widget.NewPasswordEntry()
    dbNameEntry := widget.NewEntry()

    // โหลดค่าจาก config.json ถ้ามีอยู่
    config, err := loadConfig("config.json")
    if err == nil {
        dbTypeSelect.SetSelected(config.DBType)
        hostEntry.SetText(config.Host)
        portEntry.SetText(config.Port)
        userEntry.SetText(config.Username)
        passwordEntry.SetText(config.Password)
        dbNameEntry.SetText(config.DBName)
    } else {
        log.Println("No existing config file found, starting with empty form.")
    }

    form := widget.NewForm(
        widget.NewFormItem("Database Type", dbTypeSelect),
        widget.NewFormItem("Host", hostEntry),
        widget.NewFormItem("Port", portEntry),
        widget.NewFormItem("Username", userEntry),
        widget.NewFormItem("Password", passwordEntry),
        widget.NewFormItem("Database Name", dbNameEntry),
    )

    var popup dialog.Dialog

    saveButton := widget.NewButton("Save", func() {
        config := Config{
            DBType:   dbTypeSelect.Selected,
            Host:     hostEntry.Text,
            Port:     portEntry.Text,
            Username: userEntry.Text,
            Password: passwordEntry.Text,
            DBName:   dbNameEntry.Text,
        }

        if testConnection(config) {
            saveConfig(config)
            dialog.ShowInformation("Success", "Connection Successful and Config Saved!", myWindow)

            // แสดง Popup แจ้งเตือนก่อน Restart แอพ
            showRestartConfirmation(myWindow, popup)

            //popup.Hide() // ปิด Popup เมื่อเชื่อมต่อสำเร็จ
            //restartApp()  // Restart แอพพลิเคชัน
        } else {
            dialog.ShowError(fmt.Errorf("Failed to connect to the database"), myWindow)
        }
    })

    cancelButton := widget.NewButton("Cancel", func() {
        if popup != nil {
            popup.Hide() // ปิด Popup โดยไม่ปิดแอพพลิเคชัน
        }
    })

    buttonContainer := container.NewHBox(saveButton, cancelButton)
    formContainer := container.NewVBox(form, buttonContainer)

    popup = dialog.NewCustom("Database Connection", "Close", formContainer, myWindow)
    popup.Resize(fyne.NewSize(600, 400))
    popup.Show()
}


// showRestartConfirmation แสดง Popup แจ้งเตือนก่อน Restart แอพ
func showRestartConfirmation(myWindow fyne.Window, popup dialog.Dialog) {
    dialog.ShowConfirm(
        "Restart Required",
        "เนื่องจากมีการเปลี่ยนแปลงค่ากำหนดเชื่อมต่อข้อมูล ระบบจำเป็นต้อง Restart เพื่อทำการเชื่อมต่อฐานข้อมูลใหม่อย่างสมบูรณ์",
        func(confirm bool) {
            if confirm {
                popup.Hide() // ปิด Popup การตั้งค่า
                restartApp() // ทำการ Restart แอพพลิเคชัน
            }
        },
        myWindow,
    )
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

// saveConfig บันทึกค่าการเชื่อมต่อฐานข้อมูลลงในไฟล์ config.json
func saveConfig(config Config) {
    file, err := os.Create("config.json")
    if err != nil {
        fmt.Println("Error creating config file:", err)
        return
    }
    defer file.Close()

    encoder := json.NewEncoder(file)
    if err := encoder.Encode(config); err != nil {
        fmt.Println("Error saving config:", err)
    } else {
        fmt.Println("Config saved successfully!")
    }
}

// restartApp ทำการรีสตาร์ทแอพพลิเคชัน
func restartApp() {
    exe, err := os.Executable()
    if err != nil {
        log.Fatalf("Failed to find executable: %v", err)
    }
    cmd := exec.Command(exe)
    cmd.Start()
    os.Exit(0) // ปิดแอพพลิเคชันเดิม
}
