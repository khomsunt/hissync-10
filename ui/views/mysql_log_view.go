package views

import (
	"context"
	"database/sql"
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
	logText.Disable() // ✅ ใช้ Disable() แทน SetReadOnly()

	// แสดง Log บน UI
	appendLog := func(text string) {
		logText.SetText(logText.Text + text + "\n")
	}

	// เรียกใช้ Goroutine เพื่ออ่าน Binlog
	go func() {
		// ตั้งค่าการเชื่อมต่อ MySQL
		mysqlDSN := `root:5HAg3rWT#m@tcp(209.15.97.58:3306)/`
		db, err := sql.Open("mysql", mysqlDSN)
		if err != nil {
			appendLog(fmt.Sprintf("❌ ไม่สามารถเชื่อมต่อ MySQL: %v", err))
			return
		}
		//defer db.Close()

		// ดึง Binlog ล่าสุด
		var binlogFile string
		var binlogPos uint32
		err = db.QueryRow("SHOW MASTER STATUS").Scan(&binlogFile, &binlogPos)
		if err != nil {
			appendLog(fmt.Sprintf("❌ ไม่สามารถดึง Binlog ล่าสุด: %v", err))
			return
		}

		appendLog(fmt.Sprintf("✅ Binlog ล่าสุด: %s, Position: %d", binlogFile, binlogPos))

		// ตั้งค่าการอ่าน Binlog
		cfg := replication.BinlogSyncerConfig{
			ServerID: 100, // ต้องไม่ซ้ำกับ Server ID อื่นในระบบ Replication
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

		// ใช้ context เพื่อควบคุมการอ่าน Event
		ctx := context.Background()

		for {
			ev, err := streamer.GetEvent(ctx)
			if err != nil {
				appendLog(fmt.Sprintf("❌ เกิดข้อผิดพลาดในการอ่าน Binlog: %v", err))
				return
			}

			// แสดงข้อมูล Event
			appendLog(fmt.Sprintf("📌 Event: %T", ev.Event))
		}
	}()

	// Return UI
	return container.NewVBox(
		widget.NewLabel("MySQL Binlog Viewer"),
		logText,
	)
}
