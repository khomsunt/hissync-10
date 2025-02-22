package ui

import (
	"fmt"
	"hissync-10/ui/forms"

	"fyne.io/fyne/v2"
	"fyne.io/fyne/v2/container"
	"fyne.io/fyne/v2/widget"
)

// CreateTopbarMenu สร้างเมนูด้านบนพร้อมเมนูย่อยและเรียกใช้ Connection Form
func CreateTopbarMenu(myApp fyne.App, myWindow fyne.Window, contentContainer *fyne.Container) *fyne.MainMenu {
    // สร้างเมนู File
    newItem := fyne.NewMenuItem("New", func() {
        fmt.Println("New File Created")
    })
    openItem := fyne.NewMenuItem("Open", func() {
        fmt.Println("Open File")
    })
    saveItem := fyne.NewMenuItem("Save", func() {
        fmt.Println("File Saved")
    })
    quitItem := fyne.NewMenuItem("Quit", func() {
        myApp.Quit()
    })

    fileMenu := fyne.NewMenu("File", newItem, openItem, saveItem, quitItem)

    // สร้างเมนู Data พร้อมเมนูย่อย Data Sync.
    dataSyncItem := fyne.NewMenuItem("Data Sync.", func() {
        ShowDataSyncSidebar(contentContainer)
    })

    dataMenu := fyne.NewMenu("Data", dataSyncItem)

    // สร้างเมนู Setting พร้อมเมนูย่อย Connection
    connectionItem := fyne.NewMenuItem("Connection", func() {
        forms.ShowConnectionForm(myWindow) // เรียกใช้ฟอร์มจากไฟล์ connection_form.go
    })

    settingMenu := fyne.NewMenu("Setting", connectionItem)

    // สร้างเมนู Help
    aboutItem := fyne.NewMenuItem("About", func() {
        fmt.Println("About this app")
    })

    helpMenu := fyne.NewMenu("Help", aboutItem)

    // รวมเมนูทั้งหมดเป็น MainMenu โดยจัดลำดับให้ Data มาก่อน Setting
    return fyne.NewMainMenu(fileMenu, dataMenu, settingMenu, helpMenu)
}

// ShowDataSyncSidebar แสดง Sidebar Menu สำหรับ Data Sync.
func ShowDataSyncSidebar(contentContainer *fyne.Container) {
    // สร้าง Sidebar Menu ด้วยเมนู "ข้อมูลค้างส่ง"
    menuButton := widget.NewButton("ข้อมูลค้างส่ง", func() {
        // เมื่อคลิกที่ "ข้อมูลค้างส่ง" ให้แสดงข้อความ "ข้อมูล" ในหน้าจอด้านขวา
        contentContainer.Objects = []fyne.CanvasObject{
            widget.NewLabel("ข้อมูล"),
        }
        contentContainer.Refresh()
    })

    sidebar := container.NewVBox(menuButton)

    // ตั้งค่าให้แสดง Sidebar และเนื้อหาหลัก
    contentContainer.Objects = []fyne.CanvasObject{
        container.NewHSplit(sidebar, widget.NewLabel("โปรดเลือกเมนูทางด้านซ้าย")),
    }
    contentContainer.Refresh()
}
