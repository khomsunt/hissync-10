package ui

import (
	"fmt"

	"fyne.io/fyne/v2/theme"
	"fyne.io/fyne/v2/widget"
)

func CreateToolbar() *widget.Toolbar {
    return widget.NewToolbar(
        widget.NewToolbarAction(theme.ContentAddIcon(), func() {
            fmt.Println("Add button clicked")
        }),
        widget.NewToolbarAction(theme.ContentRemoveIcon(), func() {
            fmt.Println("Remove button clicked")
        }),
        widget.NewToolbarSpacer(),
        widget.NewToolbarAction(theme.DocumentSaveIcon(), func() {
            fmt.Println("Save button clicked")
        }),
        widget.NewToolbarAction(theme.MailSendIcon(), func() {
            fmt.Println("Send button clicked")
        }),
    )
}
