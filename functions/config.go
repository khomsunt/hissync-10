package config

import (
	"encoding/json"
	"fmt"
	"os"
)

// Config โครงสร้างของ config.json
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

// LoadConfig โหลดการตั้งค่าจาก config.json
func LoadConfig(filePath string) (*Config, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return nil, fmt.Errorf("ไม่สามารถเปิดไฟล์ config.json: %v", err)
	}
	defer file.Close()

	config := &Config{}
	decoder := json.NewDecoder(file)
	err = decoder.Decode(config)
	if err != nil {
		return nil, fmt.Errorf("ไม่สามารถอ่าน config.json: %v", err)
	}

	return config, nil
}
