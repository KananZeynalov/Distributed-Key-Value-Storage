package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
)

func setKeyValue(url string, key string, value string) error {
	data := map[string]string{
		"key":   key,
		"value": value,
	}
	jsonData, err := json.Marshal(data)
	if err != nil {
		return err
	}

	req, err := http.NewRequest("POST", url, bytes.NewBuffer(jsonData))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("failed to set key-value pair, status code: %d", resp.StatusCode)
	}

	return nil
}

func getKeyValue(url string, key string) (string, error) {
	req, err := http.NewRequest("GET", fmt.Sprintf("%s?key=%s", url, key), nil)
	if err != nil {
		return "", err
	}

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("failed to get key-value pair, status code: %d", resp.StatusCode)
	}

	var result map[string]string
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return "", err
	}

	value, ok := result["value"]
	if !ok {
		return "", fmt.Errorf("key not found in response")
	}

	return value, nil
}
