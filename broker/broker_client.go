package broker

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

// NotifyPeersOfEachOther sends notifications to all peers about each other via HTTP.
func NotifyPeersOfEachOther(ll *LinkedList) {
	current := ll.Head
	if ll.Head == nil {
		fmt.Println("List is empty")
		return
	}
	for {
		ipAddr := current.IpAddress
		if current.Next.IpAddress != ipAddr {
			peerIP := current.Next.IpAddress
			url := fmt.Sprintf("http://%s/notify", ipAddr)
			data := map[string]string{
				"peer_ip": peerIP,
			}
			jsonData, err := json.Marshal(data)
			if err != nil {
				fmt.Printf("Error marshalling data: %v\n", err)
				current = current.Next
				continue
			}

			req, err := http.NewRequest("POST", url, bytes.NewBuffer(jsonData))
			if err != nil {
				fmt.Printf("Error creating request: %v\n", err)
				current = current.Next
				continue
			}
			req.Header.Set("Content-Type", "application/json")

			client := &http.Client{}
			resp, err := client.Do(req)
			if err != nil {
				fmt.Printf("Error sending request: %v\n", err)
				current = current.Next
				continue
			}
			resp.Body.Close()

			if resp.StatusCode != http.StatusOK {
				fmt.Printf("Failed to notify peer, status code: %d\n", resp.StatusCode)
			}
		}

		current = current.Next
		if current == ll.Head {
			break // Completed a full circle
		}
	}
}