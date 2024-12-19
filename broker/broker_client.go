package broker

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"time"
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

//NotifyPeersOfEachOther sends notifications to all peers about each other via HTTP.
// func NotifyPeersOfEachOther(ll *LinkedList) {
// 	current := ll.Head
// 	if ll.Head == nil {
// 		fmt.Println("List is empty")
// 		return
// 	}
// 	for {
// 		ipAddr := current.IpAddress
// 		if current.Next.IpAddress != ipAddr {
// 			peerIP := current.Next.IpAddress
// 			url := fmt.Sprintf("http://%s/notify", ipAddr)
// 			data := map[string]string{
// 				"peer_ip": peerIP,
// 			}
// 			jsonData, err := json.Marshal(data)
// 			if err != nil {
// 				fmt.Printf("Error marshalling data: %v\n", err)
// 				current = current.Next
// 				continue
// 			}

// 			req, err := http.NewRequest("POST", url, bytes.NewBuffer(jsonData))
// 			if err != nil {
// 				fmt.Printf("Error creating request: %v\n", err)
// 				current = current.Next
// 				continue
// 			}
// 			req.Header.Set("Content-Type", "application/json")

// 			client := &http.Client{}
// 			resp, err := client.Do(req)
// 			if err != nil {
// 				fmt.Printf("Error sending request: %v\n", err)
// 				current = current.Next
// 				continue
// 			}
// 			resp.Body.Close()

// 			if resp.StatusCode != http.StatusOK {
// 				fmt.Printf("Failed to notify peer, status code: %d\n", resp.StatusCode)
// 			}
// 		}

// 		current = current.Next
// 		if current == ll.Head {
// 			break // Completed a full circle
// 		}
// 	}
// }

func NotifyPeersOfEachOther(ll *LinkedList) {
	// Check if the list is empty
	if ll.Head == nil {
		fmt.Println("Peer list is empty. No notifications sent.")
		return
	}

	// Create a snapshot of the linked list
	var peers []*StoreNode
	current := ll.Head
	for {
		peers = append(peers, current)
		current = current.Next
		if current == ll.Head {
			break // Completed a full circle
		}
	}

	// Notify each peer about the next peer
	for _, peer := range peers {
		ipAddr := peer.IpAddress
		nextPeerIP := peer.Next.IpAddress

		// Skip notification if IP addresses are invalid or identical
		if ipAddr == "" || nextPeerIP == "" {
			fmt.Printf("Skipping notification for invalid IPs: current=%s, next=%s\n", ipAddr, nextPeerIP)
			continue
		}
		if ipAddr == nextPeerIP {
			fmt.Printf("Skipping notification: current IP (%s) is the same as next IP (%s)\n", ipAddr, nextPeerIP)
			continue
		}

		// Prepare the notification payload
		url := fmt.Sprintf("http://%s/notify", ipAddr)
		data := map[string]string{"peer_ip": nextPeerIP}
		jsonData, err := json.Marshal(data)
		if err != nil {
			fmt.Printf("Error marshalling data for %s: %v\n", ipAddr, err)
			continue
		}

		// Create and send the HTTP request
		req, err := http.NewRequest("POST", url, bytes.NewBuffer(jsonData))
		if err != nil {
			fmt.Printf("Error creating request to %s: %v\n", ipAddr, err)
			continue
		}
		req.Header.Set("Content-Type", "application/json")

		client := &http.Client{Timeout: 10 * time.Second} // Set timeout to prevent hanging requests
		resp, err := client.Do(req)
		if err != nil {
			fmt.Printf("Error sending request to %s: %v\n", ipAddr, err)
			continue
		}
		resp.Body.Close()

		// Handle response status
		if resp.StatusCode != http.StatusOK {
			fmt.Printf("Failed to notify peer at %s, status code: %d\n", ipAddr, resp.StatusCode)
		} else {
			fmt.Printf("Successfully notified peer at %s about %s\n", ipAddr, nextPeerIP)
		}
	}
}

func StartPeriodicSnapshot(kvstore_ip string, interval string) error {
	// Ensure the interval parameter is provided
	if interval == "" {
		return fmt.Errorf("interval parameter cannot be empty")
	}

	// Create the URL with the interval parameter
	url := fmt.Sprintf("http://%s/start-snapshots?interval=%s", kvstore_ip, interval)

	// Create and send the HTTP request
	resp, err := http.Get(url)
	if err != nil {
		return fmt.Errorf("error sending periodic snapshots request: %v", err)
	}
	defer resp.Body.Close()

	// Check for a successful response
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("error response from store starting snapshots: %s", resp.Status)
	}

	return nil
}
