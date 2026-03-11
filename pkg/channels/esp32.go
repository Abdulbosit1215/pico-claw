package channels

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"time"

	"github.com/sipeed/picoclaw/pkg/bus"
	"github.com/sipeed/picoclaw/pkg/config"
	"github.com/sipeed/picoclaw/pkg/logger"
)

// ESP32Channel is a TCP server channel that communicates with ESP32 devices.
// The ESP32 connects to this server over WiFi and exchanges JSON messages.
type ESP32Channel struct {
	*BaseChannel
	config     config.ESP32Config
	listener   net.Listener
	clients    map[net.Conn]bool
	clientsMux sync.RWMutex
	running    bool
}

// ESP32Message is the JSON message format used by the ESP32 device.
type ESP32Message struct {
	Type      string                 `json:"type"`
	DeviceID  string                 `json:"device_id"`
	Timestamp float64                `json:"timestamp"`
	Data      map[string]interface{} `json:"data"`
}

func NewESP32Channel(cfg config.ESP32Config, bus *bus.MessageBus) (*ESP32Channel, error) {
	base := NewBaseChannel("esp32", cfg, bus, cfg.AllowFrom)

	return &ESP32Channel{
		BaseChannel: base,
		config:      cfg,
		clients:     make(map[net.Conn]bool),
		running:     false,
	}, nil
}

func (c *ESP32Channel) Start(ctx context.Context) error {
	logger.InfoC("esp32", "Starting ESP32 channel server")

	addr := fmt.Sprintf("%s:%d", c.config.Host, c.config.Port)
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("failed to listen on %s: %w", addr, err)
	}

	c.listener = listener
	c.setRunning(true)

	logger.InfoCF("esp32", "ESP32 server listening", map[string]interface{}{
		"host": c.config.Host,
		"port": c.config.Port,
	})

	go c.acceptConnections(ctx)

	return nil
}

func (c *ESP32Channel) acceptConnections(ctx context.Context) {
	logger.DebugC("esp32", "Starting connection acceptor")

	for {
		select {
		case <-ctx.Done():
			logger.InfoC("esp32", "Stopping connection acceptor")
			return
		default:
			conn, err := c.listener.Accept()
			if err != nil {
				if c.running {
					logger.ErrorCF("esp32", "Failed to accept connection", map[string]interface{}{
						"error": err.Error(),
					})
				}
				return
			}

			logger.InfoCF("esp32", "New connection from ESP32 device", map[string]interface{}{
				"remote_addr": conn.RemoteAddr().String(),
			})

			c.clientsMux.Lock()
			c.clients[conn] = true
			c.clientsMux.Unlock()

			go c.handleConnection(conn, ctx)
		}
	}
}

func (c *ESP32Channel) handleConnection(conn net.Conn, ctx context.Context) {
	logger.DebugC("esp32", "Handling ESP32 connection")

	defer func() {
		conn.Close()
		c.clientsMux.Lock()
		delete(c.clients, conn)
		c.clientsMux.Unlock()
		logger.DebugC("esp32", "Connection closed")
	}()

	decoder := json.NewDecoder(conn)

	for {
		select {
		case <-ctx.Done():
			return
		default:
			var msg ESP32Message
			if err := decoder.Decode(&msg); err != nil {
				if !errors.Is(err, io.EOF) {
					logger.ErrorCF("esp32", "Failed to decode message", map[string]interface{}{
						"error": err.Error(),
					})
				}
				return
			}

			c.processMessage(msg, conn)
		}
	}
}

func (c *ESP32Channel) processMessage(msg ESP32Message, conn net.Conn) {
	switch msg.Type {
	case "sensor_data":
		c.handleSensorData(msg)
	case "alert":
		c.handleAlert(msg)
	case "heartbeat":
		logger.DebugCF("esp32", "Received heartbeat", map[string]interface{}{
			"device_id": msg.DeviceID,
		})
	case "status":
		c.handleStatusUpdate(msg)
	default:
		logger.WarnCF("esp32", "Unknown message type", map[string]interface{}{
			"type":      msg.Type,
			"device_id": msg.DeviceID,
		})
	}
}

func (c *ESP32Channel) handleSensorData(msg ESP32Message) {
	logger.InfoCF("esp32", "Sensor data received", map[string]interface{}{
		"device_id": msg.DeviceID,
		"timestamp": msg.Timestamp,
		"data":      msg.Data,
	})

	senderID := msg.DeviceID
	if senderID == "" {
		senderID = "esp32"
	}
	chatID := senderID

	content := fmt.Sprintf("📡 ESP32 sensor data from %s", senderID)
	for k, v := range msg.Data {
		content += fmt.Sprintf("\n%s: %v", k, v)
	}

	metadata := map[string]string{
		"device_id": senderID,
		"timestamp": fmt.Sprintf("%.0f", msg.Timestamp),
	}
	for k, v := range msg.Data {
		metadata[k] = fmt.Sprintf("%v", v)
	}

	c.HandleMessage(senderID, chatID, content, []string{}, metadata)
}

func (c *ESP32Channel) handleAlert(msg ESP32Message) {
	logger.WarnCF("esp32", "Alert from ESP32 device", map[string]interface{}{
		"device_id": msg.DeviceID,
		"data":      msg.Data,
	})

	senderID := msg.DeviceID
	if senderID == "" {
		senderID = "esp32"
	}
	chatID := senderID

	alertMsg, ok := msg.Data["message"].(string)
	if !ok {
		alertMsg = fmt.Sprintf("%v", msg.Data)
	}

	content := fmt.Sprintf("🚨 Alert from ESP32 device %s: %s", senderID, alertMsg)

	metadata := map[string]string{
		"device_id": senderID,
		"timestamp": fmt.Sprintf("%.0f", msg.Timestamp),
	}

	c.HandleMessage(senderID, chatID, content, []string{}, metadata)
}

func (c *ESP32Channel) handleStatusUpdate(msg ESP32Message) {
	logger.InfoCF("esp32", "Status update from ESP32 device", map[string]interface{}{
		"device_id": msg.DeviceID,
		"status":    msg.Data,
	})
}

func (c *ESP32Channel) Stop(ctx context.Context) error {
	logger.InfoC("esp32", "Stopping ESP32 channel")
	c.setRunning(false)

	if c.listener != nil {
		c.listener.Close()
	}

	c.clientsMux.Lock()
	defer c.clientsMux.Unlock()

	for conn := range c.clients {
		conn.Close()
	}
	c.clients = make(map[net.Conn]bool)

	logger.InfoC("esp32", "ESP32 channel stopped")
	return nil
}

func (c *ESP32Channel) Send(ctx context.Context, msg bus.OutboundMessage) error {
	if !c.IsRunning() {
		return fmt.Errorf("esp32 channel not running")
	}

	c.clientsMux.RLock()
	defer c.clientsMux.RUnlock()

	if len(c.clients) == 0 {
		logger.WarnC("esp32", "No ESP32 devices connected")
		return fmt.Errorf("no connected ESP32 devices")
	}

	response := map[string]interface{}{
		"type":      "command",
		"timestamp": float64(time.Now().UnixMilli()),
		"message":   msg.Content,
		"chat_id":   msg.ChatID,
	}

	data, err := json.Marshal(response)
	if err != nil {
		return fmt.Errorf("failed to marshal response: %w", err)
	}
	data = append(data, '\n')

	var sendErr error
	for conn := range c.clients {
		if _, err := conn.Write(data); err != nil {
			logger.ErrorCF("esp32", "Failed to send to client", map[string]interface{}{
				"client": conn.RemoteAddr().String(),
				"error":  err.Error(),
			})
			sendErr = err
		}
	}

	return sendErr
}
