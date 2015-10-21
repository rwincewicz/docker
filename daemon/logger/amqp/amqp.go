// +build linux

package amqp

import (
	"bytes"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"net/url"
	"strings"
	"time"

	"github.com/Sirupsen/logrus"
	"github.com/docker/docker/daemon/logger"
	"github.com/streadway/amqp"
)

const name = "amqp"

type amqpLogger struct {
	ctx        logger.Context
	fields     amqpFields
	connection *amqpConnection
}

// Data structure holding information about the current connection
// with a broker as well as a list of other available brokers
type amqpConnection struct {
	broker     int
	brokerURLs []*amqpBroker
	conn       *amqp.Connection
	c          *amqp.Channel
	conf       <-chan amqp.Confirmation
	err        error
}

// Data structure to hold the connection settings for each broker
type amqpBroker struct {
	BrokerURL  *url.URL
	Exchange   string
	Queue      string
	RoutingKey string
	Tag        string
	CertPath   string
	KeyPath    string
	Confirm    bool
}

// Data structure to store the data for the log message
type amqpMessage struct {
	Message   string     `json:"message"`
	Version   string     `json:"@version"`
	Timestamp time.Time  `json:"@timestamp"`
	Tags      amqpFields `json:"tags"`
	Host      string     `json:"host"`
	Path      string     `json:"path"`
}

// Data about the host and container that is required when sending
// the log message
type amqpFields struct {
	Hostname      string
	ContainerID   string
	ContainerName string
	ImageID       string
	ImageName     string
	Command       string
	Tag           string
	Created       time.Time
}

func init() {
	if err := logger.RegisterLogDriver(name, New); err != nil {
		logrus.Fatal(err)
	}
	if err := logger.RegisterLogOptValidator(name, ValidateLogOpt); err != nil {
		logrus.Fatal(err)
	}
}

// New creates a new amqp logger using the configuration passed in the
// context.
func New(ctx logger.Context) (logger.Logger, error) {
	// collect extra data for AMQP message
	hostname, err := ctx.Hostname()
	if err != nil {
		return nil, fmt.Errorf("Cannot access hostname to set source field: %v", err)
	}

	logrus.Infof("URLs: %v", ctx.Config["amqp-url"])

	// remove trailing slash from container name
	containerName := bytes.TrimLeft([]byte(ctx.ContainerName), "/")

	fields := amqpFields{
		Hostname:      hostname,
		ContainerID:   ctx.ContainerID,
		ContainerName: string(containerName),
		ImageID:       ctx.ContainerImageID,
		ImageName:     ctx.ContainerImageName,
		Command:       ctx.Command(),
		Tag:           ctx.Config["amqp-tag"],
		Created:       ctx.ContainerCreated,
	}

	broker := 0

	connection, err := connect(ctx, broker)
	if err != nil {
		return nil, fmt.Errorf("Could not connect: %v", err)
	}

	return &amqpLogger{
		ctx:        ctx,
		fields:     fields,
		connection: connection,
	}, nil
}

func connect(ctx logger.Context, broker int) (connection *amqpConnection, err error) {
	var conn *amqp.Connection
	var c *amqp.Channel

	var conf <-chan amqp.Confirmation
	connectURLs := parseURL(ctx.Config["amqp-url"])
	logrus.Info(connectURLs)
	if err != nil {
		logrus.Errorf("Invalid AMQP URL - %v", err)
		return nil, err
	}

	if connectURLs[0].Scheme == "amqps" {
		logrus.Infof("Connecting to AMQP: %s", connectURLs[broker])

		cfg := new(tls.Config)
		if cert, err := tls.LoadX509KeyPair(ctx.Config["amqp-cert"], ctx.Config["amqp-key"]); err == nil {
			cfg.Certificates = append(cfg.Certificates, cert)
		}
		conn, err = amqp.DialTLS(connectURLs[broker].String(), cfg)
		if err != nil {
			logrus.Errorf("Could not connect to AMQP server - %v", err)
			return nil, err
		}
	} else {
		logrus.Infof("Connecting to AMQP: %s", connectURLs[broker])
		conn, err = amqp.Dial(connectURLs[0].String())
		if err != nil {
			logrus.Errorf("Could not connect to AMQP server - %v", err)
			return nil, err
		}
	}

	c, err = conn.Channel()
	if err != nil {
		logrus.Errorf("Could not open channel - %v", err)
		return nil, err
	}

	err = c.ExchangeDeclare(currentBroker.Exchange, "direct", true, false, false, false, nil)
	if err != nil {
		logrus.Errorf("Could not create exchange - %v", err)
		return nil, err
	}

	_, err = c.QueueDeclare(currentBroker.Queue, true, false, false, false, nil)
	if err != nil {
		logrus.Errorf("Could not create queue - %v", err)
		return nil, err
	}

	err = c.QueueBind(currentBroker.Queue, currentBroker.RoutingKey, currentBroker.Exchange, false, nil)
	if err != nil {
		logrus.Errorf("Could not bind queue to exchange - %v", err)
		return nil, err
	}

	logrus.Info("Connection set up")
	return &amqpConnection{
		broker:     broker,
		brokerURLs: brokerURLs,
		conn:       conn,
		c:          c,
		conf:       conf,
		err:        err,
	}, nil
}

// If the connection fails at any point then close the current connection and
// try to connect to the next broker in the list.
func reconnect(s *amqpLogger) (err error) {
	logrus.Warn("Unable to send message to AMQP broker")
	logrus.Info("Attempting to reconnect")
	s.Close()
	// Move to the next broker in the list. If at the end of the
	// list then go back to the start
	if len(s.connection.brokerURLs) > s.connection.broker+1 {
		s.connection.broker++
	} else {
		s.connection.broker = 0
	}
	connection, err := connect(s.ctx, s.connection.broker)
	if err != nil {
		logrus.Errorf("Could not reconnect: %v", err)
		return err
	} else {
		logrus.Info("Reconnected")
		s.connection = connection
		return nil
	}
}

// Take the log message and publish it to the currently connected broker
func (s *amqpLogger) Log(msg *logger.Message) (err error) {
	// Remove trailing and leading whitespace
	short := bytes.TrimSpace([]byte(msg.Line))

	if s.connection == nil {
		err = reconnect(s)
		if err != nil {
			return err
		}
	}

	if string(short) != "" {
		m := amqpMessage{
			Version:   "1",
			Host:      s.fields.Hostname,
			Message:   string(short),
			Timestamp: time.Now(),
			Path:      s.fields.ContainerID,
			Tags:      s.fields,
		}

		messagejson, err := json.Marshal(m)
		if err != nil {
			logrus.Errorf("Could not serialise event - %v", err)
			return err
		}

		amqpmsg := amqp.Publishing{
			DeliveryMode: amqp.Persistent,
			Timestamp:    time.Now(),
			ContentType:  "application/json",
			Body:         messagejson,
		}

		if s.ctx.Config["amqp-confirm"] == "true" && s.connection != nil {
			defer confirmOne(s.connection.conf)
		}

		err = s.connection.c.Publish(s.ctx.Config["amqp-exchange"], s.ctx.Config["amqp-routingkey"], false, false, amqpmsg)
		if err != nil {
			err = reconnect(s)
			if err != nil {
				return err
			}
		}

	}
	return nil
}

// Cleanly close the connection with the broker.
func (s *amqpLogger) Close() error {
	logrus.Info("Closing connection")
	if s.connection != nil {
		if s.connection.c != nil {
			s.connection.c.Close()
		}
		if s.connection.conn != nil {
			s.connection.conn.Close()
		}
	}
	return nil
}

func (s *amqpLogger) Name() string {
	return name
}

// ValidateLogOpt checks for the amqp-specific log options
func ValidateLogOpt(cfg map[string]string) error {
	for key := range cfg {
		switch key {
		case "amqp-cert":
		case "amqp-key":
		case "amqp-url":
		case "amqp-exchange":
		case "amqp-queue":
		case "amqp-routingkey":
		case "amqp-tag":
		case "amqp-confirm":
		case "amqp-settings":
		default:
			return fmt.Errorf("unknown log opt '%s' for amqp log driver", key)
		}
	}
	return nil
}
