// +build linux

package amqp

import (
	"bytes"
	"encoding/json"
	"fmt"
	"time"

	"github.com/Sirupsen/logrus"
	"github.com/docker/docker/daemon/logger"
	"github.com/streadway/amqp"
)

const name = "amqp"

type AMQPLogger struct {
	ctx    logger.Context
	fields AMQPFields
	conn   *amqp.Connection
	c      *amqp.Channel
}

type AMQPMessage struct {
	Message   string     `json:"message"`
	Version   string     `json:"@version"`
	Timestamp time.Time  `json:"@timestamp"`
	Tags      AMQPFields `json:"tags"`
	Host      string     `json:"host"`
	Path      string     `json:"path"`
}

type AMQPFields struct {
	Hostname      string
	ContainerId   string
	ContainerName string
	ImageId       string
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

func New(ctx logger.Context) (logger.Logger, error) {
	// collect extra data for AMQP message
	hostname, err := ctx.Hostname()
	if err != nil {
		return nil, fmt.Errorf("amqp: cannot access hostname to set source field")
	}

	// remove trailing slash from container name
	containerName := bytes.TrimLeft([]byte(ctx.ContainerName), "/")

	fields := AMQPFields{
		Hostname:      hostname,
		ContainerId:   ctx.ContainerID,
		ContainerName: string(containerName),
		ImageId:       ctx.ContainerImageID,
		ImageName:     ctx.ContainerImageName,
		Command:       ctx.Command(),
		Tag:           ctx.Config["amqp-tag"],
		Created:       ctx.ContainerCreated,
	}

	connectUrl := "amqp://" + ctx.Config["amqp-username"] + ":" + ctx.Config["amqp-password"] + "@" + ctx.Config["amqp-host"] + ":" + ctx.Config["amqp-port"] + "/" + ctx.Config["amqp-vhost"]

	logrus.Infof("Connecting to AMQP: %s", connectUrl)

	conn, err := amqp.Dial(connectUrl)
	if err != nil {
		fmt.Errorf("Could not connect to AMQP server", err)
	}

	c, err := conn.Channel()
	if err != nil {
		fmt.Errorf("Could not open channel", err)
	}

	err = c.ExchangeDeclare(ctx.Config["amqp-exchange"], "direct", true, false, false, false, nil)
	if err != nil {
		fmt.Errorf("Could not create exchange")
	}

	_, err = c.QueueDeclare(ctx.Config["amqp-queue"], true, false, false, false, nil)
	if err != nil {
		fmt.Errorf("Could not create queue", err)
	}

	err = c.QueueBind(ctx.Config["amqp-queue"], ctx.Config["amqp-routingkey"], ctx.Config["amqp-exchange"], false, nil)
	if err != nil {
		fmt.Errorf("Could not bind queue to exchange", err)
	}

	return &AMQPLogger{
		ctx:    ctx,
		fields: fields,
		conn:   conn,
		c:      c,
	}, nil
}

func (s *AMQPLogger) Log(msg *logger.Message) error {
	// remove trailing and leading whitespace
	short := bytes.TrimSpace([]byte(msg.Line))

	//level := "INFO"
	//if msg.Source == "stderr" {
	//		level = "ERROR"
	//	}

	m := AMQPMessage{
		Version:   "1",
		Host:      s.fields.Hostname,
		Message:   string(short),
		Timestamp: time.Now(),
		Path:      s.fields.ContainerId,
		Tags:      s.fields,
	}

	messagejson, err := json.Marshal(m)
	if err != nil {
		fmt.Errorf("Could not serialise event", err)
	}

	amqpmsg := amqp.Publishing{
		DeliveryMode: amqp.Persistent,
		Timestamp:    time.Now(),
		ContentType:  "application/json",
		Body:         messagejson,
	}

	err = s.c.Publish(s.ctx.Config["amqp-exchange"], s.ctx.Config["amqp-routingkey"], false, false, amqpmsg)
	if err != nil {
		fmt.Errorf("Could not send message", err)
	}

	return nil
}

func (s *AMQPLogger) Close() error {
	return s.conn.Close()
}

func (s *AMQPLogger) Name() string {
	return name
}

func ValidateLogOpt(cfg map[string]string) error {
	for key := range cfg {
		switch key {
		case "amqp-host":
		case "amqp-port":
		case "amqp-vhost":
		case "amqp-username":
		case "amqp-password":
		case "amqp-exchange":
		case "amqp-routingkey":
		case "amqp-tag":
		default:
			return fmt.Errorf("unknown log opt '%s' for amqp log driver", key)
		}
	}
	return nil
}
