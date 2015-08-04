// +build linux

package amqp

import (
	"bytes"
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
}

type AMQPMessage struct {
	Message   string     `json:"message"`
	Version   string     `json:"@version"`
	Timestamp time.Time  `json:"@timestamp"`
	Tags      AMQPFields `json:"tags"`
	Host      string     `json:"host"`
	Path      *string    `json:"path"`
}

type AMQPFields struct {
	Hostname      string
	ContainerId   string
	ContainerName string
	ImageId       string
	ImageName     string
	Command       string
	Tag           string
	Created       string
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

	return &AMQPLogger{
		ctx:    ctx,
		fields: fields,
	}, nil
}

func (s *AMQPLogger) Log(msg *logger.Message) error {
	// remove trailing and leading whitespace
	short := bytes.TrimSpace([]byte(msg.Line))

	level := "INFO"
	if msg.Source == "stderr" {
		level = "ERROR"
	}

	m := AMQPMessage{
		Version:   "1",
		Host:      s.fields.hostname,
		Message:   string(short),
		Timestamp: time.Now(),
		Path:      s.fields.containerId,
		Tag:       s.fields,
	}

	connectUrl := "amqp://" + s.ctx.Config["amqp-username"] + ":" + s.ctx.Config["amqp-password"] + "@" + s.ctx.Config["amqp-host"] + ":" + s.ctx.Config["amqp-port"] + "/" + s.ctx.Config["amqp-vhost"]

	conn, err := amqp.Dial(connectUrl)
	if err != nil {
		fmt.Errorf("Could not connect to AMQP server", err)
	}

	defer conn.Close()

	c, err := conn.Channel()
	if err != nil {
		fmt.Errorf("Could not open channel", err)
	}

	err = c.ExchangeDeclare(s.ctx.Config["amqp-exchange"], "direct", true, false, false, false, nil)
	if err != nil {
		fmt.Errorf("Could not create exchange")
	}

	_, err = c.QueueDeclare(s.ctx.Config["amqp-queue"], true, false, false, false, nil)
	if err != nil {
		fmt.Errorf("Could not create queue", err)
	}

	err = c.QueueBind(s.ctx.Config["amqp-queue"], s.ctx.Config["amqp-routingkey"], s.ctx.Config["amqp-exchange"], false, nil)
	if err != nil {
		fmt.Errorf("Could not bind queue to exchange", err)
	}

	messagejson, err := json.Marshal(m)
	if err != nil {
		emit("Could not serialise event", err)
	}

	amqpmsg := amqp.Publishing{
		DeliveryMode: amqp.Persistent,
		Timestamp:    time.Now(),
		ContentType:  "application/json",
		Body:         messagejson,
	}

	err = c.Publish(s.ctx.Config["amqp-exchange"], s.ctx.Config["amqp-routingkey"], false, false, amqpmsg)
	if err != nil {
		fmt.Errorf("Could not send message", err)
	}

	return nil
}

func (s *AMQPLogger) Close() error {
	return s.writer.Close()
}

func (s *AMQPLogger) Name() string {
	return name
}

func ValidateLogOpt(cfg map[string]string) error {
	for key := range cfg {
		switch key {
		case "amqp-address":
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
