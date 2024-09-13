package app

import (
	"crypto/tls"
	"crypto/x509"
	"os"
	"strconv"
	"strings"

	"github.com/IBM/sarama"
	"github.com/sirupsen/logrus"
)

type Config struct {
	Brokers          []string
	Topic            string
	UserName         string
	Password         string
	SecurityProtocol string
	SaslMechanism    string
	CaLocation       string
	CertLocation     string
	KeyLocation      string
	MaxMessages      int64
	SleepTimeMS      int64
	InSecureTLS      bool

	Log *logrus.Logger
}

// NewConfigFromEnv collects config from env variables
func NewConfigFromEnv() *Config {
	logger := logrus.New()
	var maxMessages string
	maxMessages, e := os.LookupEnv("KAFKA_MAX_MESSAGES")
	if !e {
		maxMessages = strconv.Itoa(1000)
	}
	maxMessagesInt, err := strconv.ParseInt(maxMessages, 10, 64)
	if err != nil {
		logger.Error("failed to parse max messages", err)
		os.Exit(0)
	}
	if maxMessagesInt == -1 {
		maxMessagesInt = 9223372036854775807
	}

	sleepTimeMs, e := os.LookupEnv("KAFKA_SLEEP_TIME_MS")
	if !e {
		sleepTimeMs = strconv.Itoa(1000)
	}
	sleepTimeMsInt, err := strconv.ParseInt(sleepTimeMs, 10, 64)
	if err != nil {
		logger.Error("failed to parse sleep time", err)
		os.Exit(0)
	}

	return &Config{
		Brokers:          strings.Split(os.Getenv("KAFKA_BROKERS"), ","),
		Topic:            os.Getenv("KAFKA_TOPIC"),
		UserName:         os.Getenv("KAFKA_USERNAME"),
		Password:         os.Getenv("KAFKA_PASSWORD"),
		SecurityProtocol: os.Getenv("KAFKA_SECURITY_PROTOCOL"),
		SaslMechanism:    os.Getenv("KAFKA_SASL_MECHANISM"),
		CaLocation:       os.Getenv("KAFKA_CA_LOCATION"),
		CertLocation:     os.Getenv("KAFKA_CERT_LOCATION"),
		KeyLocation:      os.Getenv("KAFKA_KEY_LOCATION"),
		InSecureTLS:      os.Getenv("KAFKA_INSECURE_TLS") == "true",
		MaxMessages:      maxMessagesInt,
		SleepTimeMS:      sleepTimeMsInt,

		Log: logger,
	}
}

func NewConfig() *Config {
	return NewConfigFromEnv()
}

func (c *Config) GetConfig() *sarama.Config {
	config := sarama.NewConfig()
	if strings.Contains(c.SecurityProtocol, "SASL") {
		config.Net.SASL.Enable = true
		config.Net.SASL.User = c.UserName
		config.Net.SASL.Password = c.Password
		config.Net.SASL.Mechanism = sarama.SASLMechanism(c.SaslMechanism)
	}

	if strings.Contains(c.SecurityProtocol, "SSL") {
		if c.InSecureTLS {
			config.Net.TLS.Enable = true
			config.Net.TLS.Config = &tls.Config{
				InsecureSkipVerify: true,
			}
		} else {
			// get tls cert, clientCA and rootCA for tls config
			clientCA := x509.NewCertPool()
			rootCA := x509.NewCertPool()

			tlsCert, err := os.ReadFile(c.CertLocation)
			if err != nil {
				c.Log.Error("failed to read client certificate", err)
				return nil
			}
			tlsKey, err := os.ReadFile(c.KeyLocation)
			if err != nil {
				c.Log.Error("failed to read client key", err)
				return nil
			}

			crt, err := tls.X509KeyPair(tlsCert, tlsKey)
			if err != nil {
				c.Log.Error("failed to parse private key pair", err)
				return nil
			}
			ca, err := os.ReadFile(c.CaLocation)
			if err != nil {
				c.Log.Error("failed to read CA certificate", err)
				return nil
			}
			clientCA.AppendCertsFromPEM(ca)
			rootCA.AppendCertsFromPEM(ca)

			config.Net.TLS.Enable = true
			config.Net.TLS.Config = &tls.Config{
				Certificates: []tls.Certificate{crt},
				ClientAuth:   tls.RequireAndVerifyClientCert,
				ClientCAs:    clientCA,
				RootCAs:      rootCA,
				MaxVersion:   tls.VersionTLS13,
			}
		}
	}

	return config
}
