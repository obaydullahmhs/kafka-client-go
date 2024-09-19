package app

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"
	"strings"

	"github.com/IBM/sarama"
	"github.com/sirupsen/logrus"
)

type Config struct {
	Brokers          []string
	UserName         string
	Password         string
	SecurityProtocol string
	SaslMechanism    string
	CaLocation       string
	CertLocation     string
	KeyLocation      string
	InSecureTLS      bool

	Log *logrus.Logger
}

func envWithAlias(key string, alias string) string {
	if alias == "" {
		return key
	}
	return fmt.Sprintf("%s_%s", alias, key)
}

// NewConfigFromEnv collects config from env variables
func NewConfigFromEnv(alias string) *Config {

	return &Config{
		Brokers:          strings.Split(os.Getenv(envWithAlias("KAFKA_BROKERS", alias)), ","),
		UserName:         os.Getenv(envWithAlias("KAFKA_USERNAME", alias)),
		Password:         os.Getenv(envWithAlias("KAFKA_PASSWORD", alias)),
		SecurityProtocol: os.Getenv(envWithAlias("KAFKA_SECURITY_PROTOCOL", alias)),
		SaslMechanism:    os.Getenv(envWithAlias("KAFKA_SASL_MECHANISM", alias)),
		CaLocation:       os.Getenv(envWithAlias("KAFKA_CA_LOCATION", alias)),
		CertLocation:     os.Getenv(envWithAlias("KAFKA_CERT_LOCATION", alias)),
		KeyLocation:      os.Getenv(envWithAlias("KAFKA_KEY_LOCATION", alias)),
		InSecureTLS:      os.Getenv(envWithAlias("KAFKA_INSECURE_TLS", alias)) == "true",

		Log: logrus.New(),
	}
}

func NewConfig(alias string) *Config {
	return NewConfigFromEnv(alias)
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
