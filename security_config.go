package kafkaflow

import "github.com/confluentinc/confluent-kafka-go/v2/kafka"

func ApplySecurityConfig(cm *kafka.ConfigMap, sc *SecurityConfig) {
	if sc == nil {
		return
	}

	switch sc.Protocol {
	case SecurityProtocolSsl:
		cm.SetKey("security.protocol", "SSL")
	case SecurityProtocolSaslPlaintext:
		cm.SetKey("security.protocol", "SASL_PLAINTEXT")
	case SecurityProtocolSaslSsl:
		cm.SetKey("security.protocol", "SASL_SSL")
	}

	if sc.Sasl != nil {
		cm.SetKey("sasl.mechanisms", string(sc.Sasl.Mechanism))
		if sc.Sasl.Username != "" {
			cm.SetKey("sasl.username", sc.Sasl.Username)
		}
		if sc.Sasl.Password != "" {
			cm.SetKey("sasl.password", sc.Sasl.Password)
		}
	}

	if sc.Ssl != nil && sc.Ssl.CaLocation != "" {
		cm.SetKey("ssl.ca.location", sc.Ssl.CaLocation)
	}
}
