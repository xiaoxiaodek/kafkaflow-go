package kafkaflow

type SecurityProtocol string

const (
	SecurityProtocolPlaintext    SecurityProtocol = "plaintext"
	SecurityProtocolSsl          SecurityProtocol = "ssl"
	SecurityProtocolSaslPlaintext SecurityProtocol = "sasl_plaintext"
	SecurityProtocolSaslSsl      SecurityProtocol = "sasl_ssl"
)

type SaslMechanism string

const (
	SaslMechanismPlain       SaslMechanism = "PLAIN"
	SaslMechanismScramSha256 SaslMechanism = "SCRAM-SHA-256"
	SaslMechanismScramSha512 SaslMechanism = "SCRAM-SHA-512"
	SaslMechanismOAuthBearer SaslMechanism = "OAUTHBEARER"
)

type SecurityConfig struct {
	Protocol SecurityProtocol
	Sasl     *SaslConfig
	Ssl      *SslConfig
}

type SaslConfig struct {
	Mechanism SaslMechanism
	Username  string
	Password  string
}

type SslConfig struct {
	CaLocation string
}
