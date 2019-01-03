package protocol

// Config struct is for config file load
type Config struct {
	ReportIntervalSeconds int `json:"reportIntervalSeconds"`
	Kafka                 struct {
		BrokerServers string `json:"brokerServers"`
		Topic         string `json:"topic"`
	} `json:"kafka"`
	Translator struct {
		FullClassName string `json:"fullClassName"`
		MetricFormat  string `json:"metricFormat"`
	} `json:"translator"`
	Service struct {
		CustomTags string `json:"customTags"`
		Name       string `json:"name"`
		Department string `json:"department"`
	} `json:"service"`
}
