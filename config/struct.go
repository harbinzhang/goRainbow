package config

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

// Field represents a Field
type Field struct {
	Title string `json:"title"`
	Value string `json:"value"`
	Short bool   `json:"short,omitempty"`
}

// LagMessage is a simulator for Burrow lag message
type LagMessage struct {
	Attachments []struct {
		Color  string  `json:"color"`
		Title  string  `json:"title"`
		Fields []Field `json:"fields"`
	} `json:"attachments"`
}

type Partition struct {
	Topic     string `json:"topic"`
	Partition int    `json:"partition"`
	Owner     string `json:"owner"`
	Status    string `json:"status"`
	Start     struct {
		Offset    int   `json:"offset"`
		Timestamp int64 `json:"timestamp"`
		Lag       int   `json:"lag"`
	} `json:"start"`
	End struct {
		Offset    int   `json:"offset"`
		Timestamp int64 `json:"timestamp"`
		Lag       int   `json:"lag"`
	} `json:"end"`
	CurrentLag int     `json:"current_lag"`
	Complete   float64 `json:"complete"`
}

type MaxLag struct {
	Topic     string `json:"topic"`
	Partition int    `json:"partition"`
	Owner     string `json:"owner"`
	Status    string `json:"status"`
	Start     struct {
		Offset    int   `json:"offset"`
		Timestamp int64 `json:"timestamp"`
		Lag       int   `json:"lag"`
	} `json:"start"`
	End struct {
		Offset    int   `json:"offset"`
		Timestamp int64 `json:"timestamp"`
		Lag       int   `json:"lag"`
	} `json:"end"`
	CurrentLag int     `json:"current_lag"`
	Complete   float64 `json:"complete"`
}

// LagStatus is "status" from v3/kafka/{cluster}/consumer/{consumer}/lag
type LagStatus struct {
	Error   bool   `json:"error"`
	Message string `json:"message"`
	Status  struct {
		Cluster        string      `json:"cluster"`
		Group          string      `json:"group"`
		Status         string      `json:"status"`
		Complete       float64     `json:"complete"`
		Partitions     []Partition `json:"partitions"`
		PartitionCount int         `json:"partition_count"`
		Maxlag         MaxLag      `json:"maxlag"`
		Totallag       int         `json:"totallag"`
	} `json:"status"`
	Request struct {
		URL  string `json:"url"`
		Host string `json:"host"`
	} `json:"request"`
}

// TopicOffset is for topic/offset
type TopicOffset struct {
	Error   bool   `json:"error"`
	Message string `json:"message"`
	Offsets []int  `json:"offsets"`
	Request struct {
		URL  string `json:"url"`
		Host string `json:"host"`
	} `json:"request"`
}

// LagInfo contains all lag info in all partitions
type LagInfo struct {
	APIKey string `json:"api_key"`
	App    string `json:"app"`
	Block  bool   `json:"block"`
	Events []struct {
		Event struct {
			Severity        string  `json:"severity"`
			Cluster         string  `json:"cluster"`
			Group           string  `json:"group"`
			TotalLag        string  `json:"totalLag"`
			Start           string  `json:"start"`
			Complete        float64 `json:"complete"`
			MaxLagPartition struct {
				Topic     string `json:"topic"`
				Partition int    `json:"partition"`
				Owner     string `json:"owner"`
				Status    string `json:"status"`
				Start     struct {
					Offset    int   `json:"offset"`
					Timestamp int64 `json:"timestamp"`
					Lag       int   `json:"lag"`
				} `json:"start"`
				End struct {
					Offset    int   `json:"offset"`
					Timestamp int64 `json:"timestamp"`
					Lag       int   `json:"lag"`
				} `json:"end"`
				CurrentLag int     `json:"current_lag"`
				Complete   float64 `json:"complete"`
			} `json:"maxLagPartition"`
			Partitions []struct {
				Topic     string `json:"topic"`
				Partition int    `json:"partition"`
				Owner     string `json:"owner"`
				Status    string `json:"status"`
				Start     struct {
					Offset    int   `json:"offset"`
					Timestamp int64 `json:"timestamp"`
					Lag       int   `json:"lag"`
				} `json:"start"`
				End struct {
					Offset    int   `json:"offset"`
					Timestamp int64 `json:"timestamp"`
					Lag       int   `json:"lag"`
				} `json:"end"`
				CurrentLag int     `json:"current_lag"`
				Complete   float64 `json:"complete"`
			} `json:"partitions"`
		} `json:"event"`
	} `json:"events"`
}

// type LagInfo struct {
// 	APIKey string `json:"api_key"`
// 	App    string `json:"app"`
// 	Block  bool   `json:"block"`
// 	Events []struct {
// 		Event struct {
// 			Severity   string  `json:"severity"`
// 			Cluster    string  `json:"cluster"`
// 			Group      string  `json:"group"`
// 			TotalLag   string  `json:"totalLag"`
// 			Start      string  `json:"start"`
// 			Complete   float64 `json:"complete"`
// 			Partitions []struct {
// 				Topic     string `json:"topic"`
// 				Partition int    `json:"partition"`
// 				Owner     string `json:"owner"`
// 				Status    string `json:"status"`
// 				Start     struct {
// 					Offset    int   `json:"offset"`
// 					Timestamp int64 `json:"timestamp"`
// 					Lag       int   `json:"lag"`
// 				} `json:"start"`
// 				End struct {
// 					Offset    int   `json:"offset"`
// 					Timestamp int64 `json:"timestamp"`
// 					Lag       int   `json:"lag"`
// 				} `json:"end"`
// 				CurrentLag int     `json:"current_lag"`
// 				Complete   float64 `json:"complete"`
// 			} `json:"partitions"`
// 		} `json:"event"`
// 	} `json:"events"`
// }
