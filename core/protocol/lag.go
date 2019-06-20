package protocol

// LagMessage is a simulator for Burrow lag message
type LagMessage struct {
	Attachments []struct {
		Color  string  `json:"color"`
		Title  string  `json:"title"`
		Fields []Field `json:"fields"`
	} `json:"attachments"`
}

// Field represents a Field
type Field struct {
	Title string `json:"title"`
	Value string `json:"value"`
	Short bool   `json:"short,omitempty"`
}

// Partition represents a partition
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

// MaxLag represents max partition lag
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

// LagInfo is a customized struct to add an accurate timestamp
type LagInfo struct {
	Lag       LagStatus `json:"lag"`
	Timestamp int64     `json:"timestamp"`
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
