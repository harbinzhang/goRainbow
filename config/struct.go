package config

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

// LagInfo contains all lag info in all partitions
type LagInfo struct {
	APIKey string `json:"api_key"`
	App    string `json:"app"`
	Block  bool   `json:"block"`
	Events []struct {
		Event struct {
			Severity   string `json:"severity"`
			Cluster    string `json:"cluster"`
			Group      string `json:"group"`
			TotalLag   string `json:"totalLag"`
			Start      string `json:"start"`
			Complete   int    `json:"complete"`
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
