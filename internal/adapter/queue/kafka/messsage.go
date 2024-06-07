package kafka

type ErrorMessage struct {
	OriginPartition int    `json:"originPartition"`
	Topic           string `json:"originTopic"`
	OriginOffset    int64  `json:"originOffset"`
	Error           string `json:"error"`
}
