package node

import "encoding/json"

type Node struct {
	ID          string `json:"id"`
	ServiceName string `json:"service_name"`
	Namespace   string `json:"namespace"`
}

func (n *Node) MarshalBinary() (data []byte, err error) {
	return json.Marshal(n)
}

func (n *Node) UnmarshalBinary(data []byte) error {
	return json.Unmarshal(data, n)
}
