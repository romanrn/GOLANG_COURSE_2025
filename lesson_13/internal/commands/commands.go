package commands

const (
	PingCommandName             string = "PING"
	CreateCollectionCommandName string = "CREATE_COLLECTION"
	GetCollectionCommandName    string = "GET_COLLECTION"
	DeleteCollectionCommandName string = "DELETE_COLLECTION"
	PutCommandName              string = "PUT"
	GetCommandName              string = "GET"
	DeleteCommandName           string = "DELETE"
	ListCommandName             string = "LIST"
)

type PutCommandRequestPayload struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

type PutCommandResponsePayload struct{}

type GetCommandRequestPayload struct {
	Key string `json:"key"`
}

type GetCommandResponsePayload struct {
	Value string `json:"value"`
	Ok    bool   `json:"ok"`
}

type DeleteCommandRequestPayload struct {
	Key string `json:"key"`
}

type DeleteCommandResponsePayload struct {
	Ok bool `json:"ok"`
}
