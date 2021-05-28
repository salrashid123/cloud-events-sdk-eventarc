package main

import (
	"context"
	"fmt"

	cloudevents "github.com/cloudevents/sdk-go/v2"
	//auditv1 "github.com/googleapis/google-cloudevents-go/cloud/audit/v1"
	logging "google.golang.org/api/logging/v2"

	pubsubv1 "github.com/googleapis/google-cloudevents-go/cloud/pubsub/v1"

	pscontext "github.com/cloudevents/sdk-go/protocol/pubsub/v2/context"
)

const (
	messagePublishedEventType = "google.cloud.pubsub.topic.v1.messagePublished"
	auditLogEventType         = "google.cloud.audit.log.v1.written"
)

func Receive(ctx context.Context, event cloudevents.Event) error {
	fmt.Printf("  EventID: %s\n", event.ID())
	fmt.Printf("  EventType: %s \n", event.Type())
	fmt.Printf("  Event Context: %+v\n", event.Context)
	fmt.Printf("  Protocol Context: %+v\n", pscontext.ProtocolContextFrom(ctx))

	switch event.Type() {

	case messagePublishedEventType:

		pubsubData := &pubsubv1.MessagePublishedData{}
		if err := event.DataAs(pubsubData); err != nil {
			fmt.Printf("DataAs Error %v", err)
			return err
		}

		fmt.Printf("Pubsub Message  %s\n", string(pubsubData.Message.Data))

	case auditLogEventType:

		//auditData := &auditv1.LogEntryData{}
		auditData := &logging.LogEntry{}
		if err := event.DataAs(auditData); err != nil {
			fmt.Printf("DataAs Error %v", err)
			return fmt.Errorf("Got Data Error: %s\n", err)
		}
		fmt.Printf("Audit auditLogEventType Severity %s\n", auditData.Severity)

	default:
		fmt.Printf("ERROR>>> unknown Event type %s", event.Type())
		return fmt.Errorf("Unknown Event type")
	}
	return nil
}

func main() {

	c, err := cloudevents.NewDefaultClient()
	if err != nil {
		fmt.Printf("failed to create client, %v", err)
	}
	fmt.Println("Starting Server")

	err = c.StartReceiver(context.Background(), Receive)
	if err != nil {
		fmt.Printf("failed to StartReceiver, %v", err)
	}
}
