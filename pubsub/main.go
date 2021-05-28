package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"os"

	cepubsub "github.com/cloudevents/sdk-go/protocol/pubsub/v2"
	pscontext "github.com/cloudevents/sdk-go/protocol/pubsub/v2/context"
	cloudevents "github.com/cloudevents/sdk-go/v2"
	"github.com/cloudevents/sdk-go/v2/event"

	//auditv1 "github.com/googleapis/google-cloudevents-go/cloud/audit/v1"
	pubsubv1 "github.com/googleapis/google-cloudevents-go/cloud/pubsub/v1"
	p "google.golang.org/api/pubsub/v1"

	lg "google.golang.org/api/logging/v2"
)

var (
	projectID, topicID, subID *string
)

const (
	messagePublishedEventType = "google.cloud.pubsub.topic.v1.messagePublished"
	pubSubEventType           = "com.google.cloud.pubsub.topic.publish"
	auditLogEventType         = "google.cloud.audit.log.v1.written"
)

func main() {
	projectID = flag.String("projectID", "", "ProjectID for topic and subscriber")
	topicID = flag.String("topicID", "", "Topic run-events")
	subID = flag.String("subID", "", "Subscription cloud-events-auditlog | cloud-events-pubsub")
	mode := flag.String("mode", "subscribe", "(required for mode=mode) mode=subscribe|publish")
	message := flag.String("message", "fooo", "message to publish")

	flag.Parse()

	if *mode == "subscribe" {
		err := pullMsgs(*projectID, *subID)
		if err != nil {
			panic(err)
		}
	}
	if *mode == "publish" {
		err := sendMsg(*message, *projectID, *topicID)
		if err != nil {
			panic(err)
		}
	}
}

func receive(ctx context.Context, event event.Event) error {

	log.Printf("Event Context: %+v\n", event.Context)
	log.Printf("Protocol Context: %+v\n", pscontext.ProtocolContextFrom(ctx))
	log.Printf("%s\n", event.ID())

	switch event.Type() {
	case messagePublishedEventType:

		pubsubData := &pubsubv1.MessagePublishedData{}
		if err := event.DataAs(pubsubData); err != nil {
			fmt.Printf("DataAs Error %v", err)
			return err
		}
		log.Printf("Pubsub MessagePublishedData %s\n", string(pubsubData.Message.Data))

	case pubSubEventType:

		pubsubData := &p.PubsubMessage{}
		if err := event.DataAs(pubsubData); err != nil {
			fmt.Printf("DataAs Error %v", err)
			return err
		}
		log.Printf("Pubsub PubsubMessage %s\n", pubsubData.Data)
	case auditLogEventType:

		//auditData := &auditv1.LogEntryData{}
		auditData := &lg.LogEntry{}
		if err := event.DataAs(auditData); err != nil {
			fmt.Printf("DataAs Error %v", err)
			return fmt.Errorf("Got Data Error: %s\n", err)
		}
		log.Printf("Audit v ResourceName %s\n", auditData.Severity)

	default:
		return errors.New("could not parse Cloud Event TYpe")
	}
	return nil
}

func pullMsgs(projectId, subID string) error {
	t, err := cepubsub.New(context.Background(),
		cepubsub.WithProjectID(projectId),
		cepubsub.WithSubscriptionID(subID))
	if err != nil {
		return err
	}
	c, err := cloudevents.NewClient(t)
	if err != nil {
		return err
	}

	log.Println("Created client, listening...")
	ctx := context.Background()
	if err := c.StartReceiver(ctx, receive); err != nil {
		return err
	}
	return nil
}

func sendMsg(msg string, projectID, topicID string) error {
	t, err := cepubsub.New(context.Background(),
		cepubsub.WithProjectID(projectID),
		cepubsub.WithTopicID(topicID), cepubsub.AllowCreateSubscription(false))
	if err != nil {
		log.Fatalf("failed to create pubsub transport, %s", err.Error())
	}
	c, err := cloudevents.NewClient(t, cloudevents.WithTimeNow(), cloudevents.WithUUIDs())
	if err != nil {
		log.Fatalf("failed to create client, %s", err.Error())
	}
	event := cloudevents.NewEvent()
	event.SetType(pubSubEventType)
	event.SetSource("github.com/cloudevents/sdk-go/samples/pubsub/sender/")
	_ = event.SetData(cloudevents.ApplicationJSON, p.PubsubMessage{
		Data: msg,
	})

	if result := c.Send(context.Background(), event); cloudevents.IsUndelivered(result) {
		log.Printf("failed to send: %v\n", err)
		os.Exit(1)
	} else {
		log.Printf("sent, accepted: %t\n", cloudevents.IsACK(result))
	}
	return nil
}
