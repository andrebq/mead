package mead

import (
	"github.com/andrebq/alejson"
	"github.com/nats-io/nats.go"
	"github.com/sirupsen/logrus"

	"github.com/kode4food/ale/data"
)

type (
	// Mailbox contains a subscription to a NATS subject
	Mailbox struct {
		name  string
		group string
		sub   *nats.Subscription

		h MailHandler
	}

	// MailHandler process a message
	MailHandler func(data.Object) data.Object
)

func (m *Mailbox) process(msg *nats.Msg) {
	h := m.h
	if h == nil {
		// TODO: add a metric here to count how msgs were dropped
		return
	}
	val, err := alejson.Unmarshal(string(msg.Data))
	if err != nil {
		// TODO: add another metric here
		return
	}
	obj, ok := val.(data.Object)
	if !ok {
		// TODO: add another metric here
	}
	reply := h(obj)
	if reply == nil {
		return
	}
	replyJSON, err := alejson.Marshal(reply)
	if err != nil {
		// TODO: add another metric here
		return
	}
	err = msg.Respond([]byte(replyJSON))
	if err != nil {
		// TODO: add logging here
		m.logentry().WithField("mailbox", m.name).
			WithField("group", m.group).
			WithError(err).
			WithField("action", "reply")
	}
}

func (m *Mailbox) logentry() *logrus.Entry {
	return logrus.WithField("system", "mead").
		WithField("subsystem", "mailbox")
}
