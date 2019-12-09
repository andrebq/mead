package mead

import (
	"errors"

	"github.com/nats-io/nats.go"
	uuid "github.com/satori/go.uuid"
	"gopkg.in/tomb.v2"
)

type (
	// PO (short for PostOffice) is responsible for managing multple mailboxes
	//
	// Each mailbox is represented by a subscription to a subject topic in a NATS.
	PO struct {
		conn *nats.Conn
		t    tomb.Tomb
	}
)

// NewPO returns a connected PO with some pre-existing subscriptions
func NewPO(url string) (*PO, error) {
	conn, err := nats.Connect(url)
	if err != nil {
		return nil, err
	}
	po := &PO{conn: conn}
	po.t.Go(po.bookkeeping)
	return po, nil
}

// Close the PO box and drains all connections
func (p *PO) Close() error {
	err := errors.New("closed-by-user")
	p.t.Kill(err)
	p.conn.Drain()
	if e := p.t.Wait(); e != err {
		return e
	}
	return nil
}

// AcquireMailbox opens a new subscription for the given name and optionally register
// it with the group information.
//
// Using a group allows for the messages sent to name to be distributed by multiple nodes,
// as long as they belong to the same group.
//
// Not providing a group name defaults to a pub/sub mode where all nodes subscribing to
// the given name will receive copies of the message.
func (p *PO) AcquireMailbox(name, group string, handler MailHandler) (*Mailbox, error) {
	// TODO: add some metric gathering
	// TODO: might add some logic in the future
	return p.createAndSubscribe(name, group, handler)
}

// AcquirePrivateMailbox opens a new subscription for a random name.
//
// This Mailbox is unique therefore there is no negotiation regarding locks or anything
func (p *PO) AcquirePrivateMailbox(group string, handler MailHandler) (*Mailbox, error) {
	return p.createAndSubscribe(uuid.NewV4().String(), group, handler)
}

func (p *PO) createAndSubscribe(name, group string, handler MailHandler) (*Mailbox, error) {
	mb := &Mailbox{name: name, group: group, h: handler}
	var err error
	if len(group) > 0 {
		mb.sub, err = p.conn.QueueSubscribe(name, group, mb.process)
	} else {
		mb.sub, err = p.conn.Subscribe(name, mb.process)
	}
	if err != nil {
		return nil, err
	}
	return mb, nil
}
