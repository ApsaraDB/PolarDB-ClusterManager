package status

import (
	"fmt"
	"gitlab.alibaba-inc.com/RDS_PG/polardb_cluster_manager/pkg/common"
	"os/exec"
	"strings"
	"sync"
	"time"
)

type State struct {
	Name           string                 `json:"name"`
	Reason         []string               `json:"reason,omitempty"`
	Value          map[string]interface{} `json:"value,omitempty"`
	ReceivedEvents int                    `json:"received_events,omitempty"`
	StartTimestamp time.Time              `json:"start_timestamp,omitempty"`
	Mutex          sync.Mutex
}

type TransitionAction interface {
	Transition(s Status, state *State, event common.Event, toStateName string, args ...interface{})
	Name() string
}

// Transition is a state transition and all data are literal values that simplifies FSM usage and make it generic.
type Transition struct {
	From   string
	Event  string
	To     string
	Action TransitionAction
}

// StateMachine is a FSM that can handle transitions of a lot of objects. delegate and transitions are configured before use them.
type StateMachine struct {
	transitions []Transition
}

// Error is an error when processing event and state changing.
type Error interface {
	error
	BadEvent() common.Event
	CurrentState() State
}

type smError struct {
	badEvent     common.Event
	currentState State
}

func (e smError) Error() string {
	return fmt.Sprintf("state machine error: cannot find transition for event [%s] when in state [%s]\n", e.badEvent, e.currentState)
}

func (e smError) BadEvent() common.Event {
	return e.badEvent
}

func (e smError) CurrentState() State {
	return e.currentState
}

// NewStateMachine creates a new state machine.
func NewStateMachine(transitions ...Transition) *StateMachine {
	return &StateMachine{transitions: transitions}
}

// Trigger fires a event. You must pass current state of the processing object, other info about this object can be passed with args.
func (m *StateMachine) Trigger(s Status, state *State, event common.Event, args ...interface{}) Error {
	trans := m.findTransMatching(*state, event.Name())
	if trans == nil {
		return nil
		//return smError{event, *state}
	}

	if trans.Action != nil {
		trans.Action.Transition(s, state, event, trans.To, args)
	}
	return nil
}

// findTransMatching gets corresponding transition according to current state and event.
func (m *StateMachine) findTransMatching(fromState State, event string) *Transition {
	for _, v := range m.transitions {
		if v.From == fromState.Name && v.Event == event {
			return &v
		}
	}
	return nil
}

// Export exports the state diagram into a file.
func (m *StateMachine) Export(outfile string) error {
	return m.ExportWithDetails(outfile, "png", "dot", "72", "-Gsize=10,5 -Gdpi=200")
}

// ExportWithDetails  exports the state diagram with more graphviz options.
func (m *StateMachine) ExportWithDetails(outfile string, format string, layout string, scale string, more string) error {
	dot := `digraph StateMachine {

	rankdir=LR
	node[width=1 fixedsize=true shape=circle style=filled fillcolor="darkorchid1" ]
	
	`

	for _, t := range m.transitions {
		link := fmt.Sprintf(`%s -> %s [label="%s | %s"]`, t.From, t.To, t.Event, t.Action)
		dot = dot + "\r\n" + link
	}

	dot = dot + "\r\n}"
	cmd := fmt.Sprintf("dot -o%s -T%s -K%s -s%s %s", outfile, format, layout, scale, more)

	return system(cmd, dot)
}

func system(c string, dot string) error {
	cmd := exec.Command(`/bin/sh`, `-c`, c)
	cmd.Stdin = strings.NewReader(dot)
	return cmd.Run()

}
