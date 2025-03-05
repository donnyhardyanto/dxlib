package state_diagram

import (
	"fmt"
	"github.com/pkg/errors"
	"time"
	_ "time/tzdata"
)

type State struct {
	NameId string
}

type StateConnection struct {
	FromStateNameId string
	ToStateNameId   string
	ActionNameId    string
}

type ActionHistory struct {
	At              time.Time
	ActionNameId    string
	ActorNameId     string
	FromStateNameId string
	ToStateNameId   string
}

type StateDiagram struct {
	States             []State
	Connections        []StateConnection
	ActionHistory      []ActionHistory
	OnEnterState       func(fromStateNameId string, toStateNameId string, actionNameId string, actorNameId string)
	OnLeaveState       func(fromStateNameId string, toStateNameId string, actionNameId string, actorNameId string)
	OnActionStart      func(fromStateNameId string, toStateNameId string, actionNameId string, actorNameId string)
	OnActionEnd        func(fromStateNameId string, toStateNameId string, actionNameId string, actorNameId string)
	OnNewActionHistory func(actionHistory ActionHistory)
	CurrentState       string
}

type StateDiagramInterface interface {
	SetState(stateNameId string)
	Action(actionNameId string, actorNameId string)
	ActionTo(actionNameId string, actorNameId string, toStateNameId string)
	GetState() string
	GetStateAsStrings() []string
	IsStateNameIdExist(s string) bool
}

func (sd *StateDiagram) IsStateNameIdExist(s string) bool {
	for _, state := range sd.States {
		if state.NameId == s {
			return true
		}
	}
	return false
}

func (sd *StateDiagram) GetStateAsStrings() []string {
	var stateNameIds []string
	for _, state := range sd.States {
		stateNameIds = append(stateNameIds, state.NameId)
	}
	return stateNameIds
}

func (sd *StateDiagram) SetState(stateNameId string) (err error) {
	if !sd.IsStateNameIdExist(stateNameId) {
		return errors.New("STATE_NAME_ID_NOT_FOUND")
	}
	sd.CurrentState = stateNameId
	return nil
}

func (sd *StateDiagram) Action(actionNameId string, actorNameId string) (err error) {
	if sd.CurrentState == "" {
		return errors.New("CURRENT_STATE_NOT_SET")
	}
	for _, connection := range sd.Connections {
		if connection.FromStateNameId == sd.CurrentState && connection.ActionNameId == actionNameId {
			prevState := sd.CurrentState
			if sd.OnActionStart != nil {
				sd.OnActionStart(sd.CurrentState, connection.ToStateNameId, actionNameId, actorNameId)
			}
			if sd.OnLeaveState != nil {
				sd.OnLeaveState(sd.CurrentState, connection.ToStateNameId, actionNameId, actorNameId)
			}
			sd.CurrentState = connection.ToStateNameId
			if sd.OnEnterState != nil {
				sd.OnEnterState(prevState, sd.CurrentState, actionNameId, actorNameId)
			}
			if sd.OnActionEnd != nil {
				sd.OnActionEnd(prevState, sd.CurrentState, actionNameId, actorNameId)
			}
			actionHistory := ActionHistory{
				At:              time.Now(),
				ActionNameId:    actionNameId,
				ActorNameId:     actorNameId,
				FromStateNameId: prevState,
				ToStateNameId:   sd.CurrentState,
			}
			sd.ActionHistory = append(sd.ActionHistory, actionHistory)
			if sd.OnNewActionHistory != nil {
				sd.OnNewActionHistory(actionHistory)
			}
			return nil
		}
	}
	return errors.New("ACTION_NOT_FOUND")
}

func (sd *StateDiagram) GetState() string {
	return sd.CurrentState
}

func NewStateDiagram() *StateDiagram {
	return &StateDiagram{}
}

func test() {
	sd := NewStateDiagram()
	sd.States = append(sd.States, State{NameId: "WAITING_ASSIGNMENT"})
	sd.States = append(sd.States, State{NameId: "IN_PROGRESS"})
	sd.States = append(sd.States, State{NameId: "COMPLETED"})
	sd.States = append(sd.States, State{NameId: "CANCELLED_BY_CUSTOMER"})
	sd.Connections = append(sd.Connections, StateConnection{FromStateNameId: "WAITING_ASSIGNMENT", ToStateNameId: "IN_PROGRESS", ActionNameId: "ASSIGNED"})
	sd.Connections = append(sd.Connections, StateConnection{FromStateNameId: "IN_PROGRESS", ToStateNameId: "COMPLETED", ActionNameId: "FINISH"})
	sd.Connections = append(sd.Connections, StateConnection{FromStateNameId: "IN_PROGRESS", ToStateNameId: "CANCELLED_BY_CUSTOMER", ActionNameId: "CANCEL"})
	sd.Connections = append(sd.Connections, StateConnection{FromStateNameId: "WAITING_ASSIGNMENT", ToStateNameId: "CANCELLED_BY_CUSTOMER", ActionNameId: "CANCEL"})

	err := sd.SetState("WAITING_ASSIGNMENT")
	if err != nil {
		fmt.Printf("%s", err.Error())
	}
	err = sd.Action("ASSIGNED", "SYSTEM")
	if err != nil {
		fmt.Printf("%s", err.Error())
	}
	fmt.Printf("%s", sd.GetState())
	err = sd.Action("FINISH", "SYSTEM")
	if err != nil {
		fmt.Printf("%s", err.Error())
	}
	fmt.Printf("%s", sd.GetState())
	err = sd.Action("CANCEL", "SYSTEM")
	if err != nil {
		fmt.Printf("%s", err.Error())
	}
}
